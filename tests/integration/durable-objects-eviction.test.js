import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import {
  delay,
  deployAndPromote,
  encodeClientTextFrame,
  gatewayFetch,
  readIntegrationJson,
  readJsonServerFrame,
  recreateDoSingleRuntime,
  responseJson,
  setupIntegrationSuite,
  structuredServiceLogEvents,
  uniqueNs,
  waitUntil,
  withDoMultiRuntimes,
  wsHandshake,
} from "./helpers/index.js";
import {
  doHostId,
  doInternalInvoke,
  redisGetDoOwner,
  redisGetDoStorageId,
  redisSetDoOwner,
} from "./helpers/durable-objects.js";

const DO_EVICTION_WORKER = readFileSync(
  new URL("../../test-workers/do-ws-hibernation/src/index.js", import.meta.url),
  "utf8"
);

/** @param {boolean} preventEviction */
async function waitForDoRuntimeReady(preventEviction) {
  await waitUntil("do-runtime residency config", () => {
    const events = structuredServiceLogEvents("do-runtime", "do_actor_residency_configured");
    return events.at(-1)?.prevent_eviction === preventEviction;
  });
}

/**
 * @param {string} ns
 * @param {{ label?: string, sessionPolicy?: "preserve" | "restart" }} [options]
 */
async function deployProbe(ns, { label = "fixture", sessionPolicy = "preserve" } = {}) {
  return await deployAndPromote(ns, "probe", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_EVICTION_WORKER },
    vars: { EVICTION_BUILD_LABEL: label },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
    ...(sessionPolicy === "restart" ? { sessionPolicy } : {}),
  });
}

/** @param {string} ns @param {string} [name] */
async function readHttpCounter(ns, name = "http") {
  return await readIntegrationJson(
    await gatewayFetch(ns, `/probe/eviction-counter?name=${encodeURIComponent(name)}`),
    200,
    "DO eviction HTTP counter"
  );
}

/**
 * @param {number} startedAt
 * @param {number} durationMs
 */
async function waitForElapsed(startedAt, durationMs) {
  await delay(Math.max(0, startedAt + durationMs - Date.now()));
}

setupIntegrationSuite({
  afterStackUp: async () => {
    await waitForDoRuntimeReady(true);
    await recreateDoSingleRuntime({ preventEviction: false });
    await waitForDoRuntimeReady(false);
  },
});

test.after(async () => {
  await recreateDoSingleRuntime({ preventEviction: true });
  await waitForDoRuntimeReady(true);
});

test("evictable Durable Objects preserve SQLite and quiescent hibernating WebSockets", async () => {
  const httpNs = uniqueNs("do-eviction-http");
  const webSocketNs = uniqueNs("do-eviction-ws");
  await deployProbe(httpNs);
  await deployProbe(webSocketNs);

  assert.deepEqual(await readHttpCounter(httpNs), {
    buildLabel: "fixture",
    memoryHits: 1,
    storageHits: 1,
  });
  assert.deepEqual(await readHttpCounter(httpNs), {
    buildLabel: "fixture",
    memoryHits: 2,
    storageHits: 2,
  });
  const shortIdleStartedAt = Date.now();

  const { status, socket } = await wsHandshake(webSocketNs, "/probe?name=ws&tag=vip");
  try {
    assert.equal(status, 101);
    let message = readJsonServerFrame(socket);
    socket.write(encodeClientTextFrame("eviction-probe"));
    const initial = await message;
    assert.equal(initial.buildLabel, "fixture");
    assert.equal(initial.seen, 1);
    assert.equal(initial.memoryMessages, 1);
    assert.deepEqual(initial.tags, ["vip"]);
    assert.equal(initial.allSockets, 1);
    const webSocketIdleStartedAt = Date.now();

    // Cross workerd's current inactivity shutdown window.
    await waitForElapsed(shortIdleStartedAt, 15_000);
    assert.deepEqual(await readHttpCounter(httpNs), {
      buildLabel: "fixture",
      memoryHits: 1,
      storageHits: 3,
    });
    assert.deepEqual(await readHttpCounter(httpNs), {
      buildLabel: "fixture",
      memoryHits: 2,
      storageHits: 4,
    });
    const longIdleStartedAt = Date.now();

    // Exercise another reconstruction after a longer idle interval. This does
    // not claim to observe the internal ActorContainer cleanup pass itself.
    await waitForElapsed(longIdleStartedAt, 75_000);
    assert.deepEqual(await readHttpCounter(httpNs), {
      buildLabel: "fixture",
      memoryHits: 1,
      storageHits: 5,
    });
    assert.deepEqual(await readHttpCounter(httpNs), {
      buildLabel: "fixture",
      memoryHits: 2,
      storageHits: 6,
    });

    // The gate covers a quiescent socket across more than two current cleanup
    // intervals; it does not cover in-flight frame races during eviction.
    await waitForElapsed(webSocketIdleStartedAt, 150_000);
    message = readJsonServerFrame(socket);
    socket.write(encodeClientTextFrame("eviction-probe"));
    const restored = await message;
    assert.equal(restored.buildLabel, "fixture");
    assert.equal(restored.id, initial.id);
    assert.equal(restored.joinedAt, initial.joinedAt);
    assert.equal(restored.seen, 2);
    assert.equal(restored.memoryMessages, 1);
    assert.deepEqual(restored.tags, ["vip"]);
    assert.equal(restored.allSockets, 1);
  } finally {
    socket.destroy();
  }
});

test("host eviction preserves the facet session-policy fence", async () => {
  const ns = uniqueNs("do-eviction-session-policy");
  const objectName = "policy";
  await deployProbe(ns, { label: "v1" });
  assert.deepEqual(await readHttpCounter(ns, objectName), {
    buildLabel: "v1",
    memoryHits: 1,
    storageHits: 1,
  });

  await delay(15_000);
  await deployProbe(ns, { label: "v2", sessionPolicy: "restart" });
  assert.deepEqual(await readHttpCounter(ns, objectName), {
    buildLabel: "v2",
    memoryHits: 1,
    storageHits: 2,
  });

  const { status, socket } = await wsHandshake(ns, `/probe?name=${objectName}&tag=policy`);
  try {
    assert.equal(status, 101);
    let message = readJsonServerFrame(socket);
    socket.write(encodeClientTextFrame("eviction-probe"));
    const initial = await message;
    assert.equal(initial.buildLabel, "v2");
    assert.equal(initial.seen, 1);

    await delay(15_000);
    assert.deepEqual(await readHttpCounter(ns, objectName), {
      buildLabel: "v2",
      memoryHits: 1,
      storageHits: 3,
    });

    message = readJsonServerFrame(socket);
    socket.write(encodeClientTextFrame("eviction-probe"));
    const afterSecondEviction = await message;
    assert.equal(afterSecondEviction.buildLabel, "v2");
    assert.equal(afterSecondEviction.seen, 2);
  } finally {
    socket.destroy();
  }
});

test("restart session policy fences each evictable runtime's retained native facet", async () => {
  const ns = uniqueNs("do-eviction-owner-roundtrip");
  const objectName = "roundtrip";
  const v1 = await deployProbe(ns, { label: "v1" });
  const doStorageId = redisGetDoStorageId(ns, "probe");
  const ownerKey = doHostId(ns, "probe", "Room", objectName);

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service, /** @type {string} */ version) => (
      doInternalInvoke(service, {
        ns,
        worker: "probe",
        version,
        doStorageId,
        className: "Room",
        objectName,
        request: {
          method: "GET",
          url: "https://do.internal/eviction-counter",
          headers: {},
        },
      })
    );
    const expireOwner = (/** @type {string} */ taskId) => {
      const owner = redisGetDoOwner(ownerKey);
      assert.equal(owner.taskId, taskId);
      redisSetDoOwner(ownerKey, { ...owner, leaseExpiresAt: Date.now() - 1000 });
    };

    const first = invoke("do-runtime-a", v1);
    assert.equal(first.status, 200, first.body);
    assert.deepEqual(responseJson(first), {
      buildLabel: "v1",
      memoryHits: 1,
      storageHits: 1,
    });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-a");

    await delay(15_000);
    const v2 = await deployProbe(ns, { label: "v2", sessionPolicy: "restart" });

    expireOwner("do-runtime-a");
    const onB = invoke("do-runtime-b", v2);
    assert.equal(onB.status, 200, onB.body);
    assert.deepEqual(responseJson(onB), {
      buildLabel: "v2",
      memoryHits: 1,
      storageHits: 2,
    });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-b");

    expireOwner("do-runtime-b");
    const backOnA = invoke("do-runtime-a", v2);
    assert.equal(backOnA.status, 200, backOnA.body);
    assert.deepEqual(responseJson(backOnA), {
      buildLabel: "v2",
      memoryHits: 1,
      storageHits: 3,
    });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-a");
  }, {
    preventEviction: false,
    renewStartDelayMs: 600_000,
    renewIntervalMs: 600_000,
  });
});
