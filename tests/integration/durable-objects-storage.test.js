import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  composeRecreate,
  composeRestart,
  deployAndPromote,
  gatewayFetch,
  uniqueNs,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import {
  DO_WORKER,
  doVersionWorker,
  redisGetDoStorageId,
} from "./helpers/durable-objects.js";
import { redisKeys } from "./helpers/redis.js";

setupIntegrationSuite();

test("Durable Object SQLite storage survives do-runtime restart while memory resets", async () => {
  const ns = uniqueNs("do-restart");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });

  const before = await gatewayFetch(ns, "/counter?name=alice");
  const beforeText = await before.text();
  assert.equal(before.status, 200, beforeText);
  assert.deepEqual(responseJson({ body: beforeText }), {
    objectId: "alice",
    memory: 1,
    storage: 1,
    body: "from-worker",
  });

  composeRestart("do-runtime");

  const after = await gatewayFetch(ns, "/counter?name=alice");
  const afterText = await after.text();
  assert.equal(after.status, 200, afterText);
  assert.deepEqual(responseJson({ body: afterText }), {
    objectId: "alice",
    memory: 1,
    storage: 2,
    body: "from-worker",
  });
});

test("default session policy preserves the loaded class until host actor restart", async () => {
  const ns = uniqueNs("do-version");
  await deployAndPromote(ns, "versioned", {
    mainModule: "worker.js",
    modules: { "worker.js": doVersionWorker("v1") },
    bindings: {
      VERSIONED: { type: "do", className: "Versioned" },
    },
  });

  const first = await gatewayFetch(ns, "/versioned");
  const firstText = await first.text();
  assert.equal(first.status, 200, firstText);
  assert.deepEqual(responseJson({ body: firstText }), { label: "v1", memory: 1, storage: 1 });

  await deployAndPromote(ns, "versioned", {
    mainModule: "worker.js",
    modules: { "worker.js": doVersionWorker("v2") },
    bindings: {
      VERSIONED: { type: "do", className: "Versioned" },
    },
  });

  const stillLoaded = await gatewayFetch(ns, "/versioned");
  const stillLoadedText = await stillLoaded.text();
  assert.equal(stillLoaded.status, 200, stillLoadedText);
  assert.deepEqual(responseJson({ body: stillLoadedText }), { label: "v1", memory: 2, storage: 2 });

  composeRecreate("do-runtime");
  const afterRestart = await gatewayFetch(ns, "/versioned");
  const afterRestartText = await afterRestart.text();
  assert.equal(afterRestart.status, 200, afterRestartText);
  assert.deepEqual(responseJson({ body: afterRestartText }), { label: "v2", memory: 1, storage: 3 });
});

test("a restart session policy replaces the loaded Durable Object class and preserves SQLite", async () => {
  const ns = uniqueNs("do-session-restart");
  await deployAndPromote(ns, "versioned", {
    mainModule: "worker.js",
    modules: { "worker.js": doVersionWorker("v1") },
    bindings: {
      VERSIONED: { type: "do", className: "Versioned" },
    },
  });

  const first = await gatewayFetch(ns, "/versioned");
  const firstText = await first.text();
  assert.equal(first.status, 200, firstText);
  assert.deepEqual(responseJson({ body: firstText }), { label: "v1", memory: 1, storage: 1 });

  await deployAndPromote(ns, "versioned", {
    mainModule: "worker.js",
    modules: { "worker.js": doVersionWorker("v2") },
    bindings: {
      VERSIONED: { type: "do", className: "Versioned" },
    },
    sessionPolicy: "restart",
  });

  const restarted = await gatewayFetch(ns, "/versioned");
  const restartedText = await restarted.text();
  assert.equal(restarted.status, 200, restartedText);
  assert.deepEqual(responseJson({ body: restartedText }), { label: "v2", memory: 1, storage: 2 });
});

test("worker delete soft-deletes registered Durable Object SQLite storage", async () => {
  const ns = uniqueNs("do-storage-delete");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });

  const objectNames = Array.from({ length: 10 }, (_, index) => `room-${index}`);
  for (const name of objectNames) {
    const response = await gatewayFetch(ns, `/counter?name=${name}`);
    assert.equal(response.status, 200, await response.text());
  }
  const second = await gatewayFetch(ns, "/counter?name=room-0");
  const secondJson = responseJson({ body: await second.text() });
  assert.equal(second.status, 200);
  assert.equal(secondJson.storage, 2);
  const firstStorageId = redisGetDoStorageId(ns, "counter");
  assert.match(firstStorageId, /^do_[a-f0-9]{32}$/);
  assert.notDeepEqual(redisKeys(`do:objects:${firstStorageId}`), []);

  const deleted = await adminFetch(`/ns/${ns}/worker/counter/delete`, { method: "POST" });
  const deletedText = await deleted.text();
  assert.equal(deleted.status, 200, deletedText);
  const deletedJson = responseJson({ body: deletedText });
  assert.deepEqual(deletedJson.durableObjects.storageRetention, {
    retained: true,
    objects: objectNames.length,
    doStorageId: firstStorageId,
  });
  assert.notDeepEqual(redisKeys(`do:objects:${firstStorageId}`), []);

  composeRecreate("do-runtime");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const secondStorageId = redisGetDoStorageId(ns, "counter");
  assert.match(secondStorageId, /^do_[a-f0-9]{32}$/);
  assert.notEqual(secondStorageId, firstStorageId);
  const afterRedeploy = await gatewayFetch(ns, "/counter?name=room-0");
  const afterRedeployText = await afterRedeploy.text();
  assert.equal(afterRedeploy.status, 200, afterRedeployText);
  assert.deepEqual(responseJson({ body: afterRedeployText }), {
    objectId: "room-0",
    memory: 1,
    storage: 1,
    body: "from-worker",
  });
  const afterRedeploySecondBatch = await gatewayFetch(ns, "/counter?name=room-9");
  const afterRedeploySecondBatchText = await afterRedeploySecondBatch.text();
  assert.equal(afterRedeploySecondBatch.status, 200, afterRedeploySecondBatchText);
  assert.equal(responseJson({ body: afterRedeploySecondBatchText }).storage, 1);
});
