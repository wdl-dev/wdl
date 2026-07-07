import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  composeStart,
  composeRecreate,
  composeRestart,
  composeStop,
  assertStatus,
  deployAndPromote,
  encodeClientCloseFrame,
  encodeClientTextFrame,
  envoyStat,
  gatewayFetch,
  gatewayUrl,
  frameJson,
  readJsonServerFrame,
  readOneServerTextFrame,
  serviceInternalGet,
  serviceInternalPost,
  serviceWebSocketRoundTrip,
  uniqueNs,
  waitUntil,
  withDoMultiRuntimes,
  withDoOwnerTask,
  wsHandshake,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import { prometheusCounter } from "./helpers/prometheus.js";
import { doHostId, redisGetDoStorageId, redisSetDoOwner } from "./helpers/durable-objects.js";

setupIntegrationSuite();

const DO_WS_WORKER = readFileSync(
  new URL("../../test-workers/do-ws/src/index.js", import.meta.url),
  "utf8"
);
const DO_WS_PUSH_WORKER = readFileSync(
  new URL("../../test-workers/do-ws-push/src/index.js", import.meta.url),
  "utf8"
);
const DO_WS_HIBERNATION_WORKER = readFileSync(
  new URL("../../test-workers/do-ws-hibernation/src/index.js", import.meta.url),
  "utf8"
);

function doRemoteOwnerResolutions() {
  const metrics = serviceInternalGet("do-runtime", 8788, "/_metrics").body;
  return prometheusCounter(metrics, "wdl_do_owner_resolutions_total", {
    service: "do-runtime",
    outcome: "remote",
  });
}

/** @param {string} service */
function doWebSocketUpgradeOk(service) {
  const metrics = serviceInternalGet(service, 8788, "/_metrics").body;
  return prometheusCounter(metrics, "wdl_do_websocket_upgrades_total", {
    service: "do-runtime",
    outcome: "ok",
  });
}

/** @param {string} outcome */
async function gatewayWebSocketProxyCount(outcome) {
  const metrics = await (await fetch(gatewayUrl("/_metrics"))).text();
  return prometheusCounter(metrics, "wdl_websocket_proxies_total", {
    service: "gateway",
    outcome,
  });
}

async function gatewayWebSocketProxyEstablished() {
  return await gatewayWebSocketProxyCount("established");
}

/** @param {string} outcome */
async function gatewayWebSocketSessionLifetimeCount(outcome) {
  const metrics = await (await fetch(gatewayUrl("/_metrics"))).text();
  return prometheusCounter(metrics, "wdl_websocket_session_lifetime_ms_count", {
    service: "gateway",
    outcome,
  });
}

test("Durable Object WebSocket upgrade flows through gateway, Envoy, user-runtime, and do-runtime", async () => {
  const ns = uniqueNs("do-ws");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const beforeUserEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const beforeDoEnvoy = envoyStat("cluster.do_router.upstream_rq_total");
  const beforeGatewayProxy = await gatewayWebSocketProxyEstablished();
  const { status, headers, socket, head } = await wsHandshake(ns, "/chat?name=alice");
  try {
    assert.equal(status, 101);
    assert.equal((headers.upgrade || "").toLowerCase(), "websocket");
    assert.equal((headers.connection || "").toLowerCase(), "upgrade");
    assert.ok(headers["x-request-id"], "x-request-id should land on the DO 101 response");
    assert.equal(head.length, 0, "no frame data should precede our send");

    const first = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("hello"));
    assert.deepEqual(frameJson(await first), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      text: "hello",
    });

    const second = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("again"));
    assert.deepEqual(frameJson(await second), {
      objectId: "alice",
      memory: 2,
      storage: 2,
      text: "again",
    });

    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") > beforeUserEnvoy,
      "gateway should reach user-runtime through Envoy for DO websocket upgrades"
    );
    assert.ok(
      envoyStat("cluster.do_router.upstream_rq_total") > beforeDoEnvoy,
      "user-runtime should reach do-runtime through Envoy for DO websocket upgrades"
    );
    assert.ok(
      await gatewayWebSocketProxyEstablished() > beforeGatewayProxy,
      "gateway should hold the external DO websocket while proxying to the runtime path"
    );
  } finally {
    socket.destroy();
  }
});

test("gateway-held Durable Object WebSocket reconnects backend after do-runtime restart", async () => {
  const ns = uniqueNs("do-ws-reconnect");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const beforeUserEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const { status, socket } = await wsHandshake(ns, "/chat?name=restart");
  try {
    assert.equal(status, 101);

    const first = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("before"));
    assert.deepEqual(frameJson(await first), {
      objectId: "restart",
      memory: 1,
      storage: 1,
      text: "before",
    });

    composeRecreate("do-runtime");

    const second = readOneServerTextFrame(socket, { timeoutMs: 10_000 });
    socket.write(encodeClientTextFrame("after-1"));
    assert.deepEqual(frameJson(await second), {
      objectId: "restart",
      memory: 1,
      storage: 2,
      text: "after-1",
    });
    socket.write(encodeClientTextFrame("after-2"));
    assert.deepEqual(await readJsonServerFrame(socket), {
      objectId: "restart",
      memory: 2,
      storage: 3,
      text: "after-2",
    });
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should keep the client DO websocket open while replacing its backend websocket"
    );
    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") >= beforeUserEnvoy + 2,
      "DO websocket reconnect should establish a second backend request through the user-runtime Envoy cluster"
    );
    assert.ok(
      doWebSocketUpgradeOk("do-runtime") >= 1,
      "DO websocket reconnect should establish a backend upgrade on the recreated do-runtime"
    );
  } finally {
    socket.destroy();
  }
});

test("gateway-held Durable Object WebSocket proactively reconnects backend for server-pushed frames", async () => {
  const ns = uniqueNs("do-ws-push-reconnect");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_PUSH_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const beforeUserEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const { status, socket } = await wsHandshake(ns, "/chat?name=push");
  try {
    assert.equal(status, 101);
    assert.deepEqual(await readJsonServerFrame(socket), {
      objectId: "push",
      memory: 1,
      storage: 1,
      text: "open",
    });

    composeRecreate("do-runtime");

    assert.deepEqual(await readJsonServerFrame(socket, { timeoutMs: 15_000 }), {
      objectId: "push",
      memory: 1,
      storage: 2,
      text: "open",
    });
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should proactively reconnect the backend DO websocket after upstream close"
    );
    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") >= beforeUserEnvoy + 2,
      "proactive DO websocket reconnect should establish a second user-runtime request through Envoy"
    );
    assert.ok(
      doWebSocketUpgradeOk("do-runtime") >= 1,
      "proactive DO websocket reconnect should establish a backend upgrade on the recreated do-runtime"
    );
  } finally {
    socket.destroy();
  }
});

test("Durable Object hibernation WebSocket API round-trips through the gateway-held path", async () => {
  const ns = uniqueNs("do-ws-hibernate-api");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_HIBERNATION_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const { status, socket } = await wsHandshake(ns, "/chat?name=hibernating");
  try {
    assert.equal(status, 101);
    socket.write(encodeClientTextFrame("ping"));
    assert.equal(await readOneServerTextFrame(socket), "pong");

    socket.write(encodeClientTextFrame("hello"));
    assert.deepEqual(await readJsonServerFrame(socket), {
      id: "hibernating",
      joinedAt: 123,
      seen: 0,
      tags: ["room"],
      roomSockets: 1,
      vipSockets: 0,
      allSockets: 1,
      text: "hello",
    });
  } finally {
    socket.destroy();
  }
});

test("Durable Object hibernation tracks tags, attachment mutation, and auto-response", async () => {
  const ns = uniqueNs("do-ws-hibernate-tags");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_HIBERNATION_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const autoResponse = await gatewayFetch(ns, "/chat/auto-response?name=multi");
  assert.equal(autoResponse.status, 200);
  assert.deepEqual(await responseJson(autoResponse), {
    request: "ping",
    response: "pong",
  });

  const first = await wsHandshake(ns, "/chat?name=multi&tag=room&tag=vip");
  const second = await wsHandshake(ns, "/chat?name=multi&tag=room");
  try {
    assert.equal(first.status, 101);
    assert.equal(second.status, 101);

    first.socket.write(encodeClientTextFrame("bump"));
    assert.deepEqual(await readJsonServerFrame(first.socket), {
      id: "multi",
      seen: 1,
      tags: ["room", "vip"],
    });
    first.socket.write(encodeClientTextFrame("bump"));
    assert.deepEqual(await readJsonServerFrame(first.socket), {
      id: "multi",
      seen: 2,
      tags: ["room", "vip"],
    });

    second.socket.write(encodeClientTextFrame("hello"));
    assert.deepEqual(await readJsonServerFrame(second.socket), {
      id: "multi",
      joinedAt: 123,
      seen: 0,
      tags: ["room"],
      roomSockets: 2,
      vipSockets: 1,
      allSockets: 2,
      text: "hello",
    });

  } finally {
    first.socket.destroy();
    second.socket.destroy();
  }
});

test("gateway-held Durable Object hibernation WebSocket reconnects backend after do-runtime restart", async () => {
  const ns = uniqueNs("do-ws-hibernate-reconnect");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_HIBERNATION_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const { status, socket } = await wsHandshake(ns, "/chat?name=hibernating-restart");
  try {
    assert.equal(status, 101);
    socket.write(encodeClientTextFrame("before"));
    assert.deepEqual(await readJsonServerFrame(socket), {
      id: "hibernating-restart",
      joinedAt: 123,
      seen: 0,
      tags: ["room"],
      roomSockets: 1,
      vipSockets: 0,
      allSockets: 1,
      text: "before",
    });

    composeRecreate("do-runtime");

    socket.write(encodeClientTextFrame("after"));
    assert.deepEqual(await readJsonServerFrame(socket, { timeoutMs: 10_000 }), {
      id: "hibernating-restart",
      joinedAt: 123,
      seen: 0,
      tags: ["room"],
      roomSockets: 1,
      vipSockets: 0,
      allSockets: 1,
      text: "after",
    });
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should reconnect the backend hibernation websocket after do-runtime restart"
    );
  } finally {
    socket.destroy();
  }
});

test("Durable Object hibernation WebSocket close handler observes attachments", async () => {
  const ns = uniqueNs("do-ws-hibernate-close");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_HIBERNATION_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const beforeClientClosedSessions = await gatewayWebSocketSessionLifetimeCount("client_closed");
  const { status, socket } = await wsHandshake(ns, "/chat?name=closer");
  try {
    assert.equal(status, 101);
    socket.write(encodeClientCloseFrame(1000, "done"));
    await waitUntil("DO hibernation close handler storage update", async () => {
      const response = await gatewayFetch(ns, "/chat/status?name=closer");
      assert.equal(response.status, 200);
      const body = await responseJson(response);
      assert.deepEqual(body, {
        code: 1000,
        reason: "done",
        clean: true,
        attachmentId: "closer",
      });
      return true;
    }, { timeoutMs: 8000 });
    await waitUntil("gateway DO websocket session lifetime metric", async () => {
      assert.ok(
        await gatewayWebSocketSessionLifetimeCount("client_closed") > beforeClientClosedSessions,
        "gateway should report DO websocket session lifetime after client close"
      );
      return true;
    }, { timeoutMs: 8000 });
  } finally {
    socket.destroy();
  }
});

test("Durable Object WebSocket owner hint connects directly to the owner endpoint", async () => {
  const ns = uniqueNs("do-ws-hint");
  await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "chat");
  const ownerKey = doHostId(ns, "chat", "Room", "hinted");

  let schedulerStopped = false;
  await withDoOwnerTask(async () => {
    try {
      redisSetDoOwner(ownerKey, {
        ownerKey,
        hostId: ownerKey,
        ns,
        worker: "chat",
        doStorageId,
        className: "Room",
        taskId: "do-runtime-a",
        endpoint: "do-runtime-a:8788",
        generation: 21,
        leaseExpiresAt: Date.now() + 60_000,
      });
      composeStop("scheduler");
      schedulerStopped = true;
      composeRestart("envoy");

      const beforeDoEnvoy = envoyStat("cluster.do_router.upstream_rq_total");
      const beforeRemote = doRemoteOwnerResolutions();
      const beforeOwnerUpgrade = doWebSocketUpgradeOk("do-runtime-a");
      const { status, socket } = await wsHandshake(ns, "/chat?name=hinted");
      try {
        assert.equal(status, 101);
        const first = readOneServerTextFrame(socket);
        socket.write(encodeClientTextFrame("owner-hint"));
        assert.deepEqual(frameJson(await first), {
          objectId: "hinted",
          memory: 1,
          storage: 1,
          text: "owner-hint",
        });
        assert.ok(
          doRemoteOwnerResolutions() > beforeRemote,
          "first WebSocket router hop should land on a non-owner and return an owner hint"
        );
        assert.ok(
          doWebSocketUpgradeOk("do-runtime-a") > beforeOwnerUpgrade,
          "owner-hinted WebSocket should establish the final upgrade on the owner task"
        );
        assert.equal(
          envoyStat("cluster.do_router.upstream_rq_total"),
          beforeDoEnvoy + 1,
          "owner-hinted WebSocket should use Envoy only for the first router hint hop"
        );
      } finally {
        socket.destroy();
      }
    } finally {
      if (schedulerStopped) {
        composeStart("scheduler");
      }
    }
  });
});

test("do-runtime replicas forward Durable Object WebSocket upgrades and recover after drain", async () => {
  const ns = uniqueNs("do-ws-multi");
  const version = await deployAndPromote(ns, "chat", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WS_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  await withDoMultiRuntimes(async () => {
    const doStorageId = redisGetDoStorageId(ns, "chat");
    const ownerKey = doHostId(ns, "chat", "Room", "alice");
    redisSetDoOwner(ownerKey, {
      ownerKey,
      hostId: ownerKey,
      ns,
      worker: "chat",
      doStorageId,
      className: "Room",
      taskId: "do-runtime-a",
      endpoint: "do-runtime-a:8788",
      generation: 10,
      leaseExpiresAt: Date.now() + 60_000,
    });
    const headers = {
      "x-wdl-do-ns": ns,
      "x-wdl-do-worker": "chat",
      "x-wdl-do-version": version,
      "x-wdl-do-storage-id": doStorageId,
      "x-wdl-do-class-name": "Room",
      "x-wdl-do-object-name": "alice",
      "x-wdl-do-request-url": "https://do.internal/connect",
      "x-request-id": `do-ws-${ns}`,
    };

    const owner = serviceWebSocketRoundTrip(
      "do-runtime-a",
      8788,
      "/internal/do/connect",
      headers,
      "owner"
    );
    assert.equal(owner.status, 101);
    assert.deepEqual(frameJson(owner.frameText), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      text: "owner",
    });

    const drainIdle = serviceInternalPost("do-runtime-c", 8788, "/internal/do/drain", {});
    assert.equal(drainIdle.status, 200, drainIdle.body);
    assert.equal(responseJson(drainIdle).released, 0);

    const forwardedFromDraining = serviceWebSocketRoundTrip(
      "do-runtime-c",
      8788,
      "/internal/do/connect",
      headers,
      "forwarded-from-draining"
    );
    assert.equal(forwardedFromDraining.status, 101);
    assert.deepEqual(frameJson(forwardedFromDraining.frameText), {
      objectId: "alice",
      memory: 2,
      storage: 2,
      text: "forwarded-from-draining",
    });

    const forwarded = serviceWebSocketRoundTrip(
      "do-runtime-b",
      8788,
      "/internal/do/connect",
      headers,
      "forwarded"
    );
    assert.equal(forwarded.status, 101);
    assert.deepEqual(frameJson(forwarded.frameText), {
      objectId: "alice",
      memory: 3,
      storage: 3,
      text: "forwarded",
    });

    const drain = serviceInternalPost("do-runtime-a", 8788, "/internal/do/drain", {});
    assert.equal(drain.status, 200, drain.body);
    assert.equal(responseJson(drain).released, 1);

    const drainedOwner = serviceWebSocketRoundTrip(
      "do-runtime-a",
      8788,
      "/internal/do/connect",
      headers,
      "drained-owner"
    );
    assertStatus(drainedOwner, 503, "drained DO owner websocket", drainedOwner);

    const afterDrain = serviceWebSocketRoundTrip(
      "do-runtime-b",
      8788,
      "/internal/do/connect",
      headers,
      "after-drain"
    );
    assert.equal(afterDrain.status, 101);
    assert.deepEqual(frameJson(afterDrain.frameText), {
      objectId: "alice",
      memory: 1,
      storage: 4,
      text: "after-drain",
    });
  });
});
