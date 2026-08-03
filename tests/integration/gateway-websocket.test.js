import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import {
  adminPost,
  composeRecreate,
  delay,
  deployAndPromote,
  encodeClientBinaryFrame,
  encodeClientCloseFrame,
  encodeClientCloseFrameWithoutStatus,
  encodeClientTextFrame,
  envoyStat,
  gatewayFetch,
  parseStdoutJson,
  readOneServerBinaryFrame,
  readOneServerCloseFrame,
  readOneServerTextFrame,
  responseJson,
  sh,
  hostWsHandshake,
  gatewayUrl,
  wsHandshake,
  GATEWAY_HOST,
  GATEWAY_PORT,
  uniqueNs,
  setupIntegrationSuite,
  waitUntil,
} from "./helpers/index.js";
import { prometheusCounter } from "./helpers/prometheus.js";

setupIntegrationSuite();

const GATEWAY_HANG_PATTERN = /had hung and would never generate a response/i;
const TRACKED_WEBSOCKET_WORKER = readFileSync(
  new URL("../../test-workers/ws-lifecycle/src/index.js", import.meta.url),
  "utf8"
);

/** @param {string} version */
function versionedWebSocketWorker(version) {
  return `
    const version = ${JSON.stringify(version)};
    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response(version);
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        server.addEventListener("message", (evt) => {
          server.send(version + ":" + evt.data);
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
}

/**
 * @typedef {{
 *   active: number,
 *   opened: number,
 *   closed: number,
 *   lastClose: { code: number | null, reason: string } | null,
 * }} TrackedWebSocketState
 * @typedef {{
 *   socketFds: number,
 *   closeWait: number,
 *   processStartTicks: string,
 *   socketInodes: string[],
 * }} GatewaySocketStats
 * @typedef {{ baselineInodes: Set<string>, trackedInodes: Set<string> }} GatewaySocketTracker
 */
const GATEWAY_SOCKET_PROBE = `
  import { readdirSync, readFileSync, readlinkSync } from "node:fs";

  const socketInodes = [];
  for (const fd of readdirSync("/proc/1/fd")) {
    try {
      const match = readlinkSync("/proc/1/fd/" + fd).match(/^socket:\\[(\\d+)\\]$/);
      if (match) socketInodes.push(match[1]);
    } catch {}
  }

  let closeWait = 0;
  for (const table of ["/proc/1/net/tcp", "/proc/1/net/tcp6"]) {
    const rows = readFileSync(table, "utf8").trimEnd().split("\\n").slice(1);
    for (const row of rows) {
      if (row.trim().split(/\\s+/)[3] === "08") closeWait += 1;
    }
  }

  const stat = readFileSync("/proc/1/stat", "utf8");
  const processStartTicks = stat.slice(stat.lastIndexOf(") ") + 2).trim().split(/\\s+/)[19];
  process.stdout.write(JSON.stringify({
    socketFds: socketInodes.length,
    closeWait,
    processStartTicks,
    socketInodes: socketInodes.sort(),
  }) + "\\n");
`;

/** @returns {GatewaySocketStats} */
function gatewaySocketStats() {
  const containerId = sh(["docker", "compose", "ps", "-q", "gateway"]).trim();
  assert.match(containerId, /^[0-9a-f]{12,64}$/i, "gateway container id");
  const output = sh(
    [
      "docker",
      "run",
      "-i",
      "--rm",
      "--pull=never",
      "--network=none",
      `--pid=container:${containerId}`,
      "node:24-slim",
      "node",
      "--input-type=module",
    ],
    { input: GATEWAY_SOCKET_PROBE }
  );
  const stats = /** @type {GatewaySocketStats} */ (
    parseStdoutJson(output, "gateway socket probe stdout")
  );
  assert.equal(stats.socketFds, stats.socketInodes.length, "gateway socket probe fd count");
  return stats;
}

async function gatewaySocketBaseline() {
  // Let preparatory HTTP metric/state reads drop their keep-alive sockets,
  // then sample across Docker health checks to find the process socket floor.
  await delay(5_000);
  /** @type {GatewaySocketStats | null} */
  let baseline = null;
  let processStartTicks = "";
  for (let sample = 0; sample < 8; sample += 1) {
    const current = gatewaySocketStats();
    if (processStartTicks) {
      assert.equal(current.processStartTicks, processStartTicks, "gateway restarted during baseline");
    } else {
      processStartTicks = current.processStartTicks;
    }
    if (current.closeWait === 0 && (!baseline || current.socketFds < baseline.socketFds)) {
      baseline = current;
    }
    if (sample < 7) await delay(250);
  }
  assert.ok(baseline, "gateway should reach a baseline without CLOSE_WAIT sockets");
  return baseline;
}

/**
 * @param {GatewaySocketStats} baseline
 * @param {GatewaySocketStats} active
 * @param {number} minimumNewSockets
 * @returns {GatewaySocketTracker}
 */
function gatewaySocketTracker(baseline, active, minimumNewSockets) {
  assert.equal(active.processStartTicks, baseline.processStartTicks);
  const baselineInodes = new Set(baseline.socketInodes);
  const trackedInodes = new Set(
    active.socketInodes.filter((inode) => !baselineInodes.has(inode))
  );
  assert.ok(
    trackedInodes.size >= minimumNewSockets,
    `gateway socket probe observed ${trackedInodes.size}, expected at least ${minimumNewSockets}`
  );
  return { baselineInodes, trackedInodes };
}

/**
 * @param {GatewaySocketStats} baseline
 * @param {GatewaySocketTracker} tracker
 * @param {string} label
 */
async function waitForGatewaySocketsReleased(baseline, tracker, label) {
  let stableSamples = 0;
  let current = gatewaySocketStats();
  /** @type {string[]} */
  let retainedInodes = [];
  try {
    await waitUntil(label, () => {
      current = gatewaySocketStats();
      assert.equal(
        current.processStartTicks,
        baseline.processStartTicks,
        "gateway restarted while releasing websocket sockets"
      );
      for (const inode of current.socketInodes) {
        if (!tracker.baselineInodes.has(inode)) tracker.trackedInodes.add(inode);
      }
      retainedInodes = current.socketInodes.filter((inode) => tracker.trackedInodes.has(inode));
      if (
        current.closeWait === 0 &&
        current.socketFds <= baseline.socketFds &&
        retainedInodes.length === 0
      ) {
        stableSamples += 1;
      } else {
        stableSamples = 0;
      }
      return stableSamples >= 2;
    }, { timeoutMs: 15_000, intervalMs: 250 });
  } catch (err) {
    throw new Error(
      `${label}: baseline=${baseline.socketFds}, current=${current.socketFds}, ` +
        `CLOSE_WAIT=${current.closeWait}, retained=${retainedInodes.join(",") || "none"}`,
      { cause: err }
    );
  }
}

/** @param {string} since */
async function assertNoGatewayHangSince(since) {
  // Historical lifecycle failures reached abortFromHang within 500 ms. Leave
  // the Gateway idle before reading logs so this check cannot mask the signal.
  await delay(1_000);
  const logs = sh(["docker", "compose", "logs", "--no-color", `--since=${since}`, "gateway"]);
  assert.doesNotMatch(logs, GATEWAY_HANG_PATTERN);
}

/** @param {import("node:net").Socket} socket */
function waitForSocketClose(socket) {
  if (socket.destroyed) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for websocket TCP close"));
    }, 5_000);
    function cleanup() {
      clearTimeout(timer);
      socket.off("close", onClose);
      socket.off("error", onError);
    }
    function onClose() {
      cleanup();
      resolve(undefined);
    }
    /** @param {unknown} err */
    function onError(err) {
      cleanup();
      reject(err);
    }
    socket.on("close", onClose);
    socket.on("error", onError);
  });
}

/** @param {string} ns @param {string} name @returns {Promise<TrackedWebSocketState>} */
async function trackedWebSocketState(ns, name) {
  const response = await gatewayFetch(ns, `/${name}`);
  assert.equal(response.status, 200, "tracked websocket state response");
  return /** @type {TrackedWebSocketState} */ (responseJson(response));
}

/** @param {string} ns @param {string} name @param {string} payload */
async function openEchoWebSocket(ns, name, payload) {
  const { status, socket } = await wsHandshake(ns, `/${name}`);
  socket.on("error", () => {});
  try {
    assert.equal(status, 101);
    const echoed = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame(payload));
    assert.equal(await echoed, `echo:${payload}`);
    return socket;
  } catch (err) {
    socket.destroy();
    throw err;
  }
}


/** @param {string} outcome */
async function gatewayWebSocketProxyCount(outcome) {
  const body = await (await fetch(gatewayUrl("/_metrics"))).text();
  return prometheusCounter(body, "wdl_websocket_proxies_total", {
    service: "gateway",
    outcome,
  });
}

async function gatewayWebSocketProxyEstablished() {
  return await gatewayWebSocketProxyCount("established");
}

/** @param {string} state */
async function gatewayWebSocketProxyConnections(state) {
  const body = await (await fetch(gatewayUrl("/_metrics"))).text();
  return prometheusCounter(body, "wdl_websocket_proxy_connections", {
    service: "gateway",
    state,
  });
}

/** @param {string} label */
async function waitForNoActiveGatewayWebSockets(label) {
  await waitUntil(
    label,
    async () => (await gatewayWebSocketProxyConnections("active")) === 0,
    { timeoutMs: 10_000, intervalMs: 100 }
  );
}

/** @param {string} outcome */
async function gatewayWebSocketSessionLifetimeCount(outcome) {
  const body = await (await fetch(gatewayUrl("/_metrics"))).text();
  return prometheusCounter(body, "wdl_websocket_session_lifetime_ms_count", {
    service: "gateway",
    outcome,
  });
}

test("ws upgrade: client ⇄ gateway ⇄ runtime ⇄ loaded worker echoes text and binary frames", async () => {
  const ns = uniqueNs("ws");
  const name = "echo";
  const code = `
    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response("need upgrade", { status: 426 });
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.binaryType = "arraybuffer";
        server.accept();
        server.addEventListener("message", (evt) => {
          if (evt.data instanceof ArrayBuffer) {
            server.send(evt.data);
            return;
          }
          server.send("echo:" + evt.data);
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  const beforeEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const beforeProxy = await gatewayWebSocketProxyEstablished();
  const beforeActiveConnections = await gatewayWebSocketProxyConnections("active");
  const { status, headers, socket, head } = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(status, 101);
    assert.equal((headers.upgrade || "").toLowerCase(), "websocket");
    assert.equal((headers.connection || "").toLowerCase(), "upgrade");
    assert.ok(headers["x-request-id"], "x-request-id should land on the 101 response");
    assert.equal(head.length, 0, "no frame data should precede our send");

    const received = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("hello"));
    assert.equal(await received, "echo:hello");

    const binary = Buffer.from([0, 1, 127, 128, 255]);
    const binaryReceived = readOneServerBinaryFrame(socket);
    socket.write(encodeClientBinaryFrame(binary));
    assert.deepEqual(await binaryReceived, binary);
    const afterEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
    assert.ok(afterEnvoy > beforeEnvoy, "gateway should reach user-runtime through Envoy for websocket upgrades");
    assert.ok(
      await gatewayWebSocketProxyEstablished() > beforeProxy,
      "gateway should proxy the external websocket to runtime"
    );
    assert.ok(
      await gatewayWebSocketProxyConnections("active") > beforeActiveConnections,
      "gateway should report the active held websocket"
    );
  } finally {
    socket.destroy();
  }
});

test("ws close without a status remains status-free across gateway and runtime", async () => {
  const ns = uniqueNs("ws-no-status-close");
  const name = "echo";
  const code = `
    let observedClose = null;

    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return Response.json(observedClose);
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        server.addEventListener("close", (evt) => {
          observedClose = { code: evt.code, reason: evt.reason };
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  const { status, socket } = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(status, 101);
    const reciprocalClose = readOneServerCloseFrame(socket);
    socket.write(encodeClientCloseFrameWithoutStatus());
    assert.deepEqual(await reciprocalClose, { code: null, reason: "" });

    /** @type {{ code: number, reason: string } | null} */
    let backendClose = null;
    await waitUntil("backend observes status-free websocket close", async () => {
      const response = await gatewayFetch(ns, `/${name}`);
      if (response.status !== 200) return false;
      backendClose = await responseJson(response);
      return backendClose !== null;
    }, { timeoutMs: 10_000, intervalMs: 100 });
    assert.deepEqual(backendClose, { code: 1005, reason: "" });
  } finally {
    socket.destroy();
  }
});

test("gateway-proxied ws survives idle then cleanly releases both peers and process sockets", async () => {
  const ns = uniqueNs("ws-clean-close");
  const name = "echo";
  await deployAndPromote(ns, name, { code: TRACKED_WEBSOCKET_WORKER });

  assert.deepEqual(await trackedWebSocketState(ns, name), {
    active: 0,
    opened: 0,
    closed: 0,
    lastClose: null,
  });
  await waitForNoActiveGatewayWebSockets(
    "gateway active websocket gauge is idle before clean close test"
  );
  const socketBaseline = await gatewaySocketBaseline();
  const logSince = new Date().toISOString();
  /** @type {import("node:net").Socket | null} */
  let socket = null;

  try {
    const openedSocket = await openEchoWebSocket(ns, name, "clean-close");
    socket = openedSocket;
    const socketTracker = gatewaySocketTracker(
      socketBaseline,
      gatewaySocketStats(),
      1
    );
    await delay(1_000);
    const afterIdle = readOneServerTextFrame(openedSocket);
    openedSocket.write(encodeClientTextFrame("after-idle"));
    assert.equal(await afterIdle, "echo:after-idle");

    const reciprocalClose = readOneServerCloseFrame(openedSocket);
    openedSocket.write(encodeClientCloseFrame(1000, "test complete"));
    assert.deepEqual(await reciprocalClose, { code: 1000, reason: "test complete" });
    await waitForSocketClose(openedSocket);
    await waitForGatewaySocketsReleased(
      socketBaseline,
      socketTracker,
      "gateway process sockets return to baseline after clean close"
    );

    await waitUntil("backend releases cleanly closed websocket", async () => {
      const state = await trackedWebSocketState(ns, name);
      return state.active === 0 && state.closed === 1;
    }, { timeoutMs: 10_000, intervalMs: 100 });
    const finalState = await trackedWebSocketState(ns, name);
    assert.deepEqual(finalState, {
      active: 0,
      opened: 1,
      closed: 1,
      lastClose: { code: 1000, reason: "test complete" },
    });
    await waitForNoActiveGatewayWebSockets(
      "gateway active websocket gauge returns to zero after clean close"
    );
    await assertNoGatewayHangSince(logSince);
  } finally {
    socket?.destroy();
  }
});

test("gateway-proxied ws client resets release public and backend sockets", async () => {
  const ns = uniqueNs("ws-client-reset");
  const name = "echo";
  const connectionCount = 32;
  await deployAndPromote(ns, name, { code: TRACKED_WEBSOCKET_WORKER });

  assert.equal((await trackedWebSocketState(ns, name)).active, 0);
  await waitForNoActiveGatewayWebSockets(
    "gateway active websocket gauge is idle before client reset test"
  );
  const socketBaseline = await gatewaySocketBaseline();
  const logSince = new Date().toISOString();
  /** @type {import("node:net").Socket[]} */
  const sockets = [];

  try {
    const attempts = await Promise.allSettled(
      Array.from(
        { length: connectionCount },
        (_value, index) => openEchoWebSocket(ns, name, `reset-${index}`)
      )
    );
    for (const attempt of attempts) {
      if (attempt.status === "fulfilled") sockets.push(attempt.value);
    }
    const failed = attempts.find((attempt) => attempt.status === "rejected");
    if (failed?.status === "rejected") throw failed.reason;

    assert.equal(
      (await trackedWebSocketState(ns, name)).active,
      connectionCount,
      "all backend websocket sessions should remain active before client resets"
    );
    const activeSockets = gatewaySocketStats();
    const socketTracker = gatewaySocketTracker(
      socketBaseline,
      activeSockets,
      connectionCount
    );

    for (const socket of sockets) socket.resetAndDestroy();
    await waitForGatewaySocketsReleased(
      socketBaseline,
      socketTracker,
      "gateway process sockets return to baseline after client resets"
    );

    await waitUntil("backend releases reset websocket batch", async () => {
      const state = await trackedWebSocketState(ns, name);
      return state.active === 0 && state.closed === connectionCount;
    }, { timeoutMs: 15_000, intervalMs: 100 });
    const finalState = await trackedWebSocketState(ns, name);
    assert.equal(finalState.opened, connectionCount);
    assert.equal(finalState.closed, connectionCount);
    await waitForNoActiveGatewayWebSockets(
      "gateway active websocket gauge returns to zero after client resets"
    );
    await assertNoGatewayHangSince(logSince);
  } finally {
    for (const socket of sockets) socket.destroy();
  }
});

test("gateway-proxied ws reconnects backend after user-runtime restart", async () => {
  const ns = uniqueNs("ws-reconnect");
  const name = "echo";
  const code = `
    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response("need upgrade", { status: 426 });
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        server.addEventListener("message", (evt) => {
          server.send("echo:" + evt.data);
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const beforeEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const { status, socket } = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(status, 101);

    const first = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("before"));
    assert.equal(await first, "echo:before");

    composeRecreate("user-runtime");

    const second = readOneServerTextFrame(socket, { timeoutMs: 10_000 });
    socket.write(encodeClientTextFrame("after-1"));
    assert.equal(await second, "echo:after-1");
    socket.write(encodeClientTextFrame("after-2"));
    assert.equal(await readOneServerTextFrame(socket), "echo:after-2");
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should keep the client websocket open while replacing its backend websocket"
    );
    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") >= beforeEnvoy + 2,
      "gateway reconnect should establish a second backend websocket through the user-runtime Envoy cluster"
    );
  } finally {
    socket.destroy();
  }
});

test("gateway drains an old websocket but does not reconnect its inactive version", async () => {
  const ns = uniqueNs("ws-active-version");
  const name = "echo";
  await deployAndPromote(ns, name, { code: versionedWebSocketWorker("v1") });

  const first = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(first.status, 101);
    first.socket.write(encodeClientTextFrame("before"));
    assert.equal(await readOneServerTextFrame(first.socket), "v1:before");

    await deployAndPromote(ns, name, { code: versionedWebSocketWorker("v2") });
    await waitUntil("gateway serves the promoted websocket worker", async () => {
      const response = await gatewayFetch(ns, `/${name}`);
      return response.status === 200 && await response.text() === "v2";
    }, { timeoutMs: 10_000, intervalMs: 100 });

    first.socket.write(encodeClientTextFrame("drain"));
    assert.equal(
      await readOneServerTextFrame(first.socket),
      "v1:drain",
      "the admitted v1 websocket should drain after v2 cold-load eviction"
    );

    const beforeRestart = await gatewayWebSocketProxyCount("lifecycle_restart");
    const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
    const closeFrame = readOneServerCloseFrame(first.socket, { timeoutMs: 30_000 });
    composeRecreate("user-runtime");
    await waitUntil(
      "gateway rejects reconnect to the inactive websocket version",
      async () => await gatewayWebSocketProxyCount("lifecycle_restart") > beforeRestart,
      { timeoutMs: 15_000, intervalMs: 100 }
    );
    assert.equal(
      await gatewayWebSocketProxyCount("reconnected"),
      beforeReconnect,
      "gateway must not reconnect the inactive v1 backend"
    );
    assert.deepEqual(await closeFrame, { code: 1012, reason: "service restart" });
    if (!first.socket.destroyed) {
      first.socket.write(encodeClientCloseFrame(1012, "service restart"));
    }
    await waitForSocketClose(first.socket);
  } finally {
    first.socket.destroy();
  }

  await waitUntil("recreated user-runtime serves the active websocket worker", async () => {
    const response = await gatewayFetch(ns, `/${name}`);
    return response.status === 200 && await response.text() === "v2";
  }, { timeoutMs: 15_000, intervalMs: 100 });

  const second = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(second.status, 101);
    second.socket.write(encodeClientTextFrame("after"));
    assert.equal(await readOneServerTextFrame(second.socket), "v2:after");
  } finally {
    second.socket.destroy();
  }
});

test("gateway-proxied pattern-routed ws reconnects backend after user-runtime restart", async () => {
  const ns = uniqueNs("ws-pattern-reconnect");
  const name = "echo";
  const host = `${ns}.routes.workers.example`;
  const code = `
    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response("need upgrade", { status: 426 });
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        server.addEventListener("message", (evt) => {
          server.send("echo:" + evt.data);
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  const hosts = await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  assert.equal(hosts.status, 200);
  await deployAndPromote(ns, name, { code, routes: [`${host}/ws/*`] });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const beforeEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const { status, socket } = await hostWsHandshake(host, "/ws/room");
  try {
    assert.equal(status, 101);

    const first = readOneServerTextFrame(socket);
    socket.write(encodeClientTextFrame("before"));
    assert.equal(await first, "echo:before");

    composeRecreate("user-runtime");

    const second = readOneServerTextFrame(socket, { timeoutMs: 10_000 });
    socket.write(encodeClientTextFrame("after"));
    assert.equal(await second, "echo:after");
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should reconnect pattern-routed backend websockets"
    );
    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") >= beforeEnvoy + 2,
      "pattern-routed websocket reconnect should establish a second backend request through Envoy"
    );
  } finally {
    socket.destroy();
  }
});

test("gateway-proxied ws proactively reconnects backend for server-pushed frames", async () => {
  const ns = uniqueNs("ws-push-reconnect");
  const name = "push";
  const code = `
    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response("need upgrade", { status: 426 });
        }
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        setTimeout(() => {
          try {
            server.send("open");
          } catch {}
        }, 100);
        server.addEventListener("message", (evt) => {
          server.send("echo:" + evt.data);
        });
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  const beforeReconnect = await gatewayWebSocketProxyCount("reconnected");
  const beforeEnvoy = envoyStat("cluster.user_runtime.upstream_rq_total");
  const { status, socket } = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(status, 101);
    assert.equal(await readOneServerTextFrame(socket), "open");

    composeRecreate("user-runtime");

    assert.equal(
      await readOneServerTextFrame(socket, { timeoutMs: 15_000 }),
      "open"
    );
    assert.ok(
      await gatewayWebSocketProxyCount("reconnected") > beforeReconnect,
      "gateway should proactively reconnect the backend websocket after upstream close"
    );
    assert.ok(
      envoyStat("cluster.user_runtime.upstream_rq_total") >= beforeEnvoy + 2,
      "proactive websocket reconnect should establish a second backend request through Envoy"
    );
  } finally {
    socket.destroy();
  }
});

test("gateway-proxied ws closes with 1011 when backend reconnect cannot produce an upgrade", async () => {
  const ns = uniqueNs("ws-reconnect-fail");
  const name = "fail";
  const code = `
    let accepted = false;

    export default {
      async fetch(request) {
        if (request.headers.get("Upgrade") !== "websocket") {
          return new Response("need upgrade", { status: 426 });
        }
        if (accepted) {
          return new Response("backend unavailable", { status: 503 });
        }
        accepted = true;
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        server.accept();
        setTimeout(() => {
          server.close(1011, "backend lost");
        }, 2_000);
        return new Response(null, { status: 101, webSocket: client });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  const beforeFailed = await gatewayWebSocketProxyCount("reconnect_failed");
  const beforeSessionFailures = await gatewayWebSocketSessionLifetimeCount("reconnect_failed");
  await waitForNoActiveGatewayWebSockets(
    "gateway active websocket gauge is idle before reconnect exhaustion test"
  );
  const socketBaseline = await gatewaySocketBaseline();
  const logSince = new Date().toISOString();
  const { status, socket } = await wsHandshake(ns, `/${name}`);
  try {
    assert.equal(status, 101);
    const socketTracker = gatewaySocketTracker(
      socketBaseline,
      gatewaySocketStats(),
      1
    );
    const close = await readOneServerCloseFrame(socket, { timeoutMs: 15_000 });
    assert.deepEqual(close, { code: 1011, reason: "upstream reconnect failed" });
    if (!socket.destroyed) socket.write(encodeClientCloseFrame(close.code, close.reason));
    await waitForSocketClose(socket);
    await waitForGatewaySocketsReleased(
      socketBaseline,
      socketTracker,
      "gateway process sockets return to baseline after reconnect exhaustion"
    );
    assert.ok(
      await gatewayWebSocketProxyCount("reconnect_failed") > beforeFailed,
      "gateway should report bounded websocket reconnect failure"
    );
    assert.ok(
      await gatewayWebSocketSessionLifetimeCount("reconnect_failed") > beforeSessionFailures,
      "gateway should report websocket session lifetime for reconnect failures"
    );
    await waitForNoActiveGatewayWebSockets(
      "gateway active websocket gauge returns to zero after reconnect exhaustion"
    );
    await assertNoGatewayHangSince(logSince);
  } finally {
    socket.destroy();
  }
});

test("non-ws requests still go through the respond() rewrite (x-request-id preserved)", async () => {
  const ns = uniqueNs("ws");
  const name = "no-upgrade";
  const code = `
    export default {
      async fetch() {
        return new Response("plain", { status: 200 });
      },
    };
  `;
  await deployAndPromote(ns, name, { code });

  await new Promise((resolve, reject) => {
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: `/${name}`,
      headers: { Host: `${ns}.workers.local` },
      agent: false,
    }, (res) => {
      /** @type {Buffer[]} */
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try {
          assert.equal(res.statusCode, 200);
          assert.equal(Buffer.concat(chunks).toString("utf8"), "plain");
          assert.ok(res.headers["x-request-id"], "request id header preserved on non-101");
          resolve(undefined);
        } catch (e) { reject(e); }
      });
    });
    req.on("error", reject);
    req.end();
  });
});
