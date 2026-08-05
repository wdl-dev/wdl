import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { withMockedPropertyDescriptor } from "../helpers/mock-global.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const GATEWAY_INDEX_TEST_STATE = {
  dispatch: {
    kind: "forward",
    route: "pattern",
    namespace: "demo",
    worker: "worker",
    version: "v1",
    bindingName: "RUNTIME_USER",
    prefix: "",
    forwardPath: "/same",
  },
  forwardedRequests: /** @type {Array<{
    url: string,
    method: string,
    body: string,
    marker: string | null,
  }>} */ ([]),
  dispatchCalls: /** @type {Array<{
    normalizedHost: string,
    normalizedAdminHost: string,
    platformDomain: string,
  }>} */ ([]),
  routingOptionsCalls: /** @type {object[]} */ ([]),
  websocketAdjustments: /** @type {unknown[][]} */ ([]),
  websocketEvents: /** @type {unknown[][]} */ ([]),
  websocketOptionCalls: /** @type {object[]} */ ([]),
  websocketProxyCalls: /** @type {any[]} */ ([]),
  websocketUpstreamCalls: /** @type {any[]} */ ([]),
  policySnapshotCalls: /** @type {Array<{ namespace: string, worker: string }>} */ ([]),
  policySnapshots: /** @type {Array<{
    kind: "active",
    version: string,
    mode: "preserve" | "restart",
    restartSequence: number,
  } | { kind: "inactive" } | Error>} */ ([]),
  lifecycleRegistrations: /** @type {any[]} */ ([]),
  runtimeForwardCalls: /** @type {unknown[][]} */ ([]),
  timingEvents: /** @type {string[]} */ ([]),
  routingUnavailable: false,
};

/** @type {typeof globalThis & { __gatewayIndexTestState?: typeof GATEWAY_INDEX_TEST_STATE }} */
const gatewayIndexGlobal = globalThis;
gatewayIndexGlobal.__gatewayIndexTestState = GATEWAY_INDEX_TEST_STATE;

/**
 * @param {string} version
 * @param {"preserve" | "restart"} [mode]
 * @param {number} [restartSequence]
 * @returns {{ kind: "active", version: string, mode: "preserve" | "restart", restartSequence: number }}
 */
function activeLifecycle(version, mode = "preserve", restartSequence = 0) {
  return { kind: "active", version, mode, restartSequence };
}

const runtimeUrl = moduleDataUrl(`
export class GatewayRoutingUnavailableError extends Error {
  constructor() {
    super("routing unavailable");
    this.status = 503;
    this.code = "gateway_routing_unavailable";
    this.publicMessage = "Gateway routing temporarily unavailable";
  }
}
export function createGatewayLifecycleRedis() { return {}; }
export function createGatewayRedis() { return {}; }
export function ensureGatewaySubscriber() { return null; }
export function gatewayHealthSnapshot() { return {}; }
export function gatewayRoutingOptionsFromEnv(env) {
  globalThis.__gatewayIndexTestState.routingOptionsCalls.push(env);
  return env.__routingOptions || {
    platformDomain: "workers.example",
    normalizedAdminHost: "",
  };
}
export async function readWebSocketLifecycleSnapshot(_redis, namespace, worker) {
  const state = globalThis.__gatewayIndexTestState;
  state.timingEvents.push("lifecycle");
  state.policySnapshotCalls.push({ namespace, worker });
  const snapshot = state.policySnapshots.shift();
  if (snapshot instanceof Error) throw snapshot;
  return snapshot || { kind: "active", version: "v1", mode: "preserve", restartSequence: 0 };
}
export function registerGatewayWebSocketLifecycle(namespace, worker, snapshot, handlers) {
  globalThis.__gatewayIndexTestState.lifecycleRegistrations.push({
    namespace,
    worker,
    snapshot,
    handlers,
  });
  return () => {};
}
export const log = (...args) => globalThis.__gatewayIndexTestState.websocketEvents.push(args);
export const metrics = {};
export function adjustGatewayWebSocketProxyBufferedMessages(...args) {
  globalThis.__gatewayIndexTestState.websocketAdjustments.push(["buffered", ...args]);
}
export function adjustGatewayWebSocketProxyConnections(...args) {
  globalThis.__gatewayIndexTestState.websocketAdjustments.push(["connections", ...args]);
}
export function prepareGatewayMetrics() {}
export function recordGatewayWebSocketProxy() {}
export function recordGatewayWebSocketSessionLifetime(...args) {
  globalThis.__gatewayIndexTestState.websocketAdjustments.push(["lifetime", ...args]);
}
export function recordRuntimeForwardDuration(...args) {
  globalThis.__gatewayIndexTestState.runtimeForwardCalls.push(args);
}
export function runtimeForwardOutcome() { return "error"; }
`);

const dispatchUrl = moduleDataUrl(`
import { GatewayRoutingUnavailableError } from ${JSON.stringify(runtimeUrl)};
export async function resolveGatewayDispatch(options) {
  const state = globalThis.__gatewayIndexTestState;
  state.dispatchCalls.push({
    normalizedHost: options.normalizedHost,
    normalizedAdminHost: options.normalizedAdminHost,
    platformDomain: options.platformDomain,
  });
  if (state.routingUnavailable) throw new GatewayRoutingUnavailableError();
  return state.dispatch;
}
`);

const requestScopeUrl = moduleDataUrl(`
export function createHttpRequestScope() {
  return {
    requestId: "rid-gateway-index",
    setRoute() {},
    respond(response) { return response; },
    markError() {},
    complete() {},
  };
}
`);

const observabilityUrl = moduleDataUrl(`
export function createLogLevelBinder() { return () => {}; }
`);

const gatewayLibOwnerUrl = repositoryFileUrl("gateway/lib.js");
const gatewayLibUrl = moduleDataUrl(`
export {
  deleteGatewayInternalHeaders,
  isWebSocketUpgrade,
  normalizeRequestHost,
} from ${JSON.stringify(gatewayLibOwnerUrl)};
`);

const workerIdUrl = moduleDataUrl(`
export function formatWorkerId() { return "demo:worker:v1"; }
`);

const websocketUrl = moduleDataUrl(`
export function createGatewayWebSocketUpstreamFetch(request, upstream) {
  globalThis.__gatewayIndexTestState.websocketUpstreamCalls.push({ request, upstream });
  return async () => await upstream.fetch(new Request(request));
}
export function proxyGatewayWebSocket(initial, upstreamFetch, recordProxyOutcome, observability, options) {
  globalThis.__gatewayIndexTestState.websocketProxyCalls.push({
    initial,
    upstreamFetch,
    recordProxyOutcome,
    observability,
    options,
  });
  return new Response("proxied");
}
export function webSocketProxyOptionsFromEnv(env) {
  globalThis.__gatewayIndexTestState.websocketOptionCalls.push(env);
  return { maxBufferedClientMessages: 7 };
}
`);

const gatewayIndex = (await importRepositoryModule(
  "gateway/index.js",
  importSpecifierReplacements({
    "shared-respond": repositoryFileUrl("shared/respond.js"),
    "shared-observability": observabilityUrl,
    "shared-request-scope": requestScopeUrl,
    "gateway-lib": gatewayLibUrl,
    "gateway-dispatch": dispatchUrl,
    "gateway-runtime": runtimeUrl,
    "gateway-websocket": websocketUrl,
    "shared-worker-id": workerIdUrl,
  })
)).default;

beforeEach(() => {
  GATEWAY_INDEX_TEST_STATE.dispatch = {
    kind: "forward",
    route: "pattern",
    namespace: "demo",
    worker: "worker",
    version: "v1",
    bindingName: "RUNTIME_USER",
    prefix: "",
    forwardPath: "/same",
  };
  GATEWAY_INDEX_TEST_STATE.forwardedRequests.length = 0;
  GATEWAY_INDEX_TEST_STATE.dispatchCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.routingOptionsCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.websocketAdjustments.length = 0;
  GATEWAY_INDEX_TEST_STATE.websocketEvents.length = 0;
  GATEWAY_INDEX_TEST_STATE.websocketOptionCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.websocketUpstreamCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.policySnapshotCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.policySnapshots.length = 0;
  GATEWAY_INDEX_TEST_STATE.lifecycleRegistrations.length = 0;
  GATEWAY_INDEX_TEST_STATE.runtimeForwardCalls.length = 0;
  GATEWAY_INDEX_TEST_STATE.timingEvents.length = 0;
  GATEWAY_INDEX_TEST_STATE.routingUnavailable = false;
});

test("gateway returns a public 503 when routing snapshots stay invalidated", async () => {
  GATEWAY_INDEX_TEST_STATE.routingUnavailable = true;
  const response = await gatewayIndex.fetch(
    new Request("https://demo.workers.example/worker"),
    { REDIS_ADDR: "redis:6379" },
    /** @type {any} */ ({ waitUntil() {} })
  );

  await assertJsonResponse(response, 503, {
    error: "gateway_routing_unavailable",
    message: "Gateway routing temporarily unavailable",
    request_id: "rid-gateway-index",
  });
});

test("gateway forwards runtime-owned routing options by env identity", async () => {
  const runtimeBinding = { async fetch() { return new Response("ok"); } };
  const firstEnv = {
    REDIS_ADDR: "redis:6379",
    __routingOptions: {
      platformDomain: "first.workers.example",
      normalizedAdminHost: "first-admin.example",
    },
    RUNTIME_USER: runtimeBinding,
  };
  const secondEnv = {
    REDIS_ADDR: "redis:6379",
    __routingOptions: {
      platformDomain: "second.workers.example",
      normalizedAdminHost: "second-admin.example",
    },
    RUNTIME_USER: runtimeBinding,
  };

  for (const env of [firstEnv, secondEnv, firstEnv]) {
    const response = await gatewayIndex.fetch(
      new Request("https://custom.example/same"),
      /** @type {any} */ (env),
      /** @type {any} */ ({ waitUntil() {} })
    );
    assert.equal(response.status, 200);
  }

  assert.deepEqual(
    GATEWAY_INDEX_TEST_STATE.routingOptionsCalls,
    [firstEnv, secondEnv, firstEnv]
  );
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.dispatchCalls, [
    {
      normalizedHost: "custom.example",
      normalizedAdminHost: "first-admin.example",
      platformDomain: "first.workers.example",
    },
    {
      normalizedHost: "custom.example",
      normalizedAdminHost: "second-admin.example",
      platformDomain: "second.workers.example",
    },
    {
      normalizedHost: "custom.example",
      normalizedAdminHost: "first-admin.example",
      platformDomain: "first.workers.example",
    },
  ]);
});

test("gateway preserves unchanged-path requests and rewrites changed paths", async () => {
  const runtimeBinding = {
    /** @param {Request} request */
    async fetch(request) {
      GATEWAY_INDEX_TEST_STATE.forwardedRequests.push({
        url: request.url,
        method: request.method,
        body: await request.text(),
        marker: request.headers.get("x-marker"),
      });
      return new Response("ok");
    },
  };
  const env = {
    REDIS_ADDR: "redis:6379",
    ADMIN_HOST: "ADMIN.EXAMPLE",
    RUNTIME_USER: runtimeBinding,
  };
  const pathnameDescriptor = Object.getOwnPropertyDescriptor(URL.prototype, "pathname");
  if (!pathnameDescriptor?.get || !pathnameDescriptor.set) {
    throw new Error("URL.pathname accessor is unavailable");
  }
  const { get: getPathname, set: setPathname } = pathnameDescriptor;
  let pathnameWrites = 0;

  await withMockedPropertyDescriptor(URL.prototype, "pathname", {
    configurable: pathnameDescriptor.configurable,
    enumerable: pathnameDescriptor.enumerable,
    get: getPathname,
    set(value) {
      pathnameWrites += 1;
      return Reflect.apply(setPathname, this, [value]);
    },
  }, async () => {
    const first = await gatewayIndex.fetch(
      new Request("https://custom.example/same?x=1", {
        method: "POST",
        headers: { "x-marker": "same" },
        body: "payload",
      }),
      /** @type {any} */ (env),
      /** @type {any} */ ({ waitUntil() {} })
    );
    assert.equal(first.status, 200);
    assert.equal(pathnameWrites, 0);

    GATEWAY_INDEX_TEST_STATE.dispatch = {
      ...GATEWAY_INDEX_TEST_STATE.dispatch,
      forwardPath: "/trimmed",
    };
    const second = await gatewayIndex.fetch(
      new Request("https://custom.example/same?x=2"),
      /** @type {any} */ (env),
      /** @type {any} */ ({ waitUntil() {} })
    );
    assert.equal(second.status, 200);
    assert.equal(pathnameWrites, 1);
  });

  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.forwardedRequests, [
    {
      url: "https://custom.example/same?x=1",
      method: "POST",
      body: "payload",
      marker: "same",
    },
    {
      url: "https://custom.example/trimmed?x=2",
      method: "GET",
      body: "",
      marker: null,
    },
  ]);
});

test("gateway proxies websocket upgrades through the routed runtime binding", async () => {
  /** @type {Request[]} */
  const requests = [];
  const accepted = { status: 101, webSocket: {} };
  const runtimeBinding = {
    /** @param {Request} request */
    async fetch(request) {
      requests.push(request);
      return accepted;
    },
  };
  const env = {
    REDIS_ADDR: "redis:6379",
    RUNTIME_USER: runtimeBinding,
  };
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1", "restart", 4),
    activeLifecycle("v2", "restart", 5)
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: {
        Upgrade: "websocket",
        "x-wdl-forged-private": "forged",
      },
    }),
    /** @type {any} */ (env),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  assert.equal(await response.text(), "proxied");
  assert.equal(requests.length, 1);
  assert.equal(requests[0].headers.get("upgrade"), "websocket");
  assert.equal(requests[0].headers.get("x-worker-id"), "demo:worker:v1");
  assert.equal(requests[0].headers.get("x-wdl-forged-private"), null);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketUpstreamCalls.length, 1);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketUpstreamCalls[0].upstream, runtimeBinding);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.websocketOptionCalls, [env]);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length, 1);
  const proxyCall = GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0];
  assert.equal(proxyCall.initial, accepted);
  assert.equal(proxyCall.options.maxBufferedClientMessages, 7);
  assert.equal(typeof proxyCall.options.checkLifecycle, "function");
  const lifecycleHandlers = { restart() {}, fail() {} };
  proxyCall.options.registerLifecycle(lifecycleHandlers);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.lifecycleRegistrations, [{
    namespace: "demo",
    worker: "worker",
    snapshot: activeLifecycle("v1", "restart", 4),
    handlers: lifecycleHandlers,
  }]);
  assert.equal(await proxyCall.options.checkLifecycle(), "restart");
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.policySnapshotCalls, [
    { namespace: "demo", worker: "worker" },
    { namespace: "demo", worker: "worker" },
  ]);
  proxyCall.observability.recordEvent("warn", "ws_retry", { detail: "again" });
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.websocketEvents, [[
    "warn",
    "ws_retry",
    {
      request_id: "rid-gateway-index",
      namespace: "demo",
      worker: "worker",
      version: "v1",
      binding: "RUNTIME_USER",
      detail: "again",
    },
  ]]);
});

test("gateway fails closed when a websocket session policy sequence regresses", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1", "restart", 4),
    activeLifecycle("v1", "preserve", 3)
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  const options = GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0].options;
  await assert.rejects(options.checkLifecycle(), /routing unavailable/);
});

test("gateway rejects websocket handshakes superseded by a restart session policy", async () => {
  let upstreamCalled = false;
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(activeLifecycle("v2", "restart", 5));

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          upstreamCalled = true;
          return new Response("unexpected");
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  await assertJsonResponse(response, 503, {
    error: "gateway_routing_unavailable",
    message: "Gateway routing temporarily unavailable",
    request_id: "rid-gateway-index",
  });
  assert.equal(upstreamCalled, false);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length, 0);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.runtimeForwardCalls, []);
});

test("gateway rejects websocket handshakes superseded by a preserve session policy", async () => {
  let upstreamCalled = false;
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(activeLifecycle("v2", "preserve", 4));

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          upstreamCalled = true;
          return new Response("unexpected");
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  await assertJsonResponse(response, 503, {
    error: "gateway_routing_unavailable",
    message: "Gateway routing temporarily unavailable",
    request_id: "rid-gateway-index",
  });
  assert.equal(upstreamCalled, false);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length, 0);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.runtimeForwardCalls, []);
});

test("gateway does not reconnect an old websocket worker after a preserve promotion", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1"),
    activeLifecycle("v1"),
    activeLifecycle("v2")
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  const options = GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0].options;
  assert.equal(await options.checkLifecycle(), "continue");
  assert.equal(await options.checkLifecycle(), "restart");
});

test("gateway acknowledges a superseded restart when the latest projection preserves", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1", "preserve", 0),
    activeLifecycle("v1", "preserve", 1),
    activeLifecycle("v1", "restart", 2)
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  const options = GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0].options;
  assert.equal(await options.checkLifecycle(), "continue");
  assert.equal(await options.checkLifecycle(), "restart");
});

test("gateway treats a deleted worker as a service restart after admission", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1"),
    { kind: "inactive" }
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  assert.equal(
    await GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0].options.checkLifecycle(),
    "restart"
  );
});

test("gateway rejects a stale websocket handshake after whole-worker delete", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push({ kind: "inactive" });

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          throw new Error("inactive workers must not reach runtime");
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  await assertJsonResponse(response, 503, {
    error: "gateway_routing_unavailable",
    message: "Gateway routing temporarily unavailable",
    request_id: "rid-gateway-index",
  });
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length, 0);
});

test("gateway distinguishes transient websocket lifecycle read failures", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(
    activeLifecycle("v1"),
    new Error("redis failover"),
    activeLifecycle("v1")
  );

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 200);
  const options = GATEWAY_INDEX_TEST_STATE.websocketProxyCalls[0].options;
  assert.equal(await options.checkLifecycle(), "retry");
  assert.equal(await options.checkLifecycle(), "continue");
});

test("gateway starts runtime forward timing after websocket lifecycle admission", async () => {
  GATEWAY_INDEX_TEST_STATE.policySnapshots.push(activeLifecycle("v1"));
  let now = 100;

  const response = await withMockedPropertyDescriptor(Date, "now", {
    configurable: true,
    value() {
      GATEWAY_INDEX_TEST_STATE.timingEvents.push("clock");
      now += 10;
      return now;
    },
  }, () => gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: {
        async fetch() {
          return { status: 101, webSocket: {} };
        },
      },
    }),
    /** @type {any} */ ({ waitUntil() {} })
  ));

  assert.equal(response.status, 200);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.timingEvents, [
    "lifecycle",
    "clock",
    "clock",
  ]);
  assert.deepEqual(GATEWAY_INDEX_TEST_STATE.runtimeForwardCalls, [[
    10,
    "RUNTIME_USER",
    "error",
  ]]);
});

test("gateway returns rejected websocket upgrades without starting the proxy", async () => {
  const runtimeBinding = {
    async fetch() {
      return new Response("upgrade rejected", { status: 426 });
    },
  };

  const response = await gatewayIndex.fetch(
    new Request("https://custom.example/same", {
      headers: { Upgrade: "websocket" },
    }),
    /** @type {any} */ ({
      REDIS_ADDR: "redis:6379",
      RUNTIME_USER: runtimeBinding,
    }),
    /** @type {any} */ ({ waitUntil() {} })
  );

  assert.equal(response.status, 426);
  assert.equal(await response.text(), "upgrade rejected");
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketUpstreamCalls.length, 1);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketProxyCalls.length, 0);
  assert.equal(GATEWAY_INDEX_TEST_STATE.websocketOptionCalls.length, 0);
});
