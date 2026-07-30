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
  routingUnavailable: false,
};

/** @type {typeof globalThis & { __gatewayIndexTestState?: typeof GATEWAY_INDEX_TEST_STATE }} */
const gatewayIndexGlobal = globalThis;
gatewayIndexGlobal.__gatewayIndexTestState = GATEWAY_INDEX_TEST_STATE;

const runtimeUrl = moduleDataUrl(`
export class GatewayRoutingUnavailableError extends Error {
  constructor() {
    super("routing unavailable");
    this.status = 503;
    this.code = "gateway_routing_unavailable";
    this.publicMessage = "Gateway routing temporarily unavailable";
  }
}
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
export const log = () => {};
export const metrics = {};
export function prepareGatewayMetrics() {}
export function recordGatewayWebSocketProxy() {}
export function recordRuntimeForwardDuration() {}
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

const holderUrl = moduleDataUrl(`
export class GatewayWsHolder {}
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
    "shared-worker-id": workerIdUrl,
    "gateway-holder": holderUrl,
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
