import { test } from "node:test";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";

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
export const log = () => {};
export const metrics = {};
export function prepareGatewayMetrics() {}
export function recordGatewayWebSocketProxy() {}
export function recordRuntimeForwardDuration() {}
export function runtimeForwardOutcome() { return "error"; }
`);

const dispatchUrl = moduleDataUrl(`
import { GatewayRoutingUnavailableError } from ${JSON.stringify(runtimeUrl)};
export async function resolveGatewayDispatch() {
  throw new GatewayRoutingUnavailableError();
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

const gatewayLibUrl = moduleDataUrl(`
export function deleteGatewayInternalHeaders() {}
export function isWebSocketUpgrade() { return false; }
export function normalizeRequestHost(host) { return host; }
`);

const workerIdUrl = moduleDataUrl(`
export function formatWorkerId() { return "demo:worker:v1"; }
`);

const nsPatternUrl = moduleDataUrl(`
export function platformDomainFromEnv() { return "workers.example"; }
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
    "shared-ns-pattern": nsPatternUrl,
    "gateway-holder": holderUrl,
  })
)).default;

test("gateway returns a public 503 when routing snapshots stay invalidated", async () => {
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
