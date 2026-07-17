import { test } from "node:test";
import assert from "node:assert/strict";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const redisUrl = moduleDataUrl(`
export class RedisClient {}
export class RedisSubscriber {}
`);
const nsPatternUrl = moduleDataUrl(`
export function isValidRouteNs() { return true; }
`);
const routeProjectionUrl = moduleDataUrl(`
export function decodePatternProjection(raw) { return raw; }
`);
const gatewayLibUrl = moduleDataUrl(`
export function isCanonicalPatternHost() { return true; }
export function sortPatterns(entries) { return { sorted: entries, errors: [] }; }
`);

const src = applyModuleReplacements(readRepositoryFile("gateway/runtime.js"), [
  [/from "shared-redis";/, `from ${JSON.stringify(redisUrl)};`],
  [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
  [/from "shared-route-projection";/, `from ${JSON.stringify(routeProjectionUrl)};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(repositoryFileUrl("shared/worker-contract.js"))};`],
  [/from "gateway-lib";/, `from ${JSON.stringify(gatewayLibUrl)};`],
]);

const { runtimeForwardOutcome } = await import(moduleDataUrl(src));

test("runtimeForwardOutcome treats websocket upgrades as successful forwards", () => {
  assert.equal(runtimeForwardOutcome({ status: 101 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 200 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 302 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 400 }), "error");
  assert.equal(runtimeForwardOutcome({ status: 503 }), "error");
  assert.equal(runtimeForwardOutcome(null), "error");
});
