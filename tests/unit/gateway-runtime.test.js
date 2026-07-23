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
export class RedisSubscriber {
  constructor(_addr, _channels, handlers) {
    globalThis.__gatewayRuntimeSubscriberHandlers = handlers;
  }
  start() { return Promise.resolve(); }
}
`);
const nsPatternUrl = moduleDataUrl(`
export function isValidRouteNs() { return true; }
`);
const routeProjectionUrl = moduleDataUrl(`
export function decodePatternProjection(raw) {
  globalThis.__gatewayRuntimeDecodeCalls?.push(raw);
  return raw;
}
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

let runtimeLoadSerial = 0;

async function loadGatewayRuntime() {
  runtimeLoadSerial += 1;
  return import(moduleDataUrl(`// gateway runtime test ${runtimeLoadSerial}\n${src}`));
}

const gatewayTestGlobal = /** @type {any} */ (globalThis);

const { runtimeForwardOutcome } = await loadGatewayRuntime();

test("runtimeForwardOutcome treats websocket upgrades as successful forwards", () => {
  assert.equal(runtimeForwardOutcome({ status: 101 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 200 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 302 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 400 }), "error");
  assert.equal(runtimeForwardOutcome({ status: 503 }), "error");
  assert.equal(runtimeForwardOutcome(null), "error");
});

test("resolveNamespaceRoutes fills the gate and route cache in one cold read", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} setKey @param {string} hashKey */
    async sMembersAndHGetAll(setKey, hashKey) {
      calls.push(["sMembersAndHGetAll", setKey, hashKey]);
      return {
        members: ["demo"],
        hash: { app: "v3" },
      };
    },
    /** @param {string} key */
    async hGetAll(key) {
      calls.push(["hGetAll", key]);
      throw new Error("unexpected route reload");
    },
  };

  const cold = await resolveNamespaceRoutes(redis, "demo");
  const hot = await resolveNamespaceRoutes(redis, "demo");

  assert.equal(cold.known, true);
  assert.equal(cold.cacheHit, false);
  assert.deepEqual([...cold.routes], [["app", "v3"]]);
  assert.equal(hot.known, true);
  assert.equal(hot.cacheHit, true);
  assert.equal(hot.routes, cold.routes);
  assert.deepEqual(calls, [["sMembersAndHGetAll", "namespaces", "routes:demo"]]);
});

test("resolveNamespaceRoutes keeps concurrent cold replies associated with their namespace", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  const alpha = Promise.withResolvers();
  const beta = Promise.withResolvers();
  const redis = {
    /** @param {string} _setKey @param {string} hashKey */
    async sMembersAndHGetAll(_setKey, hashKey) {
      if (hashKey === "routes:alpha") return await alpha.promise;
      if (hashKey === "routes:beta") return await beta.promise;
      throw new Error(`unexpected route key ${hashKey}`);
    },
  };

  const alphaPending = resolveNamespaceRoutes(redis, "alpha");
  const betaPending = resolveNamespaceRoutes(redis, "beta");
  beta.resolve({ members: ["alpha", "beta"], hash: { api: "v2" } });
  const betaResult = await betaPending;
  alpha.resolve({ members: ["alpha", "beta"], hash: { app: "v1" } });
  const alphaResult = await alphaPending;

  assert.deepEqual([...alphaResult.routes], [["app", "v1"]]);
  assert.deepEqual([...betaResult.routes], [["api", "v2"]]);
  assert.equal((await resolveNamespaceRoutes(redis, "alpha")).routes, alphaResult.routes);
  assert.equal((await resolveNamespaceRoutes(redis, "beta")).routes, betaResult.routes);
});

test("resolveNamespaceRoutes ignores a fetched hash for an unknown namespace", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  const redis = {
    async sMembersAndHGetAll() {
      return {
        members: ["other"],
        hash: { app: "v3" },
      };
    },
  };

  assert.deepEqual(await resolveNamespaceRoutes(redis, "missing"), {
    known: false,
    routes: null,
    cacheHit: false,
  });
});

test("resolveHostPatterns fills the host gate and pattern cache in one cold read", async () => {
  const { resolveHostPatterns } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} setKey @param {string} hashKey */
    async sMembersAndHGetAll(setKey, hashKey) {
      calls.push(["sMembersAndHGetAll", setKey, hashKey]);
      return {
        members: ["api.example"],
        hash: { "/app/*": "projection" },
      };
    },
    /** @param {string} key */
    async hGetAll(key) {
      calls.push(["hGetAll", key]);
      throw new Error("unexpected pattern reload");
    },
  };

  const cold = await resolveHostPatterns(redis, "api.example", "rid-cold");
  const hot = await resolveHostPatterns(redis, "api.example", "rid-hot");

  assert.equal(cold.known, true);
  assert.equal(cold.cacheHit, false);
  assert.deepEqual(cold.patterns, { "/app/*": "projection" });
  assert.equal(hot.known, true);
  assert.equal(hot.cacheHit, true);
  assert.equal(hot.patterns, cold.patterns);
  assert.deepEqual(calls, [[
    "sMembersAndHGetAll",
    "declared-hosts",
    "patterns:api.example",
  ]]);
});

test("resolveHostPatterns does not decode projections for an undeclared host", async () => {
  const { resolveHostPatterns } = await loadGatewayRuntime();
  const testGlobal = /** @type {any} */ (globalThis);
  testGlobal.__gatewayRuntimeDecodeCalls = [];
  const redis = {
    async sMembersAndHGetAll() {
      return {
        members: ["other.example"],
        hash: { "/app/*": "must-not-decode" },
      };
    },
  };

  try {
    assert.deepEqual(await resolveHostPatterns(redis, "missing.example", "rid-miss"), {
      known: false,
      patterns: null,
      cacheHit: false,
    });
    assert.deepEqual(testGlobal.__gatewayRuntimeDecodeCalls, []);
  } finally {
    delete testGlobal.__gatewayRuntimeDecodeCalls;
  }
});

test("route invalidation prevents an older cold snapshot from restoring stale state", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads === 1) return await firstRead.promise;
      return { members: ["demo"], hash: { app: "v2" } };
    },
  };

  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
    handlers.onConnect();
    const pending = resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", new TextEncoder().encode("demo"));
    firstRead.resolve({ members: ["demo"], hash: { app: "v1" } });

    const result = await pending;
    assert.equal(result.known, true);
    assert.deepEqual([...result.routes], [["app", "v2"]]);
    assert.equal(reads, 2);
  } finally {
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("pattern invalidation prevents an older cold snapshot from restoring stale state", async () => {
  const { ensureGatewaySubscriber, resolveHostPatterns } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads === 1) return await firstRead.promise;
      return { members: ["api.example"], hash: { "/v2/*": "projection-v2" } };
    },
  };

  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
    handlers.onConnect();
    const pending = resolveHostPatterns(redis, "api.example", "rid");
    handlers.onMessage("patterns:invalidate", new TextEncoder().encode("api.example"));
    firstRead.resolve({ members: ["api.example"], hash: { "/v1/*": "projection-v1" } });

    const result = await pending;
    assert.equal(result.known, true);
    assert.deepEqual(result.patterns, { "/v2/*": "projection-v2" });
    assert.equal(reads, 2);
  } finally {
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("route resolution fails closed after bounded invalidation churn", async () => {
  const {
    GatewayRoutingUnavailableError,
    ensureGatewaySubscriber,
    resolveNamespaceRoutes,
  } = await loadGatewayRuntime();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads <= 5) {
        gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
          "routes:invalidate",
          new TextEncoder().encode("other")
        );
      }
      return { members: ["demo"], hash: { app: "v1" } };
    },
  };

  try {
    await ensureGatewaySubscriber("redis:6379");
    gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onConnect();
    await assert.rejects(
      resolveNamespaceRoutes(redis, "demo"),
      (err) => {
        assert.ok(err instanceof GatewayRoutingUnavailableError);
        const unavailable = /** @type {{ status: unknown, code: unknown, publicMessage: unknown }} */ (err);
        assert.equal(unavailable.status, 503);
        assert.equal(unavailable.code, "gateway_routing_unavailable");
        assert.equal(unavailable.publicMessage, "Gateway routing temporarily unavailable");
        return true;
      }
    );
    assert.equal(reads, 5);
  } finally {
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("pattern resolution fails closed after bounded invalidation churn", async () => {
  const {
    GatewayRoutingUnavailableError,
    ensureGatewaySubscriber,
    resolveHostPatterns,
  } = await loadGatewayRuntime();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads <= 5) {
        gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
          "patterns:invalidate",
          new TextEncoder().encode("other.example")
        );
      }
      return { members: ["api.example"], hash: { "/": "projection" } };
    },
  };

  try {
    await ensureGatewaySubscriber("redis:6379");
    gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onConnect();
    await assert.rejects(
      resolveHostPatterns(redis, "api.example", "rid"),
      (err) => err instanceof GatewayRoutingUnavailableError
    );
    assert.equal(reads, 5);
  } finally {
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});
