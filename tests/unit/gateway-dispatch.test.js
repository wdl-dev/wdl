import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";

const nsPatternUrl = repositoryModuleDataUrl("shared/ns-pattern.js");
const gatewayLibUrl = repositoryModuleDataUrl("gateway/lib.js");
const workerContractUrl = repositoryModuleDataUrl("shared/worker-contract.js");
const gatewayRuntimeUrl = moduleDataUrl(`
function deps() {
  return globalThis.__gatewayDispatchTestDeps;
}
export function recordRoutingLookup(...args) {
  return deps().recordRoutingLookup(...args);
}
export function ensureKnownNs(...args) {
  return deps().ensureKnownNs(...args);
}
export function ensureKnownPatternHosts(...args) {
  return deps().ensureKnownPatternHosts(...args);
}
export function getCachedNsRoutes(...args) {
  return deps().getCachedNsRoutes(...args);
}
export function loadNsRoutes(...args) {
  return deps().loadNsRoutes(...args);
}
export function getCachedPatterns(...args) {
  return deps().getCachedPatterns(...args);
}
export function loadPatternsForHost(...args) {
  return deps().loadPatternsForHost(...args);
}
export function recordPatternMatchComparisons(...args) {
  return deps().recordPatternMatchComparisons(...args);
}
`);
const dispatchSource = applyModuleReplacements(readRepositoryFile("gateway/dispatch.js"), [
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
  [/from "gateway-lib";/, `from ${JSON.stringify(gatewayLibUrl)};`],
  [/from "gateway-runtime";/, `from ${JSON.stringify(gatewayRuntimeUrl)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(workerContractUrl)};`],
]);

const { resolveGatewayDispatch } = await import(moduleDataUrl(dispatchSource));

/**
 * @param {{ knownNs?: Set<string>, knownPatternHosts?: Set<string>, nsRoutes?: Map<string, any>, routeCache?: Map<string, Map<string, any>>, patterns?: any[], adminHost?: string }} [opts]
 */
function makeDeps({
  knownNs = new Set(),
  knownPatternHosts = new Set(),
  nsRoutes = new Map(),
  routeCache = new Map(),
  patterns = [],
} = {}) {
  /** @type {Array<[string, string]>} */
  const lookups = [];
  let ensureKnownNsCalls = 0;
  let ensureKnownPatternHostsCalls = 0;
  let loadPatternsForHostCalls = 0;
  return {
    lookups,
    get ensureKnownNsCalls() {
      return ensureKnownNsCalls;
    },
    get ensureKnownPatternHostsCalls() {
      return ensureKnownPatternHostsCalls;
    },
    get loadPatternsForHostCalls() {
      return loadPatternsForHostCalls;
    },
    deps: {
      redis: {},
      requestId: "rid-test",
      /** @param {string} scope @param {string} outcome */
      recordRoutingLookup(scope, outcome) {
        lookups.push([scope, outcome]);
      },
      /** @param {any} _redis */
      async ensureKnownNs(_redis) {
        ensureKnownNsCalls += 1;
        return knownNs;
      },
      /** @param {any} _redis */
      async ensureKnownPatternHosts(_redis) {
        ensureKnownPatternHostsCalls += 1;
        return knownPatternHosts;
      },
      /** @param {string} namespace */
      getCachedNsRoutes(namespace) {
        return routeCache.get(namespace) || null;
      },
      /** @param {any} _redis @param {string} _namespace */
      async loadNsRoutes(_redis, _namespace) {
        return nsRoutes;
      },
      /** @param {string} _host */
      getCachedPatterns(_host) {
        return null;
      },
      /** @param {any} _redis @param {string} _host @param {string} _requestId */
      async loadPatternsForHost(_redis, _host, _requestId) {
        loadPatternsForHostCalls += 1;
        return patterns;
      },
      recordPatternMatchComparisons() {},
    },
  };
}

/**
 * @param {string} url
 * @param {{ knownNs?: Set<string>, knownPatternHosts?: Set<string>, nsRoutes?: Map<string, any>, routeCache?: Map<string, Map<string, any>>, patterns?: any[], adminHost?: string }} [options]
 */
function dispatch(url, options = {}) {
  const parsed = new URL(url);
  const ctx = makeDeps(options);
  /** @type {any} */ (globalThis).__gatewayDispatchTestDeps = ctx.deps;
  return {
    lookups: ctx.lookups,
    get ensureKnownNsCalls() {
      return ctx.ensureKnownNsCalls;
    },
    get ensureKnownPatternHostsCalls() {
      return ctx.ensureKnownPatternHostsCalls;
    },
    get loadPatternsForHostCalls() {
      return ctx.loadPatternsForHostCalls;
    },
    result: resolveGatewayDispatch({
      url: parsed,
      normalizedHost: parsed.hostname,
      normalizedAdminHost: options.adminHost || "",
      platformDomain: "workers.local",
      redis: ctx.deps.redis,
      requestId: ctx.deps.requestId,
    }),
  };
}

test("resolveGatewayDispatch routes admin host to control without Redis gates", async () => {
  const { result, ensureKnownNsCalls } = dispatch("https://admin.local/reload", {
    adminHost: "admin.local",
  });

  assert.deepEqual(await result, {
    kind: "forward",
    route: "worker_fetch_admin_host",
    bindingName: "CONTROL",
    forwardPath: "/reload",
    prefix: "/",
    namespace: null,
    worker: null,
    version: null,
  });
  assert.equal(ensureKnownNsCalls, 0);
});

test("resolveGatewayDispatch lets admin host bypass runtime-internal data path guards", async () => {
  const { result } = dispatch("https://admin.local/internal/workflows/run", {
    adminHost: "admin.local",
  });

  assert.deepEqual(await result, {
    kind: "forward",
    route: "worker_fetch_admin_host",
    bindingName: "CONTROL",
    forwardPath: "/internal/workflows/run",
    prefix: "/",
    namespace: null,
    worker: null,
    version: null,
  });
});

test("resolveGatewayDispatch rejects reserved subdomains before Redis namespace lookup", async () => {
  const ctx = dispatch("https://__platform__.workers.local/worker/path");

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "__platform__",
    worker: null,
    version: null,
  });
  assert.equal(ctx.ensureKnownNsCalls, 0);
});

test("resolveGatewayDispatch preserves namespace context for unknown namespaces", async () => {
  const ctx = dispatch("https://missing.workers.local/app/path");

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "missing",
    worker: null,
    version: null,
  });
  assert.deepEqual(ctx.lookups, [["namespace_gate", "miss"]]);
});

test("resolveGatewayDispatch preserves namespace and worker context for unknown workers", async () => {
  const ctx = dispatch("https://demo.workers.local/missing/path", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["app", "v3"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "demo",
    worker: "missing",
    version: null,
  });
  assert.deepEqual(ctx.lookups, [
    ["namespace_gate", "hit"],
    ["route_cache", "miss"],
  ]);
});

test("resolveGatewayDispatch rejects malformed subdomain worker segments before route lookup", async () => {
  const ctx = dispatch("https://demo.workers.local/bad%3Aworker/path", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["bad:worker", "v1"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "demo",
    worker: "bad%3Aworker",
    version: null,
  });
  assert.deepEqual(ctx.lookups, [["namespace_gate", "hit"]]);
});

test("resolveGatewayDispatch rejects malformed subdomain route versions", async () => {
  const ctx = dispatch("https://demo.workers.local/app/path", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["app", "bad:version"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "demo",
    worker: "app",
    version: "bad:version",
  });
  assert.deepEqual(ctx.lookups, [
    ["namespace_gate", "hit"],
    ["route_cache", "miss"],
  ]);
});

test("resolveGatewayDispatch uses cached namespace subdomain route projections", async () => {
  const ctx = dispatch("https://demo.workers.local/app/path", {
    knownNs: new Set(["demo"]),
    routeCache: new Map([["demo", new Map([["app", "v9"]])]]),
    nsRoutes: new Map([["app", "stale"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_subdomain",
    bindingName: "RUNTIME_USER",
    forwardPath: "/path",
    prefix: "/app",
    namespace: "demo",
    worker: "app",
    version: "v9",
  });
  assert.deepEqual(ctx.lookups, [
    ["namespace_gate", "hit"],
    ["route_cache", "hit"],
  ]);
});

test("resolveGatewayDispatch preserves namespace context for empty subdomain paths", async () => {
  const ctx = dispatch("https://demo.workers.local/", {
    knownNs: new Set(["demo"]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_subdomain",
    namespace: "demo",
    worker: null,
    version: null,
  });
});

test("resolveGatewayDispatch resolves subdomain workers through known ns and routes", async () => {
  const ctx = dispatch("https://demo.workers.local/app/sub/path?q=1", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["app", "v3"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_subdomain",
    bindingName: "RUNTIME_USER",
    forwardPath: "/sub/path",
    prefix: "/app",
    namespace: "demo",
    worker: "app",
    version: "v3",
  });
  assert.deepEqual(ctx.lookups, [
    ["namespace_gate", "hit"],
    ["route_cache", "miss"],
  ]);
});

test("resolveGatewayDispatch preserves subdomain path suffix fidelity", async () => {
  const ctx = dispatch("https://demo.workers.local/app//sub/path/", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["app", "v3"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_subdomain",
    bindingName: "RUNTIME_USER",
    forwardPath: "//sub/path/",
    prefix: "/app",
    namespace: "demo",
    worker: "app",
    version: "v3",
  });
});

test("resolveGatewayDispatch treats runtime-looking paths as tenant fetch paths", async () => {
  const ctx = dispatch("https://demo.workers.local/app/internal/workflows/run", {
    knownNs: new Set(["demo"]),
    nsRoutes: new Map([["app", "v1"]]),
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_subdomain",
    bindingName: "RUNTIME_USER",
    forwardPath: "/internal/workflows/run",
    prefix: "/app",
    namespace: "demo",
    worker: "app",
    version: "v1",
  });
});

test("resolveGatewayDispatch routes tenant pattern hits to user runtime", async () => {
  const ctx = dispatch("https://custom.workers.example/app/now", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/app/*",
      kind: "prefix",
      value: "/app/",
      ns: "tenant",
      worker: "site",
      version: "v8",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_pattern",
    bindingName: "RUNTIME_USER",
    forwardPath: "/app/now",
    prefix: "/app/*",
    namespace: "tenant",
    worker: "site",
    version: "v8",
  });
});

test("resolveGatewayDispatch rejects malformed pattern worker names", async () => {
  const ctx = dispatch("https://custom.workers.example/app/now", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/app/*",
      kind: "prefix",
      value: "/app/",
      ns: "tenant",
      worker: "bad:worker",
      version: "v8",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_pattern",
    namespace: "tenant",
    worker: "bad:worker",
    version: "v8",
  });
});

test("resolveGatewayDispatch rejects malformed pattern versions", async () => {
  const ctx = dispatch("https://custom.workers.example/app/now", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/app/*",
      kind: "prefix",
      value: "/app/",
      ns: "tenant",
      worker: "site",
      version: "bad:version",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_pattern",
    namespace: "tenant",
    worker: "site",
    version: "bad:version",
  });
});

test("resolveGatewayDispatch returns context-free misses when pattern matching fails", async () => {
  const ctx = dispatch("https://custom.workers.example/nope", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/app/*",
      kind: "prefix",
      value: "/app/",
      ns: "tenant",
      worker: "site",
      version: "v8",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_pattern",
    namespace: null,
    worker: null,
    version: null,
  });
});

test("resolveGatewayDispatch rejects undeclared pattern hosts before pattern lookup", async () => {
  const ctx = dispatch("https://spray.workers.example/app/now", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/app/*",
      kind: "prefix",
      value: "/app/",
      ns: "tenant",
      worker: "site",
      version: "v8",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_pattern",
    namespace: null,
    worker: null,
    version: null,
  });
  assert.equal(ctx.ensureKnownPatternHostsCalls, 1);
  assert.equal(ctx.loadPatternsForHostCalls, 0);
  assert.deepEqual(ctx.lookups, [["pattern_host_gate", "miss"]]);
});

test("resolveGatewayDispatch routes allowed __system__ pattern hits to system runtime", async () => {
  const ctx = dispatch("https://custom.workers.example/jobs/now", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/jobs/*",
      kind: "prefix",
      value: "/jobs/",
      ns: "__system__",
      worker: "cron",
      version: "v2",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "forward",
    route: "worker_fetch_pattern",
    bindingName: "RUNTIME_SYSTEM",
    forwardPath: "/jobs/now",
    prefix: "/jobs/*",
    namespace: "__system__",
    worker: "cron",
    version: "v2",
  });
});

test("resolveGatewayDispatch rejects disallowed reserved pattern hits", async () => {
  const ctx = dispatch("https://custom.workers.example/platform", {
    knownPatternHosts: new Set(["custom.workers.example"]),
    patterns: [{
      slot: "/platform",
      kind: "exact",
      value: "/platform",
      ns: "__platform__",
      worker: "p",
      version: "v1",
    }],
  });

  assert.deepEqual(await ctx.result, {
    kind: "not_found",
    route: "worker_fetch_pattern",
    namespace: "__platform__",
    worker: "p",
    version: "v1",
  });
});
