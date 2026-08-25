import { test } from "node:test";
import assert from "node:assert/strict";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { d1ProtocolDataUrl, d1QueryWireDataUrl } from "../helpers/load-d1-protocol.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { sharedOwnerForwarderUrl } from "../helpers/load-owner-harness.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";


const taskIdentityUrl = moduleDataUrl(`
export async function resolveTaskIdentity() { return { taskId: "task-a", endpoint: "d1-runtime-a:8787" }; }
`);
const timeoutUrl = moduleDataUrl(`
export function createD1QueryDeadline() {
  return { signal: new AbortController().signal, clear() {} };
}
`);
const ownerRegistryUrl = moduleDataUrl(`
export async function ownerLeaseExpiredByRedisTime(_env, owner) {
  return /** @type {any} */ (globalThis).__d1RouterLeaseExpired?.(owner) ?? false;
}
export async function resolveDbOwner(env, query, options) {
  return /** @type {any} */ (globalThis).__d1RouterResolveDbOwner?.(env, query, options) ||
    { taskId: "task-a", endpoint: "d1-runtime-a:8787", generation: 1 };
}
export async function takeoverExpiredOwner(env, owner) {
  return /** @type {any} */ (globalThis).__d1RouterTakeoverExpiredOwner?.(env, owner) || owner;
}
`);
const productionReadCacheUrl = repositoryFileUrl("d1-runtime/read-cache.js");
const readCacheUrl = moduleDataUrl(`
export {
  cacheKeyStringsCouldFit,
  isReadCacheableQuery,
  payloadChangedDb,
  readCacheConfig,
  statementMayBeIdempotentSchemaDdl,
  statementMayChangeDb,
} from ${JSON.stringify(productionReadCacheUrl)};
export class D1ReadCache {
  constructor(env = {}) {
    this.invalidations = [];
    this.finished = [];
    this.retainedBytes = 0;
    this.config = { maxBytes: Number(env.maxBytes ?? env.D1_READ_CACHE_MAX_BYTES ?? 64 * 1024 * 1024) };
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances?.push(this);
  }
  beginRead(query, owner) {
    return /** @type {any} */ (globalThis).__d1RouterTestBeginRead?.(query, owner, this) || { hit: false, token: null };
  }
  finishRead(token, _query, _owner, bytes, valueEncoding = null) {
    this.finished.push({ token, bytes, valueEncoding });
    this.retainedBytes += /** @type {any} */ (globalThis).__d1RouterTestRetainedBytes?.(bytes) ?? bytes.byteLength;
    return true;
  }
  invalidate(reason) {
    this.invalidations.push(reason);
    this.retainedBytes = 0;
  }
  retire() {
    this.retainedBytes = 0;
  }
}
`);
const testHooksUrl = moduleDataUrl(`
export function assertD1TestHooksEnabled() {}
export function normalizeD1TestHookRequest(value) { return value; }
`);
const ownerClientUrl = moduleDataUrl(`
export async function forwardToOwner(...args) {
  return /** @type {any} */ (globalThis).__d1RouterForwardToOwner?.(...args) ||
    Response.json({ success: true });
}
export async function probeOwner(...args) {
  return /** @type {any} */ (globalThis).__d1RouterProbeOwner?.(...args) ||
    { outcome: "owner-alive" };
}
`);
const productionProtocolUrl = d1ProtocolDataUrl();
const {
  D1_ACTOR_QUERY_CONTENT_TYPE: PRODUCTION_D1_ACTOR_QUERY_CONTENT_TYPE,
} = await import(productionProtocolUrl);
const protocolUrl = moduleDataUrl(`
export class D1ProtocolError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
export { D1_ACTOR_QUERY_CONTENT_TYPE } from ${JSON.stringify(productionProtocolUrl)};
export function classifyD1Error() { return { status: 500, code: "internal-error" }; }
export function d1ErrorPayload() { return {}; }
export function encodeD1ActorQueryRequest(query, owner) {
  return JSON.stringify({ ...query, owner });
}
export function normalizeQueryRequest(value) { return value; }
export async function readD1QueryRequest(request) { return await request.json(); }
export async function readD1QueryResponseBytes(response) {
  return new Uint8Array(await response.arrayBuffer());
}
export async function readD1QueryResponseWithBytes(response) {
  globalThis.__d1RouterDecodeCalls = (globalThis.__d1RouterDecodeCalls || 0) + 1;
  const bytes = await readD1QueryResponseBytes(response);
  return { bytes, payload: JSON.parse(new TextDecoder().decode(bytes)) };
}
`);
const stateUrl = moduleDataUrl(`
export const SERVICE = "d1-runtime";
export const metrics = {
  increment(name, labels, value) {
    globalThis.__d1RouterMetricCalls?.push({ name, labels, value });
  },
  observe() {},
};
export function log() {}
`);
const httpUrl = moduleDataUrl(`
export function json(data, init = {}) {
  return Response.json(data, init);
}
export function d1QueryResponse(data, init = {}) {
  globalThis.__d1RouterReencodeCalls = (globalThis.__d1RouterReencodeCalls || 0) + 1;
  return Response.json(data, init);
}
export function d1QueryBytesResponse(bytes, init = {}) {
  return new Response(bytes, {
    ...init,
    headers: { "content-type": "application/" + "vnd.wdl.d1-query-response", ...(init.headers || {}) },
  });
}
`);

/** @param {string} readCacheModuleUrl @param {string} tag */
function routerModuleSource(readCacheModuleUrl, tag) {
  return `${applyModuleReplacements(readRepositoryFile("d1-runtime/router.js"), [
    [/from "d1-runtime-protocol";/, `from ${JSON.stringify(protocolUrl)};`],
    [/from "d1-runtime-task-identity";/, `from ${JSON.stringify(taskIdentityUrl)};`],
    [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
    [/from "shared-d1-timeout";/, `from ${JSON.stringify(timeoutUrl)};`],
    [/from "shared-d1-query-wire";/, `from ${JSON.stringify(d1QueryWireDataUrl())};`],
    [/from "d1-runtime-read-cache";/, `from ${JSON.stringify(readCacheModuleUrl)};`],
    [/from "d1-runtime-test-hooks";/, `from ${JSON.stringify(testHooksUrl)};`],
    [/from "d1-runtime-owner-registry";/, `from ${JSON.stringify(ownerRegistryUrl)};`],
    [/from "d1-runtime-owner-client";/, `from ${JSON.stringify(ownerClientUrl)};`],
    [/from "shared-owner-forwarder";/, `from ${JSON.stringify(sharedOwnerForwarderUrl())};`],
    [/from "d1-runtime-state";/, `from ${JSON.stringify(stateUrl)};`],
    [/from "d1-runtime-http";/, `from ${JSON.stringify(httpUrl)};`],
  ])}
export function routerReadCacheStateForTest() {
  return { keys: [...routerReadCaches.keys()], bytes: routerReadCacheBytes };
}
export function routerReadCacheForTest(dbKey) {
  return routerReadCaches.get(dbKey);
}
export function seedRouterReadCacheForTest(dbKey, env = {}) {
  const cache = new D1ReadCache(env, metrics, { service: SERVICE });
  routerReadCaches.set(dbKey, cache);
  return cache;
}
export function resetRouterReadCachesForTest() {
  routerReadCaches.clear();
  routerReadCacheBytes = 0;
}
// ${tag}
`;
}

const src = routerModuleSource(readCacheUrl, "stub-read-cache");

const {
  handleQuery,
  resetRouterReadCachesForTest,
  routeQueryToOwner,
  routerReadCacheStateForTest,
  seedRouterReadCacheForTest,
} = await import(moduleDataUrl(src));

test("D1 router uses takeover owner even after refresh is disabled", async () => {
  const query = {
    dbKey: "tenant-a:main",
    namespace: "tenant-a",
    databaseId: "main",
    binding: null,
    mode: "all",
    slot: 1,
    statements: [{ sql: "select 1", params: [] }],
  };
  const oldOwner = {
    dbKey: query.dbKey,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 7,
    leaseExpiresAt: Date.now() - 1_000,
  };
  const takeoverOwner = {
    dbKey: query.dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 8,
    leaseExpiresAt: Date.now() + 60_000,
  };
  /** @type {any} */ (globalThis).__d1RouterProbeOwner = async () => ({ outcome: "probe-unavailable" });
  /** @type {any} */ (globalThis).__d1RouterLeaseExpired = () => true;
  /** @type {any} */ (globalThis).__d1RouterTakeoverExpiredOwner = async () => takeoverOwner;
  /** @type {any} */ (globalThis).__d1RouterForwardToOwner = async (
    /** @type {unknown} */ _query,
    /** @type {unknown} */ _env,
    /** @type {{ taskId: string }} */ owner,
  ) => {
    throw new Error(`unexpected forward to ${owner.taskId}`);
  };
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async (/** @type {unknown} */ _url, /** @type {{ body?: unknown, headers?: HeadersInit }} */ init) => {
            assert.equal(
              new Headers(init.headers).get("content-type"),
              PRODUCTION_D1_ACTOR_QUERY_CONTENT_TYPE
            );
            return Response.json({
              success: true,
              owner: parseJsonObjectRequestBody(init, "D1 router forwarded request body").owner,
            });
          },
        };
      },
    },
  };
  try {
    const response = await routeQueryToOwner(query, env, oldOwner, false, "rid", 1);

    const body = await readJsonResponse(response, 200);
    assert.equal(response.headers.get("x-wdl-d1-owner-task-id"), "task-a");
    assert.equal(response.headers.get("x-wdl-d1-owner-generation"), "8");
    assert.equal(body.owner.taskId, "task-a");
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterProbeOwner;
    delete /** @type {any} */ (globalThis).__d1RouterLeaseExpired;
    delete /** @type {any} */ (globalThis).__d1RouterTakeoverExpiredOwner;
    delete /** @type {any} */ (globalThis).__d1RouterForwardToOwner;
  }
});

test("D1 router owner-not-ready errors do not expose owner task identity", async () => {
  const query = {
    dbKey: "tenant-a:main",
    namespace: "tenant-a",
    databaseId: "main",
    binding: null,
    mode: "all",
    slot: 1,
    statements: [{ sql: "select 1", params: [] }],
  };
  const owner = {
    dbKey: query.dbKey,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
  };
  /** @type {any} */ (globalThis).__d1RouterProbeOwner = async () => ({ outcome: "stale-generation" });
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async () => owner;
  try {
    await assert.rejects(
      () => routeQueryToOwner(query, {}, owner, false, "rid", 1),
      (err) => {
        assert.ok(err instanceof Error);
        assert.equal(/** @type {{ code?: unknown }} */ (err).code, "owner-not-ready");
        assert.match(err.message, /owner is stale-generation/);
        assert.doesNotMatch(err.message, /\btask-b\b/);
        return true;
      }
    );
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterProbeOwner;
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
  }
});

test("D1 router invalidates read cache for changed all/raw payloads without a cache token", async () => {
  const dbKey = "tenant-a:main-raw";
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: null });
  seedRouterReadCacheForTest(dbKey);
  const payload = {
    success: true,
    results: [{ id: "m1" }],
    meta: { changed_db: true },
  };
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => Response.json(payload),
        };
      },
    },
  };
  const request = new Request("http://d1-runtime/query", {
    method: "POST",
    body: JSON.stringify({
      dbKey,
      namespace: "tenant-a",
      databaseId: "main-raw",
      mode: "all",
      statements: [{ sql: "insert into messages (id) values ('m1') returning id", params: [] }],
    }),
  });

  const response = await handleQuery(request, env, "rid");

  assert.equal(response.status, 200);
  assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations, [
    "write",
    "write",
  ]);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].finished, []);
});

test("D1 router returns row/column payloads without objectifying internal responses", async () => {
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: "read-token" });
  const payload = {
    success: true,
    results: { columns: ["id"], rows: [["m1"]] },
    meta: { changed_db: false },
  };
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => Response.json(payload),
        };
      },
    },
  };
  const request = new Request("http://d1-runtime/query", {
    method: "POST",
    body: JSON.stringify({
      dbKey: "tenant-a:main-raw-read",
      namespace: "tenant-a",
      databaseId: "main-raw-read",
      mode: "all",
      statements: [{ sql: "select id from messages where id = ?", params: ["m1"] }],
    }),
  });

  const response = await handleQuery(request, env, "rid");

  await assertJsonResponse(response, 200, payload);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].finished, [{
    token: "read-token",
    bytes: new TextEncoder().encode(JSON.stringify(payload)),
    valueEncoding: null,
  }]);
});

test("D1 router forwards success-classified bytes without decoding them", async () => {
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: "read-token" });
  /** @type {any} */ (globalThis).__d1RouterReencodeCalls = 0;
  /** @type {any} */ (globalThis).__d1RouterDecodeCalls = 0;
  const wireBody = "opaque-success-bytes";
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => new Response(wireBody, {
            headers: {
              "content-type": "application/" + "vnd.wdl.d1-query-response",
              "x-wdl-d1-result": "ok",
              "x-wdl-d1-changed-db": "0",
              "x-wdl-d1-value-encoding": "native-bytes-v1",
            },
          }),
        };
      },
    },
  };
  const request = new Request("http://d1-runtime/query", {
    method: "POST",
    body: JSON.stringify({
      dbKey: "tenant-a:wire-passthrough",
      namespace: "tenant-a",
      databaseId: "wire-passthrough",
      mode: "all",
      statements: [{ sql: "select id from messages", params: [] }],
    }),
  });

  try {
    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 200);
    assert.equal(await response.text(), wireBody);
    assert.equal(response.headers.get("x-wdl-d1-result"), "ok");
    assert.equal(response.headers.get("x-wdl-d1-changed-db"), "0");
    assert.equal(response.headers.get("x-wdl-d1-value-encoding"), "native-bytes-v1");
    assert.equal(/** @type {any} */ (globalThis).__d1RouterDecodeCalls, 0);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterReencodeCalls, 0);
    assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].finished, [{
      token: "read-token",
      bytes: new TextEncoder().encode(wireBody),
      valueEncoding: "native-bytes-v1",
    }]);
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterTestBeginRead;
    delete /** @type {any} */ (globalThis).__d1RouterDecodeCalls;
    delete /** @type {any} */ (globalThis).__d1RouterReencodeCalls;
  }
});

test("D1 router preserves an explicit empty value-encoding header", async () => {
  /** @type {any} */ (globalThis).__d1RouterDecodeCalls = 0;
  const payload = { success: true, results: [] };
  const env = {
    D1_READ_CACHE_TTL_MS: "0",
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => Response.json(payload, {
            headers: {
              "x-wdl-d1-result": "ok",
              "x-wdl-d1-changed-db": "0",
              "x-wdl-d1-value-encoding": "",
            },
          }),
        };
      },
    },
  };
  const request = new Request("http://d1-runtime/query", { method: "POST" });

  try {
    const response = await handleQuery(request, env, "rid", {
      read: async () => ({
        dbKey: "tenant-a:empty-encoding",
        namespace: "tenant-a",
        databaseId: "empty-encoding",
        binding: null,
        mode: "all",
        slot: 1,
        statements: [{ sql: "select 1", params: [] }],
      }),
    });

    assert.equal(response.status, 200);
    assert.equal(response.headers.has("x-wdl-d1-value-encoding"), true);
    assert.equal(response.headers.get("x-wdl-d1-value-encoding"), "");
    assert.deepEqual(await readJsonResponse(response, 200), payload);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterDecodeCalls, 1);
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterDecodeCalls;
  }
});

test("D1 router does not cache an unsupported value encoding", async () => {
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({
    hit: false,
    token: "read-token",
  });
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => Response.json(
            { success: true, results: { columns: [], rows: [] } },
            {
              headers: {
                "x-wdl-d1-result": "ok",
                "x-wdl-d1-changed-db": "0",
                "x-wdl-d1-value-encoding": "future-v2",
              },
            }
          ),
        };
      },
    },
  };
  const request = new Request("http://d1-runtime/query", {
    method: "POST",
    body: JSON.stringify({
      dbKey: "tenant-a:unsupported-encoding",
      namespace: "tenant-a",
      databaseId: "unsupported-encoding",
      mode: "all",
      statements: [{ sql: "select 1", params: [] }],
    }),
  });

  try {
    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("x-wdl-d1-value-encoding"), "future-v2");
    assert.deepEqual(
      /** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].finished,
      []
    );
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterTestBeginRead;
  }
});

test("D1 router replays cached query response bytes without re-encoding", async () => {
  const wireBody = '{"success":true, "results":["cached"],"meta":{"changed_db":false}}';
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({
    hit: true,
    bytes: new TextEncoder().encode(wireBody),
    valueEncoding: "native-bytes-v1",
  });
  /** @type {any} */ (globalThis).__d1RouterReencodeCalls = 0;
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        throw new Error("cache hit must not reach the actor");
      },
    },
  };
  const request = new Request("http://d1-runtime/query", {
    method: "POST",
    body: JSON.stringify({
      dbKey: "tenant-a:wire-cache-hit",
      namespace: "tenant-a",
      databaseId: "wire-cache-hit",
      mode: "all",
      statements: [{ sql: "select value from cache", params: [] }],
    }),
  });

  try {
    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 200);
    assert.equal(await response.text(), wireBody);
    assert.equal(response.headers.get("x-wdl-d1-result"), "ok");
    assert.equal(response.headers.get("x-wdl-d1-changed-db"), "0");
    assert.equal(response.headers.get("x-wdl-d1-value-encoding"), "native-bytes-v1");
    assert.equal(/** @type {any} */ (globalThis).__d1RouterReencodeCalls, 0);
  } finally {
    delete /** @type {any} */ (globalThis).__d1RouterTestBeginRead;
    delete /** @type {any} */ (globalThis).__d1RouterReencodeCalls;
  }
});

test("D1 router bounds aggregate read-cache bytes across databases", async () => {
  resetRouterReadCachesForTest();
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = (
    /** @type {{ dbKey: string }} */ query
  ) => ({ hit: false, token: query.dbKey });
  /** @type {any} */ (globalThis).__d1RouterTestRetainedBytes = () => 700;
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async (
    /** @type {unknown} */ _env,
    /** @type {{ dbKey: string }} */ query
  ) => ({
    dbKey: query.dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 1,
  });
  const env = {
    D1_READ_CACHE_MAX_BYTES: "1024",
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get() {
        return {
          fetch: async () => new Response(Uint8Array.from({ length: 8 }, () => 1), {
            headers: {
              "x-wdl-d1-result": "ok",
              "x-wdl-d1-changed-db": "0",
              "x-wdl-d1-value-encoding": "native-bytes-v1",
            },
          }),
        };
      },
    },
  };

  /** @param {string} dbKey */
  const read = async (dbKey) => {
    const query = {
      dbKey,
      namespace: "tenant-a",
      databaseId: dbKey,
      binding: null,
      mode: "all",
      slot: 1,
      statements: [{ sql: "select value from cache", params: [] }],
    };
    const response = await handleQuery(
      new Request("http://d1-runtime/query", { method: "POST" }),
      env,
      "rid",
      { read: async () => query }
    );
    assert.equal(response.status, 200);
  };

  try {
    await read("tenant-a:cache-a");
    assert.deepEqual(routerReadCacheStateForTest(), {
      keys: ["tenant-a:cache-a"],
      bytes: 700,
    });

    await read("tenant-a:cache-b");
    assert.deepEqual(routerReadCacheStateForTest(), {
      keys: ["tenant-a:cache-b"],
      bytes: 700,
    });

    const writeResponse = await handleQuery(
      new Request("http://d1-runtime/query", { method: "POST" }),
      env,
      "rid",
      {
        read: async () => ({
          dbKey: "tenant-a:cache-b",
          namespace: "tenant-a",
          databaseId: "tenant-a:cache-b",
          binding: null,
          mode: "run",
          slot: 1,
          statements: [{ sql: "insert into cache values (1)", params: [] }],
        }),
      }
    );
    assert.equal(writeResponse.status, 200);
    assert.deepEqual(routerReadCacheStateForTest(), {
      keys: ["tenant-a:cache-b"],
      bytes: 0,
    });
  } finally {
    resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterTestBeginRead;
    delete /** @type {any} */ (globalThis).__d1RouterTestRetainedBytes;
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
  }
});

test("D1 router refreshes cross-database LRU recency on cache hits", async () => {
  const productionRouter = await import(moduleDataUrl(routerModuleSource(
    productionReadCacheUrl,
    "production-read-cache-lru"
  )));
  productionRouter.resetRouterReadCachesForTest();
  const calls = new Map();
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async (
    /** @type {unknown} */ _env,
    /** @type {{ dbKey: string }} */ query
  ) => ({
    dbKey: query.dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 1,
  });
  const env = {
    D1_READ_CACHE_MAX_BYTES: "3000",
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get(/** @type {string} */ dbKey) {
        return {
          async fetch() {
            calls.set(dbKey, (calls.get(dbKey) || 0) + 1);
            return new Response(new Uint8Array(900), {
              headers: {
                "x-wdl-d1-result": "ok",
                "x-wdl-d1-changed-db": "0",
                "x-wdl-d1-value-encoding": "native-bytes-v1",
              },
            });
          },
        };
      },
    },
  };
  /** @param {string} dbKey */
  const read = async (dbKey) => {
    const response = await productionRouter.handleQuery(
      new Request("http://d1-runtime/query", { method: "POST" }),
      env,
      "rid",
      {
        read: async () => ({
          dbKey,
          namespace: "tenant-a",
          databaseId: dbKey,
          binding: null,
          mode: "all",
          slot: 1,
          statements: [{ sql: "select value from cache", params: [] }],
        }),
      }
    );
    assert.equal(response.status, 200);
  };

  try {
    await read("tenant-a:lru-a");
    await read("tenant-a:lru-b");
    await read("tenant-a:lru-a");
    assert.equal(calls.get("tenant-a:lru-a"), 1);

    await read("tenant-a:lru-c");
    await read("tenant-a:lru-b");
    assert.equal(calls.get("tenant-a:lru-b"), 2);
  } finally {
    productionRouter.resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
  }
});

test("D1 router creates cache instances only after request-side admission", async () => {
  const productionRouter = await import(moduleDataUrl(routerModuleSource(
    productionReadCacheUrl,
    "production-read-cache-admission"
  )));
  productionRouter.resetRouterReadCachesForTest();
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async (
    /** @type {unknown} */ _env,
    /** @type {{ dbKey: string }} */ query
  ) => ({
    dbKey: query.dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 1,
  });
  let actorCalls = 0;
  /** @type {any} */ (globalThis).__d1RouterMetricCalls = [];
  const databases = {
    idFromName(/** @type {string} */ dbKey) { return dbKey; },
    get() {
      return {
        async fetch(/** @type {unknown} */ _url, /** @type {{ body?: unknown }} */ init) {
          actorCalls += 1;
          const routed = parseJsonObjectRequestBody(init, "D1 actor request body");
          const changedDb = routed.mode === "run" || routed.mode === "exec";
          return new Response(Uint8Array.of(actorCalls), {
            headers: {
              "x-wdl-d1-result": "ok",
              "x-wdl-d1-changed-db": changedDb ? "1" : "0",
              "x-wdl-d1-value-encoding": "native-bytes-v1",
            },
          });
        },
      };
    },
  };
  const cases = [
    {
      name: "mutation",
      mode: "run",
      statements: [{ sql: "insert into records values (1)", params: [] }],
    },
    {
      name: "non-read mode",
      mode: "exec",
      statements: [{ sql: "create table if not exists records (id integer)", params: [] }],
    },
    {
      name: "BLOB parameter",
      mode: "all",
      statements: [{ sql: "select ?", params: [new Uint8Array([1])] }],
    },
    {
      name: "volatile read",
      mode: "all",
      statements: [{ sql: "select random()", params: [] }],
    },
    {
      name: "multi-statement read",
      mode: "all",
      statements: [
        { sql: "select 1", params: [] },
        { sql: "select 2", params: [] },
      ],
    },
    {
      name: "disabled cache",
      mode: "all",
      statements: [{ sql: "select 1", params: [] }],
      env: { D1_READ_CACHE_TTL_MS: "0" },
    },
    {
      name: "oversized cache key",
      mode: "all",
      statements: [{ sql: "select 1", params: [] }],
      env: { D1_READ_CACHE_MAX_BYTES: "64" },
    },
    {
      name: "exact key budget bypass",
      dbKey: "a:b",
      mode: "all",
      statements: [{ sql: "select 1", params: [] }],
      env: { D1_READ_CACHE_MAX_BYTES: "168" },
    },
  ];

  try {
    for (const item of cases) {
      const bypassMetricsBefore = /** @type {any[]} */ (
        /** @type {any} */ (globalThis).__d1RouterMetricCalls
      ).filter((call) => call.name === "d1_read_cache" && call.labels?.outcome === "bypass").length;
      const response = await productionRouter.handleQuery(
        new Request("http://d1-runtime/query", { method: "POST" }),
        { D1_DATABASES: databases, ...item.env },
        "rid",
        {
          read: async () => ({
            dbKey: item.dbKey ?? `tenant-a:${item.name}`,
            namespace: "tenant-a",
            databaseId: item.name,
            binding: null,
            mode: item.mode,
            slot: 1,
            statements: item.statements,
          }),
        }
      );
      assert.equal(response.status, 200, item.name);
      assert.deepEqual(productionRouter.routerReadCacheStateForTest().keys, [], item.name);
      const bypassMetricsAfter = /** @type {any[]} */ (
        /** @type {any} */ (globalThis).__d1RouterMetricCalls
      ).filter((call) => call.name === "d1_read_cache" && call.labels?.outcome === "bypass").length;
      assert.equal(
        bypassMetricsAfter - bypassMetricsBefore,
        item.mode === "all" || item.mode === "raw" ? 1 : 0,
        item.name
      );
    }
    assert.equal(actorCalls, cases.length);
  } finally {
    productionRouter.resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
    delete /** @type {any} */ (globalThis).__d1RouterMetricCalls;
  }
});

/** @param {"success" | "post-commit transport rejection" | "response body rejection"} completion */
async function assertPreInvalidatedMutationRace(completion) {
  const productionRouter = await import(moduleDataUrl(routerModuleSource(
    productionReadCacheUrl,
    `production-read-cache-write-race-${completion.replaceAll(" ", "-")}`
  )));
  productionRouter.resetRouterReadCachesForTest();
  const dbKey = `tenant-a:write-race-${completion.replaceAll(" ", "-")}`;
  const writeStarted = Promise.withResolvers();
  const releaseWrite = Promise.withResolvers();
  let writeCommitted = false;
  let readCalls = 0;
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async () => ({
    dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 1,
  });
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ key) { return key; },
      get() {
        return {
          async fetch(/** @type {unknown} */ _url, /** @type {{ body?: unknown }} */ init) {
            const routed = parseJsonObjectRequestBody(init, "D1 actor request body");
            if (routed.mode === "run") {
              writeStarted.resolve(undefined);
              await releaseWrite.promise;
              writeCommitted = true;
              if (completion === "post-commit transport rejection") {
                throw new Error("actor response was lost");
              }
              if (completion === "response body rejection") {
                return new Response(new ReadableStream({
                  start(controller) {
                    controller.error(new Error("actor response body was lost"));
                  },
                }), {
                  headers: {
                    "x-wdl-d1-result": "ok",
                    "x-wdl-d1-changed-db": "1",
                    "x-wdl-d1-value-encoding": "native-bytes-v1",
                  },
                });
              }
              return new Response(Uint8Array.of(9), {
                headers: {
                  "x-wdl-d1-result": "ok",
                  "x-wdl-d1-changed-db": "1",
                  "x-wdl-d1-value-encoding": "native-bytes-v1",
                },
              });
            }
            readCalls += 1;
            return new Response(Uint8Array.of(writeCommitted ? 2 : 1), {
              headers: {
                "x-wdl-d1-result": "ok",
                "x-wdl-d1-changed-db": "0",
                "x-wdl-d1-value-encoding": "native-bytes-v1",
              },
            });
          },
        };
      },
    },
  };
  /** @param {"all" | "run"} mode @param {string} sql */
  const query = async (mode, sql) => {
    const response = await productionRouter.handleQuery(
      new Request("http://d1-runtime/query", { method: "POST" }),
      env,
      "rid",
      {
        read: async () => ({
          dbKey,
          namespace: "tenant-a",
          databaseId: "write-race",
          binding: null,
          mode,
          slot: 1,
          statements: [{ sql, params: [] }],
        }),
      }
    );
    return {
      status: response.status,
      bytes: new Uint8Array(await response.arrayBuffer()),
    };
  };

  const write = query("run", "insert into cache values (1)");
  try {
    await writeStarted.promise;
    const racingRead = await query("all", "select value from cache");
    assert.equal(racingRead.status, 200);
    assert.equal(racingRead.bytes[0], 1);
    releaseWrite.resolve(undefined);
    assert.equal((await write).status, completion === "success" ? 200 : 500);
    const finalRead = await query("all", "select value from cache");
    assert.equal(finalRead.status, 200);
    assert.equal(finalRead.bytes[0], 2);
    assert.equal(readCalls, 2);
  } finally {
    releaseWrite.resolve(undefined);
    await write.catch(() => {});
    productionRouter.resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
  }
}

test("D1 router invalidates reads completed during a successful pre-invalidated mutation", async () => {
  await assertPreInvalidatedMutationRace("success");
});

test("D1 router invalidates racing reads when an actor-completed mutation result is lost", async () => {
  for (const completion of ["post-commit transport rejection", "response body rejection"]) {
    await assertPreInvalidatedMutationRace(
      /** @type {"post-commit transport rejection" | "response body rejection"} */ (completion)
    );
  }
});

test("D1 router pre-invalidates write SQL dispatched through all/raw modes", async (t) => {
  for (const writeMode of ["all", "raw"]) {
    await t.test(writeMode, async () => {
      const productionRouter = await import(moduleDataUrl(routerModuleSource(
        productionReadCacheUrl,
        `production-read-cache-${writeMode}-write-pre-invalidation`
      )));
      productionRouter.resetRouterReadCachesForTest();
      const dbKey = `tenant-a:${writeMode}-write-pre-invalidation`;
      const writeStarted = Promise.withResolvers();
      const releaseWrite = Promise.withResolvers();
      let writeCommitted = false;
      let readActorCalls = 0;
      /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async () => ({
        dbKey,
        taskId: "task-a",
        endpoint: "d1-runtime-a:8787",
        generation: 1,
      });
      const env = {
        D1_DATABASES: {
          idFromName(/** @type {string} */ key) { return key; },
          get() {
            return {
              async fetch(/** @type {unknown} */ _url, /** @type {{ body?: unknown }} */ init) {
                const routed = /** @type {any} */ (
                  parseJsonObjectRequestBody(init, "D1 actor request body")
                );
                const sql = String(routed.statements?.[0]?.sql || "");
                if (sql.startsWith("insert")) {
                  writeStarted.resolve(undefined);
                  await releaseWrite.promise;
                  writeCommitted = true;
                  return new Response(Uint8Array.of(9), {
                    headers: {
                      "x-wdl-d1-result": "ok",
                      "x-wdl-d1-changed-db": "1",
                      "x-wdl-d1-value-encoding": "native-bytes-v1",
                    },
                  });
                }
                readActorCalls += 1;
                return new Response(Uint8Array.of(writeCommitted ? 2 : 1), {
                  headers: {
                    "x-wdl-d1-result": "ok",
                    "x-wdl-d1-changed-db": "0",
                    "x-wdl-d1-value-encoding": "native-bytes-v1",
                  },
                });
              },
            };
          },
        },
      };
      /** @param {"all" | "raw"} mode @param {string} sql */
      const query = async (mode, sql) => {
        const response = await productionRouter.handleQuery(
          new Request("http://d1-runtime/query", { method: "POST" }),
          env,
          "rid",
          {
            read: async () => ({
              dbKey,
              namespace: "tenant-a",
              databaseId: `${writeMode}-write-pre-invalidation`,
              binding: null,
              mode,
              slot: 1,
              statements: [{ sql, params: [] }],
            }),
          }
        );
        return new Uint8Array(await response.arrayBuffer());
      };

      try {
        assert.equal((await query("all", "select value from cache"))[0], 1);
        assert.equal(readActorCalls, 1);
        const write = query(
          /** @type {"all" | "raw"} */ (writeMode),
          "insert into cache values (1) returning value"
        );
        await writeStarted.promise;

        assert.equal((await query("all", "select value from cache"))[0], 1);
        assert.equal(readActorCalls, 2);

        releaseWrite.resolve(undefined);
        await write;
        assert.equal((await query("all", "select value from cache"))[0], 2);
        assert.equal(readActorCalls, 3);
      } finally {
        releaseWrite.resolve(undefined);
        productionRouter.resetRouterReadCachesForTest();
        delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
      }
    });
  }
});

test("D1 router retires an evicted cache held by an in-flight read", async () => {
  const productionRouter = await import(moduleDataUrl(routerModuleSource(
    repositoryFileUrl("d1-runtime/read-cache.js"),
    "production-read-cache-retirement"
  )));
  productionRouter.resetRouterReadCachesForTest();
  const staleReadStarted = Promise.withResolvers();
  const releaseStaleRead = Promise.withResolvers();
  const calls = new Map();
  const dbA = "tenant-a:cache-instance-a";
  const dbB = "tenant-a:cache-instance-b";
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async (
    /** @type {unknown} */ _env,
    /** @type {{ dbKey: string }} */ query
  ) => ({
    dbKey: query.dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 1,
  });
  const env = {
    D1_READ_CACHE_MAX_BYTES: "2048",
    D1_DATABASES: {
      idFromName(/** @type {string} */ dbKey) { return dbKey; },
      get(/** @type {string} */ dbKey) {
        return {
          async fetch() {
            const call = (calls.get(dbKey) || 0) + 1;
            calls.set(dbKey, call);
            if (dbKey === dbA && call === 2) {
              staleReadStarted.resolve(undefined);
              await releaseStaleRead.promise;
            }
            const fill = dbKey === dbA ? call : 7;
            const size = dbKey === dbB ? 1200 : (call === 1 ? 800 : 32);
            return new Response(Uint8Array.from({ length: size }, () => fill), {
              headers: {
                "x-wdl-d1-result": "ok",
                "x-wdl-d1-changed-db": "0",
                "x-wdl-d1-value-encoding": "native-bytes-v1",
              },
            });
          },
        };
      },
    },
  };
  /** @param {string} dbKey @param {string} sql */
  const query = async (dbKey, sql) => {
    const response = await productionRouter.handleQuery(
      new Request("http://d1-runtime/query", { method: "POST" }),
      env,
      "rid",
      {
        read: async () => ({
          dbKey,
          namespace: "tenant-a",
          databaseId: dbKey,
          binding: null,
          mode: "all",
          slot: 1,
          statements: [{
            sql,
            params: [],
          }],
        }),
      }
    );
    assert.equal(response.status, 200);
    return new Uint8Array(await response.arrayBuffer());
  };

  await query(dbA, "select value from cache where id = 'prime'");
  const evictedCache = productionRouter.routerReadCacheForTest(dbA);
  assert.ok(evictedCache);
  assert.equal(evictedCache.entries.size, 1);
  assert.ok(evictedCache.retainedBytes > 800);
  const staleRead = query(dbA, "select value from cache where id = 'slow'");
  try {
    await staleReadStarted.promise;
    await query(dbB, "select value from cache");
    assert.equal(productionRouter.routerReadCacheStateForTest().keys.includes(dbA), false);
    assert.equal(evictedCache.entries.size, 0);
    assert.equal(evictedCache.retainedBytes, 0);

    releaseStaleRead.resolve(undefined);
    assert.equal((await staleRead)[0], 2);
    assert.equal((await query(dbA, "select value from cache where id = 'slow'"))[0], 3);
    assert.equal(calls.get(dbA), 3);
  } finally {
    releaseStaleRead.resolve(undefined);
    await staleRead.catch(() => {});
    productionRouter.resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
  }
});

test("D1 router delays idempotent DDL invalidation until actor reports changed_db", async () => {
  for (const [changedDb, expectedInvalidations] of [
    [false, []],
    [true, ["changed-db"]],
  ]) {
    const dbKey = `tenant-a:ddl-${changedDb}`;
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
    seedRouterReadCacheForTest(dbKey);
    /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => {
      throw new Error("exec should not enter read-cache beginRead");
    };
    const env = {
      D1_DATABASES: {
        idFromName(/** @type {string} */ dbKey) { return dbKey; },
        get() {
          return {
            fetch: async () => Response.json(
              { count: 1, duration: 1 },
              { headers: { "x-wdl-d1-changed-db": changedDb ? "1" : "0" } }
            ),
          };
        },
      },
    };
    const request = new Request("http://d1-runtime/query", {
      method: "POST",
      body: JSON.stringify({
        dbKey,
        namespace: "tenant-a",
        databaseId: `ddl-${changedDb}`,
        mode: "exec",
        statements: [{ sql: "create table if not exists inspections (id text)", params: [] }],
      }),
    });

    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 200);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1);
    assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations, expectedInvalidations);
  }
});

test("D1 router invalidates errors for delayed and pre-invalidated mutations", async () => {
  const cases = [
    {
      name: "delayed exec DDL",
      mode: "exec",
      statements: [
        { sql: "create table if not exists inspections (id text)", params: [] },
        { sql: "create index if not exists idx_missing on missing_table(id)", params: [] },
      ],
      expectedInvalidations: ["write"],
    },
    {
      name: "non-cacheable all-mode write",
      mode: "all",
      statements: [{ sql: "insert into inspections (id) values ('i2'); select * from missing_table", params: [] }],
      expectedInvalidations: ["write", "write"],
    },
  ];

  for (const item of cases) {
    const dbKey = `tenant-a:error-${item.mode}`;
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
    seedRouterReadCacheForTest(dbKey);
    /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: null });
    const env = {
      D1_DATABASES: {
        idFromName(/** @type {string} */ dbKey) { return dbKey; },
        get() {
          return {
            fetch: async () => Response.json(
              { success: false, error: "d1-error", message: "statement failed" },
              { status: 400 }
            ),
          };
        },
      },
    };
    const request = new Request("http://d1-runtime/query", {
      method: "POST",
      body: JSON.stringify({
        dbKey,
        namespace: "tenant-a",
        databaseId: `error-${item.mode}`,
        mode: item.mode,
        statements: item.statements,
      }),
    });

    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 400, item.name);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1, item.name);
    assert.deepEqual(
      /** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations,
      item.expectedInvalidations,
      item.name
    );
  }
});

test("D1 router invalidates delayed DDL when actor completion is unknown", async () => {
  const cases = [
    {
      name: "transport rejection",
      fetch: async () => {
        throw new Error("actor response was lost");
      },
    },
    {
      name: "response body rejection",
      fetch: async () => new Response(new ReadableStream({
        start(controller) {
          controller.error(new Error("actor response body was lost"));
        },
      })),
    },
  ];

  for (const item of cases) {
    const dbKey = `tenant-a:lost-ack-${item.name.replaceAll(" ", "-")}`;
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
    seedRouterReadCacheForTest(dbKey);
    /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => {
      throw new Error("exec should not enter read-cache beginRead");
    };
    const env = {
      D1_DATABASES: {
        idFromName(/** @type {string} */ key) { return key; },
        get() { return { fetch: item.fetch }; },
      },
    };
    const request = new Request("http://d1-runtime/query", {
      method: "POST",
      body: JSON.stringify({
        dbKey,
        namespace: "tenant-a",
        databaseId: dbKey.slice("tenant-a:".length),
        mode: "exec",
        statements: [{ sql: "create table if not exists inspections (id text)", params: [] }],
      }),
    });

    const response = await handleQuery(request, env, "rid");

    assert.equal(response.status, 500, item.name);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1, item.name);
    assert.deepEqual(
      /** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations,
      ["write"],
      item.name
    );
  }
});

test("D1 router invalidates a cache rebuilt after takeover when DDL completion is unknown", async () => {
  const dbKey = "tenant-a:takeover-lost-ack";
  const remoteOwner = {
    dbKey,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 7,
    leaseExpiresAt: Date.now() - 1_000,
  };
  const localOwner = {
    dbKey,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 8,
    leaseExpiresAt: Date.now() + 60_000,
  };
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: "read-token" });
  /** @type {any} */ (globalThis).__d1RouterResolveDbOwner = async (
    /** @type {unknown} */ _env,
    /** @type {{ mode?: string }} */ query,
  ) => query.mode === "exec" ? remoteOwner : localOwner;
  /** @type {any} */ (globalThis).__d1RouterProbeOwner = async () => ({ outcome: "probe-unavailable" });
  /** @type {any} */ (globalThis).__d1RouterLeaseExpired = () => true;
  /** @type {any} */ (globalThis).__d1RouterTakeoverExpiredOwner = async () => localOwner;

  /** @type {any} */
  const env = {
    D1_DATABASES: {
      idFromName(/** @type {string} */ key) { return key; },
      get() {
        return {
          async fetch(/** @type {unknown} */ _url, /** @type {{ body?: unknown }} */ init) {
            const routed = parseJsonObjectRequestBody(init, "D1 actor request body");
            if (routed.mode === "exec") {
              const readResponse = await handleQuery(new Request("http://d1-runtime/query", {
                method: "POST",
                body: JSON.stringify({
                  dbKey,
                  namespace: "tenant-a",
                  databaseId: "takeover-lost-ack",
                  mode: "all",
                  statements: [{ sql: "select name from sqlite_schema", params: [] }],
                }),
              }), env, "read-rid");
              assert.equal(readResponse.status, 200);
              throw new Error("DDL response was lost after takeover");
            }
            return Response.json(
              { success: true, results: [] },
              { headers: { "x-wdl-d1-changed-db": "0" } }
            );
          },
        };
      },
    },
  };

  try {
    resetRouterReadCachesForTest();
    const response = await handleQuery(new Request("http://d1-runtime/query", {
      method: "POST",
      body: JSON.stringify({
        dbKey,
        namespace: "tenant-a",
        databaseId: "takeover-lost-ack",
        mode: "exec",
        statements: [{ sql: "create table if not exists inspections (id text)", params: [] }],
      }),
    }), env, "ddl-rid");

    assert.equal(response.status, 500);
    assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1);
    assert.deepEqual(
      /** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations,
      ["write"]
    );
  } finally {
    resetRouterReadCachesForTest();
    delete /** @type {any} */ (globalThis).__d1RouterTestBeginRead;
    delete /** @type {any} */ (globalThis).__d1RouterResolveDbOwner;
    delete /** @type {any} */ (globalThis).__d1RouterProbeOwner;
    delete /** @type {any} */ (globalThis).__d1RouterLeaseExpired;
    delete /** @type {any} */ (globalThis).__d1RouterTakeoverExpiredOwner;
  }
});
