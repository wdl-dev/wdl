import { test } from "node:test";
import assert from "node:assert/strict";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { applyModuleReplacements, moduleDataUrl, readRepositoryFile } from "../helpers/load-shared-module.js";
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
const readCacheUrl = moduleDataUrl(`
export class D1ReadCache {
  constructor() {
    this.invalidations = [];
    this.finished = [];
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances?.push(this);
  }
  beginRead(query, owner) {
    return /** @type {any} */ (globalThis).__d1RouterTestBeginRead?.(query, owner, this) || { hit: false, token: null };
  }
  finishRead(token, payload) {
    this.finished.push({ token, payload });
    return true;
  }
  invalidate(reason) {
    this.invalidations.push(reason);
  }
}
export function statementMayBeIdempotentSchemaDdl(sql) {
  return /^\\s*create\\s+(?:table|(?:unique\\s+)?index)\\s+if\\s+not\\s+exists\\b/i.test(sql);
}
export function statementMayChangeDb(sql) {
  return /\\b(?:insert|update|delete|replace|create|drop|alter|pragma)\\b/i.test(String(sql || ""));
}
export function payloadChangedDb(payload) {
  const items = Array.isArray(payload) ? payload : [payload];
  return items.some((item) => item?.meta?.changed_db === true);
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
const ownerForwarderUrl = moduleDataUrl(`
export function parseForwardHopCount(value) {
  const parsed = Number(value ?? "0");
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : 0;
}
`);
const protocolUrl = moduleDataUrl(`
export class D1ProtocolError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
export const D1_ACTOR_QUERY_CONTENT_TYPE = "application/" + "x-wdl-d1-actor-query-v1";
export function classifyD1Error() { return { status: 500, code: "internal-error" }; }
export function d1ErrorPayload() { return {}; }
export function encodeD1ActorQueryRequest(query, owner) {
  return JSON.stringify({ ...query, owner });
}
export function normalizeQueryRequest(value) { return value; }
export async function readD1QueryRequest(request) { return await request.json(); }
export async function readD1QueryResponse(response) { return await response.json(); }
`);
const stateUrl = moduleDataUrl(`
export const SERVICE = "d1-runtime";
export const metrics = { increment() {}, observe() {} };
export function log() {}
`);
const httpUrl = moduleDataUrl(`
export function json(data, init = {}) {
  return Response.json(data, init);
}
export function d1QueryResponse(data, init = {}) {
  return Response.json(data, init);
}
`);

const src = applyModuleReplacements(readRepositoryFile("d1-runtime/router.js"), [
  [/from "d1-runtime-protocol";/, `from ${JSON.stringify(protocolUrl)};`],
  [/from "d1-runtime-task-identity";/, `from ${JSON.stringify(taskIdentityUrl)};`],
  [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
  [/from "shared-d1-timeout";/, `from ${JSON.stringify(timeoutUrl)};`],
  [/from "d1-runtime-read-cache";/, `from ${JSON.stringify(readCacheUrl)};`],
  [/from "d1-runtime-test-hooks";/, `from ${JSON.stringify(testHooksUrl)};`],
  [/from "d1-runtime-owner-registry";/, `from ${JSON.stringify(ownerRegistryUrl)};`],
  [/from "d1-runtime-owner-client";/, `from ${JSON.stringify(ownerClientUrl)};`],
  [/from "shared-owner-forwarder";/, `from ${JSON.stringify(ownerForwarderUrl)};`],
  [/from "d1-runtime-state";/, `from ${JSON.stringify(stateUrl)};`],
  [/from "d1-runtime-http";/, `from ${JSON.stringify(httpUrl)};`],
]);

const { handleQuery, parseHopCount, routeQueryToOwner } = await import(moduleDataUrl(src));

test("D1 router parseHopCount rejects NaN and negative hop headers", () => {
  assert.equal(parseHopCount("abc"), 0);
  assert.equal(parseHopCount("-1"), 0);
  assert.equal(parseHopCount("5"), 5);
  assert.equal(parseHopCount("1.9"), 1);
  assert.equal(parseHopCount(null), 0);
});

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
          fetch: async (/** @type {unknown} */ _url, /** @type {{ body?: unknown }} */ init) => Response.json({
            success: true,
            owner: parseJsonObjectRequestBody(init, "D1 router forwarded request body").owner,
          }),
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
  /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
  /** @type {any} */ (globalThis).__d1RouterTestBeginRead = () => ({ hit: false, token: null });
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
      dbKey: "tenant-a:main-raw",
      namespace: "tenant-a",
      databaseId: "main-raw",
      mode: "all",
      statements: [{ sql: "insert into messages (id) values ('m1') returning id", params: [] }],
    }),
  });

  const response = await handleQuery(request, env, "rid");

  assert.equal(response.status, 200);
  assert.equal(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances.length, 1);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations, ["changed-db"]);
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
  assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].finished, [{ token: "read-token", payload }]);
});

test("D1 router delays idempotent DDL invalidation until actor reports changed_db", async () => {
  for (const [changedDb, expectedInvalidations] of [
    [false, []],
    [true, ["changed-db"]],
  ]) {
    const dbKey = `tenant-a:ddl-${changedDb}`;
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
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

test("D1 router invalidates on errors after skipped mutation pre-invalidation", async () => {
  const cases = [
    {
      name: "delayed exec DDL",
      mode: "exec",
      statements: [
        { sql: "create table if not exists inspections (id text)", params: [] },
        { sql: "create index if not exists idx_missing on missing_table(id)", params: [] },
      ],
    },
    {
      name: "non-cacheable all-mode write",
      mode: "all",
      statements: [{ sql: "insert into inspections (id) values ('i2'); select * from missing_table", params: [] }],
    },
  ];

  for (const item of cases) {
    const dbKey = `tenant-a:error-${item.mode}`;
    /** @type {any} */ (globalThis).__d1RouterTestCacheInstances = [];
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
    assert.deepEqual(/** @type {any} */ (globalThis).__d1RouterTestCacheInstances[0].invalidations, ["write"], item.name);
  }
});
