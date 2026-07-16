import { test } from "node:test";
import assert from "node:assert/strict";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { controlD1RuntimeClientDataUrl } from "../helpers/load-d1-protocol.js";
import { applyModuleReplacements, moduleDataUrl, readRepositoryFile } from "../helpers/load-shared-module.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

const controlSharedUrl = controlSharedStubUrl(`
export const state = {
  get redis() {
    return /** @type {any} */ (globalThis).__d1LifecycleRedis;
  },
  log(level, event, fields) {
    /** @type {any} */ (globalThis).__d1LifecycleLogs = /** @type {any} */ (globalThis).__d1LifecycleLogs || [];
    /** @type {any} */ (globalThis).__d1LifecycleLogs.push({ level, event, fields });
  },
  service: "control",
};
`);

const controlLibUrl = moduleDataUrl(`
export function d1DatabasesKey(ns) { return "d1:databases:" + ns; }
`);

const modelUrl = moduleDataUrl(`
export const EXECUTE_MODES = new Set(["all", "raw", "run", "exec", "batch"]);
export const MIGRATIONS_TABLE_SQL = "create table if not exists _wdl_d1_migrations (id text)";
export function splitSqlStatements(sql) { return [{ sql, params: [] }]; }
export function validateDatabaseName() {}
export function isReadyDatabase(database) { return database?.state === "ready"; }
`);

const productionBackendUrl = controlD1RuntimeClientDataUrl();
const backendUrl = moduleDataUrl(`
export { d1RuntimeFailure, d1RuntimeFailureLogFields } from ${JSON.stringify(productionBackendUrl)};
export async function d1RuntimeQuery() {
  /** @type {any} */ (globalThis).__d1RuntimeQueryArgs = arguments;
  return /** @type {any} */ (globalThis).__d1RuntimeResult || {
    ok: false,
    status: 503,
    body: {
      error: "backend-unavailable",
      message: "backend unavailable",
      category: "internal",
      retryable: true,
    },
  };
}
export async function d1RuntimeReleaseOwner(env, ns, databaseId, owner, requestId) {
  /** @type {any} */ (globalThis).__d1ReleasedOwnerArgs = { env, ns, databaseId, owner, requestId };
  return /** @type {any} */ (globalThis).__d1ReleaseResult || { ok: true, status: 200, body: {} };
}
export async function d1RuntimeProbeOwner(env, ns, databaseId, requestId) {
  /** @type {any} */ (globalThis).__d1ProbeOwnerArgs = { env, ns, databaseId, requestId };
  return /** @type {any} */ (globalThis).__d1ProbeResult || { ok: true, status: 200, body: { owner: null }, owner: null };
}
export function d1RuntimePublicResult(value) {
  /** @type {any} */ (globalThis).__d1RuntimePublicResultArgs = arguments;
  return value;
}
`);

const storeUrl = moduleDataUrl(`
export async function commitDatabaseMetadata() {
  /** @type {any} */ (globalThis).__d1LifecycleCommitted = true;
  return /** @type {any} */ (globalThis).__d1CommitResult || { ok: true, databaseId: "d1_test" };
}
export function createDatabaseId() {
  return "d1_test";
}
export async function deleteDatabaseMetadata() {
  if (/** @type {any} */ (globalThis).__d1DeleteThrows) throw /** @type {any} */ (globalThis).__d1DeleteThrows;
  return /** @type {any} */ (globalThis).__d1DeleteResult || { deleted: true };
}
export async function getDatabase() {
  throw new Error("not used");
}
export async function getDatabases(ns, ids) {
  /** @type {any} */ (globalThis).__d1GetDatabasesArgs = { ns, ids };
  return /** @type {any} */ (globalThis).__d1GetDatabasesResult || [];
}
export async function getDatabaseIdByName() {
  return null;
}
export function isExpiredProvisional(database) {
  return database?.state === "provisional" && database.expired === true;
}
export async function markDatabaseReady() {
  /** @type {any} */ (globalThis).__d1LifecycleMarkedReady = true;
  return /** @type {any} */ (globalThis).__d1ReadyResult || { ok: true };
}
export async function resolveDatabaseRef() {
  return /** @type {any} */ (globalThis).__d1RequireResult || {
    database: { databaseId: "d1_test", databaseName: "main", state: "ready", createdAt: "now", updatedAt: "now" },
  };
}
export async function rollbackExpiredProvisionalDatabaseMetadata() {
  /** @type {any} */ (globalThis).__d1ExpiredProvisionalRolledBack = true;
  return /** @type {any} */ (globalThis).__d1ExpiredRollbackResult || { rolledBack: true };
}
export async function rollbackProvisionalDatabaseMetadata() {
  /** @type {any} */ (globalThis).__d1ProvisionalRolledBack = true;
  return /** @type {any} */ (globalThis).__d1RollbackResult || { rolledBack: true };
}
export async function updateDatabaseTombstoneOwnerRelease(ns, databaseId, status, errorMessage, now) {
  if (/** @type {any} */ (globalThis).__d1TombstoneOwnerReleaseThrows) throw /** @type {any} */ (globalThis).__d1TombstoneOwnerReleaseThrows;
  /** @type {any} */ (globalThis).__d1TombstoneOwnerRelease = { ns, databaseId, status, errorMessage, now };
}
`);

const src = applyModuleReplacements(readRepositoryFile("control/d1-lifecycle.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(controlLibUrl)};`],
  [/from "control-d1-model";/, `from ${JSON.stringify(modelUrl)};`],
  [/from "control-d1-runtime-client";/, `from ${JSON.stringify(backendUrl)};`],
  [/from "control-d1-store";/, `from ${JSON.stringify(storeUrl)};`],
]);

const { createDatabase, deleteDatabase, executeDatabase, listDatabases } = await import(moduleDataUrl(src));

test("listDatabases batches metadata reads and returns ready databases", async () => {
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];
  /** @type {any} */ (globalThis).__d1GetDatabasesArgs = null;
  /** @type {any} */ (globalThis).__d1GetDatabasesResult = [
    { databaseId: "d1_b", databaseName: "beta", state: "ready", createdAt: "c2", updatedAt: "u2" },
    { databaseId: "d1_a", databaseName: "alpha", state: "ready", createdAt: "c1", updatedAt: "u1" },
    { databaseId: "d1_pending", databaseName: "pending", state: "provisional" },
    null,
  ];
  /** @type {any} */ (globalThis).__d1LifecycleRedis = {
    /** @param {string} key */
    async sMembers(key) {
      assert.equal(key, "d1:databases:demo");
      return ["d1_pending", "d1_b", "d1_a", "d1_missing"];
    },
  };

  try {
    const response = await listDatabases({
      ns: "demo",
      requestId: "rid-list",
    });

    assert.equal(response.status, 200);
    assert.deepEqual(/** @type {any} */ (globalThis).__d1GetDatabasesArgs, {
      ns: "demo",
      ids: ["d1_a", "d1_b", "d1_missing", "d1_pending"],
    });
    await assertJsonResponse(response, 200, {
      namespace: "demo",
      databases: [
        { databaseId: "d1_b", databaseName: "beta", createdAt: "c2", updatedAt: "u2" },
        { databaseId: "d1_a", databaseName: "alpha", createdAt: "c1", updatedAt: "u1" },
      ],
    });
  } finally {
    /** @type {any} */ (globalThis).__d1LifecycleRedis = null;
    /** @type {any} */ (globalThis).__d1GetDatabasesResult = null;
  }
});

test("createDatabase: backend initialization failure rolls back provisional metadata", async () => {
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];
  /** @type {any} */ (globalThis).__d1RuntimeResult = {
    ok: false,
    status: 400,
    body: {
      error: "sql-error",
      message: "private initialization SQL",
      category: "sql",
      retryable: false,
    },
  };
  /** @type {any} */ (globalThis).__d1CommitResult = null;
  /** @type {any} */ (globalThis).__d1ReleasedOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1LifecycleCommitted = false;
  /** @type {any} */ (globalThis).__d1ProvisionalRolledBack = false;
  const response = await createDatabase({
    request: new Request("http://control/ns/demo/d1/databases", {
      method: "POST",
      body: JSON.stringify({ databaseName: "main" }),
    }),
    env: {},
    ns: "demo",
    requestId: "rid-create-fail",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "d1_database_initialize_failed");
  assert.equal(body.message, "Internal error");
  assert.equal(body.databaseId, "d1_test");
  assert.equal(body.upstreamCode, "sql-error");
  assert.equal(body.upstreamCategory, "sql");
  assert.equal(body.upstreamRetryable, false);
  assert.equal(body.upstreamStatus, 400);
  assert.equal(body.detail, undefined);
  assert.equal(/** @type {any} */ (globalThis).__d1LifecycleCommitted, true);
  assert.equal(/** @type {any} */ (globalThis).__d1ProvisionalRolledBack, true);
  assert.equal(/** @type {any} */ (globalThis).__d1ReleasedOwnerArgs, null);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1LifecycleLogs.at(-1), {
    level: "error",
    event: "d1_database_initialize_failed",
    fields: {
      request_id: "rid-create-fail",
      namespace: "demo",
      database_id: "d1_test",
      status: 503,
      reason: "d1_database_initialize_failed",
      upstream_status: 400,
      upstream_code: "sql-error",
      upstream_category: "sql",
      upstream_retryable: false,
    },
  });
});

test("executeDatabase redacts D1 runtime 5xx diagnostics and logs stable fields", async () => {
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", state: "ready", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1RuntimeResult = {
    ok: false,
    status: 503,
    body: {
      error: "owner-record-invalid",
      message: "private owner diagnostic",
      category: "ownership",
      retryable: true,
    },
  };

  const response = await executeDatabase({
    request: new Request("http://control/ns/demo/d1/databases/main/query", {
      method: "POST",
      body: JSON.stringify({ sql: "select 1" }),
    }),
    env: {},
    ns: "demo",
    databaseId: "main",
    requestId: "rid-execute-fail",
  });

  await assertJsonResponse(response, 503, {
    error: "d1_execute_failed",
    namespace: "demo",
    databaseId: "d1_test",
    message: "Internal error",
    upstreamCode: "owner-record-invalid",
    upstreamCategory: "ownership",
    upstreamRetryable: true,
    upstreamStatus: 503,
  });
  assert.deepEqual(/** @type {any} */ (globalThis).__d1LifecycleLogs.at(-1), {
    level: "error",
    event: "d1_database_execute_failed",
    fields: {
      request_id: "rid-execute-fail",
      namespace: "demo",
      database_id: "d1_test",
      status: 503,
      reason: "d1_execute_failed",
      upstream_status: 503,
      upstream_code: "owner-record-invalid",
      upstream_category: "ownership",
      upstream_retryable: true,
    },
  });
});

test("createDatabase: ready flip failure releases initialized owner and rolls back provisional metadata", async () => {
  /** @type {any} */ (globalThis).__d1RuntimeResult = {
    ok: true,
    status: 200,
    body: { count: 1, duration: 1 },
    owner: { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 3 },
  };
  /** @type {any} */ (globalThis).__d1CommitResult = { ok: true, databaseId: "d1_test" };
  /** @type {any} */ (globalThis).__d1ReadyResult = { ok: false, reason: "contention" };
  /** @type {any} */ (globalThis).__d1ReleaseResult = { ok: true, status: 200, body: {} };
  /** @type {any} */ (globalThis).__d1ReleasedOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1LifecycleCommitted = false;
  /** @type {any} */ (globalThis).__d1LifecycleMarkedReady = false;
  /** @type {any} */ (globalThis).__d1ProvisionalRolledBack = false;

  const response = await createDatabase({
    request: new Request("http://control/ns/demo/d1/databases", {
      method: "POST",
      body: JSON.stringify({ databaseName: "main" }),
    }),
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    requestId: "rid-create-race",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "d1_database_create_contention");
  assert.equal(/** @type {any} */ (globalThis).__d1LifecycleCommitted, true);
  assert.equal(/** @type {any} */ (globalThis).__d1LifecycleMarkedReady, true);
  assert.equal(/** @type {any} */ (globalThis).__d1ProvisionalRolledBack, true);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1ReleasedOwnerArgs, {
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "d1_test",
    owner: { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 3 },
    requestId: "rid-create-race",
  });
  assert.ok(/** @type {any} */ (globalThis).__d1LifecycleLogs.some((/** @type {any} */ entry) =>
    entry.level === "info" &&
    entry.event === "d1_database_orphan_owner_released" &&
    entry.fields.owner_task_id === "task-a"
  ));
  /** @type {any} */ (globalThis).__d1ReadyResult = null;
});

test("deleteDatabase: successful metadata delete releases current owner best-effort", async () => {
  const owner = { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 4 };
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1DeleteResult = { deleted: true };
  /** @type {any} */ (globalThis).__d1ProbeResult = { ok: true, status: 200, body: { owner }, owner };
  /** @type {any} */ (globalThis).__d1ReleaseResult = { ok: true, status: 200, body: {} };
  /** @type {any} */ (globalThis).__d1ProbeOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1ReleasedOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1TombstoneOwnerRelease = null;

  const response = await deleteDatabase({
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "main",
    requestId: "rid-delete",
  });

  assert.equal(response.status, 200);
  assert.deepEqual(/** @type {any} */ (globalThis).__d1ProbeOwnerArgs, {
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "d1_test",
    requestId: "rid-delete",
  });
  assert.deepEqual(/** @type {any} */ (globalThis).__d1ReleasedOwnerArgs, {
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "d1_test",
    owner,
    requestId: "rid-delete",
  });
  assert.ok(/** @type {any} */ (globalThis).__d1LifecycleLogs.some((/** @type {any} */ entry) =>
    entry.level === "info" &&
    entry.event === "d1_database_deleted_owner_released" &&
    entry.fields.owner_task_id === owner.taskId
  ));
  assert.equal(/** @type {any} */ (globalThis).__d1TombstoneOwnerRelease.status, "released");
});

test("deleteDatabase: no current owner is logged without release", async () => {
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1DeleteResult = { deleted: true };
  /** @type {any} */ (globalThis).__d1ProbeResult = { ok: true, status: 200, body: { owner: null }, owner: null };
  /** @type {any} */ (globalThis).__d1ReleaseResult = { ok: true, status: 200, body: {} };
  /** @type {any} */ (globalThis).__d1ProbeOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1ReleasedOwnerArgs = null;
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];
  /** @type {any} */ (globalThis).__d1TombstoneOwnerRelease = null;

  const response = await deleteDatabase({
    env: {},
    ns: "demo",
    databaseId: "main",
    requestId: "rid-delete-no-owner",
  });

  assert.equal(response.status, 200);
  assert.equal(/** @type {any} */ (globalThis).__d1ReleasedOwnerArgs, null);
  assert.ok(/** @type {any} */ (globalThis).__d1LifecycleLogs.some((/** @type {any} */ entry) =>
    entry.level === "info" &&
    entry.event === "d1_database_deleted_owner_not_found" &&
    entry.fields.database_id === "d1_test"
  ));
  assert.equal(/** @type {any} */ (globalThis).__d1TombstoneOwnerRelease.status, "not_found");
});

test("deleteDatabase: owner release failure is observable but delete still succeeds", async () => {
  const owner = { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 4 };
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1DeleteResult = { deleted: true };
  /** @type {any} */ (globalThis).__d1ProbeResult = { ok: true, status: 200, body: { owner }, owner };
  /** @type {any} */ (globalThis).__d1ReleaseResult = {
    ok: false,
    status: 503,
    body: { error: "owner-release-raced", message: "release raced" },
  };
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];
  /** @type {any} */ (globalThis).__d1TombstoneOwnerRelease = null;

  const response = await deleteDatabase({
    env: {},
    ns: "demo",
    databaseId: "main",
    requestId: "rid-delete-release-fail",
  });

  assert.equal(response.status, 200);
  assert.ok(/** @type {any} */ (globalThis).__d1LifecycleLogs.some((/** @type {any} */ entry) =>
    entry.level === "warn" &&
    entry.event === "d1_database_deleted_owner_release_failed" &&
    entry.fields.error_code === "owner-release-raced"
  ));
  assert.equal(/** @type {any} */ (globalThis).__d1TombstoneOwnerRelease.status, "failed");
  assert.equal(/** @type {any} */ (globalThis).__d1TombstoneOwnerRelease.errorMessage, "release raced");
});

test("deleteDatabase: tombstone owner-state update failure does not flip delete to 500", async () => {
  const owner = { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 4 };
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1DeleteResult = { deleted: true };
  /** @type {any} */ (globalThis).__d1ProbeResult = { ok: true, status: 200, body: { owner }, owner };
  /** @type {any} */ (globalThis).__d1ReleaseResult = { ok: true, status: 200, body: {} };
  /** @type {any} */ (globalThis).__d1TombstoneOwnerReleaseThrows = new Error("redis temporarily unavailable");
  /** @type {any} */ (globalThis).__d1LifecycleLogs = [];

  try {
    const response = await deleteDatabase({
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-delete-tombstone-update-fail",
    });

    assert.equal(response.status, 200);
    assert.ok(/** @type {any} */ (globalThis).__d1LifecycleLogs.some((/** @type {any} */ entry) =>
      entry.level === "warn" &&
      entry.event === "d1_database_deleted_tombstone_update_failed" &&
      entry.fields.owner_release_status === "released" &&
      entry.fields.error_message === "redis temporarily unavailable"
    ));
  } finally {
    /** @type {any} */ (globalThis).__d1TombstoneOwnerReleaseThrows = null;
  }
});

test("deleteDatabase: metadata delete contention maps to stable 503", async () => {
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1DeleteThrows = Object.assign(new Error("deleteDatabaseMetadata: contention"), {
    code: "d1_database_delete_contention",
    namespace: "demo",
    databaseId: "d1_test",
  });

  try {
    const response = await deleteDatabase({
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-delete-contention",
    });
    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "d1_database_delete_contention");
    assert.equal(body.databaseId, "d1_test");
    assert.equal(body.databaseName, "main");
  } finally {
    /** @type {any} */ (globalThis).__d1DeleteThrows = null;
  }
});

test("executeDatabase: raw mode preserves row-column result shape", async () => {
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", state: "ready", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1RuntimeResult = {
    ok: true,
    status: 200,
    body: {
      success: true,
      results: { columns: ["id", "id"], rows: [["m1", "shadow"]] },
      meta: { rows_read: 1 },
    },
  };
  /** @type {any} */ (globalThis).__d1RuntimeQueryArgs = null;
  /** @type {any} */ (globalThis).__d1RuntimePublicResultArgs = null;

  const response = await executeDatabase({
    request: new Request("http://control/ns/demo/d1/databases/main/query", {
      method: "POST",
      body: JSON.stringify({ sql: "select 1 as id", mode: "raw" }),
    }),
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "main",
    requestId: "rid-raw",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.mode, "raw");
  assert.deepEqual(body.result.results, {
    columns: ["id", "id"],
    rows: [["m1", "shadow"]],
  });
  assert.equal(/** @type {any} */ (globalThis).__d1RuntimeQueryArgs[3], "raw");
  assert.equal(/** @type {any} */ (globalThis).__d1RuntimePublicResultArgs[1], "raw");
});

test("executeDatabase: exec mode rejects params instead of ignoring them", async () => {
  /** @type {any} */ (globalThis).__d1RequireResult = {
    database: { databaseId: "d1_test", databaseName: "main", state: "ready", createdAt: "now", updatedAt: "now" },
  };
  /** @type {any} */ (globalThis).__d1RuntimeQueryArgs = null;

  const response = await executeDatabase({
    request: new Request("http://control/ns/demo/d1/databases/main/query", {
      method: "POST",
      body: JSON.stringify({ sql: "select ?", mode: "exec", params: [1] }),
    }),
    env: { D1_QUERY_TIMEOUT_MS: "1000" },
    ns: "demo",
    databaseId: "main",
    requestId: "rid-exec-params",
  });

  const body = await readJsonResponse(response, 400);
  assert.equal(body.error, "invalid_request");
  assert.match(body.message, /exec mode does not accept params/);
  assert.equal(/** @type {any} */ (globalThis).__d1RuntimeQueryArgs, null);
});
