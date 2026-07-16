import { afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { controlD1RuntimeClientDataUrl } from "../helpers/load-d1-protocol.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { readJsonResponse } from "../helpers/response-json.js";

/** @type {any} */
const D1_MIGRATIONS_TEST_STATE = {
  redis: null,
  session: null,
  runtimeCalls: null,
  runtimeResult: null,
  lockToken: null,
  logs: [],
  splitStatements: null,
};
/** @type {typeof globalThis & { __d1MigrationTestState?: typeof D1_MIGRATIONS_TEST_STATE }} */
const d1MigrationGlobal = globalThis;
d1MigrationGlobal.__d1MigrationTestState = D1_MIGRATIONS_TEST_STATE;

const sharedRandomIdUrl = repositoryFileUrl("shared/random-id.js");
const sharedRedisLockUrl = moduleDataUrl(applyModuleReplacements(
  readRepositoryFile("shared/redis-lock.js"),
  [[/from "shared-random-id"/g, `from ${JSON.stringify(sharedRandomIdUrl)}`]]
));

const controlSharedUrl = controlSharedStubUrl(`
export const state = {
  redis: {
    async set(...args) {
      if (/** @type {any} */ (globalThis).__d1MigrationTestState.redis?.set) return /** @type {any} */ (globalThis).__d1MigrationTestState.redis.set(...args);
      return null;
    },
    async get(...args) {
      if (/** @type {any} */ (globalThis).__d1MigrationTestState.redis?.get) return /** @type {any} */ (globalThis).__d1MigrationTestState.redis.get(...args);
      return null;
    },
    async del() { return 1; },
    async delIfEq(...args) {
      if (/** @type {any} */ (globalThis).__d1MigrationTestState.redis?.delIfEq) return /** @type {any} */ (globalThis).__d1MigrationTestState.redis.delIfEq(...args);
      return 1;
    },
    async session(fn) {
      return await fn(globalThis.__d1MigrationTestState.session);
    },
  },
  log(level, event, fields) {
    /** @type {any} */ (globalThis).__d1MigrationTestState.logs.push({ level, event, fields });
  },
};
`);

const modelUrl = moduleDataUrl(`
export const MIGRATIONS_TABLE_SQL = "create table if not exists _wdl_d1_migrations (id text)";
export function migrationStatus() { return []; }
export function normalizeMigrationApply(migration) { return migration; }
export function normalizeMigrationRef(migration) { return migration; }
export async function sha256Hex(input) { return "sha256:" + input.length; }
export function splitSqlStatements(sql) {
  const split = /** @type {any} */ (globalThis).__d1MigrationTestState.splitStatements;
  if (split) return split(sql);
  return [];
}
`);

const productionBackendUrl = controlD1RuntimeClientDataUrl();
const backendUrl = moduleDataUrl(`
export { d1RuntimeFailure, d1RuntimeFailureLogFields } from ${JSON.stringify(productionBackendUrl)};
export async function d1RuntimeQuery() {
  const args = Array.from(arguments);
  if (!Array.isArray(/** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls)) {
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls = [];
  }
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls.push({
    mode: args[3],
    statements: args[4],
  });
  if (typeof /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult === "function") {
    return /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult(...args);
  }
  return /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult || {
    ok: true,
    status: 200,
    body: { success: true, results: [] },
  };
}
export function d1RuntimePublicResult(value) {
  return value;
}
`);

const storeUrl = moduleDataUrl(`
export async function resolveDatabaseRef() {
  return { database: { databaseId: "d1_main", databaseName: "main" } };
}
`);

const src = applyModuleReplacements(readRepositoryFile("control/d1-migrations.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-d1-model";/, `from ${JSON.stringify(modelUrl)};`],
  [/from "control-d1-runtime-client";/, `from ${JSON.stringify(backendUrl)};`],
  [/from "control-d1-store";/, `from ${JSON.stringify(storeUrl)};`],
  [/from "shared-redis-lock";/, `from ${JSON.stringify(sharedRedisLockUrl)};`],
]);

const {
  applyMigrations,
  listMigrations,
  migrationStatusEndpoint,
  renewMigrationLock,
} = await import(moduleDataUrl(src));

afterEach(() => {
  D1_MIGRATIONS_TEST_STATE.redis = null;
  D1_MIGRATIONS_TEST_STATE.session = null;
  D1_MIGRATIONS_TEST_STATE.runtimeCalls = null;
  D1_MIGRATIONS_TEST_STATE.runtimeResult = null;
  D1_MIGRATIONS_TEST_STATE.lockToken = null;
  D1_MIGRATIONS_TEST_STATE.logs = [];
  D1_MIGRATIONS_TEST_STATE.splitStatements = null;
});

test("applyMigrations returns 409 when the per-database migration lock is already held", async () => {
  const response = await applyMigrations({
    request: new Request("http://control/ns/demo/d1/databases/main/migrations/apply", {
      method: "POST",
      body: JSON.stringify({
        migrations: [{ id: "0001_init.sql", name: "init", sql: "create table demo (id text)" }],
      }),
    }),
    env: {},
    ns: "demo",
    databaseId: "main",
    requestId: "rid-migration-lock",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "d1_migrations_apply_in_progress");
  assert.equal(body.databaseId, "d1_main");
  assert.equal(body.databaseName, "main");
});

test("migration read endpoints do not create the migrations table", async () => {
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls = [];
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = {
    ok: true,
    status: 200,
    body: { success: true, results: [] },
  };

  try {
    const status = await migrationStatusEndpoint({
      request: new Request("http://control/ns/demo/d1/databases/main/migrations/status", {
        method: "POST",
        body: JSON.stringify({ migrations: [{ id: "0001_init.sql", checksum: "abc" }] }),
      }),
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-migration-status",
    });
    assert.equal(status.status, 200);

    const list = await listMigrations({
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-migration-list",
    });
    assert.equal(list.status, 200);

    const calls = /** @type {Array<{ statements?: Array<{ sql: string }> }>} */ (
      /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls
    );
    const sql = calls
      .flatMap((call) => call.statements || [])
      .map((statement) => statement.sql);
    assert.equal(sql.some((statement) => /create\s+table/i.test(statement)), false);
    assert.equal(sql.every((statement) => /sqlite_master/i.test(statement)), true);
  } finally {
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeCalls = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = null;
  }
});

test("renewMigrationLock refreshes TTL when token still matches", async () => {
  /** @type {unknown[][]} */
  const commands = [];
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = {
    /** @param {unknown[]} args */
    async set(...args) {
      commands.push(["SET", ...args]);
      return "OK";
    },
  };

  const renewed = await renewMigrationLock({ key: "lock:key", token: "token-a" });

  assert.deepEqual(renewed, { ok: true });
  assert.deepEqual(commands, [
    ["SET", "lock:key", "token-a", { ttl: 600, ifeq: "token-a" }],
  ]);
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = null;
});

test("renewMigrationLock reports lost when token no longer matches", async () => {
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = {
    async set() { return null; },
  };

  const renewed = await renewMigrationLock({ key: "lock:key", token: "token-a" });

  assert.deepEqual(renewed, { ok: false, reason: "lost" });
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = null;
});

test("applyMigrations maps lock renewal loss to 409", async () => {
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = {
    /** @param {string} _key @param {string} token */
    async set(_key, token) {
      if (!/** @type {any} */ (globalThis).__d1MigrationTestState.lockToken) {
        /** @type {any} */ (globalThis).__d1MigrationTestState.lockToken = token;
        return "OK";
      }
      return null;
    },
  };
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = {
    ok: true,
    status: 200,
    body: { success: true, results: [] },
  };

  try {
    const response = await applyMigrations({
      request: new Request("http://control/ns/demo/d1/databases/main/migrations/apply", {
        method: "POST",
        body: JSON.stringify({
          migrations: [{ id: "0001_init.sql", name: "init", sql: "create table demo (id text)" }],
        }),
      }),
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-migration-contention",
    });

    const body = await readJsonResponse(response, 409);
    assert.equal(body.error, "d1_migrations_apply_lock_lost");
    assert.deepEqual(body.applied, []);
    assert.deepEqual(body.skipped, []);
  } finally {
    /** @type {any} */ (globalThis).__d1MigrationTestState.redis = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.lockToken = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = null;
  }
});

test("applyMigrations includes empty progress when reading existing migrations fails", async () => {
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = {
    async set() { return "OK"; },
    async delIfEq() { throw new Error("migration lock release unavailable"); },
  };
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = {
    ok: false,
    status: 503,
    body: { error: "d1-runtime-unavailable", message: "D1 unavailable" },
  };

  try {
    const response = await applyMigrations({
      request: new Request("http://control/ns/demo/d1/databases/main/migrations/apply", {
        method: "POST",
        body: JSON.stringify({
          migrations: [{ id: "0001_init.sql", name: "init", sql: "create table demo (id text)" }],
        }),
      }),
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-migration-read-failure",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "d1_migrations_apply_failed");
    assert.equal(body.message, "Internal error");
    assert.equal(body.upstreamCode, "d1-runtime-unavailable");
    assert.equal(body.upstreamCategory, "internal");
    assert.equal(body.upstreamRetryable, false);
    assert.deepEqual(body.applied, []);
    assert.deepEqual(body.skipped, []);
    assert.deepEqual(/** @type {any} */ (globalThis).__d1MigrationTestState.logs, [
      {
        level: "error",
        event: "d1_migrations_apply_failed",
        fields: {
          request_id: "rid-migration-read-failure",
          namespace: "demo",
          database_id: "d1_main",
          status: 503,
          reason: "d1_migrations_apply_failed",
          applied_count: 0,
          skipped_count: 0,
          upstream_status: 503,
          upstream_code: "d1-runtime-unavailable",
          upstream_category: "internal",
          upstream_retryable: false,
        },
      },
      {
        level: "warn",
        event: "d1_migration_lock_release_failed",
        fields: {
          request_id: "rid-migration-read-failure",
          namespace: "demo",
          database_id: "d1_main",
          error_name: "Error",
          error_message: "migration lock release unavailable",
        },
      },
    ]);
  } finally {
    /** @type {any} */ (globalThis).__d1MigrationTestState.redis = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = null;
  }
});

test("applyMigrations includes completed progress when a later migration fails", async () => {
  let setCalls = 0;
  /** @type {any} */ (globalThis).__d1MigrationTestState.redis = {
    /** @param {string} _key @param {string} token */
    async set(_key, token) {
      setCalls += 1;
      if (setCalls === 1) {
        /** @type {any} */ (globalThis).__d1MigrationTestState.lockToken = token;
        return "OK";
      }
      return "OK";
    },
  };
  /** @type {any} */ (globalThis).__d1MigrationTestState.splitStatements = (/** @type {string} */ sql) => [
    { sql, params: [] },
  ];
  /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = (/** @type {any[]} */ ...args) => {
    const statements = /** @type {Array<{ sql?: string }>} */ (args[4] || []);
    if (statements.some((statement) => statement.sql?.startsWith("alter table demo"))) {
      return {
        ok: false,
        status: 503,
        body: {
          error: "result-unknown",
          message: "private commit diagnostic",
          category: "result-unknown",
          retryable: false,
        },
      };
    }
    return {
      ok: true,
      status: 200,
      body: { success: true, results: [] },
    };
  };

  try {
    const response = await applyMigrations({
      request: new Request("http://control/ns/demo/d1/databases/main/migrations/apply", {
        method: "POST",
        body: JSON.stringify({
          migrations: [
            { id: "0001_init.sql", name: "init", sql: "create table demo (id text)" },
            { id: "0002_next.sql", name: "next", sql: "alter table demo add column body text" },
          ],
        }),
      }),
      env: {},
      ns: "demo",
      databaseId: "main",
      requestId: "rid-migration-partial",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "d1_migration_apply_failed");
    assert.equal(body.message, "Internal error");
    assert.equal(body.migrationId, "0002_next.sql");
    assert.equal(body.upstreamCode, "result-unknown");
    assert.equal(body.upstreamCategory, "result-unknown");
    assert.equal(body.upstreamRetryable, false);
    assert.deepEqual(
      body.applied.map((/** @type {{ id: string }} */ migration) => migration.id),
      ["0001_init.sql"]
    );
    assert.deepEqual(body.skipped, []);
    assert.deepEqual(/** @type {any} */ (globalThis).__d1MigrationTestState.logs.at(-1), {
      level: "error",
      event: "d1_migration_apply_failed",
      fields: {
        request_id: "rid-migration-partial",
        namespace: "demo",
        database_id: "d1_main",
        status: 503,
        reason: "d1_migration_apply_failed",
        migration_id: "0002_next.sql",
        applied_count: 1,
        skipped_count: 0,
        upstream_status: 503,
        upstream_code: "result-unknown",
        upstream_category: "result-unknown",
        upstream_retryable: false,
      },
    });
  } finally {
    /** @type {any} */ (globalThis).__d1MigrationTestState.redis = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.lockToken = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.runtimeResult = null;
    /** @type {any} */ (globalThis).__d1MigrationTestState.splitStatements = null;
  }
});
