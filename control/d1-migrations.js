import {
  jsonResponse,
  jsonError,
  readJsonBody,
  errMessage,
  formatError,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import {
  acquireTokenLock,
  createTokenLock,
  releaseTokenLock,
  renewTokenLock,
} from "shared-redis-lock";
import {
  migrationStatus,
  MIGRATIONS_TABLE_SQL,
  normalizeMigrationApply,
  normalizeMigrationRef,
  sha256Hex,
  splitSqlStatements,
} from "control-d1-model";
import {
  d1RuntimeFailure,
  d1RuntimeFailureLogFields,
  d1RuntimePublicResult,
  d1RuntimeQuery,
} from "control-d1-runtime-client";
import {
  resolveDatabaseRef,
} from "control-d1-store";

/**
 * @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} Fetcher
 * @typedef {{ D1_BACKEND: Fetcher, D1_QUERY_TIMEOUT_MS?: unknown, [key: string]: unknown }} D1RuntimeEnv
 * @typedef {{ key: string, token: string }} MigrationLock
 * @typedef {{ request: Request, env: D1RuntimeEnv, ns: string, databaseId: string, requestId: string }} D1MigrationRequestArgs
 * @typedef {{ env: D1RuntimeEnv, ns: string, databaseId: string, requestId: string }} D1MigrationRouteArgs
 * @typedef {{ id: string, name: string, sql: string }} MigrationApply
 * @typedef {{ id: string, name?: string, checksum: string, appliedAt?: string }} AppliedMigration
 */

/**
 * @param {string} ns
 * @param {string} databaseId
 */
function migrationLockKey(ns, databaseId) {
  return `d1:migrations-lock:${ns}:${databaseId}`;
}

const MIGRATION_LOCK_TTL_SECONDS = 600;
const MIGRATIONS_TABLE_NAME = "_wdl_d1_migrations";

/**
 * @param {string} event
 * @param {string} reason
 * @param {string} requestId
 * @param {string} ns
 * @param {string} databaseId
 * @param {{ body?: unknown, status: number }} result
 * @param {Record<string, unknown>} [extra]
 */
function logD1MigrationFailure(event, reason, requestId, ns, databaseId, result, extra = {}) {
  requireControlLog()(result.status >= 500 ? "error" : "warn", event, {
    request_id: requestId,
    namespace: ns,
    database_id: databaseId,
    status: result.status,
    reason,
    ...extra,
    ...d1RuntimeFailureLogFields(result),
  });
}

/**
 * @param {unknown[]} applied
 * @param {unknown[]} skipped
 */
function migrationProgressLogFields(applied, skipped) {
  return {
    applied_count: applied.length,
    skipped_count: skipped.length,
  };
}

/**
 * @param {unknown[]} applied
 * @param {unknown[]} skipped
 */
function migrationProgress(applied, skipped) {
  return { applied, skipped };
}

/**
 * @param {string} ns
 * @param {string} databaseId
 */
async function acquireMigrationLock(ns, databaseId) {
  const redis = requireControlRedis();
  const key = migrationLockKey(ns, databaseId);
  const lock = createTokenLock(key);
  return await acquireTokenLock(redis, lock, { ttlSeconds: MIGRATION_LOCK_TTL_SECONDS })
    ? lock
    : null;
}

/** @param {MigrationLock | null} lock */
export async function renewMigrationLock(lock) {
  if (!lock) return { ok: false, reason: "lost" };
  const redis = requireControlRedis();
  const renewed = await renewTokenLock(redis, lock, MIGRATION_LOCK_TTL_SECONDS);
  return renewed ? { ok: true } : { ok: false, reason: "lost" };
}

/**
 * @param {MigrationLock | null} lock
 * @param {{ log: import("control-shared").ControlLogger, ns: string, databaseId: string, requestId: string }} context
 */
async function releaseMigrationLock(lock, { log, ns, databaseId, requestId }) {
  if (!lock) return;
  const redis = requireControlRedis();
  await releaseTokenLock(redis, lock, {
    onError: (err) => log("warn", "d1_migration_lock_release_failed", {
      request_id: requestId,
      namespace: ns,
      database_id: databaseId,
      ...formatError(err),
    }),
  });
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string | undefined} requestId
 */
async function ensureMigrationsTable(env, ns, databaseId, requestId) {
  return await d1RuntimeQuery(env, ns, databaseId, "exec", [
    { sql: MIGRATIONS_TABLE_SQL, params: [] },
  ], requestId ?? null);
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} [requestId]
 * @returns {Promise<{ ok: true, migrations: AppliedMigration[] } | { ok: false, status: number, body: unknown }>}
 */
export async function readAppliedMigrations(env, ns, databaseId, requestId = undefined) {
  const created = await ensureMigrationsTable(env, ns, databaseId, requestId);
  if (!created.ok) return { ok: false, status: created.status, body: created.body };
  return await readAppliedMigrationsWithoutCreate(env, ns, databaseId, requestId);
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} [requestId]
 * @returns {Promise<{ ok: true, migrations: AppliedMigration[] } | { ok: false, status: number, body: unknown }>}
 */
async function readAppliedMigrationsIfTableExists(env, ns, databaseId, requestId = undefined) {
  const exists = await d1RuntimeQuery(env, ns, databaseId, "all", [
    {
      sql: "select 1 as found from sqlite_master where type = ? and name = ? limit 1",
      params: ["table", MIGRATIONS_TABLE_NAME],
    },
  ], requestId ?? null);
  if (!exists.ok) return { ok: false, status: exists.status, body: exists.body };
  const publicExists = /** @type {{ results?: unknown[] } | null | undefined} */ (
    d1RuntimePublicResult(exists.body)
  );
  if (!Array.isArray(publicExists?.results) || publicExists.results.length === 0) {
    return { ok: true, migrations: [] };
  }
  return await readAppliedMigrationsWithoutCreate(env, ns, databaseId, requestId);
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} [requestId]
 * @returns {Promise<{ ok: true, migrations: AppliedMigration[] } | { ok: false, status: number, body: unknown }>}
 */
async function readAppliedMigrationsWithoutCreate(env, ns, databaseId, requestId = undefined) {
  const selected = await d1RuntimeQuery(env, ns, databaseId, "all", [
    {
      sql: "select id, name, checksum, applied_at as appliedAt from _wdl_d1_migrations order by applied_at, id",
      params: [],
    },
  ], requestId ?? null);
  if (!selected.ok) return { ok: false, status: selected.status, body: selected.body };
  const publicResult = /** @type {{ results?: AppliedMigration[] } | null | undefined} */ (
    d1RuntimePublicResult(selected.body)
  );
  return { ok: true, migrations: publicResult?.results || [] };
}

/** @param {D1MigrationRouteArgs} args */
export async function listMigrations({ env, ns, databaseId, requestId }) {
  const checked = await resolveDatabaseRef(ns, databaseId);
  if (checked.response) return checked.response;
  const database = checked.database;
  const applied = await readAppliedMigrationsIfTableExists(env, ns, database.databaseId, requestId);
  if (applied.ok === false) {
    logD1MigrationFailure(
      "d1_migrations_list_failed", "d1_migrations_list_failed",
      requestId, ns, database.databaseId, applied
    );
    return jsonResponse(applied.status, d1RuntimeFailure("d1_migrations_list_failed", ns, database.databaseId, applied));
  }
  return jsonResponse(200, {
    namespace: ns,
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    migrations: applied.migrations,
  });
}

/** @param {D1MigrationRequestArgs} args */
export async function migrationStatusEndpoint({ request, env, ns, databaseId, requestId }) {
  const checked = await resolveDatabaseRef(ns, databaseId);
  if (checked.response) return checked.response;
  const database = checked.database;
  const parsed = await readJsonBody(request, { requireObject: true });
  if (parsed.response) return parsed.response;
  let localMigrations;
  try {
    const body = /** @type {{ migrations?: unknown[] }} */ (parsed.body);
    localMigrations = (body.migrations || []).map(normalizeMigrationRef);
  } catch (err) {
    return jsonError(400, "invalid_request", errMessage(err));
  }
  const applied = await readAppliedMigrationsIfTableExists(env, ns, database.databaseId, requestId);
  if (applied.ok === false) {
    logD1MigrationFailure(
      "d1_migrations_status_failed", "d1_migrations_status_failed",
      requestId, ns, database.databaseId, applied
    );
    return jsonResponse(applied.status, d1RuntimeFailure("d1_migrations_status_failed", ns, database.databaseId, applied));
  }
  const migrations = migrationStatus(localMigrations, applied.migrations);
  return jsonResponse(200, {
    namespace: ns,
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    migrations,
    applied: migrations.filter((m) => m.state === "applied").length,
    pending: migrations.filter((m) => m.state === "pending").length,
    drifted: migrations.filter((m) => m.state === "drifted").length,
  });
}

/** @param {D1MigrationRequestArgs} args */
export async function applyMigrations({ request, env, ns, databaseId, requestId }) {
  const log = requireControlLog();
  const checked = await resolveDatabaseRef(ns, databaseId);
  if (checked.response) return checked.response;
  const database = checked.database;
  const parsed = await readJsonBody(request, { requireObject: true });
  if (parsed.response) return parsed.response;
  let migrations;
  try {
    const body = /** @type {{ migrations?: unknown[] }} */ (parsed.body);
    migrations = /** @type {MigrationApply[]} */ ((body.migrations || []).map(normalizeMigrationApply));
  } catch (err) {
    return jsonError(400, "invalid_request", errMessage(err));
  }
  if (migrations.length === 0) {
    return jsonError(400, "invalid_request", "migrations must be a non-empty array");
  }

  // Advisory control-plane mutex for clean UX. Correctness still comes from
  // per-DB owner serialization, SQLite transaction, and the migrations table.
  const lock = await acquireMigrationLock(ns, database.databaseId);
  if (!lock) {
    return jsonError(409, "d1_migrations_apply_in_progress", "D1 migrations apply is already in progress", {
      namespace: ns,
      databaseId: database.databaseId,
      databaseName: database.databaseName,
    });
  }

  /** @type {Array<AppliedMigration & { statementCount?: number }>} */
  const appliedNow = [];
  /** @type {AppliedMigration[]} */
  const skipped = [];
  try {
    const existing = await readAppliedMigrations(env, ns, database.databaseId, requestId);
    if (existing.ok === false) {
      logD1MigrationFailure(
        "d1_migrations_apply_failed", "d1_migrations_apply_failed",
        requestId, ns, database.databaseId, existing,
        migrationProgressLogFields(appliedNow, skipped)
      );
      return jsonResponse(existing.status, d1RuntimeFailure(
        "d1_migrations_apply_failed",
        ns,
        database.databaseId,
        existing,
        migrationProgress(appliedNow, skipped)
      ));
    }
    const appliedById = new Map(existing.migrations.map((migration) => [migration.id, migration]));

    for (const migration of migrations) {
      const lockRenewal = await renewMigrationLock(lock);
      if (!lockRenewal.ok) {
        return jsonError(409, "d1_migrations_apply_lock_lost", "D1 migrations apply lock was lost", {
          namespace: ns,
          databaseId: database.databaseId,
          databaseName: database.databaseName,
          migrationId: migration.id,
          ...migrationProgress(appliedNow, skipped),
        });
      }
      const checksum = await sha256Hex(migration.sql);
      const previous = appliedById.get(migration.id);
      if (previous) {
        if (previous.checksum !== checksum) {
          return jsonError(409, "d1_migration_checksum_mismatch", "D1 migration checksum mismatch", {
            namespace: ns,
            databaseId: database.databaseId,
            databaseName: database.databaseName,
            migrationId: migration.id,
            appliedChecksum: previous.checksum,
            checksum,
            ...migrationProgress(appliedNow, skipped),
          });
        }
        skipped.push({
          id: migration.id,
          name: previous.name || migration.name,
          checksum,
          appliedAt: previous.appliedAt,
        });
        continue;
      }

      const statements = splitSqlStatements(migration.sql);
      if (statements.length === 0) {
        return jsonError(400, "d1_migration_empty_sql", "D1 migration SQL is empty", {
          namespace: ns,
          databaseId: database.databaseId,
          databaseName: database.databaseName,
          migrationId: migration.id,
          ...migrationProgress(appliedNow, skipped),
        });
      }
      const appliedAt = new Date().toISOString();
      const result = await d1RuntimeQuery(env, ns, database.databaseId, "batch", [
        ...statements,
        {
          sql: "insert into _wdl_d1_migrations (id, name, checksum, applied_at) values (?, ?, ?, ?)",
          params: [migration.id, migration.name, checksum, appliedAt],
        },
      ], requestId);
      if (!result.ok) {
        logD1MigrationFailure(
          "d1_migration_apply_failed", "d1_migration_apply_failed",
          requestId, ns, database.databaseId, result,
          { migration_id: migration.id, ...migrationProgressLogFields(appliedNow, skipped) }
        );
        return jsonResponse(result.status, d1RuntimeFailure(
          "d1_migration_apply_failed",
          ns,
          database.databaseId,
          result,
          { migrationId: migration.id, ...migrationProgress(appliedNow, skipped) }
        ));
      }
      const record = {
        id: migration.id,
        name: migration.name,
        checksum,
        appliedAt,
        statementCount: statements.length,
      };
      appliedNow.push(record);
      appliedById.set(migration.id, record);
    }
  } finally {
    await releaseMigrationLock(lock, {
      log,
      ns,
      databaseId: database.databaseId,
      requestId,
    });
  }

  log("info", "d1_migrations_applied", {
    request_id: requestId,
    namespace: ns,
    database_id: database.databaseId,
    applied: appliedNow.length,
    skipped: skipped.length,
  });
  return jsonResponse(200, {
    namespace: ns,
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    applied: appliedNow,
    skipped,
  });
}
