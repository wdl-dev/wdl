import {
  jsonResponse,
  jsonError,
  readJsonBody,
  errMessage,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { d1DatabasesKey } from "control-lib";
import {
  EXECUTE_MODES,
  MIGRATIONS_TABLE_SQL,
  isReadyDatabase,
  splitSqlStatements,
  validateDatabaseName,
} from "control-d1-model";
import {
  d1RuntimeFailure,
  d1RuntimeFailureLogFields,
  d1RuntimeProbeOwner,
  d1RuntimePublicResult,
  d1RuntimeQuery,
  d1RuntimeReleaseOwner,
} from "control-d1-runtime-client";
import {
  commitDatabaseMetadata,
  createDatabaseId,
  deleteDatabaseMetadata,
  getDatabase,
  getDatabases,
  getDatabaseIdByName,
  isExpiredProvisional,
  markDatabaseReady,
  resolveDatabaseRef,
  rollbackExpiredProvisionalDatabaseMetadata,
  rollbackProvisionalDatabaseMetadata,
  updateDatabaseTombstoneOwnerRelease,
} from "control-d1-store";

/**
 * @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} Fetcher
 * @typedef {{ D1_BACKEND: Fetcher, D1_QUERY_TIMEOUT_MS?: unknown, [key: string]: unknown }} D1RuntimeEnv
 * @typedef {{ request: Request, env: D1RuntimeEnv, ns: string, databaseId: string, requestId: string }} D1RequestArgs
 * @typedef {{ env: D1RuntimeEnv, ns: string, databaseId: string, requestId: string }} D1RouteArgs
 * @typedef {import("control-d1-runtime-client").D1OwnerHint} D1OwnerHint
 * @typedef {{ databaseId: string, databaseName: string | null, createdAt?: string, updatedAt?: string }} PublicDatabaseRecord
 * @typedef {{ owner?: D1OwnerHint | null }} InitializedOwnerResult
 */

/**
 * @param {unknown} err
 * @param {string} code
 */
function hasErrorCode(err, code) {
  return err !== null && err !== undefined && typeof err === "object" && "code" in err && err.code === code;
}

/** @param {unknown} body */
function responseBodyRecord(body) {
  return body !== null && typeof body === "object" && !Array.isArray(body)
    ? /** @type {Record<string, unknown>} */ (body)
    : {};
}

/** @param {Record<string, unknown>} body @param {string} key */
function stringField(body, key) {
  const value = body[key];
  return typeof value === "string" ? value : "";
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} requestId
 */
async function initializeDatabaseStorage(env, ns, databaseId, requestId) {
  return await d1RuntimeQuery(env, ns, databaseId, "exec", [
    { sql: MIGRATIONS_TABLE_SQL, params: [] },
  ], requestId);
}

/** @param {PublicDatabaseRecord} database */
function publicDatabaseMetadata(database) {
  return {
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    createdAt: database.createdAt,
    updatedAt: database.updatedAt,
  };
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {InitializedOwnerResult} initialized
 * @param {string} requestId
 */
async function releaseInitializedOwner(env, ns, databaseId, initialized, requestId) {
  const log = requireControlLog();
  const released = await d1RuntimeReleaseOwner(env, ns, databaseId, initialized?.owner, requestId);
  if (released.ok) {
    log("info", "d1_database_orphan_owner_released", {
      request_id: requestId,
      namespace: ns,
      database_id: databaseId,
      owner_task_id: initialized?.owner?.taskId || null,
      owner: initialized?.owner || null,
    });
    return;
  }
  const releasedBody = responseBodyRecord(released.body);
  log("warn", "d1_database_orphan_owner_release_failed", {
    request_id: requestId,
    namespace: ns,
    database_id: databaseId,
    owner_task_id: initialized?.owner?.taskId || null,
    owner: initialized?.owner || null,
    backend_status: released.status,
    error_code: stringField(releasedBody, "error"),
    error_message: stringField(releasedBody, "message") || stringField(releasedBody, "error"),
  });
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} requestId
 */
async function releaseDeletedOwner(env, ns, databaseId, requestId) {
  const log = requireControlLog();
  const probed = await d1RuntimeProbeOwner(env, ns, databaseId, requestId);
  const probedBody = responseBodyRecord(probed.body);
  if (!probed.owner) {
    log(probed.ok ? "info" : "warn", "d1_database_deleted_owner_not_found", {
      request_id: requestId,
      namespace: ns,
      database_id: databaseId,
      backend_status: probed.status,
      error_code: stringField(probedBody, "error"),
      error_message: stringField(probedBody, "message") || stringField(probedBody, "error"),
    });
    return {
      status: "not_found",
      errorCode: stringField(probedBody, "error"),
      errorMessage: stringField(probedBody, "message"),
    };
  }
  const released = await d1RuntimeReleaseOwner(env, ns, databaseId, probed.owner, requestId);
  if (released.ok) {
    log("info", "d1_database_deleted_owner_released", {
      request_id: requestId,
      namespace: ns,
      database_id: databaseId,
      owner_task_id: probed.owner.taskId || null,
      owner: probed.owner,
    });
    return { status: "released", errorCode: "", errorMessage: "" };
  }
  const releasedBody = responseBodyRecord(released.body);
  log("warn", "d1_database_deleted_owner_release_failed", {
    request_id: requestId,
    namespace: ns,
    database_id: databaseId,
    owner_task_id: probed.owner.taskId || null,
    owner: probed.owner,
    backend_status: released.status,
    error_code: stringField(releasedBody, "error"),
    error_message: stringField(releasedBody, "message") || stringField(releasedBody, "error"),
  });
  return {
    status: "failed",
    errorCode: stringField(releasedBody, "error"),
    errorMessage: stringField(releasedBody, "message") || `status ${released.status}`,
  };
}

/** @param {{ request: Request, env: D1RuntimeEnv, ns: string, requestId: string }} args */
export async function createDatabase({ request, env, ns, requestId }) {
  const log = requireControlLog();
  const parsed = await readJsonBody(request, { requireObject: true });
  if (parsed.response) return parsed.response;
  const body = /** @type {Record<string, unknown>} */ (parsed.body);

  const databaseName = body.databaseName;
  if (typeof databaseName !== "string") {
    return jsonError(400, "invalid_request", "databaseName is required");
  }
  try {
    validateDatabaseName(databaseName);
  } catch (err) {
    return jsonError(400, "invalid_request", errMessage(err));
  }
  const now = new Date().toISOString();
  const existingId = await getDatabaseIdByName(ns, databaseName);
  if (existingId) {
    const existing = await getDatabase(ns, existingId);
    if (!existing || !isExpiredProvisional(existing, now)) {
      return jsonError(409, "d1_database_exists", "D1 database already exists", {
        namespace: ns,
        databaseName,
      });
    }
    const rolledBack = await rollbackExpiredProvisionalDatabaseMetadata(ns, existing, now);
    if (!rolledBack.rolledBack) {
      const rollbackReason = "reason" in rolledBack ? rolledBack.reason || "" : "";
      if (rollbackReason === "contention") {
        return jsonError(503, "d1_database_create_contention", "D1 database create contention, retry later", {
          namespace: ns,
          databaseName,
        });
      }
      if (rollbackReason === "not-expired") {
        return jsonError(409, "d1_database_exists", "D1 database already exists", {
          namespace: ns,
          databaseName,
        });
      }
    }
  }

  const databaseId = createDatabaseId();
  const database = {
    databaseId,
    databaseName,
    createdAt: now,
    updatedAt: now,
  };

  const created = await commitDatabaseMetadata(ns, databaseName, database.databaseId, now);
  if (!created.ok) {
    const createFailure = /** @type {{ ok: false, reason?: string, error?: string }} */ (created);
    if (createFailure.error) return jsonError(400, "invalid_request", createFailure.error);
    if (createFailure.reason === "contention") {
      return jsonError(503, "d1_database_create_contention", "D1 database create contention, retry later", { namespace: ns, databaseName });
    }
    if (createFailure.reason === "id-collision") {
      return jsonError(503, "d1_database_id_collision", "D1 database id collision, retry later", { namespace: ns, databaseName });
    }
    return jsonError(409, "d1_database_exists", "D1 database already exists", {
      namespace: ns,
      databaseName,
    });
  }

  const initialized = await initializeDatabaseStorage(env, ns, database.databaseId, requestId);
  if (!initialized.ok) {
    if (initialized.owner) await releaseInitializedOwner(env, ns, database.databaseId, initialized, requestId);
    const rolledBack = await rollbackProvisionalDatabaseMetadata(ns, database);
    if (!rolledBack.rolledBack) {
      const rollbackReason = "reason" in rolledBack ? rolledBack.reason || "initialize-failed" : "initialize-failed";
      log("warn", "d1_database_provisional_rollback_failed", {
        request_id: requestId,
        namespace: ns,
        database_id: database.databaseId,
        reason: rollbackReason,
      });
    }
    log("error", "d1_database_initialize_failed", {
      request_id: requestId,
      namespace: ns,
      database_id: database.databaseId,
      status: 503,
      reason: "d1_database_initialize_failed",
      ...d1RuntimeFailureLogFields(initialized),
    });
    return jsonResponse(503, d1RuntimeFailure(
      "d1_database_initialize_failed",
      ns,
      database.databaseId,
      initialized,
      {},
      { publicStatus: 503 }
    ));
  }

  const readyAt = new Date().toISOString();
  const ready = await markDatabaseReady(ns, database, readyAt);
  if (!ready.ok) {
    const readyFailure = /** @type {{ ok: false, reason: string }} */ (ready);
    await releaseInitializedOwner(env, ns, database.databaseId, initialized, requestId);
    const rolledBack = await rollbackProvisionalDatabaseMetadata(ns, database);
    if (!rolledBack.rolledBack) {
      const rollbackReason = "reason" in rolledBack ? rolledBack.reason || readyFailure.reason : readyFailure.reason;
      log("warn", "d1_database_provisional_rollback_failed", {
        request_id: requestId,
        namespace: ns,
        database_id: database.databaseId,
        reason: rollbackReason,
      });
    }
    return jsonError(503, "d1_database_create_contention", "D1 database create contention, retry later", {
      namespace: ns,
      databaseName,
    });
  }

  log("info", "d1_database_created", {
    request_id: requestId,
    namespace: ns,
    database_id: database.databaseId,
  });
  return jsonResponse(201, {
    namespace: ns,
    ...publicDatabaseMetadata({ ...database, updatedAt: readyAt }),
    initialized: true,
  });
}

/** @param {{ ns: string, requestId: string }} args */
export async function listDatabases({ ns, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const ids = (await redis.sMembers(d1DatabasesKey(ns))).toSorted();
  /** @type {PublicDatabaseRecord[]} */
  const databases = [];
  for (const meta of await getDatabases(ns, ids)) {
    if (meta && isReadyDatabase(meta)) databases.push(publicDatabaseMetadata(meta));
  }
  log("info", "d1_databases_listed", {
    request_id: requestId,
    namespace: ns,
    count: databases.length,
  });
  return jsonResponse(200, { namespace: ns, databases });
}

/** @param {D1RouteArgs} args */
export async function deleteDatabase({ env, ns, databaseId, requestId }) {
  const log = requireControlLog();
  const checked = await resolveDatabaseRef(ns, databaseId);
  if (checked.response) return checked.response;
  const database = checked.database;

  let deleted;
  try {
    deleted = await deleteDatabaseMetadata(ns, database, new Date().toISOString(), requestId);
  } catch (err) {
    if (hasErrorCode(err, "d1_database_delete_contention")) {
      return jsonError(503, "d1_database_delete_contention", "D1 database delete contention, retry later", {
        namespace: ns,
        databaseId: database.databaseId,
        databaseName: database.databaseName,
      });
    }
    throw err;
  }
  if (!deleted.deleted) {
    const failedDelete = /** @type {{ blockers?: unknown[], malformedReferrerCount?: number }} */ (deleted);
    if (failedDelete.blockers?.length) {
      /** @type {{ namespace: string, databaseId: string, databaseName: string | null, blockers: unknown[], malformedReferrerCount?: number }} */
      const payload = {
        namespace: ns,
        databaseId: database.databaseId,
        databaseName: database.databaseName,
        blockers: failedDelete.blockers,
      };
      if ((failedDelete.malformedReferrerCount ?? 0) > 0) {
        payload.malformedReferrerCount = failedDelete.malformedReferrerCount;
      }
      return jsonError(409, "d1_database_in_use", "D1 database is in use", payload);
    }
    if ((failedDelete.malformedReferrerCount ?? 0) > 0) {
      return jsonError(409, "d1_database_in_use", "D1 database is in use", {
        namespace: ns,
        databaseId: database.databaseId,
        databaseName: database.databaseName,
        blockers: [],
        malformedReferrerCount: failedDelete.malformedReferrerCount,
      });
    }

    return jsonError(404, "d1_database_not_found", "D1 database not found", {
      namespace: ns,
      databaseRef: databaseId,
    });
  }

  const ownerRelease = await releaseDeletedOwner(env, ns, database.databaseId, requestId);
  try {
    await updateDatabaseTombstoneOwnerRelease(
      ns,
      database.databaseId,
      ownerRelease.status,
      ownerRelease.errorMessage,
      new Date().toISOString()
    );
  } catch (err) {
    log("warn", "d1_database_deleted_tombstone_update_failed", {
      request_id: requestId,
      namespace: ns,
      database_id: database.databaseId,
      owner_release_status: ownerRelease.status,
      error_message: errMessage(err),
    });
  }

  log("info", "d1_database_deleted", {
    request_id: requestId,
    namespace: ns,
    database_id: database.databaseId,
  });
  return jsonResponse(200, {
    namespace: ns,
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    deleted: true,
  });
}

/** @param {D1RequestArgs} args */
export async function executeDatabase({ request, env, ns, databaseId, requestId }) {
  const log = requireControlLog();
  const checked = await resolveDatabaseRef(ns, databaseId);
  if (checked.response) return checked.response;
  const database = checked.database;
  const parsed = await readJsonBody(request, { requireObject: true });
  if (parsed.response) return parsed.response;
  const body = /** @type {Record<string, unknown>} */ (parsed.body);

  const sql = body.sql;
  const params = body.params == null ? [] : body.params;
  const mode = body.mode == null ? "all" : body.mode;
  if (typeof sql !== "string" || !sql.trim()) {
    return jsonError(400, "invalid_request", "sql is required");
  }
  if (!Array.isArray(params)) {
    return jsonError(400, "invalid_request", "params must be an array");
  }
  if (typeof mode !== "string" || !EXECUTE_MODES.has(mode)) {
    return jsonError(400, "invalid_request", `mode must be one of ${Array.from(EXECUTE_MODES).join(", ")}`);
  }

  let statements;
  if (mode === "exec") {
    if (params.length > 0) {
      return jsonError(400, "invalid_request", "exec mode does not accept params");
    }
    statements = splitSqlStatements(sql);
    if (statements.length === 0) return jsonError(400, "invalid_request", "sql has no executable statements");
  } else {
    statements = splitSqlStatements(sql);
    if (statements.length !== 1) {
      return jsonError(400, "invalid_request", `${mode} mode requires exactly one SQL statement`);
    }
    statements[0].params = params;
  }

  const result = await d1RuntimeQuery(env, ns, database.databaseId, mode, statements, requestId);
  if (!result.ok) {
    log(result.status >= 500 ? "error" : "warn", "d1_database_execute_failed", {
      request_id: requestId,
      namespace: ns,
      database_id: database.databaseId,
      status: result.status,
      reason: "d1_execute_failed",
      ...d1RuntimeFailureLogFields(result),
    });
    return jsonResponse(result.status, d1RuntimeFailure("d1_execute_failed", ns, database.databaseId, result));
  }

  log("info", "d1_database_executed", {
    request_id: requestId,
    namespace: ns,
    database_id: database.databaseId,
    mode,
    statement_count: statements.length,
  });
  return jsonResponse(200, {
    namespace: ns,
    databaseId: database.databaseId,
    databaseName: database.databaseName,
    mode,
    result: d1RuntimePublicResult(result.body, mode),
  });
}
