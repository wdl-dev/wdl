import { DurableObject } from "cloudflare:workers";
import {
  classifyD1Error,
  D1ProtocolError,
  d1ErrorResponse,
  readD1ActorControlRequest,
  readD1ActorQueryRequest,
} from "d1-runtime-protocol";
import { assertCurrentOwnerWithLeaseBudget } from "d1-runtime-owner-registry";
import { d1QueryResponse, json, jsonError } from "d1-runtime-http";
import {
  parseIdempotentSchemaDdl,
  statementMayChangeDb,
} from "d1-runtime-read-cache";
import {
  beginPendingQuery,
  endPendingQuery,
  isDraining,
  pendingQueryCount,
  recordPayloadStorageSize,
  recordStorageSizeForDb,
} from "d1-runtime-state";
import {
  isD1ActorTestHook,
  runD1ActorTestHook,
} from "d1-runtime-test-hooks";
import { fnv1a32Utf8 } from "shared-fnv1a32";

const DEFAULT_D1_MAX_RESULT_ROWS = 65_536;
const DEFAULT_D1_MAX_RESULT_BYTES = 16 * 1024 * 1024;
const DEFAULT_D1_ACTOR_IDLE_WAIT_TIMEOUT_MS = 10_000;
const D1_ACTOR_IDLE_WAIT_POLL_MS = 25;
const utf8Encoder = new TextEncoder();
const SCHEMA_MUTATION_SQL_RE = /\b(?:create|drop|alter|reindex|vacuum|attach|detach)\b/i;

/**
 * @typedef {{ sql: string, params: unknown[] }} D1Statement
 * @typedef {{ namespace?: string, databaseId?: string, dbKey: string, slot?: string | number, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} D1Owner
 * @typedef {{ type: string, name: string }} D1Ddl
 * @typedef {{ columnNames?: string[], raw(): Iterable<unknown[]>, rowsWritten?: number, rowsRead?: number, toArray(): Array<Record<string, unknown>> }} D1Cursor
 * @typedef {{ exec(sql: string, ...params: unknown[]): D1Cursor, databaseSize: number }} D1Sql
 * @typedef {{ storage: { sql: D1Sql, transactionSync(callback: () => unknown): unknown } }} D1ActorState
 * @typedef {Record<string, unknown>} D1RuntimeEnv
 * @typedef {{ mode: string, statements: D1Statement[], owner: D1Owner, __control?: string, __holdMs?: unknown }} D1ActorQuery
 * @typedef {{ __control: string, __holdMs?: unknown, owner: D1Owner, statements: D1Statement[] }} D1ActorTestHookQuery
 * @typedef {{ remaining: number, limit: number }} D1ResultBudget
 * @typedef {{ owner: D1Owner, leaseRemainingMs?: number, guardMs?: number }} D1OwnerAssertion
 * @typedef {{ deadlineMs: number }} D1LeaseBudgetGuard
 */

/** @param {unknown} value @param {number} fallback */
function positiveIntOr(value, fallback) {
  if (value == null || value === "") return fallback;
  const n = Number(value);
  return Number.isSafeInteger(n) && n > 0 ? n : fallback;
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** @param {D1RuntimeEnv} env */
function actorIdleWaitTimeoutMs(env) {
  return positiveIntOr(env?.D1_ACTOR_IDLE_WAIT_TIMEOUT_MS, DEFAULT_D1_ACTOR_IDLE_WAIT_TIMEOUT_MS);
}

/** @param {D1RuntimeEnv} env @param {AbortSignal | null | undefined} signal */
async function waitUntilActorIdle(env, signal) {
  const deadline = Date.now() + actorIdleWaitTimeoutMs(env);
  while (pendingQueryCount() > 0) {
    if (signal?.aborted) {
      throw new D1ProtocolError(503, "drain-timeout", "D1 actor idle wait was aborted");
    }
    if (Date.now() >= deadline) {
      throw new D1ProtocolError(
        503,
        "drain-timeout",
        `D1 actor timed out waiting for ${pendingQueryCount()} in-flight query(s)`
      );
    }
    await sleep(D1_ACTOR_IDLE_WAIT_POLL_MS);
  }
}

/** @param {D1RuntimeEnv} env */
function maxResultRows(env) {
  return positiveIntOr(env?.D1_MAX_RESULT_ROWS, DEFAULT_D1_MAX_RESULT_ROWS);
}

/** @param {D1RuntimeEnv} env */
function maxResultBytes(env) {
  return positiveIntOr(env?.D1_MAX_RESULT_BYTES, DEFAULT_D1_MAX_RESULT_BYTES);
}

/** @param {number} limit @returns {D1ResultBudget} */
function createResultBudget(limit) {
  return { remaining: limit, limit };
}

/** @param {D1RuntimeEnv} env */
function monotonicNowMs(env) {
  const testNow = env?.D1_TEST_HOOKS === "1" ? env?.__d1ActorNowMs : null;
  if (typeof testNow === "function") return Number(testNow());
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

/**
 * @param {D1RuntimeEnv} env
 * @param {D1OwnerAssertion} assertion
 * @returns {D1LeaseBudgetGuard | null}
 */
function createLeaseBudgetGuard(env, assertion) {
  const remainingMs = Number(assertion.leaseRemainingMs);
  const guardMs = Number(assertion.guardMs);
  if (!Number.isFinite(remainingMs) || !Number.isFinite(guardMs)) return null;
  return { deadlineMs: monotonicNowMs(env) + Math.max(0, remainingMs - guardMs) };
}

/**
 * @param {D1ResultBudget} budget
 * @param {number} bytes
 */
function consumeResultBudget(budget, bytes) {
  if (bytes > budget.remaining) {
    throw new D1ProtocolError(
      413,
      "limit-exceeded",
      `D1 limit exceeded: maximum result bytes per request is ${budget.limit}`
    );
  }
  budget.remaining -= bytes;
}

/**
 * @param {D1RuntimeEnv} env
 * @param {D1LeaseBudgetGuard | null | undefined} guard
 */
function assertLeaseBudget(env, guard) {
  if (!guard) return;
  if (monotonicNowMs(env) < guard.deadlineMs) return;
  throw new D1ProtocolError(
    503,
    "lease-budget-exhausted",
    "D1 database owner lease budget was exhausted during local SQL dispatch"
  );
}

/** @param {unknown} value @returns {number} */
function resultValueBytes(value) {
  if (value == null) return 0;
  if (typeof value === "boolean") return 1;
  if (typeof value === "number" || typeof value === "bigint") return 8;
  if (typeof value === "string") return utf8Encoder.encode(value).byteLength;
  if (value instanceof Uint8Array) return value.byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (ArrayBuffer.isView(value)) return value.byteLength;
  if (Array.isArray(value)) {
    return value.reduce((sum, item) => sum + resultValueBytes(item), 0);
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    return Object.entries(value).reduce(
      (sum, [key, item]) => sum + utf8Encoder.encode(key).byteLength + resultValueBytes(item),
      0
    );
  }
  return utf8Encoder.encode(String(value)).byteLength;
}

/**
 * @param {unknown[]} row
 * @param {string[]} columns
 * @param {"NONE" | "ROWS_AND_COLUMNS" | "ARRAY_OF_OBJECTS"} resultsFormat
 * @returns {number}
 */
function resultRowBytes(row, columns, resultsFormat) {
  let bytes = 0;
  for (const value of row) bytes += resultValueBytes(value);
  if (resultsFormat === "ARRAY_OF_OBJECTS") {
    bytes += columns.reduce((sum, column) => sum + utf8Encoder.encode(column).byteLength, 0);
  }
  return bytes;
}

/**
 * @param {D1Cursor} cursor
 * @param {"NONE" | "ROWS_AND_COLUMNS" | "ARRAY_OF_OBJECTS"} resultsFormat
 * @param {number} [rowLimit]
 * @param {number} [byteLimit]
 * @param {D1ResultBudget} [budget]
 */
export function resultFromCursor(
  cursor,
  resultsFormat,
  rowLimit = DEFAULT_D1_MAX_RESULT_ROWS,
  byteLimit = DEFAULT_D1_MAX_RESULT_BYTES,
  budget = createResultBudget(byteLimit)
) {
  if (resultsFormat === "NONE") return [];
  const columns = cursor.columnNames || [];
  const rows = [];
  consumeResultBudget(
    budget,
    columns.reduce((sum, column) => sum + utf8Encoder.encode(column).byteLength, 0)
  );
  for (const row of cursor.raw()) {
    if (rows.length >= rowLimit) {
      throw new D1ProtocolError(
        413,
        "limit-exceeded",
        `D1 limit exceeded: maximum result rows per statement is ${rowLimit}`
      );
    }
    consumeResultBudget(budget, resultRowBytes(/** @type {unknown[]} */ (row), columns, resultsFormat));
    rows.push(row);
  }
  if (resultsFormat === "ROWS_AND_COLUMNS") {
    return { columns, rows };
  }
  return rows.map((row) => Object.fromEntries(columns.map((column, idx) => [column, row[idx]])));
}

/** @param {unknown[]} params */
function sqlParams(params) {
  return params.map((param) => Array.isArray(param) ? new Uint8Array(param) : param);
}

export class D1DatabaseActor extends DurableObject {
  // Correctness depends on Redis owner/generation fencing plus workerd's
  // synchronous DO SQL execution. Once the owner/budget check succeeds, the
  // SQL section below must stay await-free until finally/endPendingQuery()
  // runs; adding an await there would let drain/takeover observe the query as
  // idle while the old owner could still commit to SQLite.
  /**
   * @param {DurableObjectState & D1ActorState} state
   * @param {D1RuntimeEnv} env
   */
  constructor(state, env) {
    super(state, env);
    this.state = /** @type {D1ActorState} */ (state);
    this.env = env;
    this.sql = this.state.storage.sql;
    /** @type {Set<string>} */
    this.schemaObjectExistsMemo = new Set();
  }

  /** @param {Request} request */
  async fetch(request) {
    if (request.method !== "POST") {
      return jsonError(405, "method_not_allowed", "Method not allowed");
    }
    let countedPending = false;
    try {
      const contentType = request.headers.get("content-type") || "";
      /** @type {D1ActorQuery} */
      let body;
      if (contentType.split(";", 1)[0].trim().toLowerCase() === "application/json") {
        const control = /** @type {Record<string, unknown>} */ (await readD1ActorControlRequest(request));
        if (control.__control === "wait-until-idle") {
          await waitUntilActorIdle(this.env, request.signal);
          return json({ idle: true });
        }
        throw new D1ProtocolError(415, "unsupported-media-type", "D1 actor query endpoint requires binary query requests");
      } else {
        beginPendingQuery();
        countedPending = true;
        body = /** @type {D1ActorQuery} */ (await readD1ActorQueryRequest(request));
      }
      if (isDraining()) {
        throw new D1ProtocolError(503, "task-draining", "D1 task is draining");
      }
      // Implementation boundary for the class-level invariant above: after
      // this owner check succeeds, the SQL execution path below must not add
      // await points before finally/endPendingQuery().
      const ownerAssertion = await assertCurrentOwnerWithLeaseBudget(this.env, body.owner);
      const leaseGuard = createLeaseBudgetGuard(this.env, ownerAssertion);
      if (isD1ActorTestHook(body)) {
        return d1QueryResponse(await runD1ActorTestHook(this, /** @type {D1ActorTestHookQuery} */ (body)));
      }
      const resultBudget = createResultBudget(maxResultBytes(this.env));
      if (body.mode === "exec") {
        const execResult = this.execStatementsTransactionally(body.statements, body.owner, leaseGuard);
        return d1QueryResponse(execResult.payload, {
          headers: { "x-wdl-d1-changed-db": execResult.changedDb ? "1" : "0" },
        });
      }
      if (body.mode === "batch") {
        /** @type {unknown[]} */
        const batchResults = [];
        this.transactionSyncWithSchemaMemoRollback(() => {
          for (let i = 0; i < body.statements.length; i += 1) {
            try {
              batchResults.push(this.runStatement(body.statements[i], "ROWS_AND_COLUMNS", body.owner, resultBudget, leaseGuard));
            } catch (err) {
              if (err instanceof D1ProtocolError && err.code === "limit-exceeded") throw err;
              if (err instanceof D1ProtocolError && err.code === "lease-budget-exhausted") throw err;
              const classified = classifyD1Error(err);
              throw new D1ProtocolError(
                classified.status,
                "batch-statement-error",
                `D1 batch statement ${i} failed: ${classified.message}`,
                {
                  statementIndex: i,
                  causeCode: classified.code,
                  category: classified.category,
                  retryable: classified.retryable,
                }
              );
            }
          }
          return null;
        });
        recordPayloadStorageSize(body.owner?.dbKey, batchResults);
        return d1QueryResponse(batchResults);
      }
      const resultsFormat = body.mode === "run" ? "NONE" : "ROWS_AND_COLUMNS";
      const results = /** @type {D1Statement[]} */ (body.statements)
        .map((statement) => this.runStatement(statement, resultsFormat, body.owner, resultBudget, leaseGuard));
      const payload = results.length === 1 ? results[0] : results;
      recordPayloadStorageSize(body.owner?.dbKey, payload);
      return d1QueryResponse(payload);
    } catch (err) {
      return d1ErrorResponse(err);
    } finally {
      if (countedPending) endPendingQuery();
    }
  }

  /** @param {D1Statement[]} statements @param {D1Owner} owner @param {D1LeaseBudgetGuard | null} [leaseGuard] */
  execStatementsTransactionally(statements, owner, leaseGuard = null) {
    if (statements.length <= 1) return this.execStatements(statements, owner, leaseGuard);
    return /** @type {{ changedDb: boolean, payload: { count: number, duration: number } }} */ (
      this.transactionSyncWithSchemaMemoRollback(() => this.execStatements(statements, owner, leaseGuard))
    );
  }

  /** @param {() => unknown} callback */
  transactionSyncWithSchemaMemoRollback(callback) {
    const memoBefore = new Set(this.ensureSchemaObjectExistsMemo());
    try {
      return this.state.storage.transactionSync(callback);
    } catch (err) {
      this.schemaObjectExistsMemo = memoBefore;
      throw err;
    }
  }

  /** @param {D1Statement[]} statements @param {D1Owner} owner @param {D1LeaseBudgetGuard | null} [leaseGuard] */
  execStatements(statements, owner, leaseGuard = null) {
    const started = Date.now();
    let changedDb = false;
    for (const statement of statements) {
      const ddl = parseIdempotentSchemaDdl(statement.sql);
      const existedBefore = ddl ? this.schemaObjectExists(ddl, leaseGuard) : null;
      this.checkedSqlExec(statement.sql, sqlParams(statement.params), leaseGuard);
      const statementChanged = this.statementChangedDb(statement.sql, 0, ddl, existedBefore);
      changedDb ||= statementChanged;
    }
    const sizeAfter = this.sql.databaseSize;
    recordStorageSizeForDb(owner?.dbKey, sizeAfter);
    return {
      changedDb,
      payload: {
        count: statements.length,
        duration: Date.now() - started,
      },
    };
  }

  /**
   * @param {string} sql
   * @param {unknown[]} params
   * @param {D1LeaseBudgetGuard | null | undefined} leaseGuard
   */
  checkedSqlExec(sql, params = [], leaseGuard = null) {
    assertLeaseBudget(this.env, leaseGuard);
    return this.sql.exec(sql, ...params);
  }

  /** @param {D1Ddl} ddl @param {D1LeaseBudgetGuard | null} [leaseGuard] */
  schemaObjectExists(ddl, leaseGuard = null) {
    const key = schemaObjectMemoKey(ddl);
    if (this.schemaObjectExistsMemo?.has(key)) return true;
    const cursor = this.checkedSqlExec(
      "select 1 as found from sqlite_master where type = ? and name = ? limit 1",
      [ddl.type, ddl.name],
      leaseGuard
    );
    const exists = cursor.toArray().length > 0;
    if (exists) this.ensureSchemaObjectExistsMemo().add(key);
    return exists;
  }

  ensureSchemaObjectExistsMemo() {
    // Unit tests and helper paths may Object.create() the prototype without
    // running the constructor; keep memo access lazy for those direct calls.
    this.schemaObjectExistsMemo ||= new Set();
    return this.schemaObjectExistsMemo;
  }

  clearSchemaObjectExistsMemo() {
    this.schemaObjectExistsMemo?.clear();
  }

  /** @param {string} sql @param {D1Ddl | null} ddl */
  observeSchemaMutation(sql, ddl) {
    if (!ddl && SCHEMA_MUTATION_SQL_RE.test(sql.trim())) {
      this.clearSchemaObjectExistsMemo();
    }
  }

  /** @param {string} sql @param {number} changes @param {D1Ddl | null} ddl @param {boolean | null} existedBefore */
  statementChangedDb(sql, changes, ddl, existedBefore) {
    this.observeSchemaMutation(sql, ddl);
    if (changes !== 0) return true;
    if (ddl && existedBefore !== null) {
      // This post-write metadata probe must not turn a committed single
      // statement into a retryable ownership failure. The lease guard is only a
      // pre-start budget gate, not a commit-time fence.
      return !existedBefore && this.schemaObjectExists(ddl);
    }
    return statementMayChangeDb(sql);
  }

  /** @param {D1Statement} statement @param {"NONE" | "ROWS_AND_COLUMNS" | "ARRAY_OF_OBJECTS"} resultsFormat @param {D1Owner} owner @param {D1ResultBudget} resultBudget @param {D1LeaseBudgetGuard | null} [leaseGuard] */
  runStatement(statement, resultsFormat, owner, resultBudget, leaseGuard = null) {
    // Keep statement execution fully synchronous. If an await is introduced
    // anywhere under this call, pendingQueryCount() would stop accurately
    // fencing /drain against in-flight SQL.
    const started = Date.now();
    const ddl = parseIdempotentSchemaDdl(statement.sql);
    const existedBefore = ddl ? this.schemaObjectExists(ddl, leaseGuard) : null;

    const cursor = this.checkedSqlExec(statement.sql, sqlParams(statement.params), leaseGuard);
    const results = resultFromCursor(
      cursor,
      resultsFormat,
      maxResultRows(this.env),
      maxResultBytes(this.env),
      resultBudget
    );

    const changes = cursor.rowsWritten || 0;
    const lastRowId = changes > 0
      ? this.sql.exec("SELECT last_insert_rowid() as last_row_id").toArray()[0]?.last_row_id || 0
      : 0;
    const sizeAfter = this.sql.databaseSize;
    const changedDb = this.statementChangedDb(statement.sql, changes, ddl, existedBefore);
    const duration = Date.now() - started;

    return {
      success: true,
      results,
      meta: {
        duration,
        served_by: servedByLabel(owner?.taskId),
        served_by_region: "local",
        served_by_primary: true,
        timings: { sql_duration_ms: duration },
        changes,
        last_row_id: lastRowId,
        changed_db: changedDb,
        size_after: sizeAfter,
        rows_read: cursor.rowsRead || 0,
        rows_written: cursor.rowsWritten || 0,
        total_attempts: 1,
      },
    };
  }
}

// Owner task ids can be infrastructure identifiers. `served_by` is
// tenant-visible, so expose only a stable redacted correlation label. FNV-1a is
// not a security boundary.
/** @param {string | null | undefined} taskId @returns {string} */
function servedByLabel(taskId) {
  if (typeof taskId !== "string" || taskId === "") return "unknown";
  return `d1-${fnv1a32Utf8(taskId).toString(16).padStart(8, "0")}`;
}

/** @param {D1Ddl} ddl */
function schemaObjectMemoKey(ddl) {
  return `${ddl.type}\0${ddl.name}`;
}
