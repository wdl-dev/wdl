// Loaded-isolate D1 client facade. Instances live in user worker isolates,
// so prepare()/bind() return normal local objects. Only terminal operations
// call the runtime-side flat D1Database stub.

import { splitSqlStatements } from "./_wdl-sql-splitter.js";
import { normalizeD1Param } from "./_wdl-d1-params.js";
import { decodeD1Transport } from "./_wdl-d1-transport.js";
import { requestIdFromOptions } from "./_wdl-request-id.js";

const D1_SESSION_CONSTRAINT_FIRST_PRIMARY = "first-primary";
const D1_SESSION_CONSTRAINT_FIRST_UNCONSTRAINED = "first-unconstrained";
const intrinsicReflectApply = Reflect.apply;
const intrinsicWeakMapGet = WeakMap.prototype.get;
const intrinsicWeakMapHas = WeakMap.prototype.has;
const intrinsicWeakMapSet = WeakMap.prototype.set;

/**
 * @typedef {string | number | null | undefined | number[]} D1Param
 * @typedef {{ sql: string, params: D1Param[] }} SerializedStatement
 * @typedef {Record<string, unknown>} D1Row
 * @typedef {{ columns: string[], rows: unknown[][] }} RawD1Rows
 * @typedef {Record<string, unknown> & { results?: D1Row[] | RawD1Rows }} D1Result
 * @typedef {{ query(mode: string, statements: SerializedStatement[], requestId: string | null): Promise<unknown> }} D1Stub
 * @typedef {{ stub: D1Stub, requestIdOptions: object }} D1DatabaseState
 * @typedef {{ db: object, bookmarkOrConstraint: string }} D1SessionState
 * @typedef {{ dbOrSession: object, statement: string, params: D1Param[], owner: object }} D1StatementState
 */

/** @param {unknown} result */
function normalizeResults(result) {
  const record = /** @type {D1Result} */ (Object(result));
  if (!("results" in record)) return { ...record, results: [] };
  if (Array.isArray(record.results)) return record;
  const { columns, rows } = /** @type {RawD1Rows} */ (record.results);
  return {
    ...record,
    results: rows.map((row) => Object.fromEntries(columns.map((column, idx) => [column, row[idx]]))),
  };
}

/** @param {string} message */
function d1ParameterError(message) {
  if (/D1_LIMIT_ERROR/i.test(message)) {
    return new D1Error(`D1_ERROR [limit-exceeded]: ${message}`, {
      code: "limit-exceeded",
      category: "limit",
      retryable: false,
    });
  }
  if (/D1_TYPE_ERROR/i.test(message)) {
    return new D1Error(`D1_ERROR [invalid-parameter]: ${message}`, {
      code: "invalid-parameter",
      category: "invalid-request",
      retryable: false,
    });
  }
  return null;
}

/** @param {string} sql */
export function splitExecStatements(sql) {
  return splitSqlStatements(sql);
}

export class D1Error extends Error {
  /**
   * @param {string} message
   * @param {{
   *   code?: string | null,
   *   category?: string | null,
   *   retryable?: boolean,
   *   statementIndex?: number,
   *   causeCode?: string | null,
   * }} [options]
   */
  constructor(
    message,
    { code = null, category = null, retryable = false, statementIndex = undefined, causeCode = null } = {}
  ) {
    super(message);
    this.name = "D1_ERROR";
    /** @type {string | null} */
    this.code = code;
    /** @type {string | null} */
    this.category = category;
    /** @type {boolean} */
    this.retryable = retryable;
    /** @type {number | undefined} */
    this.statementIndex = statementIndex;
    /** @type {string | null} */
    this.causeCode = causeCode;
  }
}

const databaseState = /** @type {WeakMap<object, D1DatabaseState>} */ (new WeakMap());
const sessionState = /** @type {WeakMap<object, D1SessionState>} */ (new WeakMap());
const statementState = /** @type {WeakMap<object, D1StatementState>} */ (new WeakMap());

/** @param {WeakMap<object, unknown>} map @param {object} key */
function weakMapGet(map, key) {
  return intrinsicReflectApply(intrinsicWeakMapGet, map, [key]);
}

/** @param {WeakMap<object, unknown>} map @param {object} key */
function weakMapHas(map, key) {
  return intrinsicReflectApply(intrinsicWeakMapHas, map, [key]);
}

/** @param {WeakMap<object, unknown>} map @param {object} key @param {unknown} value */
function weakMapSet(map, key, value) {
  intrinsicReflectApply(intrinsicWeakMapSet, map, [key, value]);
}

/** @param {object} dbOrSession */
function rootDatabase(dbOrSession) {
  if (weakMapHas(databaseState, dbOrSession)) return dbOrSession;
  const session = /** @type {D1SessionState | undefined} */ (weakMapGet(sessionState, dbOrSession));
  if (session) return session.db;
  throw new D1Error("D1_ERROR: invalid D1 database/session object", {
    code: "invalid-database",
    category: "invalid-request",
  });
}

/** @param {object} db */
function requestIdFor(db) {
  const state = /** @type {D1DatabaseState | undefined} */ (weakMapGet(databaseState, db));
  if (!state) return null;
  return requestIdFromOptions(state.requestIdOptions);
}

/** @param {object} statement */
function serializeStatement(statement) {
  const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, statement));
  if (!state) {
    throw new D1Error("D1_ERROR: batch() expects D1PreparedStatement values", {
      code: "invalid-batch",
      category: "invalid-request",
    });
  }
  return { sql: state.statement, params: state.params };
}

/**
 * @param {object} dbOrSession
 * @param {unknown} statement
 */
function prepareFor(dbOrSession, statement) {
  if (typeof statement !== "string" || !statement.trim()) {
    throw new D1Error("D1_ERROR: prepare() requires a non-empty SQL string", {
      code: "invalid-sql",
      category: "invalid-request",
    });
  }
  return new D1PreparedStatement(dbOrSession, statement);
}

/**
 * @param {object} dbOrSession
 * @param {string} mode
 * @param {SerializedStatement[]} statements
 */
async function sendFor(dbOrSession, mode, statements) {
  const db = rootDatabase(dbOrSession);
  const state = /** @type {D1DatabaseState | undefined} */ (weakMapGet(databaseState, db));
  if (!state) throw new D1Error("D1_ERROR: invalid D1 database object", {
    code: "invalid-database",
    category: "invalid-request",
  });
  try {
    return decodeD1Transport(await state.stub.query(mode, statements, requestIdFor(db)));
  } catch (err) {
    if (err instanceof D1Error) throw err;
    const message = err instanceof Error ? err.message : String(err);
    const errRecord = /** @type {Record<string, unknown>} */ (Object(err));
    const code = typeof errRecord.code === "string"
      ? errRecord.code
      : (message.match(/D1_ERROR \[([^\]]+)\]/)?.[1] ?? null);
    throw new D1Error(message, {
      code,
      category: typeof errRecord.category === "string" ? errRecord.category : null,
      retryable: errRecord.retryable === true,
      statementIndex: Number.isInteger(errRecord.statementIndex) ? /** @type {number} */ (errRecord.statementIndex) : undefined,
      causeCode: typeof errRecord.causeCode === "string" ? errRecord.causeCode : null,
    });
  }
}

/**
 * @param {object} dbOrSession
 * @param {unknown} statements
 */
async function batchFor(dbOrSession, statements) {
  if (!Array.isArray(statements)) {
    throw new D1Error("D1_ERROR: batch() expects an array", {
      code: "invalid-batch",
      category: "invalid-request",
    });
  }
  /** @type {SerializedStatement[]} */
  const serialized = [];
  for (const statement of statements) {
    const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, statement));
    if (!state) {
      throw new D1Error("D1_ERROR: batch() expects D1PreparedStatement values", {
        code: "invalid-batch",
        category: "invalid-request",
      });
    }
    if (state.owner !== dbOrSession) {
      throw new D1Error("D1_ERROR: batch() statements must be prepared from the same D1 database/session", {
        code: "invalid-batch",
        category: "invalid-request",
      });
    }
    serialized.push({ sql: state.statement, params: state.params });
  }
  const results = await sendFor(dbOrSession, "batch", serialized);
  return Array.isArray(results) ? results.map(normalizeResults) : [normalizeResults(results)];
}

export class D1PreparedStatement {
  /**
   * @param {object} dbOrSession
   * @param {string} statement
   * @param {D1Param[]} [params]
   * @param {object} [owner]
   */
  constructor(dbOrSession, statement, params = [], owner = dbOrSession) {
    weakMapSet(statementState, this, { dbOrSession, statement, params, owner });
  }

  /** @param {...unknown} values */
  bind(...values) {
    try {
      const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, this));
      if (!state) throw new Error("invalid prepared statement");
      return new D1PreparedStatement(
        state.dbOrSession,
        state.statement,
        values.map(normalizeD1Param),
        state.owner
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const wrapped = d1ParameterError(message);
      if (wrapped) throw wrapped;
      throw err;
    }
  }

  /** @param {string} [columnName] */
  async first(columnName) {
    const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, this));
    if (!state) throw new D1Error("D1_ERROR: invalid prepared statement", {
      code: "invalid-statement",
      category: "invalid-request",
    });
    const result = normalizeResults(await sendFor(state.dbOrSession, "all", [serializeStatement(this)]));
    const rows = /** @type {D1Row[]} */ (result.results);
    const row = rows[0];
    if (row === undefined) return null;
    if (columnName !== undefined) {
      if (row[columnName] === undefined) {
        throw new D1Error(`D1_COLUMN_NOTFOUND: Column not found (${columnName})`, {
          code: "column-not-found",
          category: "invalid-request",
          retryable: false,
        });
      }
      return row[columnName];
    }
    return row;
  }

  async run() {
    const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, this));
    if (!state) throw new D1Error("D1_ERROR: invalid prepared statement", {
      code: "invalid-statement",
      category: "invalid-request",
    });
    return await sendFor(state.dbOrSession, "run", [serializeStatement(this)]);
  }

  async all() {
    const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, this));
    if (!state) throw new D1Error("D1_ERROR: invalid prepared statement", {
      code: "invalid-statement",
      category: "invalid-request",
    });
    return normalizeResults(await sendFor(state.dbOrSession, "all", [serializeStatement(this)]));
  }

  /** @param {{ columnNames?: boolean }} [options] */
  async raw(options = {}) {
    const state = /** @type {D1StatementState | undefined} */ (weakMapGet(statementState, this));
    if (!state) throw new D1Error("D1_ERROR: invalid prepared statement", {
      code: "invalid-statement",
      category: "invalid-request",
    });
    const result = await sendFor(state.dbOrSession, "raw", [serializeStatement(this)]);
    /** @type {{ results?: { columns: unknown[], rows: unknown[][] } }} */
    const rawResult = Object(result);
    const raw = rawResult.results || { columns: [], rows: [] };
    const rows = raw.rows.map((row) => row.slice());
    if (options.columnNames) rows.unshift(raw.columns.slice());
    return rows;
  }

}

export class D1DatabaseSession {
  /**
   * @param {object} db
   * @param {unknown} [constraintOrBookmark]
   */
  constructor(db, constraintOrBookmark = D1_SESSION_CONSTRAINT_FIRST_UNCONSTRAINED) {
    const normalized = String(constraintOrBookmark ?? "").trim() || D1_SESSION_CONSTRAINT_FIRST_UNCONSTRAINED;
    weakMapSet(sessionState, this, { db, bookmarkOrConstraint: normalized });
  }

  /** @param {unknown} statement */
  prepare(statement) {
    return prepareFor(this, statement);
  }

  /** @param {unknown} statements */
  async batch(statements) {
    return await batchFor(this, statements);
  }

  getBookmark() {
    const state = /** @type {D1SessionState | undefined} */ (weakMapGet(sessionState, this));
    if (!state) return null;
    const { bookmarkOrConstraint } = state;
    if (
      bookmarkOrConstraint === D1_SESSION_CONSTRAINT_FIRST_PRIMARY ||
      bookmarkOrConstraint === D1_SESSION_CONSTRAINT_FIRST_UNCONSTRAINED
    ) {
      return null;
    }
    return bookmarkOrConstraint;
  }
}

export class D1Database {
  /**
   * @param {D1Stub} stub
   * @param {{ requestId?: unknown, requestIdProvider?: unknown }} [options]
   */
  constructor(stub, options = {}) {
    weakMapSet(databaseState, this, {
      stub,
      requestIdOptions: options,
    });
  }

  /** @param {unknown} statement */
  prepare(statement) {
    return prepareFor(this, statement);
  }

  /** @param {unknown} statements */
  async batch(statements) {
    return await batchFor(this, statements);
  }

  /** @param {unknown} sql */
  async exec(sql) {
    if (typeof sql !== "string" || !sql.trim()) {
      throw new D1Error("D1_ERROR: exec() requires a non-empty SQL string", {
        code: "invalid-sql",
        category: "invalid-request",
      });
    }
    const statements = splitExecStatements(sql).map((statement) => ({
      sql: statement.sql,
      params: statement.params.map(normalizeD1Param),
    }));
    return await sendFor(this, "exec", statements);
  }

  /** @param {unknown} constraintOrBookmark */
  withSession(constraintOrBookmark) {
    return new D1DatabaseSession(this, constraintOrBookmark);
  }
}
