import { test } from "node:test";
import assert from "node:assert/strict";
import { d1TransportDataUrl } from "../helpers/load-d1-protocol.js";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

// D1 currently limits each bind parameter to 2,000,000 bytes; keep this
// mirrored with shared/d1-params.js parameter normalization behavior.
const MAX_BIND_PARAM_SIZE = 2_000_000;
// One byte above the D1 bind parameter limit, used to validate oversized
// parameter handling.
const OVERSIZED_BIND_PARAM_SIZE = MAX_BIND_PARAM_SIZE + 1;

/**
 * @typedef {Error & {
 *   code?: string,
 *   category?: string,
 *   retryable?: boolean,
 *   statementIndex?: number,
 *   causeCode?: string,
 * }} D1ErrorInstance
 * @typedef {{ sql: string, params: unknown[] }} D1Statement
 * @typedef {{ success: boolean, results?: unknown[] | { columns: string[], rows: unknown[][] }, meta?: Record<string, unknown> }} D1QueryResult
 * @typedef {{ count: number, duration: number }} D1ExecResult
 * @typedef {D1QueryResult | D1QueryResult[] | D1ExecResult} D1QueryResponse
 * @typedef {{ query(mode: string, statements: D1Statement[], requestId: string | null): Promise<D1QueryResponse> }} D1DatabaseTransport
 * @typedef {{ bind(...params: unknown[]): D1PreparedStatement, first(columnName?: string): Promise<unknown>, all(): Promise<D1QueryResult>, run(): Promise<D1QueryResponse>, raw(options?: { columnNames?: boolean }): Promise<unknown[][]> }} D1PreparedStatement
 * @typedef {{ prepare(sql: string): D1PreparedStatement, batch(statements: D1PreparedStatement[]): Promise<D1QueryResult[]>, exec(sql: string): Promise<D1ExecResult>, withSession(constraintOrBookmark?: unknown): D1DatabaseSession }} D1DatabaseInstance
 * @typedef {{ prepare(sql: string): D1PreparedStatement, batch(statements: D1PreparedStatement[]): Promise<D1QueryResult[]>, getBookmark(): string | null }} D1DatabaseSession
 * @typedef {new (transport: D1DatabaseTransport, options?: Record<string, unknown>) => D1DatabaseInstance} D1DatabaseConstructor
 * @typedef {new (message: string, options?: Record<string, unknown>) => D1ErrorInstance} D1ErrorConstructor
 */

/** @type {{ D1Error: D1ErrorConstructor, D1Database: D1DatabaseConstructor, splitExecStatements: (sql: string) => D1Statement[] }} */
const d1ClientModule = await importRepositoryModule("runtime/d1-client.js", [
  ...importSpecifierReplacements({
    "./_wdl-sql-splitter.js": repositoryFileUrl("shared/sql-splitter.js"),
    "./_wdl-d1-params.js": repositoryFileUrl("shared/d1-params.js"),
    "./_wdl-d1-transport.js": d1TransportDataUrl(),
    "./_wdl-request-id.js": repositoryFileUrl("runtime/_wdl-request-id.js"),
  }),
]);

const {
  D1Error,
  D1Database: LocalD1Database,
  splitExecStatements,
} = d1ClientModule;

/**
 * @param {((mode: string, statements: D1Statement[]) => D1QueryResponse | Promise<D1QueryResponse>) | null} [handler]
 * @param {Record<string, unknown>} [options]
 * @returns {{ db: D1DatabaseInstance, calls: Array<{ mode: string, statements: D1Statement[], requestId: string | null }> }}
 */
function makeLocalDb(handler, options = {}) {
  /** @type {Array<{ mode: string, statements: D1Statement[], requestId: string | null }>} */
  const calls = [];
  const db = new LocalD1Database({
    /** @param {string} mode @param {D1Statement[]} statements @param {string | null} requestId */
    async query(mode, statements, requestId) {
      calls.push({ mode, statements, requestId });
      return handler ? await handler(mode, statements) : {
        success: true,
        results: [{ ok: 1 }],
        meta: { duration: 0 },
      };
    },
  }, options);
  return { db, calls };
}

test("D1 local client: prepare().bind().all() sends normalized params to runtime stub", async () => {
  const { db, calls } = makeLocalDb();

  const result = await db.prepare("select ? as ok").bind(true).all();

  assert.deepEqual(result.results, [{ ok: 1 }]);
  assert.equal(calls[0].mode, "all");
  assert.deepEqual(calls[0].statements, [{ sql: "select ? as ok", params: [1] }]);
});

test("D1 local client: bind normalizes ArrayBuffer and typed-array blob params", async () => {
  const { db, calls } = makeLocalDb();
  const bytes = new Uint8Array([0, 1, 2, 255]);
  const source = new Uint8Array([10, 20, 30, 40]);

  await db.prepare("insert into blobs values (?, ?, ?)").bind(
    bytes.buffer,
    source.subarray(1, 3),
    [7, 8, 9]
  ).run();

  assert.equal(calls[0].mode, "run");
  assert.deepEqual(calls[0].statements, [{
    sql: "insert into blobs values (?, ?, ?)",
    params: [[0, 1, 2, 255], [20, 30], [7, 8, 9]],
  }]);
});

test("D1 local client: decodes tagged BLOB results for all() and raw()", async () => {
  const tagged = { __wdl_d1_binary_v1: true, base64: "AAEC/w==" };
  const { db } = makeLocalDb((mode) => {
    if (mode === "raw") {
      return {
        success: true,
        results: { columns: ["data"], rows: [[tagged]] },
        meta: { duration: 0 },
      };
    }
    return {
      success: true,
      results: [{ data: tagged }],
      meta: { duration: 0 },
    };
  });

  const allResult = await db.prepare("select data from blobs").all();
  const all = /** @type {{ results: Array<{ data: unknown }> }} */ (allResult);
  assert.ok(all.results[0].data instanceof Uint8Array);
  assert.deepEqual(Array.from(all.results[0].data), [0, 1, 2, 255]);

  const raw = await db.prepare("select data from blobs").raw();
  assert.ok(raw[0][0] instanceof Uint8Array);
  assert.deepEqual(Array.from(raw[0][0]), [0, 1, 2, 255]);
});

test("D1 local client: byte-array params reject fractional and out-of-range values", async () => {
  const { db, calls } = makeLocalDb();

  await db.prepare("select ?").bind([0, 255]).run();

  assert.equal(calls[0].mode, "run");
  assert.deepEqual(calls[0].statements, [{ sql: "select ?", params: [[0, 255]] }]);

  assert.throws(
    () => db.prepare("select ?").bind([1.5]).run(),
    (err) => err instanceof D1Error &&
      err.code === "invalid-parameter" &&
      err.category === "invalid-request" &&
      /^D1_ERROR \[invalid-parameter\]: D1_TYPE_ERROR:/.test(err.message) &&
      /Type 'object' not supported for value '1\.5'/.test(err.message)
  );
  assert.throws(
    () => db.prepare("select ?").bind([256]).run(),
    (err) => err instanceof D1Error &&
      err.code === "invalid-parameter" &&
      err.category === "invalid-request" &&
      /^D1_ERROR \[invalid-parameter\]: D1_TYPE_ERROR:/.test(err.message) &&
      /Type 'object' not supported for value '256'/.test(err.message)
  );
});

test("D1 local client: non-finite numbers throw stable invalid-parameter errors", async () => {
  const { db } = makeLocalDb();

  assert.throws(
    () => db.prepare("select ?").bind(NaN),
    (err) => err instanceof D1Error &&
      err.code === "invalid-parameter" &&
      err.category === "invalid-request" &&
      /D1_TYPE_ERROR/.test(err.message)
  );
});

test("D1 local client: oversized bind params throw stable D1 limit error", async () => {
  const { db } = makeLocalDb();

  assert.throws(
    () => db.prepare("select ?").bind("x".repeat(OVERSIZED_BIND_PARAM_SIZE)),
    (err) => err instanceof D1Error &&
      err.name === "D1_ERROR" &&
      err.code === "limit-exceeded" &&
      err.category === "limit" &&
      err.retryable === false &&
      /D1_ERROR \[limit-exceeded\]/.test(err.message)
  );
  assert.throws(
    () => db.prepare("select ?").bind(new Uint8Array(OVERSIZED_BIND_PARAM_SIZE)),
    (err) => err instanceof D1Error &&
      err.name === "D1_ERROR" &&
      err.code === "limit-exceeded" &&
      err.category === "limit" &&
      err.retryable === false &&
      /D1_ERROR \[limit-exceeded\]/.test(err.message)
  );
});

test("D1 local client: first(column) returns one column and throws for missing columns", async () => {
  const { db } = makeLocalDb(() => ({
    success: true,
    results: [{ n: 1 }],
    meta: { duration: 0 },
  }));

  assert.deepEqual(await db.prepare("select 1 as n").first(), { n: 1 });
  assert.equal(await db.prepare("select 1 as n").first("n"), 1);
  await assert.rejects(
    () => db.prepare("select 1 as n").first("missing"),
    (err) => err instanceof D1Error &&
      err.code === "column-not-found" &&
      err.category === "invalid-request" &&
      err.retryable === false &&
      /D1_COLUMN_NOTFOUND/.test(err.message)
  );
});

test("D1 local client: raw({ columnNames }) returns header row plus values", async () => {
  const { db } = makeLocalDb(() => ({
    success: true,
    results: { columns: ["id", "body"], rows: [["m1", "hello"]] },
    meta: { duration: 0 },
  }));

  assert.deepEqual(
    await db.prepare("select * from messages").raw({ columnNames: true }),
    [["id", "body"], ["m1", "hello"]]
  );
});

test("D1 local client: raw({ columnNames }) preserves empty result column names", async () => {
  const { db } = makeLocalDb(() => ({
    success: true,
    results: { columns: ["id", "body"], rows: [] },
    meta: { duration: 0 },
  }));

  assert.deepEqual(
    await db.prepare("select id, body from messages where id = ?").bind("missing").raw({ columnNames: true }),
    [["id", "body"]]
  );
});

test("D1 local client: batch accepts nested prepare().bind() statements without awaits", async () => {
  const { db, calls } = makeLocalDb(() => ([
    { success: true, results: [], meta: { changes: 1 } },
    { success: true, results: { columns: ["id"], rows: [["m1"]] }, meta: { rows_read: 1 } },
  ]));

  const results = await db.batch([
    db.prepare("insert into messages (id) values (?)").bind("m1"),
    db.prepare("select * from messages where id = ?").bind("m1"),
  ]);

  assert.equal(calls[0].mode, "batch");
  assert.deepEqual(calls[0].statements, [
    { sql: "insert into messages (id) values (?)", params: ["m1"] },
    { sql: "select * from messages where id = ?", params: ["m1"] },
  ]);
  assert.equal(results.length, 2);
  assert.deepEqual(results[1].results, [{ id: "m1" }]);
});

test("D1 local client: batch rejects statements from another database or session", async () => {
  const { db } = makeLocalDb();
  const { db: otherDb } = makeLocalDb();
  const session = db.withSession("first-primary");

  await assert.rejects(
    () => db.batch([otherDb.prepare("select 1").bind()]),
    (err) => err instanceof D1Error &&
      err.code === "invalid-batch" &&
      /same D1 database\/session/.test(err.message)
  );
  await assert.rejects(
    () => db.batch([session.prepare("select 1")]),
    (err) => err instanceof D1Error &&
      err.code === "invalid-batch" &&
      /same D1 database\/session/.test(err.message)
  );
});

test("D1 local client: exec returns D1ExecResult shape", async () => {
  const { db, calls } = makeLocalDb(() => ({ count: 2, duration: 3 }));

  const result = await db.exec("create table t (id text); insert into t values ('x')");

  assert.equal(calls[0].mode, "exec");
  assert.deepEqual(calls[0].statements, [
    { sql: "create table t (id text)", params: [] },
    { sql: "insert into t values ('x')", params: [] },
  ]);
  assert.deepEqual(result, { count: 2, duration: 3 });
});

test("D1 local client: exec splitter ignores semicolons inside strings and comments", () => {
  assert.deepEqual(
    splitExecStatements(`
      insert into messages (id, body) values ('semi', 'a;b');
      -- ignored ; in line comment
      insert into messages (id, body) values ("quoted;id", 'c');
      /* ignored ; in block comment */
      select [semi;column] from \`semi;table\`;
    `),
    [
      { sql: "insert into messages (id, body) values ('semi', 'a;b')", params: [] },
      {
        sql: "-- ignored ; in line comment\n      insert into messages (id, body) values (\"quoted;id\", 'c')",
        params: [],
      },
      {
        sql: "/* ignored ; in block comment */\n      select [semi;column] from `semi;table`",
        params: [],
      },
    ]
  );
});

test("D1 local client: exec splitter keeps CREATE TRIGGER bodies intact", () => {
  const sql = `
    create table source (id text);
    create table audit (id text);
    create trigger source_ai after insert on source
    begin
      insert into audit (id) values (new.id);
      update audit set id = id || ';seen' where id = new.id;
    end;
    insert into source values ('a');
  `;

  const statements = splitExecStatements(sql);
  assert.equal(statements.length, 4);
  assert.match(statements[2].sql, /^create trigger/i);
  assert.match(statements[2].sql, /insert into audit/);
  assert.match(statements[2].sql, /update audit/);
  assert.match(statements[3].sql, /^insert into source/);
});

test("D1 local client: exec splitter keeps CREATE TRIGGER bodies intact with trailing END comment", () => {
  const sql = `
    create table source (id text);
    create trigger source_ai after insert on source
    begin
      insert into source values ('a');
    end /* trailing comment */;
    insert into source values ('b');
  `;

  const statements = splitExecStatements(sql);
  assert.equal(statements.length, 3);
  assert.match(statements[1].sql, /^create trigger/i);
  assert.match(statements[2].sql, /^insert into source values \('b'\)/i);
});

test("D1 local client: withSession() is API-compatible without replica routing", async () => {
  const { db, calls } = makeLocalDb(() => ({
    success: true,
    results: [{ id: "s1" }],
    meta: { duration: 0 },
  }));

  const session = db.withSession("first-primary");
  assert.equal(session.getBookmark(), null);
  assert.deepEqual(await session.prepare("select ? as id").bind("s1").first(), { id: "s1" });
  assert.equal(calls[0].mode, "all");

  const resumed = db.withSession("bookmark-0001");
  assert.equal(resumed.getBookmark(), "bookmark-0001");
});

test("D1 local client: sends configured request id to query bridge", async () => {
  const { db, calls } = makeLocalDb(null, { requestId: "rid-d1" });

  await db.prepare("select 1").all();

  assert.equal(calls[0].requestId, "rid-d1");
});

test("D1 local client: resolves request id lazily for class entrypoint wrappers", async () => {
  let requestId = "rid-first";
  const { db, calls } = makeLocalDb(null, { requestIdProvider: () => requestId });

  await db.prepare("select 1").all();
  requestId = "rid-second";
  await db.prepare("select 2").all();

  assert.equal(calls[0].requestId, "rid-first");
  assert.equal(calls[1].requestId, "rid-second");
});

test("D1 local client: wraps backend errors with D1Error fields", async () => {
  const { db } = makeLocalDb(() => {
    throw Object.assign(new Error("D1_ERROR [batch-statement-error]: statement 1 failed"), {
      code: "batch-statement-error",
      category: "sql",
      retryable: false,
      statementIndex: 1,
      causeCode: "sql-error",
    });
  });

  await assert.rejects(
    () => db.prepare("select 1").all(),
    (err) => err instanceof D1Error &&
      err.name === "D1_ERROR" &&
      err.code === "batch-statement-error" &&
      err.category === "sql" &&
      err.retryable === false &&
      err.statementIndex === 1 &&
      err.causeCode === "sql-error"
  );
});
