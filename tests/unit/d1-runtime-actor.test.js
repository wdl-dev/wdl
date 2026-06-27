import { test } from "node:test";
import assert from "node:assert/strict";
import { loadD1Actor } from "../helpers/load-d1-actor.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";
import { delay } from "../helpers/timing.js";

const { D1DatabaseActor, resultFromCursor } = await loadD1Actor();

/**
 * @param {{ leaseRemainingMs?: number, guardMs?: number }} assertion
 * @param {() => Promise<void>} fn
 */
async function withD1ActorOwnerAssertion(assertion, fn) {
  await withMockedProperty(
    /** @type {Record<string, unknown>} */ (globalThis),
    "__d1ActorOwnerAssertion",
    assertion,
    fn
  );
}

test("D1 actor: run result format does not materialize cursor rows", () => {
  let rawCalled = false;
  const cursor = {
    columnNames: ["id"],
    raw() {
      rawCalled = true;
      return [["row-1"]];
    },
  };

  assert.deepEqual(resultFromCursor(cursor, "NONE"), []);
  assert.equal(rawCalled, false);
});

test("D1 actor: result materialization enforces a per-statement row cap", () => {
  const cursor = {
    columnNames: ["id"],
    raw() {
      return [["r1"], ["r2"]];
    },
  };

  assert.throws(
    () => resultFromCursor(cursor, "ROWS_AND_COLUMNS", 1),
    (err) => {
      assert.ok(err instanceof Error);
      const d1Err = /** @type {Error & { status?: number, code?: string }} */ (err);
      return d1Err.name === "D1ProtocolError" &&
        d1Err.status === 413 &&
        d1Err.code === "limit-exceeded" &&
        /maximum result rows per statement is 1/.test(d1Err.message);
    }
  );
});

test("D1 actor: result materialization enforces a request byte cap", () => {
  const cursor = {
    columnNames: ["body"],
    raw() {
      return [["0123456789"]];
    },
  };

  assert.throws(
    () => resultFromCursor(cursor, "ROWS_AND_COLUMNS", 10, 8),
    (err) => {
      assert.ok(err instanceof Error);
      const d1Err = /** @type {Error & { status?: number, code?: string }} */ (err);
      return d1Err.name === "D1ProtocolError" &&
        d1Err.status === 413 &&
        d1Err.code === "limit-exceeded" &&
        /maximum result bytes per request is 8/.test(d1Err.message);
    }
  );
});

test("D1 actor: result byte cap includes empty result column metadata", () => {
  const cursor = {
    columnNames: ["0123456789"],
    raw() {
      return [];
    },
  };

  assert.throws(
    () => resultFromCursor(cursor, "ROWS_AND_COLUMNS", 10, 8),
    (err) => {
      assert.ok(err instanceof Error);
      const d1Err = /** @type {Error & { status?: number, code?: string }} */ (err);
      return d1Err.name === "D1ProtocolError" &&
        d1Err.status === 413 &&
        d1Err.code === "limit-exceeded" &&
        /maximum result bytes per request is 8/.test(d1Err.message);
    }
  );
});

test("D1 actor: default result row cap matches the compatibility ceiling", () => {
  const cursor = {
    columnNames: ["id"],
    raw() {
      return Array.from({ length: 65_536 }, (_, idx) => [`r${idx}`]);
    },
  };

  assert.equal(resultFromCursor(cursor, "ROWS_AND_COLUMNS").rows.length, 65_536);
});

test("D1 actor: fetch maps aggregate result byte-cap overflow to limit-exceeded response", async () => {
  let calls = 0;
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 100;
        },
        exec() {
          calls += 1;
          return {
            columnNames: ["body"],
            raw: () => [[calls === 1 ? "12345" : "67890"]],
            rowsRead: 1,
            rowsWritten: 0,
          };
        },
      },
    },
  }, { D1_MAX_RESULT_BYTES: "17" });

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "all",
      owner: { taskId: "task-a" },
      statements: [
        { sql: "select body from messages", params: [] },
        { sql: "select body from messages", params: [] },
      ],
    }),
  }));

  await assertJsonResponse(response, 413, {
    success: false,
    error: "limit-exceeded",
    message: "D1 limit exceeded: maximum result bytes per request is 17",
  });
});

test("D1 actor: wait-until-idle waits for an in-flight query request", async () => {
  /** @type {() => void} */
  let releaseQueryRead = () => {};
  /** @type {() => void} */
  let markQueryReadStarted = () => {};
  const queryReadStarted = new Promise((resolve) => {
    markQueryReadStarted = () => resolve(undefined);
  });
  const queryReadGate = {
    started: markQueryReadStarted,
    promise: new Promise((resolve) => {
      releaseQueryRead = () => resolve(undefined);
    }),
  };

  await withMockedProperty(
    /** @type {Record<string, unknown>} */ (globalThis),
    "__d1ActorQueryReadGate",
    queryReadGate,
    async () => {
      const actor = new D1DatabaseActor({
        storage: {
          sql: {
            get databaseSize() {
              return 100;
            },
            exec() {
              return {
                columnNames: ["id"],
                raw: () => [[1]],
                rowsRead: 1,
                rowsWritten: 0,
              };
            },
          },
        },
      }, {});

      const queryResponsePromise = actor.fetch(new Request("http://d1-actor/query", {
        method: "POST",
        body: JSON.stringify({
          mode: "run",
          owner: { taskId: "task-a" },
          statements: [{ sql: "select 1", params: [] }],
        }),
      }));
      await queryReadStarted;

      const idleResponsePromise = actor.fetch(new Request("http://d1-actor/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ __control: "wait-until-idle" }),
      }));

      try {
        const earlyResult = await Promise.race([
          idleResponsePromise.then(() => "resolved"),
          delay(20).then(() => "pending"),
        ]);
        assert.equal(earlyResult, "pending");
      } finally {
        releaseQueryRead();
      }

      assert.equal((await queryResponsePromise).status, 200);
      await assertJsonResponse(await idleResponsePromise, 200, { idle: true });
    }
  );
});

test("D1 actor: wait-until-idle uses actor-specific timeout env", async () => {
  /** @type {() => void} */
  let releaseQueryRead = () => {};
  /** @type {() => void} */
  let markQueryReadStarted = () => {};
  const queryReadStarted = new Promise((resolve) => {
    markQueryReadStarted = () => resolve(undefined);
  });
  const queryReadGate = {
    started: markQueryReadStarted,
    promise: new Promise((resolve) => {
      releaseQueryRead = () => resolve(undefined);
    }),
  };

  await withMockedProperty(
    /** @type {Record<string, unknown>} */ (globalThis),
    "__d1ActorQueryReadGate",
    queryReadGate,
    async () => {
      const actor = new D1DatabaseActor({
        storage: {
          sql: {
            get databaseSize() {
              return 100;
            },
            exec() {
              return {
                columnNames: ["id"],
                raw: () => [[1]],
                rowsRead: 1,
                rowsWritten: 0,
              };
            },
          },
        },
      }, {
        D1_ACTOR_IDLE_WAIT_TIMEOUT_MS: "1",
        D1_DRAIN_TIMEOUT_MS: "100000",
      });

      const queryResponsePromise = actor.fetch(new Request("http://d1-actor/query", {
        method: "POST",
        body: JSON.stringify({
          mode: "run",
          owner: { taskId: "task-a" },
          statements: [{ sql: "select 1", params: [] }],
        }),
      }));
      await queryReadStarted;

      try {
        const idleResponse = await actor.fetch(new Request("http://d1-actor/query", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ __control: "wait-until-idle" }),
        }));

        await assertJsonResponse(idleResponse, 503, {
          success: false,
          error: "drain-timeout",
          message: "D1 actor timed out waiting for 1 in-flight query(s)",
        });
      } finally {
        releaseQueryRead();
      }
      assert.equal((await queryResponsePromise).status, 200);
    }
  );
});

test("D1 actor: batch preserves result byte-cap overflow as limit-exceeded", async () => {
  let calls = 0;
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 100;
        },
        exec() {
          calls += 1;
          return {
            columnNames: ["body"],
            raw: () => [[calls === 1 ? "12345" : "67890"]],
            rowsRead: 1,
            rowsWritten: 0,
          };
        },
      },
      transactionSync(/** @type {() => unknown} */ callback) {
        return callback();
      },
    },
  }, { D1_MAX_RESULT_BYTES: "17" });

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "batch",
      owner: { taskId: "task-a" },
      statements: [
        { sql: "select body from messages", params: [] },
        { sql: "select body from messages", params: [] },
      ],
    }),
  }));

  await assertJsonResponse(response, 413, {
    success: false,
    error: "limit-exceeded",
    message: "D1 limit exceeded: maximum result bytes per request is 17",
  });
});

test("D1 actor: fetch maps result row-cap overflow to limit-exceeded response", async () => {
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 100;
        },
        exec() {
          return {
            columnNames: ["id"],
            raw: () => [["r1"], ["r2"]],
            rowsRead: 2,
            rowsWritten: 0,
          };
        },
      },
    },
  }, { D1_MAX_RESULT_ROWS: "1" });

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "all",
      owner: { taskId: "task-a" },
      statements: [{ sql: "select id from messages", params: [] }],
    }),
  }));

  await assertJsonResponse(response, 413, {
    success: false,
    error: "limit-exceeded",
    message: "D1 limit exceeded: maximum result rows per statement is 1",
  });
});

test("D1 actor: all-mode fetch returns row/column results without objectifying rows", async () => {
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 100;
        },
        exec() {
          return {
            columnNames: ["id", "body"],
            raw: () => [["m1", "hello"]],
            rowsRead: 1,
            rowsWritten: 0,
          };
        },
      },
    },
  }, {});

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "all",
      owner: { taskId: "task-a" },
      statements: [{ sql: "select id, body from messages", params: [] }],
    }),
  }));

  const body = await readJsonResponse(response, 200);
  assert.deepEqual(body.results, { columns: ["id", "body"], rows: [["m1", "hello"]] });
  assert.equal(typeof body.meta.duration, "number");
  assert.equal(body.meta.timings.sql_duration_ms, body.meta.duration);
  assert.deepEqual({
    ...body,
    meta: {
      ...body.meta,
      duration: 0,
      timings: { sql_duration_ms: 0 },
    },
  }, {
    success: true,
    results: { columns: ["id", "body"], rows: [["m1", "hello"]] },
    meta: {
      duration: 0,
      served_by: "d1-5830a8b6",
      served_by_region: "local",
      served_by_primary: true,
      timings: { sql_duration_ms: 0 },
      changes: 0,
      last_row_id: 0,
      changed_db: false,
      size_after: 100,
      rows_read: 1,
      rows_written: 0,
      total_attempts: 1,
    },
  });
});

test("D1 actor: served_by is a stable redacted label, never the raw task ARN", async () => {
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 100;
        },
        exec() {
          return { columnNames: ["id"], raw: () => [["m1"]], rowsRead: 1, rowsWritten: 0 };
        },
      },
    },
  }, {});

  const taskArn = "arn:aws:ecs:example-region-1:123456789012:task/example-cluster/task-abcdef";
  async function queryServedBy() {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "all",
        owner: { taskId: taskArn },
        statements: [{ sql: "select id from messages", params: [] }],
      }),
    }));
    return (await readJsonResponse(response, 200)).meta.served_by;
  }

  const first = await queryServedBy();
  const second = await queryServedBy();

  assert.match(first, /^d1-[0-9a-f]{8}$/);
  assert.equal(second, first);
  assert.doesNotMatch(first, /arn:|123456789012|example-region-1|example-cluster|task-abcdef/);
});

test("D1 actor: read statement avoids changes/last-row probes", () => {
  /** @type {Array<{ sql: string, params: unknown[] }>} */
  const calls = [];
  let databaseSizeReads = 0;
  const actor = Object.create(D1DatabaseActor.prototype);
  actor.sql = {
    get databaseSize() {
      databaseSizeReads += 1;
      return 100;
    },
    exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
      calls.push({ sql, params });
      return {
        columnNames: ["id"],
        raw: () => [[1]],
        rowsRead: 1,
        rowsWritten: 0,
      };
    },
  };

  const result = actor.runStatement({ sql: "select id from t where id = ?", params: [1] }, "ARRAY_OF_OBJECTS", {
    taskId: "task-a",
  });

  assert.equal(calls.length, 1);
  assert.equal(databaseSizeReads, 1);
  assert.equal(calls[0].sql, "select id from t where id = ?");
  assert.equal(result.meta.changes, 0);
  assert.equal(result.meta.last_row_id, 0);
  assert.equal(result.meta.changed_db, false);
});

test("D1 actor: write statement uses rowsWritten and only probes last row id once", () => {
  /** @type {Array<{ sql: string, params: unknown[] }>} */
  const calls = [];
  let databaseSizeReads = 0;
  const actor = Object.create(D1DatabaseActor.prototype);
  actor.sql = {
    get databaseSize() {
      databaseSizeReads += 1;
      return 100;
    },
    exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
      calls.push({ sql, params });
      if (sql === "SELECT last_insert_rowid() as last_row_id") {
        return { toArray: () => [{ last_row_id: 42 }] };
      }
      return {
        columnNames: [],
        raw: () => [],
        rowsRead: 0,
        rowsWritten: 1,
      };
    },
  };

  const result = actor.runStatement({ sql: "insert into t(name) values (?)", params: ["a"] }, "NONE", {
    taskId: "task-a",
  });

  assert.deepEqual(calls.map((call) => call.sql), [
    "insert into t(name) values (?)",
    "SELECT last_insert_rowid() as last_row_id",
  ]);
  assert.equal(databaseSizeReads, 1);
  assert.equal(result.meta.changes, 1);
  assert.equal(result.meta.last_row_id, 42);
  assert.equal(result.meta.changed_db, true);
});

test("D1 actor: write-looking SQL marks changed_db without a size-before probe", () => {
  for (const sql of [
    "create table messages (id text)",
    "pragma user_version = 1",
    "analyze",
  ]) {
    let databaseSizeReads = 0;
    const actor = Object.create(D1DatabaseActor.prototype);
    actor.sql = {
      get databaseSize() {
        databaseSizeReads += 1;
        return 4096;
      },
      exec() {
        return {
          columnNames: [],
          raw: () => [],
          rowsRead: 0,
          rowsWritten: 0,
        };
      },
    };

    const result = actor.runStatement({ sql, params: [] }, "ARRAY_OF_OBJECTS", {
      taskId: "task-a",
    });

    assert.equal(databaseSizeReads, 1);
    assert.equal(result.meta.changed_db, true);
    assert.equal(result.meta.size_after, 4096);
  }
});

test("D1 actor: idempotent schema DDL only marks changed_db when sqlite_master changes", () => {
  const cases = [
    { name: "first create", before: false, after: true, changed: true },
    { name: "already exists", before: true, after: true, changed: false },
  ];

  for (const item of cases) {
    /** @type {Array<{ sql: string, params: unknown[] }>} */
    const calls = [];
    let objectReads = 0;
    const actor = Object.create(D1DatabaseActor.prototype);
    actor.sql = {
      databaseSize: 4096,
      exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
        calls.push({ sql, params });
        if (/^select 1 as found from sqlite_master/i.test(sql)) {
          objectReads += 1;
          return { toArray: () => (objectReads === 1 ? item.before : item.after) ? [{ found: 1 }] : [] };
        }
        return {
          columnNames: [],
          raw: () => [],
          rowsRead: 0,
          rowsWritten: 0,
        };
      },
    };

    const result = actor.runStatement({
      sql: "create table if not exists inspections (id text primary key)",
      params: [],
    }, "ARRAY_OF_OBJECTS", { taskId: "task-a" });

    const expectedCalls = [
      "select 1 as found from sqlite_master where type = ? and name = ? limit 1",
      "create table if not exists inspections (id text primary key)",
    ];
    if (!item.before) {
      expectedCalls.push("select 1 as found from sqlite_master where type = ? and name = ? limit 1");
    }
    assert.deepEqual(calls.map((call) => call.sql), expectedCalls, item.name);
    assert.deepEqual(calls[0].params, ["table", "inspections"]);
    assert.equal(result.meta.changed_db, item.changed, item.name);
  }
});

test("D1 actor: memoizes existing idempotent schema objects until broad schema mutation", () => {
  const actor = Object.create(D1DatabaseActor.prototype);
  let exists = true;
  let objectReads = 0;
  actor.env = {};
  actor.sql = {
    databaseSize: 4096,
    exec(/** @type {string} */ sql) {
      if (/^select 1 as found from sqlite_master/i.test(sql)) {
        objectReads += 1;
        return { toArray: () => exists ? [{ found: 1 }] : [] };
      }
      if (/^drop table/i.test(sql)) {
        exists = false;
      } else if (/^create table if not exists/i.test(sql)) {
        exists = true;
      }
      return {
        columnNames: [],
        raw: () => [],
        rowsRead: 0,
        rowsWritten: 0,
      };
    },
  };

  const owner = { taskId: "task-a" };
  const create = { sql: "create table if not exists inspections (id text primary key)", params: [] };
  const firstNoop = actor.runStatement(create, "ARRAY_OF_OBJECTS", owner, { remaining: 1024, limit: 1024 });
  const secondNoop = actor.runStatement(create, "ARRAY_OF_OBJECTS", owner, { remaining: 1024, limit: 1024 });
  assert.equal(firstNoop.meta.changed_db, false);
  assert.equal(secondNoop.meta.changed_db, false);
  assert.equal(objectReads, 1, "known-existing schema object should be memoized");

  const drop = actor.runStatement({ sql: "drop table inspections", params: [] }, "ARRAY_OF_OBJECTS", owner, { remaining: 1024, limit: 1024 });
  assert.equal(drop.meta.changed_db, true);
  const recreate = actor.runStatement(create, "ARRAY_OF_OBJECTS", owner, { remaining: 1024, limit: 1024 });
  assert.equal(recreate.meta.changed_db, true);
  assert.equal(objectReads, 3, "broad schema mutation should clear the memo before the next create");
});

test("D1 actor: exec broad schema mutation clears memo after earlier changed statement", () => {
  const actor = Object.create(D1DatabaseActor.prototype);
  let exists = true;
  let objectReads = 0;
  actor.schemaObjectExistsMemo = new Set(["table\0inspections"]);
  actor.sql = {
    databaseSize: 4096,
    exec(/** @type {string} */ sql) {
      if (/^select 1 as found from sqlite_master/i.test(sql)) {
        objectReads += 1;
        return { toArray: () => exists ? [{ found: 1 }] : [] };
      }
      if (/^drop table/i.test(sql)) {
        exists = false;
      } else if (/^create table if not exists/i.test(sql)) {
        exists = true;
      }
      return {
        columnNames: [],
        raw: () => [],
        rowsRead: 0,
        rowsWritten: 0,
      };
    },
  };

  const result = actor.execStatements([
    { sql: "insert into audit(id) values ('x')", params: [] },
    { sql: "drop table inspections", params: [] },
    { sql: "create table if not exists inspections (id text primary key)", params: [] },
  ], { dbKey: "tenant-a:main" });

  assert.equal(result.changedDb, true);
  assert.equal(objectReads, 2, "recreate must re-check sqlite_master after broad schema mutation");
  assert.equal(actor.schemaObjectExistsMemo.has("table\0inspections"), true);
});

test("D1 actor: exec reports changed_db from idempotent schema DDL object existence", () => {
  const actor = Object.create(D1DatabaseActor.prototype);
  actor.sql = {
    databaseSize: 8192,
    exec(/** @type {string} */ sql) {
      if (/^select 1 as found from sqlite_master/i.test(sql)) {
        return { toArray: () => [{ found: 1 }] };
      }
      return {};
    },
  };

  const result = actor.execStatements([
    { sql: "create index if not exists idx_inspections_created on inspections(created_at)", params: [] },
  ], { dbKey: "tenant-a:main" });

  assert.equal(result.changedDb, false);
  assert.equal(result.payload.count, 1);
  assert.equal(typeof result.payload.duration, "number");
});

test("D1 actor: exec multi-statement failure rolls back earlier writes", async () => {
  /** @type {Array<{ id: string, body: string }>} */
  let rows = [];
  let transactions = 0;
  const actor = new D1DatabaseActor({
    storage: {
      transactionSync(/** @type {() => unknown} */ callback) {
        transactions += 1;
        const snapshot = rows.map((row) => ({ ...row }));
        try {
          return callback();
        } catch (err) {
          rows = snapshot;
          throw err;
        }
      },
      sql: {
        get databaseSize() {
          return 4096 + rows.length;
        },
        exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
          if (/^insert into messages\b/i.test(sql)) {
            rows.push({ id: String(params[0]), body: String(params[1]) });
            return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 1 };
          }
          if (/^insert into missing_table\b/i.test(sql)) {
            throw new Error("no such table: missing_table");
          }
          if (/^SELECT last_insert_rowid/i.test(sql)) {
            return { toArray: () => [{ last_row_id: rows.length }] };
          }
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {});

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "exec",
      owner: { taskId: "task-a" },
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: ["m1", "before-failure"] },
        { sql: "insert into missing_table (id) values (?)", params: ["boom"] },
      ],
    }),
  }));

  assert.equal(response.status, 500);
  assert.equal(transactions, 1);
  assert.deepEqual(rows, []);
});

test("D1 actor: lease budget guard stops the next exec statement and rolls back", async () => {
  /** @type {Array<{ id: string, body: string }>} */
  let rows = [];
  /** @type {string[]} */
  const calls = [];
  let transactions = 0;
  const nowValues = [0, 10, 60];
  const actor = new D1DatabaseActor({
    storage: {
      transactionSync(/** @type {() => unknown} */ callback) {
        transactions += 1;
        const snapshot = rows.map((row) => ({ ...row }));
        try {
          return callback();
        } catch (err) {
          rows = snapshot;
          throw err;
        }
      },
      sql: {
        get databaseSize() {
          return 4096 + rows.length;
        },
        exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
          calls.push(sql);
          rows.push({ id: String(params[0]), body: String(params[1]) });
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 1 };
        },
      },
    },
  }, {
    D1_TEST_HOOKS: "1",
    __d1ActorNowMs: () => nowValues.shift() ?? 60,
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 100, guardMs: 50 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "exec",
        owner: { taskId: "task-a" },
        statements: [
          { sql: "insert into messages (id, body) values (?, ?)", params: ["m1", "before-deadline"] },
          { sql: "insert into messages (id, body) values (?, ?)", params: ["m2", "past-deadline"] },
        ],
      }),
    }));

    await assertJsonResponse(response, 503, {
      success: false,
      error: "lease-budget-exhausted",
      message: "D1 database owner lease budget was exhausted during local SQL dispatch",
    });
  });

  assert.equal(transactions, 1);
  assert.deepEqual(calls, ["insert into messages (id, body) values (?, ?)"]);
  assert.deepEqual(rows, []);
});

test("D1 actor: lease budget guard allows statements before the local deadline", async () => {
  /** @type {string[]} */
  const calls = [];
  const nowValues = [0, 10, 20];
  const actor = new D1DatabaseActor({
    storage: {
      transactionSync(/** @type {() => unknown} */ callback) {
        return callback();
      },
      sql: {
        get databaseSize() {
          return 4096;
        },
        exec(/** @type {string} */ sql) {
          calls.push(sql);
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {
    D1_TEST_HOOKS: "1",
    __d1ActorNowMs: () => nowValues.shift() ?? 20,
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 100, guardMs: 50 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "exec",
        owner: { taskId: "task-a" },
        statements: [
          { sql: "select 1", params: [] },
          { sql: "select 2", params: [] },
        ],
      }),
    }));

    assert.equal(response.status, 200);
  });

  assert.deepEqual(calls, ["select 1", "select 2"]);
});

test("D1 actor: test clock hook is ignored unless D1_TEST_HOOKS is enabled", async () => {
  let calls = 0;
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 4096;
        },
        exec() {
          calls += 1;
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {
    __d1ActorNowMs: () => {
      throw new Error("ungated test clock hook used");
    },
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 1000, guardMs: 0 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "all",
        owner: { taskId: "task-a" },
        statements: [{ sql: "select id from messages", params: [] }],
      }),
    }));

    assert.equal(response.status, 200);
  });

  assert.equal(calls, 1);
});

test("D1 actor: lease budget guard prevents a single statement past the local deadline", async () => {
  let calls = 0;
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 4096;
        },
        exec() {
          calls += 1;
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {
    D1_TEST_HOOKS: "1",
    __d1ActorNowMs: () => 0,
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 1000, guardMs: 1000 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "all",
        owner: { taskId: "task-a" },
        statements: [{ sql: "select id from messages", params: [] }],
      }),
    }));

    await assertJsonResponse(response, 503, {
      success: false,
      error: "lease-budget-exhausted",
      message: "D1 database owner lease budget was exhausted during local SQL dispatch",
    });
  });

  assert.equal(calls, 0);
});

test("D1 actor: lease budget guard does not retry-wrap committed single-statement metadata probes", async () => {
  /** @type {Array<{ id: string, body: string }>} */
  const rows = [];
  /** @type {string[]} */
  const calls = [];
  const nowValues = [0, 10, 60];
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return 4096 + rows.length;
        },
        exec(/** @type {string} */ sql, /** @type {unknown[]} */ ...params) {
          calls.push(sql);
          if (sql === "SELECT last_insert_rowid() as last_row_id") {
            return { toArray: () => [{ last_row_id: rows.length }] };
          }
          rows.push({ id: String(params[0]), body: String(params[1]) });
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 1 };
        },
      },
    },
  }, {
    D1_TEST_HOOKS: "1",
    __d1ActorNowMs: () => nowValues.shift() ?? 60,
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 100, guardMs: 50 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "run",
        owner: { taskId: "task-a" },
        statements: [
          { sql: "insert into messages (id, body) values (?, ?)", params: ["m1", "committed"] },
        ],
      }),
    }));

    const body = await readJsonResponse(response, 200);
    assert.equal(body.success, true);
    assert.equal(body.meta.last_row_id, 1);
    assert.equal(body.meta.changed_db, true);
  });

  assert.deepEqual(calls, [
    "insert into messages (id, body) values (?, ?)",
    "SELECT last_insert_rowid() as last_row_id",
  ]);
  assert.deepEqual(rows, [{ id: "m1", body: "committed" }]);
});

test("D1 actor: lease budget guard does not retry-wrap committed single-statement DDL probes", async () => {
  /** @type {string[]} */
  const calls = [];
  let exists = false;
  const nowValues = [0, 10, 20, 60];
  const actor = new D1DatabaseActor({
    storage: {
      sql: {
        get databaseSize() {
          return exists ? 4097 : 4096;
        },
        exec(/** @type {string} */ sql) {
          calls.push(sql);
          if (/^select 1 as found from sqlite_master/i.test(sql)) {
            return { toArray: () => exists ? [{ found: 1 }] : [] };
          }
          if (/^create table if not exists inspections\b/i.test(sql)) {
            exists = true;
          }
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {
    D1_TEST_HOOKS: "1",
    __d1ActorNowMs: () => nowValues.shift() ?? 60,
  });

  await withD1ActorOwnerAssertion({ leaseRemainingMs: 100, guardMs: 50 }, async () => {
    const response = await actor.fetch(new Request("http://d1-actor/query", {
      method: "POST",
      body: JSON.stringify({
        mode: "exec",
        owner: { taskId: "task-a" },
        statements: [
          { sql: "create table if not exists inspections (id text primary key)", params: [] },
        ],
      }),
    }));

    const body = await readJsonResponse(response, 200);
    assert.equal(response.headers.get("x-wdl-d1-changed-db"), "1");
    assert.equal(body.count, 1);
    assert.equal(typeof body.duration, "number");
  });

  assert.deepEqual(calls, [
    "select 1 as found from sqlite_master where type = ? and name = ? limit 1",
    "create table if not exists inspections (id text primary key)",
    "select 1 as found from sqlite_master where type = ? and name = ? limit 1",
  ]);
  assert.equal(exists, true);
});

test("D1 actor: failed transaction restores schema object memo", async () => {
  let exists = false;
  let objectReads = 0;
  const actor = new D1DatabaseActor({
    storage: {
      transactionSync(/** @type {() => unknown} */ callback) {
        const existsBefore = exists;
        try {
          return callback();
        } catch (err) {
          exists = existsBefore;
          throw err;
        }
      },
      sql: {
        databaseSize: 4096,
        exec(/** @type {string} */ sql) {
          if (/^select 1 as found from sqlite_master/i.test(sql)) {
            objectReads += 1;
            return { toArray: () => exists ? [{ found: 1 }] : [] };
          }
          if (/^create table if not exists inspections\b/i.test(sql)) {
            exists = true;
            return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
          }
          if (/^insert into missing_table\b/i.test(sql)) {
            throw new Error("no such table: missing_table");
          }
          return { columnNames: [], raw: () => [], rowsRead: 0, rowsWritten: 0 };
        },
      },
    },
  }, {});
  const create = { sql: "create table if not exists inspections (id text primary key)", params: [] };

  const response = await actor.fetch(new Request("http://d1-actor/query", {
    method: "POST",
    body: JSON.stringify({
      mode: "exec",
      owner: { taskId: "task-a" },
      statements: [
        create,
        { sql: "insert into missing_table (id) values ('boom')", params: [] },
      ],
    }),
  }));

  assert.equal(response.status, 500);
  assert.equal(exists, false);
  assert.equal(actor.schemaObjectExistsMemo.has("table\0inspections"), false);

  const result = actor.runStatement(create, "ARRAY_OF_OBJECTS", { taskId: "task-a" }, { remaining: 1024, limit: 1024 });
  assert.equal(result.meta.changed_db, true);
  assert.equal(objectReads, 4, "failed transaction memo must not hide the real create");
  assert.equal(actor.schemaObjectExistsMemo.has("table\0inspections"), true);
});
