import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";

import {
  D1ReadCache,
  isReadCacheableQuery,
  parseIdempotentSchemaDdl,
  payloadChangedDb,
  statementMayBeIdempotentSchemaDdl,
  statementMayChangeDb,
} from "../../d1-runtime/read-cache.js";

const env = { D1_READ_CACHE_TTL_MS: "1000", D1_READ_CACHE_MAX_ENTRIES: "2" };
const owner = { taskId: "task-a", dbKey: "tenant-a:d1_main", generation: 7 };
const textEncoder = new TextEncoder();

/** @param {unknown} value */
function wireBytes(value) {
  return textEncoder.encode(JSON.stringify(value));
}

/** @type {Array<{ name: string, labels: any }>} */
let observed;
const metrics = {
  /** @param {string} name @param {any} labels */
  increment(name, labels) {
    observed.push({ name, labels });
  },
};

/**
 * @param {string} sql
 * @param {Partial<{ dbKey: string, mode: string, statements: Array<{ sql: string, params?: unknown[] }> }>} [overrides]
 */
function query(sql, overrides = {}) {
  return {
    dbKey: "tenant-a:d1_main",
    mode: "all",
    statements: [{ sql, params: [] }],
    ...overrides,
  };
}

/** @param {any} [testEnv] */
function cache(testEnv) {
  testEnv ||= env;
  return new D1ReadCache(testEnv, metrics, { service: "d1-runtime" });
}

beforeEach(() => {
  observed = [];
});

test("D1 read cache: only conservative single-statement reads are cacheable", () => {
  assert.equal(isReadCacheableQuery(query("select * from messages where id = ?"), env), true);
  assert.equal(isReadCacheableQuery(query("with recent as (select * from messages) select * from recent"), env), true);
  assert.equal(isReadCacheableQuery(query("select random()"), env), false);
  assert.equal(isReadCacheableQuery(query("select current_timestamp"), env), false);
  assert.equal(isReadCacheableQuery(query("update messages set body = 'x'"), env), false);
  assert.equal(isReadCacheableQuery(query("begin")), false);
  assert.equal(isReadCacheableQuery(query("rollback")), false);
  assert.equal(isReadCacheableQuery(query("savepoint s1")), false);
  assert.equal(isReadCacheableQuery(query("select 1", { mode: "run" }), env), false);
  assert.equal(isReadCacheableQuery(query("select 1", { statements: [{ sql: "select 1" }, { sql: "select 2" }] }), env), false);
  assert.equal(isReadCacheableQuery(query("select 1"), { ...env, D1_READ_CACHE_TTL_MS: "0" }), false);
  assert.equal(isReadCacheableQuery(query("select 1"), { ...env, D1_READ_CACHE_MAX_ENTRIES: "0" }), false);
});

test("D1 read cache: write-looking SQL classifier is shared with actor changed_db", () => {
  assert.equal(statementMayChangeDb("create table messages (id text)"), true);
  assert.equal(statementMayChangeDb("pragma user_version = 1"), true);
  assert.equal(statementMayChangeDb("select * from messages"), false);
});

test("D1 read cache: identifies narrow idempotent schema DDL for delayed invalidation", () => {
  assert.equal(statementMayBeIdempotentSchemaDdl("create table if not exists inspections (id text)"), true);
  assert.equal(statementMayBeIdempotentSchemaDdl("create table if not exists inspections (id text);"), true);
  assert.equal(statementMayBeIdempotentSchemaDdl("create unique index if not exists idx_i on inspections(id)"), true);
  assert.equal(statementMayBeIdempotentSchemaDdl("create table inspections (id text)"), false);
  assert.equal(statementMayBeIdempotentSchemaDdl("create temp table if not exists inspections (id text)"), false);
  assert.equal(
    statementMayBeIdempotentSchemaDdl(
      "create table if not exists inspections (id text); insert into inspections values ('i1')"
    ),
    false
  );
  assert.equal(statementMayBeIdempotentSchemaDdl("select 'create table if not exists x'"), false);
  assert.deepEqual(parseIdempotentSchemaDdl('create table if not exists "inspection rows" (id text)'), {
    type: "table",
    name: "inspection rows",
  });
});

test("D1 read cache: stores by router cache instance and owner generation", () => {
  const c = cache();
  const q = query("select body from messages where id = ?", {
    statements: [{ sql: "select body from messages where id = ?", params: ["m1"] }],
  });
  const payload = { success: true, results: [{ body: "hello" }], meta: { changed_db: false } };
  const bytes = wireBytes(payload);

  const miss = c.beginRead(q, owner);
  assert.equal(miss.hit, false);
  assert.equal(c.finishRead(miss.token, bytes), true);
  assert.deepEqual(c.beginRead(q, owner).bytes, bytes);
  assert.equal(c.beginRead(q, { ...owner, generation: 8 }).hit, false);
});

test("D1 read cache: keys include database identity", () => {
  const c = cache();
  const q = query("select body from messages where id = ?", {
    statements: [{ sql: "select body from messages where id = ?", params: ["m1"] }],
  });
  const read = c.beginRead(q, owner);
  assert.equal(c.finishRead(read.token, wireBytes({ success: true, results: [{ body: "db-a" }], meta: {} })), true);

  const otherDb = c.beginRead({ ...q, dbKey: "tenant-b:d1_main" }, { ...owner, dbKey: "tenant-b:d1_main" });

  assert.equal(otherDb.hit, false);
});

test("D1 read cache: non-cacheable reads record bypass", () => {
  const c = cache();

  const read = c.beginRead(query("select random()"), owner);

  assert.equal(read.hit, false);
  assert.equal(read.token, null);
  assert.deepEqual(observed, [
    { name: "d1_read_cache", labels: { service: "d1-runtime", outcome: "bypass" } },
  ]);
});

test("D1 read cache: evicts oldest entry per cache instance", () => {
  const c = cache();
  for (const id of ["a", "b", "c"]) {
    const q = query(`select * from messages where id = '${id}'`);
    const read = c.beginRead(q, owner);
    c.finishRead(read.token, wireBytes({ success: true, results: [id], meta: {} }));
  }

  assert.equal(c.beginRead(query("select * from messages where id = 'a'"), owner).hit, false);
  assert.deepEqual(
    c.beginRead(query("select * from messages where id = 'b'"), owner).bytes,
    wireBytes({ success: true, results: ["b"], meta: {} })
  );
  assert.deepEqual(
    c.beginRead(query("select * from messages where id = 'c'"), owner).bytes,
    wireBytes({ success: true, results: ["c"], meta: {} })
  );
});

test("D1 read cache: expired entries are purged before lookup", () => {
  const c = cache({ ttlMs: 1000, maxEntries: 2 });
  const q = query("select * from messages where id = 'expired'");
  const read = c.beginRead(q, owner);
  /** @type {any} */ (read.token).expiresAt = 0;
  assert.equal(c.finishRead(read.token, wireBytes({ success: true, results: ["expired"], meta: {} })), true);

  const reread = c.beginRead(q, owner);

  assert.equal(reread.hit, false);
  assert.deepEqual(observed.map((entry) => entry.labels.outcome), ["miss", "store", "miss"]);
});

test("D1 read cache: mutation version prevents stale read store after write", () => {
  const c = cache();
  const q = query("select * from messages");
  const read = c.beginRead(q, owner);
  c.invalidate("write");
  assert.equal(c.finishRead(read.token, wireBytes({ success: true, results: [], meta: {} })), false);
  assert.equal(c.beginRead(q, owner).hit, false);
});

test("D1 read cache: invalidation only records a reason when entries existed", () => {
  const c = cache();
  c.invalidate("write");

  const read = c.beginRead(query("select * from messages"), owner);
  assert.equal(c.finishRead(read.token, wireBytes({ success: true, results: [], meta: {} })), true);
  c.invalidate("changed-db");

  assert.deepEqual(
    observed.filter((entry) => entry.name === "d1_read_cache_invalidations"),
    [
      {
        name: "d1_read_cache_invalidations",
        labels: { service: "d1-runtime", reason: "changed-db" },
      },
    ]
  );
});

test("D1 read cache: changed-db classification handles single and batch payloads", () => {
  const payload = { success: true, results: [], meta: { changed_db: true } };

  assert.equal(payloadChangedDb(payload), true);
  assert.equal(payloadChangedDb([payload]), true);
  assert.equal(payloadChangedDb({ success: true, meta: { changed_db: false } }), false);
});

test("D1 read cache: stores opaque query wire bytes", () => {
  const c = cache();
  const q = query("select body from messages where id = 'm1'");
  const bytes = Uint8Array.from([0x48, 0x01, 0x3a, 0x03, 0xff, 0x00, 0x7f]);
  const read = c.beginRead(q, owner);
  assert.equal(c.finishRead(read.token, bytes), true);

  const hit = c.beginRead(q, owner);
  assert.equal(hit.hit, true);
  assert.deepEqual(hit.bytes, bytes);
});
