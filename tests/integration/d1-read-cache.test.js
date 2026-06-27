import { after, test } from "node:test";
import assert from "node:assert/strict";
import {
  d1RuntimeProbe,
  d1RuntimeQuery,
  d1RuntimeQueryRaw,
  d1RuntimeRebalance,
  dbKeyOf,
  recreateD1MultiTasks,
  restoreD1SingleRuntime,
} from "./helpers/d1-runtime.js";
import { prometheusCounter } from "./helpers/prometheus.js";
import { assertNotStatus, assertStatus, serviceInternalGet, uniqueNs, setupIntegrationSuite } from "./helpers/index.js";

let usedD1MultiRuntime = false;

setupIntegrationSuite();

after(() => {
  if (usedD1MultiRuntime) restoreD1SingleRuntime();
});

function useD1MultiRuntime() {
  usedD1MultiRuntime = true;
  recreateD1MultiTasks();
}

/** @param {string} body @param {string} outcome */
function readCacheMetric(body, outcome) {
  return prometheusCounter(body, "wdl_d1_read_cache_total", {
    outcome,
    service: "d1-runtime",
  });
}

/** @param {string} body @param {string} reason */
function readCacheInvalidationMetric(body, reason) {
  return prometheusCounter(body, "wdl_d1_read_cache_invalidations_total", {
    reason,
    service: "d1-runtime",
  });
}

test("D1 runtime internal query returns row/column payloads for all-mode reads", async () => {
  const ns = uniqueNs("d1raw");
  const databaseId = "main";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table messages (id text primary key, body text)", params: [] },
      { sql: "insert into messages (id, body) values ('r1', 'raw')", params: [] },
    ],
  });
  assertStatus(init, 200, "init");

  const read = d1RuntimeQueryRaw("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select id, body from messages where id = ?", params: ["r1"] }],
  });

  assertStatus(read, 200, "read");
  assert.deepEqual(read.body.results, {
    columns: ["id", "body"],
    rows: [["r1", "raw"]],
  });
});

test("D1 read cache serves repeated owner reads and invalidates after writes", async () => {
  const ns = uniqueNs("d1cache");
  const databaseId = "main";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table messages (id text primary key, body text)", params: [] },
      { sql: "insert into messages (id, body) values ('c1', 'first')", params: [] },
    ],
  });
  assertStatus(init, 200, "init");

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const cacheMetric = (/** @type {string} */ body, /** @type {string} */ outcome) =>
    prometheusCounter(body, "wdl_d1_read_cache_total", {
      outcome,
      service: "d1-runtime",
    });
  const invalidationMetric = (/** @type {string} */ body, /** @type {string} */ reason) =>
    prometheusCounter(body, "wdl_d1_read_cache_invalidations_total", {
      reason,
      service: "d1-runtime",
    });
  const beforeMiss = cacheMetric(beforeMetrics, "miss");
  const beforeStore = cacheMetric(beforeMetrics, "store");
  const beforeHit = cacheMetric(beforeMetrics, "hit");
  const beforeWriteInvalidation = invalidationMetric(beforeMetrics, "write");

  const select = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select count(*) as n from messages", params: [] }],
  };
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);

  let metricsBody = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(cacheMetric(metricsBody, "miss") - beforeMiss, 1);
  assert.equal(cacheMetric(metricsBody, "store") - beforeStore, 1);
  assert.equal(cacheMetric(metricsBody, "hit") - beforeHit, 1);

  const write = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: "insert into messages (id, body) values ('c2', 'second')", params: [] }],
  });
  assertStatus(write, 200, "write");

  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 2 }]);
  metricsBody = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(invalidationMetric(metricsBody, "write") - beforeWriteInvalidation, 1);
  assert.equal(cacheMetric(metricsBody, "miss") - beforeMiss, 2);
  assert.equal(cacheMetric(metricsBody, "store") - beforeStore, 2);
  assert.equal(cacheMetric(metricsBody, "hit") - beforeHit, 1);
});

test("D1 read cache invalidates after non-cacheable all-mode mutations", async () => {
  const ns = uniqueNs("d1cacheallwrite");
  const databaseId = "main";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table messages (id text primary key, body text)", params: [] },
      { sql: "insert into messages (id, body) values ('c1', 'first')", params: [] },
    ],
  });
  assertStatus(init, 200, "init");

  const select = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select count(*) as n from messages", params: [] }],
  };
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);

  const returningWrite = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "insert into messages (id, body) values ('c2', 'second') returning id", params: [] },
    ],
  });
  assertStatus(returningWrite, 200, "returningWrite");
  assert.equal(returningWrite.body.meta.changed_db, true);

  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 2 }]);
});

test("D1 read cache survives no-op CREATE TABLE IF NOT EXISTS schema ensures", async () => {
  const ns = uniqueNs("d1cachenoopddl");
  const databaseId = "main";
  const schemaSql = "create table if not exists inspections (id text primary key, body text)";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: schemaSql, params: [] },
      { sql: "insert into inspections (id, body) values ('i1', 'first')", params: [] },
    ],
  });
  assertStatus(init, 200, "init");

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const cacheMetric = (/** @type {string} */ body, /** @type {string} */ outcome) =>
    prometheusCounter(body, "wdl_d1_read_cache_total", {
      outcome,
      service: "d1-runtime",
    });
  const invalidationMetric = (/** @type {string} */ body, /** @type {string} */ reason) =>
    prometheusCounter(body, "wdl_d1_read_cache_invalidations_total", {
      reason,
      service: "d1-runtime",
    });
  const beforeMiss = cacheMetric(beforeMetrics, "miss");
  const beforeStore = cacheMetric(beforeMetrics, "store");
  const beforeHit = cacheMetric(beforeMetrics, "hit");
  const beforeWriteInvalidation = invalidationMetric(beforeMetrics, "write");
  const beforeChangedInvalidation = invalidationMetric(beforeMetrics, "changed-db");

  const select = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select id, body from inspections order by id", params: [] }],
  };
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ id: "i1", body: "first" }]);

  const noOpEnsure = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: schemaSql, params: [] }],
  });
  assertStatus(noOpEnsure, 200, "noOpEnsure");
  assert.deepEqual(Object.keys(noOpEnsure.body).toSorted(), ["count", "duration"]);

  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ id: "i1", body: "first" }]);

  const afterMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(cacheMetric(afterMetrics, "miss") - beforeMiss, 1);
  assert.equal(cacheMetric(afterMetrics, "store") - beforeStore, 1);
  assert.equal(cacheMetric(afterMetrics, "hit") - beforeHit, 1);
  assert.equal(invalidationMetric(afterMetrics, "write") - beforeWriteInvalidation, 0);
  assert.equal(invalidationMetric(afterMetrics, "changed-db") - beforeChangedInvalidation, 0);
});

test("D1 read cache invalidates when an exec string mixes no-op DDL with later writes", async () => {
  const ns = uniqueNs("d1cachemixedddl");
  const databaseId = "main";
  const schemaSql = "create table if not exists inspections (id text primary key, body text)";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: schemaSql, params: [] },
      { sql: "insert into inspections (id, body) values ('i1', 'first')", params: [] },
    ],
  });
  assertStatus(init, 200, "init");

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const invalidationMetric = (/** @type {string} */ body, /** @type {string} */ reason) =>
    prometheusCounter(body, "wdl_d1_read_cache_invalidations_total", {
      reason,
      service: "d1-runtime",
    });
  const beforeWriteInvalidation = invalidationMetric(beforeMetrics, "write");

  const select = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select count(*) as n from inspections", params: [] }],
  };
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 1 }]);

  const mixed = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{
      sql: `${schemaSql}; insert into inspections (id, body) values ('i2', 'second')`,
      params: [],
    }],
  });
  assertStatus(mixed, 200, "mixed");
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select).body.results, [{ n: 2 }]);

  const afterMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(invalidationMetric(afterMetrics, "write") - beforeWriteInvalidation, 1);
});

test("D1 read cache conservatively invalidates when delayed exec DDL rolls back after an error", async () => {
  const ns = uniqueNs("d1cachepartialddl");
  const databaseId = "main";
  const selectSchema = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      {
        sql: "select name from sqlite_master where type = 'table' and name = 'partial_created'",
        params: [],
      },
    ],
  };

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const beforeWriteInvalidation = readCacheInvalidationMetric(beforeMetrics, "write");

  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, []);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, []);

  const partial = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table if not exists partial_created (id text)", params: [] },
      { sql: "create index if not exists idx_missing on missing_table(id)", params: [] },
    ],
  });
  assertNotStatus(partial, 200, "partial invalidation D1 query");
  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, []);

  const afterMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(readCacheInvalidationMetric(afterMetrics, "write") - beforeWriteInvalidation, 1);
});

test("D1 read cache invalidates after first CREATE TABLE IF NOT EXISTS", async () => {
  const ns = uniqueNs("d1cachefirsttable");
  const databaseId = "main";
  const schemaSql = "create table if not exists inspections (id text primary key)";
  const selectSchema = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      {
        sql: "select name from sqlite_master where type = 'table' and name = 'inspections'",
        params: [],
      },
    ],
  };

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const beforeChangedInvalidation = readCacheInvalidationMetric(beforeMetrics, "changed-db");
  const beforeMiss = readCacheMetric(beforeMetrics, "miss");
  const beforeStore = readCacheMetric(beforeMetrics, "store");

  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, []);
  const create = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: schemaSql, params: [] }],
  });
  assertStatus(create, 200, "create");
  assert.deepEqual(Object.keys(create.body).toSorted(), ["count", "duration"]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, [{ name: "inspections" }]);

  const afterMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(readCacheInvalidationMetric(afterMetrics, "changed-db") - beforeChangedInvalidation, 1);
  assert.equal(readCacheMetric(afterMetrics, "miss") - beforeMiss, 2);
  assert.equal(readCacheMetric(afterMetrics, "store") - beforeStore, 2);
});

test("D1 read cache invalidates after first CREATE INDEX IF NOT EXISTS", async () => {
  const ns = uniqueNs("d1cachefirstindex");
  const databaseId = "main";
  const init = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: "create table inspections (id text primary key, created_at text)", params: [] }],
  });
  assertStatus(init, 200, "init");

  const indexSql = "create unique index if not exists idx_inspections_created on inspections(created_at)";
  const selectSchema = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      {
        sql: "select name from sqlite_master where type = 'index' and name = 'idx_inspections_created'",
        params: [],
      },
    ],
  };

  const beforeMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  const beforeChangedInvalidation = readCacheInvalidationMetric(beforeMetrics, "changed-db");
  const beforeMiss = readCacheMetric(beforeMetrics, "miss");
  const beforeStore = readCacheMetric(beforeMetrics, "store");

  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, []);
  const create = d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: indexSql, params: [] }],
  });
  assertStatus(create, 200, "create");
  assert.deepEqual(Object.keys(create.body).toSorted(), ["count", "duration"]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", selectSchema).body.results, [{
    name: "idx_inspections_created",
  }]);

  const afterMetrics = serviceInternalGet("d1-runtime", 8787, "/_metrics").body;
  assert.equal(readCacheInvalidationMetric(afterMetrics, "changed-db") - beforeChangedInvalidation, 1);
  assert.equal(readCacheMetric(afterMetrics, "miss") - beforeMiss, 2);
  assert.equal(readCacheMetric(afterMetrics, "store") - beforeStore, 2);
});

test("D1 router read cache isolates identical SQL across databases", async () => {
  const ns = uniqueNs("d1cacheiso");
  const seed = (/** @type {string} */ databaseId, /** @type {string} */ body) => d1RuntimeQuery("d1-runtime", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table messages (id text primary key, body text)", params: [] },
      { sql: "insert into messages (id, body) values ('shared', ?)", params: [body] },
    ],
  });
  assert.equal(seed("main-a", "from-a").status, 200);
  assert.equal(seed("main-b", "from-b").status, 200);

  const select = (/** @type {string} */ databaseId) => ({
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = 'shared'", params: [] }],
  });

  assert.deepEqual(d1RuntimeQuery("d1-runtime", select("main-a")).body.results, [{ body: "from-a" }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select("main-a")).body.results, [{ body: "from-a" }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime", select("main-b")).body.results, [{ body: "from-b" }]);
});

test("D1 read cache does not survive ownership rebalance", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1cacherebal");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);
  const countQuery = {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select count(*) as n from messages", params: [] }],
  };

  const beforeMetrics = serviceInternalGet("d1-runtime-a", 8787, "/_metrics").body;
  const cacheMetric = (/** @type {string} */ body, /** @type {string} */ outcome) =>
    prometheusCounter(body, "wdl_d1_read_cache_total", {
      outcome,
      service: "d1-runtime",
    });
  const invalidationMetric = (/** @type {string} */ body, /** @type {string} */ reason) =>
    prometheusCounter(body, "wdl_d1_read_cache_invalidations_total", {
      reason,
      service: "d1-runtime",
    });
  const beforeStore = cacheMetric(beforeMetrics, "store");
  const beforeHit = cacheMetric(beforeMetrics, "hit");
  const beforeOwnerMovedInvalidation = invalidationMetric(beforeMetrics, "owner-moved");

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [
      { sql: "create table messages (id text primary key, body text)", params: [] },
      { sql: "insert into messages (id, body) values ('c1', 'before-rebalance')", params: [] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-a");

  assert.deepEqual(d1RuntimeQuery("d1-runtime-a", countQuery).body.results, [{ n: 1 }]);
  assert.deepEqual(d1RuntimeQuery("d1-runtime-a", countQuery).body.results, [{ n: 1 }]);
  const cachedMetrics = serviceInternalGet("d1-runtime-a", 8787, "/_metrics").body;
  assert.equal(cacheMetric(cachedMetrics, "store") - beforeStore, 1);
  assert.equal(cacheMetric(cachedMetrics, "hit") - beforeHit, 1);

  const moved = d1RuntimeRebalance("d1-runtime-a", {
    databases: [{ namespace: ns, databaseId }],
    target: { taskId: "d1-runtime-c", endpoint: "d1-runtime-c:8787" },
  });
  assertStatus(moved, 200, "moved");
  assert.equal(moved.body.results[0].outcome, "moved");

  const writeC = d1RuntimeQuery("d1-runtime-c", {
    namespace: ns,
    databaseId,
    mode: "exec",
    statements: [{ sql: "insert into messages (id, body) values ('c2', 'after-rebalance')", params: [] }],
  });
  assertStatus(writeC, 200, "writeC");

  const readViaA = d1RuntimeQuery("d1-runtime-a", countQuery);
  assertStatus(readViaA, 200, "readViaA");
  assert.deepEqual(readViaA.body.results, [{ n: 2 }]);
  assert.equal(readViaA.body.meta.served_by, "d1-2eae8f37"); // fnv1a32("d1-runtime-c")
  const movedMetrics = serviceInternalGet("d1-runtime-a", 8787, "/_metrics").body;
  assert.equal(invalidationMetric(movedMetrics, "owner-moved") - beforeOwnerMovedInvalidation, 1);
  assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-c");
});
