import { after, test } from "node:test";
import assert from "node:assert/strict";
import {
  d1RuntimeDrain,
  d1RuntimeDrainAsync,
  d1RuntimeProbe,
  d1RuntimeProbeRaw,
  d1RuntimeQuery,
  d1RuntimeQueryAsync,
  d1RuntimeRebalance,
  d1RuntimeTestHookQueryAsync,
  databaseIdOnSameSlot,
  dbKeyOf,
  D1_QUERY_CONTENT_TYPE,
  decodeD1QueryResponse,
  encodeD1QueryRequest,
  D1_LEASE_WAIT_MS,
  normalizeD1QueryBody,
  recreateD1MultiTasks,
  redisSetOwner,
  restoreD1MultiTasks,
  restoreD1SingleRuntime,
  slotOf,
} from "./helpers/d1-runtime.js";
import {
  assertStatus,
  composeStop,
  delay,
  serviceInternalPost,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";

process.env.D1_TEST_HOOKS = "1";

let usedD1MultiRuntime = false;

setupIntegrationSuite();

after(() => {
  if (usedD1MultiRuntime) restoreD1SingleRuntime();
});

function useD1MultiRuntime() {
  usedD1MultiRuntime = true;
  recreateD1MultiTasks();
}

test("D1 owner registry lets three local runtime tasks own different databases and forward to owners", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1multi");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);
  const otherDatabaseId = databaseIdOnSameSlot(ns, databaseId);
  const otherDbKey = dbKeyOf(ns, otherDatabaseId);
  const thirdDatabaseId = "third";
  const thirdDbKey = dbKeyOf(ns, thirdDatabaseId);
  assert.equal(slotOf(ns, otherDatabaseId), slotOf(ns, databaseId));

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["a1", "from-a"] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.ok(Array.isArray(initA.body));
  assert.equal(initA.body[1].success, true);

  const forwardedB = d1RuntimeQuery("d1-runtime-b", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["a1"] }],
  });
  assertStatus(forwardedB, 200, "forwardedB");
  assert.deepEqual(forwardedB.body.results, [{ body: "from-a" }]);

  const forwardedC = d1RuntimeQuery("d1-runtime-c", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["a1"] }],
  });
  assertStatus(forwardedC, 200, "forwardedC");
  assert.deepEqual(forwardedC.body.results, [{ body: "from-a" }]);

  const initB = d1RuntimeQuery("d1-runtime-b", {
    namespace: ns,
    databaseId: otherDatabaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["b1", "from-b"] },
    ],
  });
  assertStatus(initB, 200, "initB");
  assert.equal(initB.body[1].success, true);
  assert.equal(d1RuntimeProbe("d1-runtime-a", dbKey).owner.taskId, "d1-runtime-a");
  assert.equal(d1RuntimeProbe("d1-runtime-a", otherDbKey).owner.taskId, "d1-runtime-b");

  const initC = d1RuntimeQuery("d1-runtime-c", {
    namespace: ns,
    databaseId: thirdDatabaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["c1", "from-c"] },
    ],
  });
  assertStatus(initC, 200, "initC");
  assert.equal(initC.body[1].success, true);
  assert.equal(d1RuntimeProbe("d1-runtime-a", thirdDbKey).owner.taskId, "d1-runtime-c");

  const forwardedA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId: otherDatabaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["b1"] }],
  });
  assertStatus(forwardedA, 200, "forwardedA");
  assert.deepEqual(forwardedA.body.results, [{ body: "from-b" }]);

  const forwardedAToC = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId: thirdDatabaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["c1"] }],
  });
  assertStatus(forwardedAToC, 200, "forwardedAToC");
  assert.deepEqual(forwardedAToC.body.results, [{ body: "from-c" }]);

  try {
    composeStop("d1-runtime-a");
    await delay(D1_LEASE_WAIT_MS);
    const afterTakeover = d1RuntimeQuery("d1-runtime-c", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select body from messages where id = ?", params: ["a1"] }],
    });
    assertStatus(afterTakeover, 200, "afterTakeover");
    assert.deepEqual(afterTakeover.body.results, [{ body: "from-a" }]);

    const takeoverProbe = d1RuntimeProbe("d1-runtime-c", dbKey);
    assert.equal(takeoverProbe.owner.taskId, "d1-runtime-c");
  } finally {
    restoreD1MultiTasks();
  }
});

test("D1 forwarded requests still resolve owner and do not execute by header alone", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1fwdauth");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["f1", "owner-a"] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.equal(d1RuntimeProbe("d1-runtime-c", dbKey).owner.taskId, "d1-runtime-a");

  const forged = serviceInternalPost(
    "d1-runtime-c",
    8787,
    "/internal/d1/query",
    encodeD1QueryRequest({
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select body from messages where id = ?", params: ["f1"] }],
    }),
    { "content-type": D1_QUERY_CONTENT_TYPE, "x-wdl-d1-forwarded": "1" }
  );
  assertStatus(forged, 200, "forged");
  const body = normalizeD1QueryBody(decodeD1QueryResponse(Buffer.from(forged.bodyB64, "base64")));
  assert.deepEqual(body.results, [{ body: "owner-a" }]);
  assert.equal(body.meta.served_by, "d1-30ae925d"); // fnv1a32("d1-runtime-a")
  assert.equal(d1RuntimeProbe("d1-runtime-c", dbKey).owner.taskId, "d1-runtime-a");
});

test("D1 forwarded requests abort when the hop cap is exhausted", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1hopcap");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["h1", "owner-a"] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.equal(d1RuntimeProbe("d1-runtime-c", dbKey).owner.taskId, "d1-runtime-a");

  const exhausted = serviceInternalPost(
    "d1-runtime-c",
    8787,
    "/internal/d1/query",
    encodeD1QueryRequest({
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select body from messages where id = ?", params: ["h1"] }],
    }),
    {
      "content-type": D1_QUERY_CONTENT_TYPE,
      "x-wdl-d1-forwarded": "1",
      "x-wdl-d1-hop-count": "2",
    }
  );
  assertStatus(exhausted, 503, "exhausted");
  const body = decodeD1QueryResponse(Buffer.from(exhausted.bodyB64, "base64"));
  assert.equal(body.error, "forward-hop-exhausted");
  assert.match(body.message, /forward-hop-exhausted/i);
});

test("D1 cold owner claim handles burst concurrency without surfacing owner races", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1claimburst");
  const databaseId = "main";
  const services = ["d1-runtime-a", "d1-runtime-b", "d1-runtime-c"];
  const count = 20;

  const results = await Promise.all(Array.from({ length: count }, (_value, idx) => (
    d1RuntimeQueryAsync(services[idx % services.length], {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: [`burst-${idx}`, `body-${idx}`] },
      ],
    })
  )));

  for (const result of results) {
    assertStatus(result, 200, "result");
    assert.ok(Array.isArray(result.body), JSON.stringify(result.body));
    assert.equal(result.body[1].success, true, JSON.stringify(result.body));
  }

  const read = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select count(*) as n from messages" }],
  });
  assertStatus(read, 200, "read");
  assert.deepEqual(read.body.results, [{ n: count }]);
});

test("D1 idle owners are renewed by task heartbeat without health checks", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1idle");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["i1", "after-idle"] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.equal(initA.body[1].success, true);
  const initialOwner = d1RuntimeProbe("d1-runtime-a", dbKey).owner;
  assert.equal(initialOwner.taskId, "d1-runtime-a");

  await waitUntil("D1 owner heartbeat renew", () => {
    const owner = d1RuntimeProbe("d1-runtime-b", dbKey).owner;
    return owner.taskId === "d1-runtime-a" &&
      Number(owner.leaseExpiresAt) > Number(initialOwner.leaseExpiresAt);
  }, { timeoutMs: 10000, intervalMs: 500 });

  await delay(D1_LEASE_WAIT_MS);

  const readAfterIdle = d1RuntimeQuery("d1-runtime-b", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["i1"] }],
  });
  assertStatus(readAfterIdle, 200, "readAfterIdle");
  assert.deepEqual(readAfterIdle.body.results, [{ body: "after-idle" }]);
  assert.equal(readAfterIdle.body.meta.served_by, "d1-30ae925d"); // fnv1a32("d1-runtime-a")
  assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-a");
});

test("D1 draining task releases owned databases for immediate takeover", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1drain");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  try {
    const initA = d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["d1", "before-drain"] },
      ],
    });
    assertStatus(initA, 200, "initA");
    assert.equal(initA.body[1].success, true);

    const beforeDrain = d1RuntimeProbe("d1-runtime-b", dbKey);
    assert.equal(beforeDrain.owner.taskId, "d1-runtime-a");

    const drain = d1RuntimeDrain("d1-runtime-a");
    assertStatus(drain, 200, "drain");
    assert.equal(drain.body.draining, true);
    assert.equal(drain.body.released, 1);

    const drainingProbe = d1RuntimeProbeRaw("d1-runtime-a", dbKey);
    assertStatus(drainingProbe, 503, "drainingProbe");
    assert.equal(drainingProbe.body.status, "draining");

    const readAfterDrain = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select body from messages where id = ?", params: ["d1"] }],
    });
    assertStatus(readAfterDrain, 200, "readAfterDrain");
    assert.deepEqual(readAfterDrain.body.results, [{ body: "before-drain" }]);

    const afterDrain = d1RuntimeProbe("d1-runtime-b", dbKey);
    assert.equal(afterDrain.owner.taskId, "d1-runtime-b");
  } finally {
    restoreD1MultiTasks();
  }
});

test("D1 drain does not release an owner before an in-flight write finishes", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1drainwait");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  try {
    const initA = d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
      ],
    });
    assertStatus(initA, 200, "initA");
    assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-a");

    let delayedQueryDone = false;
    let drainDone = false;
    const delayedQueryPromise = d1RuntimeTestHookQueryAsync("d1-runtime-a", {
      namespace: ns,
      databaseId,
      mode: "all",
      __control: "hold-transaction",
      __holdMs: 250,
      statements: [{
        sql: "insert into messages (id, body) values ('delayed', 'before-drain')",
      }],
    }).finally(() => {
      delayedQueryDone = true;
    });

    await delay(25);

    const drainPromise = d1RuntimeDrainAsync("d1-runtime-a").finally(() => {
      drainDone = true;
    });

    await waitUntil("drain waits for in-flight write", async () => {
      if (drainDone && !delayedQueryDone) {
        throw new Error("drain completed before delayed query finished");
      }
      if (delayedQueryDone) return true;

      const ownerProbe = d1RuntimeProbe("d1-runtime-b", dbKey);
      assert.equal(ownerProbe.owner.taskId, "d1-runtime-a");

      const drainingProbe = d1RuntimeProbeRaw("d1-runtime-a", dbKey);
      if (drainingProbe.status === 503) {
        assert.equal(drainingProbe.body.status, "draining");
      }
      return false;
    }, { timeoutMs: 10_000, intervalMs: 50 });

    const delayedQuery = await delayedQueryPromise;
    assertStatus(delayedQuery, 500, "delayedQuery");
    assert.equal(delayedQuery.body.error, "test-transaction-held");

    const drain = await drainPromise;
    assertStatus(drain, 200, "drain");
    assert.equal(drain.body.released, 1);

    const readAfterDrain = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select * from messages where id = ?", params: ["delayed"] }],
    });
    assertStatus(readAfterDrain, 200, "readAfterDrain");
    assert.deepEqual(readAfterDrain.body.results, []);
  } finally {
    restoreD1MultiTasks();
  }
});

test("D1 graceful shutdown drains owned databases before container stop", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1gracedrain");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  try {
    const initA = d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["g1", "before-stop"] },
      ],
    });
    assertStatus(initA, 200, "initA");
    assert.equal(initA.body[1].success, true);
    assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-a");

    composeStop("d1-runtime-a");

    const readAfterStop = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select body from messages where id = ?", params: ["g1"] }],
    });
    assertStatus(readAfterStop, 200, "readAfterStop");
    assert.deepEqual(readAfterStop.body.results, [{ body: "before-stop" }]);
    assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-b");
  } finally {
    restoreD1MultiTasks();
  }
});

test("D1 expired owner takeover succeeds after probe failure", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1takeover");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);
  const slot = slotOf(ns, databaseId);
  redisSetOwner(dbKey, {
    namespace: ns,
    databaseId,
    dbKey,
    slot,
    taskId: "missing-owner",
    endpoint: "d1-runtime-missing:8787",
    generation: 42,
    leaseExpiresAt: Date.now() - 1_000,
  });

  const takeover = d1RuntimeQuery("d1-runtime-b", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["t1", "after-takeover"] },
    ],
  });
  assertStatus(takeover, 200, "takeover");
  assert.equal(takeover.body[1].success, true);

  const owner = d1RuntimeProbe("d1-runtime-b", dbKey).owner;
  assert.equal(owner.taskId, "d1-runtime-b");
  assert.ok(owner.generation > 42);
});

test("D1 owned database rebalances to another runtime task", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1rebalance");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const initA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["r1", "before-rebalance"] },
    ],
  });
  assertStatus(initA, 200, "initA");
  assert.equal(initA.body[1].success, true);

  const before = d1RuntimeProbe("d1-runtime-b", dbKey);
  assert.equal(before.owner.taskId, "d1-runtime-a");

  const moved = d1RuntimeRebalance("d1-runtime-a", {
    databases: [{ namespace: ns, databaseId }],
    target: { taskId: "d1-runtime-c", endpoint: "d1-runtime-c:8787" },
  });
  assertStatus(moved, 200, "moved");
  assert.equal(moved.body.results[0].outcome, "moved");
  assert.equal(moved.body.results[0].owner.taskId, "d1-runtime-c");

  const readViaA = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select body from messages where id = ?", params: ["r1"] }],
  });
  assertStatus(readViaA, 200, "readViaA");
  assert.deepEqual(readViaA.body.results, [{ body: "before-rebalance" }]);

  const after = d1RuntimeProbe("d1-runtime-a", dbKey);
  assert.equal(after.owner.taskId, "d1-runtime-c");
});
