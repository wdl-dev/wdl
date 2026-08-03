import { after, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  D1_LEASE_WAIT_MS,
  D1_QUERY_CONTENT_TYPE,
  decodeD1QueryResponse,
  dbKeyOf,
  d1RuntimeProbe,
  d1RuntimeQuery,
  d1RuntimeRebalance,
  d1RuntimeTestHookQueryAsync,
  encodeD1QueryRequest,
  normalizeD1QueryBody,
  recreateD1MultiTasks,
  restoreD1MultiTasks,
  restoreD1SingleRuntime,
  redisSetOwner,
} from "./helpers/d1-runtime.js";
import {
  assertStatus,
  runProbeNode,
  serviceInternalGet,
  sh,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
  parseStdoutJson,
} from "./helpers/index.js";
import { redisGet } from "./helpers/redis.js";

process.env.D1_TEST_HOOKS = "1";

let usedD1MultiRuntime = false;

setupIntegrationSuite();

after(() => {
  if (usedD1MultiRuntime) restoreD1SingleRuntime();
});

/** @param {{ ownerLeaseGuardMs?: number }} [options] */
function useD1MultiRuntime(options = {}) {
  usedD1MultiRuntime = true;
  recreateD1MultiTasks(options);
}

function d1UniqueKey() {
  const config = readFileSync(new URL("../../d1-runtime/config.capnp", import.meta.url), "utf8");
  const match = config.match(/uniqueKey = "([^"]+)"/);
  assert.ok(match, "d1-runtime/config.capnp should declare durable object uniqueKey");
  return match[1];
}

/** @param {string} source */
function probeNodeEval(source) {
  return runProbeNode(source).trim();
}

/** @param {string} service */
function inspectD1DataMount(service) {
  const containerId = sh(["docker", "compose", "ps", "-q", service], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "d1-multi" },
  }).trim();
  assert.ok(containerId, `missing container id for ${service}`);
  const mounts = parseStdoutJson(
    sh(["docker", "inspect", "--format", "{{json .Mounts}}", containerId], {
      stdio: "pipe",
    }).trim(),
    `${service} mounts`
  );
  const mount = mounts.find((/** @type {any} */ entry) => entry.Destination === "/data/d1");
  assert.ok(mount, `${service} should mount /data/d1`);
  assert.equal(mount.Type, "volume", `${service} /data/d1 should be a Docker volume`);
  return mount;
}

function assertD1DataVolumeSharedByRuntimesAndProbe() {
  const runtimeA = inspectD1DataMount("d1-runtime-a");
  const runtimeB = inspectD1DataMount("d1-runtime-b");
  const probe = inspectD1DataMount("test-probe");
  assert.equal(runtimeA.Name, runtimeB.Name, "D1 runtimes should mount the same d1-data volume");
  assert.equal(runtimeA.Name, probe.Name, "test-probe should observe the same d1-data volume");
  assert.equal(runtimeA.RW, true, "d1-runtime-a should mount d1-data read-write");
  assert.equal(runtimeB.RW, true, "d1-runtime-b should mount d1-data read-write");
  assert.equal(probe.RW, false, "test-probe should mount d1-data read-only");
}

/** @param {string} uniqueKey */
function inspectStorageView(uniqueKey) {
  const path = `/data/d1/${uniqueKey}`;
  const view = parseStdoutJson(probeNodeEval(`
    const fs = require("node:fs");
    try {
      const metadata = fs.statSync(${JSON.stringify(`${path}/metadata.sqlite`)});
      const sqliteFiles = fs.readdirSync(${JSON.stringify(path)})
        .filter((name) => name.endsWith(".sqlite"))
        .sort();
      console.log(JSON.stringify({
        ino: metadata.ino,
        dev: metadata.dev,
        size: metadata.size,
        mtimeMs: metadata.mtimeMs,
        sqliteFiles,
      }));
    } catch (error) {
      console.error(JSON.stringify({
        error: "Failed to inspect D1 localDisk storage view",
        inspectPath: ${JSON.stringify(path)},
        message: error && error.message,
        code: error && error.code,
        path: error && error.path,
      }));
      process.exit(1);
    }
  `), "D1 localDisk storage view");
  return {
    path,
    ...view,
  };
}

/** @param {Array<{ host: string, body: any }>} queries */
function concurrentInternalQueries(queries) {
  const queriesJsonB64 = Buffer.from(JSON.stringify(queries.map((query) => ({
    host: query.host,
    databaseId: query.body.databaseId,
    bodyB64: Buffer.from(encodeD1QueryRequest(query.body)).toString("base64"),
  })))).toString("base64");
  const source = `
    const queries = JSON.parse(Buffer.from(process.env.D1_CONCURRENT_QUERIES_B64 || "", "base64").toString("utf8"));
    async function main() {
      const results = await Promise.all(queries.map(async (query) => {
        try {
          const response = await fetch(\`http://\${query.host}:8787/internal/d1/query\`, {
            method: "POST",
            headers: {
              "content-type": ${JSON.stringify(D1_QUERY_CONTENT_TYPE)},
              "x-wdl-internal-auth": process.env.WDL_INTERNAL_AUTH_TOKEN || "local-internal-auth-token",
            },
            body: Buffer.from(query.bodyB64, "base64"),
          });
          const buffer = Buffer.from(await response.arrayBuffer());
          return {
            host: query.host,
            databaseId: query.databaseId,
            status: response.status,
            bodyB64: buffer.toString("base64"),
          };
        } catch (err) {
          return {
            host: query.host,
            databaseId: query.databaseId,
            error: String(err && err.stack || err),
          };
        }
      }));
      console.log(JSON.stringify(results));
    }
    main().catch((err) => {
      console.error(err && err.stack || err);
      process.exit(1);
    });
  `;
  return parseStdoutJson(
    runProbeNode(source, { env: { D1_CONCURRENT_QUERIES_B64: queriesJsonB64 } }).trim(),
    "concurrent D1 internal query results"
  ).map((/** @type {any} */ result) => ({
    ...result,
    body: result.bodyB64 ? normalizeD1QueryBody(decodeD1QueryResponse(Buffer.from(result.bodyB64, "base64"))) : result.body,
  }));
}

/** @param {any} queryResult */
function finalStatementRows(queryResult) {
  assertStatus(queryResult, 200, "queryResult");
  assert.ok(Array.isArray(queryResult.body), `expected multi-statement array body, got ${JSON.stringify(queryResult.body)}`);
  const last = queryResult.body.at(-1);
  assert.ok(last, `expected at least one statement result, got ${JSON.stringify(queryResult.body)}`);
  assert.equal(last.success, true, JSON.stringify(last));
  return last.results;
}

/** @param {string} service @param {string} dbKey */
function currentOwnerTaskId(service, dbKey) {
  return d1RuntimeProbe(service, dbKey).owner?.taskId ?? null;
}

function crashRecoveryRounds() {
  const raw = Number(process.env.WDL_D1_CRASH_RECOVERY_ROUNDS || 3);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 3;
}

/** @param {unknown} actual @param {unknown} expected */
function rowsEqual(actual, expected) {
  return JSON.stringify(actual) === JSON.stringify(expected);
}

/** @param {string} service @param {string} ns @param {string} databaseId */
function readCommittedRows(service, ns, databaseId) {
  return d1RuntimeQuery(service, {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [{ sql: "select id, body from messages order by id" }],
  });
}

/** @param {string} dbKey */
function testHookHoldStarted(dbKey) {
  const key = `d1:test-hook:hold-started:${encodeURIComponent(dbKey)}`;
  return redisGet(key) ?? "";
}

test("two d1 runtimes share one localDisk metadata sqlite across different DO ids", async () => {
  useD1MultiRuntime();

  const uniqueKey = d1UniqueKey();
  const ns = uniqueNs("d1sharedmeta");
  const leftDb = "left-db";
  const rightDb = "right-db";

  const [leftInit, rightInit] = await Promise.all([
    Promise.resolve(d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId: leftDb,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["l1", "from-left"] },
      ],
    })),
    Promise.resolve(d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId: rightDb,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["r1", "from-right"] },
      ],
    })),
  ]);

  assertStatus(leftInit, 200, "leftInit");
  assertStatus(rightInit, 200, "rightInit");
  assertD1DataVolumeSharedByRuntimesAndProbe();

  await waitUntil("shared localDisk metadata.sqlite", () => {
    const view = inspectStorageView(uniqueKey);
    return (
      view.sqliteFiles.length >= 3
    );
  }, { timeoutMs: 5000, intervalMs: 250 });

  const view = inspectStorageView(uniqueKey);

  assert.equal(view.path, `/data/d1/${uniqueKey}`);
  assert.ok(Number.isInteger(view.ino), "metadata.sqlite inode should be visible from the shared volume");
  assert.ok(Number.isInteger(view.dev), "metadata.sqlite device should be visible from the shared volume");
  assert.ok(
    view.sqliteFiles.length >= 3,
    `expected metadata.sqlite plus at least two actor sqlite files, got ${view.sqliteFiles.length}`
  );
});

test("concurrent DO creation across two runtimes sharing one localDisk path does not immediately fail", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1sharedstress");
  const rounds = 5;
  const perRuntimePerRound = 12;

  for (let round = 0; round < rounds; round += 1) {
    const queries = [];
    for (const host of ["d1-runtime-a", "d1-runtime-b"]) {
      const prefix = host.endsWith("-a") ? "a" : "b";
      for (let idx = 0; idx < perRuntimePerRound; idx += 1) {
        const databaseId = `${prefix}-r${round}-db${idx}`;
        queries.push({
          host,
          body: {
            namespace: ns,
            databaseId,
            mode: "all",
            statements: [
              { sql: "create table if not exists messages (id text primary key, body text)" },
              { sql: "insert into messages (id, body) values (?, ?)", params: [`${databaseId}-row`, host] },
              { sql: "select count(*) as n from messages" },
            ],
          },
        });
      }
    }

    const results = concurrentInternalQueries(queries);
    const failures = results.filter((/** @type {any} */ result) => {
      if (result.error) return true;
      if (result.status !== 200) return true;
      if (Array.isArray(result.body)) {
        return result.body.some((/** @type {any} */ entry) => entry?.success !== true);
      }
      return result.body?.success !== true;
    });

    assert.deepEqual(failures, [], `round ${round} failed: ${JSON.stringify(failures, null, 2)}`);

    for (const service of ["d1-runtime-a", "d1-runtime-b"]) {
      const health = serviceInternalGet(service, 8787, "/healthz");
      assert.equal(health.status, 200, `${service} healthz failed after round ${round}: ${health.body}`);
    }
  }
});

test("same DB survives repeated handoff across runtimes sharing one localDisk path", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1handoff");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const init = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["m0", "from-a"] },
    ],
  });
  assertStatus(init, 200, "init");

  const hops = [
    { from: "d1-runtime-a", to: "d1-runtime-b", id: "m1", body: "from-b" },
    { from: "d1-runtime-b", to: "d1-runtime-c", id: "m2", body: "from-c" },
    { from: "d1-runtime-c", to: "d1-runtime-a", id: "m3", body: "back-to-a" },
  ];

  const expectedRows = [
    { id: "m0", body: "from-a" },
    { id: "m1", body: "from-b" },
    { id: "m2", body: "from-c" },
    { id: "m3", body: "back-to-a" },
  ];

  for (const hop of hops) {
    const moved = d1RuntimeRebalance(hop.from, {
      databases: [{ namespace: ns, databaseId }],
      target: { taskId: hop.to, endpoint: `${hop.to}:8787` },
    });
    assertStatus(moved, 200, "moved");
    assert.equal(moved.body.results[0].outcome, "moved");
    assert.equal(moved.body.results[0].owner.taskId, hop.to);

    await waitUntil(`owner moved to ${hop.to}`, () => {
      const owner = d1RuntimeProbe(hop.to, dbKey).owner;
      return owner.taskId === hop.to;
    }, { timeoutMs: 5000, intervalMs: 250 });

    const write = d1RuntimeQuery(hop.to, {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: [hop.id, hop.body] },
        { sql: "select id, body from messages order by id" },
      ],
    });
    assert.deepEqual(
      finalStatementRows(write),
      expectedRows.slice(0, expectedRows.findIndex((row) => row.id === hop.id) + 1)
    );

    const forwardedRead = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select id, body from messages order by id" }],
    });
    assertStatus(forwardedRead, 200, "forwardedRead");
    assert.deepEqual(forwardedRead.body.results, expectedRows.slice(0, expectedRows.findIndex((row) => row.id === hop.id) + 1));
  }
});

test("near-expiry D1 owner renews before shared localDisk write", async () => {
  useD1MultiRuntime({ ownerLeaseGuardMs: 4000 });

  const ns = uniqueNs("d1leaseguard");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  const init = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "create table if not exists messages (id text primary key, body text)" },
      { sql: "insert into messages (id, body) values (?, ?)", params: ["m0", "before-renew"] },
    ],
  });
  assertStatus(init, 200, "init");

  const owner = d1RuntimeProbe("d1-runtime-b", dbKey).owner;
  assert.equal(owner.taskId, "d1-runtime-a");
  const nearExpiryOwner = {
    ...owner,
    leaseExpiresAt: Date.now() + 3000,
  };
  redisSetOwner(dbKey, nearExpiryOwner);

  const write = d1RuntimeQuery("d1-runtime-a", {
    namespace: ns,
    databaseId,
    mode: "all",
    statements: [
      { sql: "insert into messages (id, body) values (?, ?)", params: ["m1", "after-actor-renew"] },
      { sql: "select id, body from messages order by id" },
    ],
  });
  assert.deepEqual(finalStatementRows(write), [
    { id: "m0", body: "before-renew" },
    { id: "m1", body: "after-actor-renew" },
  ]);

  const renewedOwner = d1RuntimeProbe("d1-runtime-b", dbKey).owner;
  assert.equal(renewedOwner.taskId, "d1-runtime-a");
  assert.equal(renewedOwner.generation, owner.generation);
  assert.ok(
    Number(renewedOwner.leaseExpiresAt) > Number(nearExpiryOwner.leaseExpiresAt),
    `expected actor-side renew before write, got ${JSON.stringify({ nearExpiryOwner, renewedOwner })}`
  );
});

test("same DB survives hard owner loss with write then takeover read confirmation", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1hardloss");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);

  try {
    const init = d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["k0", "before-kill-a"] },
      ],
    });
    assertStatus(init, 200, "init");
    assert.equal(d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId, "d1-runtime-a");

    sh(["docker", "compose", "kill", "-s", "KILL", "d1-runtime-a"], {
      stdio: "pipe",
      env: { COMPOSE_PROFILES: "d1-multi" },
    });
    await waitUntil("takeover to d1-runtime-b after killing a", () => {
      const read = d1RuntimeQuery("d1-runtime-b", {
        namespace: ns,
        databaseId,
        mode: "all",
        statements: [{ sql: "select id, body from messages order by id" }],
      });
      return (
        read.status === 200 &&
        JSON.stringify(read.body.results) === JSON.stringify([{ id: "k0", body: "before-kill-a" }]) &&
        d1RuntimeProbe("d1-runtime-b", dbKey).owner.taskId === "d1-runtime-b"
      );
    }, { timeoutMs: D1_LEASE_WAIT_MS + 5000, intervalMs: 500 });

    const writeB = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: ["k1", "after-kill-a"] },
        { sql: "select id, body from messages order by id" },
      ],
    });
    assert.deepEqual(finalStatementRows(writeB), [
      { id: "k0", body: "before-kill-a" },
      { id: "k1", body: "after-kill-a" },
    ]);

    restoreD1MultiTasks();

    let ownerBeforeMove = null;
    await waitUntil("database readable after restoring runtimes", () => {
      for (const candidate of ["d1-runtime-a", "d1-runtime-b"]) {
        const read = d1RuntimeQuery(candidate, {
          namespace: ns,
          databaseId,
          mode: "all",
          statements: [{ sql: "select id, body from messages order by id" }],
        });
        if (
          read.status === 200 &&
          JSON.stringify(read.body.results) === JSON.stringify([
            { id: "k0", body: "before-kill-a" },
            { id: "k1", body: "after-kill-a" },
          ])
        ) {
          ownerBeforeMove = currentOwnerTaskId(candidate, dbKey);
          if (ownerBeforeMove === candidate) return true;
        }
      }
      return false;
    }, { timeoutMs: D1_LEASE_WAIT_MS + 5000, intervalMs: 500 });

    assert.ok(ownerBeforeMove, "restored runtimes should elect an owner after reading persisted rows");
    if (ownerBeforeMove !== "d1-runtime-c") {
      const movedToC = d1RuntimeRebalance(ownerBeforeMove, {
        databases: [{ namespace: ns, databaseId }],
        target: { taskId: "d1-runtime-c", endpoint: "d1-runtime-c:8787" },
      });
      assertStatus(movedToC, 200, "movedToC");
      assert.equal(movedToC.body.results[0].outcome, "moved");
    }

    await waitUntil("owner moved to d1-runtime-c", () => {
      return currentOwnerTaskId("d1-runtime-b", dbKey) === "d1-runtime-c";
    }, { timeoutMs: 5000, intervalMs: 250 });

    const writeC = d1RuntimeQuery("d1-runtime-c", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: ["k2", "before-kill-c"] },
        { sql: "select id, body from messages order by id" },
      ],
    });
    assert.deepEqual(finalStatementRows(writeC), [
      { id: "k0", body: "before-kill-a" },
      { id: "k1", body: "after-kill-a" },
      { id: "k2", body: "before-kill-c" },
    ]);

    sh(["docker", "compose", "kill", "-s", "KILL", "d1-runtime-c"], {
      stdio: "pipe",
      env: { COMPOSE_PROFILES: "d1-multi" },
    });
    let survivor = "d1-runtime-b";
    await waitUntil("takeover after killing c", () => {
      for (const candidate of ["d1-runtime-a", "d1-runtime-b"]) {
        const read = d1RuntimeQuery(candidate, {
          namespace: ns,
          databaseId,
          mode: "all",
          statements: [{ sql: "select id, body from messages order by id" }],
        });
        if (
          read.status === 200 &&
          JSON.stringify(read.body.results) === JSON.stringify([
            { id: "k0", body: "before-kill-a" },
            { id: "k1", body: "after-kill-a" },
            { id: "k2", body: "before-kill-c" },
          ])
        ) {
          const owner = currentOwnerTaskId(candidate, dbKey);
          if (owner === candidate) {
            survivor = candidate;
            return true;
          }
        }
      }
      return false;
    }, { timeoutMs: D1_LEASE_WAIT_MS + 5000, intervalMs: 500 });

    const finalRead = d1RuntimeQuery(survivor, {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [{ sql: "select id, body from messages order by id" }],
    });
    assertStatus(finalRead, 200, "finalRead");
    assert.deepEqual(finalRead.body.results, [
      { id: "k0", body: "before-kill-a" },
      { id: "k1", body: "after-kill-a" },
      { id: "k2", body: "before-kill-c" },
    ]);
  } finally {
    restoreD1MultiTasks();
  }
});

test("same DB survives repeated hard owner loss with committed rows intact", {
  timeout: Math.max(120_000, crashRecoveryRounds() * (D1_LEASE_WAIT_MS + 12_000)),
}, async () => {
  useD1MultiRuntime();

  const services = ["d1-runtime-a", "d1-runtime-b", "d1-runtime-c"];
  const rounds = crashRecoveryRounds();
  const ns = uniqueNs("d1crashloop");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);
  /** @type {Array<{ id: string, body: string }>} */
  const expectedRows = [];
  let owner = "d1-runtime-a";

  try {
    const init = d1RuntimeQuery(owner, {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
      ],
    });
    assertStatus(init, 200, "init");
    assert.equal(currentOwnerTaskId("d1-runtime-b", dbKey), owner);

    for (let round = 0; round < rounds; round += 1) {
      const row = {
        id: `r${String(round).padStart(4, "0")}`,
        body: `committed-${owner}-${round}`,
      };
      expectedRows.push(row);

      const write = d1RuntimeQuery(owner, {
        namespace: ns,
        databaseId,
        mode: "all",
        statements: [
          { sql: "insert into messages (id, body) values (?, ?)", params: [row.id, row.body] },
          { sql: "select id, body from messages order by id" },
        ],
      });
      assert.deepEqual(finalStatementRows(write), expectedRows);

      sh(["docker", "compose", "kill", "-s", "KILL", owner], {
        stdio: "pipe",
        env: { COMPOSE_PROFILES: "d1-multi" },
      });
      const killedOwner = owner;
      const candidates = services.filter((service) => service !== killedOwner);
      let nextOwner = null;

      await waitUntil(`round ${round} takeover after killing ${killedOwner}`, () => {
        for (const candidate of candidates) {
          const read = readCommittedRows(candidate, ns, databaseId);
          if (read.status === 200 && rowsEqual(read.body.results, expectedRows)) {
            const candidateOwner = currentOwnerTaskId(candidate, dbKey);
            if (candidateOwner === candidate) {
              nextOwner = candidate;
              return true;
            }
          }
        }
        return false;
      }, { timeoutMs: D1_LEASE_WAIT_MS + 8_000, intervalMs: 500 });

      assert.ok(nextOwner, `round ${round} should elect a survivor owner`);
      owner = nextOwner;

      restoreD1MultiTasks();
      await waitUntil(`round ${round} restored owner can reopen sqlite`, () => {
        const read = readCommittedRows(owner, ns, databaseId);
        return read.status === 200 &&
          rowsEqual(read.body.results, expectedRows) &&
          currentOwnerTaskId(owner, dbKey) === owner;
      }, { timeoutMs: D1_LEASE_WAIT_MS + 8_000, intervalMs: 500 });
    }
  } finally {
    restoreD1MultiTasks();
  }
});

test("uncommitted test-hook transaction is not visible after hard owner loss", async () => {
  useD1MultiRuntime();

  const ns = uniqueNs("d1uncommitted");
  const databaseId = "main";
  const dbKey = dbKeyOf(ns, databaseId);
  const owner = "d1-runtime-a";

  try {
    const init = d1RuntimeQuery(owner, {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "create table if not exists messages (id text primary key, body text)" },
        { sql: "insert into messages (id, body) values (?, ?)", params: ["committed", "before-hold"] },
      ],
    });
    assertStatus(init, 200, "init");
    assert.equal(currentOwnerTaskId("d1-runtime-b", dbKey), owner);

    const hold = d1RuntimeTestHookQueryAsync(owner, {
      namespace: ns,
      databaseId,
      mode: "all",
      __control: "hold-transaction",
      __holdMs: 60_000,
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: ["uncommitted", "during-hold"] },
      ],
    }).catch((err) => ({ error: String(err?.message || err) }));

    await waitUntil("test hook transaction to start", () => testHookHoldStarted(dbKey) === owner, {
      timeoutMs: 10_000,
      intervalMs: 200,
    });
    sh(["docker", "compose", "kill", "-s", "KILL", owner], {
      stdio: "pipe",
      env: { COMPOSE_PROFILES: "d1-multi" },
    });
    await hold;

    await waitUntil("takeover after killing owner during uncommitted transaction", () => {
      const read = d1RuntimeQuery("d1-runtime-b", {
        namespace: ns,
        databaseId,
        mode: "all",
        statements: [
          { sql: "select id, body from messages order by id" },
          { sql: "select id from _wdl_d1_test_hooks where id = 'hold-transaction-started'" },
        ],
      });
      if (read.status !== 200 || !Array.isArray(read.body)) return false;
      const rows = read.body[0]?.results;
      const marker = read.body[1]?.results;
      return rowsEqual(rows, [{ id: "committed", body: "before-hold" }]) &&
        rowsEqual(marker, [{ id: "hold-transaction-started" }]) &&
        currentOwnerTaskId("d1-runtime-b", dbKey) === "d1-runtime-b";
    }, { timeoutMs: D1_LEASE_WAIT_MS + 8_000, intervalMs: 500 });

    const writeAfterRecovery = d1RuntimeQuery("d1-runtime-b", {
      namespace: ns,
      databaseId,
      mode: "all",
      statements: [
        { sql: "insert into messages (id, body) values (?, ?)", params: ["after", "after-recovery"] },
        { sql: "select id, body from messages order by id" },
      ],
    });
    assert.deepEqual(finalStatementRows(writeAfterRecovery), [
      { id: "after", body: "after-recovery" },
      { id: "committed", body: "before-hold" },
    ]);
  } finally {
    restoreD1MultiTasks();
  }
});
