// D1 binding: loaded workers call env.DB through runtime's JSRPC shim. In
// local/integration topology, the initial router hop goes through Envoy while
// learned D1 owner endpoints remain direct task-specific service names.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  call,
  D1_COMPAT_WORKER,
  D1_HARNESS,
  D1_NAMED_CALLER,
  D1_NAMED_TARGET,
  d1RuntimeQuery,
  d1RuntimeRebalance,
  setup,
  startD1EnvoyOwnerPair,
  startD1Router,
  stopD1EnvoyOwnerPair,
  stopD1Router,
} from "./helpers/d1-runtime.js";
import {
  adminPost,
  assertStatus,
  deployAndPromote,
  envoyStat,
  gatewayFetch,
  runtimeInternalGet,
  serviceInternalGet,
  sh,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import { parseCounters } from "./helpers/prometheus.js";

setupIntegrationSuite();

/** @param {Map<string, number>} before @param {Map<string, number>} after @param {string} key */
function counterDelta(before, after, key) {
  return (after.get(key) ?? 0) - (before.get(key) ?? 0);
}

test("D1 binding shares one database across same-namespace loaded workers", async () => {
  const ns = uniqueNs("d1binding");
  await setup(ns, "writer");
  await setup(ns, "reader");

  const init = await call(ns, "writer", { op: "init" });
  assert.equal(init.count, 1);
  assert.equal(typeof init.duration, "number");

  assert.deepEqual(await call(ns, "writer", { op: "exec-semicolon" }), {
    id: "semi",
    body: "a;b",
  });

  const inserted = await call(ns, "writer", { op: "insert", id: "m1", body: "hello" });
  assert.equal(inserted.success, true);
  assert.equal(inserted.meta.changes, 1);
  assert.equal(inserted.meta.served_by_primary, true);
  assert.equal(typeof inserted.meta.served_by, "string");
  assert.equal(inserted.meta.timings.sql_duration_ms, inserted.meta.duration);
  assert.equal(inserted.meta.total_attempts, 1);

  assert.deepEqual(await call(ns, "reader", { op: "get", id: "m1" }), {
    id: "m1",
    body: "hello",
  });

  assert.deepEqual(await call(ns, "reader", { op: "raw", id: "m1" }), [
    ["id", "body"],
    ["m1", "hello"],
  ]);
  assert.deepEqual(await call(ns, "reader", { op: "raw-missing", id: "missing" }), [
    ["id", "body"],
  ]);

  assert.deepEqual(await call(ns, "reader", { op: "session", id: "m1" }), {
    row: { id: "m1", body: "hello" },
    bookmark: null,
  });
});

test("D1 binding reaches router through Envoy before using learned owner endpoint", async () => {
  const ns = uniqueNs("d1envoy");
  await setup(ns, "app");

  const beforeRuntime = parseCounters(runtimeInternalGet("/_metrics"));
  const beforeEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");

  const init = await call(ns, "app", { op: "init" });
  assert.equal(init.count, 1);

  const afterFirstEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");
  assert.ok(afterFirstEnvoy > beforeEnvoy, "first D1 binding call should hit the Envoy D1 router cluster");

  const inserted = await call(ns, "app", { op: "insert", id: "envoy-1", body: "learned-owner" });
  assert.equal(inserted.success, true);

  const afterSecondEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");
  assert.equal(
    afterSecondEnvoy,
    afterFirstEnvoy,
    "second D1 binding call should use the learned owner endpoint directly"
  );

  const afterRuntime = parseCounters(runtimeInternalGet("/_metrics"));
  assert.ok(
    counterDelta(
      beforeRuntime,
      afterRuntime,
      'wdl_d1_owner_hint_outcomes_total{outcome="learned",service="user-runtime"}'
    ) >= 1,
    "runtime should learn a D1 owner hint from the Envoy-routed response"
  );
  assert.ok(
    counterDelta(
      beforeRuntime,
      afterRuntime,
      'wdl_d1_owner_hint_outcomes_total{outcome="hit",service="user-runtime"}'
    ) >= 1,
    "runtime should use the learned D1 owner hint on the next call"
  );
});

test("D1 binding learns owner after Envoy-routed router forwarding", async () => {
  const ns = uniqueNs("d1envoyfwd");
  const databaseId = "main";
  const created = await adminPost(`/ns/${ns}/d1/databases`, { databaseName: databaseId });
  assertStatus(created, 201, "created");
  const physicalDatabaseId = created.json.databaseId;
  await deployAndPromote(ns, "app", {
    mainModule: "worker.js",
    modules: { "worker.js": D1_HARNESS },
    bindings: { DB: { type: "d1", databaseId } },
  });

  startD1EnvoyOwnerPair();
  try {
    const released = d1RuntimeRebalance("d1-runtime", {
      databases: [{ namespace: ns, databaseId: physicalDatabaseId }],
      target: null,
    });
    assertStatus(released, 200, "released");
    assert.equal(released.body.results[0].outcome, "released");

    const initA = d1RuntimeQuery("d1-runtime-a", {
      namespace: ns,
      databaseId: physicalDatabaseId,
      mode: "exec",
      statements: [{ sql: "create table messages (id text primary key, body text)", params: [] }],
    });
    assertStatus(initA, 200, "initA");

    const beforeRuntime = parseCounters(runtimeInternalGet("/_metrics"));
    const beforeEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");

    const first = await gatewayFetch(ns, "/app?op=insert&id=fwd-1&body=via-router");
    const firstText = await first.text();
    assert.equal(first.status, 200, firstText);
    const firstBody = responseJson({ body: firstText });
    assert.equal(firstBody.success, true);
    assert.equal(firstBody.meta.served_by, "d1-30ae925d"); // fnv1a32("d1-runtime-a")

    const afterFirstEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");
    assert.ok(afterFirstEnvoy > beforeEnvoy, "forwarded first call should hit Envoy D1 router");

    const second = await gatewayFetch(ns, "/app?op=insert&id=fwd-2&body=via-owner-hint");
    const secondText = await second.text();
    assert.equal(second.status, 200, secondText);
    const secondBody = responseJson({ body: secondText });
    assert.equal(secondBody.success, true);
    assert.equal(secondBody.meta.served_by, "d1-30ae925d"); // fnv1a32("d1-runtime-a")

    const afterSecondEnvoy = envoyStat("cluster.d1_router.upstream_rq_total");
    assert.equal(
      afterSecondEnvoy,
      afterFirstEnvoy,
      "forwarded owner hint should let the next D1 binding call skip Envoy"
    );

    const afterRuntime = parseCounters(runtimeInternalGet("/_metrics"));
    assert.ok(
      counterDelta(
        beforeRuntime,
        afterRuntime,
        'wdl_d1_owner_hint_outcomes_total{outcome="learned",service="user-runtime"}'
      ) >= 1,
      "runtime should learn the forwarded owner endpoint"
    );
    assert.ok(
      counterDelta(
        beforeRuntime,
        afterRuntime,
        'wdl_d1_owner_hint_outcomes_total{outcome="hit",service="user-runtime"}'
      ) >= 1,
      "runtime should reuse the forwarded owner endpoint"
    );
  } finally {
    stopD1EnvoyOwnerPair();
  }
});

test("D1 binding recovers after the Envoy D1 router upstream is removed and restored", async () => {
  const ns = uniqueNs("d1envoyrecover");
  await setup(ns, "app");

  stopD1Router();
  try {
    const down = await gatewayFetch(ns, "/app?op=init");
    const downText = await down.text();
    assert.ok((down.status ?? 0) >= 500, downText);
  } finally {
    startD1Router();
  }

  await waitUntil("d1-runtime restored behind Envoy", () => {
    const health = serviceInternalGet("d1-runtime", 8787, "/healthz");
    return health.status === 200;
  }, { timeoutMs: 15000, intervalMs: 500 });

  /** @type {{ count: number } | null} */
  let init = null;
  await waitUntil("D1 binding restored through Envoy", async () => {
    try {
      init = await call(ns, "app", { op: "init" });
      return init?.count === 1;
    } catch {
      return false;
    }
  }, { timeoutMs: 15000, intervalMs: 500 });
  assert.ok(init, "expected D1 binding init call to succeed after restore");
  assert.equal(/** @type {{ count: number }} */ (init).count, 1);
});

test("D1 binding wraps declared named entrypoints for service binding callers", async () => {
  const ns = uniqueNs("d1named");
  await adminPost(`/ns/${ns}/d1/databases`, {
    databaseName: "named-main",
  });
  await deployAndPromote(ns, "target", {
    mainModule: "worker.js",
    modules: { "worker.js": D1_NAMED_TARGET },
    bindings: { DB: { type: "d1", databaseId: "named-main" } },
    exports: [{ entrypoint: "Api", allowedCallers: ["*"] }],
  });
  await deployAndPromote(ns, "caller", {
    mainModule: "worker.js",
    modules: { "worker.js": D1_NAMED_CALLER },
    bindings: { API: { type: "service", service: "target", entrypoint: "Api" } },
  });

  const requestId = `rid-${ns}-named-fetch`;
  const res = await gatewayFetch(ns, "/caller/", {
    headers: { "x-request-id": requestId },
  });
  const text = await res.text();
  assert.equal(res.status, 200, text);
  const body = responseJson({ body: text });
  assert.equal(body.batch.length, 2);
  assert.equal(body.batch[0].success, true);
  assert.deepEqual(body.batch[1].results, [{ id: "named-1", body: "from-named" }]);
  assert.deepEqual(body.row, { id: "named-1", body: "from-named" });
  assert.deepEqual(body.viaFetch, {
    requestId,
    row: { id: "named-fetch", body: requestId },
  });

  await waitUntil("D1 named entrypoint request id log", () => {
    const logs = sh("docker compose logs --no-color --tail=300 d1-runtime");
    return logs.includes(`"request_id":"${requestId}"`);
  }, { timeoutMs: 10000, intervalMs: 500 });
});

test("D1 compat worker covers blob params raw edges and batch error shape", async () => {
  const ns = uniqueNs("d1compat");
  await adminPost(`/ns/${ns}/d1/databases`, {
    databaseName: "compat-main",
  });
  await deployAndPromote(ns, "compat", {
    mainModule: "worker.js",
    modules: { "worker.js": D1_COMPAT_WORKER },
    bindings: { DB: { type: "d1", databaseId: "compat-main" } },
  });

  assert.deepEqual(await call(ns, "compat", { op: "blob" }), {
    rows: [
      { id: "array-buffer", hex: "0001027FFF", size: 5 },
      { id: "typed-array", hex: "141E", size: 2 },
    ],
    selectedBlob: { isUint8Array: true, bytes: [0, 1, 2, 127, 255] },
    rawBlob: { isUint8Array: true, bytes: [20, 30] },
  });

  assert.deepEqual(await call(ns, "compat", { op: "raw" }), {
    defaultRows: [["r1", 1], ["r2", null]],
    namedRows: [["id", "value"], ["r1", 1], ["r2", null]],
    emptyRows: [],
    emptyNamedRows: [["id", "value"]],
  });

  const failedBatch = await call(ns, "compat", { op: "batch-error" });
  assert.equal(failedBatch.error.name, "D1_ERROR");
  assert.equal(failedBatch.error.code, "batch-statement-error");
  assert.equal(failedBatch.error.category, "sql");
  assert.equal(failedBatch.error.retryable, false);
  assert.equal(failedBatch.error.statementIndex, 1);
  assert.equal(failedBatch.error.causeCode, "sql-error");
  assert.match(failedBatch.error.message, /D1 batch statement 1 failed/);
  assert.deepEqual(failedBatch.rows, []);
});

test("D1 batch is transactional", async () => {
  const ns = uniqueNs("d1tx");
  await setup(ns, "app");

  await call(ns, "app", { op: "init" });
  const result = await call(ns, "app", { op: "batch-fail", id: "tx1", body: "rolled-back" });
  assert.equal(result.error, "d1_batch_failed");
  assert.match(result.message, /missing_table/);
  assert.equal(result.row, null);
});

test("D1 binding isolates same databaseId across namespaces", async () => {
  const nsA = uniqueNs("d1a");
  const nsB = uniqueNs("d1b");
  await setup(nsA, "app");
  await setup(nsB, "app");

  await call(nsA, "app", { op: "init" });
  await call(nsA, "app", { op: "insert", id: "m2", body: "from-a" });

  await call(nsB, "app", { op: "init" });
  assert.equal(await call(nsB, "app", { op: "get", id: "m2" }), null);
});
