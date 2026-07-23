// WDL Workflows scheduler paths: replica claiming, detached admission, and suspended replay.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  WORKER_CODE,
  composeScale,
  delay,
  deployAndPromote,
  gatewayFetch,
  redisSAdd,
  redisCommandCalls,
  redisSIsMember,
  redisScriptFlush,
  redisWorkflowStateHGet,
  redisWorkflowStateHSet,
  redisZScore,
  readIntegrationJson,
  responseJson,
  serviceInternalPost,
  setupIntegrationSuite,
  uniqueNs,
  waitUntil,
  withServiceStopped,
  workflowReadyShard,
  workflowReadyToken,
  workerMeta,
} from "./helpers/workflows-scenarios.js";

setupIntegrationSuite();

test("workflow scripts use EVALSHA and recover after SCRIPT FLUSH", async () => {
  const ns = uniqueNs("wfscript");
  await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  await withServiceStopped("scheduler", async () => {
    const evalshaBefore = redisCommandCalls("evalsha");
    const warmed = await gatewayFetch(ns, "/shop/create?id=script-warm");
    await readIntegrationJson(warmed, 200, "workflow response");
    assert.ok(redisCommandCalls("evalsha") > evalshaBefore);

    redisScriptFlush();
    const loadsBefore = redisCommandCalls("script load");
    const afterFlush = await gatewayFetch(ns, "/shop/create?id=script-after-flush");
    await readIntegrationJson(afterFlush, 200, "workflow response");
    assert.ok(redisCommandCalls("script load") > loadsBefore);
  });
});

test("scheduler replicas: workflow tick claims and completes one queued instance", async () => {
  const ns = uniqueNs("wfreplica");
  await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  await withServiceStopped("scheduler", async () => {
    const created = await gatewayFetch(ns, "/shop/create?id=replica-1");
    const createdBody = await readIntegrationJson(created, 200, "workflow response");
    assert.equal(createdBody.id, "replica-1");
    assert.ok(["queued", "running"].includes(createdBody.status.status));

    composeScale("scheduler", 2);

    /** @type {any} */
    let completedBody;
    await waitUntil("scheduler replicas complete workflow once", async () => {
      const status = await gatewayFetch(ns, "/shop/steps?id=replica-1");
      completedBody = await readIntegrationJson(status, 200, "workflow response");
      return completedBody.status === "completed";
    }, { timeoutMs: 60000, intervalMs: 250 });
    assert.deepEqual(completedBody.steps.entries.map((/** @type {any} */ entry) => ({
      ordinal: entry.ordinal,
      name: entry.name,
      status: entry.status,
      attempt: entry.attempt,
    })), [
      { ordinal: 0, name: "record", status: "completed", attempt: 1 },
    ]);

    await delay(2_000);
    const final = await gatewayFetch(ns, "/shop/steps?id=replica-1");
    const finalBody = await readIntegrationJson(final, 200, "workflow response");
    assert.equal(finalBody.status, "completed");
    assert.equal(finalBody.steps.entries.length, 1);
  });
});

test("missing workflow params fail before the run is claimed", async () => {
  const ns = uniqueNs("wfparams");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const workflowKey = workerMeta(ns, "shop", version).workflows[0].workflowKey;
  const instanceId = "missing-params";

  await withServiceStopped("scheduler", async () => {
    const created = await gatewayFetch(ns, `/shop/create?id=${instanceId}`);
    await readIntegrationJson(created, 200, "workflow response");
    redisWorkflowStateHSet(ns, workflowKey, instanceId, [
      "paramsRef",
      "missing-params-payload",
    ]);

    const tick = serviceInternalPost("workflows", 9120, "/internal/workflows/tick", {});
    assert.equal(tick.status, 500, tick.body);
    assert.equal(responseJson(tick).error, "workflow_payload_missing");
    assert.equal(redisWorkflowStateHGet(ns, workflowKey, instanceId, "status"), "queued");
    assert.equal(redisWorkflowStateHGet(ns, workflowKey, instanceId, "runToken"), "");
  });
});

test("later workflow ticks admit new runs while a slow run remains active", async () => {
  const ns = uniqueNs("wfadmit");
  await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  await withServiceStopped("scheduler", async () => {
    const created = await gatewayFetch(ns, "/shop/create?id=slow-admission&runDelayMs=12000");
    await readIntegrationJson(created, 200, "workflow response");

    const startedAt = performance.now();
    const tick = serviceInternalPost("workflows", 9120, "/internal/workflows/tick", {});
    const elapsedMs = performance.now() - startedAt;
    assert.equal(tick.status, 200, tick.body);
    assert.equal(responseJson(tick).workflowAdmitted >= 1, true);
    assert.equal(elapsedMs < 3000, true, `tick waited ${elapsedMs}ms for the workflow run`);

    await waitUntil("slow admitted workflow enters running state", async () => {
      const status = await gatewayFetch(ns, "/shop/get?id=slow-admission");
      return (await readIntegrationJson(status, 200, "workflow response")).status === "running";
    }, { timeoutMs: 8000, intervalMs: 100 });

    const fastCreated = await gatewayFetch(ns, "/shop/create?id=fast-admission");
    await readIntegrationJson(fastCreated, 200, "workflow response");
    const slowBeforeNextTick = await gatewayFetch(ns, "/shop/get?id=slow-admission");
    assert.equal(
      (await readIntegrationJson(slowBeforeNextTick, 200, "workflow response")).status,
      "running",
    );
    const nextTick = serviceInternalPost("workflows", 9120, "/internal/workflows/tick", {});
    assert.equal(nextTick.status, 200, nextTick.body);
    assert.equal(responseJson(nextTick).workflowAdmitted >= 1, true);

    await waitUntil("later tick completes fast workflow before the slow run", async () => {
      const status = await gatewayFetch(ns, "/shop/get?id=fast-admission");
      return (await readIntegrationJson(status, 200, "workflow response")).status === "completed";
    }, { timeoutMs: 8000, intervalMs: 100 });
    const slowAfterFast = await gatewayFetch(ns, "/shop/get?id=slow-admission");
    assert.equal(
      (await readIntegrationJson(slowAfterFast, 200, "workflow response")).status,
      "running",
    );
    await waitUntil("slow admitted workflow completes in the background", async () => {
      const status = await gatewayFetch(ns, "/shop/get?id=slow-admission");
      return (await readIntegrationJson(status, 200, "workflow response")).status === "completed";
    }, { timeoutMs: 20000, intervalMs: 100 });
  });
});

test("scheduler replicas: due sleeping workflow resumes once", async () => {
  const ns = uniqueNs("wfdue");
  await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  const created = await gatewayFetch(ns, "/shop/create?id=due-1&sleepMs=3500");
  await readIntegrationJson(created, 200, "workflow response");
  await waitUntil("workflow reaches sleep waiting state before due replica claim", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=due-1");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "settle" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });

  await withServiceStopped("scheduler", async () => {
    await delay(3800);
    composeScale("scheduler", 2);

    /** @type {any} */
    let completedBody;
    await waitUntil("scheduler replicas resume due sleeping workflow once", async () => {
      const status = await gatewayFetch(ns, "/shop/steps?id=due-1");
      completedBody = await readIntegrationJson(status, 200, "workflow response");
      return completedBody.status === "completed";
    }, { timeoutMs: 60000, intervalMs: 250 });
    assert.deepEqual(completedBody.output, {
      slept: true,
      instanceId: "due-1",
      fromEnv: "runtime-ok",
    });
    assert.deepEqual(completedBody.steps.entries.map((/** @type {any} */ entry) => ({
      ordinal: entry.ordinal,
      name: entry.name,
      status: entry.status,
      attempt: entry.attempt,
    })), [
      { ordinal: 0, name: "settle", status: "completed", attempt: 1 },
      { ordinal: 1, name: "after-sleep", status: "completed", attempt: 1 },
    ]);

    await delay(2000);
    const final = await gatewayFetch(ns, "/shop/steps?id=due-1");
    const finalBody = await readIntegrationJson(final, 200, "workflow response");
    assert.equal(finalBody.status, "completed");
    assert.equal(finalBody.steps.entries.length, 2);
  });
});

test("sleep replay keeps waiting state and clears stale ready hints", async () => {
  const ns = uniqueNs("wfcachewait");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const meta = workerMeta(ns, "shop", version);
  const workflowKey = meta.workflows[0].workflowKey;
  const instanceId = "cached-waiting-1";

  const created = await gatewayFetch(ns, `/shop/create?id=${instanceId}&sleepMs=60000`);
  await readIntegrationJson(created, 200, "workflow response");
  await waitUntil("workflow reaches future sleep waiting state before replay", async () => {
    const status = await gatewayFetch(ns, `/shop/steps?id=${instanceId}`);
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "settle" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });

  const token = workflowReadyToken(ns, workflowKey, instanceId);
  const shard = workflowReadyShard(ns, workflowKey, instanceId);
  const readyKey = `wf:ready:${shard}`;
  const dueKey = `wf:due:${shard}`;
  assert.equal(redisZScore(dueKey, token, { db: 2 }) !== null, true);

  async function tickSuspendedSleep() {
    redisSAdd(readyKey, token, { db: 2 });
    redisSAdd("wf:ready:active", String(shard), { db: 2 });
    const tick = serviceInternalPost(
      "workflows",
      9120,
      "/internal/workflows/tick",
      {},
    );
    assert.equal(tick.status, 200, tick.body);
    const tickBody = responseJson(tick);
    assert.equal(tickBody.workflowAdmitted >= 1, true);
    await waitUntil("sleep replay completes after tick admission", () =>
      redisWorkflowStateHGet(ns, workflowKey, instanceId, "status") === "waiting" &&
      redisWorkflowStateHGet(ns, workflowKey, instanceId, "runToken") === "" &&
      redisWorkflowStateHGet(ns, workflowKey, instanceId, "runLeaseExpiresAtMs") === "" &&
      !redisSIsMember(readyKey, token, { db: 2 }) &&
      redisZScore(dueKey, token, { db: 2 }) !== null,
    { timeoutMs: 10000, intervalMs: 100 });
  }

  await withServiceStopped("scheduler", async () => {
    await tickSuspendedSleep();
    await tickSuspendedSleep();
  });
});
