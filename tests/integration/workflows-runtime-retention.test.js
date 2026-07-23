// WDL Workflows retention, frozen-version, and delete-blocker paths.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  WORKER_CODE,
  adminFetch,
  composeRestart,
  composeScale,
  deployAndPromote,
  gatewayFetch,
  gatewayWorkerId,
  redisDel,
  redisSetEx,
  redisWorkflowStateHDel,
  redisWorkflowStateHGet,
  redisWorkflowStateHSet,
  readIntegrationJson,
  responseJson,
  runtimeDispatchPost,
  serviceInternalGet,
  serviceInternalPost,
  setupIntegrationSuite,
  uniqueNs,
  waitUntil,
  withServiceStopped,
  workflowRetentionKey,
  workerMeta,
} from "./helpers/workflows-scenarios.js";
import { redisZAdd, redisZScore } from "./helpers/redis.js";

setupIntegrationSuite();

test("stale workflow run cannot commit after restart generation changes", async () => {
  const ns = uniqueNs("wfstale");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const meta = workerMeta(ns, "shop", version);
  const workflowKey = meta.workflows[0].workflowKey;

  const created = await gatewayFetch(ns, "/shop/create?id=stale-1&dynamicStepName=original");
  await readIntegrationJson(created, 200, "workflow response");
  /** @type {any} */
  let firstCompleted;
  await waitUntil("workflow completes before stale generation replay", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=stale-1");
    firstCompleted = await readIntegrationJson(status, 200, "workflow response");
    return firstCompleted.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.deepEqual(firstCompleted.output, { name: "original", fromEnv: "runtime-ok" });

  const createdAtMs = Number(redisWorkflowStateHGet(ns, workflowKey, "stale-1", "createdAtMs"));
  await withServiceStopped("scheduler", async () => {
    const restarted = await gatewayFetch(ns, "/shop/restart?id=stale-1");
    const restartedBody = await readIntegrationJson(restarted, 200, "workflow response");
    assert.equal(restartedBody.status, "queued");
    assert.equal(redisWorkflowStateHGet(ns, workflowKey, "stale-1", "generation"), "2");
    redisWorkflowStateHSet(ns, workflowKey, "stale-1", [
      "runToken",
      "stale-generation-token",
      "runLeaseExpiresAtMs",
      String(Date.now() + 60_000),
    ]);

    const staleReplay = runtimeDispatchPost(
      "/internal/workflows/run",
      { "x-worker-id": gatewayWorkerId(ns, "shop", version) },
      {
        ns,
        worker: "shop",
        frozenVersion: version,
        workflowName: "orders",
        workflowKey,
        className: "OrderWorkflow",
        instanceId: "stale-1",
        generation: 1,
        createdAtMs,
        runToken: "stale-generation-token",
        params: { source: "integration", id: "stale-1", dynamicStepName: "stale-overwrite" },
      }
    );
    assert.equal(staleReplay.status, 200, staleReplay.body);
    const staleReplayBody = responseJson(staleReplay);
    assert.equal(staleReplayBody.outcome, "failed");
    assert.equal(redisWorkflowStateHGet(ns, workflowKey, "stale-1", "generation"), "2");
    assert.equal(redisWorkflowStateHGet(ns, workflowKey, "stale-1", "status"), "queued");
    redisWorkflowStateHDel(ns, workflowKey, "stale-1", [
      "runToken",
      "runLeaseExpiresAtMs",
    ]);

    composeScale("scheduler", 2);
    /** @type {any} */
    let completedBody;
    await waitUntil("restarted workflow completes without stale overwrite", async () => {
      const status = await gatewayFetch(ns, "/shop/steps?id=stale-1");
      completedBody = await readIntegrationJson(status, 200, "workflow response");
      return completedBody.status === "completed";
    }, { timeoutMs: 60000, intervalMs: 250 });
    assert.deepEqual(completedBody.output, { name: "original", fromEnv: "runtime-ok" });
    assert.deepEqual(completedBody.steps.entries.map((/** @type {any} */ entry) => ({
      ordinal: entry.ordinal,
      name: entry.name,
      status: entry.status,
      attempt: entry.attempt,
    })), [
      { ordinal: 0, name: "original", status: "completed", attempt: 1 },
    ]);
  });
});

test("workflow waiting state survives workflows restart", async () => {
  const ns = uniqueNs("wfrestart");
  await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  const created = await gatewayFetch(ns, "/shop/create?id=restart-wait&wait=1&waitTimeoutMs=30000");
  await readIntegrationJson(created, 200, "workflow response");
  await waitUntil("workflow reaches wait state before workflows restart", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=restart-wait");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });

  composeRestart("workflows");

  const sent = await gatewayFetch(ns, "/shop/event?id=restart-wait&message=after-restart");
  await readIntegrationJson(sent, 200, "workflow response");
  /** @type {any} */
  let completed;
  await waitUntil("workflow resumes after workflows restart", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=restart-wait");
    completed = await readIntegrationJson(status, 200, "workflow response");
    return completed.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(completed);
  assert.deepEqual(completed.output, { message: "after-restart", fromEnv: "runtime-ok" });
});

test("in-flight workflow keeps frozen worker version after promote", async () => {
  const ns = uniqueNs("wffrozen");
  const version1 = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-v1" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  const created = await gatewayFetch(ns, "/shop/create?id=frozen-1&waitVersion=1");
  await readIntegrationJson(created, 200, "workflow response");
  await waitUntil("workflow reaches wait state before promote", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=frozen-1");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });

  const version2 = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-v2" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  assert.notEqual(version2, version1);
  const meta = workerMeta(ns, "shop", version2);
  const workflowKey = meta.workflows[0].workflowKey;
  const staleCreate = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/create",
    {
      ns,
      worker: "shop",
      frozenVersion: version1,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      instanceId: "active-pin-1",
      params: { source: "old-isolate" },
    },
  );
  assert.equal(staleCreate.status, 200, staleCreate.body);
  assert.equal(
    redisWorkflowStateHGet(ns, workflowKey, "active-pin-1", "frozenVersion"),
    version2,
  );

  const sent = await gatewayFetch(ns, "/shop/event?id=frozen-1&message=promoted");
  await readIntegrationJson(sent, 200, "workflow response");
  /** @type {any} */
  let completed;
  await waitUntil("workflow resumes on frozen worker version after promote", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=frozen-1");
    completed = await readIntegrationJson(status, 200, "workflow response");
    return completed.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(completed);
  assert.deepEqual(completed.output, {
    message: "promoted",
    sentFromEnv: "runtime-v2",
    runFromEnv: "runtime-v1",
  });

  await deployAndPromote(ns, "shop", {
    code: `export default { async fetch() { return new Response("ok"); } };`,
    vars: { LABEL: "runtime-v3" },
  });
  const removedWorkflowCreate = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/create",
    {
      ns,
      worker: "shop",
      frozenVersion: version2,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      instanceId: "removed-workflow-1",
      params: { source: "old-isolate" },
    },
  );
  assert.notEqual(removedWorkflowCreate.status, 200, removedWorkflowCreate.body);
  assert.equal(responseJson(removedWorkflowCreate).error, "workflow_not_exported");
});

test("workflow instances block worker deletion and worker-delete lock blocks new workflow creation", async () => {
  const ns = uniqueNs("wfdel");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const meta = workerMeta(ns, "shop", version);
  const workflowKey = meta.workflows[0].workflowKey;

  const created = await gatewayFetch(ns, "/shop/create?id=delete-blocker-1&wait=1");
  await readIntegrationJson(created, 200, "workflow response");

  const deleted = await adminFetch(`/ns/${ns}/worker/shop/delete`, { method: "POST" });
  const deletedBody = await readIntegrationJson(deleted, 409, "workflow delete response");
  assert.equal(deletedBody.error, "workflow_instances_active");
  assert.equal(deletedBody.count, 1);
  assert.deepEqual(deletedBody.blockers, [{
    workflowKey,
    instanceId: "delete-blocker-1",
  }]);

  redisSetEx(`worker-delete-lock:${ns}:shop`, "test-delete-token", 30);
  try {
    const locked = serviceInternalPost(
      "workflows",
      9120,
      "/internal/workflows/create",
      {
        ns,
        worker: "shop",
        frozenVersion: version,
        workflowName: "orders",
        workflowKey,
        className: "OrderWorkflow",
        instanceId: "delete-race-1",
        params: { source: "integration" },
      },
    );
    assert.equal(locked.status, 409, locked.body);
    assert.equal(responseJson(locked).error, "workflow_worker_deleting");
  } finally {
    redisDel(`worker-delete-lock:${ns}:shop`);
  }
});

test("terminal workflow retention batches cleanup and releases delete blockers", async () => {
  const ns = uniqueNs("wfret");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  assert.ok(version);

  for (const instanceId of ["short-retention-1", "short-retention-2"]) {
    const created = await gatewayFetch(
      ns,
      `/shop/create?id=${instanceId}&retentionMs=1000`,
    );
    await readIntegrationJson(created, 200, "workflow response");
  }

  for (const instanceId of ["short-retention-1", "short-retention-2"]) {
    await waitUntil(`${instanceId} workflow completes`, async () => {
      const status = await gatewayFetch(ns, `/shop/get?id=${instanceId}`);
      const body = await readIntegrationJson(status, 200, "workflow response");
      return body.status === "completed";
    }, { timeoutMs: 60000, intervalMs: 250 });
  }

  const malformedToken = `malformed-retention-${ns}`;
  redisZAdd(workflowRetentionKey(), Date.now() - 1, malformedToken, { db: 2 });

  const blocked = await adminFetch(`/ns/${ns}/worker/shop/delete`, { method: "POST" });
  const blockedBody = await readIntegrationJson(blocked, 409, "workflow delete response");
  assert.equal(blockedBody.error, "workflow_instances_active");

  await waitUntil("retention cleanup releases workflow delete blocker", async () => {
    const deleted = await adminFetch(`/ns/${ns}/worker/shop/delete`, { method: "POST" });
    if (deleted.status === 200) return true;
    assert.equal(deleted.status, 409, await deleted.text());
    return false;
  }, { timeoutMs: 60000, intervalMs: 500 });
  assert.equal(redisZScore(workflowRetentionKey(), malformedToken, { db: 2 }), null);

  const metrics = serviceInternalGet("workflows", 9120, "/_metrics").body;
  assert.match(metrics, /wdl_workflow_retention_cleaned_total\{outcome="cleaned"\} [1-9][0-9]*/);
});
