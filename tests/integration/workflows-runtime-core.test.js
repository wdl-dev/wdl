// WDL Workflows runtime core path: loaded worker facade reaches workflows.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  KV_FACADE_RPC_METHOD,
  KV_READ_INFRASTRUCTURE_ERROR_CODE,
  WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
} from "../../runtime/infrastructure-error.js";
import { readRepositoryModuleSource } from "../helpers/load-shared-module.js";
import { prometheusCounter } from "./helpers/prometheus.js";
import {
  WORKER_CODE,
  deployAndPromote,
  dispatchWorkflowReplay,
  gatewayFetch,
  gatewayWorkerId,
  redisSMembers,
  redisWorkflowStateHDel,
  redisWorkflowStateHSet,
  redisZAdd,
  redisZScore,
  readIntegrationJson,
  responseJson,
  runtimeDispatchPost,
  serviceInternalGet,
  serviceInternalPost,
  serviceInternalPostLarge,
  setWorkflowRunningState,
  setupIntegrationSuite,
  uniqueNs,
  waitUntil,
  workflowEventTypeIndexKey,
  workerMeta,
} from "./helpers/workflows-scenarios.js";

setupIntegrationSuite();

const WORKFLOW_KV_PROVENANCE_PROBE_CODE = readRepositoryModuleSource(
  "test-workers/workflow-kv-provenance/src/index.js",
  [
    ["__WDL_KV_FACADE_RPC_METHOD__", KV_FACADE_RPC_METHOD],
    [
      "__WDL_KV_READ_INFRASTRUCTURE_ERROR_CODE__",
      KV_READ_INFRASTRUCTURE_ERROR_CODE,
    ],
    [
      "__WDL_WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN__",
      WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
    ],
  ]
);

test("workflow binding creates and reads an instance through workflows", async () => {
  const ns = uniqueNs("wfrt");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "runtime-ok" },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const meta = workerMeta(ns, "shop", version);
  const workflowKey = meta.workflows[0].workflowKey;

  const created = await gatewayFetch(ns, "/shop/create?id=order-123");
  const createdBody = await readIntegrationJson(created, 200, "workflow response");
  assert.equal(createdBody.id, "order-123");
  assert.ok(["queued", "running"].includes(createdBody.status.status));

  const fetched = await gatewayFetch(ns, "/shop/get?id=order-123");
  const fetchedBody = await readIntegrationJson(fetched, 200, "workflow response");
  assert.equal(fetchedBody.id, "order-123");
  assert.ok(["queued", "running"].includes(fetchedBody.status));
  assert.equal(fetchedBody.output, null);
  assert.equal(fetchedBody.error, null);

  const batch = await gatewayFetch(ns, "/shop/batch");
  assert.deepEqual(await readIntegrationJson(batch, 200, "workflow response"), { ids: ["batch-a", "batch-b"] });

  const mixedBatch = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/create-batch",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      entries: [
        { instanceId: "batch-a", params: { n: 4 } },
        { instanceId: "batch-c", params: { n: 5 } },
        { instanceId: "batch-c", params: { n: 6 } },
        { instanceId: "batch-d", params: { n: 7 } },
      ],
    },
  );
  assert.equal(mixedBatch.status, 200, mixedBatch.body);
  assert.deepEqual(
    responseJson(mixedBatch).instances.map((/** @type {any} */ instance) => instance.id),
    ["batch-c", "batch-d"],
  );

  const existingBatch = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/create-batch",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      entries: [
        { instanceId: "batch-a" },
        { instanceId: "batch-c" },
        { instanceId: "batch-d" },
      ],
    },
  );
  assert.equal(existingBatch.status, 200, existingBatch.body);
  assert.deepEqual(responseJson(existingBatch).instances, []);

  const firstPage = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/instances",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      options: { limit: 2 },
    },
  );
  assert.equal(firstPage.status, 200, firstPage.body);
  const firstPageBody = responseJson(firstPage);
  assert.deepEqual(firstPageBody.instances.map((/** @type {any} */ entry) => entry.id), ["order-123", "batch-a"]);
  assert.equal(firstPageBody.cursor, "2");

  const secondPage = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/instances",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      options: { limit: 2, cursor: firstPageBody.cursor },
    },
  );
  assert.equal(secondPage.status, 200, secondPage.body);
  const secondPageBody = responseJson(secondPage);
  assert.deepEqual(secondPageBody.instances.map((/** @type {any} */ entry) => entry.id), ["batch-b", "batch-c"]);
  assert.equal(secondPageBody.cursor, "4");

  const thirdPage = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/instances",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      options: { limit: 2, cursor: secondPageBody.cursor },
    },
  );
  assert.equal(thirdPage.status, 200, thirdPage.body);
  const thirdPageBody = responseJson(thirdPage);
  assert.deepEqual(thirdPageBody.instances.map((/** @type {any} */ entry) => entry.id), ["batch-d"]);
  assert.equal(thirdPageBody.cursor, null);

  assert.ok(workflowKey);

  const oversizedParams = await serviceInternalPostLarge(
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
      instanceId: "oversized-params",
      params: { blob: "x".repeat(1024 * 1024) },
    },
  );
  assert.equal(oversizedParams.status, 413, oversizedParams.body);
  assert.equal(responseJson(oversizedParams).error, "request_too_large");

  const oversizedBatch = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/create-batch",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      entries: Array.from({ length: 101 }, (_, i) => ({ instanceId: `batch-over-${i}` })),
    },
  );
  assert.equal(oversizedBatch.status, 413, oversizedBatch.body);
  assert.equal(responseJson(oversizedBatch).error, "request_too_large");

  const oversizedResult = await gatewayFetch(ns, "/shop/create?id=oversized-result&largeStepResult=1");
  await readIntegrationJson(oversizedResult, 200, "workflow response");
  /** @type {any} */
  let oversizedResultBody;
  await waitUntil("oversized workflow step result fails closed", async () => {
    const failed = await gatewayFetch(ns, "/shop/get?id=oversized-result");
    oversizedResultBody = await readIntegrationJson(failed, 200, "workflow response");
    return oversizedResultBody.status === "failed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.equal(oversizedResultBody.error.name, "workflow_payload_too_large");

  /** @type {any} */
  let completedBody;
  await waitUntil("scheduler completes workflow instances", async () => {
    const completed = await gatewayFetch(ns, "/shop/get?id=order-123");
    completedBody = await readIntegrationJson(completed, 200, "workflow response");
    return completedBody.status === "completed";
  });
  assert.ok(completedBody);
  assert.deepEqual(completedBody, {
    id: "order-123",
    status: "completed",
    output: {
      instanceId: "order-123",
      fromEnv: "runtime-ok",
      nonce: completedBody.output.nonce,
    },
    error: null,
  });
  assert.equal(typeof completedBody.output.nonce, "string");

  const mismatch = await gatewayFetch(ns, "/shop/create?id=mismatch-1&dynamicStepName=first-name");
  await readIntegrationJson(mismatch, 200, "workflow response");
  /** @type {any} */
  let mismatchBody;
  await waitUntil("scheduler completes dynamic-step workflow before mismatch replay", async () => {
    const completed = await gatewayFetch(ns, "/shop/get?id=mismatch-1");
    mismatchBody = await readIntegrationJson(completed, 200, "workflow response");
    return mismatchBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(mismatchBody);
  assert.deepEqual(mismatchBody.output, { name: "first-name", fromEnv: "runtime-ok" });

  // Directly replay the completed instance under a fresh run claim so the
  // replay-shape mismatch, not terminal-state fencing, is the only failure.
  setWorkflowRunningState(ns, workflowKey, "mismatch-1", "run-mismatch");
  try {
    const mismatchedReplay = dispatchWorkflowReplay(
      ns,
      workflowKey,
      version,
      "mismatch-1",
      "run-mismatch",
      { source: "integration", id: "mismatch-1", dynamicStepName: "second-name" }
    );
    assert.equal(mismatchedReplay.status, 200, mismatchedReplay.body);
    const mismatchedReplayBody = responseJson(mismatchedReplay);
    assert.equal(mismatchedReplayBody.outcome, "failed");
    assert.equal(mismatchedReplayBody.error.name, "workflow_step_mismatch");
  } finally {
    redisWorkflowStateHSet(ns, workflowKey, "mismatch-1", [
      "status",
      "completed",
      "updatedAtMs",
      String(Date.now()),
    ]);
    redisWorkflowStateHDel(ns, workflowKey, "mismatch-1", ["runToken", "runLeaseExpiresAtMs"]);
  }

  const steps = await gatewayFetch(ns, "/shop/steps?id=order-123");
  const stepsBody = await readIntegrationJson(steps, 200, "workflow response");
  assert.deepEqual(stepsBody.steps, {
    entries: [{
      ordinal: 0,
      name: "record",
      nameCount: 1,
      status: "completed",
      attempt: 1,
      dependencies: [],
      hasOutput: true,
      hasError: false,
      completedAtMs: stepsBody.steps.entries[0].completedAtMs,
    }],
    truncated: false,
  });
  assert.equal(typeof stepsBody.steps.entries[0].completedAtMs, "number");

  setWorkflowRunningState(ns, workflowKey, "order-123", "run-direct");
  try {
    const replay = dispatchWorkflowReplay(
      ns,
      workflowKey,
      version,
      "order-123",
      "run-direct",
      { source: "integration", id: "order-123" }
    );
    assert.equal(replay.status, 200, replay.body);
    const replayBody = responseJson(replay);
    assert.equal(replayBody.outcome, "completed");
    assert.deepEqual(replayBody.output, completedBody.output);
  } finally {
    redisWorkflowStateHSet(ns, workflowKey, "order-123", [
      "status",
      "completed",
      "updatedAtMs",
      String(Date.now()),
    ]);
    redisWorkflowStateHDel(ns, workflowKey, "order-123", ["runToken", "runLeaseExpiresAtMs"]);
  }

  const lifecycleMetricsBefore = serviceInternalGet("workflows", 9120, "/_metrics").body;
  const restarted = await gatewayFetch(ns, "/shop/restart?id=order-123");
  const restartedBody = await readIntegrationJson(restarted, 200, "workflow response");
  assert.equal(restartedBody.status, "queued");
  assert.equal(restartedBody.output, null);
  /** @type {any} */
  let restartedCompletedBody;
  await waitUntil("scheduler reruns restarted workflow from ordinal zero", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=order-123");
    restartedCompletedBody = await readIntegrationJson(status, 200, "workflow response");
    return restartedCompletedBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(restartedCompletedBody);
  assert.equal(restartedCompletedBody.output.instanceId, "order-123");
  assert.equal(restartedCompletedBody.output.fromEnv, "runtime-ok");
  assert.notEqual(restartedCompletedBody.output.nonce, completedBody.output.nonce);
  assert.deepEqual(restartedCompletedBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "record", status: "completed", attempt: 1 },
  ]);

  /** @type {any} */
  let batchABody;
  await waitUntil("scheduler completes batch workflow instance", async () => {
    const batchA = await gatewayFetch(ns, "/shop/get?id=batch-a");
    batchABody = await readIntegrationJson(batchA, 200, "workflow response");
    return batchABody.status === "completed";
  });
  assert.ok(batchABody);
  assert.deepEqual(batchABody, {
    id: "batch-a",
    status: "completed",
    output: {
      instanceId: null,
      fromEnv: "runtime-ok",
      nonce: batchABody.output.nonce,
    },
    error: null,
  });
  assert.equal(typeof batchABody.output.nonce, "string");

  const failed = await gatewayFetch(ns, "/shop/create?id=fail-1&fail=1");
  await readIntegrationJson(failed, 200, "workflow response");
  /** @type {any} */
  let failedBody;
  await waitUntil("scheduler records failed workflow step", async () => {
    const failedStatus = await gatewayFetch(ns, "/shop/steps?id=fail-1");
    failedBody = await readIntegrationJson(failedStatus, 200, "workflow response");
    return failedBody.status === "failed";
  });
  assert.ok(failedBody);
  assert.equal(failedBody.error.name, "Error");
  assert.equal(failedBody.error.message, "workflow boom");
  assert.deepEqual(failedBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
    failedAtType: typeof entry.failedAtMs,
  })), [{
    ordinal: 0,
    name: "record",
    status: "failed",
    attempt: 1,
    failedAtType: "number",
  }]);

  const sleepy = await gatewayFetch(ns, "/shop/create?id=sleep-1&sleepMs=300");
  await readIntegrationJson(sleepy, 200, "workflow response");
  /** @type {any} */
  let sleepyBody;
  await waitUntil("scheduler resumes sleeping workflow step", async () => {
    const sleepyStatus = await gatewayFetch(ns, "/shop/steps?id=sleep-1");
    sleepyBody = await readIntegrationJson(sleepyStatus, 200, "workflow response");
    return sleepyBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(sleepyBody);
  assert.deepEqual(sleepyBody.output, {
    slept: true,
    instanceId: "sleep-1",
    fromEnv: "runtime-ok",
  });
  assert.deepEqual(sleepyBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "settle", status: "completed", attempt: 1 },
    { ordinal: 1, name: "after-sleep", status: "completed", attempt: 1 },
  ]);

  const manySteps = await gatewayFetch(ns, "/shop/create?id=many-steps&manySteps=1");
  await readIntegrationJson(manySteps, 200, "workflow response");
  /** @type {any} */
  let manyStepsBody;
  await waitUntil("scheduler completes many-step workflow", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=many-steps&limit=2");
    manyStepsBody = await readIntegrationJson(status, 200, "workflow response");
    return manyStepsBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(manyStepsBody);
  assert.equal(manyStepsBody.steps.truncated, true);
  assert.deepEqual(manyStepsBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
  })), [
    { ordinal: 1, name: "many-1", status: "completed" },
    { ordinal: 2, name: "many-2", status: "completed" },
  ]);

  const parallelSteps = await gatewayFetch(ns, "/shop/create?id=parallel-steps&parallelSteps=1");
  await readIntegrationJson(parallelSteps, 200, "workflow response");
  /** @type {any} */
  let parallelStepsBody;
  await waitUntil("scheduler completes parallel-step workflow", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=parallel-steps");
    parallelStepsBody = await readIntegrationJson(status, 200, "workflow response");
    return parallelStepsBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(parallelStepsBody);
  assert.deepEqual(parallelStepsBody.output, {
    root: { name: "root", fromEnv: "runtime-ok" },
    parallel: [
      { name: "a", root: "root", fromEnv: "runtime-ok" },
      { name: "b", root: "root", fromEnv: "runtime-ok" },
      { name: "c", root: "root", fromEnv: "runtime-ok" },
    ],
    joins: [
      { names: ["a", "b"], fromEnv: "runtime-ok" },
      { names: ["b", "c"], fromEnv: "runtime-ok" },
    ],
    joined: { names: ["a+b", "b+c"], fromEnv: "runtime-ok" },
  });
  assert.deepEqual(parallelStepsBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    dependencies: entry.dependencies,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "dag-root", dependencies: [], status: "completed", attempt: 1 },
    { ordinal: 1, name: "parallel-a", dependencies: [0], status: "completed", attempt: 1 },
    { ordinal: 2, name: "parallel-b", dependencies: [0], status: "completed", attempt: 1 },
    { ordinal: 3, name: "parallel-c", dependencies: [0], status: "completed", attempt: 1 },
    { ordinal: 4, name: "join-ab", dependencies: [1, 2, 3], status: "completed", attempt: 1 },
    { ordinal: 5, name: "join-bc", dependencies: [1, 2, 3], status: "completed", attempt: 1 },
    { ordinal: 6, name: "final-join", dependencies: [4, 5], status: "completed", attempt: 1 },
  ]);

  const retrying = await gatewayFetch(ns, "/shop/create?id=retry-1&retry=1");
  await readIntegrationJson(retrying, 200, "workflow response");
  /** @type {any} */
  let retryBody;
  await waitUntil("scheduler retries workflow step once", async () => {
    const retryStatus = await gatewayFetch(ns, "/shop/steps?id=retry-1");
    retryBody = await readIntegrationJson(retryStatus, 200, "workflow response");
    return retryBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(retryBody);
  assert.deepEqual(retryBody.output, { attempt: 2, fromEnv: "runtime-ok" });
  assert.deepEqual(retryBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "flaky", status: "completed", attempt: 2 },
  ]);

  const nonRetryable = await gatewayFetch(ns, "/shop/create?id=nonretry-1&nonRetryable=1");
  await readIntegrationJson(nonRetryable, 200, "workflow response");
  /** @type {any} */
  let nonRetryableBody;
  await waitUntil("scheduler records non-retryable workflow step failure", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=nonretry-1");
    nonRetryableBody = await readIntegrationJson(status, 200, "workflow response");
    return nonRetryableBody.status === "failed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(nonRetryableBody);
  assert.equal(nonRetryableBody.error.name, "NonRetryableError");
  assert.equal(nonRetryableBody.error.message, "fatal validation");
  assert.deepEqual(nonRetryableBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    {
      ordinal: 0,
      name: "non-retryable",
      status: "failed",
      attempt: 1,
    },
  ]);

  const waitBeforeEvent = await gatewayFetch(ns, "/shop/create?id=wait-1&wait=1");
  await readIntegrationJson(waitBeforeEvent, 200, "workflow response");
  let finalWaitingBody;
  await waitUntil("workflow waits for an external event", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=wait-1");
    finalWaitingBody = await readIntegrationJson(status, 200, "workflow response");
    return finalWaitingBody.status === "waiting" &&
      finalWaitingBody.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });
  const oversizedEvent = await serviceInternalPostLarge(
    "workflows",
    9120,
    "/internal/workflows/send-event",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      instanceId: "wait-1",
      event: { type: "approval", payload: { blob: "x".repeat(256 * 1024) } },
    },
  );
  assert.equal(oversizedEvent.status, 413, oversizedEvent.body);
  assert.equal(responseJson(oversizedEvent).error, "request_too_large");
  const sent = await gatewayFetch(ns, "/shop/event?id=wait-1&message=ship-it");
  await readIntegrationJson(sent, 200, "workflow response");
  /** @type {any} */
  let eventBody;
  await waitUntil("scheduler resumes workflow after event delivery", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=wait-1");
    eventBody = await readIntegrationJson(status, 200, "workflow response");
    return eventBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(eventBody);
  assert.deepEqual(eventBody.output, { message: "ship-it", fromEnv: "runtime-ok" });
  assert.deepEqual(eventBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "approval", status: "completed", attempt: 1 },
    { ordinal: 1, name: "after-event", status: "completed", attempt: 1 },
  ]);

  const staleWait = await gatewayFetch(ns, "/shop/create?id=wait-stale-claim&wait=1&noWaitTimeout=1");
  await readIntegrationJson(staleWait, 200, "workflow response");
  await waitUntil("workflow reaches no-timeout wait state before stale claim injection", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=wait-stale-claim");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });
  redisWorkflowStateHSet(ns, workflowKey, "wait-stale-claim", [
    "runToken",
    "expired-wait-claim",
    "runLeaseExpiresAtMs",
    String(Date.now() - 1000),
  ]);
  const staleSent = await gatewayFetch(ns, "/shop/event?id=wait-stale-claim&message=after-stale-claim");
  await readIntegrationJson(staleSent, 200, "workflow response");
  /** @type {any} */
  let staleClaimBody;
  await waitUntil("event resumes no-timeout wait with expired stale run claim", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=wait-stale-claim");
    staleClaimBody = await readIntegrationJson(status, 200, "workflow response");
    return staleClaimBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(staleClaimBody);
  assert.deepEqual(staleClaimBody.output, { message: "after-stale-claim", fromEnv: "runtime-ok" });

  const waitAfterSleep = await gatewayFetch(ns, "/shop/create?id=buffered-1&waitAfterSleep=300");
  await readIntegrationJson(waitAfterSleep, 200, "workflow response");
  const bufferedSent = await gatewayFetch(ns, "/shop/event?id=buffered-1&message=buffered");
  await readIntegrationJson(bufferedSent, 200, "workflow response");
  /** @type {any} */
  let bufferedBody;
  await waitUntil("event sent before waitForEvent is buffered and consumed", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=buffered-1");
    bufferedBody = await readIntegrationJson(status, 200, "workflow response");
    return bufferedBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(bufferedBody);
  assert.deepEqual(bufferedBody.output, { message: "buffered", fromEnv: "runtime-ok" });

  const cleanupId = "event-cleanup-fence";
  const cleanupCreatedAtMs = Date.now();
  const cleanupRunToken = "run-event-cleanup";
  redisWorkflowStateHSet(ns, workflowKey, cleanupId, [
    "status",
    "running",
    "generation",
    "1",
    "createdAtMs",
    String(cleanupCreatedAtMs),
    "runToken",
    cleanupRunToken,
    "runLeaseExpiresAtMs",
    String(Date.now() + 60_000),
    "payloadBytes",
    "0",
  ]);
  const cleanupIndexKey = workflowEventTypeIndexKey(ns, workflowKey, cleanupId);
  const staleEventMember = `${Buffer.from("approval").toString("hex")}:00000000000000000001`;
  redisZAdd(cleanupIndexKey, 0, staleEventMember, { db: 2 });
  const cleanupRequest = {
    ns,
    worker: "shop",
    frozenVersion: version,
    workflowName: "orders",
    workflowKey,
    className: "OrderWorkflow",
    instanceId: cleanupId,
    generation: 1,
    createdAtMs: cleanupCreatedAtMs,
    runToken: cleanupRunToken,
    ordinal: 0,
    stepName: "approval",
    nameCount: 1,
    dependencies: [],
    config: { type: "waitForEvent", eventType: "approval", timeoutMs: null },
  };
  const staleCleanup = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/register-wait",
    { ...cleanupRequest, createdAtMs: cleanupCreatedAtMs + 1 },
  );
  assert.equal(staleCleanup.status, 500, staleCleanup.body);
  assert.equal(responseJson(staleCleanup).error, "workflow_invalid_state");
  assert.equal(redisZScore(cleanupIndexKey, staleEventMember, { db: 2 }), "0");

  const activeCleanup = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/register-wait",
    cleanupRequest,
  );
  assert.equal(activeCleanup.status, 200, activeCleanup.body);
  assert.deepEqual(responseJson(activeCleanup), { state: "waiting" });
  assert.equal(redisZScore(cleanupIndexKey, staleEventMember, { db: 2 }), null);

  const terminable = await gatewayFetch(ns, "/shop/create?id=terminate-1&wait=1");
  await readIntegrationJson(terminable, 200, "workflow response");
  await waitUntil("workflow reaches wait state before termination", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=terminate-1");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });
  const terminated = await gatewayFetch(ns, "/shop/terminate?id=terminate-1");
  const terminatedBody = await readIntegrationJson(terminated, 200, "workflow response");
  assert.equal(terminatedBody.status, "terminated");
  assert.deepEqual(terminatedBody.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
    attempt: entry.attempt,
  })), [
    { ordinal: 0, name: "approval", status: "waiting", attempt: 1 },
  ]);
  const byWorkerAfterTerminate = redisSMembers(`wf:by-worker:${ns}:shop`, { db: 2 });
  assert.ok(byWorkerAfterTerminate.includes(`${workflowKey}\tterminate-1`));
  const byVersionAfterTerminate = redisSMembers(`wf:by-version:${ns}:shop:${version}`, { db: 2 });
  assert.equal(byVersionAfterTerminate.includes(`${workflowKey}\tterminate-1`), false);

  const pausable = await gatewayFetch(ns, "/shop/create?id=pause-1&wait=1");
  await readIntegrationJson(pausable, 200, "workflow response");
  await waitUntil("workflow reaches wait state before pause", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=pause-1");
    const body = await readIntegrationJson(status, 200, "workflow response");
    return body.status === "waiting" &&
      body.steps.entries.some((/** @type {any} */ entry) => entry.name === "approval" && entry.status === "waiting");
  }, { timeoutMs: 60000, intervalMs: 250 });
  const paused = await gatewayFetch(ns, "/shop/pause?id=pause-1");
  assert.equal((await readIntegrationJson(paused, 200, "workflow response")).status, "paused");
  const pausedEvent = await gatewayFetch(ns, "/shop/event?id=pause-1&message=paused-buffer");
  assert.equal((await readIntegrationJson(pausedEvent, 200, "workflow response")).status, "paused");
  const resumed = await gatewayFetch(ns, "/shop/resume?id=pause-1");
  assert.equal((await readIntegrationJson(resumed, 200, "workflow response")).status, "queued");
  /** @type {any} */
  let resumedBody;
  await waitUntil("paused workflow resumes and consumes buffered event", async () => {
    const status = await gatewayFetch(ns, "/shop/steps?id=pause-1");
    resumedBody = await readIntegrationJson(status, 200, "workflow response");
    return resumedBody.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(resumedBody);
  assert.deepEqual(resumedBody.output, { message: "paused-buffer", fromEnv: "runtime-ok" });

  const completedList = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/instances",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      options: { limit: 100 },
    },
  );
  assert.equal(completedList.status, 200, completedList.body);
  const listedOrder = responseJson(completedList).instances.find(
    (/** @type {any} */ instance) => instance.id === "order-123",
  );
  assert.ok(listedOrder);
  assert.deepEqual(listedOrder.output, restartedCompletedBody.output);

  redisWorkflowStateHSet(ns, workflowKey, "order-123", [
    "outputRef",
    "missing-list-payload",
  ]);
  const corruptList = serviceInternalPost(
    "workflows",
    9120,
    "/internal/workflows/instances",
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      options: { limit: 100 },
    },
  );
  assert.equal(corruptList.status, 500, corruptList.body);
  assert.equal(responseJson(corruptList).error, "workflow_payload_missing");

  const metrics = serviceInternalGet("workflows", 9120, "/_metrics").body;
  for (const outcome of ["paused", "resumed", "restarted", "terminated"]) {
    const before = prometheusCounter(lifecycleMetricsBefore, "wdl_workflow_instances_total", { outcome });
    const after = prometheusCounter(metrics, "wdl_workflow_instances_total", { outcome });
    assert.ok(after - before >= 1, `expected workflow ${outcome} metric to increment`);
  }
  for (const line of [
    /wdl_workflow_instances_total\{outcome="created"\} [1-9][0-9]*/,
    /wdl_workflow_steps_total\{outcome="completed"\} [1-9][0-9]*/,
    /wdl_workflow_steps_total\{outcome="waiting"\} [1-9][0-9]*/,
    /wdl_workflow_steps_total\{outcome="failed"\} [1-9][0-9]*/,
    /wdl_workflow_dispatches_total\{outcome="completed"\} [1-9][0-9]*/,
    /wdl_workflow_dispatches_total\{outcome="failed"\} [1-9][0-9]*/,
    /wdl_workflow_dispatches_total\{outcome="suspended"\} [1-9][0-9]*/,
    /wdl_workflow_due_claims_total\{outcome="moved"\} [1-9][0-9]*/,
    /wdl_workflow_dispatch_in_flight 0/,
    /wdl_requests_total\{route="workflow_create",service="workflows",status="200"\} [1-9][0-9]*/,
    /wdl_workflow_instance_duration_ms_count [1-9][0-9]*/,
    /wdl_workflow_step_duration_ms_count [1-9][0-9]*/,
  ]) {
    assert.match(metrics, line);
  }
});

test("module-cached Workflow KV facade remains wrapped across invocations", async (t) => {
  for (const { label, compatibilityFlags } of [
    { label: "importable env", compatibilityFlags: [] },
    { label: "disallowed importable env", compatibilityFlags: ["disallow_importable_env"] },
  ]) {
    await t.test(label, async () => {
      const ns = uniqueNs("wfrt-kv-cache");
      await deployAndPromote(ns, "shop", {
        code: WORKER_CODE,
        vars: { LABEL: "runtime-ok" },
        bindings: { CACHE: { type: "kv", id: "workflow-cache" } },
        workflows: [
          { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
        ],
        compatibilityFlags,
      });

      const primed = await gatewayFetch(ns, "/shop/cache-kv");
      assert.deepEqual(
        await readIntegrationJson(primed, 200, "ordinary cached KV response"),
        { value: "ready" }
      );

      for (const id of ["cached-kv-first", "cached-kv-second"]) {
        const created = await gatewayFetch(
          ns,
          `/shop/create?id=${id}&kvKey=shared`
        );
        await readIntegrationJson(created, 200, "workflow response");
        await waitUntil(`workflow ${id} completes through cached KV facade`, async () => {
          const status = await gatewayFetch(ns, `/shop/get?id=${id}`);
          const body = await readIntegrationJson(status, 200, "workflow response");
          return body.status === "completed" && body.output === id;
        });
      }
    });
  }
});

test("Workflow KV facade ignores module-evaluation method shadows", async () => {
  const ns = uniqueNs("wfrt-kv-provenance");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-provenance" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create"),
    200,
    "workflow provenance probe create"
  );
  await waitUntil("workflow provenance probe completes", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get"),
      200,
      "workflow provenance probe status"
    );
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      captured: null,
      constructorProps: [],
      defineSucceeded: true,
      error: null,
      promisePrototypeThenCalls: 0,
      prototypeListCalls: 0,
      prototypeReportCalls: 0,
      reporterPromiseSpeciesCalls: 0,
      reporterPromiseThenCalls: 0,
      rpcPromiseThenCalls: 0,
      serviceStubReportIntercepts: 0,
      serviceStubListCalls: 0,
      serviceStubInvokeCalls: 0,
      value: "value",
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("disallow_importable_env KV provenance preserves host capability identity", async () => {
  const ns = uniqueNs("wfrt-kv-prototype");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    compatibilityFlags: ["disallow_importable_env"],
    bindings: { CACHE: { type: "kv", id: "workflow-prototype" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?prototypeList=1"),
    200,
    "workflow prototype list create"
  );
  await waitUntil("workflow bypasses Object.prototype list shadow", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=prototype-list"),
      200,
      "workflow prototype list status"
    );
    if (body.status === "failed") {
      throw new Error(`prototype list workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      listComplete: true,
      prototypeListCalls: 0,
      serviceStubListCalls: 0,
      serviceStubInvokeCalls: 0,
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow prototype reporter seed"
  );
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?capacityUncaught=1"),
    200,
    "workflow prototype reporter create"
  );
  await waitUntil("workflow bypasses ServiceStub fetch shadow", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=capacity-uncaught"),
      200,
      "workflow prototype reporter status"
    );
    if (body.status === "failed") {
      throw new Error(`prototype reporter workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      retried: true,
      runs: 2,
      serviceStubReportIntercepts: 0,
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });

  assert.deepEqual(
    await readIntegrationJson(
      await gatewayFetch(ns, "/shop/prime-fake"),
      200,
      "workflow fake facade prime"
    ),
    { seeded: true }
  );

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?captureInfrastructureError=1"),
    200,
    "workflow infrastructure Error capture create"
  );
  await waitUntil("workflow captures a real infrastructure Error", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=capture-infrastructure"),
      200,
      "workflow infrastructure Error capture status"
    );
    if (body.status === "failed") {
      throw new Error(`infrastructure Error capture failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      captured: true,
      code: KV_READ_INFRASTRUCTURE_ERROR_CODE,
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?relayInfrastructureError=1"),
    200,
    "workflow infrastructure Error relay create"
  );
  await waitUntil("same binding preserves Error provenance across instances", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=relay-infrastructure"),
      200,
      "workflow infrastructure Error relay status"
    );
    if (body.status === "failed") {
      throw new Error(`infrastructure Error relay failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, { retried: true, runs: 2 });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?fakeRelayInfrastructureError=1"),
    200,
    "workflow fake source relay create"
  );
  await waitUntil("fake source cannot overwrite real Error provenance", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=fake-relay-infrastructure"),
      200,
      "workflow fake source relay status"
    );
    if (body.status === "failed") {
      throw new Error(`fake source relay failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, { retried: true, runs: 2 });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?fakeFacade=1"),
    200,
    "workflow fake facade create"
  );
  /** @type {any} */
  let failed;
  await waitUntil("fake facade error remains a tenant failure", async () => {
    failed = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=fake-facade"),
      200,
      "workflow fake facade status"
    );
    return failed.status === "failed";
  }, { timeoutMs: 60_000, intervalMs: 250 });
  assert.deepEqual(failed.error, {
    name: "Error",
    message: "tenant forged infrastructure error",
  });
});

test("runtime bounds a pending Workflow root at the sender deadline", async () => {
  const ns = uniqueNs("wfrootdeadline");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  const workflowKey = workerMeta(ns, "shop", version).workflows[0].workflowKey;
  assert.deepEqual(
    await readIntegrationJson(
      await gatewayFetch(ns, "/shop/root-delay-status"),
      200,
      "root delay warmup"
    ),
    { started: 0 }
  );
  const startedAt = performance.now();
  const response = runtimeDispatchPost(
    "/internal/workflows/run",
    { "x-worker-id": gatewayWorkerId(ns, "shop", version) },
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      instanceId: "pending-root",
      generation: 1,
      createdAtMs: Date.now(),
      runToken: "pending-root-run",
      dispatchDeadlineMs: Date.now() + 1000,
      params: { rootDelayMs: 5000 },
    }
  );
  const elapsedMs = performance.now() - startedAt;

  assert.equal(response.status, 503, response.body);
  assert.equal(responseJson(response).error, "workflow_backend_unavailable");
  assert.equal(
    elapsedMs < 3000,
    true,
    `dispatch exceeded deadline budget at ${elapsedMs}ms`
  );
  assert.deepEqual(
    await readIntegrationJson(
      await gatewayFetch(ns, "/shop/root-delay-status"),
      200,
      "root delay status"
    ),
    { started: 1 }
  );
});

test("Workflow KV facade uses the captured RpcPromise settlement method", async () => {
  const ns = uniqueNs("wfrt-rpc-promise");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-rpc-promise" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?rpcPromiseThen=1"),
    200,
    "workflow RpcPromise create"
  );
  await waitUntil("workflow bypasses tenant RpcPromise.then", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=rpc-promise-then"),
      200,
      "workflow RpcPromise status"
    );
    if (body.status === "failed") {
      throw new Error(`RpcPromise workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, { value: null, rpcPromiseThenCalls: 0 });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("native Promise prototype pollution cannot forge Workflow KV provenance", async () => {
  const ns = uniqueNs("wfrt-promise-prototype");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-promise-prototype" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?promisePrototypeThen=1"),
    200,
    "workflow Promise prototype create"
  );
  /** @type {any} */
  let failed;
  await waitUntil("tenant Promise prototype error is terminal", async () => {
    failed = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=promise-prototype-then"),
      200,
      "workflow Promise prototype status"
    );
    if (failed.status === "completed") {
      throw new Error(`Promise prototype error retried: ${JSON.stringify(failed.output)}`);
    }
    return failed.status === "failed";
  }, { timeoutMs: 60_000, intervalMs: 250 });
  assert.deepEqual(failed.error, {
    name: "Error",
    message: "tenant Promise.prototype.then",
  });
});

test("native Promise prototype pollution cannot intercept Workflow reporter settlement", async () => {
  const ns = uniqueNs("wfrt-reporter-promise");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-reporter-promise" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });
  const workflowKey = workerMeta(ns, "shop", version).workflows[0].workflowKey;

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow reporter Promise seed"
  );
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?promiseReporterThen=1"),
    200,
    "workflow reporter Promise create"
  );
  await waitUntil("workflow reporter bypasses tenant Promise.then", async () => {
    const response = serviceInternalPost(
      "workflows",
      9120,
      "/internal/workflows/status",
      {
        ns,
        worker: "shop",
        frozenVersion: version,
        workflowName: "flow",
        workflowKey,
        className: "Flow",
        instanceId: "promise-reporter-then",
      }
    );
    assert.equal(response.status, 200, response.body);
    const body = responseJson(response);
    if (body.status === "failed") {
      throw new Error(`reporter Promise workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      retried: true,
      runs: 2,
      serviceStubReportIntercepts: 0,
      reporterPromiseSpeciesCalls: 0,
      reporterPromiseThenCalls: 0,
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("Workflow KV facade bypasses legacy ServiceStub get/put/delete helpers", async () => {
  const ns = uniqueNs("wfrt-fetcher-legacy");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    compatibilityFlags: ["fetcher_has_get_put_delete"],
    bindings: { CACHE: { type: "kv", id: "workflow-fetcher-legacy" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create"),
    200,
    "workflow legacy fetcher create"
  );
  await waitUntil("workflow uses KV RPC methods behind legacy fetcher helpers", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get"),
      200,
      "workflow legacy fetcher status"
    );
    if (body.status === "failed") {
      throw new Error(`legacy fetcher workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.equal(body.output.value, "value");
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("Workflow step descriptors remain wrapped and hide dup", async () => {
  const ns = uniqueNs("wfrt-step-facade");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-step-facade" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow step facade seed"
  );
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?stepBypass=1"),
    200,
    "workflow step facade create"
  );
  await waitUntil("workflow retries through descriptor-wrapped step callback", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=step-bypass"),
      200,
      "workflow step facade status"
    );
    if (body.status === "failed") {
      throw new Error(`step facade workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, { retried: true, runs: 2 });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("tenant KV method shadows cannot forge host provenance", async () => {
  const ns = uniqueNs("wfrt-kv-shadow");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-shadow" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?methodShadow=1"),
    200,
    "workflow method shadow create"
  );
  await waitUntil("workflow bypasses tenant KV method shadow", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=method-shadow"),
      200,
      "workflow method shadow status"
    );
    if (body.status === "failed") {
      throw new Error(`method shadow workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.equal(body.output, "trusted");
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("tenant KV argument serialization errors cannot forge host provenance", async () => {
  const ns = uniqueNs("wfrt-kv-args");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-args" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  for (const kind of ["options", "key"]) {
    await readIntegrationJson(
      await gatewayFetch(ns, `/shop/create?forgedReadArgs=${kind}`),
      200,
      "workflow forged argument create"
    );
    /** @type {any} */
    let failed;
    await waitUntil(`workflow ${kind} argument error is terminal`, async () => {
      failed = await readIntegrationJson(
        await gatewayFetch(ns, `/shop/get?id=forged-${kind}`),
        200,
        "workflow forged argument status"
      );
      return failed.status === "failed";
    }, { timeoutMs: 60_000, intervalMs: 250 });
    assert.deepEqual(failed.error, {
      name: "Error",
      message: `tenant ${kind} serialization`,
    });
  }
});

test("real-workerd KV failures retry across run and durable callback boundaries", async () => {
  const ns = uniqueNs("wfrt-kv-boundaries");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-boundaries" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow boundary seed"
  );

  for (const { query, id } of [
    { query: "directRunInfrastructure=1", id: "direct-run-infrastructure" },
    { query: "outerCatch=1", id: "outer-catch" },
  ]) {
    await readIntegrationJson(
      await gatewayFetch(ns, `/shop/create?${query}`),
      200,
      "workflow boundary create"
    );
    await waitUntil(`workflow ${id} retries`, async () => {
      const body = await readIntegrationJson(
        await gatewayFetch(ns, `/shop/get?id=${id}`),
        200,
        "workflow boundary status"
      );
      if (body.status === "failed") {
        throw new Error(`workflow boundary failed: ${JSON.stringify(body.error)}`);
      }
      if (body.status !== "completed") return false;
      assert.deepEqual(body.output, { retried: true, runs: 2 });
      return true;
    }, { timeoutMs: 60_000, intervalMs: 250 });
  }
});

test("caught real-workerd KV capacity failures may commit a step fallback", async () => {
  const ns = uniqueNs("wfrt-kv-capacity");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-capacity" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow capacity seed"
  );
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?capacityRetry=1"),
    200,
    "workflow capacity create"
  );
  await waitUntil("workflow commits caught KV capacity fallback", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=capacity"),
      200,
      "workflow capacity status"
    );
    if (body.status === "failed") {
      throw new Error(`capacity workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.equal(body.output.fallbackCommitted, true);
    assert.equal(body.output.runs, 1);
    assert.ok(body.output.rejectedReads >= 1 && body.output.rejectedReads <= 3);
    assert.equal(body.output.rejectionCodes.length, body.output.rejectedReads);
    assert.ok(
      body.output.rejectionCodes.every(
        (/** @type {unknown} */ code) => code === KV_READ_INFRASTRUCTURE_ERROR_CODE
      )
    );
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});

test("uncaught real-workerd KV capacity failure retries before step error commit", async () => {
  const ns = uniqueNs("wfrt-kv-capacity-uncaught");
  await deployAndPromote(ns, "shop", {
    code: WORKFLOW_KV_PROVENANCE_PROBE_CODE,
    bindings: { CACHE: { type: "kv", id: "workflow-capacity" } },
    workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
  });

  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/seed"),
    200,
    "workflow capacity seed"
  );
  await readIntegrationJson(
    await gatewayFetch(ns, "/shop/create?capacityUncaught=1"),
    200,
    "workflow capacity create"
  );
  await waitUntil("workflow retries after uncaught KV capacity failure", async () => {
    const body = await readIntegrationJson(
      await gatewayFetch(ns, "/shop/get?id=capacity-uncaught"),
      200,
      "workflow capacity status"
    );
    if (body.status === "failed") {
      throw new Error(`capacity workflow failed: ${JSON.stringify(body.error)}`);
    }
    if (body.status !== "completed") return false;
    assert.deepEqual(body.output, {
      retried: true,
      runs: 2,
      serviceStubReportIntercepts: 0,
    });
    return true;
  }, { timeoutMs: 60_000, intervalMs: 250 });
});
