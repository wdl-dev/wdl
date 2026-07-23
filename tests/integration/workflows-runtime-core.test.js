// WDL Workflows runtime core path: loaded worker facade reaches workflows.
import { test } from "node:test";
import assert from "node:assert/strict";
import { prometheusCounter } from "./helpers/prometheus.js";
import {
  WORKER_CODE,
  deployAndPromote,
  dispatchWorkflowReplay,
  gatewayFetch,
  redisSMembers,
  redisWorkflowStateHDel,
  redisWorkflowStateHSet,
  redisZAdd,
  redisZScore,
  readIntegrationJson,
  responseJson,
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
  assert.deepEqual(secondPageBody.instances.map((/** @type {any} */ entry) => entry.id), ["batch-b"]);
  assert.equal(secondPageBody.cursor, null);

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
