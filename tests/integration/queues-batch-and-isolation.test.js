import { test } from "node:test";
import assert from "node:assert/strict";
import {
  assertStatus,
  delay,
  gatewayWorkerId,
  runtimeInternalPost,
  uniqueNs,
  waitUntil,
  responseJson,
  queueStreamKey,
} from "./helpers/index.js";
import {
  BATCH_SIZE_RECORDER,
  BLOCKING_BATCH_RECORDER,
  FAST_QUEUE_CONSUMER,
  LONG_BLOCKING_BATCH_RECORDER,
  SLOW_FIRST_BATCH_RECORDER,
  deployConsumer,
  deployQueueConsumerWorker,
  deployQueueProducer,
  queuePendingCount,
  sendQueueMessage,
  setupQueueIntegrationSuite,
  waitForQueueConsumerGroups,
} from "./helpers/queue-scenarios.js";

setupQueueIntegrationSuite();

test("maxBatchSize caps runtime dispatch size for multi-message producer sends", async () => {
  const ns = uniqueNs("qcap");
  const consumerVersion = await deployConsumer(ns, BATCH_SIZE_RECORDER, [
    { queue: "cap", maxBatchSize: 2, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "cap");

  // sendBatch of 5 lands as 5 XADDs in one request; scheduler must still
  // preserve the declared consumer batch cap.
  const sendRes = sendQueueMessage(ns, "producer", producerVersion, ["m1", "m2", "m3", "m4", "m5"]);
  assertStatus(sendRes, 200, "batch cap producer send");

  const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
  await waitUntil("all 5 messages delivered", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.total === 5;
  }, { timeoutMs: 30_000, intervalMs: 500 });

  const finalRes = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
  const snapshot = responseJson(finalRes);
  assert.ok(
    snapshot.sizes.every((/** @type {number} */ n) => n <= 2),
    `every dispatched batch must be ≤ maxBatchSize=2, got ${JSON.stringify(snapshot.sizes)}`
  );
  assert.ok(
    snapshot.sizes.length >= 3,
    `5 messages at batch cap 2 must dispatch in ≥3 calls, got ${snapshot.sizes.length}: ${JSON.stringify(snapshot.sizes)}`
  );
});

test("a larger consumer batch cap is not reduced by an idle small-cap stream", async () => {
  const ns = uniqueNs("qmixedcap");
  const busyStream = queueStreamKey(ns, "busy");
  const idleStream = queueStreamKey(ns, "idle");
  const busyVersion = await deployQueueConsumerWorker(ns, "busy", BATCH_SIZE_RECORDER, [
    { queue: "busy", maxBatchSize: 5, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const idleVersion = await deployQueueConsumerWorker(ns, "idle", FAST_QUEUE_CONSUMER, [
    { queue: "idle", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const busyProducerVersion = await deployQueueProducer(ns, "busy", "busy-producer");
  const idleProducerVersion = await deployQueueProducer(ns, "idle", "idle-producer");

  await waitForQueueConsumerGroups([busyStream, idleStream], {
    label: "mixed-cap consumer groups are ready",
  });
  assertStatus(
    sendQueueMessage(ns, "idle-producer", idleProducerVersion, { warm: true }),
    200,
    "idle queue warmup"
  );
  const idleWorkerId = gatewayWorkerId(ns, "idle", idleVersion);
  await waitUntil("idle queue warmup is acknowledged", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": idleWorkerId }, ""));
    return snapshot.total === 1 && queuePendingCount(idleStream) === 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });

  const bodies = Array.from({ length: 20 }, (_, index) => `message-${index}`);
  assertStatus(
    sendQueueMessage(ns, "busy-producer", busyProducerVersion, bodies),
    200,
    "busy queue batch send"
  );
  const busyWorkerId = gatewayWorkerId(ns, "busy", busyVersion);
  await waitUntil("busy queue batch is delivered", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": busyWorkerId }, ""));
    return snapshot.total === bodies.length;
  }, { timeoutMs: 20_000, intervalMs: 250 });

  const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": busyWorkerId }, ""));
  assert.ok(
    snapshot.sizes.some((/** @type {number} */ size) => size > 1),
    `busy queue must use its own batch cap, got ${JSON.stringify(snapshot.sizes)}`
  );
  assert.ok(
    snapshot.sizes.every((/** @type {number} */ size) => size <= 5),
    `busy queue batches must remain within maxBatchSize=5, got ${JSON.stringify(snapshot.sizes)}`
  );
});

test("fault injection: blocked queue dispatch keeps PEL within maxBatchSize", async () => {
  const ns = uniqueNs("qpelcap");
  const queueName = "cap";
  const bodies = ["m1", "m2", "m3", "m4", "m5"];
  const streamKey = queueStreamKey(ns, queueName);
  const idleStream = queueStreamKey(ns, "idle");

  const consumerVersion = await deployConsumer(ns, BLOCKING_BATCH_RECORDER, [
    { queue: queueName, maxBatchSize: 2, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);
  const idleVersion = await deployQueueConsumerWorker(ns, "idle", FAST_QUEUE_CONSUMER, [
    { queue: "idle", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const producerVersion = await deployQueueProducer(ns, queueName);
  const idleProducerVersion = await deployQueueProducer(ns, "idle", "idle-producer");

  await waitForQueueConsumerGroups([streamKey, idleStream], {
    label: "blocked and idle consumer groups are ready",
  });
  assertStatus(
    sendQueueMessage(ns, "idle-producer", idleProducerVersion, { warm: true }),
    200,
    "idle queue warmup"
  );
  const idleWorkerId = gatewayWorkerId(ns, "idle", idleVersion);
  await waitUntil("idle queue warmup is acknowledged", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": idleWorkerId }, ""));
    return snapshot.total === 1 && queuePendingCount(idleStream) === 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });

  const sendRes = sendQueueMessage(ns, "producer", producerVersion, bodies);
  assertStatus(sendRes, 200, "PEL cap producer send");

  await waitUntil("blocked dispatch owns its first PEL batch", async () => {
    return queuePendingCount(streamKey) === 2;
  }, { timeoutMs: 15_000, intervalMs: 250 });

  const firstPending = queuePendingCount(streamKey);
  assert.equal(firstPending, 2, "probe plus top-up must fill maxBatchSize=2 exactly");

  await delay(1_000);
  const stillPending = queuePendingCount(streamKey);
  assert.equal(stillPending, 2, "top-up must not prefetch beyond maxBatchSize while blocked");

  const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
  await waitUntil("all 5 blocked messages eventually delivered", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.total === bodies.length;
  }, { timeoutMs: 45_000, intervalMs: 500 });

  const finalRes = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
  const snapshot = responseJson(finalRes);
  assert.ok(
    snapshot.sizes.every((/** @type {number} */ n) => n <= 2),
    `every dispatched batch must be ≤ maxBatchSize=2, got ${JSON.stringify(snapshot.sizes)}`
  );
  assert.ok(
    snapshot.sizes.length >= 3,
    `5 blocked messages at batch cap 2 must dispatch in ≥3 calls, got ${snapshot.sizes.length}: ${JSON.stringify(snapshot.sizes)}`
  );
  assert.equal(queuePendingCount(streamKey), 0, "PEL must drain after blocked batches ack");
});

test("a later queue dispatch is not blocked by an in-flight slow queue", async () => {
  const ns = uniqueNs("qhol");
  const slowStream = queueStreamKey(ns, "slow");
  const fastStream = queueStreamKey(ns, "fastq");

  const slowVersion = await deployQueueConsumerWorker(ns, "slow", LONG_BLOCKING_BATCH_RECORDER, [
    { queue: "slow", maxBatchSize: 100, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const fastVersion = await deployQueueConsumerWorker(ns, "fast", FAST_QUEUE_CONSUMER, [
    { queue: "fastq", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const slowProdVer = await deployQueueProducer(ns, "slow", "slow-prod");
  const fastProdVer = await deployQueueProducer(ns, "fastq", "fast-prod");
  await waitForQueueConsumerGroups([slowStream, fastStream], {
    label: "slow and fast consumer groups are ready",
  });

  const fastConsumerId = gatewayWorkerId(ns, "fast", fastVersion);
  assertStatus(
    sendQueueMessage(ns, "fast-prod", fastProdVer, { warm: true }),
    200,
    "fast queue warmup"
  );
  await waitUntil("fast queue warmup is acknowledged", async () => {
    const snapshot = responseJson(
      runtimeInternalPost("/", { "x-worker-id": fastConsumerId }, "")
    );
    return snapshot.total === 1 && queuePendingCount(fastStream) === 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });

  const slowMessages = Array.from({ length: 100 }, (_, index) => ({ index }));
  const slowSend = sendQueueMessage(ns, "slow-prod", slowProdVer, slowMessages);
  assertStatus(slowSend, 200, "slow queue send");
  const slowConsumerId = gatewayWorkerId(ns, "slow", slowVersion);
  await waitUntil("slow queue handler starts blocking", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": slowConsumerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.sizes[0] === slowMessages.length && snap.total === 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });
  assert.equal(
    queuePendingCount(slowStream),
    slowMessages.length,
    "the full slow batch must be in the PEL"
  );

  // This message arrives after the Scheduler is already awaiting the slow
  // 1 + 99 top-up handler, so same-wave parallelism cannot make the test pass.
  const fastSend = sendQueueMessage(ns, "fast-prod", fastProdVer, { fast: true });
  assertStatus(fastSend, 200, "fast queue send");

  await waitUntil("later fast queue delivered while slow remains in flight", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": fastConsumerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.total >= 2;
  }, { timeoutMs: 10_000, intervalMs: 250 });
  const slowSnapshot = responseJson(runtimeInternalPost("/", {
    "x-worker-id": slowConsumerId,
  }, ""));
  assert.equal(slowSnapshot.total, 0, "slow queue must still be in flight");
});

test("a busy stream continues promptly while another registered stream is idle", async () => {
  const ns = uniqueNs("qbusyidle");
  const busyStream = queueStreamKey(ns, "busy");
  const idleStream = queueStreamKey(ns, "idle");
  const consumerVersion = await deployQueueConsumerWorker(
    ns,
    "busy",
    SLOW_FIRST_BATCH_RECORDER,
    [{ queue: "busy", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 }]
  );
  await deployQueueConsumerWorker(ns, "idle", FAST_QUEUE_CONSUMER, [
    { queue: "idle", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);
  const producerVersion = await deployQueueProducer(ns, "busy");

  await waitForQueueConsumerGroups([busyStream, idleStream], {
    label: "busy and idle consumer groups are ready",
  });

  assertStatus(
    sendQueueMessage(ns, "producer", producerVersion, ["first", "second"]),
    200,
    "busy queue send"
  );
  const workerId = gatewayWorkerId(ns, "busy", consumerVersion);
  await waitUntil("first busy batch is in flight", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": workerId }, ""));
    return snapshot.started === 1 && snapshot.total === 0;
  }, { timeoutMs: 15_000, intervalMs: 100 });

  await waitUntil("second busy batch starts without waiting for the idle read timeout", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": workerId }, ""));
    return snapshot.secondStartedAt !== null;
  }, { timeoutMs: 8_000, intervalMs: 50 });
  const timing = responseJson(runtimeInternalPost("/", { "x-worker-id": workerId }, ""));
  assert.ok(
    timing.secondStartedAt - timing.firstCompletedAt < 1_000,
    `the second busy batch waited ${timing.secondStartedAt - timing.firstCompletedAt}ms`
  );

  await waitUntil("both busy batches complete", async () => {
    const snapshot = responseJson(runtimeInternalPost("/", { "x-worker-id": workerId }, ""));
    return snapshot.total === 2;
  }, { timeoutMs: 5_000, intervalMs: 100 });
});
