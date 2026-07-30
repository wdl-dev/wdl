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
  deployConsumer,
  deployQueueConsumerWorker,
  deployQueueProducer,
  queuePendingCount,
  sendQueueMessage,
  setupQueueIntegrationSuite,
} from "./helpers/queue-scenarios.js";
import { redisXInfoGroups } from "./helpers/redis.js";

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

test("fault injection: blocked queue dispatch keeps PEL within maxBatchSize", async () => {
  const ns = uniqueNs("qpelcap");
  const queueName = "cap";
  const bodies = ["m1", "m2", "m3", "m4", "m5"];
  const streamKey = queueStreamKey(ns, queueName);

  const consumerVersion = await deployConsumer(ns, BLOCKING_BATCH_RECORDER, [
    { queue: queueName, maxBatchSize: 2, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployQueueProducer(ns, queueName);

  const sendRes = sendQueueMessage(ns, "producer", producerVersion, bodies);
  assertStatus(sendRes, 200, "PEL cap producer send");

  await waitUntil("blocked dispatch owns its first PEL batch", async () => {
    return queuePendingCount(streamKey) > 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });

  const firstPending = queuePendingCount(streamKey);
  assert.ok(
    firstPending > 0 && firstPending <= 2,
    `blocked dispatch must leave at most maxBatchSize=2 messages pending, got ${firstPending}`
  );

  await delay(1_000);
  const stillPending = queuePendingCount(streamKey);
  assert.ok(
    stillPending > 0 && stillPending <= 2,
    `scheduler must not prefetch beyond maxBatchSize while handler is blocked, got ${stillPending}`
  );

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

  const slowVersion = await deployQueueConsumerWorker(ns, "slow", BLOCKING_BATCH_RECORDER, [
    { queue: "slow", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);

  const fastVersion = await deployQueueConsumerWorker(ns, "fast", FAST_QUEUE_CONSUMER, [
    { queue: "fastq", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 0 },
  ]);

  const slowProdVer = await deployQueueProducer(ns, "slow", "slow-prod");
  const fastProdVer = await deployQueueProducer(ns, "fastq", "fast-prod");

  await waitUntil("slow and fast consumer groups are ready", async () => {
    return [slowStream, queueStreamKey(ns, "fastq")].every((stream) => {
      const groups = redisXInfoGroups(stream, { db: 1 });
      return !groups.includes("missing") && groups.includes("wdl-scheduler");
    });
  }, { timeoutMs: 30_000, intervalMs: 500 });

  const slowSend = sendQueueMessage(ns, "slow-prod", slowProdVer, { hang: true });
  assertStatus(slowSend, 200, "slow queue send");
  const slowConsumerId = gatewayWorkerId(ns, "slow", slowVersion);
  await waitUntil("slow queue handler starts blocking", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": slowConsumerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.sizes.length === 1 && snap.total === 0;
  }, { timeoutMs: 15_000, intervalMs: 250 });
  assert.equal(queuePendingCount(slowStream), 1, "slow queue must be in the PEL");

  // This message arrives after the Scheduler is already awaiting the slow
  // handler, so same-wave parallelism cannot make the test pass.
  const fastSend = sendQueueMessage(ns, "fast-prod", fastProdVer, { fast: true });
  assertStatus(fastSend, 200, "fast queue send");

  const fastConsumerId = gatewayWorkerId(ns, "fast", fastVersion);
  await waitUntil("later fast queue delivered while slow remains in flight", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": fastConsumerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.total >= 1;
  }, { timeoutMs: 3_000, intervalMs: 250 });
  const slowSnapshot = responseJson(runtimeInternalPost("/", {
    "x-worker-id": slowConsumerId,
  }, ""));
  assert.equal(slowSnapshot.total, 0, "slow queue must still be in flight");
});
