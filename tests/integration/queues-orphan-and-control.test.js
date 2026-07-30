import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminPost,
  assertStatus,
  composeScale,
  composeStart,
  composeStop,
  uniqueNs,
  waitUntil,
  withServiceStopped,
  queueConsumerKey,
  queueDelayedKey,
  queueDelayedMessageMember,
  QUEUE_STREAM_INDEX_KEY,
  queueOrphanedKey,
  queueStreamKey,
  queueStreamMessageFields,
} from "./helpers/index.js";
import {
  redisExists,
  redisHGet,
  redisSAdd,
  redisXAdd,
  redisXClaimIdle,
  redisXInfoGroups,
  redisXLen,
  redisXPendingCount,
  redisXRangeRaw,
  redisXReadGroup,
  redisZAdd,
} from "./helpers/redis.js";
import {
  QUEUE_MEMORY_CONSUMER,
  deployConsumer,
  deployConsumerWithoutQueues,
  deployQueueProducer,
  readConsumerKeys,
  sendQueueMessage,
  setupQueueIntegrationSuite,
  waitForQueueConsumerGroups,
} from "./helpers/queue-scenarios.js";

setupQueueIntegrationSuite();

const INTEGRATION_QUEUE_PEL_IDLE_MS = 65_000;

test("PEL reap: idle pending entry is XCLAIMed and redelivered to live consumer", async () => {
  const ns = uniqueNs("q");
  const queueName = "pelq";
  const streamKey = queueStreamKey(ns, queueName);

  const consumerVersion = await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  await waitForQueueConsumerGroups(streamKey, { timeoutMs: 10_000 });

  // Stop scheduler so its XREADGROUP BLOCK can't snatch the seeded message
  // before our fake "crashed-instance" XREADGROUP puts it in PEL.
  try {
    composeStop("scheduler");
    const streamId = redisXAdd(
      streamKey,
      queueStreamMessageFields({ id: "pel-reap-msg", body: "pel-reap-msg", contentType: "text", firstSeenMs: 1 }),
      { db: 1 }
    );
    redisXReadGroup(streamKey, "wdl-scheduler", "crashed-instance", { db: 1 });
    redisXClaimIdle(streamKey, "wdl-scheduler", "crashed-instance", streamId, INTEGRATION_QUEUE_PEL_IDLE_MS, { db: 1 });

    const pelLenBefore = redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 });
    assert.equal(pelLenBefore, 1, "expected PEL entry seeded");
    composeScale("scheduler", 2);

    // The scheduler keeps PEL idle above the 60s fire timeout. The test seeds a
    // fake crashed pending entry and ages it with XCLAIM IDLE so only the 3s
    // reap interval is on the critical path.
    await waitUntil("PEL entry redelivered via reap", async () => {
      return readConsumerKeys(ns, consumerVersion).includes("pel-reap-msg");
    }, { timeoutMs: 30_000, intervalMs: 1_000 });

    const lenAfter = redisXLen(streamKey, { db: 1 });
    assert.equal(lenAfter, 0, `main stream should be drained after ack, got XLEN=${lenAfter}`);
  } finally {
    composeScale("scheduler", 1);
  }
});

test("orphan cleanup handles unread tail when consumer group was never created", async () => {
  const ns = uniqueNs("qnogroup");
  const queueName = "nogroupq";
  const streamKey = queueStreamKey(ns, queueName);
  const orphanedKey = queueOrphanedKey(ns, queueName);

  try {
    composeStop("scheduler");
    redisXAdd(
      streamKey,
      queueStreamMessageFields({ id: "nogroup-tail", body: "nogroup", contentType: "text", firstSeenMs: 1 }),
      { db: 1 }
    );
    redisSAdd(QUEUE_STREAM_INDEX_KEY, streamKey, { db: 1 });

    const groups = redisXInfoGroups(streamKey, { db: 1 });
    assert.ok(groups.includes("missing") || groups === "", `expected no consumer group, got: ${groups}`);

    composeStart("scheduler");

    await waitUntil("tail orphaned without consumer group", async () => {
      return redisXLen(orphanedKey, { db: 1 }) >= 1;
    }, { timeoutMs: 45_000, intervalMs: 1_000 });

    const orphaned = redisXRangeRaw(orphanedKey, "-", "+", { db: 1 });
    assert.ok(orphaned.includes("nogroup-tail"), `expected no-group tail in orphaned: ${orphaned}`);
    assert.equal((orphaned.match(/nogroup-tail/g) ?? []).length, 1, `no-group tail must orphan once: ${orphaned}`);
    assert.ok(orphaned.includes("stream-tail"), `expected source=stream-tail tag: ${orphaned}`);
    assert.equal(redisXLen(streamKey, { db: 1 }), 0);
  } finally {
    composeScale("scheduler", 1);
  }
});

test("consumer removal → PEL + unread tail + delayed ZSET all land in queue-orphaned", async () => {
  const ns = uniqueNs("q");
  const queueName = "orphq";
  const streamKey = queueStreamKey(ns, queueName);
  const delayedKey = queueDelayedKey(ns, queueName);
  const orphanedKey = queueOrphanedKey(ns, queueName);

  // Deploy creates stream + group via reconcile — required before seeding PEL below.
  await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  await waitForQueueConsumerGroups(streamKey, { timeoutMs: 10_000 });

  await withServiceStopped("scheduler", async () => {
    // Stop scheduler so its XREADGROUP BLOCK can't race us while seeding.
    // Seed the three paths directly:
    // 1) PEL entry — XADD + XREADGROUP as a fake consumer, don't XACK.
    redisXAdd(
      streamKey,
      queueStreamMessageFields({ id: "pel-msg", body: "pel-msg", contentType: "text", firstSeenMs: 1 }),
      { db: 1 }
    );
    redisXReadGroup(streamKey, "wdl-scheduler", "seed-consumer", { db: 1 });

    // 2) Unread tail — XADD a message we never read.
    redisXAdd(
      streamKey,
      queueStreamMessageFields({ id: "tail-msg", body: "tail-msg", contentType: "text", firstSeenMs: 2 }),
      { db: 1 }
    );

    // 3) Delayed ZSET entry — far-future score.
    const farFuture = Date.now() + 60_000;
    redisZAdd(
      delayedKey,
      farFuture,
      queueDelayedMessageMember({ id: "delayed-msg", body: "delayed-msg", contentType: "text", firstSeenMs: 3 }),
      { db: 1 }
    );

    // Redeploy consumer WITHOUT queueConsumers → promote DELs queue-consumer:<ns>:<q>.
    await deployConsumerWithoutQueues(ns);

    composeScale("scheduler", 2);

    // Scheduler restarts → reconcile sees no consumer → delayed dispatcher
    // orphans delayed → PEL reap orphans PEL + tail.
    await waitUntil("all three paths orphaned and source stream cleaned", async () => {
      const len = redisXLen(orphanedKey, { db: 1 });
      return len >= 3 && !redisExists(streamKey, { db: 1 });
    }, { timeoutMs: 45_000, intervalMs: 1_000 });

    const orphaned = redisXRangeRaw(orphanedKey, "-", "+", { db: 1 });
    assert.ok(orphaned.includes("pel-msg"), `expected pel-msg in orphaned: ${orphaned}`);
    assert.ok(orphaned.includes("tail-msg"), `expected tail-msg in orphaned: ${orphaned}`);
    assert.ok(orphaned.includes("delayed-msg"), `expected delayed-msg in orphaned: ${orphaned}`);
    assert.equal((orphaned.match(/pel-msg/g) ?? []).length, 1, `pel must orphan once: ${orphaned}`);
    assert.equal((orphaned.match(/tail-msg/g) ?? []).length, 1, `tail must orphan once: ${orphaned}`);
    assert.equal((orphaned.match(/delayed-msg/g) ?? []).length, 1, `delayed must orphan once: ${orphaned}`);
    assert.ok(orphaned.includes("consumer-removed"), "expected reason=consumer-removed tag");
  });
});

test("rollback: re-promoting consumer doesn't un-orphan already-moved messages", async () => {
  const ns = uniqueNs("q");
  const queueName = "rbkq";
  const streamKey = queueStreamKey(ns, queueName);
  const orphanedKey = queueOrphanedKey(ns, queueName);

  await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  await waitForQueueConsumerGroups(streamKey, { timeoutMs: 10_000 });

  // Stop scheduler so the seeded message doesn't get consumed before removal.
  await withServiceStopped("scheduler", async () => {
    redisXAdd(
      streamKey,
      queueStreamMessageFields({ id: "pre-removal", body: "pre", contentType: "text", firstSeenMs: 1 }),
      { db: 1 }
    );

    await deployConsumerWithoutQueues(ns);
  });

  // Scheduler reconciles → sees no consumer → orphans the unread tail.
  await waitUntil("pre-removal message orphaned", async () => {
    return redisXLen(orphanedKey, { db: 1 }) >= 1;
  }, { timeoutMs: 45_000, intervalMs: 1_000 });

  // Re-promote a consumer (rollback-shape: same queue, fresh version).
  const consumerV3 = await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployQueueProducer(ns, queueName);
  sendQueueMessage(ns, "producer", producerVersion, { post_rollback: true });

  await waitUntil("new message delivered to re-promoted consumer", async () => {
    return readConsumerKeys(ns, consumerV3).length > 0;
  }, { timeoutMs: 30_000, intervalMs: 1_000 });

  // pre-removal message stays in orphaned stream (not un-orphaned).
  const orphaned = redisXRangeRaw(orphanedKey, "-", "+", { db: 1 });
  assert.ok(orphaned.includes("pre-removal"),
    `pre-removal message must stay orphaned after rollback: ${orphaned}`);
});

// Symmetric to routing-admin.test.js's "(host, slot) conflict → 409":
// queue consumer ownership uses the same WATCH/CAS pattern, so it gets
// its own 409 path.
test("queue consumer CAS: second worker claiming same queue is rejected 409", async () => {
  const ns = uniqueNs("qcas");
  const queue = "orders";

  const first = await adminPost(`/ns/${ns}/worker/first/deploy`, {
    code: QUEUE_MEMORY_CONSUMER,
    queueConsumers: [{ queue, maxBatchSize: 10, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assertStatus(first, 201, "first consumer deploy");
  const firstPromote = await adminPost(`/ns/${ns}/worker/first/promote`, {
    version: first.json.version,
  });
  assertStatus(firstPromote, 200, "first consumer promote");

  // Second worker deploy is fine — validation only happens at promote.
  const second = await adminPost(`/ns/${ns}/worker/second/deploy`, {
    code: QUEUE_MEMORY_CONSUMER,
    queueConsumers: [{ queue, maxBatchSize: 10, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assertStatus(second, 201, "second consumer deploy");

  const secondPromote = await adminPost(`/ns/${ns}/worker/second/promote`, {
    version: second.json.version,
  });
  assertStatus(secondPromote, 409, "second consumer promote");
  assert.equal(secondPromote.json.error, "queue_consumer_conflict");
  assert.match(secondPromote.json.message, /already consumed by first/);
  assert.equal(secondPromote.json.queue, queue);

  // Registry must still point at the first worker — the rejected promote
  // cannot have leaked partial writes through the MULTI.
  const reg = redisHGet(queueConsumerKey(ns, queue), "worker") || "";
  assert.equal(reg, "first", "registry must still name the original owner");
});

// Flips the queue a worker consumes across versions — v1 → v2 adds B and
// removes A; rollback v2 → v1 must do the inverse. Exercises
// control/routing.js#promoteWithRoutes newQueueSet diff from both sides.
test("queue consumer switch: v1→v2 migrates queue, rollback inverts", async () => {
  const ns = uniqueNs("qswitch");
  const qA = "queue-a";
  const qB = "queue-b";
  const hgetWorker = (/** @type {string} */ queue) =>
    redisHGet(queueConsumerKey(ns, queue), "worker") || "";

  const v1 = await adminPost(`/ns/${ns}/worker/consumer/deploy`, {
    code: QUEUE_MEMORY_CONSUMER,
    queueConsumers: [{ queue: qA, maxBatchSize: 5, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assertStatus(v1, 201, "queue switch v1 deploy");
  const p1 = await adminPost(`/ns/${ns}/worker/consumer/promote`, { version: v1.json.version });
  assertStatus(p1, 200, "queue switch v1 promote");
  assert.equal(hgetWorker(qA), "consumer");
  assert.equal(hgetWorker(qB), "", "qB registry must be absent before v2");

  const v2 = await adminPost(`/ns/${ns}/worker/consumer/deploy`, {
    code: QUEUE_MEMORY_CONSUMER,
    queueConsumers: [{ queue: qB, maxBatchSize: 5, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assertStatus(v2, 201, "queue switch v2 deploy");
  const p2 = await adminPost(`/ns/${ns}/worker/consumer/promote`, { version: v2.json.version });
  assertStatus(p2, 200, "queue switch v2 promote");
  assert.equal(hgetWorker(qA), "", "qA registry must be DEL'd when v2 drops it");
  assert.equal(hgetWorker(qB), "consumer", "qB registry must be created on v2 promote");

  // Rollback: promote v1 again. qA comes back, qB goes away.
  const rollback = await adminPost(`/ns/${ns}/worker/consumer/promote`, { version: v1.json.version });
  assertStatus(rollback, 200, "queue switch rollback");
  assert.equal(hgetWorker(qA), "consumer", "qA registry must return after rollback to v1");
  assert.equal(hgetWorker(qB), "", "qB registry must be DEL'd on rollback");
});
