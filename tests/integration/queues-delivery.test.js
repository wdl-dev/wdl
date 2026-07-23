// Queue delivery core: producer send → Redis stream → scheduler consume
// loop → runtime /_queued → consumer worker queue() handler → ack.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  assertStatus,
  composeScale,
  delay,
  gatewayWorkerId,
  runtimeInternalPost,
  uniqueNs,
  waitUntil,
  withServiceStopped,
  responseJson,
  queueConsumerKey,
  QUEUE_CONSUMER_INDEX_KEY,
  QUEUE_STREAM_INDEX_KEY,
  queueStreamKey,
  queueStreamMessageFields,
} from "./helpers/index.js";
import {
  redisDel,
  redisHSet,
  redisSMembers,
  redisSRem,
  redisSet,
  redisXAdd,
  redisXGroupCreate,
  redisXInfoGroups,
  redisXLen,
  redisXPendingCount,
} from "./helpers/redis.js";
import {
  DELIVERY_SET_RECORDER,
  QUEUE_MEMORY_CONSUMER,
  deployConsumer,
  deployQueueConsumerWorker,
  deployQueueProducer,
  setupQueueIntegrationSuite,
} from "./helpers/queue-scenarios.js";

setupQueueIntegrationSuite();

test("producer send() → consumer queue() end-to-end via scheduler", async () => {
  const ns = uniqueNs("q");

  const consumerVersion = await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: "orders", maxBatchSize: 10, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "orders");

  const sendRes = runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "producer", producerVersion),
    "content-type": "application/json",
  }, { hello: "world" });
  assertStatus(sendRes, 200, "producer send");

  // Scheduler reconciles every QUEUE_RECONCILE_MS + BLOCK_MS worst case.
  const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
  await waitUntil("queue message delivered", async () => {
    const res = runtimeInternalPost("/", {
      "x-worker-id": consumerWorkerId,
    }, "");
    if (res.status !== 200) return false;
    const keys = responseJson(res);
    return Array.isArray(keys) && keys.length > 0;
  }, { timeoutMs: 30_000, intervalMs: 1_000 });

  const keys = responseJson(runtimeInternalPost("/", {
    "x-worker-id": consumerWorkerId,
  }, ""));
  assert.ok(keys.length >= 1, `Expected at least 1 message, got ${keys.length}`);

  const firstMsg = responseJson(runtimeInternalPost(`/?key=${keys[0]}`, {
    "x-worker-id": consumerWorkerId,
  }, ""));
  assert.deepEqual(firstMsg.body, { hello: "world" });
  assert.equal(firstMsg.queue, "orders");
  assert.equal(firstMsg.attempts, 1);
});

test("scheduler replicas: queue consumer group delivers each message once under two replicas", async () => {
  const ns = uniqueNs("qreplica");
  const queueName = "orders";
  const bodies = Array.from({ length: 12 }, (_, i) => `body-${i}`);

  await withServiceStopped("scheduler", async () => {
    const consumerVersion = await deployConsumer(ns, DELIVERY_SET_RECORDER, [
      { queue: queueName, maxBatchSize: 3, maxBatchTimeoutMs: 2000, maxRetries: 3 },
    ]);

    const producerVersion = await deployQueueProducer(ns, queueName);

    const sendRes = runtimeInternalPost("/", {
      "x-worker-id": gatewayWorkerId(ns, "producer", producerVersion),
      "content-type": "application/json",
    }, bodies);
    assertStatus(sendRes, 200, "producer send");

    composeScale("scheduler", 2);

    const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
    await waitUntil("all messages delivered once by scheduler replicas", async () => {
      const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
      if (res.status !== 200) return false;
      const snap = responseJson(res);
      return snap.deliveries.length === bodies.length;
    }, { timeoutMs: 30_000, intervalMs: 500 });

    await delay(2_000);
    const finalRes = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    assertStatus(finalRes, 200, "consumer final fetch");
    const snap = responseJson(finalRes);
    assert.equal(snap.deliveries.length, bodies.length, finalRes.body);
    assert.deepEqual([...new Set(snap.bodies)].toSorted(), bodies.toSorted());
    const streamKey = queueStreamKey(ns, queueName);
    assert.equal(redisXLen(streamKey, { db: 1 }), 0);
    assert.equal(redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 }), 0);
  });
});

test("scheduler reconcile restores multiple pre-existing consumer groups", async () => {
  const ns = uniqueNs("qreconcile");
  const queues = ["orders", "events", "audit"];
  const consumerVersion = await withServiceStopped("scheduler", async () => {
    const version = await deployConsumer(
      ns,
      DELIVERY_SET_RECORDER,
      queues.map((queue) => ({
        queue,
        maxBatchSize: 3,
        maxBatchTimeoutMs: 2000,
        maxRetries: 3,
      }))
    );
    for (const queue of queues) {
      redisXGroupCreate(queueStreamKey(ns, queue), "wdl-scheduler", { db: 1 });
      redisSRem(QUEUE_CONSUMER_INDEX_KEY, queueConsumerKey(ns, queue));
    }
    return version;
  });

  const streamKeys = queues.map((queue) => queueStreamKey(ns, queue));
  const consumerKeys = queues.map((queue) => queueConsumerKey(ns, queue));
  await waitUntil("all pre-existing queue indexes repaired at startup", async () => {
    const indexedStreams = new Set(redisSMembers(QUEUE_STREAM_INDEX_KEY, { db: 1 }));
    const indexedConsumers = new Set(redisSMembers(QUEUE_CONSUMER_INDEX_KEY));
    return streamKeys.every((key) => indexedStreams.has(key))
      && consumerKeys.every((key) => indexedConsumers.has(key));
  }, { timeoutMs: 15_000, intervalMs: 500 });

  streamKeys.forEach((streamKey, index) => {
    redisXAdd(
      streamKey,
      queueStreamMessageFields({
        id: `reconcile-${index}`,
        body: `body-${index}`,
        contentType: "text",
        firstSeenMs: index + 1,
      }),
      { db: 1 }
    );
  });

  const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
  await waitUntil("messages delivered from all reconciled groups", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    if (res.status !== 200) return false;
    return responseJson(res).bodies.length === queues.length;
  }, { timeoutMs: 30_000, intervalMs: 500 });

  const snapshot = responseJson(runtimeInternalPost("/", {
    "x-worker-id": consumerWorkerId,
  }, ""));
  assert.deepEqual(snapshot.bodies.toSorted(), ["body-0", "body-1", "body-2"]);
});

test("scheduler reconcile isolates a bad stream and continues into later consumer batches", async () => {
  const ns = uniqueNs("qreconcileerr");
  const queues = Array.from({ length: 130 }, (_, index) => `q${String(index).padStart(3, "0")}`);

  const setup = await withServiceStopped("scheduler", async () => {
    const consumerVersion = await deployConsumer(
      ns,
      DELIVERY_SET_RECORDER,
      queues.map((queue) => ({
        queue,
        maxBatchSize: 3,
        maxBatchTimeoutMs: 2000,
        maxRetries: 3,
      }))
    );
    const queueByConsumerKey = new Map(
      queues.map((queue) => [queueConsumerKey(ns, queue), queue])
    );
    const orderedConsumerKeys = redisSMembers(QUEUE_CONSUMER_INDEX_KEY)
      .filter((key) => queueByConsumerKey.has(key));
    assert.equal(orderedConsumerKeys.length, queues.length);

    const badQueue = queueByConsumerKey.get(orderedConsumerKeys[1]);
    const firstBatchQueue = queueByConsumerKey.get(orderedConsumerKeys[0]);
    const laterBatchQueue = queueByConsumerKey.get(orderedConsumerKeys[128]);
    assert.ok(badQueue && firstBatchQueue && laterBatchQueue);
    redisSet(queueStreamKey(ns, badQueue), "wrong-type", { db: 1 });

    return { consumerVersion, badQueue, firstBatchQueue, laterBatchQueue };
  });

  const badStream = queueStreamKey(ns, setup.badQueue);
  const firstBatchStream = queueStreamKey(ns, setup.firstBatchQueue);
  const laterBatchStream = queueStreamKey(ns, setup.laterBatchQueue);
  await waitUntil("healthy queue groups on both sides of reconcile failure", async () => {
    const indexed = new Set(redisSMembers(QUEUE_STREAM_INDEX_KEY, { db: 1 }));
    return indexed.has(firstBatchStream) && indexed.has(laterBatchStream);
  }, { timeoutMs: 30_000, intervalMs: 500 });
  assert.equal(
    new Set(redisSMembers(QUEUE_STREAM_INDEX_KEY, { db: 1 })).has(badStream),
    false
  );

  redisXAdd(
    laterBatchStream,
    queueStreamMessageFields({
      id: "later-batch",
      body: "later-batch",
      contentType: "text",
      firstSeenMs: 1,
    }),
    { db: 1 }
  );
  const consumerWorkerId = gatewayWorkerId(ns, "consumer", setup.consumerVersion);
  await waitUntil("later reconcile batch participates in queue consumption", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    if (res.status !== 200) return false;
    return responseJson(res).bodies.includes("later-batch");
  }, { timeoutMs: 30_000, intervalMs: 500 });
});

test("scheduler reconcile removes a registered stream that later becomes invalid", async () => {
  const ns = uniqueNs("qreconcilelateerr");
  const badQueue = "bad";
  const healthyQueue = "healthy";
  const consumerVersion = await deployConsumer(ns, DELIVERY_SET_RECORDER, [
    { queue: badQueue, maxBatchSize: 3, maxBatchTimeoutMs: 2000, maxRetries: 3 },
    { queue: healthyQueue, maxBatchSize: 3, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);
  const badStream = queueStreamKey(ns, badQueue);
  const healthyStream = queueStreamKey(ns, healthyQueue);

  await waitUntil("both queue groups registered before stream corruption", async () => {
    return [badStream, healthyStream].every((stream) => {
      const groups = redisXInfoGroups(stream, { db: 1 });
      return !groups.includes("missing") && groups.includes("wdl-scheduler");
    });
  }, { timeoutMs: 30_000, intervalMs: 500 });

  redisDel(badStream, { db: 1 });
  redisSet(badStream, "wrong-type", { db: 1 });

  const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
  redisXAdd(
    healthyStream,
    queueStreamMessageFields({
      id: "release-pre-corruption-read",
      body: "release-pre-corruption-read",
      contentType: "text",
      firstSeenMs: 1,
    }),
    { db: 1 }
  );
  await waitUntil("pre-corruption blocking read completes", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    return res.status === 200
      && responseJson(res).bodies.includes("release-pre-corruption-read");
  }, { timeoutMs: 30_000, intervalMs: 500 });

  redisXAdd(
    healthyStream,
    queueStreamMessageFields({
      id: "healthy-after-registry-removal",
      body: "healthy-after-registry-removal",
      contentType: "text",
      firstSeenMs: 2,
    }),
    { db: 1 }
  );
  await waitUntil("healthy queue continues after registry removal", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    return res.status === 200
      && responseJson(res).bodies.includes("healthy-after-registry-removal");
  }, { timeoutMs: 30_000, intervalMs: 500 });
});

test("queue dispatch rereads authoritative consumer hash before delivery", async () => {
  const ns = uniqueNs("qfresh");
  const queueName = "freshq";
  const streamKey = queueStreamKey(ns, queueName);
  const consumerKey = queueConsumerKey(ns, queueName);

  const v1 = await deployConsumer(ns, DELIVERY_SET_RECORDER, [
    { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);
  const v1WorkerId = gatewayWorkerId(ns, "consumer", v1);
  await waitUntil("consumer group created for initial version", async () => {
    const out = redisXInfoGroups(streamKey, { db: 1 });
    return !out.includes("missing") && out.includes("wdl-scheduler");
  }, { timeoutMs: 10_000, intervalMs: 500 });

  redisXAdd(
    streamKey,
    queueStreamMessageFields({ id: "before-promote", body: "before-promote", contentType: "text", firstSeenMs: 1 }),
    { db: 1 }
  );
  await waitUntil("initial version delivered first message", async () => {
    const res = runtimeInternalPost("/", { "x-worker-id": v1WorkerId }, "");
    if (res.status !== 200) return false;
    const snap = responseJson(res);
    return snap.bodies.includes("before-promote");
  }, { timeoutMs: 30_000, intervalMs: 500 });
  const v1SnapBeforePromote = responseJson(runtimeInternalPost("/", { "x-worker-id": v1WorkerId }, ""));
  assert.deepEqual(v1SnapBeforePromote.bodies, ["before-promote"]);

  const v2 = await deployQueueConsumerWorker(ns, "consumer-v2", DELIVERY_SET_RECORDER, []);
  const v2WorkerId = gatewayWorkerId(ns, "consumer-v2", v2);
  redisSRem(QUEUE_CONSUMER_INDEX_KEY, consumerKey);
  redisHSet(consumerKey, {
    worker: "consumer-v2",
    version: v2,
    max_batch_size: "1",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
    retry_delay_secs: "0",
  });
  redisXAdd(
    streamKey,
    queueStreamMessageFields({ id: "after-promote", body: "after-promote", contentType: "text", firstSeenMs: 2 }),
    { db: 1 }
  );

  let deliveredToStaleVersion = false;
  await waitUntil("authoritative consumer hash used for next message", async () => {
    const v1Res = runtimeInternalPost("/", { "x-worker-id": v1WorkerId }, "");
    if (v1Res.status === 200 && responseJson(v1Res).bodies.includes("after-promote")) {
      deliveredToStaleVersion = true;
      return true;
    }
    const v2Res = runtimeInternalPost("/", { "x-worker-id": v2WorkerId }, "");
    if (v2Res.status !== 200) return false;
    return responseJson(v2Res).bodies.includes("after-promote");
  }, { timeoutMs: 30_000, intervalMs: 500 });
  assert.equal(deliveredToStaleVersion, false);
});
