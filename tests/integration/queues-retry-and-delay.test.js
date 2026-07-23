import { test } from "node:test";
import assert from "node:assert/strict";
import {
  assertStatus,
  composeScale,
  delay,
  deployAndPromote,
  gatewayWorkerId,
  parseJsonText,
  runtimeInternalPost,
  sh,
  uniqueNs,
  waitUntil,
  withServiceStopped,
  responseJson,
  queueDelayedKey,
  QUEUE_DELAYED_INDEX_KEY,
  QUEUE_DELAYED_WAKE_STREAM,
  queueDlqKey,
  queueStreamKey,
} from "./helpers/index.js";
import {
  redisDel,
  redisCommandCalls,
  redisSet,
  redisSMembers,
  redisXLen,
  redisXPendingCount,
  redisZCard,
  redisZRange,
} from "./helpers/redis.js";
import {
  ALWAYS_THROWS_QUEUE_CONSUMER,
  ATTEMPT_RECORDER_THROWS_QUEUE_CONSUMER,
  DELIVERY_SET_RECORDER,
  QUEUE_MEMORY_CONSUMER,
  RETRY_ONCE_QUEUE_CONSUMER,
  deployConsumer,
  deployQueueProducer,
  readConsumerKeys,
  readConsumerMessage,
  sendQueueMessage,
  setupQueueIntegrationSuite,
} from "./helpers/queue-scenarios.js";

setupQueueIntegrationSuite();

/** @returns {any[]} */
function queueTransitionFailureLogs() {
  const raw = sh("docker compose logs --no-color --tail=500 scheduler");
  /** @type {any[]} */
  const failures = [];
  for (const line of raw.split("\n")) {
    const jsonStart = line.indexOf("{");
    if (jsonStart < 0) continue;
    const candidate = line.slice(jsonStart);
    if (!candidate.includes("\"event\":\"queue_message_transition_failed\"")) continue;
    try {
      const entry = parseJsonText(candidate, "queue transition failure log");
      if (entry.event === "queue_message_transition_failed") failures.push(entry);
    } catch {
      // Docker may interleave non-JSON service output with structured lines.
    }
  }
  return failures;
}

test("producer sendBatch batches delayed ZADD and emits one earliest-due wake", async () => {
  const ns = uniqueNs("qdelaybatch");
  const queueName = "delayq";
  const producerVersion = await deployAndPromote(ns, "producer", {
    code: `
      export default {
        async fetch(req, env) {
          await env.MY_Q.sendBatch([
            { body: "immediate", contentType: "text", delaySeconds: 0 },
            { body: "later", contentType: "text", delaySeconds: 60 },
            { body: "sooner", contentType: "text", delaySeconds: 30 },
          ]);
          return new Response("ok");
        },
      };`,
    bindings: { MY_Q: { type: "queue", id: queueName } },
  });

  await withServiceStopped("scheduler", async () => {
    const delayedKey = queueDelayedKey(ns, queueName);
    const zaddBefore = redisCommandCalls("zadd");
    const wakesBefore = redisXLen(QUEUE_DELAYED_WAKE_STREAM, { db: 1 });

    const sendRes = sendQueueMessage(ns, "producer", producerVersion, null);
    assertStatus(sendRes, 200, "mixed delayed batch send");

    assert.equal(redisCommandCalls("zadd"), zaddBefore + 1);
    assert.equal(redisXLen(queueStreamKey(ns, queueName), { db: 1 }), 1);
    assert.equal(redisZCard(delayedKey, { db: 1 }), 2);
    assert.equal(redisXLen(QUEUE_DELAYED_WAKE_STREAM, { db: 1 }), wakesBefore + 1);
    assert.deepEqual(
      redisZRange(delayedKey, 0, -1, { db: 1 }).map((member) => {
        const entry = JSON.parse(member);
        return Buffer.from(entry.body_b64, "base64").toString("utf8");
      }),
      ["sooner", "later"]
    );
  });
});

test("handler throw → implicit retryAll → default retry delay → second attempt delivered", async () => {
  const ns = uniqueNs("q");

  const consumerVersion = await deployConsumer(ns, RETRY_ONCE_QUEUE_CONSUMER, [
    { queue: "retryq", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3, retryDelaySeconds: 2 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "retryq");

  sendQueueMessage(ns, "producer", producerVersion, { retry_me: true });

  await waitUntil("implicit retry moved to delayed ZSET", async () => {
    return redisZCard(queueDelayedKey(ns, "retryq"), { db: 1 }) !== 0;
  }, { timeoutMs: 20_000, intervalMs: 1_000 });

  await waitUntil("retried message delivered", async () => {
    return readConsumerKeys(ns, consumerVersion).length > 0;
  }, { timeoutMs: 45_000, intervalMs: 1_000 });

  const keys = readConsumerKeys(ns, consumerVersion);
  assert.ok(keys.length >= 1);

  const msg = readConsumerMessage(ns, consumerVersion, keys[0]);
  assert.deepEqual(msg.body, { retry_me: true });
  assert.ok(msg.attempts >= 2, `Expected attempts >= 2, got ${msg.attempts}`);
});

test("maxRetries caps implicit retry — bad message goes to DLQ, not looped forever", async () => {
  const ns = uniqueNs("q");

  await deployConsumer(ns, ALWAYS_THROWS_QUEUE_CONSUMER, [
    { queue: "capq", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 2 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "capq");

  sendQueueMessage(ns, "producer", producerVersion, { poison: true });

  await waitUntil("message moved to DLQ", async () => {
    return redisXLen(queueDlqKey(ns, "capq"), { db: 1 }) !== 0;
  }, { timeoutMs: 30_000, intervalMs: 1_000 });

  const streamLen = redisXLen(queueStreamKey(ns, "capq"), { db: 1 });
  assert.equal(streamLen, 0, `Main stream should be empty, got XLEN=${streamLen}`);

  const dlqLen = redisXLen(queueDlqKey(ns, "capq"), { db: 1 });
  assert.equal(dlqLen, 1, `DLQ should have 1 entry, got ${dlqLen}`);
});

test("retry and DLQ target failures retain their source messages", async () => {
  const ns = uniqueNs("qtransitionfail");
  const suffix = Math.random().toString(36).slice(2, 10);
  const retryQueue = `retry-${suffix}`;
  const terminalQueue = `terminal-${suffix}`;
  const dlqQueue = `failures-${suffix}`;
  const retryStream = queueStreamKey(ns, retryQueue);
  const terminalStream = queueStreamKey(ns, terminalQueue);
  const delayedKey = queueDelayedKey(ns, retryQueue);
  const dlqKey = queueDlqKey(ns, dlqQueue);

  await deployConsumer(ns, ALWAYS_THROWS_QUEUE_CONSUMER, [
    {
      queue: retryQueue,
      maxBatchSize: 1,
      maxBatchTimeoutMs: 2000,
      maxRetries: 3,
      retryDelaySeconds: 2,
    },
    {
      queue: terminalQueue,
      maxBatchSize: 1,
      maxBatchTimeoutMs: 2000,
      maxRetries: 0,
      deadLetterQueue: dlqQueue,
    },
  ]);
  const producerVersion = await deployAndPromote(ns, "producer", {
    code: `
      export default {
        async fetch(_req, env) {
          await Promise.all([
            env.RETRY.send("retry"),
            env.TERMINAL.send("terminal"),
          ]);
          return new Response("ok");
        },
      };`,
    bindings: {
      RETRY: { type: "queue", id: retryQueue },
      TERMINAL: { type: "queue", id: terminalQueue },
    },
  });

  redisSet(delayedKey, "wrong-type", { db: 1 });
  redisSet(dlqKey, "wrong-type", { db: 1 });
  const sendRes = sendQueueMessage(ns, "producer", producerVersion, null);
  assertStatus(sendRes, 200, "target-failure producer send");

  /** @type {any[]} */
  let transitionFailures = [];
  await waitUntil("both failed queue transitions logged", () => {
    transitionFailures = queueTransitionFailureLogs();
    return transitionFailures.some((entry) => entry.queue === retryQueue && entry.action === "delay")
      && transitionFailures.some((entry) => entry.queue === terminalQueue && entry.action === "dlq");
  }, { timeoutMs: 20_000, intervalMs: 250 });
  const retryFailure = transitionFailures.find(
    (entry) => entry.queue === retryQueue && entry.action === "delay"
  );
  const terminalFailure = transitionFailures.find(
    (entry) => entry.queue === terminalQueue && entry.action === "dlq"
  );
  assert.match(retryFailure.error_message, /WRONGTYPE/i);
  assert.match(terminalFailure.error_message, /WRONGTYPE/i);

  assert.equal(redisXLen(retryStream, { db: 1 }), 1);
  assert.equal(redisXPendingCount(retryStream, "wdl-scheduler", { db: 1 }), 1);
  assert.equal(redisXLen(terminalStream, { db: 1 }), 1);
  assert.equal(redisXPendingCount(terminalStream, "wdl-scheduler", { db: 1 }), 1);
  await waitUntil("wrong-type delayed target removed from discovery", async () => {
    return !redisSMembers(QUEUE_DELAYED_INDEX_KEY, { db: 1 }).includes(delayedKey);
  }, { timeoutMs: 10_000, intervalMs: 500 });

  redisDel(delayedKey, { db: 1 });
  redisDel(dlqKey, { db: 1 });
});

test("one failed transition does not block a healthy sibling in the same batch", async () => {
  const ns = uniqueNs("qtransitionmixed");
  const queueName = `mixed-${Math.random().toString(36).slice(2, 10)}`;
  const streamKey = queueStreamKey(ns, queueName);
  const delayedKey = queueDelayedKey(ns, queueName);
  const consumerVersion = await deployConsumer(ns, `
    const batches = [];
    const completed = [];
    export default {
      fetch() {
        return Response.json({ batches, completed });
      },
      async queue(batch) {
        batches.push(batch.messages.map((msg) => ({
          kind: msg.body.kind,
          attempts: msg.attempts,
        })));
        for (const msg of batch.messages) {
          if (msg.body.kind === "blocked") {
            msg.retry({ delaySeconds: 2 });
          } else if (msg.attempts === 1) {
            msg.retry({ delaySeconds: 0 });
          } else {
            completed.push({ kind: msg.body.kind, attempts: msg.attempts });
            msg.ack();
          }
        }
      },
    };
  `, [{
    queue: queueName,
    maxBatchSize: 2,
    maxBatchTimeoutMs: 2000,
    maxRetries: 3,
  }]);
  const producerVersion = await deployQueueProducer(ns, queueName);

  await withServiceStopped("scheduler", async () => {
    redisSet(delayedKey, "wrong-type", { db: 1 });
    const sendRes = sendQueueMessage(ns, "producer", producerVersion, [
      { kind: "blocked" },
      { kind: "healthy" },
    ]);
    assertStatus(sendRes, 200, "mixed transition producer send");
    assert.equal(redisXLen(streamKey, { db: 1 }), 2);
  });

  await waitUntil("mixed batch failure and healthy retry both complete", () => {
    const failures = queueTransitionFailureLogs();
    const consumerState = responseJson(runtimeInternalPost("/", {
      "x-worker-id": gatewayWorkerId(ns, "consumer", consumerVersion),
    }, ""));
    return failures.some((entry) => (
      entry.queue === queueName && entry.action === "delay"
    )) && consumerState.completed.some((/** @type {any} */ entry) => (
      entry.kind === "healthy" && entry.attempts === 2
    ));
  }, { timeoutMs: 20_000, intervalMs: 250 });

  const consumerState = responseJson(runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "consumer", consumerVersion),
  }, ""));
  assert.deepEqual(
    consumerState.batches[0].map((/** @type {any} */ entry) => entry.kind).sort(),
    ["blocked", "healthy"],
  );
  assert.deepEqual(consumerState.completed, [{ kind: "healthy", attempts: 2 }]);
  await waitUntil("only the failed source message remains", () => (
    redisXLen(streamKey, { db: 1 }) === 1
      && redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 }) === 1
  ), { timeoutMs: 10_000, intervalMs: 250 });

  redisDel(delayedKey, { db: 1 });
});

test("maxRetries=N means handler sees the message N+1 times before DLQ", async () => {
  const ns = uniqueNs("q");

  const consumerVersion = await deployConsumer(ns, ATTEMPT_RECORDER_THROWS_QUEUE_CONSUMER, [
    { queue: "retry-count-q", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 2 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "retry-count-q");

  sendQueueMessage(ns, "producer", producerVersion, { count_me: true });

  // Wait for DLQ — by then the handler has seen every attempt.
  await waitUntil("message reached DLQ", async () => {
    return redisXLen(queueDlqKey(ns, "retry-count-q"), { db: 1 }) !== 0;
  }, { timeoutMs: 30_000, intervalMs: 1_000 });

  const seen = responseJson(runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "consumer", consumerVersion),
  }, ""));
  assert.deepEqual(seen, [1, 2, 3],
    `maxRetries=2 should yield attempts [1,2,3] (1 initial + 2 retries), got ${JSON.stringify(seen)}`);
});

test("producer delivery_delay default → message appears after delay", async () => {
  const ns = uniqueNs("q");

  const consumerVersion = await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: "delayq", maxBatchSize: 10, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const DELAY_PRODUCER = `
  export default {
    async fetch(req, env) {
      const body = await req.json();
      if (body.delay === undefined) await env.MY_Q.send(body.msg);
      else await env.MY_Q.send(body.msg, { delaySeconds: body.delay });
      return new Response("ok");
    },
  };`;

  const producerVersion = await deployAndPromote(ns, "producer", {
    code: DELAY_PRODUCER,
    bindings: { MY_Q: { type: "queue", id: "delayq", deliveryDelaySeconds: 3 } },
  });

  const sendRes = sendQueueMessage(ns, "producer", producerVersion, { msg: { delayed: true } });
  assertStatus(sendRes, 200, "delayed producer send");

  // Delayed sends must not touch the main stream until the wall-clock
  // dispatcher promotes them.
  assert.equal(redisXLen(queueStreamKey(ns, "delayq"), { db: 1 }), 0, "Message should not be in main stream immediately");
  assert.equal(redisZCard(queueDelayedKey(ns, "delayq"), { db: 1 }), 1, "Message should be in delayed ZSET");

  await waitUntil("delayed message delivered", async () => {
    return readConsumerKeys(ns, consumerVersion).length > 0;
  }, { timeoutMs: 30_000, intervalMs: 1_000 });

  const keys = readConsumerKeys(ns, consumerVersion);
  assert.ok(keys.length >= 1);
  const msg = readConsumerMessage(ns, consumerVersion, keys[0]);
  assert.deepEqual(msg.body, { delayed: true });
  assert.equal(msg.queue, "delayq");
});

test("scheduler replicas: delayed queue promotion claims each due member once", async () => {
  const ns = uniqueNs("qdelreplica");
  const queueName = "delayq";
  const bodies = Array.from({ length: 8 }, (_, i) => ({ id: `delay-${i}` }));

  const consumerVersion = await deployConsumer(ns, DELIVERY_SET_RECORDER, [
    { queue: queueName, maxBatchSize: 4, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const DELAY_PRODUCER = `
  export default {
    async fetch(req, env) {
      const messages = await req.json();
      for (const msg of messages) {
        await env.MY_Q.send(msg, { delaySeconds: 1 });
      }
      return new Response("ok");
    },
  };`;
  const producerVersion = await deployAndPromote(ns, "producer", {
    code: DELAY_PRODUCER,
    bindings: { MY_Q: { type: "queue", id: queueName } },
  });

  await withServiceStopped("scheduler", async () => {
    const sendRes = sendQueueMessage(ns, "producer", producerVersion, bodies);
    assertStatus(sendRes, 200, "delayed replica producer send");

    await waitUntil("delayed members parked before replica promotion", async () => {
      return redisZCard(queueDelayedKey(ns, queueName), { db: 1 }) === bodies.length;
    }, { timeoutMs: 10_000, intervalMs: 500 });
    await delay(1_300);

    composeScale("scheduler", 2);

    const consumerWorkerId = gatewayWorkerId(ns, "consumer", consumerVersion);
    await waitUntil("all delayed messages promoted exactly once", async () => {
      const res = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
      if (res.status !== 200) return false;
      const snap = responseJson(res);
      return snap.deliveries.length === bodies.length;
    }, { timeoutMs: 30_000, intervalMs: 500 });

    await delay(2_000);
    const finalRes = runtimeInternalPost("/", { "x-worker-id": consumerWorkerId }, "");
    assertStatus(finalRes, 200, "delayed replica final fetch");
    const snap = responseJson(finalRes);
    assert.equal(snap.deliveries.length, bodies.length, finalRes.body);
    assert.deepEqual(
      [...new Set(snap.bodies.map((/** @type {any} */ body) => body.id))].toSorted(),
      bodies.map((body) => body.id).toSorted()
    );
    const streamKey = queueStreamKey(ns, queueName);
    assert.equal(redisZCard(queueDelayedKey(ns, queueName), { db: 1 }), 0);
    assert.equal(redisXLen(streamKey, { db: 1 }), 0);
    assert.equal(redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 }), 0);
  });
});

test("delayed queue wake lets a later near-term message beat an older far-future one", async () => {
  const ns = uniqueNs("q");

  const consumerVersion = await deployConsumer(ns, QUEUE_MEMORY_CONSUMER, [
    { queue: "wakeq", maxBatchSize: 10, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployAndPromote(ns, "producer", {
    code: `
    export default {
      async fetch(req, env) {
        const body = await req.json();
        await env.MY_Q.send(body.msg, { delaySeconds: body.delay });
        return new Response("ok");
      },
    };`,
    bindings: { MY_Q: { type: "queue", id: "wakeq" } },
  });

  const headers = {
    "x-worker-id": gatewayWorkerId(ns, "producer", producerVersion),
    "content-type": "application/json",
  };

  composeScale("scheduler", 2);
  try {
    runtimeInternalPost("/", headers, { msg: { id: "far" }, delay: 60 });
    await waitUntil("far-future delayed message is parked", async () => {
      return redisZCard(queueDelayedKey(ns, "wakeq"), { db: 1 }) === 1;
    }, { timeoutMs: 10_000, intervalMs: 500 });
    await delay(1000);

    runtimeInternalPost("/", headers, { msg: { id: "near" }, delay: 2 });

    await waitUntil("near delayed message delivered before far-future item", async () => {
      const keys = readConsumerKeys(ns, consumerVersion);
      if (!Array.isArray(keys) || keys.length === 0) return false;
      for (const key of keys) {
        const msg = readConsumerMessage(ns, consumerVersion, key);
        if (msg?.body?.id === "near") return true;
      }
      return false;
    }, { timeoutMs: 12_000, intervalMs: 500 });
  } finally {
    composeScale("scheduler", 1);
  }
});

test("retry with delaySeconds routes through delayed ZSET before redelivery", async () => {
  const ns = uniqueNs("q");

  const DELAY_RETRY_CONSUMER = `
  const store = {};
  export default {
    async fetch(req) {
      const url = new URL(req.url);
      const key = url.searchParams.get("key");
      if (key) return Response.json(store[key] ?? null);
      return Response.json(Object.keys(store));
    },
    async queue(batch) {
      for (const msg of batch.messages) {
        if (msg.attempts === 1) {
          msg.retry({ delaySeconds: 2 });
        } else {
          store[msg.id] = { body: msg.body, attempts: msg.attempts };
          msg.ack();
        }
      }
    },
  };`;

  const consumerVersion = await deployConsumer(ns, DELAY_RETRY_CONSUMER, [
    { queue: "dretryq", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ]);

  const producerVersion = await deployQueueProducer(ns, "dretryq");

  sendQueueMessage(ns, "producer", producerVersion, { retry_delayed: true });

  await waitUntil("delayed retry delivered", async () => {
    return readConsumerKeys(ns, consumerVersion).length > 0;
  }, { timeoutMs: 45_000, intervalMs: 1_000 });

  const keys = readConsumerKeys(ns, consumerVersion);
  assert.ok(keys.length >= 1);
  const msg = readConsumerMessage(ns, consumerVersion, keys[0]);
  assert.deepEqual(msg.body, { retry_delayed: true });
  assert.ok(msg.attempts >= 2, `Expected attempts >= 2, got ${msg.attempts}`);
});
