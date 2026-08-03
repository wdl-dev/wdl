// Per-stream queue dispatch tasks must participate in scheduler shutdown
// drain. Without it SIGTERM aborts the spawn mid-batch, leaving the message
// XREADGROUP'd into PEL but never XACK+XDEL'd. PEL recovery via XAUTOCLAIM
// still works, but a clean shutdown should leave no half-acked work.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminPost,
  assertStatus,
  composeUp,
  deployAndPromote,
  gatewayWorkerId,
  runtimeInternalPost,
  setupIntegrationSuite,
  sh,
  uniqueNs,
  waitUntil,
  waitForScheduler,
  queueStreamKey,
} from "./helpers/index.js";
import { waitForQueueConsumerGroups } from "./helpers/queue-scenarios.js";
import { redisFlushAll, redisXLen, redisXPendingCount } from "./helpers/redis.js";

setupIntegrationSuite({
  async afterStackUp() {
    // Drop stale registry/PEL from prior suites (same hygiene as queues-delivery.test.js).
    redisFlushAll();
    sh(["docker", "compose", "restart", "scheduler"], { stdio: "pipe" });
    await waitForScheduler();
  },
});

// 2s sleep is long enough to overlap the SIGTERM but short enough to fit
// inside docker compose stop's 10s default kill window.
const SLOW_CONSUMER = `
let seen = 0;
export default {
  async fetch() { return Response.json({ seen }); },
  async queue(batch) {
    for (const msg of batch.messages) {
      await new Promise((r) => setTimeout(r, 2000));
      seen += 1;
      msg.ack();
    }
  },
};
`;

const PRODUCER = `
export default {
  async fetch(req, env) {
    await env.MY_Q.send(await req.json());
    return new Response("ok");
  },
};
`;

test("scheduler: SIGTERM mid-dispatch drains in-flight queue handler before exit", async () => {
  const ns = uniqueNs("draindrn");
  const queueName = "drainq";
  const streamKey = queueStreamKey(ns, queueName);

  const cdep = await adminPost(`/ns/${ns}/worker/c/deploy`, {
    code: SLOW_CONSUMER,
    queueConsumers: [
      { queue: queueName, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
    ],
  });
  assertStatus(cdep, 201, "consumer deploy");
  const cprom = await adminPost(`/ns/${ns}/worker/c/promote`, { version: cdep.json.version });
  assertStatus(cprom, 200, "consumer promote");

  const producerVersion = await deployAndPromote(ns, "p", {
    code: PRODUCER,
    bindings: { MY_Q: { type: "queue", id: queueName } },
  });

  // Wait for the consumer group so XADD lands while XREADGROUP BLOCK is active,
  // not while reconcile is still pending.
  await waitForQueueConsumerGroups(streamKey, {
    label: "scheduler registers consumer group",
    timeoutMs: 15_000,
    intervalMs: 500,
  });

  const sendRes = runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "p", producerVersion),
    "content-type": "application/json",
  }, { drain: "test" });
  assertStatus(sendRes, 200, "producer send");

  // Wait until the message is in PEL — that's when the dispatch task is
  // in-flight and SIGTERM actually races something.
  await waitUntil("scheduler picks up message into PEL", async () => {
    return redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 }) >= 1;
  }, { timeoutMs: 10_000, intervalMs: 200 });

  // -t 30s ≥ scheduler drain (25s) ≥ consumer latency (2s); docker default
  // 10s would SIGKILL before drain could finish in pathological cases.
  sh(["docker", "compose", "stop", "-t", "30", "scheduler"], { stdio: "pipe" });

  try {
    const pelCountAfter = redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 });
    assert.equal(
      pelCountAfter,
      0,
      `PEL must be empty after graceful drain, got count ${pelCountAfter}`
    );

    const xlenAfter = redisXLen(streamKey, { db: 1 });
    assert.equal(
      xlenAfter,
      0,
      `main stream must be empty after graceful drain (XACK+XDEL ran), got XLEN=${xlenAfter}`
    );
  } finally {
    // Must restore even if asserts threw — otherwise queues/crons/s3-cleanup
    // suites cascade-fail. --wait blocks on the healthcheck, not on Redis.
    composeUp(["--wait", "scheduler"], { stdio: "pipe" });
  }
});
