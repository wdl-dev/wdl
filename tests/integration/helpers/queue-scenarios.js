import {
  deployAndPromote,
} from "./admin-http.js";
import { sh } from "./cli.js";
import { gatewayWorkerId } from "./gateway-http.js";
import { responseJson } from "./http-response.js";
import { runtimeInternalPost } from "./internal-http.js";
import { redisFlushAll, redisXInfoGroups, redisXPendingCount } from "./redis.js";
import { setupIntegrationSuite, waitForScheduler, waitUntil } from "./stack.js";

export function setupQueueIntegrationSuite() {
  setupIntegrationSuite({
    async afterStackUp() {
      // Restart scheduler to drop any stale queueBlockClient / registry state
      // that might linger from prior test files.
      redisFlushAll();
      sh("docker compose restart scheduler", { stdio: "pipe" });
      await waitForScheduler();
    },
  });
}

// Consumer worker: stashes queue() messages in memory so tests can read them
// back through fetch without involving another platform service.
export const QUEUE_MEMORY_CONSUMER = `
const store = {};
export default {
  async fetch(req) {
    const url = new URL(req.url);
    const key = url.searchParams.get("key");
    if (key) return Response.json(store[key] ?? null);
    return Response.json(Object.keys(store));
  },
  async queue(batch, env, ctx) {
    for (const msg of batch.messages) {
      store[msg.id] = {
        body: msg.body,
        attempts: msg.attempts,
        queue: batch.queue,
      };
      msg.ack();
    }
  },
};
`;

export const ALWAYS_THROWS_QUEUE_CONSUMER = `
export default {
  fetch() { return new Response("ok"); },
  async queue() { throw new Error("always fail"); },
};
`;

export const ATTEMPT_RECORDER_THROWS_QUEUE_CONSUMER = `
let seen = [];
export default {
  async fetch() { return Response.json(seen); },
  async queue(batch) {
    for (const msg of batch.messages) seen.push(msg.attempts);
    throw new Error("always fail");
  },
};
`;

export const RETRY_ONCE_QUEUE_CONSUMER = `
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
        throw new Error("intentional first-attempt failure");
      }
      store[msg.id] = {
        body: msg.body,
        attempts: msg.attempts,
        queue: batch.queue,
      };
      msg.ack();
    }
  },
};
`;

export const QUEUE_PRODUCER = `
export default {
  async fetch(req, env) {
    const body = await req.json();
    if (Array.isArray(body)) {
      await env.MY_Q.sendBatch(body.map((m) => ({ body: m })));
    } else {
      await env.MY_Q.send(body);
    }
    return new Response("ok");
  },
};
`;

export const DELIVERY_SET_RECORDER = `
let deliveries = [];
export default {
  async fetch() {
    return Response.json({ deliveries, bodies: deliveries.map((d) => d.body) });
  },
  async queue(batch) {
    for (const msg of batch.messages) {
      deliveries.push({ id: msg.id, body: msg.body, attempts: msg.attempts });
      msg.ack();
    }
  },
};
`;

export const BATCH_SIZE_RECORDER = `
let sizes = [];
let total = 0;
export default {
  async fetch() { return Response.json({ sizes, total }); },
  async queue(batch) {
    sizes.push(batch.messages.length);
    for (const m of batch.messages) { total++; m.ack(); }
  },
};
`;

/** @param {number} delayMs */
function blockingBatchRecorder(delayMs) {
  return `
let sizes = [];
let total = 0;
export default {
  async fetch() { return Response.json({ sizes, total }); },
  async queue(batch) {
    sizes.push(batch.messages.length);
    await new Promise((resolve) => setTimeout(resolve, ${delayMs}));
    for (const m of batch.messages) {
      m.ack();
      total += 1;
    }
  },
};
`;
}

export const BLOCKING_BATCH_RECORDER = blockingBatchRecorder(5_000);
export const LONG_BLOCKING_BATCH_RECORDER = blockingBatchRecorder(30_000);

export const FAST_QUEUE_CONSUMER = `
const store = {};
export default {
  async fetch(req) {
    const url = new URL(req.url);
    return Response.json({ key: url.searchParams.get("key"), val: store[url.searchParams.get("key")] ?? null, total: Object.keys(store).length });
  },
  async queue(batch) {
    for (const m of batch.messages) { store[m.id] = m.body; m.ack(); }
  },
};
`;

/**
 * @param {string} ns
 * @param {string} name
 * @param {string} code
 * @param {Array<Record<string, unknown>>} queueConsumers
 */
export async function deployQueueConsumerWorker(ns, name, code, queueConsumers) {
  return await deployAndPromote(ns, name, { code, queueConsumers });
}

/**
 * @param {string} ns
 * @param {string} code
 * @param {Array<Record<string, unknown>>} queueConsumers
 */
export async function deployConsumer(ns, code, queueConsumers) {
  return await deployQueueConsumerWorker(ns, "consumer", code, queueConsumers);
}

/**
 * @param {string} ns
 * @param {string} [code]
 */
export async function deployConsumerWithoutQueues(ns, code = QUEUE_MEMORY_CONSUMER) {
  return await deployAndPromote(ns, "consumer", { code });
}

/**
 * @param {string} ns
 * @param {string} queue
 * @param {string} [producerName]
 */
export async function deployQueueProducer(ns, queue, producerName = "producer") {
  return await deployAndPromote(ns, producerName, {
    code: QUEUE_PRODUCER,
    bindings: { MY_Q: { type: "queue", id: queue } },
  });
}

/**
 * @param {string} ns
 * @param {string} producerName
 * @param {string} producerVersion
 * @param {unknown} body
 */
export function sendQueueMessage(ns, producerName, producerVersion, body) {
  return runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, producerName, producerVersion),
    "content-type": "application/json",
  }, body);
}

/**
 * @param {string} ns
 * @param {string} consumerVersion
 * @param {string} [consumerName]
 */
export function readConsumerKeys(ns, consumerVersion, consumerName = "consumer") {
  return responseJson(runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, consumerName, consumerVersion),
  }, ""));
}

/**
 * @param {string} ns
 * @param {string} consumerVersion
 * @param {string} key
 * @param {string} [consumerName]
 */
export function readConsumerMessage(ns, consumerVersion, key, consumerName = "consumer") {
  return responseJson(runtimeInternalPost(`/?key=${key}`, {
    "x-worker-id": gatewayWorkerId(ns, consumerName, consumerVersion),
  }, ""));
}

/**
 * @param {string} streamKey
 * @returns {number}
 */
export function queuePendingCount(streamKey) {
  return redisXPendingCount(streamKey, "wdl-scheduler", { db: 1 });
}

/**
 * @param {string|string[]} streams
 * @param {{ label?: string, timeoutMs?: number, intervalMs?: number }} [options]
 */
export async function waitForQueueConsumerGroups(streams, options = {}) {
  const streamKeys = Array.isArray(streams) ? streams : [streams];
  await waitUntil(
    options.label || "queue consumer groups ready",
    async () => streamKeys.every((streamKey) => {
      const groups = redisXInfoGroups(streamKey, { db: 1 });
      return !groups.includes("missing") && groups.includes("wdl-scheduler");
    }),
    {
      timeoutMs: options.timeoutMs ?? 30_000,
      intervalMs: options.intervalMs ?? 500,
    }
  );
}
