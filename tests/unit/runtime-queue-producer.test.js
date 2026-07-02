import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  runtimeLibModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { withRecordingFetch } from "../helpers/mock-fetch.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { parseBase64Json } from "../helpers/json-payload.js";
import { parseJsonArrayRequestBody } from "../helpers/request-body.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const RUNTIME_LIB_URL = runtimeLibModuleDataUrl();
const IMMEDIATE_VISIBLE_AT = 0;
// Arbitrary fixed Unix epoch ms for deterministic time-based queue assertions.
const MOCK_TIMESTAMP_BASE_MS = 1_700_000_000_000;
const mod = await importRepositoryModule("runtime/bindings/queue.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [
    /from "runtime-lib";/,
    `from ${JSON.stringify(RUNTIME_LIB_URL)};`
  ],
  [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
  [
    /from "runtime-bindings-proxy";/,
    `from ${JSON.stringify(PROXY_BINDING_URL)};`
  ],
]);
const { QueueProducer } = mod;
const { MAX_QUEUE_DELAY_SECONDS } = await import(RUNTIME_LIB_URL);
const delayRangeErrorPattern = new RegExp(
  String.raw`delaySeconds must be an integer in \[0, ${MAX_QUEUE_DELAY_SECONDS}\]`
);

/**
 * @typedef {{
 *   first_seen_ms: string,
 *   content_type: string,
 *   attempts: string,
 *   id: string,
 *   body_b64: string,
 * }} QueueEntryPayload
 * @typedef {{ visibleAt: number, entry: QueueEntryPayload }} QueueActionPayload
 * @typedef {{ url: string, init: RequestInit, body: QueueActionPayload[] }} QueueFetchCall
 */

/** @param {QueueActionPayload} queueAction */
function calculateVisibleDelayMs(queueAction) {
  return Number(queueAction.visibleAt) - Number(queueAction.entry.first_seen_ms);
}

function makeQueue(props = {}) {
  return new QueueProducer(
    { props: { ns: "demo", id: "orders", ...props } },
    {
      SERVICE_NAME: "user-runtime",
      REDIS_PROXY_URL: "http://redis-proxy:8080/",
      WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    }
  );
}

/** @param {any} response */
function assertQueueResponseShape(response) {
  assert.deepEqual(response, {
    metadata: {
      metrics: {
        backlogCount: 0,
        backlogBytes: 0,
      },
    },
  });
}

/**
 * @param {QueueFetchCall[]} calls
 * @param {() => unknown | Promise<unknown>} callback
 */
async function withQueueFetch(calls, callback) {
  await withRecordingFetch(calls, callback, {
    capture: (call, _url, init) => ({
      url: call.url,
      init,
      body: assertQueueActionArray(parseJsonArrayRequestBody(init, "queue producer request body")),
    }),
  });
}

/** @param {unknown} queueAction */
function assertQueueActionShape(queueAction) {
  assert.ok(queueAction && typeof queueAction === "object", "expected queueAction object");
  const action = /** @type {Record<string, unknown>} */ (queueAction);
  assert.ok("visibleAt" in action, "expected queueAction.visibleAt");
  assert.equal(typeof action.visibleAt, "number", "expected queueAction.visibleAt to be a number");
  assert.ok(action.entry && typeof action.entry === "object", "expected queueAction.entry object");
  const entry = /** @type {Record<string, unknown>} */ (action.entry);
  assert.ok("first_seen_ms" in entry, "expected queueAction.entry.first_seen_ms");
  assert.equal(typeof entry.first_seen_ms, "string", "expected queueAction.entry.first_seen_ms to be a string");
  assert.ok("content_type" in entry, "expected queueAction.entry.content_type");
  assert.equal(typeof entry.content_type, "string", "expected queueAction.entry.content_type to be a string");
  assert.ok("attempts" in entry, "expected queueAction.entry.attempts");
  assert.equal(typeof entry.attempts, "string", "expected queueAction.entry.attempts to be a string");
  assert.ok("id" in entry, "expected queueAction.entry.id");
  assert.equal(typeof entry.id, "string", "expected queueAction.entry.id to be a string");
  assert.ok("body_b64" in entry, "expected queueAction.entry.body_b64");
  assert.equal(typeof entry.body_b64, "string", "expected queueAction.entry.body_b64 to be a string");
  return /** @type {QueueActionPayload} */ (queueAction);
}

/** @param {unknown} body */
function assertQueueActionArray(body) {
  assert.ok(Array.isArray(body), "expected queue action array");
  return body.map((queueAction) => assertQueueActionShape(queueAction));
}

/**
 * Validates that a queue action is immediately visible and still records its enqueue time.
 * Immediate visibility is encoded with the `IMMEDIATE_VISIBLE_AT` sentinel, while
 * `first_seen_ms` preserves the enqueue timestamp.
 * @param {unknown} queueAction The queue action to validate.
 * @param {number} expectedFirstSeenMs The expected first-seen timestamp in milliseconds.
 */
function assertImmediateQueueAction(queueAction, expectedFirstSeenMs) {
  const action = assertQueueActionShape(queueAction);
  assert.equal(action.visibleAt, IMMEDIATE_VISIBLE_AT);
  assert.equal(Number(action.entry.first_seen_ms), expectedFirstSeenMs);
}

/** @param {QueueFetchCall[]} calls */
async function runSendSingleMessageAssertions(calls) {
  const q = makeQueue();
  const delaySeconds = 7;
  const response = await q.send({ hello: "world" }, { delaySeconds });

  assertQueueResponseShape(response);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://redis-proxy:8080/queue/send?ns=demo&id=orders");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].body.length, 1);
  assert.equal(calls[0].body[0].entry.content_type, "json");
  assert.equal(calls[0].body[0].entry.attempts, "0");
  assert.equal(typeof calls[0].body[0].entry.id, "string");
  assert.notEqual(calls[0].body[0].entry.id.trim(), "");
  assert.equal(calculateVisibleDelayMs(calls[0].body[0]), delaySeconds * 1_000);
  assert.deepEqual(
    parseBase64Json(calls[0].body[0].entry.body_b64, "queue producer message body"),
    { hello: "world" }
  );
}

test("QueueProducer exposes only public methods", () => {
  assert.deepEqual(Object.getOwnPropertyNames(QueueProducer.prototype).toSorted(), [
    "constructor",
    "metrics",
    "send",
    "sendBatch",
  ]);
});

test("QueueProducer.send returns QueueSendResponse shape and writes one action", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    await withMockedProperty(Date, "now", () => MOCK_TIMESTAMP_BASE_MS, async () => {
      await runSendSingleMessageAssertions(calls);
    });
  });
});

test("QueueProducer.send uses deliveryDelaySeconds unless delaySeconds is explicit", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const nowMs = MOCK_TIMESTAMP_BASE_MS + 200_000;
    await withMockedProperty(Date, "now", () => nowMs, async () => {
      const deliveryDelaySeconds = 9;
      const q = makeQueue({ deliveryDelaySeconds });
      await q.send("default-delay", { contentType: "text" });
      await q.send("immediate", { contentType: "text", delaySeconds: 0 });

      assert.equal(Number(calls[0].body[0].visibleAt), nowMs + deliveryDelaySeconds * 1_000);
      assert.equal(Number(calls[0].body[0].entry.first_seen_ms), nowMs);
      assertImmediateQueueAction(calls[1].body[0], nowMs);
    });
  });
});

test("QueueProducer.send rejects invalid delaySeconds before fetch", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const q = makeQueue();
    await assert.rejects(
      q.send("invalid-negative-delay", { contentType: "text", delaySeconds: -1 }),
      /delaySeconds must be an integer/
    );
    await assert.rejects(
      q.send("invalid-fractional-delay", { contentType: "text", delaySeconds: 1.5 }),
      /delaySeconds must be an integer/
    );
    await assert.rejects(
      q.send("invalid-too-large-delay", { contentType: "text", delaySeconds: MAX_QUEUE_DELAY_SECONDS + 1 }),
      delayRangeErrorPattern
    );
    assert.equal(calls.length, 0);
  });
});

test("QueueProducer.send rejects non-serializable JSON bodies before fetch", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const q = makeQueue();
    await assert.rejects(
      q.send(undefined),
      /queue send: json contentType requires JSON-serializable body/
    );
    await assert.rejects(
      q.send(() => {}),
      /queue send: json contentType requires JSON-serializable body/
    );
    assert.equal(calls.length, 0);
  });
});

test("QueueProducer.sendBatch rejects non-serializable JSON bodies before fetch", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const q = makeQueue();
    await assert.rejects(
      q.sendBatch([() => {}]),
      /queue send: json contentType requires JSON-serializable body/
    );
    await assert.rejects(
      q.sendBatch([{ body: undefined }]),
      /queue send: json contentType requires JSON-serializable body/
    );
    await assert.rejects(
      q.sendBatch([undefined]),
      /queue send: json contentType requires JSON-serializable body/
    );
    assert.equal(calls.length, 0);
  });
});

test("QueueProducer.sendBatch returns QueueSendBatchResponse shape and writes all actions", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const nowMs = MOCK_TIMESTAMP_BASE_MS + 300_000;
    await withMockedProperty(Date, "now", () => nowMs, async () => {
      const q = makeQueue();
      const messageDelaySeconds = 1;
      const response = await q.sendBatch([
        { body: "one", contentType: "text" },
        { body: new Uint8Array([1, 2, 3]), contentType: "bytes", delaySeconds: messageDelaySeconds },
        null,
      ]);

      assertQueueResponseShape(response);
      assert.equal(calls.length, 1);
      assert.equal(calls[0].body.length, 3);
      assert.equal(calls[0].body[0].entry.content_type, "text");
      assertImmediateQueueAction(calls[0].body[0], nowMs);
      assert.equal(calls[0].body[1].entry.content_type, "bytes");
      assert.equal(calculateVisibleDelayMs(calls[0].body[1]), messageDelaySeconds * 1_000);
      assert.equal(calls[0].body[2].entry.content_type, "json");
      assert.deepEqual(
        parseBase64Json(calls[0].body[2].entry.body_b64, "queue producer null body"),
        null
      );
    });
  });
});

test("QueueProducer.sendBatch rejects empty batch and performs no fetch", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const q = makeQueue();
    await assert.rejects(
      q.sendBatch([]),
      /messages must be a non-empty array/
    );
    assert.equal(calls.length, 0);
  });
});

test("QueueProducer.sendBatch applies binding, batch, and per-message delay precedence", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const nowMs = MOCK_TIMESTAMP_BASE_MS + 100_000;
    await withMockedProperty(Date, "now", () => nowMs, async () => {
      const deliveryDelaySeconds = 8;
      const batchDelaySeconds = 5;
      const messageDelaySeconds = 3;
      const q = makeQueue({ deliveryDelaySeconds });
      await q.sendBatch([
        { body: "binding-default", contentType: "text" },
        { body: "immediate", contentType: "text", delaySeconds: 0 },
      ]);
      await q.sendBatch([
        { body: "batch-default", contentType: "text" },
        { body: "message-delay", contentType: "text", delaySeconds: messageDelaySeconds },
      ], { delaySeconds: batchDelaySeconds });

      assert.equal(calculateVisibleDelayMs(calls[0].body[0]), deliveryDelaySeconds * 1_000);
      assertImmediateQueueAction(calls[0].body[1], nowMs);
      const batchDefaultAction = calls[1].body[0];
      const messageDelayAction = calls[1].body[1];
      assert.equal(calculateVisibleDelayMs(batchDefaultAction), batchDelaySeconds * 1_000);
      assert.equal(calculateVisibleDelayMs(messageDelayAction), messageDelaySeconds * 1_000);
      // The per-message delay is shorter than the batch default, so it should become visible earlier.
      assert.ok(messageDelayAction.visibleAt < batchDefaultAction.visibleAt);
    });
  });
});

test("QueueProducer.sendBatch accepts the maximum delaySeconds value", async () => {
  /** @type {QueueFetchCall[]} */
  const calls = [];
  await withQueueFetch(calls, async () => {
    const nowMs = MOCK_TIMESTAMP_BASE_MS + 400_000;
    await withMockedProperty(Date, "now", () => nowMs, async () => {
      const q = makeQueue();
      await q.sendBatch([
        { body: "max-delay", contentType: "text" },
      ], { delaySeconds: MAX_QUEUE_DELAY_SECONDS });

      assert.equal(calls.length, 1);
      assert.equal(calls[0].body.length, 1);
      assert.equal(calculateVisibleDelayMs(calls[0].body[0]), MAX_QUEUE_DELAY_SECONDS * 1_000);
      assert.equal(Number(calls[0].body[0].entry.first_seen_ms), nowMs);
      assert.ok(Number.isFinite(calls[0].body[0].visibleAt));
    });
  });
});

test("QueueProducer.metrics returns QueueMetrics shape", async () => {
  const q = makeQueue();

  assert.deepEqual(await q.metrics(), {
    backlogCount: 0,
    backlogBytes: 0,
  });
});
