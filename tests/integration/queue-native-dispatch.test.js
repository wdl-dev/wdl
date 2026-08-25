// Pins the workerd-native queue dispatch contract that the scheduler
// builds on: workerLoader returns a Fetcher stub whose .queue(name,
// messages[]) method invokes the loaded worker's `queue` handler with
// native ack / retry semantics. Also pins the WDL internal envelope
// /_queued accepts (body_b64 + content_type + internal retry count →
// decoded JS value + Cloudflare-facing attempts). If a workerd upgrade
// or the `service_binding_extra_handlers` flag changes shape, this test
// trips before any downstream code silently stops delivering messages.
import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import {
  deployAndPromote,
  assertStatus,
  gatewayWorkerId,
  runtimeDispatchPost,
  runtimeInternalPost,
  uniqueNs,
  setupIntegrationSuite,
  responseJson,
  queueStreamMessage,
} from "./helpers/index.js";

setupIntegrationSuite();

const CONSUMER = readFileSync(
  new URL("../../test-workers/queue-native-consumer/src/index.js", import.meta.url),
  "utf8"
);
/** @type {{ outerOutcomes: { ok: string }, innerOutcomes: { ok: string, exception: string }, requiredResultFields: string[] }} */
const QUEUE_RUNTIME_RESPONSE_CONTRACT = readRepositoryJson(
  "tests/fixtures/queue-runtime-response.json"
);

/** @param {any} out @param {string} [expectedInnerOutcome] */
function assertNativeQueueResult(
  out,
  expectedInnerOutcome = QUEUE_RUNTIME_RESPONSE_CONTRACT.innerOutcomes.ok
) {
  assert.equal(out.outcome, QUEUE_RUNTIME_RESPONSE_CONTRACT.outerOutcomes.ok);
  assert.ok(out.result && typeof out.result === "object" && !Array.isArray(out.result));
  for (const field of QUEUE_RUNTIME_RESPONSE_CONTRACT.requiredResultFields) {
    assert.equal(Object.hasOwn(out.result, field), true, `queue result must include ${field}`);
  }
  assert.equal(out.result.outcome, expectedInnerOutcome);
  assert.equal(typeof out.result.ackAll, "boolean");
  assert.ok(Array.isArray(out.result.explicitAcks));
  assert.ok(Array.isArray(out.result.retryMessages));
  assert.ok(
    out.result.retryBatch &&
    typeof out.result.retryBatch === "object" &&
    !Array.isArray(out.result.retryBatch)
  );
  assert.equal(typeof out.result.retryBatch.retry, "boolean");
}

/** @param {string} workerId @param {string} queue @param {any[]} messages */
function postQueued(workerId, queue, messages) {
  const res = runtimeDispatchPost("/_queued", { "x-worker-id": workerId }, { queue, messages });
  assertStatus(res, 200, "/_queued dispatch");
  return responseJson(res);
}

test("/_queued: stub.queue() dispatches natively, ackAll() surfaces on response", async () => {
  const ns = uniqueNs("q");
  const version = await deployAndPromote(ns, "c", { code: CONSUMER });
  const workerId = gatewayWorkerId(ns, "c", version);

  const out = postQueued(workerId, "orders", [
    queueStreamMessage({ id: "m1", body: { hello: "world" }, contentType: "json" }),
    queueStreamMessage({ id: "m2", body: { hello: "again" }, contentType: "json" }),
  ]);

  assertNativeQueueResult(out);
  assert.equal(out.result.ackAll, true);
  assert.deepEqual(out.result.explicitAcks, []);
});

test("/_queued: per-message ack() and retry({delaySeconds}) round-trip via native response", async () => {
  const ns = uniqueNs("q");
  const version = await deployAndPromote(ns, "c", { code: CONSUMER });
  const workerId = gatewayWorkerId(ns, "c", version);

  const out = postQueued(workerId, "orders", [
    queueStreamMessage({ id: "ack-me",    body: "a", contentType: "text" }),
    queueStreamMessage({ id: "retry-me",  body: "b", contentType: "text", attempts: 1 }),
    queueStreamMessage({ id: "untouched", body: "c", contentType: "text" }),
  ]);

  assertNativeQueueResult(out);
  assert.equal(out.result.ackAll, false);
  assert.deepEqual(out.result.explicitAcks, ["ack-me"]);
  assert.equal(out.result.retryMessages.length, 1);
  assert.equal(out.result.retryMessages[0].msgId, "retry-me");
  assert.equal(out.result.retryMessages[0].delaySeconds, 5);
});

test("/_queued: body fidelity across json / text / bytes content types", async () => {
  const ns = uniqueNs("q");
  const version = await deployAndPromote(ns, "c", { code: CONSUMER });
  const workerId = gatewayWorkerId(ns, "c", version);

  const firstSeenMs = 1_800_000_000_000;
  postQueued(workerId, "q", [
    queueStreamMessage({ id: "j", body: { k: 1, nested: { a: "b" } }, contentType: "json", firstSeenMs }),
    queueStreamMessage({ id: "t", body: "plain-string", contentType: "text", firstSeenMs }),
    queueStreamMessage({ id: "b", body: new Uint8Array([1, 2, 3, 255]), contentType: "bytes", firstSeenMs }),
  ]);

  const recv = runtimeInternalPost("/", { "x-worker-id": workerId }, "");
  assertStatus(recv, 200, "queue receiver fetch");
  const parsed = responseJson(recv);
  assert.equal(parsed.queue, "q");

  const byId = Object.fromEntries(parsed.messages.map((/** @type {any} */ m) => [m.id, m]));

  assert.equal(byId.j.bodyKind, "object");
  assert.equal(byId.j.attempts, 1);
  assert.deepEqual(byId.j.bodyPreview, { k: 1, nested: { a: "b" } });

  assert.equal(byId.t.bodyKind, "string");
  assert.equal(byId.t.bodyPreview, "plain-string");

  // Uint8Array round-trips intact across the isolate boundary — the
  // central assumption behind the bytes contentType. If workerd ever
  // loses it, queue() handlers would silently start getting plain objects.
  assert.equal(byId.b.bodyKind, "bytes");
  assert.deepEqual(byId.b.bodyPreview, [1, 2, 3, 255]);

  // timestamp arrived as a Date, not a string or number.
  assert.equal(byId.j.timestampMs, firstSeenMs);
});

test("/_queued: handler throw → result.outcome='exception' (not retryBatch.retry)", async () => {
  const THROWER = `
export default {
  fetch() { return new Response("ok"); },
  async queue() { throw new Error("boom"); },
};
`;
  const ns = uniqueNs("q");
  const version = await deployAndPromote(ns, "c", { code: THROWER });
  const workerId = gatewayWorkerId(ns, "c", version);

  const out = postQueued(workerId, "q", [
    queueStreamMessage({ id: "m1", body: "x", contentType: "text" }),
  ]);

  assertNativeQueueResult(out, QUEUE_RUNTIME_RESPONSE_CONTRACT.innerOutcomes.exception);
  assert.equal(out.result.retryBatch.retry, false,
    "retryBatch.retry stays false — platform must check result.outcome for throw");
});

test("/_queued: rejects malformed requests with 400", async () => {
  const ns = uniqueNs("q");
  const version = await deployAndPromote(ns, "c", { code: CONSUMER });
  const workerId = gatewayWorkerId(ns, "c", version);

  const missingMessages = runtimeDispatchPost(
    "/_queued", { "x-worker-id": workerId }, { queue: "x" }
  );
  assertStatus(missingMessages, 400, "missing messages request");

  const missingId = runtimeDispatchPost("/_queued", {}, { queue: "x", messages: [] });
  assertStatus(missingId, 400, "missing worker id request");

  const badContentType = runtimeDispatchPost(
    "/_queued",
    { "x-worker-id": workerId },
    { queue: "x", messages: [{ id: "m", first_seen_ms: 0, attempts: 0, body_b64: "", content_type: "v8" }] }
  );
  assertStatus(badContentType, 400, "bad content type request");

  const badBase64 = runtimeDispatchPost(
    "/_queued",
    { "x-worker-id": workerId },
    {
      queue: "x",
      messages: [{
        id: "m",
        first_seen_ms: 0,
        attempts: 0,
        body_b64: "%%%",
        content_type: "bytes",
      }],
    }
  );
  assertStatus(badBase64, 400, "invalid base64 request");
  assert.equal(responseJson(badBase64).error, "queue_message_decode_failed");
});
