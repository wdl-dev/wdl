import { test } from "node:test";
import assert from "node:assert/strict";
import {
  BodyTooLargeError,
  readBoundedBytes,
  readBoundedStreamBytes,
  readBoundedText,
} from "../../shared/bounded-body.js";
import { settlementWithin } from "../helpers/timing.js";

test("readBoundedText rejects oversized declared content length", async () => {
  const request = new Request("https://demo.workers.example", {
    method: "POST",
    headers: { "content-length": "5" },
    body: "abcde",
  });

  await assert.rejects(
    () => readBoundedText(request, 4),
    (err) => err instanceof BodyTooLargeError && err.maxBytes === 4
  );
});

test("readBoundedBytes rejects streamed body after crossing the byte cap", async () => {
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2]));
      controller.enqueue(new Uint8Array([3, 4]));
      controller.close();
    },
  });
  const request = new Request(
    "https://demo.workers.example",
    /** @type {RequestInit} */ (/** @type {unknown} */ ({ method: "POST", body, duplex: "half" }))
  );

  await assert.rejects(
    () => readBoundedBytes(request, 3),
    (err) => err instanceof BodyTooLargeError && err.maxBytes === 3
  );
});

test("readBoundedBytes returns an empty body when no request body exists", async () => {
  const request = new Request("https://demo.workers.example");
  const bytes = await readBoundedBytes(request, 1);
  assert.equal(bytes.byteLength, 0);
});

test("readBoundedStreamBytes copies a view backed by a larger buffer", async () => {
  const backing = new Uint8Array([0, 1, 2, 3]);
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(backing.subarray(1, 3));
      controller.close();
    },
  });

  const bytes = await readBoundedStreamBytes(stream, 2);

  assert.deepEqual([...bytes], [1, 2]);
  assert.equal(bytes.buffer.byteLength, 2);
});

test("readBoundedStreamBytes supports caller-owned overflow errors", async () => {
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]));
    },
    cancel() {
      cancelled = true;
    },
  });

  await assert.rejects(
    () => readBoundedStreamBytes(stream, 2, () => new TypeError("custom limit")),
    /custom limit/
  );
  assert.equal(cancelled, true);
});

test("readBoundedStreamBytes rejects without waiting for stream cancellation", async () => {
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]));
    },
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  });
  const outcome = await settlementWithin(
    readBoundedStreamBytes(stream, 2, () => new TypeError("custom limit")),
    1000
  );

  assert.equal(outcome.status, "rejected");
  assert.match(String("reason" in outcome ? outcome.reason : ""), /custom limit/);
  assert.equal(cancelled, true);
});
