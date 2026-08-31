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

test("readBoundedStreamBytes fills an exact declared-length buffer across chunks", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2]));
      controller.enqueue(new Uint8Array([3, 4]));
      controller.close();
    },
  });

  const bytes = await readBoundedStreamBytes(stream, 4, undefined, undefined, 4);

  assert.deepEqual([...bytes], [1, 2, 3, 4]);
  assert.equal(bytes.buffer.byteLength, 4);
});

test("readBoundedStreamBytes rejects declared-length mismatches", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]));
      controller.close();
    },
  });

  await assert.rejects(
    () => readBoundedStreamBytes(stream, 4, undefined, undefined, 2),
    /exceeds Content-Length/
  );
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

test("readBoundedStreamBytes cancels a pending reader when its signal aborts", async () => {
  /** @type {() => void} */
  let started = () => {};
  const pullStarted = new Promise((resolve) => { started = () => resolve(undefined); });
  let cancelReason;
  const stream = new ReadableStream({
    pull() {
      started();
      return new Promise(() => {});
    },
    cancel(reason) {
      cancelReason = reason;
    },
  });
  const controller = new AbortController();
  const reason = new DOMException("body deadline", "AbortError");
  const reading = readBoundedStreamBytes(stream, 16, undefined, controller.signal);

  await pullStarted;
  controller.abort(reason);

  await assert.rejects(reading, (err) => err === reason);
  assert.equal(cancelReason, reason);
});

test("readBoundedStreamBytes rejects even when read and cancel never settle", async () => {
  const reading = Promise.withResolvers();
  let cancelReason;
  let released = false;
  const stream = /** @type {ReadableStream<Uint8Array>} */ (/** @type {unknown} */ ({
    getReader() {
      return {
        read() { return reading.promise; },
        /** @param {unknown} reason */
        cancel(reason) {
          cancelReason = reason;
          return new Promise(() => {});
        },
        releaseLock() { released = true; },
      };
    },
  }));
  const controller = new AbortController();
  const reason = new DOMException("body deadline", "AbortError");
  const result = readBoundedStreamBytes(stream, 16, undefined, controller.signal);

  controller.abort(reason);

  await assert.rejects(result, (error) => error === reason);
  assert.equal(cancelReason, reason);
  assert.equal(released, true);
});

test("readBoundedStreamBytes cancels a stream when its signal is already aborted", async () => {
  let cancelReason;
  let released = false;
  const stream = /** @type {ReadableStream<Uint8Array>} */ (/** @type {unknown} */ ({
    getReader() {
      return {
        read() { return new Promise(() => {}); },
        /** @param {unknown} reason */
        cancel(reason) { cancelReason = reason; },
        releaseLock() { released = true; },
      };
    },
  }));
  const controller = new AbortController();
  const reason = new DOMException("body deadline", "AbortError");
  controller.abort(reason);

  await assert.rejects(
    readBoundedStreamBytes(stream, 16, undefined, controller.signal),
    (error) => error === reason
  );
  assert.equal(cancelReason, reason);
  assert.equal(released, true);
});

test("readBoundedStreamBytes keeps abort listeners bounded across reads", async () => {
  const chunkCount = 1024;
  const stream = new ReadableStream({
    start(controller) {
      for (let index = 0; index < chunkCount; index += 1) {
        controller.enqueue(Uint8Array.of(index & 0xff));
      }
      controller.close();
    },
  });
  const listeners = new Set();
  let added = 0;
  let removed = 0;
  let maxActive = 0;
  const signal = /** @type {AbortSignal} */ (/** @type {unknown} */ ({
    aborted: false,
    reason: undefined,
    throwIfAborted() {},
    /** @param {string} type @param {EventListener} listener */
    addEventListener(type, listener) {
      assert.equal(type, "abort");
      listeners.add(listener);
      added += 1;
      maxActive = Math.max(maxActive, listeners.size);
    },
    /** @param {string} type @param {EventListener} listener */
    removeEventListener(type, listener) {
      assert.equal(type, "abort");
      if (listeners.delete(listener)) removed += 1;
    },
  }));

  const bytes = await readBoundedStreamBytes(stream, chunkCount, undefined, signal);

  assert.equal(bytes.byteLength, chunkCount);
  assert.equal(added, 1);
  assert.equal(removed, added);
  assert.equal(maxActive, 1);
  assert.equal(listeners.size, 0);
});
