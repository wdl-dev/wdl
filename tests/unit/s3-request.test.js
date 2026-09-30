import assert from "node:assert/strict";
import { test } from "node:test";
import {
  S3_REQUEST_TIMEOUT_MS,
  S3_STREAM_IDLE_TIMEOUT_MS,
  S3_STREAM_READ_TIMEOUT_MS,
  streamS3Response,
  withS3Request,
} from "../../shared/s3-request.js";
import { readBoundedText } from "../../shared/bounded-body.js";

function context() {
  /** @type {Promise<unknown>[]} */
  const tasks = [];
  return { tasks, waitUntil(/** @type {Promise<unknown>} */ task) { tasks.push(task); } };
}

test("S3 request deadline rejects pending transport and aborts its signal", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  let finished = false;
  /** @type {AbortSignal | undefined} */
  let signal;
  const request = withS3Request(async (aborter) => {
    signal = aborter.signal;
    return new Promise(() => {});
  }).finally(() => { finished = true; });
  const rejected = assert.rejects(request, { name: "TimeoutError" });
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS - 1);
  await Promise.resolve();
  assert.equal(finished, false);
  assert.ok(signal);
  assert.equal(signal.aborted, false);
  t.mock.timers.tick(1);
  await rejected;
  assert.equal(signal.aborted, true);
});

test("S3 request rejects synchronous result construction past the deadline", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  await assert.rejects(withS3Request(async () => {
    t.mock.timers.setTime(S3_REQUEST_TIMEOUT_MS + 1);
    return "late success";
  }), { name: "TimeoutError" });
});

test("S3 XML deadline cancels a stalled reader without waiting for cancellation", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const started = Promise.withResolvers();
  let cancelled = false;
  const response = new Response(new ReadableStream({
    pull() { started.resolve(undefined); return new Promise(() => {}); },
    cancel() { cancelled = true; return new Promise(() => {}); },
  }, { highWaterMark: 0 }));
  const rejected = assert.rejects(withS3Request((aborter) => readBoundedText(response, Infinity, aborter.signal)), {
    name: "TimeoutError",
  });
  await started.promise;
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS);
  await rejected;
  assert.equal(cancelled, true);
  assert.equal(response.body?.locked, false);
});

test("S3 request releases its timer on success and aborts siblings on failure", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  /** @type {AbortSignal[]} */
  const signals = [];
  const value = await withS3Request(async (aborter) => {
    signals.push(aborter.signal);
    return await readBoundedText(new Response("<ok/>"), Infinity, aborter.signal);
  });
  assert.equal(value, "<ok/>");
  const error = new Error("head failed");
  await assert.rejects(withS3Request(async (aborter) => {
    signals.push(aborter.signal);
    throw error;
  }), (caught) => caught === error);
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS * 2);
  assert.equal(signals[0].aborted, false);
  assert.equal(signals[1].reason, error);
});

test("S3 streaming EOF releases the background task and both timers", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const ctx = context();
  const aborter = new AbortController();
  const response = new Response("streamed");
  const body = streamS3Response(response, aborter, ctx);
  assert.equal(await new Response(body).text(), "streamed");
  await Promise.all(ctx.tasks);
  assert.equal(response.body?.locked, false);
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS * 2);
  assert.equal(aborter.signal.aborted, false);
});

test("S3 streaming cancellation releases immediately even if upstream cancel stalls", async () => {
  const ctx = context();
  const aborter = new AbortController();
  let cancelled = false;
  const response = new Response(new ReadableStream({
    cancel() { cancelled = true; return new Promise(() => {}); },
  }));
  const body = streamS3Response(response, aborter, ctx);
  await body.cancel("consumer stopped");
  await Promise.all(ctx.tasks);
  assert.equal(cancelled, true);
  assert.equal(aborter.signal.aborted, true);
  assert.equal(response.body?.locked, false);
});

test("S3 streaming read timeout rejects a pending read and cleans up", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const ctx = context();
  const aborter = new AbortController();
  const started = Promise.withResolvers();
  let cancelled = false;
  const response = new Response(new ReadableStream({
    pull() { started.resolve(undefined); return new Promise(() => {}); },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 }));
  const reader = streamS3Response(response, aborter, ctx).getReader();
  const rejected = assert.rejects(reader.read(), /S3 response body read timed out/);
  await started.promise;
  t.mock.timers.tick(S3_STREAM_READ_TIMEOUT_MS);
  await rejected;
  await Promise.all(ctx.tasks);
  assert.equal(cancelled, true);
  assert.equal(aborter.signal.aborted, true);
});

test("S3 streaming does not count downstream backpressure as a stalled read", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const ctx = context();
  const aborter = new AbortController();
  const body = streamS3Response(new Response("ok"), aborter, ctx);
  t.mock.timers.tick(S3_STREAM_READ_TIMEOUT_MS + 1);
  assert.equal(aborter.signal.aborted, false);
  const reader = body.getReader();
  assert.deepEqual((await reader.read()).value, new TextEncoder().encode("ok"));
  t.mock.timers.tick(S3_STREAM_READ_TIMEOUT_MS + 1);
  assert.equal(aborter.signal.aborted, false);
  assert.equal((await reader.read()).done, true);
  await Promise.all(ctx.tasks);
});

test("S3 streaming expires an abandoned body without waiting for a pull", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const ctx = context();
  const aborter = new AbortController();
  const body = streamS3Response(new Response("unread"), aborter, ctx);
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS);
  await Promise.all(ctx.tasks);
  await assert.rejects(body.getReader().read(), /S3 response body idle timeout/);
  assert.equal(aborter.signal.aborted, true);
});

test("S3 streaming progress keeps a slow download alive beyond the idle interval", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  const ctx = context();
  const aborter = new AbortController();
  let cancelled = 0;
  let chunks = 0;
  const response = new Response(new ReadableStream({
    pull(controller) { controller.enqueue(Uint8Array.of(++chunks)); },
    cancel() { cancelled += 1; },
  }, { highWaterMark: 0 }));
  const reader = streamS3Response(response, aborter, ctx).getReader();
  for (let i = 1; i <= 12; i += 1) {
    t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS - 1);
    assert.deepEqual((await reader.read()).value, Uint8Array.of(i));
    assert.equal(aborter.signal.aborted, false);
  }
  assert.ok(Date.now() > S3_STREAM_IDLE_TIMEOUT_MS * 10);
  await reader.cancel();
  await Promise.all(ctx.tasks);
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS * 2);
  assert.equal(cancelled, 1);
  assert.equal(response.body?.locked, false);
});

test("S3 streaming reclaims a partially consumed body after its last progress", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  const ctx = context();
  const aborter = new AbortController();
  const response = new Response("partially consumed");
  const reader = streamS3Response(response, aborter, ctx).getReader();
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS - 1);
  assert.equal((await reader.read()).done, false);
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS - 1);
  assert.equal(aborter.signal.aborted, false);
  t.mock.timers.tick(1);
  await Promise.all(ctx.tasks);
  await assert.rejects(reader.read(), /S3 response body idle timeout/);
  assert.equal(response.body?.locked, false);
});

test("S3 streaming empty chunks do not extend the idle deadline", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  const ctx = context();
  const aborter = new AbortController();
  const response = new Response(new ReadableStream({
    pull(controller) { controller.enqueue(new Uint8Array()); },
  }, { highWaterMark: 0 }));
  const reader = streamS3Response(response, aborter, ctx).getReader();
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS / 2);
  assert.equal((await reader.read()).value?.byteLength, 0);
  t.mock.timers.tick(S3_STREAM_IDLE_TIMEOUT_MS / 2);
  await Promise.all(ctx.tasks);
  await assert.rejects(reader.read(), /S3 response body idle timeout/);
});

test("S3 streaming cannot revive an expired idle body before its timer runs", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  const ctx = context();
  const aborter = new AbortController();
  const body = streamS3Response(new Response("expired"), aborter, ctx);
  t.mock.timers.setTime(S3_STREAM_IDLE_TIMEOUT_MS + 1);
  await assert.rejects(body.getReader().read(), /S3 response body idle timeout/);
  await Promise.all(ctx.tasks);
  assert.equal(aborter.signal.aborted, true);
});

test("S3 streaming releases its source if background registration fails", () => {
  const aborter = new AbortController();
  const response = new Response("unread");
  const error = new Error("context closed");
  assert.throws(() => streamS3Response(response, aborter, { waitUntil() { throw error; } }),
    (caught) => caught === error);
  assert.equal(aborter.signal.reason, error);
  assert.equal(response.body?.locked, false);
});
