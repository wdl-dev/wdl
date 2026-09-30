import assert from "node:assert/strict";
import { test } from "node:test";
import {
  S3_TRANSIENT_RETRIES,
  fetchRetryableS3Post,
  isTransientS3Response,
} from "../../shared/s3-retry.js";
import { withMockedProperty } from "../helpers/mock-global.js";

test("S3 transient retry policy classifies throttling and server errors", () => {
  assert.equal(S3_TRANSIENT_RETRIES, 10);
  assert.equal(isTransientS3Response(new Response(null, { status: 429 })), true);
  assert.equal(isTransientS3Response(new Response(null, { status: 503 })), true);
  assert.equal(isTransientS3Response(new Response(null, { status: 408 })), false);
  assert.equal(isTransientS3Response(new Response(null, { status: 400 })), false);
});

test("fetchRetryableS3Post retries transport and transient response failures", async () => {
  let calls = 0;
  const client = {
    async fetch() {
      calls += 1;
      if (calls === 1) throw new Error("transport down");
      if (calls === 2) return new Response("slow down", { status: 500 });
      return new Response("ok");
    },
  };

  await withMockedProperty(Math, "random", () => 0, async () => {
    const response = await fetchRetryableS3Post(client, "https://s3.test/bucket?delete", {
      method: "POST",
    });
    assert.equal(await response.text(), "ok");
  });
  assert.equal(calls, 3);
});

test("DeleteObjects cancellation interrupts backoff without another attempt", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const aborter = new AbortController();
  let calls = 0;
  let cancelled = false;
  const backoff = Promise.withResolvers();
  await withMockedProperty(Math, "random", () => {
    backoff.resolve(undefined);
    return 1;
  }, async () => {
    const request = fetchRetryableS3Post({
      async fetch() {
        calls += 1;
        return new Response(new ReadableStream({ cancel() { cancelled = true; } }), { status: 503 });
      },
    }, "https://s3.test/bucket?delete", { method: "POST", signal: aborter.signal });
    const rejected = assert.rejects(request, { name: "AbortError" });
    await backoff.promise;
    aborter.abort();
    await rejected;
    t.mock.timers.tick(60_000);
    assert.equal(calls, 1);
    assert.equal(cancelled, true);
  });
});

test("DeleteObjects does not retry an aborted transport failure", async () => {
  const aborter = new AbortController();
  let calls = 0;
  await assert.rejects(fetchRetryableS3Post({
    async fetch() {
      calls += 1;
      aborter.abort();
      throw new Error("transport failed after abort");
    },
  }, "https://s3.test/bucket?delete", { method: "POST", signal: aborter.signal }), { name: "AbortError" });
  assert.equal(calls, 1);
});
