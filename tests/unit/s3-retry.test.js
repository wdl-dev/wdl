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
