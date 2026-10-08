import assert from "node:assert/strict";
import { S3_REQUEST_TIMEOUT_MS, S3_STREAM_READ_TIMEOUT_MS } from "shared-s3-request";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default {
  async test(_controller, env) {
    await Promise.all([
      (async () => {
        const started = Date.now();
        await assert.rejects(env.BUCKET.get("header-stall"), /S3 operation deadline exceeded/);
        assert.ok(Date.now() - started >= S3_REQUEST_TIMEOUT_MS * 0.9);
      })(),
      (async () => {
        const started = Date.now();
        await assert.rejects(env.BUCKET.put("put-stall", new Uint8Array([1, 2])), /S3 operation deadline exceeded/);
        assert.ok(Date.now() - started >= S3_REQUEST_TIMEOUT_MS * 0.9);
        const stats = await (await env.BACKEND.fetch("http://s3.test/stats")).json();
        assert.equal(stats["put-stall"], 1);
      })(),
      (async () => {
        const result = await env.BUCKET.get("body-stall");
        const started = Date.now();
        await assert.rejects(new Response(result.body).text(), /ReadableStream received over RPC disconnected prematurely/);
        const elapsed = Date.now() - started;
        assert.ok(elapsed >= S3_STREAM_READ_TIMEOUT_MS * 0.9);
        assert.ok(elapsed < S3_STREAM_READ_TIMEOUT_MS * 2);
      })(),
      (async () => {
        const result = await env.BUCKET.get("slow-consumer");
        await delay(S3_STREAM_READ_TIMEOUT_MS + 5_000);
        const bytes = new Uint8Array(await new Response(result.body).arrayBuffer());
        assert.equal(bytes.byteLength, 2 * 1024 * 1024);
        assert.ok(bytes.every((byte) => byte === 7));
        const cancelled = await env.BUCKET.get("cancelled");
        await cancelled.body.cancel();
        const next = await env.BUCKET.get("after-cancel");
        assert.equal(await new Response(next.body).text(), "ok");
      })(),
    ]);
  },
};
