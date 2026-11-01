import assert from "node:assert/strict";
import { S3_REQUEST_TIMEOUT_MS, S3_STREAM_READ_TIMEOUT_MS } from "shared-s3-request";
import { R2Bucket } from "r2-client";
import { R2_OBJECT_MAX_BUFFER_BYTES } from "./_wdl-r2-utils.js";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export const blobUpload = {
  async test(_controller, env) {
    const bucket = new R2Bucket(env.BUCKET);
    const blob = new Blob(["immutable upload"], { type: "text/html" });
    const result = await bucket.put("blob-retry", blob, { httpMetadata: { contentType: "text/plain" } });
    assert.equal(result.size, blob.size);
    const uploaded = await (await env.BACKEND.fetch("http://s3.test/uploaded?key=blob-retry")).json();
    assert.deepEqual(uploaded, {
      attempts: 2, text: "immutable upload", size: blob.size,
      contentType: "text/plain", payloadHash: "UNSIGNED-PAYLOAD",
    });
    const exact = new Blob([new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES)]);
    assert.equal((await bucket.put("blob-exact", exact)).size, exact.size);
    const exactUploaded = await (await env.BACKEND.fetch("http://s3.test/uploaded?key=blob-exact")).json();
    assert.equal(exactUploaded.size, R2_OBJECT_MAX_BUFFER_BYTES);
    await assert.rejects(bucket.put("blob-oversized", new Blob([exact, "x"])), /exceeds the 25 MiB/);
    await assert.rejects(env.BUCKET.put("blob-host-oversized", new Blob([exact, "x"])), /exceeds the 25 MiB/);

    // Native RPC trusts expectedLength. The tenant facade must instead read the
    // entire arbitrary stream and validate it before submitting any bytes.
    for (const expectedLength of [0, 3, 100]) {
      const stream = new ReadableStream({
        expectedLength,
        start(controller) {
          controller.enqueue(new TextEncoder().encode("hello"));
          controller.close();
        },
      });
      const key = `stream-${expectedLength}`;
      assert.equal((await bucket.put(key, stream)).size, 5);
      assert.equal((await (await env.BACKEND.fetch(`http://s3.test/uploaded?key=${key}`)).json()).text, "hello");
    }
    await assert.rejects(bucket.put("stream-oversized", new ReadableStream({
      expectedLength: 1,
      start(controller) {
        controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES + 1));
        controller.close();
      },
    })), /exceeds the 25 MiB/);
    const stats = await (await env.BACKEND.fetch("http://s3.test/stats")).json();
    for (const key of ["blob-oversized", "blob-host-oversized", "stream-oversized"]) {
      assert.equal(stats[key], undefined);
    }
  },
};

export default {
  async test(_controller, env) {
    await Promise.all([
      (async () => {
        const started = Date.now();
        await assert.rejects(env.BUCKET.get("header-stall"), /S3 operation deadline exceeded/);
        assert.ok(Date.now() - started >= S3_REQUEST_TIMEOUT_MS * 0.9);
      })(),
      ...[
        ["put-stall", new Uint8Array([1, 2])],
        ["blob-stall", new Blob(["hello"])],
      ].map(async ([key, body]) => {
        const started = Date.now();
        await assert.rejects(env.BUCKET.put(key, body), /S3 operation deadline exceeded/);
        assert.ok(Date.now() - started >= S3_REQUEST_TIMEOUT_MS * 0.9);
        const stats = await (await env.BACKEND.fetch("http://s3.test/stats")).json();
        assert.equal(stats[key], 1);
      }),
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
