import { test } from "node:test";
import assert from "node:assert/strict";
import { S3_ERROR_BODY_MAX_BYTES, S3_REQUEST_TIMEOUT_MS } from "../../shared/s3-request.js";

import {
  importRepositoryModule,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const { makeS3Client, putAsset } = await importRepositoryModule("control/s3.js", [
  [/import \{ SigV4Client \} from "@wdl-dev\/aws-sigv4";/, "class SigV4Client { constructor(options) { this.options = options; } }"],
  [/from "runtime-r2-utils";/g, `from ${JSON.stringify(repositoryFileUrl("runtime/r2-utils.js"))};`],
  [/from "shared-s3-retry";/g, `from ${JSON.stringify(repositoryFileUrl("shared/s3-retry.js"))};`],
  [/from "shared-s3-request";/g, `from ${JSON.stringify(repositoryFileUrl("shared/s3-request.js"))};`],
  [/from "shared-bounded-body";/g, `from ${JSON.stringify(repositoryFileUrl("shared/bounded-body.js"))};`],
  [/from "shared-respond";/g, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
]);

test("makeS3Client requires credentials outside explicit local/mock endpoints", () => {
  assert.throws(
    () => makeS3Client({
      S3_ENDPOINT: "https://assets.example",
      S3_BUCKET: "assets",
    }),
    /S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required/
  );
});

test("ASSETS upload timeout never reports a pending PUT as successful", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  /** @type {AbortSignal | null | undefined} */
  let signal;
  let calls = 0;
  const s3 = {
    endpoint: "http://s3.test", bucket: "assets",
    client: { async fetch(/** @type {string} */ _url, /** @type {RequestInit} */ init) {
      calls += 1;
      signal = init.signal;
      return new Promise(() => {});
    } },
  };
  const rejected = assert.rejects(putAsset(s3, "asset", "value", "text/plain"), { name: "TimeoutError" });
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS);
  await rejected;
  assert.ok(signal);
  assert.equal(signal.aborted, true);
  assert.equal(calls, 1);
});

test("ASSETS successful PUT discards the unused response body", async () => {
  let cancelled = false;
  const s3 = {
    endpoint: "http://s3.test", bucket: "assets",
    client: { async fetch() {
      return new Response(new ReadableStream({ cancel() { cancelled = true; } }));
    } },
  };
  await putAsset(s3, "asset", "value", "text/plain");
  assert.equal(cancelled, true);
});

test("ASSETS PUT keeps the backend status when its error body is unreadable", async () => {
  const s3 = {
    endpoint: "http://s3.test", bucket: "assets",
    client: { async fetch() {
      return new Response(new ReadableStream({
        start(controller) { controller.error(new TypeError("body disconnected")); },
      }), { status: 503 });
    } },
  };
  await assert.rejects(putAsset(s3, "asset", "value", "text/plain"), /S3 PUT asset.*503/);
});

test("ASSETS PUT bounds error details and cancels a declared oversized body", async () => {
  for (const size of [S3_ERROR_BODY_MAX_BYTES, S3_ERROR_BODY_MAX_BYTES + 1]) {
    let cancelled = false;
    let reads = 0;
    const s3 = {
      endpoint: "http://s3.test", bucket: "assets",
      client: { async fetch() {
        return new Response(new ReadableStream({
          pull(controller) {
            reads += 1;
            controller.enqueue(new TextEncoder().encode("x".repeat(size)));
            controller.close();
          },
          cancel() { cancelled = true; },
        }, { highWaterMark: 0 }), { status: 503, headers: { "content-length": String(size) } });
      } },
    };
    const oversized = size > S3_ERROR_BODY_MAX_BYTES;
    await assert.rejects(putAsset(s3, "asset", "value", "text/plain"), (error) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /S3 PUT asset.*503 /);
      assert.equal(error.message.split("503 ")[1], oversized ? "" : "x".repeat(200));
      return true;
    });
    assert.equal(cancelled, oversized);
    assert.equal(reads, oversized ? 0 : 1);
  }
});

test("ASSETS error-body handling preserves the request deadline", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const started = Promise.withResolvers();
  let cancelled = false;
  const s3 = {
    endpoint: "http://s3.test", bucket: "assets",
    client: { async fetch() {
      return new Response(new ReadableStream({
        pull() { started.resolve(undefined); return new Promise(() => {}); },
        cancel() { cancelled = true; },
      }, { highWaterMark: 0 }), { status: 503 });
    } },
  };
  const rejected = assert.rejects(putAsset(s3, "asset", "value", "text/plain"), { name: "TimeoutError" });
  await started.promise;
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS);
  await rejected;
  assert.equal(cancelled, true);
});

test("makeS3Client permits test credentials only for local/mock endpoints", () => {
  const local = makeS3Client({
    S3_ENDPOINT: "http://s3mock:9090",
    S3_BUCKET: "assets",
  });
  assert.ok(local);
  assert.equal(local.client.options.retries, 10);
  assert.ok(makeS3Client({
    S3_ENDPOINT: "https://assets.example",
    S3_BUCKET: "assets",
    S3_ALLOW_TEST_CREDENTIALS: "1",
  }));
});
