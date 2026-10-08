import assert from "node:assert/strict";
import { test } from "node:test";
import { S3_ERROR_BODY_MAX_BYTES, S3_REQUEST_TIMEOUT_MS } from "../../shared/s3-request.js";

import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const AWS_SIGV4_STUB_URL = moduleDataUrl(`
export class SigV4Client {
  constructor(config) { this.config = config; }
}
`);

const {
  deleteR2Object,
  getR2Object,
  headR2Object,
  listR2Buckets,
  listR2Objects,
  makeR2AdminClient,
} = await importRepositoryModule("control/r2.js", importSpecifierReplacements({
  "@wdl-dev/aws-sigv4": AWS_SIGV4_STUB_URL,
  "runtime-r2-utils": repositoryFileUrl("runtime/r2-utils.js"),
  "shared-s3-xml": repositoryFileUrl("shared/s3-xml.js"),
  "shared-s3-retry": repositoryFileUrl("shared/s3-retry.js"),
  "shared-s3-request": repositoryFileUrl("shared/s3-request.js"),
  "shared-bounded-body": repositoryFileUrl("shared/bounded-body.js"),
  "shared-respond": repositoryFileUrl("shared/respond.js"),
}));

/** @param {Response} response */
function r2AdminMock(response) {
  /** @type {Array<{ url: string, init?: RequestInit }>} */
  const calls = [];
  return {
    calls,
    r2: {
      endpoint: "http://s3.test",
      bucket: "wdl-r2",
      client: {
        /** @param {RequestInfo | URL} url @param {RequestInit} [init] */
        async fetch(url, init) {
          calls.push({ url: String(url), init });
          return response;
        },
      },
    },
  };
}

test("control R2 object list accepts namespaced S3 list XML", async () => {
  const { r2, calls } = r2AdminMock(new Response([
    "<aws:ListBucketResult>",
    "<aws:IsTruncated>true</aws:IsTruncated>",
    "<aws:NextContinuationToken>cursor-1</aws:NextContinuationToken>",
    "<aws:CommonPrefixes><aws:Prefix>r2/demo/uploads/folder/</aws:Prefix></aws:CommonPrefixes>",
    "<aws:Contents>",
    "<aws:Key>r2/demo/uploads/a&amp;b.txt</aws:Key>",
    "<aws:LastModified>2026-04-26T00:00:00.000Z</aws:LastModified>",
    "<aws:ETag>&quot;etag-1&quot;</aws:ETag>",
    "<aws:Size>7</aws:Size>",
    "<aws:StorageClass>STANDARD</aws:StorageClass>",
    "</aws:Contents>",
    "</aws:ListBucketResult>",
  ].join("")));

  const result = await listR2Objects({ r2, ns: "demo", bucketName: "uploads", prefix: "folder name" });

  assert.match(calls[0].url, /prefix=r2%2Fdemo%2Fuploads%2Ffolder%20name(?:&|$)/);
  assert.equal(new URL(calls[0].url).searchParams.get("prefix"), "r2/demo/uploads/folder name");
  assert.equal(result.truncated, true);
  assert.equal(result.cursor, "cursor-1");
  assert.deepEqual(result.delimitedPrefixes, ["folder/"]);
  assert.deepEqual(result.objects, [{
    key: "a&b.txt",
    size: 7,
    etag: "etag-1",
    uploaded: "2026-04-26T00:00:00.000Z",
    version: "",
    storageClass: "STANDARD",
  }]);
});

test("control R2 bucket list accepts namespaced S3 list XML", async () => {
  const { r2 } = r2AdminMock(new Response([
    "<aws:ListBucketResult>",
    "<aws:IsTruncated>true</aws:IsTruncated>",
    "<aws:NextContinuationToken>bucket-cursor</aws:NextContinuationToken>",
    "<aws:CommonPrefixes><aws:Prefix>r2/demo/assets/</aws:Prefix></aws:CommonPrefixes>",
    "</aws:ListBucketResult>",
  ].join("")));

  const result = await listR2Buckets({ r2, ns: "demo" });

  assert.equal(result.truncated, true);
  assert.equal(result.cursor, "bucket-cursor");
  assert.deepEqual(result.buckets, [{ name: "assets" }]);
});

test("control R2 object list rejects non-canonical prefixes", async () => {
  const { r2 } = r2AdminMock(new Response("<ListBucketResult />"));

  await assert.rejects(
    () => listR2Objects({ r2, ns: "demo", bucketName: "uploads", prefix: "../secret" }),
    /path segments/
  );
});

test("control R2 admin client restores S3 transient retry budget", () => {
  const r2 = makeR2AdminClient({
    R2_S3_ENDPOINT: "http://s3.test",
    R2_S3_BUCKET: "wdl-r2",
    R2_S3_ACCESS_KEY_ID: "test",
    R2_S3_SECRET_ACCESS_KEY: "test",
  });

  assert.ok(r2);
  assert.equal(r2.client.config.retries, 10);
});

test("control R2 object methods share request, error, and not-found handling", async () => {
  const getMock = r2AdminMock(new Response("missing", { status: 404 }));
  assert.equal(await getR2Object({
    r2: getMock.r2,
    ns: "demo",
    bucketName: "uploads",
    key: "a.txt",
    requestId: "rid-get",
  }), null);
  assert.equal(getMock.calls[0].init?.method, "GET");
  assert.equal(new Headers(getMock.calls[0].init?.headers).get("x-request-id"), "rid-get");

  const headMock = r2AdminMock(new Response("backend detail", { status: 503 }));
  await assert.rejects(
    () => headR2Object({
      r2: headMock.r2,
      ns: "demo",
      bucketName: "uploads",
      key: "a.txt",
    }),
    /R2 admin HEAD failed with 503: backend detail/
  );

  const deleteMock = r2AdminMock(new Response("missing", { status: 404 }));
  assert.deepEqual(await deleteR2Object({
    r2: deleteMock.r2,
    ns: "demo",
    bucketName: "uploads",
    key: "a.txt",
  }), {
    namespace: "demo",
    bucket: "uploads",
    key: "a.txt",
    status: "ok",
  });
  assert.equal(deleteMock.calls[0].init?.method, "DELETE");
});

test("control R2 list cancels a stalled XML response at the request deadline", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"] });
  const started = Promise.withResolvers();
  let cancelled = false;
  const { r2, calls } = r2AdminMock(new Response(new ReadableStream({
    pull() { started.resolve(undefined); return new Promise(() => {}); },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 })));
  const rejected = assert.rejects(listR2Objects({ r2, ns: "demo", bucketName: "uploads" }), { name: "TimeoutError" });
  await started.promise;
  t.mock.timers.tick(S3_REQUEST_TIMEOUT_MS);
  await rejected;
  assert.equal(cancelled, true);
  assert.equal(calls[0].init?.signal?.aborted, true);
});

test("control R2 GET transfers body lifetime to the request context", async () => {
  const { r2, calls } = r2AdminMock(new Response("value"));
  /** @type {Promise<unknown>[]} */
  const tasks = [];
  const ctx = { waitUntil(/** @type {Promise<unknown>} */ task) { tasks.push(task); } };
  const response = await getR2Object({ r2, ns: "demo", bucketName: "uploads", key: "key", ctx });
  assert.equal(tasks.length, 1);
  assert.equal(await response.text(), "value");
  await Promise.all(tasks);
  assert.equal(calls[0].init?.signal?.aborted, false);
});

test("control R2 keeps the backend status when its error body is unreadable", async () => {
  const { r2 } = r2AdminMock(new Response(new ReadableStream({
    start(controller) { controller.error(new TypeError("body disconnected")); },
  }), { status: 503 }));
  await assert.rejects(getR2Object({ r2, ns: "demo", bucketName: "uploads", key: "key" }), {
    name: "Error", message: "R2 admin GET failed with 503: ",
  });
});

test("control R2 omits and cancels oversized error bodies without replacing status", async () => {
  for (const operation of [getR2Object, headR2Object, deleteR2Object, listR2Objects]) {
    for (const declared of [true, false]) {
      let reads = 0;
      let cancelled = false;
      const { r2 } = r2AdminMock(new Response(new ReadableStream({
        pull(controller) {
          reads += 1;
          if (reads > 2) controller.close();
          else controller.enqueue(new Uint8Array(reads === 1 ? S3_ERROR_BODY_MAX_BYTES : 1));
        },
        cancel() { cancelled = true; },
      }, { highWaterMark: 0 }), {
        status: 503,
        headers: declared ? { "content-length": String(S3_ERROR_BODY_MAX_BYTES + 1) } : {},
      }));
      await assert.rejects(operation({ r2, ns: "demo", bucketName: "uploads", key: "key" }),
        /R2 admin \w+ failed with 503: $/);
      assert.equal(cancelled, true);
      assert.equal(reads, declared ? 0 : 2);
    }
  }
});

test("control R2 accepts an exact-limit error body but includes only a short detail", async () => {
  const { r2 } = r2AdminMock(new Response("x".repeat(S3_ERROR_BODY_MAX_BYTES), { status: 400 }));
  await assert.rejects(getR2Object({ r2, ns: "demo", bucketName: "uploads", key: "key" }), {
    message: `R2 admin GET failed with 400: ${"x".repeat(200)}`,
  });
});

test("control R2 does not apply the diagnostic cap to successful list XML", async () => {
  const { r2 } = r2AdminMock(new Response(
    `<ListBucketResult>${" ".repeat(S3_ERROR_BODY_MAX_BYTES)}<Contents><Key>r2/demo/uploads/key</Key><Size>1</Size></Contents></ListBucketResult>`
  ));
  const result = await listR2Objects({ r2, ns: "demo", bucketName: "uploads" });
  assert.equal(result.objects.length, 1);
  assert.equal(result.objects[0].key, "key");
});
