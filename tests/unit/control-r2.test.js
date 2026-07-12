import assert from "node:assert/strict";
import { test } from "node:test";

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
