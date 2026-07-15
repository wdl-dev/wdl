import assert from "node:assert/strict";
import { test } from "node:test";

import { SigV4Client as InstalledSigV4Client } from "@wdl-dev/aws-sigv4";
import { SigV4Client } from "../../shared/vendor/aws-sigv4.js";

const ACCESS_KEY_ID = "AKIDEXAMPLE";
const SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SESSION_TOKEN = "session-token-example";
const FIXED_DATETIME = "20260616T010203Z";
const S3_ENDPOINT = "https://s3.us-east-1.amazonaws.com";

/**
 * @typedef {{
 *   fetch(input: Request | string | URL, init?: import("@wdl-dev/aws-sigv4").SigV4RequestInit): Promise<Response>,
 * }} TestSigV4Client
 * @typedef {(options: import("@wdl-dev/aws-sigv4").SigV4ClientOptions) => TestSigV4Client} TestSigV4ClientFactory
 */

/** @type {TestSigV4ClientFactory} */
const createInstalledClient = (options) => new InstalledSigV4Client(options);
/** @type {TestSigV4ClientFactory} */
const createVendoredClient = (options) => new SigV4Client(options);
const SIGNER_IMPLEMENTATIONS = [
  { name: "installed package", createClient: createInstalledClient },
  { name: "vendored bundle", createClient: createVendoredClient },
];

// Fixed vectors are kept inline so vendored signer changes cannot silently
// redefine expected output.
const FIXTURES = [
  {
    name: "put object signs S3 path, query, metadata, and unsigned payload",
    url: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/a%26b.txt?partNumber=1&uploadId=upload-id`,
    init: {
      method: "PUT",
      headers: {
        "content-type": "text/plain",
        "x-amz-meta-color": "blue",
      },
      body: "hello",
    },
    expectedAuthorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260616/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-meta-color, Signature=1c6bb19462b7ea6eeb31b1e069fcba0cdb36d6c95efa6e39744fd102a2630f50",
    expectedContentSha256: "UNSIGNED-PAYLOAD",
    expectedUrl: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/a%26b.txt?partNumber=1&uploadId=upload-id`,
  },
  {
    name: "delete objects signs explicit payload hash and checksum header",
    url: `${S3_ENDPOINT}/wdl-r2?delete`,
    init: {
      method: "POST",
      headers: {
        "content-type": "application/xml",
        "x-amz-checksum-sha256": "checksum-base64",
        "x-amz-content-sha256": "e2000f6b1fc1db795626ddaf9c13324157e9f56cb7820b40d7c3253a08ee5b91",
      },
      body: "<Delete/>",
    },
    expectedAuthorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260616/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-checksum-sha256;x-amz-content-sha256;x-amz-date, Signature=93a1cf22c38a19def0aeb042bc4fbcf9e486240f71e91dbbb0dd75924e4f73a5",
    expectedContentSha256: "e2000f6b1fc1db795626ddaf9c13324157e9f56cb7820b40d7c3253a08ee5b91",
    expectedUrl: `${S3_ENDPOINT}/wdl-r2?delete`,
  },
  {
    name: "session token participates in signed headers",
    url: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/session.txt`,
    sessionToken: SESSION_TOKEN,
    init: {
      method: "HEAD",
    },
    expectedAuthorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260616/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=f54699d16ac946691025f331377c59814b85e1078bc70ea66925457e2549d0a7",
    expectedContentSha256: "UNSIGNED-PAYLOAD",
    expectedSecurityToken: SESSION_TOKEN,
    expectedUrl: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/session.txt`,
  },
  {
    name: "range header participates in signed headers",
    url: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/range.txt`,
    init: {
      method: "GET",
      headers: {
        range: "bytes=5-14",
      },
    },
    expectedAuthorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260616/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=4314b16522bc84770fe27958aeb288cd651cd367744a4c3f842ff0aa3172e43d",
    expectedContentSha256: "UNSIGNED-PAYLOAD",
    expectedUrl: `${S3_ENDPOINT}/wdl-r2/r2/demo/uploads/range.txt`,
  },
];

test("aws-sigv4 fetch emits fixed S3 SigV4 golden vectors", async () => {
  for (const implementation of SIGNER_IMPLEMENTATIONS) {
    for (const fixture of FIXTURES) {
      const label = `${implementation.name}: ${fixture.name}`;
      /** @type {Request[]} */
      const requests = [];
      const client = implementation.createClient({
        accessKeyId: ACCESS_KEY_ID,
        secretAccessKey: SECRET_ACCESS_KEY,
        sessionToken: fixture.sessionToken,
        service: "s3",
        region: "us-east-1",
        cache: new Map(),
        retries: 0,
        fetch: async (input) => {
          requests.push(/** @type {Request} */ (input));
          return new Response(null, { status: 200 });
        },
      });
      const response = await client.fetch(fixture.url, /** @type {import("@wdl-dev/aws-sigv4").SigV4RequestInit} */ ({
        ...fixture.init,
        signing: { signingDate: FIXED_DATETIME },
      }));
      assert.equal(response.status, 200, label);
      assert.equal(requests.length, 1, label);
      const signed = requests[0];
      assert.equal(signed.url, fixture.expectedUrl, label);
      assert.equal(signed.method, fixture.init.method ?? "GET", label);
      assert.equal(await signed.clone().text(), fixture.init.body ?? "", label);
      assert.equal(signed.headers.get("authorization"), fixture.expectedAuthorization, label);
      assert.equal(signed.headers.get("x-amz-date"), FIXED_DATETIME, label);
      assert.equal(signed.headers.get("x-amz-content-sha256"), fixture.expectedContentSha256, label);
      assert.equal(signed.headers.get("x-amz-security-token"), fixture.expectedSecurityToken ?? null, label);
    }
  }
});

/** @param {typeof globalThis.fetch} fetch @param {TestSigV4ClientFactory} [createClient] */
function testClient(fetch, createClient = createVendoredClient) {
  return createClient({
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    service: "s3",
    region: "us-east-1",
    cache: new Map(),
    retries: 3,
    initialRetryDelayMs: 0,
    maxRetryDelayMs: 0,
    fetch,
  });
}

test("aws-sigv4 fetch retries idempotent transient responses", async () => {
  for (const { method, body } of [
    { method: "GET", body: undefined },
    { method: "HEAD", body: undefined },
    { method: "PUT", body: "hello" },
  ]) {
    /** @type {Array<{ url: string, method: string, authorization: string | null, body: string }>} */
    const calls = [];
    const client = testClient(async (input) => {
      const request = /** @type {Request} */ (input);
      calls.push({
        url: request.url,
        method: request.method,
        authorization: request.headers.get("authorization"),
        body: await request.text(),
      });
      return new Response(null, { status: calls.length === 1 ? 500 : 200 });
    });

    const response = await client.fetch(`${S3_ENDPOINT}/wdl-r2/retry.txt`, {
      method,
      body,
      signing: { signingDate: FIXED_DATETIME },
    });

    assert.equal(response.status, 200, method);
    assert.equal(calls.length, 2, method);
    assert.equal(calls[0].url, calls[1].url, method);
    assert.equal(calls[0].method, method, method);
    assert.equal(calls[1].method, method, method);
    assert.equal(calls[0].body, calls[1].body, method);
    assert.equal(calls[0].body, body ?? "", method);
    assert.ok(calls[0].authorization?.startsWith("AWS4-HMAC-SHA256 "), method);
    assert.equal(calls[0].authorization, calls[1].authorization, method);
  }
});

test("aws-sigv4 retry does not wait for response cancellation", { timeout: 2_000 }, async () => {
  for (const implementation of SIGNER_IMPLEMENTATIONS) {
    let calls = 0;
    let cancellations = 0;
    const client = testClient(async () => {
      calls += 1;
      if (calls === 2) return new Response("ok");
      return new Response(new ReadableStream({
        cancel() {
          cancellations += 1;
          return new Promise(() => {});
        },
      }), { status: 500 });
    }, implementation.createClient);

    const response = await client.fetch(`${S3_ENDPOINT}/wdl-r2/retry-cancel.txt`, {
      signing: { signingDate: FIXED_DATETIME },
    });

    assert.equal(await response.text(), "ok", implementation.name);
    assert.equal(calls, 2, implementation.name);
    assert.equal(cancellations, 1, implementation.name);
  }
});

test("aws-sigv4 fetch does not retry POST transient responses", async () => {
  /** @type {Request[]} */
  const calls = [];
  const client = testClient(async (input) => {
    const request = /** @type {Request} */ (input);
    calls.push(request);
    return new Response(null, { status: 500 });
  });

  const response = await client.fetch(`${S3_ENDPOINT}/wdl-r2?delete`, {
    method: "POST",
    body: "<Delete/>",
    signing: { signingDate: FIXED_DATETIME },
  });

  assert.equal(response.status, 500);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
});

test("aws-sigv4 fetch with retries disabled makes one attempt", async () => {
  /** @type {Request[]} */
  const calls = [];
  const client = new SigV4Client({
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    service: "s3",
    region: "us-east-1",
    cache: new Map(),
    retries: 0,
    fetch: async (/** @type {RequestInfo | URL} */ input) => {
      const request = /** @type {Request} */ (input);
      calls.push(request);
      return new Response(null, { status: 500 });
    },
  });

  const response = await client.fetch(`${S3_ENDPOINT}/wdl-r2/retry-disabled.txt`, {
    method: "GET",
    signing: { signingDate: FIXED_DATETIME },
  });

  assert.equal(response.status, 500);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
});
