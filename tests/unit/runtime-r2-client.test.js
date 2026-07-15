import { test } from "node:test";
import assert from "node:assert/strict";
import { R2Bucket, R2Object, R2ObjectBody } from "../../runtime/r2-client.js";
import { R2_OBJECT_MAX_BUFFER_BYTES } from "../../runtime/r2-utils.js";

/**
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ status: "fulfilled", value: unknown } | { status: "rejected", reason: unknown } | { status: "pending" }>}
 */
async function settlementWithinTestWindow(promise) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise.then(
        (value) => ({ status: /** @type {const} */ ("fulfilled"), value }),
        (reason) => ({ status: /** @type {const} */ ("rejected"), reason }),
      ),
      new Promise((resolve) => {
        timeout = setTimeout(() => resolve({ status: /** @type {const} */ ("pending") }), 100);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

test("R2Bucket.list validates limit before host binding call", async () => {
  const bucket = new R2Bucket({
    async list() {
      throw new Error("host list should not be called");
    },
  });

  for (const limit of [0, 1001, 1.5, "abc", true]) {
    await assert.rejects(
      () => bucket.list({ limit }),
      /R2 list: limit must be an integer in \[1, 1000\]/,
      `expected limit ${JSON.stringify(limit)} to fail before host call`
    );
  }
});

test("R2Bucket.list normalizes valid limit for host binding", async () => {
  /** @type {any[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async list(/** @type {any} */ options, /** @type {any} */ requestMeta) {
      calls.push({ options, requestMeta });
      return { objects: [], delimitedPrefixes: [], truncated: false };
    },
  }, { requestId: "rid-list" });

  await bucket.list({ limit: "1000" });

  assert.deepEqual(calls, [{
    options: {
      prefix: undefined,
      delimiter: undefined,
      cursor: undefined,
      startAfter: undefined,
      limit: 1000,
      include: undefined,
    },
    requestMeta: { requestId: "rid-list" },
  }]);
});

test("R2Bucket host methods preserve stub receiver", async () => {
  const stub = {
    marker: "r2-stub",
    async head(/** @type {string} */ key) {
      assert.equal(this, stub);
      return {
        key,
        version: "",
        size: 0,
        etag: "abc",
        httpEtag: '"abc"',
        uploaded: Date.now(),
        httpMetadata: {},
        customMetadata: {},
        checksums: {},
        storageClass: "Standard",
      };
    },
  };
  const bucket = new R2Bucket(stub);

  const result = /** @type {any} */ (await bucket.head("receiver.txt"));

  assert.equal(result.key, "receiver.txt");
});

test("R2Bucket.get preserves onlyIf etag arrays for host binding", async () => {
  /** @type {any[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async get(/** @type {string} */ key, /** @type {any} */ options, /** @type {any} */ requestMeta) {
      calls.push({ key, options, requestMeta });
      return null;
    },
  }, { requestId: "rid-1" });

  await bucket.get("a.txt", {
    onlyIf: {
      etagMatches: ["a", "b"],
      etagDoesNotMatch: ["c"],
    },
  });

  assert.deepEqual(calls, [{
    key: "a.txt",
    options: {
      range: undefined,
      onlyIf: {
        etagMatches: ["a", "b"],
        etagDoesNotMatch: ["c"],
      },
    },
    requestMeta: { requestId: "rid-1" },
  }]);
});

test("R2Bucket.put preserves onlyIf etag arrays for host binding", async () => {
  /** @type {any[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async put(/** @type {string} */ key, /** @type {any} */ value, /** @type {any} */ options, /** @type {any} */ requestMeta) {
      calls.push({ key, value, options, requestMeta });
      return null;
    },
  }, { requestId: "rid-put" });

  const result = await bucket.put("a.txt", "hello", {
    onlyIf: {
      etagMatches: ["a", "b"],
      etagDoesNotMatch: ["c"],
    },
  });

  assert.equal(result, null);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].key, "a.txt");
  assert.deepEqual([...calls[0].value], [...new TextEncoder().encode("hello")]);
  assert.deepEqual(calls[0].options, {
    httpMetadata: undefined,
    customMetadata: undefined,
    storageClass: undefined,
    onlyIf: {
      etagMatches: ["a", "b"],
      etagDoesNotMatch: ["c"],
    },
  });
  assert.deepEqual(calls[0].requestMeta, { requestId: "rid-put" });
});

test("R2Bucket.put normalizes every Headers httpMetadata field", async () => {
  /** @type {any[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async put(/** @type {string} */ _key, /** @type {any} */ _value, /** @type {any} */ options) {
      calls.push(options);
      return null;
    },
  });
  const headers = new Headers({
    "content-type": "text/plain",
    "content-language": "en",
    "content-disposition": "inline",
    "content-encoding": "gzip",
    "cache-control": "max-age=60",
    expires: "Thu, 01 Jan 1970 00:00:00 GMT",
  });

  await bucket.put("a.txt", "hello", { httpMetadata: headers });

  assert.deepEqual(calls[0].httpMetadata, {
    contentType: "text/plain",
    contentLanguage: "en",
    contentDisposition: "inline",
    contentEncoding: "gzip",
    cacheControl: "max-age=60",
    cacheExpiry: 0,
  });
});

test("R2Bucket.put rejects non-canonical Headers expiry before the host call", async () => {
  let calls = 0;
  const bucket = new R2Bucket({
    async put() {
      calls += 1;
      return null;
    },
  });
  for (const expires of [
    "not-a-date",
    "0",
    "2026-07-13T00:00:00.000Z",
    "July 13, 2026",
    "Tue, 13 Jul 2026 00:00:00 GMT",
  ]) {
    const headers = new Headers({ expires });
    await assert.rejects(
      () => bucket.put("a.txt", "hello", { httpMetadata: headers }),
      /Expires header must be canonical IMF-fixdate/
    );
  }
  assert.equal(calls, 0);
});

test("R2Object.writeHttpMetadata writes every mapped field including epoch expiry", () => {
  const object = new R2Object({
    httpMetadata: {
      contentType: "text/plain",
      contentLanguage: "en",
      contentDisposition: "inline",
      contentEncoding: "gzip",
      cacheControl: "max-age=60",
      cacheExpiry: 0,
    },
  });
  const headers = new Headers();

  object.writeHttpMetadata(headers);

  assert.deepEqual(Object.fromEntries(headers), {
    "cache-control": "max-age=60",
    "content-disposition": "inline",
    "content-encoding": "gzip",
    "content-language": "en",
    "content-type": "text/plain",
    expires: "Thu, 01 Jan 1970 00:00:00 GMT",
  });
});

test("R2Bucket.put rejects timestamp onlyIf conditions", async () => {
  const bucket = new R2Bucket({
    async put() {
      throw new Error("host put should not be called");
    },
  });

  await assert.rejects(
    () => bucket.put("a.txt", "hello", {
      onlyIf: { uploadedBefore: new Date() },
    }),
    /put\(\{onlyIf\}\) only supports etag-based conditions/
  );
  await assert.rejects(
    () => bucket.put("a.txt", "hello", {
      onlyIf: { uploadedAfter: new Date() },
    }),
    /put\(\{onlyIf\}\) only supports etag-based conditions/
  );
});

test("R2Bucket.put reads Blob through the capped stream path", async () => {
  /** @type {any[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async put(/** @type {string} */ key, /** @type {any} */ value) {
      calls.push({ key, value });
      return {
        key,
        version: "",
        size: value.byteLength,
        etag: "abc",
        httpEtag: '"abc"',
        uploaded: Date.now(),
        httpMetadata: {},
        customMetadata: {},
        checksums: {},
        storageClass: "Standard",
      };
    },
  });
  const blob = new Blob(["hello"]);
  blob.arrayBuffer = async () => {
    throw new Error("Blob.arrayBuffer should not be used for R2 put");
  };

  const meta = /** @type {any} */ (await bucket.put("blob.txt", blob));

  assert.equal(meta.size, 5);
  assert.equal(calls.length, 1);
  assert.deepEqual([...calls[0].value], [...new TextEncoder().encode("hello")]);
});

test("R2Bucket.put keeps single-chunk ReadableStream bytes without re-copying", async () => {
  const chunk = new TextEncoder().encode("hello");
  let observed;
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed = value;
      return {
        key: "stream.txt",
        version: "",
        size: value.byteLength,
        etag: "abc",
        httpEtag: '"abc"',
        uploaded: Date.now(),
        httpMetadata: {},
        customMetadata: {},
        checksums: {},
        storageClass: "Standard",
      };
    },
  });
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
      controller.close();
    },
  });

  const meta = /** @type {any} */ (await bucket.put("stream.txt", stream));

  assert.equal(meta.size, 5);
  assert.equal(observed, chunk);
});

test("R2Bucket.put rejects an oversized stream without waiting for cancel", async () => {
  let cancelled = false;
  let hostCalls = 0;
  const bucket = new R2Bucket({
    async put() {
      hostCalls += 1;
      return null;
    },
  });
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES + 1));
    },
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  });

  const outcome = await settlementWithinTestWindow(bucket.put("huge.bin", stream));

  assert.equal(outcome.status, "rejected");
  assert.match(String("reason" in outcome ? outcome.reason : ""), /exceeds the 25 MiB WDL R2 limit/);
  assert.equal(cancelled, true);
  assert.equal(hostCalls, 0);
});

test("R2Bucket.get returns R2Object when host binding returns no body", async () => {
  const bucket = new R2Bucket({
    async get() {
      return {
        meta: {
          key: "a.txt",
          version: "",
          size: 10,
          etag: "abc",
          httpEtag: '"abc"',
          uploaded: Date.now(),
          httpMetadata: {},
          customMetadata: {},
          checksums: {},
          storageClass: "Standard",
        },
      };
    },
  });

  const obj = await bucket.get("a.txt");
  assert.ok(obj instanceof R2Object);
  assert.equal(obj instanceof R2ObjectBody, false);
  assert.equal(obj.key, "a.txt");
});

test("R2ObjectBody.bytes keeps a full-buffer single chunk without re-copying", async () => {
  const chunk = new TextEncoder().encode("hello");
  const obj = new R2ObjectBody({
    key: "a.txt",
    version: "",
    size: chunk.byteLength,
    etag: "abc",
    httpEtag: '"abc"',
    uploaded: Date.now(),
    httpMetadata: {},
    customMetadata: {},
    checksums: {},
    storageClass: "Standard",
  }, new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
      controller.close();
    },
  }));

  const bytes = await obj.bytes();

  assert.equal(bytes, chunk);
});

test("R2ObjectBody.bytes copies a sliced single chunk before exposing it", async () => {
  const backing = new TextEncoder().encode("xxhelloyy");
  const chunk = backing.subarray(2, 7);
  const obj = new R2ObjectBody({
    key: "a.txt",
    version: "",
    size: chunk.byteLength,
    etag: "abc",
    httpEtag: '"abc"',
    uploaded: Date.now(),
    httpMetadata: {},
    customMetadata: {},
    checksums: {},
    storageClass: "Standard",
  }, new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
      controller.close();
    },
  }));

  const bytes = await obj.bytes();

  assert.notEqual(bytes, chunk);
  assert.deepEqual([...bytes], [...new TextEncoder().encode("hello")]);
  assert.equal(bytes.byteOffset, 0);
  assert.equal(bytes.byteLength, bytes.buffer.byteLength);
  assert.deepEqual([...new Uint8Array(bytes.buffer)], [...bytes]);
});

test("R2ObjectBody raw body stream enforces the object byte cap", async () => {
  const obj = new R2ObjectBody({
    key: "huge.bin",
    version: "",
    size: R2_OBJECT_MAX_BUFFER_BYTES + 1,
    etag: "abc",
    httpEtag: '"abc"',
    uploaded: Date.now(),
    httpMetadata: {},
    customMetadata: {},
    checksums: {},
    storageClass: "Standard",
  }, new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES));
      controller.enqueue(new Uint8Array(1));
      controller.close();
    },
  }));

  const reader = obj.body.getReader();
  const first = await reader.read();

  assert.equal(first.done, false);
  assert.equal(first.value.byteLength, R2_OBJECT_MAX_BUFFER_BYTES);
  await assert.rejects(
    () => reader.read(),
    /R2 get: object is .* exceeds the 25 MiB WDL R2 limit/
  );
});

test("R2ObjectBody byte cap does not wait for underlying cancel", async () => {
  let cancelled = false;
  const obj = new R2ObjectBody({
    key: "huge.bin",
    version: "",
    size: R2_OBJECT_MAX_BUFFER_BYTES + 1,
    etag: "abc",
    httpEtag: '"abc"',
    uploaded: Date.now(),
    httpMetadata: {},
    customMetadata: {},
    checksums: {},
    storageClass: "Standard",
  }, new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES + 1));
    },
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  }));

  const outcome = await settlementWithinTestWindow(obj.body.getReader().read());

  assert.equal(outcome.status, "rejected");
  assert.match(String("reason" in outcome ? outcome.reason : ""), /exceeds the 25 MiB WDL R2 limit/);
  assert.equal(cancelled, true);
});

test("R2Bucket multipart upload methods fail with WDL-specific errors", () => {
  const bucket = new R2Bucket({});

  assert.throws(
    () => bucket.createMultipartUpload("big.bin"),
    /WDL R2 does not support multipart upload yet/
  );
  assert.throws(
    () => bucket.resumeMultipartUpload("big.bin", "upload-id"),
    /WDL R2 does not support multipart upload yet/
  );
});
