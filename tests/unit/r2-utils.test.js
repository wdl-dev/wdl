import { test } from "node:test";
import assert from "node:assert/strict";
import {
  R2_BUCKET_NAME_RE as RUNTIME_R2_BUCKET_NAME_RE,
  R2_HTTP_METADATA_FIELDS,
  R2_OBJECT_MAX_BUFFER_BYTES,
  assertR2BufferSize,
  encodeS3KeyPath,
  encodeS3Query,
  normalizeR2ObjectKey,
  r2BufferSourceBytes,
  r2PhysicalKey,
  r2PhysicalPrefix,
  r2RangeAndSizeFromHeaders,
  r2CacheExpiryFromHeaders,
  r2Uint8ArrayByteLength,
  r2Uint8ArrayHasResizableOrSharedBacking,
  setR2CacheExpiryHeader,
  stripR2PhysicalPrefix,
  validateR2BucketName,
} from "../../runtime/r2-utils.js";
import { R2_BUCKET_NAME_RE as CONTROL_R2_BUCKET_NAME_RE } from "../../shared/ns-pattern.js";
import { withMockedPropertyDescriptors } from "../helpers/mock-global.js";

test("r2PhysicalPrefix scopes virtual buckets under namespace", () => {
  assert.equal(
    r2PhysicalPrefix({ ns: "demo", bucketName: "uploads" }),
    "r2/demo/uploads/"
  );
  assert.equal(
    r2PhysicalKey({ ns: "demo", bucketName: "uploads" }, "dir/x.txt"),
    "r2/demo/uploads/dir/x.txt"
  );
});

test("normalizeR2ObjectKey rejects URL path traversal segments", () => {
  for (const key of [".", "..", "./x", "../x", "a/./x", "a/../x", "a//..//x"]) {
    assert.throws(
      () => normalizeR2ObjectKey(key),
      /must not contain \. or \.\. path segments/
    );
  }
  for (const key of [".hidden", "..hidden", "a/.../x", "a//x", "a/%2e%2e/x"]) {
    assert.equal(normalizeR2ObjectKey(key), key);
  }
});

test("normalizeR2ObjectKey rejects traversal after intrinsic tampering", async () => {
  await withMockedPropertyDescriptors([
    {
      target: RegExp.prototype,
      name: "exec",
      descriptor: {
        configurable: true,
        writable: true,
        value() { return null; },
      },
    },
  ], () => {
    assert.equal(normalizeR2ObjectKey("safe/path"), "safe/path");
    assert.throws(
      () => normalizeR2ObjectKey("../escape"),
      /must not contain \. or \.\./
    );
  });
});

test("stripR2PhysicalPrefix rejects backend keys outside the binding prefix", () => {
  const props = { ns: "demo", bucketName: "uploads" };
  assert.equal(stripR2PhysicalPrefix(props, "r2/demo/uploads/a.txt"), "a.txt");
  assert.throws(
    () => stripR2PhysicalPrefix(props, "r2/other/uploads/a.txt"),
    /outside the binding prefix/
  );
});

test("validateR2BucketName enforces prefix-safe virtual bucket names", () => {
  validateR2BucketName("uploads-1");
  assert.throws(() => validateR2BucketName("Uploads"), /bucket_name must match/);
  assert.throws(() => validateR2BucketName("bad/name"), /bucket_name must match/);
});

test("injected and control R2 bucket grammars stay identical", () => {
  assert.equal(RUNTIME_R2_BUCKET_NAME_RE.source, CONTROL_R2_BUCKET_NAME_RE.source);
  assert.equal(RUNTIME_R2_BUCKET_NAME_RE.flags, CONTROL_R2_BUCKET_NAME_RE.flags);
});

test("R2 HTTP metadata fields and cache expiry use one mapping", () => {
  assert.deepEqual(R2_HTTP_METADATA_FIELDS, [
    ["contentType", "content-type"],
    ["contentLanguage", "content-language"],
    ["contentDisposition", "content-disposition"],
    ["contentEncoding", "content-encoding"],
    ["cacheControl", "cache-control"],
  ]);
  const headers = new Headers();
  setR2CacheExpiryHeader(headers, 0);
  assert.equal(headers.get("expires"), "Thu, 01 Jan 1970 00:00:00 GMT");
  assert.equal(r2CacheExpiryFromHeaders(headers), 0);
  assert.equal(r2CacheExpiryFromHeaders(headers, { canonical: true }), 0);
  setR2CacheExpiryHeader(headers, "2026-07-12T00:00:00.000Z");
  assert.equal(headers.get("expires"), "Sun, 12 Jul 2026 00:00:00 GMT");
  headers.set("expires", "0");
  assert.equal(Number.isFinite(r2CacheExpiryFromHeaders(headers)), true);
  assert.equal(r2CacheExpiryFromHeaders(headers, { canonical: true }), undefined);
  headers.set("expires", "not-a-date");
  assert.equal(r2CacheExpiryFromHeaders(headers), undefined);
});

test("setR2CacheExpiryHeader rejects invalid expiry values", () => {
  for (const value of ["", "not-a-date", new Date(NaN), true]) {
    const headers = new Headers();
    assert.throws(
      () => setR2CacheExpiryHeader(headers, value),
      /cacheExpiry must be a valid Date or timestamp/
    );
    assert.equal(headers.has("expires"), false);
  }
});

test("encodeS3KeyPath percent-encodes key segments while preserving slashes", () => {
  assert.equal(
    encodeS3KeyPath("r2/demo/uploads/a b/?.txt"),
    "r2/demo/uploads/a%20b/%3F.txt"
  );
});

test("encodeS3KeyPath encodes key segments without URL path normalization", () => {
  const encoded = encodeS3KeyPath("assets/demo/site/v1/%2e%2e/%2e%2e/victim/app.js");

  assert.equal(
    encoded,
    "assets/demo/site/v1/%252e%252e/%252e%252e/victim/app.js"
  );
  assert.equal(
    new URL(`http://s3.local/bucket/${encoded}`).pathname,
    "/bucket/assets/demo/site/v1/%252e%252e/%252e%252e/victim/app.js"
  );
});

test("encodeS3Query keeps spaces as percent-encoded bytes", () => {
  assert.equal(
    encodeS3Query({
      "list-type": "2",
      prefix: "r2/demo/uploads/folder name",
      delimiter: "/",
      "continuation-token": "",
    }),
    "list-type=2&prefix=r2%2Fdemo%2Fuploads%2Ffolder%20name&delimiter=%2F"
  );
});

test("assertR2BufferSize caps buffered operations at 25MiB", () => {
  assert.equal(R2_OBJECT_MAX_BUFFER_BYTES, 25 * 1024 * 1024);
  assertR2BufferSize(R2_OBJECT_MAX_BUFFER_BYTES, "put");
  assert.throws(
    () => assertR2BufferSize(R2_OBJECT_MAX_BUFFER_BYTES + 1, "put"),
    /exceeds the 25 MiB WDL R2 limit/
  );
});

test("r2BufferSourceBytes uses intrinsic bounds for every BufferSource kind", () => {
  class MisleadingBytes extends Uint8Array {
    get buffer() { return new ArrayBuffer(0); }
    get byteOffset() { return 0; }
    get byteLength() { return 0; }
  }
  class MisleadingWords extends Uint16Array {
    get buffer() { return new ArrayBuffer(0); }
    get byteOffset() { return 0; }
    get byteLength() { return 0; }
  }
  class MisleadingView extends DataView {
    get buffer() { return new ArrayBuffer(0); }
    get byteOffset() { return 0; }
    get byteLength() { return 0; }
  }

  const source = new ArrayBuffer(8);
  new Uint8Array(source).set([0, 0, 104, 101, 108, 112, 0, 0]);
  const misleadingBytes = new MisleadingBytes(source, 2, 4);
  const misleadingWords = new MisleadingWords(source, 2, 2);
  const misleadingView = new MisleadingView(source, 2, 4);

  const sourceBytes = r2BufferSourceBytes(source);
  assert.ok(sourceBytes);
  assert.deepEqual(Array.from(sourceBytes), [0, 0, 104, 101, 108, 112, 0, 0]);
  for (const view of [misleadingBytes, misleadingWords, misleadingView]) {
    const bytes = r2BufferSourceBytes(view);
    assert.ok(bytes);
    assert.deepEqual(Array.from(bytes), [104, 101, 108, 112]);
    if (view === misleadingBytes) {
      assert.strictEqual(bytes, misleadingBytes);
    } else {
      assert.equal(Object.getPrototypeOf(bytes), Uint8Array.prototype);
    }
  }
  assert.equal(r2BufferSourceBytes("not bytes"), null);
});

test("r2BufferSourceBytes identifies ArrayBuffer by its internal slots", () => {
  const buffer = new ArrayBuffer(4);
  new Uint8Array(buffer).set([104, 101, 108, 112]);
  Object.setPrototypeOf(buffer, null);

  assert.equal(buffer instanceof ArrayBuffer, false);
  const bytes = r2BufferSourceBytes(buffer);
  assert.ok(bytes);
  assert.deepEqual(Array.from(bytes), [104, 101, 108, 112]);
});

test("r2Uint8ArrayByteLength ignores own bounds", () => {
  const ordinaryBytes = new Uint8Array([1, 2, 3]);
  Object.defineProperty(ordinaryBytes, "byteLength", { get: () => 0 });
  assert.strictEqual(r2BufferSourceBytes(ordinaryBytes), ordinaryBytes);
  assert.equal(r2Uint8ArrayByteLength(ordinaryBytes), 3);
});

test("r2Uint8ArrayHasResizableOrSharedBacking identifies dynamic backing", () => {
  assert.equal(r2Uint8ArrayHasResizableOrSharedBacking(new Uint8Array(1)), false);
  assert.equal(
    r2Uint8ArrayHasResizableOrSharedBacking(
      new Uint8Array(new ArrayBuffer(1, { maxByteLength: 2 }))
    ),
    true
  );
  assert.equal(
    r2Uint8ArrayHasResizableOrSharedBacking(
      new Uint8Array(new SharedArrayBuffer(1))
    ),
    true
  );
});

test("r2BufferSourceBytes ignores mutable prototypes when identifying view brands", () => {
  const disguisedWords = new Uint16Array(2);
  new Uint8Array(disguisedWords.buffer).set([108, 112, 33, 34]);
  Object.setPrototypeOf(disguisedWords, Uint8Array.prototype);
  const disguisedBytes = r2BufferSourceBytes(disguisedWords);
  assert.ok(disguisedBytes);
  assert.notStrictEqual(disguisedBytes, disguisedWords);
  assert.deepEqual(Array.from(disguisedBytes), [108, 112, 33, 34]);
});

test("r2BufferSourceBytes rejects detached and out-of-bounds inputs", () => {
  for (const createView of [
    (/** @type {ArrayBuffer} */ buffer) => new Uint8Array(buffer, 2, 4),
    (/** @type {ArrayBuffer} */ buffer) => new Uint16Array(buffer, 2, 2),
    (/** @type {ArrayBuffer} */ buffer) => new DataView(buffer, 2, 4),
  ]) {
    const resizable = new ArrayBuffer(8, { maxByteLength: 16 });
    const view = createView(resizable);
    resizable.resize(1);
    assert.throws(() => r2BufferSourceBytes(view), TypeError);
  }

  for (const createValue of [
    (/** @type {ArrayBuffer} */ buffer) => buffer,
    (/** @type {ArrayBuffer} */ buffer) => new Uint8Array(buffer),
    (/** @type {ArrayBuffer} */ buffer) => new Uint16Array(buffer),
    (/** @type {ArrayBuffer} */ buffer) => new DataView(buffer),
  ]) {
    const buffer = new ArrayBuffer(8);
    const value = createValue(buffer);
    structuredClone(buffer, { transfer: [buffer] });
    assert.throws(() => r2BufferSourceBytes(value), TypeError);
  }
});

test("r2Uint8ArrayByteLength rejects invalid zero-length views", () => {
  const detachedBuffer = new ArrayBuffer(0);
  const detached = new Uint8Array(detachedBuffer);
  structuredClone(detachedBuffer, { transfer: [detachedBuffer] });
  assert.throws(() => r2Uint8ArrayByteLength(detached), TypeError);

  const resizable = new ArrayBuffer(4, { maxByteLength: 8 });
  const outOfBounds = new Uint8Array(resizable, 4, 0);
  resizable.resize(2);
  assert.throws(() => r2Uint8ArrayByteLength(outOfBounds), TypeError);
});

test("R2 byte-view helpers ignore post-load intrinsic tampering", async () => {
  const source = new ArrayBuffer(8);
  new Uint8Array(source).set([0, 0, 104, 101, 108, 112, 0, 0]);
  const words = new Uint16Array(source, 2, 2);
  const view = new DataView(source, 2, 4);
  const resizable = new ArrayBuffer(8, { maxByteLength: 16 });
  const snapshotCandidate = new Uint8Array(resizable);
  const outOfBounds = new Uint8Array(resizable, 2, 4);
  resizable.resize(1);

  const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
  /** @type {Uint8Array | null | undefined} */
  let wordBytes;
  /** @type {Uint8Array | null | undefined} */
  let viewBytes;
  /** @type {unknown} */
  let outOfBoundsError;
  let hasResizableOrSharedBacking;
  await withMockedPropertyDescriptors([
    {
      target: Reflect,
      name: "apply",
      descriptor: { configurable: true, writable: true, value: () => 0 },
    },
    {
      target: ArrayBuffer,
      name: "isView",
      descriptor: { configurable: true, writable: true, value: () => false },
    },
    {
      target: ArrayBuffer.prototype,
      name: "resizable",
      descriptor: { configurable: true, get: () => false },
    },
    {
      target: typedArrayPrototype,
      name: "at",
      descriptor: { configurable: true, writable: true, value: () => undefined },
    },
    {
      target: typedArrayPrototype,
      name: Symbol.toStringTag,
      descriptor: { configurable: true, get: () => "Uint8Array" },
    },
    {
      target: globalThis,
      name: "DataView",
      descriptor: { configurable: true, writable: true, value: class FakeDataView {} },
    },
    {
      target: globalThis,
      name: "Uint8Array",
      descriptor: { configurable: true, writable: true, value: class FakeUint8Array {} },
    },
  ], () => {
    wordBytes = r2BufferSourceBytes(words);
    viewBytes = r2BufferSourceBytes(view);
    hasResizableOrSharedBacking = r2Uint8ArrayHasResizableOrSharedBacking(
      snapshotCandidate
    );
    try {
      r2BufferSourceBytes(outOfBounds);
    } catch (error) {
      outOfBoundsError = error;
    }
  });

  assert.ok(wordBytes);
  assert.ok(viewBytes);
  assert.deepEqual(Array.from(wordBytes), [104, 101, 108, 112]);
  assert.deepEqual(Array.from(viewBytes), [104, 101, 108, 112]);
  assert.ok(outOfBoundsError instanceof TypeError);
  assert.equal(hasResizableOrSharedBacking, true);
});

test("r2RangeAndSizeFromHeaders keeps object size on range responses", () => {
  const headers = new Headers({
    "content-length": "10",
    "content-range": "bytes 5-14/100",
  });
  assert.deepEqual(r2RangeAndSizeFromHeaders(headers), {
    size: 100,
    range: { offset: 5, length: 10 },
  });
});
