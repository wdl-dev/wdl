import { test } from "node:test";
import assert from "node:assert/strict";
import { R2Bucket, R2Object, R2ObjectBody } from "../../runtime/r2-client.js";
import { R2_OBJECT_MAX_BUFFER_BYTES } from "../../runtime/r2-utils.js";
import { withMockedPropertyDescriptor } from "../helpers/mock-global.js";
import {
  importRepositoryModuleFresh,
  importSpecifierReplacements,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { delay, settlementWithin } from "../helpers/timing.js";

/**
 * @param {Uint8Array} firstChunk
 * @param {() => void} mutate
 * @param {{ mutateAfterClose?: boolean, trailingChunk?: Uint8Array | null }} [options]
 */
function streamWithMutationOnSecondPull(
  firstChunk,
  mutate,
  { mutateAfterClose = false, trailingChunk = null } = {}
) {
  let pullCount = 0;
  return new ReadableStream({
    pull(controller) {
      pullCount += 1;
      if (pullCount === 1) {
        controller.enqueue(firstChunk);
        return;
      }
      if (!mutateAfterClose) mutate();
      if (trailingChunk !== null) controller.enqueue(trailingChunk);
      controller.close();
      if (mutateAfterClose) queueMicrotask(mutate);
    },
  }, { highWaterMark: 0 });
}

/** @param {Uint8Array} chunk @param {() => void} mutate */
function streamWithQueuedMutationOnPull(chunk, mutate) {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
    },
    pull(controller) {
      mutate();
      controller.close();
    },
  });
}

/** @param {...Uint8Array} chunks */
function streamFromChunks(...chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

/** @param {{ key?: string, size?: number }} [metadata] */
function r2ObjectBodyMetadata({ key = "a.txt", size = 0 } = {}) {
  return {
    key,
    version: "",
    size,
    etag: "abc",
    httpEtag: '"abc"',
    uploaded: Date.now(),
    httpMetadata: {},
    customMetadata: {},
    checksums: {},
    storageClass: "Standard",
  };
}

/**
 * @param {ReadableStream<Uint8Array>} body
 * @param {{ key?: string, size?: number }} [metadata]
 */
function createR2ObjectBody(body, metadata) {
  return new R2ObjectBody(r2ObjectBodyMetadata(metadata), body);
}

function importR2ClientFresh() {
  return importRepositoryModuleFresh(
    "runtime/r2-client.js",
    importSpecifierReplacements({
      "./_wdl-r2-utils.js": repositoryFileUrl("runtime/_wdl-r2-utils.js"),
      "./_wdl-request-id.js": repositoryFileUrl("runtime/_wdl-request-id.js"),
    })
  );
}

/** @param {keyof typeof globalThis} name @param {unknown} value */
async function importR2ClientWithGlobal(name, value) {
  return await withMockedPropertyDescriptor(
    globalThis,
    name,
    { value },
    importR2ClientFresh
  );
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
    async put(/** @type {string} */ key, /** @type {Uint8Array} */ value) {
      assert.equal(this, stub);
      assert.equal(key, "receiver.txt");
      assert.deepEqual(Array.from(value), [104, 101, 108, 112]);
      return null;
    },
  };
  const bucket = new R2Bucket(stub);

  const result = /** @type {any} */ (await bucket.head("receiver.txt"));
  await bucket.put("receiver.txt", new Uint8Array([104, 101, 108, 112]));

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

test("R2Bucket.put ignores overridden view bounds and rejects out-of-bounds views", async () => {
  /** @type {Uint8Array[]} */
  const calls = [];
  const bucket = new R2Bucket({
    async put(_key, value) {
      calls.push(value);
      return null;
    },
  });
  class MisleadingWords extends Uint16Array {
    get buffer() { return new ArrayBuffer(0); }
    get byteOffset() { return 0; }
    get byteLength() { return 0; }
  }

  const source = new ArrayBuffer(8);
  new Uint8Array(source).set([0, 0, 104, 101, 108, 112, 0, 0]);
  await bucket.put("safe.bin", new MisleadingWords(source, 2, 2));

  assert.equal(calls.length, 1);
  assert.deepEqual(Array.from(calls[0]), [104, 101, 108, 112]);
  assert.equal(Object.getPrototypeOf(calls[0]), Uint8Array.prototype);

  const resizable = new ArrayBuffer(8, { maxByteLength: 16 });
  const outOfBounds = new Uint8Array(resizable, 2, 4);
  resizable.resize(1);
  await assert.rejects(() => bucket.put("empty.bin", outOfBounds), TypeError);
  assert.equal(calls.length, 1);
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

test("R2Bucket.put uploads Blob bytes", async () => {
  /** @type {Uint8Array | undefined} */
  let observed;
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed = value;
      return null;
    },
  });

  await bucket.put("blob.txt", new Blob(["hello"]));

  assert.ok(observed);
  assert.deepEqual(Array.from(observed), Array.from(new TextEncoder().encode("hello")));
});

test("R2Bucket.put preflights Blob size through an inherited getter", async () => {
  let bytesCalls = 0;
  class PrototypeSizeBlob {
    get size() {
      return R2_OBJECT_MAX_BUFFER_BYTES + 1;
    }

    slice() {
      throw new Error("Blob.slice should not be used with a size getter");
    }

    async bytes() {
      bytesCalls += 1;
      return new Uint8Array();
    }
  }

  const { R2Bucket: PrototypeSizeR2Bucket } = await importR2ClientWithGlobal(
    "Blob",
    PrototypeSizeBlob
  );
  const blob = new PrototypeSizeBlob();
  const bucket = new PrototypeSizeR2Bucket({
    async put() {
      throw new Error("host put should not be called");
    },
  });

  await assert.rejects(
    () => bucket.put("large.bin", blob),
    /R2 put: object is .* exceeds the 25 MiB WDL R2 limit/
  );
  assert.equal(bytesCalls, 0);
});

test("R2Bucket.put preflights Blob size in legacy instance-property mode", async () => {
  let bytesCalls = 0;
  class InstanceSizeBlob {
    #size;

    /** @param {number} size */
    constructor(size) {
      this.#size = size;
      Object.defineProperty(this, "size", {
        configurable: true,
        enumerable: true,
        value: size,
      });
    }

    /** @param {number} [start] @param {number} [end] */
    slice(start = 0, end = this.#size) {
      const boundedStart = Math.min(Math.max(start, 0), this.#size);
      const boundedEnd = Math.min(Math.max(end, 0), this.#size);
      return new InstanceSizeBlob(Math.max(boundedEnd - boundedStart, 0));
    }

    async bytes() {
      bytesCalls += 1;
      return new Uint8Array();
    }
  }

  const { R2Bucket: InstanceSizeR2Bucket } = await importR2ClientWithGlobal(
    "Blob",
    InstanceSizeBlob
  );
  const blob = new InstanceSizeBlob(R2_OBJECT_MAX_BUFFER_BYTES + 1);
  const bucket = new InstanceSizeR2Bucket({
    async put() {
      throw new Error("host put should not be called");
    },
  });

  await assert.rejects(
    () => bucket.put("large.bin", blob),
    /R2 put: object is .* exceeds the 25 MiB WDL R2 limit/
  );
  assert.equal(bytesCalls, 0);
});

test("R2 buffered readers observe default-HWM mutations made before delivery", async () => {
  const deliveredBytes = [0, 0, 0, 0];
  const putChunk = new Uint8Array([104, 101, 108, 112]);
  /** @type {number[][]} */
  const putObserved = [];
  const bucket = new R2Bucket({
    async put(_key, value) {
      putObserved.push(Array.from(value));
      return null;
    },
  });

  // Default-HWM PullSteps calls pull before the queued chunk reaches its reader.
  await bucket.put("queued.bin", streamWithQueuedMutationOnPull(
    putChunk,
    () => putChunk.fill(0)
  ));

  const bodyChunk = new Uint8Array([104, 101, 108, 112]);
  const body = createR2ObjectBody(
    streamWithQueuedMutationOnPull(bodyChunk, () => bodyChunk.fill(0)),
    { key: "queued.bin", size: bodyChunk.byteLength }
  );

  assert.deepEqual(putObserved, [deliveredBytes]);
  assert.deepEqual(Array.from(await body.bytes()), deliveredBytes);
});

test("R2Bucket.put preserves fixed chunks across later stream reads", async () => {
  /** @type {number[][]} */
  const observed = [];
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed.push(Array.from(value));
      return null;
    },
  });

  for (const trailingChunk of [null, new Uint8Array([33, 34])]) {
    const first = new Uint8Array([104, 101, 108, 112]);
    const stream = streamWithMutationOnSecondPull(
      first,
      () => first.fill(0),
      { trailingChunk }
    );

    await bucket.put("fixed.bin", stream);
    assert.deepEqual(
      observed.at(-1),
      trailingChunk === null
        ? [104, 101, 108, 112]
        : [104, 101, 108, 112, 33, 34]
    );
  }
});

test("R2Bucket.put preserves resizable chunks across later stream reads", async () => {
  /** @type {number[][]} */
  const observed = [];
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed.push(Array.from(value));
      return null;
    },
  });

  for (const { resizeTo, mutateAfterClose, trailingChunk, expected } of [
    {
      resizeTo: [2],
      mutateAfterClose: false,
      trailingChunk: new Uint8Array([33, 34]),
      expected: [104, 101, 108, 112, 33, 34],
    },
    {
      resizeTo: [2, 4],
      mutateAfterClose: false,
      trailingChunk: null,
      expected: [104, 101, 108, 112],
    },
    {
      resizeTo: [2],
      mutateAfterClose: true,
      trailingChunk: null,
      expected: [104, 101, 108, 112],
    },
  ]) {
    const backing = new ArrayBuffer(4, { maxByteLength: 4 });
    const first = new Uint8Array(backing);
    first.set([104, 101, 108, 112]);
    const stream = streamWithMutationOnSecondPull(first, () => {
      for (const size of resizeTo) backing.resize(size);
    }, { mutateAfterClose, trailingChunk });

    await bucket.put("resized.bin", stream);
    assert.deepEqual(observed.at(-1), expected);
  }
});

test("R2Bucket.put preserves shared chunks across later stream reads", async () => {
  const backing = new SharedArrayBuffer(4);
  const chunk = new Uint8Array(backing);
  chunk.set([104, 101, 108, 112]);
  /** @type {number[] | undefined} */
  let observed;
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed = Array.from(value);
      return null;
    },
  });
  const stream = streamWithMutationOnSecondPull(chunk, () => chunk.fill(0));

  await bucket.put("shared.bin", stream);

  assert.deepEqual(observed, [104, 101, 108, 112]);
});

test("R2Bucket.put stabilizes zero-length resizable chunks before later reads", async () => {
  const backing = new ArrayBuffer(4, { maxByteLength: 8 });
  const chunk = new Uint8Array(backing, 4, 0);
  /** @type {Uint8Array | undefined} */
  let observed;
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed = value;
      return null;
    },
  });
  const stream = streamWithMutationOnSecondPull(chunk, () => backing.resize(2));

  await bucket.put("empty.bin", stream);

  assert.ok(observed);
  assert.notEqual(observed, chunk);
  assert.equal(observed.byteLength, 0);
  assert.doesNotThrow(() => Uint8Array.prototype.at.call(observed, 0));
});

test("R2Bucket.put preserves zero-length fixed chunks after source detachment", async () => {
  const backing = new ArrayBuffer(0);
  const chunk = new Uint8Array(backing);
  /** @type {Uint8Array | undefined} */
  let observed;
  const bucket = new R2Bucket({
    async put(_key, value) {
      observed = value;
      return null;
    },
  });
  const stream = streamWithMutationOnSecondPull(chunk, () => {
    structuredClone(backing, { transfer: [backing] });
  });

  await bucket.put("empty.bin", stream);

  assert.ok(observed);
  assert.notEqual(observed, chunk);
  assert.equal(observed.byteLength, 0);
  assert.doesNotThrow(() => Uint8Array.prototype.at.call(observed, 0));
});

test("R2Bucket.put rejects an invalid chunk without waiting for cancel", async () => {
  let cancelCalls = 0;
  let hostCalls = 0;
  const bucket = new R2Bucket({
    async put() {
      hostCalls += 1;
      return null;
    },
  });
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(/** @type {any} */ ("not bytes"));
    },
    cancel() {
      cancelCalls += 1;
      return new Promise(() => {});
    },
  });
  const outcome = await settlementWithin(bucket.put("invalid.bin", stream));

  assert.equal(outcome.status, "rejected");
  assert.match(String("reason" in outcome ? outcome.reason : ""), /must be BufferSource values/);
  assert.equal(cancelCalls, 1);
  assert.equal(stream.locked, false);
  assert.equal(hostCalls, 0);
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
  class HiddenOversizedChunk extends Uint8Array {
    get byteLength() { return 0; }
  }
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new HiddenOversizedChunk(R2_OBJECT_MAX_BUFFER_BYTES + 1));
    },
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  });

  const outcome = await settlementWithin(bucket.put("huge.bin", stream));

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

test("R2ObjectBody.bytes returns an owned exact-fit snapshot", async () => {
  const expected = new TextEncoder().encode("hello");
  const padded = new TextEncoder().encode("xxhelloyy");

  for (const chunk of [expected, padded.subarray(2, 7)]) {
    const obj = createR2ObjectBody(streamFromChunks(chunk), {
      size: chunk.byteLength,
    });
    const bytes = await obj.bytes();

    assert.notStrictEqual(bytes, chunk);
    assert.deepEqual(Array.from(bytes), Array.from(expected));
    assert.equal(bytes.byteOffset, 0);
    assert.equal(bytes.byteLength, bytes.buffer.byteLength);
  }
});

test("R2ObjectBody.arrayBuffer reuses its exact-fit byte buffer", async () => {
  const chunk = new TextEncoder().encode("hello");
  const obj = createR2ObjectBody(streamFromChunks(chunk), { size: chunk.byteLength });

  const buffer = await withMockedPropertyDescriptor(
    ArrayBuffer.prototype,
    "slice",
    {
      writable: true,
      value() { throw new Error("ArrayBuffer.prototype.slice should not be called"); },
    },
    () => obj.arrayBuffer()
  );

  assert.deepEqual([...new Uint8Array(buffer)], [...chunk]);
});

test("R2ObjectBody uses an inherited Response.bodyUsed accessor", async () => {
  const NativeResponse = Response;
  class InheritedBodyUsedResponse extends NativeResponse {}
  assert.equal(
    Object.getOwnPropertyDescriptor(InheritedBodyUsedResponse.prototype, "bodyUsed"),
    undefined
  );
  const { R2ObjectBody: InheritedModeR2ObjectBody } = await importR2ClientWithGlobal(
    "Response",
    InheritedBodyUsedResponse
  );
  const obj = new InheritedModeR2ObjectBody(
    r2ObjectBodyMetadata({ size: 1 }),
    streamFromChunks(Uint8Array.of(1))
  );

  assert.equal(obj.bodyUsed, false);
  const reader = obj.body.getReader();
  const first = await reader.read();
  reader.releaseLock();
  assert.equal(first.done, false);
  assert.equal(obj.bodyUsed, true);
});

test("R2ObjectBody reads an instance Response.bodyUsed data property", async () => {
  const trackerState = {
    /** @type {{ bodyUsed: boolean } | null} */
    current: null,
  };
  class InstanceDataResponse {
    bodyUsed = false;

    constructor() {
      trackerState.current = this;
    }
  }

  const { R2ObjectBody: InstanceModeR2ObjectBody } = await importR2ClientWithGlobal(
    "Response",
    InstanceDataResponse
  );
  const obj = new InstanceModeR2ObjectBody(
    r2ObjectBodyMetadata(),
    streamFromChunks()
  );
  const tracker = trackerState.current;
  if (!tracker) throw new TypeError("missing instance-data Response tracker");

  const descriptor = Object.getOwnPropertyDescriptor(tracker, "bodyUsed");
  assert.equal(descriptor?.get, undefined);
  assert.equal(descriptor?.value, false);
  assert.equal(obj.bodyUsed, false);
  tracker.bodyUsed = true;
  assert.equal(obj.bodyUsed, true);
  await assert.rejects(() => obj.bytes(), /Body has already been used/);
});

test("R2ObjectBody retries after an unused raw reader releases its lock", async () => {
  const obj = createR2ObjectBody(streamFromChunks(Uint8Array.of(1, 2, 3)), {
    size: 3,
  });
  const reader = obj.body.getReader();

  assert.equal(obj.bodyUsed, false);
  await assert.rejects(() => obj.arrayBuffer(), TypeError);
  assert.equal(obj.bodyUsed, false);
  reader.releaseLock();

  assert.deepEqual(Array.from(new Uint8Array(await obj.arrayBuffer())), [1, 2, 3]);
  assert.equal(obj.bodyUsed, true);
});

test("R2ObjectBody rejects convenience reads after its raw body is disturbed", async () => {
  const obj = createR2ObjectBody(
    streamFromChunks(
      Uint8Array.of(1, 2, 3),
      Uint8Array.of(4, 5, 6)
    ),
    { size: 6 }
  );

  assert.equal(obj.bodyUsed, false);
  const reader = obj.body.getReader();
  const first = await reader.read();
  reader.releaseLock();

  assert.equal(first.done, false);
  assert.ok(first.value);
  assert.deepEqual(Array.from(first.value), [1, 2, 3]);
  assert.equal(obj.bodyUsed, true);
  await assert.rejects(
    () => obj.arrayBuffer(),
    /Body has already been used/
  );
});

test("R2ObjectBody raw body keeps fixed ArrayBuffer chunks zero-copy", async () => {
  const chunk = Uint8Array.of(104, 101, 108, 112);
  const obj = createR2ObjectBody(streamFromChunks(chunk), {
    size: chunk.byteLength,
  });

  const reader = obj.body.getReader();
  const first = await reader.read();
  reader.releaseLock();

  assert.equal(first.done, false);
  assert.strictEqual(first.value, chunk);
});

test("R2ObjectBody raw body snapshots resizable ArrayBuffer chunks", async () => {
  const backing = new ArrayBuffer(4, { maxByteLength: 8 });
  const chunk = new Uint8Array(backing);
  chunk.set([104, 101, 108, 112]);
  const obj = createR2ObjectBody(streamFromChunks(chunk), {
    size: chunk.byteLength,
  });

  const reader = obj.body.getReader();
  const first = await reader.read();
  reader.releaseLock();

  assert.equal(first.done, false);
  assert.ok(first.value);
  assert.notStrictEqual(first.value, chunk);
  backing.resize(2);
  backing.resize(4);
  assert.deepEqual(Array.from(first.value), [104, 101, 108, 112]);
});

test("R2ObjectBody raw body rejects an invalid chunk without waiting for cancel", async () => {
  let cancelCalls = 0;
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(/** @type {any} */ ("not bytes"));
    },
    cancel() {
      cancelCalls += 1;
      return new Promise(() => {});
    },
  });
  const obj = createR2ObjectBody(source, { key: "invalid.bin" });
  const reader = obj.body.getReader();

  const outcome = await settlementWithin(reader.read());
  await delay(0);

  assert.equal(outcome.status, "rejected");
  assert.match(String("reason" in outcome ? outcome.reason : ""), /must be BufferSource values/);
  assert.equal(cancelCalls, 1);
  assert.equal(source.locked, false);
  reader.releaseLock();
});

test("R2ObjectBody raw body stream enforces the object byte cap", async () => {
  const obj = createR2ObjectBody(new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES));
      controller.enqueue(new Uint8Array(1));
      controller.close();
    },
  }), {
    key: "huge.bin",
    size: R2_OBJECT_MAX_BUFFER_BYTES + 1,
  });

  const reader = obj.body.getReader();
  const first = await reader.read();
  await assert.rejects(
    () => reader.read(),
    /R2 get: object is .* exceeds the 25 MiB WDL R2 limit/
  );
  assert.equal(first.done, false);
  assert.equal(first.value?.byteLength, R2_OBJECT_MAX_BUFFER_BYTES);
});

test("R2ObjectBody byte cap does not wait for underlying cancel", async () => {
  let cancelled = false;
  const obj = createR2ObjectBody(new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(R2_OBJECT_MAX_BUFFER_BYTES + 1));
    },
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  }), {
    key: "huge.bin",
    size: R2_OBJECT_MAX_BUFFER_BYTES + 1,
  });

  const outcome = await settlementWithin(obj.body.getReader().read());
  await delay(0);

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
