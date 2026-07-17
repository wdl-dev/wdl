import {
  R2_HTTP_METADATA_FIELDS,
  R2_OBJECT_MAX_BUFFER_BYTES,
  assertR2BufferSize,
  normalizeR2ListLimit,
  normalizeR2ObjectKey,
  r2CacheExpiryFromHeaders,
  setR2CacheExpiryHeader,
} from "./_wdl-r2-utils.js";
import { requestIdFromOptions } from "./_wdl-request-id.js";

/**
 * @typedef {Record<string, unknown>} AnyRecord
 * @typedef {{
 *   head?(key: string, requestMeta: object): Promise<AnyRecord | null>,
 *   get?(key: string, options: AnyRecord, requestMeta: object): Promise<null | { meta: AnyRecord, body?: ReadableStream<Uint8Array> | null }>,
 *   put?(key: string, body: Uint8Array, options: AnyRecord, requestMeta: object): Promise<AnyRecord | null>,
 *   delete?(keys: string | string[], requestMeta: object): Promise<unknown>,
 *   list?(options: AnyRecord, requestMeta: object): Promise<AnyRecord & { objects?: AnyRecord[], truncated?: unknown, cursor?: unknown, delimitedPrefixes?: unknown }>,
 * }} R2Stub
 * @typedef {{ stub: R2Stub, requestIdOptions: object }} R2BucketState
 */
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const intrinsicReflectApply = Reflect.apply;
const intrinsicWeakMapGet = WeakMap.prototype.get;
const intrinsicWeakMapSet = WeakMap.prototype.set;

/** @param {unknown} value */
function dateFromUnknown(value) {
  if (value instanceof Date || typeof value === "string" || typeof value === "number") {
    return new Date(value);
  }
  return new Date(String(value));
}

/** @param {unknown} value */
function stringOrUndefined(value) {
  return typeof value === "string" ? value : undefined;
}

/** @param {Uint8Array} bytes @returns {ArrayBuffer} */
function bytesToArrayBuffer(bytes) {
  return /** @type {ArrayBuffer} */ (
    bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
  );
}

/** @param {ReadableStreamDefaultReader<Uint8Array>} reader @param {unknown} reason */
function cancelReaderBestEffort(reader, reason) {
  try {
    void reader.cancel(reason).catch(() => {});
  } catch {}
}

/** @param {ReadableStream<Uint8Array>} stream @param {string} operation */
async function readStreamWithLimit(stream, operation) {
  const reader = stream.getReader();
  /** @type {Uint8Array[]} */
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      total += chunk.byteLength;
      if (total > R2_OBJECT_MAX_BUFFER_BYTES) {
        cancelReaderBestEffort(
          reader,
          `R2 ${operation}: object exceeds ${R2_OBJECT_MAX_BUFFER_BYTES} byte limit`
        );
        assertR2BufferSize(total, operation);
      }
      chunks.push(chunk);
    }
  } finally {
    try { reader.releaseLock(); } catch {}
  }
  if (chunks.length === 1) {
    const [chunk] = chunks;
    if (chunk.byteOffset === 0 && chunk.byteLength === chunk.buffer.byteLength) {
      return chunk;
    }
    return new Uint8Array(chunk);
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/** @param {ReadableStream<Uint8Array>} stream @param {string} operation */
function cappedReadableStream(stream, operation) {
  const reader = stream.getReader();
  let total = 0;
  return new ReadableStream({
    async pull(controller) {
      const { value, done } = await reader.read();
      if (done) {
        try { reader.releaseLock(); } catch {}
        controller.close();
        return;
      }
      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      total += chunk.byteLength;
      if (total > R2_OBJECT_MAX_BUFFER_BYTES) {
        cancelReaderBestEffort(
          reader,
          `R2 ${operation}: object exceeds ${R2_OBJECT_MAX_BUFFER_BYTES} byte limit`
        );
        try { reader.releaseLock(); } catch {}
        try {
          assertR2BufferSize(total, operation);
        } catch (err) {
          controller.error(err);
          return;
        }
      }
      controller.enqueue(chunk);
    },
    async cancel(reason) {
      try {
        await reader.cancel(reason);
      } finally {
        try { reader.releaseLock(); } catch {}
      }
    },
  });
}

/** @param {Headers} headers */
function headersToHttpMetadata(headers) {
  /** @type {AnyRecord} */
  const out = {};
  for (const [field, header] of R2_HTTP_METADATA_FIELDS) {
    out[field] = headers.get(header) || undefined;
  }
  const cacheExpiry = r2CacheExpiryFromHeaders(headers, { canonical: true });
  if (headers.has("expires") && cacheExpiry === undefined) {
    throw new TypeError("R2 httpMetadata Expires header must be canonical IMF-fixdate");
  }
  out.cacheExpiry = cacheExpiry;
  return out;
}

/** @param {unknown} input */
function normalizeHttpMetadata(input) {
  if (input == null) return undefined;
  if (input instanceof Headers) return headersToHttpMetadata(input);
  if (typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("R2 httpMetadata must be an object or Headers");
  }
  /** @type {AnyRecord} */
  const out = {};
  const record = /** @type {AnyRecord} */ (input);
  for (const [field] of R2_HTTP_METADATA_FIELDS) {
    if (record[field] != null) out[field] = String(record[field]);
  }
  if (record.cacheExpiry != null) {
    const ms = record.cacheExpiry instanceof Date
      ? record.cacheExpiry.getTime()
      : dateFromUnknown(record.cacheExpiry).getTime();
    if (!Number.isFinite(ms)) throw new TypeError("R2 httpMetadata.cacheExpiry must be a Date");
    out.cacheExpiry = ms;
  }
  return out;
}

/** @param {unknown} input */
function normalizeCustomMetadata(input) {
  if (input == null) return undefined;
  if (typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("R2 customMetadata must be an object");
  }
  /** @type {Record<string, string>} */
  const out = {};
  for (const [key, value] of Object.entries(input)) {
    if (value != null) out[key] = String(value);
  }
  return out;
}

/** @param {unknown} range */
function normalizeRange(range) {
  if (range == null) return undefined;
  if (range instanceof Headers) {
    const header = range.get("range");
    return header ? { header } : undefined;
  }
  if (typeof range !== "object" || Array.isArray(range)) {
    throw new TypeError("R2 range must be an object or Headers");
  }
  /** @type {AnyRecord} */
  const out = {};
  const record = /** @type {AnyRecord} */ (range);
  for (const key of ["offset", "length", "suffix"]) {
    if (record[key] != null) out[key] = Number(record[key]);
  }
  return out;
}

/** @param {unknown} onlyIf */
function normalizeOnlyIf(onlyIf) {
  if (onlyIf == null) return undefined;
  if (onlyIf instanceof Headers) {
    /** @type {AnyRecord} */
    const out = {};
    for (const [header, target] of [
      ["if-match", "etagMatches"],
      ["if-none-match", "etagDoesNotMatch"],
      ["if-unmodified-since", "uploadedBefore"],
      ["if-modified-since", "uploadedAfter"],
    ]) {
      const value = onlyIf.get(header);
      if (value) out[target] = value;
    }
    return out;
  }
  if (typeof onlyIf !== "object" || Array.isArray(onlyIf)) {
    throw new TypeError("R2 onlyIf must be an object or Headers");
  }
  const input = /** @type {AnyRecord} */ (onlyIf);
  /** @type {AnyRecord} */
  const out = {};
  if (input.etagMatches != null) {
    out.etagMatches = Array.isArray(input.etagMatches)
      ? input.etagMatches.map(String)
      : String(input.etagMatches);
  }
  if (input.etagDoesNotMatch != null) {
    out.etagDoesNotMatch = Array.isArray(input.etagDoesNotMatch)
      ? input.etagDoesNotMatch.map(String)
      : String(input.etagDoesNotMatch);
  }
  if (input.uploadedBefore != null) out.uploadedBefore = dateFromUnknown(input.uploadedBefore).toUTCString();
  if (input.uploadedAfter != null) out.uploadedAfter = dateFromUnknown(input.uploadedAfter).toUTCString();
  return out;
}

/** @param {AnyRecord} [options] */
function normalizeGetOptions(options = {}) {
  return {
    range: normalizeRange(options.range),
    onlyIf: normalizeOnlyIf(options.onlyIf),
  };
}

/** @param {AnyRecord} [options] */
function normalizePutOptions(options = {}) {
  const onlyIf = options.onlyIf !== null && typeof options.onlyIf === "object" && !Array.isArray(options.onlyIf)
    ? /** @type {AnyRecord} */ (options.onlyIf)
    : {};
  if (options.ssecKey != null || options.md5 != null || options.sha1 != null ||
      options.sha256 != null || options.sha384 != null || options.sha512 != null) {
    throw new TypeError("R2 put: checksums and SSE-C are not supported by WDL R2 yet");
  }
  if (onlyIf.uploadedBefore != null || onlyIf.uploadedAfter != null) {
    throw new TypeError(
      "WDL R2 put({onlyIf}) only supports etag-based conditions; " +
      "uploadedBefore/uploadedAfter are not supported yet"
    );
  }
  return {
    httpMetadata: normalizeHttpMetadata(options.httpMetadata),
    customMetadata: normalizeCustomMetadata(options.customMetadata),
    storageClass: options.storageClass == null ? undefined : String(options.storageClass),
    onlyIf: normalizeOnlyIf(options.onlyIf),
  };
}

/** @returns {never} */
function unsupportedMultipartUpload() {
  throw new TypeError("WDL R2 does not support multipart upload yet");
}

/** @param {AnyRecord} meta */
function metaWithDates(meta) {
  return {
    ...meta,
    uploaded: dateFromUnknown(meta.uploaded || Date.now()),
    httpMetadata: meta.httpMetadata || {},
    customMetadata: meta.customMetadata || {},
    checksums: meta.checksums || {},
  };
}

/** @type {WeakMap<R2Bucket, R2BucketState>} */
const bucketState = new WeakMap();

/** @param {WeakMap<object, unknown>} map @param {object} key */
function weakMapGet(map, key) {
  return intrinsicReflectApply(intrinsicWeakMapGet, map, [key]);
}

/** @param {WeakMap<object, unknown>} map @param {object} key @param {unknown} value */
function weakMapSet(map, key, value) {
  intrinsicReflectApply(intrinsicWeakMapSet, map, [key, value]);
}

/** @param {R2Bucket} bucket */
function bucketRequestMeta(bucket) {
  const state = /** @type {R2BucketState | undefined} */ (weakMapGet(bucketState, bucket));
  if (!state) return {};
  const requestId = requestIdFromOptions(state.requestIdOptions);
  return requestId ? { requestId } : {};
}

export class R2Object {
  /** @type {string} */
  key = "";
  /** @type {string | undefined} */
  version;
  /** @type {number | undefined} */
  size;
  /** @type {string | undefined} */
  etag;
  /** @type {string | undefined} */
  httpEtag;
  /** @type {Date} */
  uploaded = new Date(0);
  /** @type {AnyRecord} */
  httpMetadata = {};
  /** @type {AnyRecord} */
  customMetadata = {};
  /** @type {AnyRecord} */
  checksums = {};
  /** @type {AnyRecord | undefined} */
  range;
  /** @type {string | undefined} */
  storageClass;

  /** @param {AnyRecord} meta */
  constructor(meta) {
    Object.assign(this, metaWithDates(meta));
  }

  /** @param {Headers} headers */
  writeHttpMetadata(headers) {
    const h = this.httpMetadata || {};
    for (const [field, header] of R2_HTTP_METADATA_FIELDS) {
      const value = stringOrUndefined(h[field]);
      if (value) headers.set(header, value);
    }
    setR2CacheExpiryHeader(headers, h.cacheExpiry);
  }
}

export class R2ObjectBody extends R2Object {
  /** @type {ReadableStream<Uint8Array>} */
  body;
  /** @type {boolean} */
  bodyUsed;

  /** @param {AnyRecord} meta @param {ReadableStream<Uint8Array>} body */
  constructor(meta, body) {
    super(meta);
    this.body = cappedReadableStream(body, "get");
    this.bodyUsed = false;
  }

  takeBody() {
    if (this.bodyUsed) {
      throw new TypeError("Body has already been used. It can only be used once.");
    }
    this.bodyUsed = true;
    return this.body;
  }

  async bytes() {
    return readStreamWithLimit(this.takeBody(), "get");
  }

  async arrayBuffer() {
    return bytesToArrayBuffer(await this.bytes());
  }

  async text() {
    return utf8Decoder.decode(await this.bytes());
  }

  async json() {
    return JSON.parse(await this.text());
  }

  async blob() {
    return new Blob([await this.arrayBuffer()], {
      type: stringOrUndefined(this.httpMetadata?.contentType) || "",
    });
  }
}

/** @param {unknown} value */
async function valueToBytes(value) {
  if (value == null) return new Uint8Array(0);
  if (typeof value === "string") return utf8Encoder.encode(value);
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof Blob) {
    return readStreamWithLimit(value.stream(), "put");
  }
  if (value instanceof ReadableStream) {
    // WDL R2 does not implement multipart yet: stream PUT is buffered locally,
    // capped at 25 MiB, then sent as one S3 PUT.
    return readStreamWithLimit(value, "put");
  }
  throw new TypeError(
    "R2 put: value must be string | ArrayBuffer | typed array | Blob | ReadableStream"
  );
}

export class R2Bucket {
  /** @param {R2Stub} stub @param {{ requestIdProvider?: () => string | null, requestId?: string | null }} [options] */
  constructor(stub, options = {}) {
    // Provider is used by class-style entrypoints where the same env wrapper
    // can serve different fetch/queue/scheduled invocations with different ids.
    weakMapSet(bucketState, this, {
      stub,
      requestIdOptions: options,
    });
  }

  /** @param {string} key */
  async head(key) {
    const { stub } = /** @type {R2BucketState} */ (weakMapGet(bucketState, this));
    if (typeof stub.head !== "function") throw new TypeError("R2 stub head is not configured");
    const meta = await stub.head(normalizeR2ObjectKey(key), bucketRequestMeta(this));
    return meta ? new R2Object(meta) : null;
  }

  /** @param {string} key @param {AnyRecord} [options] */
  async get(key, options) {
    const { stub } = /** @type {R2BucketState} */ (weakMapGet(bucketState, this));
    if (typeof stub.get !== "function") throw new TypeError("R2 stub get is not configured");
    const result = await stub.get(
      normalizeR2ObjectKey(key),
      normalizeGetOptions(options),
      bucketRequestMeta(this)
    );
    if (!result) return null;
    if (!result.body) return new R2Object(result.meta);
    return new R2ObjectBody(result.meta, result.body);
  }

  /** @param {string} key @param {unknown} value @param {AnyRecord} [options] */
  async put(key, value, options) {
    const normalizedOptions = normalizePutOptions(options);
    const bytes = await valueToBytes(value);
    assertR2BufferSize(bytes.byteLength, "put");
    const { stub } = /** @type {R2BucketState} */ (weakMapGet(bucketState, this));
    if (typeof stub.put !== "function") throw new TypeError("R2 stub put is not configured");
    const meta = await stub.put(
      normalizeR2ObjectKey(key),
      bytes,
      normalizedOptions,
      bucketRequestMeta(this)
    );
    return meta ? new R2Object(meta) : null;
  }

  /** @param {unknown[]} _args */
  createMultipartUpload(..._args) {
    unsupportedMultipartUpload();
  }

  /** @param {unknown[]} _args */
  resumeMultipartUpload(..._args) {
    unsupportedMultipartUpload();
  }

  /** @param {string | string[]} keys */
  async delete(keys) {
    const { stub } = /** @type {R2BucketState} */ (weakMapGet(bucketState, this));
    if (typeof stub.delete !== "function") throw new TypeError("R2 stub delete is not configured");
    if (Array.isArray(keys)) {
      await stub.delete(
        keys.map((key) => normalizeR2ObjectKey(key)),
        bucketRequestMeta(this)
      );
      return;
    }
    await stub.delete(normalizeR2ObjectKey(keys), bucketRequestMeta(this));
  }

  /** @param {AnyRecord} [options] */
  async list(options = {}) {
    const { stub } = /** @type {R2BucketState} */ (weakMapGet(bucketState, this));
    if (typeof stub.list !== "function") throw new TypeError("R2 stub list is not configured");
    const out = await stub.list({
      prefix: options.prefix == null ? undefined : String(options.prefix),
      delimiter: options.delimiter == null ? undefined : String(options.delimiter),
      cursor: options.cursor == null ? undefined : String(options.cursor),
      startAfter: options.startAfter == null ? undefined : String(options.startAfter),
      limit: normalizeR2ListLimit(options.limit),
      include: Array.isArray(options.include) ? options.include.map(String) : undefined,
    }, bucketRequestMeta(this));
    const objects = /** @type {AnyRecord[]} */ (out.objects || []);
    return {
      ...out,
      objects: objects.map((meta) => new R2Object(meta)),
    };
  }
}
