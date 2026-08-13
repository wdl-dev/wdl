export const R2_OBJECT_MAX_BUFFER_BYTES = 25 * 1024 * 1024;
export const R2_LIST_LIMIT_MAX = 1000;
// Keep in sync with shared/ns-pattern.js. This file is embedded into loaded
// workers as _wdl-r2-utils.js, so it must stay standalone.
export const R2_BUCKET_NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;
export const R2_HTTP_METADATA_FIELDS = Object.freeze([
  Object.freeze(["contentType", "content-type"]),
  Object.freeze(["contentLanguage", "content-language"]),
  Object.freeze(["contentDisposition", "content-disposition"]),
  Object.freeze(["contentEncoding", "content-encoding"]),
  Object.freeze(["cacheControl", "cache-control"]),
]);
const R2_IMF_FIXDATE_RE = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT$/;
const IntrinsicUint8Array = Uint8Array;
const uint8ArrayPrototype = IntrinsicUint8Array.prototype;
const typedArrayPrototype = Object.getPrototypeOf(uint8ArrayPrototype);
const arrayBufferPrototype = ArrayBuffer.prototype;
const dataViewPrototype = DataView.prototype;
const intrinsicArrayBufferIsView = ArrayBuffer.isView;
const intrinsicReflectApply = Reflect.apply;
const intrinsicTypedArrayAt = typedArrayPrototype.at;
const intrinsicTypedArrayTagGet = prototypeGetter(
  typedArrayPrototype,
  Symbol.toStringTag
);
const intrinsicArrayBufferByteLengthGet = prototypeGetter(arrayBufferPrototype, "byteLength");
const intrinsicTypedArrayBufferGet = prototypeGetter(typedArrayPrototype, "buffer");
const intrinsicTypedArrayByteOffsetGet = prototypeGetter(typedArrayPrototype, "byteOffset");
const intrinsicTypedArrayByteLengthGet = prototypeGetter(typedArrayPrototype, "byteLength");
const intrinsicDataViewBufferGet = prototypeGetter(dataViewPrototype, "buffer");
const intrinsicDataViewByteOffsetGet = prototypeGetter(dataViewPrototype, "byteOffset");
const intrinsicDataViewByteLengthGet = prototypeGetter(dataViewPrototype, "byteLength");
/** @type {never[]} */
const noArguments = [];
const atZeroArguments = [0];

/** @param {object} prototype @param {PropertyKey} name */
function prototypeGetter(prototype, name) {
  const getter = Object.getOwnPropertyDescriptor(prototype, name)?.get;
  if (!getter) throw new TypeError(`missing intrinsic getter ${String(name)}`);
  return getter;
}

/**
 * Normalize an R2 BufferSource without consulting overridable view properties.
 * Returns null for values that are not an ArrayBuffer or ArrayBufferView.
 *
 * @param {unknown} value
 * @returns {Uint8Array | null}
 */
export function r2BufferSourceBytes(value) {
  if (!intrinsicArrayBufferIsView(value)) {
    try {
      intrinsicReflectApply(intrinsicArrayBufferByteLengthGet, value, noArguments);
    } catch {
      return null;
    }
    return new IntrinsicUint8Array(/** @type {ArrayBuffer} */ (value));
  }

  // Preserve zero-copy only for the real Uint8Array element kind. Prototype
  // identity is tenant-mutable and cannot serve as a typed-array brand check.
  const typedArrayTag = intrinsicReflectApply(
    intrinsicTypedArrayTagGet,
    value,
    noArguments
  );
  if (typedArrayTag === "Uint8Array") {
    intrinsicReflectApply(intrinsicTypedArrayAt, value, atZeroArguments);
    return /** @type {Uint8Array} */ (value);
  }

  let buffer;
  try {
    buffer = intrinsicReflectApply(intrinsicTypedArrayBufferGet, value, noArguments);
  } catch {
    buffer = intrinsicReflectApply(intrinsicDataViewBufferGet, value, noArguments);
    const byteOffset = intrinsicReflectApply(
      intrinsicDataViewByteOffsetGet,
      value,
      noArguments
    );
    const byteLength = intrinsicReflectApply(
      intrinsicDataViewByteLengthGet,
      value,
      noArguments
    );
    return new IntrinsicUint8Array(buffer, byteOffset, byteLength);
  }

  // Fixed-length typed arrays expose zero public bounds after their resizable
  // buffer shrinks out of range. The captured intrinsic rejects that state.
  intrinsicReflectApply(intrinsicTypedArrayAt, value, atZeroArguments);
  const byteOffset = intrinsicReflectApply(
    intrinsicTypedArrayByteOffsetGet,
    value,
    noArguments
  );
  const byteLength = intrinsicReflectApply(
    intrinsicTypedArrayByteLengthGet,
    value,
    noArguments
  );
  return new IntrinsicUint8Array(buffer, byteOffset, byteLength);
}

/** @param {Uint8Array} value */
export function r2Uint8ArrayByteLength(value) {
  return /** @type {number} */ (
    intrinsicReflectApply(intrinsicTypedArrayByteLengthGet, value, noArguments)
  );
}

/** @param {Uint8Array} value @param {number} byteLength */
export function r2Uint8ArrayIsFullBuffer(value, byteLength) {
  const buffer = intrinsicReflectApply(intrinsicTypedArrayBufferGet, value, noArguments);
  const byteOffset = intrinsicReflectApply(
    intrinsicTypedArrayByteOffsetGet,
    value,
    noArguments
  );
  let bufferByteLength;
  try {
    bufferByteLength = intrinsicReflectApply(
      intrinsicArrayBufferByteLengthGet,
      buffer,
      noArguments
    );
  } catch {
    // A SharedArrayBuffer-backed view is valid input; use the copying path when
    // the captured ArrayBuffer getter does not accept the backing-buffer brand.
    return false;
  }
  return byteOffset === 0 && byteLength === bufferByteLength;
}

/** @param {Headers} headers @param {{ canonical?: boolean }} [options] */
export function r2CacheExpiryFromHeaders(headers, { canonical = false } = {}) {
  const expires = headers.get("expires");
  if (!expires) return undefined;
  const ms = new Date(expires).getTime();
  if (canonical && (
    !R2_IMF_FIXDATE_RE.test(expires) ||
    !Number.isFinite(ms) ||
    new Date(ms).toUTCString() !== expires
  )) return undefined;
  return Number.isFinite(ms) ? ms : undefined;
}

/** @param {Headers} headers @param {unknown} value */
export function setR2CacheExpiryHeader(headers, value) {
  if (value == null) return;
  let date;
  if (value instanceof Date || typeof value === "number") {
    date = new Date(value);
  } else if (typeof value === "string" && value.trim() !== "") {
    const timestamp = Number(value);
    date = new Date(Number.isFinite(timestamp) ? timestamp : value);
  } else {
    throw new TypeError("R2 httpMetadata.cacheExpiry must be a valid Date or timestamp");
  }
  if (!Number.isFinite(date.getTime())) {
    throw new TypeError("R2 httpMetadata.cacheExpiry must be a valid Date or timestamp");
  }
  headers.set("expires", date.toUTCString());
}

/**
 * @param {unknown} bucketName
 * @returns {asserts bucketName is string}
 */
export function validateR2BucketName(bucketName) {
  if (typeof bucketName !== "string" || !R2_BUCKET_NAME_RE.test(bucketName)) {
    throw new Error(
      `r2 bucket_name must match ${R2_BUCKET_NAME_RE}, got ${JSON.stringify(bucketName)}`
    );
  }
}

/**
 * @param {unknown} key
 * @returns {string}
 */
export function normalizeR2ObjectKey(key) {
  if (typeof key !== "string" || key.length === 0) {
    throw new TypeError("R2 key must be a non-empty string");
  }
  if (key.split("/").some((segment) => segment === "." || segment === "..")) {
    throw new TypeError("R2 key must not contain . or .. path segments");
  }
  return key;
}

/**
 * @typedef {{ ns: string, bucketName: string }} R2BindingProps
 */

/**
 * @param {R2BindingProps} props
 * @returns {string}
 */
export function r2PhysicalPrefix({ ns, bucketName }) {
  if (typeof ns !== "string" || !ns) throw new Error("R2 prefix requires ns");
  validateR2BucketName(bucketName);
  return `r2/${ns}/${bucketName}/`;
}

/**
 * @param {R2BindingProps} props
 * @param {unknown} key
 * @returns {string}
 */
export function r2PhysicalKey(props, key) {
  return `${r2PhysicalPrefix(props)}${normalizeR2ObjectKey(key)}`;
}

/**
 * @param {R2BindingProps} props
 * @param {unknown} physicalKey
 * @returns {string}
 */
export function stripR2PhysicalPrefix(props, physicalKey) {
  return stripR2PhysicalPrefixWith(r2PhysicalPrefix(props), physicalKey);
}

/**
 * @param {string} prefix
 * @param {unknown} physicalKey
 * @returns {string}
 */
export function stripR2PhysicalPrefixWith(prefix, physicalKey) {
  if (typeof physicalKey !== "string" || !physicalKey.startsWith(prefix)) {
    throw new Error("R2 backend returned an object outside the binding prefix");
  }
  return physicalKey.slice(prefix.length);
}

/**
 * @param {unknown} key
 * @returns {string}
 */
export function encodeS3KeyPath(key) {
  return String(key).split("/").map((segment) => encodeURIComponent(segment)).join("/");
}

/** @param {unknown} value */
function encodeS3QueryComponent(value) {
  return encodeURIComponent(String(value)).replace(/[!'()*]/g, (ch) =>
    `%${ch.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

/**
 * Keep this implementation in sync with shared/s3-query.js. runtime/r2-utils.js
 * is injected as loaded-worker source, so it must stay self-contained.
 *
 * @param {Record<string, unknown>} params
 * @returns {string}
 */
export function encodeS3Query(params) {
  return Object.entries(params)
    .filter(([, value]) => value != null && value !== "")
    .map(([key, value]) => `${encodeS3QueryComponent(key)}=${encodeS3QueryComponent(value)}`)
    .join("&");
}

/**
 * @param {unknown} size
 * @param {string} operation
 */
export function assertR2BufferSize(size, operation) {
  if (typeof size !== "number" || !Number.isFinite(size) || size < 0) {
    throw new Error(`R2 ${operation}: invalid byte length ${size}`);
  }
  if (size > R2_OBJECT_MAX_BUFFER_BYTES) {
    throw new Error(
      `R2 ${operation}: object is ${size} bytes, exceeds the 25 MiB WDL R2 limit ` +
        "(multipart upload is not supported yet)"
    );
  }
}

/**
 * @param {unknown} limit
 * @returns {number | undefined}
 */
export function normalizeR2ListLimit(limit) {
  if (limit == null) return undefined;
  let n;
  if (typeof limit === "number") {
    n = limit;
  } else if (typeof limit === "string" && limit.trim() !== "") {
    n = Number(limit);
  } else {
    throw new TypeError(`R2 list: limit must be an integer in [1, ${R2_LIST_LIMIT_MAX}]`);
  }
  if (!Number.isInteger(n) || n < 1 || n > R2_LIST_LIMIT_MAX) {
    throw new TypeError(`R2 list: limit must be an integer in [1, ${R2_LIST_LIMIT_MAX}]`);
  }
  return n;
}

/**
 * @param {Headers} headers
 * @param {number} [fallbackSize]
 * @returns {{ size: number, range?: { offset: number, length: number } }}
 */
export function r2RangeAndSizeFromHeaders(headers, fallbackSize = 0) {
  const contentLength = Number(headers.get("content-length"));
  const contentRange = headers.get("content-range");
  if (contentRange) {
    const m = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(contentRange);
    if (m) {
      const start = Number(m[1]);
      const end = Number(m[2]);
      const total = m[3] === "*" ? NaN : Number(m[3]);
      return {
        size: Number.isFinite(total)
          ? total
          : Number.isFinite(contentLength) ? contentLength : fallbackSize,
        range: { offset: start, length: end - start + 1 },
      };
    }
  }
  return {
    size: Number.isFinite(contentLength) ? contentLength : fallbackSize,
    range: undefined,
  };
}
