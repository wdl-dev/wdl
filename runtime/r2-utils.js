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
  const prefix = r2PhysicalPrefix(props);
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
