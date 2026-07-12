import {
  R2_HTTP_METADATA_FIELDS,
  r2CacheExpiryFromHeaders,
  r2RangeAndSizeFromHeaders,
  setR2CacheExpiryHeader,
} from "runtime-r2-utils";

/**
 * @typedef {{ offset?: number, length?: number, suffix?: number, header?: string }} R2RangeOptions
 * @typedef {{
 *   [key: string]: unknown,
 *   contentType?: unknown,
 *   contentLanguage?: unknown,
 *   contentDisposition?: unknown,
 *   contentEncoding?: unknown,
 *   cacheControl?: unknown,
 *   cacheExpiry?: unknown,
 *   etagMatches?: unknown,
 *   etagDoesNotMatch?: unknown,
 *   uploadedBefore?: string,
 *   uploadedAfter?: string,
 *   httpMetadata?: Record<string, unknown>,
 *   customMetadata?: Record<string, unknown>,
 *   storageClass?: unknown,
 *   onlyIf?: Record<string, unknown>,
 *   range?: R2RangeOptions,
 *   prefix?: string,
 *   delimiter?: string,
 *   cursor?: string,
 *   startAfter?: string,
 *   limit?: unknown,
 *   include?: string[],
 * }} R2Options
 * @typedef {{
 *   key: string,
 *   version: string,
 *   size: number,
 *   etag: string,
 *   httpEtag: string,
 *   uploaded: number,
 *   httpMetadata: Record<string, unknown>,
 *   customMetadata: Record<string, string> | Record<string, unknown>,
 *   range?: unknown,
 *   checksums: Record<string, unknown>,
 *   storageClass: string,
 * }} R2Meta
 * @typedef {{ requestId?: unknown }} R2RequestMeta
 */

/** @param {unknown} etag */
export function stripEtag(etag) {
  if (!etag) return "";
  const s = String(etag);
  return s.startsWith('"') && s.endsWith('"') ? s.slice(1, -1) : s;
}

/** @param {unknown} value */
function etagForHttp(value) {
  const s = String(value);
  if (s === "*" || s.startsWith('"') || s.startsWith("W/\"")) return s;
  return `"${s}"`;
}

/** @param {unknown} value */
function etagConditionForHttp(value) {
  if (Array.isArray(value)) return value.map(etagForHttp).join(", ");
  return etagForHttp(value);
}

/**
 * @param {R2RequestMeta} [requestMeta]
 * @returns {Headers}
 */
export function headersWithRequestId(requestMeta = {}) {
  const headers = new Headers();
  if (requestMeta.requestId) headers.set("x-request-id", String(requestMeta.requestId));
  return headers;
}

/**
 * @param {Headers} headers
 * @returns {Record<string, unknown>}
 */
function httpMetadataFromHeaders(headers) {
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const [field, header] of R2_HTTP_METADATA_FIELDS) {
    const value = headers.get(header);
    if (value) out[field] = value;
  }
  const cacheExpiry = r2CacheExpiryFromHeaders(headers);
  if (cacheExpiry != null) out.cacheExpiry = cacheExpiry;
  return out;
}

/**
 * @param {Headers} headers
 * @returns {Record<string, string>}
 */
function customMetadataFromHeaders(headers) {
  /** @type {Record<string, string>} */
  const out = {};
  for (const [key, value] of headers) {
    if (key.startsWith("x-amz-meta-")) {
      Object.defineProperty(out, key.slice("x-amz-meta-".length), {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
  }
  return out;
}

/**
 * @param {string} userKey
 * @param {Headers} headers
 * @param {number} [fallbackSize]
 * @returns {R2Meta}
 */
export function metaFromHeaders(userKey, headers, fallbackSize = 0) {
  const { size, range } = r2RangeAndSizeFromHeaders(headers, fallbackSize);
  const lastModified = headers.get("last-modified");
  const uploaded = lastModified ? new Date(lastModified).getTime() : Date.now();
  const httpEtag = headers.get("etag") || "";
  return {
    key: userKey,
    version: headers.get("x-amz-version-id") || "",
    size,
    etag: stripEtag(httpEtag),
    httpEtag,
    uploaded: Number.isFinite(uploaded) ? uploaded : Date.now(),
    httpMetadata: httpMetadataFromHeaders(headers),
    customMetadata: customMetadataFromHeaders(headers),
    range,
    checksums: {},
    storageClass: headers.get("x-amz-storage-class") || "Standard",
  };
}

/**
 * @param {string} userKey
 * @param {Headers} headers
 * @param {number} bodySize
 * @param {R2Options} [options]
 * @returns {R2Meta}
 */
export function metaFromPutResponse(userKey, headers, bodySize, options = {}) {
  // S3 PUT responses usually omit Last-Modified, so uploaded falls back to
  // local runtime time. A later head()/get() may report the backend timestamp.
  const meta = metaFromHeaders(userKey, headers, bodySize);
  return {
    ...meta,
    size: bodySize,
    httpMetadata: options.httpMetadata !== undefined ? options.httpMetadata : meta.httpMetadata,
    customMetadata: options.customMetadata !== undefined
      ? options.customMetadata
      : meta.customMetadata,
    storageClass: options.storageClass !== undefined ? String(options.storageClass) : meta.storageClass,
  };
}

/**
 * @param {Headers} headers
 * @param {R2Options} [httpMetadata]
 */
export function applyHttpMetadata(headers, httpMetadata = {}) {
  for (const [field, header] of R2_HTTP_METADATA_FIELDS) {
    if (httpMetadata[field]) headers.set(header, String(httpMetadata[field]));
  }
  setR2CacheExpiryHeader(headers, httpMetadata.cacheExpiry);
}

/**
 * @param {Headers} headers
 * @param {Record<string, unknown>} [customMetadata]
 */
export function applyCustomMetadata(headers, customMetadata = {}) {
  for (const [key, value] of Object.entries(customMetadata)) {
    if (!/^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(key)) {
      throw new TypeError(`R2 customMetadata key ${JSON.stringify(key)} is not HTTP-header safe`);
    }
    headers.set(`x-amz-meta-${key}`, String(value));
  }
}

/**
 * @param {Headers} headers
 * @param {R2Options} [onlyIf]
 */
export function applyOnlyIf(headers, onlyIf = {}) {
  if (onlyIf.etagMatches) headers.set("if-match", etagConditionForHttp(onlyIf.etagMatches));
  if (onlyIf.etagDoesNotMatch) {
    headers.set("if-none-match", etagConditionForHttp(onlyIf.etagDoesNotMatch));
  }
  if (onlyIf.uploadedBefore) headers.set("if-unmodified-since", onlyIf.uploadedBefore);
  if (onlyIf.uploadedAfter) headers.set("if-modified-since", onlyIf.uploadedAfter);
}

/**
 * @param {Headers} headers
 * @param {R2Options} [options]
 */
export function applyGetOptions(headers, options = {}) {
  const range = options.range || {};
  if (range.header) {
    headers.set("range", range.header);
  } else if (Number.isFinite(range.suffix)) {
    headers.set("range", `bytes=-${range.suffix}`);
  } else if (Number.isFinite(range.offset) || Number.isFinite(range.length)) {
    const start = Number.isFinite(range.offset) ? Number(range.offset) : 0;
    const end = Number.isFinite(range.length) ? start + Number(range.length) - 1 : "";
    headers.set("range", `bytes=${start}-${end}`);
  }

  applyOnlyIf(headers, options.onlyIf);
}
