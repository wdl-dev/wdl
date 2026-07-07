/**
 * @param {unknown} value
 * @returns {string}
 */
function encodeS3QueryComponent(value) {
  return encodeURIComponent(String(value)).replace(/[!'()*]/g, (ch) =>
    `%${ch.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

/**
 * S3 SigV4 query strings use percent-encoded bytes; URLSearchParams' form
 * encoding turns spaces into '+', which is not the canonical S3 query form.
 * Keep this implementation in sync with runtime/r2-utils.js; that file is
 * embedded as loaded-worker source and cannot import shared modules.
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
