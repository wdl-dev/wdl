import { encodeD1Transport } from "shared-d1-transport";
import {
  d1QuerySuccessHeaders,
  D1_QUERY_NATIVE_VALUE_ENCODING,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  encodeD1QueryResponse,
} from "shared-d1-query-wire";
import { jsonErrorWith } from "shared-respond";

/**
 * @param {unknown} data
 * @param {ResponseInit} [init]
 */
export function json(data, init = {}) {
  return new Response(JSON.stringify(encodeD1Transport(data)), {
    ...init,
    headers: { "content-type": "application/json", ...(init.headers || {}) },
  });
}

/**
 * @param {Uint8Array<ArrayBuffer>} bytes
 * @param {ResponseInit} [init]
 */
export function d1QueryBytesResponse(bytes, init = {}) {
  return new Response(bytes, {
    ...init,
    headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE, ...(init.headers || {}) },
  });
}

/**
 * @param {unknown} data
 * @param {ResponseInit} [init]
 */
export function d1QueryResponse(data, init = {}) {
  return d1QueryBytesResponse(encodeD1QueryResponse(data), init);
}

/**
 * @param {unknown} data
 * @param {ResponseInit & { changedDb?: boolean }} [init]
 */
export function d1QuerySuccessResponse(data, init = {}) {
  const { changedDb = false, ...responseInit } = init;
  const headers = Object.fromEntries(new Headers(responseInit.headers));
  Object.assign(headers, d1QuerySuccessHeaders(changedDb, D1_QUERY_NATIVE_VALUE_ENCODING));
  return d1QueryResponse(data, { ...responseInit, headers });
}

/**
 * @param {number} status
 * @param {string} error
 * @param {string} message
 * @param {Record<string, unknown>} [extra]
 */
export function jsonError(status, error, message, extra = {}) {
  return jsonErrorWith(json, status, error, message, extra);
}
