import {
  CLASS_NAME_RE,
  METHOD_NAME_RE,
  HEADER_NAME_RE,
  STORAGE_ID_RE,
  HOST_ID_RE,
  DO_HOST_SHARD_COUNT,
  MAX_ID_BYTES,
} from "do-runtime-protocol-wire-grammar";
import { DoRuntimeError, doErrorResponse } from "do-runtime-protocol-errors";
import {
  hostIdForObject,
  isWellFormedUnicodeString,
  shardForObjectName,
} from "do-runtime-protocol-identity";
import { formatWorkerId } from "shared-worker-id";
import {
  BodyTooLargeError,
  readBoundedBytes as readRequestBoundedBytes,
  readBoundedText as readRequestBoundedText,
} from "shared-bounded-body";
import { INTERNAL_AUTH_HEADER } from "shared-internal-auth";
import { isValidRuntimeLoadNs, WORKER_NAME_RE } from "shared-ns-pattern";
import { parseVersion } from "shared-worker-contract";

export { DO_HOST_SHARD_COUNT } from "do-runtime-protocol-wire-grammar";
export { DoRuntimeError, doErrorResponse } from "do-runtime-protocol-errors";
export {
  hostIdForObject,
  hostIdForShard,
  isWellFormedUnicodeString,
  shardForObjectName,
} from "do-runtime-protocol-identity";

export const DO_OWNERSHIP_CODE = Object.freeze({
  OWNER_CLAIM_RACED: "owner_claim_raced",
  OWNER_FENCE_MISSING: "owner_fence_missing",
  STALE_OWNER_GENERATION: "stale_owner_generation",
  OWNER_LEASE_EXPIRED: "owner_lease_expired",
  STALE_OWNER_STORAGE: "stale_owner_storage",
  OWNER_LEASE_TOO_SHORT: "owner_lease_too_short",
  OWNER_RENEW_RACED: "owner_renew_raced",
  OWNER_RELEASE_RACED: "owner_release_raced",
  OWNER_UNAVAILABLE: "owner_unavailable",
  OWNER_ENDPOINT_MISSING: "owner_endpoint_missing",
  FORWARD_HOP_EXHAUSTED: "forward_hop_exhausted",
  TASK_DRAINING: "task_draining",
});
export const DO_OWNERSHIP_ERROR_CONTROL_HEADER = "x-wdl-do-ownership-error";
/** @type {Set<string>} */
const DO_OWNERSHIP_CODES = new Set(Object.values(DO_OWNERSHIP_CODE));

/** @param {unknown} err */
export function doPlatformErrorResponse(err) {
  const response = doErrorResponse(err);
  if (err instanceof DoRuntimeError && DO_OWNERSHIP_CODES.has(err.code)) {
    response.headers.set(DO_OWNERSHIP_ERROR_CONTROL_HEADER, err.code);
  }
  return response;
}

const MAX_REQUEST_BODY_BYTES = 1024 * 1024;
const MAX_INVOKE_ENVELOPE_BYTES = 2 * 1024 * 1024;
const MAX_REQUEST_HEADER_COUNT = 128;
const MAX_REQUEST_HEADER_BYTES = 64 * 1024;
export const DO_INVOKE_CONTENT_TYPE = "application/vnd.wdl.do-invoke";
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const ALARM_INTERNAL_URL = "https://do.internal/__wdl_alarm";
const ALARM_INTERNAL_HEADER = "x-wdl-do-internal-alarm";
const RPC_INTERNAL_URL = "https://do.internal/__wdl_rpc";
const RPC_INTERNAL_HEADER = "x-wdl-do-internal-rpc";
const CONNECT_HEADERS = {
  ns: "x-wdl-do-ns",
  worker: "x-wdl-do-worker",
  version: "x-wdl-do-version",
  doStorageId: "x-wdl-do-storage-id",
  className: "x-wdl-do-class-name",
  objectName: "x-wdl-do-object-name",
  requestUrl: "x-wdl-do-request-url",
  ownerKey: "x-wdl-do-owner-key",
  ownerTaskId: "x-wdl-do-owner-task-id",
  ownerGeneration: "x-wdl-do-owner-generation",
};
const OWNER_HINT_PROTOCOL_HEADERS = [
  "x-wdl-do-accept-owner-hint",
  "x-wdl-do-owner-key",
  "x-wdl-do-owner-task-id",
  "x-wdl-do-owner-endpoint",
  "x-wdl-do-owner-generation",
  "x-wdl-do-owner-hint",
];
const CONNECT_INTERNAL_HEADER_NAMES = new Set([
  INTERNAL_AUTH_HEADER,
  DO_OWNERSHIP_ERROR_CONTROL_HEADER,
  ...Object.values(CONNECT_HEADERS),
  ...OWNER_HINT_PROTOCOL_HEADERS,
  "x-wdl-do-forwarded",
  "x-wdl-do-hop-count",
  RPC_INTERNAL_HEADER,
]);
const LOCAL_ACTOR_ENVELOPE_HEADER = "x-wdl-do-local-envelope";
const LOCAL_ACTOR_ENVELOPE_MARKER = "binary";

/**
 * @typedef {Record<string, unknown>} JsonRecord
 * @typedef {{ ownerKey: string, taskId: string, generation: number }} OwnerFence
 * @typedef {{ ns: string, worker: string, version: string, doStorageId: string, workerId: string }} BundleSource
 * @typedef {{ className: string, objectName: string }} ObjectTarget
 * @typedef {{ method: string, url: string, headers: Array<[string, string]>, bodyBytes?: Uint8Array, bodyBase64?: undefined, bodyText?: undefined }} RequestSpec
 * @typedef {{ retryCount: number, isRetry: boolean, token?: string }} AlarmInfo
 * @typedef {{ method: string, args: unknown[] }} RpcInfo
 * @typedef {BundleSource & { kind: "fetch", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, request: RequestSpec }} FetchInvoke
 * @typedef {BundleSource & { kind: "alarm", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, alarm: AlarmInfo }} AlarmInvoke
 * @typedef {BundleSource & { kind: "rpc", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, rpc: RpcInfo }} RpcInvoke
 * @typedef {FetchInvoke | AlarmInvoke | RpcInvoke} DoInvoke
 * @typedef {Record<string, unknown> & { request?: Record<string, unknown> & { bodyBytes?: Uint8Array } }} EnvelopeInvoke
 */

/**
 * @param {Request} request
 * @param {{ maxBytes?: number }} [options]
 * @returns {Promise<unknown>}
 */
export async function readJsonBody(request, { maxBytes = MAX_INVOKE_ENVELOPE_BYTES } = {}) {
  const text = await readBoundedText(request, maxBytes);
  try {
    return JSON.parse(text);
  } catch {
    throw new DoRuntimeError(400, "invalid_json", "Request body must be valid JSON");
  }
}

/**
 * @param {unknown} value
 * @returns {string | null}
 */
export function nonEmptyAlarmString(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 * @returns {Promise<Uint8Array>}
 */
async function readBoundedBytes(request, maxBytes) {
  try {
    return await readRequestBoundedBytes(request, maxBytes);
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      throw new DoRuntimeError(413, "request_body_too_large", `Request body exceeds ${maxBytes} bytes`);
    }
    throw err;
  }
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 */
async function readBoundedText(request, maxBytes) {
  try {
    return await readRequestBoundedText(request, maxBytes);
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      throw new DoRuntimeError(413, "request_body_too_large", `Request body exceeds ${maxBytes} bytes`);
    }
    throw err;
  }
}

/** @param {string} value */
function byteLength(value) {
  return utf8Encoder.encode(value).byteLength;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @returns {JsonRecord}
 */
function requireRecord(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new DoRuntimeError(400, "invalid_request", `${field} must be an object`);
  }
  return /** @type {JsonRecord} */ (value);
}

/**
 * @param {unknown} value
 * @param {string} field
 * @param {{ maxBytes?: number, pattern?: RegExp | null }} [options]
 */
function requireString(value, field, { maxBytes = MAX_ID_BYTES, pattern = null } = {}) {
  if (typeof value !== "string" || value.length === 0) {
    throw new DoRuntimeError(400, "invalid_request", `${field} must be a non-empty string`);
  }
  if (!isWellFormedUnicodeString(value)) {
    throw new DoRuntimeError(400, "invalid_request", `${field} must contain well-formed Unicode`);
  }
  if (byteLength(value) > maxBytes) {
    throw new DoRuntimeError(400, "invalid_request", `${field} is too large`);
  }
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) {
      throw new DoRuntimeError(400, "invalid_request", `${field} must not contain control characters`);
    }
  }
  if (pattern && !pattern.test(value)) {
    throw new DoRuntimeError(400, "invalid_request", `${field} is not valid`);
  }
  return value;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @param {number} maxBytes
 */
function requireJsonSerializedSize(value, field, maxBytes) {
  const json = JSON.stringify(value);
  if (byteLength(json) > maxBytes) {
    throw new DoRuntimeError(413, "request_body_too_large", `${field} exceeds ${maxBytes} bytes`);
  }
}

/**
 * @param {unknown} value
 * @param {string} field
 * @param {WeakSet<object>} [seen]
 * @returns {string | null}
 */
function jsonDataError(value, field, seen = new WeakSet()) {
  if (value === null) return null;
  const type = typeof value;
  if (type === "string" || type === "boolean") return null;
  if (type === "number") return Number.isFinite(value) ? null : `${field} must be a finite number`;
  if (type === "bigint" || type === "function" || type === "symbol" || type === "undefined") {
    return `${field} must be JSON data`;
  }
  if (type !== "object") return `${field} must be JSON data`;
  const objectValue = /** @type {Record<string, unknown> | unknown[]} */ (value);
  if (seen.has(objectValue)) return `${field} must not be circular`;
  seen.add(objectValue);
  if (Array.isArray(objectValue)) {
    for (let i = 0; i < objectValue.length; i++) {
      if (!(i in objectValue)) return `${field} must not be sparse`;
      const error = jsonDataError(objectValue[i], `${field}[${i}]`, seen);
      if (error) return error;
    }
    seen.delete(objectValue);
    return null;
  }
  const proto = Object.getPrototypeOf(objectValue);
  if (proto !== Object.prototype && proto !== null) return `${field} must be a plain JSON object`;
  if (Object.getOwnPropertySymbols(objectValue).length > 0) return `${field} must not contain symbol keys`;
  for (const [key, entry] of Object.entries(objectValue)) {
    const error = jsonDataError(entry, `${field}.${key}`, seen);
    if (error) return error;
  }
  seen.delete(objectValue);
  return null;
}

/**
 * @param {unknown} value
 * @param {string} field
 */
function requireJsonData(value, field) {
  const error = jsonDataError(value, field);
  if (error) throw new DoRuntimeError(400, "invalid_request", error);
}

/**
 * @param {unknown} value
 * @returns {Array<[string, string]>}
 */
function normalizeHeaders(value) {
  if (value == null) return [];
  let totalBytes = 0;
  /**
   * @param {unknown} name
   * @param {unknown} entryValue
   */
  const normalize = (name, entryValue) => {
    const pair = normalizeHeaderPair(name, entryValue);
    totalBytes += byteLength(pair[0]) + byteLength(pair[1]);
    if (totalBytes > MAX_REQUEST_HEADER_BYTES) {
      throw new DoRuntimeError(413, "request_body_too_large", `request.headers exceeds ${MAX_REQUEST_HEADER_BYTES} bytes`);
    }
    return pair;
  };
  if (Array.isArray(value)) {
    if (value.length > MAX_REQUEST_HEADER_COUNT) {
      throw new DoRuntimeError(400, "invalid_request", `request.headers must not exceed ${MAX_REQUEST_HEADER_COUNT} entries`);
    }
    return value.map((entry, index) => {
      if (!Array.isArray(entry) || entry.length !== 2) {
        throw new DoRuntimeError(400, "invalid_request", `request.headers[${index}] must be [name, value]`);
      }
      return normalize(entry[0], entry[1]);
    });
  }
  const record = requireRecord(value, "request.headers");
  const entries = Object.entries(record);
  if (entries.length > MAX_REQUEST_HEADER_COUNT) {
    throw new DoRuntimeError(400, "invalid_request", `request.headers must not exceed ${MAX_REQUEST_HEADER_COUNT} entries`);
  }
  return entries.map(([name, headerValue]) => normalize(name, headerValue));
}

/**
 * @param {unknown} name
 * @param {unknown} value
 * @returns {[string, string]}
 */
function normalizeHeaderPair(name, value) {
  const headerName = requireString(name, "request header name", { maxBytes: 128 });
  if (!HEADER_NAME_RE.test(headerName)) {
    throw new DoRuntimeError(400, "invalid_request", `request header name ${JSON.stringify(headerName)} is not valid`);
  }
  if (typeof value !== "string") {
    throw new DoRuntimeError(400, "invalid_request", `request.headers.${headerName} must be a string`);
  }
  if (byteLength(value) > 8192) {
    throw new DoRuntimeError(400, "invalid_request", `request.headers.${headerName} is too large`);
  }
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) {
      throw new DoRuntimeError(400, "invalid_request", `request.headers.${headerName} must not contain control characters`);
    }
  }
  return [headerName, value];
}

/**
 * @param {unknown} value
 * @returns {RequestSpec}
 */
function normalizeRequestSpec(value) {
  const input = value == null ? {} : requireRecord(value, "request");
  const method = String(input.method || "GET").toUpperCase();
  if (!/^[A-Z]+$/.test(method)) {
    throw new DoRuntimeError(400, "invalid_request", "request.method is not valid");
  }
  const url = input.url == null
    ? "https://do.internal/"
    : requireString(input.url, "request.url", { maxBytes: 4096 });
  try {
    new URL(url);
  } catch {
    throw new DoRuntimeError(400, "invalid_request", "request.url is not a valid URL");
  }
  for (const field of ["bodyText", "bodyBase64", "bodyBytes"]) {
    if (input[field] != null) {
      throw new DoRuntimeError(400, "invalid_request", `request.${field} is not supported in invoke metadata`);
    }
  }
  return {
    method,
    url,
    headers: normalizeHeaders(input.headers),
  };
}

/**
 * @param {unknown} value
 * @returns {"fetch" | "alarm" | "rpc"}
 */
function normalizeInvokeKind(value) {
  if (value == null) return "fetch";
  if (value === "fetch" || value === "alarm" || value === "rpc") return value;
  throw new DoRuntimeError(400, "invalid_request", "kind must be fetch, alarm, or rpc");
}

/**
 * @param {unknown} value
 * @returns {AlarmInfo}
 */
function normalizeAlarmInfo(value) {
  const input = value == null ? {} : requireRecord(value, "alarm");
  const retryCount = input.retryCount == null ? 0 : input.retryCount;
  if (
    typeof retryCount !== "number" ||
    !Number.isInteger(retryCount) ||
    retryCount < 0
  ) {
    throw new DoRuntimeError(400, "invalid_request", "alarm.retryCount must be a non-negative integer");
  }
  const token = input.token == null ? undefined : requireString(input.token, "alarm.token");
  return {
    retryCount,
    isRetry: Boolean(input.isRetry ?? retryCount > 0),
    ...(token === undefined ? {} : { token }),
  };
}

/**
 * @param {unknown} value
 * @returns {RpcInfo}
 */
function normalizeRpcInfo(value) {
  const input = requireRecord(value, "rpc");
  const method = requireString(input.method, "rpc.method", {
    maxBytes: 256,
    pattern: METHOD_NAME_RE,
  });
  if (method === "fetch" || method === "alarm") {
    throw new DoRuntimeError(400, "invalid_request", "rpc.method is reserved");
  }
  if (!Array.isArray(input.args)) {
    throw new DoRuntimeError(400, "invalid_request", "rpc.args must be an array");
  }
  requireJsonData(input.args, "rpc.args");
  requireJsonSerializedSize(input.args, "rpc.args", MAX_REQUEST_BODY_BYTES);
  return {
    method,
    args: input.args,
  };
}

/**
 * @param {unknown} value
 * @returns {OwnerFence | null}
 */
function normalizeOwnerFence(value) {
  if (value == null) return null;
  const input = requireRecord(value, "owner");
  const generation = Number(input.generation);
  if (!Number.isSafeInteger(generation) || generation <= 0) {
    throw new DoRuntimeError(400, "invalid_request", "owner.generation must be a positive safe integer");
  }
  return {
    ownerKey: requireString(input.ownerKey, "owner.ownerKey"),
    taskId: requireString(input.taskId, "owner.taskId"),
    generation,
  };
}

/**
 * @param {unknown} value
 * @returns {{ hostId: string, doStorageId: string, className: string, shard: number }}
 */
function parseHostId(value) {
  const hostId = requireString(value, "hostId", { pattern: HOST_ID_RE });
  const parts = hostId.split(":");
  const shard = Number(hostId.slice(hostId.lastIndexOf(":shard") + ":shard".length));
  if (!Number.isInteger(shard) || shard < 0 || shard >= DO_HOST_SHARD_COUNT) {
    throw new DoRuntimeError(400, "invalid_request", "hostId shard is not valid");
  }
  return {
    hostId,
    doStorageId: parts[0],
    className: parts[1],
    shard,
  };
}

/**
 * @param {unknown} value
 * @param {BundleSource} source
 * @param {ObjectTarget} input
 */
function normalizeHostId(value, source, input) {
  const parsed = parseHostId(value);
  if (
    parsed.doStorageId !== source.doStorageId ||
    parsed.className !== input.className ||
    parsed.shard !== shardForObjectName(input.objectName)
  ) {
    throw new DoRuntimeError(400, "invalid_request", "hostId does not match object shard");
  }
  return parsed.hostId;
}

/**
 * @param {JsonRecord} input
 * @returns {BundleSource}
 */
function normalizeBundleSource(input) {
  const ns = requireString(input.ns, "ns");
  if (!isValidRuntimeLoadNs(ns)) {
    throw new DoRuntimeError(400, "invalid_request", "ns is not valid");
  }
  const worker = requireString(input.worker, "worker", { pattern: WORKER_NAME_RE });
  const version = requireString(input.version, "version");
  if (parseVersion(version) == null) {
    throw new DoRuntimeError(400, "invalid_request", "version is not valid");
  }
  const doStorageId = requireString(input.doStorageId, "doStorageId", { pattern: STORAGE_ID_RE });
  return {
    ns,
    worker,
    version,
    doStorageId,
    workerId: formatWorkerId({ namespace: ns, worker, version }),
  };
}

/**
 * @param {unknown} value
 * @returns {DoInvoke}
 */
export function normalizeDoInvokeRequest(value) {
  const input = requireRecord(value, "body");
  if (input.workerCode != null) {
    throw new DoRuntimeError(400, "invalid_request", "workerCode is not accepted by the DO invoke protocol");
  }
  const source = normalizeBundleSource(input);
  const kind = normalizeInvokeKind(input.kind);
  const className = requireString(input.className, "className", { pattern: CLASS_NAME_RE });
  const objectName = requireString(input.objectName, "objectName");
  const base = {
    kind,
    hostId: input.hostId == null
      ? hostIdForObject(source.doStorageId, className, objectName)
      : normalizeHostId(input.hostId, source, { className, objectName }),
    className,
    objectName,
    props: {
      ns: source.ns,
      worker: source.worker,
      version: source.version,
      doStorageId: source.doStorageId,
      className,
    },
    ...(input.owner == null ? {} : { owner: /** @type {OwnerFence} */ (normalizeOwnerFence(input.owner)) }),
    ...source,
  };
  if (kind === "alarm") return /** @type {DoInvoke} */ ({ ...base, alarm: normalizeAlarmInfo(input.alarm) });
  if (kind === "rpc") return /** @type {DoInvoke} */ ({ ...base, rpc: normalizeRpcInfo(input.rpc) });
  return /** @type {DoInvoke} */ ({ ...base, request: normalizeRequestSpec(input.request) });
}

/** @param {Headers} headers */
function visibleConnectHeaders(headers) {
  return [...headers.entries()].filter(([name]) => (
    !CONNECT_INTERNAL_HEADER_NAMES.has(name.toLowerCase())
  ));
}

/** @param {Request} request */
export function normalizeDoConnectRequest(request) {
  const headers = request.headers;
  const body = {
    ns: headers.get(CONNECT_HEADERS.ns),
    worker: headers.get(CONNECT_HEADERS.worker),
    version: headers.get(CONNECT_HEADERS.version),
    doStorageId: headers.get(CONNECT_HEADERS.doStorageId),
    className: headers.get(CONNECT_HEADERS.className),
    objectName: headers.get(CONNECT_HEADERS.objectName),
    owner: headers.has(CONNECT_HEADERS.ownerKey) || headers.has(CONNECT_HEADERS.ownerTaskId) || headers.has(CONNECT_HEADERS.ownerGeneration)
      ? {
          ownerKey: headers.get(CONNECT_HEADERS.ownerKey),
          taskId: headers.get(CONNECT_HEADERS.ownerTaskId),
          generation: headers.get(CONNECT_HEADERS.ownerGeneration),
        }
      : undefined,
    request: {
      method: request.method,
      url: headers.get(CONNECT_HEADERS.requestUrl) || "https://do.internal/",
      headers: visibleConnectHeaders(headers),
    },
  };
  return normalizeDoInvokeRequest(body);
}

/** @param {DoInvoke} invoke */
export function buildFacetName(invoke) {
  return `${invoke.className}:${invoke.objectName}`;
}

/** @param {{ hostId: string }} invoke */
export function buildOwnerKey(invoke) {
  return invoke.hostId;
}

/** @param {RequestSpec} spec */
export function buildForwardRequest(spec) {
  const body = spec.bodyBytes ?? null;
  /** @type {RequestInit} */
  const init = {
    method: spec.method,
    headers: spec.headers,
    ...(body == null ? {} : { body: /** @type {BodyInit} */ (body) }),
  };
  const request = new Request(spec.url, init);
  // User-controlled DO fetches must not be able to spoof internal Workflows
  // alarm delivery; only the alarm builder may attach this.
  request.headers.delete(ALARM_INTERNAL_HEADER);
  request.headers.delete(RPC_INTERNAL_HEADER);
  request.headers.delete(INTERNAL_AUTH_HEADER);
  return request;
}

/**
 * @param {EnvelopeInvoke} invoke
 * @returns {{ invoke: EnvelopeInvoke, bodyBytes: Uint8Array | null }}
 */
function invokeWithoutBodyBytes(invoke) {
  if (invoke.request?.bodyBytes) {
    const request = { ...invoke.request };
    const bodyBytes = /** @type {Uint8Array} */ (request.bodyBytes);
    delete request.bodyBytes;
    return { invoke: { ...invoke, request }, bodyBytes };
  }
  return { invoke, bodyBytes: null };
}

/** @param {string | null} requestId */
function localActorHeaders(requestId) {
  return {
    "content-type": "application/octet-stream",
    [LOCAL_ACTOR_ENVELOPE_HEADER]: LOCAL_ACTOR_ENVELOPE_MARKER,
    ...(requestId ? { "x-request-id": requestId } : {}),
  };
}

/**
 * @param {EnvelopeInvoke} metadata
 * @param {Uint8Array | null} bodyBytes
 * @param {{ maxBytes?: number, tooLargeMessage?: string }} [options]
 */
function encodeInvokeEnvelope(metadata, bodyBytes, options = {}) {
  const metadataBytes = utf8Encoder.encode(JSON.stringify(metadata));
  const body = bodyBytes == null ? new Uint8Array() : bodyBytes;
  const total = 4 + metadataBytes.length + body.length;
  if (options.maxBytes !== undefined && total > options.maxBytes) {
    throw new DoRuntimeError(413, "request_body_too_large",
      options.tooLargeMessage || `DO invoke envelope exceeds ${options.maxBytes} bytes`);
  }
  const envelope = new Uint8Array(total);
  new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength)
    .setUint32(0, metadataBytes.length, false);
  envelope.set(metadataBytes, 4);
  envelope.set(body, 4 + metadataBytes.length);
  return envelope;
}

/**
 * @param {string} url
 * @param {EnvelopeInvoke} invoke
 * @param {string | null} [requestId]
 */
export function buildLocalActorRequest(url, invoke, requestId = null) {
  const { invoke: metadata, bodyBytes } = invokeWithoutBodyBytes(invoke);
  return new Request(url, {
    method: "POST",
    headers: localActorHeaders(requestId),
    body: encodeInvokeEnvelope(metadata, bodyBytes),
  });
}

/**
 * @param {Request} request
 */
export async function readLocalActorInvokeRequest(request) {
  if (request.headers.get(LOCAL_ACTOR_ENVELOPE_HEADER) !== LOCAL_ACTOR_ENVELOPE_MARKER) {
    throw new DoRuntimeError(415, "unsupported_media_type", "DO host actor requests require the local envelope");
  }
  const bytes = await readBoundedBytes(request, MAX_INVOKE_ENVELOPE_BYTES);
  if (bytes.length < 4) {
    throw new DoRuntimeError(400, "invalid_request", "local actor envelope is truncated");
  }
  const metadataLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, false);
  if (metadataLength === 0 || 4 + metadataLength > bytes.length) {
    throw new DoRuntimeError(400, "invalid_request", "local actor envelope metadata is invalid");
  }
  let metadata;
  try {
    metadata = JSON.parse(utf8Decoder.decode(bytes.subarray(4, 4 + metadataLength)));
  } catch {
    throw new DoRuntimeError(400, "invalid_json", "Request body must be valid JSON");
  }
  const invoke = normalizeDoInvokeRequest(metadata);
  const bodyBytes = bytes.subarray(4 + metadataLength);
  if (bodyBytes.length === 0) return invoke;
  if (!("request" in invoke)) {
    throw new DoRuntimeError(400, "invalid_request", "local actor envelope body is only valid for fetch requests");
  }
  return {
    ...invoke,
    request: {
      ...invoke.request,
      bodyBytes,
    },
  };
}

/** @param {Uint8Array} bytes */
function decodeInvokeEnvelope(bytes) {
  if (bytes.length < 4) {
    throw new DoRuntimeError(400, "invalid_request", "DO invoke envelope is truncated");
  }
  const metadataLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, false);
  if (metadataLength === 0 || 4 + metadataLength > bytes.length) {
    throw new DoRuntimeError(400, "invalid_request", "DO invoke envelope metadata is invalid");
  }
  let metadata;
  try {
    metadata = JSON.parse(utf8Decoder.decode(bytes.subarray(4, 4 + metadataLength)));
  } catch {
    throw new DoRuntimeError(400, "invalid_json", "Request body must be valid JSON");
  }
  return { metadata, bodyBytes: bytes.subarray(4 + metadataLength) };
}

/**
 * @param {unknown} metadata
 * @param {Uint8Array} bodyBytes
 */
function invokeWithEnvelopeBody(metadata, bodyBytes) {
  const invoke = normalizeDoInvokeRequest(metadata);
  if (bodyBytes.length === 0) return invoke;
  if (!("request" in invoke)) {
    throw new DoRuntimeError(400, "invalid_request", "DO invoke envelope body is only valid for fetch requests");
  }
  return {
    ...invoke,
    request: {
      ...invoke.request,
      bodyBytes,
    },
  };
}

/**
 * @param {Request} request
 */
export async function readDoInvokeRequest(request) {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() !== DO_INVOKE_CONTENT_TYPE) {
    throw new DoRuntimeError(
      415,
      "unsupported_media_type",
      `DO invoke endpoint requires ${DO_INVOKE_CONTENT_TYPE}`
    );
  }
  const { metadata, bodyBytes } = decodeInvokeEnvelope(await readBoundedBytes(request, MAX_INVOKE_ENVELOPE_BYTES));
  return invokeWithEnvelopeBody(metadata, bodyBytes);
}

/** @param {EnvelopeInvoke} invoke */
export function encodeDoInvokeRequest(invoke) {
  const { invoke: metadata, bodyBytes } = invokeWithoutBodyBytes(invoke);
  return encodeInvokeEnvelope(metadata, bodyBytes, {
    maxBytes: MAX_INVOKE_ENVELOPE_BYTES,
    tooLargeMessage: `DO invoke envelope exceeds ${MAX_INVOKE_ENVELOPE_BYTES} bytes`,
  });
}

/** @param {JsonRecord} alarm @param {string | null} requestId */
export function buildAlarmRequest(alarm, requestId) {
  return new Request(ALARM_INTERNAL_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      [ALARM_INTERNAL_HEADER]: "1",
      ...(requestId ? { "x-request-id": requestId } : {}),
    },
    body: JSON.stringify(alarm),
  });
}

/**
 * @param {RpcInfo} rpc
 * @param {string | null} requestId
 */
export function buildRpcRequest(rpc, requestId) {
  return new Request(RPC_INTERNAL_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      [RPC_INTERNAL_HEADER]: "1",
      ...(requestId ? { "x-request-id": requestId } : {}),
    },
    body: JSON.stringify({ method: rpc.method, args: rpc.args }),
  });
}
