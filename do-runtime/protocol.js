import {
  CLASS_NAME_RE,
  METHOD_NAME_RE,
  HEADER_NAME_RE,
  WORKER_NAME_RE,
  NS_FIELD_RE,
  STORAGE_ID_RE,
  VERSION_RE,
  HOST_ID_RE,
  DO_HOST_SHARD_COUNT,
  MAX_ID_BYTES,
} from "do-runtime-protocol-wire-grammar";
import { DoRuntimeError } from "do-runtime-protocol-errors";
import { hostIdForObject, shardForObjectName } from "do-runtime-protocol-identity";
import { formatWorkerId } from "shared-worker-id";
import { firstWorkerdExperimentalCompatFlag } from "shared-workerd-compat-flags";
import {
  BodyTooLargeError,
  readBoundedBytes as readRequestBoundedBytes,
  readBoundedText as readRequestBoundedText,
} from "shared-bounded-body";
import { INTERNAL_AUTH_HEADER } from "shared-internal-auth";

export { DO_HOST_SHARD_COUNT } from "do-runtime-protocol-wire-grammar";
export { DoRuntimeError, doErrorResponse } from "do-runtime-protocol-errors";
export { hostIdForObject, hostIdForShard, shardForObjectName } from "do-runtime-protocol-identity";

const MAX_MODULE_COUNT = 128;
const MAX_MODULE_SOURCE_BYTES = 1024 * 1024;
const MAX_REQUEST_BODY_BYTES = 1024 * 1024;
const MAX_INVOKE_ENVELOPE_BYTES = 2 * 1024 * 1024;
const MAX_REQUEST_HEADER_COUNT = 128;
const MAX_REQUEST_HEADER_BYTES = 64 * 1024;
export const DO_INVOKE_CONTENT_TYPE = "application/vnd.wdl.do-invoke";
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const ALARM_INTERNAL_URL = "https://do.internal/__wdl_alarm";
const ALARM_INTERNAL_HEADER = "x-wdl-do-internal-alarm";
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
  ...Object.values(CONNECT_HEADERS),
  ...OWNER_HINT_PROTOCOL_HEADERS,
  "x-wdl-do-forwarded",
  "x-wdl-do-hop-count",
]);
const LOCAL_ACTOR_ENVELOPE_HEADER = "x-wdl-do-local-envelope";
const LOCAL_ACTOR_ENVELOPE_MARKER = "binary";

/**
 * @typedef {Record<string, unknown>} JsonRecord
 * @typedef {{ ownerKey: string, taskId: string, generation: number }} OwnerFence
 * @typedef {{ ns: string, worker: string, version: string, doStorageId: string, workerId: string }} BundleSource
 * @typedef {{ workerId: string, workerCode: JsonRecord }} InlineWorkerSource
 * @typedef {BundleSource | InlineWorkerSource} InvokeSource
 * @typedef {{ className: string, objectName: string }} ObjectTarget
 * @typedef {{ method: string, url: string, headers: Array<[string, string]>, bodyBytes?: Uint8Array, bodyBase64?: undefined, bodyText?: undefined }} RequestSpec
 * @typedef {{ retryCount: number, isRetry: boolean, token?: string }} AlarmInfo
 * @typedef {{ method: string, args: unknown[] }} RpcInfo
 * @typedef {InvokeSource & { kind: "fetch", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, request: RequestSpec }} FetchInvoke
 * @typedef {InvokeSource & { kind: "alarm", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, alarm: AlarmInfo }} AlarmInvoke
 * @typedef {InvokeSource & { kind: "rpc", hostId: string, className: string, objectName: string, props: Record<string, unknown>, owner?: OwnerFence | null, rpc: RpcInfo }} RpcInvoke
 * @typedef {FetchInvoke | AlarmInvoke | RpcInvoke} DoInvoke
 * @typedef {Record<string, unknown> & { request?: Record<string, unknown> & { bodyBytes?: Uint8Array } }} EnvelopeInvoke
 * @typedef {{ allowInlineWorkerCode?: boolean }} NormalizeOptions
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
  const retryCount = input.retryCount == null ? 0 : Number(input.retryCount);
  if (!Number.isInteger(retryCount) || retryCount < 0) {
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
  if (!Number.isInteger(generation) || generation < 0) {
    throw new DoRuntimeError(400, "invalid_request", "owner.generation must be a non-negative integer");
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
 * @param {InvokeSource} source
 * @param {ObjectTarget} input
 */
function normalizeHostId(value, source, input) {
  const parsed = parseHostId(value);
  if (
    "ns" in source &&
    (
      parsed.doStorageId !== source.doStorageId ||
      parsed.className !== input.className ||
      parsed.shard !== shardForObjectName(input.objectName)
    )
  ) {
    throw new DoRuntimeError(400, "invalid_request", "hostId does not match object shard");
  }
  return parsed.hostId;
}

/**
 * @param {unknown} value
 * @returns {JsonRecord}
 */
function normalizeWorkerCode(value) {
  const input = requireRecord(value, "workerCode");
  const modules = requireRecord(input.modules, "workerCode.modules");
  const entries = Object.entries(modules);
  if (entries.length === 0) {
    throw new DoRuntimeError(400, "invalid_request", "workerCode.modules must not be empty");
  }
  if (entries.length > MAX_MODULE_COUNT) {
    throw new DoRuntimeError(400, "invalid_request", "workerCode.modules has too many modules");
  }
  /** @type {Record<string, string>} */
  const normalizedModules = {};
  for (const [name, source] of entries) {
    const moduleName = requireString(name, "workerCode module name", { maxBytes: 512 });
    if (moduleName.includes("..")) {
      throw new DoRuntimeError(400, "invalid_request", "workerCode module names must not contain ..");
    }
    if (typeof source !== "string") {
      throw new DoRuntimeError(400, "invalid_request", `workerCode.modules.${moduleName} must be a string`);
    }
    if (byteLength(source) > MAX_MODULE_SOURCE_BYTES) {
      throw new DoRuntimeError(400, "invalid_request", `workerCode.modules.${moduleName} is too large`);
    }
    normalizedModules[moduleName] = source;
  }
  const mainModule = requireString(input.mainModule, "workerCode.mainModule", { maxBytes: 512 });
  if (!Object.hasOwn(normalizedModules, mainModule)) {
    throw new DoRuntimeError(400, "invalid_request", "workerCode.mainModule must reference a module");
  }
  const compatibilityDate = input.compatibilityDate == null
    ? "2026-04-24"
    : requireString(input.compatibilityDate, "workerCode.compatibilityDate", { maxBytes: 64 });
  const compatibilityFlags = Array.isArray(input.compatibilityFlags)
    ? input.compatibilityFlags.map((flag, index) => requireString(flag, `workerCode.compatibilityFlags[${index}]`, { maxBytes: 128 }))
    : ["nodejs_compat"];
  const experimentalFlag = firstWorkerdExperimentalCompatFlag(compatibilityFlags);
  if (experimentalFlag) {
    throw new DoRuntimeError(
      400,
      "experimental_compat_flag_unsupported",
      `workerCode.compatibilityFlags contains experimental workerd flag ${JSON.stringify(experimentalFlag)}, which WDL does not support for tenant workers`
    );
  }
  const env = input.env == null ? {} : requireRecord(input.env, "workerCode.env");
  return {
    compatibilityDate,
    compatibilityFlags,
    mainModule,
    modules: normalizedModules,
    env,
  };
}

/**
 * @param {JsonRecord} input
 * @returns {BundleSource}
 */
function normalizeBundleSource(input) {
  const ns = requireString(input.ns, "ns", { pattern: NS_FIELD_RE });
  const worker = requireString(input.worker, "worker", { pattern: WORKER_NAME_RE });
  const version = requireString(String(input.version ?? ""), "version", { pattern: VERSION_RE });
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
 * @param {NormalizeOptions} [options]
 * @returns {DoInvoke}
 */
export function normalizeDoInvokeRequest(value, options = {}) {
  const input = requireRecord(value, "body");
  const allowInlineWorkerCode = options.allowInlineWorkerCode === true;
  const hasInlineWorkerCode = input.workerCode != null;
  if (hasInlineWorkerCode && !allowInlineWorkerCode) {
    throw new DoRuntimeError(400, "invalid_request", "workerCode is only accepted when DO_TEST_HOOKS=1");
  }
  /** @type {InvokeSource} */
  const source = hasInlineWorkerCode
    ? {
        workerId: requireString(input.workerId, "workerId"),
        workerCode: normalizeWorkerCode(input.workerCode),
      }
    : normalizeBundleSource(input);
  const kind = normalizeInvokeKind(input.kind);
  const className = requireString(input.className, "className", { pattern: CLASS_NAME_RE });
  const objectName = requireString(input.objectName, "objectName");
  const base = {
    kind,
    hostId: input.hostId == null
      ? defaultHostId(source, { className, objectName })
      : normalizeHostId(input.hostId, source, { className, objectName }),
    className,
    objectName,
    props: "ns" in source ? {
      ns: source.ns,
      worker: source.worker,
      version: source.version,
      doStorageId: source.doStorageId,
      className,
    } : {},
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

/**
 * @param {InvokeSource} source
 * @param {ObjectTarget} input
 */
function defaultHostId(source, input) {
  if (!("ns" in source) || !source.ns || !source.worker) {
    throw new DoRuntimeError(400, "invalid_request", "hostId is required for inline workerCode test hooks");
  }
  return hostIdForObject(source.doStorageId, input.className, input.objectName);
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
 * @param {NormalizeOptions} [options]
 */
export async function readLocalActorInvokeRequest(request, options = {}) {
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
  const invoke = normalizeDoInvokeRequest(metadata, options);
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
 * @param {NormalizeOptions} options
 */
function invokeWithEnvelopeBody(metadata, bodyBytes, options) {
  const invoke = normalizeDoInvokeRequest(metadata, options);
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
 * @param {NormalizeOptions} [options]
 */
export async function readDoInvokeRequest(request, options = {}) {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() !== DO_INVOKE_CONTENT_TYPE) {
    throw new DoRuntimeError(
      415,
      "unsupported_media_type",
      `DO invoke endpoint requires ${DO_INVOKE_CONTENT_TYPE}`
    );
  }
  const { metadata, bodyBytes } = decodeInvokeEnvelope(await readBoundedBytes(request, MAX_INVOKE_ENVELOPE_BYTES));
  return invokeWithEnvelopeBody(metadata, bodyBytes, options);
}

/** @param {EnvelopeInvoke} invoke */
export function encodeDoInvokeRequest(invoke) {
  const { invoke: metadata, bodyBytes } = invokeWithoutBodyBytes(invoke);
  return encodeInvokeEnvelope(metadata, bodyBytes, {
    maxBytes: MAX_INVOKE_ENVELOPE_BYTES,
    tooLargeMessage: `DO invoke envelope exceeds ${MAX_INVOKE_ENVELOPE_BYTES} bytes`,
  });
}

/** @param {JsonRecord} alarm */
export function buildAlarmRequest(alarm) {
  return new Request(ALARM_INTERNAL_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      [ALARM_INTERNAL_HEADER]: "1",
    },
    body: JSON.stringify(alarm),
  });
}
