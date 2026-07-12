import { validOwnerEndpointForService } from "./_wdl-owner-endpoint.js";

export const DO_INVOKE_URL = "http://do-runtime/internal/do/invoke";
export const DO_CONNECT_URL = "http://do-runtime/internal/do/connect";
export const DO_INVOKE_CONTENT_TYPE = "application/vnd.wdl.do-invoke";
export const MAX_DO_REQUEST_BODY_BYTES = 1024 * 1024;
export const MAX_DO_INVOKE_ENVELOPE_BYTES = 2 * 1024 * 1024;
export const MAX_DO_REQUEST_HEADER_COUNT = 128;
export const MAX_DO_REQUEST_HEADER_BYTES = 64 * 1024;
export const DO_ACCEPT_OWNER_HINT_HEADER = "x-wdl-do-accept-owner-hint";
export const DO_OWNER_HINT_CONTROL_HEADER = "x-wdl-do-owner-hint";
export const DO_OWNER_HINT_CODE = "do_owner_hint";
export const INTERNAL_AUTH_HEADER = "x-wdl-internal-auth";

const DO_OWNER_HINT_HEADERS = {
  ownerKey: "x-wdl-do-owner-key",
  taskId: "x-wdl-do-owner-task-id",
  endpoint: "x-wdl-do-owner-endpoint",
  generation: "x-wdl-do-owner-generation",
};
const DO_OWNER_HINT_STRIP_HEADERS = [
  ...Object.values(DO_OWNER_HINT_HEADERS),
  DO_OWNER_HINT_CONTROL_HEADER,
];
const DO_FETCH_STRIP_HEADERS = [
  ...DO_OWNER_HINT_STRIP_HEADERS,
  DO_ACCEPT_OWNER_HINT_HEADER,
  INTERNAL_AUTH_HEADER,
  "x-wdl-do-hop-count",
];
const DO_CONNECT_STRIP_HEADERS = [
  ...DO_FETCH_STRIP_HEADERS,
];
const OWNER_ENDPOINT_UNAVAILABLE_STATUSES = new Set([502, 503, 504]);
export const OWNER_RACE_RETRY_CODES = new Set([
  "stale_owner_generation",
  "owner_claim_raced",
  "owner_fence_missing",
  "owner_lease_expired",
  "owner_lease_too_short",
]);
export const OWNER_HINT_STALE_CODES = new Set([
  "stale_owner_generation",
  "owner_claim_raced",
  "owner_fence_missing",
  "owner_lease_expired",
  "stale_owner_storage",
  "owner_lease_too_short",
  "owner_renew_raced",
  "owner_release_raced",
  "owner_unavailable",
  "owner_endpoint_missing",
  "forward_hop_exhausted",
  "task_draining",
]);
const utf8Encoder = new TextEncoder();

const DO_CONNECT_HEADERS = {
  ns: "x-wdl-do-ns",
  worker: "x-wdl-do-worker",
  version: "x-wdl-do-version",
  doStorageId: "x-wdl-do-storage-id",
  className: "x-wdl-do-class-name",
  objectName: "x-wdl-do-object-name",
  requestUrl: "x-wdl-do-request-url",
};
const RPC_RESERVED_METHODS = new Set(["fetch", "alarm"]);

/**
 * @typedef {{ ns: string, worker: string, version: string, doStorageId: string, className: string }} DoBindingProps
 * @typedef {{ ownerKey: string, taskId: string, endpoint: string, generation: number }} DoOwnerHint
 * @typedef {(url: string, init?: RequestInit) => Promise<Response>} DoFetch
 * @typedef {{
 *   get: (value: unknown) => unknown,
 *   set: (value: unknown, hint: unknown) => unknown,
 *   delete: (value: unknown) => unknown,
 * }} DoOwnerHintCache
 * @typedef {{
 *   routerFetch: DoFetch,
 *   routerUrl: string,
 *   ownerFetch: DoFetch | typeof fetch | null | undefined,
 *   ownerPath: string,
 *   init: RequestInit,
 *   cache: DoOwnerHintCache,
 *   hintKey: string,
 *   replayOwnerUnavailable?: boolean,
 * }} DoOwnerHintDispatchOptions
 */

/** @param {DoBindingProps} props @param {string} objectName */
export function doOwnerHintCacheKey(props, objectName) {
  return `${props.doStorageId}:${props.className}:${objectName}`;
}

/** @param {Response} response */
export async function staleDoOwnerHintResponse(response) {
  if (response.status < 400) return false;
  try {
    const body = await response.clone().json();
    return OWNER_HINT_STALE_CODES.has(body?.error);
  } catch {
    return false;
  }
}

/** @param {string} value */
function encodeUtf8(value) {
  return utf8Encoder.encode(value);
}

/**
 * @param {Record<string, unknown>} metadata
 * @param {Uint8Array | null} [bodyBytes]
 * @returns {Uint8Array}
 */
function encodeDoInvokeEnvelope(metadata, bodyBytes = null) {
  const metadataBytes = encodeUtf8(JSON.stringify(metadata));
  const body = bodyBytes == null ? new Uint8Array() : bodyBytes;
  if (metadataBytes.length + body.length + 4 > MAX_DO_INVOKE_ENVELOPE_BYTES) {
    throw new TypeError(`Durable Object invoke envelope exceeds ${MAX_DO_INVOKE_ENVELOPE_BYTES} bytes`);
  }
  const envelope = new Uint8Array(4 + metadataBytes.length + body.length);
  new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength).setUint32(0, metadataBytes.length, false);
  envelope.set(metadataBytes, 4);
  envelope.set(body, 4 + metadataBytes.length);
  return envelope;
}

/** @param {string} value */
function byteLength(value) {
  return utf8Encoder.encode(value).byteLength;
}

/**
 * @param {Request} request
 * @returns {Promise<Uint8Array>}
 */
async function readRequestBodyBytes(request) {
  // This source is injected into loaded workers, so keep the bounded reader
  // local instead of importing a helper that would add another facade module.
  const contentLength = request.headers.get("content-length");
  if (contentLength != null && contentLength !== "") {
    const declared = Number(contentLength);
    if (Number.isFinite(declared) && declared > MAX_DO_REQUEST_BODY_BYTES) {
      throw new TypeError(`Durable Object fetch body exceeds ${MAX_DO_REQUEST_BODY_BYTES} bytes`);
    }
  }
  if (!request.body) return new Uint8Array();

  const reader = request.body.getReader();
  /** @type {Uint8Array[]} */
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_DO_REQUEST_BODY_BYTES) {
        reader.cancel().catch(() => {});
        throw new TypeError(`Durable Object fetch body exceeds ${MAX_DO_REQUEST_BODY_BYTES} bytes`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
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

/** @param {unknown} args */
export function assertJsonRpcArgs(args) {
  if (!Array.isArray(args)) throw new TypeError("rpc.args must be an array");
  const error = jsonDataError(args, "rpc.args");
  if (error) throw new TypeError(error);
}

/** @param {unknown} method */
export function assertRpcMethod(method) {
  if (typeof method !== "string" || !/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(method)) {
    throw new TypeError("rpc.method is not valid");
  }
  if (RPC_RESERVED_METHODS.has(method)) {
    throw new TypeError("rpc.method is reserved");
  }
}

/**
 * @param {DoBindingProps} props
 * @param {string} objectName
 * @param {string} method
 * @param {unknown[]} args
 * @returns {Uint8Array}
 */
export function rpcInvokeBody(props, objectName, method, args) {
  assertRpcMethod(method);
  assertJsonRpcArgs(args);
  if (byteLength(JSON.stringify(args)) > MAX_DO_REQUEST_BODY_BYTES) {
    throw new TypeError(`rpc.args exceeds ${MAX_DO_REQUEST_BODY_BYTES} bytes`);
  }
  return encodeDoInvokeEnvelope({
    ns: props.ns,
    worker: props.worker,
    version: props.version,
    doStorageId: props.doStorageId,
    className: props.className,
    objectName,
    kind: "rpc",
    rpc: { method, args },
  });
}

/**
 * @param {string | null | undefined} requestId
 * @returns {Record<string, string>}
 */
function binaryInvokeHeaders(requestId) {
  return {
    "content-type": DO_INVOKE_CONTENT_TYPE,
    [DO_ACCEPT_OWNER_HINT_HEADER]: "1",
    ...(requestId ? { "x-request-id": requestId } : {}),
  };
}

/**
 * @param {DoBindingProps} props
 * @param {string} objectName
 * @param {Request} request
 * @param {string | null | undefined} requestId
 * @returns {Promise<RequestInit>}
 */
export async function fetchInvokeInit(props, objectName, request, requestId) {
  const { spec, bodyBytes } = await requestSpec(request, requestId);
  return {
    method: "POST",
    headers: binaryInvokeHeaders(requestId),
    body: /** @type {BodyInit} */ (encodeDoInvokeEnvelope({
      ns: props.ns,
      worker: props.worker,
      version: props.version,
      doStorageId: props.doStorageId,
      className: props.className,
      objectName,
      request: spec,
    }, bodyBytes)),
  };
}

/**
 * @param {DoBindingProps} props
 * @param {string} objectName
 * @param {string} method
 * @param {unknown[]} args
 * @param {string | null | undefined} requestId
 * @returns {RequestInit}
 */
export function rpcInvokeInit(props, objectName, method, args, requestId) {
  return {
    method: "POST",
    headers: binaryInvokeHeaders(requestId),
    body: /** @type {BodyInit} */ (rpcInvokeBody(props, objectName, method, args)),
  };
}

/** @param {Response} response */
export async function rpcResultFromResponse(response) {
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const err = new Error(body?.message || `Durable Object RPC failed with status ${response.status}`);
    if (body?.name) err.name = body.name;
    if (body?.error) Object.defineProperty(err, "code", { value: body.error });
    if (typeof body?.stack === "string" && body.stack) err.stack = body.stack;
    throw err;
  }
  return body?.result;
}

/** @param {Response} response */
export async function retryableOwnerRaceResponse(response) {
  if (response.status !== 503) return false;
  try {
    const body = await response.clone().json();
    return OWNER_RACE_RETRY_CODES.has(body?.error);
  } catch {
    return false;
  }
}

/**
 * @param {Request} request
 * @param {string | null | undefined} requestId
 * @returns {Promise<{ spec: { method: string, url: string, headers: [string, string][] }, bodyBytes: Uint8Array | null }>}
 */
export async function requestSpec(request, requestId) {
  const forwarded = new Request(request);
  for (const name of DO_FETCH_STRIP_HEADERS) {
    forwarded.headers.delete(name);
  }
  if (requestId && !forwarded.headers.has("x-request-id")) {
    forwarded.headers.set("x-request-id", requestId);
  }
  const method = forwarded.method.toUpperCase();
  const headers = [...forwarded.headers.entries()];
  enforceRequestHeadersBudget(headers);
  const spec = {
    method,
    url: forwarded.url,
    headers,
  };
  let bodyBytes = null;
  if (method !== "GET" && method !== "HEAD") {
    const body = await readRequestBodyBytes(forwarded);
    if (body.byteLength > 0) bodyBytes = body;
  }
  return { spec, bodyBytes };
}

/** @param {[string, string][]} headers */
function enforceRequestHeadersBudget(headers) {
  if (headers.length > MAX_DO_REQUEST_HEADER_COUNT) {
    throw new TypeError(`Durable Object fetch headers exceed ${MAX_DO_REQUEST_HEADER_COUNT} entries`);
  }
  let total = 0;
  for (const [name, value] of headers) {
    total += byteLength(name) + byteLength(value);
    if (total > MAX_DO_REQUEST_HEADER_BYTES) {
      throw new TypeError(`Durable Object fetch headers exceed ${MAX_DO_REQUEST_HEADER_BYTES} bytes`);
    }
  }
}

/** @param {Request} request */
export function isWebSocketUpgrade(request) {
  return (request.headers.get("Upgrade") || "").toLowerCase() === "websocket";
}

/** @param {Request} request */
export function replayOwnerUnavailableForFetch(request) {
  const method = request.method.toUpperCase();
  return method === "GET" || method === "HEAD";
}

/**
 * @param {DoBindingProps} props
 * @param {string} objectName
 * @param {Request} request
 * @param {string | null | undefined} requestId
 */
export function connectHeaders(props, objectName, request, requestId) {
  const headers = new Headers(request.headers);
  for (const name of DO_CONNECT_STRIP_HEADERS) headers.delete(name);
  headers.set(DO_CONNECT_HEADERS.ns, props.ns);
  headers.set(DO_CONNECT_HEADERS.worker, props.worker);
  headers.set(DO_CONNECT_HEADERS.version, props.version);
  headers.set(DO_CONNECT_HEADERS.doStorageId, props.doStorageId);
  headers.set(DO_CONNECT_HEADERS.className, props.className);
  headers.set(DO_CONNECT_HEADERS.objectName, objectName);
  headers.set(DO_CONNECT_HEADERS.requestUrl, request.url);
  if (requestId && !headers.has("x-request-id")) headers.set("x-request-id", requestId);
  headers.set(DO_ACCEPT_OWNER_HINT_HEADER, "1");
  return headers;
}

/**
 * @param {DoOwnerHint} owner
 * @param {string} pathname
 */
export function ownerRequestUrl(owner, pathname) {
  if (!validOwnerEndpoint(owner.endpoint)) {
    throw new Error("Invalid DO owner endpoint");
  }
  return `http://${owner.endpoint}${pathname}`;
}

/**
 * @param {DoOwnerHint} owner
 * @param {{ control?: boolean }} [options]
 * @returns {Record<string, string>}
 */
export function ownerHintHeaders(owner, { control = false } = {}) {
  const headers = {
    [DO_OWNER_HINT_HEADERS.ownerKey]: owner.ownerKey,
    [DO_OWNER_HINT_HEADERS.taskId]: owner.taskId,
    [DO_OWNER_HINT_HEADERS.endpoint]: owner.endpoint,
    [DO_OWNER_HINT_HEADERS.generation]: String(owner.generation),
  };
  if (control) headers[DO_OWNER_HINT_CONTROL_HEADER] = "1";
  return headers;
}

/**
 * @param {Headers} headers
 * @returns {DoOwnerHint | null}
 */
export function ownerHintFromHeaders(headers) {
  const ownerKey = headers.get(DO_OWNER_HINT_HEADERS.ownerKey);
  const taskId = headers.get(DO_OWNER_HINT_HEADERS.taskId);
  const endpoint = headers.get(DO_OWNER_HINT_HEADERS.endpoint);
  const rawGeneration = headers.get(DO_OWNER_HINT_HEADERS.generation);
  if (rawGeneration == null || rawGeneration === "") return null;
  const generation = Number(rawGeneration);
  if (!ownerKey || !taskId || !validOwnerEndpoint(endpoint) || !Number.isInteger(generation) || generation < 0) {
    return null;
  }
  return { ownerKey, taskId, endpoint: String(endpoint), generation };
}

/** @param {Response} response */
export async function ownerHintFromResponse(response) {
  if (response.status !== 409) return null;
  // Only do-runtime-authored headers are trusted. Tenant DO code controls the
  // response body and may intentionally return a do_owner_hint-shaped 409.
  if (response.headers.get(DO_OWNER_HINT_CONTROL_HEADER) !== "1") return null;
  return ownerHintFromHeaders(response.headers);
}

/** @param {unknown} endpoint */
function validOwnerEndpoint(endpoint) {
  return validOwnerEndpointForService(endpoint, 8788, "do-runtime");
}

/**
 * @param {Response} response
 * @param {DoFetch | typeof fetch | null | undefined} ownerFetch
 * @param {string} pathname
 * @param {RequestInit} init
 */
export async function followOwnerHint(response, ownerFetch, pathname, init) {
  const hint = await ownerHintFromResponse(response);
  if (!hint || typeof ownerFetch !== "function") return response;
  const direct = await ownerFetch(ownerRequestUrl(hint, pathname), init);
  if (ownerEndpointUnavailableResponse(direct)) {
    throw new Error(`DO owner endpoint returned ${direct.status}`);
  }
  return direct;
}

export function ownerUnavailableResponse() {
  return Response.json(
    { error: "owner_unavailable", message: "DO owner is unavailable; request outcome may be unknown" },
    { status: 503 }
  );
}

/**
 * @param {{
 *   routerFetch: DoFetch,
 *   routerUrl: string,
 *   ownerFetch: DoFetch | typeof fetch | null | undefined,
 *   ownerPath: string,
 *   init: RequestInit,
 *   cachedHint?: DoOwnerHint | null,
 *   rememberHint?: (hint: DoOwnerHint) => void,
 *   clearHint?: () => void,
 *   staleCachedResponse?: (response: Response) => Promise<boolean>,
 *   bypassOwnerHintResponse?: (response: Response) => Promise<boolean>,
 *   replayOwnerUnavailable?: boolean,
 * }} options
 */
export async function dispatchDoRequestWithOwnerHint({
  routerFetch,
  routerUrl,
  ownerFetch,
  ownerPath,
  init,
  cachedHint = null,
  rememberHint = (_hint) => {},
  clearHint = () => {},
  staleCachedResponse = async (_response) => false,
  bypassOwnerHintResponse = async (_response) => false,
  replayOwnerUnavailable = false,
}) {
  if (cachedHint && typeof ownerFetch === "function") {
    try {
      const direct = await ownerFetch(ownerRequestUrl(cachedHint, ownerPath), init);
      if (await ownerHintFromResponse(direct)) {
        clearHint();
      } else if (await staleCachedResponse(direct)) {
        clearHint();
      } else if (ownerEndpointUnavailableResponse(direct)) {
        clearHint();
        if (replayOwnerUnavailable) {
          return await routerFetch(routerUrl, withoutOwnerHintOptIn(init));
        }
        return ownerUnavailableResponse();
      } else {
        const learned = ownerHintFromHeaders(direct.headers);
        if (learned) rememberHint(learned);
        return direct;
      }
    } catch {
      clearHint();
      if (replayOwnerUnavailable) {
        return await routerFetch(routerUrl, withoutOwnerHintOptIn(init));
      }
      return ownerUnavailableResponse();
    }
  }

  const routed = await routerFetch(routerUrl, init);
  if (await bypassOwnerHintResponse(routed)) return routed;
  const hinted = await ownerHintFromResponse(routed);
  if (!hinted || typeof ownerFetch !== "function") {
    if (hinted) rememberHint(hinted);
    else {
      const learned = ownerHintFromHeaders(routed.headers);
      if (learned) rememberHint(learned);
    }
    return routed;
  }
  rememberHint(hinted);
  try {
    const direct = await followOwnerHint(routed, ownerFetch, ownerPath, init);
    const learned = ownerHintFromHeaders(direct.headers);
    if (learned) rememberHint(learned);
    return direct;
  } catch {
    clearHint();
    if (replayOwnerUnavailable) {
      return await routerFetch(routerUrl, withoutOwnerHintOptIn(init));
    }
    return ownerUnavailableResponse();
  }
}

/**
 * @param {DoOwnerHintDispatchOptions} options
 * @param {boolean} retryOwnerRace
 */
async function dispatchDoWithHintCache({
  routerFetch,
  routerUrl,
  ownerFetch,
  ownerPath,
  init,
  cache,
  hintKey,
  replayOwnerUnavailable = false,
}, retryOwnerRace) {
  let response = await dispatchDoRequestWithOwnerHint({
    routerFetch,
    routerUrl,
    ownerFetch,
    ownerPath,
    init,
    cachedHint: /** @type {DoOwnerHint | null} */ (cache.get(hintKey)),
    rememberHint: (hint) => cache.set(hintKey, hint),
    clearHint: () => cache.delete(hintKey),
    staleCachedResponse: staleDoOwnerHintResponse,
    bypassOwnerHintResponse: retryOwnerRace
      ? retryableOwnerRaceResponse
      : async (_response) => false,
    replayOwnerUnavailable,
  });
  if (retryOwnerRace && await retryableOwnerRaceResponse(response)) {
    cache.delete(hintKey);
    response = await routerFetch(routerUrl, withoutOwnerHintOptIn(init));
  }
  return stripOwnerHintHeaders(response);
}

/** @param {DoOwnerHintDispatchOptions} options */
export async function dispatchDoInvokeWithHintCache(options) {
  return await dispatchDoWithHintCache(options, true);
}

/** @param {DoOwnerHintDispatchOptions} options */
export async function dispatchDoConnectWithHintCache(options) {
  return await dispatchDoWithHintCache({
    ...options,
    replayOwnerUnavailable: false,
  }, false);
}

/** @param {Response} response */
export function ownerEndpointUnavailableResponse(response) {
  return OWNER_ENDPOINT_UNAVAILABLE_STATUSES.has(response.status) &&
    !ownerHintFromHeaders(response.headers);
}

/** @param {RequestInit} init */
export function withoutOwnerHintOptIn(init) {
  const headers = new Headers(init.headers || {});
  headers.delete(DO_ACCEPT_OWNER_HINT_HEADER);
  return { ...init, headers };
}

/** @param {Response} response */
export function stripOwnerHintHeaders(response) {
  const headers = new Headers(response.headers);
  for (const name of DO_OWNER_HINT_STRIP_HEADERS) {
    headers.delete(name);
  }
  const init = /** @type {ResponseInit & { webSocket?: WebSocket }} */ ({
    status: response.status,
    statusText: response.statusText,
    headers,
  });
  const webSocket = /** @type {{ webSocket?: WebSocket }} */ (response).webSocket;
  if (webSocket) init.webSocket = webSocket;
  return new Response(response.status === 101 ? null : response.body, init);
}
