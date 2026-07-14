import { validOwnerEndpointForService } from "./_wdl-owner-endpoint.js";

export const DO_INVOKE_URL = "http://do-runtime/internal/do/invoke";
export const DO_CONNECT_URL = "http://do-runtime/internal/do/connect";
export const DO_INVOKE_CONTENT_TYPE = "application/vnd.wdl.do-invoke";
export const MAX_DO_REQUEST_BODY_BYTES = 1024 * 1024;
export const MAX_DO_INVOKE_ENVELOPE_BYTES = 2 * 1024 * 1024;
export const MAX_DO_REQUEST_HEADER_COUNT = 128;
export const MAX_DO_REQUEST_HEADER_BYTES = 64 * 1024;
const MAX_DO_RPC_METHOD_BYTES = 256;
export const DO_ACCEPT_OWNER_HINT_HEADER = "x-wdl-do-accept-owner-hint";
export const DO_OWNER_HINT_CONTROL_HEADER = "x-wdl-do-owner-hint";
export const DO_OWNERSHIP_ERROR_CONTROL_HEADER = "x-wdl-do-ownership-error";
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
  DO_OWNERSHIP_ERROR_CONTROL_HEADER,
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
const OWNER_RACE_RETRY_CODES = new Set([
  "stale_owner_generation",
  "owner_claim_raced",
  "owner_fence_missing",
  "owner_lease_expired",
  "owner_lease_too_short",
]);
const OWNER_HINT_STALE_CODES = new Set([
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
const IntrinsicArray = Array;
const IntrinsicDataView = DataView;
const IntrinsicHeaders = Headers;
const IntrinsicJSON = JSON;
const IntrinsicNumber = Number;
const IntrinsicObject = Object;
const IntrinsicRequest = Request;
const IntrinsicResponse = Response;
const IntrinsicUint8Array = Uint8Array;
const IntrinsicWeakSet = WeakSet;
const intrinsicReflectApply = Reflect.apply;
const intrinsicArrayForEach = Array.prototype.forEach;
const intrinsicArrayIsArray = Array.isArray;
const intrinsicArrayPush = Array.prototype.push;
const intrinsicDataViewSetUint32 = DataView.prototype.setUint32;
const intrinsicHeadersAppend = Headers.prototype.append;
const intrinsicHeadersDelete = Headers.prototype.delete;
const intrinsicHeadersForEach = Headers.prototype.forEach;
const intrinsicHeadersGet = Headers.prototype.get;
const intrinsicHeadersHas = Headers.prototype.has;
const intrinsicHeadersSet = Headers.prototype.set;
const intrinsicJsonStringify = JSON.stringify;
const intrinsicNumberIsFinite = Number.isFinite;
const intrinsicNumberIsSafeInteger = Number.isSafeInteger;
const intrinsicObjectCreate = Object.create;
const intrinsicObjectEntries = Object.entries;
const intrinsicObjectGetOwnPropertySymbols = Object.getOwnPropertySymbols;
const intrinsicObjectGetPrototypeOf = Object.getPrototypeOf;
const intrinsicObjectSetPrototypeOf = Object.setPrototypeOf;
const intrinsicPromiseThen = Promise.prototype.then;
const intrinsicReadableStreamGetReader = ReadableStream.prototype.getReader;
const intrinsicReadableStreamReaderCancel = ReadableStreamDefaultReader.prototype.cancel;
const intrinsicReadableStreamReaderRead = ReadableStreamDefaultReader.prototype.read;
const intrinsicReadableStreamReaderReleaseLock = ReadableStreamDefaultReader.prototype.releaseLock;
const intrinsicRegExpTest = RegExp.prototype.test;
const intrinsicSetHas = Set.prototype.has;
const intrinsicStringToLowerCase = String.prototype.toLowerCase;
const intrinsicStringToUpperCase = String.prototype.toUpperCase;
const intrinsicTextEncoderEncode = TextEncoder.prototype.encode;
const intrinsicUint8ArraySet = Uint8Array.prototype.set;
const intrinsicUint8ArrayBufferGet = /** @type {(this: Uint8Array) => ArrayBufferLike} */ (
  prototypeGetter(Uint8Array.prototype, "buffer")
);
const intrinsicUint8ArrayByteLengthGet = /** @type {(this: Uint8Array) => number} */ (
  prototypeGetter(Uint8Array.prototype, "byteLength")
);
const intrinsicWeakSetAdd = WeakSet.prototype.add;
const intrinsicWeakSetDelete = WeakSet.prototype.delete;
const intrinsicWeakSetHas = WeakSet.prototype.has;
const intrinsicRequestHeadersGet = /** @type {(this: Request) => Headers} */ (
  prototypeGetter(Request.prototype, "headers")
);
const intrinsicRequestBodyGet = /** @type {(this: Request) => ReadableStream<Uint8Array> | null} */ (
  prototypeGetter(Request.prototype, "body")
);
const intrinsicRequestMethodGet = /** @type {(this: Request) => string} */ (
  prototypeGetter(Request.prototype, "method")
);
const intrinsicRequestUrlGet = /** @type {(this: Request) => string} */ (
  prototypeGetter(Request.prototype, "url")
);
const intrinsicResponseBodyGet = /** @type {(this: Response) => ReadableStream<Uint8Array> | null} */ (
  prototypeGetter(Response.prototype, "body")
);
const intrinsicResponseHeadersGet = /** @type {(this: Response) => Headers} */ (
  prototypeGetter(Response.prototype, "headers")
);
const intrinsicResponseStatusGet = /** @type {(this: Response) => number} */ (
  prototypeGetter(Response.prototype, "status")
);
const intrinsicResponseStatusTextGet = /** @type {(this: Response) => string} */ (
  prototypeGetter(Response.prototype, "statusText")
);
const intrinsicResponseWebSocketGet = /** @type {((this: Response) => WebSocket | null) | undefined} */ (
  prototypeGetter(Response.prototype, "webSocket")
);
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

// workerd places some WebIDL mixin getters on a parent prototype. Resolve
// them once before tenant module evaluation rather than reading patched getters.
/** @param {object | null} prototype @param {string} name */
function prototypeGetter(prototype, name) {
  let current = prototype;
  while (current) {
    const getter = Object.getOwnPropertyDescriptor(current, name)?.get;
    if (getter) return getter;
    current = Object.getPrototypeOf(current);
  }
  return undefined;
}

/** @param {DoBindingProps} props @param {string} objectName */
export function doOwnerHintCacheKey(props, objectName) {
  return `${props.doStorageId}:${props.className}:${objectName}`;
}

/** @param {Response} response */
export async function staleDoOwnerHintResponse(response) {
  if (responseStatus(response) < 400) return false;
  const code = responseHeader(response, DO_OWNERSHIP_ERROR_CONTROL_HEADER);
  return code !== null && setHas(OWNER_HINT_STALE_CODES, code);
}

/** @template T @param {T[]} values @param {(value: T) => void} callback */
function forEachArray(values, callback) {
  intrinsicReflectApply(intrinsicArrayForEach, values, [callback]);
}

/** @template T @param {T[]} values @param {T} value */
function pushArray(values, value) {
  intrinsicReflectApply(intrinsicArrayPush, values, [value]);
}

/** @param {Uint8Array} target @param {Uint8Array} source @param {number} offset */
function setBytes(target, source, offset) {
  intrinsicReflectApply(intrinsicUint8ArraySet, target, [source, offset]);
}

/** @param {Uint8Array} value */
function byteArrayBuffer(value) {
  return intrinsicReflectApply(intrinsicUint8ArrayBufferGet, value, []);
}

/** @param {Uint8Array} value */
function byteArrayLength(value) {
  return intrinsicReflectApply(intrinsicUint8ArrayByteLengthGet, value, []);
}

/** @param {Headers} headers @param {string} name @param {string} value */
function headerAppend(headers, name, value) {
  intrinsicReflectApply(intrinsicHeadersAppend, headers, [name, value]);
}

/** @param {Headers} headers @param {string} name */
function headerDelete(headers, name) {
  intrinsicReflectApply(intrinsicHeadersDelete, headers, [name]);
}

/** @param {Headers} headers @param {string} name */
function headerValue(headers, name) {
  return intrinsicReflectApply(intrinsicHeadersGet, headers, [name]);
}

/** @param {Headers} headers @param {string} name */
function headerHas(headers, name) {
  return intrinsicReflectApply(intrinsicHeadersHas, headers, [name]);
}

/** @param {Headers} headers @param {string} name @param {string} value */
function headerSet(headers, name, value) {
  intrinsicReflectApply(intrinsicHeadersSet, headers, [name, value]);
}

/** @param {Headers} source */
function copyHeaders(source) {
  const out = new IntrinsicHeaders();
  intrinsicReflectApply(intrinsicHeadersForEach, source, [
    /** @param {string} value @param {string} name */
    (value, name) => headerAppend(out, name, value),
  ]);
  return out;
}

/** @param {Headers} headers */
function headerEntries(headers) {
  /** @type {[string, string][]} */
  const out = [];
  intrinsicReflectApply(intrinsicHeadersForEach, headers, [
    /** @param {string} value @param {string} name */
    (value, name) => pushArray(out, [name, value]),
  ]);
  return out;
}

/** @template T @param {Set<T>} set @param {T} value */
function setHas(set, value) {
  return intrinsicReflectApply(intrinsicSetHas, set, [value]);
}

/** @param {string} value */
function stringToLowerCase(value) {
  return intrinsicReflectApply(intrinsicStringToLowerCase, value, []);
}

/** @param {string} value */
function stringToUpperCase(value) {
  return intrinsicReflectApply(intrinsicStringToUpperCase, value, []);
}

/** @param {RegExp} regexp @param {string} value */
function regexpTest(regexp, value) {
  return intrinsicReflectApply(intrinsicRegExpTest, regexp, [value]);
}

/** @param {Request} request */
function requestHeaders(request) {
  return intrinsicReflectApply(intrinsicRequestHeadersGet, request, []);
}

/** @param {Request} request */
function requestBody(request) {
  return intrinsicReflectApply(intrinsicRequestBodyGet, request, []);
}

/** @param {Request} request */
function requestMethod(request) {
  return intrinsicReflectApply(intrinsicRequestMethodGet, request, []);
}

/** @param {Request} request */
function requestUrl(request) {
  return intrinsicReflectApply(intrinsicRequestUrlGet, request, []);
}

/** @param {ReadableStream<Uint8Array>} stream */
function streamReader(stream) {
  return /** @type {ReadableStreamDefaultReader<Uint8Array>} */ (
    intrinsicReflectApply(intrinsicReadableStreamGetReader, stream, [])
  );
}

/** @param {ReadableStreamDefaultReader<Uint8Array>} reader */
function readStreamChunk(reader) {
  return intrinsicReflectApply(intrinsicReadableStreamReaderRead, reader, []);
}

/** @param {ReadableStreamDefaultReader<Uint8Array>} reader */
function releaseStreamReader(reader) {
  intrinsicReflectApply(intrinsicReadableStreamReaderReleaseLock, reader, []);
}

/** @param {ReadableStreamDefaultReader<Uint8Array>} reader */
function cancelStreamReader(reader) {
  try {
    const promise = intrinsicReflectApply(intrinsicReadableStreamReaderCancel, reader, []);
    intrinsicReflectApply(intrinsicPromiseThen, promise, [undefined, () => {}]);
  } catch {
    // Cancellation is best-effort after the bounded reader has already rejected.
  }
}

/** @param {Response} response */
function responseBody(response) {
  return intrinsicReflectApply(intrinsicResponseBodyGet, response, []);
}

/** @param {Response} response */
function responseHeaders(response) {
  return intrinsicReflectApply(intrinsicResponseHeadersGet, response, []);
}

/** @param {Response} response */
function responseStatus(response) {
  return intrinsicReflectApply(intrinsicResponseStatusGet, response, []);
}

/** @param {Response} response */
function responseStatusText(response) {
  return intrinsicReflectApply(intrinsicResponseStatusTextGet, response, []);
}

/** @param {Response} response */
function responseWebSocket(response) {
  if (!intrinsicResponseWebSocketGet) return null;
  return intrinsicReflectApply(intrinsicResponseWebSocketGet, response, []);
}

/** @param {Response} response @param {string} name */
function responseHeader(response, name) {
  return headerValue(responseHeaders(response), name);
}

/** @param {string} value */
function encodeUtf8(value) {
  return intrinsicReflectApply(intrinsicTextEncoderEncode, utf8Encoder, [value]);
}

/** @param {unknown} value */
function numberValue(value) {
  return intrinsicReflectApply(IntrinsicNumber, undefined, [value]);
}

/** @param {unknown} value */
function stringifyJson(value) {
  return intrinsicReflectApply(intrinsicJsonStringify, IntrinsicJSON, [cloneJsonData(value)]);
}

/** @param {unknown} value @param {WeakSet<object>} [seen] */
function cloneJsonData(value, seen = new IntrinsicWeakSet()) {
  if (value === null || typeof value !== "object") return value;
  const objectValue = /** @type {object} */ (value);
  if (intrinsicReflectApply(intrinsicWeakSetHas, seen, [objectValue])) {
    throw new TypeError("Cannot serialize circular JSON data");
  }
  intrinsicReflectApply(intrinsicWeakSetAdd, seen, [objectValue]);
  try {
    if (intrinsicReflectApply(intrinsicArrayIsArray, IntrinsicArray, [objectValue])) {
      const source = /** @type {unknown[]} */ (objectValue);
      const out = new IntrinsicArray(source.length);
      intrinsicReflectApply(intrinsicObjectSetPrototypeOf, IntrinsicObject, [out, null]);
      for (let i = 0; i < source.length; i++) out[i] = cloneJsonData(source[i], seen);
      return out;
    }
    const out = /** @type {Record<string, unknown>} */ (
      intrinsicReflectApply(intrinsicObjectCreate, IntrinsicObject, [null])
    );
    const entries = /** @type {[string, unknown][]} */ (
      intrinsicReflectApply(intrinsicObjectEntries, IntrinsicObject, [objectValue])
    );
    forEachArray(entries, (entry) => {
      out[entry[0]] = cloneJsonData(entry[1], seen);
    });
    return out;
  } finally {
    intrinsicReflectApply(intrinsicWeakSetDelete, seen, [objectValue]);
  }
}

/**
 * @param {Record<string, unknown>} metadata
 * @param {Uint8Array | null} [bodyBytes]
 * @returns {Uint8Array}
 */
function encodeDoInvokeEnvelope(metadata, bodyBytes = null) {
  const metadataBytes = encodeUtf8(stringifyJson(metadata));
  const body = bodyBytes == null ? new IntrinsicUint8Array() : bodyBytes;
  const metadataLength = byteArrayLength(metadataBytes);
  const bodyLength = byteArrayLength(body);
  const envelopeLength = 4 + metadataLength + bodyLength;
  if (envelopeLength > MAX_DO_INVOKE_ENVELOPE_BYTES) {
    throw new TypeError(`Durable Object invoke envelope exceeds ${MAX_DO_INVOKE_ENVELOPE_BYTES} bytes`);
  }
  const envelope = new IntrinsicUint8Array(envelopeLength);
  const view = new IntrinsicDataView(byteArrayBuffer(envelope), 0, envelopeLength);
  intrinsicReflectApply(intrinsicDataViewSetUint32, view, [0, metadataLength, false]);
  setBytes(envelope, metadataBytes, 4);
  setBytes(envelope, body, 4 + metadataLength);
  return envelope;
}

/** @param {string} value */
function byteLength(value) {
  return byteArrayLength(encodeUtf8(value));
}

/**
 * @param {Request} request
 * @returns {Promise<Uint8Array>}
 */
async function readRequestBodyBytes(request) {
  // This source is injected into loaded workers, so keep the bounded reader
  // local instead of importing a helper that would add another facade module.
  const contentLength = headerValue(requestHeaders(request), "content-length");
  if (contentLength != null && contentLength !== "") {
    const declared = numberValue(contentLength);
    if (intrinsicReflectApply(intrinsicNumberIsFinite, IntrinsicNumber, [declared]) && declared > MAX_DO_REQUEST_BODY_BYTES) {
      throw new TypeError(`Durable Object fetch body exceeds ${MAX_DO_REQUEST_BODY_BYTES} bytes`);
    }
  }
  const requestStream = requestBody(request);
  if (!requestStream) return new IntrinsicUint8Array();

  const reader = streamReader(requestStream);
  /** @type {Uint8Array[]} */
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await readStreamChunk(reader);
      if (done) break;
      total += byteArrayLength(value);
      if (total > MAX_DO_REQUEST_BODY_BYTES) {
        cancelStreamReader(reader);
        throw new TypeError(`Durable Object fetch body exceeds ${MAX_DO_REQUEST_BODY_BYTES} bytes`);
      }
      pushArray(chunks, value);
    }
  } finally {
    releaseStreamReader(reader);
  }

  const body = new IntrinsicUint8Array(total);
  let offset = 0;
  forEachArray(chunks, (chunk) => {
    setBytes(body, chunk, offset);
    offset += byteArrayLength(chunk);
  });
  return body;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @param {WeakSet<object>} [seen]
 * @returns {unknown}
 */
function cloneJsonRpcData(value, field, seen = new IntrinsicWeakSet()) {
  if (value === null) return null;
  const type = typeof value;
  if (type === "string" || type === "boolean") return value;
  if (type === "number") {
    if (!intrinsicReflectApply(intrinsicNumberIsFinite, IntrinsicNumber, [value])) {
      throw new TypeError(`${field} must be a finite number`);
    }
    return value;
  }
  if (type === "bigint" || type === "function" || type === "symbol" || type === "undefined") {
    throw new TypeError(`${field} must be JSON data`);
  }
  if (type !== "object") throw new TypeError(`${field} must be JSON data`);
  const objectValue = /** @type {Record<string, unknown> | unknown[]} */ (value);
  if (intrinsicReflectApply(intrinsicWeakSetHas, seen, [objectValue])) {
    throw new TypeError(`${field} must not be circular`);
  }
  intrinsicReflectApply(intrinsicWeakSetAdd, seen, [objectValue]);
  try {
    if (intrinsicReflectApply(intrinsicArrayIsArray, IntrinsicArray, [objectValue])) {
      const arrayValue = /** @type {unknown[]} */ (objectValue);
      const length = arrayValue.length;
      const out = new IntrinsicArray(length);
      intrinsicReflectApply(intrinsicObjectSetPrototypeOf, IntrinsicObject, [out, null]);
      for (let i = 0; i < length; i++) {
        if (!(i in arrayValue)) throw new TypeError(`${field} must not be sparse`);
        out[i] = cloneJsonRpcData(arrayValue[i], `${field}[${i}]`, seen);
      }
      return out;
    }
    const proto = intrinsicReflectApply(intrinsicObjectGetPrototypeOf, IntrinsicObject, [objectValue]);
    if (proto !== IntrinsicObject.prototype && proto !== null) {
      throw new TypeError(`${field} must be a plain JSON object`);
    }
    const symbols = intrinsicReflectApply(intrinsicObjectGetOwnPropertySymbols, IntrinsicObject, [objectValue]);
    if (symbols.length > 0) throw new TypeError(`${field} must not contain symbol keys`);
    const out = /** @type {Record<string, unknown>} */ (
      intrinsicReflectApply(intrinsicObjectCreate, IntrinsicObject, [null])
    );
    const entries = /** @type {[string, unknown][]} */ (
      intrinsicReflectApply(intrinsicObjectEntries, IntrinsicObject, [objectValue])
    );
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      out[entry[0]] = cloneJsonRpcData(entry[1], `${field}.${entry[0]}`, seen);
    }
    return out;
  } finally {
    intrinsicReflectApply(intrinsicWeakSetDelete, seen, [objectValue]);
  }
}

/** @param {unknown} method */
export function assertRpcMethod(method) {
  if (
    typeof method !== "string" ||
    byteLength(method) > MAX_DO_RPC_METHOD_BYTES ||
    !regexpTest(/^[A-Za-z_$][A-Za-z0-9_$]*$/, method)
  ) {
    throw new TypeError("rpc.method is not valid");
  }
  if (setHas(RPC_RESERVED_METHODS, method)) {
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
  if (!intrinsicReflectApply(intrinsicArrayIsArray, IntrinsicArray, [args])) {
    throw new TypeError("rpc.args must be an array");
  }
  const stableArgs = /** @type {unknown[]} */ (cloneJsonRpcData(args, "rpc.args"));
  if (byteLength(stringifyJson(stableArgs)) > MAX_DO_REQUEST_BODY_BYTES) {
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
    rpc: { method, args: stableArgs },
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
    const err = new Error(body?.message || `Durable Object RPC failed with status ${responseStatus(response)}`);
    if (body?.name) err.name = body.name;
    if (body?.error) Object.defineProperty(err, "code", { value: body.error });
    if (typeof body?.stack === "string" && body.stack) err.stack = body.stack;
    throw err;
  }
  return body?.result;
}

/** @param {Response} response */
export async function retryableOwnerRaceResponse(response) {
  if (responseStatus(response) !== 503) return false;
  const code = responseHeader(response, DO_OWNERSHIP_ERROR_CONTROL_HEADER);
  return code !== null && setHas(OWNER_RACE_RETRY_CODES, code);
}

/**
 * @param {Request} request
 * @param {string | null | undefined} requestId
 * @returns {Promise<{ spec: { method: string, url: string, headers: [string, string][] }, bodyBytes: Uint8Array | null }>}
 */
export async function requestSpec(request, requestId) {
  const forwarded = new IntrinsicRequest(request);
  const forwardedHeaders = requestHeaders(forwarded);
  forEachArray(DO_FETCH_STRIP_HEADERS, (name) => {
    headerDelete(forwardedHeaders, name);
  });
  if (requestId && !headerHas(forwardedHeaders, "x-request-id")) {
    headerSet(forwardedHeaders, "x-request-id", requestId);
  }
  const method = stringToUpperCase(requestMethod(forwarded));
  const headers = headerEntries(forwardedHeaders);
  enforceRequestHeadersBudget(headers);
  const spec = {
    method,
    url: requestUrl(forwarded),
    headers,
  };
  let bodyBytes = null;
  if (method !== "GET" && method !== "HEAD") {
    const body = await readRequestBodyBytes(forwarded);
    if (byteArrayLength(body) > 0) bodyBytes = body;
  }
  return { spec, bodyBytes };
}

/** @param {[string, string][]} headers */
function enforceRequestHeadersBudget(headers) {
  if (headers.length > MAX_DO_REQUEST_HEADER_COUNT) {
    throw new TypeError(`Durable Object fetch headers exceed ${MAX_DO_REQUEST_HEADER_COUNT} entries`);
  }
  let total = 0;
  forEachArray(headers, (entry) => {
    const name = entry[0];
    const value = entry[1];
    total += byteLength(name) + byteLength(value);
    if (total > MAX_DO_REQUEST_HEADER_BYTES) {
      throw new TypeError(`Durable Object fetch headers exceed ${MAX_DO_REQUEST_HEADER_BYTES} bytes`);
    }
  });
}

/** @param {Request} request */
export function isWebSocketUpgrade(request) {
  return stringToLowerCase(headerValue(requestHeaders(request), "Upgrade") || "") === "websocket";
}

/** @param {Request} request */
export function replayOwnerUnavailableForFetch(request) {
  const method = stringToUpperCase(requestMethod(request));
  return method === "GET" || method === "HEAD";
}

/**
 * @param {DoBindingProps} props
 * @param {string} objectName
 * @param {Request} request
 * @param {string | null | undefined} requestId
 */
export function connectHeaders(props, objectName, request, requestId) {
  const headers = copyHeaders(requestHeaders(request));
  forEachArray(DO_CONNECT_STRIP_HEADERS, (name) => headerDelete(headers, name));
  headerSet(headers, DO_CONNECT_HEADERS.ns, props.ns);
  headerSet(headers, DO_CONNECT_HEADERS.worker, props.worker);
  headerSet(headers, DO_CONNECT_HEADERS.version, props.version);
  headerSet(headers, DO_CONNECT_HEADERS.doStorageId, props.doStorageId);
  headerSet(headers, DO_CONNECT_HEADERS.className, props.className);
  headerSet(headers, DO_CONNECT_HEADERS.objectName, objectName);
  headerSet(headers, DO_CONNECT_HEADERS.requestUrl, requestUrl(request));
  if (requestId && !headerHas(headers, "x-request-id")) headerSet(headers, "x-request-id", requestId);
  headerSet(headers, DO_ACCEPT_OWNER_HINT_HEADER, "1");
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
  const ownerKey = headerValue(headers, DO_OWNER_HINT_HEADERS.ownerKey);
  const taskId = headerValue(headers, DO_OWNER_HINT_HEADERS.taskId);
  const endpoint = headerValue(headers, DO_OWNER_HINT_HEADERS.endpoint);
  const rawGeneration = headerValue(headers, DO_OWNER_HINT_HEADERS.generation);
  if (rawGeneration == null || rawGeneration === "") return null;
  const generation = numberValue(rawGeneration);
  if (
    !ownerKey || !taskId || !validOwnerEndpoint(endpoint) ||
    !intrinsicReflectApply(intrinsicNumberIsSafeInteger, IntrinsicNumber, [generation]) || generation <= 0
  ) {
    return null;
  }
  return { ownerKey, taskId, endpoint: /** @type {string} */ (endpoint), generation };
}

/** @param {Response} response */
export async function ownerHintFromResponse(response) {
  if (responseStatus(response) !== 409) return null;
  // Only do-runtime-authored headers are trusted. Tenant DO code controls the
  // response body and may intentionally return a do_owner_hint-shaped 409.
  const headers = responseHeaders(response);
  if (headerValue(headers, DO_OWNER_HINT_CONTROL_HEADER) !== "1") return null;
  return ownerHintFromHeaders(headers);
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
    throw new Error(`DO owner endpoint returned ${responseStatus(direct)}`);
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
        const learned = ownerHintFromHeaders(responseHeaders(direct));
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
      const learned = ownerHintFromHeaders(responseHeaders(routed));
      if (learned) rememberHint(learned);
    }
    return routed;
  }
  rememberHint(hinted);
  try {
    const direct = await followOwnerHint(routed, ownerFetch, ownerPath, init);
    const learned = ownerHintFromHeaders(responseHeaders(direct));
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
  return setHas(OWNER_ENDPOINT_UNAVAILABLE_STATUSES, responseStatus(response)) &&
    !ownerHintFromHeaders(responseHeaders(response));
}

/** @param {RequestInit} init */
export function withoutOwnerHintOptIn(init) {
  const headers = init.headers instanceof IntrinsicHeaders
    ? copyHeaders(init.headers)
    : new IntrinsicHeaders(init.headers || {});
  headerDelete(headers, DO_ACCEPT_OWNER_HINT_HEADER);
  return { ...init, headers };
}

/** @param {Response} response */
export function stripOwnerHintHeaders(response) {
  const headers = copyHeaders(responseHeaders(response));
  forEachArray(DO_OWNER_HINT_STRIP_HEADERS, (name) => {
    headerDelete(headers, name);
  });
  const status = responseStatus(response);
  const init = /** @type {ResponseInit & { webSocket?: WebSocket }} */ ({
    status,
    statusText: responseStatusText(response),
    headers,
  });
  const webSocket = responseWebSocket(response);
  if (webSocket) init.webSocket = webSocket;
  return new IntrinsicResponse(status === 101 ? null : responseBody(response), init);
}
