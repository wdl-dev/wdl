import { sanitizeRequestId } from "./_wdl-request-id.js";

const DO_BINDING_OBJECT_HEADER = "x-wdl-do-binding-object-name";
const DO_BINDING_REQUEST_ID_HEADER = "x-wdl-do-binding-request-id";
export const DO_OWNER_HEADERS = Object.freeze({
  ownerKey: "x-wdl-do-owner-key",
  taskId: "x-wdl-do-owner-task-id",
  endpoint: "x-wdl-do-owner-endpoint",
  generation: "x-wdl-do-owner-generation",
  hint: "x-wdl-do-owner-hint",
});
export const DO_OWNER_CONTROL_HEADERS = Object.freeze({
  acceptHint: "x-wdl-do-accept-owner-hint",
  ownershipError: "x-wdl-do-ownership-error",
});
export const DO_CONNECT_HEADERS = Object.freeze({
  ns: "x-wdl-do-ns",
  worker: "x-wdl-do-worker",
  version: "x-wdl-do-version",
  doStorageId: "x-wdl-do-storage-id",
  className: "x-wdl-do-class-name",
  objectName: "x-wdl-do-object-name",
  requestUrl: "x-wdl-do-request-url",
});

const IntrinsicHeaders = Headers;
const IntrinsicRequest = Request;
const intrinsicDecodeURIComponent = decodeURIComponent;
const intrinsicEncodeURIComponent = encodeURIComponent;
const intrinsicReflectApply = Reflect.apply;
const intrinsicHeadersAppend = Headers.prototype.append;
const intrinsicHeadersDelete = Headers.prototype.delete;
const intrinsicHeadersForEach = Headers.prototype.forEach;
const intrinsicHeadersGet = Headers.prototype.get;
const intrinsicHeadersSet = Headers.prototype.set;
const intrinsicRequestHeadersGet = /** @type {(this: Request) => Headers} */ (
  Object.getOwnPropertyDescriptor(Request.prototype, "headers")?.get
);

if (typeof intrinsicRequestHeadersGet !== "function") {
  throw new Error("Request.headers getter is unavailable");
}

/** @param {Headers} headers @param {string} name */
function headerDelete(headers, name) {
  intrinsicReflectApply(intrinsicHeadersDelete, headers, [name]);
}

/** @param {Headers} headers @param {string} name */
function headerValue(headers, name) {
  return intrinsicReflectApply(intrinsicHeadersGet, headers, [name]);
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
    (value, name) => {
      intrinsicReflectApply(intrinsicHeadersAppend, out, [name, value]);
    },
  ]);
  return out;
}

/** @param {string} objectName */
export function encodeDoObjectNameHeader(objectName) {
  return intrinsicReflectApply(intrinsicEncodeURIComponent, undefined, [objectName]);
}

/** @param {string | null} encoded */
export function decodeDoObjectNameHeader(encoded) {
  if (!encoded) {
    throw new TypeError("Durable Object transport requires an object name");
  }
  let objectName;
  try {
    objectName = intrinsicReflectApply(intrinsicDecodeURIComponent, undefined, [encoded]);
  } catch {
    throw new TypeError("Durable Object transport has an invalid object name");
  }
  if (!objectName || encodeDoObjectNameHeader(objectName) !== encoded) {
    throw new TypeError("Durable Object transport has an invalid object name");
  }
  return objectName;
}

/**
 * Build the request sent through a binding-scoped WorkerEntrypoint.fetch().
 * The host adapter owns namespace/class identity; the tenant supplies only the
 * object name and original public request.
 *
 * @param {string} objectName
 * @param {Request} request
 * @param {string | null} requestId
 */
export function scopedDoRequest(objectName, request, requestId) {
  const headers = copyHeaders(intrinsicReflectApply(intrinsicRequestHeadersGet, request, []));
  headerSet(headers, DO_BINDING_OBJECT_HEADER, encodeDoObjectNameHeader(objectName));
  if (requestId) {
    headerSet(headers, DO_BINDING_REQUEST_ID_HEADER, requestId);
  } else {
    headerDelete(headers, DO_BINDING_REQUEST_ID_HEADER);
  }
  return new IntrinsicRequest(request, { headers });
}

/** @param {Request} request */
export function readScopedDoRequest(request) {
  const sourceHeaders = intrinsicReflectApply(intrinsicRequestHeadersGet, request, []);
  const objectName = decodeDoObjectNameHeader(headerValue(sourceHeaders, DO_BINDING_OBJECT_HEADER));
  const requestId = sanitizeRequestId(
    headerValue(sourceHeaders, DO_BINDING_REQUEST_ID_HEADER)
  );
  const headers = copyHeaders(sourceHeaders);
  headerDelete(headers, DO_BINDING_OBJECT_HEADER);
  headerDelete(headers, DO_BINDING_REQUEST_ID_HEADER);
  return {
    objectName,
    requestId,
    request: new IntrinsicRequest(request, { headers }),
  };
}
