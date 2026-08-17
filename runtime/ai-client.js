/**
 * Tenant-realm AI facade. This file is embedded as text and injected into
 * dynamic Workers with the shared request-id helper.
 */

import { requestIdFromOptions } from "./_wdl-request-id.js";

const AI_ORIGIN = "https://ai.wdl";
const ALLOWED_OPTIONS = new Set(["signal", "websocket"]);
const REJECTED_OPTIONS = new Set([
  "returnRawResponse",
  "tags",
  "queueRequest",
  "gateway",
  "prefix",
  "extraHeaders",
  "sessionOptions",
]);
const AI_MODEL_REFERENCE_RE =
  /^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?\/(?![0-9]+$)[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$/;
const intrinsicReflectApply = Reflect.apply;
const intrinsicAbortSignalThrowIfAborted = AbortSignal.prototype.throwIfAborted;
const intrinsicArrayIsArray = Array.isArray;
const intrinsicRegExpExec = RegExp.prototype.exec;
const intrinsicSetHas = Set.prototype.has;
const intrinsicHeadersSet = Headers.prototype.set;
const intrinsicWeakMapGet = WeakMap.prototype.get;
const intrinsicWeakMapSet = WeakMap.prototype.set;
const intrinsicObjectEntries = Object.entries;
const intrinsicObjectHasOwn = Object.hasOwn;
const intrinsicStructuredClone = structuredClone;
const IntrinsicHeaders = Headers;
const IntrinsicRequest = Request;
const intrinsicResponseJson = Response.prototype.json;

/** @param {unknown} value @returns {value is unknown[]} */
function isArray(value) {
  return intrinsicReflectApply(intrinsicArrayIsArray, Array, [value]);
}

/** @param {Set<string>} target @param {string} value */
function setHas(target, value) {
  return intrinsicReflectApply(intrinsicSetHas, target, [value]);
}

/** @param {unknown[]} values @param {unknown} expected */
function arrayIncludes(values, expected) {
  for (let index = 0; index < values.length; index += 1) {
    if (values[index] === expected) return true;
  }
  return false;
}

/** @param {RegExp} pattern @param {string} value */
function regexMatches(pattern, value) {
  return intrinsicReflectApply(intrinsicRegExpExec, pattern, [value]) !== null;
}

/** @param {AbortSignal | undefined} signal */
function throwIfAborted(signal) {
  if (signal) intrinsicReflectApply(intrinsicAbortSignalThrowIfAborted, signal, []);
}

/** @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} AiFetcher */
/** @typedef {{ fetcher: AiFetcher, requestIdOptions: object, catalogScope: WeakKey }} AiState */

const state = new WeakMap();
const catalogSnapshots = new WeakMap();

/** @param {WeakKey} target @param {unknown} value */
function weakMapSet(target, value) {
  intrinsicReflectApply(intrinsicWeakMapSet, state, [target, value]);
}

/** @param {WeakKey} target @returns {AiState} */
function weakMapGet(target) {
  const value = intrinsicReflectApply(intrinsicWeakMapGet, state, [target]);
  if (!value) throw new TypeError("Illegal AI binding invocation");
  return value;
}

/** @param {WeakKey} target @returns {unknown[] | undefined} */
function catalogSnapshot(target) {
  return intrinsicReflectApply(intrinsicWeakMapGet, catalogSnapshots, [target]);
}

/** @param {WeakKey} target @param {unknown[]} models */
function rememberCatalogSnapshot(target, models) {
  intrinsicReflectApply(intrinsicWeakMapSet, catalogSnapshots, [target, models]);
}

/** @param {unknown[]} models */
function cloneModels(models) {
  return intrinsicReflectApply(intrinsicStructuredClone, undefined, [models]);
}

/** @param {Response} response */
async function responseJson(response) {
  return await intrinsicReflectApply(intrinsicResponseJson, response, []);
}

/** @param {AiState} binding @param {RequestInfo | URL} input @param {RequestInit} [init] */
async function fetchRequest(binding, input, init = undefined) {
  const { fetcher, requestIdOptions } = binding;
  if (!fetcher || typeof fetcher.fetch !== "function") {
    throw new TypeError("AI host Fetcher is not configured");
  }
  const request = new IntrinsicRequest(input, init);
  const requestId = requestIdFromOptions(requestIdOptions);
  if (requestId) {
    intrinsicReflectApply(intrinsicHeadersSet, request.headers, ["x-request-id", requestId]);
  }
  return await intrinsicReflectApply(fetcher.fetch, fetcher, [request]);
}

/** @param {unknown} raw */
function normalizeOptions(raw) {
  if (raw == null) return {};
  if (typeof raw !== "object" || isArray(raw)) {
    throw new TypeError("AI options must be an object");
  }
  const options = /** @type {Record<string, unknown>} */ (raw);
  const entries = intrinsicReflectApply(intrinsicObjectEntries, Object, [options]);
  for (let index = 0; index < entries.length; index += 1) {
    const key = entries[index][0];
    if (setHas(REJECTED_OPTIONS, key)) {
      if (key === "returnRawResponse") {
        throw new TypeError("AI returnRawResponse is not supported; use env.AI.fetch() for raw responses");
      }
      throw new TypeError(`AI option ${key} is not supported`);
    }
    if (!setHas(ALLOWED_OPTIONS, key)) throw new TypeError(`AI option ${key} is not supported`);
  }
  if (options.websocket !== undefined && typeof options.websocket !== "boolean") {
    throw new TypeError("AI option websocket must be boolean");
  }
  return options;
}

/** @param {unknown} payload @param {number} status */
function aiError(payload, status) {
  const record = payload && typeof payload === "object"
    ? /** @type {Record<string, unknown>} */ (payload)
    : {};
  const providerError = record.error && typeof record.error === "object" &&
      !isArray(record.error)
    ? /** @type {Record<string, unknown>} */ (record.error)
    : null;
  let message = `AI request failed with status ${status}`;
  if (typeof record.message === "string" && record.message) {
    message = record.message;
  } else if (typeof providerError?.message === "string" && providerError.message) {
    message = providerError.message;
  }
  let code = "ai_request_failed";
  if (typeof record.error === "string" && record.error) {
    code = record.error;
  } else if (typeof providerError?.code === "string" && providerError.code) {
    code = providerError.code;
  } else if (typeof providerError?.type === "string" && providerError.type) {
    code = providerError.type;
  }
  const error = new Error(message);
  error.name = "AIError";
  return Object.assign(error, { code, status });
}

/** @param {Response} response */
async function requireOkJson(response) {
  let payload;
  try {
    payload = await responseJson(response);
  } catch {
    throw aiError(null, response.ok ? 502 : response.status);
  }
  if (!response.ok) throw aiError(payload, response.status);
  return payload;
}

/** @param {AiState} binding @param {AbortSignal | undefined} signal */
async function loadModels(binding, signal) {
  const response = await fetchRequest(binding, `${AI_ORIGIN}/v1/models`, {
    method: "GET",
    signal,
  });
  const payload = await requireOkJson(response);
  const record = payload && typeof payload === "object"
    ? /** @type {Record<string, unknown>} */ (payload)
    : null;
  if (!record || !isArray(record.models)) {
    throw aiError({ message: "AI models response is malformed" }, 502);
  }
  return record.models;
}

/** @param {AiState} binding @param {AbortSignal | undefined} signal */
async function catalogModels(binding, signal) {
  throwIfAborted(signal);
  const cached = catalogSnapshot(binding.catalogScope);
  if (cached) return cached;
  // Retain only settled data; an in-flight fetch Promise belongs to one request IoContext.
  let models;
  try {
    models = await loadModels(binding, signal);
  } catch (err) {
    throwIfAborted(signal);
    const settled = catalogSnapshot(binding.catalogScope);
    if (settled) return settled;
    throw err;
  }
  throwIfAborted(signal);
  const settled = catalogSnapshot(binding.catalogScope);
  if (settled) return settled;
  rememberCatalogSnapshot(binding.catalogScope, models);
  return models;
}

/** @param {unknown[]} models @param {string} model */
function modelDescriptor(models, model) {
  for (let index = 0; index < models.length; index += 1) {
    const descriptor = models[index];
    if (
      descriptor &&
      typeof descriptor === "object" &&
      /** @type {Record<string, unknown>} */ (descriptor).id === model
    ) return descriptor;
  }
  throw aiError({ error: "ai_model_not_found", message: "AI model not found" }, 404);
}

/** @param {Record<string, unknown>} descriptor @param {string} transport */
function requireTransport(descriptor, transport) {
  if (!isArray(descriptor.transports) || !arrayIncludes(descriptor.transports, transport)) {
    throw aiError({
      error: "ai_transport_unavailable",
      message: `AI model does not support ${transport}`,
    }, 409);
  }
}

/** @param {string} protocol */
function protocolPath(protocol) {
  if (protocol === "responses") return "/v1/responses";
  if (protocol === "chat_completions") return "/v1/chat/completions";
  if (protocol === "embeddings") return "/v1/embeddings";
  throw new TypeError(`AI protocol ${protocol} requires websocket: true`);
}

export class Ai {
  /** @param {AiFetcher} fetcher @param {object} [requestIdOptions] @param {WeakKey | null} [catalogScope] */
  constructor(fetcher, requestIdOptions = {}, catalogScope = null) {
    weakMapSet(this, { fetcher, requestIdOptions, catalogScope: catalogScope ?? this });
  }

  /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
  async fetch(input, init = undefined) {
    return await fetchRequest(weakMapGet(this), input, init);
  }

  async models() {
    const binding = weakMapGet(this);
    return cloneModels(await catalogModels(binding, undefined));
  }

  /** @param {string} model @param {unknown} inputs @param {Record<string, unknown>} [rawOptions] */
  async run(model, inputs, rawOptions = {}) {
    if (
      typeof model !== "string" ||
      !regexMatches(AI_MODEL_REFERENCE_RE, model)
    ) {
      throw aiError({
        error: "ai_invalid_model",
        message: "AI model must be <provider>/<alias>",
      }, 400);
    }
    const options = normalizeOptions(rawOptions);
    const signal = /** @type {AbortSignal | undefined} */ (options.signal);
    const binding = weakMapGet(this);
    const websocket = options.websocket === true;
    /** @type {Record<string, unknown> | null} */
    let body = null;
    let stream = false;
    if (websocket) {
      if (inputs !== null) throw new TypeError("AI websocket run requires inputs to be null");
    } else {
      if (!inputs || typeof inputs !== "object" || isArray(inputs)) {
        throw new TypeError("AI inputs must be an object");
      }
      body = {
        .../** @type {Record<string, unknown>} */ (inputs),
        model,
      };
      stream = intrinsicObjectHasOwn(body, "stream") && body.stream === true;
      if (body.stream !== undefined && typeof body.stream !== "boolean") {
        throw new TypeError("AI inputs.stream must be boolean");
      }
    }
    const models = await catalogModels(binding, signal);
    throwIfAborted(signal);
    const descriptor = /** @type {Record<string, unknown>} */ (
      modelDescriptor(models, model)
    );

    if (websocket) {
      const protocol = descriptor.protocol;
      const transport = protocol === "responses"
        ? "responses_websocket"
        : protocol === "realtime"
          ? "realtime_websocket"
          : null;
      if (!transport) throw new TypeError(`AI protocol ${protocol} does not support websocket mode`);
      requireTransport(descriptor, transport);
      const url = new URL(`${AI_ORIGIN}${protocol === "responses" ? "/v1/responses" : "/v1/realtime"}`);
      url.searchParams.set("model", model);
      const response = await fetchRequest(binding, url, {
        method: "GET",
        headers: new IntrinsicHeaders({ Upgrade: "websocket" }),
        signal,
      });
      const websocketResponse = /** @type {Response & { webSocket?: WebSocket }} */ (response);
      if (websocketResponse.status !== 101 || !websocketResponse.webSocket) {
        let payload = null;
        try { payload = await responseJson(websocketResponse); } catch {}
        throw aiError(payload, websocketResponse.status);
      }
      return websocketResponse;
    }

    if (!body) throw new TypeError("AI inputs must be an object");
    requireTransport(descriptor, stream ? "sse" : "http");
    const response = await fetchRequest(binding,
      `${AI_ORIGIN}${protocolPath(String(descriptor.protocol))}`,
      {
        method: "POST",
        headers: new IntrinsicHeaders({
          Accept: stream ? "text/event-stream" : "application/json",
          "Content-Type": "application/json",
        }),
        body: JSON.stringify(body),
        signal,
      }
    );
    if (stream) {
      if (!response.ok) await requireOkJson(response);
      if (!response.body) throw aiError({ message: "AI stream response has no body" }, 502);
      return response.body;
    }
    return await requireOkJson(response);
  }
}
