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
const intrinsicReflectApply = Reflect.apply;
const intrinsicHeadersSet = Headers.prototype.set;
const intrinsicWeakMapGet = WeakMap.prototype.get;
const intrinsicWeakMapSet = WeakMap.prototype.set;
const intrinsicObjectEntries = Object.entries;
const intrinsicObjectHasOwn = Object.hasOwn;
const IntrinsicHeaders = Headers;
const IntrinsicRequest = Request;
const intrinsicResponseJson = Response.prototype.json;

/** @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} AiFetcher */
/** @typedef {{ fetcher: AiFetcher, requestIdOptions: object }} AiState */

const state = new WeakMap();

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
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new TypeError("AI options must be an object");
  }
  const options = /** @type {Record<string, unknown>} */ (raw);
  for (const [key] of intrinsicReflectApply(intrinsicObjectEntries, Object, [options])) {
    if (REJECTED_OPTIONS.has(key)) {
      if (key === "returnRawResponse") {
        throw new TypeError("AI returnRawResponse is not supported; use env.AI.fetch() for raw responses");
      }
      throw new TypeError(`AI option ${key} is not supported`);
    }
    if (!ALLOWED_OPTIONS.has(key)) throw new TypeError(`AI option ${key} is not supported`);
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
      !Array.isArray(record.error)
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
async function listModels(binding, signal) {
  const response = await fetchRequest(binding, `${AI_ORIGIN}/v1/models`, {
    method: "GET",
    signal,
  });
  const payload = await requireOkJson(response);
  const record = payload && typeof payload === "object"
    ? /** @type {Record<string, unknown>} */ (payload)
    : null;
  if (!record || !Array.isArray(record.models)) {
    throw aiError({ message: "AI models response is malformed" }, 502);
  }
  return record.models;
}

/** @param {unknown[]} models @param {string} model */
function modelDescriptor(models, model) {
  for (const descriptor of models) {
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
  if (!Array.isArray(descriptor.transports) || !descriptor.transports.includes(transport)) {
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
  /** @param {AiFetcher} fetcher @param {object} [requestIdOptions] */
  constructor(fetcher, requestIdOptions = {}) {
    weakMapSet(this, { fetcher, requestIdOptions });
  }

  /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
  async fetch(input, init = undefined) {
    return await fetchRequest(weakMapGet(this), input, init);
  }

  async models() {
    return await listModels(weakMapGet(this), undefined);
  }

  /** @param {string} model @param {unknown} inputs @param {Record<string, unknown>} [rawOptions] */
  async run(model, inputs, rawOptions = {}) {
    if (typeof model !== "string" || model.length === 0) {
      throw new TypeError("AI model must be a non-empty string");
    }
    const options = normalizeOptions(rawOptions);
    const signal = /** @type {AbortSignal | undefined} */ (options.signal);
    const binding = weakMapGet(this);
    const descriptor = /** @type {Record<string, unknown>} */ (
      modelDescriptor(await listModels(binding, signal), model)
    );

    if (options.websocket === true) {
      if (inputs !== null) throw new TypeError("AI websocket run requires inputs to be null");
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

    if (!inputs || typeof inputs !== "object" || Array.isArray(inputs)) {
      throw new TypeError("AI inputs must be an object");
    }
    const body = /** @type {Record<string, unknown>} */ ({
      .../** @type {Record<string, unknown>} */ (inputs),
      model,
    });
    const stream = intrinsicObjectHasOwn(body, "stream") && body.stream === true;
    if (body.stream !== undefined && typeof body.stream !== "boolean") {
      throw new TypeError("AI inputs.stream must be boolean");
    }
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
