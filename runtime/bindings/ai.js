import { WorkerEntrypoint } from "cloudflare:workers";
import {
  normalizeAiModelsRequest,
  normalizeAiModelsResponse,
  normalizeAiResolveRequest,
  normalizeAiResolveResponse,
  parseAiModelReference,
} from "shared-ai-contract";
import {
  BodyTooLargeError,
  readBoundedBytes,
  readBoundedStreamBytes,
} from "shared-bounded-body";
import { errorMessage } from "shared-errors";
import { withInternalAuth } from "shared-internal-auth";
import { ensureRequestId, logStructured } from "shared-observability";
import { discardResponseBody, jsonError } from "shared-respond";
import { utf8ByteLength } from "shared-utf8";
import { aiRuntimeSetting } from "shared-ai-runtime-config";
import {
  WEBSOCKET_RECONNECT_POLICY_DISABLED,
  WEBSOCKET_RECONNECT_POLICY_HEADER,
} from "shared-worker-contract";
import { recordBindingOperation } from "runtime-metrics";
import {
  acquireAiLease,
  aiPoolStateForTest,
  resetAiPoolStateForTest,
} from "runtime-bindings-ai-capacity";
import {
  AiProviderRequestError,
  aiProviderHttpRequest,
  aiProviderResponseHeaders,
  aiProviderWebSocketRequest,
  expectedAiProviderDestination,
} from "runtime-bindings-ai-provider";
import { createAiStreamingResponse } from "runtime-bindings-ai-sse";
import {
  AI_WS_FRAME_MAX_BYTES,
  AI_WS_MAX_JSON_DEPTH,
  AI_WS_MAX_BYTES,
  closeAiWebSocket,
  createAiWebSocketBridge,
} from "runtime-bindings-ai-websocket";
import {
  requireRedisProxyBaseUrl,
  serviceNameFromEnv,
} from "runtime-bindings-proxy";

export const AI_REQUEST_MAX_BYTES = 1024 * 1024;
export const AI_REQUEST_MAX_JSON_DEPTH = 128;
export const AI_RESPONSE_MAX_BYTES = 4 * 1024 * 1024;
export const AI_STREAM_MAX_BYTES = 32 * 1024 * 1024;
export const AI_STREAM_FRAME_MAX_BYTES = 1024 * 1024;
export { AI_WS_FRAME_MAX_BYTES, AI_WS_MAX_JSON_DEPTH, AI_WS_MAX_BYTES };
export { aiPoolStateForTest, resetAiPoolStateForTest };

const AI_INTERNAL_RESPONSE_MAX_BYTES = 512 * 1024;
const AI_RESOLVE_ATTEMPTS = 2;
const AI_VIRTUAL_ORIGIN = "https://ai.wdl";
const JSON_CONTENT_TYPE = "application/json";
const SSE_CONTENT_TYPE = "text/event-stream";
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });
/** @type {ReadonlyMap<string, string>} */
const RESOLVER_FAILURE_MESSAGES = new Map([
  ["ai_credential_not_configured", "AI provider credential is not configured"],
  ["ai_state_corrupt", "AI provider state is invalid"],
  ["secret_decrypt_failed", "AI provider credential is unavailable"],
]);

/** @typedef {{ ns: string, worker: string, version: string }} AiBindingProps */
/** @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} AiNetwork */
/** @typedef {Record<string, unknown> & { AI_NETWORK?: AiNetwork, SERVICE_NAME?: unknown, REDIS_PROXY_URL?: unknown, WDL_INTERNAL_AUTH_TOKEN?: unknown }} AiBindingEnv */
/** @typedef {{ ctx: { props: AiBindingProps, waitUntil(promise: Promise<unknown>): void }, env: AiBindingEnv }} AiHostBinding */

class AiBindingError extends Error {
  /** @param {number} status @param {string} code @param {string} message */
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/** @param {AiBinding} binding @returns {AiHostBinding} */
function aiBinding(binding) {
  return /** @type {AiHostBinding} */ (/** @type {unknown} */ (binding));
}

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {string} text */
function assertRequestJsonTextDepth(text) {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
    } else if (character === "{" || character === "[") {
      depth += 1;
      if (depth > AI_REQUEST_MAX_JSON_DEPTH) {
        throw new AiBindingError(
          400,
          "ai_request_too_deep",
          `AI request JSON exceeds ${AI_REQUEST_MAX_JSON_DEPTH} levels`
        );
      }
    } else if (character === "}" || character === "]") {
      depth -= 1;
    }
  }
}

/** @param {string | null} contentType @param {string} expected */
function hasContentType(contentType, expected) {
  return (contentType || "").split(";", 1)[0].trim().toLowerCase() === expected;
}

/** @param {Request} request */
function virtualUrl(request) {
  const url = new URL(request.url);
  if (url.origin !== AI_VIRTUAL_ORIGIN || url.username || url.password || url.hash) {
    throw new AiBindingError(400, "ai_invalid_request", `AI requests must use ${AI_VIRTUAL_ORIGIN}`);
  }
  return url;
}

/** @param {Request} request @param {AbortSignal} signal */
async function requestModelBody(request, signal) {
  if (!hasContentType(request.headers.get("content-type"), JSON_CONTENT_TYPE)) {
    throw new AiBindingError(415, "ai_unsupported_media_type", "AI request content-type must be application/json");
  }
  let text;
  try {
    text = utf8Decoder.decode(await readBoundedBytes(request, AI_REQUEST_MAX_BYTES, signal));
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      throw new AiBindingError(413, "ai_request_too_large", `AI request exceeds ${AI_REQUEST_MAX_BYTES} bytes`);
    }
    signal.throwIfAborted();
    throw new AiBindingError(400, "ai_request_body_unreadable", "AI request body could not be read");
  }
  assertRequestJsonTextDepth(text);
  let parsed;
  try { parsed = JSON.parse(text); } catch {
    throw new AiBindingError(400, "ai_invalid_json", "AI request body must be valid JSON");
  }
  if (!isRecord(parsed)) {
    throw new AiBindingError(400, "ai_invalid_request", "AI request body must be an object");
  }
  return /** @type {Record<string, unknown>} */ (parsed);
}

/** @param {string} pathname */
function protocolForPath(pathname) {
  if (pathname === "/v1/responses") return "responses";
  if (pathname === "/v1/chat/completions") return "chat_completions";
  if (pathname === "/v1/embeddings") return "embeddings";
  return null;
}

/** @param {unknown} value @param {string} label */
function requireModel(value, label = "model") {
  if (typeof value !== "string" || value.length === 0) {
    throw new AiBindingError(400, "ai_invalid_model", `${label} must be <provider>/<alias>`);
  }
  try {
    parseAiModelReference(value);
  } catch {
    throw new AiBindingError(400, "ai_invalid_model", `${label} must be <provider>/<alias>`);
  }
  return value;
}

/** @param {AiHostBinding} binding @param {string} path @param {unknown} body @param {AbortSignal} signal @param {string} requestId */
async function internalAiRequest(binding, path, body, signal, requestId) {
  const base = requireRedisProxyBaseUrl(binding.env, "AI binding");
  for (let attempt = 1; attempt <= AI_RESOLVE_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(`${base}${path}`, {
        method: "POST",
        headers: withInternalAuth({
          "content-type": JSON_CONTENT_TYPE,
          "x-request-id": requestId,
        }, binding.env),
        body: JSON.stringify(body),
        signal,
      });
      const bytes = response.body
        ? await readBoundedStreamBytes(response.body, AI_INTERNAL_RESPONSE_MAX_BYTES, undefined, signal)
        : new Uint8Array();
      let payload = null;
      try { payload = bytes.byteLength ? JSON.parse(utf8Decoder.decode(bytes)) : null; } catch {}
      if (!response.ok) {
        const code = isRecord(payload) && typeof payload.error === "string"
          ? payload.error
          : "ai_resolver_unavailable";
        let message = isRecord(payload) && typeof payload.message === "string"
          ? payload.message
          : "AI resolver is unavailable";
        const retryable = code === "redis_error" || response.status === 502 || response.status === 504;
        if (retryable && attempt < AI_RESOLVE_ATTEMPTS) continue;
        if (response.status >= 500) {
          message = RESOLVER_FAILURE_MESSAGES.get(code) ?? "AI resolver is unavailable";
        }
        throw new AiBindingError(
          response.status >= 500 ? 503 : response.status,
          code,
          message
        );
      }
      if (payload == null) throw new Error("AI resolver returned an empty response");
      return payload;
    } catch (err) {
      if (err instanceof AiBindingError) throw err;
      if (signal.aborted || attempt === AI_RESOLVE_ATTEMPTS) break;
      await new Promise((resolve) => setTimeout(resolve, 25 * attempt));
    }
  }
  throw new AiBindingError(
    signal.aborted ? 504 : 503,
    signal.aborted ? "ai_resolver_timeout" : "ai_resolver_unavailable",
    signal.aborted ? "AI resolver timed out" : "AI resolver is unavailable"
  );
}

/** @param {AiHostBinding} binding @param {string} model @param {string} protocol @param {string} transport @param {AbortSignal} signal @param {string} requestId */
async function resolveModel(binding, model, protocol, transport, signal, requestId) {
  const request = normalizeAiResolveRequest({
    ns: binding.ctx.props.ns,
    model,
    protocol,
    transport,
  });
  const payload = await internalAiRequest(binding, "/ai/resolve", request, signal, requestId);
  let resolved;
  try { resolved = normalizeAiResolveResponse(payload); } catch {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI resolver returned malformed state");
  }
  const requested = parseAiModelReference(request.model);
  if (
    resolved.provider !== requested.provider ||
    resolved.alias !== requested.alias ||
    resolved.protocol !== protocol ||
    resolved.transport !== transport
  ) {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI resolver returned mismatched state");
  }
  const expected = expectedAiProviderDestination(
    resolved.kind,
    resolved.protocol,
    resolved.transport
  );
  if (!expected || resolved.destination !== expected) {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI resolver returned an invalid destination");
  }
  return resolved;
}

/** @param {AiHostBinding} binding @param {AbortSignal} signal @param {string} requestId */
async function visibleModels(binding, signal, requestId) {
  const request = normalizeAiModelsRequest({
    ns: binding.ctx.props.ns,
  });
  const payload = await internalAiRequest(binding, "/ai/models", request, signal, requestId);
  let parsed;
  try { parsed = normalizeAiModelsResponse(payload); } catch {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI model list is malformed");
  }
  return parsed.models.map((model) => ({
    id: model.id,
    protocol: model.protocol,
    transports: model.transports,
    inputModalities: model.inputModalities,
    outputModalities: model.outputModalities,
    capabilities: model.capabilities,
  }));
}

/** @param {AiBindingEnv} env */
function requireAiNetwork(env) {
  if (!env.AI_NETWORK || typeof env.AI_NETWORK.fetch !== "function") {
    throw new AiBindingError(503, "ai_network_unavailable", "AI public network is not configured");
  }
  return env.AI_NETWORK;
}

/** @param {Response} response @param {AbortController} aborter */
async function readProviderResponseBytes(response, aborter) {
  try {
    return response.body
      ? await readBoundedStreamBytes(
        response.body,
        AI_RESPONSE_MAX_BYTES,
        undefined,
        aborter.signal
      )
      : new Uint8Array();
  } catch (err) {
    aborter.abort(err);
    if (err instanceof BodyTooLargeError) {
      throw new AiBindingError(
        502,
        "ai_provider_response_too_large",
        `AI provider response exceeds ${AI_RESPONSE_MAX_BYTES} bytes`
      );
    }
    throw err;
  }
}

/** @param {Response} response @param {AbortController} aborter @param {AiBindingError} error */
async function rejectProviderResponse(response, aborter, error) {
  aborter.abort(error);
  await discardResponseBody(response);
  throw error;
}

/** @param {AiHostBinding} binding @param {Request} request @param {URL} url @param {string} requestId */
async function handleModels(binding, request, url, requestId) {
  if (
    request.method !== "GET" ||
    url.search !== "" ||
    (request.headers.get("upgrade") || "").toLowerCase() === "websocket"
  ) {
    throw new AiBindingError(405, "ai_method_not_allowed", "AI models requires GET without query parameters");
  }
  const aborter = new AbortController();
  let expired = false;
  let cancelled = false;
  const lease = acquireAiLease(
    binding,
    "request",
    aiRuntimeSetting(binding.env, "AI_REQUEST_BUDGET_MS"),
    () => { expired = true; aborter.abort(); }
  );
  if (!lease) throw new AiBindingError(429, "ai_capacity_exhausted", "AI request capacity is exhausted");
  const abort = () => { cancelled = true; aborter.abort(); };
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  try {
    const models = await visibleModels(binding, aborter.signal, requestId);
    aborter.signal.throwIfAborted();
    return new Response(JSON.stringify({ models }), {
      status: 200,
      headers: { "content-type": JSON_CONTENT_TYPE, "x-request-id": requestId },
    });
  } catch (err) {
    if (expired) throw new AiBindingError(504, "ai_request_timeout", "AI models request timed out");
    throw err;
  } finally {
    request.signal.removeEventListener("abort", abort);
    lease.release(cancelled ? "cancelled" : "completed");
  }
}

/** @param {AiHostBinding} binding @param {Request} request @param {URL} url @param {string} requestId */
async function handleHttp(binding, request, url, requestId) {
  if (request.method !== "POST" || url.search !== "") {
    throw new AiBindingError(405, "ai_method_not_allowed", "AI inference endpoints require POST without query parameters");
  }
  const protocol = protocolForPath(url.pathname);
  if (!protocol) throw new AiBindingError(404, "ai_not_found", "AI endpoint not found");
  const aborter = new AbortController();
  let expired = false;
  let cancelled = false;
  /** @type {() => void} */
  let streamDeadline = () => {};
  /** @type {(reason?: unknown) => void} */
  let streamCancel = () => {};
  const lease = acquireAiLease(binding, "request", aiRuntimeSetting(
    binding.env, "AI_REQUEST_BUDGET_MS"
  ), () => {
    expired = true;
    aborter.abort();
    streamDeadline();
  });
  if (!lease) throw new AiBindingError(429, "ai_capacity_exhausted", "AI request capacity is exhausted");
  const abort = () => {
    cancelled = true;
    aborter.abort();
    streamCancel(request.signal.reason);
  };
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  let streamOwnsLease = false;
  try {
    const body = await requestModelBody(request, aborter.signal);
    const model = requireModel(body.model);
    const stream = body.stream === true;
    if (body.stream !== undefined && typeof body.stream !== "boolean") {
      throw new AiBindingError(400, "ai_invalid_request", "AI stream must be boolean");
    }
    if (protocol === "embeddings" && stream) {
      throw new AiBindingError(400, "ai_transport_unavailable", "AI embeddings do not support streaming");
    }
    if (stream && !lease.transfer("stream")) {
      throw new AiBindingError(429, "ai_capacity_exhausted", "AI stream capacity is exhausted");
    }
    const resolved = await resolveModel(
      binding,
      model,
      protocol,
      stream ? "sse" : "http",
      aborter.signal,
      requestId
    );
    aborter.signal.throwIfAborted();
    const network = requireAiNetwork(binding.env);
    let provider;
    try {
      provider = aiProviderHttpRequest(resolved, body, stream);
    } catch (err) {
      if (err instanceof AiProviderRequestError) {
        throw new AiBindingError(400, err.code, err.message);
      }
      throw err;
    }
    if (utf8ByteLength(provider.body) > AI_REQUEST_MAX_BYTES) {
      throw new AiBindingError(
        413,
        "ai_request_too_large",
        `AI request exceeds ${AI_REQUEST_MAX_BYTES} bytes`
      );
    }
    const response = await network.fetch(provider.destination, {
      method: "POST",
      headers: provider.headers,
      body: provider.body,
      redirect: "manual",
      signal: aborter.signal,
    });
    if (aborter.signal.aborted) {
      await discardResponseBody(response);
      aborter.signal.throwIfAborted();
    }
    if (response.status >= 300 && response.status < 400) {
      await rejectProviderResponse(
        response,
        aborter,
        new AiBindingError(502, "ai_provider_redirect", "AI provider redirect was rejected")
      );
    }
    const headers = aiProviderResponseHeaders(response, requestId);
    if (stream && response.ok) {
      if (!hasContentType(response.headers.get("content-type"), SSE_CONTENT_TYPE) || !response.body) {
        await rejectProviderResponse(
          response,
          aborter,
          new AiBindingError(
            502,
            "ai_provider_invalid_response",
            "AI provider did not return an event stream"
          )
        );
      }
      const streamLifecycle = createAiStreamingResponse({
        response,
        protocol,
        lease,
        aborter,
        idleMs: aiRuntimeSetting(binding.env, "AI_STREAM_IDLE_TIMEOUT_MS"),
        maxDurationMs: aiRuntimeSetting(binding.env, "AI_STREAM_MAX_DURATION_MS"),
        maxBytes: AI_STREAM_MAX_BYTES,
        maxFrameBytes: AI_STREAM_FRAME_MAX_BYTES,
        onCleanup: () => request.signal.removeEventListener("abort", abort),
      });
      streamDeadline = streamLifecycle.deadline;
      streamCancel = streamLifecycle.cancel;
      streamOwnsLease = true;
      return new Response(streamLifecycle.body, { status: response.status, headers });
    }
    const bytes = await readProviderResponseBytes(response, aborter);
    aborter.signal.throwIfAborted();
    return new Response(/** @type {BodyInit} */ (bytes), { status: response.status, headers });
  } catch (err) {
    if (expired) throw new AiBindingError(504, "ai_request_timeout", "AI request timed out");
    throw err;
  } finally {
    if (!streamOwnsLease) {
      request.signal.removeEventListener("abort", abort);
      lease.release(cancelled ? "cancelled" : "completed");
    }
  }
}

/** @param {AiHostBinding} binding @param {Request} request @param {URL} url @param {string} requestId */
async function handleWebSocket(binding, request, url, requestId) {
  if (request.method !== "GET" || (request.headers.get("upgrade") || "").toLowerCase() !== "websocket") {
    throw new AiBindingError(405, "ai_method_not_allowed", "AI websocket endpoints require GET Upgrade");
  }
  const protocol = url.pathname === "/v1/responses"
    ? "responses"
    : url.pathname === "/v1/realtime"
      ? "realtime"
      : null;
  if (!protocol) throw new AiBindingError(404, "ai_not_found", "AI websocket endpoint not found");
  if (
    [...url.searchParams.keys()].some((key) => key !== "model") ||
    url.searchParams.getAll("model").length !== 1
  ) {
    throw new AiBindingError(400, "ai_invalid_request", "AI websocket query contains unsupported fields");
  }
  const model = requireModel(url.searchParams.get("model"), "model query parameter");
  const aborter = new AbortController();
  let expired = false;
  let cancelled = false;
  /** @type {WebSocket | null} */
  let providerSocket = null;
  let closeSession = () => {};
  const lease = acquireAiLease(binding, "websocket", aiRuntimeSetting(
    binding.env, "AI_WS_HANDSHAKE_BUDGET_MS"
  ), () => {
    expired = true;
    aborter.abort();
    closeSession();
  });
  if (!lease) throw new AiBindingError(429, "ai_capacity_exhausted", "AI websocket capacity is exhausted");
  const abort = () => { cancelled = true; aborter.abort(); };
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  try {
    const transport = protocol === "responses" ? "responses_websocket" : "realtime_websocket";
    const resolved = await resolveModel(
      binding,
      model,
      protocol,
      transport,
      aborter.signal,
      requestId
    );
    aborter.signal.throwIfAborted();
    const provider = aiProviderWebSocketRequest(resolved);
    const response = await requireAiNetwork(binding.env).fetch(provider.destination, {
      method: "GET",
      headers: provider.headers,
      redirect: "manual",
      signal: aborter.signal,
    });
    if (aborter.signal.aborted) {
      if (response.status === 101 && response.webSocket) {
        providerSocket = response.webSocket;
        providerSocket.accept();
        closeAiWebSocket(providerSocket, 1012, "AI websocket handshake ended");
      } else {
        await discardResponseBody(response);
      }
      aborter.signal.throwIfAborted();
    }
    if (response.status >= 300 && response.status < 400) {
      await rejectProviderResponse(
        response,
        aborter,
        new AiBindingError(502, "ai_provider_redirect", "AI provider redirect was rejected")
      );
    }
    if (response.status !== 101 || !response.webSocket) {
      const bytes = await readProviderResponseBytes(response, aborter);
      lease.release("provider_rejected");
      return new Response(/** @type {BodyInit} */ (bytes), {
        status: response.status,
        headers: aiProviderResponseHeaders(response, requestId),
      });
    }
    providerSocket = response.webSocket;
    providerSocket.binaryType = "arraybuffer";
    providerSocket.accept();
    request.signal.removeEventListener("abort", abort);
    const operatorMax = aiRuntimeSetting(binding.env, "AI_WS_MAX_DURATION_MS");
    lease.schedule(Math.min(provider.maxDurationMs, operatorMax));
    const bridge = createAiWebSocketBridge({
      model: {
        protocol: resolved.protocol,
        upstreamModel: resolved.upstreamModel,
        binaryFrames: resolved.capabilities.binaryFrames,
      },
      publicModel: model,
      providerSocket,
      aborter,
      idleMs: aiRuntimeSetting(binding.env, "AI_WS_IDLE_TIMEOUT_MS"),
      onFinish: lease.release,
    });
    closeSession = bridge.close;
    return new Response(null, {
      status: 101,
      headers: {
        "x-request-id": requestId,
        [WEBSOCKET_RECONNECT_POLICY_HEADER]: WEBSOCKET_RECONNECT_POLICY_DISABLED,
      },
      webSocket: bridge.client,
    });
  } catch (err) {
    closeAiWebSocket(providerSocket, 1011, "AI websocket setup failed");
    lease.release(expired ? "deadline" : cancelled ? "cancelled" : "setup_error");
    if (expired) throw new AiBindingError(504, "ai_websocket_timeout", "AI websocket handshake timed out");
    throw err;
  } finally {
    request.signal.removeEventListener("abort", abort);
  }
}

/** @param {unknown} err @param {string} requestId */
function bindingErrorResponse(err, requestId) {
  if (err instanceof AiBindingError) {
    return jsonError(err.status, err.code, err.message, { request_id: requestId });
  }
  return jsonError(502, "ai_binding_error", "AI binding request failed", { request_id: requestId });
}

export class AiBinding extends WorkerEntrypoint {
  /** @param {Request} request */
  async fetch(request) {
    const binding = aiBinding(this);
    const requestId = ensureRequestId(request.headers);
    let operation = "fetch";
    try {
      const url = virtualUrl(request);
      const websocket = (request.headers.get("upgrade") || "").toLowerCase() === "websocket";
      if (url.pathname === "/v1/models") operation = "models";
      else if (url.pathname === "/v1/responses") {
        operation = websocket ? "responses_websocket" : "responses";
      }
      else if (url.pathname === "/v1/chat/completions") operation = "chat_completions";
      else if (url.pathname === "/v1/embeddings") operation = "embeddings";
      else if (url.pathname === "/v1/realtime") operation = "realtime_websocket";
      return await recordBindingOperation(
        serviceNameFromEnv(binding.env),
        "ai",
        operation,
        async () => {
          if (url.pathname === "/v1/models") return await handleModels(binding, request, url, requestId);
          if (websocket) return await handleWebSocket(binding, request, url, requestId);
          return await handleHttp(binding, request, url, requestId);
        }
      );
    } catch (err) {
      logStructured(serviceNameFromEnv(binding.env), "warn", "ai_binding_request_rejected", {
        request_id: requestId,
        namespace: binding.ctx.props.ns,
        worker: binding.ctx.props.worker,
        version: binding.ctx.props.version,
        operation,
        code: err instanceof AiBindingError ? err.code : "ai_binding_error",
        error_name: err instanceof Error ? err.name : "Error",
        error_message: errorMessage(err),
      });
      return bindingErrorResponse(err, requestId);
    }
  }
}
