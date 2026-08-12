import { WorkerEntrypoint } from "cloudflare:workers";
import {
  normalizeAiModelsRequest,
  normalizeAiModelsResponse,
  normalizeAiResolveRequest,
  normalizeAiResolveResponse,
} from "shared-ai-contract";
import {
  BodyTooLargeError,
  readBoundedStreamBytes,
  readBoundedText,
} from "shared-bounded-body";
import { errorMessage } from "shared-errors";
import { withInternalAuth } from "shared-internal-auth";
import { ensureRequestId, logStructured } from "shared-observability";
import { discardResponseBody, jsonError } from "shared-respond";
import { recordBindingOperation, metrics } from "runtime-metrics";
import {
  requireRedisProxyBaseUrl,
  serviceNameFromEnv,
} from "runtime-bindings-proxy";

export const AI_REQUEST_MAX_BYTES = 8 * 1024 * 1024;
export const AI_RESPONSE_MAX_BYTES = 16 * 1024 * 1024;
export const AI_STREAM_MAX_BYTES = 32 * 1024 * 1024;
export const AI_STREAM_FRAME_MAX_BYTES = 1024 * 1024;
export const AI_WS_FRAME_MAX_BYTES = 1024 * 1024;
export const AI_WS_MAX_BYTES = 128 * 1024 * 1024;

const AI_INTERNAL_RESPONSE_MAX_BYTES = 512 * 1024;
const AI_REQUEST_MAX_IN_FLIGHT_DEFAULT = 32;
const AI_STREAM_MAX_IN_FLIGHT_DEFAULT = 16;
const AI_WS_MAX_SESSIONS_DEFAULT = 8;
const AI_REQUEST_BUDGET_MS_DEFAULT = 120_000;
const AI_STREAM_IDLE_TIMEOUT_MS_DEFAULT = 30_000;
const AI_STREAM_MAX_DURATION_MS_DEFAULT = 300_000;
const AI_WS_HANDSHAKE_BUDGET_MS_DEFAULT = 15_000;
const AI_WS_IDLE_TIMEOUT_MS_DEFAULT = 120_000;
const AI_WS_MAX_DURATION_MS_DEFAULT = 24 * 60_000;
const AI_OPENAI_WS_MAX_DURATION_MS = 59 * 60_000;
const AI_XAI_WS_MAX_DURATION_MS = 24 * 60_000;
const AI_RESOLVE_ATTEMPTS = 2;
const AI_VIRTUAL_ORIGIN = "https://ai.wdl";
const JSON_CONTENT_TYPE = "application/json";
const SSE_CONTENT_TYPE = "text/event-stream";
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();

/** @typedef {"request" | "stream" | "websocket"} AiPoolName */
/** @typedef {{ inUse: number, highWater: number }} AiPoolState */
/** @typedef {{ ns: string, worker: string, version: string, binding: string }} AiBindingProps */
/** @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} AiNetwork */
/** @typedef {Record<string, unknown> & { AI_NETWORK?: AiNetwork, SERVICE_NAME?: unknown, REDIS_PROXY_URL?: unknown, WDL_INTERNAL_AUTH_TOKEN?: unknown }} AiBindingEnv */
/** @typedef {{ ctx: { props: AiBindingProps, waitUntil(promise: Promise<unknown>): void }, env: AiBindingEnv }} AiHostBinding */
/** @typedef {ReturnType<typeof normalizeAiResolveResponse>} AiResolved */

const poolStates = Object.freeze({
  request: { inUse: 0, highWater: 0 },
  stream: { inUse: 0, highWater: 0 },
  websocket: { inUse: 0, highWater: 0 },
});

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

/** @param {AiBindingEnv} env @param {string} name @param {number} fallback @param {number} max */
function setting(env, name, fallback, max) {
  const raw = Number(env[name] ?? fallback);
  if (!Number.isFinite(raw) || raw <= 0) return fallback;
  return Math.max(1, Math.min(Math.trunc(raw), max));
}

/** @param {AiBindingEnv} env @param {AiPoolName} pool */
function poolLimit(env, pool) {
  if (pool === "request") {
    return setting(env, "AI_REQUEST_MAX_IN_FLIGHT", AI_REQUEST_MAX_IN_FLIGHT_DEFAULT, 4096);
  }
  if (pool === "stream") {
    return setting(env, "AI_STREAM_MAX_IN_FLIGHT", AI_STREAM_MAX_IN_FLIGHT_DEFAULT, 1024);
  }
  return setting(env, "AI_WS_MAX_SESSIONS", AI_WS_MAX_SESSIONS_DEFAULT, 1024);
}

/** @param {AiBindingEnv} env @param {AiPoolName} pool */
function updatePoolGauge(env, pool) {
  const state = poolStates[pool];
  const labels = { service: serviceNameFromEnv(env), pool };
  metrics.setGauge("ai_pool_in_use", labels, state.inUse);
  metrics.setGauge("ai_pool_high_water", labels, state.highWater);
}

/**
 * The returned deadline task is registered before resolution or provider I/O.
 * It owns the final abort/release path if caller cancellation is not delivered.
 *
 * @param {AiHostBinding} binding
 * @param {AiPoolName} pool
 * @param {number} durationMs
 * @param {() => void} onDeadline
 */
function acquireLease(binding, pool, durationMs, onDeadline) {
  const env = binding.env;
  const state = poolStates[pool];
  if (state.inUse >= poolLimit(env, pool)) {
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool, outcome: "saturated",
    });
    return null;
  }
  state.inUse += 1;
  state.highWater = Math.max(state.highWater, state.inUse);
  updatePoolGauge(env, pool);
  metrics.increment("ai_pool_events", {
    service: serviceNameFromEnv(env), pool, outcome: "acquired",
  });

  /** @type {AiPoolName} */
  let activePool = pool;
  let released = false;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let timer = null;
  /** @type {() => void} */
  let settle = () => {};
  /** @type {Promise<void>} */
  const task = new Promise((resolve) => { settle = () => resolve(); });

  /** @param {string} outcome */
  const release = (outcome) => {
    if (released) return false;
    released = true;
    if (timer !== null) clearTimeout(timer);
    timer = null;
    const activeState = poolStates[activePool];
    activeState.inUse = Math.max(0, activeState.inUse - 1);
    updatePoolGauge(env, activePool);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: activePool, outcome,
    });
    settle();
    return true;
  };

  /** @param {AiPoolName} nextPool */
  const transfer = (nextPool) => {
    if (released) return false;
    if (nextPool === activePool) return true;
    const nextState = poolStates[nextPool];
    if (nextState.inUse >= poolLimit(env, nextPool)) {
      metrics.increment("ai_pool_events", {
        service: serviceNameFromEnv(env), pool: nextPool, outcome: "saturated",
      });
      return false;
    }
    const previousPool = activePool;
    const previousState = poolStates[previousPool];
    previousState.inUse = Math.max(0, previousState.inUse - 1);
    updatePoolGauge(env, previousPool);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: previousPool, outcome: "transferred",
    });

    activePool = nextPool;
    nextState.inUse += 1;
    nextState.highWater = Math.max(nextState.highWater, nextState.inUse);
    updatePoolGauge(env, nextPool);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: nextPool, outcome: "acquired",
    });
    return true;
  };

  /** @param {number} ms */
  const schedule = (ms) => {
    if (released) return;
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      if (released) return;
      try { onDeadline(); } catch {}
      release("deadline");
    }, ms);
  };
  schedule(durationMs);
  binding.ctx.waitUntil(task);
  return { release, schedule, transfer, get released() { return released; } };
}

/** @lintignore data-URL unit tests import this hook. */
export function resetAiPoolStateForTest() {
  for (const state of Object.values(poolStates)) {
    state.inUse = 0;
    state.highWater = 0;
  }
}

/** @lintignore data-URL unit tests import this hook. */
export function aiPoolStateForTest() {
  return Object.fromEntries(Object.entries(poolStates).map(([name, state]) => [name, { ...state }]));
}

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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
function requestModelBody(request, signal) {
  if (!hasContentType(request.headers.get("content-type"), JSON_CONTENT_TYPE)) {
    throw new AiBindingError(415, "ai_unsupported_media_type", "AI request content-type must be application/json");
  }
  return readBoundedText(request, AI_REQUEST_MAX_BYTES, signal).catch((err) => {
    if (err instanceof BodyTooLargeError) {
      throw new AiBindingError(413, "ai_request_too_large", `AI request exceeds ${AI_REQUEST_MAX_BYTES} bytes`);
    }
    throw err;
  }).then((text) => {
    let parsed;
    try { parsed = JSON.parse(text); } catch {
      throw new AiBindingError(400, "ai_invalid_json", "AI request body must be valid JSON");
    }
    if (!isRecord(parsed)) throw new AiBindingError(400, "ai_invalid_request", "AI request body must be an object");
    return /** @type {Record<string, unknown>} */ (parsed);
  });
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
  return value;
}

/** @param {string} kind @param {string} protocol @param {string} transport */
function expectedDestination(kind, protocol, transport) {
  const websocket = transport === "responses_websocket" || transport === "realtime_websocket";
  if (kind === "deepseek") {
    if (websocket || protocol === "realtime" || protocol === "embeddings") return null;
    return `https://api.deepseek.com${protocol === "responses" ? "/responses" : "/chat/completions"}`;
  }
  const host = kind === "openai" ? "api.openai.com" : kind === "xai" ? "api.x.ai" : null;
  if (!host) return null;
  const path = protocol === "responses"
    ? "/v1/responses"
    : protocol === "chat_completions"
      ? "/v1/chat/completions"
      : protocol === "embeddings"
        ? "/v1/embeddings"
        : protocol === "realtime"
          ? "/v1/realtime"
          : null;
  return path ? `${websocket ? "wss" : "https"}://${host}${path}` : null;
}

/** @param {AiHostBinding} binding @param {string} path @param {unknown} body @param {AbortSignal} signal */
async function internalAiRequest(binding, path, body, signal) {
  const base = requireRedisProxyBaseUrl(binding.env, "AI binding");
  for (let attempt = 1; attempt <= AI_RESOLVE_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(`${base}${path}`, {
        method: "POST",
        headers: withInternalAuth({ "content-type": JSON_CONTENT_TYPE }, binding.env),
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
        const message = isRecord(payload) && typeof payload.message === "string"
          ? payload.message
          : "AI resolver is unavailable";
        const retryable = code === "redis_error" || response.status === 502 || response.status === 504;
        if (retryable && attempt < AI_RESOLVE_ATTEMPTS) continue;
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

/** @param {AiHostBinding} binding @param {string} model @param {string} protocol @param {string} transport @param {AbortSignal} signal */
async function resolveModel(binding, model, protocol, transport, signal) {
  const request = normalizeAiResolveRequest({
    ns: binding.ctx.props.ns,
    binding: binding.ctx.props.binding,
    model,
    protocol,
    transport,
  });
  const payload = await internalAiRequest(binding, "/ai/resolve", request, signal);
  let resolved;
  try { resolved = normalizeAiResolveResponse(payload); } catch {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI resolver returned malformed state");
  }
  const expected = expectedDestination(resolved.kind, resolved.protocol, resolved.transport);
  if (!expected || resolved.destination !== expected) {
    throw new AiBindingError(503, "ai_resolver_invalid", "AI resolver returned an invalid destination");
  }
  return resolved;
}

/** @param {AiHostBinding} binding @param {AbortSignal} signal */
async function visibleModels(binding, signal) {
  const request = normalizeAiModelsRequest({
    ns: binding.ctx.props.ns,
    binding: binding.ctx.props.binding,
  });
  const payload = await internalAiRequest(binding, "/ai/models", request, signal);
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

/** @param {Record<string, unknown>} body */
function requestedModalities(body) {
  const found = new Set(["text"]);
  const pending = [body.input, body.messages];
  while (pending.length > 0) {
    const value = pending.pop();
    if (Array.isArray(value)) {
      for (const entry of value) pending.push(entry);
      continue;
    }
    if (!isRecord(value)) continue;
    const record = /** @type {Record<string, unknown>} */ (value);
    const type = typeof record.type === "string" ? record.type : "";
    if (type.includes("image") || type.includes("file") || Object.hasOwn(record, "image_url")) {
      found.add("image");
    }
    if (type.includes("audio") || Object.hasOwn(record, "input_audio")) found.add("audio");
    for (const entry of Object.values(record)) pending.push(entry);
  }
  return found;
}

/** @param {AiResolved} resolved @param {Record<string, unknown>} body */
function enforceProviderRequest(resolved, body) {
  if (body.background === true) {
    throw new AiBindingError(400, "ai_background_unsupported", "Background AI responses are not supported");
  }
  for (const modality of requestedModalities(body)) {
    if (!resolved.inputModalities.includes(modality)) {
      throw new AiBindingError(400, "ai_input_modality_unsupported", `AI model does not support ${modality} input`);
    }
  }
  if (body.previous_response_id != null && !resolved.capabilities.previousResponseId) {
    throw new AiBindingError(400, "ai_continuation_unsupported", "AI model does not support previous_response_id");
  }
  if (resolved.kind === "deepseek") {
    if (body.previous_response_id != null || body.conversation != null) {
      throw new AiBindingError(400, "ai_continuation_unsupported", "DeepSeek continuation state is not supported");
    }
    if (body.store === true) {
      throw new AiBindingError(400, "ai_store_unsupported", "DeepSeek stored responses are not supported");
    }
  }
}

/** @param {AiResolved} resolved @param {Record<string, unknown>} body */
function providerBody(resolved, body) {
  enforceProviderRequest(resolved, body);
  return JSON.stringify({ ...body, model: resolved.upstreamModel });
}

/** @param {AiResolved} resolved @param {boolean} stream */
function providerHeaders(resolved, stream) {
  return new Headers({
    Accept: stream ? SSE_CONTENT_TYPE : JSON_CONTENT_TYPE,
    Authorization: `Bearer ${resolved.credential}`,
    "Content-Type": JSON_CONTENT_TYPE,
  });
}

/** @param {string | null} value */
function boundedHeader(value) {
  if (!value || value.length > 256 || /[^\x20-\x7e]/.test(value)) return null;
  return value;
}

/** @param {Response} response @param {string} requestId */
function providerResponseHeaders(response, requestId) {
  const headers = new Headers({ "x-request-id": requestId });
  for (const name of ["content-type", "retry-after", "openai-request-id", "x-request-id"]) {
    const value = boundedHeader(response.headers.get(name));
    if (value) headers.set(name, value);
  }
  headers.set("x-request-id", requestId);
  return headers;
}

/** @param {AiBindingEnv} env */
function requireAiNetwork(env) {
  if (!env.AI_NETWORK || typeof env.AI_NETWORK.fetch !== "function") {
    throw new AiBindingError(503, "ai_network_unavailable", "AI public network is not configured");
  }
  return env.AI_NETWORK;
}

/** @param {Uint8Array} left @param {Uint8Array} right */
function appendBytes(left, right) {
  if (left.byteLength === 0) return right;
  const out = new Uint8Array(left.byteLength + right.byteLength);
  out.set(left, 0);
  out.set(right, left.byteLength);
  return out;
}

/** @param {Uint8Array} bytes */
function sseFrameEnd(bytes) {
  let lineStart = 0;
  for (let index = 0; index < bytes.byteLength;) {
    const byte = bytes[index];
    if (byte !== 0x0a && byte !== 0x0d) {
      index += 1;
      continue;
    }
    if (byte === 0x0d && index + 1 >= bytes.byteLength) return -1;
    const end = byte === 0x0d && bytes[index + 1] === 0x0a ? index + 2 : index + 1;
    if (index === lineStart) return end;
    lineStart = end;
    index = end;
  }
  return -1;
}

/**
 * @param {Uint8Array} frame
 * @param {string} protocol
 * @returns {"completed" | "provider_error" | null}
 */
function sseTerminalOutcome(frame, protocol) {
  const text = utf8Decoder.decode(frame);
  let event = "";
  const data = [];
  for (const rawLine of text.split(/\r\n|\r|\n/)) {
    if (rawLine.startsWith("event:")) event = rawLine.slice(6).trimStart();
    if (rawLine.startsWith("data:")) data.push(rawLine.slice(5).trimStart());
  }
  if (data.length === 0) return null;
  const joined = data.join("\n");
  if (protocol === "chat_completions") {
    if (joined.trim() === "[DONE]") return "completed";
    JSON.parse(joined);
    return null;
  }
  const payload = JSON.parse(joined);
  const type = event || (isRecord(payload) && typeof payload.type === "string" ? payload.type : "");
  if (type === "error") return "provider_error";
  return type === "response.completed" || type === "response.incomplete" ||
      type === "response.failed"
    ? "completed"
    : null;
}

/**
 * @param {Response} response
 * @param {string} protocol
 * @param {ReturnType<typeof acquireLease>} lease
 * @param {AbortController} aborter
 * @param {AiBindingEnv} env
 * @param {() => void} onCleanup
 */
function streamingResponse(response, protocol, lease, aborter, env, onCleanup) {
  if (!lease || !response.body) throw new Error("AI stream lifecycle is not configured");
  const reader = response.body.getReader();
  /** @type {Uint8Array<ArrayBufferLike>} */
  let pending = new Uint8Array();
  let total = 0;
  let terminal = false;
  let closed = false;
  /** @type {ReadableStreamDefaultController<Uint8Array> | null} */
  let output = null;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let idleTimer = null;
  const idleMs = setting(env, "AI_STREAM_IDLE_TIMEOUT_MS", AI_STREAM_IDLE_TIMEOUT_MS_DEFAULT, 30 * 60_000);

  /** @param {string} outcome */
  const cleanup = (outcome) => {
    if (closed) return;
    closed = true;
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = null;
    lease.release(outcome);
    try { reader.releaseLock(); } catch {}
    try { onCleanup(); } catch {}
  };
  /** @param {Error} error @param {string} outcome */
  const fail = (error, outcome) => {
    if (closed) return;
    try { aborter.abort(error); } catch {}
    try { void reader.cancel(error).catch(() => {}); } catch {}
    try { output?.error(error); } catch {}
    cleanup(outcome);
  };
  const resetIdle = () => {
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      fail(new Error("AI stream idle timeout"), "idle_timeout");
    }, idleMs);
  };
  resetIdle();
  lease.schedule(setting(env, "AI_STREAM_MAX_DURATION_MS", AI_STREAM_MAX_DURATION_MS_DEFAULT, 60 * 60_000));

  const body = new ReadableStream({
    start(controller) { output = controller; },
    async pull(controller) {
      try {
        for (;;) {
          const frameEnd = sseFrameEnd(pending);
          if (frameEnd >= 0) {
            if (frameEnd > AI_STREAM_FRAME_MAX_BYTES) {
              throw new Error(`AI stream frame exceeds ${AI_STREAM_FRAME_MAX_BYTES} bytes`);
            }
            const frame = pending.slice(0, frameEnd);
            pending = pending.slice(frameEnd);
            const terminalOutcome = sseTerminalOutcome(frame, protocol);
            controller.enqueue(frame);
            if (terminalOutcome !== null) {
              terminal = true;
              try { void reader.cancel("AI stream terminal event").catch(() => {}); } catch {}
              cleanup(terminalOutcome);
              controller.close();
            }
            return;
          }
          if (pending.byteLength > AI_STREAM_FRAME_MAX_BYTES) {
            throw new Error(`AI stream frame exceeds ${AI_STREAM_FRAME_MAX_BYTES} bytes`);
          }
          const { value, done } = await reader.read();
          if (done) {
            if (!terminal) throw new Error("AI stream ended before its terminal event");
            cleanup("completed");
            controller.close();
            return;
          }
          const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
          total += chunk.byteLength;
          if (total > AI_STREAM_MAX_BYTES) {
            throw new Error(`AI stream exceeds ${AI_STREAM_MAX_BYTES} bytes`);
          }
          pending = appendBytes(pending, chunk);
          resetIdle();
        }
      } catch (err) {
        fail(err instanceof Error ? err : new Error(errorMessage(err)), "stream_error");
      }
    },
    async cancel(reason) {
      try { await reader.cancel(reason); } catch {}
      cleanup("cancelled");
    },
  });
  return {
    body,
    /** @param {unknown} reason */
    cancel(reason) {
      const error = reason instanceof Error
        ? reason
        : new DOMException("This operation was aborted", "AbortError");
      fail(error, "cancelled");
    },
    deadline() {
      fail(new Error("AI stream duration exceeded"), "deadline");
    },
  };
}

/** @param {unknown} data */
function websocketFrame(data) {
  if (typeof data === "string") {
    const bytes = utf8Encoder.encode(data).byteLength;
    return { kind: "text", data, bytes };
  }
  if (data instanceof ArrayBuffer) {
    return { kind: "binary", data, bytes: data.byteLength };
  }
  if (ArrayBuffer.isView(data)) {
    const view = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    return { kind: "binary", data: view.slice().buffer, bytes: view.byteLength };
  }
  throw new Error("AI websocket frame type is unsupported");
}

/** @param {unknown} code */
function sendableCloseCode(code) {
  const value = Number(code);
  return Number.isInteger(value) && value >= 1000 && value < 5000 &&
    ![1004, 1005, 1006, 1015].includes(value)
    ? value
    : 1011;
}

/** @param {unknown} reason */
function closeReason(reason) {
  const text = typeof reason === "string" ? reason : "AI websocket closed";
  let out = "";
  for (const character of text) {
    if (utf8Encoder.encode(out + character).byteLength > 123) break;
    out += character;
  }
  return out;
}

/** @param {WebSocket | null | undefined} socket @param {number} code @param {string} reason */
function closeSocket(socket, code, reason) {
  if (!socket) return;
  try {
    if (Number(code) === 1005) socket.close();
    else socket.close(sendableCloseCode(code), closeReason(reason));
  } catch {}
}

/** @param {AiResolved} resolved @param {string} publicModel @param {string} text */
function normalizeClientSocketText(resolved, publicModel, text) {
  let payload;
  try { payload = JSON.parse(text); } catch {
    throw new AiBindingError(400, "ai_invalid_websocket_frame", "AI websocket text frames must be JSON");
  }
  if (!isRecord(payload) || typeof payload.type !== "string") {
    throw new AiBindingError(400, "ai_invalid_websocket_frame", "AI websocket frame must contain type");
  }
  /** @param {unknown} value */
  const allowedModel = (value) => value == null || value === publicModel || value === resolved.upstreamModel;
  if (!allowedModel(payload.model)) {
    throw new AiBindingError(400, "ai_websocket_model_pinned", "AI websocket model is fixed for this connection");
  }
  if (isRecord(payload.session) && !allowedModel(payload.session.model)) {
    throw new AiBindingError(400, "ai_websocket_model_pinned", "AI realtime model is fixed for this connection");
  }
  if (resolved.protocol === "responses" && payload.type === "response.create") {
    payload.model = resolved.upstreamModel;
  }
  if (resolved.protocol === "realtime" && isRecord(payload.session) && payload.session.model != null) {
    payload.session = { ...payload.session, model: resolved.upstreamModel };
  }
  return JSON.stringify(payload);
}

/**
 * @param {AiResolved} resolved
 * @param {string} publicModel
 * @param {WebSocket} upstream
 * @param {ReturnType<typeof acquireLease>} lease
 * @param {AbortController} aborter
 * @param {AiBindingEnv} env
 */
function bridgeWebSockets(resolved, publicModel, upstream, lease, aborter, env) {
  if (!lease) throw new Error("AI websocket lifecycle is not configured");
  const pair = new WebSocketPair();
  const client = pair[0];
  const downstream = pair[1];
  downstream.binaryType = "arraybuffer";
  upstream.binaryType = "arraybuffer";
  downstream.accept();
  let closed = false;
  let clientBytes = 0;
  let providerBytes = 0;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let idleTimer = null;
  const idleMs = setting(env, "AI_WS_IDLE_TIMEOUT_MS", AI_WS_IDLE_TIMEOUT_MS_DEFAULT, 30 * 60_000);

  const resetIdle = () => {
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => finish(1012, "AI websocket idle timeout", "idle_timeout"), idleMs);
  };
  /** @param {number} code @param {string} reason @param {string} outcome */
  const finish = (code, reason, outcome) => {
    if (closed) return;
    closed = true;
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = null;
    try { aborter.abort(reason); } catch {}
    closeSocket(downstream, code, reason);
    closeSocket(upstream, code, reason);
    lease.release(outcome);
  };
  /** @param {WebSocket} target @param {"client" | "provider"} direction @param {MessageEvent} evt */
  const forward = (target, direction, evt) => {
    if (closed) return;
    try {
      const frame = websocketFrame(evt.data);
      if (frame.bytes > AI_WS_FRAME_MAX_BYTES) {
        finish(1009, "AI websocket frame too large", "frame_limit");
        return;
      }
      if (direction === "client") {
        if (frame.kind === "binary" && !resolved.capabilities.binaryFrames) {
          finish(1003, "AI model does not support binary frames", "unsupported_frame");
          return;
        }
        const forwarded = frame.kind === "text"
          ? normalizeClientSocketText(resolved, publicModel, String(frame.data))
          : frame.data;
        const forwardedFrame = websocketFrame(forwarded);
        if (forwardedFrame.bytes > AI_WS_FRAME_MAX_BYTES) {
          finish(1009, "AI websocket frame too large", "frame_limit");
          return;
        }
        clientBytes += forwardedFrame.bytes;
        if (clientBytes > AI_WS_MAX_BYTES) {
          finish(1009, "AI websocket byte limit", "byte_limit");
          return;
        }
        target.send(forwardedFrame.data);
      } else {
        providerBytes += frame.bytes;
        if (providerBytes > AI_WS_MAX_BYTES) {
          finish(1009, "AI websocket byte limit", "byte_limit");
          return;
        }
        if (frame.kind === "binary" && !resolved.capabilities.binaryFrames) {
          finish(1003, "AI model does not support binary frames", "unsupported_frame");
          return;
        }
        target.send(frame.data);
      }
      resetIdle();
    } catch {
      finish(1008, "AI websocket frame rejected", "frame_error");
    }
  };

  downstream.addEventListener("message", (evt) => forward(upstream, "client", evt));
  upstream.addEventListener("message", (evt) => forward(downstream, "provider", evt));
  downstream.addEventListener("close", (evt) => finish(evt.code, evt.reason, "client_closed"));
  upstream.addEventListener("close", (evt) => finish(evt.code, evt.reason, "provider_closed"));
  downstream.addEventListener("error", () => finish(1011, "AI websocket client error", "client_error"));
  upstream.addEventListener("error", () => finish(1011, "AI websocket provider error", "provider_error"));
  resetIdle();
  return { client, close: () => finish(1012, "AI websocket deadline", "deadline") };
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
  const lease = acquireLease(binding, "request", setting(
    binding.env, "AI_REQUEST_BUDGET_MS", AI_REQUEST_BUDGET_MS_DEFAULT, 10 * 60_000
  ), () => { expired = true; aborter.abort(); });
  if (!lease) throw new AiBindingError(429, "ai_capacity_exhausted", "AI request capacity is exhausted");
  const abort = () => { cancelled = true; aborter.abort(); };
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  try {
    const models = await visibleModels(binding, aborter.signal);
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
  const lease = acquireLease(binding, "request", setting(
    binding.env, "AI_REQUEST_BUDGET_MS", AI_REQUEST_BUDGET_MS_DEFAULT, 10 * 60_000
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
    const resolved = await resolveModel(binding, model, protocol, stream ? "sse" : "http", aborter.signal);
    aborter.signal.throwIfAborted();
    const network = requireAiNetwork(binding.env);
    const response = await network.fetch(resolved.destination, {
      method: "POST",
      headers: providerHeaders(resolved, stream),
      body: providerBody(resolved, body),
      redirect: "manual",
      signal: aborter.signal,
    });
    if (aborter.signal.aborted) {
      await discardResponseBody(response);
      aborter.signal.throwIfAborted();
    }
    if (response.status >= 300 && response.status < 400) {
      await discardResponseBody(response);
      throw new AiBindingError(502, "ai_provider_redirect", "AI provider redirect was rejected");
    }
    const headers = providerResponseHeaders(response, requestId);
    if (stream && response.ok) {
      if (!hasContentType(response.headers.get("content-type"), SSE_CONTENT_TYPE) || !response.body) {
        await discardResponseBody(response);
        throw new AiBindingError(502, "ai_provider_invalid_response", "AI provider did not return an event stream");
      }
      const streamLifecycle = streamingResponse(
        response,
        protocol,
        lease,
        aborter,
        binding.env,
        () => request.signal.removeEventListener("abort", abort)
      );
      streamDeadline = streamLifecycle.deadline;
      streamCancel = streamLifecycle.cancel;
      streamOwnsLease = true;
      return new Response(streamLifecycle.body, { status: response.status, headers });
    }
    const bytes = response.body
      ? await readBoundedStreamBytes(response.body, AI_RESPONSE_MAX_BYTES, undefined, aborter.signal)
      : new Uint8Array();
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
  if ([...url.searchParams.keys()].some((key) => key !== "model")) {
    throw new AiBindingError(400, "ai_invalid_request", "AI websocket query contains unsupported fields");
  }
  const model = requireModel(url.searchParams.get("model"), "model query parameter");
  const aborter = new AbortController();
  let expired = false;
  let cancelled = false;
  /** @type {WebSocket | null} */
  let providerSocket = null;
  let closeSession = () => {};
  const lease = acquireLease(binding, "websocket", setting(
    binding.env, "AI_WS_HANDSHAKE_BUDGET_MS", AI_WS_HANDSHAKE_BUDGET_MS_DEFAULT, 120_000
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
    const resolved = await resolveModel(binding, model, protocol, transport, aborter.signal);
    aborter.signal.throwIfAborted();
    const destination = new URL(resolved.destination);
    // Fetcher performs a WebSocket handshake through an HTTPS Upgrade request;
    // the resolver retains wss:// as the persisted transport contract.
    destination.protocol = "https:";
    if (protocol === "realtime") destination.searchParams.set("model", resolved.upstreamModel);
    const response = await requireAiNetwork(binding.env).fetch(destination, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${resolved.credential}`,
        Upgrade: "websocket",
      },
      redirect: "manual",
      signal: aborter.signal,
    });
    if (aborter.signal.aborted) {
      if (response.status === 101 && response.webSocket) {
        providerSocket = response.webSocket;
        providerSocket.accept();
        closeSocket(providerSocket, 1012, "AI websocket handshake ended");
      } else {
        await discardResponseBody(response);
      }
      aborter.signal.throwIfAborted();
    }
    if (response.status !== 101 || !response.webSocket) {
      const bytes = response.body
        ? await readBoundedStreamBytes(response.body, AI_RESPONSE_MAX_BYTES, undefined, aborter.signal)
        : new Uint8Array();
      lease.release("provider_rejected");
      return new Response(/** @type {BodyInit} */ (bytes), {
        status: response.status,
        headers: providerResponseHeaders(response, requestId),
      });
    }
    providerSocket = response.webSocket;
    providerSocket.binaryType = "arraybuffer";
    providerSocket.accept();
    request.signal.removeEventListener("abort", abort);
    const adapterMax = resolved.kind === "xai"
      ? AI_XAI_WS_MAX_DURATION_MS
      : AI_OPENAI_WS_MAX_DURATION_MS;
    const operatorMax = setting(
      binding.env, "AI_WS_MAX_DURATION_MS", AI_WS_MAX_DURATION_MS_DEFAULT, 2 * 60 * 60_000
    );
    lease.schedule(Math.min(adapterMax, operatorMax));
    const bridge = bridgeWebSockets(resolved, model, providerSocket, lease, aborter, binding.env);
    closeSession = bridge.close;
    return new Response(null, {
      status: 101,
      headers: { "x-request-id": requestId },
      webSocket: bridge.client,
    });
  } catch (err) {
    closeSocket(providerSocket, 1011, "AI websocket setup failed");
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
      if (url.pathname === "/v1/models") operation = "models";
      else if (url.pathname === "/v1/responses") operation = "responses";
      else if (url.pathname === "/v1/chat/completions") operation = "chat_completions";
      else if (url.pathname === "/v1/embeddings") operation = "embeddings";
      else if (url.pathname === "/v1/realtime") operation = "realtime_websocket";
      return await recordBindingOperation(
        serviceNameFromEnv(binding.env),
        "ai",
        operation,
        async () => {
          if (url.pathname === "/v1/models") return await handleModels(binding, request, url, requestId);
          const websocket = (request.headers.get("upgrade") || "").toLowerCase() === "websocket";
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
