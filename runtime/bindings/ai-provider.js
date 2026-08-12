const JSON_CONTENT_TYPE = "application/json";
const SSE_CONTENT_TYPE = "text/event-stream";
const AI_OPENAI_WS_MAX_DURATION_MS = 59 * 60_000;
const AI_XAI_WS_MAX_DURATION_MS = 24 * 60_000;

/**
 * @typedef {{
 *   kind: string,
 *   upstreamModel: string,
 *   protocol: string,
 *   transport: string,
 *   destination: string,
 *   credential: string,
 *   inputModalities: string[],
 *   capabilities: Record<string, boolean>,
 * }} AiResolved
 */

export class AiProviderRequestError extends Error {
  /** @param {string} code @param {string} message */
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {string} kind @param {string} protocol @param {string} transport */
export function expectedAiProviderDestination(kind, protocol, transport) {
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

/** @param {Set<string>} found @param {unknown} value */
function collectInputModalities(found, value) {
  if (typeof value === "string") {
    found.add("text");
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) collectInputModalities(found, entry);
    return;
  }
  if (!isRecord(value)) return;
  const record = /** @type {Record<string, unknown>} */ (value);
  const type = typeof record.type === "string" ? record.type : "";
  if (type === "text" || type === "input_text") found.add("text");
  if (type === "image_url" || type === "input_image") found.add("image");
  if (type === "audio" || type === "input_audio") found.add("audio");

  // Only message/content containers carry user media. Tool schemas and tool
  // arguments are opaque provider fields and must not be searched recursively.
  if (Object.hasOwn(record, "content")) collectInputModalities(found, record.content);
  if (type === "function_call_output" && typeof record.output === "string") {
    found.add("text");
  }
}

/** @param {Record<string, unknown>} body */
function requestedInputModalities(body) {
  const found = new Set();
  if (Object.hasOwn(body, "input")) collectInputModalities(found, body.input);
  if (Array.isArray(body.messages)) {
    for (const message of body.messages) {
      if (isRecord(message)) {
        const record = /** @type {Record<string, unknown>} */ (message);
        collectInputModalities(found, record.content);
      }
    }
  }
  return found;
}

/** @param {AiResolved} resolved @param {Record<string, unknown>} body */
function enforceProviderRequest(resolved, body) {
  if (body.background === true) {
    throw new AiProviderRequestError(
      "ai_background_unsupported",
      "Background AI responses are not supported"
    );
  }
  for (const modality of requestedInputModalities(body)) {
    if (!resolved.inputModalities.includes(modality)) {
      throw new AiProviderRequestError(
        "ai_input_modality_unsupported",
        `AI model does not support ${modality} input`
      );
    }
  }
  if (body.previous_response_id != null && !resolved.capabilities.previousResponseId) {
    throw new AiProviderRequestError(
      "ai_continuation_unsupported",
      "AI model does not support previous_response_id"
    );
  }
  if (resolved.kind === "deepseek") {
    if (body.previous_response_id != null || body.conversation != null) {
      throw new AiProviderRequestError(
        "ai_continuation_unsupported",
        "DeepSeek continuation state is not supported"
      );
    }
    if (body.store === true) {
      throw new AiProviderRequestError(
        "ai_store_unsupported",
        "DeepSeek stored responses are not supported"
      );
    }
  }
}

/** @param {AiResolved} resolved @param {Record<string, unknown>} body @param {boolean} stream */
export function aiProviderHttpRequest(resolved, body, stream) {
  enforceProviderRequest(resolved, body);
  return {
    destination: resolved.destination,
    headers: new Headers({
      Accept: stream ? SSE_CONTENT_TYPE : JSON_CONTENT_TYPE,
      Authorization: `Bearer ${resolved.credential}`,
      "Content-Type": JSON_CONTENT_TYPE,
    }),
    body: JSON.stringify({ ...body, model: resolved.upstreamModel }),
  };
}

/** @param {AiResolved} resolved */
export function aiProviderWebSocketRequest(resolved) {
  const destination = new URL(resolved.destination);
  // Fetcher performs a WebSocket handshake through an HTTPS Upgrade request;
  // the resolver retains wss:// as the persisted transport contract.
  destination.protocol = "https:";
  if (resolved.protocol === "realtime") {
    destination.searchParams.set("model", resolved.upstreamModel);
  }
  return {
    destination,
    headers: new Headers({
      Authorization: `Bearer ${resolved.credential}`,
      Upgrade: "websocket",
    }),
    maxDurationMs: resolved.kind === "xai"
      ? AI_XAI_WS_MAX_DURATION_MS
      : AI_OPENAI_WS_MAX_DURATION_MS,
  };
}

/** @param {string | null} value */
function boundedHeader(value) {
  if (!value || value.length > 256 || /[^\x20-\x7e]/.test(value)) return null;
  return value;
}

/** @param {Response} response @param {string} requestId */
export function aiProviderResponseHeaders(response, requestId) {
  const headers = new Headers({ "x-request-id": requestId });
  for (const name of ["content-type", "retry-after", "openai-request-id"]) {
    const value = boundedHeader(response.headers.get(name));
    if (value) headers.set(name, value);
  }
  const providerRequestId = boundedHeader(response.headers.get("x-request-id"));
  if (providerRequestId) headers.set("x-ai-provider-request-id", providerRequestId);
  return headers;
}
