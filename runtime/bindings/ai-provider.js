const JSON_CONTENT_TYPE = "application/json";
const SSE_CONTENT_TYPE = "text/event-stream";
const AI_OPENAI_WS_MAX_DURATION_MS = 59 * 60_000;
const AI_XAI_WS_MAX_DURATION_MS = 24 * 60_000;
/** @type {Readonly<Record<string, string>>} */
const PROVIDER_HOSTS = Object.freeze({
  openai: "api.openai.com",
  xai: "api.x.ai",
  deepseek: "api.deepseek.com",
});
/** @type {Readonly<Record<string, string>>} */
const OPENAI_COMPATIBLE_PATHS = Object.freeze({
  responses: "/v1/responses",
  chat_completions: "/v1/chat/completions",
  embeddings: "/v1/embeddings",
  realtime: "/v1/realtime",
});
/** @type {Readonly<Record<string, string>>} */
const DEEPSEEK_PATHS = Object.freeze({
  responses: "/responses",
  chat_completions: "/chat/completions",
});

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
  const host = PROVIDER_HOSTS[kind];
  const paths = kind === "deepseek" ? DEEPSEEK_PATHS : OPENAI_COMPATIBLE_PATHS;
  const path = paths[protocol];
  if (!host || !path || (kind === "deepseek" && websocket)) return null;
  return `${websocket ? "wss" : "https"}://${host}${path}`;
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
  if (
    type === "text" ||
    type === "input_text" ||
    type === "output_text" ||
    type === "refusal"
  ) {
    found.add("text");
  }
  if (type === "image_url" || type === "input_image") found.add("image");
  if (type === "audio" || type === "input_audio") found.add("audio");
  if (type === "file" || type === "input_file") found.add("file");
  if (type === "computer_screenshot") found.add("image");

  // Only documented message/content and replay-output carriers contribute
  // modalities. Tool schemas, arguments, IDs, and encrypted state remain opaque.
  if (Object.hasOwn(record, "content")) collectInputModalities(found, record.content);
  if (
    type === "function_call_output" ||
    type === "custom_tool_call_output" ||
    type === "computer_call_output"
  ) {
    collectInputModalities(found, record.output);
  }
  if (
    type === "local_shell_call_output" ||
    type === "apply_patch_call_output" ||
    type === "mcp_call"
  ) {
    if (typeof record.output === "string") found.add("text");
  }
  if (type === "mcp_call" && typeof record.error === "string") found.add("text");
  if (type === "mcp_list_tools" && typeof record.error === "string") found.add("text");
  if (type === "mcp_approval_response" && typeof record.reason === "string") {
    found.add("text");
  }
  if (type === "program_output" && typeof record.result === "string") found.add("text");
  if (type === "shell_call_output" && Array.isArray(record.output)) {
    for (const output of record.output) {
      if (!isRecord(output)) continue;
      const outputRecord = /** @type {Record<string, unknown>} */ (output);
      if (
        typeof outputRecord.stdout === "string" ||
        typeof outputRecord.stderr === "string"
      ) {
        found.add("text");
      }
    }
  }
  if (type === "code_interpreter_call" && Array.isArray(record.outputs)) {
    for (const output of record.outputs) {
      if (!isRecord(output)) continue;
      const outputRecord = /** @type {Record<string, unknown>} */ (output);
      if (outputRecord.type === "logs" && typeof outputRecord.logs === "string") {
        found.add("text");
      }
      if (outputRecord.type === "image" && typeof outputRecord.url === "string") {
        found.add("image");
      }
    }
  }
  if (type === "image_generation_call" && typeof record.result === "string") {
    found.add("image");
  }
  if (type === "file_search_call" && Array.isArray(record.results)) {
    for (const result of record.results) {
      if (!isRecord(result)) continue;
      const resultRecord = /** @type {Record<string, unknown>} */ (result);
      if (typeof resultRecord.text === "string") found.add("text");
    }
  }
}

/** @param {Record<string, unknown>} body @param {string} protocol */
function requestedInputModalities(body, protocol) {
  const found = new Set();
  if (
    protocol === "responses" &&
    typeof body.instructions === "string" &&
    body.instructions.length > 0
  ) {
    found.add("text");
  }
  if (Object.hasOwn(body, "input")) {
    if (protocol === "embeddings") found.add("text");
    else collectInputModalities(found, body.input);
  }
  if (protocol === "responses" && isRecord(body.prompt)) {
    const prompt = /** @type {Record<string, unknown>} */ (body.prompt);
    const variables = prompt.variables;
    if (isRecord(variables)) {
      const values = /** @type {Record<string, unknown>} */ (variables);
      for (const value of Object.values(values)) {
        collectInputModalities(found, value);
      }
    }
  }
  if (Array.isArray(body.messages)) {
    for (const message of body.messages) {
      if (isRecord(message)) {
        const record = /** @type {Record<string, unknown>} */ (message);
        collectInputModalities(found, record.content);
        if (protocol === "chat_completions") {
          if (typeof record.refusal === "string" && record.refusal.length > 0) {
            found.add("text");
          }
          if (isRecord(record.audio)) {
            const audio = /** @type {Record<string, unknown>} */ (record.audio);
            if (typeof audio.id === "string" && audio.id.length > 0) found.add("audio");
          }
        }
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
  for (const modality of requestedInputModalities(body, resolved.protocol)) {
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
