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

/** @param {unknown} value @returns {value is Record<string, unknown>} */
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

/** @param {string[]} supportedModalities @param {string} modality */
function assertInputModalitySupported(supportedModalities, modality) {
  if (!supportedModalities.includes(modality)) {
    throw new AiProviderRequestError(
      "ai_input_modality_unsupported",
      `AI model does not support ${modality} input`
    );
  }
}

/** @param {string[]} supportedModalities @param {unknown} value */
function enforceInputModalitiesInValue(supportedModalities, value) {
  if (typeof value === "string") {
    assertInputModalitySupported(supportedModalities, "text");
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) enforceInputModalitiesInValue(supportedModalities, entry);
    return;
  }
  if (!isRecord(value)) return;
  const record = value;
  const type = typeof record.type === "string" ? record.type : "";
  if (
    type === "text" ||
    type === "input_text" ||
    type === "output_text" ||
    type === "refusal"
  ) {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (type === "image_url" || type === "input_image") {
    assertInputModalitySupported(supportedModalities, "image");
  }
  if (type === "audio" || type === "input_audio") {
    assertInputModalitySupported(supportedModalities, "audio");
  }
  if (type === "file" || type === "input_file") {
    assertInputModalitySupported(supportedModalities, "file");
  }
  if (type === "computer_screenshot") {
    assertInputModalitySupported(supportedModalities, "image");
  }

  // Only documented message/content and replay-output carriers contribute
  // modalities. Tool schemas, arguments, IDs, and encrypted state remain opaque.
  if (Object.hasOwn(record, "content")) {
    enforceInputModalitiesInValue(supportedModalities, record.content);
  }
  if (
    type === "function_call_output" ||
    type === "custom_tool_call_output" ||
    type === "computer_call_output"
  ) {
    enforceInputModalitiesInValue(supportedModalities, record.output);
  }
  if (
    type === "local_shell_call_output" ||
    type === "apply_patch_call_output" ||
    type === "mcp_call"
  ) {
    if (typeof record.output === "string") {
      assertInputModalitySupported(supportedModalities, "text");
    }
  }
  if (type === "mcp_call" && typeof record.error === "string") {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (type === "mcp_list_tools" && typeof record.error === "string") {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (type === "mcp_approval_response" && typeof record.reason === "string") {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (type === "program_output" && typeof record.result === "string") {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (type === "shell_call_output" && Array.isArray(record.output)) {
    for (const output of record.output) {
      if (!isRecord(output)) continue;
      const outputRecord = output;
      if (
        typeof outputRecord.stdout === "string" ||
        typeof outputRecord.stderr === "string"
      ) {
        assertInputModalitySupported(supportedModalities, "text");
      }
    }
  }
  if (type === "code_interpreter_call" && Array.isArray(record.outputs)) {
    for (const output of record.outputs) {
      if (!isRecord(output)) continue;
      const outputRecord = output;
      if (outputRecord.type === "logs" && typeof outputRecord.logs === "string") {
        assertInputModalitySupported(supportedModalities, "text");
      }
      if (outputRecord.type === "image" && typeof outputRecord.url === "string") {
        assertInputModalitySupported(supportedModalities, "image");
      }
    }
  }
  if (type === "image_generation_call" && typeof record.result === "string") {
    assertInputModalitySupported(supportedModalities, "image");
  }
  if (type === "file_search_call" && Array.isArray(record.results)) {
    for (const result of record.results) {
      if (!isRecord(result)) continue;
      const resultRecord = result;
      if (typeof resultRecord.text === "string") {
        assertInputModalitySupported(supportedModalities, "text");
      }
    }
  }
}

/** @param {Record<string, unknown>} body @param {AiResolved} resolved */
function enforceInputModalities(body, resolved) {
  const supportedModalities = resolved.inputModalities;
  if (
    resolved.protocol === "responses" &&
    typeof body.instructions === "string" &&
    body.instructions.length > 0
  ) {
    assertInputModalitySupported(supportedModalities, "text");
  }
  if (Object.hasOwn(body, "input")) {
    if (resolved.protocol === "embeddings") {
      assertInputModalitySupported(supportedModalities, "text");
    } else enforceInputModalitiesInValue(supportedModalities, body.input);
  }
  if (resolved.protocol === "responses" && isRecord(body.prompt)) {
    const prompt = body.prompt;
    const variables = prompt.variables;
    if (isRecord(variables)) {
      const values = variables;
      for (const value of Object.values(values)) {
        enforceInputModalitiesInValue(supportedModalities, value);
      }
    }
  }
  if (Array.isArray(body.messages)) {
    for (const message of body.messages) {
      if (isRecord(message)) {
        const record = message;
        enforceInputModalitiesInValue(supportedModalities, record.content);
        if (resolved.protocol === "chat_completions") {
          if (typeof record.refusal === "string" && record.refusal.length > 0) {
            assertInputModalitySupported(supportedModalities, "text");
          }
          if (isRecord(record.audio)) {
            const audio = record.audio;
            if (typeof audio.id === "string" && audio.id.length > 0) {
              assertInputModalitySupported(supportedModalities, "audio");
            }
          }
        }
      }
    }
  }
}

/** @param {AiResolved} resolved @param {Record<string, unknown>} body */
function enforceProviderRequest(resolved, body) {
  if (body.background === true) {
    throw new AiProviderRequestError(
      "ai_background_unsupported",
      "Background AI responses are not supported"
    );
  }
  enforceInputModalities(body, resolved);
  if (body.previous_response_id != null && !resolved.capabilities.previousResponseId) {
    throw new AiProviderRequestError(
      "ai_continuation_unsupported",
      "AI model does not support previous_response_id"
    );
  }
  if (resolved.kind === "deepseek") {
    if (body.conversation != null) {
      throw new AiProviderRequestError(
        "ai_continuation_unsupported",
        "DeepSeek conversation state is not supported"
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
