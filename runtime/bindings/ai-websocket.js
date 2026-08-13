import { utf8ByteLength } from "shared-utf8";

export const AI_WS_FRAME_MAX_BYTES = 1024 * 1024;
export const AI_WS_MAX_JSON_DEPTH = 128;
export const AI_WS_MAX_BYTES = 64 * 1024 * 1024;

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {unknown} data */
function websocketFrame(data) {
  if (typeof data === "string") {
    const bytes = utf8ByteLength(data);
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
  let bytes = 0;
  let out = "";
  for (const character of text) {
    const characterBytes = utf8ByteLength(character);
    if (bytes + characterBytes > 123) break;
    bytes += characterBytes;
    out += character;
  }
  return out;
}

/** @param {WebSocket | null | undefined} socket @param {number} code @param {string} reason */
export function closeAiWebSocket(socket, code, reason) {
  if (!socket) return;
  try {
    if (Number(code) === 1005) socket.close();
    else socket.close(sendableCloseCode(code), closeReason(reason));
  } catch {}
}

/** @param {unknown} code */
function providerCloseCode(code) {
  const value = Number(code);
  if (value === 1005) return 1005;
  const normalized = sendableCloseCode(value);
  return value === 1001 || value === 1006 || value === 1011 || normalized === 1011
    ? 1013
    : normalized;
}

/** @param {string} text @param {number} start */
function jsonStringEnd(text, start) {
  let index = start + 1;
  while (index < text.length) {
    if (text[index] === "\\") index += 2;
    else if (text[index] === '"') return index + 1;
    else index += 1;
  }
  return text.length;
}

/** @param {string} text @param {number} start */
function jsonPrimitiveEnd(text, start) {
  let index = start + 1;
  while (index < text.length) {
    const character = text[index];
    if (
      character === " " || character === "\t" || character === "\n" ||
      character === "\r" || character === "," || character === "]" ||
      character === "}"
    ) break;
    index += 1;
  }
  return index;
}

/** @param {string} character */
function isJsonWhitespace(character) {
  return character === " " || character === "\t" || character === "\n" || character === "\r";
}

/** @param {"root" | "session" | "other"} scope @param {string} key */
function trackedJsonField(scope, key) {
  if (scope === "root") {
    if (key === "type") return 1;
    if (key === "model") return 2;
    if (key === "session") return 4;
  } else if (scope === "session" && key === "model") {
    return 1;
  }
  return 0;
}

/**
 * Locate only the model fields owned by the bridge. JSON.parse() already owns
 * syntax validation; this lexical pass rejects duplicate decision fields and
 * lets model replacement preserve every unrelated source token exactly.
 *
 * @param {string} text
 */
function inspectClientSocketJson(text) {
  /** @type {Array<{
   *   kind: "object" | "array",
   *   scope: "root" | "session" | "other",
   *   expectKey: boolean,
   *   pendingKey: string | null,
   *   seen: number,
   * }>} */
  const stack = [];
  /** @type {{ start: number, end: number } | null} */
  let rootModel = null;
  /** @type {{ start: number, end: number } | null} */
  let sessionModel = null;
  let rootEnd = -1;

  /** @param {number} start @param {number} end */
  const recordValue = (start, end) => {
    const context = stack[stack.length - 1];
    if (!context || context.kind !== "object") return;
    if (context.scope === "root" && context.pendingKey === "model") {
      rootModel = { start, end };
    } else if (context.scope === "session" && context.pendingKey === "model") {
      sessionModel = { start, end };
    }
    context.pendingKey = null;
  };

  let index = 0;
  while (index < text.length) {
    const character = text[index];
    if (isJsonWhitespace(character)) {
      index += 1;
      continue;
    }
    if (character === "{") {
      if (stack.length >= AI_WS_MAX_JSON_DEPTH) {
        throw new Error(`AI websocket JSON exceeds ${AI_WS_MAX_JSON_DEPTH} levels`);
      }
      const parent = stack[stack.length - 1];
      const scope = stack.length === 0
        ? "root"
        : parent?.kind === "object" &&
            parent.scope === "root" &&
            parent.pendingKey === "session"
          ? "session"
          : "other";
      if (parent?.kind === "object") parent.pendingKey = null;
      stack.push({ kind: "object", scope, expectKey: true, pendingKey: null, seen: 0 });
      index += 1;
      continue;
    }
    if (character === "[") {
      if (stack.length >= AI_WS_MAX_JSON_DEPTH) {
        throw new Error(`AI websocket JSON exceeds ${AI_WS_MAX_JSON_DEPTH} levels`);
      }
      const parent = stack[stack.length - 1];
      if (parent?.kind === "object") parent.pendingKey = null;
      stack.push({ kind: "array", scope: "other", expectKey: false, pendingKey: null, seen: 0 });
      index += 1;
      continue;
    }
    if (character === "}" || character === "]") {
      const context = stack.pop();
      if (character === "}" && context?.scope === "root") rootEnd = index;
      index += 1;
      continue;
    }
    if (character === ",") {
      const context = stack[stack.length - 1];
      if (context?.kind === "object") {
        context.expectKey = true;
        context.pendingKey = null;
      }
      index += 1;
      continue;
    }
    if (character === ":") {
      index += 1;
      continue;
    }
    if (character === '"') {
      const end = jsonStringEnd(text, index);
      const context = stack[stack.length - 1];
      if (context?.kind === "object" && context.expectKey) {
        if (context.scope !== "other") {
          const key = JSON.parse(text.slice(index, end));
          const field = trackedJsonField(context.scope, key);
          if (field !== 0 && (context.seen & field) !== 0) {
            throw new Error("AI websocket frame contains duplicate routing fields");
          }
          context.seen |= field;
          context.pendingKey =
            (context.scope === "root" && (key === "model" || key === "session")) ||
              (context.scope === "session" && key === "model")
            ? key
            : null;
        }
        context.expectKey = false;
      } else {
        recordValue(index, end);
      }
      index = end;
      continue;
    }
    const end = jsonPrimitiveEnd(text, index);
    recordValue(index, end);
    index = end;
  }
  return { rootEnd, rootModel, sessionModel };
}

/** @param {string} text @param {{ start: number, end: number }} span @param {string} value */
function replaceJsonValue(text, span, value) {
  return `${text.slice(0, span.start)}${JSON.stringify(value)}${text.slice(span.end)}`;
}

/**
 * @param {{ protocol: string, upstreamModel: string, binaryFrames: boolean }} model
 * @param {string} publicModel
 * @param {string} text
 */
function normalizeClientSocketText(model, publicModel, text) {
  const fields = inspectClientSocketJson(text);
  let payload;
  try { payload = JSON.parse(text); } catch {
    throw new Error("AI websocket text frames must be JSON");
  }
  if (!isRecord(payload) || typeof payload.type !== "string") {
    throw new Error("AI websocket frame must contain type");
  }
  /** @param {unknown} value */
  const allowedModel = (value) => value == null || value === publicModel || value === model.upstreamModel;
  if (!allowedModel(payload.model)) throw new Error("AI websocket model is fixed for this connection");
  if (isRecord(payload.session) && !allowedModel(payload.session.model)) {
    throw new Error("AI realtime model is fixed for this connection");
  }
  if (model.protocol === "responses" && payload.type === "response.create") {
    if (payload.model === model.upstreamModel) return text;
    if (fields.rootModel) return replaceJsonValue(text, fields.rootModel, model.upstreamModel);
    return `${text.slice(0, fields.rootEnd)},"model":${JSON.stringify(model.upstreamModel)}` +
      text.slice(fields.rootEnd);
  }
  if (
    model.protocol === "realtime" &&
    isRecord(payload.session) &&
    payload.session.model != null &&
    payload.session.model !== model.upstreamModel
  ) {
    if (!fields.sessionModel) throw new Error("AI realtime model field is malformed");
    return replaceJsonValue(text, fields.sessionModel, model.upstreamModel);
  }
  return text;
}

/**
 * Own an accepted provider socket and the public half of its local pair.
 *
 * @param {{
 *   model: { protocol: string, upstreamModel: string, binaryFrames: boolean },
 *   publicModel: string,
 *   providerSocket: WebSocket,
 *   aborter: AbortController,
 *   idleMs: number,
 *   onFinish(outcome: string): void,
 * }} options
 */
export function createAiWebSocketBridge(options) {
  const { model, publicModel, providerSocket, aborter, idleMs, onFinish } = options;
  const pair = new WebSocketPair();
  const client = pair[0];
  const downstream = pair[1];
  downstream.binaryType = "arraybuffer";
  providerSocket.binaryType = "arraybuffer";
  downstream.accept();
  let closed = false;
  let clientBytes = 0;
  let providerBytes = 0;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let idleTimer = null;

  const resetIdle = () => {
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => finish(1012, "AI websocket idle timeout", "idle_timeout"), idleMs);
  };
  /**
   * @param {number} code
   * @param {string} reason
   * @param {string} outcome
   * @param {{ downstreamCode?: number, downstreamReason?: string }} [close]
   */
  const finish = (code, reason, outcome, close = {}) => {
    if (closed) return;
    closed = true;
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = null;
    try { aborter.abort(); } catch {}
    closeAiWebSocket(
      downstream,
      close.downstreamCode ?? code,
      close.downstreamReason ?? reason
    );
    closeAiWebSocket(providerSocket, code, reason);
    onFinish(outcome);
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
        if (frame.kind === "binary" && !model.binaryFrames) {
          finish(1003, "AI model does not support binary frames", "unsupported_frame");
          return;
        }
        const forwarded = frame.kind === "text"
          ? normalizeClientSocketText(model, publicModel, String(frame.data))
          : frame.data;
        const forwardedFrame = forwarded === frame.data ? frame : websocketFrame(forwarded);
        if (forwardedFrame.bytes > AI_WS_FRAME_MAX_BYTES) {
          finish(1009, "AI websocket frame too large", "frame_limit");
          return;
        }
        clientBytes += Math.max(frame.bytes, forwardedFrame.bytes);
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
        if (frame.kind === "binary" && !model.binaryFrames) {
          finish(1003, "AI model does not support binary frames", "unsupported_frame");
          return;
        }
        target.send(frame.data);
        resetIdle();
      }
    } catch {
      finish(1008, "AI websocket frame rejected", "frame_error");
    }
  };

  downstream.addEventListener("message", (evt) => forward(providerSocket, "client", evt));
  providerSocket.addEventListener("message", (evt) => forward(downstream, "provider", evt));
  downstream.addEventListener("close", (evt) => finish(evt.code, evt.reason, "client_closed"));
  providerSocket.addEventListener("close", (evt) => {
    const downstreamCode = providerCloseCode(evt.code);
    finish(evt.code, evt.reason, "provider_closed", {
      downstreamCode,
      downstreamReason: downstreamCode === evt.code ? evt.reason : "AI provider connection lost",
    });
  });
  downstream.addEventListener("error", () => finish(1011, "AI websocket client error", "client_error"));
  providerSocket.addEventListener("error", () => finish(
    1011,
    "AI websocket provider error",
    "provider_error",
    { downstreamCode: 1013, downstreamReason: "AI provider connection lost" }
  ));
  resetIdle();
  return { client, close: () => finish(1012, "AI websocket deadline", "deadline") };
}
