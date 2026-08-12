export const AI_WS_FRAME_MAX_BYTES = 1024 * 1024;
export const AI_WS_MAX_BYTES = 128 * 1024 * 1024;

const utf8Encoder = new TextEncoder();

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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

/**
 * @param {{ protocol: string, upstreamModel: string, binaryFrames: boolean }} model
 * @param {string} publicModel
 * @param {string} text
 */
function normalizeClientSocketText(model, publicModel, text) {
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
    payload.model = model.upstreamModel;
  }
  if (model.protocol === "realtime" && isRecord(payload.session) && payload.session.model != null) {
    payload.session = { ...payload.session, model: model.upstreamModel };
  }
  return JSON.stringify(payload);
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
        if (frame.kind === "binary" && !model.binaryFrames) {
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
