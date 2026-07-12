import { logStructured } from "shared-observability";
import { discardResponseBody } from "shared-respond";
import { deleteGatewayInternalHeaders } from "gateway-lib";

/**
 * @typedef {{
 *   WEBSOCKET_MAX_BUFFERED_MESSAGES?: unknown,
 *   WEBSOCKET_RECONNECT_DELAYS_MS?: unknown,
 * }} GatewayWebSocketEnv
 * @typedef {{
 *   maxBufferedClientMessages?: number,
 *   reconnectDelaysMs?: number[],
 * }} GatewayWebSocketOptions
 * @typedef {{
 *   recordEvent?: (level: string, event: string, fields?: Record<string, unknown>) => void,
 *   adjustConnections?: (state: "active" | "detached", delta: number) => void,
 *   adjustBufferedMessages?: (delta: number) => void,
 *   recordSessionLifetime?: (durationMs: number, outcome: string) => void,
 * }} GatewayWebSocketObservability
 */

/**
 * @param {WebSocket} peer
 * @param {number} code
 * @param {string} reason
 */
function closeWebSocket(peer, code, reason) {
  try {
    peer.close(code, reason);
  } catch {
    // Closing a socket that is already closed or closing is harmless.
  }
}

/** @param {{ code: number }} evt */
function websocketClosedNormally(evt) {
  return evt.code === 1000;
}

// Keep these defaults mirrored with the deployment env defaults when changing
// the system default.
const RECONNECT_DELAYS_MS = [0, 100, 250, 500, 1000, 2000, 5000];
const MAX_BUFFERED_CLIENT_MESSAGES = 64;
const MAX_BUFFERED_CLIENT_MESSAGES_CAP = 1024;
const proxyOptionsByEnv = new WeakMap();

/** @param {unknown} value */
function parseNonNegativeInteger(value) {
  if (typeof value !== "string" || !/^[0-9]+$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

/** @param {Headers} headers */
function publicWebSocketResponseHeaders(headers) {
  const out = new Headers(headers);
  deleteGatewayInternalHeaders(out);
  return out;
}

/**
 * @param {GatewayWebSocketEnv} [env]
 * @returns {GatewayWebSocketOptions}
 */
export function webSocketProxyOptionsFromEnv(env = {}) {
  if (env && typeof env === "object" && proxyOptionsByEnv.has(env)) {
    return proxyOptionsByEnv.get(env);
  }

  /** @type {GatewayWebSocketOptions} */
  const options = {};
  const maxBuffered = parseNonNegativeInteger(env.WEBSOCKET_MAX_BUFFERED_MESSAGES);
  if (maxBuffered !== null && maxBuffered > MAX_BUFFERED_CLIENT_MESSAGES_CAP) {
    options.maxBufferedClientMessages = MAX_BUFFERED_CLIENT_MESSAGES_CAP;
    logStructured("gateway", "warn", "websocket_config_clamped", {
      variable: "WEBSOCKET_MAX_BUFFERED_MESSAGES",
      value: String(env.WEBSOCKET_MAX_BUFFERED_MESSAGES),
      cap: MAX_BUFFERED_CLIENT_MESSAGES_CAP,
    });
  } else if (maxBuffered !== null && maxBuffered > 0) {
    options.maxBufferedClientMessages = maxBuffered;
  } else if (maxBuffered === 0) {
    logStructured("gateway", "warn", "websocket_config_invalid", {
      variable: "WEBSOCKET_MAX_BUFFERED_MESSAGES",
      value: String(env.WEBSOCKET_MAX_BUFFERED_MESSAGES),
      minimum: 1,
      fallback: MAX_BUFFERED_CLIENT_MESSAGES,
    });
  } else if (env.WEBSOCKET_MAX_BUFFERED_MESSAGES != null) {
    logStructured("gateway", "warn", "websocket_config_invalid", {
      variable: "WEBSOCKET_MAX_BUFFERED_MESSAGES",
      value: String(env.WEBSOCKET_MAX_BUFFERED_MESSAGES),
      fallback: MAX_BUFFERED_CLIENT_MESSAGES,
    });
  }

  if (typeof env.WEBSOCKET_RECONNECT_DELAYS_MS === "string" && env.WEBSOCKET_RECONNECT_DELAYS_MS.trim()) {
    const delays = env.WEBSOCKET_RECONNECT_DELAYS_MS.split(",")
      .map(/** @param {string} part */ (part) => parseNonNegativeInteger(part.trim()));
    if (delays.length > 0 && delays.every(/** @param {number | null} delay */ (delay) => delay !== null)) {
      options.reconnectDelaysMs = /** @type {number[]} */ (delays);
    } else {
      logStructured("gateway", "warn", "websocket_config_invalid", {
        variable: "WEBSOCKET_RECONNECT_DELAYS_MS",
        value: env.WEBSOCKET_RECONNECT_DELAYS_MS,
        fallback: RECONNECT_DELAYS_MS.join(","),
      });
    }
  } else if (env.WEBSOCKET_RECONNECT_DELAYS_MS != null) {
    logStructured("gateway", "warn", "websocket_config_invalid", {
      variable: "WEBSOCKET_RECONNECT_DELAYS_MS",
      value: String(env.WEBSOCKET_RECONNECT_DELAYS_MS),
      fallback: RECONNECT_DELAYS_MS.join(","),
    });
  }

  if (env && typeof env === "object") {
    proxyOptionsByEnv.set(env, options);
  }
  return options;
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * @param {Response & { webSocket?: WebSocket }} initialResponse
 * @param {() => Promise<Response & { webSocket?: WebSocket }>} connectUpstream
 * @param {(outcome: string) => void} recordProxyOutcome
 * @param {GatewayWebSocketObservability} [observability]
 * @param {GatewayWebSocketOptions} [options]
 */
export function proxyGatewayWebSocket(
  initialResponse,
  connectUpstream,
  recordProxyOutcome,
  observability = {},
  options = {}
) {
  const pair = new WebSocketPair();
  const client = pair[0];
  const downstream = pair[1];
  /** @type {WebSocket | null} */
  let upstream = null;
  /** @type {Promise<WebSocket | null> | null} */
  let upstreamConnecting = null;
  /** @type {Promise<WebSocket | null> | null} */
  let reconnectLoop = null;
  let sendQueue = Promise.resolve();
  let queuedClientMessages = 0;
  let queueEpoch = 0;
  let downstreamClosed = false;
  let activeRecorded = false;
  let detachedRecorded = false;
  let sessionLifetimeRecorded = false;
  const sessionStartedAt = Date.now();

  downstream.accept({ allowHalfOpen: true });

  const reconnectDelaysMs = Array.isArray(options.reconnectDelaysMs)
    ? options.reconnectDelaysMs
    : RECONNECT_DELAYS_MS;
  const configuredMaxBuffered = options.maxBufferedClientMessages;
  const maxBufferedClientMessages = typeof configuredMaxBuffered === "number" &&
    Number.isInteger(configuredMaxBuffered) &&
    configuredMaxBuffered > 0
    ? configuredMaxBuffered
    : MAX_BUFFERED_CLIENT_MESSAGES;

  /** @param {string} outcome */
  function record(outcome) {
    if (typeof recordProxyOutcome === "function") recordProxyOutcome(outcome);
  }

  /**
   * @param {string} level
   * @param {string} event
   * @param {Record<string, unknown>} [fields]
   */
  function recordEvent(level, event, fields = {}) {
    if (typeof observability.recordEvent === "function") {
      observability.recordEvent(level, event, fields);
    }
  }

  /**
   * @param {"active" | "detached"} state
   * @param {number} delta
   */
  function adjustConnections(state, delta) {
    if (typeof observability.adjustConnections === "function") {
      observability.adjustConnections(state, delta);
    }
  }

  /** @param {number} delta */
  function adjustBufferedMessages(delta) {
    if (typeof observability.adjustBufferedMessages === "function") {
      observability.adjustBufferedMessages(delta);
    }
  }

  /** @param {string} outcome */
  function recordSessionLifetime(outcome) {
    if (sessionLifetimeRecorded) return;
    sessionLifetimeRecorded = true;
    if (typeof observability.recordSessionLifetime === "function") {
      observability.recordSessionLifetime(Date.now() - sessionStartedAt, outcome);
    }
  }

  function clearQueuedClientMessages() {
    if (queuedClientMessages === 0) return;
    adjustBufferedMessages(-queuedClientMessages);
    queuedClientMessages = 0;
    queueEpoch += 1;
  }

  /** @param {boolean} nextDetached */
  function setDetached(nextDetached) {
    if (detachedRecorded === nextDetached) return;
    detachedRecorded = nextDetached;
    adjustConnections("detached", nextDetached ? 1 : -1);
  }

  /** @param {string} outcome */
  function markDownstreamClosed(outcome) {
    if (downstreamClosed) return;
    downstreamClosed = true;
    recordSessionLifetime(outcome);
    if (activeRecorded) {
      activeRecorded = false;
      adjustConnections("active", -1);
    }
    setDetached(false);
    clearQueuedClientMessages();
  }

  /**
   * @param {number} code
   * @param {string} reason
   * @param {string} outcome
   */
  function closeDownstream(code, reason, outcome) {
    markDownstreamClosed(outcome);
    closeWebSocket(downstream, code, reason);
  }

  /**
   * @param {number} code
   * @param {string} reason
   * @param {string} outcome
   */
  function closeDownstreamAndUpstream(code, reason, outcome) {
    closeDownstream(code, reason, outcome);
    if (upstream) closeWebSocket(upstream, code, reason);
  }

  /** @param {WebSocket} nextUpstream */
  function attachUpstream(nextUpstream) {
    const attachedUpstream = nextUpstream;
    upstream = attachedUpstream;
    setDetached(false);
    attachedUpstream.accept({ allowHalfOpen: true });
    attachedUpstream.addEventListener("message", (evt) => {
      if (upstream !== attachedUpstream) return;
      try {
        downstream.send(evt.data);
      } catch {
        recordEvent("warn", "websocket_downstream_send_failed");
        closeDownstreamAndUpstream(1011, "downstream send failed", "downstream_error");
      }
    });
    attachedUpstream.addEventListener("close", (evt) => {
      if (upstream !== attachedUpstream) return;
      upstream = null;
      if (downstreamClosed) return;
      if (websocketClosedNormally(evt)) {
        closeDownstream(evt.code, evt.reason, "upstream_normal_close");
        return;
      }
      record("upstream_abnormal_close");
      recordEvent("warn", "websocket_upstream_abnormal_close", {
        code: evt.code,
        reason: evt.reason,
      });
      setDetached(true);
      scheduleReconnect();
    });
    attachedUpstream.addEventListener("error", () => {
      if (upstream !== attachedUpstream) return;
      upstream = null;
      if (!downstreamClosed) {
        record("upstream_error");
        recordEvent("warn", "websocket_upstream_error");
        setDetached(true);
        scheduleReconnect();
      }
    });
  }

  async function ensureUpstream() {
    if (downstreamClosed) throw new Error("WebSocket client is closed");
    if (upstream) return upstream;
    if (!upstreamConnecting) {
      upstreamConnecting = connectUpstream().then(async (response) => {
        if (response.status !== 101 || !response.webSocket) {
          await discardResponseBody(response);
          throw new Error(`WebSocket reconnect failed with status ${response.status}`);
        }
        if (downstreamClosed) {
          closeWebSocket(response.webSocket, 1001, "client closed");
          throw new Error("WebSocket reconnect completed after client close");
        }
        attachUpstream(response.webSocket);
        record("reconnected");
        return upstream;
      }).finally(() => {
        upstreamConnecting = null;
      });
    }
    return await upstreamConnecting;
  }

  async function reconnectWithBudget() {
    if (downstreamClosed) throw new Error("WebSocket client is closed");
    if (upstream) return upstream;
    if (!reconnectLoop) {
      reconnectLoop = (async () => {
        for (const delayMs of reconnectDelaysMs) {
          if (downstreamClosed) throw new Error("WebSocket client is closed");
          if (upstream) return upstream;
          if (delayMs > 0) await sleep(delayMs);
          try {
            return await ensureUpstream();
          } catch {
            // The backend may still be restarting; keep the public socket open
            // while the bounded retry loop has budget.
          }
        }
        throw new Error("WebSocket reconnect budget exhausted");
      })().finally(() => {
        reconnectLoop = null;
      });
    }
    return await reconnectLoop;
  }

  function scheduleReconnect() {
    if (downstreamClosed || upstream || reconnectLoop) return;
    reconnectWithBudget().catch(() => {
      if (!downstreamClosed && !upstream) {
        record("reconnect_failed");
        recordEvent("warn", "websocket_reconnect_failed", {
          reason: "retry_budget_exhausted",
        });
        closeDownstream(1011, "upstream reconnect failed", "reconnect_failed");
      }
    });
  }

  /** @param {string | ArrayBuffer | Blob} data */
  async function sendClientMessage(data) {
    const current = upstream || await reconnectWithBudget();
    if (!current) throw new Error("WebSocket upstream unavailable");
    try {
      current.send(data);
    } catch {
      if (upstream === current) upstream = null;
      setDetached(true);
      const reconnected = await reconnectWithBudget();
      if (!reconnected) throw new Error("WebSocket upstream unavailable");
      // A second send failure is terminal for this client frame; the caller
      // records reconnect_failed and closes the public socket rather than
      // retrying indefinitely and reordering later client frames.
      reconnected.send(data);
    }
  }

  attachUpstream(initialResponse.webSocket);

  downstream.addEventListener("message", (evt) => {
    if (queuedClientMessages >= maxBufferedClientMessages) {
      record("client_buffer_overflow");
      recordEvent("warn", "websocket_client_buffer_overflow", {
        buffered_messages: queuedClientMessages,
      });
      closeDownstreamAndUpstream(1013, "websocket send buffer full", "client_buffer_overflow");
      return;
    }
    queuedClientMessages += 1;
    const messageQueueEpoch = queueEpoch;
    adjustBufferedMessages(1);
    sendQueue = sendQueue.then(async () => {
      if (downstreamClosed || messageQueueEpoch !== queueEpoch) return;
      try {
        await sendClientMessage(evt.data);
      } catch {
        if (!downstreamClosed) {
          record("reconnect_failed");
          recordEvent("warn", "websocket_reconnect_failed", {
            reason: "send_failed",
          });
          closeDownstream(1011, "upstream send failed", "reconnect_failed");
        }
      }
    }).finally(() => {
      if (messageQueueEpoch === queueEpoch) {
        queuedClientMessages -= 1;
        adjustBufferedMessages(-1);
      }
    });
  });
  downstream.addEventListener("close", (evt) => {
    markDownstreamClosed(websocketClosedNormally(evt) ? "client_closed" : "client_error");
    if (upstream) closeWebSocket(upstream, evt.code, evt.reason);
  });
  downstream.addEventListener("error", () => {
    recordEvent("warn", "websocket_downstream_error");
    markDownstreamClosed("downstream_error");
    if (upstream) closeWebSocket(upstream, 1011, "downstream error");
  });

  record("established");
  activeRecorded = true;
  adjustConnections("active", 1);
  return new Response(null, {
    status: 101,
    headers: publicWebSocketResponseHeaders(initialResponse.headers),
    webSocket: client,
  });
}
