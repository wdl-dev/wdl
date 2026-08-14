import { logStructured } from "shared-observability";
import { discardResponseBody } from "shared-respond";
import { utf8ByteLength } from "shared-utf8";
import {
  WEBSOCKET_RECONNECT_POLICY_DISABLED,
  WEBSOCKET_RECONNECT_POLICY_HEADER,
} from "shared-worker-contract";
import { deleteGatewayInternalHeaders } from "gateway-lib";

/**
 * @typedef {{
 *   WEBSOCKET_MAX_BUFFERED_MESSAGES?: unknown,
 *   WEBSOCKET_RECONNECT_DELAYS_MS?: unknown,
 * }} GatewayWebSocketEnv
 * @typedef {{
 *   maxBufferedClientMessages?: number,
 *   reconnectDelaysMs?: number[],
 *   checkLifecycle?: () => Promise<"continue" | "restart" | "retry">,
 *   registerLifecycle?: (handlers: { restart: () => void, fail: () => void }) => () => void,
 * }} GatewayWebSocketOptions
 * @typedef {{
 *   recordEvent?: (level: string, event: string, fields?: Record<string, unknown>) => void,
 *   adjustConnections?: (state: "active" | "detached", delta: number) => void,
 *   adjustBufferedMessages?: (delta: number) => void,
 *   recordSessionLifetime?: (durationMs: number, outcome: string) => void,
 * }} GatewayWebSocketObservability
 * @typedef {{ fetch(request: Request): Promise<Response> }} GatewayWebSocketUpstream
 */

const MAX_WEBSOCKET_CLOSE_REASON_BYTES = 123;
// workerd can dispatch ErrorEvent before the matching protocol CloseEvent.
const UPSTREAM_ERROR_CLOSE_GRACE_MS = 100;
const UNSENDABLE_WEBSOCKET_CLOSE_CODES = new Set([1004, 1005, 1006, 1015]);

/** @param {number} code */
function sendableWebSocketCloseCode(code) {
  if (
    !Number.isInteger(code)
    || code < 1000
    || code >= 5000
    || UNSENDABLE_WEBSOCKET_CLOSE_CODES.has(code)
  ) {
    return 1011;
  }
  return code;
}

/** @param {string} reason */
function boundedWebSocketCloseReason(reason) {
  let bytes = 0;
  let end = 0;
  for (const character of reason) {
    const characterBytes = utf8ByteLength(character);
    if (bytes + characterBytes > MAX_WEBSOCKET_CLOSE_REASON_BYTES) {
      return reason.slice(0, end);
    }
    bytes += characterBytes;
    end += character.length;
  }
  return reason;
}

/**
 * @param {WebSocket} peer
 * @param {number} code
 * @param {string} reason
 */
function closeWebSocket(peer, code, reason) {
  try {
    if (code === 1005 && reason === "") {
      peer.close();
      return;
    }
    peer.close(sendableWebSocketCloseCode(code), boundedWebSocketCloseReason(reason));
  } catch {
    // Closing a socket that is already closed or closing is harmless.
  }
}

/** @param {WebSocket} peer */
function acceptProxyWebSocket(peer) {
  peer.binaryType = "arraybuffer";
  peer.accept();
}

/** @param {{ code: number }} evt */
function websocketClosedNormally(evt) {
  return evt.code === 1000 || evt.code === 1005;
}

/** @param {{ code: number }} evt */
function websocketCloseShouldReconnect(evt) {
  return evt.code === 1001 || evt.code === 1006 || evt.code === 1011;
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
 * Build a fresh upstream upgrade request for the initial connection and every
 * reconnect while preserving the routed URL, headers, and WebSocket key.
 * @param {Request} request
 * @param {GatewayWebSocketUpstream} upstream
 * @returns {() => Promise<Response & { webSocket?: WebSocket }>}
 */
export function createGatewayWebSocketUpstreamFetch(request, upstream) {
  const url = request.url;
  const headers = new Headers(request.headers);
  return async () => /** @type {Response & { webSocket?: WebSocket }} */ (
    await upstream.fetch(new Request(url, { method: "GET", headers }))
  );
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
  /** @type {WebSocket | null} */
  let pendingUpstream = /** @type {WebSocket} */ (initialResponse.webSocket);
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
  /** @type {(() => void) | null} */
  let unregisterLifecycle = null;
  /** @type {Promise<"continue" | "restart" | "error"> | null} */
  let lifecycleGate = null;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let upstreamErrorFallback = null;
  /** @type {WebSocket | null} */
  let upstreamErrorSocket = null;
  /** @type {Promise<void> | null} */
  let upstreamErrorGate = null;
  /** @type {((value: void | PromiseLike<void>) => void) | null} */
  let resolveUpstreamErrorGate = null;
  const sessionStartedAt = Date.now();
  const reconnectDisabled = initialResponse.headers.get(WEBSOCKET_RECONNECT_POLICY_HEADER) ===
    WEBSOCKET_RECONNECT_POLICY_DISABLED;

  acceptProxyWebSocket(downstream);

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

  /** @param {WebSocket | null} [attachedUpstream] */
  function clearUpstreamErrorFallback(attachedUpstream = null) {
    if (attachedUpstream !== null && upstreamErrorSocket !== attachedUpstream) return false;
    const pending = upstreamErrorSocket !== null;
    if (upstreamErrorFallback !== null) clearTimeout(upstreamErrorFallback);
    const resolveGate = resolveUpstreamErrorGate;
    upstreamErrorFallback = null;
    upstreamErrorSocket = null;
    upstreamErrorGate = null;
    resolveUpstreamErrorGate = null;
    resolveGate?.(undefined);
    return pending;
  }

  async function waitForUpstreamErrorDecision() {
    while (upstreamErrorGate !== null) await upstreamErrorGate;
    if (downstreamClosed) throw new Error("WebSocket client is closed");
  }

  /** @param {WebSocket} attachedUpstream */
  function scheduleUpstreamFailure(attachedUpstream) {
    if (upstream !== attachedUpstream || upstreamErrorSocket === attachedUpstream) return false;
    clearUpstreamErrorFallback();
    const { promise, resolve } = Promise.withResolvers();
    upstreamErrorSocket = attachedUpstream;
    upstreamErrorGate = promise;
    resolveUpstreamErrorGate = resolve;
    setDetached(true);
    upstreamErrorFallback = setTimeout(() => {
      if (upstreamErrorSocket !== attachedUpstream) return;
      clearUpstreamErrorFallback(attachedUpstream);
      void handleUpstreamFailure(attachedUpstream);
    }, UPSTREAM_ERROR_CLOSE_GRACE_MS);
    return true;
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
    clearUpstreamErrorFallback();
    unregisterLifecycle?.();
    unregisterLifecycle = null;
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
    const currentUpstream = upstream;
    const candidateUpstream = pendingUpstream;
    upstream = null;
    pendingUpstream = null;
    closeDownstream(code, reason, outcome);
    if (currentUpstream) closeWebSocket(currentUpstream, code, reason);
    if (candidateUpstream) {
      acceptProxyWebSocket(candidateUpstream);
      closeWebSocket(candidateUpstream, code, reason);
    }
  }

  /** @param {WebSocket} socket @param {number} code @param {string} reason */
  function closePendingUpstream(socket, code, reason) {
    if (pendingUpstream !== socket) return;
    pendingUpstream = null;
    acceptProxyWebSocket(socket);
    closeWebSocket(socket, code, reason);
  }

  function closeForLifecycleRestart() {
    if (downstreamClosed) return;
    record("lifecycle_restart");
    recordEvent("info", "websocket_lifecycle_restart");
    closeDownstreamAndUpstream(1012, "service restart", "lifecycle_restart");
  }

  function closeForLifecycleFailure() {
    if (downstreamClosed) return;
    record("lifecycle_check_failed");
    recordEvent("warn", "websocket_lifecycle_check_failed");
    closeDownstreamAndUpstream(1011, "lifecycle check failed", "lifecycle_check_failed");
  }

  /** @param {WebSocket} attachedUpstream */
  function closeWithoutReconnect(attachedUpstream) {
    if (upstream !== attachedUpstream || downstreamClosed) return;
    upstream = null;
    record("reconnect_suppressed");
    recordEvent("info", "websocket_reconnect_suppressed");
    closeDownstream(1012, "service restart", "reconnect_suppressed");
    closeWebSocket(attachedUpstream, 1012, "service restart");
  }

  function beginLifecycleCheck() {
    if (lifecycleGate) return lifecycleGate;
    const checkLifecycle = options.checkLifecycle;
    if (typeof checkLifecycle !== "function") {
      lifecycleGate = Promise.resolve("continue");
      return lifecycleGate;
    }
    lifecycleGate = (async () => {
      for (let attempt = 0; attempt < reconnectDelaysMs.length; attempt += 1) {
        const delayMs = reconnectDelaysMs[attempt];
        if (attempt > 0 && delayMs > 0) await sleep(delayMs);
        if (downstreamClosed) return "error";
        const decision = await checkLifecycle();
        if (decision !== "retry") return decision;
      }
      return "error";
    })().catch(() => "error");
    return lifecycleGate;
  }

  /**
   * @param {Promise<"continue" | "restart" | "error">} gate
   */
  async function requireLifecycleContinuation(gate) {
    const disposition = await gate;
    if (lifecycleGate === gate && disposition === "continue") lifecycleGate = null;
    if (disposition === "continue") return;
    if (downstreamClosed) {
      throw new Error("WebSocket closed before lifecycle check completed");
    }
    if (disposition === "restart") {
      closeForLifecycleRestart();
      throw new Error("WebSocket closed for service restart");
    }
    closeForLifecycleFailure();
    throw new Error("WebSocket lifecycle check failed");
  }

  async function requireReconnectAllowed() {
    if (lifecycleGate) await requireLifecycleContinuation(lifecycleGate);
  }

  /**
   * @param {WebSocket} attachedUpstream
   */
  async function handleUpstreamFailure(attachedUpstream) {
    if (upstream !== attachedUpstream) return;
    if (reconnectDisabled) {
      closeWithoutReconnect(attachedUpstream);
      return;
    }
    upstream = null;
    if (downstreamClosed) return;
    setDetached(true);
    const gate = beginLifecycleCheck();
    try {
      await requireLifecycleContinuation(gate);
    } catch {
      return;
    }
    if (downstreamClosed) return;
    scheduleReconnect();
  }

  /**
   * @param {WebSocket} attachedUpstream
   * @param {{ code: number, reason: string }} evt
   * @param {boolean} reconnect
   */
  async function handleUpstreamClose(attachedUpstream, evt, reconnect) {
    const followedError = clearUpstreamErrorFallback(attachedUpstream);
    if (upstream !== attachedUpstream) return;
    if (!reconnect) {
      upstream = null;
      if (!downstreamClosed) {
        closeDownstream(
          evt.code,
          evt.reason,
          websocketClosedNormally(evt) ? "upstream_normal_close" : "upstream_terminal_close"
        );
      }
      return;
    }
    if (!followedError) {
      record("upstream_abnormal_close");
      recordEvent("warn", "websocket_upstream_abnormal_close", {
        code: evt.code,
        reason: evt.reason,
      });
    }
    await handleUpstreamFailure(attachedUpstream);
  }

  /** @param {WebSocket} nextUpstream */
  function attachUpstream(nextUpstream) {
    clearUpstreamErrorFallback();
    if (pendingUpstream === nextUpstream) pendingUpstream = null;
    const attachedUpstream = nextUpstream;
    upstream = attachedUpstream;
    setDetached(false);
    acceptProxyWebSocket(attachedUpstream);
    attachedUpstream.addEventListener("message", (evt) => {
      if (upstream !== attachedUpstream || upstreamErrorSocket === attachedUpstream) return;
      try {
        downstream.send(evt.data);
      } catch {
        recordEvent("warn", "websocket_downstream_send_failed");
        closeDownstreamAndUpstream(1011, "downstream send failed", "downstream_error");
      }
    });
    attachedUpstream.addEventListener("close", (evt) => {
      void handleUpstreamClose(attachedUpstream, evt, websocketCloseShouldReconnect(evt));
    });
    attachedUpstream.addEventListener("error", () => {
      if (!scheduleUpstreamFailure(attachedUpstream)) return;
      record("upstream_error");
      recordEvent("warn", "websocket_upstream_error");
    });
  }

  function attachInitialUpstream() {
    const initialUpstream = pendingUpstream;
    if (!initialUpstream || downstreamClosed) return;
    attachUpstream(initialUpstream);
  }

  async function ensureUpstream() {
    if (downstreamClosed) throw new Error("WebSocket client is closed");
    await requireReconnectAllowed();
    if (upstream) return upstream;
    if (!upstreamConnecting) {
      upstreamConnecting = connectUpstream().then(async (response) => {
        if (response.status !== 101 || !response.webSocket) {
          await discardResponseBody(response);
          throw new Error(`WebSocket reconnect failed with status ${response.status}`);
        }
        const candidateUpstream = response.webSocket;
        pendingUpstream = candidateUpstream;
        if (downstreamClosed) {
          closePendingUpstream(candidateUpstream, 1001, "client closed");
          throw new Error("WebSocket reconnect completed after client close");
        }
        if (typeof options.checkLifecycle === "function") {
          const gate = beginLifecycleCheck();
          try {
            await requireLifecycleContinuation(gate);
          } catch (err) {
            closePendingUpstream(candidateUpstream, 1001, "client closed");
            throw err;
          }
        }
        if (downstreamClosed) {
          closePendingUpstream(candidateUpstream, 1001, "client closed");
          throw new Error("WebSocket closed while reconnect was being fenced");
        }
        attachUpstream(candidateUpstream);
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

  /** @param {string | ArrayBuffer} data */
  async function sendClientMessage(data) {
    await waitForUpstreamErrorDecision();
    await requireReconnectAllowed();
    await waitForUpstreamErrorDecision();
    const current = upstream || await reconnectWithBudget();
    if (!current) throw new Error("WebSocket upstream unavailable");
    try {
      current.send(data);
    } catch {
      if (reconnectDisabled) {
        closeWithoutReconnect(current);
        return;
      }
      if (upstream === current) upstream = null;
      closeWebSocket(current, 1011, "upstream send failed");
      setDetached(true);
      const gate = beginLifecycleCheck();
      await requireLifecycleContinuation(gate);
      const reconnected = await reconnectWithBudget();
      if (!reconnected) throw new Error("WebSocket upstream unavailable");
      // A second send failure is terminal for this client frame; the caller
      // records reconnect_failed and closes the public socket rather than
      // retrying indefinitely and reordering later client frames.
      reconnected.send(data);
    }
  }

  if (typeof options.registerLifecycle === "function") {
    unregisterLifecycle = options.registerLifecycle({
      restart: closeForLifecycleRestart,
      fail: closeForLifecycleFailure,
    });
  }
  if (typeof options.checkLifecycle === "function") {
    const gate = beginLifecycleCheck();
    void requireLifecycleContinuation(gate).then(
      attachInitialUpstream,
      () => {}
    );
  } else {
    attachInitialUpstream();
  }

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
          closeDownstreamAndUpstream(1011, "upstream send failed", "reconnect_failed");
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
    closeDownstreamAndUpstream(
      evt.code,
      evt.reason,
      websocketClosedNormally(evt) ? "client_closed" : "client_error"
    );
  });
  downstream.addEventListener("error", () => {
    if (downstreamClosed) return;
    recordEvent("warn", "websocket_downstream_error");
    closeDownstreamAndUpstream(1011, "downstream error", "downstream_error");
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
