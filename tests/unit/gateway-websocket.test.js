import { test } from "node:test";
import assert from "node:assert/strict";
import { parseJsonText } from "../helpers/json-payload.js";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { delay, waitUntil } from "../helpers/timing.js";

class FakeResponse {
  /**
   * @param {unknown} body
   * @param {{ status?: number, headers?: Headers, webSocket?: FakeWebSocket }} [init]
   */
  constructor(body = null, init = {}) {
    this.body = body;
    this.status = init.status ?? 200;
    this.headers = init.headers ?? new Headers();
    this.webSocket = init.webSocket;
  }
}

class FakeWebSocket {
  /** @param {string} name */
  constructor(name) {
    this.name = name;
    /** @type {unknown[]} */
    this.sent = [];
    /** @type {{ code: number, reason: string } | null} */
    this.closed = null;
    /** @type {FakeWebSocket | null} */
    this.peer = null;
    /** @type {Map<string, Array<(event: any) => void>>} */
    this.listeners = new Map();
  }

  accept() {}

  /** @param {string} type @param {(event: any) => void} callback */
  addEventListener(type, callback) {
    const callbacks = this.listeners.get(type) || [];
    callbacks.push(callback);
    this.listeners.set(type, callbacks);
  }

  /** @param {string} type @param {any} [event] */
  dispatch(type, event = {}) {
    for (const callback of this.listeners.get(type) || []) callback(event);
  }

  /** @param {unknown} data */
  send(data) {
    if (this.closed) throw new Error(`${this.name} is closed`);
    (this.peer || this).sent.push(data);
  }

  /** @param {number} code @param {string} reason */
  close(code, reason) {
    this.closed = { code, reason };
    if (this.peer) this.peer.closed = { code, reason };
  }
}

/** @type {[FakeWebSocket, FakeWebSocket] | null} */
let lastPair = null;

(/** @type {any} */ (globalThis)).Response = FakeResponse;
(/** @type {any} */ (globalThis)).WebSocketPair = class FakeWebSocketPair {
  constructor() {
    lastPair = [new FakeWebSocket("client"), new FakeWebSocket("downstream")];
    /** @type {any} */ (lastPair)[0].peer = /** @type {any} */ (lastPair)[1];
    /** @type {any} */ (lastPair)[1].peer = /** @type {any} */ (lastPair)[0];
    return lastPair;
  }
};

const { proxyGatewayWebSocket, webSocketProxyOptionsFromEnv } = await importRepositoryModule(
  "gateway/websocket.js",
  [
    [/from "shared-observability";/, `from ${JSON.stringify(repositoryFileUrl("shared/observability.js"))};`],
    [/from "shared-respond";/, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
    [/from "gateway-lib";/, `from ${JSON.stringify(repositoryFileUrl("gateway/lib.js"))};`],
  ]
);

/** @param {FakeWebSocket} socket */
function websocketResponse(socket) {
  return new Response(null, /** @type {any} */ ({ status: 101, headers: new Headers(), webSocket: socket }));
}

/** @param {any} response */
function responseWebSocket(response) {
  return /** @type {FakeWebSocket} */ (response.webSocket);
}

/** @param {() => unknown} predicate */
async function waitFor(predicate, timeoutMs = 500) {
  await waitUntil(
    "gateway websocket condition",
    () => Boolean(predicate()),
    { timeoutMs, intervalMs: 10 }
  );
}

test("gateway websocket proxy relays upstream server-pushed frames", () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {string[]} */
  const outcomes = [];

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    },
    (/** @type {string} */ outcome) => outcomes.push(outcome)
  );

  assert.equal(response.status, 101);
  assert.equal(response.webSocket, /** @type {any} */ (lastPair)[0]);
  upstream.dispatch("message", { data: "open" });
  assert.deepEqual(responseWebSocket(response).sent, ["open"]);
  assert.deepEqual(outcomes, ["established"]);
});

test("gateway websocket proxy strips internal routing headers from upgrade response", () => {
  const upstream = new FakeWebSocket("upstream");
  const response = proxyGatewayWebSocket(
    new Response(null, /** @type {any} */ ({
      status: 101,
      headers: new Headers({
        "x-worker-id": "demo:worker:v1",
        "x-worker-prefix": "/app",
        "x-wdl-upstream-binding": "ROOM",
        "x-wdl-internal-auth": "secret",
        "x-wdl-do-owner-hint": "1",
        "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
        "x-public": "ok",
      }),
      webSocket: upstream,
    })),
    async () => {
      throw new Error("not used");
    },
    () => {}
  );

  assert.equal(response.headers.get("x-worker-id"), null);
  assert.equal(response.headers.get("x-worker-prefix"), null);
  assert.equal(response.headers.get("x-wdl-upstream-binding"), null);
  assert.equal(response.headers.get("x-wdl-internal-auth"), null);
  assert.equal(response.headers.get("x-wdl-do-owner-hint"), null);
  assert.equal(response.headers.get("x-wdl-d1-owner-endpoint"), null);
  assert.equal(response.headers.get("x-public"), "ok");
});

test("gateway websocket proxy proactively reconnects after abnormal upstream close", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  /** @type {string[]} */
  const outcomes = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];
  let connects = 0;

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => {
      connects += 1;
      return websocketResponse(upstream2);
    },
    (/** @type {string} */ outcome) => outcomes.push(outcome),
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
    }
  );

  upstream1.dispatch("close", { code: 1011, reason: "runtime restart" });
  await waitFor(() => connects === 1);
  upstream2.dispatch("message", { data: "after-reconnect" });

  assert.deepEqual(responseWebSocket(response).sent, ["after-reconnect"]);
  assert.deepEqual(outcomes, ["established", "upstream_abnormal_close", "reconnected"]);
  assert.deepEqual(events, [{
    level: "warn",
    event: "websocket_upstream_abnormal_close",
    fields: { code: 1011, reason: "runtime restart" },
  }]);
});

test("gateway websocket proxy preserves client frame order while reconnecting", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  const { promise: reconnecting, resolve: resolveReconnect } = Promise.withResolvers();

  proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => await reconnecting
  );

  upstream1.dispatch("close", { code: 1011, reason: "runtime restart" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "one" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "two" });
  resolveReconnect(websocketResponse(upstream2));

  await waitFor(() => upstream2.sent.length === 2);
  assert.deepEqual(upstream2.sent, ["one", "two"]);
});

test("gateway websocket proxy drops queued client frames after downstream close", async () => {
  const upstream = new FakeWebSocket("upstream");

  proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    }
  );

  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued-after-close" });
  /** @type {any} */ (lastPair)[1].dispatch("close", { code: 1000, reason: "client done" });
  await delay(0);

  assert.deepEqual(upstream.sent, []);
});

test("gateway websocket proxy ignores delayed close from a replaced upstream", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  const upstream3 = new FakeWebSocket("upstream3");
  let connects = 0;

  proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => {
      connects += 1;
      return websocketResponse(connects === 1 ? upstream2 : upstream3);
    }
  );

  upstream1.closed = { code: 1011, reason: "broken" };
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "first" });
  await waitFor(() => upstream2.sent.length === 1);

  upstream1.dispatch("close", { code: 1011, reason: "late close" });
  upstream1.dispatch("message", { data: "stale" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "second" });
  await waitFor(() => upstream2.sent.length === 2);

  assert.deepEqual(upstream2.sent, ["first", "second"]);
  assert.deepEqual(/** @type {any} */ (lastPair)[0].sent, []);
  assert.equal(upstream3.sent.length, 0);
  assert.equal(connects, 1);
});

test("gateway websocket proxy closes the session when downstream send fails", () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {Array<unknown[]>} */
  const adjustments = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];
  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    },
    null,
    {
      adjustConnections: (/** @type {string} */ state, /** @type {number} */ delta) => adjustments.push(["connection", state, delta]),
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  /** @type {any} */ (lastPair)[1].closed = { code: 1011, reason: "write failed" };
  upstream.dispatch("message", { data: "server-push" });

  assert.deepEqual(responseWebSocket(response).closed, {
    code: 1011,
    reason: "downstream send failed",
  });
  assert.deepEqual(upstream.closed, {
    code: 1011,
    reason: "downstream send failed",
  });
  assert.deepEqual(adjustments, [
    ["connection", "active", 1],
    ["connection", "active", -1],
  ]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "downstream_error");
  assert.deepEqual(events, [{
    level: "warn",
    event: "websocket_downstream_send_failed",
    fields: {},
  }]);
});

test("gateway websocket proxy cancels failed reconnect response bodies", async () => {
  const upstream = new FakeWebSocket("upstream");
  let cancelled = false;
  /** @type {Array<[number, string]>} */
  const sessions = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => new Response(/** @type {any} */ ({
      cancel: async () => {
        cancelled = true;
      },
    }), { status: 503 }),
    null,
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    },
    {
      reconnectDelaysMs: [0],
    }
  );

  upstream.closed = { code: 1011, reason: "broken" };
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "needs-reconnect" });

  await waitFor(() => cancelled);
  await waitFor(() => responseWebSocket(response).closed !== null);
  assert.deepEqual(responseWebSocket(response).closed, {
    code: 1011,
    reason: "upstream send failed",
  });
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "reconnect_failed");
  assert.ok(sessions[0][0] >= 0);
  assert.deepEqual(events, [{
    level: "warn",
    event: "websocket_reconnect_failed",
    fields: { reason: "send_failed" },
  }]);
});

test("gateway websocket proxy keeps a client send alive across bounded reconnect", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  /** @type {string[]} */
  const outcomes = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];
  let attempts = 0;

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => {
      attempts += 1;
      if (attempts < 3) return new Response(null, { status: 503 });
      return websocketResponse(upstream2);
    },
    (/** @type {string} */ outcome) => outcomes.push(outcome),
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
    },
    {
      reconnectDelaysMs: [0, 0, 0],
    }
  );

  upstream1.closed = { code: 1011, reason: "broken" };
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "during-restart" });

  await waitFor(() => upstream2.sent.length === 1);
  assert.deepEqual(upstream2.sent, ["during-restart"]);
  assert.equal(responseWebSocket(response).closed, null);
  assert.equal(attempts, 3);
  assert.deepEqual(outcomes, ["established", "reconnected"]);
  assert.deepEqual(events, []);
});

test("gateway websocket proxy reports retry budget exhaustion", async () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {string[]} */
  const outcomes = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];
  let attempts = 0;

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      attempts += 1;
      return new Response(null, { status: 503 });
    },
    (/** @type {string} */ outcome) => outcomes.push(outcome),
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    },
    {
      reconnectDelaysMs: [0, 0, 0, 0, 0, 0, 0],
    }
  );

  upstream.dispatch("close", { code: 1011, reason: "runtime restart" });

  await waitFor(() => responseWebSocket(response).closed !== null, 10_000);
  assert.equal(attempts, 7);
  assert.deepEqual(responseWebSocket(response).closed, {
    code: 1011,
    reason: "upstream reconnect failed",
  });
  assert.deepEqual(outcomes, [
    "established",
    "upstream_abnormal_close",
    "reconnect_failed",
  ]);
  assert.deepEqual(events.at(-1), {
    level: "warn",
    event: "websocket_reconnect_failed",
    fields: { reason: "retry_budget_exhausted" },
  });
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "reconnect_failed");
});

test("gateway websocket proxy options parse reconnect delays and buffer limits from env", () => {
  assert.deepEqual(webSocketProxyOptionsFromEnv({
    WEBSOCKET_RECONNECT_DELAYS_MS: "0,25,100",
    WEBSOCKET_MAX_BUFFERED_MESSAGES: "3",
  }), {
    reconnectDelaysMs: [0, 25, 100],
    maxBufferedClientMessages: 3,
  });
  assert.deepEqual(webSocketProxyOptionsFromEnv({}), {});
});

test("gateway websocket proxy options warn once for invalid env values", async () => {
  /** @type {Array<Record<string, any>>} */
  const warnings = [];
  await withMockedProperty(console, "log", (/** @type {string} */ line) => {
    warnings.push(parseJsonText(line, "gateway websocket warning log"));
  }, async () => {
    const env = {
      WEBSOCKET_RECONNECT_DELAYS_MS: "0,nope,100",
      WEBSOCKET_MAX_BUFFERED_MESSAGES: "0",
    };

    assert.deepEqual(webSocketProxyOptionsFromEnv(env), {});
    assert.deepEqual(webSocketProxyOptionsFromEnv(env), {});
  });

  assert.deepEqual(warnings.map(({ ts: _ts, ...entry }) => entry), [
    {
      level: "warn",
      service: "gateway",
      event: "websocket_config_invalid",
      variable: "WEBSOCKET_MAX_BUFFERED_MESSAGES",
      value: "0",
      minimum: 1,
      fallback: 64,
    },
    {
      level: "warn",
      service: "gateway",
      event: "websocket_config_invalid",
      variable: "WEBSOCKET_RECONNECT_DELAYS_MS",
      value: "0,nope,100",
      fallback: "0,100,250,500,1000,2000,5000",
    },
  ]);
});

test("gateway websocket proxy options clamp oversized buffer limits", async () => {
  /** @type {Array<Record<string, any>>} */
  const warnings = [];
  await withMockedProperty(console, "log", (/** @type {string} */ line) => {
    warnings.push(parseJsonText(line, "gateway websocket warning log"));
  }, async () => {
    const env = { WEBSOCKET_MAX_BUFFERED_MESSAGES: "999999" };
    assert.deepEqual(webSocketProxyOptionsFromEnv(env), {
      maxBufferedClientMessages: 1024,
    });
    assert.deepEqual(webSocketProxyOptionsFromEnv(env), {
      maxBufferedClientMessages: 1024,
    });
  });

  assert.deepEqual(warnings.map(({ ts: _ts, ...entry }) => entry), [{
    level: "warn",
    service: "gateway",
    event: "websocket_config_clamped",
    variable: "WEBSOCKET_MAX_BUFFERED_MESSAGES",
    value: "999999",
    cap: 1024,
  }]);
});

test("gateway websocket proxy honors configured reconnect buffer limit", async () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {string[]} */
  const outcomes = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => await new Promise(() => {}),
    (/** @type {string} */ outcome) => outcomes.push(outcome),
    {
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    },
    {
      maxBufferedClientMessages: 2,
    }
  );

  upstream.dispatch("close", { code: 1011, reason: "runtime restart" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued-0" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued-1" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued-2" });

  assert.deepEqual(responseWebSocket(response).closed, {
    code: 1013,
    reason: "websocket send buffer full",
  });
  assert.ok(outcomes.includes("client_buffer_overflow"));
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "client_buffer_overflow");
});

test("gateway websocket proxy reports active, detached, and buffered gauges", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  /** @type {Array<unknown[]>} */
  const adjustments = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];
  const { promise: reconnecting, resolve: resolveReconnect } = Promise.withResolvers();

  proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => await reconnecting,
    null,
    {
      adjustConnections: (/** @type {string} */ state, /** @type {number} */ delta) => adjustments.push(["connection", state, delta]),
      adjustBufferedMessages: (/** @type {number} */ delta) => adjustments.push(["buffer", delta]),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  upstream1.dispatch("close", { code: 1011, reason: "runtime restart" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued" });
  resolveReconnect(websocketResponse(upstream2));
  await waitFor(() => upstream2.sent.length === 1);
  /** @type {any} */ (lastPair)[1].dispatch("close", { code: 1000, reason: "client done" });

  assert.deepEqual(adjustments, [
    ["connection", "active", 1],
    ["connection", "detached", 1],
    ["buffer", 1],
    ["connection", "detached", -1],
    ["buffer", -1],
    ["connection", "active", -1],
  ]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "client_closed");
  assert.ok(sessions[0][0] >= 0);
});

test("gateway websocket proxy closes a late reconnect socket after client close", async () => {
  const upstream1 = new FakeWebSocket("upstream1");
  const upstream2 = new FakeWebSocket("upstream2");
  /** @type {string[]} */
  const outcomes = [];
  const { promise: reconnecting, resolve: resolveReconnect } = Promise.withResolvers();

  proxyGatewayWebSocket(
    websocketResponse(upstream1),
    async () => await reconnecting,
    (/** @type {string} */ outcome) => outcomes.push(outcome)
  );

  upstream1.dispatch("close", { code: 1011, reason: "runtime restart" });
  /** @type {any} */ (lastPair)[1].dispatch("message", { data: "queued" });
  /** @type {any} */ (lastPair)[1].dispatch("close", { code: 1000, reason: "client done" });
  resolveReconnect(websocketResponse(upstream2));

  await waitFor(() => upstream2.closed !== null);
  assert.deepEqual(upstream2.closed, { code: 1001, reason: "client closed" });
  assert.deepEqual(outcomes, ["established", "upstream_abnormal_close"]);
});

test("gateway websocket proxy closes when client messages exceed the reconnect buffer", async () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {string[]} */
  const outcomes = [];
  /** @type {Array<unknown[]>} */
  const adjustments = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];

  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => await new Promise(() => {}),
    (/** @type {string} */ outcome) => outcomes.push(outcome),
    {
      adjustConnections: (/** @type {string} */ state, /** @type {number} */ delta) => adjustments.push(["connection", state, delta]),
      adjustBufferedMessages: (/** @type {number} */ delta) => adjustments.push(["buffer", delta]),
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  upstream.dispatch("close", { code: 1011, reason: "runtime restart" });
  for (let i = 0; i < 65; i += 1) {
    /** @type {any} */ (lastPair)[1].dispatch("message", { data: `queued-${i}` });
  }

  assert.deepEqual(responseWebSocket(response).closed, {
    code: 1013,
    reason: "websocket send buffer full",
  });
  assert.ok(outcomes.includes("client_buffer_overflow"));
  assert.deepEqual(adjustments.slice(-3), [
    ["connection", "active", -1],
    ["connection", "detached", -1],
    ["buffer", -64],
  ]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "client_buffer_overflow");
  assert.ok(sessions[0][0] >= 0);
  assert.deepEqual(events, [
    {
      level: "warn",
      event: "websocket_upstream_abnormal_close",
      fields: { code: 1011, reason: "runtime restart" },
    },
    {
      level: "warn",
      event: "websocket_client_buffer_overflow",
      fields: { buffered_messages: 64 },
    },
  ]);
});

test("gateway websocket proxy reports client error session lifetime", () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {Array<[number, string]>} */
  const sessions = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];

  proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    },
    null,
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  /** @type {any} */ (lastPair)[1].dispatch("close", { code: 1006, reason: "broken client" });

  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "client_error");
  assert.ok(sessions[0][0] >= 0);
});

test("gateway websocket proxy reports downstream error session lifetime", () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {Array<[number, string]>} */
  const sessions = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const events = [];

  proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    },
    null,
    {
      recordEvent: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) => events.push({ level, event, fields }),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  /** @type {any} */ (lastPair)[1].dispatch("error");

  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "downstream_error");
  assert.ok(sessions[0][0] >= 0);
  assert.deepEqual(events, [{
    level: "warn",
    event: "websocket_downstream_error",
    fields: {},
  }]);
});

test("gateway websocket proxy propagates normal upstream close", () => {
  const upstream = new FakeWebSocket("upstream");
  /** @type {Array<unknown[]>} */
  const adjustments = [];
  /** @type {Array<[number, string]>} */
  const sessions = [];
  const response = proxyGatewayWebSocket(
    websocketResponse(upstream),
    async () => {
      throw new Error("not used");
    },
    null,
    {
      adjustConnections: (/** @type {string} */ state, /** @type {number} */ delta) => adjustments.push(["connection", state, delta]),
      recordSessionLifetime: (/** @type {number} */ durationMs, /** @type {string} */ outcome) => sessions.push([durationMs, outcome]),
    }
  );

  upstream.dispatch("close", { code: 1000, reason: "done" });

  assert.deepEqual(responseWebSocket(response).closed, { code: 1000, reason: "done" });
  assert.deepEqual(adjustments, [
    ["connection", "active", 1],
    ["connection", "active", -1],
  ]);
  assert.equal(sessions.length, 1);
  assert.equal(sessions[0][1], "upstream_normal_close");
  assert.ok(sessions[0][0] >= 0);
});
