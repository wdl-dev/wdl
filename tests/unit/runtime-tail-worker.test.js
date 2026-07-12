import { beforeEach, afterEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { installMockFetch, makeRecordingFetch } from "../helpers/mock-fetch.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { runtimeProxyBindingStubUrl, sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";
import { delay } from "../helpers/timing.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const OBSERVABILITY_STUB_SRC = `const createLogger = (service) => (level, event, fields) => /** @type {any} */ (globalThis).__runtimeTailLogs.push({ service, level, event, fields });
const createLogLevelBinder = () => {
  let logLevelSet = false;
  return (env) => {
    if (logLevelSet) return;
    /** @type {any} */ (globalThis).__runtimeTailLevels.push(env?.LOG_LEVEL);
    logLevelSet = true;
  };
};`;
const forwarderSrc = applyModuleReplacements(readRepositoryFile("runtime/tail-forwarder.js"), [
  [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
  [/from "shared-errors";/, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
  [/from "shared-internal-auth";/, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
]);
const tailWorkerRawSrc = applyModuleReplacements(readRepositoryFile("runtime/tail-worker.js"), [
  [
    /import \{ createLogLevelBinder, createLogger \} from "shared-observability";/,
    OBSERVABILITY_STUB_SRC
  ],
]);

/** @param {string} tag */
function tailWorkerSrc(tag) {
  const forwarderUrl = moduleDataUrl(`${forwarderSrc}\n// ${tag}-forwarder`);
  return applyModuleReplacements(tailWorkerRawSrc, [
    [/from "runtime-tail-forwarder";/, `from ${JSON.stringify(forwarderUrl)};`],
  ]);
}

/** @type {any} */ (globalThis).__runtimeTailLogs = [];
/** @type {any} */ (globalThis).__runtimeTailLevels = [];
let restoreFetch = () => {};

// `__runtimeTailLogs`/`__runtimeTailLevels` are shared across all tests
// (the dataURL re-import trick gives a fresh module but reuses global
// accumulators), so reset centrally to keep state from leaking.
beforeEach(() => {
  restoreFetch();
  restoreFetch = () => {};
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  /** @type {any} */ (globalThis).__runtimeTailLevels.length = 0;
});

afterEach(() => {
  restoreFetch();
  restoreFetch = () => {};
});

const mod = await import(moduleDataUrl(tailWorkerSrc("default")));
const handler = mod.default;
const TAIL_ENV = { SERVICE_NAME: "runtime-tail" };
const TAIL_PROXY_ENV = {
  SERVICE_NAME: "user-runtime-tail",
  REDIS_PROXY_URL: "http://proxy",
  WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
};

/**
 * @param {{ workerId?: string | null, requestId?: string | null, logs: any[] }} opts
 */
function fetchEvent({ workerId = null, requestId = null, logs }) {
  return {
    event: {
      request: {
        headers: {
          ...(workerId != null ? { "x-worker-id": workerId } : {}),
          ...(requestId != null ? { "x-request-id": requestId } : {}),
        },
      },
    },
    logs,
  };
}

test("tail-worker: missing env.SERVICE_NAME emits a fail-loud tail_service_name_missing (first tick only)", async () => {
  const freshMod = await import(moduleDataUrl(tailWorkerSrc("missing-service-name")));
  const freshHandler = freshMod.default;
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  freshHandler.tail([fetchEvent({ logs: [] })]);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].event, "tail_service_name_missing");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].level, "error");
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  freshHandler.tail([fetchEvent({ logs: [] })]);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 0);
});

test("tail-worker: reads worker_id + request_id from headers and maps the console levels workerd actually delivers", () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;

  handler.tail(
    [
      fetchEvent({
        workerId: "demo:hello:v3",
        requestId: "rid-42",
        logs: [
          { level: "log", message: ["alpha", 1] },
          { level: "warn", message: { ok: true } },
          { level: "error", message: "boom" },
        ],
      }),
    ],
    TAIL_ENV,
  );

  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLogs, [
    {
      service: "runtime-tail",
      level: "info",
      event: "worker_console",
      fields: { console_level: "log", message: ["alpha", 1], worker_id: "demo:hello:v3", request_id: "rid-42" },
    },
    {
      service: "runtime-tail",
      level: "warn",
      event: "worker_console",
      fields: { console_level: "warn", message: { ok: true }, worker_id: "demo:hello:v3", request_id: "rid-42" },
    },
    {
      service: "runtime-tail",
      level: "error",
      event: "worker_console",
      fields: { console_level: "error", message: "boom", worker_id: "demo:hello:v3", request_id: "rid-42" },
    },
  ]);
});

test("tail-worker: binds LOG_LEVEL through shared observability", () => {
  /** @type {any} */ (globalThis).__runtimeTailLevels.length = 0;
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;

  handler.tail(
    [fetchEvent({ workerId: "x:y:v1", requestId: "r", logs: [{ level: "log", message: "hi" }] })],
    { ...TAIL_ENV, LOG_LEVEL: "warn" },
  );
  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLevels, ["warn"]);

  handler.tail(
    [fetchEvent({ workerId: "x:y:v1", requestId: "r", logs: [] })],
    { ...TAIL_ENV, LOG_LEVEL: "" },
  );
  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLevels, ["warn"]);

  handler.tail([fetchEvent({ workerId: "x:y:v1", requestId: "r", logs: [] })], TAIL_ENV);
  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLevels, ["warn"]);
});

test("tail-worker: sanitizes BigInt, circular refs, and unserializable values in message so JSON.stringify never throws", () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;

  /** @type {any} */
  const cyclic = { a: 1 };
  cyclic.self = cyclic;

  const withBigInt = { count: 9007199254740993n, nested: [1n, 2n] };

  const base = { workerId: "demo:x:v1", requestId: "r1" };
  handler.tail([
    fetchEvent({ ...base, logs: [{ level: "log", message: withBigInt }] }),
    fetchEvent({ ...base, logs: [{ level: "log", message: cyclic }] }),
    fetchEvent({ ...base, logs: [{ level: "log", message: ["mix", 42n, cyclic] }] }),
  ], TAIL_ENV);

  const msgs = /** @type {any} */ (globalThis).__runtimeTailLogs.map((/** @type {any} */ l) => l.fields.message);
  assert.deepEqual(msgs[0], { count: "9007199254740993n", nested: ["1n", "2n"] });
  assert.deepEqual(msgs[1], { a: 1, self: "[Circular]" });
  assert.deepEqual(msgs[2], ["mix", "42n", { a: 1, self: "[Circular]" }]);

  for (const line of /** @type {any} */ (globalThis).__runtimeTailLogs) {
    assert.doesNotThrow(() => JSON.stringify(line));
  }
});

test("tail-worker: preserves magic console object keys as data fields", () => {
  handler.tail([
    fetchEvent({
      logs: [{ level: "log", message: JSON.parse('{"__proto__":"tail-value"}') }],
    }),
  ], TAIL_ENV);

  const message = /** @type {Record<string, unknown>} */ (/** @type {any} */ (globalThis).__runtimeTailLogs[0].fields.message);
  assert.equal(Object.hasOwn(message, "__proto__"), true);
  assert.equal(message.__proto__, "tail-value");
});

test("tail-worker: getter failures in console messages degrade instead of throwing", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const message = {};
  Object.defineProperty(message, "boom", {
    enumerable: true,
    get() { throw new Error("getter exploded"); },
  });

  await handler.tail([
    fetchEvent({
      workerId: "demo:x:v1",
      requestId: "r-getter",
      logs: [{ level: "log", message }],
    }),
  ], TAIL_ENV);

  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].event, "worker_console");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].fields.message, "[object Object]");
});

test("tail-worker: omits worker_id + request_id when console events have no request headers", () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;

  handler.tail([
    null,
    { event: { scheduledTime: 123, cron: "* * * * *" }, logs: [{ level: "log", message: "cron-ran" }] },
    fetchEvent({ logs: [{ level: "log", message: "no-ids" }] }),
  ], TAIL_ENV);

  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLogs, [
    {
      service: "runtime-tail",
      level: "info",
      event: "worker_console",
      fields: { console_level: "log", message: "cron-ran" },
    },
    {
      service: "runtime-tail",
      level: "info",
      event: "worker_console",
      fields: { console_level: "log", message: "no-ids" },
    },
  ]);
});

// Fresh module instance keeps SERVICE_NAME memoization independent from the
// shared handler. Distinct trailing comment forces a separate ES-module cache
// entry.
const freshMod = await import(moduleDataUrl(tailWorkerSrc("fresh-user-runtime")));
const freshHandler = freshMod.default;

test("tail-worker: env.SERVICE_NAME propagates into the emitted service label (real capnp path)", () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  freshHandler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "rid-a", logs: [{ level: "log", message: "hello" }] })],
    { SERVICE_NAME: "user-runtime-tail" }
  );
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].service, "user-runtime-tail");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].event, "worker_console");
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  freshHandler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "rid-b", logs: [{ level: "log", message: "again" }] })],
    {}
  );
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].service, "user-runtime-tail");
});

// Active-set / postEvent tests. `freshTailHandler(tag)` re-imports the
// module via a dataURL trailing-comment so each test gets an
// independent activeSet — order-independent.

/** @param {{ activeSequence: any[] }} opts */
function makeFetchSpy({ activeSequence }) {
  /** @type {any[]} */
  const calls = [];
  let activeIdx = 0;
  restoreFetch();
  restoreFetch = installMockFetch(makeRecordingFetch(calls, {
    capture: (_call, url, init) => ({ url: String(url), method: init.method || "GET", body: init.body }),
    response: (url) => {
      const u = String(url);
      if (u.endsWith("/logs/tail/active")) {
        const list = activeIdx < activeSequence.length
          ? activeSequence[activeIdx++]
          : activeSequence.at(-1);
        return Response.json({ active: list });
      }
      if (u.endsWith("/logs/tail/append")) {
        return Response.json({});
      }
      return Response.json({}, { status: 404 });
    },
  }));
  return calls;
}

function fakeCtx() {
  /** @type {Promise<unknown>[]} */
  const tasks = [];
  return {
    waitUntil(/** @type {Promise<unknown>} */ p) { tasks.push(p); },
    tasks,
  };
}

/** @param {string} tag */
async function freshTailHandler(tag) {
  const m = await import(moduleDataUrl(tailWorkerSrc(tag)));
  return m.default;
}

function keepEventLoopAlive(intervalMs = 25) {
  const keepAliveTimer = setInterval(() => {}, intervalMs);
  return () => clearInterval(keepAliveTimer);
}

/** @param {{ body?: unknown }} call */
function tailAppendBody(call) {
  return parseJsonObjectRequestBody({ body: call.body }, "tail append request body");
}

/** @param {{ body?: unknown }} call */
function tailAppendPayload(call) {
  const body = tailAppendBody(call);
  const json = /** @type {string} */ (body.json);
  assert.equal(typeof json, "string", "tail append body.json must be a string");
  return parseJsonObjectRequestBody({ body: json }, "tail append payload");
}

test("tail-worker: 50 unsubscribed events in one batch → exactly one /logs/tail/active fetch + zero /logs/tail/append", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[]] });
  const ctx = fakeCtx();
  const handler50 = await freshTailHandler("active-set-batch-dedupe");

  const logs = Array.from({ length: 50 }, (_, i) => ({ level: "log", message: `line-${i}` }));
  await handler50.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r1", logs })],
    TAIL_PROXY_ENV,
    ctx,
  );

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 1, `expected 1 active fetch, got ${activeFetches.length}`);
  assert.equal(appendFetches.length, 0, "no subscriber → no /logs/tail/append");
  await Promise.all(ctx.tasks);
});

test("tail-worker: oversized console events are dropped whole with a metadata warning", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [["demo:x"]] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("oversized-console-drop");

  await handler.tail(
    [fetchEvent({
      workerId: "demo:x:v1",
      requestId: "r-big",
      logs: [{ level: "log", message: "x".repeat(6 * 1024) }],
    })],
    TAIL_PROXY_ENV,
    ctx,
  );
  await Promise.all(ctx.tasks);

  assert.deepEqual(/** @type {any} */ (globalThis).__runtimeTailLogs, [{
    service: "user-runtime-tail",
    level: "warn",
    event: "worker_console_dropped",
    fields: {
      code: "event_too_large",
      dropped_event: "worker_console",
      limit_bytes: 5120,
      console_level: "log",
      worker_id: "demo:x:v1",
      request_id: "r-big",
    },
  }]);
  const append = calls.find((c) => c.url.endsWith("/logs/tail/append"));
  assert.ok(append, "active tailer should receive a warning append");
  const body = tailAppendBody(append);
  const payload = tailAppendPayload(append);
  assert.equal(payload.event, "tail_warning");
  assert.equal(payload.code, "event_too_large");
  assert.equal(payload.dropped_event, "worker_console");
  const json = /** @type {string} */ (body.json);
  assert.equal(typeof json, "string");
  assert.ok(json.length < 5120);
  assert.equal(json.includes("x".repeat(100)), false);
});

test("tail-worker: nested oversized console objects are dropped before JSON stringify", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [["demo:x"]] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("oversized-nested-console-drop");
  const message = {
    outer: Array.from({ length: 80 }, (_, i) => ({
      [`key_${i}`]: "payload".repeat(16),
    })),
  };

  await handler.tail(
    [fetchEvent({
      workerId: "demo:x:v1",
      requestId: "r-nested-big",
      logs: [{ level: "warn", message }],
    })],
    TAIL_PROXY_ENV,
    ctx,
  );
  await Promise.all(ctx.tasks);

  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].event, "worker_console_dropped");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].fields.console_level, "warn");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].fields.request_id, "r-nested-big");
  const append = calls.find((c) => c.url.endsWith("/logs/tail/append"));
  assert.ok(append, "active tailer should receive only a warning append");
  const body = tailAppendBody(append);
  assert.equal(tailAppendPayload(append).event, "tail_warning");
  const json = /** @type {string} */ (body.json);
  assert.equal(typeof json, "string");
  assert.equal(json.includes("payloadpayload"), false);
});

test("tail-worker: many tiny array entries still spend budget and drop whole event", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [["demo:x"]] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("oversized-array-shape-drop");

  await handler.tail(
    [fetchEvent({
      workerId: "demo:x:v1",
      requestId: "r-many-empty",
      logs: [{ level: "log", message: Array.from({ length: 6000 }, () => "") }],
    })],
    TAIL_PROXY_ENV,
    ctx,
  );
  await Promise.all(ctx.tasks);

  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].event, "worker_console_dropped");
  assert.equal(/** @type {any} */ (globalThis).__runtimeTailLogs[0].fields.request_id, "r-many-empty");
  const append = calls.find((c) => c.url.endsWith("/logs/tail/append"));
  assert.ok(append, "active tailer should receive only a warning append");
  assert.equal(tailAppendPayload(append).event, "tail_warning");
});

test("tail-worker: many DIFFERENT unsubscribed keys in one batch still pay one refresh, not N", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[]] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("active-set-multi-key-dedupe");

  const events = Array.from({ length: 10 }, (_, i) => fetchEvent({
    workerId: `demo:w${i}:v1`,
    requestId: `r${i}`,
    logs: [{ level: "log", message: `from-${i}` }],
  }));
  await handler.tail(
    events,
    TAIL_PROXY_ENV,
    ctx,
  );

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 1,
    `expected at most 1 active fetch even across 10 unique keys; got ${activeFetches.length}`);
  assert.equal(appendFetches.length, 0);
  await Promise.all(ctx.tasks);
});

test("tail-worker: cold miss probes immediately without startup delay", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[]] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("active-set-cold-miss-immediate");

  const start = performance.now();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r-cold", logs: [{ level: "log", message: "hi" }] })],
    TAIL_PROXY_ENV,
    ctx,
  );
  const elapsed = performance.now() - start;

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  assert.equal(activeFetches.length, 1, "expected 1 cold-miss fetch");
  assert.ok(elapsed < 80,
    `expected cold miss to probe without startup delay; tail() returned in ${elapsed.toFixed(1)}ms`);
  await Promise.all(ctx.tasks);
});

test("tail-worker: fresh miss cache suppresses repeated active probes", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[]] });
  const handler = await freshTailHandler("active-set-miss-cache");

  const ctx1 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r1", logs: [{ level: "log", message: "one" }] })],
    TAIL_PROXY_ENV,
    ctx1,
  );
  await Promise.all(ctx1.tasks);

  const ctx2 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r2", logs: [{ level: "log", message: "two" }] })],
    TAIL_PROXY_ENV,
    ctx2,
  );
  await Promise.all(ctx2.tasks);

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 1,
    `expected fresh miss cache to avoid a second active probe; got ${activeFetches.length}`);
  assert.equal(appendFetches.length, 0);
});

test("tail-worker: fresh miss cache expires instead of sliding forever", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[], ["demo:x"]] });
  const handler = await freshTailHandler("active-set-miss-cache-expires");

  for (let i = 0; i < 22; i += 1) {
    const ctx = fakeCtx();
    await handler.tail(
      [fetchEvent({ workerId: "demo:x:v1", requestId: `r${i}`, logs: [{ level: "log", message: `line-${i}` }] })],
      TAIL_PROXY_ENV,
      ctx,
    );
    await Promise.all(ctx.tasks);
    if (i < 21) await delay(100);
  }

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 2,
    `expected miss cache to expire and re-probe; got ${activeFetches.length}`);
  assert.ok(appendFetches.length >= 1,
    "expected events after the re-probe discovers the active key to append");
});

test("tail-worker: miss cache is scoped to the worker key", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  const calls = makeFetchSpy({ activeSequence: [[], ["demo:y"]] });
  const handler = await freshTailHandler("active-set-miss-cache-per-key");

  const ctx1 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r1", logs: [{ level: "log", message: "one" }] })],
    TAIL_PROXY_ENV,
    ctx1,
  );
  await Promise.all(ctx1.tasks);

  const ctx2 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:y:v1", requestId: "r2", logs: [{ level: "log", message: "two" }] })],
    TAIL_PROXY_ENV,
    ctx2,
  );
  await Promise.all(ctx2.tasks);

  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 2,
    `expected a different worker key to probe despite x's fresh miss; got ${activeFetches.length}`);
  assert.equal(appendFetches.length, 1);
});

test("tail-worker: hung proxy /logs/tail/active actually times out via AbortSignal (real timeout fires)", async () => {
  // Mock fetch honors init.signal and rejects on abort but never
  // resolves on its own, so the only way the test completes is if
  // ACTIVE_FETCH_TIMEOUT_MS actually fires.
  /** @type {any[]} */
  const calls = [];
  restoreFetch();
  restoreFetch = installMockFetch(makeRecordingFetch(calls, {
    capture: (_call, url, init) => ({ url: String(url), method: init.method || "GET" }),
    response: (_url, init) => new Promise((_resolve, reject) => {
      const sig = init.signal;
      if (!sig) return reject(new Error("test mock: cache did not pass a signal"));
      const onAbort = () => reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      if (sig.aborted) return onAbort();
      sig.addEventListener("abort", onAbort, { once: true });
    }),
  }));
  const ctx = fakeCtx();
  const handler = await freshTailHandler("active-set-fetch-timeout-real");

  // AbortSignal.timeout uses an unref'd timer; without a keep-alive
  // node:test bails ("Promise still pending") before it fires.
  const stopKeepingEventLoopAlive = keepEventLoopAlive();
  let elapsed;
  try {
    const start = performance.now();
    await handler.tail(
      [fetchEvent({ workerId: "demo:x:v1", requestId: "r-hung", logs: [{ level: "log", message: "hi" }] })],
      TAIL_PROXY_ENV,
      ctx,
    );
    elapsed = performance.now() - start;
  } finally {
    stopKeepingEventLoopAlive();
  }

  // Expected: 150ms ACTIVE_FETCH_TIMEOUT_MS plus scheduler slop.
  assert.ok(elapsed >= 120,
    `expected tail() to wait for active fetch timeout (~150ms); got ${elapsed.toFixed(1)}ms`);
  assert.ok(elapsed < 400,
    `expected tail() to return within timeout + slop; got ${elapsed.toFixed(1)}ms`);
  const activeFetches = calls.filter((c) => c.url.endsWith("/logs/tail/active"));
  const appendFetches = calls.filter((c) => c.url.endsWith("/logs/tail/append"));
  assert.equal(activeFetches.length, 1, "one cold fetch should fire");
  assert.equal(appendFetches.length, 0, "no append after timeout-empty cache");
  await Promise.all(ctx.tasks);
});

test("tail-worker: positive cache evicts when proxy reports key as no longer active (no perpetual POSTs)", async () => {
  /** @type {any} */ (globalThis).__runtimeTailLogs.length = 0;
  // First refresh: active. Subsequent refreshes: empty (mirrors proxy
  // TTL expiry).
  const calls = makeFetchSpy({ activeSequence: [["demo:x"], []] });
  const ctx = fakeCtx();
  const handler = await freshTailHandler("active-set-evict");

  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r1", logs: [{ level: "log", message: "hot" }] })],
    TAIL_PROXY_ENV,
    ctx,
  );
  await Promise.all(ctx.tasks);
  let appendCount = calls.filter((c) => c.url.endsWith("/logs/tail/append")).length;
  assert.equal(appendCount, 1, "first event with active subscriber should forward");

  // Past ACTIVE_HIT_MAX_AGE_MS: next event forces a refresh.
  await delay(600);

  const ctx2 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r2", logs: [{ level: "log", message: "cold" }] })],
    TAIL_PROXY_ENV,
    ctx2,
  );
  await Promise.all(ctx2.tasks);
  appendCount = calls.filter((c) => c.url.endsWith("/logs/tail/append")).length;
  assert.equal(appendCount, 1,
    `expected no new append after eviction; got ${appendCount} total`);

  await delay(600);
  const ctx3 = fakeCtx();
  await handler.tail(
    [fetchEvent({ workerId: "demo:x:v1", requestId: "r3", logs: [{ level: "log", message: "still-cold" }] })],
    TAIL_PROXY_ENV,
    ctx3,
  );
  await Promise.all(ctx3.tasks);
  appendCount = calls.filter((c) => c.url.endsWith("/logs/tail/append")).length;
  assert.equal(appendCount, 1,
    `cache must stay evicted; got ${appendCount} total appends after key went inactive`);
});
