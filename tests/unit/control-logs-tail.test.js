import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModuleFresh,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { compileSharedAuthRoles } from "../helpers/load-auth-roles.js";
import { delay, waitUntil } from "../helpers/timing.js";

const { sharedAuthRolesUrl } = await compileSharedAuthRoles();
const SHARED_NS_PATTERN_URL = repositoryFileUrl("shared/ns-pattern.js");

/** @param {{ keepaliveMs?: number }} [options] */
function loadLogsTailHandler(options = {}) {
  /** @type {Array<[RegExp | string, string]>} */
  const replacements = [];
  if (options.keepaliveMs) {
    replacements.push([/const SSE_KEEPALIVE_MS = 5_000;/, `const SSE_KEEPALIVE_MS = ${options.keepaliveMs};`]);
  }
  replacements.push(
    [
      /import \{ envValueOr \} from "shared-env";/,
      "const envValueOr = (value, fallback) => value == null || value === '' ? fallback : value;",
    ],
    [
      /import \{ RedisSession, redisDbFromEnv \} from "shared-redis";/,
      `class RedisSession {
        constructor(addr, opts = {}) {
          this.addr = addr;
          this.opts = opts;
          /** @type {any} */ (globalThis).__tailSessions.push(this);
          this.xReadCalls = [];
          this.publishCalls = [];
          this.evalCalls = [];
          this.closed = false;
        }
        async open() {
          this.openStarted = true;
          this.socket = {};
          this.openPromise = (async () => {
            const state = /** @type {any} */ (globalThis).__tailState;
            if (state.openBlocker) await state.openBlocker;
            this.opened = true;
          })();
          await this.openPromise;
        }
        async publish(channel, payload) { this.publishCalls.push([channel, payload]); }
        async eval(script, keys, args) { this.evalCalls.push([script, keys, args]); }
        async xRead(...args) { this.xReadCalls.push(args); return null; }
        hasOpenResources() { return Boolean(this.socket); }
        async close() { this.closed = true; }
      }
      const redisDbFromEnv = (env, name) => Number(env?.[name] || 0);`,
    ],
    [
      /from "shared-auth-roles";/,
      `from ${JSON.stringify(sharedAuthRolesUrl)};`,
    ],
    [
      /from "shared-ns-pattern";/,
      `from ${JSON.stringify(SHARED_NS_PATTERN_URL)};`,
    ],
    [
      /import \{ compareStreamIds, isValidResumeId \} from "control-lib";/,
      `const compareStreamIds = () => 0;
       const isValidResumeId = () => true;`,
    ],
    [
      /import \{ controlTailRedis, errMessage, jsonError, requireControlLog \} from "control-shared";/,
      `const jsonError = (status, error, message) =>
         Response.json({ error, message }, { status });
       const errMessage = (err) => err instanceof Error ? err.message : String(err);
       const state = /** @type {any} */ (globalThis).__tailState;
       const requireControlLog = () => state.log;
       const controlTailRedis = () => state.dataRedis || state.redis;`,
    ],
  );
  return importRepositoryModuleFresh("control/handlers/logs-tail.js", replacements);
}

function resetTailState() {
  /** @type {any} */ (globalThis).__tailSessions = [];
  /** @type {any} */ (globalThis).__tailState = {
    redis: {},
    dataRedis: {
      /** @type {Array<[string, string[], string[]]>} */
      evalCalls: [],
      /** @type {Map<string, string>} */
      activeFields: new Map(),
      activeCount: 0,
      evalError: null,
      sessionCalls: 0,
      /** @template T @param {(session: any) => Promise<T>} fn */
      async session(fn) {
        this.sessionCalls += 1;
        return await fn(this);
      },
      /** @param {string} script @param {string[]} keys @param {string[]} args */
      async eval(script, keys, args) {
        this.evalCalls.push([script, keys, args]);
        if (this.evalError) throw this.evalError;
        const maxEntries = Number(args[1]);
        let count = this.activeCount;
        for (const field of args.slice(2)) {
          if (!this.activeFields.has(field)) {
            if (count >= maxEntries) continue;
            count += 1;
          }
          this.activeFields.set(field, "1");
        }
        this.activeCount = count;
        return this.activeFields.size;
      },
    },
    logs: [],
    openBlocker: null,
    log: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ data) => {
      /** @type {any} */ (globalThis).__tailState.logs.push({ level, event, data });
    },
  };
}

/** @param {{ read(): Promise<{ value?: Uint8Array, done?: boolean }> }} reader */
async function readText(reader) {
  const { value, done } = await reader.read();
  return { done, text: value ? new TextDecoder().decode(value) : "" };
}

/** @param {string} value */
function utf8(value) {
  return new TextEncoder().encode(value);
}

test("RedisTailCursor decodes stream batches and keeps per-worker cursors", async () => {
  resetTailState();
  const { RedisTailCursor, tailActivationKeys } = await loadLogsTailHandler();
  const cursor = new RedisTailCursor({ ns: "demo", workers: ["foo", "bar"] });

  const batch = cursor.decode([
    [utf8("logs:demo:foo:s"), [
      [utf8("10-0"), [utf8("json"), utf8(JSON.stringify({ event: "worker_console", message: "hi" }))]],
      [utf8("11-0"), [utf8("json"), utf8("{")]],
      [utf8("12-0"), [utf8("other"), utf8("ignored")]],
    ]],
    [utf8("logs:demo:bar:s"), [
      [utf8("20-0"), [utf8("json"), utf8(JSON.stringify({ message: "fallback" }))]],
    ]],
    [utf8("logs:demo:other:s"), [
      [utf8("99-0"), [utf8("json"), utf8(JSON.stringify({ event: "wrong_stream" }))]],
    ]],
  ]);

  assert.deepEqual(tailActivationKeys("demo", ["foo", "bar"]), ["demo:foo", "demo:bar"]);
  assert.deepEqual(cursor.ids, ["12-0", "20-0"]);
  assert.deepEqual(batch.parseFailures, [{ id: "11-0", workerName: "foo" }]);
  assert.equal(batch.events.length, 2);
  assert.deepEqual(batch.events[0], {
    id: "10-0",
    workerName: "foo",
    eventName: "worker_console",
    payloadObj: { event: "worker_console", message: "hi", worker: "foo" },
  });
  assert.deepEqual(batch.events[1], {
    id: "20-0",
    workerName: "bar",
    eventName: "worker_event",
    payloadObj: { message: "fallback", worker: "bar" },
  });
});

test("RedisTailCursor skips array payloads instead of adding object fields to them", async () => {
  resetTailState();
  const { RedisTailCursor } = await loadLogsTailHandler();
  const cursor = new RedisTailCursor({ ns: "demo", workers: ["foo"] });

  const batch = cursor.decode([
    [utf8("logs:demo:foo:s"), [
      [utf8("10-0"), [utf8("json"), utf8(JSON.stringify(["not", "an", "event"]))]],
      [utf8("11-0"), [utf8("json"), utf8(JSON.stringify({ event: "worker_console" }))]],
    ]],
  ]);

  assert.deepEqual(batch.parseFailures, []);
  assert.deepEqual(batch.events, [{
    id: "11-0",
    workerName: "foo",
    eventName: "worker_console",
    payloadObj: { event: "worker_console", worker: "foo" },
  }]);
});

test("logs tail closes after the max session lifetime so reconnect reauthorizes", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "50" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  assert.match((await readText(reader)).text, /tail-open/);
  assert.match((await readText(reader)).text, /hb|tail_warning/);
  await delay(60);
  let warning = await readText(reader);
  if (!warning.text.includes("session_expired")) {
    warning = await readText(reader);
  }
  assert.match(warning.text, /event: tail_warning/);
  assert.match(warning.text, /session_expired/);
  assert.equal((await readText(reader)).done, true);
  await Promise.all(waitUntilPromises);

  assert.equal(/** @type {any} */ (globalThis).__tailSessions.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__tailSessions[0].closed, true);
  assert.ok(/** @type {any} */ (globalThis).__tailState.logs.some((/** @type {any} */ entry) => entry.event === "tail_session_expired"));
});

test("logs tail max-session watchdog closes even without stream cancel", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "50" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail-watchdog",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  assert.match((await readText(reader)).text, /tail-open/);
  await delay(80);
  await Promise.all(waitUntilPromises);

  assert.equal(/** @type {any} */ (globalThis).__tailSessions.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__tailSessions[0].closed, true);
  assert.ok(/** @type {any} */ (globalThis).__tailState.logs.some((/** @type {any} */ entry) => entry.event === "tail_session_expired"));
  await reader.cancel().catch(() => {});
});

test("logs tail idle-pull watchdog closes abandoned streams before max-session", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler({ keepaliveMs: 5 });
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "1000" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail-idle",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  assert.match((await readText(reader)).text, /tail-open/);
  await waitUntil("tail idle watchdog to close session", () =>
    /** @type {any} */ (globalThis).__tailSessions[0]?.closed === true, {
      timeoutMs: 500, intervalMs: 5,
    });
  await Promise.all(waitUntilPromises);

  assert.equal(/** @type {any} */ (globalThis).__tailSessions.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__tailSessions[0].closed, true);
  assert.ok(/** @type {any} */ (globalThis).__tailState.logs.some((/** @type {any} */ entry) => entry.event === "tail_session_idle"));
  await reader.cancel().catch(() => {});
});

test("logs tail idle-pull watchdog closes before the first client read", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler({ keepaliveMs: 5 });
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "1000" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail-idle-before-read",
  });

  assert.equal(response.status, 200);
  await Promise.all(waitUntilPromises);

  const reader = response.body.getReader();
  /** @type {{ done?: boolean, text: string }} */
  let warning = { done: false, text: "" };
  for (let i = 0; i < 5 && !warning.text.includes("session_idle"); i += 1) {
    warning = await readText(reader);
  }
  assert.match(warning.text, /session_idle/);
  assert.equal((await readText(reader)).done, true);
  assert.ok(/** @type {any} */ (globalThis).__tailState.logs.some((/** @type {any} */ entry) => entry.event === "tail_session_idle"));
});

test("logs tail closes session if watchdog fires while Redis open is pending", async () => {
  resetTailState();
  const state = /** @type {any} */ (globalThis).__tailState;
  let releaseOpen = () => {};
  state.openBlocker = new Promise((resolve) => {
    releaseOpen = () => resolve(undefined);
  });

  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "30" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail-open-race",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  const pendingRead = readText(reader);
  const session = /** @type {any} */ (globalThis).__tailSessions[0];
  await waitUntil("tail Redis open to start", () => session.openStarted === true, {
    timeoutMs: 500, intervalMs: 5,
  });
  await delay(50);
  releaseOpen();
  await session.openPromise;
  await Promise.all(waitUntilPromises);
  await waitUntil("tail Redis session to close", () => session.closed === true, {
    timeoutMs: 500, intervalMs: 5,
  });

  assert.equal(session.opened, true);
  assert.equal(session.closed, true);
  assert.match((await pendingRead).text, /session_expired/);
  await reader.cancel().catch(() => {});
});

test("logs tail max-session watchdog closes a socket while Redis open is pending", async () => {
  resetTailState();
  const state = /** @type {any} */ (globalThis).__tailState;
  let releaseOpen = () => {};
  state.openBlocker = new Promise((resolve) => {
    releaseOpen = () => resolve(undefined);
  });

  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit", LOG_TAIL_MAX_SESSION_MS: "30" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail-open-socket-race",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  const pendingRead = readText(reader);
  const session = /** @type {any} */ (globalThis).__tailSessions[0];
  await waitUntil("tail Redis open to assign socket", () => session.socket, {
    timeoutMs: 500, intervalMs: 5,
  });
  await Promise.all(waitUntilPromises);

  assert.equal(session.closed, true);
  releaseOpen();
  await session.openPromise;
  assert.match((await pendingRead).text, /session_expired/);
  await reader.cancel().catch(() => {});
});

test("logs tail emits idle keepalives below common proxy idle timeouts", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: { REDIS_ADDR: "redis://unit" },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  assert.match((await readText(reader)).text, /tail-open/);
  assert.match((await readText(reader)).text, /^: hb /);

  assert.equal(/** @type {any} */ (globalThis).__tailSessions.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__tailSessions[0].xReadCalls[0][1], "5000");
  await reader.cancel();
  await Promise.all(waitUntilPromises);
});

test("logs tail opens its stream session on the data Redis DB", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: {
      REDIS_ADDR: "redis-control:6379",
      DATA_REDIS_ADDR: "redis-data:6379",
      DATA_REDIS_DB: "1",
    },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  const reader = response.body.getReader();
  await readText(reader);
  await reader.cancel();
  await Promise.all(waitUntilPromises);

  assert.equal(/** @type {any} */ (globalThis).__tailSessions.length, 1);
  assert.equal(/** @type {any} */ (globalThis).__tailSessions[0].addr, "redis-data:6379");
  assert.deepEqual(/** @type {any} */ (globalThis).__tailSessions[0].opts, { db: 1 });
});

test("logs tail keeps activation writes off its held blocking-read session", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: {
      REDIS_ADDR: "redis-control:6379",
      DATA_REDIS_ADDR: "redis-data:6379",
      DATA_REDIS_DB: "1",
    },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  const reader = response.body.getReader();
  await readText(reader);
  await reader.cancel();
  await Promise.all(waitUntilPromises);

  const session = /** @type {any} */ (globalThis).__tailSessions[0];
  const dataRedis = /** @type {any} */ (globalThis).__tailState.dataRedis;
  assert.equal(dataRedis.evalCalls.length, 1);
  assert.deepEqual(dataRedis.evalCalls[0].slice(1), [
    ["logs:tail:active"],
    ["30", "10000", "demo:foo"],
  ]);
  assert.equal(dataRedis.activeFields.get("demo:foo"), "1");
  assert.equal(dataRedis.sessionCalls, 0);
  assert.deepEqual(session.evalCalls, []);
  assert.ok(session.xReadCalls.length > 0);
});

test("logs tail batches multi-worker activation while preserving the active cap", async () => {
  resetTailState();
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  /** @type {any} */ (globalThis).__tailState.dataRedis.activeFields.set("demo:bar", "1");
  /** @type {any} */ (globalThis).__tailState.dataRedis.activeCount = 9_999;
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo&worker=bar&worker=baz"),
    env: {
      REDIS_ADDR: "redis-control:6379",
      DATA_REDIS_ADDR: "redis-data:6379",
      DATA_REDIS_DB: "1",
    },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  const reader = response.body.getReader();
  await readText(reader);
  await reader.cancel();
  await Promise.all(waitUntilPromises);

  const session = /** @type {any} */ (globalThis).__tailSessions[0];
  const dataRedis = /** @type {any} */ (globalThis).__tailState.dataRedis;
  assert.equal(dataRedis.evalCalls.length, 1);
  assert.deepEqual(dataRedis.evalCalls[0].slice(1), [
    ["logs:tail:active"],
    ["30", "10000", "demo:bar", "demo:baz", "demo:foo"],
  ]);
  assert.equal(dataRedis.activeFields.get("demo:bar"), "1");
  assert.equal(dataRedis.activeFields.get("demo:baz"), "1");
  assert.equal(dataRedis.activeFields.has("demo:foo"), false);
  assert.equal(dataRedis.activeCount, 10_000);
  assert.equal(dataRedis.sessionCalls, 0);
  assert.deepEqual(session.evalCalls, []);
});

test("logs tail continues blocking reads after an independent activation failure", async () => {
  resetTailState();
  const state = /** @type {any} */ (globalThis).__tailState;
  state.dataRedis.evalError = new Error("activation connection lost");
  const { handle } = await loadLogsTailHandler();
  /** @type {Promise<unknown>[]} */
  const waitUntilPromises = [];
  const response = await handle({
    request: new Request("http://control.test/ns/demo/logs/tail?worker=foo"),
    env: {
      REDIS_ADDR: "redis-control:6379",
      DATA_REDIS_ADDR: "redis-data:6379",
      DATA_REDIS_DB: "1",
    },
    ctx: { waitUntil(/** @type {Promise<unknown>} */ promise) { waitUntilPromises.push(promise); } },
    ns: "demo",
    requestId: "rid-tail",
  });

  const reader = response.body.getReader();
  await readText(reader);
  await reader.cancel();
  await Promise.all(waitUntilPromises);

  const session = /** @type {any} */ (globalThis).__tailSessions[0];
  assert.ok(session.xReadCalls.length > 0);
  assert.deepEqual(session.evalCalls, []);
  assert.ok(state.logs.some((/** @type {{ event: string }} */ entry) =>
    entry.event === "tail_heartbeat_activate_failed"));
});

test("tailMaxSessionMs defaults to fifteen minutes and ignores invalid overrides", async () => {
  resetTailState();
  const { LOG_TAIL_MAX_SESSION_MS_DEFAULT, tailMaxSessionMs } = await loadLogsTailHandler();

  assert.equal(LOG_TAIL_MAX_SESSION_MS_DEFAULT, 15 * 60 * 1000);
  assert.equal(tailMaxSessionMs({}), LOG_TAIL_MAX_SESSION_MS_DEFAULT);
  assert.equal(tailMaxSessionMs({ LOG_TAIL_MAX_SESSION_MS: "" }), LOG_TAIL_MAX_SESSION_MS_DEFAULT);
  assert.equal(tailMaxSessionMs({ LOG_TAIL_MAX_SESSION_MS: "0" }), LOG_TAIL_MAX_SESSION_MS_DEFAULT);
  assert.equal(tailMaxSessionMs({ LOG_TAIL_MAX_SESSION_MS: "120000" }), 120000);
});
