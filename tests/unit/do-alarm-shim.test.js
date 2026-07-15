import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DO_ALARM_SHIM_SOURCE,
} from "../../do-runtime/alarm-shim-source.js";
import { makeDoAlarmBinding, makeDoAlarmStorage } from "../helpers/do-alarm-shim-fixture.js";
import { applyModuleReplacements, moduleDataUrl } from "../helpers/load-shared-module.js";
import { withMockedGlobal, withMockedProperty } from "../helpers/mock-global.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const shimSource = applyModuleReplacements(DO_ALARM_SHIM_SOURCE, [
  ["function wrapStorage", "export function wrapStorage"],
  ["function formatWrappedError", "export function formatWrappedError"],
]);
const { formatWrappedError, wrapDurableObjectClass, wrapStorage } = await import(moduleDataUrl(shimSource));

test("DO alarm shim: error formatting cannot replace the original failure", () => {
  const hostile = new Error("original");
  Object.defineProperties(hostile, {
    name: { get() { throw new Error("name getter failed"); } },
    message: { get() { throw new Error("message getter failed"); } },
    code: { get() { throw new Error("code getter failed"); } },
  });
  assert.deepEqual(formatWrappedError(hostile), {
    error_name: "Error",
    error_message: "Unknown error",
  });

  const proxy = new Proxy({}, {
    get() { throw new Error("proxy getter failed"); },
    getPrototypeOf() { throw new Error("prototype trap failed"); },
  });
  assert.deepEqual(formatWrappedError(proxy), {
    error_name: "Error",
    error_message: "Unknown error",
  });
});

test("DO alarm shim: internal RPC dispatch invokes tenant methods without adding arguments", async () => {
  const { storage } = makeDoAlarmStorage();
  class RpcRoom {
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
    }
    async inspect(/** @type {unknown} */ value) {
      await Promise.resolve();
      return { value };
    }
  }
  const Wrapped = wrapDurableObjectClass(RpcRoom, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding([]) }
  );
  /** @param {string} method */
  const request = (method) => new Request("https://do.internal/__wdl_rpc", {
    method: "POST",
    headers: { "x-wdl-do-internal-rpc": "1", "x-request-id": "rid-rpc" },
    body: JSON.stringify({ method, args: ["value"] }),
  });
  await assertJsonResponse(await instance.fetch(request("inspect")), 200, {
    ok: true,
    result: {
      value: "value",
    },
  });
  await assertJsonResponse(await instance.fetch(request("missing")), 404, {
    error: "do_rpc_method_not_found",
    message: "Durable Object RPC method missing was not found",
  });
});

test("DO alarm shim: repair logging cannot replace the stored alarm result", async () => {
  /** @param {(read: () => Promise<void>) => Promise<void>} callback */
  async function readWithBrokenLogDependency(callback) {
    const { storage } = makeDoAlarmStorage({
      scheduled_time: 1234,
      retry_count: 0,
      in_flight: 0,
      token: "sqlite-token",
    });
    const alarmBinding = makeDoAlarmBinding([]);
    alarmBinding.setAlarmIndex = async () => {
      throw new Error("backend unavailable");
    };
    const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");
    await callback(async () => assert.equal(await wrapped.getAlarm(), 1234));
  }

  await readWithBrokenLogDependency((read) =>
    withMockedGlobal("Date", new Proxy(Date, {
      construct() {
        throw new Error("tenant Date");
      },
    }), read));
  await readWithBrokenLogDependency((read) =>
    withMockedProperty(JSON, "stringify", () => {
      throw new Error("tenant stringify");
    }, read));
  await readWithBrokenLogDependency((read) =>
    withMockedProperty(console, "log", () => {
      throw new Error("tenant console");
    }, read));
});

test("DO alarm shim: internal alarm dispatch ignores tenant-patched request intrinsics", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token: "captured-intrinsics-token",
  });
  class AlarmCounter {
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.alarms = 0;
    }
    async alarm() {
      this.alarms += 1;
    }
    async fetch() {
      return new Response("tenant fetch");
    }
  }
  const Wrapped = wrapDurableObjectClass(AlarmCounter, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) }
  );
  const request = new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "captured-intrinsics-token", retryCount: 0 }),
  });

  await withMockedProperty(Headers.prototype, "get", () => null, async () => {
    await withMockedProperty(Request.prototype, "json", async () => {
      throw new Error("tenant Request.json");
    }, async () => {
      await withMockedProperty(Response, "json", () => {
        throw new Error("tenant Response.json");
      }, async () => {
        const response = await instance.fetch(request);
        await assertJsonResponse(response, 200, { ok: true });
      });
    });
  });

  assert.equal(instance.alarms, 1);
  assert.equal(state.row, null);
});

test("DO alarm shim: class-field fetch cannot intercept internal alarm dispatch", async () => {
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token: "class-field-fetch-token",
  });
  class ClassFieldFetch {
    alarms = 0;
    fetchCalls = 0;
    ["fetch"] = async () => {
      this.fetchCalls += 1;
      return new Response("tenant fetch");
    };
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
    }
    async alarm() {
      this.alarms += 1;
    }
  }
  const Wrapped = wrapDurableObjectClass(ClassFieldFetch, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding([]) }
  );
  const alarmResponse = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "class-field-fetch-token", retryCount: 0 }),
  }));
  await assertJsonResponse(alarmResponse, 200, { ok: true });
  assert.equal(instance.alarms, 1);
  assert.equal(instance.fetchCalls, 0);
  assert.equal(state.row, null);

  assert.equal(await (await instance.fetch(new Request("https://do.internal/tenant"))).text(), "tenant fetch");
  assert.equal(instance.fetchCalls, 1);
});

test("DO alarm shim: class-field alarm executes before its row is cleared", async () => {
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 2,
    in_flight: 0,
    token: "class-field-alarm-token",
  });
  class ClassFieldAlarm {
    alarms = 0;
    /** @type {number | null} */
    retryCount = null;
    /** @param {{ retryCount: number }} info */
    alarm = async (info) => {
      this.alarms += 1;
      this.retryCount = info.retryCount;
    };
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
    }
  }
  const Wrapped = wrapDurableObjectClass(ClassFieldAlarm, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding([]) }
  );

  const response = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "class-field-alarm-token", retryCount: 2 }),
  }));

  await assertJsonResponse(response, 200, { ok: true });
  assert.equal(instance.alarms, 1);
  assert.equal(instance.retryCount, 2);
  assert.equal(state.row, null);
});

test("DO alarm shim: own accessors retain their instance receiver", async () => {
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token: "accessor-alarm-token",
  });
  class AccessorHandlers {
    #alarms = 0;
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      Object.defineProperties(this, {
        fetch: {
          configurable: true,
          get: () => async () => new Response(String(this.#alarms)),
        },
        alarm: {
          configurable: true,
          get: () => async () => {
            this.#alarms += 1;
          },
        },
      });
    }
  }
  const Wrapped = wrapDurableObjectClass(AccessorHandlers, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding([]) }
  );

  const alarmResponse = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "accessor-alarm-token", retryCount: 0 }),
  }));
  await assertJsonResponse(alarmResponse, 200, { ok: true });
  assert.equal(state.row, null);
  assert.equal(await (await instance.fetch(new Request("https://do.internal/status"))).text(), "1");
});

test("DO alarm shim: alarm getter remains lazy until internal alarm dispatch", async () => {
  class LazyAlarmGetter {
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
    }
    async fetch() {
      return new Response("tenant fetch");
    }
    get alarm() {
      throw new Error("alarm getter must remain lazy");
    }
  }
  const { storage } = makeDoAlarmStorage();
  const Wrapped = wrapDurableObjectClass(LazyAlarmGetter, "Room");
  const instance = new Wrapped(
    { storage, id: "alice" },
    { __WDL_DO_ALARMS__: makeDoAlarmBinding([]) }
  );

  const response = await instance.fetch(new Request("https://do.internal/tenant"));
  assert.equal(await response.text(), "tenant fetch");
});

test("DO alarm shim: storage facade ignores tenant-patched proxy intrinsics", async () => {
  const { storage } = makeDoAlarmStorage({
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "captured-proxy-token",
  });
  const alarmBinding = makeDoAlarmBinding([]);
  const hostileProxy = new Proxy(Proxy, {
    construct() {
      throw new Error("tenant Proxy");
    },
  });

  await withMockedGlobal("Proxy", hostileProxy, async () => {
    await withMockedProperty(Reflect, "get", () => {
      throw new Error("tenant Reflect.get");
    }, async () => {
      await withMockedProperty(Reflect, "apply", () => {
        throw new Error("tenant Reflect.apply");
      }, async () => {
        class AlarmReader {
          /** @param {{ storage: unknown }} ctx */
          constructor(ctx) {
            this.ctx = ctx;
          }
        }
        const Wrapped = wrapDurableObjectClass(AlarmReader, "Room");
        const instance = new Wrapped(
          { storage, id: "alice" },
          { __WDL_DO_ALARMS__: alarmBinding }
        );
        assert.equal(await instance.ctx.storage.getAlarm(), 1234);
      });
    });
  });
});

test("DO alarm shim: transaction setAlarm then deleteAlarm flushes only the final delete", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    await txn.deleteAlarm();
  });

  assert.deepEqual(calls.map(([kind]) => kind), ["delete"]);
  assert.equal(typeof /** @type {any} */ (calls[0][1]).token, "string");
  assert.notEqual(/** @type {any} */ (calls[0][1]).token, "");
  assert.equal(state.row, null);
});

test("DO alarm shim: transaction setAlarm then deleteAlarm deletes the baseline backend alarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: 500,
    retry_count: 0,
    in_flight: 0,
    token: "baseline-token",
  });
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    await txn.deleteAlarm();
  });

  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: "baseline-token" }],
  ]);
  assert.equal(state.row, null);
});

test("DO alarm shim: failed non-transactional setAlarm rolls back the SQLite alarm row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (input) => {
    calls.push(["set", input]);
    throw new Error("backend unavailable");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(wrapped.setAlarm(1000), /backend unavailable/);

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(state.row, null);
});

test("DO alarm shim: transactionSync rejects setAlarm before creating backend side effects", () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  assert.throws(() => {
    wrapped.transactionSync(() => {
      wrapped.setAlarm(1000);
    });
  }, /setAlarm\(\) cannot be used inside transactionSync\(\); use transaction\(\)/);

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: transactionSync rejects deleteAlarm and rolls back the SQLite alarm row", () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "transaction-sync-delete-alarm-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  assert.throws(() => {
    wrapped.transactionSync(() => {
      wrapped.deleteAlarm();
    });
  }, /deleteAlarm\(\) cannot be used inside transactionSync\(\); use transaction\(\)/);

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: async transaction rollback does not flush alarm backend side effects", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await txn.setAlarm(1000);
      throw new Error("rollback");
    }),
    /rollback/
  );

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: async transaction deleteAll rollback does not cancel backend alarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "transaction-delete-all-token",
  };
  const { storage, state, kv } = makeDoAlarmStorage(initial);
  kv.set("kv-key", "kv-value");
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await txn.deleteAll();
      throw new Error("rollback");
    }),
    /rollback/
  );

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: transactionSync rejects async deleteAll", () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "transaction-sync-delete-all-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  assert.throws(
    () => wrapped.transactionSync(() => wrapped.deleteAll()),
    /deleteAll\(\) cannot be used inside transactionSync\(\); use transaction\(\)/
  );

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: transaction with two setAlarm calls flushes only the final backend schedule", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    await txn.setAlarm(2000);
  });

  assert.equal(calls.length, 1);
  const [kind, rawInput] = calls[0];
  const input = /** @type {any} */ (rawInput);
  assert.equal(kind, "set");
  assert.deepEqual({
    className: input.className,
    objectName: input.objectName,
    scheduledTime: input.scheduledTime,
    retryCount: input.retryCount,
    token: input.token,
  }, {
    className: "Room",
    objectName: "alice",
    scheduledTime: 2000,
    retryCount: 0,
    token: state.row?.token,
  });
});

test("DO alarm shim: deleteAlarm backend failure restores the SQLite alarm row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "delete-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    calls.push(["delete", input]);
    throw new Error("backend unavailable");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(wrapped.deleteAlarm(), /backend unavailable/);

  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: "delete-token" }],
  ]);
  assert.deepEqual(state.row, { ...initial, last_error: null });
});

test("DO alarm shim: getAlarm repairs backend schedule from SQLite row token", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage } = makeDoAlarmStorage({
    scheduled_time: 1234,
    retry_count: 2,
    in_flight: 0,
    token: "sqlite-token",
  });
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  assert.equal(await wrapped.getAlarm(), 1234);

  assert.deepEqual(calls, [
    ["set", {
      className: "Room",
      objectName: "alice",
      scheduledTime: 1234,
      retryCount: 2,
      token: "sqlite-token",
    }],
  ]);
});

test("DO alarm shim: stale backend alarm token is ignored without clearing SQLite row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token: "current-token",
  });
  class AlarmCounter {
    /** @param {{ storage: unknown, id: string }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.alarms = 0;
    }
    async alarm() {
      this.alarms += 1;
    }
  }
  const Wrapped = wrapDurableObjectClass(AlarmCounter, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) });

  const response = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "stale-token", retryCount: 0 }),
  }));

  await assertJsonResponse(response, 200, { ok: true, ignored: true });
  assert.equal(instance.alarms, 0);
  assert.equal(state.row?.token, "current-token");
  assert.equal(state.row?.in_flight, 0);
});

test("DO alarm shim: early backend alarm is ignored without clearing SQLite row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() + 60_000,
    retry_count: 0,
    in_flight: 0,
    token: "current-token",
  });
  class AlarmCounter {
    /** @param {{ storage: unknown, id: string }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.alarms = 0;
    }
    async alarm() {
      this.alarms += 1;
    }
  }
  const Wrapped = wrapDurableObjectClass(AlarmCounter, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) });

  const response = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "current-token", retryCount: 0 }),
  }));

  await assertJsonResponse(response, 200, { ok: true, ignored: true });
  assert.equal(instance.alarms, 0);
  assert.equal(state.row?.token, "current-token");
  assert.equal(state.row?.in_flight, 0);
});

test("DO alarm shim: matching backend alarm token executes alarm and clears SQLite row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 1,
    in_flight: 0,
    token: "current-token",
  });
  class AlarmCounter {
    /** @param {{ storage: unknown, id: string }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.alarms = 0;
      this.retryCount = null;
      this.isRetry = null;
    }
    /** @param {{ retryCount: number, isRetry: boolean }} info */
    async alarm(info) {
      this.alarms += 1;
      this.retryCount = info.retryCount;
      this.isRetry = info.isRetry;
    }
  }
  const Wrapped = wrapDurableObjectClass(AlarmCounter, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) });

  const response = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "current-token", retryCount: 1 }),
  }));

  await assertJsonResponse(response, 200, { ok: true });
  assert.equal(instance.alarms, 1);
  assert.equal(instance.retryCount, 1);
  assert.equal(instance.isRetry, true);
  assert.equal(state.row, null);
});

test("DO alarm shim: failed alarm keeps row in flight and hides getAlarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token: "fail-token",
  });
  class FailingAlarm {
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
    }
    async alarm() {
      throw new Error("alarm failed");
    }
  }
  const Wrapped = wrapDurableObjectClass(FailingAlarm, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) });

  await assert.rejects(
    instance.fetch(new Request("https://do.internal/__wdl_alarm", {
      method: "POST",
      headers: { "x-wdl-do-internal-alarm": "1" },
      body: JSON.stringify({ token: "fail-token", retryCount: 0 }),
    })),
    /alarm failed/
  );

  assert.equal(state.row?.in_flight, 1);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
  assert.equal(await wrapped.getAlarm(), null);
});

test("DO alarm shim: retry dispatch reclaims an already in-flight row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() + 60_000,
    retry_count: 0,
    in_flight: 1,
    token: "retry-token",
  });
  class AlarmCounter {
    /** @param {{ storage: unknown }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.retryCount = null;
    }
    /** @param {{ retryCount: number }} info */
    async alarm(info) {
      this.retryCount = info.retryCount;
    }
  }
  const Wrapped = wrapDurableObjectClass(AlarmCounter, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) });

  const response = await instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token: "retry-token", retryCount: 2 }),
  }));

  await assertJsonResponse(response, 200, { ok: true });
  assert.equal(instance.retryCount, 2);
  assert.equal(state.row, null);
});

test("DO alarm shim: deleteAll clears alarm row and cancels backend schedule by default", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state, kv } = makeDoAlarmStorage({
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "delete-all-token",
  });
  kv.set("kv-key", "kv-value");
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.deleteAll();

  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: "delete-all-token" }],
  ]);
  assert.equal(state.row, null);
  assert.equal(kv.size, 0);
});

test("DO alarm shim: deleteAll skips _cf_ reserved SQL objects case-insensitively", async () => {
  /** @type {string[]} */
  const dropped = [];
  const storage = {
    sql: {
      /** @param {string} statement */
      exec(statement) {
        if (statement.startsWith("CREATE TABLE")) return [];
        if (statement.startsWith("SELECT scheduled_time")) return [];
        if (statement.startsWith("SELECT type, name FROM sqlite_master")) {
          return [
            { type: "table", name: "_CF_legacy" },
            { type: "index", name: "_Cf_legacy_idx" },
            { type: "table", name: "tenant_table" },
          ];
        }
        if (statement.startsWith("PRAGMA foreign_keys")) return [];
        if (statement.startsWith("DROP ")) {
          dropped.push(statement);
          return [];
        }
        throw new Error(`unexpected SQL: ${statement}`);
      },
    },
    async list() {
      return new Map();
    },
    async delete() {},
  };
  const wrapped = wrapStorage(storage, makeDoAlarmBinding([]), "Room", "alice");

  await wrapped.deleteAll();

  assert.deepEqual(dropped, ['DROP TABLE IF EXISTS "tenant_table"']);
});

test("DO alarm shim: deleteAll deleteAlarm false preserves alarm row without backend cancel", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "preserve-token",
  };
  const { storage, state, kv } = makeDoAlarmStorage(initial);
  kv.set("kv-key", "kv-value");
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.deleteAll({ deleteAlarm: false });

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, { ...initial, last_error: null });
  assert.equal(kv.size, 0);
});

test("DO alarm shim: deleteAll ignores tenant-patched array iteration", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  /** @type {string[]} */
  const dropped = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "preserve-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const originalExec = storage.sql.exec;
  /** @param {string} statement @param {...unknown} params */
  storage.sql.exec = function exec(statement, ...params) {
    if (statement.startsWith("SELECT type, name FROM sqlite_master")) {
      return [
        { type: "table", name: "tenant_table" },
        { type: "table", name: "_wdl_do_alarms" },
      ];
    }
    if (statement === 'DROP TABLE IF EXISTS "tenant_table"') {
      dropped.push(statement);
      return [];
    }
    return Reflect.apply(originalExec, this, [statement, ...params]);
  };
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
  const originalIterator = Array.prototype[Symbol.iterator];

  await withMockedProperty(Map.prototype, "keys", () => {
    throw new Error("tenant Map.keys");
  }, () => withMockedProperty(
    Array.prototype,
    Symbol.iterator,
    /** @this {any[]} */ function hostileIterator() {
      const first = this[0];
      if (
        (this.length === 1 && first?.deleteAlarm === false) ||
        (first && typeof first === "object" && "type" in first)
      ) {
        return Reflect.apply(originalIterator, [], []);
      }
      return Reflect.apply(originalIterator, this, []);
    },
    async () => {
      await wrapped.deleteAll({ deleteAlarm: false });
    }
  ));

  assert.deepEqual(dropped, ['DROP TABLE IF EXISTS "tenant_table"']);
  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, { ...initial, last_error: null });
});
