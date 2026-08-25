import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DO_ALARM_SHIM_SOURCE,
} from "../../do-runtime/alarm-shim-source.js";
import { makeDoAlarmBinding, makeDoAlarmStorage } from "../helpers/do-alarm-shim-fixture.js";
import { applyModuleReplacements, moduleDataUrl } from "../helpers/load-shared-module.js";
import {
  withMockedGlobal,
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
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

test("DO alarm shim: transaction setAlarm then deleteAlarm skips delete without a backend baseline", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    await txn.deleteAlarm();
  });

  assert.deepEqual(calls, []);
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

test("DO alarm shim: failed transaction delete restores and retries the backend baseline", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const baseline = {
    scheduled_time: 500,
    retry_count: 0,
    in_flight: 0,
    token: "transaction-delete-baseline-token",
  };
  const { storage, state } = makeDoAlarmStorage(baseline);
  let backendToken = /** @type {string | null} */ (baseline.token);
  let attempts = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    calls.push(["delete", input]);
    attempts += 1;
    if (attempts === 1) throw new Error("delete failed before mutation");
    if (backendToken === /** @type {{ token: string }} */ (input).token) backendToken = null;
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await wrapped.setAlarm(1000);
      await txn.deleteAlarm();
    }),
    /delete failed before mutation/
  );

  assert.equal(state.row?.scheduled_time, baseline.scheduled_time);
  assert.equal(state.row?.token, baseline.token);
  assert.equal(backendToken, baseline.token);

  await wrapped.deleteAlarm();

  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: baseline.token }],
    ["delete", { className: "Room", objectName: "alice", token: baseline.token }],
  ]);
  assert.equal(backendToken, null);
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

test("DO alarm shim: a later successful setAlarm installs a replacement after backend rejection", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: 500,
    retry_count: 0,
    in_flight: 0,
    token: "baseline-token",
  });
  const alarmBinding = makeDoAlarmBinding(calls);
  let attempts = 0;
  alarmBinding.setAlarmIndex = async (input) => {
    calls.push(["set", input]);
    attempts += 1;
    if (attempts === 1) throw new Error("backend unavailable");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(wrapped.setAlarm(1000), /backend unavailable/);
  const failedToken = /** @type {{ token: string }} */ (calls[0][1]).token;
  assert.notEqual(state.row?.token, failedToken);

  await wrapped.setAlarm(2000);

  assert.deepEqual(calls.map(([kind]) => kind), ["set", "set"]);
  assert.equal(state.row?.scheduled_time, 2000);
  assert.equal(state.row?.token, /** @type {{ token: string }} */ (calls[1][1]).token);
});

test("DO alarm shim: overlapping setAlarm backend mutations preserve call order", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const firstStarted = Promise.withResolvers();
  const releaseFirst = Promise.withResolvers();
  /** @type {string | null} */
  let backendToken = null;
  let attempt = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (rawInput) => {
    const input = /** @type {{ scheduledTime: number, token: string }} */ (rawInput);
    const index = attempt++;
    calls.push(["set", rawInput]);
    if (index === 0) {
      firstStarted.resolve(undefined);
      await releaseFirst.promise;
    }
    backendToken = input.token;
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  const first = wrapped.setAlarm(1000);
  await firstStarted.promise;
  const second = wrapped.setAlarm(2000);
  const secondToken = state.row?.token;
  assert.equal(calls.length, 1);

  releaseFirst.resolve(undefined);
  await Promise.all([first, second]);

  assert.deepEqual(calls.map(([, input]) => (
    /** @type {{ scheduledTime: number }} */ (input).scheduledTime
  )), [1000, 2000]);
  assert.equal(backendToken, secondToken);
  assert.equal(state.row?.token, secondToken);
});

test("DO alarm shim: transaction backend effect waits for earlier queued mutation", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const firstStarted = Promise.withResolvers();
  const releaseFirst = Promise.withResolvers();
  /** @type {string | null} */
  let backendToken = null;
  let attempt = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (rawInput) => {
    const input = /** @type {{ scheduledTime: number, token: string }} */ (rawInput);
    const index = attempt++;
    calls.push(["set", rawInput]);
    if (index === 0) {
      firstStarted.resolve(undefined);
      await releaseFirst.promise;
    }
    backendToken = input.token;
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  const first = wrapped.setAlarm(1000);
  await firstStarted.promise;
  const transaction = wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(2000);
  });
  const transactionToken = state.row?.token;
  await Promise.resolve();
  const callsBeforeRelease = calls.map(([, input]) => (
    /** @type {{ scheduledTime: number }} */ (input).scheduledTime
  ));

  releaseFirst.resolve(undefined);
  await Promise.all([first, transaction]);

  assert.deepEqual(callsBeforeRelease, [1000]);
  assert.deepEqual(calls.map(([, input]) => (
    /** @type {{ scheduledTime: number }} */ (input).scheduledTime
  )), [1000, 2000]);
  assert.equal(backendToken, transactionToken);
  assert.equal(state.row?.token, transactionToken);
});

test("DO alarm shim: transaction fence remains until native transaction promise settles", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const nativeTransaction = storage.transaction;
  const localCommitFinished = Promise.withResolvers();
  const releaseTransaction = Promise.withResolvers();
  storage.transaction = async (/** @type {(txn?: unknown) => unknown} */ callback) => {
    const result = await nativeTransaction(callback);
    localCommitFinished.resolve(undefined);
    await releaseTransaction.promise;
    return result;
  };
  /** @type {string | null} */
  let backendToken = null;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (rawInput) => {
    const input = /** @type {{ token: string }} */ (rawInput);
    calls.push(["set", rawInput]);
    backendToken = input.token;
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  const transaction = wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
  });
  await localCommitFinished.promise;
  try {
    assert.throws(() => wrapped.setAlarm(2000), /after transaction completion/);
    assert.equal(calls.length, 0);
  } finally {
    releaseTransaction.resolve(undefined);
  }
  await transaction;
  const replacement = wrapped.setAlarm(2000);
  const replacementToken = state.row?.token;
  await replacement;

  assert.deepEqual(calls.map(([, input]) => (
    /** @type {{ scheduledTime: number }} */ (input).scheduledTime
  )), [1000, 2000]);
  assert.equal(backendToken, replacementToken);
  assert.equal(state.row?.token, replacementToken);
});

test("DO alarm shim: transaction state ignores Array prototype metadata", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await withMockedPropertyDescriptor(
    /** @type {any} */ (Array.prototype),
    "alarmReservation",
    { get() { throw new Error("polluted alarmReservation"); } },
    () => withMockedPropertyDescriptor(
      /** @type {any} */ (Array.prototype),
      "nested",
      { get() { throw new Error("polluted nested"); } },
      async () => {
        await wrapped.transaction(async (/** @type {any} */ txn) => {
          await txn.setAlarm(1000);
        });
      }
    )
  );

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(state.row?.scheduled_time, 1000);
});

test("DO alarm shim: transaction reservation does not assimilate a function thenable", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await withMockedPropertyDescriptor(
    /** @type {any} */ (Function.prototype),
    "then",
    { get() { throw new Error("polluted Function.prototype.then"); } },
    async () => {
      await wrapped.transaction(async (/** @type {any} */ txn) => {
        await txn.setAlarm(1000);
      });
    }
  );

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(state.row?.scheduled_time, 1000);
});

test("DO alarm shim: getAlarm repair cannot overwrite a later setAlarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: 1000,
    retry_count: 0,
    in_flight: 0,
    token: "repair-baseline-token",
  });
  const repairStarted = Promise.withResolvers();
  const releaseRepair = Promise.withResolvers();
  /** @type {string | null} */
  let backendToken = null;
  let attempt = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (rawInput) => {
    const input = /** @type {{ token: string }} */ (rawInput);
    const index = attempt++;
    calls.push(["set", rawInput]);
    if (index === 0) {
      repairStarted.resolve(undefined);
      await releaseRepair.promise;
    }
    backendToken = input.token;
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  const repair = wrapped.getAlarm();
  await repairStarted.promise;
  const replacement = wrapped.setAlarm(2000);
  const replacementToken = state.row?.token;
  assert.equal(calls.length, 1);

  releaseRepair.resolve(undefined);
  await Promise.all([repair, replacement]);

  assert.equal(backendToken, replacementToken);
  assert.equal(state.row?.token, replacementToken);
  assert.deepEqual(calls.map(([kind]) => kind), ["set", "set"]);
});

test("DO alarm shim: input validation failure preserves the previous alarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 500,
    retry_count: 0,
    in_flight: 0,
    token: "validation-baseline-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await assert.rejects(wrapped.setAlarm(0), /alarm time <= 0/);

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: scheduled alarms use distinct tokens from the pre-captured RNG", async () => {
  const firstModuleTokens = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
  ];
  const secondModuleTokens = ["33333333-3333-4333-8333-333333333333"];
  /** @param {string[]} tokens @param {string} marker */
  const importShimWithCapturedTokens = async (tokens, marker) => {
    let index = 0;
    return await withMockedProperty(
      crypto,
      "randomUUID",
      () => /** @type {ReturnType<typeof crypto.randomUUID>} */ (tokens[index++]),
      async () => await import(moduleDataUrl(`${shimSource}\n// ${marker}`)),
    );
  };
  const firstModule = await importShimWithCapturedTokens(firstModuleTokens, "alarm-token-module-1");
  const secondModule = await importShimWithCapturedTokens(secondModuleTokens, "alarm-token-module-2");
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const alarmBinding = makeDoAlarmBinding(calls);
  const firstWrapped = firstModule.wrapStorage(storage, alarmBinding, "Room", "alice");
  const secondWrapped = secondModule.wrapStorage(storage, alarmBinding, "Room", "alice");
  const patchedToken = "00000000-0000-4000-8000-000000000000";

  await withMockedProperty(crypto, "randomUUID", () => patchedToken, async () => {
    await firstWrapped.setAlarm(1000);
    await firstWrapped.setAlarm(2000);
    await secondWrapped.setAlarm(3000);
  });

  const tokens = calls.map(([, input]) => /** @type {{ token: string }} */ (input).token);
  assert.deepEqual(tokens, [...firstModuleTokens, ...secondModuleTokens]);
  assert.equal(state.row?.token, secondModuleTokens[0]);
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

test("DO alarm shim: transactionSync marker is shared across owning and callback proxies", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "shared-sync-transaction-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    assert.throws(
      () => wrapped.transactionSync(() => txn.setAlarm(1000)),
      /setAlarm\(\) cannot be used inside transactionSync\(\)/
    );
    assert.throws(
      () => wrapped.transactionSync(() => txn.deleteAlarm()),
      /deleteAlarm\(\) cannot be used inside transactionSync\(\)/
    );
  });

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: async transaction rollback does not flush alarm backend side effects", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state, kv } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await txn.put("rolled-back-key", "rolled-back-value");
      await txn.setAlarm(1000);
      throw new Error("rollback");
    }),
    /rollback/
  );

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
  assert.equal(kv.has("rolled-back-key"), false);
});

test("DO alarm shim: transaction getAlarm is local-only and explicit rollback discards setAlarm", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    assert.equal(await txn.getAlarm(), 1000);
    txn.rollback();
    assert.doesNotThrow(() => txn.rollback());
    assert.throws(() => txn.getAlarm(), /after transaction rollback/);
  });

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: class storage aliases share one alarm transaction context", async (t) => {
  class StorageAliases {
    /** @param {{ storage: any }} ctx */
    constructor(ctx) {
      this.ctx = ctx;
      this.cachedStorage = ctx.storage;
    }
  }

  await t.test("commit", async () => {
    /** @type {unknown[][]} */
    const calls = [];
    const { storage, state } = makeDoAlarmStorage();
    const Wrapped = wrapDurableObjectClass(StorageAliases, "Room");
    const instance = new Wrapped(
      { storage, id: "alice" },
      { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) }
    );
    assert.equal(instance.cachedStorage, instance.ctx.storage);

    await instance.cachedStorage.transaction(async (/** @type {any} */ txn) => {
      await txn.setAlarm(1000);
      await instance.ctx.storage.setAlarm(2000);
    });

    assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
    assert.equal(/** @type {{ scheduledTime: number }} */ (calls[0][1]).scheduledTime, 2000);
    assert.equal(state.row?.scheduled_time, 2000);
    assert.equal(state.row?.token, /** @type {{ token: string }} */ (calls[0][1]).token);
  });

  for (const outcome of ["rollback", "throw"]) {
    await t.test(outcome, async () => {
      /** @type {unknown[][]} */
      const calls = [];
      const initial = outcome === "rollback" ? {
        scheduled_time: 500,
        retry_count: 0,
        in_flight: 0,
        token: "class-alias-baseline-token",
      } : null;
      const { storage, state } = makeDoAlarmStorage(initial);
      const Wrapped = wrapDurableObjectClass(StorageAliases, "Room");
      const instance = new Wrapped(
        { storage, id: "alice" },
        { __WDL_DO_ALARMS__: makeDoAlarmBinding(calls) }
      );
      const operation = instance.cachedStorage.transaction(async (/** @type {any} */ txn) => {
        if (outcome === "rollback") {
          await instance.ctx.storage.deleteAlarm();
          txn.rollback();
          return;
        }
        await instance.ctx.storage.setAlarm(1000);
        throw new Error("class alias transaction failed");
      });

      if (outcome === "throw") {
        await assert.rejects(operation, /class alias transaction failed/);
      } else {
        await operation;
      }
      assert.deepEqual(calls, []);
      assert.deepEqual(state.row, initial);
    });
  }
});

test("DO alarm shim: nested transaction rejects alarm APIs across every storage alias", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ outerTxn) => {
    await outerTxn.setAlarm(1000);
    await wrapped.transaction(async (/** @type {any} */ innerTxn) => {
      for (const target of [innerTxn, outerTxn, wrapped]) {
        assert.throws(() => target.setAlarm(2000), /inside nested transaction/);
      }
    });
    outerTxn.rollback();
  });

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: an unawaited nested transaction cannot restore a closed outer fence", async (t) => {
  for (const outcome of ["resolved", "rejected"]) {
    await t.test(outcome, async () => {
      /** @type {unknown[][]} */
      const calls = [];
      const { storage, state, kv } = makeDoAlarmStorage();
      const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
      const childStarted = Promise.withResolvers();
      const releaseChild = Promise.withResolvers();
      /** @type {Promise<string> | null} */
      let childSettled = null;

      await wrapped.transaction(async () => {
        const child = wrapped.transaction(async () => {
          childStarted.resolve(undefined);
          await releaseChild.promise;
          if (outcome === "rejected") throw new Error("nested transaction failed");
        });
        childSettled = child.then(
          () => "resolved",
          /** @param {unknown} err */
          (err) => err instanceof Error ? err.message : String(err)
        );
        await childStarted.promise;
      });

      releaseChild.resolve(undefined);
      assert.equal(
        await childSettled,
        outcome === "resolved" ? "resolved" : "nested transaction failed"
      );
      await wrapped.setAlarm(1000);
      await wrapped.transaction(async (/** @type {any} */ txn) => {
        await txn.put("after-nested", "ok");
      });

      assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
      assert.equal(state.row?.scheduled_time, 1000);
      assert.equal(kv.get("after-nested"), "ok");
    });
  }
});

test("DO alarm shim: failed native rollback keeps queued alarm side effects", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
  const childStarted = Promise.withResolvers();
  const releaseChild = Promise.withResolvers();

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    const child = wrapped.transaction(async () => {
      childStarted.resolve(undefined);
      await releaseChild.promise;
    });
    await childStarted.promise;
    assert.throws(() => txn.rollback(), /nested transaction is active/);
    releaseChild.resolve(undefined);
    await child;
  });

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(state.row?.scheduled_time, 1000);
});

test("DO alarm shim: owning storage shares transaction-local alarm context", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async () => {
    await wrapped.setAlarm(1000);
    assert.equal(await wrapped.getAlarm(), 1000);
    assert.deepEqual(calls, []);
  });

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(state.row?.scheduled_time, 1000);
});

test("DO alarm shim: same-event owning-storage alarm follows native transaction rollback", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
  const transactionStarted = Promise.withResolvers();
  const releaseTransaction = Promise.withResolvers();

  const transaction = wrapped.transaction(async (/** @type {any} */ txn) => {
    await txn.setAlarm(1000);
    transactionStarted.resolve(undefined);
    await releaseTransaction.promise;
    txn.rollback();
  });
  await transactionStarted.promise;

  await wrapped.setAlarm(2000);
  releaseTransaction.resolve(undefined);
  await transaction;

  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: callback reaction cannot cross native transaction settlement", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, state } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
  const callbackEntered = Promise.withResolvers();
  const callbackResult = Promise.withResolvers();

  const transaction = wrapped.transaction(() => {
    callbackEntered.resolve(undefined);
    return callbackResult.promise;
  });
  await callbackEntered.promise;
  const outsideReaction = callbackResult.promise.catch(() => {
    try {
      wrapped.setAlarm(2000);
      return "fulfilled";
    } catch (err) {
      return err instanceof Error ? err.message : String(err);
    }
  });

  callbackResult.reject(new Error("transaction rollback"));
  await assert.rejects(transaction, /transaction rollback/);

  assert.match(await outsideReaction, /after transaction completion/);
  assert.deepEqual(calls, []);
  assert.equal(state.row, null);
});

test("DO alarm shim: explicit rollback discards outer-storage delete side effects", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "rollback-deleteAlarm-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    await wrapped.deleteAlarm();
    txn.rollback();
    assert.throws(() => wrapped.setAlarm(2000), /after transaction rollback/);
  });

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: escaped transaction proxy rejects alarm operations after completion", async () => {
  const { storage } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding([]), "Room", "alice");
  /** @type {any} */
  let escapedTxn;

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    escapedTxn = txn;
  });

  for (const operation of ["setAlarm", "getAlarm", "deleteAlarm"]) {
    assert.throws(
      () => operation === "setAlarm" ? escapedTxn[operation](1000) : escapedTxn[operation](),
      /after transaction completion/
    );
  }
  assert.equal(escapedTxn.deleteAll, undefined);
  assert.throws(() => escapedTxn.rollback(), /completed/);
});

test("DO alarm shim: escaped rolled-back transaction keeps rollback idempotent", async () => {
  const { storage } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding([]), "Room", "alice");
  /** @type {any} */
  let escapedTxn;

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    escapedTxn = txn;
    txn.rollback();
  });

  assert.doesNotThrow(() => escapedTxn.rollback());
});

test("DO alarm shim: later transactions cannot reactivate an escaped transaction proxy", async (t) => {
  for (const outcome of ["committed", "rolled back"]) {
    await t.test(outcome, async () => {
      /** @type {unknown[][]} */
      const calls = [];
      const { storage, state } = makeDoAlarmStorage();
      const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");
      /** @type {any} */
      let escapedTxn;

      await wrapped.transaction(async (/** @type {any} */ txn) => {
        escapedTxn = txn;
        if (outcome === "rolled back") txn.rollback();
      });

      await wrapped.transaction(async () => {
        const expected = outcome === "rolled back" ? /after transaction rollback/ : /after transaction completion/;
        assert.throws(() => escapedTxn.setAlarm(1000), expected);
        assert.throws(() => escapedTxn.deleteAlarm(), expected);
        assert.throws(
          () => wrapped.transactionSync(() => escapedTxn.getAlarm()),
          expected
        );
      });

      assert.deepEqual(calls, []);
      assert.equal(state.row, null);
    });
  }
});

test("DO alarm shim: callback transaction does not synthesize storage-only methods", async () => {
  const { storage } = makeDoAlarmStorage();
  const wrapped = wrapStorage(storage, makeDoAlarmBinding([]), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    assert.equal(txn.transaction, undefined);
    assert.equal(txn.transactionSync, undefined);
    assert.equal(txn.deleteAll, undefined);
  });
});

test("DO alarm shim: transactional backend rejection leaves other committed storage writes intact", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const { storage, kv } = makeDoAlarmStorage();
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.setAlarmIndex = async (input) => {
    calls.push(["set", input]);
    throw new Error("backend unavailable");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await txn.put("committed-key", "committed-value");
      await txn.setAlarm(1000);
    }),
    /backend unavailable/
  );

  assert.deepEqual(calls.map(([kind]) => kind), ["set"]);
  assert.equal(kv.get("committed-key"), "committed-value");
});

test("DO alarm shim: async transaction rejects deleteAll before local or backend mutation", async () => {
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

  await wrapped.transaction(async (/** @type {any} */ txn) => {
    assert.equal(txn.deleteAll, undefined);
    assert.throws(
      () => wrapped.deleteAll(),
      /deleteAll\(\) cannot be used inside transaction\(\)/
    );
    txn.rollback();
  });

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
  assert.equal(kv.get("kv-key"), "kv-value");
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
    /deleteAll\(\) cannot be used inside transactionSync\(\); call it outside the transaction/
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

test("DO alarm shim: delete fence does not validate unrelated persisted alarm fields", async (t) => {
  for (const row of [{
    scheduled_time: 0,
    retry_count: 0,
    in_flight: 0,
    token: "corrupt-scheduled-time-token",
  }, {
    scheduled_time: 1234,
    retry_count: -1,
    in_flight: 0,
    token: "corrupt-retry-count-token",
  }]) {
    await t.test(row.token, async () => {
      /** @type {unknown[][]} */
      const calls = [];
      const { storage, state } = makeDoAlarmStorage(row);
      const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

      await wrapped.deleteAlarm();

      assert.deepEqual(calls, [
        ["delete", { className: "Room", objectName: "alice", token: row.token }],
      ]);
      assert.equal(state.row, null);
    });
  }
});

test("DO alarm shim: stale delete compensation cannot overwrite a concurrent alarm", async (t) => {
  for (const transactional of [false, true]) {
    await t.test(transactional ? "transaction post-commit" : "non-transactional", async () => {
      /** @type {unknown[][]} */
      const calls = [];
      const initial = {
        scheduled_time: 1234,
        retry_count: 0,
        in_flight: 0,
        token: "concurrent-delete-baseline-token",
      };
      const { storage, state } = makeDoAlarmStorage(initial);
      const deleteStarted = Promise.withResolvers();
      const releaseDelete = Promise.withResolvers();
      const alarmBinding = makeDoAlarmBinding(calls);
      alarmBinding.deleteAlarmIndex = async (input) => {
        calls.push(["delete", input]);
        deleteStarted.resolve(undefined);
        await releaseDelete.promise;
        throw new Error("stale delete failed");
      };
      const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

      const deletion = transactional
        ? wrapped.transaction(async (/** @type {any} */ txn) => txn.deleteAlarm())
        : wrapped.deleteAlarm();
      await deleteStarted.promise;
      assert.notEqual(state.row?.token, initial.token);
      assert.equal(state.row?.in_flight, 1);
      assert.equal(await wrapped.getAlarm(), null);

      const replacement = wrapped.setAlarm(2000);
      const replacementToken = state.row?.token;
      assert.equal(typeof replacementToken, "string");
      assert.notEqual(replacementToken, initial.token);

      const rejected = assert.rejects(deletion, /stale delete failed/);
      releaseDelete.resolve(undefined);
      await rejected;
      await replacement;

      assert.equal(state.row?.scheduled_time, 2000);
      assert.equal(state.row?.token, replacementToken);
      assert.deepEqual(calls.map(([kind]) => kind), ["delete", "set"]);
      assert.equal(/** @type {{ token: string }} */ (calls[0][1]).token, initial.token);
      assert.equal(/** @type {{ token: string }} */ (calls[1][1]).token, replacementToken);
    });
  }
});

test("DO alarm shim: concurrent deletes converge when either backend delete succeeds", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "concurrent-delete-lineage-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  const started = [Promise.withResolvers(), Promise.withResolvers()];
  const release = [Promise.withResolvers(), Promise.withResolvers()];
  let attempt = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    const index = attempt++;
    calls.push(["delete", input]);
    started[index].resolve(undefined);
    await release[index].promise;
    if (index === 1) throw new Error("second delete failed");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  const firstDelete = wrapped.deleteAlarm();
  await started[0].promise;
  const secondDelete = wrapped.deleteAlarm();

  release[0].resolve(undefined);
  await firstDelete;
  assert.equal(state.row, null);
  await started[1].promise;

  const secondRejected = assert.rejects(secondDelete, /second delete failed/);
  release[1].resolve(undefined);
  await secondRejected;

  assert.equal(state.row, null);
  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: initial.token }],
    ["delete", { className: "Room", objectName: "alice", token: initial.token }],
  ]);
});

test("DO alarm shim: alarm completion wins over a failed concurrent delete", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const token = "alarm-completion-delete-lineage-token";
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: Date.now() - 1000,
    retry_count: 0,
    in_flight: 0,
    token,
  });
  const alarmStarted = Promise.withResolvers();
  const releaseAlarm = Promise.withResolvers();
  const deleteStarted = Promise.withResolvers();
  const releaseDelete = Promise.withResolvers();
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    calls.push(["delete", input]);
    deleteStarted.resolve(undefined);
    await releaseDelete.promise;
    throw new Error("concurrent delete failed");
  };
  class SlowAlarm {
    async alarm() {
      alarmStarted.resolve(undefined);
      await releaseAlarm.promise;
    }
  }
  const Wrapped = wrapDurableObjectClass(SlowAlarm, "Room");
  const instance = new Wrapped({ storage, id: "alice" }, { __WDL_DO_ALARMS__: alarmBinding });
  const delivery = instance.fetch(new Request("https://do.internal/__wdl_alarm", {
    method: "POST",
    headers: { "x-wdl-do-internal-alarm": "1" },
    body: JSON.stringify({ token, retryCount: 0 }),
  }));
  await alarmStarted.promise;

  const deletion = wrapStorage(storage, alarmBinding, "Room", "alice").deleteAlarm();
  await deleteStarted.promise;
  const deleteRejected = assert.rejects(deletion, /concurrent delete failed/);

  releaseAlarm.resolve(undefined);
  await assertJsonResponse(await delivery, 200, { ok: true });
  assert.equal(state.row, null);

  releaseDelete.resolve(undefined);
  await deleteRejected;
  assert.equal(state.row, null);
});

test("DO alarm shim: getAlarm attempts best-effort repair after a lost delete acknowledgement", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "lost-delete-ack-token",
  };
  const { storage, state } = makeDoAlarmStorage(initial);
  /** @type {null | { className: string, objectName: string, scheduledTime: number, retryCount: number, token: string }} */
  let backendAlarm = {
    className: "Room",
    objectName: "alice",
    scheduledTime: initial.scheduled_time,
    retryCount: initial.retry_count,
    token: initial.token,
  };
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    calls.push(["delete", input]);
    backendAlarm = null;
    throw new Error("delete acknowledgement lost");
  };
  alarmBinding.setAlarmIndex = async (rawInput) => {
    calls.push(["set", rawInput]);
    const input = /** @type {{ className: string, objectName: string, scheduledTime: number, retryCount: number, token: string }} */ (
      rawInput
    );
    backendAlarm = { ...input };
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(wrapped.deleteAlarm(), /delete acknowledgement lost/);
  assert.equal(backendAlarm, null);
  assert.equal(state.row?.token, initial.token);

  assert.equal(await wrapped.getAlarm(), initial.scheduled_time);
  assert.deepEqual(backendAlarm, {
    className: "Room",
    objectName: "alice",
    scheduledTime: initial.scheduled_time,
    retryCount: initial.retry_count,
    token: initial.token,
  });
  assert.deepEqual(calls.map(([kind]) => kind), ["delete", "set"]);
});

test("DO alarm shim: a later deleteAlarm completes a transactional delete after a lost acknowledgement", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "transaction-lost-delete-ack-token",
  };
  const { storage, state, kv } = makeDoAlarmStorage(initial);
  let backendToken = /** @type {string | null} */ (initial.token);
  let deleteAttempts = 0;
  const alarmBinding = makeDoAlarmBinding(calls);
  alarmBinding.deleteAlarmIndex = async (input) => {
    calls.push(["delete", input]);
    backendToken = null;
    deleteAttempts += 1;
    if (deleteAttempts === 1) throw new Error("delete acknowledgement lost");
  };
  const wrapped = wrapStorage(storage, alarmBinding, "Room", "alice");

  await assert.rejects(
    wrapped.transaction(async (/** @type {any} */ txn) => {
      await txn.put("committed-key", "committed-value");
      await txn.deleteAlarm();
    }),
    /delete acknowledgement lost/
  );

  assert.equal(kv.get("committed-key"), "committed-value");
  assert.equal(state.row?.token, initial.token);
  assert.equal(backendToken, null);

  await wrapped.deleteAlarm();
  assert.equal(state.row, null);
  assert.equal(backendToken, null);
  assert.deepEqual(calls.map(([kind]) => kind), ["delete", "delete"]);
});

test("DO alarm shim: transaction delete unwraps a persisted delete fence", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const backendToken = "persisted-transaction-delete-token";
  const { storage, state } = makeDoAlarmStorage({
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 1,
    token: `delete:persisted-fence:${backendToken}`,
  });
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.transaction(async (/** @type {any} */ txn) => txn.deleteAlarm());

  assert.deepEqual(calls, [
    ["delete", { className: "Room", objectName: "alice", token: backendToken }],
  ]);
  assert.equal(state.row, null);
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

test("DO alarm shim: best-effort deleteAll clears storage and backend alarm", async () => {
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

test("DO alarm shim: best-effort deleteAll bounds KV batches to 128 keys", async () => {
  const { storage, kv } = makeDoAlarmStorage();
  for (let i = 0; i < 129; i += 1) kv.set(`key-${i.toString().padStart(3, "0")}`, i);
  /** @type {number[]} */
  const batchSizes = [];
  const fixtureDelete = storage.delete;
  storage.delete = async (/** @type {string[] | string} */ keys) => {
    batchSizes.push(Array.isArray(keys) ? keys.length : 1);
    await fixtureDelete(keys);
  };
  const wrapped = wrapStorage(storage, makeDoAlarmBinding([]), "Room", "alice");

  await wrapped.deleteAll({ deleteAlarm: false });

  assert.deepEqual(batchSizes, [128, 1]);
  assert.equal(kv.size, 0);
});

test("DO alarm shim: best-effort deleteAll skips _cf_ SQL names case-insensitively", async () => {
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

test("DO alarm shim: best-effort deleteAll preserves the raw WDL alarm row", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 0,
    retry_count: -1,
    in_flight: 1,
    token: "preserved-delete-all-token",
    last_error: "legacy-error",
  };
  const { storage, state, kv } = makeDoAlarmStorage(initial);
  kv.set("kv-key", "kv-value");
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await wrapped.deleteAll({ deleteAlarm: false });

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
  assert.equal(kv.size, 0);
});

test("DO alarm shim: best-effort deleteAll ignores tenant-patched collection iteration", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  /** @type {string[]} */
  const dropped = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "delete-all-intrinsic-token",
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
  assert.deepEqual(state.row, initial);
});

test("DO alarm shim: best-effort deleteAll exposes partial failure", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const initial = {
    scheduled_time: 1234,
    retry_count: 0,
    in_flight: 0,
    token: "partial-delete-all-token",
  };
  const { storage, state, kv } = makeDoAlarmStorage(initial);
  kv.set("kv-key", "kv-value");
  storage.delete = async () => {
    kv.clear();
    throw new Error("KV delete failed after partial mutation");
  };
  const wrapped = wrapStorage(storage, makeDoAlarmBinding(calls), "Room", "alice");

  await assert.rejects(wrapped.deleteAll(), /KV delete failed after partial mutation/);

  assert.deepEqual(calls, []);
  assert.deepEqual(state.row, initial);
  assert.equal(kv.size, 0);
});
