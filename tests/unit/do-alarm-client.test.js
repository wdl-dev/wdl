import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";

import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  readRepositoryJson,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";
import { settlementWithin } from "../helpers/timing.js";

const PROTOCOL_STUB_URL = moduleDataUrl(`
export class DoRuntimeError extends Error {
  details;
  constructor(status, code, message, details = undefined) {
    super(message);
    this.status = status;
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}
export function nonEmptyAlarmString(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}
export function isWellFormedUnicodeString(value) {
  return typeof value === "string" && value.isWellFormed();
}
`);
const STATE_STUB_URL = moduleDataUrl(`
export function log(...args) {
  globalThis.__doAlarmClientLogs.push(args);
}
`);
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const ALARM_RESPONSE_URL = moduleDataUrl(applyModuleReplacements(
  readRepositoryFile("shared/do-alarm-response.js"),
  [
    [/from "shared-bounded-body";/, `from ${JSON.stringify(repositoryFileUrl("shared/bounded-body.js"))};`],
    [/from "shared-respond";/, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
  ]
));
const alarmResponseModule = await import(ALARM_RESPONSE_URL);
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const alarmResponseContract = /** @type {{
 * maxBytes: number,
 * deadlineMs: number,
 * actorSuccessVariants: Record<string, Record<string, unknown>>,
 * dispatchSuccessVariants: Record<string, { ok: true, ignored: boolean }>,
 * dispatchErrorVariants: Record<string, { status: number, body: Record<string, unknown> }>,
 * mutationSuccessVariants: {
 *   set: Record<string, unknown>,
 *   delete: Record<string, unknown>,
 *   cleanup: Record<string, unknown>,
 * },
 * mutationFailureClasses: Record<string, {
 *   minStatus: number,
 *   maxStatus: number,
 *   runtimeCode: string,
 * }>,
 * mutationRequiredFields: string[],
 * }} */ (
  readRepositoryJson("tests/fixtures/do-alarm-response.json")
);

/** @param {{ WORKFLOWS_BACKEND?: unknown, WDL_INTERNAL_AUTH_TOKEN?: unknown }} env */
function alarmEnv(env) {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN, ...env };
}

function loadAlarmModule() {
  const source = applyModuleReplacements(readRepositoryFile("do-runtime/alarm.js"), [
    [/from "do-runtime-protocol";/, `from ${JSON.stringify(PROTOCOL_STUB_URL)};`],
    [/from "do-runtime-state";/, `from ${JSON.stringify(STATE_STUB_URL)};`],
    [/from "shared-internal-auth";/, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
    [/from "shared-do-alarm-response";/, `from ${JSON.stringify(ALARM_RESPONSE_URL)};`],
    [/from "shared-observability";/, `from ${JSON.stringify(repositoryFileUrl("shared/observability.js"))};`],
    [/from "shared-respond";/, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
  ]);
  return import(moduleDataUrl(source));
}

/** @type {Array<{ input: RequestInfo | URL, init?: RequestInit }>} */
let calls;

beforeEach(() => {
  calls = [];
  /** @type {any} */ (globalThis).__doAlarmClientLogs = [];
});

/** @returns {Array<[string, string, Record<string, unknown>]>} */
function alarmClientLogs() {
  return /** @type {Array<[string, string, Record<string, unknown>]>} */ (
    /** @type {any} */ (globalThis).__doAlarmClientLogs
  );
}

test("DO alarm response contract matches the cross-language fixture", async () => {
  assert.equal(alarmResponseModule.DO_ALARM_RESPONSE_MAX_BYTES, alarmResponseContract.maxBytes);
  assert.equal(
    alarmResponseModule.DO_ALARM_RESPONSE_DEADLINE_MS,
    alarmResponseContract.deadlineMs,
  );
  for (const [name, actorResponse] of Object.entries(alarmResponseContract.actorSuccessVariants)) {
    const expected = alarmResponseContract.dispatchSuccessVariants[name];
    assert.deepEqual(
      alarmResponseModule.parseDoAlarmDispatchSuccess(JSON.stringify(actorResponse)),
      { ignored: expected.ignored },
    );
  }
  for (const response of [
    alarmResponseContract.mutationSuccessVariants.set,
    alarmResponseContract.mutationSuccessVariants.delete,
  ]) {
    assert.deepEqual(
      alarmResponseModule.parseDoAlarmMutationSuccess(JSON.stringify(response)),
      response,
    );
  }
  const cleanup = alarmResponseContract.mutationSuccessVariants.cleanup;
  assert.deepEqual(
    alarmResponseModule.parseDoAlarmCleanupSuccess(JSON.stringify(cleanup)),
    cleanup,
  );
  assert.deepEqual(
    Object.keys(cleanup).toSorted(),
    alarmResponseContract.mutationRequiredFields,
  );

  let cancelled = false;
  const oversized = new Response(new ReadableStream({
    cancel() { cancelled = true; },
  }), {
    headers: { "content-length": String(alarmResponseContract.maxBytes + 1) },
  });
  await assert.rejects(() => alarmResponseModule.readDoAlarmResponseText(oversized));
  await Promise.resolve();
  assert.equal(cancelled, true);
});

test("DO alarm response deadline does not wait for read or cancel", async () => {
  const reading = Promise.withResolvers();
  let cancelReason;
  let released = false;
  const response = /** @type {Response} */ (/** @type {unknown} */ ({
    headers: new Headers(),
    body: {
      getReader() {
        return {
          read() { return reading.promise; },
          /** @param {unknown} reason */
          cancel(reason) {
            cancelReason = reason;
            return new Promise(() => {});
          },
          releaseLock() { released = true; },
        };
      },
    },
  }));
  const controller = new AbortController();
  const reason = new DOMException("alarm body deadline", "AbortError");
  const result = alarmResponseModule.readDoAlarmResponseText(response, controller.signal);

  controller.abort(reason);

  await assert.rejects(result, (error) => error === reason);
  assert.equal(cancelReason, reason);
  assert.equal(released, true);
});

/** @param {number} [index] */
function alarmRequestBody(index = 0) {
  assert.ok(calls[index], `expected alarm backend call ${index}`);
  return parseJsonObjectRequestBody(calls[index].init, "DO alarm backend request body");
}

function workflowsBackend(response = Response.json({
  ok: true,
  jobId: "doa-default",
  changed: true,
  deleted: 0,
})) {
  return {
    /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
    fetch(input, init) {
      calls.push({ input, init });
      return Promise.resolve(response);
    },
  };
}

const props = {
  ns: "demo",
  worker: "alarms",
  version: "v1",
  doStorageId: "do_123",
};

test("setAlarmIndex creates a Workflows-backed DO alarm job", async () => {
  const mod = await loadAlarmModule();
  const scheduledTime = Date.now() + 123456;
  const result = await mod.setAlarmIndex(
    alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(Response.json({
      ok: true,
      jobId: "doa-1",
      changed: true,
      deleted: 0,
    })) }),
    props,
    {
      className: "Room",
      objectName: "alice",
      scheduledTime,
      retryCount: 2,
      token: "row-token",
    },
  );

  assert.deepEqual(result, { ok: true, jobId: "doa-1", changed: true, deleted: 0 });
  assert.equal(calls.length, 1);
  assert.equal(String(calls[0].input), "http://workflows/internal/workflows/do-alarms/set");
  assert.equal(calls[0].init?.method, "POST");
  assert.ok(calls[0].init?.signal instanceof AbortSignal);
  assert.deepEqual(alarmRequestBody(), {
    ns: "demo",
    worker: "alarms",
    version: "v1",
    doStorageId: "do_123",
    className: "Room",
    objectName: "alice",
    scheduledTime,
    retryCount: 2,
    token: "row-token",
  });
});

test("setAlarmIndex defaults missing retryCount to zero", async () => {
  const mod = await loadAlarmModule();
  await mod.setAlarmIndex(
    alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend() }),
    props,
    {
      className: "Room",
      objectName: "alice",
      scheduledTime: 123456,
      token: "row-token",
    },
  );

  assert.equal(alarmRequestBody().retryCount, 0);
});

test("deleteAlarmIndex deletes a Workflows-backed DO alarm job by SQLite row token", async () => {
  const mod = await loadAlarmModule();
  await mod.deleteAlarmIndex(
    alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend() }),
    props,
    {
      className: "Room",
      objectName: "alice",
      token: "row-token",
    },
  );

  assert.equal(String(calls[0].input), "http://workflows/internal/workflows/do-alarms/delete");
  assert.deepEqual(alarmRequestBody(), {
    ns: "demo",
    worker: "alarms",
    doStorageId: "do_123",
    className: "Room",
    objectName: "alice",
    token: "row-token",
  });
});

test("alarm backend is required", async () => {
  const mod = await loadAlarmModule();

  await assert.rejects(
    () => mod.setAlarmIndex({}, props, {
      className: "Room",
      objectName: "alice",
      scheduledTime: 123456,
      token: "row-token",
    }),
    { status: 503, code: "do_alarm_backend_unavailable" },
  );
});

test("alarm backend request setup failures are sanitized before dispatch", async () => {
  const mod = await loadAlarmModule();

  await assert.rejects(
    () => mod.setAlarmIndex(
      alarmEnv({
        WDL_INTERNAL_AUTH_TOKEN: "invalid token",
        WORKFLOWS_BACKEND: workflowsBackend(),
      }),
      props,
      {
        className: "Room",
        objectName: "alice",
        scheduledTime: 123456,
        token: "row-token",
      },
    ),
    (error) => {
      const record = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (error);
      assert.equal(record.status, 503);
      assert.equal(record.code, "do_alarm_backend_unavailable");
      assert.equal(record.details, undefined);
      return true;
    },
  );
  assert.equal(calls.length, 0);
  const log = alarmClientLogs().at(-1);
  assert.ok(log);
  assert.equal(log[1], "do_alarm_backend_mutation_failed");
  assert.equal(log[2].phase, "request_setup");
  assert.match(String(log[2].error_message), /WDL_INTERNAL_AUTH_TOKEN/);
  assert.equal(JSON.stringify(log[2]).includes("invalid token"), false);
  assert.equal(JSON.stringify(log[2]).includes("row-token"), false);
});

test("alarm backend 5xx responses are result-unknown and sanitized", async (t) => {
  const failureClass = alarmResponseContract.mutationFailureClasses.resultUnknown;
  for (const operation of ["setAlarmIndex", "deleteAlarmIndex"]) {
    for (const status of [failureClass.minStatus, failureClass.maxStatus]) {
      await t.test(`${operation}: ${status}`, async () => {
        const mod = await loadAlarmModule();
        const input = operation === "setAlarmIndex"
          ? {
              className: "Room",
              objectName: "alice",
              scheduledTime: 123456,
              token: "row-token",
            }
          : { className: "Room", objectName: "alice", token: "row-token" };
        const mutate = operation === "setAlarmIndex"
          ? mod.setAlarmIndex
          : mod.deleteAlarmIndex;

        await assert.rejects(
          () => mutate(
            alarmEnv({
              WORKFLOWS_BACKEND: workflowsBackend(Response.json({
                error: "redis_error",
                message: "internal Redis diagnostic",
              }, { status })),
            }),
            props,
            input,
          ),
          (error) => {
            const record = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (error);
            assert.equal(record.status, 503);
            assert.equal(record.code, failureClass.runtimeCode);
            assert.equal(record.details, undefined);
            return true;
          },
        );
      });
    }
  }
});

test("alarm backend 4xx responses remain definite sanitized rejections", async (t) => {
  const failureClass = alarmResponseContract
    .mutationFailureClasses.definitePreMutationRejection;

  for (const status of [failureClass.minStatus, failureClass.maxStatus]) {
    await t.test(String(status), async () => {
      const mod = await loadAlarmModule();
      await assert.rejects(
        () => mod.setAlarmIndex(
          alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(Response.json({
            error: "invalid_request",
            message: "internal validation diagnostic",
          }, { status })) }),
          props,
          {
            className: "Room",
            objectName: "alice",
            scheduledTime: 123456,
            token: "row-token",
          },
        ),
        (error) => {
          const record = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (error);
          assert.equal(record.status, 503);
          assert.equal(record.code, failureClass.runtimeCode);
          assert.equal(record.details, undefined);
          return true;
        },
      );
    });
  }
});

test("alarm backend non-2xx responses are classified without waiting for the body", async () => {
  const mod = await loadAlarmModule();
  let cancelled = false;
  const response = new Response(new ReadableStream({
    pull() {
      return new Promise(() => {});
    },
    cancel() {
      cancelled = true;
    },
  }), { status: 500 });

  const outcome = await settlementWithin(mod.deleteAlarmIndex(
    alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(response) }),
    props,
    { className: "Room", objectName: "alice", token: "row-token" },
  ));
  assert.equal(outcome.status, "rejected");
  if (outcome.status !== "rejected") return;
  const record = /** @type {{ code?: unknown, details?: unknown }} */ (outcome.reason);
  assert.equal(record.code, "do_alarm_result_unknown");
  assert.equal(record.details, undefined);
  assert.equal(cancelled, true);
});

test("alarm backend fetch rejections are result-unknown after mutation dispatch begins", async (t) => {
  for (const operation of ["setAlarmIndex", "deleteAlarmIndex"]) {
    await t.test(operation, async () => {
      const mod = await loadAlarmModule();
      const backend = {
        /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
        fetch(input, init) {
          calls.push({ input, init });
          return Promise.reject(new Error("backend response lost"));
        },
      };
      const input = operation === "setAlarmIndex"
        ? {
            className: "Room",
            objectName: "alice",
            scheduledTime: 123456,
            token: "row-token",
          }
        : { className: "Room", objectName: "alice", token: "row-token" };
      const mutate = operation === "setAlarmIndex"
        ? mod.setAlarmIndex
        : mod.deleteAlarmIndex;

      await assert.rejects(
        () => mutate(alarmEnv({ WORKFLOWS_BACKEND: backend }), props, input),
        (error) => {
          const record = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (error);
          assert.equal(record.status, 503);
          assert.equal(record.code, "do_alarm_result_unknown");
          assert.equal(record.details, undefined);
          return true;
        },
      );
      const log = alarmClientLogs().at(-1);
      assert.ok(log);
      assert.equal(log[0], "warn");
      assert.equal(log[1], "do_alarm_backend_mutation_failed");
      assert.equal(log[2].operation, operation === "setAlarmIndex" ? "set" : "delete");
      assert.equal(log[2].namespace, "demo");
      assert.equal(log[2].worker, "alarms");
      assert.equal(log[2].class_name, "Room");
      assert.equal(log[2].object_name, "alice");
      assert.equal(log[2].phase, "transport");
      assert.equal(log[2].error_message, "backend response lost");
      assert.equal(JSON.stringify(log[2]).includes("row-token"), false);
    });
  }
});

test("unreadable alarm backend 2xx responses are result-unknown", async () => {
  const mod = await loadAlarmModule();
  const response = new Response(new ReadableStream({
    start(controller) {
      controller.error(new Error("internal response-body diagnostic"));
    },
  }), { status: 200 });

  await assert.rejects(
    () => mod.deleteAlarmIndex(
      alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(response) }),
      props,
      { className: "Room", objectName: "alice", token: "row-token" },
    ),
    (error) => {
      const record = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (error);
      assert.equal(record.status, 503);
      assert.equal(record.code, "do_alarm_result_unknown");
      assert.equal(record.details, undefined);
      return true;
    },
  );
});

test("alarm mutation responses fail closed before SQLite compensation decisions", async (t) => {
  const cases = [
    ["empty", new Response("")],
    ["invalid JSON", new Response("not-json")],
    ["scalar", Response.json(true)],
    ["empty object", Response.json({})],
    ["ok false", Response.json({ ok: false, jobId: "doa-1", changed: false, deleted: 0 })],
    ["missing jobId", Response.json({ ok: true, changed: false, deleted: 0 })],
    ["wrong changed type", Response.json({ ok: true, jobId: "doa-1", changed: 0, deleted: 0 })],
    ["wrong deleted type", Response.json({ ok: true, jobId: "doa-1", changed: false, deleted: "0" })],
    ["unknown field", Response.json({ ok: true, jobId: "doa-1", changed: false, deleted: 0, extra: true })],
    ["oversized", new Response("x".repeat(alarmResponseContract.maxBytes + 1))],
    ["invalid UTF-8", new Response(new Uint8Array([0x7b, 0x22, 0x78, 0x22, 0x3a, 0x22, 0x80, 0x22, 0x7d]))],
  ];

  for (const operation of ["setAlarmIndex", "deleteAlarmIndex"]) {
    for (const [label, response] of cases) {
      await t.test(`${operation}: ${label}`, async () => {
        const mod = await loadAlarmModule();
        const input = operation === "setAlarmIndex"
          ? {
              className: "Room",
              objectName: "alice",
              scheduledTime: 123456,
              token: "row-token",
            }
          : { className: "Room", objectName: "alice", token: "row-token" };
        const mutate = operation === "setAlarmIndex"
          ? mod.setAlarmIndex
          : mod.deleteAlarmIndex;
        await assert.rejects(
          () => mutate(
            alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(/** @type {Response} */ (response).clone()) }),
            props,
            input,
          ),
          { status: 503, code: "do_alarm_result_unknown" },
        );
      });
    }
  }
});

test("alarm input validation remains local before backend calls", async () => {
  const mod = await loadAlarmModule();

  assert.throws(
    () => mod.normalizeAlarmScheduledTime(0),
    /setAlarm\(\) cannot be called with an alarm time <= 0/,
  );
  await assert.rejects(
    () => mod.setAlarmIndex(
      alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend() }),
      props,
      {
        className: "Room",
        objectName: "alice",
        scheduledTime: 123456,
        retryCount: -1,
        token: "row-token",
      },
    ),
    /retryCount must be a non-negative integer/,
  );
  await assert.rejects(
    () => mod.setAlarmIndex(
      alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend() }),
      props,
      {
        className: "Room",
        objectName: "\ud800",
        scheduledTime: 123456,
        token: "row-token",
      },
    ),
    /objectName must contain well-formed Unicode/,
  );
  assert.equal(calls.length, 0);
});
