import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";

import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const PROTOCOL_STUB_URL = moduleDataUrl(`
export class DoRuntimeError extends Error {
  constructor(status, code, message, details = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}
export function nonEmptyAlarmString(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}
export function isWellFormedUnicodeString(value) {
  return typeof value === "string" && value.isWellFormed();
}
`);
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

/** @param {{ WORKFLOWS_BACKEND?: unknown }} env */
function alarmEnv(env) {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN, ...env };
}

function loadAlarmModule() {
  const source = applyModuleReplacements(readRepositoryFile("do-runtime/alarm.js"), [
    [/from "do-runtime-protocol";/, `from ${JSON.stringify(PROTOCOL_STUB_URL)};`],
    [/from "shared-internal-auth";/, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
    [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  ]);
  return import(moduleDataUrl(source));
}

/** @type {Array<{ input: RequestInfo | URL, init?: RequestInit }>} */
let calls;

beforeEach(() => {
  calls = [];
});

/** @param {number} [index] */
function alarmRequestBody(index = 0) {
  assert.ok(calls[index], `expected alarm backend call ${index}`);
  return parseJsonObjectRequestBody(calls[index].init, "DO alarm backend request body");
}

function workflowsBackend(response = Response.json({ ok: true })) {
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
    alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(Response.json({ ok: true, jobId: "doa-1" })) }),
    props,
    {
      className: "Room",
      objectName: "alice",
      scheduledTime,
      retryCount: 2,
      token: "row-token",
    },
  );

  assert.deepEqual(result, { ok: true, jobId: "doa-1" });
  assert.equal(calls.length, 1);
  assert.equal(String(calls[0].input), "http://workflows/internal/workflows/do-alarms/set");
  assert.equal(calls[0].init?.method, "POST");
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

test("alarm backend failures are surfaced as DO runtime errors", async () => {
  const mod = await loadAlarmModule();

  await assert.rejects(
    () => mod.setAlarmIndex(
      alarmEnv({ WORKFLOWS_BACKEND: workflowsBackend(Response.json({ error: "boom" }, { status: 500 })) }),
      props,
      {
        className: "Room",
        objectName: "alice",
        scheduledTime: 123456,
        token: "row-token",
      },
    ),
    { status: 503, code: "do_alarm_backend_failed" },
  );
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
