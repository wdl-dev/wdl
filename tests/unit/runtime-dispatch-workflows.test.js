import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { parseJsonText } from "../helpers/json-payload.js";
import { loadRuntimeDispatch } from "../helpers/load-runtime-dispatch.js";
import {
  importRepositoryModule,
  readRepositoryJson,
} from "../helpers/load-shared-module.js";
import {
  jsonRequest,
  makeScope,
  makeStub,
  makeWorkflowBackend,
} from "../helpers/runtime-dispatch-fixtures.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { delay } from "../helpers/timing.js";

const { runtimeDispatch, runtimeDispatchWorkflowStep } = await loadRuntimeDispatch();
const workflowJson = await importRepositoryModule("runtime/dispatch/workflow-json.js");
const workflowLimits = /** @type {{ resultBytesMax: number, backendRequestBytesMax: number, payloadTooLargeCode: string }} */ (
  readRepositoryJson("tests/fixtures/workflow-limits.json")
);
const {
  _resetWorkflowReplayCacheForTest,
  _stringifyWorkflowBackendBodyForTest,
  _stringifyWorkflowJsonForTest,
  handleWorkflowNotifyDispatch,
  handleWorkflowRunDispatch,
  readWorkflowNotifyDispatch,
  readWorkflowRunDispatch,
} = runtimeDispatch;
const {
  MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN,
  MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN,
  workflowError,
} = runtimeDispatchWorkflowStep;

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

/** @param {unknown} backend */
function workflowEnv(backend) {
  return { WORKFLOWS_BACKEND: backend, WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
}

beforeEach(() => {
  _resetWorkflowReplayCacheForTest();
});

test("workflow payload limits match the shared Rust/JS contract", () => {
  assert.equal(workflowJson.WORKFLOW_RESULT_BYTES_MAX, workflowLimits.resultBytesMax);
  assert.equal(
    workflowJson.WORKFLOW_BACKEND_REQUEST_BYTES_MAX,
    workflowLimits.backendRequestBytesMax
  );
  assert.equal(
    workflowJson.WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
    workflowLimits.payloadTooLargeCode
  );
  assert.throws(
    () => workflowJson._stringifyWorkflowJsonForTest("x", 0),
    (err) => err instanceof Error && err.name === workflowLimits.payloadTooLargeCode
  );
});

test("workflow bounded JSON serializer matches JSON.stringify for supported values", () => {
  const inherited = Object.create({ hidden: true });
  inherited.visible = "yes";
  const nestedToJson = {
    x: {
      toJSON() {
        return {
          y: "outer",
          toJSON() {
            return { y: "inner" };
          },
        };
      },
    },
  };
  const customString = new String("abc");
  customString.toString = () => "xyz";
  customString.valueOf = () => "def";
  const customNumber = /** @type {any} */ (new Number(3));
  customNumber.toString = () => "4";
  customNumber.valueOf = () => ({});
  const nonCallableNumber = /** @type {any} */ (new Number(3));
  nonCallableNumber.valueOf = 3;
  nonCallableNumber.toString = () => "4";
  const nullPrimitiveNumber = /** @type {any} */ (new Number(3));
  nullPrimitiveNumber[Symbol.toPrimitive] = null;
  nullPrimitiveNumber.valueOf = () => ({});
  nullPrimitiveNumber.toString = () => "4";
  const customBoolean = new Boolean(false);
  customBoolean.valueOf = () => true;
  const fakeString = Object.create(String.prototype);
  const fakeNumber = Object.create(Number.prototype);
  const fakeBoolean = Object.create(Boolean.prototype);
  const fakeBigInt = { [Symbol.toStringTag]: "BigInt", ok: true };
  const cases = [
    null,
    true,
    false,
    0,
    -0,
    NaN,
    Infinity,
    "ascii",
    "中文",
    "😀",
    "\u0000\b\t\n\f\r\"\\",
    "\ud800",
    "\udc00",
    "😀".repeat(8200),
    `${"a".repeat(8191)}😀aaa`,
    new String("boxed"),
    new Number(3),
    new Number(NaN),
    new Boolean(false),
    customString,
    customNumber,
    nonCallableNumber,
    nullPrimitiveNumber,
    customBoolean,
    fakeString,
    fakeNumber,
    fakeBoolean,
    fakeBigInt,
    ["a", undefined, () => "skip", Symbol("skip"), null, 3],
    { b: 2, a: [3, { y: null, x: "ok" }], skipped: undefined, fn() {}, sym: Symbol("skip") },
    { toJSON() { return { z: "ok" }; } },
    { date: new Date("2026-05-13T12:00:00.000Z") },
    nestedToJson,
    inherited,
  ];
  for (const value of cases) {
    assert.equal(_stringifyWorkflowJsonForTest(value), JSON.stringify(value));
  }
});

test("workflow bounded JSON serializer rejects circular values and BigInt like JSON.stringify", () => {
  /** @type {any} */
  const circular = {};
  circular.self = circular;
  /** @type {any} */
  const numberToBigInt = new Number(3);
  numberToBigInt[Symbol.toPrimitive] = () => 4n;
  assert.throws(() => _stringifyWorkflowJsonForTest(circular), /circular/i);
  assert.throws(() => _stringifyWorkflowJsonForTest(1n), /BigInt/);
  assert.throws(() => _stringifyWorkflowJsonForTest(Object(1n)), /BigInt/);
  assert.throws(() => _stringifyWorkflowJsonForTest(numberToBigInt), /BigInt|Cannot convert/);
});

test("runtime dispatch JSON reader rejects oversized bodies before parsing", async () => {
  const response = (await readWorkflowNotifyDispatch(new Request("https://runtime.internal/_workflow", {
    method: "POST",
    headers: { "content-length": String(256 * 1024 + 1) },
    body: "{}",
  }))).response;
  const body = await readJsonResponse(response, 413);
  assert.equal(body.error, "request_body_too_large");
});

test("workflow bounded JSON serializer does not over-count split surrogate pairs", () => {
  const value = `${"a".repeat(8191)}😀`;
  const maxBytes = new TextEncoder().encode(JSON.stringify(value)).byteLength;
  assert.equal(_stringifyWorkflowJsonForTest(value, maxBytes), JSON.stringify(value));
  assert.throws(
    () => _stringifyWorkflowJsonForTest(value, maxBytes - 1),
    /workflow_payload_too_large/
  );
});

test("workflow backend body serializer enforces per-field result caps in one pass", () => {
  const output = {
    toJSON() {
      return "x".repeat(1024 * 1024 + 1);
    },
  };
  assert.throws(
    () => _stringifyWorkflowBackendBodyForTest("commit-step-success", {
      ns: "demo",
      output,
    }),
    /Workflow step output exceeds the 1048576 byte limit/
  );
});

test("readWorkflowRunDispatch normalizes workflow run payload", async () => {
  const parsed = await readWorkflowRunDispatch(jsonRequest({
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 3,
    createdAtMs: 12345,
    runToken: "run-1",
    params: { orderId: 123 },
  }));

  assert.deepEqual(parsed.body, {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 3,
    createdAtMs: 12345,
    runToken: "run-1",
    event: { payload: { orderId: 123 } },
  });
});

test("readWorkflowRunDispatch accepts max-size workflow params with framing", async () => {
  const params = "x".repeat(1024 * 1024 - 2);
  const parsed = await readWorkflowRunDispatch(jsonRequest({
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 3,
    createdAtMs: 12345,
    runToken: "run-1",
    params,
  }));

  assert.equal(parsed.response, undefined);
  assert.equal(parsed.body.event.payload.length, 1024 * 1024 - 2);
});

test("handleWorkflowRunDispatch invokes named workflow run with step.do facade", async () => {
  const scope = makeScope();
  scope.requestId = "rid-workflow";
  /** @type {any[]} */
  const calls = [];
  /** @type {string[]} */
  let stepSurface = [];
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 2 });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "not_found", message: "not found" }, { status: 404 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      createdAtMs: 12345,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ event, /** @type {any} */ step) {
            calls.push(event);
            stepSurface = Object.keys(step).toSorted();
            return await step.do("charge", async (/** @type {{ attempt: number }} */ { attempt }) => ({
              charged: event.payload.orderId,
              attempt,
            }));
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual({
    outcome: body.outcome,
    output: body.output,
  }, {
    outcome: "completed",
    output: { charged: 123, attempt: 2 },
  });
  assert.equal(typeof body.duration_ms, "number");
  assert.deepEqual(calls, [{ payload: { orderId: 123 } }]);
  assert.deepEqual(stepSurface, ["do", "sleep", "sleepUntil", "waitForEvent"]);
  assert.deepEqual(backend.calls.map((c) => c.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
  assert.deepEqual(backend.calls.map((c) => c.headers["x-request-id"]), [
    "rid-workflow",
    "rid-workflow",
    "rid-workflow",
  ]);
  assert.deepEqual(backend.calls.map((c) => c.headers["x-wdl-internal-auth"]), [
    TEST_INTERNAL_AUTH_TOKEN,
    TEST_INTERNAL_AUTH_TOKEN,
    TEST_INTERNAL_AUTH_TOKEN,
  ]);
  assert.deepEqual(backend.calls.slice(1).map((c) => ({
    ordinal: c.body.ordinal,
    stepName: c.body.stepName,
    nameCount: c.body.nameCount,
    createdAtMs: c.body.createdAtMs,
    config: c.body.config,
    output: c.body.output,
  })), [
    {
      ordinal: 0,
      stepName: "charge",
      nameCount: 1,
      createdAtMs: 12345,
      config: null,
      output: undefined,
    },
    {
      ordinal: 0,
      stepName: "charge",
      nameCount: 1,
      createdAtMs: 12345,
      config: null,
      output: { charged: 123, attempt: 2 },
    },
  ]);
  assert.equal(backend.calls[2].body.attempt, 2);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rejects oversized terminal output before response construction", async () => {
  const scope = makeScope();
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-oversized",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(makeWorkflowBackend(async () => Response.json({ state: "run" }))),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run() {
            return "x".repeat(1024 * 1024 + 1);
          },
        },
      },
    }),
  });

  assert.equal(res.status, 200);
  const text = await res.text();
  assert.ok(text.length < 2048, "oversized output must not be echoed in the response body");
  const body = parseJsonText(text, "workflow dispatch response");
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_payload_too_large");
});

test("handleWorkflowRunDispatch rejects oversized step output before backend request construction", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) {
      throw new Error("oversized step output must not be sent to workflows backend");
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-step-oversized",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("large", async () => "x".repeat(1024 * 1024 + 1));
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_payload_too_large");
  assert.deepEqual(backend.calls.map((c) => c.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
  ]);
});

test("handleWorkflowRunDispatch serializes step output once before backend request construction", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  let toJsonCalls = 0;
  const trickyOutput = {
    toJSON() {
      toJsonCalls += 1;
      return toJsonCalls === 1 ? "small" : "x".repeat(1024 * 1024 + 1);
    },
  };

  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-step-once",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("tricky", async () => trickyOutput);
          },
        },
      },
    }),
  });

  assert.equal(res.status, 200);
  assert.equal(backend.calls.find((c) => c.url.endsWith("/commit-step-success"))?.body.output, "small");
});

test("handleWorkflowRunDispatch replays completed step.do output without callback", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "complete", output: { charged: 123 } });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => ({
      steps: [{
        ordinal: 0,
        name: "charge",
        nameCount: 1,
        dependencies: [],
        config: "null",
        status: "completed",
        attempt: 1,
        output: { charged: 123 },
      }],
      nextOrdinal: body.startOrdinal + 1,
      done: true,
    }),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      createdAtMs: 12345,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => {
              callbackCalls += 1;
              return { charged: 999 };
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual({
    outcome: body.outcome,
    output: body.output,
  }, {
    outcome: "completed",
    output: { charged: 123 },
  });
  assert.equal(callbackCalls, 0);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch replays failed step.do as ordinary persisted error", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async () => {
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => ({
      steps: [{
        ordinal: 0,
        name: "charge",
        nameCount: 1,
        dependencies: [],
        config: "null",
        status: "failed",
        attempt: 1,
        error: {
          name: "workflow_invalid_step",
          message: "persisted user error",
        },
      }],
      nextOrdinal: body.startOrdinal + 1,
      done: true,
    }),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      createdAtMs: 12345,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => "not called");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual(body.error, {
    name: "workflow_invalid_step",
    message: "persisted user error",
  });
  assert.equal(Object.hasOwn(body.error, "code"), false);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
  ]);
  assert.equal(backend.calls[0].body.createdAtMs, 12345);
});

test("handleWorkflowRunDispatch rejects replay records without dependency shape", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ error: "workflow_step_mismatch", message: "shape mismatch" }, { status: 409 });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: () => ({
      steps: [{
        ordinal: 0,
        name: "charge",
        nameCount: 1,
        config: "null",
        status: "completed",
        attempt: 1,
        output: { charged: 123 },
      }],
      nextOrdinal: 1,
      done: true,
    }),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-missing-dependencies",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => {
              callbackCalls += 1;
              return { charged: 999 };
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_step_mismatch");
  assert.equal(callbackCalls, 0);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
  ]);
});

test("handleWorkflowRunDispatch coalesces concurrent step.do replay fetches", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  /** @type {any[]} */
  const replayStarts = [];
  const backend = makeWorkflowBackend(async () => {
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => {
      replayStarts.push(body.startOrdinal);
      return {
        steps: [
          {
            ordinal: 0,
            name: "a",
            nameCount: 1,
            dependencies: [],
            config: "null",
            status: "completed",
            attempt: 1,
            output: "cached-a",
          },
          {
            ordinal: 1,
            name: "b",
            nameCount: 1,
            dependencies: [],
            config: "null",
            status: "completed",
            attempt: 1,
            output: "cached-b",
          },
        ],
        nextOrdinal: body.startOrdinal + 2,
        done: true,
      };
    },
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-parallel-replay",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const results = await Promise.all([
              step.do("a", async () => {
                callbackCalls += 1;
                return "fresh-a";
              }),
              step.do("b", async () => {
                callbackCalls += 1;
                return "fresh-b";
              }),
            ]);
            return results;
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual({
    outcome: body.outcome,
    output: body.output,
  }, {
    outcome: "completed",
    output: ["cached-a", "cached-b"],
  });
  assert.equal(callbackCalls, 0);
  assert.deepEqual(replayStarts, [0]);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch falls back when replay page is unavailable", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "complete", output: { charged: 123 } });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: () => Response.json(
      { error: "redis_error", message: "temporary replay cache miss" },
      { status: 500 }
    ),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => {
              callbackCalls += 1;
              return { charged: 999 };
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual({
    outcome: body.outcome,
    output: body.output,
  }, {
    outcome: "completed",
    output: { charged: 123 },
  });
  assert.equal(callbackCalls, 0);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch isolates replay cache by instance creation time", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => body.createdAtMs === 1000
      ? {
          steps: [{
            ordinal: 0,
            name: "charge",
            nameCount: 1,
            dependencies: [],
            config: "null",
            status: "completed",
            attempt: 1,
            output: { charged: "old" },
          }],
          nextOrdinal: body.startOrdinal + 1,
          done: true,
        }
      : { steps: [], nextOrdinal: body.startOrdinal, done: true },
  });
  const baseRun = {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 1,
    runToken: "run-1",
    event: { payload: { orderId: 123 } },
  };
  const stub = makeStub({
    entrypoints: {
      OrderWorkflow: {
        async run(/** @type {any} */ _event, /** @type {any} */ step) {
          return await step.do("charge", async () => {
            callbackCalls += 1;
            return { charged: "new" };
          });
        },
      },
    },
  });

  const first = await handleWorkflowRunDispatch({
    run: { ...baseRun, createdAtMs: 1000 },
    scope,
    env: workflowEnv(backend),
    stub,
  });
  const second = await handleWorkflowRunDispatch({
    run: { ...baseRun, createdAtMs: 2000 },
    scope,
    env: workflowEnv(backend),
    stub,
  });

  const firstBody = await readJsonResponse(first, 200);
  const secondBody = await readJsonResponse(second, 200);
  assert.equal(callbackCalls, 1);
  assert.deepEqual(firstBody.output, { charged: "old" });
  assert.deepEqual(secondBody.output, { charged: "new" });
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowNotifyDispatch invokes reserved workflow notify entrypoint", async () => {
  const scope = makeScope();
  scope.requestId = "rid-1";
  const parsed = await readWorkflowNotifyDispatch(jsonRequest({
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 1,
    callback: { kind: "do", binding: "ROOMS", idFromName: "room-a" },
    progress: { event: "workflow_step_completed" },
  }));
  /** @type {Request | null} */
  let notifyRequest = null;
  const res = await handleWorkflowNotifyDispatch({
    notify: parsed.body,
    scope,
    stub: makeStub({
      entrypoints: {
        __WdlWorkflowNotify__: {
          async fetch(/** @type {Request} */ request) {
            notifyRequest = request;
            return new Response(null, { status: 204 });
          },
        },
      },
    }),
  });

  assert.equal(res.status, 204);
  assert.ok(notifyRequest);
  const captured = /** @type {Request} */ (notifyRequest);
  assert.equal(captured.headers.get("x-request-id"), "rid-1");
  assert.deepEqual(await captured.json(), parsed.body);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch allows concurrent step.do calls", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const outputs = await Promise.all([
              step.do("a", async () => "a"),
              step.do("b", async () => "b"),
            ]);
            return outputs;
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.deepEqual(body.output, ["a", "b"]);
  assert.deepEqual(
    backend.calls.map((call) => call.url.split("/").at(-1)).toSorted(),
    [
      "claim-step",
      "claim-step",
      "commit-step-success",
      "commit-step-success",
      "replay-steps",
    ]
  );
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => ({
    ordinal: call.body.ordinal,
    stepName: call.body.stepName,
    dependencies: call.body.dependencies,
  })), [
    { ordinal: 0, stepName: "a", dependencies: [] },
    { ordinal: 1, stepName: "b", dependencies: [] },
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rejects starting a step while another step callback is in flight", async () => {
  const scope = makeScope();
  let releaseSlow = () => {};
  let markCallbackStarted = () => {};
  const slow = new Promise((resolve) => {
    releaseSlow = () => resolve(undefined);
  });
  const callbackStarted = new Promise((resolve) => {
    markCallbackStarted = () => resolve(undefined);
  });
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-delayed-fanout",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const a = step.do("a", async () => {
              markCallbackStarted();
              await slow;
              return "a";
            });
            await callbackStarted;
            try {
              await step.do("b", async () => "b");
            } finally {
              releaseSlow();
              await a.catch(() => {});
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /callback is in flight/);
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => call.body.stepName), [
    "a",
  ]);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-error")), false);
});

test("handleWorkflowRunDispatch records DAG dependencies after a parallel join", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-dag",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const [a, b] = await Promise.all([
              step.do("a", async () => "a"),
              step.do("b", async () => "b"),
            ]);
            return await step.do("join", async () => `${a}-${b}`);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "a-b");
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => ({
    ordinal: call.body.ordinal,
    stepName: call.body.stepName,
    dependencies: call.body.dependencies,
  })), [
    { ordinal: 0, stepName: "a", dependencies: [] },
    { ordinal: 1, stepName: "b", dependencies: [] },
    { ordinal: 2, stepName: "join", dependencies: [0, 1] },
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch allows fan-out after an awaited root step", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-root-fanout",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const root = await step.do("root", async () => "root");
            const [a, b] = await Promise.all([
              step.do("a", async () => `${root}-a`),
              step.do("b", async () => `${root}-b`),
            ]);
            return await step.do("join", async () => `${a}-${b}`);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "root-a-root-b");
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => ({
    ordinal: call.body.ordinal,
    stepName: call.body.stepName,
    dependencies: call.body.dependencies,
  })), [
    { ordinal: 0, stepName: "root", dependencies: [] },
    { ordinal: 1, stepName: "a", dependencies: [0] },
    { ordinal: 2, stepName: "b", dependencies: [0] },
    { ordinal: 3, stepName: "join", dependencies: [1, 2] },
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rejects new step.do after awaiting part of an unfinished fan-out", async () => {
  const scope = makeScope();
  let releaseSlowCommit = () => {};
  const slowCommit = new Promise((resolve) => {
    releaseSlowCommit = () => resolve(undefined);
  });
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) {
      if (body.stepName === "slow") await slowCommit;
      return Response.json({ state: "complete" });
    }
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-fanout-after-await",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const slow = step.do("slow", async () => "slow");
            const fast = await step.do("fast", async () => "fast");
            try {
              await step.do("after-fast", async () => fast);
            } finally {
              releaseSlowCommit();
              await slow.catch(() => {});
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /fan-out/);
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => call.body.stepName), [
    "slow",
    "fast",
  ]);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/claim-step") && call.body.stepName === "after-fast"), false);
});

test("handleWorkflowRunDispatch records serial fan-out and join DAG dependencies", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-dag-fanout",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const root = await step.do("root", async () => "root");
            const [left, right] = await Promise.all([
              step.do("left", async () => `${root}-left`),
              step.do("right", async () => `${root}-right`),
            ]);
            return await step.do("join", async () => `${left}:${right}`);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "root-left:root-right");
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => ({
    ordinal: call.body.ordinal,
    stepName: call.body.stepName,
    dependencies: call.body.dependencies,
  })), [
    { ordinal: 0, stepName: "root", dependencies: [] },
    { ordinal: 1, stepName: "left", dependencies: [0] },
    { ordinal: 2, stepName: "right", dependencies: [0] },
    { ordinal: 3, stepName: "join", dependencies: [1, 2] },
  ]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rejects unresolved step.do before a suspension", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-race-divergence",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const slow = step.do("slow", async () => {
              await delay(10);
              return "slow";
            });
            const fast = step.do("fast", async () => "fast");
            await Promise.race([slow, fast]);
            await step.sleep("after-race", 1000);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /callback is in flight|suspending steps/);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), false);
});

test("handleWorkflowRunDispatch rejects suspending steps while another step is in flight", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const first = step.do("slow", async () => "ok");
            try {
              await step.sleep("settle", 1000);
            } finally {
              await first;
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /suspending steps/);
  assert.equal(scope.errors.length, 1);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), false);
});

test("handleWorkflowRunDispatch rejects nested step.do calls inside a step callback", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-nested-step",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("outer", async () => {
              await Promise.resolve();
              return await step.do("inner", async () => "inner");
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /callback is in flight/);
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => call.body.stepName), [
    "outer",
  ]);
  const errorCommit = backend.calls.find((call) => call.url.endsWith("/commit-step-error"));
  assert.equal(errorCommit?.body.nonRetryable, true);
});

test("handleWorkflowRunDispatch rejects suspending steps inside a step callback", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-nested-sleep",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("outer", async () => {
              await step.sleep("inner-sleep", 1000);
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /callback is in flight/);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), false);
  const errorCommit = backend.calls.find((call) => call.url.endsWith("/commit-step-error"));
  assert.equal(errorCommit?.body.nonRetryable, true);
});

test("handleWorkflowRunDispatch does not let unawaited invalid steps turn into suspension", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.do("", async () => "must-not-run");
            await step.sleep("settle", 1000);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /step name/);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), false);
});

test("handleWorkflowRunDispatch rejects runs that return before step.do settles", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.do("unawaited", async () => "late");
            return "returned-too-early";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /still in flight/);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-success")), false);
});

test("handleWorkflowRunDispatch caps active step.do fan-out before backend claims", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-too-many-steps",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const steps = [];
            for (let i = 0; i <= MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN; i++) {
              steps.push(step.do(`step-${i}`, async () => i));
            }
            return await Promise.all(steps);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "request_too_large");
  assert.match(body.error.message, /more than 1000 workflow steps/);
  assert.equal(backend.calls.filter((call) => call.url.endsWith("/claim-step")).length, 0);
});

test("handleWorkflowRunDispatch caps fresh backend steps started in one dispatch turn", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-too-many-fresh-steps",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            for (let i = 0; i <= MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN; i++) {
              await step.do(`step-${i}`, async () => i);
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "request_too_large");
  assert.match(body.error.message, /started more than 1000 steps/);
  assert.equal(backend.calls.filter((call) => call.url.endsWith("/claim-step")).length, MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN);
});

test("handleWorkflowRunDispatch does not count replay hits against the started-step cap", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => body.startOrdinal <= MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN
      ? {
          steps: [{
            ordinal: body.startOrdinal,
            name: `step-${body.startOrdinal}`,
            nameCount: 1,
            dependencies: body.startOrdinal === 0 ? [] : [body.startOrdinal - 1],
            config: "null",
            status: "completed",
            output: body.startOrdinal,
          }],
          nextOrdinal: body.startOrdinal + 1,
          done: false,
        }
      : {
          steps: [],
          nextOrdinal: body.startOrdinal,
          done: true,
        },
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-many-replay-hits",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            for (let i = 0; i <= MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN; i++) {
              await step.do(`step-${i}`, async () => i);
            }
            return await step.do("fresh-after-replay", async () => "fresh");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "fresh");
  assert.equal(backend.calls.filter((call) => call.url.endsWith("/claim-step")).length, 1);
});

test("handleWorkflowRunDispatch closes in-flight step.do when the run throws", async () => {
  const scope = makeScope();
  let releaseSlow = () => {};
  const slow = new Promise((resolve) => {
    releaseSlow = () => resolve(undefined);
  });
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-throw-with-inflight",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.do("late", async () => {
              await slow;
              return "late";
            });
            await delay(0);
            throw new Error("run failed");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.message, "run failed");
  releaseSlow();
  await delay(0);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-success")), false);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-error")), false);
});

test("handleWorkflowRunDispatch closes unawaited sleep before backend registration", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-unawaited-sleep",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.sleep("later", 60_000);
            return "done";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  await delay(0);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), false);
});

test("handleWorkflowRunDispatch fails when an unawaited sleep registers before run return", async () => {
  const scope = makeScope();
  let markSleepRegistered = () => {};
  const sleepRegistered = new Promise((resolve) => {
    markSleepRegistered = () => resolve(undefined);
  });
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) {
      markSleepRegistered();
      return Response.json({ state: "waiting" });
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-unawaited-sleep-registered",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.sleep("later", 60_000);
            await sleepRegistered;
            return "done";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-sleep")), true);
});

test("handleWorkflowRunDispatch closes unawaited waitForEvent before backend registration", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-unawaited-wait",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.waitForEvent("later", { type: "ready" });
            return "done";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  await delay(0);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/register-wait")), false);
});

test("handleWorkflowRunDispatch commits failed step.do errors", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => {
              throw new TypeError("card declined");
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, {
    name: "TypeError",
    message: "card declined",
  });
  assert.deepEqual(backend.calls.map((c) => c.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-error",
  ]);
  assert.deepEqual(backend.calls[2].body.error, {
    name: "TypeError",
    message: "card declined",
  });
  assert.equal(scope.errors.length, 1);
});

test("handleWorkflowRunDispatch commits hostile terminal step.do failures", async () => {
  const scope = makeScope();
  const throwable = new Proxy(Object.create(null), {
    get() {
      throw new Error("throwable field trap");
    },
    getPrototypeOf() {
      throw new Error("throwable prototype trap");
    },
  });
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.do("charge", async () => {
                throw throwable;
              });
            } catch {
              return "swallowed";
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, {
    name: "Error",
    message: "Workflow run failed",
  });
  assert.deepEqual(backend.calls[2].body.error, body.error);
  assert.equal(scope.errors.length, 1);
});

test("workflowError falls back when throwable conversion throws", () => {
  const throwable = {
    toString() {
      throw new Error("conversion failed");
    },
  };
  assert.deepEqual(workflowError(throwable), {
    name: "Error",
    message: "Workflow run failed",
  });
});

test("workflow internal error codes cannot be forged with Error.name", async () => {
  const forged = new Error("not internal");
  forged.name = "workflow_invalid_step";
  assert.deepEqual(workflowError(forged), {
    name: "workflow_invalid_step",
    message: "not internal",
  });
});

test("handleWorkflowRunDispatch does not trust a forged WorkflowSuspended name", async () => {
  const scope = makeScope();
  const forged = { name: "WorkflowSuspended", message: "forged suspension" };
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-forged-suspension",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.do("forged", () => Promise.reject(forged));
            } catch {
              return "tenant recovered";
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, forged);
  assert.deepEqual(scope.errors, [forged]);
});

test("handleWorkflowRunDispatch rejects new durable steps after swallowed terminal step.do failure", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-terminal-then-fallback",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.do("primary", async () => {
                throw new TypeError("primary failed");
              });
            } catch {}
            return await step.do("fallback", async () => "must-not-commit");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, {
    name: "TypeError",
    message: "primary failed",
  });
  assert.deepEqual(backend.calls.filter((call) => call.url.endsWith("/claim-step")).map((call) => call.body.stepName), [
    "primary",
  ]);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-success")), false);
});

test("handleWorkflowRunDispatch suspends when failed step.do is retryable", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "waiting", dueAtMs: 1234 });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", { retries: { limit: 2, delayMs: 1, backoff: "constant" } }, async () => {
              throw new Error("transient");
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "suspended");
  assert.equal(backend.calls[2].body.attempt, 1);
  assert.deepEqual(backend.calls[2].body.config, { retries: { limit: 2, delayMs: 1, backoff: "constant" } });
  assert.equal(backend.calls[2].body.nonRetryable, false);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rechecks cached retry waiting step with backend", async () => {
  const scope = makeScope();
  let callbackRan = false;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "waiting", dueAtMs: 4102444800000 });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => ({
      steps: [{
        ordinal: 0,
        name: "charge",
        nameCount: 1,
        dependencies: [],
        config: '{"retries":{"backoff":"constant","delayMs":1,"limit":2}}',
        status: "waiting",
        attempt: 1,
        dueAtMs: 4102444800000,
      }],
      nextOrdinal: body.startOrdinal + 1,
      done: true,
    }),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", { retries: { limit: 2, delayMs: 1, backoff: "constant" } }, async () => {
              callbackRan = true;
              return "charged";
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "suspended");
  assert.equal(callbackRan, false);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
  ]);
  assert.deepEqual(backend.calls[1].body.config, {
    retries: { limit: 2, delayMs: 1, backoff: "constant" },
  });
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch lets parallel sibling commit after retry suspension", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) {
      assert.equal(body.stepName, "retryable");
      return Response.json({ state: "waiting", dueAtMs: 1234 });
    }
    if (url.endsWith("/commit-step-success")) {
      assert.equal(body.stepName, "ok");
      return Response.json({ state: "complete" });
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-parallel-waiting-sibling",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await Promise.all([
              step.do("retryable", { retries: { limit: 2, delayMs: 1 } }, async () => {
                throw new Error("transient");
              }),
              step.do("ok", async () => {
                await Promise.resolve();
                return "ok";
              }),
            ]);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "suspended");
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-success")), true);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch does not let retry suspension hide parallel terminal failure", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) {
      if (body.stepName === "retryable") return Response.json({ state: "waiting", dueAtMs: 1234 });
      return Response.json({ state: "failed" });
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await Promise.all([
              step.do("retryable", { retries: { limit: 2, delayMs: 1 } }, async () => {
                throw new Error("transient");
              }),
              step.do("terminal", async () => {
                await Promise.resolve();
                throw new TypeError("fatal");
              }),
            ]);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, {
    name: "TypeError",
    message: "fatal",
  });
  assert.equal(scope.errors.length, 1);
});

test("handleWorkflowRunDispatch marks NonRetryableError step failures terminal", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", { retries: { limit: 10, delayMs: 1 } }, async () => {
              const err = new Error("do not retry");
              err.name = "NonRetryableError";
              throw err;
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "NonRetryableError");
  assert.equal(backend.calls[2].body.nonRetryable, true);
  assert.equal(scope.errors.length, 1);
});

test("handleWorkflowRunDispatch keeps falsey step failures terminal after tenant catch", async () => {
  for (const [thrown, expectedMessage] of [
    [false, "false"],
    [0, "0"],
    ["", ""],
    [null, "Workflow run failed"],
    [undefined, "Workflow run failed"],
  ]) {
    _resetWorkflowReplayCacheForTest();
    const scope = makeScope();
    const backend = makeWorkflowBackend(async (url) => {
      if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
      if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
      return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
    });
    const res = await handleWorkflowRunDispatch({
      run: {
        ns: "demo",
        worker: "shop",
        frozenVersion: "v1",
        workflowName: "orders",
        workflowKey: "wf_abc",
        className: "OrderWorkflow",
        instanceId: "inst-1",
        generation: 1,
        runToken: "run-1",
        event: { payload: { orderId: 123 } },
      },
      scope,
      env: workflowEnv(backend),
      stub: makeStub({
        entrypoints: {
          OrderWorkflow: {
            async run(/** @type {any} */ _event, /** @type {any} */ step) {
              try {
                await step.do("caught", () => Promise.reject(thrown));
              } catch {
                return "tenant recovered";
              }
              return "unreachable";
            },
          },
        },
      }),
    });

    const body = await readJsonResponse(res, 200, String(thrown));
    assert.equal(body.outcome, "failed");
    assert.equal(body.error.message, expectedMessage);
    assert.deepEqual(scope.errors, [thrown]);
  }
});

test("handleWorkflowRunDispatch suspends on step.waitForEvent", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.waitForEvent("approval", { type: "approval", timeout: "5s" });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "suspended");
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-wait",
  ]);
  assert.equal(backend.calls[1].body.stepName, "approval");
  assert.deepEqual(backend.calls[1].body.config, {
    type: "waitForEvent",
    eventType: "approval",
    timeoutMs: 5000,
  });
  assert.equal(typeof backend.calls[1].body.dueAtMs, "number");
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch returns completed step.waitForEvent payload", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) {
      return Response.json({ state: "complete", output: { message: "approved" } });
    }
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const payload = await step.waitForEvent("approval", { type: "approval" });
            return await step.do("after-event", async () => payload);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.deepEqual(body.output, { message: "approved" });
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-wait",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
});

test("handleWorkflowRunDispatch rechecks cached waiting step.waitForEvent records", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) {
      return Response.json({ state: "complete", output: { message: "buffered" } });
    }
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => ({
      steps: [{
        ordinal: 0,
        name: "approval",
        nameCount: 1,
        dependencies: [],
        config: '{"eventType":"approval","timeoutMs":null,"type":"waitForEvent"}',
        status: "waiting",
        attempt: 1,
        dueAtMs: null,
      }],
      nextOrdinal: body.startOrdinal + 1,
      done: true,
    }),
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            const payload = await step.waitForEvent("approval", { type: "approval" });
            return await step.do("after-event", async () => payload);
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.deepEqual(body.output, { message: "buffered" });
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-wait",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
});

test("handleWorkflowRunDispatch suspends on step.sleep", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await step.sleep("settle", "2 seconds");
            return "done";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "suspended");
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-sleep",
  ]);
  assert.equal(backend.calls[1].body.stepName, "settle");
  assert.deepEqual(backend.calls[1].body.config, { type: "sleep", durationMs: 2000 });
  assert.equal(typeof backend.calls[1].body.dueAtMs, "number");
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch resumes when step.sleep is complete", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "complete" });
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await step.sleepUntil("settle", 4102444800000);
            return await step.do("after", async () => "ok");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "ok");
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-sleep",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
  assert.deepEqual(backend.calls[1].body.config, {
    type: "sleepUntil",
    dueAtMs: 4102444800000,
  });
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch rejects swallowed step.sleep suspension", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.sleep("settle", 1000);
            } catch {
              return "swallowed";
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /returned after a step suspension/);
  assert.equal(scope.errors.length, 1);
});

test("handleWorkflowRunDispatch rejects step calls after swallowed suspension before they commit", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const res = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-1",
      generation: 1,
      runToken: "run-1",
      event: { payload: { orderId: 123 } },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.sleep("settle", 1000);
            } catch {}
            return await step.do("dirty-after-suspension", async () => "must-not-run");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.name, "workflow_invalid_step");
  assert.match(body.error.message, /after a suspension/);
  assert.deepEqual(
    backend.calls.map((call) => call.url),
    [
      "http://workflows/internal/workflows/replay-steps",
      "http://workflows/internal/workflows/register-sleep",
    ]
  );
  assert.equal(scope.errors.length, 1);
});
