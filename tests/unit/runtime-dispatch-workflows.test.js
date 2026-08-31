import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { parseJsonText } from "../helpers/json-payload.js";
import { loadRuntimeDispatch } from "../helpers/load-runtime-dispatch.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
  readRepositoryJson,
} from "../helpers/load-shared-module.js";
import {
  jsonRequest,
  makeCtx,
  makeScope,
  makeStub,
  makeWorkflowBackend,
} from "../helpers/runtime-dispatch-fixtures.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { delay, settlementWithin } from "../helpers/timing.js";
import { WORKFLOW_INFRASTRUCTURE_REPORTER_PROP } from "../../runtime/load/module-rewrite.js";

const {
  runtimeDispatch,
  runtimeDispatchWorkflowReplayCache,
  runtimeDispatchWorkflowStep,
  runtimeInfrastructureError,
} = await loadRuntimeDispatch();

/** @param {{ props?: Record<string, unknown> } | undefined} options */
function workflowInfrastructureReporter(options) {
  const reporter = /** @type {{ fetch?: unknown } | undefined} */ (
    options?.props?.[WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]
  );
  assert.ok(reporter && typeof reporter.fetch === "function");
  const reporterFetch = /** @type {(request: Request) => unknown} */ (reporter.fetch);
  return {
    /** @param {string} code */
    report(code) {
      reporterFetch.call(
        reporter,
        new Request(
          `${runtimeInfrastructureError.WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN}/${code}`
        )
      );
    },
  };
}
const workflowJson = await importRepositoryModule(
  "runtime/dispatch/workflow-json.js",
  importSpecifierReplacements({
    "shared-utf8": repositoryFileUrl("shared/utf8.js"),
  })
);
const workflowLimits = /** @type {{
 *   resultBytesMax: number,
 *   backendRequestBytesMax: number,
 *   jsonContainerDepthMax: number,
 *   rejectLoneSurrogates: boolean,
 *   payloadTooLargeCode: string,
 * }} */ (
  readRepositoryJson("tests/fixtures/workflow-limits.json")
);
const workflowRuntimeResponse = /** @type {{
 *   runtimeOutcomes: { completed: string, failed: string, suspended: string },
 *   terminalPayloadFields: { completed: string, failed: string },
 *   retryableBackendErrors: Record<string, { code: string, status: number }>,
 * }} */ (
  readRepositoryJson("tests/fixtures/workflow-runtime-response.json")
);
const workflowStepResponse = /** @type {{
 *   stepKinds: { do: string, sleep: string, sleepUntil: string, waitForEvent: string },
 *   claimTerminalVariants: Record<string, { state: string, payloadField: string }>,
 *   registerWaitTerminalVariants: Record<string, { state: string, payloadField: string }>,
 *   replayTerminalVariants: Record<string, { status: string, payloadField: string }>,
 * }} */ (
  readRepositoryJson("tests/fixtures/workflow-step-response.json")
);
const internalAuthContract = /** @type {{
 *   failure: { status: number, error: string, message: string },
 * }} */ (
  readRepositoryJson("tests/fixtures/internal-auth-contract.json")
);
const {
  handleWorkflowNotifyDispatch,
  handleWorkflowRunDispatch: handleWorkflowRunDispatchRaw,
  readWorkflowNotifyDispatch,
  readWorkflowRunDispatch,
} = runtimeDispatch;

/** @param {any} args */
function handleWorkflowRunDispatch(args) {
  return handleWorkflowRunDispatchRaw({
    ...args,
    ctx: args.ctx ?? makeCtx(runtimeDispatch.WorkflowInfrastructureReporter),
    run: {
      dispatchDeadlineMs: Date.now() + 60_000,
      ...args.run,
    },
  });
}
const {
  MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN,
  MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN,
  WORKFLOW_BACKEND_RESPONSE_MAX_BYTES,
  workflowError,
} = runtimeDispatchWorkflowStep;
const {
  _resetWorkflowReplayCacheForTest,
  getWorkflowReplayCache,
  WORKFLOW_REPLAY_CACHE_MAX_INSTANCES,
} = runtimeDispatchWorkflowReplayCache;
const {
  stringifyWorkflowJson,
  workflowBackendBody,
} = workflowJson;

/** @param {unknown} value @param {number} [maxBytes] */
function stringifyWorkflowJsonForTest(value, maxBytes) {
  return stringifyWorkflowJson(value, "test value", maxBytes);
}

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

/** @param {unknown} backend */
function workflowEnv(backend) {
  return { WORKFLOWS_BACKEND: backend, WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
}

beforeEach(() => {
  _resetWorkflowReplayCacheForTest();
});

/** @param {number} depth */
function nestedArrays(depth) {
  let value = null;
  for (let i = 0; i < depth; i += 1) value = [value];
  return value;
}

test("workflow payload limits match the shared Rust/JS contract", () => {
  assert.equal(workflowJson.WORKFLOW_RESULT_BYTES_MAX, workflowLimits.resultBytesMax);
  assert.equal(
    workflowJson.WORKFLOW_BACKEND_REQUEST_BYTES_MAX,
    workflowLimits.backendRequestBytesMax
  );
  assert.equal(
    workflowJson.WORKFLOW_JSON_CONTAINER_DEPTH_MAX,
    workflowLimits.jsonContainerDepthMax
  );
  assert.equal(workflowLimits.rejectLoneSurrogates, true);
  assert.equal(
    workflowJson.WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
    workflowLimits.payloadTooLargeCode
  );
  assert.throws(
    () => stringifyWorkflowJsonForTest("x", 0),
    (err) => err instanceof Error && err.name === workflowLimits.payloadTooLargeCode
  );
});

test("workflow step kinds match the shared Rust/JS contract", () => {
  assert.deepEqual(runtimeDispatchWorkflowStep.WORKFLOW_STEP_KINDS, workflowStepResponse.stepKinds);
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
    assert.equal(stringifyWorkflowJsonForTest(value), JSON.stringify(value));
  }
});

test("workflow bounded JSON serializer rejects circular values and BigInt like JSON.stringify", () => {
  /** @type {any} */
  const circular = {};
  circular.self = circular;
  /** @type {any} */
  const numberToBigInt = new Number(3);
  numberToBigInt[Symbol.toPrimitive] = () => 4n;
  assert.throws(() => stringifyWorkflowJsonForTest(circular), /circular/i);
  assert.throws(() => stringifyWorkflowJsonForTest(1n), /BigInt/);
  assert.throws(() => stringifyWorkflowJsonForTest(Object(1n)), /BigInt/);
  assert.throws(() => stringifyWorkflowJsonForTest(numberToBigInt), /BigInt|Cannot convert/);
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
  assert.equal(stringifyWorkflowJsonForTest(value, maxBytes), JSON.stringify(value));
  assert.throws(
    () => stringifyWorkflowJsonForTest(value, maxBytes - 1),
    /workflow_payload_too_large/
  );
});

test("workflow JSON writer matches the Rust Unicode and nesting domain", () => {
  for (const value of ["\ud800", "\udc00", { "\ud800": null }, { "\udc00": null }]) {
    assert.throws(
      () => stringifyWorkflowJsonForTest(value),
      /unpaired UTF-16 surrogate/
    );
  }

  assert.equal(
    stringifyWorkflowJsonForTest(nestedArrays(workflowLimits.jsonContainerDepthMax)),
    JSON.stringify(nestedArrays(workflowLimits.jsonContainerDepthMax))
  );
  assert.throws(
    () => stringifyWorkflowJsonForTest(
      nestedArrays(workflowLimits.jsonContainerDepthMax + 1)
    ),
    /JSON nesting limit/
  );

  const tenantDepthMax = workflowLimits.jsonContainerDepthMax - 1;
  assert.doesNotThrow(() => workflowBackendBody("claim-step", {
    config: nestedArrays(tenantDepthMax),
  }));
  assert.doesNotThrow(() => workflowJson.stringifyWorkflowResult(
    nestedArrays(tenantDepthMax),
    "output"
  ));
  assert.throws(
    () => workflowBackendBody("claim-step", {
      config: nestedArrays(tenantDepthMax + 1),
    }),
    /JSON nesting limit/
  );
  assert.throws(
    () => workflowJson.stringifyWorkflowResult(
      nestedArrays(tenantDepthMax + 1),
      "output"
    ),
    /JSON nesting limit/
  );
});

test("workflow backend body serializer enforces per-field result caps in one pass", () => {
  const output = {
    toJSON() {
      return "x".repeat(1024 * 1024 + 1);
    },
  };
  assert.throws(
    () => workflowBackendBody("commit-step-success", {
      ns: "demo",
      output,
    }),
    /Workflow step output exceeds the 1048576 byte limit/
  );
});

test("workflow step success captures output JSON during the backend serialization pass", () => {
  const text = "\"}],\\\n";
  let reads = 0;
  // The getter instruments one serializer traversal; it is not a cross-JSRPC value contract.
  const output = {
    get text() {
      reads += 1;
      return text;
    },
    nested: [{ value: "a,b" }],
  };
  const expectedOutput = JSON.stringify({ text, nested: [{ value: "a,b" }] });
  const serialized = workflowJson.workflowStepSuccessBackendBody({
    ns: "demo",
    output,
  });

  assert.equal(serialized.outputJson, expectedOutput);
  assert.equal(serialized.bodyJson, `{"ns":"demo","output":${expectedOutput}}`);
  assert.equal(reads, 1);
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
    dispatchDeadlineMs: 2_000_000_000_000,
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
    dispatchDeadlineMs: 2_000_000_000_000,
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
    dispatchDeadlineMs: 2_000_000_000_000,
    params,
  }));

  assert.equal(parsed.response, undefined);
  assert.equal(parsed.body.event.payload.length, 1024 * 1024 - 2);
  assert.equal(parsed.body.dispatchDeadlineMs, 2_000_000_000_000);
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
  assert.equal(
    Object.hasOwn(body, workflowRuntimeResponse.terminalPayloadFields.completed),
    true
  );
  assert.deepEqual({
    outcome: body.outcome,
    output: body.output,
  }, {
    outcome: workflowRuntimeResponse.runtimeOutcomes.completed,
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

test("handleWorkflowRunDispatch rejects an expired deadline before tenant dispatch", async () => {
  let getEntrypointCalls = 0;
  let runCalls = 0;
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_expired-before-run",
      className: "OrderWorkflow",
      instanceId: "inst-expired-before-run",
      generation: 1,
      runToken: "run-1",
      dispatchDeadlineMs: Date.now() - 1,
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          run() {
            runCalls += 1;
            return "must not run";
          },
        },
      },
    }, {
      onGetEntrypoint() {
        getEntrypointCalls += 1;
      },
    }),
  });

  assert.equal(
    (await readJsonResponse(response, 503)).error,
    "workflow_backend_unavailable"
  );
  assert.equal(getEntrypointCalls, 0);
  assert.equal(runCalls, 0);
});

test("handleWorkflowRunDispatch rechecks the deadline immediately before tenant run", async () => {
  let now = Date.now();
  const deadlineMs = now + 1_000;
  let runCalls = 0;
  await withMockedProperty(Date, "now", () => now, async () => {
    const response = await handleWorkflowRunDispatch({
      run: {
        ns: "demo",
        worker: "shop",
        frozenVersion: "v1",
        workflowName: "orders",
        workflowKey: "wf_expired-after-entrypoint",
        className: "OrderWorkflow",
        instanceId: "inst-expired-after-entrypoint",
        generation: 1,
        runToken: "run-1",
        dispatchDeadlineMs: deadlineMs,
        event: { payload: {} },
      },
      scope: makeScope(),
      env: workflowEnv(null),
      stub: makeStub({
        entrypoints: {
          OrderWorkflow: {
            run() {
              runCalls += 1;
              return "must not run";
            },
          },
        },
      }, {
        onGetEntrypoint() {
          now = deadlineMs + 1;
        },
      }),
    });

    assert.equal(
      (await readJsonResponse(response, 503)).error,
      "workflow_backend_unavailable"
    );
  });
  assert.equal(runCalls, 0);
});

test("handleWorkflowRunDispatch rejects terminal outcomes settled after the deadline", async (t) => {
  for (const outcome of ["completed", "failed", "suspended"]) {
    await t.test(outcome, async () => {
      let now = 1_000;
      const deadlineMs = 2_000;
      const backend = outcome === "suspended"
        ? makeWorkflowBackend(async (url) => {
          if (url.endsWith("/register-wait")) return Response.json({ state: "waiting" });
          return Response.json(
            { error: "unexpected", message: "unexpected backend call" },
            { status: 500 }
          );
        })
        : null;
      await withMockedProperty(Date, "now", () => now, async () => {
        const response = await handleWorkflowRunDispatch({
          run: {
            ns: "demo",
            worker: "shop",
            frozenVersion: "v1",
            workflowName: "orders",
            workflowKey: `wf_expired-${outcome}`,
            className: "OrderWorkflow",
            instanceId: `inst-expired-${outcome}`,
            generation: 1,
            runToken: "run-1",
            dispatchDeadlineMs: deadlineMs,
            event: { payload: {} },
          },
          scope: makeScope(),
          env: workflowEnv(backend),
          stub: makeStub({
            entrypoints: {
              OrderWorkflow: {
                async run(/** @type {any} */ _event, /** @type {any} */ step) {
                  if (outcome === "suspended") {
                    try {
                      return await step.waitForEvent("approval", { type: "approval" });
                    } finally {
                      now = deadlineMs + 1;
                    }
                  }
                  now = deadlineMs + 1;
                  if (outcome === "failed") throw new Error("tenant failure");
                  return "tenant output";
                },
              },
            },
          }),
        });

        assert.equal(
          (await readJsonResponse(response, 503)).error,
          "workflow_backend_unavailable"
        );
      });
    });
  }
});

test("handleWorkflowRunDispatch bounds a root run Promise at the absolute deadline", async () => {
  const scope = makeScope();
  scope.requestId = "rid-hung-root";
  const lateRun = Promise.withResolvers();
  /** @type {Array<{ report(code: string): Promise<void> | void }>} */
  const issuedReporters = [];
  const dispatch = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_hung-root",
      className: "OrderWorkflow",
      instanceId: "inst-hung-root",
      generation: 1,
      runToken: "run-1",
      dispatchDeadlineMs: Date.now() + 100,
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          run() { return lateRun.promise; },
        },
      },
    }, {
      onGetEntrypoint(options) {
        issuedReporters.push(workflowInfrastructureReporter(options));
      },
    }),
  });

  const settled = await settlementWithin(dispatch, 1000);
  assert.equal(settled.status, "fulfilled");
  if (settled.status !== "fulfilled") throw new Error("workflow dispatch did not settle");
  assert.equal(
    (await readJsonResponse(settled.value, 503)).error,
    "workflow_backend_unavailable"
  );
  assert.equal(scope.errors.length, 1);
  const staleReporter = issuedReporters[0];
  assert.ok(staleReporter);
  assert.throws(
    () => staleReporter.report(
      runtimeInfrastructureError.KV_READ_INFRASTRUCTURE_ERROR_CODE
    ),
    /closed or invalid/
  );
  lateRun.reject(new Error("late tenant rejection"));
  await delay(0);
});

test("handleWorkflowRunDispatch rechecks the deadline after terminal response construction", async (t) => {
  for (const outcome of ["completed", "failed"]) {
    await t.test(outcome, async () => {
      let now = 1_000;
      const deadlineMs = 2_000;
      const failed = {};
      Object.defineProperty(failed, "name", { value: "Error" });
      Object.defineProperty(failed, "message", {
        get() {
          now = deadlineMs + 1;
          return "tenant failure";
        },
      });
      const completed = {
        toJSON() {
          now = deadlineMs + 1;
          return ["tenant output"];
        },
      };
      await withMockedProperty(Date, "now", () => now, async () => {
        const response = await handleWorkflowRunDispatch({
          run: {
            ns: "demo",
            worker: "shop",
            frozenVersion: "v1",
            workflowName: "orders",
            workflowKey: `wf_terminal-build-${outcome}`,
            className: "OrderWorkflow",
            instanceId: `inst-terminal-build-${outcome}`,
            generation: 1,
            runToken: "run-1",
            dispatchDeadlineMs: deadlineMs,
            event: { payload: {} },
          },
          scope: makeScope(),
          env: workflowEnv(null),
          stub: makeStub({
            entrypoints: {
              OrderWorkflow: {
                async run() {
                  if (outcome === "failed") throw failed;
                  return completed;
                },
              },
            },
          }),
        });

        assert.equal(
          (await readJsonResponse(response, 503)).error,
          "workflow_backend_unavailable"
        );
      });
    });
  }
});

test("handleWorkflowRunDispatch returns retryable transport errors for backend infrastructure failures", async (t) => {
  const cases = [
    {
      name: "missing backend binding",
      backend: null,
    },
    {
      name: "backend transport rejection",
      backend: makeWorkflowBackend(async () => {
        throw new Error("private-transport-diagnostic");
      }),
    },
    {
      name: "internal auth failure",
      backend: makeWorkflowBackend(async () => Response.json(
        {
          error: internalAuthContract.failure.error,
          message: internalAuthContract.failure.message,
        },
        { status: internalAuthContract.failure.status }
      )),
    },
    ...Object.entries(workflowRuntimeResponse.retryableBackendErrors).map(([
      name,
      { code, status },
    ]) => ({
      name: `${name} backend failure`,
      backend: makeWorkflowBackend(async () => Response.json(
        { error: code, message: `private-${name}-diagnostic` },
        { status }
      )),
    })),
    ...[502, 503, 504].map((status) => ({
      name: `upstream HTTP ${status}`,
      backend: makeWorkflowBackend(async () => Response.json(
        { error: "upstream_failure", message: "private-upstream-diagnostic" },
        { status }
      )),
    })),
    {
      name: "malformed success response",
      backend: makeWorkflowBackend(async () => Response.json([])),
    },
    {
      name: "unknown claim state",
      backend: makeWorkflowBackend(async () => Response.json({ state: "unknown" })),
    },
    {
      name: "completed claim missing output",
      backend: makeWorkflowBackend(async () => Response.json({
        state: workflowStepResponse.claimTerminalVariants.completed.state,
      })),
    },
    {
      name: "failed claim missing error",
      backend: makeWorkflowBackend(async () => Response.json({
        state: workflowStepResponse.claimTerminalVariants.failed.state,
      })),
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.name, async () => {
      const scope = makeScope();
      scope.requestId = "rid-infrastructure";
      /** @type {unknown} */
      let tenantObservedError = null;
      const response = await handleWorkflowRunDispatch({
        run: {
          ns: "demo",
          worker: "shop",
          frozenVersion: "v1",
          workflowName: "orders",
          workflowKey: "wf_abc",
          className: "OrderWorkflow",
          instanceId: `inst-${testCase.name}`,
          generation: 1,
          createdAtMs: 12345,
          runToken: "run-1",
          event: { payload: {} },
        },
        scope,
        env: workflowEnv(testCase.backend),
        stub: makeStub({
          entrypoints: {
            OrderWorkflow: {
              async run(/** @type {any} */ _event, /** @type {any} */ step) {
                try {
                  await step.do("charge", async () => "charged");
                } catch (err) {
                  tenantObservedError = err;
                  return "tenant-swallowed-backend-error";
                }
              },
            },
          },
        }),
      });

      const body = await readJsonResponse(response, 503);
      assert.deepEqual(body, {
        request_id: "rid-infrastructure",
        error: "workflow_backend_unavailable",
        message: "Workflow backend is unavailable",
      });
      assert.equal(scope.errors.length, 1);
      assert.equal(/** @type {Error} */ (scope.errors[0]).name, "workflow_backend_unavailable");
      assert.equal(
        /** @type {Error} */ (tenantObservedError).name,
        "workflow_backend_unavailable"
      );
      assert.equal(
        /** @type {Error} */ (tenantObservedError).message,
        "Workflow backend is unavailable"
      );
      assert.equal(
        Object.hasOwn(/** @type {object} */ (tenantObservedError), "cause"),
        false
      );
      for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(
        /** @type {object} */ (tenantObservedError)
      ))) {
        if ("value" in descriptor && typeof descriptor.value === "string") {
          assert.doesNotMatch(descriptor.value, /private-/);
        }
      }
      if (testCase.name === "backend transport rejection") {
        assert.match(
          /** @type {Error} */ (scope.errors[0]).message,
          /private-transport-diagnostic/
        );
      }
      assert.doesNotMatch(JSON.stringify(body), /private-|tenant-swallowed/);
    });
  }
});

test("handleWorkflowRunDispatch falls back to claim for replay records missing terminal payloads", async (t) => {
  const cases = [
    {
      status: workflowStepResponse.replayTerminalVariants.completed.status,
      claim: {
        state: workflowStepResponse.claimTerminalVariants.completed.state,
        output: "authoritative output",
      },
      expected: { outcome: "completed", output: "authoritative output" },
    },
    {
      status: workflowStepResponse.replayTerminalVariants.failed.status,
      claim: {
        state: workflowStepResponse.claimTerminalVariants.failed.state,
        error: { name: "RangeError", message: "authoritative failure" },
      },
      expected: {
        outcome: "failed",
        error: { name: "RangeError", message: "authoritative failure" },
      },
    },
  ];

  for (const testCase of cases) {
    await t.test(testCase.status, async () => {
      const scope = makeScope();
      let callbackCalls = 0;
      const backend = makeWorkflowBackend(async (url) => {
        if (url.endsWith("/claim-step")) return Response.json(testCase.claim);
        return Response.json(
          { error: "unexpected", message: "unexpected backend call" },
          { status: 500 }
        );
      }, {
        replayPage: (body) => ({
          steps: [{
            ordinal: body.startOrdinal,
            name: "charge",
            nameCount: 1,
            dependencies: [],
            kind: workflowStepResponse.stepKinds.do,
            config: "null",
            status: testCase.status,
            attempt: 1,
          }],
          nextOrdinal: body.startOrdinal + 1,
          done: true,
        }),
      });
      const response = await handleWorkflowRunDispatch({
        run: {
          ns: "demo",
          worker: "shop",
          frozenVersion: "v1",
          workflowName: "orders",
          workflowKey: "wf_abc",
          className: "OrderWorkflow",
          instanceId: `inst-replay-missing-${testCase.status}`,
          generation: 1,
          createdAtMs: 12345,
          runToken: "run-1",
          event: { payload: {} },
        },
        scope,
        env: workflowEnv(backend),
        stub: makeStub({
          entrypoints: {
            OrderWorkflow: {
              async run(/** @type {any} */ _event, /** @type {any} */ step) {
                return await step.do("charge", async () => {
                  callbackCalls += 1;
                  return "unexpected callback";
                });
              },
            },
          },
        }),
      });

      const body = await readJsonResponse(response, 200);
      assert.deepEqual(
        testCase.status === "completed"
          ? { outcome: body.outcome, output: body.output }
          : { outcome: body.outcome, error: body.error },
        testCase.expected
      );
      assert.equal(callbackCalls, 0);
      assert.deepEqual(backend.calls.map((call) => call.url), [
        "http://workflows/internal/workflows/replay-steps",
        "http://workflows/internal/workflows/claim-step",
      ]);
      if (testCase.status === "completed") {
        assert.deepEqual(scope.errors, []);
      } else {
        assert.equal(scope.errors.length, 1);
        assert.equal(/** @type {Error} */ (scope.errors[0]).name, "RangeError");
        assert.equal(/** @type {Error} */ (scope.errors[0]).message, "authoritative failure");
      }
    });
  }
});

test("handleWorkflowRunDispatch preserves explicit null step payloads", async (t) => {
  for (const source of ["claim", "replay"]) {
    await t.test(source, async () => {
      const scope = makeScope();
      let callbackCalls = 0;
      const backend = makeWorkflowBackend(async (url) => {
        if (url.endsWith("/claim-step")) {
          return Response.json({
            state: workflowStepResponse.claimTerminalVariants.completed.state,
            output: null,
          });
        }
        return Response.json(
          { error: "unexpected", message: "unexpected backend call" },
          { status: 500 }
        );
      }, {
        replayPage: (body) => source === "replay"
          ? {
              steps: [{
                ordinal: body.startOrdinal,
                name: "charge",
                nameCount: 1,
                dependencies: [],
                kind: workflowStepResponse.stepKinds.do,
                config: "null",
                status: workflowStepResponse.replayTerminalVariants.completed.status,
                attempt: 1,
                output: null,
              }],
              nextOrdinal: body.startOrdinal + 1,
              done: true,
            }
          : { steps: [], nextOrdinal: body.startOrdinal, done: true },
      });
      const response = await handleWorkflowRunDispatch({
        run: {
          ns: "demo",
          worker: "shop",
          frozenVersion: "v1",
          workflowName: "orders",
          workflowKey: "wf_abc",
          className: "OrderWorkflow",
          instanceId: `inst-explicit-null-${source}`,
          generation: 1,
          createdAtMs: 12345,
          runToken: "run-1",
          event: { payload: {} },
        },
        scope,
        env: workflowEnv(backend),
        stub: makeStub({
          entrypoints: {
            OrderWorkflow: {
              async run(/** @type {any} */ _event, /** @type {any} */ step) {
                return await step.do("charge", async () => {
                  callbackCalls += 1;
                  return "unexpected callback";
                });
              },
            },
          },
        }),
      });

      const body = await readJsonResponse(response, 200);
      assert.equal(body.outcome, "completed");
      assert.equal(Object.hasOwn(body, "output"), true);
      assert.equal(body.output, null);
      assert.equal(callbackCalls, 0);
      assert.deepEqual(scope.errors, []);
    });
  }
});

test("handleWorkflowRunDispatch keeps deterministic backend state errors terminal", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async () => Response.json(
    {
      error: "workflow_invalid_state",
      message: "Workflow service request failed",
    },
    { status: 500 }
  ));

  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-invalid-state",
      generation: 1,
      createdAtMs: 12345,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", async () => "charged");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(
    Object.hasOwn(body, workflowRuntimeResponse.terminalPayloadFields.failed),
    true
  );
  assert.equal(body.outcome, workflowRuntimeResponse.runtimeOutcomes.failed);
  assert.deepEqual(body.error, {
    name: "workflow_invalid_state",
    message: "Workflow service request failed",
  });
});

test("handleWorkflowRunDispatch bounds a stalled authoritative backend body", async () => {
  let bodyReadStarted = false;
  const stalled = new ReadableStream({
    pull() {
      bodyReadStarted = true;
      return new Promise(() => {});
    },
    cancel() { return new Promise(() => {}); },
  });
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return new Response(stalled);
    throw new Error(`unexpected backend call: ${url}`);
  });
  const outcome = await settlementWithin(handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_backend-body-deadline",
      className: "OrderWorkflow",
      instanceId: "inst-backend-body-deadline",
      generation: 1,
      runToken: "run-1",
      dispatchDeadlineMs: Date.now() + 200,
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          /** @param {unknown} _event @param {any} step */
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("stalled", async () => "unused");
          },
        },
      },
    }),
  }), 1000);

  assert.equal(outcome.status, "fulfilled");
  if (outcome.status !== "fulfilled") return;
  assert.equal(bodyReadStarted, true);
  assert.equal(
    (await readJsonResponse(outcome.value, 503)).error,
    "workflow_backend_unavailable"
  );
});

test("handleWorkflowRunDispatch rejects an authoritative ACK parsed after its deadline", async () => {
  let now = Date.now();
  const deadlineMs = now + 1_000;
  let callbackRuns = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) {
      return new Response('{"state":"run","attempt":1}');
    }
    throw new Error(`unexpected backend call: ${url}`);
  });
  const intrinsicJsonParse = JSON.parse;
  await withMockedProperty(Date, "now", () => now, () => withMockedProperty(
    JSON,
    "parse",
    (text, reviver) => {
      const parsed = intrinsicJsonParse(text, reviver);
      if (text === '{"state":"run","attempt":1}') now = deadlineMs + 1;
      return parsed;
    },
    async () => {
      const response = await handleWorkflowRunDispatch({
        run: {
          ns: "demo",
          worker: "shop",
          frozenVersion: "v1",
          workflowName: "orders",
          workflowKey: "wf_backend-parse-deadline",
          className: "OrderWorkflow",
          instanceId: "inst-backend-parse-deadline",
          generation: 1,
          runToken: "run-1",
          dispatchDeadlineMs: deadlineMs,
          event: { payload: {} },
        },
        scope: makeScope(),
        env: workflowEnv(backend),
        stub: makeStub({
          entrypoints: {
            OrderWorkflow: {
              /** @param {unknown} _event @param {any} step */
              async run(_event, step) {
                return await step.do("late-ack", async () => {
                  callbackRuns += 1;
                  return "unused";
                });
              },
            },
          },
        }),
      });

      assert.equal(
        (await readJsonResponse(response, 503)).error,
        "workflow_backend_unavailable"
      );
      assert.equal(callbackRuns, 0);
    }
  ));
});

test("handleWorkflowRunDispatch rejects an identity response length mismatch", async () => {
  const payload = '{"state":"run","attempt":1}';
  let callbackRuns = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) {
      return new Response(payload, {
        headers: {
          "content-length": String(new TextEncoder().encode(payload).byteLength + 1),
        },
      });
    }
    throw new Error(`unexpected backend call: ${url}`);
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_backend-length-mismatch",
      className: "OrderWorkflow",
      instanceId: "inst-backend-length-mismatch",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          /** @param {unknown} _event @param {any} step */
          async run(_event, step) {
            return await step.do("length-mismatch", async () => {
              callbackRuns += 1;
              return "unused";
            });
          },
        },
      },
    }),
  });

  assert.equal(
    (await readJsonResponse(response, 503)).error,
    "workflow_backend_unavailable"
  );
  assert.equal(callbackRuns, 0);
});

test("handleWorkflowRunDispatch keeps compressed response lengths on the chunk fallback", async () => {
  const payload = '{"state":"run","attempt":1}';
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) {
      return new Response(payload, {
        headers: {
          "content-encoding": "gzip",
          "content-length": String(new TextEncoder().encode(payload).byteLength + 1),
        },
      });
    }
    if (url.endsWith("/commit-step-success")) {
      return Response.json({ state: "complete" });
    }
    throw new Error(`unexpected backend call: ${url}`);
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_backend-compressed-length",
      className: "OrderWorkflow",
      instanceId: "inst-backend-compressed-length",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          /** @param {unknown} _event @param {any} step */
          async run(_event, step) {
            return await step.do("compressed-length", async () => "ok");
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "ok");
});

test("handleWorkflowRunDispatch rejects oversized authoritative backend bodies", async () => {
  assert.equal(WORKFLOW_BACKEND_RESPONSE_MAX_BYTES, 32 * 1024 * 1024);
  let cancelled = false;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) {
      return new Response(new ReadableStream({
        cancel() { cancelled = true; },
      }), {
        headers: {
          "content-length": String(WORKFLOW_BACKEND_RESPONSE_MAX_BYTES + 1),
        },
      });
    }
    throw new Error(`unexpected backend call: ${url}`);
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_backend-body-limit",
      className: "OrderWorkflow",
      instanceId: "inst-backend-body-limit",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          /** @param {unknown} _event @param {any} step */
          async run(_event, step) {
            return await step.do("oversized", async () => "unused");
          },
        },
      },
    }),
  });

  assert.equal((await readJsonResponse(response, 503)).error, "workflow_backend_unavailable");
  await Promise.resolve();
  assert.equal(cancelled, true);
});

test("handleWorkflowRunDispatch does not commit a lost step-success acknowledgement as failure", async () => {
  const scope = makeScope();
  scope.requestId = "rid-lost-commit";
  let callbackCalls = 0;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) {
      return Response.json(
        { error: "redis_error", message: "private-commit-diagnostic" },
        { status: 500 }
      );
    }
    throw new Error(`unexpected backend call ${url}`);
  });

  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-lost-commit",
      generation: 1,
      createdAtMs: 12345,
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
              return await step.do("charge", async () => {
                callbackCalls += 1;
                return "charged";
              });
            } catch {
              return "tenant-swallowed-backend-error";
            }
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "workflow_backend_unavailable");
  assert.equal(body.message, "Workflow backend is unavailable");
  assert.equal(callbackCalls, 1);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
  assert.equal(
    backend.calls.some((call) => call.url.endsWith("/commit-step-error")),
    false
  );
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

test("handleWorkflowRunDispatch normalizes undefined step output before commit", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
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
      instanceId: "inst-step-undefined",
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
            await step.do("empty", async () => undefined);
            return "done";
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "done");
  assert.equal(
    backend.calls.find((c) => c.url.endsWith("/commit-step-success"))?.body.output,
    null
  );
  assert.deepEqual(scope.errors, []);
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
        kind: workflowStepResponse.stepKinds.do,
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

test("handleWorkflowRunDispatch does not replay a completed sleep as step.do", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  const config = { type: "sleep", durationMs: 1000 };
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    return Response.json(
      { error: "unexpected", message: "unexpected backend call" },
      { status: 500 }
    );
  }, {
    replayPage: (body) => ({
      steps: [{
        ordinal: body.startOrdinal,
        name: "charge",
        nameCount: 1,
        dependencies: [],
        kind: workflowStepResponse.stepKinds.sleep,
        config: '{"durationMs":1000,"type":"sleep"}',
        status: "completed",
        attempt: 1,
        output: null,
      }],
      nextOrdinal: body.startOrdinal + 1,
      done: true,
    }),
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-kind-mismatch",
      generation: 1,
      createdAtMs: 12345,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("charge", config, async () => {
              callbackCalls += 1;
              return "callback output";
            });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, "callback output");
  assert.equal(callbackCalls, 1);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/claim-step",
    "http://workflows/internal/workflows/commit-step-success",
  ]);
});

test("handleWorkflowRunDispatch keeps an evicted in-flight replay page locally usable", async () => {
  const scope = makeScope();
  const stepCount = 40;
  let callbackCalls = 0;
  let appliedCachePressure = false;
  const run = {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-evicted-replay",
    generation: 1,
    createdAtMs: 12345,
    runToken: "run-1",
    event: { payload: {} },
  };
  const controllerCache = getWorkflowReplayCache(run);
  const replaySteps = Array.from({ length: stepCount }, (_, ordinal) => ({
    ordinal,
    name: `step-${ordinal}`,
    nameCount: 1,
    dependencies: ordinal === 0 ? [] : [ordinal - 1],
    kind: workflowStepResponse.stepKinds.do,
    config: "null",
    status: "completed",
    output: ordinal,
  }));
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "complete", output: "claimed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => {
      if (!appliedCachePressure) {
        appliedCachePressure = true;
        for (let i = 0; i < WORKFLOW_REPLAY_CACHE_MAX_INSTANCES; i += 1) {
          getWorkflowReplayCache({
            ns: "pressure",
            workflowKey: "wf_pressure",
            instanceId: `inst-${i}`,
            generation: 1,
            createdAtMs: i,
            runToken: "pressure",
          });
        }
      }
      return {
        steps: replaySteps.slice(body.startOrdinal),
        nextOrdinal: stepCount,
        done: true,
      };
    },
  });
  const res = await handleWorkflowRunDispatch({
    run,
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            let output = null;
            for (let ordinal = 0; ordinal < stepCount; ordinal += 1) {
              output = await step.do(`step-${ordinal}`, async () => {
                callbackCalls += 1;
                return "fresh";
              });
            }
            return output;
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual({ outcome: body.outcome, output: body.output }, {
    outcome: "completed",
    output: stepCount - 1,
  });
  assert.equal(callbackCalls, 0);
  assert.equal(backend.calls.filter((call) => call.url.endsWith("/replay-steps")).length, 1);
  assert.equal(backend.calls.filter((call) => call.url.endsWith("/claim-step")).length, 0);
  assert.equal(controllerCache.steps.size, 0);
  assert.equal(controllerCache.bytes, 0);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch reuses replay pages across run claims", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  /** @type {number[]} */
  const replayStarts = [];
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step") && body.ordinal === 2) {
      return Response.json({ state: "run", attempt: 1 });
    }
    if (url.endsWith("/commit-step-success") && body.ordinal === 2) {
      return Response.json({ state: "complete" });
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  }, {
    replayPage: (body) => {
      replayStarts.push(body.startOrdinal);
      return {
        steps: [{
          ordinal: body.startOrdinal,
          name: body.startOrdinal === 0 ? "first" : "second",
          nameCount: 1,
          dependencies: body.startOrdinal === 0 ? [] : [0],
          kind: workflowStepResponse.stepKinds.do,
          config: "null",
          status: "completed",
          attempt: 1,
          output: body.startOrdinal,
        }],
        nextOrdinal: body.startOrdinal + 1,
        done: true,
      };
    },
  });
  const baseRun = {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-resume-cache",
    generation: 1,
    createdAtMs: 12345,
    event: { payload: {} },
  };
  const first = await handleWorkflowRunDispatch({
    run: { ...baseRun, runToken: "run-1" },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            return await step.do("first", async () => {
              callbackCalls += 1;
              return "fresh-first";
            });
          },
        },
      },
    }),
  });
  const second = await handleWorkflowRunDispatch({
    run: { ...baseRun, runToken: "run-2" },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await step.do("first", async () => {
              callbackCalls += 1;
              return "fresh-first";
            });
            await step.do("second", async () => {
              callbackCalls += 1;
              return "fresh-second";
            });
            return await step.do("third", async () => {
              callbackCalls += 1;
              return "fresh-third";
            });
          },
        },
      },
    }),
  });

  assert.equal((await readJsonResponse(first, 200)).output, 0);
  assert.equal((await readJsonResponse(second, 200)).output, "fresh-third");
  assert.equal(callbackCalls, 1);
  assert.deepEqual(replayStarts, [0, 1]);
  assert.deepEqual(scope.errors, []);
});

test("handleWorkflowRunDispatch isolates cached outputs from tenant mutation across run claims", async () => {
  const scope = makeScope();
  let callbackCalls = 0;
  let dispatchCount = 0;
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-success")) {
      assert.deepEqual(body.output, {
        nested: { value: 1 },
        items: ["persisted"],
      });
      return Response.json({ state: "complete" });
    }
    if (url.endsWith("/register-sleep")) return Response.json({ state: "waiting" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const baseRun = {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-mutable-cache",
    generation: 1,
    createdAtMs: 12345,
    event: { payload: {} },
  };
  const stub = makeStub({
    entrypoints: {
      OrderWorkflow: {
        async run(/** @type {any} */ _event, /** @type {any} */ step) {
          const output = /** @type {{ nested: { value: number }, items: string[] }} */ (
            await step.do("mutable", async () => {
              callbackCalls += 1;
              return {
                nested: { value: 1 },
                items: ["persisted"],
              };
            })
          );
          dispatchCount += 1;
          if (dispatchCount === 1) {
            output.nested.value = 999;
            output.items.push("mutated");
            await step.sleep("pause", 1000);
          }
          return output;
        },
      },
    },
  });

  const first = await handleWorkflowRunDispatch({
    run: { ...baseRun, runToken: "run-1" },
    scope,
    env: workflowEnv(backend),
    stub,
  });
  assert.equal((await readJsonResponse(first, 200)).outcome, "suspended");
  const callsAfterFirstClaim = backend.calls.length;

  const second = await handleWorkflowRunDispatch({
    run: { ...baseRun, runToken: "run-2" },
    scope,
    env: workflowEnv(backend),
    stub,
  });

  assert.deepEqual((await readJsonResponse(second, 200)).output, {
    nested: { value: 1 },
    items: ["persisted"],
  });
  assert.equal(callbackCalls, 1);
  assert.equal(backend.calls.length, callsAfterFirstClaim);
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
        kind: workflowStepResponse.stepKinds.do,
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
        kind: workflowStepResponse.stepKinds.do,
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
            kind: workflowStepResponse.stepKinds.do,
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
            kind: workflowStepResponse.stepKinds.do,
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

test("handleWorkflowRunDispatch falls back when replay pagination does not advance", async (t) => {
  for (const nextOrdinal of [0, -1]) {
    await t.test(`nextOrdinal ${nextOrdinal}`, async () => {
      const backend = makeWorkflowBackend(async (url, request) => {
        if (url.endsWith("/claim-step")) {
          return request.runToken === "run-first"
            ? Response.json(
                { error: "redis_error", message: "temporary claim failure" },
                { status: 503 }
              )
            : Response.json({ state: "complete", output: "authoritative" });
        }
        return Response.json(
          { error: "unexpected", message: "unexpected backend call" },
          { status: 500 }
        );
      }, {
        replayPage: (body) => {
          if (body.runToken !== "run-first") {
            return Response.json(
              { error: "redis_error", message: "temporary replay failure" },
              { status: 503 }
            );
          }
          return {
            steps: ["rejected-current", "rejected-later"].map((output, index) => ({
              ordinal: body.startOrdinal + index,
              name: index === 0 ? "charge" : "later",
              nameCount: 1,
              dependencies: [],
              kind: workflowStepResponse.stepKinds.do,
              config: "null",
              status: "completed",
              attempt: 1,
              output,
            })),
            nextOrdinal,
            done: false,
          };
        },
      });
      const dispatch = (
        /** @type {string} */ runToken,
        /** @type {any} */ scope
      ) =>
        handleWorkflowRunDispatch({
          run: {
            ns: "demo",
            worker: "shop",
            frozenVersion: "v1",
            workflowName: "orders",
            workflowKey: "wf_abc",
            className: "OrderWorkflow",
            instanceId: `inst-no-progress-${nextOrdinal}`,
            generation: 1,
            createdAtMs: 12345,
            runToken,
            event: { payload: {} },
          },
          scope,
          env: workflowEnv(backend),
          stub: makeStub({
            entrypoints: {
              OrderWorkflow: {
                async run(/** @type {any} */ _event, /** @type {any} */ step) {
                  return await step.do("charge", async () => "callback");
                },
              },
            },
          }),
        });

      await readJsonResponse(await dispatch("run-first", makeScope()), 503);
      const secondScope = makeScope();
      const body = await readJsonResponse(await dispatch("run-second", secondScope), 200);
      assert.deepEqual({ outcome: body.outcome, output: body.output }, {
        outcome: "completed",
        output: "authoritative",
      });
      assert.deepEqual(backend.calls.map((call) => call.url), [
        "http://workflows/internal/workflows/replay-steps",
        "http://workflows/internal/workflows/claim-step",
        "http://workflows/internal/workflows/replay-steps",
        "http://workflows/internal/workflows/claim-step",
      ]);
      assert.deepEqual(secondScope.errors, []);
    });
  }
});

test("overlapping Workflow replay controllers do not move the shared cursor backward", async () => {
  const firstStarted = Promise.withResolvers();
  const releaseFirst = Promise.withResolvers();
  const firstPage = {
    steps: [{
      ordinal: 0,
      name: "charge",
      nameCount: 1,
      dependencies: [],
      kind: workflowStepResponse.stepKinds.do,
      config: "null",
      status: "completed",
      attempt: 1,
      output: "first",
    }],
    nextOrdinal: 1,
    done: false,
  };
  const delayedFirstPage = new Response(new ReadableStream({
    start(controller) {
      firstStarted.resolve(undefined);
      void releaseFirst.promise.then(() => {
        controller.enqueue(new TextEncoder().encode(JSON.stringify(firstPage)));
        controller.close();
      });
    },
  }), { headers: { "content-type": "application/json" } });
  const firstBackend = makeWorkflowBackend(async () =>
    Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 }), {
    replayPage: () => delayedFirstPage,
  });
  const secondBackend = makeWorkflowBackend(async () =>
    Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 }), {
    replayPage: () => ({
      steps: ["second-current", "second-later"].map((output, ordinal) => ({
        ordinal,
        name: ordinal === 0 ? "charge" : "later",
        nameCount: 1,
        dependencies: [],
        kind: workflowStepResponse.stepKinds.do,
        config: "null",
        status: "completed",
        attempt: 1,
        output,
      })),
      nextOrdinal: 2,
      done: false,
    }),
  });
  const baseRun = {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "orders",
    workflowKey: "wf_abc",
    className: "OrderWorkflow",
    instanceId: "inst-overlapping-replay-cursor",
    generation: 1,
    createdAtMs: 12345,
    event: { payload: {} },
  };
  const dispatch = (
    /** @type {string} */ runToken,
    /** @type {any} */ backend
  ) =>
    handleWorkflowRunDispatch({
      run: { ...baseRun, runToken },
      scope: makeScope(),
      env: workflowEnv(backend),
      stub: makeStub({
        entrypoints: {
          OrderWorkflow: {
            async run(/** @type {any} */ _event, /** @type {any} */ step) {
              return await step.do("charge", async () => "callback");
            },
          },
        },
      }),
    });

  const firstDispatch = dispatch("run-first", firstBackend);
  await firstStarted.promise;
  const secondBody = await readJsonResponse(
    await dispatch("run-second", secondBackend),
    200
  );
  assert.equal(secondBody.output, "second-current");
  releaseFirst.resolve(undefined);
  const firstBody = await readJsonResponse(await firstDispatch, 200);
  assert.equal(firstBody.output, "first");

  const cache = getWorkflowReplayCache({ ...baseRun, runToken: "run-second" });
  assert.equal(cache.nextOrdinal, 2);
  assert.equal(firstBackend.calls.length, 1);
  assert.equal(secondBackend.calls.length, 1);
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
            kind: workflowStepResponse.stepKinds.do,
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
  const slow = Promise.withResolvers();
  const callbackStarted = Promise.withResolvers();
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
              callbackStarted.resolve(undefined);
              await slow.promise;
              return "a";
            });
            await callbackStarted.promise;
            try {
              await step.do("b", async () => "b");
            } finally {
              slow.resolve(undefined);
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

test("handleWorkflowRunDispatch lets a slow Promise.all sibling settle within the dispatch deadline", async () => {
  const scope = makeScope();
  const slowStarted = Promise.withResolvers();
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) {
      assert.equal(body.stepName, "terminal");
      return Response.json({ state: "failed" });
    }
    if (url.endsWith("/commit-step-success")) {
      assert.equal(body.stepName, "slow");
      return Response.json({ state: "complete" });
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const dispatch = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-slow-sibling",
      generation: 1,
      runToken: "run-1",
      dispatchDeadlineMs: Date.now() + 3000,
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            await Promise.all([
              step.do("terminal", async () => {
                await slowStarted.promise;
                throw new TypeError("fatal");
              }),
              step.do("slow", async () => {
                slowStarted.resolve(undefined);
                await delay(1100);
                return "slow result";
              }),
            ]);
          },
        },
      },
    }),
  });

  assert.equal((await settlementWithin(dispatch, 1000)).status, "pending");
  const body = await readJsonResponse(await dispatch, 200);
  assert.equal(body.outcome, "failed");
  assert.deepEqual(body.error, { name: "TypeError", message: "fatal" });
  assert.equal(
    backend.calls.some((call) => call.url.endsWith("/commit-step-success") && call.body.stepName === "slow"),
    true
  );
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
  const slowCommit = Promise.withResolvers();
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) {
      if (body.stepName === "slow") await slowCommit.promise;
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
              slowCommit.resolve(undefined);
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
            kind: workflowStepResponse.stepKinds.do,
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
  const slow = Promise.withResolvers();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    if (url.endsWith("/commit-step-success")) return Response.json({ state: "complete" });
    if (url.endsWith("/commit-step-error")) return Response.json({ state: "failed" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const dispatch = handleWorkflowRunDispatch({
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
              await slow.promise;
              return "late";
            });
            await delay(0);
            throw new Error("run failed");
          },
        },
      },
    }),
  });

  assert.equal((await settlementWithin(dispatch, 10)).status, "pending");
  slow.resolve(undefined);
  const body = await readJsonResponse(await dispatch, 200);
  assert.equal(body.outcome, "failed");
  assert.equal(body.error.message, "run failed");
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-success")), false);
  assert.equal(backend.calls.some((call) => call.url.endsWith("/commit-step-error")), false);
});

test("handleWorkflowRunDispatch returns result-unknown when the sender deadline expires", async () => {
  const scope = makeScope();
  scope.requestId = "rid-hung-sibling";
  const callbackStarted = Promise.withResolvers();
  const neverSettles = new Promise(() => {});
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run" });
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  await delay(200);
  const dispatchDeadlineMs = Date.now() + 1300;
  const dispatch = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-hung-sibling",
      generation: 1,
      runToken: "run-1",
      dispatchDeadlineMs,
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            step.do("hang", async () => {
              callbackStarted.resolve(undefined);
              await neverSettles;
            });
            await callbackStarted.promise;
            throw new Error("run failed");
          },
        },
      },
    }),
  });

  assert.equal((await settlementWithin(dispatch, 10)).status, "pending");
  const settled = await settlementWithin(dispatch, 750);
  assert.equal(settled.status, "fulfilled");
  if (settled.status !== "fulfilled") throw new Error("workflow dispatch did not settle");
  const body = await readJsonResponse(settled.value, 503);
  assert.deepEqual(body, {
    error: "workflow_backend_unavailable",
    message: "Workflow backend is unavailable",
    request_id: scope.requestId,
  });
  assert.equal(scope.errors.length, 1);
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
  const sleepRegistered = Promise.withResolvers();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-sleep")) {
      sleepRegistered.resolve(undefined);
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
            await sleepRegistered.promise;
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

test("workflowError preserves throwable names and messages", async () => {
  const forged = new Error("not internal");
  forged.name = "workflow_invalid_step";
  assert.deepEqual(workflowError(forged), {
    name: "workflow_invalid_step",
    message: "not internal",
  });
});

test("handleWorkflowRunDispatch does not trust a forged infrastructure error name", async () => {
  const scope = makeScope();
  const forged = new Error("tenant failure");
  forged.name = workflowRuntimeResponse.retryableBackendErrors.unavailable.code;
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-forged-infrastructure",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run() {
            throw forged;
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.outcome, workflowRuntimeResponse.runtimeOutcomes.failed);
  assert.deepEqual(body.error, {
    name: workflowRuntimeResponse.retryableBackendErrors.unavailable.code,
    message: "tenant failure",
  });
  assert.deepEqual(scope.errors, [forged]);
});

test("handleWorkflowRunDispatch retries when the run boundary reports a KV failure", async () => {
  const scope = makeScope();
  /** @type {{ report(code: string): Promise<void> | void } | null} */
  let reporter = null;
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_reported-run-failure",
      className: "OrderWorkflow",
      instanceId: "inst-reported-run-failure",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope,
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run() {
            await reporter?.report(
              runtimeInfrastructureError.KV_READ_INFRASTRUCTURE_ERROR_CODE
            );
            throw new Error("KV read failed");
          },
        },
      },
    }, {
      onGetEntrypoint(options) {
        reporter = workflowInfrastructureReporter(options);
      },
    }),
  });

  assert.equal((await readJsonResponse(response, 503)).error, "workflow_backend_unavailable");
  assert.equal(scope.errors.length, 1);
  assert.equal(
    /** @type {Error} */ (scope.errors[0]).message,
    "Runtime KV infrastructure failure escaped tenant boundary"
  );
});

test("handleWorkflowRunDispatch does not commit a step error after its callback reports a KV failure", async () => {
  /** @type {{ report(code: string): Promise<void> | void } | null} */
  let reporter = null;
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) {
      throw new Error("reported KV failure must not become a durable step error");
    }
    throw new Error(`unexpected backend call: ${url}`);
  });
  const response = await handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_reported-step-failure",
      className: "OrderWorkflow",
      instanceId: "inst-reported-step-failure",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(backend),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run(/** @type {any} */ _event, /** @type {any} */ step) {
            try {
              await step.do("read", async () => {
                await reporter?.report(
                  runtimeInfrastructureError.KV_READ_INFRASTRUCTURE_ERROR_CODE
                );
                throw new Error("KV read failed");
              });
            } catch {
              return "outer fallback";
            }
          },
        },
      },
    }, {
      onGetEntrypoint(options) {
        reporter = workflowInfrastructureReporter(options);
      },
    }),
  });

  assert.equal((await readJsonResponse(response, 503)).error, "workflow_backend_unavailable");
  assert.equal(
    backend.calls.some((call) => call.url.endsWith("/commit-step-error")),
    false
  );
});

test("handleWorkflowRunDispatch keeps infrastructure reporters isolated per dispatch", async () => {
  /** @type {{ report(code: string): Promise<void> | void } | null} */
  let failingReporter = null;
  /** @type {Array<{ report(code: string): Promise<void> | void }>} */
  const issuedReporters = [];
  const failing = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_reporter-isolation",
      className: "OrderWorkflow",
      instanceId: "inst-reporter-failing",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run() {
            await failingReporter?.report(
              runtimeInfrastructureError.KV_READ_INFRASTRUCTURE_ERROR_CODE
            );
            throw new Error("KV read failed");
          },
        },
      },
    }, {
      onGetEntrypoint(options) {
        const reporter = workflowInfrastructureReporter(options);
        failingReporter = reporter;
        issuedReporters.push(reporter);
      },
    }),
  });
  const healthy = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_reporter-isolation",
      className: "OrderWorkflow",
      instanceId: "inst-reporter-healthy",
      generation: 1,
      runToken: "run-1",
      event: { payload: {} },
    },
    scope: makeScope(),
    env: workflowEnv(null),
    stub: makeStub({
      entrypoints: {
        OrderWorkflow: {
          async run() { return "healthy"; },
        },
      },
    }),
  });

  const [failedResponse, healthyResponse] = await Promise.all([failing, healthy]);
  assert.equal((await readJsonResponse(failedResponse, 503)).error, "workflow_backend_unavailable");
  const healthyBody = await readJsonResponse(healthyResponse, 200);
  assert.equal(healthyBody.outcome, "completed");
  assert.equal(healthyBody.output, "healthy");
  assert.equal(typeof healthyBody.duration_ms, "number");
  const staleReporter = issuedReporters[0];
  assert.ok(staleReporter);
  assert.throws(
    () => staleReporter.report(
      runtimeInfrastructureError.KV_READ_INFRASTRUCTURE_ERROR_CODE
    ),
    /closed or invalid/
  );
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
  assert.equal(body.outcome, workflowRuntimeResponse.runtimeOutcomes.suspended);
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
        kind: workflowStepResponse.stepKinds.do,
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

test("handleWorkflowRunDispatch lets a late fan-out infrastructure failure override terminal failure", async () => {
  const scope = makeScope();
  scope.requestId = "rid-parallel-infrastructure";
  const terminalCommitted = Promise.withResolvers();
  const successCommitStarted = Promise.withResolvers();
  const releaseSuccessCommit = Promise.withResolvers();
  let sideEffects = 0;
  const backend = makeWorkflowBackend(async (url, body) => {
    if (url.endsWith("/claim-step")) return Response.json({ state: "run", attempt: 1 });
    if (url.endsWith("/commit-step-error")) {
      assert.equal(body.stepName, "terminal");
      terminalCommitted.resolve(undefined);
      return Response.json({ state: "failed" });
    }
    if (url.endsWith("/commit-step-success")) {
      assert.equal(body.stepName, "side-effect");
      successCommitStarted.resolve(undefined);
      await releaseSuccessCommit.promise;
      return Response.json(
        { error: "redis_error", message: "private commit diagnostic" },
        { status: 500 }
      );
    }
    return Response.json({ error: "unexpected", message: "unexpected backend call" }, { status: 500 });
  });
  const dispatch = handleWorkflowRunDispatch({
    run: {
      ns: "demo",
      worker: "shop",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "wf_abc",
      className: "OrderWorkflow",
      instanceId: "inst-parallel-late-infrastructure",
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
              step.do("terminal", async () => {
                throw new TypeError("fatal");
              }),
              step.do("side-effect", async () => {
                sideEffects += 1;
                return "committed externally";
              }),
            ]);
          },
        },
      },
    }),
  });

  await Promise.all([terminalCommitted.promise, successCommitStarted.promise]);
  assert.equal(sideEffects, 1);
  assert.equal((await settlementWithin(dispatch, 10)).status, "pending");
  releaseSuccessCommit.resolve(undefined);

  const body = await readJsonResponse(await dispatch, 503);
  assert.deepEqual(body, {
    error: "workflow_backend_unavailable",
    message: "Workflow backend is unavailable",
    request_id: scope.requestId,
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

test("handleWorkflowRunDispatch rejects completed step.waitForEvent without output", async () => {
  const scope = makeScope();
  scope.requestId = "rid-register-wait-output";
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) {
      return Response.json({
        state: workflowStepResponse.registerWaitTerminalVariants.completed.state,
      });
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
      instanceId: "inst-missing-wait-output",
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
              return await step.waitForEvent("approval", { type: "approval" });
            } catch {
              return "tenant-swallowed-register-wait-error";
            }
          },
        },
      },
    }),
  });

  assert.deepEqual(await readJsonResponse(res, 503), {
    request_id: "rid-register-wait-output",
    error: "workflow_backend_unavailable",
    message: "Workflow backend is unavailable",
  });
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-wait",
  ]);
  assert.equal(/** @type {Error} */ (scope.errors[0]).name, "workflow_backend_unavailable");
});

test("handleWorkflowRunDispatch preserves explicit null step.waitForEvent output", async () => {
  const scope = makeScope();
  const backend = makeWorkflowBackend(async (url) => {
    if (url.endsWith("/register-wait")) {
      return Response.json({
        state: workflowStepResponse.registerWaitTerminalVariants.completed.state,
        output: null,
      });
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
      instanceId: "inst-null-wait-output",
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
            return await step.waitForEvent("approval", { type: "approval" });
          },
        },
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "completed");
  assert.equal(body.output, null);
  assert.deepEqual(backend.calls.map((call) => call.url), [
    "http://workflows/internal/workflows/replay-steps",
    "http://workflows/internal/workflows/register-wait",
  ]);
  assert.deepEqual(scope.errors, []);
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
        kind: workflowStepResponse.stepKinds.waitForEvent,
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
