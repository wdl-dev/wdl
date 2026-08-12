import { beforeEach, afterEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
} from "../helpers/load-shared-module.js";
import { parseJsonText } from "../helpers/json-payload.js";
import { installConsoleMethodCapture } from "../helpers/output-capture.js";

/** @type {any} */ (globalThis).__doStateGaugeSamples = [];
/** @type {any} */ (globalThis).__doStateMetricSamples = [];

const observabilityUrl = moduleDataUrl(`
export function createLogger() { return function log() {}; }
export function logStructured(service, level, event, fields = {}) {
  /** @type {any} */ (globalThis).__doStateErrorLogs.push({ service, level, event, ...fields });
}
`);

const runtimeMetricsUrl = moduleDataUrl(`
export const metrics = {
  setGauge(name, labels, value) {
    /** @type {any} */ (globalThis).__doStateGaugeSamples.push({ name, labels, value });
  },
  increment(name, labels) {
    /** @type {any} */ (globalThis).__doStateMetricSamples.push({ type: "increment", name, labels });
  },
  observe(name, labels, value) {
    /** @type {any} */ (globalThis).__doStateMetricSamples.push({ type: "observe", name, labels, value });
  },
};
`);

const src = applyModuleReplacements(readRepositoryFile("do-runtime/state.js"), [
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "runtime-metrics";/, `from ${JSON.stringify(runtimeMetricsUrl)};`],
]);

const {
  beginInFlightDispatch,
  currentInFlightDispatches,
  endInFlightDispatch,
  recordDoInvoke,
  recordDoWebSocketUpgrade,
  setDraining,
  waitForInFlightDispatches,
} = await import(moduleDataUrl(src));

let restoreConsoleError = () => {};

beforeEach(() => {
  restoreConsoleError();
  restoreConsoleError = () => {};
  setDraining(false);
  while (currentInFlightDispatches() > 0) endInFlightDispatch();
  /** @type {any} */ (globalThis).__doStateGaugeSamples = [];
  /** @type {any} */ (globalThis).__doStateMetricSamples = [];
  /** @type {any} */ (globalThis).__doStateErrorLogs = [];
  restoreConsoleError = installConsoleMethodCapture(
    "error",
    /** @type {any[]} */ (/** @type {any} */ (globalThis).__doStateErrorLogs),
    (line) => parseJsonText(String(line), "DO state error log")
  );
});

afterEach(() => {
  restoreConsoleError();
  restoreConsoleError = () => {};
});

test("DO state: in-flight waits resolve after active dispatches finish", async () => {
  assert.equal(beginInFlightDispatch(), true);
  assert.equal(currentInFlightDispatches(), 1);

  const waiting = waitForInFlightDispatches(1000);
  endInFlightDispatch();
  const result = await waiting;

  assert.equal(result.drained, true);
  assert.equal(result.inFlight, 0);
  assert.equal(currentInFlightDispatches(), 0);
  assert.deepEqual(
    /** @type {any} */ (globalThis).__doStateGaugeSamples.map((/** @type {any} */ sample) => sample.value),
    [1, 0]
  );
});

test("DO state: draining rejects new dispatches and times out while old work is active", async () => {
  assert.equal(beginInFlightDispatch(), true);
  setDraining(true);

  assert.equal(beginInFlightDispatch(), false);
  const result = await waitForInFlightDispatches(1);

  assert.equal(result.drained, false);
  assert.equal(result.inFlight, 1);
  endInFlightDispatch();
});

test("DO state: invoke metrics use low-cardinality kind and outcome labels", async () => {
  const ok = await recordDoInvoke("fetch", async () => new Response("ok"));
  assert.equal(ok.status, 200);
  const clientError = await recordDoInvoke("alarm", async () => new Response("bad request", { status: 400 }));
  assert.equal(clientError.status, 400);
  const serverError = await recordDoInvoke("alarm", async () => new Response("fail", { status: 503 }));
  assert.equal(serverError.status, 503);
  const rpcOk = await recordDoInvoke("rpc", async () => new Response("rpc-ok"));
  assert.equal(rpcOk.status, 200);
  // "queue" is intentionally not an allowed invoke kind; unknown kinds normalize to "fetch".
  await assert.rejects(
    recordDoInvoke("queue", async () => {
      throw new Error("boom");
    }),
    /boom/
  );

  assert.deepEqual(/** @type {any} */ (globalThis).__doStateMetricSamples, [
    { type: "increment", name: "do_invokes", labels: { service: "do-runtime", kind: "fetch", outcome: "ok" } },
    { type: "observe", name: "do_invoke_duration_ms", labels: { service: "do-runtime", kind: "fetch", outcome: "ok" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[1].value },
    { type: "increment", name: "do_invokes", labels: { service: "do-runtime", kind: "alarm", outcome: "client_error" } },
    { type: "observe", name: "do_invoke_duration_ms", labels: { service: "do-runtime", kind: "alarm", outcome: "client_error" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[3].value },
    { type: "increment", name: "do_invokes", labels: { service: "do-runtime", kind: "alarm", outcome: "server_error" } },
    { type: "observe", name: "do_invoke_duration_ms", labels: { service: "do-runtime", kind: "alarm", outcome: "server_error" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[5].value },
    { type: "increment", name: "do_invokes", labels: { service: "do-runtime", kind: "rpc", outcome: "ok" } },
    { type: "observe", name: "do_invoke_duration_ms", labels: { service: "do-runtime", kind: "rpc", outcome: "ok" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[7].value },
    // From the "queue" invocation above: unknown kind is normalized to "fetch".
    { type: "increment", name: "do_invokes", labels: { service: "do-runtime", kind: "fetch", outcome: "server_error" } },
    { type: "observe", name: "do_invoke_duration_ms", labels: { service: "do-runtime", kind: "fetch", outcome: "server_error" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[9].value },
  ]);
  for (const sample of /** @type {any} */ (globalThis).__doStateMetricSamples.filter((/** @type {any} */ s) => s.type === "observe")) {
    assert.equal(Number.isFinite(sample.value), true);
  }
});

test("DO state: WebSocket upgrade metrics classify response outcomes", async () => {
  await recordDoWebSocketUpgrade(async () => new Response(null, { status: 204 }));
  await recordDoWebSocketUpgrade(async () => new Response("denied", { status: 403 }));
  await assert.rejects(
    recordDoWebSocketUpgrade(async () => {
      throw new Error("network");
    }),
    /network/
  );

  assert.deepEqual(/** @type {any} */ (globalThis).__doStateMetricSamples, [
    { type: "increment", name: "do_websocket_upgrades", labels: { service: "do-runtime", outcome: "ok" } },
    { type: "observe", name: "do_websocket_upgrade_duration_ms", labels: { service: "do-runtime", outcome: "ok" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[1].value },
    { type: "increment", name: "do_websocket_upgrades", labels: { service: "do-runtime", outcome: "client_error" } },
    { type: "observe", name: "do_websocket_upgrade_duration_ms", labels: { service: "do-runtime", outcome: "client_error" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[3].value },
    { type: "increment", name: "do_websocket_upgrades", labels: { service: "do-runtime", outcome: "server_error" } },
    { type: "observe", name: "do_websocket_upgrade_duration_ms", labels: { service: "do-runtime", outcome: "server_error" }, value: /** @type {any} */ (globalThis).__doStateMetricSamples[5].value },
  ]);
});
