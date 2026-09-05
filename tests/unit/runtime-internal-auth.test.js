import { test } from "node:test";
import assert from "node:assert/strict";

import {
  applyModuleReplacements,
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

const workerEntrypointUrl = moduleDataUrl(`
export class WorkerEntrypoint {
  constructor() {
    this.env = /** @type {Record<string, unknown>} */ (globalThis).__runtimeInternalEnv;
    this.ctx = { waitUntil() {} };
  }
}
`);
const runtimeStateUrl = moduleDataUrl(`
export function bindRuntime() {
  return {
    serviceName: "runtime-internal",
    metrics: { renderPrometheus() { return "# HELP runtime_internal_test_metric\\n"; } },
    log() {},
    requestScope() {
      return {
        requestId: "rid-runtime-internal",
        setRoute() {},
        respond(response) { return response; },
        markError() {},
        complete() {},
      };
    },
  };
}
export function evictSiblings() { return Promise.resolve(); }
export function recordLoadedWorker() {}
export function runtimeServiceAllowsNamespace() { return true; }
`);
const aiCapacityUrl = moduleDataUrl(`
export function prepareAiCapacityMetrics(env) {
  globalThis.__runtimeInternalPrepareAiMetrics.push(env);
}
`);
const kvCapacityUrl = moduleDataUrl(`
export function prepareKvReadCapacityMetrics(env) {
  globalThis.__runtimeInternalPrepareKvMetrics.push(env);
}
`);
const workflowReplayCacheUrl = moduleDataUrl(`
export function prepareWorkflowReplayCacheMetrics() {
  globalThis.__runtimeInternalPrepareWorkflowMetrics += 1;
}
`);
const runtimeDispatchUrl = moduleDataUrl(`
export async function handleQueuedDispatch() { throw new Error("unexpected queued dispatch"); }
export async function handleScheduledDispatch() { throw new Error("unexpected scheduled dispatch"); }
export async function handleWorkflowNotifyDispatch() { throw new Error("unexpected workflow notify dispatch"); }
export async function handleWorkflowRunDispatch() { throw new Error("unexpected workflow run dispatch"); }
export async function readWorkflowNotifyDispatch() { throw new Error("unexpected workflow notify body read"); }
export async function readWorkflowRunDispatch() { throw new Error("unexpected workflow run body read"); }
`);
const runtimeLoadUrl = moduleDataUrl(`
export function getLoadedWorkerStub() {
  throw new Error("unexpected worker load");
}
`);
const IMPORT_STUBS = {
  "cloudflare:workers": workerEntrypointUrl,
  "shared-worker-id": repositoryFileUrl("shared/worker-id.js"),
  "shared-respond": repositoryFileUrl("shared/respond.js"),
  "shared-internal-auth": sharedInternalAuthUrl(),
  "runtime-dispatch": runtimeDispatchUrl,
  "runtime-dispatch-workflow-replay-cache": workflowReplayCacheUrl,
  "runtime-bindings-ai-capacity": aiCapacityUrl,
  "runtime-bindings-kv-capacity": kvCapacityUrl,
  "runtime-load": runtimeLoadUrl,
  "runtime-state": runtimeStateUrl,
};

const src = applyModuleReplacements(
  readRepositoryFile("runtime/internal.js"),
  importSpecifierReplacements(IMPORT_STUBS)
);

const { default: RuntimeInternal } = await import(moduleDataUrl(src));

function runtimeInternal() {
  /** @type {Record<string, unknown>} */ (globalThis).__runtimeInternalEnv = {
    WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN,
    LOADER: { get() { throw new Error("unexpected loader access"); } },
  };
  /** @type {any} */ (globalThis).__runtimeInternalPrepareAiMetrics = [];
  /** @type {any} */ (globalThis).__runtimeInternalPrepareKvMetrics = [];
  /** @type {any} */ (globalThis).__runtimeInternalPrepareWorkflowMetrics = 0;
  return new RuntimeInternal();
}

test("runtime internal publishes derived gauges immediately before metrics render", async () => {
  const runtime = runtimeInternal();
  const response = await runtime.fetch(new Request("https://runtime.internal/_metrics"));

  assert.equal(response.status, 200);
  assert.equal(await response.text(), "# HELP runtime_internal_test_metric\n");
  assert.deepEqual(
    /** @type {any} */ (globalThis).__runtimeInternalPrepareAiMetrics,
    [/** @type {any} */ (globalThis).__runtimeInternalEnv]
  );
  assert.deepEqual(
    /** @type {any} */ (globalThis).__runtimeInternalPrepareKvMetrics,
    [/** @type {any} */ (globalThis).__runtimeInternalEnv]
  );
  assert.equal(/** @type {any} */ (globalThis).__runtimeInternalPrepareWorkflowMetrics, 1);
});

test("runtime internal rejects private dispatch without valid internal auth token", async () => {
  const runtime = runtimeInternal();

  for (const headers of [new Headers(), new Headers({ "x-wdl-internal-auth": "wrong" })]) {
    headers.set("x-worker-id", "demo:worker:v1");
    const response = await runtime.fetch(new Request("https://runtime.internal/_scheduled", {
      method: "POST",
      headers,
      body: "{}",
    }));

    await assertJsonResponse(response, 401, {
      error: "internal_auth_failed",
      message: "Internal authentication failed",
    });
  }
});
