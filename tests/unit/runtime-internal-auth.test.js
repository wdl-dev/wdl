import { test } from "node:test";

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
    metrics: null,
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
const emptyBindingUrl = moduleDataUrl(`
export class KV {}
export class Assets {}
export class ServiceBinding {}
export class QueueProducer {}
export class D1Database {}
export class R2Bucket {}
export class DurableObjectNamespace {}
export class InternalAuthBackend {}
`);

const IMPORT_STUBS = {
  "cloudflare:workers": workerEntrypointUrl,
  "shared-worker-id": repositoryFileUrl("shared/worker-id.js"),
  "shared-respond": repositoryFileUrl("shared/respond.js"),
  "shared-internal-auth": sharedInternalAuthUrl(),
  "runtime-dispatch": runtimeDispatchUrl,
  "runtime-load": runtimeLoadUrl,
  "runtime-state": runtimeStateUrl,
  "runtime-bindings-kv": emptyBindingUrl,
  "runtime-bindings-assets": emptyBindingUrl,
  "runtime-bindings-service": emptyBindingUrl,
  "runtime-bindings-queue": emptyBindingUrl,
  "runtime-bindings-d1": emptyBindingUrl,
  "runtime-bindings-r2": emptyBindingUrl,
  "runtime-bindings-do": emptyBindingUrl,
  "runtime-bindings-internal-auth-backend": emptyBindingUrl,
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
  return new RuntimeInternal();
}

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
