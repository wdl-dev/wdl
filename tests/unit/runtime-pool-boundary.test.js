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
    this.env = /** @type {Record<string, unknown>} */ (globalThis).__runtimePoolEnv;
    this.ctx = { waitUntil() {} };
  }
}
`);
const runtimeStateUrl = moduleDataUrl(`
export function bindRuntime(env) {
  return {
    serviceName: typeof env.SERVICE_NAME === "string" ? env.SERVICE_NAME : "runtime",
    metrics: null,
    log() {},
    requestScope() {
      return {
        requestId: "rid-runtime-pool",
        setRoute() {},
        respond(response) { return response; },
        markError() {},
        complete() {},
      };
    },
  };
}
export function runtimeServiceAllowsNamespace(service, namespace) {
  return service === (namespace === "__system__" ? "system-runtime" : "user-runtime");
}
export function evictSiblings() { return Promise.resolve(); }
export function recordLoadedWorker() {}
`);
const runtimeDispatchUrl = moduleDataUrl(`
export async function handleFetchDispatch() { return new Response("unexpected dispatch"); }
export async function handleQueuedDispatch() { return new Response("unexpected queued dispatch"); }
export async function handleScheduledDispatch() { return new Response("unexpected scheduled dispatch"); }
export async function handleWorkflowNotifyDispatch() { return new Response("unexpected workflow notify dispatch"); }
export async function handleWorkflowRunDispatch() { return new Response("workflow run dispatched"); }
export async function readWorkflowNotifyDispatch(request) { return { body: await request.json() }; }
export async function readWorkflowRunDispatch(request) { return { body: await request.json() }; }
`);
const runtimeLoadUrl = moduleDataUrl(`
export function getLoadedWorkerStub({ env, ns, worker, version }) {
  const workerId = \`\${ns}:\${worker}:\${version}\`;
  return { workerId, stub: env.LOADER.get(workerId, async () => ({})) };
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
const emptyInternalUrl = moduleDataUrl(`export default class RuntimeInternal {}`);

const IMPORT_STUBS = {
  "cloudflare:workers": workerEntrypointUrl,
  "shared-worker-id": repositoryFileUrl("shared/worker-id.js"),
  "shared-respond": repositoryFileUrl("shared/respond.js"),
  "shared-internal-auth": sharedInternalAuthUrl(),
  "runtime-dispatch": runtimeDispatchUrl,
  "runtime-load": runtimeLoadUrl,
  "runtime-state": runtimeStateUrl,
  "runtime-internal": emptyInternalUrl,
  "runtime-bindings-kv": emptyBindingUrl,
  "runtime-bindings-assets": emptyBindingUrl,
  "runtime-bindings-service": emptyBindingUrl,
  "runtime-bindings-queue": emptyBindingUrl,
  "runtime-bindings-d1": emptyBindingUrl,
  "runtime-bindings-r2": emptyBindingUrl,
  "runtime-bindings-do": emptyBindingUrl,
  "runtime-bindings-internal-auth-backend": emptyBindingUrl,
};

const runtimeIndexSrc = applyModuleReplacements(
  readRepositoryFile("runtime/index.js"),
  importSpecifierReplacements(IMPORT_STUBS)
);
const runtimeInternalSrc = applyModuleReplacements(
  readRepositoryFile("runtime/internal.js"),
  importSpecifierReplacements(IMPORT_STUBS)
);

const { default: Runtime } = await import(moduleDataUrl(runtimeIndexSrc));
const { default: RuntimeInternal } = await import(moduleDataUrl(runtimeInternalSrc));

/** @param {string} serviceName @param {{ get(id: string, factory: () => Promise<unknown>): unknown }} [loader] */
function setRuntimePoolEnv(serviceName, loader = { get() { throw new Error("unexpected loader access"); } }) {
  /** @type {Record<string, unknown>} */ (globalThis).__runtimePoolEnv = {
    SERVICE_NAME: serviceName,
    WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN,
    LOADER: loader,
  };
}

function authHeaders(extra = {}) {
  return {
    "x-wdl-internal-auth": TEST_INTERNAL_AUTH_TOKEN,
    ...extra,
  };
}

/** @param {Response} response */
async function assertPoolMismatch(response) {
  await assertJsonResponse(response, 403, {
    error: "runtime_pool_mismatch",
    message: "Worker namespace is not allowed in this runtime pool",
  });
}

test("runtime public dispatch rejects __system__ workers on user-runtime", async () => {
  setRuntimePoolEnv("user-runtime");
  const response = await new Runtime().fetch(new Request("https://runtime.test/", {
    headers: { "x-worker-id": "__system__:s3-cleanup:v1" },
  }));

  await assertPoolMismatch(response);
});

test("runtime public dispatch rejects tenant workers on system-runtime", async () => {
  setRuntimePoolEnv("system-runtime");
  const response = await new Runtime().fetch(new Request("https://runtime.test/", {
    headers: { "x-worker-id": "demo:api:v1" },
  }));

  await assertPoolMismatch(response);
});

test("runtime internal dispatch rejects __system__ workers on user-runtime", async () => {
  setRuntimePoolEnv("user-runtime");
  const response = await new RuntimeInternal().fetch(new Request("https://runtime.internal/_scheduled", {
    method: "POST",
    headers: authHeaders({ "x-worker-id": "__system__:s3-cleanup:v1" }),
    body: "{}",
  }));

  await assertPoolMismatch(response);
});

test("runtime workflow dispatch rejects tenant body identities on system-runtime", async () => {
  setRuntimePoolEnv("system-runtime");
  const response = await new RuntimeInternal().fetch(new Request("https://runtime.internal/internal/workflows/run", {
    method: "POST",
    headers: authHeaders({ "content-type": "application/json" }),
    body: JSON.stringify({
      ns: "demo",
      worker: "api",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "orders",
      className: "Orders",
      instanceId: "order-1",
      generation: 1,
      runToken: "run-token",
      createdAtMs: 1,
      event: {},
    }),
  }));

  await assertPoolMismatch(response);
});

test("runtime workflow dispatch allows __platform__ body identities on user-runtime", async () => {
  setRuntimePoolEnv("user-runtime", { get() { return {}; } });
  const response = await new RuntimeInternal().fetch(new Request("https://runtime.internal/internal/workflows/run", {
    method: "POST",
    headers: authHeaders({ "content-type": "application/json" }),
    body: JSON.stringify({
      ns: "__platform__",
      worker: "platform-api",
      frozenVersion: "v1",
      workflowName: "orders",
      workflowKey: "orders",
      className: "Orders",
      instanceId: "order-1",
      generation: 1,
      runToken: "run-token",
      createdAtMs: 1,
      event: {},
    }),
  }));

  assert.equal(response.status, 200);
  assert.equal(await response.text(), "workflow run dispatched");
});
