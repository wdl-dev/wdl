// Runtime internal dispatch socket. Public gateway traffic goes to
// runtime/index.js on :8081; scheduler/workflows call this worker on :8088.

import { WorkerEntrypoint } from "cloudflare:workers";
import { parseDispatchWorkerId } from "shared-worker-id";
import {
  internalErrorResponse,
  jsonError,
  jsonResponse,
  prometheusResponse,
} from "shared-respond";
import {
  internalAuthFailureResponse,
  verifyInternalAuthHeaders,
} from "shared-internal-auth";
import {
  handleQueuedDispatch,
  handleScheduledDispatch,
  handleWorkflowNotifyDispatch,
  handleWorkflowRunDispatch,
  readWorkflowNotifyDispatch,
  readWorkflowRunDispatch,
} from "runtime-dispatch";
import { getLoadedWorkerStub } from "runtime-load";
import {
  bindRuntime,
  runtimeServiceAllowsNamespace,
} from "runtime-state";

// Capability adapters re-exported here so `ctx.exports.<Name>` works for
// internal scheduled/queued/workflow dispatch cold-loads too.
export { KV } from "runtime-bindings-kv";
export { Assets } from "runtime-bindings-assets";
export { ServiceBinding } from "runtime-bindings-service";
export { QueueProducer } from "runtime-bindings-queue";
export { D1Database } from "runtime-bindings-d1";
export { R2Bucket } from "runtime-bindings-r2";
export { DurableObjectNamespace } from "runtime-bindings-do";
export { InternalAuthBackend } from "runtime-bindings-internal-auth-backend";

/**
 * @typedef {{ get(id: string, factory: () => Promise<unknown>): import("runtime-dispatch").LoadedWorkerStub }} RuntimeLoader
 * @typedef {{ LOADER: RuntimeLoader, [key: string]: unknown }} RuntimeInternalEnv
 * @typedef {{ metrics: import("runtime-load").RuntimeLoaderMetrics | null | undefined, log: (level: string, event: string, fields?: Record<string, unknown>) => void }} RuntimeBinding
 * @typedef {{ requestId: string }} RuntimeScope
 */

/**
 * @param {{ env: RuntimeInternalEnv, ctx: { waitUntil(promise: Promise<unknown>): void }, runtime: RuntimeBinding, scope: RuntimeScope, namespace: string, workerName: string, version: string, evictOnLoad?: boolean }} opts
 */
function loadStub({ env, ctx, runtime, scope, namespace, workerName, version, evictOnLoad = false }) {
  return getLoadedWorkerStub({
    requestId: scope.requestId, env, ctx,
    ns: namespace, worker: workerName, version,
    metrics: runtime.metrics, log: runtime.log,
    evictOnLoad,
  });
}

/** @param {{ serviceName: string }} runtime @param {string} namespace */
function runtimePoolMismatchResponse(runtime, namespace) {
  return runtimeServiceAllowsNamespace(runtime.serviceName, namespace)
    ? null
    : jsonError(403, "runtime_pool_mismatch", "Worker namespace is not allowed in this runtime pool");
}

export default class RuntimeInternal extends WorkerEntrypoint {
  /** @param {Request} request */
  async fetch(request) {
    const env = this.env;
    const ctx = this.ctx;
    const runtime = bindRuntime(env);
    /** @type {string | null} */
    let namespace = null;
    /** @type {string | null} */
    let workerName = null;
    /** @type {string | null} */
    let version = null;
    const scope = runtime.requestScope(request, {
      route: "runtime_internal",
      extras: () => ({ namespace, worker: workerName, version }),
    });

    try {
      const pathname = new URL(request.url).pathname;

      if (request.method === "GET" && pathname === "/_healthz") {
        scope.setRoute("healthz");
        return scope.respond(jsonResponse(200, {
          ok: true,
          service: runtime.serviceName,
        }));
      }

      if (request.method === "GET" && pathname === "/_metrics") {
        scope.setRoute("metrics");
        return scope.respond(prometheusResponse(runtime.metrics));
      }

      if (!verifyInternalAuthHeaders(request.headers, env)) {
        return scope.respond(internalAuthFailureResponse());
      }

      if (request.method === "POST" && pathname === "/internal/workflows/run") {
        scope.setRoute("workflow_run");
        const parsed = await readWorkflowRunDispatch(request);
        if ("response" in parsed) return scope.respond(parsed.response);
        const run = parsed.body;
        namespace = run.ns;
        workerName = run.worker;
        version = run.frozenVersion;
        const mismatch = runtimePoolMismatchResponse(runtime, namespace);
        if (mismatch) return scope.respond(mismatch);
        const { workerId, stub } = loadStub({ env, ctx, runtime, scope, namespace, workerName, version });
        return await handleWorkflowRunDispatch({
          run, stub, scope, env, ctx,
          identity: {
            namespace,
            workerName,
            workerId,
            requestId: scope.requestId,
          },
        });
      }

      if (request.method === "POST" && pathname === "/internal/workflows/notify") {
        scope.setRoute("workflow_notify");
        const parsed = await readWorkflowNotifyDispatch(request);
        if ("response" in parsed) return scope.respond(parsed.response);
        const notify = parsed.body;
        namespace = notify.ns;
        workerName = notify.worker;
        version = notify.frozenVersion;
        const mismatch = runtimePoolMismatchResponse(runtime, namespace);
        if (mismatch) return scope.respond(mismatch);
        const { stub } = loadStub({ env, ctx, runtime, scope, namespace, workerName, version });
        return await handleWorkflowNotifyDispatch({ notify, stub, scope });
      }

      const workerId = request.headers.get("x-worker-id");
      if (!workerId) {
        return scope.respond(jsonError(400, "missing_worker_id", "Missing x-worker-id header"));
      }

      const parsed = parseDispatchWorkerId(workerId);
      if (!parsed) {
        return scope.respond(jsonError(400, "malformed_worker_id", `Malformed x-worker-id "${workerId}"`));
      }
      ({ namespace, worker: workerName, version } = parsed);
      const mismatch = runtimePoolMismatchResponse(runtime, namespace);
      if (mismatch) return scope.respond(mismatch);

      if (request.method === "POST" && pathname === "/_scheduled") {
        scope.setRoute("scheduled");
        const { stub } = loadStub({
          env, ctx, runtime, scope,
          namespace, workerName, version,
          evictOnLoad: true,
        });
        return await handleScheduledDispatch({
          request, stub, scope, env, ctx,
          identity: {
            namespace,
            workerName,
            workerId,
            requestId: scope.requestId,
          },
        });
      }

      if (request.method === "POST" && pathname === "/_queued") {
        scope.setRoute("queued");
        const { stub } = loadStub({
          env, ctx, runtime, scope,
          namespace, workerName, version,
          evictOnLoad: true,
        });
        return await handleQueuedDispatch({
          request, stub, scope, env, ctx,
          identity: {
            namespace,
            workerName,
            workerId,
            requestId: scope.requestId,
          },
        });
      }

      return scope.respond(jsonError(404, "not_found", "Not found"));
    } catch (err) {
      scope.markError(err);
      return scope.respond(internalErrorResponse(502, "runtime_error", "Runtime error", scope.requestId));
    } finally {
      scope.complete();
    }
  }
}
