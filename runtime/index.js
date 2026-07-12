// No invalidation protocol on runtime: x-worker-id is immutable, so a
// new version = new cache key = natural miss that re-pulls the bundle.

import { WorkerEntrypoint } from "cloudflare:workers";
import {
  internalErrorResponse,
  jsonError,
} from "shared-respond";
import { stripInternalAuthHeader } from "shared-internal-auth";
import { parseDispatchWorkerId } from "shared-worker-id";
import {
  handleFetchDispatch,
} from "runtime-dispatch";
import { getLoadedWorkerStub } from "runtime-load";
import {
  bindRuntime,
  runtimeServiceAllowsNamespace,
} from "runtime-state";

export { default as internal } from "runtime-internal";

// Capability adapters re-exported here so `ctx.exports.<Name>` sees them
// at load time; adding a new binding type = one more export line.
export { KV } from "runtime-bindings-kv";
export { Assets } from "runtime-bindings-assets";
export { ServiceBinding } from "runtime-bindings-service";
export { QueueProducer } from "runtime-bindings-queue";
export { D1Database } from "runtime-bindings-d1";
export { R2Bucket } from "runtime-bindings-r2";
export { DurableObjectNamespace } from "runtime-bindings-do";
export { InternalAuthBackend } from "runtime-bindings-internal-auth-backend";

export default class Runtime extends WorkerEntrypoint {
  /** @param {Request} request */
  async fetch(request) {
    const env = this.env;
    const ctx = this.ctx;
    const runtime = bindRuntime(env);
    const workerId = request.headers.get("x-worker-id");
    /** @type {string | null} */
    let namespace = null;
    /** @type {string | null} */
    let workerName = null;
    /** @type {string | null} */
    let version = null;
    const scope = runtime.requestScope(request, {
      route: "worker_fetch",
      extras: () => ({ namespace, worker: workerName, version }),
    });

    try {
      if (!workerId) {
        return scope.respond(jsonError(400, "missing_worker_id", "Missing x-worker-id header"));
      }

      const parsed = parseDispatchWorkerId(workerId);
      if (!parsed) {
        return scope.respond(jsonError(400, "malformed_worker_id", `Malformed x-worker-id "${workerId}"`));
      }
      ({ namespace, worker: workerName, version } = parsed);
      if (!runtimeServiceAllowsNamespace(runtime.serviceName, namespace)) {
        return scope.respond(jsonError(403, "runtime_pool_mismatch", "Worker namespace is not allowed in this runtime pool"));
      }

      // Gateway-routed traffic is the authoritative "this version is active"
      // signal. Service-binding cold-loads use the same helper without eviction.
      const { stub } = getLoadedWorkerStub({
        requestId: scope.requestId, env, ctx,
        ns: namespace, worker: workerName, version, workerId,
        metrics: runtime.metrics, log: runtime.log,
        evictOnLoad: true,
      });

      const forwardRequest = new Request(request);
      stripInternalAuthHeader(forwardRequest.headers);
      forwardRequest.headers.set("x-request-id", scope.requestId);
      return await handleFetchDispatch({
        request: forwardRequest, stub, scope, env, ctx,
        identity: {
          namespace,
          workerName,
          workerId,
          requestId: scope.requestId,
        },
      });
    } catch (err) {
      scope.markError(err);
      return scope.respond(internalErrorResponse(502, "runtime_error", "Runtime error", scope.requestId));
    } finally {
      scope.complete();
    }
  }
}
