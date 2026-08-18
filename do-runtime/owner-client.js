import {
  DO_INVOKE_CONTENT_TYPE,
  DO_OWNERSHIP_CODE,
  DoRuntimeError,
  encodeDoInvokeRequest,
} from "do-runtime-protocol";
import { DO_OWNER_HEADERS } from "_wdl-do-scoped-request.js";
import {
  log,
  metrics,
  SERVICE,
} from "do-runtime-state";
import { forwardOwnerRequest } from "shared-owner-forwarder";

/**
 * @typedef {{ ownerKey: string, taskId: string, endpoint?: string | null, generation: number }} DoOwner
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 */

const DO_OWNER_PORT = 8788;

/** @param {DoOwner} owner */
function ownerFence(owner) {
  return {
    ownerKey: owner.ownerKey,
    taskId: owner.taskId,
    generation: owner.generation,
  };
}

/**
 * @param {DoInvoke} invoke
 * @param {Record<string, unknown>} env
 * @param {DoOwner} owner
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 * @param {string} [pathname]
 */
export async function forwardToOwner(invoke, env, owner, requestId = null, hopCount = 0, pathname = "/internal/do/invoke") {
  return await forwardOwnerRequest({
    env,
    endpoint: owner.endpoint,
    endpointPort: DO_OWNER_PORT,
    endpointService: "do-runtime",
    pathname,
    requestId,
    hopCount,
    body: encodeDoInvokeRequest({ ...invoke, owner: ownerFence(owner) }),
    metrics,
    metricName: "do_forwards",
    service: SERVICE,
    log,
    logEvent: "do_forward_complete",
    buildHeaders: (nextHopCount) => ({
      "content-type": DO_INVOKE_CONTENT_TYPE,
      "x-wdl-do-forwarded": "1",
      "x-wdl-do-hop-count": String(nextHopCount),
    }),
    logFields: () => ({
      namespace: "ns" in invoke ? invoke.ns : undefined,
      worker: "worker" in invoke ? invoke.worker : undefined,
      class_name: invoke.className,
      object_name: invoke.objectName,
      owner_task_id: owner.taskId,
      owner_endpoint: owner.endpoint,
      path: pathname,
    }),
    missingEndpointError: () =>
      new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_ENDPOINT_MISSING, `DO scope ${owner.ownerKey} owner has no endpoint`),
    invalidEndpointError: () =>
      new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_UNAVAILABLE, `DO scope ${owner.ownerKey} owner endpoint is invalid`),
    hopExhaustedError: () =>
      new DoRuntimeError(
        503,
        DO_OWNERSHIP_CODE.FORWARD_HOP_EXHAUSTED,
        `DO scope ${owner.ownerKey} exceeded the maximum forward depth for ${pathname}`
      ),
    unavailableError: () => new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_UNAVAILABLE, "DO owner is unavailable"),
  });
}

/**
 * @param {Request} request
 * @param {DoInvoke} invoke
 * @param {Record<string, unknown>} env
 * @param {DoOwner} owner
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 */
export async function forwardConnectToOwner(request, invoke, env, owner, requestId = null, hopCount = 0) {
  return await forwardOwnerRequest({
    env,
    endpoint: owner.endpoint,
    endpointPort: DO_OWNER_PORT,
    endpointService: "do-runtime",
    pathname: "/internal/do/connect",
    method: request.method,
    requestId,
    hopCount,
    metrics,
    metricName: "do_forwards",
    service: SERVICE,
    log,
    logEvent: "do_connect_forward_complete",
    buildHeaders: (nextHopCount) => {
      const headers = new Headers(request.headers);
      headers.set("x-wdl-do-forwarded", "1");
      headers.set("x-wdl-do-hop-count", String(nextHopCount));
      headers.set(DO_OWNER_HEADERS.ownerKey, owner.ownerKey);
      headers.set(DO_OWNER_HEADERS.taskId, owner.taskId);
      headers.set(DO_OWNER_HEADERS.generation, String(owner.generation));
      return headers;
    },
    logFields: () => ({
      namespace: "ns" in invoke ? invoke.ns : undefined,
      worker: "worker" in invoke ? invoke.worker : undefined,
      class_name: invoke.className,
      object_name: invoke.objectName,
      owner_task_id: owner.taskId,
      owner_endpoint: owner.endpoint,
    }),
    missingEndpointError: () =>
      new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_ENDPOINT_MISSING, `DO scope ${owner.ownerKey} owner has no endpoint`),
    invalidEndpointError: () =>
      new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_UNAVAILABLE, `DO scope ${owner.ownerKey} owner endpoint is invalid`),
    hopExhaustedError: () =>
      new DoRuntimeError(
        503,
        DO_OWNERSHIP_CODE.FORWARD_HOP_EXHAUSTED,
        `DO scope ${owner.ownerKey} exceeded the maximum forward depth`
      ),
    unavailableError: () => new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_UNAVAILABLE, "DO owner is unavailable"),
  });
}
