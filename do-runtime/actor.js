import { DurableObject } from "cloudflare:workers";
import { loadDoWorkerCode } from "do-runtime-load";
import { objectRegistryMember, rememberDoObject } from "do-runtime-object-registry";
import {
  buildAlarmRequest,
  buildFacetName,
  buildForwardRequest,
  buildRpcRequest,
  DO_OWNERSHIP_ERROR_CONTROL_HEADER,
  DO_OWNERSHIP_CODE,
  doPlatformErrorResponse,
  DoRuntimeError,
  normalizeDoConnectRequest,
  readLocalActorInvokeRequest,
} from "do-runtime-protocol";
import {
  assertCurrentOwnerWithLeaseBudget,
  forgetOwnedScope,
  ownerLeaseGuardMs,
} from "do-runtime-owner-registry";
import {
  beginInFlightDispatch,
  endInFlightDispatch,
  log,
  metrics,
  SERVICE,
} from "do-runtime-state";
import { formatError } from "shared-observability";
import { rebuildResponseWithHeaders } from "shared-respond";

/**
 * @typedef {{ LOADER: { get(key: string, loader: () => Promise<unknown>): DoWorkerStub } }} DoEnv
 * @typedef {{ getDurableObjectClass(className: string, options: { props: Record<string, unknown> }): DurableObjectClass }} DoWorkerStub
 * @typedef {{ fetch(request: Request): Promise<Response> }} DoFacet
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {{ ownerKey: string, hostId?: string, className?: string, ns: string, worker: string, doStorageId: string, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} DoOwner
 */

export class WdlDoHostActor extends DurableObject {
  /** @type {Map<string, DoWorkerStub>} */
  workers;
  /** @type {Set<string>} */
  facetNames;
  /** @type {number} */
  facetHighWater;
  /** @type {Set<string>} */
  registeredObjectMembers;

  /**
   * @param {DurableObjectState} ctx
   * @param {DoEnv} env
   */
  constructor(ctx, env) {
    super(ctx, env);
    this.workers = new Map();
    this.facetNames = new Set();
    this.facetHighWater = 0;
    this.registeredObjectMembers = new Set();
  }

  /**
   * @param {DoInvoke} invoke
   * @param {string | null} requestId
   */
  tenantWorker(invoke, requestId) {
    const existing = this.workers.get(invoke.workerId);
    if (existing) return existing;
    const worker = this.env.LOADER.get(invoke.workerId, () => (
      loadDoWorkerCode(
        this.env,
        this.ctx,
        invoke,
        requestId
      )
    ));
    this.workers.set(invoke.workerId, worker);
    return worker;
  }

  /** @param {DoInvoke} invoke */
  rememberFacet(invoke) {
    const facetName = buildFacetName(invoke);
    const before = this.facetNames.size;
    this.facetNames.add(facetName);
    if (this.facetNames.size !== before) {
      metrics.setGauge("do_host_actor_facet_count", { service: SERVICE }, this.facetNames.size);
      if (this.facetNames.size > this.facetHighWater) {
        this.facetHighWater = this.facetNames.size;
        metrics.setGauge("do_host_actor_facet_high_water", { service: SERVICE }, this.facetHighWater);
      }
    }
    return facetName;
  }

  /**
   * @param {DoInvoke} invoke
   * @returns {Promise<boolean>} whether registry I/O was attempted
   */
  async rememberObject(invoke) {
    if (!("doStorageId" in invoke) || typeof invoke.doStorageId !== "string") return false;
    const member = objectRegistryMember(invoke);
    if (this.registeredObjectMembers.has(member)) return false;
    try {
      await rememberDoObject(this.env, invoke);
    } catch (err) {
      const workerId = "workerId" in invoke ? invoke.workerId : "";
      log("warn", "do_object_registry_remember_failed", {
        member,
        worker_id: workerId,
        ...formatError(err),
      });
      return true;
    }
    this.registeredObjectMembers.add(member);
    metrics.setGauge("do_host_actor_object_registry_size", { service: SERVICE }, this.registeredObjectMembers.size);
    return true;
  }

  /** @param {Request} request */
  async fetch(request) {
    try {
      const url = new URL(request.url);
      if (url.pathname === "/connect") {
        const invoke = /** @type {DoInvoke} */ (normalizeDoConnectRequest(request));
        if (!("request" in invoke)) {
          throw new Error("DO connect request did not normalize to a fetch invoke");
        }
        return await this.dispatchWithFence(invoke, async () => {
          const requestId = request.headers.get("x-request-id") || null;
          const cls = this.tenantWorker(invoke, requestId).getDurableObjectClass(invoke.className, {
            props: invoke.props,
          });
          const facet = this.ctx.facets.get(this.rememberFacet(invoke), () => ({
            class: cls,
            id: invoke.objectName,
          }));
          return await facet.fetch(buildForwardRequest(invoke.request));
        });
      }
      if (url.pathname === "/delete-storage") {
        const invoke = /** @type {DoInvoke} */ (await readLocalActorInvokeRequest(request));
        await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, { storageScope: invoke });
        const facetName = buildFacetName(invoke);
        this.ctx.facets.delete(facetName);
        this.facetNames.delete(facetName);
        this.registeredObjectMembers.delete(objectRegistryMember(invoke));
        metrics.setGauge("do_host_actor_facet_count", { service: SERVICE }, this.facetNames.size);
        metrics.setGauge("do_host_actor_object_registry_size", { service: SERVICE }, this.registeredObjectMembers.size);
        return Response.json({ ok: true });
      }
      const invoke = /** @type {DoInvoke} */ (await readLocalActorInvokeRequest(request));
      return await this.dispatchWithFence(invoke, async () => {
        const requestId = request.headers.get("x-request-id") || null;
        const cls = this.tenantWorker(invoke, requestId).getDurableObjectClass(invoke.className, {
          props: invoke.props,
        });
        const facet = this.ctx.facets.get(this.rememberFacet(invoke), () => ({
          class: cls,
          id: invoke.objectName,
        }));
        if (invoke.kind === "alarm") {
          return await facet.fetch(buildAlarmRequest(invoke.alarm, requestId));
        }
        if (invoke.kind === "rpc") {
          return await dispatchRpc(facet, invoke.rpc, requestId);
        }
        return await facet.fetch(buildForwardRequest(invoke.request));
      });
    } catch (err) {
      return doPlatformErrorResponse(err);
    }
  }

  /**
   * @param {DoInvoke} invoke
   * @param {() => Promise<Response>} run
   */
  async dispatchWithFence(invoke, run) {
    if (!beginInFlightDispatch()) {
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
    }
    try {
      let fenced = await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, { storageScope: invoke });
      if (await this.rememberObject(invoke)) {
        fenced = await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, { storageScope: invoke });
      }
      const { owner, leaseRemainingMs } = fenced;
      return withoutOwnershipErrorControlHeader(
        await this.dispatchWithLeaseBudget(invoke, owner, leaseRemainingMs, run)
      );
    } finally {
      endInFlightDispatch();
    }
  }

  /**
   * @param {DoInvoke} invoke
   * @param {DoOwner} owner
   * @param {number} leaseRemainingMs
   * @param {() => Promise<Response>} run
   */
  async dispatchWithLeaseBudget(invoke, owner, leaseRemainingMs, run) {
    const facetName = buildFacetName(invoke);
    const guardMs = ownerLeaseGuardMs(this.env);
    let done = false;
    let timer = null;
    let scheduleFailureReason = null;
    const isDone = () => done;

    /**
     * @param {string} reason
     * @param {unknown} [err]
     */
    const abortFacet = (reason, err = null) => {
      if (isDone()) return;
      forgetOwnedScope(owner.ownerKey);
      log("error", "do_owner_lease_budget_exhausted", {
        owner_key: owner.ownerKey,
        owner_task_id: owner.taskId,
        generation: owner.generation,
        facet_name: facetName,
        reason,
        ...(err ? formatError(err) : {}),
      });
      this.ctx.facets.abort(facetName, new Error(`DO owner lease budget exhausted: ${reason}`));
    };

    /** @param {number} remainingMs */
    const schedule = (remainingMs) => {
      if (!Number.isFinite(remainingMs) || remainingMs <= 0 || remainingMs < guardMs) {
        scheduleFailureReason = remainingMs <= 0 ? "expired" : "lease_guard";
        abortFacet(scheduleFailureReason);
        return false;
      }
      timer = setTimeout(async () => {
        timer = null;
        if (isDone()) return;
        try {
          const renewed = await assertCurrentOwnerWithLeaseBudget(this.env, owner, {
            renewNearExpiry: false,
            storageScope: invoke,
          });
          if (isDone()) return;
          schedule(renewed.leaseRemainingMs);
        } catch (err) {
          if (isDone()) return;
          abortFacet("fence_failed", err);
        }
      }, Math.max(1, remainingMs - guardMs));
      return true;
    };

    if (!schedule(leaseRemainingMs)) {
      if (scheduleFailureReason === "lease_guard") {
        throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_LEASE_TOO_SHORT, `DO scope ${owner.ownerKey} owner lease has insufficient remaining budget`);
      }
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_LEASE_EXPIRED, `DO scope ${owner.ownerKey} owner lease has expired`);
    }
    try {
      return await run();
    } finally {
      done = true;
      if (timer) clearTimeout(timer);
    }
  }
}

/** @param {Response} response */
function withoutOwnershipErrorControlHeader(response) {
  if (!response.headers.has(DO_OWNERSHIP_ERROR_CONTROL_HEADER)) return response;
  const headers = new Headers(response.headers);
  headers.delete(DO_OWNERSHIP_ERROR_CONTROL_HEADER);
  return rebuildResponseWithHeaders(response, headers);
}

/**
 * @param {DoFacet} facet
 * @param {{ method: string, args: unknown[] }} rpc
 * @param {string | null} requestId
 */
export async function dispatchRpc(facet, rpc, requestId) {
  return await facet.fetch(buildRpcRequest(rpc, requestId));
}
