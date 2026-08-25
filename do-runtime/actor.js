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
} from "do-runtime-state";
import { formatError } from "shared-observability";
import { rebuildResponseWithHeaders } from "shared-respond";
import { SESSION_POLICY_RESTART } from "shared-worker-contract";

// Native facets are task-local while host SQLite is shared. Without an authoritative
// native-container retirement signal, task rows cannot be TTL-pruned; facet_name leads
// the key so storage cleanup can still delete every task row through the primary index.
const CREATE_FACET_SESSION_POLICY_TABLE =
  "CREATE TABLE IF NOT EXISTS wdl_facet_session_policy (" +
  "task_id TEXT NOT NULL, facet_name TEXT NOT NULL, restart_sequence INTEGER NOT NULL, " +
  "PRIMARY KEY (facet_name, task_id))";
const FACET_REGISTRATION_CACHE_MAX_ENTRIES = 10_000;
const OBJECT_REGISTRY_MEMO_MAX_ENTRIES = 10_000;

/**
 * @typedef {{ LOADER: { get(key: string, loader: () => Promise<unknown>): DoWorkerStub } }} DoEnv
 * @typedef {{ getDurableObjectClass(className: string, options: { props: Record<string, unknown> }): DurableObjectClass }} DoWorkerStub
 * @typedef {{ fetch(request: Request): Promise<Response> }} DoFacet
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {{ ownerKey: string, hostId?: string, className?: string, ns: string, worker: string, doStorageId: string, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} DoOwner
 * @typedef {{ restartSequence: number }} FacetRegistration
 */

export class WdlDoHostActor extends DurableObject {
  /** @type {Map<string, FacetRegistration>} facet name -> registration */
  facetWorkers;
  /** @type {Set<string>} */
  registeredObjectMembers;
  /** @type {boolean} */
  facetSessionPolicyTableReady;

  /**
   * @param {DurableObjectState} ctx
   * @param {DoEnv} env
   */
  constructor(ctx, env) {
    super(ctx, env);
    this.facetWorkers = new Map();
    this.registeredObjectMembers = new Set();
    this.facetSessionPolicyTableReady = false;
  }

  /**
   * @param {DoInvoke} invoke
   * @param {string | null} requestId
   */
  tenantWorker(invoke, requestId) {
    return this.env.LOADER.get(invoke.workerId, () => (
      loadDoWorkerCode(
        this.env,
        this.ctx,
        invoke,
        requestId
      )
    ));
  }

  ensureFacetSessionPolicyTable() {
    if (this.facetSessionPolicyTableReady) return;
    this.ctx.storage.sql.exec(CREATE_FACET_SESSION_POLICY_TABLE);
    this.facetSessionPolicyTableReady = true;
  }

  /** @param {string} facetName @param {number} restartSequence */
  cacheFacetRegistration(facetName, restartSequence) {
    const registration = { restartSequence };
    this.facetWorkers.set(facetName, registration);
    if (this.facetWorkers.size > FACET_REGISTRATION_CACHE_MAX_ENTRIES) {
      const oldest = this.facetWorkers.keys().next().value;
      if (oldest !== undefined) this.facetWorkers.delete(oldest);
    }
    return registration;
  }

  /** @param {string} taskId @param {string} facetName */
  readFacetRegistration(taskId, facetName) {
    const cached = this.facetWorkers.get(facetName);
    if (cached) return cached;
    this.ensureFacetSessionPolicyTable();
    const row = [...this.ctx.storage.sql.exec(
      "SELECT restart_sequence FROM wdl_facet_session_policy WHERE task_id = ? AND facet_name = ?",
      taskId,
      facetName
    )][0];
    if (!row) return null;
    const restartSequence = row.restart_sequence;
    if (
      typeof restartSequence !== "number" ||
      !Number.isSafeInteger(restartSequence) ||
      restartSequence < 0
    ) {
      throw new DoRuntimeError(503, "session_policy_state_invalid", "facet session policy state is invalid");
    }
    return this.cacheFacetRegistration(facetName, restartSequence);
  }

  /** @param {string} taskId @param {string} facetName @param {number} restartSequence */
  writeFacetRegistration(taskId, facetName, restartSequence) {
    this.ensureFacetSessionPolicyTable();
    this.ctx.storage.sql.exec(
      "INSERT INTO wdl_facet_session_policy (task_id, facet_name, restart_sequence) VALUES (?, ?, ?) " +
        "ON CONFLICT(facet_name, task_id) DO UPDATE SET restart_sequence = excluded.restart_sequence",
      taskId,
      facetName,
      restartSequence
    );
  }

  /** @param {string} facetName */
  deleteFacetRegistration(facetName) {
    this.ensureFacetSessionPolicyTable();
    this.ctx.storage.sql.exec(
      "DELETE FROM wdl_facet_session_policy WHERE facet_name = ?",
      facetName
    );
  }

  /** @param {DoInvoke} invoke @param {string} taskId */
  rememberFacet(invoke, taskId) {
    const facetName = buildFacetName(invoke);
    const existing = this.readFacetRegistration(taskId, facetName);
    if (existing && invoke.restartSequence < existing.restartSequence) {
      throw new DoRuntimeError(
        503,
        "session_policy_version_stale",
        `Durable Object restart ${invoke.restartSequence} was superseded by ${existing.restartSequence}`
      );
    }
    const advanceFacet = existing && invoke.restartSequence > existing.restartSequence;
    const restartFacet = advanceFacet && invoke.sessionPolicy === SESSION_POLICY_RESTART;
    if (restartFacet) {
      this.ctx.facets.abort(
        facetName,
        new Error(`Durable Object restarted for ${invoke.workerId}`)
      );
      this.facetWorkers.delete(facetName);
      log("info", "session_policy_restart_facet_on_dispatch", {
        namespace: invoke.ns,
        worker: invoke.worker,
        version: invoke.version,
        restart_sequence: invoke.restartSequence,
        facet_name: facetName,
      });
    }
    if (existing && advanceFacet && !restartFacet) {
      this.writeFacetRegistration(taskId, facetName, invoke.restartSequence);
      existing.restartSequence = invoke.restartSequence;
    }
    if (!this.facetWorkers.has(facetName)) {
      this.writeFacetRegistration(taskId, facetName, invoke.restartSequence);
      this.cacheFacetRegistration(facetName, invoke.restartSequence);
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
    if (this.registeredObjectMembers.size > OBJECT_REGISTRY_MEMO_MAX_ENTRIES) {
      const oldest = this.registeredObjectMembers.values().next().value;
      if (oldest !== undefined) this.registeredObjectMembers.delete(oldest);
    }
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
        return await this.dispatchWithFence(invoke, async (owner) => {
          const requestId = request.headers.get("x-request-id") || null;
          const facetName = this.rememberFacet(invoke, owner.taskId);
          const facet = this.ctx.facets.get(facetName, () => ({
            class: this.tenantWorker(invoke, requestId).getDurableObjectClass(invoke.className, {
              props: invoke.props,
            }),
            id: invoke.objectName,
          }));
          return await facet.fetch(buildForwardRequest(invoke.request));
        });
      }
      if (url.pathname === "/delete-storage") {
        const invoke = /** @type {DoInvoke} */ (await readLocalActorInvokeRequest(request));
        if (!beginInFlightDispatch()) {
          throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
        }
        try {
          await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, { storageScope: invoke });
          const facetName = buildFacetName(invoke);
          this.ctx.facets.delete(facetName);
          this.facetWorkers.delete(facetName);
          this.deleteFacetRegistration(facetName);
          this.registeredObjectMembers.delete(objectRegistryMember(invoke));
          return Response.json({ ok: true });
        } finally {
          endInFlightDispatch();
        }
      }
      const invoke = /** @type {DoInvoke} */ (await readLocalActorInvokeRequest(request));
      return await this.dispatchWithFence(invoke, async (owner) => {
        const requestId = request.headers.get("x-request-id") || null;
        const facetName = this.rememberFacet(invoke, owner.taskId);
        const facet = this.ctx.facets.get(facetName, () => ({
          class: this.tenantWorker(invoke, requestId).getDurableObjectClass(invoke.className, {
            props: invoke.props,
          }),
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
   * @param {(owner: DoOwner) => Promise<Response>} run
   */
  async dispatchWithFence(invoke, run) {
    if (!beginInFlightDispatch()) {
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
    }
    try {
      let fenced = await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, {
        sessionPolicyInvoke: invoke,
        storageScope: invoke,
      });
      if (await this.rememberObject(invoke)) {
        fenced = await assertCurrentOwnerWithLeaseBudget(this.env, invoke.owner, {
          sessionPolicyInvoke: invoke,
          storageScope: invoke,
        });
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
   * @param {(owner: DoOwner) => Promise<Response>} run
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
      return await run(owner);
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
