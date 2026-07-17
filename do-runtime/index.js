import { createLogLevelBinder } from "shared-observability";
import {
  internalAuthFailureResponse,
  verifyInternalAuthHeaders,
} from "shared-internal-auth";
import { createHttpRequestScope } from "shared-request-scope";
import { discardResponseBody, prometheusResponse, rebuildResponseWithHeaders } from "shared-respond";
import { boundedPositiveIntEnv } from "shared-owner-lease";
import { parseVersion } from "shared-worker-contract";
import { formatWorkerId } from "shared-worker-id";
import { WdlDoHostActor } from "do-runtime-actor";
import { handleAlarmDispatch } from "do-runtime-alarm-dispatch";
import {
  buildLocalActorRequest,
  doPlatformErrorResponse,
  DoRuntimeError,
  hostIdForObject,
  hostIdForShard,
  normalizeDoConnectRequest,
  readDoInvokeRequest,
  readJsonBody,
} from "do-runtime-protocol";
import { json, jsonError } from "do-runtime-http";
import {
  parseObjectRegistryMember,
} from "do-runtime-object-registry";
import {
  isDraining,
  log,
  metrics,
  ownedScopes,
  recordDoInvoke,
  recordDoWebSocketUpgrade,
  SERVICE,
  setDraining,
  waitForInFlightDispatches,
} from "do-runtime-state";
import { peekTaskIdentity, resolveTaskIdentity } from "do-runtime-task-identity";
import {
  drainOwnedScopes,
  ownerTtlSeconds,
  readOwner,
  renewOwnedScopes,
  resolveDoOwner,
} from "do-runtime-owner-registry";
import { forwardConnectToOwner, forwardToOwner, parseHopCount } from "do-runtime-owner-client";
import {
  DO_ACCEPT_OWNER_HINT_HEADER,
  DO_OWNER_HINT_CONTROL_HEADER,
  DO_OWNER_HINT_CODE,
  ownerHintHeaders,
} from "runtime-do-transport";

export { WdlDoHostActor };
export { KV } from "runtime-bindings-kv";
export { Assets } from "runtime-bindings-assets";
export { ServiceBinding } from "runtime-bindings-service";
export { QueueProducer } from "runtime-bindings-queue";
export { D1Database } from "runtime-bindings-d1";
export { R2Bucket } from "runtime-bindings-r2";
export { DurableObjectNamespace } from "runtime-bindings-do";
export { InternalAuthBackend } from "runtime-bindings-internal-auth-backend";
export { DoAlarmBinding } from "do-runtime-alarm-binding";

const bindLogLevel = createLogLevelBinder();
const STORAGE_DELETE_WORKER_CONCURRENCY = 8;
const DEFAULT_DRAIN_IN_FLIGHT_TIMEOUT_MS = 8000;
/** @type {{ method: string, url: string, headers: Array<[string, string]> }} */
const STORAGE_DELETE_REQUEST = {
  method: "POST",
  url: "https://do-runtime.internal/delete-storage",
  headers: [],
};

/**
 * @typedef {Record<string, unknown> & { LOG_LEVEL?: unknown, REDIS_ADDR?: string, DO_HOSTS: DurableObjectNamespace, DO_DRAIN_IN_FLIGHT_TIMEOUT_MS?: unknown, DO_RENEW_INTERVAL_MS?: unknown }} DoEnv
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {{ ownerKey: string, hostId?: string, className?: string, ns: string, worker: string, doStorageId: string, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} DoOwner
 * @typedef {{ requestId?: string | null, hopCount?: number, forwardPath?: string, localUrl?: string, request?: { method: string, url: string, headers: Array<[string, string]> } | null, metricKind?: string | null, acceptOwnerHint?: boolean }} DispatchOptions
 * @typedef {{ ns?: unknown, worker?: unknown, version?: unknown, doStorageId?: unknown, members?: unknown }} StorageDeleteInput
 */

/**
 * @param {string} method
 * @param {string} pathname
 */
function routeName(method, pathname) {
  if (method === "GET" && pathname === "/healthz") return "healthz";
  if (method === "GET" && pathname === "/_metrics") return "metrics";
  if (method === "POST" && pathname === "/internal/do/invoke") return "do_invoke";
  if (method === "GET" && pathname === "/internal/do/connect") return "do_connect";
  if (method === "POST" && pathname === "/internal/do/alarms/dispatch") return "do_alarms_dispatch";
  if (method === "GET" && pathname === "/internal/do/probe") return "do_probe";
  if (method === "POST" && pathname === "/internal/do/renew") return "do_renew";
  if (method === "POST" && pathname === "/internal/do/drain") return "do_drain";
  if (method === "POST" && pathname === "/internal/do/storage/delete") return "do_storage_delete";
  if (method === "POST" && pathname === "/internal/do/storage/delete-worker") return "do_storage_delete_worker";
  return "not_found";
}

/** @param {DoEnv} env */
function healthResponse(env) {
  const identity = peekTaskIdentity(env);
  return json({
    ok: !isDraining(),
    service: SERVICE,
    status: isDraining() ? "draining" : "ok",
    draining: isDraining(),
    taskId: identity?.taskId || null,
    endpoint: identity?.endpoint || null,
    ownerTtlSeconds: ownerTtlSeconds(env),
    ownerScopes: { owned: ownedScopes.size },
  }, { status: isDraining() ? 503 : 200 });
}

/** @param {DoOwner} owner */
function ownerFence(owner) {
  return {
    ownerKey: owner.ownerKey,
    taskId: owner.taskId,
    generation: owner.generation,
  };
}

/** @param {DoOwner} owner */
function ownerHint(owner) {
  return {
    ownerKey: owner.ownerKey,
    taskId: owner.taskId,
    endpoint: owner.endpoint,
    generation: owner.generation,
  };
}

/** @param {DoOwner} owner */
function ownerHintResponse(owner) {
  return json({
    error: DO_OWNER_HINT_CODE,
    message: "Durable Object owner is remote; retry the owner endpoint",
    owner: ownerHint(owner),
  }, {
    status: 409,
    headers: ownerHintHeaders(owner, { control: true }),
  });
}

/**
 * @param {Response} response
 * @param {DoOwner} owner
 */
function withOwnerHintHeaders(response, owner) {
  const headers = new Headers(response.headers);
  headers.delete(DO_OWNER_HINT_CONTROL_HEADER);
  for (const [name, value] of Object.entries(ownerHintHeaders(owner))) {
    headers.set(name, value);
  }
  return rebuildResponseWithHeaders(response, headers);
}

/** @param {Request} request */
function acceptsOwnerHint(request) {
  // Strict opt-in: only the runtime facade sends "1"; user-controlled truthy
  // strings must not silently switch the router into hint-only behavior.
  return request.headers.get(DO_ACCEPT_OWNER_HINT_HEADER) === "1";
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 * @param {boolean} [acceptOwnerHint]
 */
async function dispatchInvoke(env, invoke, requestId = null, hopCount = 0, acceptOwnerHint = false) {
  return await dispatchToOwner(env, invoke, { requestId, hopCount, metricKind: invoke.kind, acceptOwnerHint });
}

/**
 * @param {DoInvoke} invoke
 * @param {DispatchOptions["request"]} request
 */
function withRequestOverride(invoke, request) {
  return request ? { ...invoke, request } : invoke;
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {DispatchOptions} [options]
 */
async function dispatchToOwner(
  env,
  invoke,
  {
    requestId = null,
    hopCount = 0,
    forwardPath = "/internal/do/invoke",
    localUrl = "https://do-runtime.internal/invoke",
    request = null,
    metricKind = null,
    acceptOwnerHint = false,
  } = {}
) {
  const owner = await resolveDoOwner(env, invoke);
  const localTask = await resolveTaskIdentity(env);
  const dispatchPayload = withRequestOverride(invoke, request);
  if (owner.taskId !== localTask.taskId) {
    if (acceptOwnerHint) return ownerHintResponse(owner);
    return await forwardToOwner(dispatchPayload, env, owner, requestId, hopCount, forwardPath);
  }

  const id = env.DO_HOSTS.idFromName(invoke.hostId);
  const stub = env.DO_HOSTS.get(id);
  const fencedInvoke = { ...dispatchPayload, owner: ownerFence(owner) };
  const fetchLocal = () => stub.fetch(buildLocalActorRequest(localUrl, fencedInvoke, requestId));
  const response = metricKind != null ? await recordDoInvoke(metricKind, fetchLocal) : await fetchLocal();
  return withOwnerHintHeaders(response, owner);
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 */
async function dispatchStorageDelete(env, invoke, requestId = null, hopCount = 0) {
  return await dispatchToOwner(env, invoke, {
    requestId,
    hopCount,
    forwardPath: "/internal/do/storage/delete",
    localUrl: "https://do-runtime.internal/delete-storage",
    request: STORAGE_DELETE_REQUEST,
  });
}

/**
 * @param {Request} request
 * @param {DoEnv} env
 * @param {string} requestId
 */
async function handleInvoke(request, env, requestId) {
  const invoke = await readDoInvokeRequest(request);
  return await dispatchInvoke(
    env,
    invoke,
    requestId,
    parseHopCount(request.headers.get("x-wdl-do-hop-count")),
    acceptsOwnerHint(request)
  );
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {Request} request
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 * @param {boolean} [acceptOwnerHint]
 */
async function dispatchConnect(env, invoke, request, requestId = null, hopCount = 0, acceptOwnerHint = false) {
  const owner = await resolveDoOwner(env, invoke);
  const localTask = await resolveTaskIdentity(env);
  if (owner.taskId !== localTask.taskId) {
    if (acceptOwnerHint) return ownerHintResponse(owner);
    return await forwardConnectToOwner(request, invoke, env, owner, requestId, hopCount);
  }

  const id = env.DO_HOSTS.idFromName(invoke.hostId);
  const stub = env.DO_HOSTS.get(id);
  const headers = new Headers(request.headers);
  headers.set("x-wdl-do-owner-key", owner.ownerKey);
  headers.set("x-wdl-do-owner-task-id", owner.taskId);
  headers.set("x-wdl-do-owner-generation", String(owner.generation));
  const response = await recordDoWebSocketUpgrade(() => stub.fetch("https://do-runtime.internal/connect", {
    method: request.method,
    headers,
  }));
  return withOwnerHintHeaders(response, owner);
}

/**
 * @param {Request} request
 * @param {DoEnv} env
 * @param {string} requestId
 */
async function handleConnect(request, env, requestId) {
  const invoke = normalizeDoConnectRequest(request);
  return await dispatchConnect(
    env,
    invoke,
    request,
    requestId,
    parseHopCount(request.headers.get("x-wdl-do-hop-count")),
    acceptsOwnerHint(request)
  );
}

/**
 * @param {URL} url
 * @param {DoEnv} env
 */
async function handleProbe(url, env) {
  const ownerKey = url.searchParams.get("ownerKey");
  const identity = peekTaskIdentity(env) || await resolveTaskIdentity(env);
  const owner = ownerKey ? await readOwner(env, ownerKey) : null;
  const rawGeneration = url.searchParams.get("generation");
  const generation = rawGeneration == null || rawGeneration === "" ? null : Number(rawGeneration);
  return json({
    status: isDraining() ? "draining" : "owner_alive",
    service: SERVICE,
    draining: isDraining(),
    taskId: identity.taskId,
    endpoint: identity.endpoint,
    ownerKey,
    generation: generation != null && Number.isInteger(generation) && generation >= 0 ? generation : null,
    owner,
    ownerScopes: { owned: ownedScopes.size },
  }, { status: isDraining() ? 503 : 200 });
}

/** @param {DoEnv} env */
async function handleRenew(env) {
  return json(await renewOwnedScopes(env));
}

/** @param {DoEnv} env */
async function handleDrain(env) {
  setDraining(true);
  const wait = await waitForInFlightDispatches(
    boundedPositiveIntEnv(env, "DO_DRAIN_IN_FLIGHT_TIMEOUT_MS", DEFAULT_DRAIN_IN_FLIGHT_TIMEOUT_MS, Infinity)
  );
  if (!wait.drained) {
    log("warn", "do_drain_in_flight_timeout", {
      in_flight: wait.inFlight,
      waited_ms: wait.waitedMs,
    });
    return jsonError(503, "do_drain_in_flight_timeout", "DO drain timed out waiting for in-flight handlers", {
      draining: isDraining(),
      inFlight: wait.inFlight,
      waitedMs: wait.waitedMs,
      owned: ownedScopes.size,
      released: 0,
    });
  }
  const result = await drainOwnedScopes(env);
  return json({
    ...result,
    inFlight: wait.inFlight,
    drainWaitMs: wait.waitedMs,
  });
}

/**
 * @param {Request} request
 * @param {DoEnv} env
 * @param {string} requestId
 */
async function handleStorageDelete(request, env, requestId) {
  const invoke = await readDoInvokeRequest(request);
  return await dispatchStorageDelete(
    env,
    invoke,
    requestId,
    parseHopCount(request.headers.get("x-wdl-do-hop-count"))
  );
}

/**
 * @param {string} member
 * @param {Response} response
 * @param {string} text
 */
function storageDeleteResponseError(member, response, text) {
  let body = null;
  try {
    body = JSON.parse(text);
  } catch {}
  return {
    member,
    error: typeof body?.error === "string" ? body.error : "storage_delete_failed",
    message: typeof body?.message === "string"
      ? body.message
      : `DO storage delete failed with status ${response.status}`,
    status: response.status,
  };
}

/**
 * @param {string} member
 * @param {unknown} err
 */
function storageDeleteExceptionError(member, err) {
  if (err instanceof DoRuntimeError) {
    return {
      member,
      error: err.code,
      message: err.message,
      status: err.status,
    };
  }
  return {
    member,
    error: "storage_delete_failed",
    message: "DO storage delete failed",
  };
}

/**
 * @param {DoEnv} env
 * @param {Request} request
 * @param {string | null} [requestId]
 */
async function handleStorageDeleteWorker(env, request, requestId = null) {
  const input = /** @type {StorageDeleteInput} */ (await readJsonBody(request));
  const ns = typeof input.ns === "string" ? input.ns : "";
  const worker = typeof input.worker === "string" ? input.worker : "";
  const version = typeof input.version === "string" ? input.version : "";
  const doStorageId = typeof input.doStorageId === "string" ? input.doStorageId : "";
  const members = Array.isArray(input.members) ? input.members.filter((/** @type {unknown} */ m) => typeof m === "string") : [];
  if (!ns || !worker || !version || !doStorageId) {
    return jsonError(400, "invalid_request", "ns, worker, version, and doStorageId are required");
  }
  if (parseVersion(version) == null) {
    return jsonError(400, "invalid_request", "version is invalid");
  }

  let deleted = 0;
  /** @type {Array<Record<string, unknown>>} */
  const errors = [];
  let nextIndex = 0;
  async function deleteNextMember() {
    while (true) {
      const index = nextIndex++;
      if (index >= members.length) return;
      const member = members[index];
      const parsed = parseObjectRegistryMember(member);
      if (!parsed) {
        errors.push({ member, error: "invalid_member" });
        continue;
      }
      /** @type {DoInvoke} */
      const invoke = {
        kind: "fetch",
        ns,
        worker,
        version,
        doStorageId,
        workerId: formatWorkerId({ namespace: ns, worker, version }),
        hostId: parsed.shard == null
          ? hostIdForObject(doStorageId, parsed.className, parsed.objectName)
          : hostIdForShard(doStorageId, parsed.className, parsed.shard),
        className: parsed.className,
        objectName: parsed.objectName,
        props: {
          ns,
          worker,
          version,
          className: parsed.className,
        },
        request: STORAGE_DELETE_REQUEST,
      };
      try {
        const response = await dispatchStorageDelete(env, invoke, requestId);
        if (!response.ok) {
          const text = await response.text();
          errors.push(storageDeleteResponseError(member, response, text));
          continue;
        }
        await discardResponseBody(response);
        deleted += 1;
      } catch (err) {
        errors.push(storageDeleteExceptionError(member, err));
      }
    }
  }
  await Promise.all(
    Array.from(
      { length: Math.min(STORAGE_DELETE_WORKER_CONCURRENCY, members.length) },
      () => deleteNextMember()
    )
  );
  return json({ ok: errors.length === 0, deleted, errors }, { status: errors.length ? 207 : 200 });
}

export default {
  /**
   * @param {Request} request
   * @param {DoEnv} env
   */
  async fetch(request, env) {
    const url = new URL(request.url);
    bindLogLevel(env);
    const scope = createHttpRequestScope({
      request,
      service: SERVICE,
      metrics,
      log,
      route: routeName(request.method, url.pathname),
      probeRoutes: ["healthz", "metrics", "do_probe"],
    });

    try {
      if (request.method === "GET" && url.pathname === "/healthz") {
        return scope.respond(healthResponse(env));
      }
      if (request.method === "GET" && url.pathname === "/_metrics") {
        return scope.respond(prometheusResponse(metrics));
      }
      if (!verifyInternalAuthHeaders(request.headers, env)) {
        return scope.respond(internalAuthFailureResponse());
      }
      if (request.method === "POST" && url.pathname === "/internal/do/invoke") {
        return scope.respond(await handleInvoke(request, env, scope.requestId));
      }
      if (request.method === "GET" && url.pathname === "/internal/do/connect") {
        return scope.respond(await handleConnect(request, env, scope.requestId));
      }
      if (request.method === "POST" && url.pathname === "/internal/do/alarms/dispatch") {
        return scope.respond(await handleAlarmDispatch(request, env, dispatchInvoke, scope.requestId));
      }
      if (request.method === "GET" && url.pathname === "/internal/do/probe") {
        return scope.respond(await handleProbe(url, env));
      }
      if (request.method === "POST" && url.pathname === "/internal/do/renew") {
        return scope.respond(await handleRenew(env));
      }
      if (request.method === "POST" && url.pathname === "/internal/do/drain") {
        return scope.respond(await handleDrain(env));
      }
      if (request.method === "POST" && url.pathname === "/internal/do/storage/delete") {
        return scope.respond(await handleStorageDelete(request, env, scope.requestId));
      }
      if (request.method === "POST" && url.pathname === "/internal/do/storage/delete-worker") {
        return scope.respond(await handleStorageDeleteWorker(env, request, scope.requestId));
      }
      return scope.respond(jsonError(404, "not_found", "Not found"));
    } catch (err) {
      scope.markError(err);
      return scope.respond(doPlatformErrorResponse(err));
    } finally {
      scope.complete();
    }
  },
};
