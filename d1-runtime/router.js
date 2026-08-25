import {
  classifyD1Error,
  D1ProtocolError,
  D1_ACTOR_QUERY_CONTENT_TYPE,
  encodeD1ActorQueryRequest,
  d1ErrorPayload,
  normalizeQueryRequest,
  readD1QueryRequest,
  readD1QueryResponseBytes,
  readD1QueryResponseWithBytes,
} from "d1-runtime-protocol";
import {
  resolveTaskIdentity,
} from "d1-runtime-task-identity";
import {
  ensureRequestId,
  formatError,
} from "shared-observability";
import {
  createD1QueryDeadline,
} from "shared-d1-timeout";
import {
  d1QuerySuccessHeaders,
  D1_OWNER_HINT_HEADERS,
  D1_QUERY_RESULT_HEADERS,
  readD1QuerySuccessHeaders,
} from "shared-d1-query-wire";
import {
  D1ReadCache,
  payloadChangedDb,
  statementMayBeIdempotentSchemaDdl,
  statementMayChangeDb,
} from "d1-runtime-read-cache";
import {
  assertD1TestHooksEnabled,
  normalizeD1TestHookRequest,
} from "d1-runtime-test-hooks";
import {
  ownerLeaseExpiredByRedisTime,
  resolveDbOwner,
  takeoverExpiredOwner,
} from "d1-runtime-owner-registry";
import {
  forwardToOwner,
  probeOwner,
} from "d1-runtime-owner-client";
import {
  parseForwardHopCount,
} from "shared-owner-forwarder";
import {
  log,
  metrics,
  SERVICE,
} from "d1-runtime-state";
import { d1QueryBytesResponse, d1QueryResponse } from "d1-runtime-http";

const D1_FORWARDED_RESPONSE_HEADER_NAMES = Object.freeze([
  ...Object.values(D1_OWNER_HINT_HEADERS),
  ...Object.values(D1_QUERY_RESULT_HEADERS),
]);
const ROUTER_READ_CACHE_MAX_DBS = 10_000;

/**
 * @typedef {Record<string, unknown>} AnyRecord
 * @typedef {Record<string, unknown> & { D1_DATABASES?: DurableObjectNamespace, D1_READ_CACHE_TTL_MS?: unknown, D1_READ_CACHE_MAX_ENTRIES?: unknown, D1_READ_CACHE_MAX_BYTES?: unknown, D1_QUERY_TIMEOUT_MS?: unknown }} D1Env
 * @typedef {import("d1-runtime-protocol").NormalizedStatement} D1Statement
 * @typedef {{ dbKey: string, namespace: string, databaseId: string, binding: string | null, mode: string, slot: number, statements: D1Statement[] }} D1Query
 * @typedef {{ taskId: string, endpoint: string, generation: number, dbKey: string }} D1Owner
 * @typedef {{ token: unknown, hit: boolean, bytes?: Uint8Array<ArrayBuffer>, valueEncoding?: string | null, preInvalidated?: boolean, mayHaveWrittenWithoutPreInvalidation?: boolean }} RouterRead
 */

/** @type {Map<string, D1ReadCache>} */
const routerReadCaches = new Map();
let routerReadCacheBytes = 0;

/** @param {D1ReadCache} cache @param {number} previousBytes */
function accountRouterReadCacheBytes(cache, previousBytes) {
  routerReadCacheBytes += cache.retainedBytes - previousBytes;
}

/** @param {string} dbKey @param {string | null} [reason] */
function removeRouterReadCache(dbKey, reason = null) {
  const cache = routerReadCaches.get(dbKey);
  if (!cache) return false;
  const previousBytes = cache.retainedBytes;
  if (reason !== null) cache.invalidate(reason);
  cache.retire();
  routerReadCacheBytes -= previousBytes;
  routerReadCaches.delete(dbKey);
  return true;
}

/** @param {number} maxBytes */
function enforceRouterReadCacheByteBudget(maxBytes) {
  while (routerReadCacheBytes > maxBytes) {
    const oldestKey = routerReadCaches.keys().next().value;
    if (oldestKey === undefined || !removeRouterReadCache(oldestKey)) break;
  }
}

/** @param {D1Env} env @param {string} dbKey */
function getRouterReadCache(env, dbKey) {
  let cache = routerReadCaches.get(dbKey);
  if (cache) {
    routerReadCaches.delete(dbKey);
    routerReadCaches.set(dbKey, cache);
    return cache;
  }
  while (routerReadCaches.size >= ROUTER_READ_CACHE_MAX_DBS) {
    const oldestKey = routerReadCaches.keys().next().value;
    if (oldestKey === undefined) break;
    removeRouterReadCache(oldestKey);
  }
  cache = new D1ReadCache(env, metrics, { service: SERVICE });
  routerReadCaches.set(dbKey, cache);
  return cache;
}

/** @param {string} dbKey */
function forgetRouterReadCache(dbKey) {
  removeRouterReadCache(dbKey, "owner-moved");
}

/** @param {string} dbKey @param {string} reason */
function invalidateRouterReadCache(dbKey, reason) {
  const cache = routerReadCaches.get(dbKey);
  if (!cache) return;
  const previousBytes = cache.retainedBytes;
  cache.invalidate(reason);
  accountRouterReadCacheBytes(cache, previousBytes);
}

/** @param {D1Owner} owner */
function ownerHeaders(owner) {
  return {
    [D1_OWNER_HINT_HEADERS.taskId]: String(owner.taskId || ""),
    [D1_OWNER_HINT_HEADERS.endpoint]: String(owner.endpoint || ""),
    [D1_OWNER_HINT_HEADERS.generation]: owner.generation == null ? "" : String(owner.generation),
  };
}

/** @param {Headers} headers */
function copyForwardedResponseHeaders(headers) {
  /** @type {Record<string, string>} */
  const out = {};
  for (const name of D1_FORWARDED_RESPONSE_HEADER_NAMES) {
    const value = headers.get(name);
    if (value) out[name] = value;
  }
  return out;
}

/** @param {Response} response @param {D1Owner} owner */
function withOwnerHeaders(response, owner) {
  const headers = new Headers(response.headers);
  for (const [name, value] of Object.entries(ownerHeaders(owner))) headers.set(name, value);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

/** @param {D1Query | null | undefined} query */
function mutatesDb(query) {
  return query?.mode === "exec" || query?.mode === "batch" || query?.mode === "run";
}

/** @param {D1Query | null | undefined} query */
function canDelayMutationInvalidation(query) {
  return query?.mode === "exec" &&
    Array.isArray(query.statements) &&
    query.statements.length > 0 &&
    query.statements.every((statement) => statementMayBeIdempotentSchemaDdl(statement?.sql));
}

/** @param {D1Query | null | undefined} query */
function queryMayChangeDb(query) {
  return Array.isArray(query?.statements) &&
    query.statements.some((statement) => statementMayChangeDb(statement?.sql));
}

/** @param {D1Query} query @param {D1Env} env @param {D1Owner} owner */
async function beginRouterRead(query, env, owner) {
  const localTask = await resolveTaskIdentity(env);
  if (owner.taskId !== localTask.taskId) {
    forgetRouterReadCache(query.dbKey);
    return { token: null, hit: false };
  }
  const cache = getRouterReadCache(env, query.dbKey);
  const delayedMutationInvalidation = mutatesDb(query) && canDelayMutationInvalidation(query);
  if (mutatesDb(query) && !delayedMutationInvalidation) {
    invalidateRouterReadCache(query.dbKey, "write");
    return { token: null, hit: false, preInvalidated: true };
  }
  const mayHaveWrittenWithoutPreInvalidation = delayedMutationInvalidation || queryMayChangeDb(query);
  if (query?.mode !== "all" && query?.mode !== "raw") {
    return { token: null, hit: false, preInvalidated: false, mayHaveWrittenWithoutPreInvalidation };
  }
  const previousBytes = cache.retainedBytes;
  const read = cache.beginRead(query, owner);
  accountRouterReadCacheBytes(cache, previousBytes);
  return {
    token: read.hit ? null : read.token,
    hit: read.hit === true,
    bytes: read.hit ? read.bytes : undefined,
    valueEncoding: read.hit ? read.valueEncoding : undefined,
    preInvalidated: false,
    mayHaveWrittenWithoutPreInvalidation,
  };
}

/** @param {{ requestId: string, query: D1Query | null, startedAt: number, status: number, code: string, outcome: string, forwarded: boolean }} details */
function recordQueryComplete({ requestId, query, startedAt, status, code, outcome, forwarded }) {
  const durationMs = Date.now() - startedAt;
  metrics.increment("d1_queries", { service: SERVICE, mode: query?.mode || "unknown", outcome });
  metrics.observe("d1_query_duration_ms", { service: SERVICE, mode: query?.mode || "unknown", outcome }, durationMs);
  if (outcome === "error") metrics.increment("d1_query_errors", { service: SERVICE, code });
  log(outcome === "error" ? "warn" : "info", "d1_query_complete", {
    request_id: requestId,
    namespace: query?.namespace,
    database_id: query?.databaseId,
    mode: query?.mode,
    slot: query?.slot,
    status,
    error_code: outcome === "error" ? code : undefined,
    duration_ms: durationMs,
    forwarded,
  });
}

/**
 * @param {Request} request
 * @param {D1Env} env
 * @param {string | null} [requestId]
 * @param {{ normalize?: (body: unknown) => D1Query, read?: (request: Request) => Promise<unknown> }} [options]
 */
export async function handleQuery(
  request,
  env,
  requestId = null,
  { normalize = normalizeQueryRequest, read = readD1QueryRequest } = {},
) {
  const startedAt = Date.now();
  requestId = requestId || ensureRequestId(request.headers);
  // Forwarding is not an authorization signal. It only suppresses duplicate
  // edge metrics; the receiver still resolves owner and the actor still fences.
  const forwarded = request.headers.get("x-wdl-d1-forwarded") === "1";
  const hopCount = parseForwardHopCount(request.headers.get("x-wdl-d1-hop-count"));
  /** @type {D1Query | null} */
  let query = null;
  let status;
  let code;
  let outcome;
  try {
    query = /** @type {D1Query} */ (normalize(await read(request)));
    const owner = await resolveDbOwner(env, query);
    const routerRead = await beginRouterRead(query, env, owner);
    if (routerRead.hit) {
      status = 200;
      code = "ok";
      outcome = "ok";
      if (!forwarded) {
        recordQueryComplete({ requestId, query, startedAt, status, code, outcome, forwarded });
      }
      return d1QueryBytesResponse(
        /** @type {Uint8Array<ArrayBuffer>} */ (routerRead.bytes),
        {
          status,
          headers: {
            ...ownerHeaders(owner),
            ...d1QuerySuccessHeaders(false, routerRead.valueEncoding ?? null),
          },
        }
      );
    }
    const response = await routeQueryToOwner(
      query,
      env,
      owner,
      true,
      requestId,
      hopCount
    );
    status = response.status;
    const responseValueEncoding = response.headers.get(D1_QUERY_RESULT_HEADERS.valueEncoding);
    const successHeaders = response.ok ? readD1QuerySuccessHeaders(response.headers) : null;
    /** @type {Uint8Array<ArrayBuffer>} */
    let bytes;
    /** @type {unknown} */
    let payload = null;
    if (successHeaders) {
      bytes = await readD1QueryResponseBytes(response);
      code = "ok";
      outcome = "ok";
    } else {
      ({ bytes, payload } = await readD1QueryResponseWithBytes(response));
      const payloadRecord = /** @type {Record<string, unknown>} */ (Object(payload));
      code = typeof payloadRecord.error === "string" ? payloadRecord.error : (response.ok ? "ok" : "internal-error");
      outcome = response.ok && payloadRecord.success !== false ? "ok" : "error";
    }
    if (outcome === "ok") {
      const responseChangedDb = successHeaders?.changedDb ??
        (response.headers.get(D1_QUERY_RESULT_HEADERS.changedDb) === "1" || payloadChangedDb(payload));
      if (responseChangedDb && !routerRead.preInvalidated) {
        invalidateRouterReadCache(query.dbKey, "changed-db");
      } else if (
        routerRead.token &&
        (responseValueEncoding === null || successHeaders?.valueEncoding === responseValueEncoding)
      ) {
        const cache = routerReadCaches.get(query.dbKey);
        if (cache) {
          const previousBytes = cache.retainedBytes;
          const stored = cache.finishRead(
            routerRead.token,
            query,
            owner,
            bytes,
            successHeaders?.valueEncoding ?? null
          );
          accountRouterReadCacheBytes(cache, previousBytes);
          if (stored) {
            routerReadCaches.delete(query.dbKey);
            routerReadCaches.set(query.dbKey, cache);
            enforceRouterReadCacheByteBudget(cache.config.maxBytes);
          }
        }
      }
    } else if (routerRead.mayHaveWrittenWithoutPreInvalidation) {
      invalidateRouterReadCache(query.dbKey, "write");
    }
    const headers = copyForwardedResponseHeaders(response.headers);
    if (!forwarded) {
      recordQueryComplete({ requestId, query, startedAt, status, code, outcome, forwarded });
    }
    return d1QueryBytesResponse(bytes, { status, headers });
  } catch (err) {
    const classified = classifyD1Error(err);
    status = classified.status;
    code = classified.code;
    if (!forwarded) {
      recordQueryComplete({ requestId, query, startedAt, status, code, outcome: "error", forwarded });
    }
    log("error", "d1_query_failed", {
      request_id: requestId,
      namespace: query?.namespace,
      database_id: query?.databaseId,
      mode: query?.mode,
      status,
      error_code: code,
      ...formatError(err),
    });
    return d1QueryResponse(d1ErrorPayload(err), { status });
  }
}

/** @param {Request} request @param {D1Env} env @param {string | null} [requestId] */
export async function handleTestHookQuery(request, env, requestId = null) {
  assertD1TestHooksEnabled(env);
  return await handleQuery(request, env, requestId, {
    normalize: normalizeD1TestHookRequest,
    read: (hookRequest) => hookRequest.json(),
  });
}

/**
 * @param {D1Query} query
 * @param {D1Env} env
 * @param {D1Owner} owner
 * @param {boolean} allowRefresh
 * @param {string | null} [requestId]
 * @param {number} [hopCount]
 */
export async function routeQueryToOwner(
  query,
  env,
  owner,
  allowRefresh,
  requestId = null,
  hopCount = 0
) {
  const localTask = await resolveTaskIdentity(env);
  if (owner.taskId !== localTask.taskId) {
    // Probe is only a reachability hint. Execution authority still comes from
    // registry ownership and generation checks, never from a successful probe.
    const probe = await probeOwner(env, query, owner);
    if (probe.outcome === "draining" || probe.outcome === "stale-generation") {
      const refreshed = await resolveDbOwner(env, query, { refresh: true });
      if (allowRefresh && refreshed.taskId !== owner.taskId) {
        return await routeQueryToOwner(query, env, refreshed, false, requestId, hopCount);
      }
      throw new D1ProtocolError(
        503,
        "owner-not-ready",
        `D1 database ${query.dbKey} owner is ${probe.outcome}`
      );
    }
    if (
      (probe.outcome === "probe-unavailable" || probe.outcome === "probe-unhealthy") &&
      await ownerLeaseExpiredByRedisTime(env, owner)
    ) {
      const takeover = await takeoverExpiredOwner(env, owner);
      if (takeover.taskId !== owner.taskId) {
        return await routeQueryToOwner(query, env, takeover, false, requestId, hopCount);
      }
    }
    return await forwardToOwner(query, env, owner, requestId, hopCount);
  }
  if (!env.D1_DATABASES) {
    throw new D1ProtocolError(503, "d1_actor_unavailable", "D1 actor namespace is not configured");
  }
  const id = env.D1_DATABASES.idFromName(query.dbKey);
  const stub = env.D1_DATABASES.get(id);
  const deadline = createD1QueryDeadline(env);
  try {
    const response = await stub.fetch("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": D1_ACTOR_QUERY_CONTENT_TYPE },
      signal: deadline.signal,
      body: encodeD1ActorQueryRequest(query, owner),
    });
    return withOwnerHeaders(response, owner);
  } finally {
    deadline.clear();
  }
}
