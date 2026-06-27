import {
  D1ProtocolError,
} from "d1-runtime-protocol";
import {
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  encodeD1QueryRequest,
} from "shared-d1-query-wire";
import {
  createD1QueryDeadline,
  isD1QueryTimeoutError,
} from "shared-d1-timeout";
import { errorMessage } from "shared-errors";
import {
  probeTimeoutMs,
} from "d1-runtime-owner-registry";
import {
  log,
  metrics,
  SERVICE,
} from "d1-runtime-state";
import { withInternalAuth } from "shared-internal-auth";
import { forwardOwnerRequest } from "shared-owner-forwarder";

/**
 * @typedef {Record<string, unknown> & { D1_QUERY_TIMEOUT_MS?: unknown }} D1Env
 * @typedef {import("shared-d1-query-wire").D1QueryStatementInput} D1Statement
 * @typedef {{ dbKey: string, namespace: string, databaseId: string, binding?: string | null, mode: string, slot?: string | number, statements: D1Statement[] }} D1Query
 * @typedef {{ taskId: string, endpoint?: string | null, generation: number }} D1Owner
 */

const OWNER_UNAVAILABLE_STATUSES = new Set([502, 503, 504]);

/** @param {Headers} headers */
function isD1QueryResponse(headers) {
  return String(headers.get("content-type") || "").toLowerCase().split(";")[0].trim() === D1_QUERY_RESPONSE_CONTENT_TYPE;
}

/** @param {Response} response */
function ownerEndpointReturnedUnknownResult(response) {
  return OWNER_UNAVAILABLE_STATUSES.has(response.status) && !isD1QueryResponse(response.headers);
}

/** @param {D1Query} query @param {string} detail */
function resultUnknownError(query, detail) {
  return new D1ProtocolError(
    503,
    "result-unknown",
    `D1 database ${query.dbKey} owner response was lost after forwarding; outcome may be unknown, do not blindly retry non-idempotent requests: ${detail}`
  );
}

/** @param {D1Env} env @param {D1Query} query @param {D1Owner} owner */
export async function probeOwner(env, query, owner) {
  if (!owner.endpoint) return { outcome: "owner-endpoint-missing" };
  try {
    const res = await fetch(
      `http://${owner.endpoint}/internal/d1/probe?dbKey=${encodeURIComponent(query.dbKey)}&generation=${owner.generation}`,
      {
        headers: withInternalAuth(undefined, env),
        signal: AbortSignal.timeout(probeTimeoutMs(env)),
      }
    );
    const body = await res.json().catch(() => null);
    if (res.status === 503 && body?.status === "draining") return { outcome: "draining", body };
    if (!res.ok) return { outcome: "probe-unhealthy", status: res.status, body };
    if (
      body?.owner &&
      (body.owner.taskId !== owner.taskId || body.owner.generation !== owner.generation)
    ) {
      return { outcome: "stale-generation", body };
    }
    return { outcome: "owner-alive", body };
  } catch (err) {
    return { outcome: "probe-unavailable", error: errorMessage(err) };
  }
}

/** @param {D1Query} query @param {D1Env} env @param {D1Owner} owner @param {string | null} [requestId] @param {number} [hopCount] */
export async function forwardToOwner(query, env, owner, requestId = null, hopCount = 0) {
  const deadline = createD1QueryDeadline(env);
  try {
    const response = await forwardOwnerRequest({
      env,
      endpoint: owner.endpoint,
      pathname: "/internal/d1/query",
      requestId,
      hopCount,
      signal: deadline.signal,
      body: encodeD1QueryRequest({
        namespace: query.namespace,
        databaseId: query.databaseId,
        binding: query.binding,
        mode: query.mode,
        statements: query.statements,
      }),
      metrics,
      metricName: "d1_forwards",
      service: SERVICE,
      log,
      logEvent: "d1_forward_complete",
      buildHeaders: (nextHopCount) => ({
        "content-type": D1_QUERY_CONTENT_TYPE,
        "x-wdl-d1-forwarded": "1",
        "x-wdl-d1-hop-count": String(nextHopCount),
      }),
      logFields: () => ({
        namespace: query.namespace,
        database_id: query.databaseId,
        slot: query.slot,
        owner_task_id: owner.taskId,
        owner_endpoint: owner.endpoint,
      }),
      missingEndpointError: () =>
        new D1ProtocolError(503, "owner-endpoint-missing", `D1 database ${query.dbKey} owner has no endpoint`),
      hopExhaustedError: () =>
        new D1ProtocolError(
          503,
          "forward-hop-exhausted",
          `D1 database ${query.dbKey} exceeded the maximum forward depth`
        ),
      isTimeoutError: isD1QueryTimeoutError,
      unavailableError: () => resultUnknownError(query, "owner transport failed after forwarding"),
    });
    if (ownerEndpointReturnedUnknownResult(response)) {
      throw resultUnknownError(query, `owner endpoint returned HTTP ${response.status}`);
    }
    return response;
  } finally {
    deadline.clear();
  }
}
