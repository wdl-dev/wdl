import {
  d1BackendUnavailablePayload,
  createD1QueryDeadline,
  d1QueryTimeoutPayload,
  isD1QueryTimeoutError,
} from "shared-d1-timeout";
import { decodeD1TransportForJson } from "shared-d1-transport";
import {
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  decodeD1QueryResponse,
  encodeD1QueryRequest,
} from "shared-d1-query-wire";
import { sanitizeJsonErrorDetails } from "shared-respond";
import { withInternalAuth } from "shared-internal-auth";
import { validOwnerEndpointForService } from "shared-owner-endpoint";

/**
 * @typedef {{ fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }} Fetcher
 * @typedef {{ D1_BACKEND: Fetcher, D1_QUERY_TIMEOUT_MS?: unknown, [key: string]: unknown }} D1RuntimeEnv
 * @typedef {{ taskId: string | null, endpoint: string | null, generation: number | null }} D1OwnerHint
 * @typedef {{ ok: boolean, status: number, body: unknown, owner?: D1OwnerHint | null }} D1RuntimeResult
 */

/** @param {unknown} err */
function d1RuntimeTransportPayload(err) {
  if (isD1QueryTimeoutError(err)) return d1QueryTimeoutPayload();
  return d1BackendUnavailablePayload();
}

/** @param {Headers} headers */
function d1OwnerGenerationFromHeaders(headers) {
  const raw = headers.get("x-wdl-d1-owner-generation");
  if (raw == null || raw === "") return null;
  const generation = Number(raw);
  return Number.isSafeInteger(generation) && generation > 0 ? generation : null;
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} mode
 * @param {{ sql: string, params?: unknown[] }[]} statements
 * @param {string | null} [requestId]
 * @returns {Promise<D1RuntimeResult>}
 */
export async function d1RuntimeQuery(env, ns, databaseId, mode, statements, requestId = null) {
  let res;
  const deadline = createD1QueryDeadline(env);
  /** @type {Record<string, string>} */
  const headers = { "content-type": D1_QUERY_CONTENT_TYPE };
  if (typeof requestId === "string" && requestId) headers["x-request-id"] = requestId;
  try {
    res = await env.D1_BACKEND.fetch("http://d1-runtime/internal/d1/query", {
      method: "POST",
      headers: withInternalAuth(headers, env),
      signal: deadline.signal,
      body: encodeD1QueryRequest({
        namespace: ns,
        databaseId,
        binding: null,
        mode,
        statements,
      }),
    });
  } catch (err) {
    const body = d1RuntimeTransportPayload(err);
    return { ok: false, status: body.error === "timeout" ? 504 : 503, body };
  } finally {
    deadline.clear();
  }
  let body;
  let validResponse = true;
  const contentType = res.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() === D1_QUERY_RESPONSE_CONTENT_TYPE) {
    try {
      body = decodeD1QueryResponse(new Uint8Array(await res.arrayBuffer()));
    } catch (err) {
      validResponse = false;
      body = {
        error: "invalid_d1_runtime_response",
        message: err instanceof Error ? err.message : "invalid d1-runtime response",
        category: "invalid-response",
        retryable: false,
        causeCode: "binary-decode-failed",
      };
    }
  } else {
    validResponse = false;
    body = {
      error: "invalid_d1_runtime_response",
      message: `unsupported d1-runtime response content-type ${contentType || "none"}`,
      category: "invalid-response",
      retryable: false,
      causeCode: "unsupported-content-type",
    };
  }
  const owner = {
    taskId: res.headers.get("x-wdl-d1-owner-task-id") || null,
    endpoint: res.headers.get("x-wdl-d1-owner-endpoint") || null,
    generation: d1OwnerGenerationFromHeaders(res.headers),
  };
  return {
    ok: res.ok && validResponse,
    status: validResponse ? res.status : 502,
    body,
    owner: validResponse && owner.taskId && owner.endpoint && owner.generation != null ? owner : null,
  };
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {D1OwnerHint | null | undefined} owner
 * @param {string | null} [requestId]
 * @returns {Promise<D1RuntimeResult>}
 */
export async function d1RuntimeReleaseOwner(env, ns, databaseId, owner, requestId = null) {
  if (!owner?.endpoint) {
    return {
      ok: false,
      status: 0,
      body: { error: "owner-endpoint-missing", message: "missing owner endpoint" },
    };
  }
  if (!validOwnerEndpointForService(owner.endpoint, 8787, "d1-runtime")) {
    return {
      ok: false,
      status: 0,
      body: { error: "owner-endpoint-invalid", message: "invalid owner endpoint" },
    };
  }
  const deadline = createD1QueryDeadline(env);
  /** @type {Record<string, string>} */
  const headers = { "content-type": "application/json" };
  if (typeof requestId === "string" && requestId) headers["x-request-id"] = requestId;
  try {
    // This must target the concrete owner task. Do not replace it with
    // env.D1_BACKEND.fetch(), which goes through Service Connect and may hit
    // any d1-runtime router.
    const res = await fetch(`http://${owner.endpoint}/internal/d1/rebalance`, {
      method: "POST",
      headers: withInternalAuth(headers, env),
      signal: deadline.signal,
      body: JSON.stringify({
        databases: [{ namespace: ns, databaseId }],
        target: null,
      }),
    });
    const text = await res.text();
    let body;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = {
        error: "invalid_d1_runtime_release_response",
        message: text || "invalid d1-runtime release response",
      };
    }
    return { ok: res.ok, status: res.status, body };
  } catch (err) {
    const body = d1RuntimeTransportPayload(err);
    return { ok: false, status: body.error === "timeout" ? 504 : 503, body };
  } finally {
    deadline.clear();
  }
}

/**
 * @param {D1RuntimeEnv} env
 * @param {string} ns
 * @param {string} databaseId
 * @param {string | null} [requestId]
 * @returns {Promise<D1RuntimeResult>}
 */
export async function d1RuntimeProbeOwner(env, ns, databaseId, requestId = null) {
  const deadline = createD1QueryDeadline(env);
  /** @type {Record<string, string>} */
  const headers = {};
  if (typeof requestId === "string" && requestId) headers["x-request-id"] = requestId;
  const dbKey = `${ns}:${databaseId}`;
  try {
    const res = await env.D1_BACKEND.fetch(
      `http://d1-runtime/internal/d1/probe?dbKey=${encodeURIComponent(dbKey)}`,
      {
        method: "GET",
        headers: withInternalAuth(headers, env),
        signal: deadline.signal,
      }
    );
    const text = await res.text();
    let body;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = {
        error: "invalid_d1_runtime_probe_response",
        message: text || "invalid d1-runtime probe response",
      };
    }
    return {
      ok: res.ok || Boolean(Object(body).owner),
      status: res.status,
      body,
      owner: /** @type {D1OwnerHint | null} */ (Object(body).owner || null),
    };
  } catch (err) {
    const body = d1RuntimeTransportPayload(err);
    return {
      ok: false,
      status: body.error === "timeout" ? 504 : 503,
      body,
      owner: null,
    };
  } finally {
    deadline.clear();
  }
}

/**
 * @param {unknown} value
 * @param {string} [mode]
 * @returns {unknown}
 */
export function d1RuntimePublicResult(value, mode = "all") {
  const decoded = decodeD1TransportForJson(value);
  // Raw mode is the only public control surface that must preserve column
  // order and duplicate column names. Other modes normalize row/column payloads
  // back to object rows; exec output is already a no-op pass-through.
  if (mode === "raw") return decoded;
  return normalizeD1PublicResult(decoded);
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
function normalizeD1PublicResult(value) {
  if (Array.isArray(value)) return value.map(normalizeD1PublicResult);
  const record = /** @type {Record<string, unknown>} */ (Object(value));
  if (!value || typeof value !== "object" || !Object.hasOwn(record, "results") || Array.isArray(record.results)) return value;
  const { columns, rows } = /** @type {{ columns?: unknown, rows?: unknown }} */ (record.results || {});
  if (!Array.isArray(columns) || !Array.isArray(rows)) return value;
  return {
    ...record,
    results: rows.map((row) => Object.fromEntries(columns.map((column, idx) => [column, row[idx]]))),
  };
}

/**
 * @param {Record<string, unknown>} extra
 * @returns {Record<string, unknown>}
 */
function d1RuntimeFailureExtra(extra) {
  const sanitized = /** @type {Record<string, unknown>} */ (sanitizeJsonErrorDetails(extra) || {});
  for (const key of ["upstreamCode", "upstreamCategory", "upstreamRetryable", "upstreamStatus", "detail"]) {
    delete sanitized[key];
  }
  return sanitized;
}

const D1_RUNTIME_CLASSIFIER_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

/** @param {unknown} value @param {string} fallback */
function d1RuntimeClassifierToken(value, fallback) {
  return typeof value === "string" && D1_RUNTIME_CLASSIFIER_RE.test(value) ? value : fallback;
}

/**
 * Stable diagnostics for Control logs. Raw backend messages stay in the D1
 * runtime's own request log and are omitted from redacted 5xx responses.
 *
 * @param {{ body?: unknown, status?: unknown } | null | undefined} result
 */
export function d1RuntimeFailureLogFields(result) {
  const body = /** @type {Record<string, unknown>} */ (Object(result?.body || {}));
  const causeCode = d1RuntimeClassifierToken(body.causeCode, "");
  return {
    ...(typeof result?.status === "number" ? { upstream_status: result.status } : {}),
    upstream_code: d1RuntimeClassifierToken(body.error, "d1-runtime-error"),
    upstream_category: d1RuntimeClassifierToken(body.category, "internal"),
    upstream_retryable: body.retryable === true,
    ...(causeCode ? { upstream_cause_code: causeCode } : {}),
  };
}

/**
 * @param {string} error
 * @param {string} ns
 * @param {string} databaseId
 * @param {{ body?: unknown, status?: unknown } | null | undefined} result
 * @param {Record<string, unknown>} [extra]
 * @param {{ publicStatus?: number }} [options]
 */
export function d1RuntimeFailure(error, ns, databaseId, result, extra = {}, options = {}) {
  const body = /** @type {Record<string, unknown>} */ (Object(result?.body || {}));
  const status = typeof result?.status === "number" ? result.status : 500;
  const publicStatus = typeof options.publicStatus === "number" ? options.publicStatus : status;
  const publicFields = {
    ...d1RuntimeFailureExtra(extra),
    error,
    namespace: ns,
    databaseId,
  };
  if (publicStatus < 400 || publicStatus >= 500) {
    return {
      ...publicFields,
      message: "Internal error",
      upstreamCode: d1RuntimeClassifierToken(body.error, "d1-runtime-error"),
      upstreamCategory: d1RuntimeClassifierToken(body.category, "internal"),
      upstreamRetryable: body.retryable === true,
      ...(typeof result?.status === "number" ? { upstreamStatus: result.status } : {}),
    };
  }
  const upstreamCode = typeof body.error === "string" ? body.error : "d1-runtime-error";
  return {
    ...publicFields,
    message: typeof body.message === "string"
      ? body.message
      : typeof body.error === "string"
        ? body.error
        : "D1 runtime request failed",
    upstreamCode,
    upstreamCategory: typeof body.category === "string" ? body.category : "internal",
    upstreamRetryable: body.retryable === true,
    upstreamStatus: result?.status,
    detail: body,
  };
}
