// Runtime-side D1 binding stub. Loaded workers do not use this object
// directly in the normal path: runtime/load.js wraps D1 bindings with a
// local client facade inside the loaded isolate so prepare()/bind()/batch()
// stay synchronous/local and avoid nested JSRPC promise serialization.

import { WorkerEntrypoint } from "cloudflare:workers";
import {
  d1BackendUnavailablePayload,
  createD1QueryDeadline,
  d1ResultUnknownPayload,
  d1QueryTimeoutPayload,
  isD1QueryTimeoutError,
} from "shared-d1-timeout";
import {
  D1_OWNER_HINT_HEADERS,
  D1_OWNERSHIP_CODES,
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_NATIVE_VALUE_ENCODING,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  D1_QUERY_RESULT_HEADERS,
  decodeD1QueryResponse,
  encodeD1QueryRequest,
} from "shared-d1-query-wire";
import { decodeD1Transport } from "shared-d1-transport";
import { metrics } from "runtime-metrics";
import { createOwnerHintCache } from "runtime-owner-hint-cache";
import { validOwnerEndpointForService } from "shared-owner-endpoint";
import { serviceNameFromEnv } from "runtime-bindings-proxy";
import { withInternalAuth } from "shared-internal-auth";
import { errorMessage } from "shared-errors";
import { logStructured } from "shared-observability";
import { contentTypeEssence } from "shared-respond";

const OWNER_HINT_STALE_CODES = new Set(D1_OWNERSHIP_CODES);
const OWNER_ENDPOINT_UNAVAILABLE_STATUSES = new Set([502, 503, 504]);
const ownerHintKeys = new WeakMap();
const ownerHintCache = createOwnerHintCache({
  keyFor: (db) => ownerHintKeys.get(/** @type {WeakKey} */ (db)),
});

class D1ResultUnknownError extends Error {
  /** @param {unknown} cause */
  constructor(cause) {
    super(errorMessage(cause));
    this.name = "D1ResultUnknownError";
    this.cause = cause;
  }
}

/**
 * @typedef {{ ns: string, databaseId: string, binding?: string }} D1BindingProps
 * @typedef {{
 *   D1_BACKEND: { fetch(url: string, init?: RequestInit): Promise<Response> },
 *   SERVICE_NAME?: string,
 *   D1_QUERY_TIMEOUT_MS?: unknown,
 * }} D1BindingEnv
 * @typedef {{ ctx: { props: D1BindingProps }, env: D1BindingEnv }} D1Binding
 * @typedef {import("shared-d1-query-wire").D1QueryStatementInput} D1Statement
 * @typedef {Record<string, unknown> & { success?: unknown, error?: unknown, message?: unknown, category?: unknown, retryable?: unknown, statementIndex?: unknown, causeCode?: unknown }} D1Payload
 * @typedef {{ taskId: string, endpoint: string, generation: number }} D1OwnerHint
 */

/** @param {D1Database} db @returns {D1Binding} */
function d1Binding(db) {
  return /** @type {D1Binding} */ (/** @type {unknown} */ (db));
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function clearD1OwnerHintsForTest() {
  ownerHintCache.clearForTest();
}

/**
 * @lintignore data-URL unit tests import this hook from a rewritten module.
 * @param {number | null} maxEntries
 */
export function setD1OwnerHintMaxEntriesForTest(maxEntries) {
  ownerHintCache.setMaxEntriesForTest(maxEntries);
}

/** @param {D1BindingProps} props */
function ownerHintKey(props) {
  return `${props?.ns || ""}:${props?.databaseId || ""}`;
}

/**
 * @param {D1Payload} payload
 * @param {number} status
 * @returns {never}
 */
function throwD1Payload(payload, status) {
  // D1 machine error codes are an in-tree contract. The additive binary-value
  // encoding fallback is handled separately while parsing successful payloads.
  const machineCode = payload?.error || null;
  const code = machineCode ? ` [${machineCode}]` : "";
  const message = payload?.message || `backend status ${status}`;
  const err = new Error(`D1_ERROR${code}: ${message}`);
  err.name = "D1_ERROR";
  throw Object.assign(err, {
    code: machineCode,
    category: payload?.category || null,
    retryable: payload?.retryable === true,
    statementIndex: Number.isInteger(payload?.statementIndex) ? payload.statementIndex : undefined,
    causeCode: payload?.causeCode || null,
  });
}

/**
 * @param {unknown} err
 * @returns {never}
 */
function throwD1TransportError(err) {
  const timedOut = isD1QueryTimeoutError(err);
  throwD1Payload(
    err instanceof D1ResultUnknownError
      ? d1ResultUnknownPayload()
      : timedOut
        ? d1QueryTimeoutPayload()
        : d1BackendUnavailablePayload(),
    timedOut ? 504 : 503
  );
}

/**
 * @param {Headers} headers
 * @returns {D1OwnerHint | null}
 */
function ownerHintFromHeaders(headers) {
  const taskId = headers.get(D1_OWNER_HINT_HEADERS.taskId);
  const endpoint = headers.get(D1_OWNER_HINT_HEADERS.endpoint);
  const rawGeneration = headers.get(D1_OWNER_HINT_HEADERS.generation);
  if (rawGeneration == null || rawGeneration === "") return null;
  const generation = Number(rawGeneration);
  if (
    !taskId ||
    !validOwnerEndpointForService(endpoint, 8787, "d1-runtime") ||
    !Number.isSafeInteger(generation) ||
    generation <= 0
  ) {
    return null;
  }
  return { taskId, endpoint: String(endpoint), generation };
}

/** @param {D1BindingEnv} env */
function serviceName(env) {
  return serviceNameFromEnv(env);
}

/**
 * @param {D1BindingEnv} env
 * @param {string} outcome
 */
function recordOwnerHintOutcome(env, outcome) {
  metrics.increment("d1_owner_hint_outcomes", {
    service: serviceName(env),
    outcome,
  });
}

/** @param {D1Binding} db */
function getOwnerHint(db) {
  return /** @type {D1OwnerHint | null} */ (ownerHintCache.get(db));
}

/**
 * @param {D1Binding} db
 * @param {D1OwnerHint} ownerHint
 */
function setOwnerHint(db, ownerHint) {
  return ownerHintCache.set(db, ownerHint);
}

/** @param {D1Binding} db */
function deleteOwnerHint(db) {
  ownerHintCache.delete(db);
}

/**
 * @param {Response} response
 * @param {string} causeCode
 * @param {unknown} [cause]
 */
function invalidD1ResponseError(response, causeCode, cause = null) {
  const detail = cause == null ? "" : `: ${errorMessage(cause)}`;
  return Object.assign(
    new Error(`D1_ERROR: D1 runtime returned invalid response status ${response.status}${detail}`),
    { causeCode }
  );
}

/** @param {unknown} err */
function invalidD1ResponseCauseCode(err) {
  try {
    const causeCode = /** @type {{ causeCode?: unknown }} */ (err)?.causeCode;
    return typeof causeCode === "string" && causeCode ? causeCode : "invalid-response";
  } catch {
    return "invalid-response";
  }
}

/**
 * @param {D1Binding} db
 * @param {Response} response
 * @param {string | null} requestId
 * @param {unknown} err
 */
function recordInvalidD1Response(db, response, requestId, err) {
  logStructured(serviceName(db.env), "warn", "d1_runtime_response_invalid", {
    request_id: requestId,
    namespace: db.ctx.props.ns,
    database_id: db.ctx.props.databaseId,
    upstream_status: response.status,
    upstream_cause_code: invalidD1ResponseCauseCode(err),
  });
}

/**
 * @param {Response} response
 * @returns {Promise<D1Payload>}
 */
async function parseD1Payload(response) {
  const contentType = response.headers.get("content-type") || "";
  if (!d1PayloadContentType(contentType)) {
    throw invalidD1ResponseError(response, "unsupported-content-type");
  }
  /** @type {Uint8Array} */
  let bytes;
  try {
    bytes = new Uint8Array(await response.arrayBuffer());
  } catch (err) {
    throw invalidD1ResponseError(response, "response-body-read-failed", err);
  }
  /** @type {unknown} */
  let payload;
  try {
    payload = decodeD1QueryResponse(bytes);
  } catch (err) {
    throw invalidD1ResponseError(response, "binary-decode-failed", err);
  }
  const valueEncoding = response.headers.get(D1_QUERY_RESULT_HEADERS.valueEncoding);
  if (valueEncoding === D1_QUERY_NATIVE_VALUE_ENCODING) {
    return /** @type {D1Payload} */ (Object(payload));
  }
  if (valueEncoding === null) {
    try {
      return /** @type {D1Payload} */ (Object(decodeD1Transport(payload)));
    } catch (err) {
      const causeCode = Object(err)?.code === "unsupported-d1-transport-version"
        ? "unsupported-d1-transport-version"
        : "legacy-value-decode-failed";
      throw invalidD1ResponseError(response, causeCode, err);
    }
  }
  throw invalidD1ResponseError(response, "unsupported-value-encoding");
}

/** @param {string} contentType */
function d1PayloadContentType(contentType) {
  return contentTypeEssence(contentType) === D1_QUERY_RESPONSE_CONTENT_TYPE;
}

/** @param {Response} response */
function ownerEndpointUnavailableResponse(response) {
  return OWNER_ENDPOINT_UNAVAILABLE_STATUSES.has(response.status) &&
    !d1PayloadContentType(response.headers.get("content-type") || "");
}

/**
 * @param {D1Binding} db
 * @param {string} mode
 * @param {D1Statement[]} statements
 * @returns {Uint8Array}
 */
function requestBody(db, mode, statements) {
  const { ns, databaseId, binding } = db.ctx.props;
  return encodeD1QueryRequest({
    namespace: ns,
    databaseId,
    binding: binding || null,
    mode,
    statements,
  });
}

/**
 * @param {D1Binding} db
 * @param {HeadersInit} headers
 * @param {AbortSignal} signal
 * @param {BodyInit} body
 * @returns {Promise<Response>}
 */
async function fetchViaRouter(db, headers, signal, body) {
  return await db.env.D1_BACKEND.fetch("http://d1-runtime/internal/d1/query", {
    method: "POST",
    headers: withInternalAuth(headers, db.env),
    signal,
    body,
  });
}

/**
 * @param {D1Binding} db
 * @param {HeadersInit} headers
 * @param {AbortSignal} signal
 * @param {BodyInit} body
 * @returns {Promise<Response | null>}
 */
async function fetchViaOwner(db, headers, signal, body) {
  const ownerHint = getOwnerHint(db);
  if (!ownerHint?.endpoint) {
    recordOwnerHintOutcome(db.env, "miss");
    return null;
  }
  if (!validOwnerEndpointForService(ownerHint.endpoint, 8787, "d1-runtime")) {
    deleteOwnerHint(db);
    recordOwnerHintOutcome(db.env, "cleared");
    return null;
  }
  try {
    return await fetch(`http://${ownerHint.endpoint}/internal/d1/query`, {
      method: "POST",
      headers: withInternalAuth(headers, db.env),
      signal,
      body,
    });
  } catch (err) {
    deleteOwnerHint(db);
    recordOwnerHintOutcome(db.env, "cleared");
    if (isD1QueryTimeoutError(err)) throw err;
    throw new D1ResultUnknownError(err);
  }
}

/**
 * @param {D1Binding} db
 * @param {string} mode
 * @param {D1Statement[]} statements
 * @param {string | null} [requestId]
 * @returns {Promise<D1Payload>}
 */
async function sendQuery(db, mode, statements, requestId = null) {
  /** @type {Response} */
  let response;
  let usedOwnerHint = false;
  const deadline = createD1QueryDeadline(db.env);
  /** @type {Record<string, string>} */
  const headers = { "content-type": D1_QUERY_CONTENT_TYPE };
  if (typeof requestId === "string" && requestId) headers["x-request-id"] = requestId;
  const body = requestBody(db, mode, statements);
  try {
    try {
      const ownerResponse = await fetchViaOwner(db, headers, deadline.signal, /** @type {BodyInit} */ (body));
      usedOwnerHint = ownerResponse != null;
      response = ownerResponse || await fetchViaRouter(db, headers, deadline.signal, /** @type {BodyInit} */ (body));
    } catch (err) {
      throwD1TransportError(err);
    }
    if (usedOwnerHint && ownerEndpointUnavailableResponse(response)) {
      deleteOwnerHint(db);
      recordOwnerHintOutcome(db.env, "cleared");
      throwD1TransportError(new D1ResultUnknownError(new Error(`D1 owner endpoint returned ${response.status}`)));
    }
    /** @type {D1Payload} */
    let payload;
    try {
      payload = await parseD1Payload(response);
    } catch (err) {
      recordInvalidD1Response(db, response, requestId, err);
      if (usedOwnerHint) {
        deleteOwnerHint(db);
        recordOwnerHintOutcome(db.env, "cleared");
      }
      throwD1TransportError(new D1ResultUnknownError(err));
    }
    if (
      usedOwnerHint &&
      (!response.ok || payload?.success === false) &&
      typeof payload.error === "string" &&
      OWNER_HINT_STALE_CODES.has(payload.error)
    ) {
      deleteOwnerHint(db);
      recordOwnerHintOutcome(db.env, "cleared");
      try {
        response = await fetchViaRouter(db, headers, deadline.signal, /** @type {BodyInit} */ (body));
        usedOwnerHint = false;
      } catch (err) {
        throwD1TransportError(err);
      }
      try {
        payload = await parseD1Payload(response);
      } catch (err) {
        recordInvalidD1Response(db, response, requestId, err);
        throwD1TransportError(new D1ResultUnknownError(err));
      }
    }
    if (usedOwnerHint) recordOwnerHintOutcome(db.env, "hit");
    if (!response.ok || payload?.success === false) {
      throwD1Payload(payload, response.status);
    }
    const ownerHint = ownerHintFromHeaders(response.headers);
    if (ownerHint && setOwnerHint(db, ownerHint)) {
      recordOwnerHintOutcome(db.env, "learned");
    }
    return payload;
  } finally {
    deadline.clear();
  }
}

export class D1Database extends WorkerEntrypoint {
  /**
   * @param {ExecutionContext & { props: D1BindingProps }} ctx
   * @param {D1BindingEnv} env
   */
  constructor(ctx, env) {
    super(ctx, env);
    ownerHintKeys.set(this, ownerHintKey(ctx.props));
  }

  /**
   * @param {string} mode
   * @param {D1Statement[]} statements
   * @param {string | null} [requestId]
   */
  async query(mode, statements, requestId = null) {
    return await sendQuery(d1Binding(this), mode, statements, requestId);
  }
}
