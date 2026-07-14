import { normalizeD1Param } from "shared-d1-params";
import { fnv1a32CodeUnits } from "shared-fnv1a32";
import { BodyTooLargeError, readBoundedBytes as readRequestBoundedBytes } from "shared-bounded-body";
import { errorMessage as sharedErrorMessage } from "shared-errors";
import { D1_DATABASE_ID_RE, isValidRuntimeLoadNs } from "shared-ns-pattern";
import {
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  decodeD1QueryRequest,
  decodeD1QueryResponse,
  encodeD1QueryRequest,
  encodeD1QueryResponse,
} from "shared-d1-query-wire";

export {
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  encodeD1QueryRequest,
  encodeD1QueryResponse,
} from "shared-d1-query-wire";

export const D1_ACTOR_QUERY_CONTENT_TYPE = "application/vnd.wdl.d1-actor-query";
const SLOT_COUNT = 4096;
const QUERY_MODES = new Set(["all", "raw", "run", "exec", "batch"]);
export const D1_MAX_QUERY_ENVELOPE_BYTES = 8 * 1024 * 1024;
export const D1_MAX_QUERY_PAYLOAD_BYTES = 8 * 1024 * 1024;
export const D1_MAX_STATEMENTS = 1000;
export const D1_MAX_SQL_STATEMENT_BYTES = 100_000;
export const D1_MAX_BOUND_PARAMS = 100;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

/**
 * @typedef {string | number | boolean | null | undefined | number[]} D1Param
 * @typedef {{ sql: string, params: D1Param[] }} NormalizedStatement
 * @typedef {{
 *   namespace: string,
 *   databaseId: string,
 *   dbKey: string,
 *   slot: number,
 *   binding: string | null,
 *   mode: string,
 *   statements: NormalizedStatement[],
 *   __control?: unknown,
 *   __holdMs?: unknown,
 * }} NormalizedQuery
 * @typedef {{ status: number, code: string, category: string, retryable: boolean, message: string }} ClassifiedD1Error
 * @typedef {Record<string, unknown>} ActorMetadata
 */

/** @param {string} value */
function textBytes(value) {
  return utf8Encoder.encode(value).byteLength;
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 * @param {string} label
 */
async function readBoundedBytes(request, maxBytes, label) {
  try {
    return await readRequestBoundedBytes(request, maxBytes);
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      throw new D1ProtocolError(
        413,
        "limit-exceeded",
        `D1 limit exceeded: maximum ${label} body is ${maxBytes} bytes`
      );
    }
    throw err;
  }
}

export class D1ProtocolError extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {string} message
   * @param {Record<string, unknown>} [extra]
   */
  constructor(status, code, message, extra = {}) {
    super(message);
    this.name = "D1ProtocolError";
    /** @type {number} */
    this.status = status;
    /** @type {string} */
    this.code = code;
    Object.assign(this, extra);
  }
}

/**
 * @param {unknown} namespace
 * @param {unknown} databaseId
 */
export function dbKeyOf(namespace, databaseId) {
  if (!isValidRuntimeLoadNs(namespace)) {
    throw new D1ProtocolError(400, "invalid-namespace", "namespace is invalid");
  }
  if (typeof databaseId !== "string" || !D1_DATABASE_ID_RE.test(databaseId)) {
    throw new D1ProtocolError(400, "invalid-database-id", "databaseId is invalid");
  }
  return `${namespace}:${databaseId}`;
}

/**
 * @param {unknown} namespace
 * @param {unknown} databaseId
 * @param {number} [slotCount]
 */
export function slotOf(namespace, databaseId, slotCount = SLOT_COUNT) {
  const key = dbKeyOf(namespace, databaseId);
  return fnv1a32CodeUnits(key) % slotCount;
}

export { normalizeD1Param };

/**
 * @param {unknown} statement
 * @returns {NormalizedStatement}
 */
export function normalizeStatement(statement) {
  if (!statement || typeof statement !== "object" || Array.isArray(statement)) {
    throw new D1ProtocolError(400, "invalid-statement", "statement must be an object");
  }
  const record = /** @type {Record<string, unknown>} */ (statement);
  const sql = record.sql;
  if (typeof sql !== "string" || !sql.trim()) {
    throw new D1ProtocolError(400, "invalid-sql", "statement.sql is required");
  }
  if (textBytes(sql) > D1_MAX_SQL_STATEMENT_BYTES) {
    throw new D1ProtocolError(
      413,
      "limit-exceeded",
      `D1 limit exceeded: maximum SQL statement length is ${D1_MAX_SQL_STATEMENT_BYTES} bytes`
    );
  }
  const params = record.params == null ? [] : record.params;
  if (!Array.isArray(params)) {
    throw new D1ProtocolError(400, "invalid-params", "statement.params must be an array");
  }
  if (params.length > D1_MAX_BOUND_PARAMS) {
    throw new D1ProtocolError(
      413,
      "limit-exceeded",
      `D1 limit exceeded: maximum bound parameters per query is ${D1_MAX_BOUND_PARAMS}`
    );
  }
  return { sql, params: params.map(normalizeD1Param) };
}

/** @param {D1Param} value */
function paramPayloadBytes(value) {
  if (value == null) return 0;
  if (typeof value === "number" || typeof value === "boolean") return 8;
  if (typeof value === "string") return textBytes(value);
  if (Array.isArray(value)) return value.length;
  return 0;
}

/** @param {NormalizedStatement[]} statements */
function assertQueryShapeLimits(statements) {
  if (statements.length > D1_MAX_STATEMENTS) {
    throw new D1ProtocolError(
      413,
      "limit-exceeded",
      `D1 limit exceeded: maximum statements per request is ${D1_MAX_STATEMENTS}`
    );
  }
  let total = 0;
  for (const statement of statements) {
    total += textBytes(statement.sql);
    for (const param of statement.params) total += paramPayloadBytes(param);
    if (total > D1_MAX_QUERY_PAYLOAD_BYTES) {
      throw new D1ProtocolError(
        413,
        "limit-exceeded",
        `D1 limit exceeded: maximum aggregate query payload is ${D1_MAX_QUERY_PAYLOAD_BYTES} bytes`
      );
    }
  }
}

/**
 * @param {unknown} body
 * @returns {NormalizedQuery}
 */
export function normalizeQueryRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new D1ProtocolError(400, "invalid-body", "request body must be an object");
  }
  const record = /** @type {Record<string, unknown>} */ (body);
  const namespace = record.namespace;
  const databaseId = record.databaseId;
  const dbKey = dbKeyOf(namespace, databaseId);
  const statements = Array.isArray(record.statements)
    ? record.statements.map(normalizeStatement)
    : [normalizeStatement({ sql: record.sql, params: record.params })];
  if (statements.length === 0) {
    throw new D1ProtocolError(400, "empty-statements", "at least one statement is required");
  }
  assertQueryShapeLimits(statements);
  const mode = record.mode == null ? "all" : record.mode;
  if (typeof mode !== "string" || !QUERY_MODES.has(mode)) {
    throw new D1ProtocolError(400, "invalid-mode", `mode must be one of ${Array.from(QUERY_MODES).join(", ")}`);
  }
  if (record.__control != null) {
    throw new D1ProtocolError(
      400,
      "invalid-control",
      "__control is not accepted on the D1 query endpoint"
    );
  }
  return {
    namespace: /** @type {string} */ (namespace),
    databaseId: /** @type {string} */ (databaseId),
    dbKey,
    slot: slotOf(namespace, databaseId),
    binding: typeof record.binding === "string" && record.binding ? record.binding : null,
    mode,
    statements,
  };
}

/** @param {Request} request @param {{ maxBytes?: number }} [options] */
export async function readD1QueryRequest(request, { maxBytes = D1_MAX_QUERY_ENVELOPE_BYTES } = {}) {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() !== D1_QUERY_CONTENT_TYPE) {
    throw new D1ProtocolError(
      415,
      "unsupported-media-type",
      `D1 query endpoint requires ${D1_QUERY_CONTENT_TYPE}`
    );
  }
  try {
    return decodeD1QueryRequest(await readBoundedBytes(request, maxBytes, "D1 query"));
  } catch (err) {
    if (err instanceof D1ProtocolError) throw err;
    const message = sharedErrorMessage(err);
    throw new D1ProtocolError(400, "invalid-body", `D1 query body is invalid: ${message}`);
  }
}

/** @param {Response} response */
export async function readD1QueryResponse(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() !== D1_QUERY_RESPONSE_CONTENT_TYPE) {
    throw new D1ProtocolError(
      502,
      "invalid-response",
      `D1 query response requires ${D1_QUERY_RESPONSE_CONTENT_TYPE}`
    );
  }
  try {
    return decodeD1QueryResponse(new Uint8Array(await response.arrayBuffer()));
  } catch (err) {
    const message = sharedErrorMessage(err);
    throw new D1ProtocolError(502, "invalid-response", `D1 query response is invalid: ${message}`);
  }
}

/**
 * @param {Record<string, unknown>} metadata
 * @param {NormalizedQuery} query
 */
function encodeActorEnvelope(metadata, query) {
  const metadataBytes = utf8Encoder.encode(JSON.stringify(metadata));
  const queryBytes = encodeD1QueryRequest({
    namespace: query.namespace,
    databaseId: query.databaseId,
    binding: query.binding,
    mode: query.mode,
    statements: query.statements,
  });
  const envelope = new Uint8Array(4 + metadataBytes.length + queryBytes.length);
  new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength)
    .setUint32(0, metadataBytes.length, false);
  envelope.set(metadataBytes, 4);
  envelope.set(queryBytes, 4 + metadataBytes.length);
  return envelope;
}

/**
 * @param {NormalizedQuery} query
 * @param {unknown} owner
 */
export function encodeD1ActorQueryRequest(query, owner) {
  return encodeActorEnvelope({
    owner,
    ...(query.__control == null ? {} : { __control: query.__control }),
    ...(query.__holdMs == null ? {} : { __holdMs: query.__holdMs }),
  }, query);
}

/** @param {Uint8Array} bytes */
function decodeActorEnvelope(bytes) {
  if (bytes.length < 4) {
    throw new D1ProtocolError(400, "invalid-body", "D1 actor query envelope is truncated");
  }
  const metadataLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, false);
  if (metadataLength === 0 || 4 + metadataLength > bytes.length) {
    throw new D1ProtocolError(400, "invalid-body", "D1 actor query envelope metadata is invalid");
  }
  let metadata;
  try {
    metadata = JSON.parse(utf8Decoder.decode(bytes.subarray(4, 4 + metadataLength)));
  } catch {
    throw new D1ProtocolError(400, "invalid-json", "D1 actor query metadata must be valid JSON");
  }
  let query;
  try {
    query = normalizeQueryRequest(decodeD1QueryRequest(bytes.subarray(4 + metadataLength)));
  } catch (err) {
    if (err instanceof D1ProtocolError) throw err;
    const message = sharedErrorMessage(err);
    throw new D1ProtocolError(400, "invalid-body", `D1 actor query body is invalid: ${message}`);
  }
  const record = /** @type {ActorMetadata} */ (Object(metadata));
  return {
    ...query,
    owner: record.owner,
    ...(record.__control == null ? {} : { __control: record.__control }),
    ...(record.__holdMs == null ? {} : { __holdMs: record.__holdMs }),
  };
}

/** @param {Request} request @param {{ maxBytes?: number }} [options] */
export async function readD1ActorQueryRequest(request, { maxBytes = D1_MAX_QUERY_ENVELOPE_BYTES } = {}) {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.split(";", 1)[0].trim().toLowerCase() !== D1_ACTOR_QUERY_CONTENT_TYPE) {
    throw new D1ProtocolError(
      415,
      "unsupported-media-type",
      `D1 actor query endpoint requires ${D1_ACTOR_QUERY_CONTENT_TYPE}`
    );
  }
  return decodeActorEnvelope(await readBoundedBytes(request, maxBytes, "D1 actor query"));
}

/** @param {Request} request @param {{ maxBytes?: number, label?: string }} [options] */
export async function readD1JsonObjectRequest(request, {
  maxBytes = D1_MAX_QUERY_ENVELOPE_BYTES,
  label = "D1 JSON",
} = {}) {
  try {
    const body = JSON.parse(utf8Decoder.decode(await readBoundedBytes(request, maxBytes, label)));
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      throw new D1ProtocolError(400, "invalid-json", `${label} body must be a JSON object`);
    }
    return body;
  } catch (err) {
    if (err instanceof D1ProtocolError) throw err;
    throw new D1ProtocolError(400, "invalid-json", `${label} body must be valid JSON`);
  }
}

/** @param {Request} request @param {{ maxBytes?: number }} [options] */
export async function readD1ActorControlRequest(request, { maxBytes = D1_MAX_QUERY_ENVELOPE_BYTES } = {}) {
  return readD1JsonObjectRequest(request, { maxBytes, label: "D1 actor control" });
}

const OWNERSHIP_CODES = new Set([
  "not-owner",
  "owner-not-ready",
  "owner-unavailable",
  "owner-record-invalid",
  "owner-endpoint-missing",
  "owner-endpoint-invalid",
  "forward-hop-exhausted",
  "owner-claim-raced",
  "owner-takeover-raced",
  "owner-rebalance-raced",
  "owner-release-raced",
  "owner-renew-raced",
  "owner-lease-expired",
  "owner-lease-too-short",
  "lease-budget-exhausted",
  "task-draining",
]);

/** @param {unknown} err */
function errorMessage(err) {
  const message = sharedErrorMessage(err);
  return message || "D1 operation failed";
}

/**
 * @param {unknown} err
 * @returns {ClassifiedD1Error}
 */
export function classifyD1Error(err) {
  const message = errorMessage(err);
  if (err instanceof D1ProtocolError) {
    const errRecord = /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (err));
    if (err.code === "batch-statement-error") {
      return {
        status: err.status,
        code: err.code,
        category: typeof errRecord.category === "string" ? errRecord.category : "sql",
        retryable: errRecord.retryable === true,
        message,
      };
    }
    if (OWNERSHIP_CODES.has(err.code)) {
      return {
        status: err.status >= 500 ? err.status : 503,
        code: err.code,
        category: "ownership",
        retryable: true,
        message: `D1 database is temporarily unavailable while ownership is changing; retry the request (${err.code}).`,
      };
    }
    if (err.code === "registry-unavailable") {
      return {
        status: 503,
        code: err.code,
        category: "ownership",
        retryable: true,
        message: "D1 ownership registry is unavailable; retry the request.",
      };
    }
    if (err.code === "result-unknown") {
      return {
        status: 503,
        code: err.code,
        category: "result-unknown",
        retryable: false,
        message,
      };
    }
    if (err.code === "limit-exceeded") {
      return {
        status: err.status,
        code: err.code,
        category: "limit",
        retryable: false,
        message,
      };
    }
    return {
      status: err.status,
      code: err.code,
      category: "invalid-request",
      retryable: false,
      message,
    };
  }

  if (/quota|SQLITE_FULL|database or disk is full|too large/i.test(message)) {
    return {
      status: 507,
      code: "quota-exceeded",
      category: "quota",
      retryable: false,
      message: `D1 quota exceeded: ${message}`,
    };
  }
  if (/timeout|timed out|deadline|AbortError/i.test(message)) {
    return {
      status: 504,
      code: "timeout",
      category: "timeout",
      retryable: false,
      message: "D1 request timed out; write outcome may be unknown, do not blindly retry non-idempotent requests.",
    };
  }
  if (/D1_TYPE_ERROR/i.test(message)) {
    return {
      status: 400,
      code: "invalid-parameter",
      category: "invalid-request",
      retryable: false,
      message,
    };
  }
  if (/D1_LIMIT_ERROR/i.test(message)) {
    return {
      status: 413,
      code: "limit-exceeded",
      category: "limit",
      retryable: false,
      message,
    };
  }
  if (/SQLITE_|sqlite|sql|syntax error|no such table|constraint/i.test(message)) {
    return {
      status: 400,
      code: "sql-error",
      category: "sql",
      retryable: false,
      message: `SQL error: ${message}`,
    };
  }
  return {
    status: 500,
    code: "internal-error",
    category: "internal",
    retryable: false,
    message: "D1 internal error.",
  };
}

/** @param {unknown} err */
export function d1ErrorPayload(err) {
  const classified = classifyD1Error(err);
  /** @type {{
   *   success: false,
   *   error: string,
   *   message: string,
   *   category: string,
   *   retryable: boolean,
   *   statementIndex?: number,
   *   causeCode?: string,
   * }}
   */
  const payload = {
    success: false,
    error: classified.code,
    message: classified.message,
    category: classified.category,
    retryable: classified.retryable,
  };
  if (err && typeof err === "object") {
    const errRecord = /** @type {Record<string, unknown>} */ (err);
    if (Number.isInteger(errRecord.statementIndex)) payload.statementIndex = /** @type {number} */ (errRecord.statementIndex);
    if (typeof errRecord.causeCode === "string") payload.causeCode = errRecord.causeCode;
  }
  return payload;
}

/** @param {unknown} err */
export function d1ErrorResponse(err) {
  const classified = classifyD1Error(err);
  return new Response(encodeD1QueryResponse(d1ErrorPayload(err)), {
    status: classified.status,
    headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
  });
}
