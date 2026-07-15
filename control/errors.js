import { jsonError } from "shared-respond";
import { errorMessage } from "shared-errors";

// Control owns this base abort contract. Routing and auth remain separate;
// deploy subclasses it where commit cleanup needs its own catch boundary.

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

const SERVER_DIAGNOSTIC_DETAIL_KEYS = new Set([
  "cause",
  "detail",
  "error_detail",
  "stack",
  "stage",
]);
const CODED_ERROR_LOG_CONTEXT_RESERVED_KEYS = new Set([
  ...SERVER_DIAGNOSTIC_DETAIL_KEYS,
  "code",
  "error_message",
  "message",
  "metadata_version",
  "reason",
  "status",
  "version",
]);
const CONTROL_LOG_STRING_MAX_CHARS = 2048;
const CONTROL_LOG_TRUNCATION_SUFFIX = "...";

/** @param {string} value */
function boundedControlLogString(value) {
  if (value.length <= CONTROL_LOG_STRING_MAX_CHARS) return value;
  const prefixLength = CONTROL_LOG_STRING_MAX_CHARS - CONTROL_LOG_TRUNCATION_SUFFIX.length;
  return `${value.slice(0, prefixLength)}${CONTROL_LOG_TRUNCATION_SUFFIX}`;
}

/** @param {unknown} value */
function codedErrorLogContext(value) {
  if (!isRecord(value)) return {};
  return Object.fromEntries(
    Object.entries(value).filter(([key]) => !CODED_ERROR_LOG_CONTEXT_RESERVED_KEYS.has(key))
  );
}

/**
 * @param {number} status
 * @param {Record<string, unknown>} details
 */
function publicErrorDetails(status, details) {
  if (status < 500) return details;
  return Object.fromEntries(
    Object.entries(details).filter(([key]) => !SERVER_DIAGNOSTIC_DETAIL_KEYS.has(key))
  );
}

export class ControlAbort extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {{ message?: string, [key: string]: unknown }} [details]
   */
  constructor(status, code, details = {}) {
    super(details?.message || code);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

/**
 * Return the bounded diagnostics that a server-side coded error may add to a
 * structured log after those fields have been removed from the wire response.
 *
 * @param {ControlAbort} err
 * @returns {Record<string, string>}
 */
export function controlAbortLogDetails(err) {
  if (err.status < 500 || !isRecord(err.details)) return {};
  /** @type {Record<string, string>} */
  const out = {};
  if (typeof err.details.version === "string") {
    out.metadata_version = boundedControlLogString(err.details.version);
  }
  if (typeof err.details.stage === "string") {
    out.stage = boundedControlLogString(err.details.stage);
  }
  const detail = typeof err.details.error_detail === "string"
    ? err.details.error_detail
    : err.details.detail;
  if (typeof detail === "string") {
    out.error_detail = boundedControlLogString(detail);
  }
  return out;
}

/**
 * @param {unknown} err
 * @param {string} [fallbackCode]
 * @param {{ errorDetail?: string, context?: Record<string, unknown> }} [diagnostics]
 * @returns {{ status: number, reason: string, error_message: string, [key: string]: unknown }}
 */
export function codedErrorLogFields(err, fallbackCode = "internal_error", diagnostics = {}) {
  const record = isRecord(err) ? err : {};
  const status = typeof record.status === "number" ? record.status : 500;
  const reason = typeof record.code === "string" && record.code ? record.code : fallbackCode;
  const message = typeof record.message === "string" ? record.message : errorMessage(err);
  return {
    ...codedErrorLogContext(diagnostics.context),
    status,
    reason: boundedControlLogString(reason),
    error_message: boundedControlLogString(message),
    ...(err instanceof ControlAbort ? controlAbortLogDetails(err) : {}),
    ...(typeof diagnostics.errorDetail === "string"
      ? { error_detail: boundedControlLogString(diagnostics.errorDetail) }
      : {}),
  };
}

// Domain errors flow through jsonError() so details cannot shadow the
// top-level wire contract.
/**
 * @param {unknown} err
 * @param {string} [fallbackCode]
 * @param {Record<string, unknown>} [extraDetails]
 */
export function codedErrorResponse(err, fallbackCode = "internal_error", extraDetails = {}) {
  const record = isRecord(err) ? err : {};
  const details = isRecord(record.details) ? record.details : {};
  const status = typeof record.status === "number" ? record.status : 500;
  const code = typeof record.code === "string" && record.code ? record.code : fallbackCode;
  const recordMessage = typeof record.message === "string" && record.message ? record.message : undefined;
  let message = "Internal error";
  if (status < 500) {
    message = err instanceof ControlAbort
      ? (typeof err.details.message === "string" && err.details.message) || err.message || code
      : recordMessage || (typeof details.message === "string" && details.message) || code;
  }
  return jsonError(
    status,
    code,
    message,
    publicErrorDetails(status, { ...details, ...extraDetails }),
  );
}

/**
 * Log a secret-envelope provider failure internally while returning only the
 * stable coded 503 contract to the caller.
 *
 * @param {{
 *   err: import("shared-secret-envelope").SecretEnvelopeError,
 *   log: (level: string, event: string, fields: Record<string, unknown>) => void,
 *   event: string,
 *   fields: Record<string, unknown>,
 *   responseDetails?: Record<string, unknown>,
 * }} args
 */
export function secretEnvelopeErrorResponse({
  err,
  log,
  event,
  fields,
  responseDetails = {},
}) {
  const diagnostic = errorMessage(err);
  log("error", event, {
    ...fields,
    ...codedErrorLogFields(
      { status: 503, code: err.code, message: diagnostic },
      err.code,
      { errorDetail: diagnostic }
    ),
  });
  return codedErrorResponse({ status: 503, code: err.code }, err.code, responseDetails);
}

/**
 * @param {ControlAbort} err
 * @param {Record<string, unknown>} [extraDetails]
 */
export function controlAbortResponse(err, extraDetails = {}) {
  return codedErrorResponse(err, err.code, extraDetails);
}
