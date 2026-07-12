import { jsonError } from "shared-respond";

// Control owns this base abort contract. Routing and auth remain separate;
// deploy subclasses it where commit cleanup needs its own catch boundary.

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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
  const message = err instanceof ControlAbort
    ? (typeof err.details.message === "string" && err.details.message) || err.message || code
    : recordMessage || (typeof details.message === "string" && details.message) || code;
  return jsonError(
    status,
    code,
    message,
    { ...details, ...extraDetails },
  );
}

/**
 * @param {ControlAbort} err
 * @param {Record<string, unknown>} [extraDetails]
 */
export function controlAbortResponse(err, extraDetails = {}) {
  return codedErrorResponse(err, err.code, extraDetails);
}
