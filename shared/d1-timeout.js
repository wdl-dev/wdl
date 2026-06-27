import { envValueOr } from "shared-env";
import { errorMessage } from "./errors.js";

export const DEFAULT_D1_QUERY_TIMEOUT_MS = 30000;

/**
 * @param {{ D1_QUERY_TIMEOUT_MS?: unknown } | null | undefined} env
 * @returns {number}
 */
export function d1QueryTimeoutMs(env) {
  const raw = Number(envValueOr(env?.D1_QUERY_TIMEOUT_MS, DEFAULT_D1_QUERY_TIMEOUT_MS));
  if (!Number.isInteger(raw) || raw <= 0) return DEFAULT_D1_QUERY_TIMEOUT_MS;
  return raw;
}

/**
 * @param {{ D1_QUERY_TIMEOUT_MS?: unknown } | null | undefined} env
 * @returns {{ signal: AbortSignal, clear(): void }}
 */
export function createD1QueryDeadline(env) {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new DOMException("The operation was aborted due to timeout", "TimeoutError"));
  }, d1QueryTimeoutMs(env));
  return {
    signal: controller.signal,
    clear() {
      clearTimeout(timer);
    },
  };
}

/**
 * @param {unknown} err
 * @returns {boolean}
 */
export function isD1QueryTimeoutError(err) {
  const name = err instanceof Error ? err.name : "";
  const message = errorMessage(err);
  return /timeout|timed out|deadline|AbortError|TimeoutError/i.test(`${name} ${message}`);
}

export function d1QueryTimeoutPayload() {
  return {
    success: false,
    error: "timeout",
    message: "D1 request timed out; write outcome may be unknown, do not blindly retry non-idempotent requests.",
    category: "timeout",
    retryable: false,
  };
}

/** @returns {{ success: false, error: "backend-unavailable", message: string, category: "internal", retryable: true }} */
export function d1BackendUnavailablePayload() {
  return {
    success: false,
    error: "backend-unavailable",
    message: "D1 backend is unavailable.",
    category: "internal",
    retryable: true,
  };
}

/** @returns {{ success: false, error: "result-unknown", message: string, category: "result-unknown", retryable: false }} */
export function d1ResultUnknownPayload() {
  return {
    success: false,
    error: "result-unknown",
    message: "D1 owner response was lost after the request was sent; outcome may be unknown, do not blindly retry non-idempotent requests.",
    category: "result-unknown",
    retryable: false,
  };
}
