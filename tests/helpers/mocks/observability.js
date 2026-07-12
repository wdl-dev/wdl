// Tests that drive logger or metrics spies inline their own variant — the
// spy shape is per-test.

import { moduleDataUrl, repositoryFileUrl } from "../load-shared-module.js";

const SHARED_OBSERVABILITY_URL = repositoryFileUrl("shared/observability.js");

const OBSERVABILITY_NOOP_SOURCE = String.raw`
import { formatError, sanitizeRequestId } from ${JSON.stringify(SHARED_OBSERVABILITY_URL)};

export { formatError, sanitizeRequestId };

export class MetricsRegistry {
  increment() {}
  observe() {}
  setGauge() {}
}
export function createLogger() {
  return function log() {};
}
export function createLogLevelBinder() {
  return function bindLogLevel() {};
}
export function recordRedisCommand() {}
export function recordRequestComplete() {}
export function ensureRequestId(headersLike) {
  if (!headersLike) return generateRequestId();
  const raw = typeof headersLike.get === "function"
    ? headersLike.get("x-request-id")
    : headersLike["x-request-id"];
  return sanitizeRequestId(raw) || generateRequestId();
}
export function generateRequestId() {
  return "rid";
}
export function logStructured() {}
export function setLogLevel() {}
`;

export const OBSERVABILITY_NOOP_URL = moduleDataUrl(OBSERVABILITY_NOOP_SOURCE);
