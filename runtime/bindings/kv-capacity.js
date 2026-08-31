import { runtimeInfrastructureError } from "runtime-infrastructure-error";
import { metrics } from "runtime-metrics";
import { serviceNameFromEnv } from "runtime-bindings-proxy";

export const KV_READ_IN_FLIGHT_MAX_BYTES = 32 * 1024 * 1024;
export const KV_READ_LEASE_MAX_MS = 5_000;
export const KV_READ_CAPACITY_ERROR_MESSAGE = "KV read capacity is exhausted";
export const KV_READ_TIMEOUT_ERROR_MESSAGE = "KV read response timed out";

const state = { inUseBytes: 0, highWaterBytes: 0 };

/** @param {Response} response */
function responseContentLength(response) {
  const raw = response.headers.get("content-length");
  if (raw !== null && /^(?:0|[1-9][0-9]*)$/.test(raw)) {
    const value = Number(raw);
    if (Number.isSafeInteger(value)) return value;
  }
  return null;
}

/**
 * @param {{ env: Record<string, unknown>, ctx: { waitUntil(promise: Promise<unknown>): void } }} binding
 * @param {Response} response
 * @param {() => void} onDeadline
 */
export function acquireKvReadLease(binding, response, onDeadline) {
  const contentLength = responseContentLength(response);
  const bytes = contentLength === null
    ? KV_READ_IN_FLIGHT_MAX_BYTES
    : Math.min(KV_READ_IN_FLIGHT_MAX_BYTES, Math.max(1, contentLength));
  const service = serviceNameFromEnv(binding.env);
  if (state.inUseBytes + bytes > KV_READ_IN_FLIGHT_MAX_BYTES) {
    metrics.increment("kv_read_capacity_events", { service, outcome: "saturated" });
    return null;
  }

  state.inUseBytes += bytes;
  state.highWaterBytes = Math.max(state.highWaterBytes, state.inUseBytes);
  metrics.increment("kv_read_capacity_events", { service, outcome: "acquired" });
  let released = false;
  const { promise: task, resolve: settle } = Promise.withResolvers();
  const timer = setTimeout(() => {
    if (released) return;
    try { onDeadline(); } catch {}
    release("deadline");
  }, KV_READ_LEASE_MAX_MS);
  const release = (outcome = "completed") => {
    if (released) return false;
    released = true;
    clearTimeout(timer);
    state.inUseBytes = Math.max(0, state.inUseBytes - bytes);
    metrics.increment("kv_read_capacity_events", { service, outcome });
    settle(undefined);
    return true;
  };
  try {
    binding.ctx.waitUntil(task);
  } catch (error) {
    release("setup_error");
    throw error;
  }
  return {
    bytes,
    contentLength,
    release,
  };
}

/** @param {Record<string, unknown>} env */
export function prepareKvReadCapacityMetrics(env) {
  const labels = { service: serviceNameFromEnv(env) };
  metrics.setGauge("kv_read_in_flight_bytes", labels, state.inUseBytes);
  metrics.setGauge("kv_read_in_flight_high_water_bytes", labels, state.highWaterBytes);
}

export function kvReadCapacityError() {
  return runtimeInfrastructureError(
    KV_READ_CAPACITY_ERROR_MESSAGE
  );
}

export function kvReadTimeoutError() {
  return runtimeInfrastructureError(
    KV_READ_TIMEOUT_ERROR_MESSAGE
  );
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function resetKvReadCapacityForTest() {
  state.inUseBytes = 0;
  state.highWaterBytes = 0;
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function kvReadCapacityStateForTest() {
  return { ...state };
}
