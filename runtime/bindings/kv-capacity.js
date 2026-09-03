import { runtimeInfrastructureError } from "runtime-infrastructure-error";
import { metrics } from "runtime-metrics";
import { serviceNameFromEnv } from "runtime-bindings-proxy";

export const KV_READ_IN_FLIGHT_MAX_BYTES = 32 * 1024 * 1024;
export const KV_READ_DEADLINE_MS = 5_000;
export const KV_READ_CAPACITY_ERROR_MESSAGE = "KV read capacity is exhausted";
export const KV_READ_TIMEOUT_ERROR_MESSAGE = "KV read response timed out";

const state = { inUseBytes: 0, highWaterBytes: 0 };

/** @param {Response} response */
function responseContentLength(response) {
  const contentEncoding = response.headers.get("content-encoding");
  if (
    contentEncoding !== null &&
    contentEncoding.trim().toLowerCase() !== "identity"
  ) {
    return null;
  }
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
 * @param {AbortSignal} deadlineSignal
 */
export function acquireKvReadLease(binding, response, deadlineSignal) {
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
  const release = (outcome = "completed") => {
    if (released) return false;
    released = true;
    deadlineSignal.removeEventListener("abort", onAbort);
    state.inUseBytes = Math.max(0, state.inUseBytes - bytes);
    metrics.increment("kv_read_capacity_events", { service, outcome });
    settle(undefined);
    return true;
  };
  const onAbort = () => { release("deadline"); };
  deadlineSignal.addEventListener("abort", onAbort, { once: true });
  if (deadlineSignal.aborted) onAbort();
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

/**
 * @template T
 * @param {(aborter: AbortController) => Promise<T>} callback
 * @returns {Promise<T>}
 */
export async function withKvReadDeadline(callback) {
  const aborter = new AbortController();
  /** @type {(reason?: unknown) => void} */
  let rejectDeadline = () => {};
  /** @type {Promise<never>} */
  const deadline = new Promise((_, reject) => { rejectDeadline = reject; });
  const timer = setTimeout(() => {
    const error = kvReadTimeoutError();
    rejectDeadline(error);
    aborter.abort(error);
  }, KV_READ_DEADLINE_MS);
  try {
    return await Promise.race([callback(aborter), deadline]);
  } finally {
    clearTimeout(timer);
  }
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

function kvReadTimeoutError() {
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
