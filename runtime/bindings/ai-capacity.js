import { aiRuntimeSetting } from "shared-ai-runtime-config";
import { metrics } from "runtime-metrics";
import { serviceNameFromEnv } from "runtime-bindings-proxy";

/** @typedef {"request" | "stream" | "websocket"} AiPoolName */
/** @typedef {{ inUse: number, highWater: number }} AiPoolState */
/** @typedef {Record<string, unknown> & { SERVICE_NAME?: unknown }} AiCapacityEnv */
/** @typedef {{ ctx: { waitUntil(promise: Promise<unknown>): void }, env: AiCapacityEnv }} AiCapacityHost */

const poolStates = Object.freeze({
  request: { inUse: 0, highWater: 0 },
  stream: { inUse: 0, highWater: 0 },
  websocket: { inUse: 0, highWater: 0 },
});

/** @param {AiCapacityEnv} env @param {AiPoolName} pool */
function poolLimit(env, pool) {
  if (pool === "request") {
    return aiRuntimeSetting(env, "AI_REQUEST_MAX_IN_FLIGHT");
  }
  if (pool === "stream") {
    return aiRuntimeSetting(env, "AI_STREAM_MAX_IN_FLIGHT");
  }
  return aiRuntimeSetting(env, "AI_WS_MAX_SESSIONS");
}

/** @param {AiCapacityEnv} env */
export function prepareAiCapacityMetrics(env) {
  const service = serviceNameFromEnv(env);
  for (const [pool, state] of Object.entries(poolStates)) {
    const labels = { service, pool };
    metrics.setGauge("ai_pool_in_use", labels, state.inUse);
    metrics.setGauge("ai_pool_high_water", labels, state.highWater);
  }
}

/**
 * Register the deadline task before resolution or provider I/O. The task owns
 * the final abort/release path when caller cancellation is not delivered.
 *
 * @param {AiCapacityHost} binding
 * @param {AiPoolName} pool
 * @param {number} durationMs
 * @param {() => void} onDeadline
 */
export function acquireAiLease(binding, pool, durationMs, onDeadline) {
  const env = binding.env;
  const state = poolStates[pool];
  if (state.inUse >= poolLimit(env, pool)) {
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool, outcome: "saturated",
    });
    return null;
  }
  state.inUse += 1;
  state.highWater = Math.max(state.highWater, state.inUse);
  metrics.increment("ai_pool_events", {
    service: serviceNameFromEnv(env), pool, outcome: "acquired",
  });

  /** @type {AiPoolName} */
  let activePool = pool;
  let released = false;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let timer = null;
  /** @type {() => void} */
  let settle = () => {};
  /** @type {Promise<void>} */
  const task = new Promise((resolve) => { settle = () => resolve(); });

  /** @param {string} outcome */
  const release = (outcome) => {
    if (released) return false;
    released = true;
    if (timer !== null) clearTimeout(timer);
    timer = null;
    const activeState = poolStates[activePool];
    activeState.inUse = Math.max(0, activeState.inUse - 1);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: activePool, outcome,
    });
    settle();
    return true;
  };

  /** @param {AiPoolName} nextPool */
  const transfer = (nextPool) => {
    if (released) return false;
    if (nextPool === activePool) return true;
    const nextState = poolStates[nextPool];
    if (nextState.inUse >= poolLimit(env, nextPool)) {
      metrics.increment("ai_pool_events", {
        service: serviceNameFromEnv(env), pool: nextPool, outcome: "saturated",
      });
      return false;
    }
    const previousPool = activePool;
    const previousState = poolStates[previousPool];
    previousState.inUse = Math.max(0, previousState.inUse - 1);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: previousPool, outcome: "transferred",
    });

    activePool = nextPool;
    nextState.inUse += 1;
    nextState.highWater = Math.max(nextState.highWater, nextState.inUse);
    metrics.increment("ai_pool_events", {
      service: serviceNameFromEnv(env), pool: nextPool, outcome: "acquired",
    });
    return true;
  };

  /** @param {number} ms */
  const schedule = (ms) => {
    if (released) return;
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      if (released) return;
      try { onDeadline(); } catch {}
      release("deadline");
    }, ms);
  };
  schedule(durationMs);
  try {
    binding.ctx.waitUntil(task);
  } catch (err) {
    release("setup_error");
    throw err;
  }
  return { release, schedule, transfer, get released() { return released; } };
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function resetAiPoolStateForTest() {
  for (const state of Object.values(poolStates)) {
    state.inUse = 0;
    state.highWater = 0;
  }
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function aiPoolStateForTest() {
  return Object.fromEntries(Object.entries(poolStates).map(([name, state]) => [name, { ...state }]));
}
