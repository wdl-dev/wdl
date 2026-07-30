import { metrics } from "runtime-metrics";

const utf8Encoder = new TextEncoder();

/** @param {{ bytes: Uint8Array }} left @param {{ bytes: Uint8Array }} right */
function compareEncodedKeys(left, right) {
  const length = Math.min(left.bytes.length, right.bytes.length);
  for (let i = 0; i < length; i += 1) {
    if (left.bytes[i] !== right.bytes[i]) return left.bytes[i] - right.bytes[i];
  }
  return left.bytes.length - right.bytes.length;
}

/** @param {number} value */
function canonicalJsonNumber(value) {
  return JSON.stringify(value);
}

/** @param {unknown} value @returns {string} */
export function canonicalJson(value) {
  const json = JSON.stringify(value);
  const normalized = json === undefined ? null : JSON.parse(json);
  return canonicalJsonValue(normalized);
}

/** @param {unknown} value @returns {string} */
function canonicalJsonValue(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJsonValue).join(",")}]`;
  if (value && typeof value === "object") {
    const record = /** @type {Record<string, unknown>} */ (value);
    const keys = Object.keys(record)
      .map((key) => ({ key, bytes: utf8Encoder.encode(key) }));
    keys.sort(compareEncodedKeys);
    return `{${keys.map(({ key }) => `${JSON.stringify(key)}:${canonicalJsonValue(record[key])}`).join(",")}}`;
  }
  return typeof value === "number" ? canonicalJsonNumber(value) : JSON.stringify(value);
}

export const WORKFLOW_REPLAY_PAGE_SIZE = 64;
export const WORKFLOW_REPLAY_CACHE_MAX_INSTANCES = 256;
const WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE = 256;
/**
 * @typedef {{
 *   ordinal?: number,
 *   name?: unknown,
 *   nameCount?: unknown,
 *   dependencies?: unknown,
 *   config?: unknown,
 *   status?: unknown,
 *   output?: unknown,
 *   error?: { name?: unknown, message?: unknown } | null,
 *   dueAtMs?: unknown,
 *   [key: string]: unknown,
 * }} WorkflowReplayStepRecord
 * @typedef {{ key: string, lastRunToken: string, steps: Map<number, WorkflowReplayStepRecord>, nextOrdinal: number, complete: boolean }} WorkflowReplayCache
 */

/** @type {Map<string, WorkflowReplayCache>} */
const workflowReplayCaches = new Map();
let workflowReplayCacheSteps = 0;

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function _resetWorkflowReplayCacheForTest() {
  workflowReplayCaches.clear();
  workflowReplayCacheSteps = 0;
  recordWorkflowReplayCacheSize();
}

function recordWorkflowReplayCacheSize() {
  metrics.setGauge("workflow_replay_cache_instances", {}, workflowReplayCaches.size);
  metrics.setGauge("workflow_replay_cache_steps", {}, workflowReplayCacheSteps);
}

/** @param {string} outcome */
export function recordWorkflowReplayCacheOutcome(outcome) {
  metrics.increment("workflow_replay_cache", { outcome });
  recordWorkflowReplayCacheSize();
}

/** @param {{ ns: string, workflowKey: string, instanceId: string, generation: number, createdAtMs: number }} run */
function workflowReplayCacheKey(run) {
  return `${run.ns}\t${run.workflowKey}\t${run.instanceId}\t${run.generation}\t${run.createdAtMs}`;
}

/** @param {{ ns: string, workflowKey: string, instanceId: string, generation: number, createdAtMs: number, runToken: string }} run */
export function workflowReplayIdentity(run) {
  return {
    ns: run.ns,
    workflowKey: run.workflowKey,
    instanceId: run.instanceId,
    generation: run.generation,
    createdAtMs: run.createdAtMs,
    runToken: run.runToken,
  };
}

/** @param {{ ns: string, workflowKey: string, instanceId: string, generation: number, createdAtMs: number, runToken: string }} run */
export function getWorkflowReplayCache(run) {
  const key = workflowReplayCacheKey(run);
  const existing = workflowReplayCaches.get(key);
  if (existing) {
    workflowReplayCaches.delete(key);
    workflowReplayCaches.set(key, existing);
    if (existing.lastRunToken !== run.runToken) {
      existing.lastRunToken = run.runToken;
      existing.complete = false;
    }
    return existing;
  }
  const created = {
    key,
    lastRunToken: run.runToken,
    steps: new Map(),
    nextOrdinal: 0,
    complete: false,
  };
  workflowReplayCaches.set(key, created);
  while (workflowReplayCaches.size > WORKFLOW_REPLAY_CACHE_MAX_INSTANCES) {
    const oldest = workflowReplayCaches.keys().next().value;
    if (oldest === undefined) break;
    const evicted = workflowReplayCaches.get(oldest);
    if (evicted) workflowReplayCacheSteps -= evicted.steps.size;
    workflowReplayCaches.delete(oldest);
  }
  recordWorkflowReplayCacheSize();
  return created;
}

/**
 * @param {WorkflowReplayCache} cache
 * @param {number} ordinal
 * @param {WorkflowReplayStepRecord} step
 */
export function rememberWorkflowReplayStep(cache, ordinal, step) {
  const countInGlobalCache = workflowReplayCaches.get(cache.key) === cache;
  if (cache.steps.has(ordinal)) {
    cache.steps.delete(ordinal);
  } else if (countInGlobalCache) {
    workflowReplayCacheSteps += 1;
  }
  cache.steps.set(ordinal, step);
  while (cache.steps.size > WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE) {
    const oldest = cache.steps.keys().next().value;
    if (oldest === undefined) break;
    if (cache.steps.delete(oldest) && countInGlobalCache) workflowReplayCacheSteps -= 1;
  }
  recordWorkflowReplayCacheSize();
}
