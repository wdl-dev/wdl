import { metrics } from "runtime-metrics";
import { utf8ByteLength } from "shared-utf8";

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
  if (Array.isArray(value)) {
    let json = "[";
    for (let i = 0; i < value.length; i += 1) {
      if (i > 0) json += ",";
      json += canonicalJsonValue(value[i]);
    }
    return `${json}]`;
  }
  if (value && typeof value === "object") {
    const record = /** @type {Record<string, unknown>} */ (value);
    const keys = Object.keys(record)
      .map((key) => ({ key, bytes: utf8Encoder.encode(key) }));
    keys.sort(compareEncodedKeys);
    let json = "{";
    for (let i = 0; i < keys.length; i += 1) {
      if (i > 0) json += ",";
      const key = keys[i].key;
      json += `${JSON.stringify(key)}:${canonicalJsonValue(record[key])}`;
    }
    return `${json}}`;
  }
  return typeof value === "number" ? canonicalJsonNumber(value) : JSON.stringify(value);
}

export const WORKFLOW_REPLAY_PAGE_SIZE = 64;
export const WORKFLOW_REPLAY_CACHE_MAX_INSTANCES = 256;
export const WORKFLOW_REPLAY_CACHE_MAX_BYTES = 16 * 1024 * 1024;
export const WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES = 64 * 1024 * 1024;
const WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE = 256;

export class WorkflowReplayCapacityError extends Error {
  constructor() {
    super("Workflow replay working-set capacity is exhausted");
    this.name = "WorkflowReplayCapacityError";
  }
}
/**
 * @typedef {{
 *   name?: unknown,
 *   nameCount?: unknown,
 *   dependencies?: unknown,
 *   kind?: unknown,
 *   config?: unknown,
 *   status?: unknown,
 *   outputJson?: string,
 *   error?: { name?: unknown, message?: unknown } | null,
 * }} WorkflowReplayStepRecord
 * @typedef {WorkflowReplayStepRecord & {
 *   ordinal?: number,
 *   output?: unknown,
 *   [key: string]: unknown,
 * }} WorkflowReplayStepInput
 * @typedef {{ key: string, lastRunToken: string, steps: Map<number, WorkflowReplayStepRecord>, nextOrdinal: number, complete: boolean, bytes: number, activeControllers: number, released: boolean }} WorkflowReplayCache
 */

/** @type {Map<string, WorkflowReplayCache>} */
const workflowReplayCaches = new Map();
/** @type {Set<WorkflowReplayCache>} */
const detachedReplayCaches = new Set();
/** @type {Set<{ reserve: (bytes: number) => void, release: () => void }>} */
const replayReadLeases = new Set();
let workflowReplayCacheSteps = 0;
let workflowReplayCacheBytes = 0;
let workflowReplayActiveBytes = 0;
let workflowReplayDetachedBytes = 0;
let workflowReplayReadBytes = 0;
let workflowReplayHighWaterBytes = 0;
/** @type {WeakMap<WorkflowReplayStepRecord, number>} */
let workflowReplayStepBytes = new WeakMap();

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function _resetWorkflowReplayCacheForTest() {
  for (const lease of replayReadLeases) lease.release();
  for (const cache of new Set([...workflowReplayCaches.values(), ...detachedReplayCaches])) {
    cache.steps.clear();
    cache.bytes = 0;
    cache.activeControllers = 0;
    cache.released = true;
  }
  workflowReplayCaches.clear();
  detachedReplayCaches.clear();
  workflowReplayCacheSteps = 0;
  workflowReplayCacheBytes = 0;
  workflowReplayActiveBytes = 0;
  workflowReplayDetachedBytes = 0;
  workflowReplayReadBytes = 0;
  workflowReplayHighWaterBytes = 0;
  workflowReplayStepBytes = new WeakMap();
}

export function prepareWorkflowReplayCacheMetrics() {
  metrics.setGauge("workflow_replay_cache_instances", {}, workflowReplayCaches.size);
  metrics.setGauge("workflow_replay_cache_steps", {}, workflowReplayCacheSteps);
  metrics.setGauge("workflow_replay_cache_bytes", {}, workflowReplayCacheBytes);
  metrics.setGauge("workflow_replay_active_bytes", {}, workflowReplayActiveBytes);
  metrics.setGauge("workflow_replay_detached_bytes", {}, workflowReplayDetachedBytes);
  metrics.setGauge("workflow_replay_read_in_flight_bytes", {}, workflowReplayReadBytes);
  metrics.setGauge("workflow_replay_working_set_bytes", {}, replayWorkingSetBytes());
  metrics.setGauge("workflow_replay_working_set_high_water_bytes", {}, workflowReplayHighWaterBytes);
}

function replayWorkingSetBytes() {
  return workflowReplayCacheBytes + workflowReplayDetachedBytes + workflowReplayReadBytes;
}

function recordReplayHighWater() {
  workflowReplayHighWaterBytes = Math.max(workflowReplayHighWaterBytes, replayWorkingSetBytes());
}

/** @param {number} bytes @param {WorkflowReplayCache} [keep] */
function reserveWorkingSet(bytes, keep) {
  if (replayWorkingSetBytes() + bytes <= WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES) return;
  for (const [key, cache] of workflowReplayCaches) {
    if (cache === keep || cache.activeControllers > 0) continue;
    evictWorkflowReplayCache(key);
    if (replayWorkingSetBytes() + bytes <= WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES) return;
  }
  recordWorkflowReplayCacheOutcome("saturated");
  throw new WorkflowReplayCapacityError();
}

export function createWorkflowReplayReadLease() {
  let reservedBytes = 0;
  let released = false;
  const lease = {
    /** @param {number} bytes */
    reserve(bytes) {
      if (released) throw new WorkflowReplayCapacityError();
      reserveWorkingSet(Math.max(0, bytes - reservedBytes));
      workflowReplayReadBytes += bytes - reservedBytes;
      reservedBytes = bytes;
      recordReplayHighWater();
    },
    release() {
      if (released) return;
      released = true;
      workflowReplayReadBytes -= reservedBytes;
      replayReadLeases.delete(lease);
    },
  };
  replayReadLeases.add(lease);
  return lease;
}

/** @param {string} outcome */
export function recordWorkflowReplayCacheOutcome(outcome) {
  metrics.increment("workflow_replay_cache", { outcome });
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

/** @param {WorkflowReplayStepRecord} step */
function serializedReplayStepBytes(step) {
  const configBytes = typeof step.config === "string" ? utf8ByteLength(step.config) : 0;
  const outputBytes = typeof step.outputJson === "string" ? utf8ByteLength(step.outputJson) : 0;
  const metadataJson = JSON.stringify([
    step.name ?? null,
    step.nameCount ?? null,
    step.dependencies ?? null,
    step.kind ?? null,
    step.status ?? null,
    step.error ?? null,
  ]) ?? "null";
  return configBytes + outputBytes + utf8ByteLength(metadataJson);
}

/** @param {unknown} status @param {unknown} error */
function projectReplayStepError(status, error) {
  if (status !== "failed" || !error || typeof error !== "object") return undefined;
  const record = /** @type {Record<string, unknown>} */ (error);
  const name = typeof record.name === "string" ? record.name : undefined;
  const message = typeof record.message === "string" ? record.message : undefined;
  if (name !== undefined && message !== undefined) return { name, message };
  if (name !== undefined) return { name };
  if (message !== undefined) return { message };
  return undefined;
}

/** @param {WorkflowReplayCache} cache @param {number} ordinal */
function deleteReplayStep(cache, ordinal) {
  const step = cache.steps.get(ordinal);
  if (!step || !cache.steps.delete(ordinal)) return;
  const bytes = workflowReplayStepBytes.get(step) ?? 0;
  cache.bytes -= bytes;
  if (cache.activeControllers > 0) workflowReplayActiveBytes -= bytes;
  if (detachedReplayCaches.has(cache)) workflowReplayDetachedBytes -= bytes;
  if (workflowReplayCaches.get(cache.key) === cache) {
    workflowReplayCacheSteps -= 1;
    workflowReplayCacheBytes -= bytes;
  }
}

/** @param {WorkflowReplayCache} cache */
function clearWorkflowReplayCache(cache) {
  if (detachedReplayCaches.delete(cache)) workflowReplayDetachedBytes -= cache.bytes;
  cache.released = true;
  cache.steps.clear();
  cache.nextOrdinal = 0;
  cache.complete = false;
  cache.bytes = 0;
}

/** @param {string} key */
function evictWorkflowReplayCache(key) {
  const cache = workflowReplayCaches.get(key);
  if (!cache) return;
  workflowReplayCacheSteps -= cache.steps.size;
  workflowReplayCacheBytes -= cache.bytes;
  workflowReplayCaches.delete(key);
  if (cache.activeControllers === 0) {
    clearWorkflowReplayCache(cache);
  } else {
    detachedReplayCaches.add(cache);
    workflowReplayDetachedBytes += cache.bytes;
  }
}

function evictOldestWorkflowReplayCache() {
  const oldest = workflowReplayCaches.keys().next().value;
  if (oldest !== undefined) evictWorkflowReplayCache(oldest);
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
    bytes: 0,
    activeControllers: 0,
    released: false,
  };
  workflowReplayCaches.set(key, created);
  while (workflowReplayCaches.size > WORKFLOW_REPLAY_CACHE_MAX_INSTANCES) {
    evictOldestWorkflowReplayCache();
  }
  return created;
}

/** @param {{ ns: string, workflowKey: string, instanceId: string, generation: number, createdAtMs: number, runToken: string }} run */
export function acquireWorkflowReplayCache(run) {
  const cache = getWorkflowReplayCache(run);
  if (cache.activeControllers === 0) workflowReplayActiveBytes += cache.bytes;
  cache.activeControllers += 1;
  return cache;
}

/** @param {WorkflowReplayCache} cache */
export function releaseWorkflowReplayCache(cache) {
  if (cache.activeControllers > 0) {
    cache.activeControllers -= 1;
    if (cache.activeControllers === 0) workflowReplayActiveBytes -= cache.bytes;
  }
  if (cache.activeControllers > 0 || workflowReplayCaches.get(cache.key) === cache) return;
  clearWorkflowReplayCache(cache);
}

/**
 * @param {WorkflowReplayCache} cache
 * @param {number} ordinal
 * @param {WorkflowReplayStepInput} step
 */
export function rememberWorkflowReplayStep(cache, ordinal, step) {
  if (cache.released) return;
  const countInGlobalCache = workflowReplayCaches.get(cache.key) === cache;
  /** @type {WorkflowReplayStepRecord} */
  const storedStep = {
    name: step.name,
    nameCount: step.nameCount,
    dependencies: step.dependencies,
    kind: step.kind,
    config: step.config,
    status: step.status,
    outputJson: step.outputJson,
  };
  const error = projectReplayStepError(step.status, step.error);
  if (error !== undefined) storedStep.error = error;
  if (storedStep.status === "completed" && typeof storedStep.outputJson !== "string") {
    storedStep.outputJson = JSON.stringify(step.output ?? null) ?? "null";
  }
  const bytes = serializedReplayStepBytes(storedStep);
  if (bytes > WORKFLOW_REPLAY_CACHE_MAX_BYTES) {
    deleteReplayStep(cache, ordinal);
    return;
  }
  const previous = cache.steps.get(ordinal);
  const oldest = !previous && cache.steps.size >= WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE
    ? cache.steps.keys().next().value
    : undefined;
  const evicted = oldest === undefined ? undefined : cache.steps.get(oldest);
  const replacedBytes = (previous ? workflowReplayStepBytes.get(previous) ?? 0 : 0) +
    (evicted ? workflowReplayStepBytes.get(evicted) ?? 0 : 0);
  reserveWorkingSet(Math.max(0, bytes - replacedBytes), cache);
  deleteReplayStep(cache, ordinal);
  if (oldest !== undefined) deleteReplayStep(cache, oldest);
  workflowReplayStepBytes.set(storedStep, bytes);
  cache.steps.set(ordinal, storedStep);
  cache.bytes += bytes;
  if (cache.activeControllers > 0) workflowReplayActiveBytes += bytes;
  if (detachedReplayCaches.has(cache)) workflowReplayDetachedBytes += bytes;
  if (countInGlobalCache) {
    workflowReplayCacheSteps += 1;
    workflowReplayCacheBytes += bytes;
  }
  recordReplayHighWater();
  if (countInGlobalCache) {
    while (workflowReplayCacheBytes > WORKFLOW_REPLAY_CACHE_MAX_BYTES) {
      evictOldestWorkflowReplayCache();
    }
  }
}

/** @param {WorkflowReplayStepRecord} step */
export function readWorkflowReplayStepOutput(step) {
  return typeof step.outputJson === "string" ? JSON.parse(step.outputJson) : null;
}
