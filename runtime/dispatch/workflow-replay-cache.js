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
const WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE = 256;
/**
 * @typedef {{
 *   name?: unknown,
 *   nameCount?: unknown,
 *   dependencies?: unknown,
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
 * @typedef {{ key: string, lastRunToken: string, steps: Map<number, WorkflowReplayStepRecord>, nextOrdinal: number, complete: boolean, bytes: number }} WorkflowReplayCache
 */

/** @type {Map<string, WorkflowReplayCache>} */
const workflowReplayCaches = new Map();
let workflowReplayCacheSteps = 0;
let workflowReplayCacheBytes = 0;
/** @type {WeakMap<WorkflowReplayStepRecord, number>} */
let workflowReplayStepBytes = new WeakMap();

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function _resetWorkflowReplayCacheForTest() {
  workflowReplayCaches.clear();
  workflowReplayCacheSteps = 0;
  workflowReplayCacheBytes = 0;
  workflowReplayStepBytes = new WeakMap();
  recordWorkflowReplayCacheSize();
}

function recordWorkflowReplayCacheSize() {
  metrics.setGauge("workflow_replay_cache_instances", {}, workflowReplayCaches.size);
  metrics.setGauge("workflow_replay_cache_steps", {}, workflowReplayCacheSteps);
  metrics.setGauge("workflow_replay_cache_bytes", {}, workflowReplayCacheBytes);
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

/** @param {WorkflowReplayStepRecord} step */
function serializedReplayStepBytes(step) {
  const configBytes = typeof step.config === "string" ? utf8ByteLength(step.config) : 0;
  const outputBytes = typeof step.outputJson === "string" ? utf8ByteLength(step.outputJson) : 0;
  const metadataJson = JSON.stringify([
    step.name ?? null,
    step.nameCount ?? null,
    step.dependencies ?? null,
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
  workflowReplayCacheSteps -= 1;
  workflowReplayCacheBytes -= bytes;
}

/** @param {string} key */
function evictWorkflowReplayCache(key) {
  const cache = workflowReplayCaches.get(key);
  if (!cache) return;
  workflowReplayCacheSteps -= cache.steps.size;
  workflowReplayCacheBytes -= cache.bytes;
  workflowReplayCaches.delete(key);
  cache.steps.clear();
  cache.bytes = 0;
  cache.complete = false;
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
  };
  workflowReplayCaches.set(key, created);
  while (workflowReplayCaches.size > WORKFLOW_REPLAY_CACHE_MAX_INSTANCES) {
    evictOldestWorkflowReplayCache();
  }
  recordWorkflowReplayCacheSize();
  return created;
}

/**
 * @param {WorkflowReplayCache} cache
 * @param {number} ordinal
 * @param {WorkflowReplayStepInput} step
 */
export function rememberWorkflowReplayStep(cache, ordinal, step) {
  if (workflowReplayCaches.get(cache.key) !== cache) return;
  /** @type {WorkflowReplayStepRecord} */
  const storedStep = {
    name: step.name,
    nameCount: step.nameCount,
    dependencies: step.dependencies,
    config: step.config,
    status: step.status,
    outputJson: step.outputJson,
  };
  const error = projectReplayStepError(step.status, step.error);
  if (error !== undefined) storedStep.error = error;
  if (storedStep.status === "completed" && typeof storedStep.outputJson !== "string") {
    storedStep.outputJson = JSON.stringify(step.output ?? null) ?? "null";
  }
  if (cache.steps.has(ordinal)) deleteReplayStep(cache, ordinal);
  const bytes = serializedReplayStepBytes(storedStep);
  if (bytes > WORKFLOW_REPLAY_CACHE_MAX_BYTES) {
    recordWorkflowReplayCacheSize();
    return;
  }
  workflowReplayStepBytes.set(storedStep, bytes);
  cache.steps.set(ordinal, storedStep);
  cache.bytes += bytes;
  workflowReplayCacheSteps += 1;
  workflowReplayCacheBytes += bytes;
  while (cache.steps.size > WORKFLOW_REPLAY_CACHE_MAX_STEPS_PER_INSTANCE) {
    const oldest = cache.steps.keys().next().value;
    if (oldest === undefined) break;
    deleteReplayStep(cache, oldest);
  }
  while (workflowReplayCacheBytes > WORKFLOW_REPLAY_CACHE_MAX_BYTES) {
    evictOldestWorkflowReplayCache();
  }
  recordWorkflowReplayCacheSize();
}

/** @param {WorkflowReplayStepRecord} step */
export function readWorkflowReplayStepOutput(step) {
  return typeof step.outputJson === "string" ? JSON.parse(step.outputJson) : null;
}
