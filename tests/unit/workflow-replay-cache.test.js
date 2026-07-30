import assert from "node:assert/strict";
import { test } from "node:test";
import {
  freshModuleDataUrl,
  importRepositoryModuleFresh,
  readRepositoryJson,
} from "../helpers/load-shared-module.js";

const CANONICAL_JSON_PARITY_CASES = readRepositoryJson(
  "tests/fixtures/workflow-canonical-json-parity.json"
);

function metricsState() {
  return /** @type {any} */ (globalThis).__workflowReplayCacheMetrics;
}

async function loadReplayCacheModule() {
  /** @type {any} */ (globalThis).__workflowReplayCacheMetrics = {
    /** @type {Array<{ name: string, labels: any, value: number }>} */
    gauges: [],
    /** @type {Array<{ name: string, labels: any }>} */
    increments: [],
    /**
     * @param {string} name
     * @param {any} labels
     * @param {number} value
     */
    setGauge(name, labels, value) {
      this.gauges.push({ name, labels, value });
    },
    /**
     * @param {string} name
     * @param {any} labels
     */
    increment(name, labels) {
      this.increments.push({ name, labels });
    },
  };
  // Re-snapshot the metrics binding each load so test isolation works:
  // the metrics module evaluates `globalThis.__...` once at import time.
  const metricsUrl = freshModuleDataUrl(
    "export const metrics = globalThis.__workflowReplayCacheMetrics;"
  );
  return await importRepositoryModuleFresh("runtime/dispatch/workflow-replay-cache.js", [
    [/from "runtime-metrics";/, `from ${JSON.stringify(metricsUrl)};`],
  ]);
}

const CREATED_AT_BASE_MS = 1700000000000;

/** @param {number} index */
function run(index) {
  return {
    ns: "demo",
    workflowKey: "wf",
    instanceId: `inst-${index}`,
    generation: 1,
    createdAtMs: CREATED_AT_BASE_MS + index,
    runToken: "run",
  };
}

/**
 * @param {number} index
 * @param {string} runToken
 */
function runWithToken(index, runToken) {
  return { ...run(index), runToken };
}

/** @param {string} name */
function latestGauge(name) {
  const entries = metricsState().gauges.filter((/** @type {any} */ entry) => entry.name === name);
  assert.ok(entries.length > 0, `missing ${name} gauge`);
  return entries.at(-1).value;
}

/**
 * @param {string} name
 * @param {number} startIndex
 */
function latestGaugeSince(name, startIndex) {
  const entries = metricsState().gauges
    .slice(startIndex)
    .filter((/** @type {any} */ entry) => entry.name === name);
  assert.ok(entries.length > 0, `missing ${name} gauge in this test`);
  return entries.at(-1).value;
}

test("workflow replay cache gauges track step count across replacement and eviction", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    getWorkflowReplayCache,
    rememberWorkflowReplayStep,
    recordWorkflowReplayCacheOutcome,
    WORKFLOW_REPLAY_CACHE_MAX_INSTANCES,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();
  const workflowReplayCachePrefillCount = WORKFLOW_REPLAY_CACHE_MAX_INSTANCES - 1;

  const cache = getWorkflowReplayCache(run(0));
  const cacheHit = getWorkflowReplayCache(run(0));
  assert.equal(cacheHit, cache);
  assert.equal(latestGauge("workflow_replay_cache_instances"), 1);
  rememberWorkflowReplayStep(cache, 0, { status: "completed" });
  rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "updated" });
  recordWorkflowReplayCacheOutcome("hit");
  assert.equal(
    metricsState().increments.some((/** @type {any} */ entry) => entry.name === "workflow_replay_cache" && entry.labels?.outcome === "hit"),
    true,
  );
  recordWorkflowReplayCacheOutcome("miss");
  assert.equal(
    metricsState().increments.some((/** @type {any} */ entry) => entry.name === "workflow_replay_cache" && entry.labels?.outcome === "miss"),
    true,
  );
  assert.equal(latestGauge("workflow_replay_cache_instances"), 1);
  assert.equal(latestGauge("workflow_replay_cache_steps"), 1);
  rememberWorkflowReplayStep(cache, 1, { status: "completed", output: "second-step" });
  assert.equal(latestGauge("workflow_replay_cache_steps"), 2);

  for (let i = 1; i <= workflowReplayCachePrefillCount; i += 1) {
    const next = getWorkflowReplayCache(run(i));
    rememberWorkflowReplayStep(next, 0, { status: "completed" });
  }
  assert.equal(latestGauge("workflow_replay_cache_instances"), WORKFLOW_REPLAY_CACHE_MAX_INSTANCES);
  assert.equal(latestGauge("workflow_replay_cache_steps"), WORKFLOW_REPLAY_CACHE_MAX_INSTANCES + 1);

  const evicting = getWorkflowReplayCache(run(WORKFLOW_REPLAY_CACHE_MAX_INSTANCES));
  rememberWorkflowReplayStep(evicting, 0, { status: "completed" });
  assert.equal(latestGauge("workflow_replay_cache_instances"), WORKFLOW_REPLAY_CACHE_MAX_INSTANCES);
  assert.equal(latestGauge("workflow_replay_cache_steps"), WORKFLOW_REPLAY_CACHE_MAX_INSTANCES);

  const instanceGaugeBeforeStaleWrite = latestGauge("workflow_replay_cache_instances");
  const stepGaugeBeforeStaleWrite = latestGauge("workflow_replay_cache_steps");
  // Evicted handles may still serve their in-flight dispatch locally, but they
  // must not be reinserted into or counted by the shared replay cache gauges.
  rememberWorkflowReplayStep(cache, 1, { status: "completed", output: "stale-after-eviction" });
  assert.equal(cache.steps.has(1), true);
  assert.equal(latestGauge("workflow_replay_cache_instances"), instanceGaugeBeforeStaleWrite);
  assert.equal(latestGauge("workflow_replay_cache_steps"), stepGaugeBeforeStaleWrite);
  const currentRun0Cache = getWorkflowReplayCache(run(0));
  assert.equal(currentRun0Cache.steps.has(1), false);
  assert.notEqual(currentRun0Cache, cache);
});

test("workflow replay canonical JSON matches Rust canonical form for cache comparisons", async () => {
  const { canonicalJson } = await loadReplayCacheModule();
  const arrayWithHole = new Array(3);
  arrayWithHole[0] = undefined;
  arrayWithHole[2] = 2;

  assert.equal(
    canonicalJson({ b: undefined, a: 1, c: arrayWithHole }),
    "{\"a\":1,\"c\":[null,null,2]}"
  );
  for (const entry of CANONICAL_JSON_PARITY_CASES) {
    const actual = canonicalJson(JSON.parse(entry.rawJson));
    assert.equal(actual, entry.jsExpected, entry.id);
    if (!entry.knownDivergence) {
      assert.equal(entry.jsExpected, entry.rustExpected, entry.id);
    }
  }
});

test("workflow replay cache reuses entries across claims in one incarnation", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    getWorkflowReplayCache,
    rememberWorkflowReplayStep,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();
  const gaugeStartIndex = metricsState().gauges.length;

  const oldClaim = getWorkflowReplayCache(runWithToken(0, "run-old"));
  rememberWorkflowReplayStep(oldClaim, 0, { status: "completed", output: "cached-0" });
  rememberWorkflowReplayStep(oldClaim, 1, { status: "completed", output: "cached-1" });
  rememberWorkflowReplayStep(oldClaim, 2, { status: "completed", output: "cached-2" });
  oldClaim.complete = true;

  const newClaim = getWorkflowReplayCache(runWithToken(0, "run-new"));
  assert.equal(oldClaim, newClaim);
  assert.equal(newClaim.complete, false);
  assert.equal(newClaim.steps.get(0)?.output, "cached-0");
  assert.equal(latestGaugeSince("workflow_replay_cache_instances", gaugeStartIndex), 1);
  assert.equal(latestGaugeSince("workflow_replay_cache_steps", gaugeStartIndex), 3);

  newClaim.complete = true;
  assert.equal(getWorkflowReplayCache(runWithToken(0, "run-new")).complete, true);
  assert.equal(latestGaugeSince("workflow_replay_cache_instances", gaugeStartIndex), 1);
  assert.equal(latestGaugeSince("workflow_replay_cache_steps", gaugeStartIndex), 3);
});
