import assert from "node:assert/strict";
import { test } from "node:test";
import {
  freshModuleDataUrl,
  importRepositoryModuleFresh,
  readRepositoryJson,
  repositoryFileUrl,
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
  const mod = await importRepositoryModuleFresh("runtime/dispatch/workflow-replay-cache.js", [
    [/from "runtime-metrics";/, `from ${JSON.stringify(metricsUrl)};`],
    [/from "shared-utf8";/, `from ${JSON.stringify(repositoryFileUrl("shared/utf8.js"))};`],
  ]);
  metricsState().prepare = mod.prepareWorkflowReplayCacheMetrics;
  return mod;
}

const CREATED_AT_BASE_MS = 1700000000000;
const WORKFLOW_REPLAY_CACHE_STEP_LIMIT = 256;

test("replay working-set admission counts detached state and releases the last controller", async () => {
  const mod = await loadReplayCacheModule();
  /** @type {import("../../runtime/dispatch/workflow-replay-cache.js").WorkflowReplayCache[]} */
  const caches = [];
  let shared;
  for (let index = 0; index < 6; index += 1) {
    const cache = mod.acquireWorkflowReplayCache(run(index));
    if (index === 0) shared = mod.acquireWorkflowReplayCache(run(index));
    mod.rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "x".repeat(8 * 1024 * 1024) });
    caches.push(cache);
  }
  assert.equal(shared, caches[0]);
  assert.ok(shared);
  const activeBytes = caches.reduce((sum, cache) => sum + cache.bytes, 0);
  assert.equal(latestGauge("workflow_replay_active_bytes"), activeBytes);
  assert.equal(latestGauge("workflow_replay_working_set_bytes"), activeBytes);
  assert.ok(latestGauge("workflow_replay_detached_bytes") > 0);
  assert.equal(
    latestGauge("workflow_replay_cache_bytes") + latestGauge("workflow_replay_detached_bytes"),
    activeBytes,
  );
  const lease = mod.createWorkflowReplayReadLease();
  lease.reserve(mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES - activeBytes);
  assert.throws(() => mod.rememberWorkflowReplayStep(caches[0], 1, { status: "completed", output: "new" }), mod.WorkflowReplayCapacityError);
  assert.equal(caches[0].steps.size, 1);
  assert.equal(latestGauge("workflow_replay_working_set_bytes"), mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES);
  mod.releaseWorkflowReplayCache(caches[0]);
  assert.equal(latestGauge("workflow_replay_active_bytes"), activeBytes);
  const firstBytes = shared.bytes;
  mod.releaseWorkflowReplayCache(shared);
  assert.equal(latestGauge("workflow_replay_active_bytes"), activeBytes - firstBytes);
  assert.equal(shared.bytes, 0);
  mod.rememberWorkflowReplayStep(caches[1], 1, { status: "completed", output: "recovered" });
  for (const cache of caches.slice(1)) mod.releaseWorkflowReplayCache(cache);
  lease.release();
  lease.release();
  assert.equal(latestGauge("workflow_replay_active_bytes"), 0);
  assert.equal(latestGauge("workflow_replay_detached_bytes"), 0);
  assert.equal(latestGauge("workflow_replay_read_in_flight_bytes"), 0);
  assert.equal(latestGauge("workflow_replay_working_set_bytes"), latestGauge("workflow_replay_cache_bytes"));
  assert.equal(latestGauge("workflow_replay_working_set_high_water_bytes"), mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES);
  assert.ok(metricsState().increments.some((/** @type {any} */ entry) => entry.labels.outcome === "saturated"));
});

test("replay read admission evicts unused retained caches and shrinks unknown-length reservations", async () => {
  const mod = await loadReplayCacheModule();
  const cache = mod.getWorkflowReplayCache(run(0));
  mod.rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "retained" });
  const lease = mod.createWorkflowReplayReadLease();
  lease.reserve(mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES);
  assert.equal(cache.released, true);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), 0);
  lease.reserve(128);
  assert.equal(latestGauge("workflow_replay_read_in_flight_bytes"), 128);
  lease.release();
  assert.equal(latestGauge("workflow_replay_working_set_bytes"), 0);
  assert.equal(latestGauge("workflow_replay_working_set_high_water_bytes"), mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES);
});

test("replay admission preserves replaced records on refusal and accounts for step eviction", async () => {
  const mod = await loadReplayCacheModule();
  const cache = mod.acquireWorkflowReplayCache(run(0));
  for (let ordinal = 0; ordinal < WORKFLOW_REPLAY_CACHE_STEP_LIMIT; ordinal += 1) {
    mod.rememberWorkflowReplayStep(cache, ordinal, { status: "completed", output: "old" });
  }
  const lease = mod.createWorkflowReplayReadLease();
  lease.reserve(mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES - cache.bytes);
  assert.throws(() => mod.rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "larger" }), mod.WorkflowReplayCapacityError);
  assert.equal(mod.readWorkflowReplayStepOutput(cache.steps.get(0)), "old");
  mod.rememberWorkflowReplayStep(cache, WORKFLOW_REPLAY_CACHE_STEP_LIMIT, { status: "completed", output: "old" });
  assert.equal(cache.steps.has(0), false);
  assert.equal(cache.steps.size, WORKFLOW_REPLAY_CACHE_STEP_LIMIT);
  assert.equal(latestGauge("workflow_replay_working_set_bytes"), mod.WORKFLOW_REPLAY_WORKING_SET_MAX_BYTES);
  mod.releaseWorkflowReplayCache(cache);
  lease.release();
});

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
  metricsState().prepare();
  const entries = metricsState().gauges.filter((/** @type {any} */ entry) => entry.name === name);
  assert.ok(entries.length > 0, `missing ${name} gauge`);
  return entries.at(-1).value;
}

/**
 * @param {string} name
 * @param {number} startIndex
 */
function latestGaugeSince(name, startIndex) {
  metricsState().prepare();
  const entries = metricsState().gauges
    .slice(startIndex)
    .filter((/** @type {any} */ entry) => entry.name === name);
  assert.ok(entries.length > 0, `missing ${name} gauge in this test`);
  return entries.at(-1).value;
}

test("workflow replay cache gauges track step count across replacement and eviction", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    acquireWorkflowReplayCache,
    getWorkflowReplayCache,
    readWorkflowReplayStepOutput,
    releaseWorkflowReplayCache,
    rememberWorkflowReplayStep,
    recordWorkflowReplayCacheOutcome,
    WORKFLOW_REPLAY_CACHE_MAX_INSTANCES,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();
  const workflowReplayCachePrefillCount = WORKFLOW_REPLAY_CACHE_MAX_INSTANCES - 1;

  const cache = acquireWorkflowReplayCache(run(0));
  const cacheHit = getWorkflowReplayCache(run(0));
  assert.equal(cacheHit, cache);
  assert.equal(metricsState().gauges.length, 0);
  assert.equal(latestGauge("workflow_replay_cache_instances"), 1);
  const gaugesBeforeMutation = metricsState().gauges.length;
  rememberWorkflowReplayStep(cache, 0, { status: "completed" });
  rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "updated" });
  assert.equal(metricsState().gauges.length, gaugesBeforeMutation);
  const gaugeCountBeforeOutcomes = metricsState().gauges.length;
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
  assert.equal(metricsState().gauges.length, gaugeCountBeforeOutcomes);
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
  const byteGaugeBeforeStaleWrite = latestGauge("workflow_replay_cache_bytes");
  assert.equal(cache.steps.size, 2);
  rememberWorkflowReplayStep(cache, 2, { status: "completed", output: "local-after-eviction" });
  assert.equal(cache.steps.size, 3);
  assert.equal(readWorkflowReplayStepOutput(cache.steps.get(2)), "local-after-eviction");
  assert.equal(latestGauge("workflow_replay_cache_instances"), instanceGaugeBeforeStaleWrite);
  assert.equal(latestGauge("workflow_replay_cache_steps"), stepGaugeBeforeStaleWrite);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), byteGaugeBeforeStaleWrite);
  releaseWorkflowReplayCache(cache);
  assert.equal(cache.steps.size, 0);
  assert.equal(cache.bytes, 0);
  const currentRun0Cache = getWorkflowReplayCache(run(0));
  assert.equal(currentRun0Cache.steps.has(2), false);
  assert.notEqual(currentRun0Cache, cache);
});

test("workflow replay cache clears released controller state on later global eviction", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    acquireWorkflowReplayCache,
    getWorkflowReplayCache,
    releaseWorkflowReplayCache,
    rememberWorkflowReplayStep,
    WORKFLOW_REPLAY_CACHE_MAX_INSTANCES,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();

  const cache = acquireWorkflowReplayCache(run(0));
  rememberWorkflowReplayStep(cache, 0, { status: "completed", output: "retained" });
  releaseWorkflowReplayCache(cache);
  assert.equal(cache.steps.size, 1);

  for (let i = 1; i <= WORKFLOW_REPLAY_CACHE_MAX_INSTANCES; i += 1) {
    getWorkflowReplayCache(run(i));
  }
  assert.equal(cache.steps.size, 0);
  assert.equal(cache.bytes, 0);

  rememberWorkflowReplayStep(cache, 1, { status: "completed", output: "late" });
  assert.equal(cache.steps.size, 0);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), 0);
});

test("workflow replay cache keeps detached state until every controller releases it", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    acquireWorkflowReplayCache,
    getWorkflowReplayCache,
    releaseWorkflowReplayCache,
    rememberWorkflowReplayStep,
    WORKFLOW_REPLAY_CACHE_MAX_INSTANCES,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();

  const firstController = acquireWorkflowReplayCache(run(0));
  const secondController = acquireWorkflowReplayCache(run(0));
  assert.equal(secondController, firstController);
  rememberWorkflowReplayStep(firstController, 0, {
    status: "completed",
    output: "shared",
  });

  for (let i = 1; i <= WORKFLOW_REPLAY_CACHE_MAX_INSTANCES; i += 1) {
    getWorkflowReplayCache(run(i));
  }
  releaseWorkflowReplayCache(firstController);
  assert.equal(firstController.steps.size, 1);

  releaseWorkflowReplayCache(secondController);
  assert.equal(firstController.steps.size, 0);
  assert.equal(firstController.bytes, 0);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), 0);
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
    acquireWorkflowReplayCache,
    getWorkflowReplayCache,
    readWorkflowReplayStepOutput,
    releaseWorkflowReplayCache,
    rememberWorkflowReplayStep,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();
  const gaugeStartIndex = metricsState().gauges.length;

  const oldClaim = getWorkflowReplayCache(runWithToken(0, "run-old"));
  const mutableOutput = {
    nested: { value: 1 },
    items: ["persisted"],
  };
  rememberWorkflowReplayStep(oldClaim, 0, { status: "completed", output: mutableOutput });
  rememberWorkflowReplayStep(oldClaim, 1, { status: "completed", output: "cached-1" });
  rememberWorkflowReplayStep(oldClaim, 2, { status: "completed", output: "cached-2" });
  oldClaim.complete = true;
  mutableOutput.nested.value = 999;
  mutableOutput.items.push("mutated");

  const newClaim = acquireWorkflowReplayCache(runWithToken(0, "run-new"));
  assert.equal(oldClaim, newClaim);
  assert.equal(newClaim.complete, false);
  releaseWorkflowReplayCache(newClaim);
  assert.equal(newClaim.steps.size, 3);
  const firstRead = readWorkflowReplayStepOutput(newClaim.steps.get(0));
  assert.deepEqual(firstRead, {
    nested: { value: 1 },
    items: ["persisted"],
  });
  firstRead.nested.value = 999;
  firstRead.items.push("mutated");
  assert.deepEqual(readWorkflowReplayStepOutput(newClaim.steps.get(0)), {
    nested: { value: 1 },
    items: ["persisted"],
  });
  assert.equal(latestGaugeSince("workflow_replay_cache_instances", gaugeStartIndex), 1);
  assert.equal(latestGaugeSince("workflow_replay_cache_steps", gaugeStartIndex), 3);

  newClaim.complete = true;
  assert.equal(getWorkflowReplayCache(runWithToken(0, "run-new")).complete, true);
  assert.equal(latestGaugeSince("workflow_replay_cache_instances", gaugeStartIndex), 1);
  assert.equal(latestGaugeSince("workflow_replay_cache_steps", gaugeStartIndex), 3);
});

test("workflow replay cache bounds retained serialized bytes and resets its gauge", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    acquireWorkflowReplayCache,
    getWorkflowReplayCache,
    releaseWorkflowReplayCache,
    rememberWorkflowReplayStep,
    WORKFLOW_REPLAY_CACHE_MAX_BYTES,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();

  const first = acquireWorkflowReplayCache(run(0));
  const retained = JSON.stringify("a".repeat(Math.floor(WORKFLOW_REPLAY_CACHE_MAX_BYTES * 0.6)));
  rememberWorkflowReplayStep(first, 0, {
    ordinal: 0,
    name: "large",
    dependencies: [],
    config: "null",
    status: "completed",
    outputJson: retained,
  });
  const firstBytes = latestGauge("workflow_replay_cache_bytes");
  assert.ok(firstBytes > 0 && firstBytes <= WORKFLOW_REPLAY_CACHE_MAX_BYTES);

  const second = getWorkflowReplayCache(run(1));
  rememberWorkflowReplayStep(second, 0, {
    ordinal: 0,
    name: "large",
    dependencies: [],
    config: "null",
    status: "completed",
    outputJson: retained,
  });
  assert.equal(latestGauge("workflow_replay_cache_bytes"), firstBytes);
  assert.equal(second.steps.size, 1);
  assert.equal(first.steps.size, 1);
  releaseWorkflowReplayCache(first);
  assert.equal(first.steps.size, 0);
  const reloadedFirst = getWorkflowReplayCache(run(0));
  assert.notEqual(reloadedFirst, first);
  assert.equal(reloadedFirst.steps.size, 0);

  const oversized = getWorkflowReplayCache(run(2));
  rememberWorkflowReplayStep(oversized, 0, {
    ordinal: 0,
    name: "oversized",
    dependencies: [],
    config: "null",
    status: "completed",
    outputJson: "x".repeat(WORKFLOW_REPLAY_CACHE_MAX_BYTES + 1),
  });
  assert.equal(oversized.steps.size, 0);
  assert.ok(latestGauge("workflow_replay_cache_bytes") <= WORKFLOW_REPLAY_CACHE_MAX_BYTES);

  _resetWorkflowReplayCacheForTest();
  const projected = getWorkflowReplayCache(run(3));
  const oversizedMetadata = "x".repeat(WORKFLOW_REPLAY_CACHE_MAX_BYTES + 1);
  rememberWorkflowReplayStep(projected, 0, {
    ordinal: 0,
    name: "projected",
    nameCount: 1,
    dependencies: [],
    config: "null",
    status: "failed",
    error: {
      name: "RangeError",
      message: "bad input",
      details: oversizedMetadata,
    },
    attempt: 7,
    dueAtMs: CREATED_AT_BASE_MS,
    unconsumed: oversizedMetadata,
  });
  const projectedStep = projected.steps.get(0);
  assert.ok(projectedStep);
  assert.equal(Object.hasOwn(projectedStep, "ordinal"), false);
  assert.equal(Object.hasOwn(projectedStep, "attempt"), false);
  assert.equal(Object.hasOwn(projectedStep, "dueAtMs"), false);
  assert.equal(Object.hasOwn(projectedStep, "unconsumed"), false);
  assert.deepEqual(projectedStep.error, {
    name: "RangeError",
    message: "bad input",
  });

  rememberWorkflowReplayStep(projected, 1, {
    status: "waiting",
    error: {
      name: "RetryableError",
      message: oversizedMetadata,
    },
  });
  const waitingStep = projected.steps.get(1);
  assert.ok(waitingStep);
  assert.equal(Object.hasOwn(waitingStep, "error"), false);

  rememberWorkflowReplayStep(projected, 2, {
    status: "failed",
    error: {
      name: { nested: oversizedMetadata },
      message: [oversizedMetadata],
    },
  });
  const invalidErrorStep = projected.steps.get(2);
  assert.ok(invalidErrorStep);
  assert.equal(Object.hasOwn(invalidErrorStep, "error"), false);
  assert.ok(latestGauge("workflow_replay_cache_bytes") < 1024);

  _resetWorkflowReplayCacheForTest();
  assert.equal(latestGauge("workflow_replay_cache_bytes"), 0);
});

test("workflow replay cache byte gauge stays exact across replacement and step eviction", async () => {
  const {
    _resetWorkflowReplayCacheForTest,
    getWorkflowReplayCache,
    rememberWorkflowReplayStep,
  } = await loadReplayCacheModule();
  _resetWorkflowReplayCacheForTest();

  const replacement = {
    status: "completed",
    outputJson: JSON.stringify("replacement-output"),
  };
  const cache = getWorkflowReplayCache(run(0));
  rememberWorkflowReplayStep(cache, 0, {
    status: "completed",
    outputJson: JSON.stringify("initial"),
  });
  rememberWorkflowReplayStep(cache, 0, replacement);
  const replacementBytes = latestGauge("workflow_replay_cache_bytes");
  assert.equal(cache.steps.size, 1);
  assert.equal(latestGauge("workflow_replay_cache_steps"), 1);

  _resetWorkflowReplayCacheForTest();
  const reference = getWorkflowReplayCache(run(1));
  rememberWorkflowReplayStep(reference, 0, replacement);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), replacementBytes);

  _resetWorkflowReplayCacheForTest();
  const bounded = getWorkflowReplayCache(run(2));
  const retainedStep = {
    status: "completed",
    outputJson: JSON.stringify("retained"),
  };
  for (let ordinal = 0; ordinal < WORKFLOW_REPLAY_CACHE_STEP_LIMIT; ordinal += 1) {
    rememberWorkflowReplayStep(bounded, ordinal, retainedStep);
  }
  const bytesAtLimit = latestGauge("workflow_replay_cache_bytes");
  assert.equal(bounded.steps.size, WORKFLOW_REPLAY_CACHE_STEP_LIMIT);
  assert.equal(latestGauge("workflow_replay_cache_steps"), WORKFLOW_REPLAY_CACHE_STEP_LIMIT);

  rememberWorkflowReplayStep(bounded, WORKFLOW_REPLAY_CACHE_STEP_LIMIT, retainedStep);
  assert.equal(bounded.steps.size, WORKFLOW_REPLAY_CACHE_STEP_LIMIT);
  assert.equal(bounded.steps.has(0), false);
  assert.equal(bounded.steps.has(WORKFLOW_REPLAY_CACHE_STEP_LIMIT), true);
  assert.equal(latestGauge("workflow_replay_cache_steps"), WORKFLOW_REPLAY_CACHE_STEP_LIMIT);
  assert.equal(latestGauge("workflow_replay_cache_bytes"), bytesAtLimit);
});
