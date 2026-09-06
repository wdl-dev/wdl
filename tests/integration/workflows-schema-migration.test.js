import assert from "node:assert/strict";
import { test } from "node:test";

import { readRepositoryJson, readRepositoryModuleSource } from "../helpers/load-shared-module.js";
import { composeRestart, composeRun, composeScale, composeStop } from "./helpers/compose.js";
import {
  DO_ALARM_WORKER, doAlarmJobId, doAlarmStateKey, redisAddDoAlarmDue,
  redisDoAlarmJobExists, redisDoAlarmReadyIncludes, waitForJson,
} from "./helpers/durable-objects.js";
import { deployAndPromote, gatewayFetch, readIntegrationJson, serviceInternalPost, setupIntegrationSuite, uniqueNs } from "./helpers/index.js";
import { parseJsonText } from "./helpers/json-payload.js";
import {
  redisCommand, redisDel, redisExists, redisFlushAll, redisGet, redisHGet, redisHGetAll, redisHGetJson,
  redisHSet, redisInfoInteger, redisKeys, redisSAdd, redisSet, redisZAdd, redisZScore,
} from "./helpers/redis.js";
import { workerMeta, workflowInstanceStateKey, workflowReadyShard, workflowReadyToken } from "./helpers/workflows-scenarios.js";

const DB = { db: 2 };
const ARCHIVE = { db: 15 };
const SCHEMA_KEY = "wf:schema_version";
const MIGRATION_KEY = "wf:schema3-migration";
const LONG_HISTORY_STEP_COUNT = 1500;
const CODE = readRepositoryModuleSource("test-workers/workflow-schema-migration/src/index.js");
const CONTRACT = /** @type {{ defaults: Record<string, any>, cases: any[], rawCases: any[] }} */ (
  readRepositoryJson("tests/fixtures/workflow-schema2-steps.json")
);

setupIntegrationSuite();

/** @param {"check"|"apply"|"resume"} mode @param {boolean} [deleteArchive] */
function migrate(mode, deleteArchive = false) {
  return parseJsonText(composeRun("workflows", [
    "/workflows", "schema3-migrate", mode, ...(deleteArchive ? ["--delete-archive"] : []),
  ]), `schema3-migrate ${mode}`);
}

function stopWriters() {
  composeStop("scheduler");
  composeStop("workflows");
}

function restoreStack() {
  redisFlushAll();
  redisSet(SCHEMA_KEY, "3", DB);
  composeScale("workflows", 1);
  composeScale("scheduler", 1);
}

/** @param {string} ns @param {string} workflowKey @param {string} id @param {string} suffix */
function instanceKey(ns, workflowKey, id, suffix) {
  return workflowInstanceStateKey(ns, workflowKey, id).slice(0, -"state".length) + suffix;
}

/** @param {string} ns @param {string} workflowKey @param {string} id */
function instancePayloadBytes(ns, workflowKey, id) {
  return ["payloads", "steps", "step-summaries", "events"].flatMap((suffix) => (
    Object.values(redisHGetAll(instanceKey(ns, workflowKey, id, suffix), DB))
  )).reduce((total, value) => total + Buffer.byteLength(value), 0);
}

/** @param {string} ns @param {string} path */
async function request(ns, path) {
  return readIntegrationJson(await gatewayFetch(ns, path), 200, path);
}

/** @param {string} ns */
async function deployWorkflow(ns) {
  const version = await deployAndPromote(ns, "workflow", {
    code: CODE,
    bindings: { CACHE: { type: "kv", id: "migration" } },
    workflows: [{ name: "migration", binding: "FLOW", className: "MigrationWorkflow" }],
  });
  return { version, workflowKey: workerMeta(ns, "workflow", version).workflows[0].workflowKey };
}

/** @param {string} ns @param {string} workflowKey @param {string} id */
function downgradeStepRecords(ns, workflowKey, id) {
  const key = (/** @type {string} */ suffix) => instanceKey(ns, workflowKey, id, suffix);
  const steps = redisHGetAll(key("steps"), DB);
  for (const [ordinal, raw] of Object.entries(steps)) {
    const step = parseJsonText(raw, "schema-3 step");
    delete step.kind;
    redisHSet(key("steps"), { [ordinal]: JSON.stringify(step) }, DB);
  }
  redisHSet(key("state"), { payloadBytes: String(instancePayloadBytes(ns, workflowKey, id)) }, DB);
}

test("migration restores normal Workflow replay, identity and lifecycle while retaining DB15", async () => {
  const ns = uniqueNs("migration");
  const { version, workflowKey } = await deployWorkflow(ns);
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js", modules: { "worker.js": DO_ALARM_WORKER },
    bindings: { ALARMS: { type: "do", className: "AlarmCounter" } },
  });
  for (const mode of ["wait", "sleep", "retry", "done", "buffered"]) {
    await request(ns, `/workflow/create?id=${mode}&mode=${mode === "buffered" ? "wait" : mode}`);
    await waitForJson(`pre-migration ${mode}`, () => request(ns, `/workflow/status?id=${mode}`),
      (status) => status.status === (mode === "done" ? "completed" : "waiting"), 15_000);
    assert.deepEqual(await request(ns, `/workflow/effect?id=${mode}`), { count: 1 });
  }
  await request(ns, "/workflow/pause?id=sleep");
  composeStop("scheduler");
  try {
    await request(ns, "/workflow/event?id=buffered");
    await request(ns, "/alarms/schedule-soon?name=migrated");
    composeStop("workflows");
    const expiredDue = Date.now() - 1;
    for (const mode of ["sleep", "retry"]) {
      const key = instanceKey(ns, workflowKey, mode, "steps");
      const record = redisHGetJson(key, "1", DB);
      record.dueAtMs = expiredDue;
      redisHSet(key, { 1: JSON.stringify(record) }, DB);
    }
    const retryDueKey = `wf:due:${workflowReadyShard(ns, workflowKey, "retry")}`;
    redisZAdd(retryDueKey, expiredDue, workflowReadyToken(ns, workflowKey, "retry"), DB);
    for (const mode of ["wait", "sleep", "retry", "done", "buffered"]) downgradeStepRecords(ns, workflowKey, mode);
    redisSet(SCHEMA_KEY, "2", DB);
    const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "migrated");
    const alarmKey = doAlarmStateKey(jobId);
    const expiry = Date.now() - 1;
    redisHSet(alarmKey, { status: "running", runToken: "old-dispatch", runLeaseExpiresAtMs: String(Date.now() + 60_000) }, DB);
    assert.throws(() => migrate("check"), /unexpired running DO alarm/);
    redisHSet(alarmKey, { runLeaseExpiresAtMs: String(expiry) }, DB);
    redisAddDoAlarmDue(expiry, jobId);
    assert.equal(redisDoAlarmReadyIncludes(jobId), false);

    const checked = migrate("check");
    assert.equal(checked.instanceCount, 5);
    assert.equal(checked.convertedStepCount, 9);
    assert.equal(checked.migrationState, "none");
    assert.equal(redisGet(MIGRATION_KEY), null);
    assert.equal(redisCommand(["DBSIZE"], ARCHIVE), "0");
    const oldWait = redisHGetAll(instanceKey(ns, workflowKey, "wait", "state"), DB);
    const applied = migrate("apply");
    assert.equal(applied.migrationState, "complete");
    assert.equal(applied.archiveDeleted, false);
    assert.equal(redisGet(SCHEMA_KEY, DB), "3");
    assert.equal(redisGet(SCHEMA_KEY, ARCHIVE), "2");
    assert.deepEqual(redisHGetAll(instanceKey(ns, workflowKey, "wait", "state"), ARCHIVE), oldWait);
    assert.deepEqual(redisHGetAll(alarmKey, DB), redisHGetAll(alarmKey, ARCHIVE));
    assert.equal(redisHGetJson(instanceKey(ns, workflowKey, "wait", "steps"), "1", DB).kind, "waitForEvent");
    assert.equal(redisHGet(instanceKey(ns, workflowKey, "sleep", "state"), "status", DB), "paused");
    assert.equal(redisZScore(retryDueKey, workflowReadyToken(ns, workflowKey, "retry"), DB), String(expiredDue));

    composeScale("workflows", 1);
    composeRestart("user-runtime");
    assert.equal((await request(ns, "/workflow/status?id=wait")).status, "waiting");
    const lifecycle = serviceInternalPost("workflows", 9120, "/internal/workflows/lifecycle/check-delete", { ns, worker: "workflow", version });
    assert.equal(lifecycle.status, 200, lifecycle.body);
    assert.equal(parseJsonText(lifecycle.body, "lifecycle result").allowed, false);
    await request(ns, "/workflow/create?id=done&mode=done");
    assert.deepEqual(await request(ns, "/workflow/effect?id=done"), { count: 1 });
    composeScale("scheduler", 1);
    await request(ns, "/workflow/event?id=wait");
    await waitForJson("migrated waiting Workflow completes", () => request(ns, "/workflow/status?id=wait"),
      (status) => status.status === "completed", 15_000);
    assert.deepEqual(await request(ns, "/workflow/effect?id=wait"), { count: 1 });
    assert.equal((await request(ns, "/workflow/status?id=sleep")).status, "paused");
    await request(ns, "/workflow/resume?id=sleep");
    for (const id of ["sleep", "retry", "buffered"]) {
      await waitForJson(`migrated ${id} completes`, () => request(ns, `/workflow/status?id=${id}`),
        (status) => status.status === "completed", 15_000);
      assert.deepEqual(await request(ns, `/workflow/effect?id=${id}`), { count: 1 });
    }
    await waitForJson("migrated alarm delivers", () => request(ns, "/alarms/status?name=migrated"),
      (status) => status.alarms === 1 && status.pending === null, 15_000);
    await request(ns, "/workflow/create?id=new&mode=done");
    await waitForJson("new Workflow completes", () => request(ns, "/workflow/status?id=new"),
      (status) => status.status === "completed", 15_000);
    assert.equal(migrate("apply").migrationState, "complete");
    assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "migrated"), false);
    assert.equal(migrate("apply", true).archiveDeleted, true);
    assert.equal(redisCommand(["DBSIZE"], ARCHIVE), "0");
    assert.equal((await request(ns, "/workflow/status?id=wait")).status, "completed");
    assert.equal(migrate("apply", true).migrationState, "complete");
    assert.throws(() => migrate("resume"), /No incomplete schema3 migration/);
  } finally { restoreStack(); }
});

/** @param {Record<string, any>} record */
function stepSummary(record) {
  return {
    ordinal: record.ordinal, name: record.stepName, nameCount: record.nameCount,
    dependencies: record.dependencies, status: record.status, attempt: record.attempt,
    ...(record.outputRef !== null ? { outputRef: record.outputRef } : {}),
    ...(record.errorRef !== null ? { errorRef: record.errorRef } : {}),
    hasOutput: record.outputRef !== null || Object.hasOwn(record, "output"),
    hasError: record.errorRef !== null || Object.hasOwn(record, "error"),
    ...(record.completedAtMs !== null ? { completedAtMs: record.completedAtMs } : {}),
    ...(record.failedAtMs !== null ? { failedAtMs: record.failedAtMs } : {}),
  };
}

/** @param {string} ns @param {string} workflowKey @param {string} version @param {any} entry */
function seedCase(ns, workflowKey, version, entry) {
  const id = entry.id;
  const key = (/** @type {string} */ suffix) => instanceKey(ns, workflowKey, id, suffix);
  const raw = entry.recordJson ?? JSON.stringify({ ...CONTRACT.defaults, ...entry.record });
  const record = parseJsonText(raw, "legacy step fixture");
  const payloads = { params: { id, mode: "done" }, ...entry.payloads };
  redisHSet(key("payloads"), Object.fromEntries(Object.entries(payloads).map(([field, value]) => [field, JSON.stringify(value)])), DB);
  redisHSet(key("steps"), { 0: raw }, DB);
  redisHSet(key("step-summaries"), { 0: JSON.stringify(stepSummary(record)) }, DB);
  redisZAdd(key("step-summary-index"), 0, "0", DB);
  if (entry.events) redisHSet(key("events"), Object.fromEntries(Object.entries(entry.events).map(([field, value]) => [field, JSON.stringify(value)])), DB);
  redisHSet(key("state"), {
    ns, worker: "workflow", frozenVersion: version, workflowName: "migration", workflowKey,
    className: "MigrationWorkflow", instanceId: id, status: "paused", generation: "1",
    createdAtMs: "100", updatedAtMs: "123", paramsRef: "params", payloadsKey: key("payloads"),
    payloadBytes: String(instancePayloadBytes(ns, workflowKey, id)), eventSeq: "1", successRetentionMs: "86400000", errorRetentionMs: "86400000",
  }, DB);
  redisSAdd(`wf:by-worker:${ns}:workflow`, `${workflowKey}\t${id}`, DB);
  redisSAdd(`wf:by-version:${ns}:workflow:${version}`, `${workflowKey}\t${id}`, DB);
  redisZAdd(`wf:by-workflow:${ns}:workflow:${workflowKey}`, 100, id, DB);
}

/** @param {string} ns @param {string} workflowKey @param {string} version */
function seedLongHistory(ns, workflowKey, version) {
  const id = "long-history";
  seedCase(ns, workflowKey, version, { ...CONTRACT.cases[0], id });
  const key = (/** @type {string} */ suffix) => instanceKey(ns, workflowKey, id, suffix);
  /** @type {Record<string, string>} */
  const steps = {};
  /** @type {Record<string, string>} */
  const summaries = {};
  const index = [];
  for (let ordinal = 0; ordinal < LONG_HISTORY_STEP_COUNT; ordinal++) {
    const record = { ...CONTRACT.defaults, ordinal, nameCount: ordinal + 1, dependencies: ordinal ? [ordinal - 1] : [], output: 1 };
    steps[ordinal] = JSON.stringify(record);
    summaries[ordinal] = JSON.stringify(stepSummary(record));
    index.push(String(ordinal), String(ordinal));
  }
  redisHSet(key("steps"), steps, DB);
  redisHSet(key("step-summaries"), summaries, DB);
  redisCommand(["ZADD", key("step-summary-index"), ...index], DB);
  redisHSet(key("state"), { payloadBytes: String(instancePayloadBytes(ns, workflowKey, id)) }, DB);
}

test("migration preflights every legacy step kind and rejects damaged state before swapping", async () => {
  const ns = uniqueNs("migration-kinds");
  const { version, workflowKey } = await deployWorkflow(ns);
  stopWriters();
  try {
    const cases = [...CONTRACT.cases, ...CONTRACT.rawCases];
    for (const entry of cases) seedCase(ns, workflowKey, version, entry);
    seedLongHistory(ns, workflowKey, version);
    redisSet(SCHEMA_KEY, "2", DB);
    redisSet("wf:unknown", "foreign", DB);
    assert.throws(() => migrate("check"), /not dedicated/);
    redisDel("wf:unknown", DB);
    redisZAdd("wf:pending-version:migration:worker:1", Date.now() + 30_000, "pending", DB);
    redisCommand(["EXPIRE", "wf:pending-version:migration:worker:1", "60"], DB);
    assert.throws(() => migrate("check"), /expiring Redis keys/);
    redisDel("wf:pending-version:migration:worker:1", DB);
    const payloadKey = instanceKey(ns, workflowKey, "do-null", "payloads");
    const previous = redisHGet(payloadKey, "step:0:output", DB);
    assert.notEqual(previous, null);
    redisCommand(["HDEL", payloadKey, "step:0:output"], DB);
    assert.throws(() => migrate("apply"), /payload accounting|payload is missing/);
    assert.equal(redisGet(SCHEMA_KEY, DB), "2");
    assert.equal(redisGet(MIGRATION_KEY), null);
    redisHSet(payloadKey, { "step:0:output": /** @type {string} */ (previous) }, DB);
    assert.throws(() => migrate("check", true), /only valid with apply or resume/);
    const checked = migrate("check");
    assert.equal(checked.instanceCount, cases.length + 1);
    assert.equal(checked.convertedStepCount, cases.length + LONG_HISTORY_STEP_COUNT);
    const readyKeys = redisKeys("wf:ready:*", DB).sort();
    migrate("apply");
    for (const entry of cases) {
      const key = instanceKey(ns, workflowKey, entry.id, "steps");
      const legacy = redisHGetJson(key, "0", ARCHIVE);
      const current = redisHGetJson(key, "0", DB);
      assert.deepEqual(current, { ...legacy, kind: entry.kind }, entry.id);
    }
    const last = redisHGetJson(instanceKey(ns, workflowKey, "long-history", "steps"), String(LONG_HISTORY_STEP_COUNT - 1), DB);
    assert.equal(last.kind, "do");
    assert.equal(last.nameCount, LONG_HISTORY_STEP_COUNT);
    assert.deepEqual(last.dependencies, [LONG_HISTORY_STEP_COUNT - 2]);
    assert.deepEqual(redisKeys("wf:ready:*", DB).sort(), readyKeys);
  } finally { restoreStack(); }
});

test("wrong Redis family types are rejected before SWAPDB or ownership acquisition", () => {
  stopWriters();
  try {
    redisSet(SCHEMA_KEY, "2", DB);
    for (const [key, command] of [
      ["wf:ready:active", "SET"],
      ["wf:due:0", "SADD"],
      ["wf:by-worker:demo:worker", "LPUSH"],
      ["wf:instance:{demo:wf:id}:events-by-type", "SET"],
      ["wf:internal:do-alarm:ready:active", "SET"],
      ["wf:internal:do-alarm:due:0", "LPUSH"],
    ]) {
      redisCommand([command, key, "wrong-type"], DB);
      for (const mode of /** @type {const} */ (["check", "apply"])) {
        assert.throws(() => migrate(mode), /invalid Redis type/);
        assert.equal(redisGet(SCHEMA_KEY, DB), "2");
        assert.equal(redisGet(MIGRATION_KEY), null);
        assert.equal(redisCommand(["DBSIZE"], ARCHIVE), "0");
      }
      redisDel(key, DB);
    }
  } finally { restoreStack(); }
});

for (const afterSwap of [false, true]) {
  test(`migration resumes ${afterSwap ? "partial copy after" : "acquired ownership before"} SWAPDB`, async () => {
    const ns = uniqueNs("migration-resume");
    const { version, workflowKey } = await deployWorkflow(ns);
    stopWriters();
    try {
      seedCase(ns, workflowKey, version, CONTRACT.cases[0]);
      const id = CONTRACT.cases[0].id;
      const stateKey = instanceKey(ns, workflowKey, id, "state");
      redisHSet(stateKey, { status: "running", runToken: "old-root", runLeaseExpiresAtMs: "200" }, DB);
      redisSet(SCHEMA_KEY, "2", DB);
      redisSet(MIGRATION_KEY, "in_progress:0000000000000000");
      if (afterSwap) {
        redisCommand(["SWAPDB", "2", "15"]);
        redisHSet(instanceKey(ns, workflowKey, id, "steps"), { 0: "stale-partial-copy" }, DB);
      }
      assert.throws(() => migrate("apply"), /Another schema3 migration task/);
      const resumed = migrate("resume");
      assert.equal(resumed.migrationState, "complete");
      assert.equal(redisGet(SCHEMA_KEY, DB), "3");
      assert.equal(redisHGet(stateKey, "status", DB), "queued");
      assert.equal(redisHGet(stateKey, "runToken", DB), null);
      const shard = workflowReadyShard(ns, workflowKey, id);
      assert.equal(redisCommand(["SISMEMBER", `wf:ready:${shard}`, workflowReadyToken(ns, workflowKey, id)], DB), "1");
      assert.equal(redisHGetJson(instanceKey(ns, workflowKey, id, "steps"), "0", DB).kind, "do");
      assert.equal(redisExists(stateKey, ARCHIVE), true);
    } finally { restoreStack(); }
  });
}

test("published schema marker resumes only finalization and explicit archive deletion", () => {
  stopWriters();
  try {
    redisSet(SCHEMA_KEY, "2", ARCHIVE);
    redisSet(MIGRATION_KEY, "in_progress:0000000000000000");
    redisSet(SCHEMA_KEY, "3", DB);
    redisSet("wf:ready:cursor", "17", DB);
    assert.equal(migrate("resume").migrationState, "complete");
    assert.equal(redisGet("wf:ready:cursor", DB), "17");
    assert.equal(redisGet(SCHEMA_KEY, ARCHIVE), "2");
    assert.throws(() => migrate("resume"), /No incomplete schema3 migration/);
    assert.equal(migrate("apply", true).archiveDeleted, true);
    assert.equal(redisGet("wf:ready:cursor", DB), "17");
    assert.equal(redisCommand(["DBSIZE"], ARCHIVE), "0");
  } finally { restoreStack(); }
});

test("marker-only schema2 migrates completely and opt-in archive deletion uses lazy freeing", async () => {
  stopWriters();
  try {
    redisCommand(["FLUSHDB"], DB);
    redisSet(SCHEMA_KEY, "2", DB);
    const freedBefore = redisInfoInteger("memory", "lazyfreed_objects");
    const report = migrate("apply", true);
    assert.equal(report.migrationState, "complete");
    assert.equal(report.instanceCount, 0);
    assert.equal(report.archiveDeleted, true);
    assert.equal(redisGet(SCHEMA_KEY, DB), "3");
    assert.equal(redisCommand(["DBSIZE"], ARCHIVE), "0");
    await waitForJson("archive memory released asynchronously",
      async () => redisInfoInteger("memory", "lazyfreed_objects"),
      (freed) => freed > freedBefore, 5000);
  } finally { restoreStack(); }
});
