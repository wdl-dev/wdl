import assert from "node:assert/strict";
import { test } from "node:test";

import { composeExec, composeRestart, composeScale, composeStop } from "./helpers/compose.js";
import {
  DO_ALARM_WORKER,
  doAlarmJobId,
  doAlarmStateKey,
  redisAddDoAlarmDue,
  redisDoAlarmJobExists,
  redisDoAlarmReadyIncludes,
  redisGetDoAlarmJob,
  waitForJson,
} from "./helpers/durable-objects.js";
import {
  deployAndPromote,
  gatewayFetch,
  responseJson,
  serviceInternalPost,
  setupIntegrationSuite,
  uniqueNs,
} from "./helpers/index.js";
import { parseJsonText } from "./helpers/json-payload.js";
import {
  redisCommand,
  redisDel,
  redisExists,
  redisFlushAll,
  redisGet,
  redisHGet,
  redisHGetAll,
  redisHSet,
  redisKeys,
  redisSet,
  redisSetEx,
} from "./helpers/redis.js";

const ACTIVE_DB = 2;
const ARCHIVE_DB = 15;
const SCHEMA_KEY = "wf:schema_version";
const RESET_KEY = "wf:schema3-reset";
const WORKFLOW_STATE_KEY = "wf:instance:{reset:wf_reset:instance}:state";
const CRASH_JOB_ID = `doa-${"a".repeat(64)}`;
const CRASH_ALARM_KEY = `wf:internal:do-alarm:{${CRASH_JOB_ID}}:state`;

setupIntegrationSuite();

/** @param {"check"|"apply"|"resume"} mode */
function schema3Reset(mode) {
  return parseJsonText(
    composeExec("workflows", ["/workflows", "schema3-reset", mode]),
    `schema3-reset ${mode} output`
  );
}

function restoreSchema3Stack() {
  redisFlushAll();
  redisSet(SCHEMA_KEY, "3", { db: ACTIVE_DB });
  composeRestart("workflows");
  composeScale("scheduler", 1);
}

test("schema3 reset archives Workflow state and preserves live DO alarms", async () => {
  const ns = uniqueNs("schema-reset");
  const worker = "alarms";
  const objectName = "migration";

  await deployAndPromote(ns, worker, {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  composeStop("scheduler");
  try {
    const scheduled = await gatewayFetch(ns, `/alarms/schedule-soon?name=${objectName}`);
    const scheduledText = await scheduled.text();
    assert.equal(scheduled.status, 200, scheduledText);
    assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

    const jobId = doAlarmJobId(ns, worker, "AlarmCounter", objectName);
    assert.match(jobId, /^doa-[0-9a-f]{64}$/);
    const alarmStateKey = doAlarmStateKey(jobId);
    const schema2Alarm = redisGetDoAlarmJob(ns, worker, "AlarmCounter", objectName);
    assert.equal(schema2Alarm.status, "waiting");

    redisHSet(WORKFLOW_STATE_KEY, { status: "completed", generation: "1" }, { db: ACTIVE_DB });
    redisSet(SCHEMA_KEY, "2", { db: ACTIVE_DB });
    assert.equal(redisCommand(["DBSIZE"], { db: ARCHIVE_DB }), "0");

    redisSet("wf:unknown", "foreign", { db: ACTIVE_DB });
    assert.throws(() => schema3Reset("check"), /schema3-reset|not dedicated/);
    redisDel("wf:unknown", { db: ACTIVE_DB });

    redisSetEx("wf:pending-version:reset:alarms:1", "pending", 60, { db: ACTIVE_DB });
    assert.throws(() => schema3Reset("check"), /schema3-reset|expiring Redis keys/);
    redisDel("wf:pending-version:reset:alarms:1", { db: ACTIVE_DB });

    const activeLeaseExpiresAtMs = Date.now() + 60_000;
    redisHSet(
      alarmStateKey,
      {
        status: "running",
        runToken: "schema2-running",
        runLeaseExpiresAtMs: String(activeLeaseExpiresAtMs),
      },
      { db: ACTIVE_DB }
    );
    redisAddDoAlarmDue(activeLeaseExpiresAtMs, jobId);
    assert.equal(redisDoAlarmReadyIncludes(jobId), false);
    assert.throws(() => schema3Reset("check"), /schema3-reset|unexpired running DO alarm/);
    const expiredLeaseAtMs = Date.now() - 1;
    redisHSet(
      alarmStateKey,
      { runLeaseExpiresAtMs: String(expiredLeaseAtMs) },
      { db: ACTIVE_DB }
    );
    redisAddDoAlarmDue(expiredLeaseAtMs, jobId);
    assert.equal(redisDoAlarmReadyIncludes(jobId), false);

    const schema2KeyCount = Number(redisCommand(["DBSIZE"], { db: ACTIVE_DB }));
    const schema2AlarmKeyCount = redisKeys("wf:internal:do-alarm:*", { db: ACTIVE_DB }).length;
    const checked = schema3Reset("check");
    assert.equal(checked.phase, "schema2_active");
    assert.equal(checked.resetState, "none");
    assert.equal(checked.alarmKeyCount, schema2AlarmKeyCount);
    assert.equal(checked.estimatedAlarmCopyBytes > 0, true);
    assert.equal(Array.isArray(checked.warnings), true);

    const applied = schema3Reset("apply");
    assert.equal(applied.phase, "schema3_prepared");
    assert.equal(applied.resetState, "archive_pending");
    assert.equal(applied.archiveKeyCount, schema2KeyCount);
    assert.equal(applied.alarmKeyCount, schema2AlarmKeyCount);

    assert.equal(redisGet(SCHEMA_KEY, { db: ACTIVE_DB }), "3");
    assert.equal(redisGet(SCHEMA_KEY, { db: ARCHIVE_DB }), "2");
    assert.equal(redisGet(RESET_KEY), "archive_pending");
    assert.equal(redisExists(WORKFLOW_STATE_KEY, { db: ACTIVE_DB }), false);
    assert.equal(redisExists(WORKFLOW_STATE_KEY, { db: ARCHIVE_DB }), true);
    assert.deepEqual(
      redisHGetAll(alarmStateKey, { db: ACTIVE_DB }),
      redisHGetAll(alarmStateKey, { db: ARCHIVE_DB })
    );

    composeRestart("workflows");
    const blocked = serviceInternalPost("workflows", 9120, "/internal/workflows/status", {});
    assert.equal(blocked.status, 409, blocked.body);
    assert.equal(responseJson(blocked).error, "workflow_migration_pending");

    composeScale("scheduler", 1);
    await waitForJson(
      "schema3-reset migrated alarm delivery",
      async () => {
        const status = await gatewayFetch(ns, `/alarms/status?name=${objectName}`);
        const text = await status.text();
        assert.equal(status.status, 200, text);
        return responseJson({ body: text });
      },
      (status) => status.alarms === 1 && status.pending === null,
      10_000
    );
    assert.equal(redisDoAlarmJobExists(ns, worker, "AlarmCounter", objectName), false);
    assert.equal(redisExists(alarmStateKey, { db: ARCHIVE_DB }), true);
    assert.equal(
      redisKeys("wf:*", { db: ACTIVE_DB }).every((key) => (
        key === SCHEMA_KEY || key.startsWith("wf:internal:do-alarm:")
      )),
      true
    );

    assert.equal(schema3Reset("apply").resetState, "archive_pending");
    assert.equal(redisDoAlarmJobExists(ns, worker, "AlarmCounter", objectName), false);
  } finally {
    restoreSchema3Stack();
  }
});

test("schema3 reset resumes after SWAPDB before alarm copy", () => {
  composeStop("scheduler");
  try {
    redisSet(SCHEMA_KEY, "2", { db: ACTIVE_DB });
    redisHSet(
      CRASH_ALARM_KEY,
      { status: "waiting", updatedAtMs: "before-swap" },
      { db: ACTIVE_DB }
    );
    redisSet(RESET_KEY, "in_progress:0000000000000000");
    assert.equal(redisCommand(["SWAPDB", "2", "15"]), "OK");
    assert.equal(redisGet(SCHEMA_KEY, { db: ACTIVE_DB }), null);
    assert.equal(redisGet(SCHEMA_KEY, { db: ARCHIVE_DB }), "2");
    redisHSet(
      CRASH_ALARM_KEY,
      { status: "waiting", updatedAtMs: "partial-stale" },
      { db: ACTIVE_DB }
    );

    assert.throws(() => schema3Reset("apply"), /schema3-reset|Another schema3 reset task/);
    const resumed = schema3Reset("resume");
    assert.equal(resumed.phase, "schema3_prepared");
    assert.equal(resumed.resetState, "archive_pending");
    assert.equal(redisGet(SCHEMA_KEY, { db: ACTIVE_DB }), "3");
    assert.equal(redisExists(CRASH_ALARM_KEY, { db: ACTIVE_DB }), true);
    assert.equal(
      redisHGet(CRASH_ALARM_KEY, "updatedAtMs", { db: ACTIVE_DB }),
      "before-swap"
    );
  } finally {
    restoreSchema3Stack();
  }
});

test("schema3 reset finalizes published marker without replaying archive", () => {
  composeStop("scheduler");
  try {
    redisSet(SCHEMA_KEY, "2", { db: ARCHIVE_DB });
    redisHSet(
      CRASH_ALARM_KEY,
      { status: "waiting", updatedAtMs: "archived" },
      { db: ARCHIVE_DB }
    );
    redisSet(SCHEMA_KEY, "3", { db: ACTIVE_DB });
    redisHSet(
      CRASH_ALARM_KEY,
      { status: "waiting", updatedAtMs: "live" },
      { db: ACTIVE_DB }
    );
    redisSet(RESET_KEY, "in_progress:0000000000000000");

    const resumed = schema3Reset("resume");
    assert.equal(resumed.resetState, "archive_pending");
    assert.equal(redisHGet(CRASH_ALARM_KEY, "updatedAtMs", { db: ACTIVE_DB }), "live");
    assert.throws(() => schema3Reset("resume"), /No incomplete schema3 reset/);
    assert.equal(schema3Reset("apply").resetState, "archive_pending");
    assert.equal(redisHGet(CRASH_ALARM_KEY, "updatedAtMs", { db: ACTIVE_DB }), "live");
  } finally {
    restoreSchema3Stack();
  }
});
