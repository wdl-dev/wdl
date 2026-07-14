import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  composeUpNoBuildFlag,
  composeScale,
  delay,
  deployAndPromote,
  gatewayFetch,
  sh,
  serviceInternalPost,
  uniqueNs,
  withDoMultiRuntimes,
  withServiceStopped,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import { redisDel } from "./helpers/redis.js";
import {
  DO_ALARM_WORKER,
  doAlarmJobId,
  doAlarmJobIdForStorage,
  doHostId,
  redisAddDoAlarmReady,
  redisAddDoAlarmDue,
  redisAddDoAlarmByWorker,
  redisDeleteDoAlarmJob,
  redisDoAlarmDueIncludes,
  redisDoAlarmJobExists,
  redisDoAlarmJobIdsForWorker,
  redisDoAlarmReadyIncludes,
  redisDoAlarmStateKeysForWorker,
  redisGetDoAlarmJob,
  redisGetDoAlarmJobById,
  redisGetDoOwner,
  redisGetDoStorageId,
  redisRemoveDoAlarmDue,
  redisSetDoAlarmJob,
  redisSetDoOwner,
  waitForJson,
} from "./helpers/durable-objects.js";
import { redisKeys } from "./helpers/redis.js";

const ALARM_ABSENCE_SETTLE_DELAY_MS = 1_500;
const REPLICA_DUPLICATE_OBSERVATION_DELAY_MS = 2_000;
const REPLACEMENT_DUPLICATE_OBSERVATION_DELAY_MS = 2_500;

/**
 * @typedef {Object} CorruptJobSpec
 * @property {string} jobId
 * @property {string} objectName
 * @property {Record<string, string>} fields
 * @property {string[]=} baseOmit
 * @property {boolean=} expectByWorkerIndexRemoved
 */

setupIntegrationSuite();

const DO_BLOCKING_ALARM_WORKER = readFileSync(
  new URL("../../test-workers/do-blocking-alarm/src/index.js", import.meta.url),
  "utf8"
);

const DO_ACCESSOR_ALARM_WORKER = `
const instances = new WeakMap();

export class AccessorAlarm {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
    instances.set(this, true);
  }

  #assertReceiver() {
    if (!instances.has(this)) throw new TypeError("invalid AccessorAlarm receiver");
  }

  get fetch() {
    this.#assertReceiver();
    return async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/schedule") {
        await this.#ctx.storage.setAlarm(Date.now() - 1000);
        return Response.json({ pending: true });
      }
      return Response.json({
        alarms: (await this.#ctx.storage.get("alarms")) ?? 0,
        pending: await this.#ctx.storage.getAlarm(),
      });
    };
  }

  get alarm() {
    this.#assertReceiver();
    return async () => {
      const alarms = (await this.#ctx.storage.get("alarms")) ?? 0;
      await this.#ctx.storage.put("alarms", alarms + 1);
    };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.ACCESSORS.idFromName(url.searchParams.get("name") || "main");
    return await env.ACCESSORS.get(id).fetch("https://do.internal" + url.pathname);
  },
};
`;

/**
 * @param {string} ns
 * @param {string} worker
 * @param {string} className
 * @param {string} objectName
 * @param {number} [timeoutMs]
 * @returns {Promise<Record<string, string>>}
 */
function waitForDoAlarmJob(ns, worker, className, objectName, timeoutMs = 5000) {
  return waitForJson(
    `Workflows DO alarm job ${ns}/${worker}/${className}/${objectName}`,
    async () => redisGetDoAlarmJob(ns, worker, className, objectName),
    (record) => Object.keys(record).length > 0,
    timeoutMs
  );
}

/**
 * @param {string} jobId
 * @param {number} [timeoutMs]
 */
async function waitForDoAlarmDue(jobId, timeoutMs = 5000) {
  await waitForJson(
    `Workflows DO alarm due index ${jobId}`,
    async () => ({ due: redisDoAlarmDueIncludes(jobId) }),
    (record) => record.due === true,
    timeoutMs
  );
}

/**
 * @param {string} ns
 * @param {string} name
 */
async function fetchDoAlarmStatusDuringOwnerRecovery(ns, name) {
  const status = await gatewayFetch(ns, `/alarms/status?name=${name}`);
  const statusText = await status.text();
  if (status.status === 502 || status.status === 503 || status.status === 504) {
    return { transientStatus: status.status, body: statusText };
  }
  assert.equal(status.status, 200, statusText);
  return responseJson({ body: statusText });
}

test("do-runtime shim supports storage alarms on SQLite-backed Durable Objects", async () => {
  const ns = uniqueNs("do-alarm");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const deleted = await gatewayFetch(ns, "/alarms/delete?name=alice");
  const deletedText = await deleted.text();
  assert.equal(deleted.status, 200, deletedText);
  assert.deepEqual(responseJson({ body: deletedText }), { alarm: null });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule?name=alice");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), {
    pending: true,
    envHasInternalAlarmBinding: false,
  });

  const constructorCached = await gatewayFetch(ns, "/alarms/schedule-constructor-cache?name=ctor");
  const constructorCachedText = await constructorCached.text();
  assert.equal(constructorCached.status, 200, constructorCachedText);
  assert.deepEqual(responseJson({ body: constructorCachedText }), { pending: true });

  const statusJson = await waitForJson(
    "workflows-delivered DO alarm",
    async () => {
      const status = await gatewayFetch(ns, "/alarms/status?name=alice");
      const statusText = await status.text();
      assert.equal(status.status, 200, statusText);
      return responseJson({ body: statusText });
    },
    (json) => json.alarms === 1 && json.pending === null
  );
  assert.deepEqual(statusJson, {
    alarms: 1,
    alarmDuringHandlerWasNull: 1,
    pending: null,
  });

  const txScheduled = await gatewayFetch(ns, "/alarms/schedule-transaction?name=tx");
  const txScheduledText = await txScheduled.text();
  assert.equal(txScheduled.status, 200, txScheduledText);
  assert.deepEqual(responseJson({ body: txScheduledText }), { pending: true });

  const txTwiceScheduled = await gatewayFetch(ns, "/alarms/schedule-transaction-twice?name=twice");
  const txTwiceScheduledText = await txTwiceScheduled.text();
  assert.equal(txTwiceScheduled.status, 200, txTwiceScheduledText);
  assert.deepEqual(responseJson({ body: txTwiceScheduledText }), { pending: true });
  await waitForJson(
    "workflows-delivered final DO alarm from multi-setAlarm transaction",
    async () => {
      const status = await gatewayFetch(ns, "/alarms/status?name=twice");
      const statusText = await status.text();
      assert.equal(status.status, 200, statusText);
      return responseJson({ body: statusText });
    },
    (json) => json.alarms === 1 && json.pending === null
  );

  const txSyncScheduled = await gatewayFetch(ns, "/alarms/schedule-transaction-sync?name=txsync");
  const txSyncScheduledText = await txSyncScheduled.text();
  assert.equal(txSyncScheduled.status, 200, txSyncScheduledText);
  assert.deepEqual(responseJson({ body: txSyncScheduledText }), {
    rejected: true,
    message: "setAlarm() cannot be used inside transactionSync(); use transaction()",
    pending: null,
  });
  await delay(ALARM_ABSENCE_SETTLE_DELAY_MS);
  const txSyncStatus = await gatewayFetch(ns, "/alarms/status?name=txsync");
  const txSyncStatusText = await txSyncStatus.text();
  assert.equal(txSyncStatus.status, 200, txSyncStatusText);
  assert.deepEqual(responseJson({ body: txSyncStatusText }), {
    alarms: 0,
    alarmDuringHandlerWasNull: 0,
    pending: null,
  });

  const replaced = await gatewayFetch(ns, "/alarms/replace-future-with-due?name=replace");
  const replacedText = await replaced.text();
  assert.equal(replaced.status, 200, replacedText);
  assert.deepEqual(responseJson({ body: replacedText }), { pending: true });
  await waitForJson(
    "workflows-delivered replacement DO alarm exactly once",
    async () => {
      const status = await gatewayFetch(ns, "/alarms/status?name=replace");
      const statusText = await status.text();
      assert.equal(status.status, 200, statusText);
      return responseJson({ body: statusText });
    },
    (json) => json.alarms === 1 && json.pending === null
  );
  await delay(REPLACEMENT_DUPLICATE_OBSERVATION_DELAY_MS);
  const replacementFinal = await gatewayFetch(ns, "/alarms/status?name=replace");
  const replacementFinalText = await replacementFinal.text();
  assert.equal(replacementFinal.status, 200, replacementFinalText);
  assert.equal(responseJson({ body: replacementFinalText }).alarms, 1);

  const rolledBack = await gatewayFetch(ns, "/alarms/schedule-rollback?name=bob");
  const rolledBackText = await rolledBack.text();
  assert.equal(rolledBack.status, 200, rolledBackText);
  assert.deepEqual(responseJson({ body: rolledBackText }), { pending: null });
  const rolledBackJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "bob");
  assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "bob"), false);
  assert.equal(redisDoAlarmDueIncludes(rolledBackJobId), false);
  await delay(ALARM_ABSENCE_SETTLE_DELAY_MS);
  const rolledBackStatus = await gatewayFetch(ns, "/alarms/status?name=bob");
  const rolledBackStatusText = await rolledBackStatus.text();
  assert.equal(rolledBackStatus.status, 200, rolledBackStatusText);
  assert.equal(responseJson({ body: rolledBackStatusText }).alarms, 0);

  const shortDeleted = await gatewayFetch(ns, "/alarms/schedule-short-delete?name=shortdelete");
  const shortDeletedText = await shortDeleted.text();
  assert.equal(shortDeleted.status, 200, shortDeletedText);
  assert.deepEqual(responseJson({ body: shortDeletedText }), { alarm: null });
  await delay(ALARM_ABSENCE_SETTLE_DELAY_MS);
  const shortDeletedStatus = await gatewayFetch(ns, "/alarms/status?name=shortdelete");
  const shortDeletedStatusText = await shortDeletedStatus.text();
  assert.equal(shortDeletedStatus.status, 200, shortDeletedStatusText);
  assert.equal(responseJson({ body: shortDeletedStatusText }).alarms, 0);

  const txSetThenDelete = await gatewayFetch(ns, "/alarms/delete-after-transaction-sync-set?name=txdelete");
  const txSetThenDeleteText = await txSetThenDelete.text();
  assert.equal(txSetThenDelete.status, 200, txSetThenDeleteText);
  assert.deepEqual(responseJson({ body: txSetThenDeleteText }), {
    rejected: true,
    message: "deleteAlarm() cannot be used inside transactionSync(); use transaction()",
    alarm: null,
  });
  const txSetThenDeleteJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "txdelete");
  assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "txdelete"), false);
  assert.equal(redisDoAlarmDueIncludes(txSetThenDeleteJobId), false);

  const spoof = await gatewayFetch(ns, "/alarms/spoof?name=alice");
  const spoofText = await spoof.text();
  assert.equal(spoof.status, 200, spoofText);
  const spoofJson = responseJson({ body: spoofText });
  assert.equal(spoofJson.alarms, 1);
  assert.equal(spoofJson.alarmDuringHandlerWasNull, 1);
  assert.equal(spoofJson.pending, null);
});

test("do-runtime alarm wrapper preserves accessor handler receivers through host proxies", async () => {
  const ns = uniqueNs("do-alarm-accessor");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ACCESSOR_ALARM_WORKER },
    bindings: {
      ACCESSORS: { type: "do", className: "AccessorAlarm" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule?name=alice");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

  const status = await waitForJson(
    "accessor-backed DO alarm",
    async () => {
      const response = await gatewayFetch(ns, "/alarms/status?name=alice");
      const body = await response.text();
      assert.equal(response.status, 200, body);
      return responseJson({ body });
    },
    (json) => json.alarms === 1 && json.pending === null
  );
  assert.deepEqual(status, { alarms: 1, pending: null });
});

test("scheduler replicas: Workflows DO alarm tick delivers a due alarm once", async () => {
  const ns = uniqueNs("do-alarm-replica");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  await withServiceStopped("scheduler", async () => {
    const scheduled = await gatewayFetch(ns, "/alarms/schedule?name=replica");
    const scheduledText = await scheduled.text();
    assert.equal(scheduled.status, 200, scheduledText);
    assert.deepEqual(responseJson({ body: scheduledText }), {
      pending: true,
      envHasInternalAlarmBinding: false,
    });

    composeScale("scheduler", 2);

    const statusJson = await waitForJson(
      "scheduler replicas delivered one DO alarm",
      async () => {
        const status = await gatewayFetch(ns, "/alarms/status?name=replica");
        const statusText = await status.text();
        assert.equal(status.status, 200, statusText);
        return responseJson({ body: statusText });
      },
      (json) => json.alarms === 1 && json.pending === null
    );
    assert.deepEqual(statusJson, {
      alarms: 1,
      alarmDuringHandlerWasNull: 1,
      pending: null,
    });

    await delay(REPLICA_DUPLICATE_OBSERVATION_DELAY_MS);
    const final = await gatewayFetch(ns, "/alarms/status?name=replica");
    const finalText = await final.text();
    assert.equal(final.status, 200, finalText);
    assert.equal(responseJson({ body: finalText }).alarms, 1);
  });
});

test("worker delete clears Durable Object owner and Workflows alarm jobs", async () => {
  const ns = uniqueNs("do-delete");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-future?name=alice");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });
  await waitForDoAlarmJob(ns, "alarms", "AlarmCounter", "alice");
  assert.notDeepEqual(redisDoAlarmJobIdsForWorker(ns, "alarms"), []);
  const ownerPattern = `do:owner:scope:${encodeURIComponent(`${redisGetDoStorageId(ns, "alarms")}:`)}*`;
  const initialOwnerKeys = redisKeys(ownerPattern);
  assert.notDeepEqual(initialOwnerKeys, []);
  for (const key of initialOwnerKeys) {
    redisDel(key);
  }
  assert.deepEqual(redisKeys(ownerPattern), []);

  const deleted = await adminFetch(`/ns/${ns}/worker/alarms/delete`, { method: "POST" });
  const deletedText = await deleted.text();
  assert.equal(deleted.status, 200, deletedText);

  assert.deepEqual(redisDoAlarmStateKeysForWorker(ns, "alarms"), []);
  assert.deepEqual(redisKeys(ownerPattern), []);
  assert.deepEqual(redisDoAlarmJobIdsForWorker(ns, "alarms"), []);
});

test("Workflows DO alarm cleanup is fenced to the deleted storage id", async () => {
  const ns = uniqueNs("do-cleanup-fence");
  const worker = "alarms";
  const dueAt = Date.now() + 120_000;
  const oldStorageId = "do_old_cleanup_storage";
  const newStorageId = "do_new_cleanup_storage";
  const oldJobId = doAlarmJobIdForStorage(ns, worker, oldStorageId, "AlarmCounter", "old-object");
  const newJobId = doAlarmJobIdForStorage(ns, worker, newStorageId, "AlarmCounter", "new-object");

  for (const [jobId, doStorageId, objectName] of [
    [oldJobId, oldStorageId, "old-object"],
    [newJobId, newStorageId, "new-object"],
  ]) {
    redisSetDoAlarmJob(jobId, {
      status: "waiting",
      generation: "1",
      ns,
      worker,
      scheduledVersion: "v1",
      doStorageId,
      className: "AlarmCounter",
      objectName,
      dueAtMs: String(dueAt),
      retryCount: "0",
      rowToken: `row-token-${objectName}`,
      createdAtMs: String(Date.now()),
      updatedAtMs: String(Date.now()),
    });
    redisAddDoAlarmByWorker(ns, worker, jobId);
    redisAddDoAlarmDue(dueAt, jobId);
  }

  assert.equal(redisDoAlarmDueIncludes(oldJobId), true);
  assert.equal(redisDoAlarmDueIncludes(newJobId), true);

  const cleanup = serviceInternalPost("workflows", 9120, "/internal/workflows/do-alarms/cleanup-worker", {
    ns,
    worker,
    doStorageId: oldStorageId,
  });
  assert.equal(cleanup.status, 200, cleanup.body);
  assert.deepEqual(responseJson(cleanup), {
    ok: true,
    jobId: null,
    changed: true,
    deleted: 1,
  });

  assert.deepEqual(redisGetDoAlarmJobById(oldJobId), {});
  assert.equal(redisDoAlarmDueIncludes(oldJobId), false);

  assert.equal(redisGetDoAlarmJobById(newJobId).doStorageId, newStorageId);
  assert.equal(redisDoAlarmDueIncludes(newJobId), true);
  const indexedJobs = redisDoAlarmJobIdsForWorker(ns, worker);
  assert.equal(indexedJobs.includes(oldJobId), false);
  assert.equal(indexedJobs.includes(newJobId), true);
});

test("late DO alarm writes after worker delete do not recreate Workflows alarm jobs", async () => {
  const ns = uniqueNs("do-delete-late-alarm");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const doStorageId = redisGetDoStorageId(ns, "alarms");
  assert.match(doStorageId, /^do_/);
  const jobId = doAlarmJobIdForStorage(ns, "alarms", doStorageId, "AlarmCounter", "late");

  const deleted = await adminFetch(`/ns/${ns}/worker/alarms/delete`, { method: "POST" });
  const deletedText = await deleted.text();
  assert.equal(deleted.status, 200, deletedText);

  const lateSet = serviceInternalPost("workflows", 9120, "/internal/workflows/do-alarms/set", {
    ns,
    worker: "alarms",
    version: "v1",
    doStorageId,
    className: "AlarmCounter",
    objectName: "late",
    scheduledTime: Date.now() + 60_000,
    retryCount: 0,
    token: "late-row-token",
  });
  assert.equal(lateSet.status, 200, lateSet.body);
  assert.deepEqual(responseJson(lateSet), {
    ok: true,
    jobId,
    changed: false,
    deleted: 0,
  });
  assert.deepEqual(redisDoAlarmJobIdsForWorker(ns, "alarms"), []);
  assert.deepEqual(redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "late"), {});
  assert.equal(redisDoAlarmDueIncludes(jobId), false);
});

test("deleteAll defaults to clearing Durable Object alarms while deleteAlarm:false preserves them", async () => {
  const ns = uniqueNs("do-delete-all-alarm");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const defaultDeleted = await gatewayFetch(ns, "/alarms/delete-all-default?name=default");
  const defaultDeletedText = await defaultDeleted.text();
  assert.equal(defaultDeleted.status, 200, defaultDeletedText);
  assert.deepEqual(responseJson({ body: defaultDeletedText }), { kv: null, sqlTableExists: false, alarm: null });
  const defaultJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "default");
  assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "default"), false);
  assert.equal(redisDoAlarmDueIncludes(defaultJobId), false);

  const emptyOptionsDeleted = await gatewayFetch(ns, "/alarms/delete-all-empty-options?name=empty");
  const emptyOptionsDeletedText = await emptyOptionsDeleted.text();
  assert.equal(emptyOptionsDeleted.status, 200, emptyOptionsDeletedText);
  assert.deepEqual(responseJson({ body: emptyOptionsDeletedText }), { kv: null, sqlTableExists: false, alarm: null });
  const emptyOptionsJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "empty");
  assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "empty"), false);
  assert.equal(redisDoAlarmDueIncludes(emptyOptionsJobId), false);

  const explicitTrueDeleted = await gatewayFetch(ns, "/alarms/delete-all-explicit-true?name=explicit");
  const explicitTrueDeletedText = await explicitTrueDeleted.text();
  assert.equal(explicitTrueDeleted.status, 200, explicitTrueDeletedText);
  assert.deepEqual(responseJson({ body: explicitTrueDeletedText }), { kv: null, sqlTableExists: false, alarm: null });
  const explicitTrueJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "explicit");
  assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "explicit"), false);
  assert.equal(redisDoAlarmDueIncludes(explicitTrueJobId), false);

  const keepAlarm = await gatewayFetch(ns, "/alarms/delete-all-keep-alarm?name=keep");
  const keepAlarmText = await keepAlarm.text();
  assert.equal(keepAlarm.status, 200, keepAlarmText);
  assert.deepEqual(responseJson({ body: keepAlarmText }), { kv: null, sqlTableExists: false, pending: true });
  const keepJobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "keep");
  await waitForDoAlarmJob(ns, "alarms", "AlarmCounter", "keep");
  assert.equal(redisDoAlarmDueIncludes(keepJobId), true);
});

test("deleteAll clears common Durable Object SQL object types", async () => {
  const ns = uniqueNs("do-delete-all-sql");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const response = await gatewayFetch(ns, "/alarms/delete-all-sql-edges?name=sql-edges");
  const text = await response.text();
  assert.equal(response.status, 200, text);
  assert.deepEqual(responseJson({ body: text }), {
    names: [],
    sqliteSequenceRows: [],
    recreatedId: 1,
  });
});

test("alarms scheduled by deleted retained versions retarget to the active worker version", async () => {
  const ns = uniqueNs("do-alarm-retarget");
  const v1 = await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-soon?name=retarget");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

  const v2 = await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });
  assert.notEqual(v2, v1);

  const deleted = await adminFetch(`/ns/${ns}/worker/alarms/versions/${v1}`, { method: "DELETE" });
  const deletedText = await deleted.text();
  assert.equal(deleted.status, 200, deletedText);

  const statusJson = await waitForJson(
    "retargeted DO alarm",
    async () => {
      const status = await gatewayFetch(ns, "/alarms/status?name=retarget");
      const statusText = await status.text();
      assert.equal(status.status, 200, statusText);
      return responseJson({ body: statusText });
    },
    (json) => json.alarms === 1 && json.pending === null,
    10000
  );
  assert.equal(statusJson.alarms, 1);
});

test("failing DO alarm handlers retry with backoff then discard at max tries", async () => {
  const ns = uniqueNs("do-alarm-failure");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-failing?name=fail");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

  const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "fail");
  const retried = await waitForJson(
    "failed DO alarm retried through Workflows backend",
    async () => redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "fail"),
    (record) => record.status === "waiting" && Number(record.retryCount) >= 1 && Boolean(record.lastError),
    15_000
  );
  assert.equal(redisDoAlarmDueIncludes(jobId), true);
  assert.equal(redisDoAlarmReadyIncludes(jobId), false);

  const dueNow = Date.now() - 1000;
  redisSetDoAlarmJob(jobId, {
    status: "waiting",
    retryCount: "6",
    dueAtMs: String(dueNow),
    lastError: retried.lastError,
  });
  redisAddDoAlarmDue(dueNow, jobId);

  await waitForJson(
    "failed DO alarm discarded after max tries",
    async () => ({
      job: redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "fail"),
      byWorker: redisDoAlarmJobIdsForWorker(ns, "alarms"),
    }),
    ({ job, byWorker }) => Object.keys(job).length === 0 && !byWorker.includes(jobId),
    20_000
  );

  const status = await gatewayFetch(ns, "/alarms/failure-status?name=fail");
  const statusText = await status.text();
  assert.equal(status.status, 200, statusText);
  const statusJson = responseJson({ body: statusText });
  assert.equal(statusJson.pending, null);
  assert.equal(statusJson.retry, 6);
});

test("stale ready hints do not claim alarms rescheduled into the future", async () => {
  const ns = uniqueNs("do-alarm-stale-ready");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-future?name=future");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });
  await waitForDoAlarmJob(ns, "alarms", "AlarmCounter", "future");

  const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "future");
  await waitForDoAlarmDue(jobId);
  assert.equal(redisDoAlarmDueIncludes(jobId), true);
  redisAddDoAlarmReady(jobId);

  const tick = serviceInternalPost("workflows", 9120, "/internal/workflows/tick", {});
  assert.equal(tick.status, 200, tick.body);
  const tickBody = responseJson(tick);
  assert.equal(tickBody.doAlarmDispatched, 0);

  const record = redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "future");
  assert.equal(record.status, "waiting");
  assert.equal(redisDoAlarmDueIncludes(jobId), true);
  assert.equal(redisDoAlarmReadyIncludes(jobId), false);
});

test("expired running DO alarm claims redeliver from the ready hint", async () => {
  const ns = uniqueNs("do-alarm-expired-run");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  await withServiceStopped("scheduler", async () => {
    const scheduled = await gatewayFetch(ns, "/alarms/schedule?name=expired-run");
    const scheduledText = await scheduled.text();
    assert.equal(scheduled.status, 200, scheduledText);
    assert.deepEqual(responseJson({ body: scheduledText }), {
      pending: true,
      envHasInternalAlarmBinding: false,
    });

    const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "expired-run");
    const claimed = await waitForDoAlarmJob(ns, "alarms", "AlarmCounter", "expired-run");
    redisSetDoAlarmJob(jobId, {
      ...claimed,
      status: "running",
      runToken: "expired-run-token",
      runLeaseExpiresAtMs: String(Date.now() - 1000),
      dueAtMs: String(Date.now() - 1000),
    });
    redisRemoveDoAlarmDue(jobId);
    redisAddDoAlarmReady(jobId);

    const tick = serviceInternalPost("workflows", 9120, "/internal/workflows/tick", {});
    assert.equal(tick.status, 200, tick.body);
    const tickBody = responseJson(tick);
    assert.equal(tickBody.doAlarmDispatched, 1);
    assert.equal(tickBody.doAlarmDelivered, 1);
    assert.equal(redisDoAlarmJobExists(ns, "alarms", "AlarmCounter", "expired-run"), false);

    const status = await gatewayFetch(ns, "/alarms/status?name=expired-run");
    const statusText = await status.text();
    assert.equal(status.status, 200, statusText);
    assert.equal(responseJson({ body: statusText }).alarms, 1);
  });
});

test("malformed Workflows DO alarm job is discarded without poisoning later alarms", async () => {
  const ns = uniqueNs("do-alarm-corrupt");
  const version = await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const storageId = redisGetDoStorageId(ns, "alarms");
  const dueAt = Date.now() - 1000;
  /** @type {CorruptJobSpec[]} */
  const corruptJobs = [
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-missing-row-token"),
      objectName: "corrupt-missing-row-token",
      fields: {
        status: "waiting",
        objectName: "corrupt-missing-row-token",
      },
    },
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-missing-status"),
      objectName: "corrupt-missing-status",
      fields: {
        objectName: "corrupt-missing-status",
      },
    },
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-invalid-status"),
      objectName: "corrupt-invalid-status",
      fields: {
        status: "paused",
        objectName: "corrupt-invalid-status",
      },
    },
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-missing-namespace"),
      objectName: "corrupt-missing-namespace",
      baseOmit: ["ns"],
      expectByWorkerIndexRemoved: false,
      fields: {
        status: "waiting",
        objectName: "corrupt-missing-namespace",
      },
    },
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-invalid-due-at-ms"),
      objectName: "corrupt-invalid-due-at-ms",
      fields: {
        status: "waiting",
        objectName: "corrupt-invalid-due-at-ms",
        dueAtMs: "not-a-number",
      },
    },
    {
      jobId: doAlarmJobId(ns, "alarms", "AlarmCounter", "corrupt-missing-class-name"),
      objectName: "corrupt-missing-class-name",
      baseOmit: ["className"],
      fields: {
        status: "waiting",
        objectName: "corrupt-missing-class-name",
      },
    },
  ];
  for (const corrupt of corruptJobs) {
    const baseFields = {
      ns,
      worker: "alarms",
      scheduledVersion: version,
      doStorageId: storageId,
      className: "AlarmCounter",
      dueAtMs: String(dueAt),
      retryCount: "0",
    };
    for (const field of corrupt.baseOmit ?? []) {
      delete /** @type {Record<string, string>} */ (baseFields)[field];
    }
    redisSetDoAlarmJob(corrupt.jobId, {
      ...baseFields,
      ...corrupt.fields,
    });
    redisAddDoAlarmByWorker(ns, "alarms", corrupt.jobId);
    redisAddDoAlarmDue(dueAt, corrupt.jobId);
  }

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-soon?name=after-corrupt");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);

  await waitForJson(
    "real DO alarm after corrupt Workflows job",
    async () => {
      const status = await gatewayFetch(ns, "/alarms/status?name=after-corrupt");
      const statusText = await status.text();
      assert.equal(status.status, 200, statusText);
      return responseJson({ body: statusText });
    },
    (json) => json.alarms === 1 && json.pending === null,
    15000
  );
  await waitForJson(
    "corrupt Workflows DO alarm jobs discarded",
    async () => corruptJobs.map((corrupt) =>
      redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", corrupt.objectName)
    ),
    (/** @type {Array<Record<string, string>>} */ records) =>
      records.every((record) => Object.keys(record).length === 0),
    5000
  );
  for (const corrupt of corruptJobs) {
    assert.equal(redisDoAlarmDueIncludes(corrupt.jobId), false);
    if (corrupt.expectByWorkerIndexRemoved !== false) {
      assert.equal(redisDoAlarmJobIdsForWorker(ns, "alarms").includes(corrupt.jobId), false);
    }
  }
});

test("getAlarm repairs a missing backend due-index entry from SQLite storage", async () => {
  const ns = uniqueNs("do-alarm-repair");
  await deployAndPromote(ns, "alarms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_ALARM_WORKER },
    bindings: {
      ALARMS: { type: "do", className: "AlarmCounter" },
    },
  });

  const scheduled = await gatewayFetch(ns, "/alarms/schedule-repair?name=repair");
  const scheduledText = await scheduled.text();
  assert.equal(scheduled.status, 200, scheduledText);
  assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

  const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "repair");
  redisDeleteDoAlarmJob(jobId);
  redisRemoveDoAlarmDue(jobId);
  assert.deepEqual(redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "repair"), {});

  const status = await gatewayFetch(ns, "/alarms/status?name=repair");
  const statusText = await status.text();
  assert.equal(status.status, 200, statusText);
  assert.equal(typeof responseJson({ body: statusText }).pending, "number");
  await waitForDoAlarmJob(ns, "alarms", "AlarmCounter", "repair");
  assert.equal(redisDoAlarmDueIncludes(jobId), true);

  const repairedRecord = redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "repair");
  assert.equal(repairedRecord?.objectName, "repair");

  await waitForJson(
    "workflows-delivered repaired DO alarm",
    async () => {
      const repairedStatus = await gatewayFetch(ns, "/alarms/status?name=repair");
      const repairedStatusText = await repairedStatus.text();
      assert.equal(repairedStatus.status, 200, repairedStatusText);
      return responseJson({ body: repairedStatusText });
    },
    (json) => json.alarms === 1 && json.pending === null,
    45000
  );
});

test("leased DO alarm redelivers after owner task crash before completion", async () => {
  await withDoMultiRuntimes(async () => {
    const ns = uniqueNs("do-alarm-crash");
    await deployAndPromote(ns, "alarms", {
      mainModule: "worker.js",
      modules: { "worker.js": DO_BLOCKING_ALARM_WORKER },
      bindings: {
        ALARMS: { type: "do", className: "AlarmCounter" },
      },
    });

    const scheduled = await gatewayFetch(ns, "/alarms/schedule-blocking?name=crash");
    const scheduledText = await scheduled.text();
    assert.equal(scheduled.status, 200, scheduledText);
    assert.deepEqual(responseJson({ body: scheduledText }), { pending: true });

    const jobId = doAlarmJobId(ns, "alarms", "AlarmCounter", "crash");
    const claimed = await waitForJson(
      "DO alarm Workflows claim lease",
      async () => redisGetDoAlarmJob(ns, "alarms", "AlarmCounter", "crash"),
      (record) => record.status === "running" && Number(record.runLeaseExpiresAtMs) > Date.now() + 5000,
      20000
    );

    const ownerKey = doHostId(ns, "alarms", "AlarmCounter", "crash");
    const owner = redisGetDoOwner(ownerKey);
    assert.ok(owner?.taskId, `missing owner for ${ownerKey}`);
    const killedTask = owner.taskId;
    assert.match(
      killedTask,
      /^[A-Za-z0-9._-]+$/,
      `unexpected taskId format for shell command: ${killedTask}`
    );

    try {
      sh(`COMPOSE_PROFILES=do-multi docker compose kill -s KILL ${killedTask}`, { stdio: "pipe" });
      redisSetDoOwner(ownerKey, { ...owner, leaseExpiresAt: Date.now() - 1000 });

      const retryDue = Date.now() - 1000;
      redisSetDoAlarmJob(jobId, {
        ...claimed,
        status: "waiting",
        dueAtMs: String(retryDue),
        runToken: "",
        runLeaseExpiresAtMs: "",
      });
      redisAddDoAlarmByWorker(ns, "alarms", jobId);
      redisAddDoAlarmDue(retryDue, jobId);

      await waitForJson(
        "redelivered DO alarm after owner crash",
        async () => fetchDoAlarmStatusDuringOwnerRecovery(ns, "crash"),
        (json) => json.started === 1 && json.alarms === 1 && json.pending === null,
        15000
      );
    } finally {
      sh(`COMPOSE_PROFILES=do-multi docker compose up -d${composeUpNoBuildFlag()} --wait ${killedTask}`, {
        stdio: "pipe",
      });
    }
  });
});
