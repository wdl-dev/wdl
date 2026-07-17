import assert from "node:assert/strict";
import { test } from "node:test";
import {
  importRepositoryModule,
  readRepositoryJson,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const SCHEDULER_PROJECTION_CONTRACT = readRepositoryJson(
  "tests/fixtures/scheduler-projection-contract.json"
);
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");

const {
  cronEntryJson,
  cronMetaJson,
  cronRefMember,
  cronSlotExpireAt,
  cronSlotKey,
  cronWorkerKey,
  queueConsumerFields,
  stageCronSlotRef,
  stageCronProjection,
  stageCronWorkerIndexed,
  stageCronWorkerRemoved,
  stageD1ReferrerAdds,
  stageD1ReferrerRemovals,
  stageQueueConsumerProjection,
} = await importRepositoryModule("control/lifecycle-indexes.js", [
  [/import \{[\s\S]*?\} from "control-lib";/,
    `const d1DatabaseReferrersKey = (ns, databaseId) => \`d1db:\${ns}:\${databaseId}:referrers\`;
const encodeReferrerMember = ({ callerNs, callerWorker, callerVersion, binding }) =>
  [callerNs, callerWorker, callerVersion, binding].join("\\t");
const referrersKey = (ns, worker, version) => \`referrers:\${ns}:\${worker}:\${version}\`;
const workersIndexKey = (ns) => \`workers:\${ns}\`;`],
  [/import \{ QUEUE_CONSUMER_INDEX_KEY, queueConsumerKey \} from "shared-queue-keys";/,
    `const QUEUE_CONSUMER_INDEX_KEY = "queue-consumer-index";
const queueConsumerKey = (ns, queue) => \`queue-consumer:\${ns}:\${queue}\`;`],
  [/from "shared-worker-contract"/, `from ${JSON.stringify(WORKER_CONTRACT_URL)}`],
]);

function recordMulti() {
  /** @type {unknown[][]} */
  const calls = [];
  const multi = {
    calls,
    /** @param {unknown[]} args */
    del(...args) {
      calls.push(["del", ...args]);
      return this;
    },
    /** @param {unknown[]} args */
    hSet(...args) {
      calls.push(["hSet", ...args]);
      return this;
    },
    /** @param {unknown[]} args */
    hDel(...args) {
      calls.push(["hDel", ...args]);
      return this;
    },
    /** @param {unknown[]} args */
    expireAt(...args) {
      calls.push(["expireAt", ...args]);
      return this;
    },
    /** @param {unknown[]} args */
    sAdd(...args) {
      calls.push(["sAdd", ...args]);
      return this;
    },
    /** @param {unknown[]} args */
    sRem(...args) {
      calls.push(["sRem", ...args]);
      return this;
    },
  };
  return multi;
}

test("cron worker index helpers maintain non-authoritative discovery members", () => {
  const multi = recordMulti();
  stageCronWorkerIndexed(multi, "tenant", "worker");
  stageCronWorkerRemoved(multi, "tenant", "worker");

  assert.deepEqual(multi.calls, [
    ["sAdd", "cron:index:workers", "crons:tenant:worker"],
    ["sRem", "cron:index:workers", "crons:tenant:worker"],
  ]);
});

test("cron key helpers compose worker, slot, and ref members", () => {
  const cron = SCHEDULER_PROJECTION_CONTRACT.cron;
  assert.equal(cronWorkerKey(cron.ns, cron.worker), cron.workerKey);
  assert.equal(cronSlotKey(cron.slotMs), cron.slotKey);
  assert.equal(cronSlotExpireAt(cron.slotMs), cron.slotExpireAt);
  assert.equal(cronRefMember(cron.ns, cron.worker, cron.cronId, cron.gen), cron.reference);
});

test("stageCronSlotRef writes ref and expiry together", () => {
  const cron = SCHEDULER_PROJECTION_CONTRACT.cron;
  const multi = recordMulti();
  stageCronSlotRef(multi, cron.ns, cron.worker, {
    id: cron.cronId,
    gen: cron.gen,
    slot: cron.slotMs,
  });

  assert.deepEqual(multi.calls, [
    ["sAdd", cron.slotKey, cron.reference],
    ["expireAt", cron.slotKey, cron.slotExpireAt],
  ]);
});

test("stageCronProjection writes the shared scheduler projection contract", () => {
  const cron = SCHEDULER_PROJECTION_CONTRACT.cron;
  const multi = recordMulti();
  const entry = {
    id: cron.cronId,
    cron: cron.entry.cron,
    timezone: cron.entry.timezone,
    gen: cron.entry.gen,
    slot: cron.slotMs,
  };
  stageCronProjection(multi, {
    ns: cron.ns,
    worker: cron.worker,
    version: cron.meta.version,
    cronKey: cron.workerKey,
    existingHash: {},
    crons: [{ cron: entry.cron, timezone: entry.timezone }],
    plan: { cronSeq: cron.meta.seq, addedWithPlacement: [entry], removed: [] },
  });

  assert.equal(cronMetaJson(cron.meta.version, cron.meta.seq), cron.meta.json);
  assert.equal(cronEntryJson(entry), cron.entry.json);
  assert.deepEqual(multi.calls, [
    ["sAdd", cron.workerIndexKey, cron.workerKey],
    ["hSet", cron.workerKey, "__meta__", cron.meta.json],
    ["hSet", cron.workerKey, cron.cronId, cron.entry.json],
    ["sAdd", cron.slotKey, cron.reference],
    ["expireAt", cron.slotKey, cron.slotExpireAt],
  ]);
});

test("stageQueueConsumerProjection writes full optional queue consumer fields", () => {
  const contract = SCHEDULER_PROJECTION_CONTRACT.queueConsumer;
  const multi = recordMulti();
  assert.equal(contract.input.queue, contract.queue);
  assert.deepEqual(
    queueConsumerFields(contract.worker, contract.version, contract.input),
    contract.fields
  );
  stageQueueConsumerProjection(
    multi,
    contract.ns,
    contract.worker,
    contract.version,
    contract.input
  );

  assert.deepEqual(multi.calls, [
    ["del", `queue-consumer:${contract.ns}:${contract.input.queue}`],
    ["hSet", `queue-consumer:${contract.ns}:${contract.input.queue}`, contract.fields],
    ["sAdd", "queue-consumer-index", `queue-consumer:${contract.ns}:${contract.input.queue}`],
  ]);
});

test("stageQueueConsumerProjection omits absent optional queue consumer fields", () => {
  const multi = recordMulti();
  stageQueueConsumerProjection(multi, "tenant", "worker", "v3", {
    queue: "jobs",
    maxBatchSize: 10,
    maxBatchTimeoutMs: 250,
    maxRetries: 4,
  });

  assert.deepEqual(multi.calls[1], [
    "hSet",
    "queue-consumer:tenant:jobs",
    {
      worker: "worker",
      version: "v3",
      max_batch_size: "10",
      max_batch_timeout_ms: "250",
      max_retries: "4",
    },
  ]);
});

test("stageQueueConsumerProjection preserves retryDelaySeconds zero without dead letter queue", () => {
  const multi = recordMulti();
  stageQueueConsumerProjection(multi, "tenant", "worker", "v3", {
    queue: "jobs",
    maxBatchSize: 10,
    maxBatchTimeoutMs: 250,
    maxRetries: 4,
    retryDelaySeconds: 0,
  });

  assert.deepEqual(multi.calls[1][2], {
    worker: "worker",
    version: "v3",
    max_batch_size: "10",
    max_batch_timeout_ms: "250",
    max_retries: "4",
    retry_delay_secs: "0",
  });
});

test("stageD1Referrer helpers use caller-provided database id accessor", () => {
  const refs = [
    {
      binding: "DB",
      databaseId: "frozen-db",
      resolvedDatabaseId: "resolved-db",
    },
  ];

  const adds = recordMulti();
  stageD1ReferrerAdds(adds, {
    ns: "tenant",
    worker: "caller",
    version: "v2",
    refs,
    databaseIdFor: (/** @type {any} */ ref) => ref.resolvedDatabaseId,
  });
  assert.deepEqual(adds.calls, [
    ["sAdd", "d1db:tenant:resolved-db:referrers", "tenant\tcaller\tv2\tDB"],
  ]);

  const removals = recordMulti();
  stageD1ReferrerRemovals(removals, {
    ns: "tenant",
    worker: "caller",
    version: "v2",
    refs,
    databaseIdFor: (/** @type {any} */ ref) => ref.databaseId,
  });
  assert.deepEqual(removals.calls, [
    ["sRem", "d1db:tenant:frozen-db:referrers", "tenant\tcaller\tv2\tDB"],
  ]);
});
