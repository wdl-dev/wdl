import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyFakeRedisOp,
  createFakeRedis,
  createFakeRedisState,
} from "../helpers/mocks/fake-redis.js";
import { loadControlRouting } from "../helpers/load-control-routing.js";
import { loadControlLib } from "../helpers/load-control-lib.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { bundleKey as productionBundleKey } from "../../shared/worker-contract.js";

const { promoteWithRoutes, bumpActiveAndPromote, reconcileHosts } =
  await loadControlRouting();
const { controlLib } = await loadControlLib();
const { encodeReferrerMember } = controlLib;
const CRON_ID = /** @type {{ cron: { cronId: string } }} */ (
  readRepositoryJson("tests/fixtures/scheduler-projection-contract.json")
).cron.cronId;

function makeRedis() {
  return createFakeRedis();
}

/**
 * @param {ReturnType<typeof makeRedis>} redis
 * @param {string} version
 * @param {any} meta
 */
function seedBundle(redis, version, meta) {
  redis.state.hashes.set(productionBundleKey("demo", "worker", version), {
    __meta__: JSON.stringify(meta),
  });
}

/**
 * @param {string} ns @param {string} worker @param {string} version @param {string} kind @param {string} value
 */
function patternProjection(ns, worker, version, kind, value) {
  return ["v2", ns, worker, version, kind, value].join("\t");
}

/** @param {Record<string, string>} cronHash */
function onlyCronEntry(cronHash) {
  const found = Object.entries(cronHash).find(([field]) => field !== "__meta__");
  assert.ok(found);
  return JSON.parse(found[1]);
}

/** @param {Record<string, string>} cronHash */
function readMeta(cronHash) {
  const raw = cronHash["__meta__"];
  assert.equal(typeof raw, "string");
  return JSON.parse(raw);
}

/**
 * @param {unknown} err
 * @param {number} status
 * @param {string} code
 */
function assertRoutingErrorShape(err, status, code) {
  assert.ok(err && typeof err === "object");
  const shaped = /** @type {{ status?: unknown, code?: unknown, details?: unknown }} */ (err);
  assert.equal(shaped.status, status);
  assert.equal(shaped.code, code);
  assert.ok(shaped.details && typeof shaped.details === "object");
  return /** @type {{ details: Record<string, any> }} */ (shaped);
}

const consumerWithOptions = {
  queue: "jobs",
  maxBatchSize: 5,
  maxBatchTimeoutMs: 2000,
  maxRetries: 3,
  retryDelaySeconds: 45,
  deadLetterQueue: "jobs-dlq",
};

const consumerWithoutOptions = {
  queue: "jobs",
  maxBatchSize: 5,
  maxBatchTimeoutMs: 2000,
  maxRetries: 3,
};

test("promoteWithRoutes replaces queue consumer hash so removed optional fields disappear", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { queueConsumers: [consumerWithOptions] });
  seedBundle(redis, "v2", { queueConsumers: [consumerWithoutOptions] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.hashes.set("queue-consumer:demo:jobs", {
    worker: "worker",
    version: "v1",
    max_batch_size: "5",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
    retry_delay_secs: "45",
    dead_letter_queue: "jobs-dlq",
  });

  await promoteWithRoutes(redis, "demo", "worker", "v2");

  assert.deepEqual(redis.state.hashes.get("queue-consumer:demo:jobs"), {
    worker: "worker",
    version: "v2",
    max_batch_size: "5",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
  });
  const queueOps = redis.state.ops.filter((op) => op[1] === "queue-consumer:demo:jobs");
  assert.deepEqual(queueOps.map((op) => op[0]), ["del", "hSet"]);
  assert.ok(redis.state.sets.get("queue:index:consumers")?.has("queue-consumer:demo:jobs"));
});

test("promoteWithRoutes removes queue consumer discovery index entries for removed consumers", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { queueConsumers: [consumerWithOptions] });
  seedBundle(redis, "v2", { queueConsumers: [] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.hashes.set("queue-consumer:demo:jobs", {
    worker: "worker",
    version: "v1",
    max_batch_size: "5",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
  });
  redis.state.sets.set("queue:index:consumers", new Set(["queue-consumer:demo:jobs"]));

  await promoteWithRoutes(redis, "demo", "worker", "v2");

  assert.equal(redis.state.hashes.has("queue-consumer:demo:jobs"), false);
  assert.equal(redis.state.sets.get("queue:index:consumers")?.has("queue-consumer:demo:jobs") ?? false, false);
  assert.ok(redis.state.ops.some((op) =>
    op[0] === "sRem" &&
    op[1] === "queue:index:consumers" &&
    op[2] === "queue-consumer:demo:jobs"
  ));
});

for (const [label, rawMeta] of [
  ["missing", null],
  ["empty", ""],
]) {
  test(`promoteWithRoutes rejects ${label} active bundle metadata before changing projections`, async () => {
    const redis = makeRedis();
    seedBundle(redis, "v2", { routes: [], queueConsumers: [] });
    if (rawMeta !== null) {
      redis.state.hashes.set(productionBundleKey("demo", "worker", "v1"), {
        __meta__: rawMeta,
      });
    }
    redis.state.hashes.set("routes:demo", { worker: "v1" });
    redis.state.hashes.set("patterns:api.example", {
      "/": patternProjection("demo", "worker", "v1", "exact", "/"),
    });
    redis.state.hashes.set("queue-consumer:demo:jobs", {
      worker: "worker",
      version: "v1",
    });

    await assert.rejects(
      promoteWithRoutes(redis, "demo", "worker", "v2"),
      (err) => {
        assertRoutingErrorShape(err, 500, "corrupt_meta");
        return true;
      }
    );

    assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v1");
    assert.equal(redis.state.hashes.get("queue-consumer:demo:jobs")?.version, "v1");
    assert.equal(redis.state.hashes.get("patterns:api.example")?.["/"],
      patternProjection("demo", "worker", "v1", "exact", "/"));
  });
}

test("promoteWithRoutes skips empty platform route versions while checking exported as names", async () => {
  const redis = makeRedis();
  redis.state.hashes.set(productionBundleKey("__platform__", "api", "v1"), {
    __meta__: JSON.stringify({ exports: [{ name: "default", as: "demo" }] }),
  });
  redis.state.hashes.set(productionBundleKey("__platform__", "other", "v2"), {
    __meta__: JSON.stringify({ exports: [{ name: "default", as: "other-demo" }] }),
  });
  redis.state.hashes.set("routes:__platform__", { stale: "", other: "v2" });

  await promoteWithRoutes(redis, "__platform__", "api", "v1");

  assert.equal(redis.state.hashes.get("routes:__platform__")?.api, "v1");
  assert.deepEqual(
    redis.state.commands.find((op) => op[0] === "hGetMany"),
    ["hGetMany", [[productionBundleKey("__platform__", "other", "v2"), "__meta__"]]]
  );
});

test("promoteWithRoutes rejects a platform route whose active metadata is missing", async () => {
  const redis = makeRedis();
  redis.state.hashes.set(productionBundleKey("__platform__", "api", "v1"), {
    __meta__: JSON.stringify({ exports: [{ entrypoint: "default", as: "demo" }] }),
  });
  redis.state.hashes.set("routes:__platform__", { other: "v2" });

  await assert.rejects(
    promoteWithRoutes(redis, "__platform__", "api", "v1"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_meta");
      return true;
    }
  );

  assert.equal(redis.state.hashes.get("routes:__platform__")?.api, undefined);
});

test("promoteWithRoutes retries from the new active when the prior bundle is deleted", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { routes: [], queueConsumers: [] });
  seedBundle(redis, "v2", { routes: [], queueConsumers: [] });
  seedBundle(redis, "v3", { routes: [], queueConsumers: [consumerWithoutOptions] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });

  const originalSession = redis.session.bind(redis);
  let attempts = 0;
  redis.session = async (fn) => {
    attempts += 1;
    return await originalSession(async (iso) => {
      if (attempts !== 1) return await fn(iso);
      const originalHGet = iso.hGet.bind(iso);
      let raced = false;
      return await fn({
        ...iso,
        async hGet(key, field) {
          const value = await originalHGet(key, field);
          if (!raced && key === "routes:demo" && field === "worker") {
            raced = true;
            redis.state.hashes.set("routes:demo", { worker: "v3" });
            redis.state.hashes.set("queue-consumer:demo:jobs", {
              worker: "worker",
              version: "v3",
              max_batch_size: "5",
              max_batch_timeout_ms: "2000",
              max_retries: "3",
            });
            redis.state.sets.set(
              "queue:index:consumers",
              new Set(["queue-consumer:demo:jobs"])
            );
            redis.state.hashes.delete(productionBundleKey("demo", "worker", "v1"));
          }
          return value;
        },
      });
    });
  };

  await promoteWithRoutes(redis, "demo", "worker", "v2");

  assert.equal(attempts, 2);
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v2");
  assert.equal(redis.state.hashes.has("queue-consumer:demo:jobs"), false);
  assert.equal(
    redis.state.sets.get("queue:index:consumers")?.has("queue-consumer:demo:jobs") ?? false,
    false
  );
});

test("promoteWithRoutes retries when a platform route is deleted during as validation", async () => {
  const redis = makeRedis();
  redis.state.hashes.set(productionBundleKey("__platform__", "api", "v1"), {
    __meta__: JSON.stringify({ exports: [{ entrypoint: "default", as: "demo" }] }),
  });
  redis.state.hashes.set(productionBundleKey("__platform__", "other", "v2"), {
    __meta__: JSON.stringify({ exports: [{ entrypoint: "default", as: "other" }] }),
  });
  redis.state.hashes.set("routes:__platform__", { other: "v2" });

  const originalSession = redis.session.bind(redis);
  let attempts = 0;
  redis.session = async (fn) => {
    attempts += 1;
    return await originalSession(async (iso) => {
      if (attempts !== 1) return await fn(iso);
      const originalHGetMany = iso.hGetMany.bind(iso);
      let raced = false;
      return await fn({
        ...iso,
        async hGetMany(pairs) {
          if (!raced && pairs.some(([key]) =>
            key === productionBundleKey("__platform__", "other", "v2")
          )) {
            raced = true;
            delete redis.state.hashes.get("routes:__platform__")?.other;
            redis.state.hashes.delete(productionBundleKey("__platform__", "other", "v2"));
          }
          return await originalHGetMany(pairs);
        },
      });
    });
  };

  await promoteWithRoutes(redis, "__platform__", "api", "v1");

  assert.equal(attempts, 2);
  assert.equal(redis.state.hashes.get("routes:__platform__")?.api, "v1");
});

test("promoteWithRoutes rejects non-object candidate bundle metadata", async () => {
  const redis = makeRedis();
  redis.state.hashes.set(productionBundleKey("demo", "worker", "v1"), {
    __meta__: "[]",
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_meta");
      return true;
    }
  );
});

test("promoteWithRoutes rejects malformed cron metadata", async () => {
  const redis = makeRedis();
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const logs = [];
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: "{bad",
    abc123: "{also-bad",
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1", {
      requestId: "rid-promote",
      log: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
        logs.push({ level, event, fields }),
    }),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );

  assert.deepEqual(logs, [
    {
      level: "warn",
      event: "cron_projection_malformed",
      fields: { request_id: "rid-promote", namespace: "demo", worker: "worker", field: "__meta__" },
    },
  ]);
});

test("promoteWithRoutes rejects malformed cron sequence metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: JSON.stringify({ version: "v0", seq: "bad" }),
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );
});

test("promoteWithRoutes rejects non-object cron metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: "[]",
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );
});

test("promoteWithRoutes allocates cron generations from the permanent epoch", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
  });

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  const cronHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(cronHash);
  const meta = readMeta(cronHash);
  const entry = onlyCronEntry(cronHash);
  assert.deepEqual(meta, { version: "v1" });
  assert.equal(entry.gen, 1024);
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1024");
  assert.ok(redis.state.watched.includes("cron:seq:demo:worker"));
});

test("promoteWithRoutes never reuses a cron generation after clearing the projection", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
  });
  seedBundle(redis, "v2", { crons: [] });
  seedBundle(redis, "v3", {
    crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
  });

  await promoteWithRoutes(redis, "demo", "worker", "v1");
  const firstHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(firstHash);
  const firstEntry = onlyCronEntry(firstHash);

  await promoteWithRoutes(redis, "demo", "worker", "v2");
  assert.equal(redis.state.hashes.has("crons:demo:worker"), false);
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1024");

  await promoteWithRoutes(redis, "demo", "worker", "v3");
  const recreatedHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(recreatedHash);
  const recreatedEntry = onlyCronEntry(recreatedHash);
  assert.equal(recreatedEntry.gen, firstEntry.gen + 1);
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1025");
});

test("concurrent cron promotion retries from the committed generation counter", async () => {
  const state = createFakeRedisState();
  const redis = createFakeRedis(state, {
    onExecFailure(ops) {
      for (const op of ops) applyFakeRedisOp(state, op);
    },
  });
  redis.execFailures = 1;
  seedBundle(redis, "v1", {
    crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
  });

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  const cronHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(cronHash);
  assert.equal(onlyCronEntry(cronHash).gen, 1024);
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1024");
  assert.equal(redis.state.execFailures, 0);
});

const invalidCronSequences = /** @type {Array<[string, string]>} */ ([
  ["malformed", "not-a-number"],
  ["below the reserved epoch", "100"],
]);
for (const [label, sequence] of invalidCronSequences) {
  test(`promoteWithRoutes rejects permanent cron sequence: ${label}`, async () => {
    const redis = makeRedis();
    seedBundle(redis, "v2", {
      crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
    });
    redis.state.hashes.set("crons:demo:worker", {
      __meta__: JSON.stringify({ version: "v1" }),
      existing: JSON.stringify({ cron: "*/5 * * * *", timezone: "UTC", gen: 1 }),
    });
    redis.state.strings.set("cron:seq:demo:worker", sequence);

    await assert.rejects(
      promoteWithRoutes(redis, "demo", "worker", "v2"),
      (err) => {
        assertRoutingErrorShape(err, 500, "corrupt_cron_sequence");
        return true;
      }
    );
  });
}

test("promoteWithRoutes treats the permanent cron sequence as authoritative", async () => {
  const redis = makeRedis();
  const cron = { cron: "*/5 * * * *", timezone: "UTC" };
  const id = CRON_ID;
  seedBundle(redis, "v2", { crons: [cron] });
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: JSON.stringify({ version: "v1", seq: "obsolete" }),
    [id]: JSON.stringify({ ...cron, gen: 1024 }),
  });
  redis.state.strings.set("cron:seq:demo:worker", "1024");

  await promoteWithRoutes(redis, "demo", "worker", "v2");

  const cronHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(cronHash);
  assert.deepEqual(readMeta(cronHash), { version: "v2" });
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1024");
});

test("promoteWithRoutes rejects projection metadata without a permanent sequence", async () => {
  const redis = makeRedis();
  const cron = { cron: "*/5 * * * *", timezone: "UTC" };
  const id = CRON_ID;
  seedBundle(redis, "v2", { crons: [cron] });
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: JSON.stringify({ version: "v1" }),
    [id]: JSON.stringify({ ...cron, gen: 1024 }),
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v2"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_sequence");
      return true;
    }
  );
});

test("promoteWithRoutes watches and rejects missing service-binding target bundles", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      TARGET: {
        type: "service",
        ns: "other",
        service: "api",
        version: "v3",
      },
    },
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "service_binding_dependency_missing");
      assert.equal(shaped.details.broken_dependency.targetNs, "other");
      return true;
    }
  );
  assert.ok(redis.state.watched.includes(productionBundleKey("other", "api", "v3")));
  assert.equal(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")), false);
});

test("promoteWithRoutes rejects empty service-binding target metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      TARGET: {
        type: "service",
        ns: "other",
        service: "api",
        version: "v3",
      },
    },
  });
  redis.state.hashes.set(productionBundleKey("other", "api", "v3"), { __meta__: "" });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "service_binding_dependency_missing");
      assert.equal(shaped.details.broken_dependency.targetNs, "other");
      return true;
    }
  );
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, undefined);
});

test("promoteWithRoutes batches service-binding dependency watches", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      TARGET_A: {
        type: "service",
        ns: "other",
        service: "api",
        version: "v3",
      },
      TARGET_B: {
        type: "service",
        ns: "other",
        service: "queue",
        version: "v4",
      },
    },
  });
  redis.state.hashes.set(productionBundleKey("other", "api", "v3"), { __meta__: "{}" });
  redis.state.hashes.set(productionBundleKey("other", "queue", "v4"), { __meta__: "{}" });

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  assert.ok(redis.state.watchBatches.some((batch) =>
    batch.length === 2 &&
    batch.includes(productionBundleKey("other", "api", "v3")) &&
    batch.includes(productionBundleKey("other", "queue", "v4"))
  ));
});

test("promoteWithRoutes reads active bundle metadata once", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { routes: [], queueConsumers: [] });
  seedBundle(redis, "v2", { routes: [], queueConsumers: [] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });

  await promoteWithRoutes(redis, "demo", "worker", "v2");

  assert.equal(redis.state.commands.filter(([command, key, field]) =>
    command === "hGet" &&
    key === productionBundleKey("demo", "worker", "v1") &&
    field === "__meta__"
  ).length, 1);
});

test("promoteWithRoutes batches D1, service, and queue dependency reads", async () => {
  const redis = makeRedis();
  const d1Keys = ["d1_a", "d1_b"].map((databaseId) => `d1:database:demo:${databaseId}`);
  const servicePairs = [
    [productionBundleKey("other", "api", "v3"), "__meta__"],
    [productionBundleKey("other", "jobs", "v4"), "__meta__"],
  ];
  const queuePairs = [
    ["queue-consumer:demo:queue-a", "worker"],
    ["queue-consumer:demo:queue-b", "worker"],
  ];
  seedBundle(redis, "v1", {
    bindings: {
      DB_A: { type: "d1", databaseId: "d1_a" },
      DB_B: { type: "d1", databaseId: "d1_b" },
      SERVICE_A: { type: "service", ns: "other", service: "api", version: "v3" },
      SERVICE_B: { type: "service", ns: "other", service: "jobs", version: "v4" },
    },
    queueConsumers: [
      { ...consumerWithoutOptions, queue: "queue-a" },
      { ...consumerWithoutOptions, queue: "queue-b" },
    ],
  });
  for (const key of d1Keys) redis.state.hashes.set(key, { state: "ready" });
  for (const [key] of servicePairs) redis.state.hashes.set(key, { __meta__: "{}" });
  for (const [key] of queuePairs) redis.state.hashes.set(key, { worker: "worker" });

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  assert.ok(redis.state.commands.some((command) =>
    command[0] === "exists" && command.slice(1).toSorted().join("\n") === d1Keys.toSorted().join("\n")
  ));
  const batchedHashReads = redis.state.commands
    .filter(([command]) => command === "hGetMany")
    .map(([, pairs]) => /** @type {Array<[string, string]>} */ (pairs));
  assert.deepEqual(
    redis.state.commands.find(([command]) => command === "hStrLenMany")?.[1],
    servicePairs
  );
  assert.deepEqual(
    batchedHashReads.find((pairs) => pairs[0]?.[0] === queuePairs[0][0]),
    queuePairs
  );
  assert.equal(redis.state.commands.filter(([command, key]) =>
    command === "hGetAll" && queuePairs.some(([queueKey]) => queueKey === key)
  ).length, 0);
});

test("promoteWithRoutes deduplicates and bounds service dependency metadata probes", async () => {
  const redis = makeRedis();
  const bindings = Object.fromEntries(Array.from({ length: 66 }, (_, index) => [
    `SERVICE_${index}`,
    {
      type: "service",
      ns: "other",
      service: `target-${index % 65}`,
      version: "v1",
    },
  ]));
  seedBundle(redis, "v1", { bindings });
  for (let index = 0; index < 65; index += 1) {
    redis.state.hashes.set(productionBundleKey("other", `target-${index}`, "v1"), {
      __meta__: "{}",
    });
  }

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  const reads = redis.state.commands
    .filter(([command]) => command === "hStrLenMany")
    .map(([, pairs]) => /** @type {Array<[string, string]>} */ (pairs));
  assert.deepEqual(reads.map((pairs) => pairs.length), [64, 1]);
  assert.equal(new Set(reads.flat().map(([key]) => key)).size, 65);
});

test("bumpActiveAndPromote also rewrites full queue consumer projection", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { queueConsumers: [consumerWithoutOptions] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.hashes.set("queue-consumer:demo:jobs", {
    worker: "worker",
    version: "v1",
    max_batch_size: "5",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
    retry_delay_secs: "45",
    dead_letter_queue: "jobs-dlq",
  });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  const result = await bumpActiveAndPromote(redis, "demo", "worker");

  assert.equal(result.version, "v2");
  assert.deepEqual(redis.state.hashes.get("queue-consumer:demo:jobs"), {
    worker: "worker",
    version: "v2",
    max_batch_size: "5",
    max_batch_timeout_ms: "2000",
    max_retries: "3",
  });
  assert.equal(redis.state.hashes.has("crons:demo:worker"), false);
  assert.equal(redis.state.strings.has("cron:seq:demo:worker"), false);
  assert.equal(redis.state.sets.get("cron:index:workers")?.size ?? 0, 0);
});

test("bumpActiveAndPromote batches pattern projection reads across hosts", async () => {
  const redis = makeRedis();
  const routes = ["api.example", "admin.example"].map((host) => ({
    host,
    slot: "/*",
    kind: "prefix",
    value: "/",
  }));
  seedBundle(redis, "v1", { routes });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.sets.set("hosts:demo", new Set(routes.map((route) => route.host)));
  for (const route of routes) {
    redis.state.hashes.set(`patterns:${route.host}`, {
      [route.slot]: patternProjection("demo", "worker", "v1", route.kind, route.value),
    });
  }
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await bumpActiveAndPromote(redis, "demo", "worker");

  assert.deepEqual(
    redis.state.commands.filter(([command]) => command === "hGetAllMany"),
    [["hGetAllMany", ["patterns:api.example", "patterns:admin.example"]]]
  );
  assert.equal(
    redis.state.commands.some(([command, key]) =>
      command === "hGetAll" && typeof key === "string" && key.startsWith("patterns:")
    ),
    false
  );
});

test("bumpActiveAndPromote retries when active changes before source metadata read", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  seedBundle(redis, "v3", {});
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "3");

  const originalSession = redis.session.bind(redis);
  let attempts = 0;
  redis.session = async (fn) => {
    attempts += 1;
    return await originalSession(async (iso) => {
      if (attempts !== 1) return await fn(iso);
      const originalHGet = iso.hGet.bind(iso);
      let raced = false;
      return await fn({
        ...iso,
        async hGet(key, field) {
          const value = await originalHGet(key, field);
          if (!raced && key === "routes:demo" && field === "worker") {
            raced = true;
            redis.state.hashes.set("routes:demo", { worker: "v3" });
            redis.state.hashes.delete(productionBundleKey("demo", "worker", "v1"));
          }
          return value;
        },
      });
    });
  };

  const result = await bumpActiveAndPromote(redis, "demo", "worker");

  assert.equal(attempts, 2);
  assert.equal(result.previousVersion, "v3");
  assert.equal(result.version, "v4");
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v4");
  assert.ok(redis.state.hashes.has(productionBundleKey("demo", "worker", "v4")));
});

test("promoteWithRoutes rejects missing D1 dependency databases", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      DB: { type: "d1", databaseId: "d1_main" },
    },
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "d1_database_dependency_missing");
      assert.deepEqual(shaped.details.broken_d1_dependency, { binding: "DB", databaseId: "d1_main" });
      return true;
    }
  );
  assert.ok(redis.state.watched.includes("d1:database:demo:d1_main"));
  assert.equal(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")), false);
});

test("bumpActiveAndPromote stages D1 referrers from production binding metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      DB: { type: "d1", databaseId: "d1_main" },
    },
  });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.hashes.set("d1:database:demo:d1_main", { state: "ready" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  const result = await bumpActiveAndPromote(redis, "demo", "worker");

  assert.equal(result.version, "v2");
  assert.ok(redis.state.watched.includes("d1:database:demo:d1_main"));
  assert.ok(redis.state.sets.get("d1:database-referrers:demo:d1_main")?.has(encodeReferrerMember({
    callerNs: "demo",
    callerWorker: "worker",
    callerVersion: "v2",
    binding: "DB",
  })));
});

test("bumpActiveAndPromote lets callers stage writes in the same copy transaction", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  const result = await bumpActiveAndPromote(redis, "demo", "worker", {
    /** @param {{ iso: { watch: (...keys: string[]) => Promise<unknown> }, multi: { hSet: (key: string, field: string, value: string) => unknown }, currentVersion: string, newVersion: string }} context */
    stageBeforeCopy: async ({ iso, multi, currentVersion, newVersion }) => {
      assert.equal(currentVersion, "v1");
      assert.equal(newVersion, "v2");
      await iso.watch("secrets:demo:worker");
      multi.hSet("secrets:demo:worker", "TOKEN", "encrypted");
    },
  });

  assert.equal(result.version, "v2");
  assert.equal(redis.state.hashes.get("secrets:demo:worker")?.TOKEN, "encrypted");
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v2");
  assert.ok(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")));
  assert.ok(redis.state.watched.includes("secrets:demo:worker"));
  const stageIndex = redis.state.ops.findIndex((op) =>
    op[0] === "hSet" && op[1] === "secrets:demo:worker"
  );
  const copyIndex = redis.state.ops.findIndex((op) =>
    op[0] === "copy" &&
    op[1] === productionBundleKey("demo", "worker", "v1") &&
    op[2] === productionBundleKey("demo", "worker", "v2")
  );
  assert.ok(stageIndex >= 0);
  assert.ok(copyIndex > stageIndex);
  assert.deepEqual(redis.state.ops[copyIndex][3], { REPLACE: true });
});

test("bumpActiveAndPromote aborts copy and route flip when staged writes fail", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker", {
      /** @param {{ iso: { watch: (...keys: string[]) => Promise<unknown> } }} context */
      stageBeforeCopy: async ({ iso }) => {
        await iso.watch("secrets:demo:worker");
        throw new Error("budget rejected");
      },
    }),
    /budget rejected/
  );

  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v1");
  assert.equal(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")), false);
  assert.equal(redis.state.ops.some((op) => op[0] === "copy"), false);
  assert.equal(redis.state.ops.some((op) => op[0] === "hSet" && op[1] === "routes:demo"), false);
  assert.ok(redis.state.watched.includes("secrets:demo:worker"));
});

test("bumpActiveAndPromote preserves bundle_copy_failed for a missing source bundle", async () => {
  const redis = makeRedis();
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      assertRoutingErrorShape(err, 500, "bundle_copy_failed");
      return true;
    }
  );
  assert.equal(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")), false);
});

test("bumpActiveAndPromote rejects active routes that no longer declare their hosts", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    routes: [{
      host: "app.workers.example",
      slot: "/*",
      kind: "prefix",
      value: "/",
    }],
  });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 403, "host_not_declared");
      assert.equal(shaped.details.host, "app.workers.example");
      return true;
    }
  );
});

test("bumpActiveAndPromote fails closed on malformed held pattern projections", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    routes: [{
      host: "app.workers.example",
      slot: "/*",
      kind: "prefix",
      value: "/",
    }],
  });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.hashes.set("patterns:app.workers.example", { "/*": "not-a-projection" });
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_pattern_projection");
      return true;
    }
  );

  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v1");
  assert.equal(redis.state.hashes.has(productionBundleKey("demo", "worker", "v2")), false);
});

test("bumpActiveAndPromote rejects malformed cron metadata", async () => {
  const redis = makeRedis();
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const logs = [];
  seedBundle(redis, "v1", { queueConsumers: [] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");
  redis.state.hashes.set("crons:demo:worker", { __meta__: "{bad" });

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker", {
      requestId: "rid-secret",
      log: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
        logs.push({ level, event, fields }),
    }),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );

  assert.deepEqual(logs, [
    {
      level: "warn",
      event: "cron_projection_malformed",
      fields: { request_id: "rid-secret", namespace: "demo", worker: "worker", field: "__meta__" },
    },
  ]);
});

test("bumpActiveAndPromote initializes a missing permanent sequence from projection metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    crons: [{ cron: "*/5 * * * *", timezone: "UTC" }],
  });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: JSON.stringify({ version: "v1", seq: 1 }),
    existing: JSON.stringify({ cron: "*/5 * * * *", timezone: "UTC", gen: 1 }),
  });

  const result = await bumpActiveAndPromote(redis, "demo", "worker");

  assert.equal(result.version, "v2");
  assert.equal(redis.state.strings.get("cron:seq:demo:worker"), "1023");
  const cronHash = redis.state.hashes.get("crons:demo:worker");
  assert.ok(cronHash);
  assert.deepEqual(readMeta(cronHash), { version: "v2" });
});

test("bumpActiveAndPromote rejects malformed cron sequence metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");
  redis.state.hashes.set("crons:demo:worker", {
    __meta__: JSON.stringify({ version: "v1", seq: -1 }),
  });

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );
});

test("bumpActiveAndPromote rejects non-object cron metadata", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {});
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");
  redis.state.hashes.set("crons:demo:worker", { __meta__: JSON.stringify("bad") });

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      assertRoutingErrorShape(err, 500, "corrupt_cron_meta");
      return true;
    }
  );
});

test("bumpActiveAndPromote rejects missing service-binding target bundles", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      TARGET: {
        type: "service",
        ns: "other",
        service: "api",
        version: "v3",
      },
    },
  });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "service_binding_dependency_missing");
      assert.equal(shaped.details.broken_dependency.targetNs, "other");
      return true;
    }
  );
  assert.ok(redis.state.watched.includes(productionBundleKey("other", "api", "v3")));
});

test("promoteWithRoutes rejects a custom host already owned by another namespace", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    routes: [{
      host: "app.workers.example",
      slot: "/admin/*",
      kind: "prefix",
      value: "/admin/",
    }],
  });
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.hashes.set("patterns:app.workers.example", {
    "/*": patternProjection("other", "site", "v9", "prefix", "/"),
  });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "route_conflict");
      assert.equal(shaped.details.host, "app.workers.example");
      assert.equal(shaped.details.slot, "/*");
      assert.equal(Object.hasOwn(shaped.details, "held"), false);
      return true;
    }
  );
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, undefined);
});

test("promoteWithRoutes fails closed on malformed occupied pattern projections", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    routes: [{
      host: "app.workers.example",
      slot: "/*",
      kind: "prefix",
      value: "/",
    }],
  });
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.hashes.set("patterns:app.workers.example", { "/*": "not-a-projection" });

  await assert.rejects(
    promoteWithRoutes(redis, "demo", "worker", "v1"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 500, "corrupt_pattern_projection");
      assert.deepEqual(shaped.details, { host: "app.workers.example", slot: "/*" });
      return true;
    }
  );
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, undefined);
});

test("reconcileHosts stages declared host indexes and pattern invalidation", async () => {
  const redis = makeRedis();

  await reconcileHosts(redis, "demo", { hosts: ["app.workers.example"] }, "workers.local");

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example"), true);
  assert.equal(redis.state.sets.get("declared-hosts")?.has("app.workers.example"), true);
  assert.equal(redis.state.sets.get("host-declarations:app.workers.example")?.has("demo"), true);
  assert.equal(redis.state.strings.get("declared-hosts:revision"), "1");
  assert.ok(redis.state.ops.some((op) =>
    op[0] === "publish" &&
    op[1] === "patterns:invalidate" &&
    op[2] === "app.workers.example"
  ));
});

test("reconcileHosts no-op watches only the namespace host source", async () => {
  const redis = makeRedis();
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));

  await reconcileHosts(redis, "demo", { hosts: ["app.workers.example"] }, "workers.local");

  assert.deepEqual(redis.state.watchBatches, [["hosts:demo"]]);
  assert.equal(redis.state.ops.length, 0);
});

test("reconcileHosts preserves global host gate while another namespace still declares host", async () => {
  const redis = makeRedis();
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.sets.set("declared-hosts", new Set(["app.workers.example"]));
  redis.state.sets.set("host-declarations:app.workers.example", new Set(["demo", "other"]));

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example") ?? false, false);
  assert.equal(redis.state.sets.get("declared-hosts")?.has("app.workers.example"), true);
  assert.deepEqual(redis.state.sets.get("host-declarations:app.workers.example"), new Set(["other"]));
});

test("reconcileHosts removes global host gate after the final declaration is removed", async () => {
  const redis = makeRedis();
  const hosts = ["api.workers.example", "app.workers.example"];
  redis.state.sets.set("hosts:demo", new Set(hosts));
  redis.state.sets.set("declared-hosts", new Set(hosts));
  for (const host of hosts) {
    redis.state.sets.set(`host-declarations:${host}`, new Set(["demo"]));
  }

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  assert.deepEqual(
    redis.state.commands.filter((command) => command[0] === "sMembersMany"),
    [["sMembersMany", hosts.map((host) => `host-declarations:${host}`)]]
  );
  assert.equal(redis.state.sets.has("hosts:demo"), false);
  assert.equal(redis.state.sets.has("declared-hosts"), false);
  for (const host of hosts) {
    assert.equal(redis.state.sets.has(`host-declarations:${host}`), false);
  }
});

test("reconcileHosts retries when declarations change during host removal", async () => {
  const state = createFakeRedisState();
  const redis = createFakeRedis(state);
  const host = "app.workers.example";
  state.sets.set("hosts:demo", new Set([host]));
  state.sets.set("declared-hosts", new Set([host]));
  state.sets.set(`host-declarations:${host}`, new Set(["demo", "other"]));

  const originalSession = redis.session.bind(redis);
  let attempts = 0;
  let injectedDrift = false;
  redis.session = async (fn) => {
    attempts += 1;
    return await originalSession(async (iso) => {
      const sMembersMany = iso.sMembersMany.bind(iso);
      return await fn({
        ...iso,
        async sMembersMany(keys) {
          const declarations = await sMembersMany(keys);
          if (!injectedDrift && keys.includes(`host-declarations:${host}`)) {
            injectedDrift = true;
            // Another namespace removes its declaration after this attempt read
            // the old set. The watched declaration must force a fresh decision.
            state.sets.set(`host-declarations:${host}`, new Set(["demo"]));
          }
          return declarations;
        },
      });
    });
  };

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  assert.equal(attempts, 2);
  assert.equal(state.sets.has("declared-hosts"), false);
  assert.equal(state.sets.has(`host-declarations:${host}`), false);
  assert.equal(
    state.watchBatches.filter((keys) => keys.includes(`patterns:${host}`)).length,
    2
  );
  assert.equal(
    state.watchBatches.filter((keys) => keys.includes(`host-declarations:${host}`)).length,
    2
  );
});

test("reconcileHosts bounds host removal reads", async () => {
  const redis = makeRedis();
  const hosts = Array.from({ length: 65 }, (_, index) => `host-${index}.workers.example`);
  redis.state.sets.set("hosts:demo", new Set(hosts));
  redis.state.sets.set("declared-hosts", new Set(hosts));
  for (const host of hosts) {
    redis.state.sets.set(`host-declarations:${host}`, new Set(["demo"]));
  }

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  const membershipReads = redis.state.commands
    .filter((command) => command[0] === "sMIsMember")
    .map((command) => command[2]);
  const declarationReads = redis.state.commands
    .filter((command) => command[0] === "sMembersMany")
    .map((command) => command[1]);
  assert.deepEqual(membershipReads, [hosts.slice(0, 64), hosts.slice(64)]);
  assert.deepEqual(declarationReads, [
    hosts.slice(0, 64).map((host) => `host-declarations:${host}`),
    hosts.slice(64).map((host) => `host-declarations:${host}`),
  ]);
});

test("reconcileHosts rejects a live pattern found in a later removal batch", async () => {
  const redis = makeRedis();
  const hosts = Array.from({ length: 65 }, (_, index) => `host-${index}.workers.example`);
  const liveHost = hosts.at(-1);
  assert.ok(liveHost);
  redis.state.sets.set("hosts:demo", new Set(hosts));
  redis.state.sets.set("declared-hosts", new Set(hosts));
  redis.state.sets.set("ns-hosts:demo", new Set([liveHost]));
  redis.state.hashes.set(`patterns:${liveHost}`, {
    "/*": patternProjection("demo", "worker", "v1", "prefix", "/"),
  });
  for (const host of hosts) {
    redis.state.sets.set(`host-declarations:${host}`, new Set(["demo"]));
  }

  await assert.rejects(
    reconcileHosts(redis, "demo", { hosts: [] }, "workers.local"),
    (err) => {
      const shaped = assertRoutingErrorShape(err, 409, "host_in_use");
      assert.equal(shaped.details.host, liveHost);
      assert.equal(shaped.details.slot, "/*");
      return true;
    }
  );
  assert.deepEqual(redis.state.sets.get("hosts:demo"), new Set(hosts));
  assert.deepEqual(
    redis.state.commands.filter(([command, keys]) =>
      command === "hGetAllMany" && Array.isArray(keys) && keys.length > 0
    ),
    [["hGetAllMany", [`patterns:${liveHost}`]]]
  );
});
