import { test } from "node:test";
import assert from "node:assert/strict";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";
import { loadControlRouting } from "../helpers/load-control-routing.js";
import { loadControlLib } from "../helpers/load-control-lib.js";
import { bundleKey as productionBundleKey } from "../../shared/version.js";

const { promoteWithRoutes, bumpActiveAndPromote, reconcileHosts } =
  await loadControlRouting();
const { controlLib } = await loadControlLib();
const { encodeReferrerMember } = controlLib;

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

test("reconcileHosts stages declared host indexes and pattern invalidation", async () => {
  const redis = makeRedis();

  await reconcileHosts(redis, "demo", { hosts: ["app.workers.example"] }, "workers.local");

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example"), true);
  assert.equal(redis.state.sets.get("declared-hosts")?.has("app.workers.example"), true);
  assert.equal(redis.state.sets.get("host-declarations:app.workers.example")?.has("demo"), true);
  assert.ok(redis.state.ops.some((op) =>
    op[0] === "publish" &&
    op[1] === "patterns:invalidate" &&
    op[2] === "app.workers.example"
  ));
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
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.sets.set("declared-hosts", new Set(["app.workers.example"]));
  redis.state.sets.set("host-declarations:app.workers.example", new Set(["demo"]));

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example") ?? false, false);
  assert.equal(redis.state.sets.has("declared-hosts"), false);
  assert.equal(redis.state.sets.has("host-declarations:app.workers.example"), false);
});
