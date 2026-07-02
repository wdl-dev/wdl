import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";

const lifecycleIndexesStub = `
function refMember(ref) { return JSON.stringify(ref); }
function referrersKey(ns, worker, version) { return \`worker-version-referrers:\${ns}:\${worker}:\${version}\`; }
function d1DatabaseReferrersKey(ns, id) { return \`d1:database-referrers:\${ns}:\${id}\`; }
function queueConsumerKey(ns, queue) { return \`queue-consumer:\${ns}:\${queue}\`; }
export function cronWorkerKey(ns, worker) {
  return \`crons:\${ns}:\${worker}\`;
}
export function stageCronSlotRef(multi, ns, worker, entry) {
  const key = \`cron-slot:\${entry.slot}\`;
  multi.sAdd(key, \`\${ns}:\${worker}:\${entry.id}:\${entry.gen}\`);
  multi.expireAt(key, Math.floor(entry.slot / 1000) + 600);
}
export function stageD1ReferrerAdds(multi, { ns, worker, version, refs, databaseIdFor }) {
  for (const ref of refs) multi.sAdd(d1DatabaseReferrersKey(ns, databaseIdFor(ref)), refMember({ callerNs: ns, callerWorker: worker, callerVersion: version, binding: ref.binding }));
}
export function stageOutgoingReferrerAdds(multi, { ns, worker, version, refs }) {
  for (const ref of refs) multi.sAdd(referrersKey(ref.targetNs, ref.targetWorker, ref.targetVersion), refMember({ callerNs: ns, callerWorker: worker, callerVersion: version, binding: ref.binding }));
}
export function stageQueueConsumerProjection(multi, ns, worker, version, consumer) {
  const key = queueConsumerKey(ns, consumer.queue);
  multi.del(key);
  multi.hSet(key, { worker, version, max_batch_size: String(consumer.maxBatchSize), max_batch_timeout_ms: String(consumer.maxBatchTimeoutMs), max_retries: String(consumer.maxRetries), ...(consumer.deadLetterQueue ? { dead_letter_queue: consumer.deadLetterQueue } : {}), ...(consumer.retryDelaySeconds != null ? { retry_delay_secs: String(consumer.retryDelaySeconds) } : {}) });
  multi.sAdd("queue:index:consumers", key);
}
export function stageQueueConsumerRemoval(multi, ns, queue) {
  const key = queueConsumerKey(ns, queue);
  multi.del(key);
  multi.sRem("queue:index:consumers", key);
}
export function stageCronWorkerIndexed(multi, ns, worker) {
  multi.sAdd("cron:index:workers", cronWorkerKey(ns, worker));
}
export function stageCronWorkerRemoved(multi, ns, worker) {
  multi.sRem("cron:index:workers", cronWorkerKey(ns, worker));
}
export function stageWorkerVersionIndexUpsert(multi, ns, worker, version, versionNumber) {
  multi.sAdd(\`workers:\${ns}\`, worker);
  multi.zAdd(\`worker-versions:\${ns}:\${worker}\`, versionNumber, version);
}
`;
const lifecycleIndexesUrl = moduleDataUrl(lifecycleIndexesStub);
const routePlanSrc = applyModuleReplacements(readRepositoryFile("control/routing/route-plan.js"), [
  [
    /import \{ decodePatternProjection \} from "shared-route-projection";/,
    `const __patternSep = "\\t";
     const decodePatternProjection = (raw) => {
       if (typeof raw !== "string") return null;
       const parts = raw.split(__patternSep);
       if (parts.length !== 6 || parts[0] !== "v2") return null;
       const [, ns, worker, version, kind, value] = parts;
       if (!ns || !worker || !version || !value || (kind !== "exact" && kind !== "prefix")) return null;
       return { ns, worker, version, kind, value };
     };`
  ],
]);
const routePlanUrl = moduleDataUrl(routePlanSrc);
const src = applyModuleReplacements(readRepositoryFile("control/routing.js"), [
  [
    /import \{\n {2}DECLARED_HOSTS_KEY,\n {2}HOST_DECLARATIONS_PREFIX,\n {2}runOptimistic,\n\} from "control-shared";/,
    `const DECLARED_HOSTS_KEY = "declared-hosts";
    const HOST_DECLARATIONS_PREFIX = "host-declarations:";
    class WatchError extends Error {}
    async function runOptimistic(redis, { attempts = 5, onExhausted, onWatchError, shouldRetryResult }, fn) {
      for (let attempt = 0; attempt < attempts; attempt += 1) {
        try {
          const result = await redis.session((session) => fn(session, attempt));
          if (shouldRetryResult?.(result, attempt)) continue;
          return result;
        } catch (err) {
          if (err instanceof WatchError) {
            onWatchError?.(err, attempt);
            continue;
          }
          throw err;
        }
      }
      return await onExhausted();
    }`
  ],
  [
    /import \{\n {2}d1DatabaseKey,\n {2}deleteLockKey,\n {2}extractD1Refs, extractOutgoingRefs,\n\} from "control-lib";/,
    `const d1DatabaseKey = (ns, id) => \`d1:database:\${ns}:\${id}\`;
     const deleteLockKey = (ns, worker) => \`worker-delete-lock:\${ns}:\${worker}\`;
     const extractD1Refs = () => [];
     const extractOutgoingRefs = (bindings = {}) => Object.entries(bindings || {})
       .filter(([, spec]) => spec?.type === "service" && spec.targetVersion)
       .map(([binding, spec]) => ({
         binding,
         targetNs: spec.targetNs,
         targetWorker: spec.targetWorker,
         targetVersion: spec.targetVersion,
       }));`
  ],
  [/from "control-lifecycle-indexes";/, `from ${JSON.stringify(lifecycleIndexesUrl)};`],
  [/import \{ parseHostList \} from "control-topology";/, "const parseHostList = (value) => Array.isArray(value) ? value : [];"],
  [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  [
    /import \{ decodePatternProjection, encodePatternProjection \} from "shared-route-projection";/,
    `const __patternSep = "\\t";
     const encodePatternProjection = ({ ns, worker, version, kind, value }) =>
       ["v2", ns, worker, version, kind, value].join(__patternSep);
     const decodePatternProjection = (raw) => {
       if (typeof raw !== "string") return null;
       const parts = raw.split(__patternSep);
       if (parts.length !== 6 || parts[0] !== "v2") return null;
       const [, ns, worker, version, kind, value] = parts;
       if (!ns || !worker || !version || !value || (kind !== "exact" && kind !== "prefix")) return null;
       return { ns, worker, version, kind, value };
     };`
  ],
  [
    /import \{ bundleKey, formatVersion, parseVersion, patternsKey, routesKey \} from "shared-version";/,
    `const bundleKey = (ns, worker, version) => \`bundle:\${ns}:\${worker}:\${version}\`;
     const formatVersion = (num) => \`v\${num}\`;
     const parseVersion = (version) => Number(/^v(\\d+)$/.exec(version)?.[1] || NaN);
     const patternsKey = (host) => \`patterns:\${host}\`;
     const routesKey = (ns) => \`routes:\${ns}\`;`
  ],
  [
    /import \{ diffCrons, nextFireMs, slotMsFor \} from "control-cron-index";/,
    "const diffCrons = () => ({ added: [], removed: [] }); const nextFireMs = () => null; const slotMsFor = () => 0;",
  ],
  [
    /import \{ isReservedNs, ROUTES_ALLOWED_RESERVED_NS \} from "shared-ns-pattern";/,
    `const isReservedNs = (ns) => ns === "__system__" || ns === "__platform__" || ns === "__community__";
     const ROUTES_ALLOWED_RESERVED_NS = new Set(["__system__"]);`
  ],
  [/import \{ PLATFORM_TIER_RESERVED_NS \} from "shared-auth-roles";/, "const PLATFORM_TIER_RESERVED_NS = new Set([\"__platform__\"]);"],
  [/import \{ queueConsumerKey \} from "shared-queue-keys";/, "const queueConsumerKey = (ns, queue) => `queue-consumer:${ns}:${queue}`;"],
  [/from "control-routing-route-plan";/, `from ${JSON.stringify(routePlanUrl)};`],
]);

const { promoteWithRoutes, bumpActiveAndPromote, reconcileHosts } =
  await import(moduleDataUrl(src));

function makeRedis() {
  return createFakeRedis();
}

/**
 * @param {ReturnType<typeof makeRedis>} redis
 * @param {string} version
 * @param {any} meta
 */
function seedBundle(redis, version, meta) {
  redis.state.hashes.set(`bundle:demo:worker:${version}`, {
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
  assert.equal(redis.state.sets.get("queue:index:consumers")?.has("queue-consumer:demo:jobs"), false);
  assert.ok(redis.state.ops.some((op) =>
    op[0] === "sRem" &&
    op[1] === "queue:index:consumers" &&
    op[2] === "queue-consumer:demo:jobs"
  ));
});

test("promoteWithRoutes skips empty platform route versions while checking exported as names", async () => {
  const redis = makeRedis();
  redis.state.hashes.set("bundle:__platform__:api:v1", {
    __meta__: JSON.stringify({ exports: [{ name: "default", as: "demo" }] }),
  });
  redis.state.hashes.set("bundle:__platform__:other:v2", {
    __meta__: JSON.stringify({ exports: [{ name: "default", as: "other-demo" }] }),
  });
  redis.state.hashes.set("routes:__platform__", { stale: "", other: "v2" });

  await promoteWithRoutes(redis, "__platform__", "api", "v1");

  assert.equal(redis.state.hashes.get("routes:__platform__")?.api, "v1");
  assert.deepEqual(
    redis.state.commands.find((op) => op[0] === "hGetMany"),
    ["hGetMany", [["bundle:__platform__:other:v2", "__meta__"]]]
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
        targetNs: "other",
        targetWorker: "api",
        targetVersion: "v3",
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
  assert.ok(redis.state.watched.includes("bundle:other:api:v3"));
  assert.equal(redis.state.hashes.has("bundle:demo:worker:v2"), false);
});

test("promoteWithRoutes batches service-binding dependency watches", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", {
    bindings: {
      TARGET_A: {
        type: "service",
        targetNs: "other",
        targetWorker: "api",
        targetVersion: "v3",
      },
      TARGET_B: {
        type: "service",
        targetNs: "other",
        targetWorker: "queue",
        targetVersion: "v4",
      },
    },
  });
  redis.state.hashes.set("bundle:other:api:v3", { __meta__: "{}" });
  redis.state.hashes.set("bundle:other:queue:v4", { __meta__: "{}" });

  await promoteWithRoutes(redis, "demo", "worker", "v1");

  assert.ok(redis.state.watchBatches.some((batch) =>
    batch.length === 2 &&
    batch.includes("bundle:other:api:v3") &&
    batch.includes("bundle:other:queue:v4")
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

test("bumpActiveAndPromote runs beforeStageCopy inside the watched copy transaction", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { queueConsumers: [] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");
  /** @type {unknown} */
  let seen = null;

  const result = await bumpActiveAndPromote(redis, "demo", "worker", {
    /** @param {{ iso: any, currentVersion: string, newVersion: string, sourceMeta: any }} context */
    async beforeStageCopy(context) {
      const { iso, currentVersion, newVersion, sourceMeta } = context;
      await iso.watch("secrets:demo", "secrets:demo:worker");
      seen = {
        currentVersion,
        newVersion,
        route: await iso.hGet("routes:demo", "worker"),
        queueConsumers: sourceMeta.queueConsumers,
      };
    },
  });

  assert.equal(result.version, "v2");
  assert.deepEqual(seen, {
    currentVersion: "v1",
    newVersion: "v2",
    route: "v1",
    queueConsumers: [],
  });
  assert.ok(redis.state.watchBatches.some((batch) =>
    batch.length === 2 &&
    batch.includes("secrets:demo") &&
    batch.includes("secrets:demo:worker")
  ));
  assert.ok(redis.state.hashes.has("bundle:demo:worker:v2"));
});

test("bumpActiveAndPromote does not copy or flip routes when beforeStageCopy rejects", async () => {
  const redis = makeRedis();
  seedBundle(redis, "v1", { queueConsumers: [] });
  redis.state.hashes.set("routes:demo", { worker: "v1" });
  redis.state.strings.set("worker:demo:worker:next_version", "1");

  await assert.rejects(
    bumpActiveAndPromote(redis, "demo", "worker", {
      async beforeStageCopy() {
        throw new Error("budget failed");
      },
    }),
    /budget failed/
  );

  assert.equal(redis.state.hashes.has("bundle:demo:worker:v2"), false);
  assert.equal(redis.state.hashes.get("routes:demo")?.worker, "v1");
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
        targetNs: "other",
        targetWorker: "api",
        targetVersion: "v3",
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
  assert.ok(redis.state.watched.includes("bundle:other:api:v3"));
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

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example"), false);
  assert.equal(redis.state.sets.get("declared-hosts")?.has("app.workers.example"), true);
  assert.deepEqual(redis.state.sets.get("host-declarations:app.workers.example"), new Set(["other"]));
});

test("reconcileHosts removes global host gate after the final declaration is removed", async () => {
  const redis = makeRedis();
  redis.state.sets.set("hosts:demo", new Set(["app.workers.example"]));
  redis.state.sets.set("declared-hosts", new Set(["app.workers.example"]));
  redis.state.sets.set("host-declarations:app.workers.example", new Set(["demo"]));

  await reconcileHosts(redis, "demo", { hosts: [] }, "workers.local");

  assert.equal(redis.state.sets.get("hosts:demo")?.has("app.workers.example"), false);
  assert.equal(redis.state.sets.get("declared-hosts")?.has("app.workers.example"), false);
  assert.equal(redis.state.sets.has("host-declarations:app.workers.example"), false);
});
