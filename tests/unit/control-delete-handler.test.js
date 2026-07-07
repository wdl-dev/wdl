import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importControlHandler,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

const { lifecycleIndexesUrl } = await compileControlGraph();

const controlSharedExtraSource = `
export const ROUTES_CHANNEL = "routes:invalidate";
export const ROUTES_FLUSH_CHANNEL = "routes:flush";
export const PATTERNS_CHANNEL = "patterns:invalidate";
export async function acquireDeleteLock() { return "lock-token"; }
export async function assertWorkflowDeleteAllowed(args) {
  /** @type {any} */ (globalThis).__deleteHandlerState.workflowChecks.push(args);
  if (/** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker) {
    const blocker = /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker;
    const err = new ControlAbort(blocker.status, blocker.code, blocker.details);
    err.message = blocker.message;
    throw err;
  }
}
export async function cleanupDoAlarmsForWorker(args) {
  /** @type {any} */ (globalThis).__deleteHandlerState.doAlarmCleanups.push(args);
  if (/** @type {any} */ (globalThis).__deleteHandlerDoAlarmCleanupError) {
    throw new Error("do alarm cleanup unavailable");
  }
}
export async function releaseDeleteLock() {
  /** @type {any} */ (globalThis).__deleteHandlerState.releaseCalls += 1;
}
export function buildS3CleanupTaskId() { return "s3cleanup:test"; }
export async function recordS3CleanupIntent(intent) {
  /** @type {any} */ (globalThis).__deleteHandlerState.cleanupIntents.push(intent);
  throw new Error("data redis unavailable");
}
`;

const versionsControlSharedExtraSource = `
export function hasCompleteBundle() { return true; }
export async function acquireDeleteLock() { return "lock-token"; }
export async function assertWorkflowDeleteAllowed(args) {
  /** @type {any} */ (globalThis).__versionDeleteHandlerState.workflowChecks.push(args);
}
export async function releaseDeleteLock() {
  /** @type {any} */ (globalThis).__versionDeleteHandlerState.releaseCalls += 1;
}
export function buildS3CleanupTaskId() { return "s3cleanup:version-test"; }
export async function recordS3CleanupIntent(intent) {
  /** @type {any} */ (globalThis).__versionDeleteHandlerState.cleanupIntents.push(intent);
  throw new Error("data redis unavailable");
}
`;

const controlLibUrl = moduleDataUrl(`
export function workersIndexKey(ns) { return "workers:" + ns; }
export function workerVersionsKey(ns, name) { return "worker-versions:" + ns + ":" + name; }
export function referrersKey(ns, name, version) {
  return "worker-version-referrers:" + ns + ":" + name + ":" + version;
}
export function d1DatabaseReferrersKey(ns, databaseId) {
  return "d1:database-referrers:" + ns + ":" + databaseId;
}
export function deleteLockKey(ns, worker) { return "worker-delete-lock:" + ns + ":" + worker; }
export function encodeReferrerMember(ref) { return JSON.stringify(ref); }
export function doObjectRegistryKey(id) { return "do:objects:" + id; }
export function doOwnerScopeScanPatternForStorage(id) { return "do:owner:" + id + ":*"; }
export function doStorageIdKey(ns, worker) { return "worker:do-storage:" + ns + ":" + worker; }
export function workflowDefsKey(ns, worker) { return "wf:defs:" + ns + ":" + worker; }
export function extractD1Refs() { return []; }
export function extractOutgoingRefs() { return []; }
export function formatReferrerBlocker(members) { return { referrers: members }; }
`);

const sharedVersionUrl = moduleDataUrl(`
export function routesKey(ns) { return "routes:" + ns; }
export function patternsKey(host) { return "patterns:" + host; }
export function formatVersion(n) { return "v" + n; }
export function parseVersion(tag) {
  const match = /^v([1-9][0-9]*)$/.exec(tag);
  return match ? Number(match[1]) : null;
}
export function bundleKey(ns, worker, version) {
  const n = parseVersion(version);
  if (n == null) throw new Error("invalid version tag " + JSON.stringify(version));
  return "worker:" + ns + ":" + worker + ":v:" + n;
}
`);

const sharedSecretKeysUrl = repositoryFileUrl("shared/secret-keys.js");

const sharedRedisUrl = moduleDataUrl(`
const decoder = new TextDecoder();
export function decodeBulk(value) {
  if (value == null) return null;
  return value instanceof Uint8Array ? decoder.decode(value) : String(value);
}
export class WatchError extends Error {}
`);

const sharedQueueKeysUrl = moduleDataUrl(`
export const QUEUE_CONSUMER_INDEX_KEY = "queue:index:consumers";
export function queueConsumerScanPrefix(ns) { return "queue-consumer:" + ns + ":"; }
`);
const sharedRouteProjectionUrl = moduleDataUrl(`
export function decodePatternProjection(raw) {
  if (typeof raw !== "string") return null;
  const parts = raw.split("\\t");
  if (parts.length !== 6 || parts[0] !== "v2") return null;
  const [, ns, worker, version, kind, value] = parts;
  if (!ns || !worker || !version || !value || (kind !== "exact" && kind !== "prefix")) return null;
  return { ns, worker, version, kind, value };
}
`);

const deletePlanSrc = applyModuleReplacements(readRepositoryFile("control/handlers/delete-plan.js"), [
  [/from "control-lib";/, `from ${JSON.stringify(controlLibUrl)};`],
  [/from "control-lifecycle-indexes";/, `from ${JSON.stringify(lifecycleIndexesUrl)};`],
  [/from "shared-version";/, `from ${JSON.stringify(sharedVersionUrl)};`],
  [/from "shared-secret-keys";/, `from ${JSON.stringify(sharedSecretKeysUrl)};`],
]);
const deletePlanUrl = moduleDataUrl(deletePlanSrc);

const { handle } = await importControlHandler("control/handlers/delete.js", {
  globalName: "__deleteHandlerState",
  extraSharedSource: controlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "shared-version": sharedVersionUrl,
    "shared-secret-keys": sharedSecretKeysUrl,
    "shared-redis": sharedRedisUrl,
    "shared-queue-keys": sharedQueueKeysUrl,
    "shared-route-projection": sharedRouteProjectionUrl,
    "control-handlers-delete-plan": deletePlanUrl,
  },
});

const { handle: handleVersions } = await importControlHandler("control/handlers/versions.js", {
  globalName: "__versionDeleteHandlerState",
  extraSharedSource: versionsControlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "shared-version": sharedVersionUrl,
    "shared-secret-keys": sharedSecretKeysUrl,
    "shared-redis": sharedRedisUrl,
  },
});

/**
 * @param {{
 *   activeVersion?: string | null,
 *   assetPrefix?: string | null,
 *   hasWorkerSecrets?: boolean,
 *   doStorageId?: string | null,
 *   doOwnerKeys?: string[],
 *   doOwnerKeysDuringExec?: string[],
 *   queueConsumerKeys?: string[],
 *   queueConsumerWorker?: string | null,
 *   queueConsumerWorkerDuringExec?: string | null,
 *   referrersByVersion?: Record<string, string[]>,
 *   retainedVersions?: string[],
 * }} [opts]
 */
function resetDeleteHandlerState({
  activeVersion = "v1",
  assetPrefix = "assets/demo/api/v1/",
  hasWorkerSecrets = false,
  doStorageId = "do_old",
  doOwnerKeys = [],
  doOwnerKeysDuringExec = doOwnerKeys,
  queueConsumerKeys = ["queue-consumer:demo:jobs"],
  queueConsumerWorker = "api",
  queueConsumerWorkerDuringExec = queueConsumerWorker,
  referrersByVersion = {},
  retainedVersions = ["v1"],
} = {}) {
  const referrers = /** @type {Record<string, string[]>} */ (referrersByVersion);
  const meta = JSON.stringify({
    ...(assetPrefix ? { assets: { prefix: assetPrefix } } : {}),
    bindings: {},
    routes: [],
  });
  /** @type {unknown[][]} */
  const multiCalls = [];
  /** @type {unknown[][]} */
  const commands = [];
  /** @param {string} key */
  function versionReferrers(key) {
    const match = /^worker-version-referrers:demo:api:(v[1-9][0-9]*)$/.exec(key);
    if (match) return referrers[match[1]] || [];
    return [];
  }
  const session = {
    async watch() {},
    async unwatch() {},
    /** @param {string} key */
    async zRange(key) {
      if (key === "worker-versions:demo:api") return retainedVersions;
      return [];
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (key === "routes:demo" && field === "api") return activeVersion;
      if (queueConsumerKeys.includes(key) && field === "worker") {
        return queueConsumerWorkerDuringExec;
      }
      return null;
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      commands.push(["HGETMANY", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) =>
        queueConsumerKeys.includes(key) && field === "worker" ? queueConsumerWorkerDuringExec : null
      );
    },
    /** @param {string} key */
    async exists(key) {
      return key === "secrets:demo:api" && hasWorkerSecrets ? 1 : 0;
    },
    /** @param {string} key */
    async sMembers(key) { return versionReferrers(key); },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      commands.push(["SESSION_HGETALLMANY", keys]);
      return keys.map(() => ({}));
    },
    /** @param {string} _cursor @param {string} pattern */
    async scan(_cursor, pattern) {
      if (pattern === "do:owner:do_old:*") return ["0", doOwnerKeysDuringExec];
      if (pattern === "queue-consumer:demo:*" && queueConsumerWorkerDuringExec) {
        return ["0", queueConsumerKeys];
      }
      return ["0", []];
    },
    /** @param {unknown[]} args */
    async del(...args) { multiCalls.push(["DEL", ...args]); },
    async hGetAll() { return {}; },
    multi() {
      return {
        /** @param {unknown[]} args */
        hDel(...args) { multiCalls.push(["HDEL", ...args]); return this; },
        /** @param {unknown[]} args */
        sRem(...args) { multiCalls.push(["SREM", ...args]); return this; },
        /** @param {unknown[]} args */
        del(...args) { multiCalls.push(["DEL", ...args]); return this; },
        /** @param {unknown[]} args */
        zRem(...args) { multiCalls.push(["ZREM", ...args]); return this; },
        /** @param {unknown[]} args */
        publish(...args) { multiCalls.push(["PUBLISH", ...args]); return this; },
        async exec() { multiCalls.push(["EXEC"]); },
      };
    },
  };
  const redis = {
    /** @param {string} key */
    async get(key) {
      if (key === "worker:do-storage:demo:api") return doStorageId;
      return null;
    },
    /** @param {string} _cursor @param {string} pattern */
    async scan(_cursor, pattern) {
      if (pattern === "do:owner:do_old:*") return ["0", doOwnerKeys];
      if (pattern === "queue-consumer:demo:*" && queueConsumerWorker) {
        return ["0", queueConsumerKeys];
      }
      return ["0", []];
    },
    /** @param {string} key */
    async zRange(key) {
      if (key === "worker-versions:demo:api") return retainedVersions;
      return [];
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (/^worker:demo:api:v:\d+$/.test(key) && field === "__meta__") return meta;
      if (queueConsumerKeys.includes(key) && field === "worker") return queueConsumerWorker;
      return null;
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      commands.push(["CLIENT_HGETMANY", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) =>
        queueConsumerKeys.includes(key) && field === "worker" ? queueConsumerWorker : null
      );
    },
    /** @param {string} key */
    async sMembers(key) { return versionReferrers(key); },
    /** @param {string} key */
    async hGetAll(key) {
      if (key === "routes:demo" && activeVersion) return { api: activeVersion };
      return {};
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      commands.push(["CLIENT_HGETALLMANY", keys]);
      return keys.map((key) => {
        if (key === "routes:demo" && activeVersion) return { api: activeVersion };
        return {};
      });
    },
    /** @param {string} key */
    async exists(key) {
      return key === "secrets:demo:api" && hasWorkerSecrets ? 1 : 0;
    },
    /** @param {unknown[]} args */
    async del(...args) { multiCalls.push(["DEL", ...args]); },
    /** @param {(s: typeof session) => Promise<unknown>} fn */
    async session(fn) { return await fn(session); },
  };
  return installControlHandlerState("__deleteHandlerState", {
    commands,
    cleanupIntents: [],
    doAlarmCleanups: [],
    logs: [],
    multiCalls,
    releaseCalls: 0,
    redis,
    metrics: { increment() {}, observe() {} },
    service: "control",
    workflowChecks: [],
  });
}

/** @param {{ assetPrefix?: string | null }} [opts] */
function resetVersionDeleteHandlerState({ assetPrefix = "assets/demo/api/v1/" } = {}) {
  const meta = JSON.stringify({
    ...(assetPrefix ? { assets: { prefix: assetPrefix } } : {}),
    bindings: {},
  });
  /** @type {unknown[][]} */
  const multiCalls = [];
  const session = {
    async watch() {},
    async unwatch() {},
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (key === "routes:demo" && field === "api") return null;
      if (key === "worker:demo:api:v:1" && field === "__meta__") return meta;
      return null;
    },
    /** @param {string} key */
    async zRange(key) {
      if (key === "worker-versions:demo:api") return ["v1"];
      return [];
    },
    async sMembers() { return []; },
    async exists() { return 0; },
    multi() {
      return {
        /** @param {unknown[]} args */
        del(...args) { multiCalls.push(["DEL", ...args]); return this; },
        /** @param {unknown[]} args */
        zRem(...args) { multiCalls.push(["ZREM", ...args]); return this; },
        /** @param {unknown[]} args */
        sRem(...args) { multiCalls.push(["SREM", ...args]); return this; },
        async exec() { multiCalls.push(["EXEC"]); },
      };
    },
  };
  const redis = {
    /** @param {(s: typeof session) => Promise<unknown>} fn */
    async session(fn) { return await fn(session); },
  };
  return installControlHandlerState("__versionDeleteHandlerState", {
    cleanupIntents: [],
    logs: [],
    multiCalls,
    releaseCalls: 0,
    redis,
    metrics: { increment() {}, observe() {} },
    service: "control",
    workflowChecks: [],
  });
}

function resetVersionListHandlerState() {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (key === "routes:demo" && field === "api") return "v2";
      return null;
    },
    /**
     * @param {string} key
     * @param {number} start
     * @param {number} stop
     */
    async zRange(key, start, stop) {
      assert.equal(key, "worker-versions:demo:api");
      assert.equal(start, 0);
      assert.equal(stop, -1);
      return ["v1", "v2"];
    },
    async get() {
      throw new Error("versions GET must not read next_version");
    },
    async hExistsMany() {
      throw new Error("versions GET must not scan historical bundle keys");
    },
  };
  return installControlHandlerState("__versionDeleteHandlerState", {
    cleanupIntents: [],
    logs: [],
    multiCalls: [],
    releaseCalls: 0,
    redis,
    metrics: { increment() {}, observe() {} },
    service: "control",
    workflowChecks: [],
  });
}

test("versions GET reads retained versions from the worker version index", async () => {
  resetVersionListHandlerState();

  const response = await handleVersions({
    method: "GET",
    ns: "demo",
    name: "api",
    subPath: [],
    principal: {},
    requestId: "rid-versions-list",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    name: "api",
    versions: [
      { version: "v1", active: false },
      { version: "v2", active: true },
    ],
  });
});

test("worker delete reports cleanup_queue_failed when data-plane cleanup enqueue fails", async () => {
  const testState = resetDeleteHandlerState();

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(testState.workflowChecks, [{ ns: "demo", worker: "api", allowCleanup: true }]);
  assert.deepEqual(testState.doAlarmCleanups, [{ ns: "demo", worker: "api", doStorageId: "do_old" }]);
  assert.equal(body.assets.queueHint, "failed");
  assert.deepEqual(body.assets.warnings, [{
    code: "cleanup_queue_failed",
    message: "Worker content cleanup was not queued; retry deletion cleanup manually.",
  }]);
  assert.deepEqual(testState.cleanupIntents, [{
    taskId: "s3cleanup:test",
    prefixes: ["assets/demo/api/v1/"],
    source: {
      kind: "delete-worker",
      ns: "demo",
      worker: "api",
      versions: ["v1"],
      requestId: "rid-delete",
    },
  }]);
  assert.equal(testState.releaseCalls, 1);
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "SREM" &&
    call[1] === "queue:index:consumers" &&
    call[2] === "queue-consumer:demo:jobs"
  ));
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "DEL" &&
    call.includes("wf:defs:demo:api")
  ));
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "HDEL" &&
    call[1] === "routes:demo" &&
    call[2] === "api"
  ));
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "SREM" &&
    call[1] === "workers:demo" &&
    call[2] === "api"
  ));
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "ZREM" &&
    call[1] === "worker-versions:demo:api" &&
    call[2] === "v1"
  ));
  assert.ok(testState.logs.some((/** @type {any} */ entry) =>
    entry.level === "warn" &&
    entry.event === "worker_cleanup_queue_failed" &&
    entry.fields.error_message === "data redis unavailable"
  ));
});

test("worker delete collects retained version metadata without serial round trips", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    doStorageId: null,
    queueConsumerWorker: null,
    retainedVersions: ["v1", "v2", "v3"],
  });
  const originalHGet = testState.redis.hGet.bind(testState.redis);
  let activeBundleMetaReads = 0;
  let maxActiveBundleMetaReads = 0;
  testState.redis.hGet = async (/** @type {string} */ key, /** @type {string} */ field) => {
    if (/^worker:demo:api:v:\d+$/.test(key) && field === "__meta__") {
      activeBundleMetaReads += 1;
      maxActiveBundleMetaReads = Math.max(maxActiveBundleMetaReads, activeBundleMetaReads);
      await Promise.resolve();
      activeBundleMetaReads -= 1;
    }
    return await originalHGet(key, field);
  };

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-concurrent-collect",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(body.versionsDeleted, ["v1", "v2", "v3"]);
  assert.ok(
    maxActiveBundleMetaReads > 1,
    `expected concurrent retained-version meta reads, saw ${maxActiveBundleMetaReads}`
  );
});

test("worker delete retries instead of committing when DO owner keys drift after collection", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    doOwnerKeys: [],
    doOwnerKeysDuringExec: ["do:owner:do_old:Room:shard0"],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-owner-drift",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "whole_delete_contention");
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.deepEqual(testState.doAlarmCleanups, []);
});

test("worker delete retries instead of committing when queue consumer keys drift after collection", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    queueConsumerKeys: ["queue-consumer:demo:jobs", "queue-consumer:demo:email"],
    queueConsumerWorker: null,
    queueConsumerWorkerDuringExec: "api",
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-queue-consumer-drift",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "whole_delete_contention");
  assert.deepEqual(
    testState.commands.find((/** @type {unknown[]} */ call) => call[0] === "HGETMANY"),
    ["HGETMANY", [
      ["queue-consumer:demo:jobs", "worker"],
      ["queue-consumer:demo:email", "worker"],
    ]]
  );
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.deepEqual(testState.doAlarmCleanups, []);
});

test("worker delete dry-run includes workflow lifecycle blockers", async () => {
  const testState = resetDeleteHandlerState();
  /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker = {
    status: 409,
    code: "workflow_instances_active",
    message: "demo/api has active workflow instances",
    details: {
      count: 1,
      blockers: [{ workflowKey: "wf_old", instanceId: "inst-1" }],
    },
  };

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-dry-run-workflows",
  });

  delete /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker;
  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, false);
  assert.deepEqual(testState.workflowChecks, [{ ns: "demo", worker: "api" }]);
  assert.deepEqual(body.workflowBlocker, {
    error: "workflow_instances_active",
    message: "demo/api has active workflow instances",
    count: 1,
    blockers: [{ workflowKey: "wf_old", instanceId: "inst-1" }],
  });
  assert.equal(testState.releaseCalls, 0);
});

test("worker delete reports workflow and version referrer blockers together", async () => {
  const referrer = JSON.stringify({
    callerNs: "demo",
    callerWorker: "web",
    callerVersion: "v1",
    binding: "API",
  });
  const testState = resetDeleteHandlerState({
    referrersByVersion: { v1: [referrer] },
  });
  /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker = {
    status: 409,
    code: "workflow_instances_active",
    message: "demo/api has active workflow instances",
    details: {
      count: 1,
      blockers: [{ workflowKey: "wf_old", instanceId: "inst-1" }],
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
      url: new URL("http://control/ns/demo/worker/api/delete"),
      ns: "demo",
      name: "api",
      principal: { kind: "ops" },
      requestId: "rid-delete-both-blockers",
    });

    const body = await readJsonResponse(response, 409);
    assert.equal(body.error, "workflow_instances_active");
    assert.equal(body.count, 1);
    assert.deepEqual(body.blockers, [{ workflowKey: "wf_old", instanceId: "inst-1" }]);
    assert.deepEqual(body.versionBlockers, [{
      version: "v1",
      referrers: [referrer],
    }]);
    assert.deepEqual(testState.workflowChecks, [{
      ns: "demo",
      worker: "api",
      allowCleanup: true,
    }]);
    assert.deepEqual(testState.doAlarmCleanups, []);
    assert.equal(testState.releaseCalls, 1);
    assert.deepEqual(testState.cleanupIntents, []);
  } finally {
    delete /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker;
  }
});

test("worker delete keeps DO alarm jobs when version referrers block deletion", async () => {
  const referrer = JSON.stringify({
    callerNs: "demo",
    callerWorker: "web",
    callerVersion: "v1",
    binding: "API",
  });
  const testState = resetDeleteHandlerState({
    referrersByVersion: { v1: [referrer] },
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-version-blocker",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "version_referenced");
  assert.deepEqual(body.blockers, [{
    version: "v1",
    referrers: [referrer],
  }]);
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo",
    worker: "api",
    allowCleanup: true,
  }]);
  assert.deepEqual(testState.doAlarmCleanups, []);
  assert.equal(testState.multiCalls.length, 0);
});

test("worker delete logs post-commit DO alarm cleanup failure without rolling back deletion", async () => {
  const testState = resetDeleteHandlerState({ assetPrefix: null });
  /** @type {any} */ (globalThis).__deleteHandlerDoAlarmCleanupError = true;

  try {
    const response = await handle({
      request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
      url: new URL("http://control/ns/demo/worker/api/delete"),
      ns: "demo",
      name: "api",
      principal: { kind: "ops" },
      requestId: "rid-delete-alarm-cleanup-failed",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.deleted, true);
    assert.deepEqual(testState.doAlarmCleanups, [{ ns: "demo", worker: "api", doStorageId: "do_old" }]);
    assert.ok(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"));
    assert.ok(testState.logs.some((/** @type {any} */ entry) =>
      entry.level === "warn" &&
      entry.event === "worker_do_alarm_cleanup_failed" &&
      entry.fields.error_message === "do alarm cleanup unavailable"
    ));
  } finally {
    delete /** @type {any} */ (globalThis).__deleteHandlerDoAlarmCleanupError;
  }
});

test("worker delete keeps workflow blocker even when worker lifecycle is already absent", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    queueConsumerWorker: null,
    retainedVersions: [],
  });
  /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker = {
    status: 409,
    code: "workflow_instances_active",
    message: "demo/api has active workflow instances",
    details: {
      count: 1,
      blockers: [{ workflowKey: "wf_old", instanceId: "inst-1" }],
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
      url: new URL("http://control/ns/demo/worker/api/delete"),
      ns: "demo",
      name: "api",
      principal: { kind: "ops" },
      requestId: "rid-delete-workflow-no-lifecycle",
    });

    const body = await readJsonResponse(response, 409);
    assert.equal(body.error, "workflow_instances_active");
    assert.equal(body.count, 1);
    assert.deepEqual(body.blockers, [{ workflowKey: "wf_old", instanceId: "inst-1" }]);
    assert.deepEqual(testState.workflowChecks, [{
      ns: "demo",
      worker: "api",
      allowCleanup: true,
    }]);
    assert.equal(testState.releaseCalls, 1);
    assert.deepEqual(testState.cleanupIntents, []);
    assert.equal(testState.multiCalls.length, 0);
  } finally {
    delete /** @type {any} */ (globalThis).__deleteHandlerWorkflowBlocker;
  }
});

test("worker delete retry compensates DO alarm cleanup when stale storage pointer remains", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    queueConsumerWorker: null,
    retainedVersions: [],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-noop-alarm-cleanup",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, false);
  assert.deepEqual(testState.doAlarmCleanups, [{ ns: "demo", worker: "api", doStorageId: "do_old" }]);
  assert.deepEqual(testState.multiCalls, [["DEL", "worker:do-storage:demo:api"]]);
});

test("worker delete noop skips DO alarm cleanup when old storage id is absent", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    doStorageId: null,
    queueConsumerWorker: null,
    retainedVersions: [],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-noop-no-storage-id",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, false);
  assert.deepEqual(testState.doAlarmCleanups, []);
  assert.equal(testState.multiCalls.length, 0);
});

test("worker delete reports queueHint none when no content cleanup is needed", async () => {
  const testState = resetDeleteHandlerState({ assetPrefix: null });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-no-cleanup",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(testState.workflowChecks, [{ ns: "demo", worker: "api", allowCleanup: true }]);
  assert.deepEqual(testState.doAlarmCleanups, [{ ns: "demo", worker: "api", doStorageId: "do_old" }]);
  assert.equal(body.assets.queueHint, "none");
  assert.deepEqual(body.assets.warnings, []);
  assert.deepEqual(testState.cleanupIntents, []);
  assert.equal(testState.releaseCalls, 1);
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "SREM" &&
    call[1] === "queue:index:consumers" &&
    call[2] === "queue-consumer:demo:jobs"
  ));
});

test("version delete reports cleanup_queue_failed when data-plane cleanup enqueue fails", async () => {
  const testState = resetVersionDeleteHandlerState();

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo",
    worker: "api",
    version: "v1",
    allowCleanup: true,
  }]);
  assert.equal(body.assets.queueHint, "failed");
  assert.deepEqual(body.assets.warnings, [{
    code: "cleanup_queue_failed",
    message: "Version content cleanup was not queued; retry deletion cleanup manually.",
  }]);
  assert.deepEqual(testState.cleanupIntents, [{
    taskId: "s3cleanup:version-test",
    prefixes: ["assets/demo/api/v1/"],
    source: {
      kind: "delete-version",
      ns: "demo",
      name: "api",
      version: "v1",
      requestId: "rid-version-delete",
    },
  }]);
  assert.equal(testState.releaseCalls, 1);
  assert.ok(testState.logs.some((/** @type {any} */ entry) =>
    entry.level === "warn" &&
    entry.event === "version_cleanup_queue_failed" &&
    entry.fields.error_message === "data redis unavailable"
  ));
});

test("version delete reports queueHint none when no content cleanup is needed", async () => {
  const testState = resetVersionDeleteHandlerState({ assetPrefix: null });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-no-cleanup",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo",
    worker: "api",
    version: "v1",
    allowCleanup: true,
  }]);
  assert.equal(body.assets.queueHint, "none");
  assert.deepEqual(body.assets.warnings, []);
  assert.deepEqual(testState.cleanupIntents, []);
  assert.equal(testState.releaseCalls, 1);
});
