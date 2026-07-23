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
import { sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

const { libUrl: productionControlLibUrl, lifecycleIndexesUrl } = await compileControlGraph();
const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");

const controlSharedExtraSource = `
export {
  PATTERNS_CHANNEL,
  ROUTES_CHANNEL,
  ROUTES_FLUSH_CHANNEL,
} from ${JSON.stringify(workerContractUrl)};
export async function acquireDeleteLock(_redis, _ns, _worker, kind) {
  /** @type {any} */ (globalThis).__deleteHandlerState.lockKinds.push(kind);
  return "lock-token";
}
export async function renewDeleteLock() { return true; }
export function deleteLockExpiredDetails(ns, worker, version) {
  return {
    namespace: ns,
    name: worker,
    ...(version === undefined ? {} : { version }),
    message: "worker delete lock expired; retry the request",
  };
}
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
export async function acquireDeleteLock(_redis, _ns, _worker, kind) {
  /** @type {any} */ (globalThis).__versionDeleteHandlerState.lockKinds.push(kind);
  return "lock-token";
}
export async function renewDeleteLock() { return true; }
export function deleteLockExpiredDetails(ns, worker, version) {
  return {
    namespace: ns,
    name: worker,
    ...(version === undefined ? {} : { version }),
    message: "worker delete lock expired; retry the request",
  };
}
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
export {
  bundleAssetPrefix,
  d1DatabaseReferrersKey,
  doObjectRegistryKey,
  encodeReferrerMember,
  parseBundleMeta,
  parsePatternProjection,
  referrersKey,
  workersIndexKey,
  workflowDefsKey,
} from ${JSON.stringify(productionControlLibUrl)};
export function extractD1Refs() { return []; }
export function extractOutgoingRefs() { return []; }
export function formatReferrerBlocker(members) { return { referrers: members }; }
`);

const sharedSecretKeysUrl = repositoryFileUrl("shared/secret-keys.js");

const sharedRedisUrl = sharedRedisStubUrl();
const { WatchError: TestWatchError } = await import(sharedRedisUrl);

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
  [/from "shared-worker-contract";/, `from ${JSON.stringify(workerContractUrl)};`],
  [/from "shared-secret-keys";/, `from ${JSON.stringify(sharedSecretKeysUrl)};`],
]);
const deletePlanUrl = moduleDataUrl(deletePlanSrc);

const { handle } = await importControlHandler("control/handlers/delete.js", {
  globalName: "__deleteHandlerState",
  extraSharedSource: controlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "shared-worker-contract": workerContractUrl,
    "shared-secret-keys": sharedSecretKeysUrl,
    "shared-redis": sharedRedisUrl,
    "shared-queue-keys": sharedQueueKeysUrl,
    "shared-route-projection": sharedRouteProjectionUrl,
    "shared-respond": repositoryFileUrl("shared/respond.js"),
    "control-handlers-delete-plan": deletePlanUrl,
  },
});

const { handle: handleVersions } = await importControlHandler("control/handlers/versions.js", {
  globalName: "__versionDeleteHandlerState",
  extraSharedSource: versionsControlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "shared-worker-contract": workerContractUrl,
    "shared-secret-keys": sharedSecretKeysUrl,
    "shared-redis": sharedRedisUrl,
  },
});

/**
 * @param {{
 *   activeVersion?: string | null,
 *   assetPrefix?: string | null,
 *   hasWorkerSecrets?: boolean,
 *   hasWorkflowDefs?: boolean,
 *   doStorageId?: string | null,
 *   doStorageIdDuringExec?: string | null,
 *   doOwnerKeys?: string[],
 *   doOwnerKeysDuringExec?: string[],
 *   doObjectCount?: number,
 *   doObjectCountDuringExec?: number,
 *   queueConsumerKeys?: string[],
 *   queueConsumerKeysDuringExec?: string[],
 *   queueConsumerWorker?: string | null,
 *   queueConsumerWorkerDuringExec?: string | null,
 *   referrersByVersion?: Record<string, string[]>,
 *   retainedVersions?: string[],
 *   bundleMetaRaw?: string | null,
 *   lockTokenDuringExec?: string | null,
 *   replaceLockAfterRead?: boolean,
 *   siblingVersion?: string | null,
 *   siblingVersionBeforeFirstWatch?: string | null,
 *   patternRecords?: Record<string, Record<string, string>>,
 *   patternRecordsDuringExec?: Record<string, Record<string, string>>,
 * }} [opts]
 */
function resetDeleteHandlerState({
  activeVersion = "v1",
  assetPrefix = "assets/demo/api/v1/",
  hasWorkerSecrets = false,
  hasWorkflowDefs = false,
  doStorageId = "do_old",
  doStorageIdDuringExec = doStorageId,
  doOwnerKeys = [],
  doOwnerKeysDuringExec = doOwnerKeys,
  doObjectCount = 0,
  doObjectCountDuringExec = doObjectCount,
  queueConsumerKeys = ["queue-consumer:demo:jobs"],
  queueConsumerKeysDuringExec = queueConsumerKeys,
  queueConsumerWorker = "api",
  queueConsumerWorkerDuringExec = queueConsumerWorker,
  referrersByVersion = {},
  retainedVersions = ["v1"],
  bundleMetaRaw,
  lockTokenDuringExec = "lock-token",
  replaceLockAfterRead = false,
  siblingVersion = null,
  siblingVersionBeforeFirstWatch = null,
  patternRecords = {},
  patternRecordsDuringExec = patternRecords,
} = {}) {
  const referrers = /** @type {Record<string, string[]>} */ (referrersByVersion);
  const meta = bundleMetaRaw === undefined
    ? JSON.stringify({
      ...(assetPrefix ? { assets: { prefix: assetPrefix } } : {}),
      bindings: {},
      routes: [],
    })
    : bundleMetaRaw;
  /** @type {unknown[][]} */
  const multiCalls = [];
  /** @type {unknown[][]} */
  const commands = [];
  /** @type {string[]} */
  let watchedKeys = [];
  /** @type {string[][]} */
  const watchBatches = [];
  let watchedLockToken = lockTokenDuringExec;
  let currentLockToken = lockTokenDuringExec;
  let lockReplaced = false;
  /** @type {Record<string, string>} */
  const routeVersions = {
    ...(activeVersion ? { api: activeVersion } : {}),
    ...(siblingVersion ? { workerB: siblingVersion } : {}),
  };
  let siblingChanged = false;
  /** @param {string} key */
  function versionReferrers(key) {
    const match = /^worker-version-referrers:demo:api:(v[1-9][0-9]*)$/.exec(key);
    if (match) return referrers[match[1]] || [];
    return [];
  }
  const session = {
    /** @param {string[]} keys */
    async watch(...keys) {
      if (
        siblingVersionBeforeFirstWatch &&
        !siblingChanged &&
        keys.includes("routes:demo")
      ) {
        routeVersions.workerB = siblingVersionBeforeFirstWatch;
        siblingChanged = true;
      }
      watchedKeys = keys;
      watchBatches.push(keys);
      watchedLockToken = currentLockToken;
    },
    async unwatch() { watchedKeys = []; },
    /** @param {string} key */
    async get(key) {
      if (key === "worker-delete-lock:demo:api") {
        const token = currentLockToken;
        if (replaceLockAfterRead && !lockReplaced) {
          currentLockToken = "replacement-token";
          lockReplaced = true;
        }
        return token;
      }
      if (key === "worker:do-storage:demo:api") return doStorageIdDuringExec;
      return null;
    },
    /** @param {string} key */
    async zRange(key) {
      if (key === "worker-versions:demo:api") return retainedVersions;
      return [];
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (key === "routes:demo") return routeVersions[field] || null;
      if (key.startsWith("patterns:")) return patternRecordsDuringExec[key]?.[field] || null;
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
      if (key === "secrets:demo:api") return hasWorkerSecrets ? 1 : 0;
      if (key === "wf:defs:demo:api") return hasWorkflowDefs ? 1 : 0;
      return 0;
    },
    /** @param {string[]} keys */
    async existsMany(keys) {
      commands.push(["SESSION_EXISTSMANY", keys]);
      return keys.map((key) => {
        if (key === "secrets:demo:api") return hasWorkerSecrets;
        if (key === "wf:defs:demo:api") return hasWorkflowDefs;
        return false;
      });
    },
    /** @param {string} key */
    async sMembers(key) { return versionReferrers(key); },
    /** @param {string} key */
    async sCard(key) {
      commands.push(["SESSION_SCARD", key]);
      return key === "do:objects:do_old" ? doObjectCountDuringExec : 0;
    },
    /** @param {string[]} keys */
    async sMembersMany(keys) {
      commands.push(["SESSION_SMEMBERSMANY", keys]);
      return keys.map((key) => versionReferrers(key));
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      commands.push(["SESSION_HGETALLMANY", keys]);
      return keys.map((key) => {
        if (queueConsumerKeys.includes(key)) {
          return queueConsumerWorkerDuringExec ? { worker: queueConsumerWorkerDuringExec } : {};
        }
        return patternRecordsDuringExec[key] || {};
      });
    },
    /** @param {string} _cursor @param {string} pattern */
    async scan(_cursor, pattern) {
      commands.push(["SESSION_SCAN", pattern]);
      if (pattern === "do:owner:scope:do_old%3A*") return ["0", doOwnerKeysDuringExec];
      if (pattern === "queue-consumer:demo:*" && queueConsumerWorkerDuringExec) {
        return ["0", queueConsumerKeysDuringExec];
      }
      return ["0", []];
    },
    /** @param {unknown[]} args */
    async del(...args) { multiCalls.push(["DEL", ...args]); },
    /** @param {string} key */
    async hGetAll(key) {
      if (key === "routes:demo") return { ...routeVersions };
      return {};
    },
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
        async exec() {
          if (
            watchedKeys.includes("worker-delete-lock:demo:api") &&
            currentLockToken !== watchedLockToken
          ) {
            throw new TestWatchError();
          }
          multiCalls.push(["EXEC"]);
        },
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
      if (pattern === "do:owner:scope:do_old%3A*") return ["0", doOwnerKeys];
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
      if (key === "routes:demo") return routeVersions[field] || null;
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
    async sCard(key) {
      commands.push(["CLIENT_SCARD", key]);
      return key === "do:objects:do_old" ? doObjectCount : 0;
    },
    /** @param {string} key */
    async hGetAll(key) {
      if (key === "routes:demo") return { ...routeVersions };
      return {};
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      commands.push(["CLIENT_HGETALLMANY", keys]);
      return keys.map((key) => {
        if (key === "routes:demo" && activeVersion) return { api: activeVersion };
        return patternRecords[key] || {};
      });
    },
    /** @param {string} key */
    async exists(key) {
      if (key === "secrets:demo:api") return hasWorkerSecrets ? 1 : 0;
      if (key === "wf:defs:demo:api") return hasWorkflowDefs ? 1 : 0;
      return 0;
    },
    /** @param {string[]} keys */
    async existsMany(keys) {
      commands.push(["CLIENT_EXISTSMANY", keys]);
      return keys.map((key) => {
        if (key === "secrets:demo:api") return hasWorkerSecrets;
        if (key === "wf:defs:demo:api") return hasWorkflowDefs;
        return false;
      });
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
    lockKinds: [],
    multiCalls,
    releaseCalls: 0,
    redis,
    metrics: { increment() {}, observe() {} },
    service: "control",
    workflowChecks: [],
    watchBatches,
    get watchedKeys() { return watchedKeys; },
  });
}

/** @param {{ assetPrefix?: string | null, bundleMetaRaw?: string | null, retainedVersions?: string[], siblingMetaRaw?: string | null, siblingMetaByVersion?: Record<string, string | null>, lockTokenDuringExec?: string | null, hasWorkflowDefs?: boolean }} [opts] */
function resetVersionDeleteHandlerState({
  assetPrefix = "assets/demo/api/v1/",
  bundleMetaRaw,
  retainedVersions = ["v1"],
  siblingMetaRaw = null,
  siblingMetaByVersion = {},
  lockTokenDuringExec = "lock-token",
  hasWorkflowDefs = false,
} = {}) {
  const meta = bundleMetaRaw === undefined
    ? JSON.stringify({
      ...(assetPrefix ? { assets: { prefix: assetPrefix } } : {}),
      bindings: {},
    })
    : bundleMetaRaw;
  /** @type {unknown[][]} */
  const multiCalls = [];
  /** @type {unknown[][]} */
  const readCommands = [];
  /** @type {string[][]} */
  const watchBatches = [];
  /** @param {string} key @param {string} field */
  const readHashField = (key, field) => {
    if (key === "routes:demo" && field === "api") return null;
    if (key === "worker:demo:api:v:1" && field === "__meta__") return meta;
    if (field === "__meta__") {
      const versionNumber = key.match(/^worker:demo:api:v:(\d+)$/)?.[1];
      const version = versionNumber ? `v${versionNumber}` : null;
      if (version && Object.hasOwn(siblingMetaByVersion, version)) {
        return siblingMetaByVersion[version];
      }
      if (version === "v2") return siblingMetaRaw;
    }
    return null;
  };
  /** @param {string} key */
  const keyExists = (key) => key === "wf:defs:demo:api" && hasWorkflowDefs ? 1 : 0;
  const session = {
    /** @param {string[]} keys */
    async watch(...keys) { watchBatches.push(keys); },
    async unwatch() {},
    /** @param {string} key */
    async get(key) {
      if (key === "worker-delete-lock:demo:api") return lockTokenDuringExec;
      return null;
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      readCommands.push(["HGET", key, field]);
      return readHashField(key, field);
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      readCommands.push(["HGETMANY", pairs]);
      return pairs.map(([key, field]) => readHashField(key, field));
    },
    /** @param {string} key */
    async zRange(key) {
      if (key === "worker-versions:demo:api") return retainedVersions;
      return [];
    },
    async sMembers() { return []; },
    /** @param {string} key */
    async exists(key) {
      readCommands.push(["EXISTS", key]);
      return keyExists(key);
    },
    /** @param {string[]} keys */
    async existsMany(keys) {
      readCommands.push(["EXISTSMANY", keys]);
      return keys.map((key) => keyExists(key) > 0);
    },
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
    lockKinds: [],
    multiCalls,
    readCommands,
    releaseCalls: 0,
    redis,
    metrics: { increment() {}, observe() {} },
    service: "control",
    watchBatches,
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
  assert.deepEqual(
    testState.commands.find((/** @type {unknown[]} */ call) => call[0] === "CLIENT_EXISTSMANY"),
    ["CLIENT_EXISTSMANY", ["secrets:demo:api", "wf:defs:demo:api"]]
  );
  assert.deepEqual(testState.lockKinds, ["whole"]);
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo", worker: "api", allowCleanup: true, requestId: "rid-delete",
  }]);
  assert.deepEqual(testState.doAlarmCleanups, [{
    ns: "demo", worker: "api", doStorageId: "do_old", requestId: "rid-delete",
  }]);
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

test("worker delete paths cancel ignored request bodies without awaiting cancellation", { timeout: 1000 }, async () => {
  for (const suffix of ["", "?dry_run=1"]) {
    resetDeleteHandlerState();
    let cancelCalls = 0;
    const stream = new ReadableStream({
      cancel() {
        cancelCalls += 1;
        return new Promise(() => {});
      },
    });
    const request = new Request(`http://control/ns/demo/worker/api/delete${suffix}`, /** @type {RequestInit} */ ({
      method: "POST",
      body: stream,
      duplex: "half",
    }));

    const response = await handle({
      request,
      url: new URL(request.url),
      ns: "demo",
      name: "api",
      principal: { kind: "ops" },
      requestId: "rid-delete-body",
    });

    assert.equal(response.status, 200, suffix || "execute");
    assert.equal(cancelCalls, 1, suffix || "execute");
  }
});

test("worker delete classifies non-object retained bundle metadata as corrupt", async () => {
  const testState = resetDeleteHandlerState({ bundleMetaRaw: "[]" });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-corrupt-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);
  const rejection = /** @type {any} */ (testState.logs.find((/** @type {any} */ entry) =>
    entry.event === "worker_delete_rejected"
  ));
  assert.ok(rejection);
  assert.equal(rejection.level, "error");
  assert.equal(rejection.fields.metadata_version, "v1");
  assert.equal(rejection.fields.stage, "retained_meta_parse");
  assert.equal(rejection.fields.error_detail, "__meta__ must be a JSON object");
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.equal(testState.releaseCalls, 1);
});

test("worker delete fails closed when indexed active bundle metadata is missing", async () => {
  const testState = resetDeleteHandlerState({
    retainedVersions: [],
    bundleMetaRaw: null,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-missing-active-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.stage, undefined);
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.equal(testState.releaseCalls, 1);
});

test("worker delete dry-run logs redacted metadata diagnostics", async () => {
  const testState = resetDeleteHandlerState({ bundleMetaRaw: "[]" });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-dry-run-corrupt-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);
  const rejection = /** @type {any} */ (testState.logs.find((/** @type {any} */ entry) =>
    entry.event === "worker_dry_run_rejected"
  ));
  assert.ok(rejection);
  assert.equal(rejection.level, "error");
  assert.equal(rejection.fields.metadata_version, "v1");
  assert.equal(rejection.fields.stage, "retained_meta_parse");
  assert.equal(rejection.fields.error_detail, "__meta__ must be a JSON object");
});

for (const { label, bundleMetaRaw } of [
  { label: "missing", bundleMetaRaw: null },
  { label: "empty", bundleMetaRaw: "" },
]) {
  test(`worker delete fails closed when retained bundle metadata is ${label}`, async () => {
    const testState = resetDeleteHandlerState({
      activeVersion: null,
      bundleMetaRaw,
    });

    const response = await handle({
      request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
      url: new URL("http://control/ns/demo/worker/api/delete"),
      ns: "demo",
      name: "api",
      principal: { kind: "ops" },
      requestId: `rid-delete-${label}-retained-meta`,
    });

    const body = await readJsonResponse(response, 500);
    assert.equal(body.error, "corrupt_meta");
    assert.equal(body.stage, undefined);
    assert.equal(body.detail, undefined);
    assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
    assert.equal(testState.releaseCalls, 1);
  });
}

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
  assert.deepEqual(testState.lockKinds, ["whole"]);
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
    doOwnerKeysDuringExec: ["do:owner:scope:do_old%3ARoom%3Ashard0"],
    doObjectCount: 0,
    doObjectCountDuringExec: 0,
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
  assert.ok(testState.commands.some((/** @type {unknown[]} */ call) =>
    call[0] === "SESSION_SCAN" && call[1] === "do:owner:scope:do_old%3A*"
  ));
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.deepEqual(testState.doAlarmCleanups, []);
});

test("worker delete retries instead of committing when DO object count drifts after collection", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    doObjectCount: 0,
    doObjectCountDuringExec: 1,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-object-count-drift",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "whole_delete_contention");
  assert.equal(testState.commands.some((/** @type {unknown[]} */ call) =>
    call[0] === "SESSION_SCAN" && call[1] === "do:owner:scope:do_old%3A*"
  ), false);
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

test("worker delete deduplicates Redis SCAN results before drift checks and counts", async () => {
  const ownerKey = "do:owner:scope:do_old%3ARoom%3Ashard0";
  const queueKey = "queue-consumer:demo:jobs";
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    doOwnerKeys: [ownerKey, ownerKey],
    doOwnerKeysDuringExec: [ownerKey],
    queueConsumerKeys: [queueKey, queueKey],
    queueConsumerKeysDuringExec: [queueKey],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-scan-duplicates",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.equal(body.queueConsumersRemoved, 1);
  assert.deepEqual(
    testState.commands.find((/** @type {unknown[]} */ call) => call[0] === "CLIENT_HGETMANY"),
    ["CLIENT_HGETMANY", [[queueKey, "worker"]]],
  );
  assert.equal(
    (testState.watchBatches.at(-1) || []).filter((key) => key === ownerKey).length,
    1,
  );
});

test("worker delete bounds and batches under-WATCH projection snapshots", async () => {
  const retainedVersions = Array.from({ length: 65 }, (_, index) => `v${index + 1}`);
  const queueConsumerKeys = ["queue-consumer:demo:jobs", "queue-consumer:demo:email"];
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    hasWorkerSecrets: true,
    hasWorkflowDefs: true,
    queueConsumerKeys,
    retainedVersions,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-batched-snapshot",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.deepEqual(
    testState.commands.find((/** @type {unknown[]} */ call) => call[0] === "SESSION_EXISTSMANY"),
    ["SESSION_EXISTSMANY", ["secrets:demo:api", "wf:defs:demo:api"]]
  );
  const setBatches = testState.commands
    .filter((/** @type {unknown[]} */ call) => call[0] === "SESSION_SMEMBERSMANY")
    .map((call) => /** @type {string[]} */ (call[1]));
  assert.equal(setBatches.length, 5);
  assert.ok(setBatches.every((keys) => keys.length <= 16));
  assert.deepEqual(
    setBatches.flat(),
    retainedVersions.map((version) => `worker-version-referrers:demo:api:${version}`)
  );
  assert.ok(testState.commands.some((/** @type {unknown[]} */ call) =>
    call[0] === "SESSION_SCARD" && call[1] === "do:objects:do_old"
  ));
  assert.deepEqual(
    testState.commands.find((/** @type {unknown[]} */ call) =>
      call[0] === "SESSION_HGETALLMANY" && Array.isArray(call[1]) && call[1][0]?.startsWith("queue-consumer:")),
    ["SESSION_HGETALLMANY", queueConsumerKeys]
  );
});

test("worker delete uses the under-WATCH sibling route snapshot", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    siblingVersionBeforeFirstWatch: "v2",
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-sibling-promote-race",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.equal(body.namespaceStillActive, true);
  assert.equal(testState.watchBatches.length, 1);
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "SREM" && call[1] === "namespaces" && call[2] === "demo"
  ), false);
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "PUBLISH" && call[1] === "routes:flush"
  ), false);
  assert.ok(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "PUBLISH" && call[1] === "routes:invalidate" && call[2] === "demo"
  ));
});

test("worker delete ignores an unrelated sibling version change before WATCH", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    siblingVersion: "v2",
    siblingVersionBeforeFirstWatch: "v3",
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-sibling-version-race",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.equal(body.namespaceStillActive, true);
  assert.equal(testState.watchBatches.length, 1);
});

test("worker delete recomputes sibling host ownership under WATCH", async () => {
  const host = "app.example";
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    bundleMetaRaw: JSON.stringify({
      bindings: {},
      routes: [{ host, slot: "target-slot" }],
    }),
    siblingVersionBeforeFirstWatch: "v2",
    patternRecordsDuringExec: {
      [`patterns:${host}`]: {
        "sibling-slot": "v2\tdemo\tworkerB\tv2\texact\t/other",
      },
    },
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-sibling-host-race",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.namespaceStillActive, true);
  assert.equal(testState.watchBatches.length, 1);
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) =>
    call[0] === "SREM" && call[1] === "ns-hosts:demo" && call.includes(host)
  ), false);
});

test("worker delete fails closed on malformed held pattern projections", async () => {
  const host = "app.example";
  const slot = "target-slot";
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    bundleMetaRaw: JSON.stringify({
      bindings: {},
      routes: [{ host, slot }],
    }),
    patternRecordsDuringExec: {
      [`patterns:${host}`]: { [slot]: "not-a-projection" },
    },
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-corrupt-pattern",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_pattern_projection");
  assert.equal(testState.multiCalls.some((call) => call[0] === "EXEC"), false);
});

test("worker delete cannot commit after its delete lock token is replaced", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    lockTokenDuringExec: "replacement-token",
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-lock-replaced",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "deleting");
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.deepEqual(testState.doAlarmCleanups, []);
});

test("worker delete WATCH rejects a lock replacement after the token read", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    replaceLockAfterRead: true,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-lock-replaced-after-read",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "deleting");
  assert.ok(testState.watchBatches.some((/** @type {string[]} */ keys) =>
    keys.includes("worker-delete-lock:demo:api")
  ));
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
});

test("worker delete rejects an active version missing from the retained index", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    retainedVersions: [],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-active-index-drift",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "projection_drift");
  assert.equal(body.active_version, "v1");
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
});

test("worker delete dry-run rejects an active version missing from the retained index", async () => {
  const testState = resetDeleteHandlerState({
    assetPrefix: null,
    retainedVersions: [],
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-dry-run-active-index-drift",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "projection_drift");
  assert.equal(body.active_version, "v1");
  assert.equal(body.dryRun, true);
  assert.equal(testState.releaseCalls, 0);
});

test("worker delete dry-run retries an active/index snapshot split by a secret bump", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: "v2",
    assetPrefix: null,
    retainedVersions: ["v1", "v2"],
  });
  let versionReads = 0;
  testState.redis.zRange = async (/** @type {string} */ key) => {
    assert.equal(key, "worker-versions:demo:api");
    versionReads += 1;
    return versionReads === 1 ? ["v1"] : ["v1", "v2"];
  };

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-dry-run-secret-bump-race",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.dryRun, true);
  assert.deepEqual(body.versionsDeleted, ["v1", "v2"]);
  assert.equal(versionReads, 3);
  assert.equal(testState.releaseCalls, 0);
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
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo", worker: "api", requestId: "rid-dry-run-workflows",
  }]);
  assert.deepEqual(body.workflowBlocker, {
    error: "workflow_instances_active",
    message: "demo/api has active workflow instances",
    count: 1,
    blockers: [{ workflowKey: "wf_old", instanceId: "inst-1" }],
  });
  assert.equal(testState.releaseCalls, 0);
});

test("worker delete dry-run retries when a retained version is deleted during collection", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    bundleMetaRaw: null,
    doStorageId: null,
    queueConsumerWorker: null,
  });
  let versionReads = 0;
  testState.redis.zRange = async (/** @type {string} */ key) => {
    assert.equal(key, "worker-versions:demo:api");
    versionReads += 1;
    return versionReads === 1 ? ["v1"] : [];
  };

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-dry-run-version-delete-race",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, false);
  assert.equal(body.noop, true);
  assert.deepEqual(body.versionsDeleted, []);
  assert.equal(versionReads, 3);
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
      requestId: "rid-delete-both-blockers",
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
    requestId: "rid-delete-version-blocker",
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
    assert.deepEqual(testState.doAlarmCleanups, [{
      ns: "demo",
      worker: "api",
      doStorageId: "do_old",
      requestId: "rid-delete-alarm-cleanup-failed",
    }]);
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
      requestId: "rid-delete-workflow-no-lifecycle",
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
  assert.deepEqual(testState.doAlarmCleanups, [{
    ns: "demo",
    worker: "api",
    doStorageId: "do_old",
    requestId: "rid-delete-noop-alarm-cleanup",
  }]);
  assert.deepEqual(testState.multiCalls, [
    ["DEL", "worker:do-storage:demo:api"],
    ["EXEC"],
  ]);
});

test("worker delete noop cannot clean residual state after its lock is replaced", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    queueConsumerWorker: null,
    retainedVersions: [],
    lockTokenDuringExec: "replacement-token",
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-noop-lock-replaced",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "deleting");
  assert.deepEqual(testState.multiCalls, []);
  assert.deepEqual(testState.doAlarmCleanups, []);
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

test("worker delete removes orphaned workflow definitions without active versions", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    doStorageId: null,
    queueConsumerWorker: null,
    retainedVersions: [],
    hasWorkflowDefs: true,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-orphan-workflow-defs",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.ok(testState.watchBatches.some((keys) => keys.includes("wf:defs:demo:api")));
  assert.ok(testState.multiCalls.some((call) =>
    call[0] === "DEL" && call.includes("wf:defs:demo:api")
  ));
});

test("worker delete dry-run reports orphaned workflow definitions", async () => {
  const testState = resetDeleteHandlerState({
    activeVersion: null,
    assetPrefix: null,
    doStorageId: null,
    queueConsumerWorker: null,
    retainedVersions: [],
    hasWorkflowDefs: true,
  });

  const response = await handle({
    request: new Request("http://control/ns/demo/worker/api/delete?dry_run=1", { method: "POST" }),
    url: new URL("http://control/ns/demo/worker/api/delete?dry_run=1"),
    ns: "demo",
    name: "api",
    principal: { kind: "ops" },
    requestId: "rid-delete-dry-run-orphan-workflow-defs",
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.deleted, true);
  assert.equal(body.hasWorkflowDefs, true);
  assert.deepEqual(body.versionsDeleted, []);
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
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo", worker: "api", allowCleanup: true, requestId: "rid-delete-no-cleanup",
  }]);
  assert.deepEqual(testState.doAlarmCleanups, [{
    ns: "demo", worker: "api", doStorageId: "do_old", requestId: "rid-delete-no-cleanup",
  }]);
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
  assert.deepEqual(testState.lockKinds, ["version"]);
  assert.deepEqual(testState.workflowChecks, [{
    ns: "demo",
    worker: "api",
    version: "v1",
    allowCleanup: true,
    requestId: "rid-version-delete",
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

test("version delete cannot commit after its delete lock token is replaced", async () => {
  const testState = resetVersionDeleteHandlerState({
    lockTokenDuringExec: "replacement-token",
  });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-lock-replaced",
  });

  const body = await readJsonResponse(response, 409);
  assert.equal(body.error, "deleting");
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.deepEqual(testState.cleanupIntents, []);
});

test("version delete keeps a definitions-only worker discoverable", async () => {
  const testState = resetVersionDeleteHandlerState({
    assetPrefix: null,
    hasWorkflowDefs: true,
  });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-workflow-defs",
  });

  await readJsonResponse(response, 200);
  assert.ok(testState.watchBatches.some((keys) => keys.includes("wf:defs:demo:api")));
  assert.equal(testState.multiCalls.some((call) =>
    call[0] === "SREM" && call[1] === "workers:demo" && call[2] === "api"
  ), false);
});

test("version delete batches sibling metadata and lifecycle existence reads", async () => {
  const siblingVersions = Array.from({ length: 33 }, (_, index) => `v${index + 2}`);
  const testState = resetVersionDeleteHandlerState({
    retainedVersions: ["v1", ...siblingVersions],
    siblingMetaByVersion: Object.fromEntries(siblingVersions.map((version) => [
      version,
      JSON.stringify({ assets: { prefix: `assets/demo/api/${version}/` }, bindings: {} }),
    ])),
  });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-batched-reads",
  });

  await readJsonResponse(response, 200);
  const siblingReadBatches = testState.readCommands.filter((command) => command[0] === "HGETMANY");
  const siblingReadPairs = siblingReadBatches.flatMap((command) => /** @type {string[][]} */ (command[1]));
  assert.ok(siblingReadBatches.length > 1);
  assert.ok(siblingReadBatches.every((command) => /** @type {unknown[]} */ (command[1]).length <= 32));
  assert.deepEqual(siblingReadPairs, siblingVersions.map((version) => [
    `worker:demo:api:v:${version.slice(1)}`,
    "__meta__",
  ]));
  assert.deepEqual(testState.readCommands.filter((command) => command[0] === "EXISTSMANY"), [[
    "EXISTSMANY",
    ["secrets:demo:api", "wf:defs:demo:api"],
  ]]);
  assert.equal(testState.readCommands.some((command) =>
    command[0] === "HGET" && /^worker:demo:api:v:(?:[2-9]|[1-9]\d+)$/.test(String(command[1]))
  ), false);
  assert.equal(testState.readCommands.some((command) => command[0] === "EXISTS"), false);
});

test("version delete classifies non-object bundle metadata as corrupt", async () => {
  const testState = resetVersionDeleteHandlerState({ bundleMetaRaw: "null" });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-corrupt-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.namespace, "demo");
  assert.equal(body.name, "api");
  assert.equal(body.version, "v1");
  const rejection = /** @type {any} */ (testState.logs.find((/** @type {any} */ entry) =>
    entry.event === "version_delete_rejected"
  ));
  assert.ok(rejection);
  assert.equal(rejection.level, "error");
  assert.equal(rejection.fields.metadata_version, "v1");
  assert.equal(rejection.fields.stage, "target_meta_parse");
  assert.equal(rejection.fields.error_detail, "__meta__ must be a JSON object");
  assert.deepEqual(testState.cleanupIntents, []);
  assert.equal(testState.releaseCalls, 1);
});

test("version delete classifies indexed target metadata absence as corrupt", async () => {
  const testState = resetVersionDeleteHandlerState({ bundleMetaRaw: null });

  const response = await handleVersions({
    method: "DELETE",
    ns: "demo",
    name: "api",
    subPath: ["v1"],
    principal: { kind: "ops" },
    requestId: "rid-version-delete-missing-target-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.version, "v1");
  assert.deepEqual(testState.cleanupIntents, []);
  assert.equal(testState.multiCalls.some((/** @type {any} */ call) => call[0] === "EXEC"), false);
  assert.equal(testState.releaseCalls, 1);
});

for (const [label, siblingMetaRaw] of [
  ["missing", null],
  ["empty", ""],
]) {
  test(`version delete fails closed when indexed sibling metadata is ${label}`, async () => {
    const testState = resetVersionDeleteHandlerState({
      retainedVersions: ["v1", "v2"],
      siblingMetaRaw,
    });

    const response = await handleVersions({
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["v1"],
      principal: { kind: "ops" },
      requestId: `rid-version-delete-${label}-sibling-meta`,
    });

    const body = await readJsonResponse(response, 500);
    assert.equal(body.error, "corrupt_meta");
    assert.equal(body.version, "v2");
    assert.equal(body.stage, undefined);
    assert.equal(body.detail, undefined);
    const rejection = /** @type {any} */ (testState.logs.find((/** @type {any} */ entry) =>
      entry.event === "version_delete_rejected"
    ));
    assert.ok(rejection);
    assert.equal(rejection.level, "error");
    assert.equal(rejection.fields.version, "v1");
    assert.equal(rejection.fields.metadata_version, "v2");
    assert.equal(rejection.fields.stage, "sibling_meta_parse");
    assert.equal(typeof rejection.fields.error_detail, "string");
    assert.deepEqual(testState.cleanupIntents, []);
    assert.equal(testState.releaseCalls, 1);
  });
}

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
    requestId: "rid-version-delete-no-cleanup",
  }]);
  assert.equal(body.assets.queueHint, "none");
  assert.deepEqual(body.assets.warnings, []);
  assert.deepEqual(testState.cleanupIntents, []);
  assert.equal(testState.releaseCalls, 1);
});
