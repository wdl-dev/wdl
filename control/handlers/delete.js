// The delete-lock covers only the Redis critical section (collect → WATCH
// → MULTI/EXEC); physical storage cleanup is outside worker-delete. Release
// is compare-and-delete — a TTL-expired-then-reacquired lock must not get
// freed by the request whose lock expired.

import {
  jsonResponse, jsonError,
  requireControlLog, requireControlRedis,
  errMessage,
  acquireDeleteLock, releaseDeleteLock, renewDeleteLock, deleteLockExpiredDetails,
  assertWorkflowDeleteAllowed, cleanupDoAlarmsForWorker,
  buildS3CleanupTaskId, recordCleanupIntentOrWarn,
  ControlAbort, codedErrorLogFields, controlAbortResponse,
  withOptimisticRetries,
  ROUTES_CHANNEL, ROUTES_FLUSH_CHANNEL, PATTERNS_CHANNEL,
} from "control-shared";
import {
  referrersKey,
  doObjectRegistryKey,
  workflowDefsKey,
  extractD1Refs, extractOutgoingRefs,
  formatReferrerBlocker,
  bundleAssetPrefix,
  parseBundleMeta,
  parsePatternProjection,
} from "control-lib";
import {
  WHOLE_DELETE_LOCK_KIND,
  bundleKey,
  deleteLockKey,
  doOwnerScopeScanPatternForStorage,
  doStorageIdKey,
  hostsKey,
  patternsKey,
  routesKey,
  workerVersionsKey,
} from "shared-worker-contract";
import { workerSecretsKey } from "shared-secret-keys";
import { decodeBulk, WatchError } from "shared-redis";
import { queueConsumerScanPrefix } from "shared-queue-keys";
import { discardResponseBody } from "shared-respond";
import { buildWorkerDeleteCleanup, stageWorkerDelete } from "control-handlers-delete-plan";

const MAX_DELETE_ATTEMPTS = 5;
const DELETE_COLLECT_READ_CONCURRENCY = 16;

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("control-shared").ControlLogger} ControlLogger
 * @typedef {import("control-lib").AccessPrincipal} AccessPrincipal
 * @typedef {{ scan(cursor: string, match: string, count?: number): Promise<[string, string[]]> }} RedisScanner
 * @typedef {{ request: Request, env?: Record<string, unknown>, url: URL, ns: string, name: string, principal?: AccessPrincipal | null, requestId: string }} DeleteHandlerArgs
 * @typedef {{ redis: RedisClient, ns: string, name: string }} DeleteCollectArgs
 * @typedef {{ host: string, slot: string, [key: string]: unknown }} RouteSlot
 * @typedef {{ binding: string, databaseId: string }} D1Ref
 * @typedef {{ targetNs: string, targetWorker: string, targetVersion: string, binding: string }} OutgoingRef
 * @typedef {{
 *   ns: string,
 *   name: string,
 *   queueConsumerKeys: string[],
 *   doStorageId: string | null,
 *   doObjectRegistry: string | null,
 *   doObjectMembers: string[],
 *   doOwnerKeys: string[],
 *   retainedVersions: string[],
 *   prefixByVersion: Record<string, string>,
 *   d1RefsByVersion: Record<string, D1Ref[]>,
 *   outgoingRefsByVersion: Record<string, OutgoingRef[]>,
 *   referrersByVersion: Record<string, string[]>,
 *   activeVersion: string | null,
 *   activeRoutes: RouteSlot[],
 *   affectedHosts: string[],
 *   hostsLosingNsOwnership: string[],
 *   namespaceStillActive: boolean,
 *   hasWorkerSecrets: boolean,
 *   hasWorkflowDefs: boolean,
 * }} DeleteInputs
 * @typedef {{ retained: boolean, objects: number, doStorageId?: string }} DoStorageRetention
 * @typedef {{ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs?: number }} CleanupIntent
 * @typedef {{ noop: true, namespaceStillActive: boolean, doStorageRetention: DoStorageRetention } | {
 *   noop: false,
 *   activeVersion: string | null,
 *   retainedVersions: string[],
 *   affectedHosts: string[],
 *   queueConsumersRemoved: number,
 *   namespaceStillActive: boolean,
 *   cleanupTaskId: string | null,
 *   cleanupIntent: CleanupIntent | null,
 *   dedupedPrefixes: string[],
 *   doStorageRetention?: DoStorageRetention,
 * }} DeleteOutcome
 */

class WholeDeleteError extends ControlAbort {}

/** @param {unknown} raw @param {string} host @param {string} slot */
function requirePatternProjection(raw, host, slot) {
  return parsePatternProjection(raw, {
    host,
    slot,
    makeError: (details) => new WholeDeleteError(500, "corrupt_pattern_projection", {
      ...details,
      stage: "pattern_projection_parse",
    }),
  });
}

/** @param {string} ns @param {string} name @returns {never} */
function throwWholeDeleteContention(ns, name) {
  throw new WholeDeleteError(503, "whole_delete_contention", {
    namespace: ns, name,
    message: `exhausted ${MAX_DELETE_ATTEMPTS} attempts; retry later`,
  });
}

/** @param {string} ns @param {string} name @returns {never} */
function throwDeleteLockExpired(ns, name) {
  throw new WholeDeleteError(409, "deleting", deleteLockExpiredDetails(ns, name));
}

// Raised when collection or an under-WATCH check observes state that
// changed across reads; callers retry from a fresh snapshot.
class DriftSignal extends Error {
  /** @param {string} reason */
  constructor(reason) { super(reason); this.name = "DriftSignal"; }
}

/** @param {DeleteHandlerArgs} args */
export async function handle({ request, env: _env, url, ns, name, principal, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const dryRun = url.searchParams.get("dry_run") === "1";

  // Flags live in the query. Cancel an unexpected body without buffering it or
  // allowing a non-settling cancellation to block either delete path.
  await discardResponseBody(request);

  if (dryRun) {
    return handleDryRun({ redis, ns, name, principal, requestId, log });
  }

  const lockToken = await acquireDeleteLock(redis, ns, name, WHOLE_DELETE_LOCK_KIND);
  if (!lockToken) {
    log("warn", "worker_delete_rejected", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      status: 409,
      reason: "deleting",
    });
    return jsonError(409, "deleting", `${ns}/${name} is being deleted by another request`);
  }

  try {
    let outcome;
    try {
      outcome = await executeWholeDelete({
        redis, ns, name, principal, requestId, log, lockToken,
      });
    } catch (err) {
      if (err instanceof ControlAbort) {
        log(err.status >= 500 ? "error" : "warn", "worker_delete_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          ...codedErrorLogFields(err),
        });
        return controlAbortResponse(err);
      }
      throw err;
    }

    if (outcome.noop) {
      log("info", "worker_delete_noop", {
        request_id: requestId,
        namespace: ns,
        worker: name,
      });
      return jsonResponse(200, {
        namespace: ns, name, deleted: false,
        versionsDeleted: [],
        activeDeleted: null,
        affectedHosts: [],
        queueConsumersRemoved: 0,
        namespaceStillActive: outcome.namespaceStillActive,
        durableObjects: {
          storageRetention: outcome.doStorageRetention || { retained: false, objects: 0 },
        },
      });
    }

    const deletedOutcome = /** @type {Extract<DeleteOutcome, { noop: false }>} */ (outcome);
    const { queueHintStatus, warnings } = await recordCleanupIntentOrWarn({
      cleanupIntent: deletedOutcome.cleanupIntent,
      cleanupTaskId: deletedOutcome.cleanupTaskId,
      warningMessage: "Worker content cleanup was not queued; retry deletion cleanup manually.",
      logEvent: "worker_cleanup_queue_failed",
      logFields: { request_id: requestId, namespace: ns, worker: name },
      log,
    });

    log("info", "worker_deleted", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      active_deleted: deletedOutcome.activeVersion,
      versions_deleted: deletedOutcome.retainedVersions.length,
      affected_hosts: deletedOutcome.affectedHosts.length,
      queue_consumers_removed: deletedOutcome.queueConsumersRemoved,
      namespace_still_active: deletedOutcome.namespaceStillActive,
      cleanup_task_id: deletedOutcome.cleanupTaskId,
    });

    return jsonResponse(200, {
      namespace: ns,
      name,
      deleted: true,
      activeDeleted: deletedOutcome.activeVersion || null,
      versionsDeleted: deletedOutcome.retainedVersions,
      affectedHosts: deletedOutcome.affectedHosts,
      queueConsumersRemoved: deletedOutcome.queueConsumersRemoved,
      namespaceStillActive: deletedOutcome.namespaceStillActive,
      assets: {
        queueHint: queueHintStatus,
        warnings,
      },
      durableObjects: {
        storageRetention: deletedOutcome.doStorageRetention || { retained: false, objects: 0 },
      },
    });
  } finally {
    await releaseDeleteLock(redis, ns, name, lockToken, requestId);
  }
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, principal?: AccessPrincipal | null, requestId: string, log: ControlLogger }} args
 */
async function handleDryRun({ redis, ns, name, principal, requestId, log }) {
  /** @type {DeleteInputs | null} */
  let collected = null;
  let workflowBlocker = null;
  try {
    collected = await withOptimisticRetries(
      async () => {
        const snapshot = await collectDeleteInputs({ redis, ns, name });
        await assertDryRunActiveVersionRetained(redis, snapshot);
        return snapshot;
      },
      {
        attempts: MAX_DELETE_ATTEMPTS,
        isRetryableError: (err) => err instanceof DriftSignal,
        onExhausted: () => throwWholeDeleteContention(ns, name),
      }
    );
    await assertWorkflowDeleteAllowed({ ns, worker: name, requestId });
  } catch (err) {
    if (err instanceof WholeDeleteError) {
      log(err.status >= 500 ? "error" : "warn", "worker_dry_run_rejected", {
        request_id: requestId, namespace: ns, worker: name,
        ...codedErrorLogFields(err),
      });
      return controlAbortResponse(err, { dryRun: true });
    }
    if (err instanceof ControlAbort && err.code === "workflow_instances_active") {
      workflowBlocker = err;
    } else if (err instanceof ControlAbort) {
      log(err.status >= 500 ? "error" : "warn", "worker_dry_run_rejected", {
        request_id: requestId,
        namespace: ns,
        worker: name,
        ...codedErrorLogFields(err),
      });
      return controlAbortResponse(err, { dryRun: true });
    } else {
      throw err;
    }
  }
  if (!collected) throw new WholeDeleteError(500, "delete_inputs_unavailable", { namespace: ns, name });
  const blockers = buildReferrerBlockers({
    retainedVersions: collected.retainedVersions,
    referrersByVersion: collected.referrersByVersion,
    targetNs: ns,
    principal,
  });
  const noop =
    !collected.activeVersion &&
    collected.retainedVersions.length === 0 &&
    !collected.hasWorkerSecrets &&
    !collected.hasWorkflowDefs;

  log("info", "worker_delete_dry_run", {
    request_id: requestId,
    namespace: ns,
    worker: name,
    versions: collected.retainedVersions.length,
    blockers: blockers.length,
    noop,
  });

  /** @type {Record<string, unknown>} */
  const payload = {
    dryRun: true,
    namespace: ns,
    name,
    deleted: blockers.length === 0 && !noop,
    activeDeleted: collected.activeVersion || null,
    versionsDeleted: collected.retainedVersions,
    affectedHosts: collected.affectedHosts,
    queueConsumersRemoved: collected.queueConsumerKeys.length,
    namespaceStillActive: collected.namespaceStillActive,
    hasWorkerSecrets: collected.hasWorkerSecrets,
    hasWorkflowDefs: collected.hasWorkflowDefs,
    durableObjects: {
      storageRetention: describeDoStorageRetention(collected),
    },
  };
  if (blockers.length > 0) {
    payload.deleted = false;
    payload.blockers = blockers;
  }
  if (workflowBlocker) {
    payload.deleted = false;
    payload.workflowBlocker = {
      error: workflowBlocker.code,
      message: workflowBlocker.message,
      count: workflowBlocker.details?.count || 0,
      blockers: workflowBlocker.details?.blockers || [],
    };
  }
  if (noop) {
    payload.deleted = false;
    payload.noop = true;
  }
  return jsonResponse(200, payload);
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, principal?: AccessPrincipal | null, requestId: string, log: ControlLogger, lockToken: string }} args
 * @returns {Promise<DeleteOutcome>}
 */
async function executeWholeDelete({ redis, ns, name, principal, requestId, log, lockToken }) {
  return await withOptimisticRetries(async () => {
    const collected = await collectDeleteInputs({ redis, ns, name });
    assertActiveVersionRetained(collected);
    let workflowBlocker = null;
    try {
      await assertWorkflowDeleteAllowed({ ns, worker: name, allowCleanup: true, requestId });
    } catch (err) {
      if (err instanceof ControlAbort && err.code === "workflow_instances_active") {
        workflowBlocker = err;
      } else {
        throw err;
      }
    }

    const hasWorkerLifecycle =
      collected.activeVersion ||
      collected.retainedVersions.length > 0 ||
      collected.hasWorkerSecrets ||
      collected.hasWorkflowDefs;
    if (workflowBlocker && !hasWorkerLifecycle) {
      throw workflowBlocker;
    }
    if (!hasWorkerLifecycle) {
      if (!await renewDeleteLock(redis, ns, name, lockToken)) {
        throwDeleteLockExpired(ns, name);
      }
      await deleteResidualDoRedis(redis, collected, lockToken);
      await cleanupDoAlarmsOrWarn({
        ns, worker: name, doStorageId: collected.doStorageId, requestId, log,
      });
      return {
        noop: true,
        namespaceStillActive: collected.namespaceStillActive,
        doStorageRetention: describeDoStorageRetention(collected),
      };
    }

    const blockers = buildReferrerBlockers({
      retainedVersions: collected.retainedVersions,
      referrersByVersion: collected.referrersByVersion,
      targetNs: ns,
      principal,
    });
    if (workflowBlocker) {
      if (blockers.length > 0) {
        workflowBlocker.details = {
          ...(workflowBlocker.details || {}),
          versionBlockers: blockers,
        };
      }
      throw workflowBlocker;
    }
    if (blockers.length > 0) {
      throw new WholeDeleteError(409, "version_referenced", {
        namespace: ns, name,
        blockers,
      });
    }
    if (!await renewDeleteLock(redis, ns, name, lockToken)) {
      throwDeleteLockExpired(ns, name);
    }

    const result = await runSessionEXEC({
      redis, ns, name, principal, requestId, collected, lockToken,
    });
    await cleanupDoAlarmsOrWarn({
      ns, worker: name, doStorageId: collected.doStorageId, requestId, log,
    });
    result.doStorageRetention = describeDoStorageRetention(collected);
    return result;
  }, {
    attempts: MAX_DELETE_ATTEMPTS,
    isRetryableError: (err) => err instanceof DriftSignal || err instanceof WatchError,
    onExhausted: () => throwWholeDeleteContention(ns, name),
  });
}

/** @param {DeleteInputs} collected */
function assertActiveVersionRetained(collected) {
  if (!collected.activeVersion || collected.retainedVersions.includes(collected.activeVersion)) return;
  throw new WholeDeleteError(409, "projection_drift", {
    namespace: collected.ns,
    name: collected.name,
    active_version: collected.activeVersion,
    reason: "active_not_in_worker_versions",
  });
}

/** @param {RedisClient} redis @param {DeleteInputs} collected */
async function assertDryRunActiveVersionRetained(redis, collected) {
  if (!collected.activeVersion || collected.retainedVersions.includes(collected.activeVersion)) return;
  const [retainedVersions, activeVersion] = await Promise.all([
    redis.zRange(workerVersionsKey(collected.ns, collected.name), 0, -1),
    redis.hGet(routesKey(collected.ns), collected.name),
  ]);
  if (
    activeVersion !== collected.activeVersion ||
    !arraysShallowEqual(retainedVersions, collected.retainedVersions)
  ) {
    throw new DriftSignal("active version or retained versions changed during dry-run");
  }
  assertActiveVersionRetained(collected);
}

/** @param {DeleteInputs} collected */
function describeDoStorageRetention(collected) {
  return {
    retained: collected.doObjectMembers.length > 0,
    objects: collected.doObjectMembers.length,
    ...(collected.doStorageId ? { doStorageId: collected.doStorageId } : {}),
  };
}

/**
 * @param {RedisScanner} redis
 * @param {string | null} doStorageId
 */
async function scanDoOwnerKeys(redis, doStorageId) {
  if (!doStorageId) return [];
  const keys = new Set();
  let cursor = "0";
  do {
    const [next, found] = await redis.scan(cursor, doOwnerScopeScanPatternForStorage(doStorageId), 100);
    for (const key of found) keys.add(key);
    cursor = next;
  } while (cursor !== "0");
  return [...keys];
}

/**
 * @param {RedisScanner & Pick<RedisClient, "hGet"> & Partial<Pick<RedisClient, "hGetMany">>} redis
 * @param {string} ns
 * @param {string} name
 */
async function scanQueueConsumerKeysForWorker(redis, ns, name) {
  const queueConsumerKeys = new Set();
  const prefix = queueConsumerScanPrefix(ns);
  let cursor = "0";
  do {
    const [next, found] = await redis.scan(cursor, `${prefix}*`, 100);
    const keys = [...new Set(found)];
    const workers = typeof redis.hGetMany === "function"
      ? await redis.hGetMany(keys.map((k) => [k, "worker"]))
      : await mapConcurrent(keys, DELETE_COLLECT_READ_CONCURRENCY, (k) =>
        redis.hGet(k, "worker")
      );
    for (let i = 0; i < keys.length; i += 1) {
      if (workers[i] === name) queueConsumerKeys.add(keys[i]);
    }
    cursor = next;
  } while (cursor !== "0");
  return [...queueConsumerKeys];
}

/**
 * @param {RedisClient} redis
 * @param {DeleteInputs} collected
 * @param {string} lockToken
 */
async function deleteResidualDoRedis(redis, collected, lockToken) {
  await redis.session(async (iso) => {
    const lockKey = deleteLockKey(collected.ns, collected.name);
    const storageKey = doStorageIdKey(collected.ns, collected.name);
    await iso.watch(
      lockKey,
      routesKey(collected.ns),
      workerVersionsKey(collected.ns, collected.name),
      workerSecretsKey(collected.ns, collected.name),
      workflowDefsKey(collected.ns, collected.name),
      storageKey,
      ...collected.doOwnerKeys,
    );

    if (await iso.get(lockKey) !== lockToken) {
      await iso.unwatch();
      throwDeleteLockExpired(collected.ns, collected.name);
    }
    const activeVersion = await iso.hGet(routesKey(collected.ns), collected.name);
    const retainedVersions = await iso.zRange(workerVersionsKey(collected.ns, collected.name), 0, -1);
    const hasWorkerSecrets = (await iso.exists(workerSecretsKey(collected.ns, collected.name))) > 0;
    const hasWorkflowDefs = (await iso.exists(workflowDefsKey(collected.ns, collected.name))) > 0;
    if (activeVersion || retainedVersions.length > 0 || hasWorkerSecrets || hasWorkflowDefs) {
      await iso.unwatch();
      throw new DriftSignal("worker lifecycle appeared during residual cleanup");
    }
    if (await iso.get(storageKey) !== collected.doStorageId) {
      await iso.unwatch();
      throw new DriftSignal("DO storage pointer changed during residual cleanup");
    }
    const currentOwnerKeys = await scanDoOwnerKeys(iso, collected.doStorageId);
    if (!arraysShallowEqual(currentOwnerKeys.toSorted(), collected.doOwnerKeys.toSorted())) {
      await iso.unwatch();
      throw new DriftSignal("DO owner keys changed during residual cleanup");
    }

    if (!collected.doStorageId && collected.doOwnerKeys.length === 0) {
      await iso.unwatch();
      return;
    }
    const multi = iso.multi();
    if (collected.doOwnerKeys.length) multi.del(...collected.doOwnerKeys);
    if (collected.doStorageId) multi.del(storageKey);
    await multi.exec();
  });
}

/**
 * @param {{ ns: string, worker: string, doStorageId: string | null, requestId: string, log: ControlLogger }} args
 */
async function cleanupDoAlarmsOrWarn({ ns, worker, doStorageId, requestId, log }) {
  if (!doStorageId) return;
  try {
    await cleanupDoAlarmsForWorker({ ns, worker, doStorageId, requestId });
  } catch (err) {
    log("warn", "worker_do_alarm_cleanup_failed", {
      request_id: requestId,
      namespace: ns,
      worker,
      error_message: errMessage(err),
    });
  }
}

/**
 * @param {DeleteCollectArgs} args
 * @returns {Promise<DeleteInputs>}
 */
async function collectDeleteInputs({ redis, ns, name }) {
  const doStorageId = /** @type {string | null} */ (
    decodeBulk((await redis.get(doStorageIdKey(ns, name))) ?? null) ?? null
  );
  const doObjectRegistry = doStorageId ? doObjectRegistryKey(doStorageId) : null;
  const doObjectMembers = doObjectRegistry ? await redis.sMembers(doObjectRegistry) : [];

  const doOwnerKeys = await scanDoOwnerKeys(redis, doStorageId);

  const queueConsumerKeys = await scanQueueConsumerKeysForWorker(redis, ns, name);

  // Metadata behind retained/active indexes is required: proceeding without
  // it would omit S3 and reverse-ref cleanup from a successful delete.
  const retainedVersions = await redis.zRange(workerVersionsKey(ns, name), 0, -1);
  /** @type {Record<string, string>} */
  const prefixByVersion = {};
  /** @type {Record<string, D1Ref[]>} */
  const d1RefsByVersion = {};
  /** @type {Record<string, OutgoingRef[]>} */
  const outgoingRefsByVersion = {};
  /** @type {Record<string, string[]>} */
  const referrersByVersion = {};
  const retainedReads = await mapConcurrent(retainedVersions, DELETE_COLLECT_READ_CONCURRENCY, async (ver) => {
    const [rawMeta, referrers] = await Promise.all([
      redis.hGet(bundleKey(ns, name, ver), "__meta__"),
      redis.sMembers(referrersKey(ns, name, ver)),
    ]);
    return { ver, rawMeta, referrers };
  });
  for (const { ver, rawMeta, referrers } of retainedReads) {
    if (rawMeta == null) {
      const currentVersions = await redis.zRange(workerVersionsKey(ns, name), 0, -1);
      if (!currentVersions.includes(ver)) {
        throw new DriftSignal("retained versions changed during collection");
      }
    }
    const meta = parseBundleMeta(rawMeta, {
      ns,
      worker: name,
      version: ver,
      makeError: ({ reason }) => new WholeDeleteError(500, "corrupt_meta", {
        namespace: ns, name, version: ver,
        stage: "retained_meta_parse",
        detail: reason,
      }),
    });
    const assetPrefix = bundleAssetPrefix(meta);
    if (assetPrefix !== null) prefixByVersion[ver] = assetPrefix;
    d1RefsByVersion[ver] = extractD1Refs(meta.bindings);
    outgoingRefsByVersion[ver] = extractOutgoingRefs(meta.bindings, ns);
    referrersByVersion[ver] = referrers;
  }

  const routesHash = await redis.hGetAll(routesKey(ns));
  const activeVersion = routesHash[name] || null;

  /** @type {RouteSlot[]} */
  let activeRoutes = [];
  if (activeVersion) {
    const rawMeta = await redis.hGet(bundleKey(ns, name, activeVersion), "__meta__");
    if (rawMeta == null) {
      const currentActive = await redis.hGet(routesKey(ns), name);
      if (currentActive !== activeVersion) {
        throw new DriftSignal("active version changed during collection");
      }
    }
    const meta = parseBundleMeta(rawMeta, {
      ns,
      worker: name,
      version: activeVersion,
      makeError: ({ reason }) => new WholeDeleteError(500, "corrupt_meta", {
        namespace: ns, name, version: activeVersion,
        stage: "active_meta_parse",
        detail: reason,
      }),
    });
    activeRoutes = normalizeActiveRoutes(meta, {
      namespace: ns,
      name,
      version: activeVersion,
    });
  }

  // A host stays in ns-hosts if any OTHER worker in this ns still holds a
  // pattern slot on it — SREMing because we just happen to own a slot
  // there would strand those other workers from their declared host.
  const hostsInActiveRoutes = [...new Set(activeRoutes.map((r) => r.host))];
  const hostPatternReads = await redis.hGetAllMany(hostsInActiveRoutes.map((host) => patternsKey(host)));
  const hostsLosingNsOwnership = findHostsLosingNsOwnership(
    ns,
    activeRoutes,
    hostsInActiveRoutes,
    hostPatternReads,
  );

  // Drives the EXEC-time channel choice. Empty namespaces need routes:flush
  // because routes:invalidate <ns> would re-add the freshly SREM'd ns to
  // gateway knownNs.
  const otherActive = Object.keys(routesHash).filter((k) => k !== name);
  const namespaceStillActive = otherActive.length > 0;

  const hasWorkerSecrets = (await redis.exists(workerSecretsKey(ns, name))) > 0;
  const hasWorkflowDefs = (await redis.exists(workflowDefsKey(ns, name))) > 0;

  return {
    ns,
    name,
    queueConsumerKeys,
    doStorageId,
    doObjectRegistry,
    doObjectMembers,
    doOwnerKeys,
    retainedVersions,
    prefixByVersion,
    d1RefsByVersion,
    outgoingRefsByVersion,
    referrersByVersion,
    activeVersion,
    activeRoutes,
    affectedHosts: hostsInActiveRoutes,
    hostsLosingNsOwnership,
    namespaceStillActive,
    hasWorkerSecrets,
    hasWorkflowDefs,
  };
}

/**
 * @param {unknown} route
 * @returns {route is RouteSlot}
 */
function isRouteSlot(route) {
  const record = /** @type {Record<string, unknown> | null} */ (
    route && typeof route === "object" ? route : null
  );
  return Boolean(record && typeof record.host === "string" && typeof record.slot === "string");
}

/**
 * @param {unknown} meta
 * @param {{ namespace: string, name: string, version: string }} details
 * @returns {RouteSlot[]}
 */
function normalizeActiveRoutes(meta, details) {
  const record = /** @type {{ routes?: unknown } | null} */ (
    meta && typeof meta === "object" ? meta : null
  );
  if (!record || record.routes == null) return [];
  if (!Array.isArray(record.routes)) {
    throw new WholeDeleteError(500, "corrupt_meta", {
      ...details,
      stage: "active_meta_routes",
    });
  }
  /** @type {RouteSlot[]} */
  const routes = [];
  for (const route of record.routes) {
    if (!isRouteSlot(route)) {
      throw new WholeDeleteError(500, "corrupt_meta", {
        ...details,
        stage: "active_meta_routes",
      });
    }
    routes.push(route);
  }
  return routes;
}

/**
 * @param {{ retainedVersions: string[], referrersByVersion: Record<string, string[]>, targetNs: string, principal?: AccessPrincipal | null }} args
 */
function buildReferrerBlockers({ retainedVersions, referrersByVersion, targetNs, principal }) {
  /** @type {Array<Record<string, unknown>>} */
  const out = [];
  for (const ver of retainedVersions) {
    const mem = referrersByVersion[ver] || [];
    if (mem.length === 0) continue;
    out.push({
      version: ver,
      ...formatReferrerBlocker(mem, { targetNs, principal }),
    });
  }
  return out;
}

/**
 * @param {string[]} a
 * @param {string[]} b
 */
function arraysShallowEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * @param {string} ns
 * @param {RouteSlot[]} activeRoutes
 * @param {string[]} hosts
 * @param {Array<Record<string, string | null | undefined>>} patternRecords
 */
function findHostsLosingNsOwnership(ns, activeRoutes, hosts, patternRecords) {
  const losing = [];
  for (let i = 0; i < hosts.length; i += 1) {
    const host = hosts[i];
    const ourSlots = new Set(
      activeRoutes.filter((route) => route.host === host).map((route) => route.slot)
    );
    let siblingOwnsHost = false;
    for (const [slot, raw] of Object.entries(patternRecords[i] || {})) {
      if (ourSlots.has(slot)) continue;
      const projection = requirePatternProjection(raw, host, slot);
      if (projection.ns === ns) {
        siblingOwnsHost = true;
        break;
      }
    }
    if (!siblingOwnsHost) losing.push(host);
  }
  return losing;
}

/**
 * @template T,U
 * @param {T[]} items
 * @param {number} concurrency
 * @param {(item: T) => Promise<U>} fn
 * @returns {Promise<U[]>}
 */
async function mapConcurrent(items, concurrency, fn) {
  /** @type {U[]} */
  const out = [];
  for (let i = 0; i < items.length; i += concurrency) {
    out.push(...await Promise.all(items.slice(i, i + concurrency).map(fn)));
  }
  return out;
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, principal?: AccessPrincipal | null, requestId: string, collected: DeleteInputs, lockToken: string }} args
 * @returns {Promise<Extract<DeleteOutcome, { noop: false }>>}
 */
async function runSessionEXEC({ redis, ns, name, principal, requestId, collected, lockToken }) {
  return await redis.session(async (iso) => {
    const watchKeys = [
      routesKey(ns),
      hostsKey(ns),
      deleteLockKey(ns, name),
      doStorageIdKey(ns, name),
      workerVersionsKey(ns, name),
      workerSecretsKey(ns, name),
      workflowDefsKey(ns, name),
      ...(collected.doObjectRegistry ? [collected.doObjectRegistry] : []),
      ...collected.doOwnerKeys,
      ...collected.affectedHosts.map((h) => patternsKey(h)),
      ...collected.queueConsumerKeys,
      ...collected.retainedVersions.flatMap((ver) => [
        bundleKey(ns, name, ver),
        referrersKey(ns, name, ver),
      ]),
    ];
    await iso.watch(...watchKeys);

    if (await iso.get(deleteLockKey(ns, name)) !== lockToken) {
      await iso.unwatch();
      throwDeleteLockExpired(ns, name);
    }

    // Drift guards — normally unreachable for sanctioned control writers once
    // the delete lock is held. Keep whole-worker delete fail-closed if
    // control-owned projections no longer match the collected snapshot.
    const curVersions = await iso.zRange(workerVersionsKey(ns, name), 0, -1);
    if (!arraysShallowEqual(curVersions, collected.retainedVersions)) {
      await iso.unwatch();
      throw new DriftSignal("worker_versions changed during delete");
    }
    const curRoutes = await iso.hGetAll(routesKey(ns));
    const curActive = curRoutes[name] || null;
    if (curActive !== collected.activeVersion) {
      await iso.unwatch();
      throw new DriftSignal("active version changed during delete");
    }
    if (curActive && !curVersions.includes(curActive)) {
      await iso.unwatch();
      throw new WholeDeleteError(409, "projection_drift", {
        namespace: ns, name,
        active_version: curActive,
        reason: "active_not_in_worker_versions",
      });
    }
    const currentNamespaceStillActive = Object.keys(curRoutes).some((worker) => worker !== name);
    const currentPatternRecords = await iso.hGetAllMany(
      collected.affectedHosts.map((host) => patternsKey(host))
    );
    const currentHostsLosingNsOwnership = findHostsLosingNsOwnership(
      ns,
      collected.activeRoutes,
      collected.affectedHosts,
      currentPatternRecords,
    );
    const curHasSecrets = (await iso.exists(workerSecretsKey(ns, name))) > 0;
    if (curHasSecrets !== collected.hasWorkerSecrets) {
      await iso.unwatch();
      throw new DriftSignal("secrets presence changed during delete");
    }
    const curHasWorkflowDefs = (await iso.exists(workflowDefsKey(ns, name))) > 0;
    if (curHasWorkflowDefs !== collected.hasWorkflowDefs) {
      await iso.unwatch();
      throw new DriftSignal("workflow definitions presence changed during delete");
    }
    if (collected.doObjectRegistry) {
      const curDoObjectMembers = await iso.sMembers(collected.doObjectRegistry);
      if (!arraysShallowEqual(curDoObjectMembers.toSorted(), collected.doObjectMembers.toSorted())) {
        await iso.unwatch();
        throw new DriftSignal("DO object registry changed during delete");
      }
    }
    const curDoOwnerKeys = await scanDoOwnerKeys(iso, collected.doStorageId);
    if (!arraysShallowEqual(curDoOwnerKeys.toSorted(), collected.doOwnerKeys.toSorted())) {
      await iso.unwatch();
      throw new DriftSignal("DO owner keys changed during delete");
    }
    const curQueueConsumerKeys = await scanQueueConsumerKeysForWorker(iso, ns, name);
    if (!arraysShallowEqual(curQueueConsumerKeys.toSorted(), collected.queueConsumerKeys.toSorted())) {
      await iso.unwatch();
      throw new DriftSignal("queue consumer keys changed during delete");
    }

    // The delete lock blocks sanctioned control writers; this re-check catches
    // pre-EXEC referrer drift in the watched lifecycle state before commit.
    for (const ver of collected.retainedVersions) {
      const mem = await iso.sMembers(referrersKey(ns, name, ver));
      if (mem.length > 0) {
        await iso.unwatch();
        throw new WholeDeleteError(409, "version_referenced", {
          namespace: ns, name,
          blockers: [{
            version: ver,
            ...formatReferrerBlocker(mem, { targetNs: ns, principal }),
          }],
        });
      }
    }

    for (const r of collected.activeRoutes) {
      const held = await iso.hGet(patternsKey(r.host), r.slot);
      if (held == null) continue;
      const parsed = requirePatternProjection(held, r.host, r.slot);
      if (parsed.ns !== ns || parsed.worker !== name) {
        await iso.unwatch();
        throw new WholeDeleteError(409, "projection_drift", {
          host: r.host, slot: r.slot, owner: parsed,
        });
      }
    }

    for (const qKey of collected.queueConsumerKeys) {
      const held = await iso.hGetAll(qKey);
      if (held && held.worker && held.worker !== name) {
        await iso.unwatch();
        throw new WholeDeleteError(409, "projection_drift", {
          queue_consumer: qKey, held,
        });
      }
    }

    const commitInputs = {
      ...collected,
      hostsLosingNsOwnership: currentHostsLosingNsOwnership,
      namespaceStillActive: currentNamespaceStillActive,
    };
    const { cleanupTaskId, cleanupIntent, dedupedPrefixes } =
      buildWorkerDeleteCleanup(commitInputs, requestId, buildS3CleanupTaskId);

    const multi = iso.multi();
    stageWorkerDelete(multi, {
      collected: commitInputs,
      channels: {
        routes: ROUTES_CHANNEL,
        routesFlush: ROUTES_FLUSH_CHANNEL,
        patterns: PATTERNS_CHANNEL,
      },
    });

    await multi.exec();

    return {
      noop: false,
      activeVersion: collected.activeVersion,
      retainedVersions: collected.retainedVersions,
      affectedHosts: collected.affectedHosts,
      queueConsumersRemoved: collected.queueConsumerKeys.length,
      namespaceStillActive: currentNamespaceStillActive,
      cleanupTaskId,
      cleanupIntent,
      dedupedPrefixes,
    };
  });
}
