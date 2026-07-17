import {
  jsonResponse, jsonError,
  acquireDeleteLock, releaseDeleteLock, renewDeleteLock, deleteLockExpiredDetails,
  assertWorkflowDeleteAllowed,
  buildS3CleanupTaskId, recordCleanupIntentOrWarn,
  ControlAbort, codedErrorLogFields, controlAbortResponse,
  requireControlLog,
  requireControlRedis,
  runOptimistic,
} from "control-shared";
import {
  referrersKey,
  extractD1Refs,
  extractOutgoingRefs, formatReferrerBlocker,
  bundleAssetPrefix,
  parseBundleMeta,
  workflowDefsKey,
} from "control-lib";
import {
  stageD1ReferrerRemovals,
  stageOutgoingReferrerRemovals,
  stageWorkerHidden,
  stageWorkerVersionIndexDelete,
  stageWorkerVersionIndexRemove,
} from "control-lifecycle-indexes";
import {
  VERSION_DELETE_LOCK_KIND,
  bundleKey,
  deleteLockKey,
  parseVersion,
  routesKey,
  workerVersionsKey,
} from "shared-worker-contract";
import { workerSecretsKey } from "shared-secret-keys";

const MAX_DELETE_ATTEMPTS = 5;

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("control-lib").AccessPrincipal} AccessPrincipal
 */

class VersionDeleteError extends ControlAbort {}

/**
 * @param {{ method: string, ns: string, name: string, subPath: string[], principal?: AccessPrincipal | null, requestId: string }} args
 */
export async function handle({ method, ns, name, subPath, principal, requestId }) {
  if (method === "GET" && subPath.length === 0) {
    return handleGet({ ns, name });
  }
  if (method === "DELETE" && subPath.length === 1) {
    return handleDelete({
      ns, name, version: subPath[0], principal, requestId,
    });
  }
  return jsonError(405, "method_not_allowed", "Method not allowed");
}

/** @param {{ ns: string, name: string }} args */
async function handleGet({ ns, name }) {
  const redis = requireControlRedis();
  const activeVersion = await redis.hGet(routesKey(ns), name);
  const retainedVersions = await redis.zRange(workerVersionsKey(ns, name), 0, -1);
  const versions = retainedVersions
    .filter((version) => parseVersion(version) !== null)
    .map((version) => ({ version, active: version === activeVersion }));
  return jsonResponse(200, { namespace: ns, name, versions });
}

/** @param {{ ns: string, name: string, version: string, principal?: AccessPrincipal | null, requestId: string }} args */
async function handleDelete({ ns, name, version, principal, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();

  if (parseVersion(version) == null) {
    return jsonError(400, "invalid_version", `Version must be "v<int>", got ${JSON.stringify(version)}`);
  }

  const lockToken = await acquireDeleteLock(redis, ns, name, VERSION_DELETE_LOCK_KIND);
  if (!lockToken) {
    log("warn", "version_delete_rejected", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version,
      status: 409,
      reason: "deleting",
    });
    return jsonError(409, "deleting", `${ns}/${name} is being mutated by another delete request`);
  }

  try {
    let result;
    try {
      await assertWorkflowDeleteAllowed({
        ns, worker: name, version, allowCleanup: true, requestId,
      });
      if (!await renewDeleteLock(redis, ns, name, lockToken)) {
        throw new VersionDeleteError(409, "deleting", deleteLockExpiredDetails(ns, name, version));
      }
      result = await executeVersionDelete({
        redis, ns, name, version, principal, requestId, lockToken,
      });
    } catch (err) {
      if (err instanceof ControlAbort) {
        log(err.status >= 500 ? "error" : "warn", "version_delete_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          version,
          ...codedErrorLogFields(err),
        });
        return controlAbortResponse(err);
      }
      throw err;
    }

    const { queueHintStatus, warnings } = await recordCleanupIntentOrWarn({
      cleanupIntent: result.cleanupIntent,
      cleanupTaskId: result.cleanupTaskId,
      warningMessage: "Version content cleanup was not queued; retry deletion cleanup manually.",
      logEvent: "version_cleanup_queue_failed",
      logFields: { request_id: requestId, namespace: ns, worker: name, version },
      log,
    });

    log("info", "version_deleted", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version,
      cleanup_task_id: result.cleanupTaskId || null,
      skipped_shared_prefix: result.skippedSharedPrefix,
      last_retained_no_active: result.lastRetainedNoActive,
    });

    const payload = {
      namespace: ns, name, version, deleted: true,
      assets: {
        skippedSharedPrefix: result.skippedSharedPrefix,
        queueHint: queueHintStatus,
        warnings,
      },
    };
    return jsonResponse(200, payload);
  } finally {
    await releaseDeleteLock(redis, ns, name, lockToken, requestId);
  }
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, version: string, principal?: AccessPrincipal | null, requestId: string, lockToken: string }} args
 */
async function executeVersionDelete({ redis, ns, name, version, principal, requestId, lockToken }) {
  return await runOptimistic(redis, {
    attempts: MAX_DELETE_ATTEMPTS,
    onExhausted: () => {
      throw new VersionDeleteError(503, "version_delete_contention", {
        namespace: ns, name, version,
        message: `exhausted ${MAX_DELETE_ATTEMPTS} retries`,
      });
    },
  }, async (iso) => {
    await iso.watch(
      deleteLockKey(ns, name),
      routesKey(ns),
      workerVersionsKey(ns, name),
      workerSecretsKey(ns, name),
      workflowDefsKey(ns, name),
      bundleKey(ns, name, version),
      referrersKey(ns, name, version),
    );

    if (await iso.get(deleteLockKey(ns, name)) !== lockToken) {
      await iso.unwatch();
      throw new VersionDeleteError(409, "deleting", deleteLockExpiredDetails(ns, name, version));
    }

    const currentActive = await iso.hGet(routesKey(ns), name);
    if (currentActive === version) {
      throw new VersionDeleteError(409, "active_version", {
        namespace: ns, name, version,
      });
    }

    const currentVersions = await iso.zRange(workerVersionsKey(ns, name), 0, -1);
    if (!currentVersions.includes(version)) {
      throw new VersionDeleteError(404, "version_not_found", {
        namespace: ns, name, version,
      });
    }
    if (currentActive && !currentVersions.includes(currentActive)) {
      throw new VersionDeleteError(409, "projection_drift", {
        namespace: ns, name,
        active_version: currentActive,
        reason: "active_not_in_worker_versions",
      });
    }

    const bundleMetaRaw = await iso.hGet(bundleKey(ns, name, version), "__meta__");
    const bundleMeta = parseBundleMeta(bundleMetaRaw, {
      ns,
      worker: name,
      version,
      makeError: ({ reason }) => new VersionDeleteError(500, "corrupt_meta", {
        namespace: ns, name, version,
        stage: "target_meta_parse",
        detail: reason,
      }),
    });

    const referrerMembers = await iso.sMembers(referrersKey(ns, name, version));
    if (referrerMembers.length > 0) {
      throw new VersionDeleteError(409, "version_referenced", {
        namespace: ns, name, version,
        ...formatReferrerBlocker(referrerMembers, { targetNs: ns, principal }),
      });
    }

    const outgoingRefs = extractOutgoingRefs(bundleMeta.bindings, ns);
    const d1Refs = extractD1Refs(bundleMeta.bindings);
    const prefix = bundleAssetPrefix(bundleMeta);

    // Secret-bump COPY can make siblings share the same assets prefix;
    // skip the cleanup task if any other retained version still points
    // at it. Corrupt sibling meta fail-closes rather than risk deleting
    // content that an unreadable sibling still references.
    let skipS3Cleanup = false;
    if (prefix) {
      for (const otherV of currentVersions) {
        if (otherV === version) continue;
        const otherRaw = await iso.hGet(bundleKey(ns, name, otherV), "__meta__");
        const otherMeta = parseBundleMeta(otherRaw, {
          ns,
          worker: name,
          version: otherV,
          makeError: ({ reason }) => new VersionDeleteError(500, "corrupt_meta", {
            namespace: ns, name, version: otherV,
            stage: "sibling_meta_parse",
            detail: reason,
          }),
        });
        if (bundleAssetPrefix(otherMeta) === prefix) {
          skipS3Cleanup = true;
          break;
        }
      }
    }

    const hasWorkerSecrets = (await iso.exists(workerSecretsKey(ns, name))) > 0;
    const hasWorkflowDefs = (await iso.exists(workflowDefsKey(ns, name))) > 0;
    const lastRetainedNoActive =
      currentVersions.length === 1 &&
      currentVersions[0] === version &&
      !currentActive;

    const willCreateCleanup = Boolean(prefix) && !skipS3Cleanup;
    const cleanupTaskId = willCreateCleanup ? buildS3CleanupTaskId() : null;
    const cleanupPrefix = /** @type {string} */ (prefix);

    const multi = iso.multi();
    multi.del(bundleKey(ns, name, version));
    stageWorkerVersionIndexRemove(multi, ns, name, version);
    multi.del(referrersKey(ns, name, version));
    stageOutgoingReferrerRemovals(multi, { ns, worker: name, version, refs: outgoingRefs });
    stageD1ReferrerRemovals(multi, {
      ns, worker: name, version, refs: d1Refs,
      databaseIdFor: (ref) => /** @type {string} */ (ref.databaseId),
    });
    if (lastRetainedNoActive) {
      // Single-version DELETE leaves non-version lifecycle state for
      // whole-delete. Keep those workers discoverable through workers:<ns>.
      stageWorkerVersionIndexDelete(multi, ns, name);
      if (!hasWorkerSecrets && !hasWorkflowDefs) {
        stageWorkerHidden(multi, ns, name);
      }
    }

    await multi.exec();
    // Queue streams live on the data Redis DB. Commit the control-plane
    // delete first, then enqueue the best-effort cleanup intent.
    return {
      cleanupTaskId,
      cleanupIntent: willCreateCleanup ? {
        taskId: /** @type {string} */ (cleanupTaskId),
        prefixes: [cleanupPrefix],
        source: { kind: "delete-version", ns, name, version, requestId },
      } : null,
      skippedSharedPrefix: Boolean(prefix) && skipS3Cleanup,
      lastRetainedNoActive,
    };
  });
}
