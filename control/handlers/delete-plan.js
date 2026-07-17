import {
  referrersKey,
  workflowDefsKey,
} from "control-lib";
import {
  cronWorkerKey,
  stageCronWorkerRemoved,
  stageD1ReferrerRemovals,
  stageOutgoingReferrerRemovals,
  stageQueueConsumerKeyRemoval,
  stageWorkerHidden,
  stageWorkerVersionIndexRemove,
} from "control-lifecycle-indexes";
import {
  NAMESPACES_KEY,
  bundleKey,
  doStorageIdKey,
  nsHostsKey,
  patternsKey,
  routesKey,
} from "shared-worker-contract";
import { workerSecretsKey } from "shared-secret-keys";

/**
 * @typedef {{ host: string, slot: string, [key: string]: unknown }} RouteSlot
 * @typedef {{ binding: string, databaseId: string }} D1Ref
 * @typedef {{ targetNs: string, targetWorker: string, targetVersion: string, binding: string }} OutgoingRef
 * @typedef {{
 *   ns: string,
 *   name: string,
 *   queueConsumerKeys: string[],
 *   doStorageId: string | null,
 *   doOwnerKeys: string[],
 *   retainedVersions: string[],
 *   prefixByVersion: Record<string, string>,
 *   d1RefsByVersion: Record<string, D1Ref[]>,
 *   outgoingRefsByVersion: Record<string, OutgoingRef[]>,
 *   activeVersion: string | null,
 *   activeRoutes: RouteSlot[],
 *   affectedHosts: string[],
 *   hostsLosingNsOwnership: string[],
 *   namespaceStillActive: boolean,
 *   hasWorkerSecrets: boolean,
 *   hasWorkflowDefs: boolean,
 * }} DeleteInputs
 * @typedef {import("control-lifecycle-indexes").RedisMulti} RedisMulti
 * @typedef {{ routes: string, routesFlush: string, patterns: string }} DeleteChannels
 * @typedef {{ taskId: string, prefixes: string[], source: Record<string, unknown> }} CleanupIntent
 */

/**
 * @param {DeleteInputs} collected
 * @param {string} requestId
 * @param {() => string} makeCleanupTaskId
 * @returns {{ cleanupTaskId: string | null, cleanupIntent: CleanupIntent | null, dedupedPrefixes: string[] }}
 */
export function buildWorkerDeleteCleanup(collected, requestId, makeCleanupTaskId) {
  // One cleanup task per delete operation (not per version) — dedup so
  // shared assets prefixes from secret-bump COPY don't fan out.
  const dedupedPrefixes = [
    ...new Set(
      collected.retainedVersions
        .map((v) => collected.prefixByVersion[v])
        .filter(Boolean)
    ),
  ];
  const cleanupTaskId = dedupedPrefixes.length > 0 ? makeCleanupTaskId() : null;
  return {
    cleanupTaskId,
    cleanupIntent: cleanupTaskId ? {
      taskId: cleanupTaskId,
      prefixes: dedupedPrefixes,
      source: {
        kind: "delete-worker",
        ns: collected.ns,
        worker: collected.name,
        versions: collected.retainedVersions,
        requestId,
      },
    } : null,
    dedupedPrefixes,
  };
}

/**
 * @param {RedisMulti} multi
 * @param {{ collected: DeleteInputs, channels: DeleteChannels }} args
 */
export function stageWorkerDelete(multi, { collected, channels }) {
  const { ns, name } = collected;
  for (const r of collected.activeRoutes) {
    multi.hDel(patternsKey(r.host), r.slot);
  }
  if (collected.activeVersion) {
    multi.hDel(routesKey(ns), name);
  }
  if (collected.hostsLosingNsOwnership.length) {
    multi.sRem(nsHostsKey(ns), collected.hostsLosingNsOwnership);
  }
  if (!collected.namespaceStillActive) {
    multi.sRem(NAMESPACES_KEY, ns);
  }

  multi.del(cronWorkerKey(ns, name));
  stageCronWorkerRemoved(multi, ns, name);
  for (const qKey of collected.queueConsumerKeys) {
    stageQueueConsumerKeyRemoval(multi, qKey);
  }
  if (collected.hasWorkerSecrets) {
    multi.del(workerSecretsKey(ns, name));
  }
  multi.del(workflowDefsKey(ns, name));
  if (collected.doOwnerKeys.length) {
    multi.del(...collected.doOwnerKeys);
  }
  if (collected.doStorageId) {
    multi.del(doStorageIdKey(ns, name));
  }
  for (const ver of collected.retainedVersions) {
    multi.del(bundleKey(ns, name, ver));
    stageWorkerVersionIndexRemove(multi, ns, name, ver);
    multi.del(referrersKey(ns, name, ver));
    stageOutgoingReferrerRemovals(multi, {
      ns, worker: name, version: ver, refs: collected.outgoingRefsByVersion[ver] || [],
    });
    stageD1ReferrerRemovals(multi, {
      ns, worker: name, version: ver, refs: collected.d1RefsByVersion[ver] || [],
      databaseIdFor: (ref) => /** @type {D1Ref} */ (ref).databaseId,
    });
  }

  // `worker:<ns>:<name>:next_version` is deliberately NOT DEL'd —
  // workerLoader has no eviction, so a reused <ns:name:version> id
  // could hit a cached isolate from the pre-delete generation.
  stageWorkerHidden(multi, ns, name);

  if (collected.namespaceStillActive) {
    multi.publish(channels.routes, ns);
  } else {
    multi.publish(channels.routesFlush, "");
  }
  for (const h of collected.affectedHosts) {
    multi.publish(channels.patterns, h);
  }
}
