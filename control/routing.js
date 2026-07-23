// Every multi-key mutation runs in a fresh RedisSession so WATCH state
// stays connection-local; a nil EXEC raises WatchError and runOptimistic()
// retries with fresh reads.

import { runOptimistic } from "control-shared";
import {
  d1DatabaseKey,
  extractD1Refs, extractOutgoingRefs,
  parseBundleMeta,
  parsePatternProjection,
} from "control-lib";
import {
  cronWorkerKey,
  stageCronProjection,
  stageCronProjectionMeta,
  stageCronSequence,
  stageD1ReferrerAdds,
  stageOutgoingReferrerAdds,
  stageQueueConsumerProjection,
  stageQueueConsumerRemoval,
  stageWorkerVersionIndexUpsert,
} from "control-lifecycle-indexes";
import { parseHostList } from "control-topology";
import { encodePatternProjection } from "shared-route-projection";
import {
  CRON_GENERATION_EPOCH,
  DECLARED_HOSTS_KEY,
  DECLARED_HOSTS_REVISION_KEY,
  NAMESPACES_KEY,
  PATTERNS_CHANNEL,
  ROUTES_CHANNEL,
  bundleKey,
  cronSequenceKey,
  deleteLockKey,
  formatVersion,
  hostDeclarationsKey,
  hostsKey,
  nextVersionKey,
  nsHostsKey,
  parseVersion,
  patternsKey,
  routesKey,
} from "shared-worker-contract";
import { errorMessage } from "shared-errors";
import { diffCrons, nextFireMs, slotMsFor } from "control-cron-index";
import { isReservedNs, ROUTES_ALLOWED_RESERVED_NS } from "shared-ns-pattern";
import { PLATFORM_TIER_RESERVED_NS } from "shared-auth-roles";
import { queueConsumerKey } from "shared-queue-keys";
import { WatchError } from "shared-redis";
import {
  computeAffectedHosts,
  computeNsHostDeltas,
  computeRouteKeySet,
  routeKey,
  stringHash,
} from "control-routing-route-plan";

const MAX_ATTEMPTS = 5;
const HOST_RECONCILE_READ_BATCH_SIZE = 64;
const DEPENDENCY_READ_BATCH_SIZE = 64;

/**
 * @typedef {import("shared-route-projection").PatternProjection} PatternProjection
 * @typedef {Pick<PatternProjection, "kind" | "value"> & { host: string, slot: string }} RoutePattern
 * @typedef {{ cron: string, timezone: string }} CronSpec
 * @typedef {{ queue: string, maxBatchSize: number, maxBatchTimeoutMs: number, maxRetries: number, retryDelaySeconds?: number, deadLetterQueue?: string }} QueueConsumer
 * @typedef {{ binding: string, databaseId: string }} D1Ref
 * @typedef {{ targetNs: string, targetWorker: string, targetVersion: string, binding: string }} OutgoingRef
 * @typedef {{ as?: string }} ExportSpec
 * @typedef {{ version?: string | null, seq?: unknown }} CronMeta
 * @typedef {{ cronSeq: number, persistSequence: boolean, addedWithPlacement: Array<CronSpec & { id: string, gen: number, slot: number }>, removed: Array<{ id: string, gen: string | number }> }} CronPlan
 * @typedef {{ newQueueConsumers: QueueConsumer[], removedQueueConsumers: QueueConsumer[] }} QueuePlan
 * @typedef {{ newRoutes: RoutePattern[], newCrons: CronSpec[], newQueueConsumers: QueueConsumer[], newExports: ExportSpec[], d1Refs: D1Ref[], outgoingRefs: OutgoingRef[] }} PromoteBundleInputs
 * @typedef {{ oldRoutes: RoutePattern[], oldQueueConsumers: QueueConsumer[], affectedHosts: Set<string>, hostState: HostState }} PromoteObservedState
 * @typedef {{ newRouteKeys: Set<string>, nsHostsAdd: string[], nsHostsRem: string[], cronKey: string, cronHash: Record<string, string>, cronPlan: CronPlan, queuePlan: QueuePlan }} PromoteStagePlan
 * @typedef {{ log?: (level: string, event: string, fields: Record<string, unknown>) => void, requestId?: string, ns?: string, workerName?: string }} LogContext
 * @typedef {{ iso: RedisIso, multi: RedisMulti, currentVersion: string, newVersion: string, sourceMeta: BundleMeta }} BumpStageContext
 * @typedef {LogContext & { stageBeforeCopy?: (context: BumpStageContext) => void | Promise<void> }} BumpOptions
 * @typedef {{ routes?: RoutePattern[], crons?: CronSpec[], queueConsumers?: QueueConsumer[], exports?: ExportSpec[], bindings?: unknown }} BundleMeta
 * @typedef {Record<string, Record<string, string | null | undefined>>} HostState
 * @typedef {import("shared-redis").RedisMulti} RedisMulti
 * @typedef {{ watch: (...keys: string[]) => Promise<unknown>, unwatch: () => Promise<unknown>, hGet: (key: string, field: string) => Promise<string | null | undefined>, hGetMany: (pairs: Array<[string, string]>) => Promise<Array<string | null | undefined>>, hGetAll: (key: string) => Promise<Record<string, string | null | undefined>>, hGetAllMany: (keys: string[]) => Promise<Array<Record<string, string | null | undefined>>>, hGetAllAndGet: (hashKey: string, stringKey: string) => Promise<{ hash: Record<string, string | null | undefined>, value: string | null | undefined }>, hStrLenMany: (pairs: Array<[string, string]>) => Promise<number[]>, get: (key: string) => Promise<string | null | undefined>, exists: (...keys: string[]) => Promise<number>, existsMany: (keys: string[]) => Promise<boolean[]>, sMIsMember: (key: string, ...members: string[]) => Promise<boolean[]>, sMembers: (key: string) => Promise<string[]>, sMembersMany: (keys: string[]) => Promise<string[][]>, zRange: (key: string, start: number, stop: number) => Promise<string[]>, copy: (src: string, dst: string, options?: Record<string, unknown>) => Promise<number>, multi: () => RedisMulti }} RedisIso
 * @typedef {{ hGet: (key: string, field: string) => Promise<string | null | undefined>, incr: (key: string) => Promise<number>, session: <T>(fn: (iso: RedisIso) => Promise<T>) => Promise<T> }} RedisClient
 */

// Routing owns promotion/route-specific machine codes. Keep this separate from
// ControlAbort so pure routing helpers do not inherit handler abort semantics.
export class RoutingError extends Error {
  /** @param {number} status @param {string} code @param {string} message @param {Record<string, unknown>} [details] */
  constructor(status, code, message, details = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

/** @param {unknown} raw @param {string} host @param {string} slot */
function requirePatternProjection(raw, host, slot) {
  return parsePatternProjection(raw, {
    host,
    slot,
    makeError: (details) => new RoutingError(
      500,
      "corrupt_pattern_projection",
      `Pattern projection for ${host}${slot} is corrupt`,
      details
    ),
  });
}

/** @param {Array<string | null | undefined | false>} keys @returns {string[]} */
function uniqueKeys(keys) {
  const unique = new Set();
  for (const key of keys) {
    if (typeof key === "string" && key.length > 0) unique.add(key);
  }
  return [...unique];
}

/** @param {RedisIso} iso @param {Array<string | null | undefined | false>} keys */
async function watchKeys(iso, keys) {
  const unique = uniqueKeys(keys);
  if (unique.length) await iso.watch(...unique);
}

/** @param {LogContext} options @param {string} field */
function warnMalformedCronProjection(options, field) {
  if (typeof options?.log !== "function") return;
  options.log("warn", "cron_projection_malformed", {
    ...(options.requestId ? { request_id: options.requestId } : {}),
    namespace: options.ns,
    worker: options.workerName,
    field,
  });
}

/** @param {CronMeta} meta @param {LogContext} logContext */
function cronSequenceSeedFromProjection(meta, logContext) {
  if (!Object.hasOwn(meta, "seq")) return null;
  const value = meta.seq;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    warnMalformedCronProjection(logContext, "__meta__.seq");
    throw new RoutingError(
      500,
      "corrupt_cron_meta",
      `Corrupt cron sequence for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
    );
  }
  return value;
}

/** @param {LogContext} logContext @param {string} message @returns {never} */
function corruptCronSequence(logContext, message) {
  warnMalformedCronProjection(logContext, "cron_sequence");
  throw new RoutingError(500, "corrupt_cron_sequence", message);
}

/** @param {string | null | undefined} raw @param {LogContext} logContext */
function parseCronSequence(raw, logContext) {
  if (raw == null) return null;
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    corruptCronSequence(
      logContext,
      `Corrupt cron sequence for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
    );
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    corruptCronSequence(
      logContext,
      `Corrupt cron sequence for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
    );
  }
  return parsed;
}

/**
 * @param {CronMeta | null} cronMeta
 * @param {string | null | undefined} rawSequence
 * @param {LogContext} logContext
 */
function resolveCronSequence(cronMeta, rawSequence, logContext) {
  const persistedSequence = parseCronSequence(rawSequence, logContext);
  if (persistedSequence != null) {
    if (persistedSequence < CRON_GENERATION_EPOCH - 1) {
      corruptCronSequence(
        logContext,
        `Cron sequence regressed for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
      );
    }
    return { cronSeq: persistedSequence, persistSequence: false };
  }
  if (cronMeta == null) {
    return { cronSeq: CRON_GENERATION_EPOCH - 1, persistSequence: true };
  }
  const projectionSeed = cronSequenceSeedFromProjection(cronMeta, logContext);
  if (projectionSeed == null) {
    corruptCronSequence(
      logContext,
      `Missing cron sequence for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
    );
  }
  return {
    cronSeq: Math.max(projectionSeed, CRON_GENERATION_EPOCH - 1),
    persistSequence: true,
  };
}

/** @param {LogContext} logContext @returns {never} */
function corruptCronMeta(logContext) {
  warnMalformedCronProjection(logContext, "__meta__");
  throw new RoutingError(
    500,
    "corrupt_cron_meta",
    `Corrupt cron metadata for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
  );
}

/** @param {string} raw @param {LogContext} logContext */
function parseCronMeta(raw, logContext) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    corruptCronMeta(logContext);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    corruptCronMeta(logContext);
  }
  return /** @type {CronMeta} */ (parsed);
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {string | null | undefined} version */
async function readMeta(iso, ns, workerName, version) {
  if (!version) return null;
  const raw = await iso.hGet(bundleKey(ns, workerName, version), "__meta__");
  if (raw == null) await retryIfRouteChanged(iso, ns, workerName, version);
  return routingBundleMeta(ns, workerName, version, raw);
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {string} version */
async function retryIfRouteChanged(iso, ns, workerName, version) {
  // A watched route can move after its snapshot but before the dependent
  // metadata read. Retry that race; a stable route still fails as corruption.
  const currentVersion = await iso.hGet(routesKey(ns), workerName);
  if (currentVersion !== version) throw new WatchError();
}

/**
 * @param {string} ns
 * @param {string} workerName
 * @param {string} version
 * @param {unknown} raw
 */
function routingBundleMeta(ns, workerName, version, raw) {
  // Swallowing would hide old routes/consumers from the diff and leak them.
  return /** @type {BundleMeta} */ (parseBundleMeta(raw, {
    ns,
    worker: workerName,
    version,
    makeError: ({ message }) => new RoutingError(500, "corrupt_meta", message),
  }));
}

// Callers stage their own prelude (HDEL removed slots, cron diff,
// queue-consumer updates, ns-hosts deltas) into the same `multi`.
// Embedding version in each slot value lets gateway build workerId
// straight from a pattern hit.
/** @param {RedisMulti} multi @param {string} ns @param {string} workerName @param {string} newVersion @param {RoutePattern[]} routes @param {Set<string>} affectedHosts */
function stageVersionFlip(multi, ns, workerName, newVersion, routes, affectedHosts) {
  for (const r of routes) {
    const value = encodePatternProjection({
      ns, worker: workerName, version: newVersion, kind: r.kind, value: r.value,
    });
    multi.hSet(patternsKey(r.host), r.slot, value);
  }
  multi.hSet(routesKey(ns), workerName, newVersion);
  for (const h of affectedHosts) multi.publish(PATTERNS_CHANNEL, h);
  multi.publish(ROUTES_CHANNEL, ns);
}

/** @param {Record<string, string>} cronHash @param {string | null | undefined} rawSequence @param {CronSpec[]} newCrons @param {number} now @param {LogContext} logContext */
function computeCronPlan(cronHash, rawSequence, newCrons, now, logContext) {
  /** @type {CronMeta | null} */
  let cronMeta = null;
  /** @type {Record<string, import("./cron-index.js").ExistingCronEntry>} */
  const oldCronById = {};
  for (const [k, v] of Object.entries(cronHash)) {
    if (k === "__meta__") {
      cronMeta = parseCronMeta(v, logContext);
    } else {
      try { oldCronById[k] = JSON.parse(v); } catch { warnMalformedCronProjection(logContext, k); }
    }
  }
  const cronDiff = diffCrons(oldCronById, newCrons);
  const hasExistingCronState = Object.keys(cronHash).length > 0;
  const hasCronState = hasExistingCronState || newCrons.length > 0;
  if (!hasCronState) {
    if (rawSequence != null) {
      const persistedSequence = parseCronSequence(rawSequence, logContext);
      if (persistedSequence == null || persistedSequence < CRON_GENERATION_EPOCH - 1) {
        corruptCronSequence(
          logContext,
          `Cron sequence regressed for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
        );
      }
    }
    return { cronSeq: 0, persistSequence: false, addedWithPlacement: [], removed: [] };
  }
  if (hasExistingCronState && cronMeta == null) corruptCronMeta(logContext);
  const sequence = resolveCronSequence(cronMeta, rawSequence, logContext);
  let cronSeq = sequence.cronSeq;
  const addedWithPlacement = [];
  for (const entry of cronDiff.added) {
    if (cronSeq >= Number.MAX_SAFE_INTEGER) {
      throw new RoutingError(
        500,
        "cron_sequence_exhausted",
        `Cron sequence exhausted for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
      );
    }
    cronSeq += 1;
    addedWithPlacement.push({
      ...entry,
      gen: cronSeq,
      slot: slotMsFor(nextFireMs(entry.cron, entry.timezone, now)),
    });
  }
  return {
    cronSeq,
    persistSequence: sequence.persistSequence || addedWithPlacement.length > 0,
    addedWithPlacement,
    removed: cronDiff.removed,
  };
}

/**
 * @param {RedisMulti} multi
 * @param {string} ns
 * @param {string[]} toAdd
 * @param {Array<{ host: string, declaredNs: string[] }>} removals
 */
function stageDeclaredHostChanges(multi, ns, toAdd, removals) {
  const changedHosts = new Set([...toAdd, ...removals.map((entry) => entry.host)]);
  for (const host of toAdd) {
    multi.sAdd(hostsKey(ns), host);
    multi.sAdd(hostDeclarationsKey(host), ns);
    multi.sAdd(DECLARED_HOSTS_KEY, host);
  }
  for (const { host, declaredNs } of removals) {
    multi.sRem(hostsKey(ns), host);
    multi.sRem(hostDeclarationsKey(host), ns);
    if (declaredNs.filter((declared) => declared !== ns).length === 0) {
      multi.del(hostDeclarationsKey(host));
      multi.sRem(DECLARED_HOSTS_KEY, host);
    }
  }
  for (const host of changedHosts) multi.publish(PATTERNS_CHANNEL, host);
  if (changedHosts.size > 0) multi.incr(DECLARED_HOSTS_REVISION_KEY);
}

/** @param {QueueConsumer[]} oldQueueConsumers @param {QueueConsumer[]} newQueueConsumers */
function computeQueueConsumerPlan(oldQueueConsumers, newQueueConsumers) {
  const newQueueSet = new Set(newQueueConsumers.map((c) => c.queue));
  return {
    newQueueConsumers,
    removedQueueConsumers: oldQueueConsumers.filter((c) => !newQueueSet.has(c.queue)),
  };
}

/** @param {RedisMulti} multi @param {string} ns @param {string} workerName @param {string} newVersion @param {QueuePlan} queuePlan */
function stageQueueConsumerPlan(multi, ns, workerName, newVersion, queuePlan) {
  // Queue consumers: replace every declared projection so optional
  // fields removed from the manifest disappear on the next promote.
  // Scheduler reconciles on POLL_MS so no invalidate channel needed.
  for (const c of queuePlan.newQueueConsumers) {
    stageQueueConsumerProjection(multi, ns, workerName, newVersion, c);
  }
  for (const c of queuePlan.removedQueueConsumers) {
    stageQueueConsumerRemoval(multi, ns, c.queue);
  }
}

/** @param {RedisIso} iso @param {string} ns @param {string} targetLabel @param {D1Ref[]} d1Refs */
async function assertD1DependenciesPresent(iso, ns, targetLabel, d1Refs) {
  const keys = d1Refs.map((ref) => d1DatabaseKey(ns, ref.databaseId));
  await watchKeys(iso, keys);
  if (keys.length === 0 || await iso.exists(...keys) === keys.length) return;
  const exists = await iso.existsMany(keys);
  const missingIndex = exists.findIndex((present) => !present);
  if (missingIndex !== -1) {
    const ref = d1Refs[missingIndex];
    throw new RoutingError(
      409,
      "d1_database_dependency_missing",
      `Cannot ${targetLabel}: D1 binding "${ref.binding}" ` +
        `references deleted database "${ref.databaseId}"`,
      { broken_d1_dependency: ref }
    );
  }
}

/** @param {RedisIso} iso @param {string} targetLabel @param {OutgoingRef[]} outgoingRefs */
async function assertOutgoingDependenciesPresent(iso, targetLabel, outgoingRefs) {
  /** @type {Map<string, OutgoingRef>} */
  const firstRefByKey = new Map();
  for (const ref of outgoingRefs) {
    const key = bundleKey(ref.targetNs, ref.targetWorker, ref.targetVersion);
    if (!firstRefByKey.has(key)) firstRefByKey.set(key, ref);
  }
  const keys = [...firstRefByKey.keys()];
  await watchKeys(iso, keys);
  for (let offset = 0; offset < keys.length; offset += DEPENDENCY_READ_BATCH_SIZE) {
    const batch = keys.slice(offset, offset + DEPENDENCY_READ_BATCH_SIZE);
    const lengths = await iso.hStrLenMany(batch.map((key) => [key, "__meta__"]));
    const missingIndex = lengths.findIndex((length) => length === 0);
    if (missingIndex !== -1) {
      const ref = /** @type {OutgoingRef} */ (firstRefByKey.get(batch[missingIndex]));
      throw new RoutingError(
        409,
        "service_binding_dependency_missing",
        `Cannot ${targetLabel}: binding "${ref.binding}" ` +
          `depends on ${ref.targetNs}/${ref.targetWorker}/${ref.targetVersion} ` +
          `which is no longer present`,
        { broken_dependency: ref }
      );
    }
  }
}

/**
 * @param {RedisIso} iso
 * @param {string} ns
 * @param {string} workerName
 * @param {QueueConsumer[]} oldQueueConsumers
 * @param {QueueConsumer[]} newQueueConsumers
 */
async function assertQueueConsumerOwnership(
  iso,
  ns,
  workerName,
  oldQueueConsumers,
  newQueueConsumers
) {
  const queueUnion = new Set(oldQueueConsumers.map((c) => c.queue))
    .union(new Set(newQueueConsumers.map((c) => c.queue)));
  await watchKeys(iso, [...queueUnion].map((q) => queueConsumerKey(ns, q)));
  const workerFields = newQueueConsumers.length > 0
    ? await iso.hGetMany(newQueueConsumers.map((consumer) => [
      queueConsumerKey(ns, consumer.queue),
      "worker",
    ]))
    : [];
  for (const [index, consumer] of newQueueConsumers.entries()) {
    const heldWorker = workerFields[index];
    if (heldWorker && heldWorker !== workerName) {
      throw new RoutingError(
        409,
        "queue_consumer_conflict",
        `Queue "${consumer.queue}" is already consumed by ${heldWorker}`,
        { queue: consumer.queue }
      );
    }
  }
}

/** @param {RedisIso} iso @param {string} ns @param {RoutePattern[]} newRoutes */
async function assertDeclaredHosts(iso, ns, newRoutes) {
  const newHosts = [...new Set(newRoutes.map((r) => r.host))];
  if (!newHosts.length) return;
  const memberFlags = await iso.sMIsMember(hostsKey(ns), ...newHosts);
  const missingIdx = memberFlags.findIndex((flag) => !flag);
  if (missingIdx !== -1) {
    const host = newHosts[missingIdx];
    throw new RoutingError(
      403,
      "host_not_declared",
      `Host "${host}" is not declared for namespace "${ns}". ` +
        `POST /ns/${ns}/hosts first.`,
      { host }
    );
  }
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {RoutePattern[]} newRoutes @param {Set<string>} affectedHosts */
async function readHostStateAndAssertRouteConflicts(iso, ns, workerName, newRoutes, affectedHosts) {
  /** @type {HostState} */
  const hostState = {};
  /** @type {Map<string, { foreign: { slot: string, held: PatternProjection } | null, slots: Map<string, PatternProjection> }>} */
  const analysisByHost = new Map();
  const hosts = [...affectedHosts];
  const states = hosts.length > 0
    ? await iso.hGetAllMany(hosts.map((host) => patternsKey(host)))
    : [];
  for (let index = 0; index < hosts.length; index += 1) {
    const h = hosts[index];
    const state = states[index];
    hostState[h] = state;
    /** @type {{ slot: string, held: PatternProjection } | null} */
    let foreign = null;
    /** @type {Map<string, PatternProjection>} */
    const slots = new Map();
    for (const [slot, raw] of Object.entries(state)) {
      const held = requirePatternProjection(raw, h, slot);
      slots.set(slot, held);
      if (!foreign && held.ns && held.ns !== ns) foreign = { slot, held };
    }
    analysisByHost.set(h, { foreign, slots });
  }
  for (const r of newRoutes) {
    const analysis = analysisByHost.get(r.host);
    if (analysis?.foreign) {
      throw new RoutingError(
        409,
        "route_conflict",
        `Host ${r.host} is already owned by another namespace`,
        { host: r.host, slot: analysis.foreign.slot }
      );
    }
    const held = analysis?.slots.get(r.slot);
    if (!held) continue;
    if (held.ns !== ns || held.worker !== workerName) {
      throw new RoutingError(
        409,
        "route_conflict",
        `Route ${r.host}${r.slot} is already owned by ${held.ns}/${held.worker}`,
        { host: r.host, slot: r.slot }
      );
    }
  }
  return hostState;
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {ExportSpec[]} newExports */
async function assertPlatformAsAvailable(iso, ns, workerName, newExports) {
  if (!PLATFORM_TIER_RESERVED_NS.has(ns) || !newExports.some((e) => e.as)) return;
  // `as` is globally unique across every PLATFORM_TIER_RESERVED_NS member
  // (linker resolves on `as` alone). WATCH every member's routes so a
  // parallel promote in a different platform-tier ns bounces via WatchError.
  await watchKeys(
    iso,
    [...PLATFORM_TIER_RESERVED_NS]
      .filter((otherNs) => otherNs !== ns)
      .map((otherNs) => routesKey(otherNs))
  );
  for (const otherNs of PLATFORM_TIER_RESERVED_NS) {
    const activeRoutes = await iso.hGetAll(routesKey(otherNs));
    const routeEntries = Object.entries(activeRoutes).filter(([otherWorker, otherVersion]) => (
      !(otherNs === ns && otherWorker === workerName) &&
      typeof otherVersion === "string" &&
      otherVersion !== ""
    ));
    const rawMetas = await iso.hGetMany(routeEntries.map(([otherWorker, otherVersion]) => [
      bundleKey(otherNs, otherWorker, /** @type {string} */ (otherVersion)),
      "__meta__",
    ]));
    for (let i = 0; i < routeEntries.length; i += 1) {
      const [otherWorker, otherVersion] = routeEntries[i];
      const rawMeta = rawMetas[i];
      if (rawMeta == null) {
        await retryIfRouteChanged(
          iso,
          otherNs,
          otherWorker,
          /** @type {string} */ (otherVersion)
        );
      }
      const otherMeta = routingBundleMeta(
        otherNs,
        otherWorker,
        /** @type {string} */ (otherVersion),
        rawMeta
      );
      if (!Array.isArray(otherMeta.exports)) continue;
      const heldAs = new Set();
      for (const e of otherMeta.exports) if (e.as) heldAs.add(e.as);
      for (const e of newExports) {
        if (e.as && heldAs.has(e.as)) {
          throw new RoutingError(
            409,
            "platform_as_conflict",
            `platform "as" "${e.as}" is already claimed by ${otherNs}/${otherWorker}`,
            { as: e.as, heldBy: `${otherNs}/${otherWorker}` }
          );
        }
      }
    }
  }
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {string} newVersion */
async function readPromoteBundleInputs(iso, ns, workerName, newVersion) {
  const callerLock = await iso.get(deleteLockKey(ns, workerName));
  if (callerLock) {
    throw new RoutingError(
      409,
      "deleting",
      `${ns}/${workerName} is being deleted`
    );
  }

  // Every retry re-reads under WATCH: a concurrent single-version
  // delete (DEL bundle) surfaces here as a 404.
  const metaRaw = await iso.hGet(bundleKey(ns, workerName, newVersion), "__meta__");
  if (metaRaw == null) {
    throw new RoutingError(404, "version_not_found", `Version ${newVersion} not found for ${ns}/${workerName}`);
  }
  const meta = routingBundleMeta(ns, workerName, newVersion, metaRaw);

  const newRoutes = Array.isArray(meta.routes) ? meta.routes : [];
  const newCrons = Array.isArray(meta.crons) ? meta.crons : [];
  const newQueueConsumers = Array.isArray(meta.queueConsumers) ? meta.queueConsumers : [];
  const newExports = Array.isArray(meta.exports) ? meta.exports : [];

  // Reserved-ns allow-list: deploy-side gate is not the last line of
  // defense — bundles written out-of-band could skip it.
  if (isReservedNs(ns) && !ROUTES_ALLOWED_RESERVED_NS.has(ns) && newRoutes.length > 0) {
    throw new RoutingError(
      400,
      "reserved_namespace_routes",
      `Namespace "${ns}" is reserved and may not declare routes (JSRPC-only)`,
      { ns, routes: newRoutes.length }
    );
  }

  return {
    newRoutes,
    newCrons,
    newQueueConsumers,
    newExports,
    d1Refs: extractD1Refs(meta.bindings),
    outgoingRefs: extractOutgoingRefs(meta.bindings, ns),
  };
}

/**
 * @param {RedisIso} iso
 * @param {string} ns
 * @param {string} workerName
 * @param {string} newVersion
 * @param {PromoteBundleInputs} inputs
 */
async function readAndAssertPromoteState(iso, ns, workerName, newVersion, inputs) {
  await assertD1DependenciesPresent(iso, ns, `promote ${ns}/${workerName}/${newVersion}`, inputs.d1Refs);
  await assertOutgoingDependenciesPresent(iso, `promote ${ns}/${workerName}/${newVersion}`, inputs.outgoingRefs);

  const currentVersion = await iso.hGet(routesKey(ns), workerName);
  const currentMeta = await readMeta(iso, ns, workerName, currentVersion);
  const oldRoutes = currentMeta && Array.isArray(currentMeta.routes) ? currentMeta.routes : [];
  const oldQueueConsumers = currentMeta && Array.isArray(currentMeta.queueConsumers)
    ? currentMeta.queueConsumers
    : [];

  // Queue-consumer ownership check — symmetric with pattern-slot 409.
  await assertQueueConsumerOwnership(
    iso,
    ns,
    workerName,
    oldQueueConsumers,
    inputs.newQueueConsumers
  );

  const affectedHosts = computeAffectedHosts(oldRoutes, inputs.newRoutes);
  await watchKeys(iso, [...affectedHosts].map((h) => patternsKey(h)));
  await assertDeclaredHosts(iso, ns, inputs.newRoutes);
  const hostState = await readHostStateAndAssertRouteConflicts(
    iso,
    ns,
    workerName,
    inputs.newRoutes,
    affectedHosts
  );
  await assertPlatformAsAvailable(iso, ns, workerName, inputs.newExports);

  return { oldRoutes, oldQueueConsumers, affectedHosts, hostState };
}

/**
 * @param {RedisIso} iso
 * @param {string} ns
 * @param {string} workerName
 * @param {string} cronKey
 * @param {LogContext} logContext
 * @param {PromoteBundleInputs} inputs
 * @param {PromoteObservedState} observed
 */
async function buildPromoteStagePlan(iso, ns, workerName, cronKey, logContext, inputs, observed) {
  const newRouteKeys = computeRouteKeySet(inputs.newRoutes);
  const { nsHostsAdd, nsHostsRem } = computeNsHostDeltas(
    ns,
    observed.affectedHosts,
    observed.oldRoutes,
    inputs.newRoutes,
    newRouteKeys,
    observed.hostState
  );

  const cronSnapshot = await iso.hGetAllAndGet(cronKey, cronSequenceKey(ns, workerName));
  const cronHash = stringHash(cronSnapshot.hash);
  const cronPlan = computeCronPlan(
    cronHash,
    cronSnapshot.value,
    inputs.newCrons,
    Date.now(),
    logContext
  );
  const queuePlan = computeQueueConsumerPlan(
    observed.oldQueueConsumers,
    inputs.newQueueConsumers
  );

  return {
    newRouteKeys,
    nsHostsAdd,
    nsHostsRem,
    cronKey,
    cronHash,
    cronPlan,
    queuePlan,
  };
}

/**
 * @param {RedisMulti} multi
 * @param {string} ns
 * @param {string} workerName
 * @param {string} newVersion
 * @param {PromoteBundleInputs} inputs
 * @param {PromoteObservedState} observed
 * @param {PromoteStagePlan} plan
 */
function stagePromoteWithRoutes(multi, ns, workerName, newVersion, inputs, observed, plan) {
  for (const r of observed.oldRoutes) {
    if (!plan.newRouteKeys.has(routeKey(r))) {
      multi.hDel(patternsKey(r.host), r.slot);
    }
  }
  stageVersionFlip(multi, ns, workerName, newVersion, inputs.newRoutes, observed.affectedHosts);
  // In the same MULTI as the route flip — closes the half-state where
  // routes:<ns> is set but the gateway's knownNs gate still 404s.
  multi.sAdd(NAMESPACES_KEY, ns);
  if (plan.nsHostsAdd.length) multi.sAdd(nsHostsKey(ns), plan.nsHostsAdd);
  if (plan.nsHostsRem.length) multi.sRem(nsHostsKey(ns), plan.nsHostsRem);
  stageD1ReferrerAdds(multi, {
    ns, worker: workerName, version: newVersion, refs: inputs.d1Refs,
    databaseIdFor: (ref) => String(ref.databaseId),
  });
  stageCronProjection(multi, {
    ns,
    worker: workerName,
    version: newVersion,
    cronKey: plan.cronKey,
    existingHash: plan.cronHash,
    crons: inputs.newCrons,
    plan: plan.cronPlan,
  });
  stageQueueConsumerPlan(multi, ns, workerName, newVersion, plan.queuePlan);
}

// Meta is re-read inside the session under WATCH so a racing hard-delete
// can't flip `routes:<ns>` onto a bundle about to vanish.
// Throws RoutingError(400|403|404|409|503). Returns {version, affectedHosts}.
/** @param {RedisClient} redis @param {string} ns @param {string} workerName @param {string} newVersion @param {LogContext} [options] */
export async function promoteWithRoutes(redis, ns, workerName, newVersion, options = {}) {
  const logContext = { ...options, ns, workerName };
  const cronKey = cronWorkerKey(ns, workerName);

  return await runOptimistic(redis, {
    attempts: MAX_ATTEMPTS,
    onExhausted: () => {
      throw new RoutingError(
        503,
        "promote_contention",
        `promote contention exhausted after ${MAX_ATTEMPTS} attempts, retry later`,
        { attempts: MAX_ATTEMPTS }
      );
    },
  }, async (iso) => {
    await iso.watch(
      routesKey(ns),
      hostsKey(ns),
      cronKey,
      cronSequenceKey(ns, workerName),
      deleteLockKey(ns, workerName),
      bundleKey(ns, workerName, newVersion),
    );

    const inputs = await readPromoteBundleInputs(iso, ns, workerName, newVersion);
    const observed = await readAndAssertPromoteState(iso, ns, workerName, newVersion, inputs);
    const plan = await buildPromoteStagePlan(
      iso,
      ns,
      workerName,
      cronKey,
      logContext,
      inputs,
      observed
    );

    const multi = iso.multi();
    stagePromoteWithRoutes(multi, ns, workerName, newVersion, inputs, observed, plan);
    await multi.exec();
    return { version: newVersion, affectedHosts: [...observed.affectedHosts] };
  });
}

// COPY-then-promote in one WATCH loop so a concurrent promote between
// "read active" and "promote new" can't silently roll content back.
// Throws RoutingError(404) when no active version exists to copy;
// RoutingError(409, "caller_deleting") if a whole-delete is in flight.
/** @param {RedisClient} redis @param {string} ns @param {string} workerName @param {BumpOptions} [options] */
export async function bumpActiveAndPromote(redis, ns, workerName, options = {}) {
  const logContext = { ...options, ns, workerName };
  // Avoid burning a version number on the pre-deploy path.
  const preCheck = await redis.hGet(routesKey(ns), workerName);
  if (!preCheck) {
    throw new RoutingError(
      404,
      "active_version_not_found",
      `${ns}/${workerName} has no active version to bump from`
    );
  }

  const newNum = await redis.incr(nextVersionKey(ns, workerName));
  const newVersion = formatVersion(newNum);

  return await runOptimistic(redis, {
    attempts: MAX_ATTEMPTS,
    onExhausted: () => {
      throw new RoutingError(
        503,
        "bump_contention",
        `bump contention exhausted after ${MAX_ATTEMPTS} attempts, retry later`,
        { attempts: MAX_ATTEMPTS }
      );
    },
  }, async (iso) => {
    await iso.watch(
      routesKey(ns),
      hostsKey(ns),
      deleteLockKey(ns, workerName),
    );

    const callerLock = await iso.get(deleteLockKey(ns, workerName));
    if (callerLock) {
      throw new RoutingError(
        409,
        "caller_deleting",
        `${ns}/${workerName} is being deleted`
      );
    }

    const currentVersion = await iso.hGet(routesKey(ns), workerName);
    if (!currentVersion) {
      throw new RoutingError(
        404,
        "active_version_not_found",
        `${ns}/${workerName} has no active version to bump from`
      );
    }

    const srcKey = bundleKey(ns, workerName, currentVersion);
    const dstKey = bundleKey(ns, workerName, newVersion);
    await iso.watch(srcKey, dstKey);

    const srcMetaRaw = await iso.hGet(srcKey, "__meta__");
    if (srcMetaRaw == null) {
      await retryIfRouteChanged(iso, ns, workerName, currentVersion);
      throw new RoutingError(
        500,
        "bundle_copy_failed",
        `COPY ${srcKey} → ${dstKey} failed (source missing?)`
      );
    }
    const srcMeta = routingBundleMeta(ns, workerName, currentVersion, srcMetaRaw);

    const routes = srcMeta && Array.isArray(srcMeta.routes) ? srcMeta.routes : [];
    const queueConsumers = srcMeta && Array.isArray(srcMeta.queueConsumers) ? srcMeta.queueConsumers : [];
    const outgoingRefs = extractOutgoingRefs(srcMeta && srcMeta.bindings, ns);
    const d1Refs = extractD1Refs(srcMeta && srcMeta.bindings);
    await assertDeclaredHosts(iso, ns, routes);
    await assertOutgoingDependenciesPresent(iso, `bump ${ns}/${workerName}`, outgoingRefs);
    await assertD1DependenciesPresent(iso, ns, `bump ${ns}/${workerName}`, d1Refs);

    const affectedHosts = new Set();
    for (const r of routes) affectedHosts.add(r.host);
    await watchKeys(iso, [...affectedHosts].map((h) => patternsKey(h)));
    const hosts = [...affectedHosts];
    const patternStates = hosts.length > 0
      ? await iso.hGetAllMany(hosts.map((host) => patternsKey(host)))
      : [];
    for (let index = 0; index < hosts.length; index += 1) {
      for (const [slot, raw] of Object.entries(patternStates[index])) {
        requirePatternProjection(raw, hosts[index], slot);
      }
    }

    // Scheduler builds x-worker-id from cron __meta__.version.
    const cronKey = cronWorkerKey(ns, workerName);
    const cronSeqKey = cronSequenceKey(ns, workerName);
    await iso.watch(cronKey, cronSeqKey);
    const cronSnapshot = await iso.hGetAllAndGet(cronKey, cronSeqKey);
    const cronMetaRaw = cronSnapshot.hash.__meta__;
    let cronMetaParsed = null;
    let cronSequence = null;
    if (cronMetaRaw) {
      cronMetaParsed = parseCronMeta(cronMetaRaw, logContext);
      cronSequence = resolveCronSequence(cronMetaParsed, cronSnapshot.value, logContext);
    } else if (Object.keys(cronSnapshot.hash).length > 0) {
      corruptCronMeta(logContext);
    } else if (cronSnapshot.value != null) {
      const persistedSequence = parseCronSequence(cronSnapshot.value, logContext);
      if (persistedSequence == null || persistedSequence < CRON_GENERATION_EPOCH - 1) {
        corruptCronSequence(
          logContext,
          `Cron sequence regressed for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
        );
      }
    }

    await watchKeys(iso, queueConsumers.map((c) => queueConsumerKey(ns, c.queue)));

    const newVNum = parseVersion(newVersion);
    if (newVNum == null) {
      throw new RoutingError(500, "invalid_generated_version", `bumpActiveAndPromote: bad new version tag ${newVersion}`);
    }

    const multi = iso.multi();
    if (typeof options.stageBeforeCopy === "function") {
      await options.stageBeforeCopy({
        iso,
        multi,
        currentVersion,
        newVersion,
        sourceMeta: srcMeta,
      });
    }
    multi.copy(srcKey, dstKey, { REPLACE: true });
    stageVersionFlip(multi, ns, workerName, newVersion, routes, affectedHosts);
    // Idempotent — also heals namespaces drift (manual SREM, recovery scripts).
    multi.sAdd(NAMESPACES_KEY, ns);

    // Maintain the indexes so a later hard-delete can collect this
    // bumped version's referrers.
    stageWorkerVersionIndexUpsert(multi, ns, workerName, newVersion, newVNum);
    stageOutgoingReferrerAdds(multi, {
      ns, worker: workerName, version: newVersion, refs: outgoingRefs,
    });
    stageD1ReferrerAdds(multi, {
      ns, worker: workerName, version: newVersion, refs: d1Refs,
      databaseIdFor: (ref) => String(ref.databaseId),
    });

    if (cronMetaParsed && cronSequence) {
      if (cronSequence.persistSequence) {
        stageCronSequence(multi, ns, workerName, cronSequence.cronSeq);
      }
      stageCronProjectionMeta(multi, {
        ns,
        worker: workerName,
        version: newVersion,
        cronKey,
      });
    }
    for (const c of queueConsumers) {
      stageQueueConsumerProjection(multi, ns, workerName, newVersion, c);
    }

    await multi.exec();
    return {
      version: newVersion,
      previousVersion: currentVersion,
      affectedHosts: [...affectedHosts],
    };
  });
}

// Reconcile hosts:<ns> to match body.hosts. Removal of a host with live
// patterns owned by this ns is rejected (409).
/** @param {RedisClient} redis @param {string} ns @param {unknown} body @param {string} platformDomain */
export async function reconcileHosts(redis, ns, body, platformDomain) {
  if (!body || typeof body !== "object") {
    throw new RoutingError(400, "invalid_request", "Body must be an object { hosts: [...] }");
  }
  const hostsBody = /** @type {{ hosts?: unknown }} */ (body);
  // Guard against `{}` / `{hosts: null}` silently clearing the set.
  if (!Object.hasOwn(body, "hosts") || !Array.isArray(hostsBody.hosts)) {
    throw new RoutingError(400, "invalid_request", "Body must include 'hosts' (array of strings)");
  }
  let bodyNormalized;
  try {
    bodyNormalized = parseHostList(hostsBody.hosts, platformDomain);
  } catch (err) {
    const message = errorMessage(err);
    throw new RoutingError(400, "invalid_request", message);
  }
  const bodySet = new Set(bodyNormalized);

  return await runOptimistic(redis, {
    attempts: MAX_ATTEMPTS,
    onExhausted: () => {
      throw new RoutingError(
        503,
        "hosts_reconcile_contention",
        `hosts reconcile contention after ${MAX_ATTEMPTS} attempts, retry later`,
        { attempts: MAX_ATTEMPTS }
      );
    },
  }, async (iso) => {
    await iso.watch(hostsKey(ns));
    const current = await iso.sMembers(hostsKey(ns));
    const currentSet = new Set(current);
    const toAdd = [...bodySet.difference(currentSet)];
    const toRemove = [...currentSet.difference(bodySet)];
    const changedHosts = [...new Set([...toAdd, ...toRemove])];
    if (changedHosts.length === 0) {
      await iso.unwatch();
      return [...bodySet].toSorted();
    }

    await watchKeys(iso, toRemove.map((h) => patternsKey(h)));
    await watchKeys(iso, toRemove.map(hostDeclarationsKey));

    const removals = [];
    for (let offset = 0; offset < toRemove.length; offset += HOST_RECONCILE_READ_BATCH_SIZE) {
      const batch = toRemove.slice(offset, offset + HOST_RECONCILE_READ_BATCH_SIZE);

      // Reverse-index fast path; HGETALL only to enrich the 409 message.
      const reverseFlags = await iso.sMIsMember(nsHostsKey(ns), ...batch);
      const matchedHosts = batch.filter((_host, index) => reverseFlags[index]);
      const matchedPatterns = matchedHosts.length > 0
        ? await iso.hGetAllMany(matchedHosts.map(patternsKey))
        : [];
      for (let i = 0; i < matchedHosts.length; i++) {
        const h = matchedHosts[i];
        const entries = matchedPatterns[i];
        for (const [slot, raw] of Object.entries(entries)) {
          const parsed = requirePatternProjection(raw, h, slot);
          if (parsed.ns === ns) {
            throw new RoutingError(
              409,
              "host_in_use",
              `Cannot remove host "${h}": live pattern ${h}${slot} still references namespace "${ns}"`,
              { host: h, slot }
            );
          }
        }
      }

      const declarations = await iso.sMembersMany(batch.map(hostDeclarationsKey));
      for (let i = 0; i < batch.length; i++) {
        removals.push({ host: batch[i], declaredNs: declarations[i] });
      }
    }

    const multi = iso.multi();
    stageDeclaredHostChanges(multi, ns, toAdd, removals);
    await multi.exec();
    return [...bodySet].toSorted();
  });
}
