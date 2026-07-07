// Every multi-key mutation runs in a fresh RedisSession so WATCH state
// stays connection-local; a nil EXEC raises WatchError and runOptimistic()
// retries with fresh reads.

import {
  DECLARED_HOSTS_KEY,
  HOST_DECLARATIONS_PREFIX,
  runOptimistic,
} from "control-shared";
import {
  d1DatabaseKey,
  deleteLockKey,
  extractD1Refs, extractOutgoingRefs,
} from "control-lib";
import {
  cronWorkerKey,
  stageCronSlotRef,
  stageCronWorkerIndexed,
  stageCronWorkerRemoved,
  stageD1ReferrerAdds,
  stageOutgoingReferrerAdds,
  stageQueueConsumerProjection,
  stageQueueConsumerRemoval,
  stageWorkerVersionIndexUpsert,
} from "control-lifecycle-indexes";
import { parseHostList } from "control-topology";
import { decodePatternProjection, encodePatternProjection } from "shared-route-projection";
import { bundleKey, formatVersion, parseVersion, patternsKey, routesKey } from "shared-version";
import { errorMessage } from "shared-errors";
import { diffCrons, nextFireMs, slotMsFor } from "control-cron-index";
import { isReservedNs, ROUTES_ALLOWED_RESERVED_NS } from "shared-ns-pattern";
import { PLATFORM_TIER_RESERVED_NS } from "shared-auth-roles";
import { queueConsumerKey } from "shared-queue-keys";
import {
  computeAffectedHosts,
  computeNsHostDeltas,
  computeRouteKeySet,
  routeKey,
  stringHash,
} from "control-routing-route-plan";

const MAX_ATTEMPTS = 5;
const PATTERNS_CHANNEL = "patterns:invalidate";
const ROUTES_CHANNEL = "routes:invalidate";

/** @param {string} host */
function hostDeclarationsKey(host) {
  return `${HOST_DECLARATIONS_PREFIX}${host}`;
}

/**
 * @typedef {import("shared-route-projection").PatternProjection} PatternProjection
 * @typedef {Pick<PatternProjection, "kind" | "value"> & { host: string, slot: string }} RoutePattern
 * @typedef {{ cron: string, timezone: string }} CronSpec
 * @typedef {{ queue: string, maxBatchSize: number, maxBatchTimeoutMs: number, maxRetries: number, retryDelaySeconds?: number, deadLetterQueue?: string }} QueueConsumer
 * @typedef {{ binding: string, databaseId: string }} D1Ref
 * @typedef {{ targetNs: string, targetWorker: string, targetVersion: string, binding: string }} OutgoingRef
 * @typedef {{ as?: string }} ExportSpec
 * @typedef {{ version?: string | null, seq?: number }} CronMeta
 * @typedef {{ cronSeq: number, addedWithPlacement: Array<CronSpec & { id: string, gen: number, slot: number }>, removed: Array<{ id: string, gen: string | number }> }} CronPlan
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
 * @typedef {{ watch: (...keys: string[]) => Promise<unknown>, unwatch: () => Promise<unknown>, hGet: (key: string, field: string) => Promise<string | null | undefined>, hGetMany: (pairs: Array<[string, string]>) => Promise<Array<string | null | undefined>>, hGetAll: (key: string) => Promise<Record<string, string | null | undefined>>, get: (key: string) => Promise<string | null | undefined>, exists: (key: string) => Promise<number>, sMIsMember: (key: string, ...members: string[]) => Promise<boolean[]>, sMembers: (key: string) => Promise<string[]>, zRange: (key: string, start: number, stop: number) => Promise<string[]>, copy: (src: string, dst: string, options?: Record<string, unknown>) => Promise<number>, multi: () => RedisMulti }} RedisIso
 * @typedef {{ hGet: (key: string, field: string) => Promise<string | null | undefined>, incr: (key: string) => Promise<number>, session: <T>(fn: (iso: RedisIso) => Promise<T>) => Promise<T> }} RedisClient
 */

export class RoutingError extends Error {
  /** @param {number} status @param {string} code @param {string} message @param {Record<string, unknown>} [details] */
  constructor(status, code, message, details = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
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

/** @param {CronMeta | null | undefined} meta @param {LogContext} logContext */
function cronSeqFromMeta(meta, logContext) {
  const value = meta?.seq ?? 0;
  if (!Number.isInteger(value) || value < 0) {
    warnMalformedCronProjection(logContext, "__meta__.seq");
    throw new RoutingError(
      500,
      "corrupt_cron_meta",
      `Corrupt cron sequence for ${logContext.ns || "unknown"}/${logContext.workerName || "unknown"}`
    );
  }
  return value;
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
  if (!raw) return null;
  return parseBundleMeta(ns, workerName, version, raw);
}

/**
 * @param {string} ns
 * @param {string} workerName
 * @param {string} version
 * @param {string} raw
 */
function parseBundleMeta(ns, workerName, version, raw) {
  try {
    return /** @type {BundleMeta} */ (JSON.parse(raw));
  } catch {
    // Swallowing would hide old routes/consumers from the diff and leak them.
    throw new RoutingError(
      500,
      "corrupt_meta",
      `Corrupt __meta__ for ${ns}/${workerName}/${version}`
    );
  }
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {string | null | undefined} version */
async function readRoutesFromMeta(iso, ns, workerName, version) {
  const meta = await readMeta(iso, ns, workerName, version);
  return meta && Array.isArray(meta.routes) ? meta.routes : [];
}

/** @param {RedisIso} iso @param {string} ns @param {string} workerName @param {string | null | undefined} version */
async function readQueueConsumersFromMeta(iso, ns, workerName, version) {
  const meta = await readMeta(iso, ns, workerName, version);
  return meta && Array.isArray(meta.queueConsumers) ? meta.queueConsumers : [];
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

/** @param {Record<string, string>} cronHash @param {CronSpec[]} newCrons @param {number} now @param {LogContext} logContext */
function computeCronPlan(cronHash, newCrons, now, logContext) {
  /** @type {CronMeta} */
  let cronMeta = { version: null, seq: 0 };
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
  let cronSeq = cronSeqFromMeta(cronMeta, logContext);
  const addedWithPlacement = cronDiff.added.map((e) => ({
    ...e,
    gen: ++cronSeq,
    slot: slotMsFor(nextFireMs(e.cron, e.timezone, now)),
  }));
  return { cronSeq, addedWithPlacement, removed: cronDiff.removed };
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
    multi.sAdd(`hosts:${ns}`, host);
    multi.sAdd(hostDeclarationsKey(host), ns);
    multi.sAdd(DECLARED_HOSTS_KEY, host);
  }
  for (const { host, declaredNs } of removals) {
    multi.sRem(`hosts:${ns}`, host);
    multi.sRem(hostDeclarationsKey(host), ns);
    if (declaredNs.filter((declared) => declared !== ns).length === 0) {
      multi.del(hostDeclarationsKey(host));
      multi.sRem(DECLARED_HOSTS_KEY, host);
    }
  }
  for (const host of changedHosts) multi.publish(PATTERNS_CHANNEL, host);
}

/** @param {RedisMulti} multi @param {string} ns @param {string} workerName @param {string} newVersion @param {string} cronKey @param {Record<string, string>} cronHash @param {CronSpec[]} newCrons @param {CronPlan} cronPlan */
function stageCronPlan(multi, ns, workerName, newVersion, cronKey, cronHash, newCrons, cronPlan) {
  // __meta__.version lets scheduler build x-worker-id without a
  // second HGET; DEL when no crons remain so stale refs decay.
  if (newCrons.length === 0) {
    if (Object.keys(cronHash).length) multi.del(cronKey);
    stageCronWorkerRemoved(multi, ns, workerName);
    return;
  }
  stageCronWorkerIndexed(multi, ns, workerName);
  multi.hSet(cronKey, "__meta__", JSON.stringify({
    version: newVersion, seq: cronPlan.cronSeq,
  }));
  for (const e of cronPlan.addedWithPlacement) {
    multi.hSet(cronKey, e.id, JSON.stringify({
      cron: e.cron, timezone: e.timezone, gen: e.gen,
    }));
    stageCronSlotRef(multi, ns, workerName, e);
  }
  for (const r of cronPlan.removed) {
    multi.hDel(cronKey, r.id);
  }
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
  await watchKeys(iso, d1Refs.map((ref) => d1DatabaseKey(ns, ref.databaseId)));
  for (const ref of d1Refs) {
    const exists = await iso.exists(d1DatabaseKey(ns, ref.databaseId));
    if (exists === 0) {
      throw new RoutingError(
        409,
        "d1_database_dependency_missing",
        `Cannot ${targetLabel}: D1 binding "${ref.binding}" ` +
          `references deleted database "${ref.databaseId}"`,
        { broken_d1_dependency: ref }
      );
    }
  }
}

/** @param {RedisIso} iso @param {string} targetLabel @param {OutgoingRef[]} outgoingRefs */
async function assertOutgoingDependenciesPresent(iso, targetLabel, outgoingRefs) {
  await watchKeys(iso, outgoingRefs.map((ref) => bundleKey(ref.targetNs, ref.targetWorker, ref.targetVersion)));
  for (const ref of outgoingRefs) {
    const targetMetaRaw = await iso.hGet(
      bundleKey(ref.targetNs, ref.targetWorker, ref.targetVersion),
      "__meta__"
    );
    if (!targetMetaRaw) {
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
  for (const c of newQueueConsumers) {
    const held = await iso.hGetAll(queueConsumerKey(ns, c.queue));
    if (held && held.worker && held.worker !== workerName) {
      throw new RoutingError(
        409,
        "queue_consumer_conflict",
        `Queue "${c.queue}" is already consumed by ${held.worker}`,
        { queue: c.queue }
      );
    }
  }
}

/** @param {RedisIso} iso @param {string} ns @param {RoutePattern[]} newRoutes */
async function assertDeclaredHosts(iso, ns, newRoutes) {
  const newHosts = [...new Set(newRoutes.map((r) => r.host))];
  if (!newHosts.length) return;
  const memberFlags = await iso.sMIsMember(`hosts:${ns}`, ...newHosts);
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
  for (const h of affectedHosts) {
    const state = await iso.hGetAll(patternsKey(h));
    hostState[h] = state;
    /** @type {{ slot: string, held: PatternProjection } | null} */
    let foreign = null;
    /** @type {Map<string, PatternProjection>} */
    const slots = new Map();
    for (const [slot, raw] of Object.entries(state)) {
      const held = decodePatternProjection(raw);
      if (!held) continue;
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
      const otherMeta = typeof rawMeta === "string"
        ? parseBundleMeta(otherNs, otherWorker, /** @type {string} */ (otherVersion), rawMeta)
        : null;
      if (!otherMeta || !Array.isArray(otherMeta.exports)) continue;
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
  if (!metaRaw) {
    throw new RoutingError(404, "version_not_found", `Version ${newVersion} not found for ${ns}/${workerName}`);
  }
  const meta = parseBundleMeta(ns, workerName, newVersion, metaRaw);

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
  const oldRoutes = await readRoutesFromMeta(iso, ns, workerName, currentVersion);
  const oldQueueConsumers = await readQueueConsumersFromMeta(iso, ns, workerName, currentVersion);

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

  const cronHash = stringHash(await iso.hGetAll(cronKey));
  const cronPlan = computeCronPlan(cronHash, inputs.newCrons, Date.now(), logContext);
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
  multi.sAdd("namespaces", ns);
  if (plan.nsHostsAdd.length) multi.sAdd(`ns-hosts:${ns}`, plan.nsHostsAdd);
  if (plan.nsHostsRem.length) multi.sRem(`ns-hosts:${ns}`, plan.nsHostsRem);
  stageD1ReferrerAdds(multi, {
    ns, worker: workerName, version: newVersion, refs: inputs.d1Refs,
    databaseIdFor: (ref) => String(ref.databaseId),
  });
  stageCronPlan(
    multi,
    ns,
    workerName,
    newVersion,
    plan.cronKey,
    plan.cronHash,
    inputs.newCrons,
    plan.cronPlan
  );
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
      `hosts:${ns}`,
      cronKey,
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

  const newNum = await redis.incr(`worker:${ns}:${workerName}:next_version`);
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
      `hosts:${ns}`,
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

    const srcMeta = await readMeta(iso, ns, workerName, currentVersion);
    if (!srcMeta) {
      throw new RoutingError(
        500,
        "bundle_copy_failed",
        `COPY ${srcKey} → ${dstKey} failed (source missing?)`
      );
    }

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

    // Scheduler builds x-worker-id from cron __meta__.version.
    const cronKey = cronWorkerKey(ns, workerName);
    await iso.watch(cronKey);
    const cronMetaRaw = await iso.hGet(cronKey, "__meta__");
    let cronMetaParsed = null;
    if (cronMetaRaw) {
      cronMetaParsed = parseCronMeta(cronMetaRaw, logContext);
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
    multi.sAdd("namespaces", ns);

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

    if (cronMetaParsed) {
      stageCronWorkerIndexed(multi, ns, workerName);
      multi.hSet(cronKey, "__meta__", JSON.stringify({
        version: newVersion, seq: cronSeqFromMeta(cronMetaParsed, logContext),
      }));
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
    await iso.watch(`hosts:${ns}`);
    const current = await iso.sMembers(`hosts:${ns}`);
    const currentSet = new Set(current);
    const toAdd = [...bodySet.difference(currentSet)];
    const toRemove = [...currentSet.difference(bodySet)];
    const changedHosts = [...new Set([...toAdd, ...toRemove])];

    const allHosts = currentSet.union(bodySet);
    await watchKeys(iso, [...allHosts].map((h) => patternsKey(h)));
    await watchKeys(iso, [
      DECLARED_HOSTS_KEY,
      ...changedHosts.map(hostDeclarationsKey),
    ]);

    // Reverse-index fast path; HGETALL only to enrich the 409 message.
    const reverseFlags = toRemove.length
      ? await iso.sMIsMember(`ns-hosts:${ns}`, ...toRemove)
      : [];
    for (let i = 0; i < toRemove.length; i++) {
      const h = toRemove[i];
      if (!reverseFlags[i]) continue;
      const entries = await iso.hGetAll(patternsKey(h));
      for (const [slot, raw] of Object.entries(entries)) {
        const parsed = decodePatternProjection(raw);
        if (parsed && parsed.ns === ns) {
          throw new RoutingError(
            409,
            "host_in_use",
            `Cannot remove host "${h}": live pattern ${h}${slot} still references namespace "${ns}"`,
            { host: h, slot }
          );
        }
      }
    }

    const removals = [];
    for (const host of toRemove) {
      removals.push({
        host,
        declaredNs: await iso.sMembers(hostDeclarationsKey(host)),
      });
    }

    const multi = iso.multi();
    stageDeclaredHostChanges(multi, ns, toAdd, removals);
    await multi.exec();
    return [...bodySet].toSorted();
  });
}
