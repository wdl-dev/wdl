import {
  D1ProtocolError,
  dbKeyOf,
  slotOf,
} from "d1-runtime-protocol";
import {
  resolveTaskIdentity,
} from "d1-runtime-task-identity";
import { decodeBulk, WatchError } from "shared-redis";
import { envValueOr } from "shared-env";
import { errorMessage } from "shared-errors";
import { createRequiredRedisClient } from "shared-redis-client";
import { validOwnerEndpointForService } from "shared-owner-endpoint";
import {
  boundedPositiveIntEnv,
  currentOwnerGenerationCounter,
  nextOwnerGeneration,
  ownerLeaseExpiresAt,
  ownerLeaseExpired,
  parseOwnerRecord,
  redisServerTimeMs,
  withOwnerWatchRetries,
} from "shared-owner-lease";
import {
  ownerFenceMatches,
  ownerProtocolKeys,
  readOwnerRecord,
  readOwnerRecordWithRedisTime,
  stageOwnerClaim,
  stageOwnerRelease,
  stageOwnerRenew,
} from "shared-owner-protocol";
import {
  isDraining,
  forgetStorageSize,
  log,
  metrics,
  observedD1Owners,
  ownedDbs,
  pendingQueryCount,
  setDraining,
  SERVICE,
} from "d1-runtime-state";

const DEFAULT_OWNER_TTL_SECONDS = 120;
const DEFAULT_PROBE_TIMEOUT_MS = 500;
const DEFAULT_OBSERVED_OWNER_TTL_MS = 30_000;
const DEFAULT_OBSERVED_OWNER_MAX_ENTRIES = 10_000;
const DEFAULT_DRAIN_WAIT_TIMEOUT_MS = 10_000;
const DEFAULT_RENEW_CONCURRENCY = 8;
const MAX_RENEW_CONCURRENCY = 64;
const DEFAULT_DRAIN_CONCURRENCY = 16;
const MAX_DRAIN_CONCURRENCY = 64;
const DEFAULT_OWNER_LEASE_GUARD_MS = 1_000;
const DRAIN_WAIT_POLL_MS = 25;
const OWNER_PREFIX = "d1:owner:db:";
const OWNER_CLAIM_RETRIES = 3;
const OWNER_CLAIM_WINNER_READ_DELAYS_MS = [0, 10, 25, 50, 100];
const D1_OWNER_PORT = 8787;

/**
 * @typedef {{ idFromName(name: string): unknown, get(id: unknown): { fetch(url: string, init?: RequestInit): Promise<Response> } }} D1Namespace
 * @typedef {Record<string, unknown> & { REDIS_ADDR?: string, REDIS_DB?: unknown, D1_DATABASES?: D1Namespace, D1_OWNER_TTL_SECONDS?: unknown, D1_OWNER_LEASE_GUARD_MS?: unknown, D1_PROBE_TIMEOUT_MS?: unknown, D1_OBSERVED_OWNER_TTL_MS?: unknown, D1_OBSERVED_OWNER_MAX_ENTRIES?: unknown, D1_DRAIN_TIMEOUT_MS?: unknown, D1_RENEW_CONCURRENCY?: unknown, D1_DRAIN_CONCURRENCY?: unknown }} D1Env
 * @typedef {{ namespace?: string, databaseId?: string, dbKey: string, slot?: string | number }} D1Identity
 * @typedef {{ namespace?: string, databaseId?: string, dbKey: string, slot?: string | number, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} D1Owner
 * @typedef {{ taskId: string, endpoint: string }} D1Target
 * @typedef {import("shared-redis").RedisSetOptions} RedisSetOptions
 * @typedef {{ get(key: string): Promise<string | Uint8Array | null>, getWithTime(key: string): Promise<{ value: string | Uint8Array | null, nowMs: number }>, time(): Promise<number>, set(key: string, value: string, options?: RedisSetOptions): Promise<unknown>, session<T>(callback: (session: import("shared-redis").RedisSession) => Promise<T>): Promise<T> }} RedisClient
 * @typedef {{ pendingObservedMax: number, waitedMs: number, released?: number, alreadyLost?: number, errors?: Array<unknown> }} DrainResult
 */

/** @param {D1Env} env */
export function redisClient(env) {
  return createRequiredRedisClient(
    env,
    D1ProtocolError,
    "registry-unavailable",
    "D1 owner registry is not configured"
  );
}

/** @param {D1Env} env */
export function ownerTtlSeconds(env) {
  const raw = Number(envValueOr(env.D1_OWNER_TTL_SECONDS, DEFAULT_OWNER_TTL_SECONDS));
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_OWNER_TTL_SECONDS;
}

/** @param {D1Env} env */
export function ownerLeaseGuardMs(env) {
  const raw = Number(envValueOr(env.D1_OWNER_LEASE_GUARD_MS, DEFAULT_OWNER_LEASE_GUARD_MS));
  return Number.isFinite(raw) && raw >= 0 ? Math.trunc(raw) : DEFAULT_OWNER_LEASE_GUARD_MS;
}

/** @param {D1Env} env */
export function probeTimeoutMs(env) {
  const raw = Number(envValueOr(env.D1_PROBE_TIMEOUT_MS, DEFAULT_PROBE_TIMEOUT_MS));
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_PROBE_TIMEOUT_MS;
}

/** @param {D1Env} env */
export function observedOwnerTtlMs(env) {
  const raw = Number(envValueOr(env.D1_OBSERVED_OWNER_TTL_MS, DEFAULT_OBSERVED_OWNER_TTL_MS));
  return Number.isFinite(raw) && raw >= 0 ? raw : DEFAULT_OBSERVED_OWNER_TTL_MS;
}

/** @param {D1Env} env */
export function observedOwnerMaxEntries(env) {
  const raw = Number(envValueOr(env.D1_OBSERVED_OWNER_MAX_ENTRIES, DEFAULT_OBSERVED_OWNER_MAX_ENTRIES));
  return Number.isInteger(raw) && raw >= 0 ? raw : DEFAULT_OBSERVED_OWNER_MAX_ENTRIES;
}

/** @param {D1Env} env */
export function renewConcurrency(env) {
  return boundedPositiveIntEnv(env, "D1_RENEW_CONCURRENCY", DEFAULT_RENEW_CONCURRENCY, MAX_RENEW_CONCURRENCY);
}

/** @param {D1Env} env */
export function drainConcurrency(env) {
  return boundedPositiveIntEnv(env, "D1_DRAIN_CONCURRENCY", DEFAULT_DRAIN_CONCURRENCY, MAX_DRAIN_CONCURRENCY);
}

/** @param {string} dbKey */
export function ownerKeyOf(dbKey) {
  return ownerProtocolKeys(OWNER_PREFIX, dbKey).ownerKey;
}

/** @param {string} dbKey */
export function ownerGenerationKeyOf(dbKey) {
  return ownerProtocolKeys(OWNER_PREFIX, dbKey).generationKey;
}

/**
 * @param {D1Owner | null} owner
 * @param {string} expectedDbKey
 */
function ownerMatchesScope(owner, expectedDbKey) {
  if (
    !owner ||
    typeof owner.namespace !== "string" ||
    typeof owner.databaseId !== "string" ||
    typeof owner.dbKey !== "string" ||
    typeof owner.slot !== "number" ||
    !Number.isSafeInteger(owner.slot) || owner.slot < 0
  ) return false;
  try {
    return owner.dbKey === expectedDbKey && dbKeyOf(owner.namespace, owner.databaseId) === expectedDbKey;
  } catch {
    return false;
  }
}

/** @param {string | null | undefined} raw @param {string} expectedDbKey @returns {D1Owner | null} */
export function parseOwner(raw, expectedDbKey) {
  if (raw == null) return null;
  const owner = /** @type {D1Owner | null} */ (parseOwnerRecord(raw));
  if (
    !owner ||
    !ownerMatchesScope(owner, expectedDbKey) ||
    typeof owner.taskId !== "string" || !owner.taskId ||
    !validOwnerEndpointForService(owner.endpoint, D1_OWNER_PORT, "d1-runtime")
  ) {
    throw new D1ProtocolError(503, "owner-record-invalid", "D1 owner record is invalid");
  }
  return owner;
}

/** @param {string} dbKey @returns {never} */
function notOwner(dbKey) {
  throw new D1ProtocolError(
    409,
    "not-owner",
    `D1 database ${dbKey} is owned by another task`
  );
}

/** @param {D1Env} env @param {string} dbKey */
export async function readOwner(env, dbKey) {
  return await readOwnerFromClient(redisClient(env), dbKey);
}

/** @param {RedisClient} client @param {string} dbKey */
async function readOwnerFromClient(client, dbKey) {
  return await readOwnerRecord(client, ownerKeyOf(dbKey), (raw) => parseOwner(decodeBulk(raw), dbKey));
}

/** @param {{ getWithTime(key: string): Promise<{ value: string | Uint8Array | null | undefined, nowMs: number }> }} client @param {string} dbKey */
async function readOwnerWithTimeFromClient(client, dbKey) {
  return await readOwnerRecordWithRedisTime(client, ownerKeyOf(dbKey), (raw) => parseOwner(decodeBulk(raw), dbKey));
}

/** @param {RedisClient} client @param {string} dbKey */
async function readOwnerWithTimeAfterClaimRace(client, dbKey) {
  for (const delayMs of OWNER_CLAIM_WINNER_READ_DELAYS_MS) {
    if (delayMs > 0) await sleep(delayMs);
    const result = await readOwnerWithTimeFromClient(client, dbKey);
    if (result.owner) return result;
  }
  return null;
}

/** @param {D1Env} env @param {D1Identity} identity @param {number} generation @param {D1Target} target @param {number} nowMs */
function ownerRecordFor(env, identity, generation, target, nowMs) {
  if (!validOwnerEndpointForService(target.endpoint, D1_OWNER_PORT, "d1-runtime")) {
    throw new D1ProtocolError(503, "owner-record-invalid", "D1 owner target endpoint is invalid");
  }
  const ttl = ownerTtlSeconds(env);
  return {
    namespace: identity.namespace,
    databaseId: identity.databaseId,
    dbKey: identity.dbKey,
    slot: identity.slot,
    taskId: target.taskId,
    endpoint: target.endpoint,
    generation,
    leaseExpiresAt: ownerLeaseExpiresAt(nowMs, ttl),
  };
}

/** @param {RedisClient} client @param {D1Owner | null | undefined} owner */
async function ownerLeaseExpiredByRedisClient(client, owner) {
  return ownerLeaseExpired(owner, await redisServerTimeMs(client));
}

/** @param {D1Env} env @param {D1Owner | null | undefined} owner */
export async function ownerLeaseExpiredByRedisTime(env, owner) {
  return ownerLeaseExpiredByRedisClient(redisClient(env), owner);
}

/** @param {D1Owner} owner */
function rememberOwner(owner) {
  ownedDbs.set(owner.dbKey, owner);
}

/** @param {string} dbKey */
function forgetOwnedDb(dbKey) {
  ownedDbs.delete(dbKey);
  forgetStorageSize(dbKey);
}

/** @param {string} dbKey */
function forgetObservedOwner(dbKey) {
  observedD1Owners.delete(dbKey);
}

function observedCacheNowMs() {
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

/**
 * @param {D1Env} env
 * @param {D1Owner | null | undefined} owner
 * @param {number | null} [redisNowMs]
 */
function rememberObservedOwner(env, owner, redisNowMs = null) {
  if (!owner?.dbKey) return;
  const ttlMs = observedOwnerTtlMs(env);
  const maxEntries = observedOwnerMaxEntries(env);
  const now = observedCacheNowMs();
  const hasRedisNow = typeof redisNowMs === "number" && Number.isSafeInteger(redisNowMs) && redisNowMs >= 0;
  if (ttlMs <= 0 || maxEntries <= 0 || !hasRedisNow || ownerLeaseExpired(owner, redisNowMs)) {
    forgetObservedOwner(owner.dbKey);
    return;
  }
  const ttlExpiresAt = now + ttlMs;
  const leaseExpiresAt = Number(owner.leaseExpiresAt ?? 0);
  const leaseRemainingMs = Math.max(0, leaseExpiresAt - redisNowMs);
  const expiresAt = Math.min(ttlExpiresAt, now + leaseRemainingMs);
  observedD1Owners.delete(owner.dbKey);
  observedD1Owners.set(owner.dbKey, { owner, expiresAt, redisTimeBound: true });
  while (observedD1Owners.size > maxEntries) {
    const oldestKey = observedD1Owners.keys().next().value;
    if (oldestKey === undefined) break;
    observedD1Owners.delete(oldestKey);
  }
}

/** @param {D1Env} env @param {D1Identity} identity @param {D1Target} localTask @param {{ refresh?: boolean }} [options] */
function cachedObservedOwner(env, identity, localTask, options = {}) {
  if (
    options.refresh === true ||
    observedOwnerTtlMs(env) <= 0 ||
    observedOwnerMaxEntries(env) <= 0
  ) return null;
  const entry = observedD1Owners.get(identity.dbKey);
  const now = observedCacheNowMs();
  if (!entry || entry.redisTimeBound !== true || entry.expiresAt <= now) {
    forgetObservedOwner(identity.dbKey);
    return null;
  }
  if (entry.owner.taskId === localTask.taskId) {
    if (isDraining()) {
      throw new D1ProtocolError(503, "task-draining", "D1 task is draining");
    }
    rememberOwner(entry.owner);
    recordOwnerResolution("cached_local");
  } else {
    forgetOwnedDb(identity.dbKey);
    recordOwnerResolution("cached_remote");
  }
  observedD1Owners.delete(identity.dbKey);
  observedD1Owners.set(identity.dbKey, entry);
  return entry.owner;
}

/** @param {string} outcome */
function recordOwnerResolution(outcome) {
  metrics.increment("d1_owner_resolutions", { service: SERVICE, outcome });
}

/** @param {string} outcome */
function recordGenerationRepair(outcome) {
  metrics.increment("d1_owner_generation_repairs", { service: SERVICE, outcome });
}

/** @param {RedisClient} client @param {D1Owner} owner */
async function repairGenerationCounterIfStale(client, owner) {
  const generationKey = ownerGenerationKeyOf(owner.dbKey);
  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(generationKey);
    const currentCounter = await currentOwnerGenerationCounter(session, generationKey);
    if (currentCounter >= owner.generation) {
      await session.unwatch();
      return owner;
    }
    await session.multi().set(generationKey, String(owner.generation)).exec();
    recordGenerationRepair("ok");
    return owner;
  }), "owner-claim-raced", `failed to repair D1 owner generation counter for ${owner.dbKey}`);
}

/** @param {D1Env} env */
function drainWaitTimeoutMs(env) {
  const raw = Number(envValueOr(env.D1_DRAIN_TIMEOUT_MS, DEFAULT_DRAIN_WAIT_TIMEOUT_MS));
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_DRAIN_WAIT_TIMEOUT_MS;
}

/** @param {number} ms */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** @template T @param {() => Promise<T>} operation @param {string} exhaustedCode @param {string} exhaustedMessage @returns {Promise<T>} */
async function withWatchRetries(operation, exhaustedCode, exhaustedMessage) {
  return await withOwnerWatchRetries(operation, {
    retries: OWNER_CLAIM_RETRIES,
    isWatchError: (err) => err instanceof WatchError,
    createError: (status, code, message) => new D1ProtocolError(status, code, message),
    exhaustedCode,
    exhaustedMessage,
  });
}

/** @param {D1Env} env */
async function waitForPendingQueriesToDrain(env) {
  const started = Date.now();
  const deadline = Date.now() + drainWaitTimeoutMs(env);
  let pendingObservedMax = pendingQueryCount();
  while (pendingQueryCount() > 0) {
    pendingObservedMax = Math.max(pendingObservedMax, pendingQueryCount());
    if (Date.now() >= deadline) {
      throw new D1ProtocolError(
        503,
        "drain-timeout",
        `D1 drain timed out waiting for ${pendingQueryCount()} in-flight query(s)`,
        {
          pendingObservedMax,
          waitedMs: Date.now() - started,
        }
      );
    }
    await sleep(DRAIN_WAIT_POLL_MS);
  }
  return {
    pendingObservedMax,
    waitedMs: Date.now() - started,
  };
}

/** @param {D1Env} env @param {D1Owner} owner @param {number} deadline */
async function waitForOwnedActorToDrain(env, owner, deadline) {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) {
    throw new D1ProtocolError(503, "drain-timeout", "D1 drain timed out waiting for owned actors to become idle");
  }
  if (!env.D1_DATABASES) {
    throw new D1ProtocolError(503, "missing-d1-binding", "D1_DATABASES binding is not configured");
  }
  const id = env.D1_DATABASES.idFromName(owner.dbKey);
  const stub = env.D1_DATABASES.get(id);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), remainingMs);
  const started = Date.now();
  try {
    const response = await stub.fetch("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify({ __control: "wait-until-idle" }),
    });
    if (!response.ok) {
      throw new D1ProtocolError(
        503,
        "drain-timeout",
        `D1 drain failed waiting for ${owner.dbKey} to become idle`
      );
    }
    return { waitedMs: Date.now() - started };
  } catch (err) {
    if (/AbortError|timeout|timed out/i.test(errorMessage(err))) {
      throw new D1ProtocolError(
        503,
        "drain-timeout",
        `D1 drain timed out waiting for ${owner.dbKey} to become idle`
      );
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

/** @param {D1Env} env @param {D1Owner[]} entries */
async function waitForOwnedActorsToDrain(env, entries) {
  const started = Date.now();
  const deadline = started + drainWaitTimeoutMs(env);
  let nextIndex = 0;
  let released = 0;
  let alreadyLost = 0;
  let pendingObservedMax = pendingQueryCount();
  /** @type {Array<{ dbKey: string, error: string }>} */
  const errors = [];
  const width = Math.max(1, Math.min(drainConcurrency(env), entries.length || 1));
  await Promise.all(Array.from({ length: width }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= entries.length) return;
      const owner = entries[currentIndex];
      try {
        pendingObservedMax = Math.max(pendingObservedMax, pendingQueryCount());
        await waitForOwnedActorToDrain(env, owner, deadline);
        pendingObservedMax = Math.max(pendingObservedMax, pendingQueryCount());
        const result = await releaseOwner(env, owner);
        // These counters are only mutated in synchronous sections between
        // awaits; keep it that way if this loop grows.
        if (result.released) released += 1;
        else alreadyLost += 1;
      } catch (err) {
        errors.push({
          dbKey: owner.dbKey,
          error: errorMessage(err),
        });
      }
    }
  }));
  return {
    pendingObservedMax,
    waitedMs: Date.now() - started,
    released,
    alreadyLost,
    errors,
  };
}

/** @param {D1Env} env @param {D1Owner} owner */
export async function renewOwner(env, owner) {
  const client = redisClient(env);
  const key = ownerKeyOf(owner.dbKey);
  const localTask = await resolveTaskIdentity(env);
  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(key);
    const { owner: current, nowMs } = await readOwnerWithTimeFromClient(session, owner.dbKey);
    if (!ownerFenceMatches(current, owner)) {
      await session.unwatch();
      forgetOwnedDb(owner.dbKey);
      if (current) rememberObservedOwner(env, current);
      else forgetObservedOwner(owner.dbKey);
      return { renewed: false, owner: current, nowMs };
    }
    if (ownerLeaseExpired(current, nowMs)) {
      await session.unwatch();
      forgetOwnedDb(owner.dbKey);
      forgetObservedOwner(owner.dbKey);
      return { renewed: false, owner: current, nowMs };
    }

    const renewed = ownerRecordFor(env, owner, owner.generation, localTask, nowMs);
    await stageOwnerRenew(session.multi(), key, renewed, ownerTtlSeconds(env)).exec();
    ownedDbs.set(owner.dbKey, renewed);
    rememberObservedOwner(env, renewed, nowMs);
    return { renewed: true, owner: renewed, nowMs };
  }), "owner-renew-raced", `failed to renew D1 database ${owner.dbKey}`);
}

/** @param {D1Env} env @param {D1Owner} owner */
export async function releaseOwner(env, owner) {
  const client = redisClient(env);
  const key = ownerKeyOf(owner.dbKey);
  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(key);
    const current = parseOwner(await session.get(key), owner.dbKey);
    if (!ownerFenceMatches(current, owner)) {
      await session.unwatch();
      forgetOwnedDb(owner.dbKey);
      if (current) rememberObservedOwner(env, current);
      else forgetObservedOwner(owner.dbKey);
      return { released: false, owner: current };
    }

    await stageOwnerRelease(session.multi(), key).exec();
    forgetOwnedDb(owner.dbKey);
    forgetObservedOwner(owner.dbKey);
    return { released: true, owner: null };
  }), "owner-release-raced", `failed to release D1 database ${owner.dbKey}`);
}

/** @param {D1Env} env @param {D1Owner} staleOwner @returns {Promise<D1Owner>} */
export async function takeoverExpiredOwner(env, staleOwner) {
  const localTask = await resolveTaskIdentity(env);
  if (isDraining()) {
    throw new D1ProtocolError(503, "task-draining", "D1 task is draining");
  }
  const client = redisClient(env);
  const key = ownerKeyOf(staleOwner.dbKey);
  const generationKey = ownerGenerationKeyOf(staleOwner.dbKey);
  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(key, generationKey);
    const { owner: current, nowMs } = await readOwnerWithTimeFromClient(session, staleOwner.dbKey);
    if (!current) {
      await session.unwatch();
      forgetObservedOwner(staleOwner.dbKey);
      return await resolveDbOwner(env, staleOwner, { refresh: true });
    }
    if (!ownerFenceMatches(current, staleOwner)) {
      await session.unwatch();
      if (current.taskId === localTask.taskId) rememberOwner(current);
      rememberObservedOwner(env, current);
      return current;
    }
    if (!ownerLeaseExpired(current, nowMs)) {
      await session.unwatch();
      rememberObservedOwner(env, current, nowMs);
      return current;
    }

    const generation = await nextOwnerGeneration(session, generationKey, current.generation);
    const owner = ownerRecordFor(env, current, generation, localTask, nowMs);
    await stageOwnerClaim(session.multi(), { ownerKey: key, generationKey }, owner, ownerTtlSeconds(env)).exec();
    rememberOwner(owner);
    rememberObservedOwner(env, owner, nowMs);
    metrics.increment("d1_owner_takeovers", { service: SERVICE, outcome: "ok" });
    log("warn", "d1_owner_takeover", {
      namespace: owner.namespace,
      database_id: owner.databaseId,
      slot: owner.slot,
      previous_owner_task_id: current.taskId,
      owner_task_id: owner.taskId,
      generation,
    });
    return owner;
  }), "owner-takeover-raced", `failed to take over D1 database ${staleOwner.dbKey}`);
}

/** @param {D1Env} env */
export async function renewOwnedDbs(env) {
  if (isDraining()) {
    return {
      owned: ownedDbs.size,
      renewed: 0,
      lost: 0,
      errors: [],
    };
  }
  const entries = Array.from(ownedDbs.values());
  let renewed = 0;
  let lost = 0;
  /** @type {string[]} */
  const errors = [];
  let nextIndex = 0;
  const width = Math.max(1, Math.min(renewConcurrency(env), entries.length || 1));
  await Promise.all(Array.from({ length: width }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= entries.length) return;
      const owner = entries[currentIndex];
      try {
        const result = await renewOwner(env, owner);
        if (result.renewed) renewed += 1;
        else {
          lost += 1;
          metrics.increment("d1_lease_renew_failures", { service: SERVICE, reason: "lost-owner" });
        }
      } catch (err) {
        metrics.increment("d1_lease_renew_failures", { service: SERVICE, reason: "error" });
        errors.push(errorMessage(err));
      }
    }
  }));
  return {
    owned: ownedDbs.size,
    renewed,
    lost,
    errors,
  };
}

/** @param {D1Env} env */
export async function drainOwnedDbs(env) {
  setDraining(true);
  const entries = Array.from(ownedDbs.values());
  /** @type {DrainResult} */
  let drainWait = { pendingObservedMax: pendingQueryCount(), waitedMs: 0 };
  let released = 0;
  let alreadyLost = 0;
  /** @type {Array<unknown>} */
  const errors = [];

  if (env.D1_DATABASES && entries.length > 0) {
    drainWait = await waitForOwnedActorsToDrain(env, entries);
    released = drainWait.released ?? 0;
    alreadyLost = drainWait.alreadyLost ?? 0;
    errors.push(...(drainWait.errors ?? []));
  } else {
    try {
      drainWait = await waitForPendingQueriesToDrain(env);
    } catch (err) {
      const details = /** @type {{ pendingObservedMax?: unknown, waitedMs?: unknown }} */ (
        err && typeof err === "object" ? err : {}
      );
      const pendingObservedMax = Math.max(
        Number(drainWait.pendingObservedMax ?? 0),
        Number(details.pendingObservedMax ?? 0)
      );
      return {
        draining: isDraining(),
        owned: ownedDbs.size,
        pending: pendingQueryCount(),
        pendingObservedMax,
        waitedMs: Number(details.waitedMs ?? drainWait.waitedMs ?? 0),
        released: 0,
        alreadyLost: 0,
        errors: [{ dbKey: null, error: errorMessage(err) }],
      };
    }
    for (const owner of entries) {
      try {
        const result = await releaseOwner(env, owner);
        if (result.released) released += 1;
        else alreadyLost += 1;
      } catch (err) {
        errors.push({ dbKey: owner.dbKey, error: errorMessage(err) });
      }
    }
  }

  return {
    draining: isDraining(),
    owned: ownedDbs.size,
    pending: pendingQueryCount(),
    pendingObservedMax: drainWait.pendingObservedMax,
    waitedMs: drainWait.waitedMs,
    released,
    alreadyLost,
    errors,
  };
}

/** @param {unknown} databases */
export function normalizeDatabases(databases) {
  if (!Array.isArray(databases) || databases.length === 0) {
    throw new D1ProtocolError(400, "invalid-databases", "databases must be a non-empty array");
  }
  return databases.map((database) => {
    if (!database || typeof database !== "object" || Array.isArray(database)) {
      throw new D1ProtocolError(400, "invalid-database", "database must be an object");
    }
    const record = /** @type {Record<string, unknown>} */ (database);
    const namespace = record.namespace;
    const databaseId = record.databaseId;
    const dbKey = dbKeyOf(namespace, databaseId);
    const normalizedNamespace = /** @type {string} */ (namespace);
    const normalizedDatabaseId = /** @type {string} */ (databaseId);
    return {
      namespace: normalizedNamespace,
      databaseId: normalizedDatabaseId,
      dbKey,
      slot: slotOf(normalizedNamespace, normalizedDatabaseId),
    };
  });
}

/** @param {unknown} target */
export function normalizeTarget(target) {
  if (target == null) return null;
  if (!target || typeof target !== "object" || Array.isArray(target)) {
    throw new D1ProtocolError(400, "invalid-target", "target must be an object");
  }
  const record = /** @type {Record<string, unknown>} */ (target);
  if (typeof record.taskId !== "string" || !record.taskId) {
    throw new D1ProtocolError(400, "invalid-target", "target.taskId is required");
  }
  if (typeof record.endpoint !== "string" || !record.endpoint) {
    throw new D1ProtocolError(400, "invalid-target", "target.endpoint is required");
  }
  if (!validOwnerEndpointForService(record.endpoint, D1_OWNER_PORT, "d1-runtime")) {
    throw new D1ProtocolError(400, "invalid-target", "target.endpoint is invalid");
  }
  return { taskId: record.taskId, endpoint: record.endpoint };
}

/** @param {D1Env} env @param {D1Identity} database @param {D1Target | null} target */
export async function rebalanceDatabase(env, database, target) {
  const client = redisClient(env);
  const key = ownerKeyOf(database.dbKey);
  const generationKey = ownerGenerationKeyOf(database.dbKey);
  const localTask = await resolveTaskIdentity(env);
  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(key, generationKey);
    const current = parseOwner(await session.get(key), database.dbKey);
    if (!current || current.taskId !== localTask.taskId) {
      await session.unwatch();
      forgetOwnedDb(database.dbKey);
      if (current) rememberObservedOwner(env, current);
      else forgetObservedOwner(database.dbKey);
      return { database, outcome: "not-owner", owner: current };
    }

    if (!target) {
      await stageOwnerRelease(session.multi(), key).exec();
      forgetOwnedDb(database.dbKey);
      forgetObservedOwner(database.dbKey);
      return { database, outcome: "released", owner: null };
    }

    if (target.taskId === current.taskId && target.endpoint === current.endpoint) {
      await session.unwatch();
      ownedDbs.set(database.dbKey, current);
      rememberObservedOwner(env, current);
      return { database, outcome: "unchanged", owner: current };
    }

    const generation = await nextOwnerGeneration(session, generationKey, current.generation);
    const nowMs = await redisServerTimeMs(session);
    const nextOwner = ownerRecordFor(env, database, generation, target, nowMs);
    await stageOwnerClaim(session.multi(), { ownerKey: key, generationKey }, nextOwner, ownerTtlSeconds(env)).exec();
    forgetOwnedDb(database.dbKey);
    rememberObservedOwner(env, nextOwner, nowMs);
    return { database, outcome: "moved", owner: nextOwner };
  }), "owner-rebalance-raced", `failed to rebalance D1 database ${database.dbKey}`);
}

/** @param {D1Env} env @param {{ databases?: unknown, target?: unknown }} body */
export async function rebalanceOwnedDbs(env, body) {
  const localTask = await resolveTaskIdentity(env);
  const databases = normalizeDatabases(body?.databases);
  const target = normalizeTarget(body?.target);
  /** @type {Array<unknown>} */
  const results = [];
  for (const database of databases) {
    results.push(await rebalanceDatabase(env, database, target));
  }
  return {
    taskId: localTask.taskId,
    target,
    results,
  };
}

/** @param {D1Env} env @param {D1Identity} identity @param {{ refresh?: boolean }} [options] @returns {Promise<D1Owner>} */
export async function resolveDbOwner(env, identity, options = {}) {
  const localTask = await resolveTaskIdentity(env);
  const taskId = localTask.taskId;
  const observed = cachedObservedOwner(env, identity, localTask, options);
  if (observed) return observed;
  const client = redisClient(env);
  const key = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);

  const { owner: existing, nowMs: existingNowMs } = await readOwnerWithTimeFromClient(client, identity.dbKey);
  if (existing) {
    if (ownerLeaseExpired(existing, existingNowMs)) {
      forgetOwnedDb(identity.dbKey);
      forgetObservedOwner(identity.dbKey);
      return await takeoverExpiredOwner(env, existing);
    }
    if (existing.taskId !== taskId) {
      forgetOwnedDb(identity.dbKey);
      rememberObservedOwner(env, existing, existingNowMs);
      recordOwnerResolution("remote");
      return existing;
    }
    if (isDraining()) {
      throw new D1ProtocolError(503, "task-draining", "D1 task is draining");
    }
    const alreadyLocal = ownedDbs.has(identity.dbKey);
    rememberOwner(existing);
    rememberObservedOwner(env, existing, existingNowMs);
    if (!alreadyLocal) {
      try {
        await repairGenerationCounterIfStale(client, existing);
      } catch (err) {
        recordGenerationRepair("error");
        log("warn", "d1_owner_generation_repair_failed", {
          namespace: existing.namespace,
          database_id: existing.databaseId,
          slot: existing.slot,
          owner_task_id: existing.taskId,
          generation: existing.generation,
          error_code: /** @type {{ code?: unknown }} */ (err && typeof err === "object" ? err : {}).code || undefined,
          error_message: errorMessage(err),
        });
      }
    }
    recordOwnerResolution("local");
    return existing;
  }
  if (isDraining()) {
    throw new D1ProtocolError(503, "task-draining", "D1 task is draining");
  }

  try {
    return await withWatchRetries(async () => client.session(async (session) => {
      await session.watch(key, generationKey);
      const { owner: racedOwner, nowMs } = await readOwnerWithTimeFromClient(session, identity.dbKey);
      if (racedOwner) {
        await session.unwatch();
        if (ownerLeaseExpired(racedOwner, nowMs)) {
          forgetOwnedDb(identity.dbKey);
          forgetObservedOwner(identity.dbKey);
          return await takeoverExpiredOwner(env, racedOwner);
        }
        if (racedOwner.taskId === taskId) rememberOwner(racedOwner);
        rememberObservedOwner(env, racedOwner, nowMs);
        recordOwnerResolution("race_resolved");
        return racedOwner;
      }

      const generation = await nextOwnerGeneration(session, generationKey);
      const owner = ownerRecordFor(env, identity, generation, localTask, nowMs);
      await stageOwnerClaim(session.multi(), { ownerKey: key, generationKey }, owner, ownerTtlSeconds(env)).exec();
      rememberOwner(owner);
      rememberObservedOwner(env, owner, nowMs);
      recordOwnerResolution("claimed");
      return owner;
    }), "owner-claim-raced", `failed to resolve D1 database ${identity.dbKey}`);
  } catch (err) {
    if (/** @type {{ code?: unknown }} */ (err && typeof err === "object" ? err : {}).code !== "owner-claim-raced") throw err;
    const race = await readOwnerWithTimeAfterClaimRace(client, identity.dbKey);
    if (race?.owner) {
      const { owner, nowMs } = race;
      if (ownerLeaseExpired(owner, nowMs)) {
        forgetOwnedDb(identity.dbKey);
        forgetObservedOwner(identity.dbKey);
        return await takeoverExpiredOwner(env, owner);
      }
      if (owner.taskId === taskId) rememberOwner(owner);
      rememberObservedOwner(env, owner, nowMs);
      recordOwnerResolution("race_resolved");
      return owner;
    }
    recordOwnerResolution("race_failed");
    throw err;
  }
}

/**
 * @param {D1Owner} owner
 * @param {number} redisNowMs
 * @param {number} guardMs
 */
function ownerAssertionWithBudget(owner, redisNowMs, guardMs) {
  return {
    owner,
    leaseRemainingMs: Number(owner.leaseExpiresAt ?? 0) - redisNowMs,
    guardMs,
  };
}

/** @param {D1Env} env @param {D1Owner} owner */
export async function assertCurrentOwnerWithLeaseBudget(env, owner) {
  const client = redisClient(env);
  const { owner: current, nowMs } = await readOwnerWithTimeFromClient(client, owner.dbKey);
  if (!current || !ownerFenceMatches(current, owner)) {
    notOwner(owner.dbKey);
  }
  const currentOwner = current;
  if (ownerLeaseExpired(currentOwner, nowMs)) {
    forgetOwnedDb(owner.dbKey);
    forgetObservedOwner(owner.dbKey);
    throw new D1ProtocolError(
      503,
      "owner-lease-expired",
      `D1 database ${owner.dbKey} owner lease has expired`
    );
  }
  const remainingMs = Number(currentOwner.leaseExpiresAt ?? 0) - nowMs;
  const guardMs = ownerLeaseGuardMs(env);
  if (remainingMs < guardMs) {
    const renewed = await renewOwner(env, currentOwner);
    if (
      renewed.renewed &&
      renewed.owner &&
      ownerFenceMatches(renewed.owner, owner) &&
      Number(renewed.owner.leaseExpiresAt ?? 0) - renewed.nowMs >= guardMs
    ) {
      return ownerAssertionWithBudget(renewed.owner, renewed.nowMs, guardMs);
    }
    forgetOwnedDb(owner.dbKey);
    forgetObservedOwner(owner.dbKey);
    throw new D1ProtocolError(
      503,
      "owner-lease-too-short",
      `D1 database ${owner.dbKey} owner lease has insufficient remaining budget`
    );
  }
  return ownerAssertionWithBudget(currentOwner, nowMs, guardMs);
}
