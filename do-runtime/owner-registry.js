import {
  decodeBulk,
  WatchError,
} from "shared-redis";
import {
  buildOwnerKey,
  DO_OWNERSHIP_CODE,
  DoRuntimeError,
  hostIdForShard,
} from "do-runtime-protocol";
import {
  resolveTaskIdentity,
} from "do-runtime-task-identity";
import {
  currentInFlightDispatches,
  isDraining,
  log,
  metrics,
  ownedScopes,
  SERVICE,
} from "do-runtime-state";
import { createRedisClient } from "do-runtime-redis";
import { envValueOr } from "shared-env";
import { errorMessage } from "shared-errors";
import { isValidRuntimeLoadNs, WORKER_NAME_RE } from "shared-ns-pattern";
import { validOwnerEndpointForService } from "shared-owner-endpoint";
import {
  boundedPositiveIntEnv,
  nextOwnerGenerationFromSnapshot,
  ownerLeaseExpiresAt,
  ownerLeaseExpired,
  parseOwnerGenerationCounter,
  parseOwnerRecord,
  withOwnerWatchRetries,
} from "shared-owner-lease";
import {
  ownerFenceMatches,
  ownerProtocolKeys,
  releaseOwnerRecords,
  readOwnerRecord,
  readOwnerSnapshotWithRedisTime,
  stageOwnerClaim,
  stageOwnerRenew,
} from "shared-owner-protocol";
import {
  DO_OWNER_SCOPE_PREFIX,
  VERSION_DELETE_LOCK_KIND,
  deleteLockKey,
  doStorageIdKey,
  parseDeleteLockKind,
} from "shared-worker-contract";

const DEFAULT_OWNER_TTL_SECONDS = 120;
const DEFAULT_RENEW_CONCURRENCY = 8;
const MAX_RENEW_CONCURRENCY = 64;
const DEFAULT_OWNER_LEASE_GUARD_MS = 1_000;
const OWNER_CLAIM_RETRIES = 3;
const OWNER_RENEW_FRACTION = 0.5;
const DO_OWNER_PORT = 8788;
const RENEW_OWNER_WITH_STORAGE_FENCE_SCRIPT = `
local owner = redis.call("GET", KEYS[1])
if owner ~= ARGV[1] then
  return 0
end
local storage_id = redis.call("GET", KEYS[2])
if storage_id ~= ARGV[2] then
  return -1
end
redis.call("SET", KEYS[1], ARGV[3], "EX", ARGV[4])
return 1
`;

/**
 * @typedef {Record<string, unknown> & { REDIS_ADDR?: unknown, REDIS_DB?: unknown, DO_OWNER_TTL_SECONDS?: unknown, DO_OWNER_LEASE_GUARD_MS?: unknown, DO_RENEW_CONCURRENCY?: unknown }} DoEnv
 * @typedef {{ taskId: string, endpoint: string }} LocalTask
 * @typedef {{ ownerKey: string, hostId?: string, className?: string, ns: string, worker: string, doStorageId: string, taskId: string, endpoint: string, generation: number, leaseExpiresAt?: number }} DoOwner
 * @typedef {{ ownerKey: string, taskId: string, generation: number }} OwnerFence
 * @typedef {{ ns: string, worker: string }} BundleScope
 * @typedef {BundleScope & { doStorageId: string }} StorageScope
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {{ hostId: string, className?: string, ns: string, worker: string, doStorageId: string }} InvokeScope
 * @typedef {{ get(key: string): Promise<string | Uint8Array | null | undefined>, getWithTime(key: string): Promise<{ value: string | Uint8Array | null | undefined, nowMs: number }>, getManyWithTime(keys: string[]): Promise<{ values: Array<string | Uint8Array | null | undefined>, nowMs: number }>, time(): Promise<number>, watch?(...keys: string[]): Promise<unknown>, unwatch?(): Promise<unknown>, multi?(): import("shared-redis").RedisMulti }} RedisLike
 */

/** @param {DoEnv} env */
export function redisClient(env) {
  return createRedisClient(env, "registry_unavailable", "DO owner registry is not configured");
}

/** @param {DoEnv} env */
export function ownerTtlSeconds(env) {
  const raw = Number(envValueOr(env.DO_OWNER_TTL_SECONDS, DEFAULT_OWNER_TTL_SECONDS));
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_OWNER_TTL_SECONDS;
}

/** @param {DoEnv} env */
export function ownerLeaseGuardMs(env) {
  const raw = Number(envValueOr(env.DO_OWNER_LEASE_GUARD_MS, DEFAULT_OWNER_LEASE_GUARD_MS));
  return Number.isFinite(raw) && raw >= 0 ? Math.trunc(raw) : DEFAULT_OWNER_LEASE_GUARD_MS;
}

/** @param {DoEnv} env */
export function renewConcurrency(env) {
  return boundedPositiveIntEnv(env, "DO_RENEW_CONCURRENCY", DEFAULT_RENEW_CONCURRENCY, MAX_RENEW_CONCURRENCY);
}

/** @param {string} ownerKey */
export function ownerKeyOf(ownerKey) {
  return ownerProtocolKeys(DO_OWNER_SCOPE_PREFIX, ownerKey).ownerKey;
}

/** @param {string} ownerKey */
export function ownerGenerationKeyOf(ownerKey) {
  return ownerProtocolKeys(DO_OWNER_SCOPE_PREFIX, ownerKey).generationKey;
}

/** @param {unknown} value @returns {value is BundleScope & Record<string, unknown>} */
function isValidBundleScope(value) {
  const scope = /** @type {Record<string, unknown>} */ (value);
  return isValidRuntimeLoadNs(scope?.ns) &&
    typeof scope?.worker === "string" &&
    WORKER_NAME_RE.test(scope.worker);
}

/** @param {unknown} value @returns {value is StorageScope & Record<string, unknown>} */
function isValidStorageScope(value) {
  const scope = /** @type {Record<string, unknown>} */ (value);
  return isValidBundleScope(scope) && typeof scope?.doStorageId === "string" && Boolean(scope.doStorageId);
}

/** @param {DoInvoke} invoke @returns {InvokeScope} */
function requireInvokeScope(invoke) {
  const record = /** @type {Record<string, unknown>} */ (invoke);
  if (
    typeof record.hostId !== "string" || !record.hostId ||
    !isValidBundleScope(record) ||
    typeof record.doStorageId !== "string" || !record.doStorageId
  ) {
    throw new DoRuntimeError(400, "invalid_request", "DO owner request has missing or invalid bundle scope");
  }
  return {
    hostId: record.hostId,
    className: typeof record.className === "string" ? record.className : undefined,
    ns: record.ns,
    worker: record.worker,
    doStorageId: record.doStorageId,
  };
}

/**
 * @param {DoOwner | null} owner
 * @param {string} expectedOwnerKey
 * @param {BundleScope | null | undefined} expectedBundleScope
 */
function ownerMatchesScope(owner, expectedOwnerKey, expectedBundleScope) {
  if (
    !owner ||
    typeof owner.ownerKey !== "string" || owner.ownerKey !== expectedOwnerKey ||
    typeof owner.hostId !== "string" || owner.hostId !== expectedOwnerKey ||
    typeof owner.className !== "string" || !owner.className ||
    !isValidBundleScope(owner) ||
    typeof owner.doStorageId !== "string" || !owner.doStorageId
  ) return false;
  if (
    expectedBundleScope &&
    (!isValidBundleScope(expectedBundleScope) ||
      owner.ns !== expectedBundleScope.ns ||
      owner.worker !== expectedBundleScope.worker)
  ) return false;
  const shardMarker = ":shard";
  const markerIndex = expectedOwnerKey.lastIndexOf(shardMarker);
  if (markerIndex < 0) return false;
  const shard = Number(expectedOwnerKey.slice(markerIndex + shardMarker.length));
  try {
    return hostIdForShard(owner.doStorageId, owner.className, shard) === expectedOwnerKey;
  } catch {
    return false;
  }
}

/**
 * @param {unknown} raw
 * @param {string} expectedOwnerKey
 * @param {BundleScope | null} [expectedBundleScope]
 * @returns {DoOwner | null}
 */
export function parseOwner(raw, expectedOwnerKey, expectedBundleScope = null) {
  if (raw == null) return null;
  const owner = /** @type {DoOwner | null} */ (
    parseOwnerRecord(
      /** @type {string | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined} */ (raw)
    )
  );
  if (
    !owner ||
    !ownerMatchesScope(owner, expectedOwnerKey, expectedBundleScope) ||
    typeof owner.taskId !== "string" || !owner.taskId ||
    !validOwnerEndpointForService(owner.endpoint, DO_OWNER_PORT, "do-runtime")
  ) {
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_UNAVAILABLE, "DO owner record is invalid");
  }
  return owner;
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {LocalTask} localTask
 * @param {number} generation
 * @param {number} nowMs
 * @returns {DoOwner}
 */
function ownerRecordFor(env, invoke, localTask, generation, nowMs) {
  const ttl = ownerTtlSeconds(env);
  const scope = requireInvokeScope(invoke);
  return {
    ownerKey: scope.hostId,
    hostId: scope.hostId,
    className: scope.className,
    ns: scope.ns,
    worker: scope.worker,
    doStorageId: scope.doStorageId,
    taskId: localTask.taskId,
    endpoint: localTask.endpoint,
    generation,
    leaseExpiresAt: ownerLeaseExpiresAt(nowMs, ttl),
  };
}

/**
 * @param {DoEnv} env
 * @param {DoOwner} current
 * @param {LocalTask} localTask
 * @param {number} nowMs
 * @returns {DoOwner}
 */
function renewedOwnerRecordFor(env, current, localTask, nowMs) {
  const ttl = ownerTtlSeconds(env);
  return {
    ...current,
    taskId: localTask.taskId,
    endpoint: localTask.endpoint,
    leaseExpiresAt: ownerLeaseExpiresAt(nowMs, ttl),
  };
}

/**
 * @param {RedisLike} client
 * @param {string} ownerKey
 * @returns {Promise<DoOwner | null>}
 */
async function readOwnerFromClient(client, ownerKey) {
  return await readOwnerRecord(client, ownerKeyOf(ownerKey), (raw) => parseOwner(raw, ownerKey));
}

/**
 * @param {unknown} raw
 * @param {Pick<DoOwner, "doStorageId"> | null | undefined} owner
 */
function ownerStoragePointerMatches(raw, owner) {
  return typeof owner?.doStorageId === "string" &&
    owner.doStorageId !== "" &&
    decodeBulk(raw) === owner.doStorageId;
}

/** @param {DoOwner | null | undefined} owner */
function ownerHasStoragePointer(owner) {
  return Boolean(owner?.ns && owner.worker && owner.doStorageId);
}

/**
 * @param {DoEnv} env
 * @param {string} ownerKey
 */
export async function readOwner(env, ownerKey) {
  return await readOwnerFromClient(redisClient(env), ownerKey);
}

/**
 * @param {DoEnv} env
 * @param {DoOwner | null | undefined} owner
 * @param {number} now
 */
export function shouldRenewOwnerLease(env, owner, now) {
  if (!Number.isSafeInteger(now) || now < 0) {
    throw new Error("Owner lease renewal time base is invalid");
  }
  const ttlMs = ownerTtlSeconds(env) * 1000;
  const remainingMs = Number(owner?.leaseExpiresAt ?? 0) - now;
  return remainingMs <= ttlMs * OWNER_RENEW_FRACTION;
}

/** @param {DoOwner} owner */
function rememberOwner(owner) {
  ownedScopes.set(owner.ownerKey, owner);
}

/** @param {string} ownerKey */
export function forgetOwnedScope(ownerKey) {
  ownedScopes.delete(ownerKey);
}

/** @param {string} outcome */
function recordOwnerResolution(outcome) {
  metrics.increment("do_owner_resolutions", { service: SERVICE, outcome });
}

/**
 * @param {DoOwner} owner
 * @param {string} reason
 */
function logLeaseLossWithInFlight(owner, reason) {
  // Renew observes owner loss; per-dispatch watchdogs enforce the fence for
  // in-flight handlers. This log is operator visibility, not state transition.
  if (currentInFlightDispatches() === 0) return;
  log("error", "do_owner_lease_lost_with_in_flight_dispatches", {
    owner_key: owner.ownerKey,
    owner_task_id: owner.taskId,
    generation: owner.generation,
    reason,
    in_flight: currentInFlightDispatches(),
  });
}

/**
 * @template T
 * @param {() => Promise<T>} operation
 * @param {string} exhaustedCode
 * @param {string} exhaustedMessage
 */
async function withWatchRetries(operation, exhaustedCode, exhaustedMessage) {
  return await withOwnerWatchRetries(operation, {
    retries: OWNER_CLAIM_RETRIES,
    isWatchError: (err) => err instanceof WatchError,
    createError: (status, code, message) => new DoRuntimeError(status, code, message),
    exhaustedCode,
    exhaustedMessage,
  });
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @returns {Promise<DoOwner>}
 */
export async function resolveDoOwner(env, invoke) {
  const localTask = await resolveTaskIdentity(env);
  const client = redisClient(env);
  const scope = requireInvokeScope(invoke);
  const ownerKey = buildOwnerKey(scope);
  const key = ownerKeyOf(ownerKey);
  const generationKey = ownerGenerationKeyOf(ownerKey);
  const workerDeleteLockKey = deleteLockKey(scope.ns, scope.worker);
  const storagePointerKey = doStorageIdKey(scope.ns, scope.worker);

  const { owner: snapshotOwner, relatedValues, nowMs: snapshotNowMs } = await readOwnerSnapshotWithRedisTime(
    client,
    key,
    [generationKey, workerDeleteLockKey, storagePointerKey],
    (raw) => parseOwner(raw, ownerKey, scope)
  );
  const [generationRaw, deleteLockRaw, storagePointerRaw] = relatedValues;
  const deleteLockToken = decodeBulk(deleteLockRaw);
  if (
    deleteLockToken != null &&
    parseDeleteLockKind(deleteLockToken) !== VERSION_DELETE_LOCK_KIND
  ) {
    forgetOwnedScope(ownerKey);
    throw new DoRuntimeError(
      503,
      DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE,
      `DO scope ${ownerKey} worker is being deleted`
    );
  }
  if (snapshotOwner && !ownerLeaseExpired(snapshotOwner, snapshotNowMs)) {
    if (!ownerStoragePointerMatches(storagePointerRaw, snapshotOwner)) {
      forgetOwnedScope(ownerKey);
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE, `DO scope ${ownerKey} no longer matches active worker storage`);
    }
    if (snapshotOwner.taskId !== localTask.taskId) {
      forgetOwnedScope(ownerKey);
      recordOwnerResolution("remote");
      return snapshotOwner;
    }
    if (isDraining()) {
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
    }
    const alreadyLocal = ownedScopes.has(ownerKey);
    const counter = alreadyLocal
      ? snapshotOwner.generation
      : parseOwnerGenerationCounter(
        /** @type {string | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined} */ (generationRaw),
        generationKey
      );
    if (
      counter >= snapshotOwner.generation &&
      !shouldRenewOwnerLease(env, snapshotOwner, snapshotNowMs)
    ) {
      rememberOwner(snapshotOwner);
      recordOwnerResolution("local");
      return snapshotOwner;
    }
  } else if (isDraining()) {
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
  }

  return await withWatchRetries(async () => client.session(async (session) => {
    await session.watch(key, generationKey, workerDeleteLockKey, storagePointerKey);
    const { owner: current, relatedValues, nowMs } = await readOwnerSnapshotWithRedisTime(
      session,
      key,
      [generationKey, workerDeleteLockKey, storagePointerKey],
      (raw) => parseOwner(raw, ownerKey, scope)
    );
    const [generationRaw, deleteLockRaw, storagePointerRaw] = relatedValues;
    const deleteLockToken = decodeBulk(deleteLockRaw);
    if (
      deleteLockToken != null &&
      parseDeleteLockKind(deleteLockToken) !== VERSION_DELETE_LOCK_KIND
    ) {
      await session.unwatch();
      forgetOwnedScope(ownerKey);
      throw new DoRuntimeError(
        503,
        DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE,
        `DO scope ${ownerKey} worker is being deleted`
      );
    }
    if (current && !ownerLeaseExpired(current, nowMs)) {
      if (!ownerStoragePointerMatches(storagePointerRaw, current)) {
        await session.unwatch();
        forgetOwnedScope(ownerKey);
        throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE, `DO scope ${ownerKey} no longer matches active worker storage`);
      }
      if (current.taskId !== localTask.taskId) {
        await session.unwatch();
        forgetOwnedScope(ownerKey);
        recordOwnerResolution("remote");
        return current;
      }
      if (isDraining()) {
        throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
      }
      const alreadyLocal = ownedScopes.has(ownerKey);
      const counter = alreadyLocal
        ? current.generation
        : parseOwnerGenerationCounter(generationRaw, generationKey);
      const repairGeneration = counter < current.generation;
      const renewLease = shouldRenewOwnerLease(env, current, nowMs);
      if (!repairGeneration && !renewLease) {
        await session.unwatch();
        rememberOwner(current);
        recordOwnerResolution("local");
        return current;
      }
      const renewed = renewLease ? ownerRecordFor(env, invoke, localTask, current.generation, nowMs) : current;
      const multi = session.multi();
      if (repairGeneration) multi.set(generationKey, String(current.generation));
      if (renewLease) stageOwnerRenew(multi, key, renewed, ownerTtlSeconds(env));
      await multi.exec();
      rememberOwner(renewed);
      recordOwnerResolution(renewLease ? "local_renewed" : "local");
      return renewed;
    }

    if (isDraining()) {
      throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.TASK_DRAINING, "DO task is draining");
    }
    if (!ownerStoragePointerMatches(storagePointerRaw, scope)) {
      await session.unwatch();
      forgetOwnedScope(ownerKey);
      throw new DoRuntimeError(
        503,
        DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE,
        `DO scope ${ownerKey} no longer matches active worker storage`
      );
    }
    const generation = nextOwnerGenerationFromSnapshot(
      generationRaw,
      generationKey,
      current?.generation
    );
    const owner = ownerRecordFor(env, invoke, localTask, generation, nowMs);
    await stageOwnerClaim(session.multi(), { ownerKey: key, generationKey }, owner, ownerTtlSeconds(env)).exec();
    rememberOwner(owner);
    recordOwnerResolution(current ? "takeover" : "claimed");
    log(current ? "warn" : "info", current ? "do_owner_takeover" : "do_owner_claimed", {
      namespace: scope.ns,
      worker: scope.worker,
      class_name: scope.className,
      previous_owner_task_id: current?.taskId,
      owner_task_id: owner.taskId,
      generation,
    });
    return owner;
  }), DO_OWNERSHIP_CODE.OWNER_CLAIM_RACED, `failed to resolve DO owner for ${ownerKey}`);
}

/**
 * @param {DoEnv} env
 * @param {OwnerFence | null | undefined} owner
 * @param {{ renewNearExpiry?: boolean, storageScope: StorageScope }} options
 * @returns {Promise<{ owner: DoOwner, leaseRemainingMs: number }>}
 */
export async function assertCurrentOwnerWithLeaseBudget(env, owner, options) {
  if (!owner?.ownerKey || !owner.taskId || owner.generation == null) {
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING, "DO host request is missing an owner generation fence");
  }
  const client = redisClient(env);
  const storageScope = options.storageScope;
  if (!isValidStorageScope(storageScope)) {
    throw new Error("DO owner assertion requires a valid storage scope");
  }
  const snapshot = await readOwnerSnapshotWithRedisTime(
    client,
    ownerKeyOf(owner.ownerKey),
    [doStorageIdKey(storageScope.ns, storageScope.worker)],
    (raw) => parseOwner(raw, owner.ownerKey, storageScope)
  );
  const current = snapshot.owner;
  const nowMs = snapshot.nowMs;
  const storagePointerCurrent =
    current?.doStorageId === storageScope.doStorageId &&
    ownerStoragePointerMatches(snapshot.relatedValues[0], current);
  if (!current || !ownerFenceMatches(current, owner)) {
    forgetOwnedScope(owner.ownerKey);
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.STALE_OWNER_GENERATION, `DO scope ${owner.ownerKey} owner generation is stale`);
  }
  if (ownerLeaseExpired(current, nowMs)) {
    forgetOwnedScope(owner.ownerKey);
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_LEASE_EXPIRED, `DO scope ${owner.ownerKey} owner lease has expired`);
  }
  if (!storagePointerCurrent) {
    forgetOwnedScope(owner.ownerKey);
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE, `DO scope ${owner.ownerKey} no longer matches active worker storage`);
  }
  const leaseRemainingMs = Number(current.leaseExpiresAt ?? 0) - nowMs;
  if (leaseRemainingMs < ownerLeaseGuardMs(env)) {
    if (options.renewNearExpiry !== false) {
      const renewed = await renewOwner(env, current);
      const renewedLeaseRemainingMs = Number(renewed.owner?.leaseExpiresAt ?? 0) - renewed.nowMs;
      if (
        renewed.renewed &&
        renewed.owner &&
        ownerFenceMatches(renewed.owner, owner) &&
        renewedLeaseRemainingMs >= ownerLeaseGuardMs(env)
      ) {
        return {
          owner: renewed.owner,
          leaseRemainingMs: renewedLeaseRemainingMs,
        };
      }
    }
    forgetOwnedScope(owner.ownerKey);
    throw new DoRuntimeError(503, DO_OWNERSHIP_CODE.OWNER_LEASE_TOO_SHORT, `DO scope ${owner.ownerKey} owner lease has insufficient remaining budget`);
  }
  return {
    owner: current,
    leaseRemainingMs,
  };
}

/**
 * @param {DoEnv} env
 * @param {DoOwner} owner
 */
export async function renewOwner(env, owner) {
  const localTask = await resolveTaskIdentity(env);
  const client = redisClient(env);
  const key = ownerKeyOf(owner.ownerKey);
  const storagePointerKey = doStorageIdKey(owner.ns, owner.worker);
  for (let attempt = 0; attempt < OWNER_CLAIM_RETRIES; attempt += 1) {
    const { owner: current, rawOwner, relatedValues, nowMs } = await readOwnerSnapshotWithRedisTime(
      client,
      key,
      [storagePointerKey],
      (raw) => parseOwner(raw, owner.ownerKey, owner)
    );
    if (!current || !ownerFenceMatches(current, owner)) {
      forgetOwnedScope(owner.ownerKey);
      return { renewed: false, owner: current, nowMs };
    }
    if (!ownerHasStoragePointer(current)) {
      forgetOwnedScope(owner.ownerKey);
      return { renewed: false, owner: current, nowMs };
    }
    if (!ownerStoragePointerMatches(relatedValues[0], current)) {
      forgetOwnedScope(owner.ownerKey);
      return { renewed: false, owner: current, nowMs };
    }
    if (ownerLeaseExpired(current, nowMs)) {
      forgetOwnedScope(owner.ownerKey);
      return { renewed: false, owner: current, nowMs };
    }
    const renewed = renewedOwnerRecordFor(env, current, localTask, nowMs);
    const expected = rawOwner instanceof ArrayBuffer ? new Uint8Array(rawOwner) : rawOwner;
    if (expected == null) break;
    const result = await client.eval(
      RENEW_OWNER_WITH_STORAGE_FENCE_SCRIPT,
      [key, storagePointerKey],
      [expected, current.doStorageId, JSON.stringify(renewed), String(ownerTtlSeconds(env))]
    );
    if (result === 1) {
      rememberOwner(renewed);
      return { renewed: true, owner: renewed, nowMs };
    }
    if (result === -1) {
      forgetOwnedScope(owner.ownerKey);
      return { renewed: false, owner: current, nowMs };
    }
  }
  throw new DoRuntimeError(
    503,
    DO_OWNERSHIP_CODE.OWNER_RENEW_RACED,
    `failed to renew DO owner for ${owner.ownerKey}`
  );
}

/** @param {DoEnv} env */
export async function renewOwnedScopes(env) {
  const entries = Array.from(ownedScopes.values());
  if (isDraining()) {
    return {
      draining: true,
      owned: entries.length,
      renewed: 0,
      lost: 0,
      errors: [],
    };
  }

  let renewed = 0;
  let lost = 0;
  /** @type {Array<{ ownerKey: string, error: string }>} */
  const errors = [];
  let nextIndex = 0;
  async function renewNextScope() {
    while (true) {
      const index = nextIndex++;
      if (index >= entries.length) return;
      const owner = entries[index];
      try {
        const result = await renewOwner(env, owner);
        if (result.renewed) renewed += 1;
        else {
          lost += 1;
          logLeaseLossWithInFlight(owner, "lost_owner");
          metrics.increment("do_lease_renew_failures", { service: SERVICE, reason: "lost_owner" });
        }
      } catch (err) {
        errors.push({
          ownerKey: owner.ownerKey,
          error: errorMessage(err),
        });
        logLeaseLossWithInFlight(owner, "error");
        metrics.increment("do_lease_renew_failures", { service: SERVICE, reason: "error" });
      }
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(renewConcurrency(env), entries.length) },
      () => renewNextScope()
    )
  );
  return {
    draining: false,
    owned: ownedScopes.size,
    renewed,
    lost,
    errors,
  };
}

/** @param {DoEnv} env @param {DoOwner[]} owners */
async function releaseOwners(env, owners) {
  const results = await releaseOwnerRecords(
    redisClient(env),
    owners.map((owner) => ({ ownerKey: ownerKeyOf(owner.ownerKey), expected: owner })),
    (raw, expected) => parseOwner(raw, expected.ownerKey)
  );
  return results.map((result, index) => {
    const expected = owners[index];
    if (result.error) return { expected, result: null, error: result.error };
    forgetOwnedScope(expected.ownerKey);
    return { expected, result, error: null };
  });
}

/** @param {DoEnv} env */
export async function drainOwnedScopes(env) {
  const entries = Array.from(ownedScopes.values());
  let released = 0;
  let alreadyLost = 0;
  /** @type {Array<{ ownerKey: string, error: string }>} */
  const errors = [];
  let outcomes;
  try {
    outcomes = await releaseOwners(env, entries);
  } catch (err) {
    outcomes = entries.map((expected) => ({ expected, result: null, error: err }));
  }
  for (const outcome of outcomes) {
    if (outcome.result == null) {
      errors.push({ ownerKey: outcome.expected.ownerKey, error: errorMessage(outcome.error) });
    } else if (outcome.result.released) {
      released += 1;
    } else {
      alreadyLost += 1;
    }
  }
  return {
    draining: isDraining(),
    owned: ownedScopes.size,
    released,
    alreadyLost,
    errors,
  };
}
