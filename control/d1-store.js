import { errMessage, jsonError, prefixedId, requireControlRedis, runOptimistic } from "control-shared";
import {
  d1DatabaseKey,
  d1DatabaseNameKey,
  d1DatabaseReferrersKey,
  d1DatabaseTombstoneKey,
  d1DatabaseTombstonesKey,
  d1DatabasesKey,
  formatD1ReferrerBlockers,
} from "control-lib";
import { decodeBulk } from "shared-redis";
import {
  D1_DATABASE_STATE_PROVISIONAL,
  D1_DATABASE_STATE_READY,
  D1_DATABASE_STATE_TOMBSTONED,
  decodeDatabaseHash,
  isReadyDatabase,
  validateDatabaseId,
  validateDatabaseName,
  validateDatabaseRef,
} from "control-d1-model";

const D1_STORAGE_NAMESPACE = "wdl-d1-storage-v1";
const PROVISIONAL_TTL_MS = 90 * 1000;

/**
 * @typedef {import("control-d1-model").D1DatabaseRecord} D1DatabaseRecord
 * @typedef {{ hash: Record<string, string | null | undefined>, value: string | Uint8Array | null | undefined }} D1RefSnapshot
 * @typedef {{
 *   hGetAll(key: string): Promise<Record<string, string | null | undefined>>,
 *   hGetAllAndGet(hashKey: string, stringKey: string): Promise<D1RefSnapshot>,
 * }} D1RefReader
 * @typedef {{ ok: true, databaseId: string } | { ok: false, reason: string } | { ok: false, error: string }} CommitDatabaseResult
 * @typedef {{ ok: true } | { ok: false, reason: string }} ReadyDatabaseResult
 * @typedef {{ rolledBack: true } | { rolledBack: false, reason?: string }} RollbackDatabaseResult
 * @typedef {{ deleted: true, tombstoneKey: string } | { deleted: false, blockers?: unknown[], malformedReferrerCount?: number }} DeleteDatabaseResult
 */

/** @param {D1DatabaseRecord} database */
function requireDatabaseName(database) {
  if (!database.databaseName) {
    throw new Error(`D1 database ${database.databaseId} is missing databaseName`);
  }
  return database.databaseName;
}

/**
 * @param {string} ns
 * @param {string} databaseId
 */
export async function getDatabase(ns, databaseId) {
  const redis = requireControlRedis();
  return decodeDatabaseHash(await redis.hGetAll(d1DatabaseKey(ns, databaseId)));
}

/**
 * @param {string} ns
 * @param {string[]} databaseIds
 */
export async function getDatabases(ns, databaseIds) {
  if (databaseIds.length === 0) return [];
  const redis = requireControlRedis();
  const hashes = await redis.session(async (session) =>
    session.hGetAllMany(databaseIds.map((databaseId) => d1DatabaseKey(ns, databaseId)))
  );
  return hashes.map(decodeDatabaseHash);
}

export function createDatabaseId() {
  return prefixedId("d1_");
}

/**
 * @param {string} ns
 * @param {string} databaseName
 */
export async function getDatabaseIdByName(ns, databaseName) {
  const redis = requireControlRedis();
  return decodeBulk(await redis.get(d1DatabaseNameKey(ns, databaseName)));
}

/**
 * @param {D1RefReader} reader
 * @param {string} ns
 * @param {string} databaseRef
 */
export async function resolveDatabaseRefFrom(reader, ns, databaseRef) {
  const snapshot = await reader.hGetAllAndGet(
    d1DatabaseKey(ns, databaseRef),
    d1DatabaseNameKey(ns, databaseRef)
  );
  const byId = decodeDatabaseHash(snapshot.hash);
  if (byId) return isReadyDatabase(byId) ? byId : null;
  const databaseId = decodeBulk(snapshot.value);
  if (!databaseId) return null;
  const byName = decodeDatabaseHash(await reader.hGetAll(d1DatabaseKey(ns, databaseId)));
  return isReadyDatabase(byName) ? byName : null;
}

/**
 * @param {string} ns
 * @param {string} databaseRef
 */
export async function resolveDatabaseRef(ns, databaseRef) {
  try {
    validateDatabaseRef(databaseRef);
  } catch (err) {
    return { response: jsonError(400, "invalid_request", errMessage(err)) };
  }

  const redis = requireControlRedis();
  const database = await resolveDatabaseRefFrom(redis, ns, databaseRef);
  if (database) return { database };

  return {
    response: jsonError(404, "d1_database_not_found", "D1 database not found", {
      namespace: ns,
      databaseRef,
    }),
  };
}

/** @param {string} now */
function provisionalUntilFrom(now) {
  const timestamp = Date.parse(now);
  return new Date((Number.isFinite(timestamp) ? timestamp : Date.now()) + PROVISIONAL_TTL_MS).toISOString();
}

/**
 * @param {D1DatabaseRecord | null | undefined} database
 * @param {string} now
 */
export function isExpiredProvisional(database, now) {
  if (database?.state !== D1_DATABASE_STATE_PROVISIONAL) return false;
  const deadline = Date.parse(database.provisionalUntil || "");
  const current = Date.parse(now);
  return Number.isFinite(deadline) && Number.isFinite(current) && deadline <= current;
}

/**
 * @param {string} ns
 * @param {string} databaseName
 * @param {string} databaseId
 * @param {string} now
 * @returns {Promise<CommitDatabaseResult>}
 */
export async function commitDatabaseMetadata(ns, databaseName, databaseId, now) {
  const redis = requireControlRedis();
  if (typeof databaseName !== "string") {
    return { ok: false, error: "databaseName is required" };
  }
  try {
    validateDatabaseName(databaseName);
  } catch (err) {
    return { ok: false, error: errMessage(err) };
  }
  try {
    validateDatabaseId(databaseId);
  } catch (err) {
    return { ok: false, error: errMessage(err) };
  }
  let lastReason = "contention";
  return await runOptimistic(redis, {
    attempts: 5,
    onExhausted: () => ({ ok: false, reason: lastReason }),
    onWatchError: () => {
      lastReason = "contention";
    },
    shouldRetryResult: (created) => {
      if (created.ok || ("reason" in created && created.reason === "name-exists")) return false;
      lastReason = "reason" in created ? created.reason : "invalid";
      return true;
    },
  }, async (session) => {
    const key = d1DatabaseKey(ns, databaseId);
    const nameKey = d1DatabaseNameKey(ns, databaseName);
    await session.watch(key, nameKey);
    const [nameExists, idExists] = await session.existsMany([nameKey, key]);
    if (nameExists) {
      return { ok: false, reason: "name-exists" };
    }
    if (idExists) {
      return { ok: false, reason: "id-collision" };
    }
    await session.multi()
      .hSet(key, {
        databaseId,
        databaseName,
        state: D1_DATABASE_STATE_PROVISIONAL,
        provisionalUntil: provisionalUntilFrom(now),
        createdAt: now,
        updatedAt: now,
      })
      .set(nameKey, databaseId)
      .sAdd(d1DatabasesKey(ns), databaseId)
      .exec();
    return { ok: true, databaseId };
  });
}

/**
 * @param {string} ns
 * @param {D1DatabaseRecord} database
 * @param {string} now
 * @returns {Promise<ReadyDatabaseResult>}
 */
export async function markDatabaseReady(ns, database, now) {
  const redis = requireControlRedis();
  return await runOptimistic(redis, {
    attempts: 5,
    onExhausted: () => ({ ok: false, reason: "contention" }),
  }, async (session) => {
    const key = d1DatabaseKey(ns, database.databaseId);
    const nameKey = d1DatabaseNameKey(ns, requireDatabaseName(database));
    await session.watch(key, nameKey);
    const snapshot = await session.hGetAllAndGet(key, nameKey);
    const current = decodeDatabaseHash(snapshot.hash);
    const currentIdForName = decodeBulk(snapshot.value);
    if (!current || current.databaseId !== database.databaseId || current.state !== D1_DATABASE_STATE_PROVISIONAL) {
      return { ok: false, reason: "not-provisional" };
    }
    if (currentIdForName !== database.databaseId) {
      return { ok: false, reason: "alias-mismatch" };
    }
    await session.multi()
      .hSet(key, {
        state: D1_DATABASE_STATE_READY,
        updatedAt: now,
      })
      .hDel(key, "provisionalUntil")
      .exec();
    return { ok: true };
  });
}

/**
 * @param {string} ns
 * @param {D1DatabaseRecord} database
 * @returns {Promise<RollbackDatabaseResult>}
 */
export async function rollbackProvisionalDatabaseMetadata(ns, database) {
  const redis = requireControlRedis();
  return await runOptimistic(redis, {
    attempts: 5,
    onExhausted: () => ({ rolledBack: false, reason: "contention" }),
  }, async (session) => {
    const key = d1DatabaseKey(ns, database.databaseId);
    const nameKey = d1DatabaseNameKey(ns, requireDatabaseName(database));
    const referrersKey = d1DatabaseReferrersKey(ns, database.databaseId);
    await session.watch(key, nameKey, referrersKey);
    const snapshot = await session.hGetAllAndGet(key, nameKey);
    const current = decodeDatabaseHash(snapshot.hash);
    const currentIdForName = decodeBulk(snapshot.value);
    if (!current || current.databaseId !== database.databaseId || current.state !== D1_DATABASE_STATE_PROVISIONAL) {
      return { rolledBack: false };
    }
    const multi = session.multi()
      .del(key)
      .del(referrersKey)
      .sRem(d1DatabasesKey(ns), database.databaseId);
    if (currentIdForName === database.databaseId) multi.del(nameKey);
    await multi.exec();
    return { rolledBack: true };
  });
}

/**
 * @param {string} ns
 * @param {D1DatabaseRecord} database
 * @param {string} now
 * @returns {Promise<RollbackDatabaseResult>}
 */
export async function rollbackExpiredProvisionalDatabaseMetadata(ns, database, now) {
  if (!isExpiredProvisional(database, now)) return { rolledBack: false, reason: "not-expired" };
  return await rollbackProvisionalDatabaseMetadata(ns, database);
}

/**
 * @param {string} ns
 * @param {D1DatabaseRecord} database
 * @param {string} now
 * @param {string} requestId
 * @returns {Promise<DeleteDatabaseResult>}
 */
export async function deleteDatabaseMetadata(ns, database, now, requestId) {
  const redis = requireControlRedis();
  return await runOptimistic(redis, {
    attempts: 5,
    onExhausted: () => {
      throw Object.assign(new Error("deleteDatabaseMetadata: contention"), {
        code: "d1_database_delete_contention",
        namespace: ns,
        databaseId: database.databaseId,
      });
    },
  }, async (session) => {
    const key = d1DatabaseKey(ns, database.databaseId);
    const nameKey = d1DatabaseNameKey(ns, requireDatabaseName(database));
    const referrersKey = d1DatabaseReferrersKey(ns, database.databaseId);
    const tombstoneKey = d1DatabaseTombstoneKey(ns, database.databaseId);
    await session.watch(key, nameKey, referrersKey, tombstoneKey);
    const snapshot = await session.hGetAllGetSMembers(key, nameKey, referrersKey);
    const current = decodeDatabaseHash(snapshot.hash);
    const currentIdForName = decodeBulk(snapshot.value);
    if (!current || current.databaseId !== database.databaseId || current.state !== D1_DATABASE_STATE_READY) {
      return { deleted: false };
    }
    const rawBlockers = snapshot.members;
    if (rawBlockers.length > 0) {
      const { blockers, malformedReferrerCount } = formatD1ReferrerBlockers(rawBlockers);
      return { deleted: false, blockers, malformedReferrerCount };
    }
    const multi = session.multi()
      .hSet(tombstoneKey, {
        namespace: ns,
        databaseId: current.databaseId,
        databaseName: current.databaseName || "",
        dbKey: `${ns}:${current.databaseId}`,
        storageNamespace: D1_STORAGE_NAMESPACE,
        state: D1_DATABASE_STATE_TOMBSTONED,
        deletedAt: now,
        updatedAt: now,
        requestId: requestId || "",
        ownerReleaseState: "pending",
        ownerReleaseError: "",
      })
      .sAdd(d1DatabaseTombstonesKey(ns), current.databaseId)
      .del(key)
      .del(referrersKey)
      .sRem(d1DatabasesKey(ns), database.databaseId);
    if (currentIdForName === database.databaseId) multi.del(nameKey);
    await multi.exec();
    return { deleted: true, tombstoneKey };
  });
}

/**
 * @param {string} ns
 * @param {string} databaseId
 * @param {string} status
 * @param {string} errorMessage
 * @param {string} now
 */
export async function updateDatabaseTombstoneOwnerRelease(ns, databaseId, status, errorMessage, now) {
  const redis = requireControlRedis();
  const tombstoneKey = d1DatabaseTombstoneKey(ns, databaseId);
  await redis.hSet(tombstoneKey, {
    ownerReleaseState: status,
    ownerReleaseError: errorMessage || "",
    updatedAt: now,
  });
}
