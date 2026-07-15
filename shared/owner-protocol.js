import {
  ownerGenerationScopedKey,
  ownerScopedKey,
} from "shared-owner-lease";

/** @typedef {{ ownerKey: string, generationKey: string }} OwnerProtocolKeys */
/** @typedef {{ taskId?: string, generation?: number }} OwnerFenceRecord */
/** @typedef {{ ttl?: number }} OwnerSetOptions */
/**
 * @typedef {{
 *   set(key: string, value: string, options?: OwnerSetOptions): OwnerWriteMulti,
 *   del(...keys: string[]): OwnerWriteMulti,
 *   exec(): Promise<unknown>,
 * }} OwnerWriteMulti
 * @typedef {{ value: string | Uint8Array | ArrayBuffer | null | undefined, nowMs: number }} OwnerReadWithTimeResult
 * @typedef {{ values: Array<string | Uint8Array | ArrayBuffer | null | undefined>, nowMs: number }} OwnerSnapshotReadWithTimeResult
 */
/**
 * @template T
 * @callback OwnerParser
 * @param {string | Uint8Array | ArrayBuffer | null | undefined} raw
 * @returns {T | null}
 */

/**
 * @template T
 * @param {string | Uint8Array | ArrayBuffer | null | undefined} value
 * @param {number} nowMs
 * @param {OwnerParser<T>} parseOwner
 * @returns {{ owner: T | null, nowMs: number }}
 */
function parseTimedOwnerRead(value, nowMs, parseOwner) {
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) {
    throw new Error("Redis server time is invalid");
  }
  return { owner: parseOwner(value), nowMs };
}

/**
 * @param {string} prefix
 * @param {string} scope
 * @returns {OwnerProtocolKeys}
 */
export function ownerProtocolKeys(prefix, scope) {
  return {
    ownerKey: ownerScopedKey(prefix, scope),
    generationKey: ownerGenerationScopedKey(prefix, scope),
  };
}

/**
 * @param {OwnerFenceRecord | null | undefined} current
 * @param {OwnerFenceRecord | null | undefined} expected
 */
export function ownerFenceMatches(current, expected) {
  return Boolean(
    current &&
    expected &&
    typeof current.taskId === "string" &&
    current.taskId === expected.taskId &&
    typeof current.generation === "number" &&
    current.generation === expected.generation
  );
}

/**
 * @template T
 * @param {{ get(key: string): Promise<string | Uint8Array | ArrayBuffer | null | undefined> }} client
 * @param {string} ownerKey
 * @param {OwnerParser<T>} parseOwner
 * @returns {Promise<T | null>}
 */
export async function readOwnerRecord(client, ownerKey, parseOwner) {
  return parseOwner(await client.get(ownerKey));
}

/**
 * @template T
 * @param {{ getWithTime(key: string): Promise<OwnerReadWithTimeResult> }} client
 * @param {string} ownerKey
 * @param {OwnerParser<T>} parseOwner
 * @returns {Promise<{ owner: T | null, nowMs: number }>}
 */
export async function readOwnerRecordWithRedisTime(client, ownerKey, parseOwner) {
  const result = await client.getWithTime(ownerKey);
  return parseTimedOwnerRead(result.value, result.nowMs, parseOwner);
}

/**
 * @template T
 * @param {{ getManyWithTime(keys: string[]): Promise<OwnerSnapshotReadWithTimeResult> }} client
 * @param {string} ownerKey
 * @param {string[]} relatedKeys
 * @param {OwnerParser<T>} parseOwner
 * @returns {Promise<{ owner: T | null, relatedValues: Array<string | Uint8Array | ArrayBuffer | null | undefined>, nowMs: number }>}
 */
export async function readOwnerSnapshotWithRedisTime(client, ownerKey, relatedKeys, parseOwner) {
  const result = await client.getManyWithTime([ownerKey, ...relatedKeys]);
  return {
    ...parseTimedOwnerRead(result.values[0], result.nowMs, parseOwner),
    relatedValues: result.values.slice(1),
  };
}

/**
 * @param {OwnerWriteMulti} multi
 * @param {OwnerProtocolKeys} keys
 * @param {{ generation: number } & Record<string, unknown>} owner
 * @param {number} ttlSeconds
 * @returns {OwnerWriteMulti}
 */
export function stageOwnerClaim(multi, keys, owner, ttlSeconds) {
  return multi
    .set(keys.generationKey, String(owner.generation))
    .set(keys.ownerKey, JSON.stringify(owner), { ttl: ttlSeconds });
}

/**
 * @param {OwnerWriteMulti} multi
 * @param {string} ownerKey
 * @param {Record<string, unknown>} owner
 * @param {number} ttlSeconds
 * @returns {OwnerWriteMulti}
 */
export function stageOwnerRenew(multi, ownerKey, owner, ttlSeconds) {
  return multi.set(ownerKey, JSON.stringify(owner), { ttl: ttlSeconds });
}

/**
 * @param {OwnerWriteMulti} multi
 * @param {string} ownerKey
 * @returns {OwnerWriteMulti}
 */
export function stageOwnerRelease(multi, ownerKey) {
  return multi.del(ownerKey);
}
