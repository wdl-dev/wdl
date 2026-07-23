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
 * @typedef {{ getMany(keys: string[]): Promise<Array<string | Uint8Array | ArrayBuffer | null | undefined>>, delIfEqMany(entries: Array<[string, string | Uint8Array]>): Promise<number[]> }} OwnerReleaseClient
 */
/**
 * @template T
 * @callback OwnerParser
 * @param {string | Uint8Array | ArrayBuffer | null | undefined} raw
 * @returns {T | null}
 */

/**
 * @template {OwnerFenceRecord} T
 * @callback ScopedOwnerParser
 * @param {string | Uint8Array | ArrayBuffer | null | undefined} raw
 * @param {T} expected
 * @returns {T | null}
 */

/**
 * @template T
 * @param {string | Uint8Array | ArrayBuffer | null | undefined} value
 * @param {number} nowMs
 * @param {OwnerParser<T>} parseOwner
 * @returns {{ owner: T | null, rawOwner: string | Uint8Array | ArrayBuffer | null | undefined, nowMs: number }}
 */
function parseTimedOwnerRead(value, nowMs, parseOwner) {
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) {
    throw new Error("Redis server time is invalid");
  }
  return { owner: parseOwner(value), rawOwner: value, nowMs };
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
 * @returns {Promise<{ owner: T | null, rawOwner: string | Uint8Array | ArrayBuffer | null | undefined, nowMs: number }>}
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
 * @returns {Promise<{ owner: T | null, rawOwner: string | Uint8Array | ArrayBuffer | null | undefined, relatedValues: Array<string | Uint8Array | ArrayBuffer | null | undefined>, nowMs: number }>}
 */
export async function readOwnerSnapshotWithRedisTime(client, ownerKey, relatedKeys, parseOwner) {
  const result = await client.getManyWithTime([ownerKey, ...relatedKeys]);
  return {
    ...parseTimedOwnerRead(result.values[0], result.nowMs, parseOwner),
    relatedValues: result.values.slice(1),
  };
}

const OWNER_RELEASE_BATCH_SIZE = 256;
const OWNER_RELEASE_ATTEMPTS = 3;

/**
 * @template {OwnerFenceRecord} T
 * @param {OwnerReleaseClient} client
 * @param {Array<{ ownerKey: string, expected: T }>} entries
 * @param {ScopedOwnerParser<T>} parseOwner
 * @returns {Promise<Array<{ released: boolean, owner: T | null, error?: unknown }>>}
 */
async function releaseOwnerRecordBatch(client, entries, parseOwner) {
  if (entries.length === 0) return [];
  const rawOwners = await client.getMany(entries.map(({ ownerKey }) => ownerKey));
  if (rawOwners.length !== entries.length) {
    throw new Error("Redis owner snapshot reply count mismatch");
  }

  /** @type {Array<{ released: boolean, owner: T | null, error?: unknown } | undefined>} */
  const results = Array(entries.length);
  /** @type {Array<{ index: number, ownerKey: string, rawOwner: string | Uint8Array }>} */
  let candidates = [];
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    const rawOwner = rawOwners[index];
    try {
      const owner = parseOwner(rawOwner, entry.expected);
      if (!ownerFenceMatches(owner, entry.expected) || rawOwner == null) {
        results[index] = { released: false, owner };
        continue;
      }
      candidates.push({
        index,
        ownerKey: entry.ownerKey,
        rawOwner: rawOwner instanceof ArrayBuffer ? new Uint8Array(rawOwner) : rawOwner,
      });
    } catch (error) {
      results[index] = { released: false, owner: null, error };
    }
  }

  for (let attempt = 0; candidates.length > 0; attempt += 1) {
    /** @type {number[]} */
    let released;
    try {
      released = await client.delIfEqMany(
        candidates.map(({ ownerKey, rawOwner }) => [ownerKey, rawOwner])
      );
      if (released.length !== candidates.length) {
        throw new Error("Redis owner release reply count mismatch");
      }
    } catch (error) {
      for (const candidate of candidates) {
        results[candidate.index] = { released: false, owner: null, error };
      }
      break;
    }

    const raced = candidates.filter((_candidate, index) => released[index] !== 1);
    for (let index = 0; index < candidates.length; index += 1) {
      if (released[index] === 1) {
        results[candidates[index].index] = { released: true, owner: null };
      }
    }
    if (raced.length === 0) break;

    /** @type {Array<string | Uint8Array | ArrayBuffer | null | undefined>} */
    let currentOwners;
    try {
      currentOwners = await client.getMany(raced.map(({ ownerKey }) => ownerKey));
      if (currentOwners.length !== raced.length) {
        throw new Error("Redis owner race reply count mismatch");
      }
    } catch (error) {
      for (const candidate of raced) {
        results[candidate.index] = { released: false, owner: null, error };
      }
      break;
    }
    candidates = [];
    for (let index = 0; index < raced.length; index += 1) {
      const candidate = raced[index];
      const entry = entries[candidate.index];
      const rawOwner = currentOwners[index];
      try {
        const owner = parseOwner(rawOwner, entry.expected);
        if (!ownerFenceMatches(owner, entry.expected) || rawOwner == null) {
          results[candidate.index] = { released: false, owner };
        } else if (attempt + 1 >= OWNER_RELEASE_ATTEMPTS) {
          results[candidate.index] = {
            released: false,
            owner: null,
            error: new Error("Owner release raced after bounded retries"),
          };
        } else {
          candidates.push({
            index: candidate.index,
            ownerKey: candidate.ownerKey,
            rawOwner: rawOwner instanceof ArrayBuffer ? new Uint8Array(rawOwner) : rawOwner,
          });
        }
      } catch (error) {
        results[candidate.index] = { released: false, owner: null, error };
      }
    }
  }

  return /** @type {Array<{ released: boolean, owner: T | null, error?: unknown }>} */ (results);
}

/**
 * Delete owner records only when their exact stored bytes still match the
 * fence-validated snapshot. Failed comparisons are reread; lease-only updates
 * under the same fence receive bounded retries, while a new fence is reported
 * to the caller. Fixed-size batches bound request and reply buffers while
 * preserving each independent owner CAS.
 *
 * @template {OwnerFenceRecord} T
 * @param {OwnerReleaseClient} client
 * @param {Array<{ ownerKey: string, expected: T }>} entries
 * @param {ScopedOwnerParser<T>} parseOwner
 * @returns {Promise<Array<{ released: boolean, owner: T | null, error?: unknown }>>}
 */
export async function releaseOwnerRecords(client, entries, parseOwner) {
  /** @type {Array<{ released: boolean, owner: T | null, error?: unknown }>} */
  const results = [];
  for (let offset = 0; offset < entries.length; offset += OWNER_RELEASE_BATCH_SIZE) {
    const batch = entries.slice(offset, offset + OWNER_RELEASE_BATCH_SIZE);
    try {
      results.push(...await releaseOwnerRecordBatch(client, batch, parseOwner));
    } catch (error) {
      results.push(...batch.map(() => ({ released: false, owner: null, error })));
    }
  }
  return results;
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
