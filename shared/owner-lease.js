import { envValueOr } from "shared-env";
import { withOptimisticRetries } from "shared-optimistic-retry";

const utf8Decoder = new TextDecoder();
const OWNER_GENERATION_COUNTER = /^(?:0|[1-9]\d*)$/;

/**
 * @typedef {{ leaseExpiresAt?: number | null }} OwnerLeaseRecord
 * @typedef {OwnerLeaseRecord & { generation: number } & Record<string, unknown>} OwnerRecord
 * @typedef {{ get(key: string): Promise<string | number | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined> }} RedisGetSession
 * @typedef {{ time(): Promise<number> }} RedisTimeClient
 * @typedef {(status: number, code: string, message: string) => Error} ErrorFactory
 */

/**
 * @param {string} prefix
 * @param {string} scope
 * @returns {string}
 */
export function ownerScopedKey(prefix, scope) {
  return `${prefix}${encodeURIComponent(scope)}`;
}

/**
 * @param {string} prefix
 * @param {string} scope
 * @returns {string}
 */
export function ownerGenerationScopedKey(prefix, scope) {
  return `${ownerScopedKey(prefix, scope)}:generation`;
}

/**
 * @param {string | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined} raw
 * @returns {OwnerRecord | null}
 */
export function parseOwnerRecord(raw) {
  if (!raw) return null;
  const text = typeof raw === "string" ? raw : utf8Decoder.decode(raw);
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const record = /** @type {Record<string, unknown>} */ (parsed);
  if (!Number.isSafeInteger(record.generation) || /** @type {number} */ (record.generation) <= 0) {
    return null;
  }
  if (
    record.leaseExpiresAt !== undefined &&
    record.leaseExpiresAt !== null &&
    (typeof record.leaseExpiresAt !== "number" || !Number.isFinite(record.leaseExpiresAt))
  ) {
    return null;
  }
  return /** @type {OwnerRecord} */ (record);
}

/**
 * @param {OwnerLeaseRecord | null | undefined} owner
 * @param {number} [now]
 * @returns {boolean}
 */
export function ownerLeaseExpired(owner, now = Date.now()) {
  const leaseExpiresAt = Number(owner?.leaseExpiresAt);
  return !Number.isFinite(leaseExpiresAt) || leaseExpiresAt <= now;
}

/**
 * @param {RedisTimeClient} client
 * @returns {Promise<number>}
 */
export async function redisServerTimeMs(client) {
  const now = await client.time();
  if (!Number.isSafeInteger(now) || now < 0) {
    throw new Error("Redis server time is invalid");
  }
  return now;
}

/**
 * @param {number} nowMs
 * @param {number} ttlSeconds
 * @returns {number}
 */
export function ownerLeaseExpiresAt(nowMs, ttlSeconds) {
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) {
    throw new Error("Owner lease time base is invalid");
  }
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error("Owner lease TTL is invalid");
  }
  const ttlMs = ttlSeconds * 1000;
  return Math.min(Number.MAX_SAFE_INTEGER, nowMs + ttlMs);
}

/**
 * @param {string | number | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined} raw
 * @param {string} generationKey
 * @returns {number}
 */
export function parseOwnerGenerationCounter(raw, generationKey) {
  if (raw === null || raw === undefined || raw === "") return 0;
  if (typeof raw === "number") {
    if (Number.isSafeInteger(raw) && raw >= 0) return raw;
    throw new Error(`Owner generation counter is corrupt: ${generationKey}`);
  }
  const text = typeof raw === "string" ? raw : utf8Decoder.decode(raw);
  if (text === "") return 0;
  if (!OWNER_GENERATION_COUNTER.test(text)) {
    throw new Error(`Owner generation counter is corrupt: ${generationKey}`);
  }
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Owner generation counter is corrupt: ${generationKey}`);
  }
  return parsed;
}

/**
 * @param {RedisGetSession} session
 * @param {string} generationKey
 * @returns {Promise<number>}
 */
export async function currentOwnerGenerationCounter(session, generationKey) {
  return parseOwnerGenerationCounter(await session.get(generationKey), generationKey);
}

/**
 * @param {string | number | Uint8Array<ArrayBufferLike> | ArrayBuffer | null | undefined} raw
 * @param {string} generationKey
 * @param {number} [currentGeneration]
 * @returns {number}
 */
export function nextOwnerGenerationFromSnapshot(raw, generationKey, currentGeneration = 0) {
  const currentCounter = parseOwnerGenerationCounter(raw, generationKey);
  const previousGeneration = Math.max(currentCounter, currentGeneration);
  if (!Number.isSafeInteger(previousGeneration) || previousGeneration >= Number.MAX_SAFE_INTEGER) {
    throw new Error(`Owner generation counter is exhausted: ${generationKey}`);
  }
  return previousGeneration + 1;
}

/**
 * @param {Record<string, unknown> | null | undefined} env
 * @param {string} name
 * @param {number} fallback
 * @param {number} max
 * @returns {number}
 */
export function boundedPositiveIntEnv(env, name, fallback, max) {
  const raw = Number(envValueOr(env?.[name], fallback));
  if (!Number.isFinite(raw) || raw <= 0) return fallback;
  const upper = Number.isFinite(max) && max > 0 ? Math.trunc(max) : Infinity;
  return Math.max(1, Math.min(Math.trunc(raw), upper));
}

/**
 * @template T
 * @param {() => Promise<T>} operation
 * @param {{
 *   retries?: number,
 *   isWatchError: (err: unknown) => boolean,
 *   createError: ErrorFactory,
 *   exhaustedCode: string,
 *   exhaustedMessage: string,
 * }} opts
 * @returns {Promise<T>}
 */
export async function withOwnerWatchRetries(operation, {
  retries = 3,
  isWatchError,
  createError,
  exhaustedCode,
  exhaustedMessage,
}) {
  const maxAttempts = Number.isInteger(retries) && retries > 0 ? retries : 1;
  return await withOptimisticRetries(
    async () => await operation(),
    {
      attempts: maxAttempts,
      isRetryableError: (err) => typeof isWatchError === "function" && isWatchError(err),
      onExhausted: () => {
        throw createError(503, exhaustedCode, exhaustedMessage);
      },
    }
  );
}
