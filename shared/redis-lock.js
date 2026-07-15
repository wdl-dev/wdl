import { randomHex } from "shared-random-id";

/**
 * @typedef {{ key: string, token: string }} TokenLock
 * @typedef {{ set(key: string, value: string, options: { nx?: boolean, ttl?: number, ifeq?: string }): Promise<unknown> }} TokenLockSetClient
 * @typedef {{ delIfEq(key: string, value: string): Promise<unknown> }} TokenLockReleaseClient
 */

/** @param {string} key @returns {TokenLock} */
export function createTokenLock(key) {
  return { key, token: randomHex(16) };
}

/**
 * @param {TokenLockSetClient} client
 * @param {TokenLock} lock
 * @param {{ ttlSeconds: number }} options
 */
export async function acquireTokenLock(client, lock, { ttlSeconds }) {
  if (!("set" in client) || typeof client.set !== "function") {
    throw new TypeError("token lock acquire requires set()");
  }
  return await client.set(lock.key, lock.token, {
    nx: true,
    ttl: ttlSeconds,
  }) === "OK";
}

/**
 * @param {TokenLockSetClient} client
 * @param {TokenLock} lock
 * @param {number} ttlSeconds
 */
export async function renewTokenLock(client, lock, ttlSeconds) {
  return await client.set(lock.key, lock.token, {
    ttl: ttlSeconds,
    ifeq: lock.token,
  }) === "OK";
}

/**
 * Release is best-effort by contract: expiry bounds a leaked lock, while a
 * release exception must not replace the operation's real success or failure.
 * @param {TokenLockReleaseClient} client
 * @param {TokenLock | null | undefined} lock
 * @param {{ onError?: (err: unknown) => void }} [options]
 */
export async function releaseTokenLock(client, lock, { onError } = {}) {
  if (!lock) return;
  try {
    await client.delIfEq(lock.key, lock.token);
  } catch (err) {
    try {
      onError?.(err);
    } catch {
      // Logging is also best-effort on the release path.
    }
  }
}
