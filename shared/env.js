const CANONICAL_POSITIVE_DECIMAL = /^[1-9]\d*$/;

/**
 * Treat nullish and explicitly-empty env values as absent while preserving
 * numeric zero and other falsy-but-meaningful values.
 *
 * @template T
 * @param {T | null | undefined | ""} value
 * @param {T} fallback
 * @returns {T}
 */
export function envValueOr(value, fallback) {
  return value == null || value === "" ? fallback : value;
}

/**
 * @param {Record<string, unknown> | null | undefined} env
 * @param {string} name
 * @param {number} fallback
 * @param {number} max
 * @returns {number}
 */
export function canonicalPositiveIntegerEnv(env, name, fallback, max) {
  const raw = env?.[name];
  if (typeof raw !== "string" || !CANONICAL_POSITIVE_DECIMAL.test(raw)) return fallback;
  const value = Number(raw);
  return Number.isSafeInteger(value) && value <= max ? value : fallback;
}
