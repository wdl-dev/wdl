/** @param {number} ms */
export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * @template T
 * @param {Promise<T>} promise
 * @param {number} [timeoutMs]
 * @returns {Promise<{ status: "fulfilled", value: T } | { status: "rejected", reason: unknown } | { status: "pending" }>}
 */
export async function settlementWithin(promise, timeoutMs = 100) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise.then(
        (value) => ({ status: /** @type {const} */ ("fulfilled"), value }),
        (reason) => ({ status: /** @type {const} */ ("rejected"), reason }),
      ),
      new Promise((resolve) => {
        timeout = setTimeout(
          () => resolve({ status: /** @type {const} */ ("pending") }),
          timeoutMs
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/**
 * @param {string} label
 * @param {() => boolean | Promise<boolean | undefined | void>} check
 * @param {{ timeoutMs?: number, intervalMs?: number }} [opts]
 */
export async function waitUntil(label, check, { timeoutMs = 60000, intervalMs = 500 } = {}) {
  const deadline = Date.now() + timeoutMs;
  /** @type {unknown} */
  let lastErr;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (err) {
      lastErr = err;
    }
    await delay(intervalMs);
  }
  throw new Error(`timeout waiting for ${label}${lastErr instanceof Error ? `: ${lastErr.message}` : ""}`);
}
