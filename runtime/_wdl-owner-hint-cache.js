// Host-only process-local owner-hint cache shared by binding adapters in this
// loader isolate. Tenant workers run in separate isolates and never receive it.

/**
 * @param {{
 *   defaultMaxEntries?: number,
 *   keyFor?: (value: unknown) => unknown,
 * }} [options]
 */
export function createOwnerHintCache({
  defaultMaxEntries = 10_000,
  keyFor = (value) => value,
} = {}) {
  /** @type {Map<unknown, unknown>} */
  const entries = new Map();
  /** @type {number | null} */
  let maxEntriesForTest = null;

  function maxEntries() {
    const override = maxEntriesForTest;
    return typeof override === "number" &&
      Number.isInteger(override) &&
      override > 0
      ? override
      : defaultMaxEntries;
  }

  /** @param {unknown} value */
  function keyOf(value) {
    const key = keyFor(value);
    return key == null ? null : key;
  }

  function trim() {
    while (entries.size > maxEntries()) {
      const oldestKey = entries.keys().next().value;
      if (oldestKey === undefined) break;
      entries.delete(oldestKey);
    }
  }

  return {
    clearForTest() {
      entries.clear();
      maxEntriesForTest = null;
    },

    /** @param {number | null} maxEntriesValue */
    setMaxEntriesForTest(maxEntriesValue) {
      entries.clear();
      maxEntriesForTest = maxEntriesValue;
    },

    /** @param {unknown} value */
    get(value) {
      const key = keyOf(value);
      if (key == null) return null;
      const hint = entries.get(key);
      if (!hint) return null;
      entries.delete(key);
      entries.set(key, hint);
      return hint;
    },

    /**
     * @param {unknown} value
     * @param {unknown} hint
     */
    set(value, hint) {
      const key = keyOf(value);
      if (key == null) return false;
      entries.delete(key);
      entries.set(key, hint);
      trim();
      return true;
    },

    /** @param {unknown} value */
    delete(value) {
      const key = keyOf(value);
      if (key == null) return false;
      return entries.delete(key);
    },
  };
}
