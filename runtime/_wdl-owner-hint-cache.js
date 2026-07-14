// This module is evaluated before tenant code. Keep cache state inaccessible
// after tenant top-level evaluation mutates the shared isolate realm.
const IntrinsicMap = Map;
const IntrinsicNumber = Number;
const intrinsicReflectApply = Reflect.apply;
const intrinsicMapClear = Map.prototype.clear;
const intrinsicMapDelete = Map.prototype.delete;
const intrinsicMapGet = Map.prototype.get;
const intrinsicMapKeys = Map.prototype.keys;
const intrinsicMapSet = Map.prototype.set;
const intrinsicMapSizeGet = /** @type {(this: Map<unknown, unknown>) => number} */ (
  Object.getOwnPropertyDescriptor(Map.prototype, "size")?.get
);
const intrinsicNumberIsInteger = Number.isInteger;
const intrinsicMapIteratorNext = Object.getPrototypeOf(
  intrinsicReflectApply(intrinsicMapKeys, new IntrinsicMap(), [])
).next;

/** @param {Map<unknown, unknown>} map */
function mapSize(map) {
  return intrinsicReflectApply(intrinsicMapSizeGet, map, []);
}

/** @param {Map<unknown, unknown>} map @param {unknown} key */
function mapGet(map, key) {
  return intrinsicReflectApply(intrinsicMapGet, map, [key]);
}

/** @param {Map<unknown, unknown>} map @param {unknown} key @param {unknown} value */
function mapSet(map, key, value) {
  intrinsicReflectApply(intrinsicMapSet, map, [key, value]);
}

/** @param {Map<unknown, unknown>} map @param {unknown} key */
function mapDelete(map, key) {
  return intrinsicReflectApply(intrinsicMapDelete, map, [key]);
}

/** @param {Map<unknown, unknown>} map */
function mapClear(map) {
  intrinsicReflectApply(intrinsicMapClear, map, []);
}

/** @param {Map<unknown, unknown>} map */
function firstMapKey(map) {
  const iterator = intrinsicReflectApply(intrinsicMapKeys, map, []);
  return intrinsicReflectApply(intrinsicMapIteratorNext, iterator, []).value;
}

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
  const entries = new IntrinsicMap();
  /** @type {number | null} */
  let maxEntriesForTest = null;

  function maxEntries() {
    const override = maxEntriesForTest;
    return typeof override === "number" &&
      intrinsicReflectApply(intrinsicNumberIsInteger, IntrinsicNumber, [override]) &&
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
    while (mapSize(entries) > maxEntries()) {
      const oldestKey = firstMapKey(entries);
      if (oldestKey === undefined) break;
      mapDelete(entries, oldestKey);
    }
  }

  return {
    clearForTest() {
      mapClear(entries);
      maxEntriesForTest = null;
    },

    /** @param {number | null} maxEntriesValue */
    setMaxEntriesForTest(maxEntriesValue) {
      mapClear(entries);
      maxEntriesForTest = maxEntriesValue;
    },

    /** @param {unknown} value */
    get(value) {
      const key = keyOf(value);
      if (key == null) return null;
      const hint = mapGet(entries, key);
      if (!hint) return null;
      mapDelete(entries, key);
      mapSet(entries, key, hint);
      return hint;
    },

    /**
     * @param {unknown} value
     * @param {unknown} hint
     */
    set(value, hint) {
      const key = keyOf(value);
      if (key == null) return false;
      mapDelete(entries, key);
      mapSet(entries, key, hint);
      trim();
      return true;
    },

    /** @param {unknown} value */
    delete(value) {
      const key = keyOf(value);
      if (key == null) return false;
      return mapDelete(entries, key);
    },
  };
}
