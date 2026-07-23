import {
  moduleDataUrl,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../load-shared-module.js";

const SHARED_OBSERVABILITY_URL = repositoryFileUrl("shared/observability.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const SHARED_REDIS_RESP_URL = repositoryModuleDataUrl("shared/redis-resp.js", [
  [/from "shared-observability";/, `from ${JSON.stringify(SHARED_OBSERVABILITY_URL)};`],
  [/from "\.\/errors\.js";/, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
]);
const utf8Encoder = new TextEncoder();

/** @typedef {Map<string, string>} FakeRedisStrings */
/** @typedef {Map<string, Record<string, string>>} FakeRedisHashes */
/** @typedef {Map<string, Set<string>>} FakeRedisSets */
/** @typedef {Map<string, Map<string, number>>} FakeRedisZSets */

/**
 * @typedef {{
 *   strings: FakeRedisStrings,
 *   hashes: FakeRedisHashes,
 *   sets: FakeRedisSets,
 *   zsets: FakeRedisZSets,
 *   ops: unknown[][],
 *   watched: string[],
 *   watchBatches: string[][],
 *   commands: unknown[][],
 *   expirations: Map<string, number>,
 *   revisions: Map<string, number>,
 *   execFailures: number,
 *   nowMs: number,
 * }} FakeRedisState
 */

export class FakeRedisWatchError extends Error {
  constructor(message = "watched key changed") {
    super(message);
    this.name = "WatchError";
  }
}

/**
 * Canonical `shared-redis` test surface. Callers may append only the
 * state-bound exports their module graph needs.
 * @param {string} [extraSource]
 */
export function sharedRedisStubUrl(extraSource = "") {
  return moduleDataUrl(`
import { FakeRedisWatchError as WatchError } from ${JSON.stringify(import.meta.url)};
import { decodeBulk } from ${JSON.stringify(SHARED_REDIS_RESP_URL)};
export { WatchError, decodeBulk };
${extraSource}
`);
}

/** @returns {FakeRedisState} */
export function createFakeRedisState() {
  return {
    strings: new Map(),
    hashes: new Map(),
    sets: new Map(),
    zsets: new Map(),
    ops: [],
    watched: [],
    watchBatches: [],
    commands: [],
    expirations: new Map(),
    revisions: new Map(),
    execFailures: 0,
    nowMs: Date.now(),
  };
}

/** @param {FakeRedisState} state */
export function resetFakeRedisState(state) {
  state.strings.clear();
  state.hashes.clear();
  state.sets.clear();
  state.zsets.clear();
  state.ops.length = 0;
  state.watched.length = 0;
  state.watchBatches.length = 0;
  state.commands.length = 0;
  state.expirations.clear();
  state.revisions.clear();
  state.execFailures = 0;
  state.nowMs = Date.now();
}

/** @param {FakeRedisState} state @param {string} key @param {string} field */
function hashField(state, key, field) {
  const hash = state.hashes.get(key);
  return hash && Object.hasOwn(hash, field) ? hash[field] : null;
}

/**
 * @param {FakeRedisState} [state]
 * @param {{ encodeGet?: boolean, nowMs?: () => number, onExecFailure?: (ops: unknown[][], remainingFailures: number) => void }} [options]
 * @returns {ReturnType<typeof createFakeRedisClient> & FakeRedisState & { state: FakeRedisState }}
 */
export function createFakeRedis(state = createFakeRedisState(), options = {}) {
  return {
    state,
    strings: state.strings,
    hashes: state.hashes,
    sets: state.sets,
    zsets: state.zsets,
    expirations: state.expirations,
    revisions: state.revisions,
    get execFailures() { return state.execFailures; },
    set execFailures(value) { state.execFailures = value; },
    ops: state.ops,
    watched: state.watched,
    watchBatches: state.watchBatches,
    commands: state.commands,
    get nowMs() { return state.nowMs; },
    set nowMs(value) { state.nowMs = value; },
    ...createFakeRedisClient(state, options),
  };
}

/**
 * @param {FakeRedisState} state
 * @param {{ encodeGet?: boolean, nowMs?: () => number, onExecFailure?: (ops: unknown[][], remainingFailures: number) => void }} [options]
 */
export function createFakeRedisClient(state, options = {}) {
  return {
    /** @param {string} key */
    async get(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["get", key]);
      return encodeMaybe(state.strings.get(key) ?? null, options);
    },
    /** @param {string} key */
    async getWithTime(key) {
      return { value: await this.get(key), nowMs: await this.time() };
    },
    /** @param {string[]} keys */
    async getManyWithTime(keys) {
      if (keys.length === 0) throw new Error("getManyWithTime requires at least one key");
      state.commands.push(["getManyWithTime", [...keys]]);
      return {
        values: keys.map((key) => {
          expireIfNeeded(state, key, options);
          return encodeMaybe(state.strings.get(key) ?? null, options);
        }),
        nowMs: await this.time(),
      };
    },
    /** @param {string[]} keys */
    async getMany(keys) {
      state.commands.push(["getMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return encodeMaybe(state.strings.get(key) ?? null, options);
      });
    },
    async time() {
      return currentFakeRedisTimeMs(state, options);
    },
    /** @param {string} key */
    async incr(key) {
      state.commands.push(["incr", key]);
      const next = Number(state.strings.get(key) || 0) + 1;
      state.strings.set(key, String(next));
      markKeyModified(state, key);
      return next;
    },
    /** @param {string} key @param {string} value @param {{ nx?: boolean, ttl?: number, ifeq?: string | Uint8Array }} [setOptions] */
    async set(key, value, setOptions = {}) {
      expireIfNeeded(state, key, options);
      state.commands.push(["set", key, value, { ...setOptions }]);
      if (setOptions.nx && keyExists(state, key, options)) return null;
      if (setOptions.ifeq != null && !storedStringEquals(state.strings.get(key), setOptions.ifeq)) return null;
      state.strings.set(key, value);
      if (typeof setOptions.ttl === "number") {
        state.expirations.set(key, currentFakeRedisTimeMs(state, options) + setOptions.ttl * 1000);
      } else {
        state.expirations.delete(key);
      }
      markKeyModified(state, key);
      return "OK";
    },
    /** @param {...string} keys */
    async del(...keys) {
      state.commands.push(["del", ...keys]);
      let removed = 0;
      for (const key of keys) removed += deleteKey(state, key, options);
      return removed;
    },
    /** @param {string} key @param {string | Uint8Array} value */
    async delIfEq(key, value) {
      expireIfNeeded(state, key, options);
      state.commands.push(["delIfEq", key, value]);
      if (!storedStringEquals(state.strings.get(key), value)) return 0;
      return deleteKey(state, key, options);
    },
    /** @param {Array<[string, string | Uint8Array]>} entries */
    async delIfEqMany(entries) {
      state.commands.push(["delIfEqMany", entries.map(([key, value]) => [key, value])]);
      return entries.map(([key, value]) => {
        expireIfNeeded(state, key, options);
        if (!storedStringEquals(state.strings.get(key), value)) return 0;
        return deleteKey(state, key, options);
      });
    },
    /** @param {string} cursor @param {string} match @param {number} [count] */
    async scan(cursor, match, count = 100) {
      const result = scanKeys(state, cursor, match, count, options);
      state.commands.push(["scan", cursor, match, count, result]);
      return result;
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hGet", key, field]);
      return hashField(state, key, field);
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      state.commands.push(["hGetMany", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) => {
        expireIfNeeded(state, key, options);
        return hashField(state, key, field);
      });
    },
    /** @param {string} key @param {string[]} fields */
    async hMGet(key, fields) {
      if (fields.length === 0) return [];
      expireIfNeeded(state, key, options);
      state.commands.push(["hMGet", key, [...fields]]);
      return fields.map((field) => hashField(state, key, field));
    },
    /** @param {string} key */
    async hGetAll(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hGetAll", key]);
      return { ...(state.hashes.get(key) || {}) };
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      state.commands.push(["hGetAllMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return { ...(state.hashes.get(key) || {}) };
      });
    },
    /** @param {Array<[string, string]>} pairs */
    async hStrLenMany(pairs) {
      state.commands.push(["hStrLenMany", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) => {
        expireIfNeeded(state, key, options);
        return redisByteLength(hashField(state, key, field));
      });
    },
    /** @param {string} hashKey @param {string} stringKey */
    async hGetAllAndGet(hashKey, stringKey) {
      expireIfNeeded(state, hashKey, options);
      expireIfNeeded(state, stringKey, options);
      state.commands.push(["hGetAllAndGet", hashKey, stringKey]);
      return {
        hash: { ...(state.hashes.get(hashKey) || {}) },
        value: state.strings.get(stringKey) ?? null,
      };
    },
    /** @param {string} setKey @param {string} hashKey */
    async sMembersAndHGetAll(setKey, hashKey) {
      expireIfNeeded(state, setKey, options);
      expireIfNeeded(state, hashKey, options);
      state.commands.push(["sMembersAndHGetAll", setKey, hashKey]);
      return {
        members: [...(state.sets.get(setKey) || new Set())],
        hash: { ...(state.hashes.get(hashKey) || {}) },
      };
    },
    /** @param {string} key @param {Record<string, string>} fields */
    async hSet(key, fields) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hSet", key, { ...fields }]);
      setHashFields(state, key, fields);
    },
    /** @param {string} key @param {...string} fields */
    async hDel(key, ...fields) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hDel", key, ...fields]);
      const hash = state.hashes.get(key);
      if (!hash) return 0;
      let removed = 0;
      for (const field of fields) {
        if (Object.hasOwn(hash, field)) {
          delete hash[field];
          removed += 1;
        }
      }
      if (Object.keys(hash).length === 0) state.hashes.delete(key);
      if (removed > 0) markKeyModified(state, key);
      return removed;
    },
    /** @param {string} key */
    async hKeys(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hKeys", key]);
      return Object.keys(state.hashes.get(key) || {});
    },
    /** @param {string} key @param {string} field */
    async hExists(key, field) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hExists", key, field]);
      return Object.hasOwn(state.hashes.get(key) || {}, field);
    },
    /** @param {string} key */
    async sMembers(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sMembers", key]);
      return [...(state.sets.get(key) || new Set())];
    },
    /** @param {string[]} keys */
    async sMembersMany(keys) {
      state.commands.push(["sMembersMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return [...(state.sets.get(key) || new Set())];
      });
    },
    /** @param {string} key */
    async sCard(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sCard", key]);
      return state.sets.get(key)?.size || 0;
    },
    /** @param {string[]} keys */
    async sCardMany(keys) {
      state.commands.push(["sCardMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return state.sets.get(key)?.size || 0;
      });
    },
    /** @param {string} key @param {string} member */
    async sAdd(key, member) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sAdd", key, member]);
      const set = ensureSet(state, key);
      const before = set.size;
      set.add(member);
      if (set.size !== before) markKeyModified(state, key);
    },
    /** @param {string} key */
    async zCard(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["zCard", key]);
      return state.zsets.get(key)?.size || 0;
    },
    /** @param {string} key @param {number} start @param {number} stop */
    async zRange(key, start, stop) {
      expireIfNeeded(state, key, options);
      const result = zRangeMembers(state, key, start, stop);
      state.commands.push(["zRange", key, start, stop, result]);
      return result;
    },
    /** @param {...string} keys */
    async exists(...keys) {
      state.commands.push(["exists", ...keys]);
      return keys.reduce((count, key) => {
        expireIfNeeded(state, key, options);
        return count + (keyExists(state, key, options) ? 1 : 0);
      }, 0);
    },
    /** @param {(session: ReturnType<typeof createFakeRedisSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(createFakeRedisSession(state, options));
    },
  };
}

/**
 * @param {FakeRedisState} state
 * @param {{ nowMs?: () => number, onExecFailure?: (ops: unknown[][], remainingFailures: number) => void }} [options]
 */
export function createFakeRedisSession(state, options = {}) {
  /** @type {Map<string, { fingerprint: string, revision: number }>} */
  const watchedSnapshots = new Map();
  const watchContext = {
    assertUnchanged() {
      for (const [key, snapshot] of watchedSnapshots) {
        if (
          keyRevision(state, key) !== snapshot.revision ||
          fakeKeyFingerprint(state, key, options) !== snapshot.fingerprint
        ) {
          throw new FakeRedisWatchError();
        }
      }
    },
    clear() {
      watchedSnapshots.clear();
    },
  };
  return {
    /** @param {string[]} keys */
    async watch(...keys) {
      state.watched.push(...keys);
      state.watchBatches.push(keys);
      for (const key of keys) {
        if (!watchedSnapshots.has(key)) {
          const fingerprint = fakeKeyFingerprint(state, key, options);
          watchedSnapshots.set(key, {
            fingerprint,
            revision: keyRevision(state, key),
          });
        }
      }
    },
    async unwatch() {
      watchContext.clear();
    },
    /** @param {string} key */
    async get(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["get", key]);
      return state.strings.get(key) ?? null;
    },
    /** @param {string[]} keys */
    async getMany(keys) {
      state.commands.push(["getMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return state.strings.get(key) ?? null;
      });
    },
    /** @param {string} key */
    async getWithTime(key) {
      return { value: await this.get(key), nowMs: await this.time() };
    },
    /** @param {string[]} keys */
    async getManyWithTime(keys) {
      if (keys.length === 0) throw new Error("getManyWithTime requires at least one key");
      state.commands.push(["getManyWithTime", [...keys]]);
      return {
        values: keys.map((key) => {
          expireIfNeeded(state, key, options);
          return state.strings.get(key) ?? null;
        }),
        nowMs: await this.time(),
      };
    },
    async time() {
      return currentFakeRedisTimeMs(state, options);
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hGet", key, field]);
      return hashField(state, key, field);
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      state.commands.push(["hGetMany", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) => {
        expireIfNeeded(state, key, options);
        return hashField(state, key, field);
      });
    },
    /** @param {string} key @param {string[]} fields */
    async hMGet(key, fields) {
      if (fields.length === 0) return [];
      expireIfNeeded(state, key, options);
      state.commands.push(["hMGet", key, [...fields]]);
      return fields.map((field) => hashField(state, key, field));
    },
    /** @param {string} key */
    async hGetAll(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hGetAll", key]);
      return { ...(state.hashes.get(key) || {}) };
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      state.commands.push(["hGetAllMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return { ...(state.hashes.get(key) || {}) };
      });
    },
    /** @param {Array<[string, string]>} pairs */
    async hStrLenMany(pairs) {
      state.commands.push(["hStrLenMany", pairs.map(([key, field]) => [key, field])]);
      return pairs.map(([key, field]) => {
        expireIfNeeded(state, key, options);
        return redisByteLength(hashField(state, key, field));
      });
    },
    /** @param {string} hashKey @param {string} stringKey */
    async hGetAllAndGet(hashKey, stringKey) {
      expireIfNeeded(state, hashKey, options);
      expireIfNeeded(state, stringKey, options);
      state.commands.push(["hGetAllAndGet", hashKey, stringKey]);
      return {
        hash: { ...(state.hashes.get(hashKey) || {}) },
        value: state.strings.get(stringKey) ?? null,
      };
    },
    /** @param {string} hashKey @param {string} stringKey @param {string} setKey */
    async hGetAllGetSMembers(hashKey, stringKey, setKey) {
      expireIfNeeded(state, hashKey, options);
      expireIfNeeded(state, stringKey, options);
      expireIfNeeded(state, setKey, options);
      state.commands.push(["hGetAllGetSMembers", hashKey, stringKey, setKey]);
      return {
        hash: { ...(state.hashes.get(hashKey) || {}) },
        value: state.strings.get(stringKey) ?? null,
        members: [...(state.sets.get(setKey) || new Set())],
      };
    },
    /** @param {string} key */
    async hKeys(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hKeys", key]);
      return Object.keys(state.hashes.get(key) || {});
    },
    /** @param {string} key @param {string} field */
    async hExists(key, field) {
      expireIfNeeded(state, key, options);
      state.commands.push(["hExists", key, field]);
      return Object.hasOwn(state.hashes.get(key) || {}, field);
    },
    /** @param {...string} keys */
    async exists(...keys) {
      state.commands.push(["exists", ...keys]);
      return keys.reduce((count, key) => {
        expireIfNeeded(state, key, options);
        return count + (keyExists(state, key, options) ? 1 : 0);
      }, 0);
    },
    /** @param {string[]} keys */
    async existsMany(keys) {
      state.commands.push(["existsMany", [...keys]]);
      return keys.map((key) => keyExists(state, key, options));
    },
    /** @param {string} key @param {string[]} members */
    async sMIsMember(key, ...members) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sMIsMember", key, [...members]]);
      const set = state.sets.get(key) || new Set();
      return members.map((member) => set.has(member));
    },
    /** @param {string} key */
    async sMembers(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sMembers", key]);
      return [...(state.sets.get(key) || new Set())];
    },
    /** @param {string[]} keys */
    async sMembersMany(keys) {
      state.commands.push(["sMembersMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return [...(state.sets.get(key) || new Set())];
      });
    },
    /** @param {string} key */
    async sCard(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["sCard", key]);
      return state.sets.get(key)?.size || 0;
    },
    /** @param {string[]} keys */
    async sCardMany(keys) {
      state.commands.push(["sCardMany", [...keys]]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return state.sets.get(key)?.size || 0;
      });
    },
    /** @param {string} key */
    async zCard(key) {
      expireIfNeeded(state, key, options);
      state.commands.push(["zCard", key]);
      return state.zsets.get(key)?.size || 0;
    },
    /** @param {string} key @param {number} start @param {number} stop */
    async zRange(key, start, stop) {
      expireIfNeeded(state, key, options);
      const result = zRangeMembers(state, key, start, stop);
      state.commands.push(["zRange", key, start, stop, result]);
      return result;
    },
    /** @param {string[]} keys @param {number} start @param {number} stop */
    async zRangeMany(keys, start, stop) {
      state.commands.push(["zRangeMany", [...keys], start, stop]);
      return keys.map((key) => {
        expireIfNeeded(state, key, options);
        return zRangeMembers(state, key, start, stop);
      });
    },
    /** @param {...string} keys */
    async del(...keys) {
      state.commands.push(["del", ...keys]);
      let removed = 0;
      for (const key of keys) removed += deleteKey(state, key, options);
      return removed;
    },
    /** @param {string} key @param {string | Uint8Array} value */
    async delIfEq(key, value) {
      expireIfNeeded(state, key, options);
      state.commands.push(["delIfEq", key, value]);
      if (!storedStringEquals(state.strings.get(key), value)) return 0;
      return deleteKey(state, key, options);
    },
    /** @param {string} cursor @param {string} match @param {number} [count] */
    async scan(cursor, match, count = 100) {
      const result = scanKeys(state, cursor, match, count, options);
      state.commands.push(["scan", cursor, match, count, result]);
      return result;
    },
    /** @param {string} src @param {string} dst @param {{ REPLACE?: boolean }} [opts] */
    async copy(src, dst, opts = {}) {
      state.commands.push(["copy", src, dst, { ...opts }]);
      return copyKey(state, src, dst, opts, options);
    },
    multi() {
      return createFakeRedisMulti(state, options, watchContext);
    },
  };
}

/**
 * @param {FakeRedisState} state
 * @param {{ nowMs?: () => number, onExecFailure?: (ops: unknown[][], remainingFailures: number) => void }} [options]
 * @param {{ assertUnchanged: () => void, clear: () => void }} [watchContext]
 */
export function createFakeRedisMulti(state, options = {}, watchContext = undefined) {
  /** @type {unknown[][]} */
  const ops = [];
  const chain = {
    /** @param {string} key @param {Record<string, string> | string} fieldsOrField @param {string} [maybeValue] */
    hSet(key, fieldsOrField, maybeValue) {
      const fields = typeof fieldsOrField === "object"
        ? fieldsOrField
        : { [fieldsOrField]: maybeValue };
      ops.push(["hSet", key, { ...fields }]);
      return chain;
    },
    /** @param {string} key @param {string[]} fields */
    hDel(key, ...fields) {
      ops.push(["hDel", key, ...fields]);
      return chain;
    },
    /** @param {string} key @param {string} value @param {Record<string, unknown>} [options] */
    set(key, value, options = undefined) {
      ops.push(["set", key, value, options]);
      return chain;
    },
    /** @param {string} key */
    incr(key) {
      ops.push(["incr", key]);
      return chain;
    },
    /** @param {string[]} keys */
    del(...keys) {
      ops.push(["del", ...keys]);
      return chain;
    },
    /** @param {string} key @param {(string | string[])[]} values */
    sAdd(key, ...values) {
      for (const value of values.flat()) ops.push(["sAdd", key, value]);
      return chain;
    },
    /** @param {string} key @param {(string | string[])[]} values */
    sRem(key, ...values) {
      for (const value of values.flat()) ops.push(["sRem", key, value]);
      return chain;
    },
    /** @param {string} key @param {number} score @param {string} member */
    zAdd(key, score, member) {
      ops.push(["zAdd", key, score, member]);
      return chain;
    },
    /** @param {string} key @param {string} member */
    zRem(key, member) {
      ops.push(["zRem", key, member]);
      return chain;
    },
    /** @param {string} key @param {string} value */
    publish(key, value) {
      ops.push(["publish", key, value]);
      return chain;
    },
    /** @param {string} key @param {number} ts */
    expireAt(key, ts) {
      ops.push(["expireAt", key, ts]);
      return chain;
    },
    /** @param {string} src @param {string} dst @param {{ REPLACE?: boolean }} [opts] */
    copy(src, dst, opts = {}) {
      ops.push(["copy", src, dst, { ...opts }]);
      return chain;
    },
    async exec() {
      try {
        watchContext?.assertUnchanged();
        if (state.execFailures > 0) {
          state.execFailures -= 1;
          options.onExecFailure?.(ops, state.execFailures);
          throw new FakeRedisWatchError();
        }
        state.ops.push(...ops);
        state.commands.push(...ops);
        return ops.map((op) => applyFakeRedisOp(state, op, options));
      } finally {
        watchContext?.clear();
      }
    },
  };
  return chain;
}

/**
 * @param {FakeRedisState} state
 * @param {unknown[]} op
 * @param {{ nowMs?: () => number }} [options]
 */
export function applyFakeRedisOp(state, op, options = {}) {
  const kind = /** @type {string} */ (op[0]);
  if (kind === "del") {
    let removed = 0;
    for (const key of /** @type {string[]} */ (op.slice(1))) {
      removed += deleteKey(state, key, options);
    }
    return removed;
  }

  const key = /** @type {string} */ (op[1]);
  if (kind === "set") {
    const setOptions = /** @type {{ nx?: boolean, ttl?: number, ifeq?: string | Uint8Array } | undefined} */ (op[3]) ?? {};
    expireIfNeeded(state, key, options);
    if (setOptions.nx && keyExists(state, key, options)) return null;
    if (setOptions.ifeq != null && !storedStringEquals(state.strings.get(key), setOptions.ifeq)) return null;
    state.strings.set(key, /** @type {string} */ (op[2]));
    if (typeof setOptions.ttl === "number") {
      state.expirations.set(key, currentFakeRedisTimeMs(state, options) + setOptions.ttl * 1000);
    } else {
      state.expirations.delete(key);
    }
    markKeyModified(state, key);
    return "OK";
  }
  if (kind === "incr") {
    const next = Number(state.strings.get(key) || 0) + 1;
    state.strings.set(key, String(next));
    markKeyModified(state, key);
    return next;
  }
  if (kind === "hSet") {
    return setHashFields(state, key, /** @type {Record<string, string>} */ (op[2]));
  }
  if (kind === "hDel") {
    const hash = state.hashes.get(key);
    if (!hash) return 0;
    let removed = 0;
    for (const field of /** @type {string[]} */ (op.slice(2))) {
      if (Object.hasOwn(hash, field)) {
        delete hash[field];
        removed += 1;
      }
    }
    if (Object.keys(hash).length === 0) state.hashes.delete(key);
    if (removed > 0) markKeyModified(state, key);
    return removed;
  }
  if (kind === "copy") {
    const dst = /** @type {string} */ (op[2]);
    const copyOptions = /** @type {{ REPLACE?: boolean }} */ (
      op[3] && typeof op[3] === "object" ? op[3] : {}
    );
    return copyKey(state, key, dst, copyOptions, options);
  }
  if (kind === "sAdd") {
    const set = ensureSet(state, key);
    const before = set.size;
    set.add(/** @type {string} */ (op[2]));
    const added = set.size > before ? 1 : 0;
    if (added === 1) markKeyModified(state, key);
    return added;
  }
  if (kind === "sRem") {
    const set = state.sets.get(key);
    const removed = set?.delete(/** @type {string} */ (op[2])) ? 1 : 0;
    if (set?.size === 0) state.sets.delete(key);
    if (removed === 1) markKeyModified(state, key);
    return removed;
  }
  if (kind === "zAdd") {
    const zset = state.zsets.get(key) || new Map();
    const member = /** @type {string} */ (op[3]);
    const score = /** @type {number} */ (op[2]);
    const previous = zset.get(member);
    const added = zset.has(member) ? 0 : 1;
    zset.set(member, score);
    state.zsets.set(key, zset);
    if (previous !== score) markKeyModified(state, key);
    return added;
  }
  if (kind === "zRem") {
    const zset = state.zsets.get(key);
    const removed = zset?.delete(/** @type {string} */ (op[2])) ? 1 : 0;
    if (zset?.size === 0) state.zsets.delete(key);
    if (removed === 1) markKeyModified(state, key);
    return removed;
  }
  if (kind === "publish") return 0;
  if (kind === "expireAt") {
    expireIfNeeded(state, key, options);
    if (!keyExists(state, key, options)) return 0;
    state.expirations.set(key, Number(op[2]) * 1000);
    markKeyModified(state, key);
    return 1;
  }
  return undefined;
}

/** @param {FakeRedisState} state @param {string} key */
function ensureSet(state, key) {
  if (!state.sets.has(key)) state.sets.set(key, new Set());
  return /** @type {Set<string>} */ (state.sets.get(key));
}

/** @param {FakeRedisState} state @param {string} key @param {Record<string, string>} fields */
function setHashFields(state, key, fields) {
  const hash = state.hashes.get(key) || Object.create(null);
  let added = 0;
  for (const field of Object.keys(fields)) {
    if (!Object.hasOwn(hash, field)) added += 1;
  }
  state.hashes.set(key, { ...hash, ...fields });
  markKeyModified(state, key);
  return added;
}

/** @param {FakeRedisState} state @param {string} key @param {number} start @param {number} stop */
function zRangeMembers(state, key, start, stop) {
  const entries = [...(state.zsets.get(key) || new Map())]
    .toSorted(([aMember, aScore], [bMember, bScore]) => {
      if (aScore !== bScore) return aScore - bScore;
      if (aMember < bMember) return -1;
      if (aMember > bMember) return 1;
      return 0;
    })
    .map(([member]) => member);
  const len = entries.length;
  const normalize = (/** @type {number} */ index) => index < 0 ? len + index : index;
  const first = Math.max(0, normalize(start));
  const last = Math.min(len - 1, normalize(stop));
  if (len === 0 || first > last) return [];
  return entries.slice(first, last + 1);
}

/** @param {FakeRedisState} state @param {string} key */
function keyExists(state, key, options = {}) {
  expireIfNeeded(state, key, options);
  return state.strings.has(key) ||
    Object.keys(state.hashes.get(key) || {}).length > 0 ||
    (state.sets.get(key)?.size || 0) > 0 ||
    (state.zsets.get(key)?.size || 0) > 0;
}

/**
 * Snapshot one logical key so direct test-state mutations obey WATCH semantics.
 * @param {FakeRedisState} state
 * @param {string} key
 * @param {{ nowMs?: () => number }} [options]
 */
function fakeKeyFingerprint(state, key, options = {}) {
  expireIfNeeded(state, key, options);
  if (!keyExists(state, key, options)) return "absent";
  const expiresAt = state.expirations.get(key) ?? null;
  if (state.strings.has(key)) {
    return JSON.stringify(["string", state.strings.get(key), expiresAt]);
  }
  if (state.hashes.has(key)) {
    const entries = Object.entries(state.hashes.get(key) || {}).toSorted(([a], [b]) =>
      a.localeCompare(b)
    );
    return JSON.stringify(["hash", entries, expiresAt]);
  }
  if (state.sets.has(key)) {
    return JSON.stringify(["set", [...(state.sets.get(key) || [])].toSorted(), expiresAt]);
  }
  const entries = [...(state.zsets.get(key) || new Map())].toSorted(([a], [b]) =>
    a.localeCompare(b)
  );
  return JSON.stringify(["zset", entries, expiresAt]);
}

/** @param {FakeRedisState} state @param {string} key */
function keyRevision(state, key) {
  return state.revisions.get(key) ?? 0;
}

/** @param {unknown} value */
function redisByteLength(value) {
  if (value == null) return 0;
  if (value instanceof Uint8Array) return value.byteLength;
  return new TextEncoder().encode(String(value)).byteLength;
}

/** @param {FakeRedisState} state @param {string} key */
function markKeyModified(state, key) {
  state.revisions.set(key, keyRevision(state, key) + 1);
}

/** @param {FakeRedisState} state @param {string} key */
function clearKeyData(state, key) {
  state.strings.delete(key);
  state.hashes.delete(key);
  state.sets.delete(key);
  state.zsets.delete(key);
  state.expirations.delete(key);
}

/**
 * @param {FakeRedisState} state
 * @param {string} src
 * @param {string} dst
 * @param {{ REPLACE?: boolean }} copyOptions
 * @param {{ nowMs?: () => number }} [options]
 */
function copyKey(state, src, dst, copyOptions, options = {}) {
  if (src === dst) throw new Error("source and destination objects are the same");
  expireIfNeeded(state, src, options);
  if (!keyExists(state, src, options)) return 0;
  if (!copyOptions.REPLACE && keyExists(state, dst, options)) return 0;

  clearKeyData(state, dst);
  if (state.strings.has(src)) {
    state.strings.set(dst, /** @type {string} */ (state.strings.get(src)));
  } else if (state.hashes.has(src)) {
    state.hashes.set(dst, { ...(state.hashes.get(src) || {}) });
  } else if (state.sets.has(src)) {
    state.sets.set(dst, new Set(state.sets.get(src)));
  } else {
    state.zsets.set(dst, new Map(state.zsets.get(src)));
  }
  const expiresAt = state.expirations.get(src);
  if (expiresAt != null) state.expirations.set(dst, expiresAt);
  markKeyModified(state, dst);
  return 1;
}

/**
 * @param {FakeRedisState} state
 * @param {string} key
 * @param {{ nowMs?: () => number }} [options]
 */
function deleteKey(state, key, options = {}) {
  expireIfNeeded(state, key, options);
  const existed = keyExists(state, key, options);
  clearKeyData(state, key);
  if (existed) markKeyModified(state, key);
  return existed ? 1 : 0;
}

/**
 * @param {FakeRedisState} state
 * @param {string} key
 * @param {{ nowMs?: () => number }} [options]
 */
function expireIfNeeded(state, key, options = {}) {
  const expiresAt = state.expirations.get(key);
  if (expiresAt == null || currentFakeRedisTimeMs(state, options) < expiresAt) return;
  clearKeyData(state, key);
  markKeyModified(state, key);
}

/**
 * @param {FakeRedisState} state
 * @param {string} cursor
 * @param {string} match
 * @param {number} count
 * @param {{ nowMs?: () => number }} [options]
 * @returns {[string, string[]]}
 */
function scanKeys(state, cursor, match, count, options = {}) {
  const pattern = globToRegExp(match);
  const keys = [...new Set([
    ...state.strings.keys(),
    ...state.hashes.keys(),
    ...state.sets.keys(),
    ...state.zsets.keys(),
  ])]
    .filter((key) => {
      expireIfNeeded(state, key, options);
      return keyExists(state, key, options) && pattern.test(key);
    })
    .toSorted();
  const start = Math.max(0, Number(cursor) || 0);
  const safeCount = Math.max(1, count | 0);
  const page = keys.slice(start, start + safeCount);
  const next = start + safeCount >= keys.length ? "0" : String(start + safeCount);
  return [next, page];
}

/** @param {string} glob */
function globToRegExp(glob) {
  const escaped = glob
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".");
  return new RegExp(`^${escaped}$`);
}

/** @param {string | null} value @param {{ encodeGet?: boolean }} options */
function encodeMaybe(value, options) {
  if (!options.encodeGet || value == null) return value;
  return new TextEncoder().encode(value);
}

/** @param {string | undefined} stored @param {string | Uint8Array} expected */
function storedStringEquals(stored, expected) {
  if (stored == null) return false;
  if (typeof expected === "string") return stored === expected;
  const storedBytes = utf8Encoder.encode(stored);
  if (storedBytes.length !== expected.length) return false;
  return storedBytes.every((value, index) => value === expected[index]);
}

/** @param {FakeRedisState} state @param {{ nowMs?: () => number }} options */
function currentFakeRedisTimeMs(state, options) {
  return options.nowMs?.() ?? state.nowMs;
}
