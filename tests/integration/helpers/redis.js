// Typed Redis CLI wrappers for integration tests. Every command goes through
// composeExec("redis", ...) and returns a parsed value.
// Single-value raw strings from redis-cli are trimmed before returning.

import { composeExec } from "./compose.js";
import { parseJsonText } from "./json-payload.js";

/**
 * @param {string[]} args
 * @param {{ db?: number }} [options]
 * @returns {string}
 */
export function redisCommand(args, options = {}) {
  const dbArgs = options.db == null ? [] : ["-n", String(options.db)];
  return composeExec("redis", ["redis-cli", ...dbArgs, ...args]).trim();
}

/**
 * @param {string} script
 * @param {string[]} keys
 * @param {string[]} args
 * @param {{ db?: number }} [options]
 * @returns {string}
 */
export function redisEval(script, keys, args, options = {}) {
  return redisCommand(["EVAL", script, String(keys.length), ...keys, ...args], options);
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string | null} */
export function redisGet(key, options = {}) {
  const val = redisCommand(["GET", key], options);
  return val === "" ? null : val;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {boolean} */
export function redisExists(key, options = {}) {
  return redisCommand(["EXISTS", key], options) === "1";
}

/** @param {string[]} keys @param {{ db?: number }} [options] @returns {number} */
export function redisExistsCount(keys, options = {}) {
  return Number(redisCommand(["EXISTS", ...keys], options));
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string | null} */
export function redisGetRaw(key, options = {}) {
  const val = redisCommand(["--raw", "GET", key], options);
  return val === "" ? null : val;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {any | null} */
export function redisGetJson(key, options = {}) {
  const val = redisGetRaw(key, options);
  return val == null ? null : parseJsonText(val, `Redis string ${key}`);
}

/** @param {Record<string, string>} hash @param {string} field @param {string} [label] @returns {any} */
export function redisHashJsonField(hash, field, label = `Redis hash field ${field}`) {
  if (!(field in hash)) throw new Error(`expected ${label} to be present`);
  return parseJsonText(hash[field], label);
}

/** @param {string} member @param {string} [label] @returns {any} */
export function redisJsonMember(member, label = "Redis member JSON") {
  return parseJsonText(member, label);
}

/** @param {string[]} members @param {string} [label] @returns {any[]} */
export function redisJsonMembers(members, label = "Redis member JSON") {
  return members.map((member, index) => redisJsonMember(member, `${label}[${index}]`));
}

/** @param {string} key @param {string} value @param {{ db?: number }} [options] */
export function redisSet(key, value, options = {}) {
  redisCommand(["SET", key, value], options);
}

/** @param {string} key @param {string} value @param {string} expected @param {{ db?: number }} [options] */
export function redisSetIfEq(key, value, expected, options = {}) {
  return redisCommand(["SET", key, value, "IFEQ", expected], options) === "OK";
}

/** @param {string} key @param {string} expected @param {{ db?: number }} [options] */
export function redisDelIfEq(key, expected, options = {}) {
  return Number(redisCommand(["DELIFEQ", key, expected], options));
}

/** @param {string} key @param {unknown} value @param {{ db?: number }} [options] */
export function redisSetJson(key, value, options = {}) {
  redisSet(key, JSON.stringify(value), options);
}

/** @param {string} key @param {{ db?: number }} [options] @returns {Record<string, string>} */
export function redisHGetAll(key, options = {}) {
  const out = redisCommand(["HGETALL", key], options);
  if (!out) return {};
  const parts = out.split("\n");
  /** @type {Record<string, string>} */
  const result = {};
  for (let i = 0; i < parts.length - 1; i += 2) {
    result[parts[i]] = parts[i + 1];
  }
  return result;
}

/** @param {string} key @param {string} field @param {{ db?: number }} [options] @returns {string | null} */
export function redisHGet(key, field, options = {}) {
  const val = redisCommand(["HGET", key, field], options);
  return val === "" ? null : val;
}

/** @param {string} key @param {string[]} fields @param {{ db?: number }} [options] @returns {Array<string | null>} */
export function redisHMGet(key, fields, options = {}) {
  if (fields.length === 0) return [];
  const dbArgs = options.db == null ? [] : ["-n", String(options.db)];
  const out = composeExec("redis", ["redis-cli", ...dbArgs, "--raw", "HMGET", key, ...fields]);
  // Preserve empty lines: redis-cli --raw renders HMGET nil slots as blanks.
  const text = out.endsWith("\n") ? out.slice(0, -1) : out;
  return text.split("\n").map((value) => value === "" ? null : value);
}

/** @param {string} key @param {string} field @param {{ db?: number, label?: string }} [options] @returns {any} */
export function redisHGetJson(key, field, options = {}) {
  const val = redisHGet(key, field, options);
  if (val == null) throw new Error(`expected ${options.label || `${key} ${field}`} to be present`);
  return parseJsonText(val, options.label || `${key} ${field}`);
}

/** @param {string} key @param {Record<string, string>} fields @param {{ db?: number }} [options] @returns {number} */
export function redisHSet(key, fields, options = {}) {
  const args = Object.entries(fields).flatMap(([field, value]) => [field, value]);
  return Number(redisCommand(["HSET", key, ...args], options));
}

/** @param {string} key @param {string[]} fields @param {{ db?: number }} [options] @returns {number} */
export function redisHDel(key, fields, options = {}) {
  return Number(redisCommand(["HDEL", key, ...fields], options));
}

/** @param {string} key @param {string} field @param {{ db?: number }} [options] @returns {number} */
export function redisHStrLen(key, field, options = {}) {
  return Number(redisCommand(["HSTRLEN", key, field], options));
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string[]} */
export function redisSMembers(key, options = {}) {
  const out = redisCommand(["SMEMBERS", key], options);
  return out ? out.split("\n") : [];
}

/** @param {string} key @param {string|string[]} members @param {{ db?: number }} [options] @returns {number} */
export function redisSAdd(key, members, options = {}) {
  const values = Array.isArray(members) ? members : [members];
  if (values.length === 0) return 0;
  return Number(redisCommand(["SADD", key, ...values], options));
}

/** @param {string} key @param {string|string[]} members @param {{ db?: number }} [options] @returns {number} */
export function redisSRem(key, members, options = {}) {
  const values = Array.isArray(members) ? members : [members];
  if (values.length === 0) return 0;
  return Number(redisCommand(["SREM", key, ...values], options));
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisSCard(key, options = {}) {
  return Number(redisCommand(["SCARD", key], options));
}

/** @param {string} key @param {number} [start] @param {number} [stop] @param {{ db?: number }} [options] @returns {string[]} */
export function redisZRange(key, start = 0, stop = -1, options = {}) {
  const out = redisCommand(["ZRANGE", key, String(start), String(stop)], options);
  return out ? out.split("\n") : [];
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] @returns {string | null} */
export function redisZScore(key, member, options = {}) {
  const out = redisCommand(["ZSCORE", key, member], options);
  return out === "" ? null : out;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisZCard(key, options = {}) {
  return Number(redisCommand(["ZCARD", key], options));
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisZRem(key, member, options = {}) {
  redisCommand(["ZREM", key, member], options);
}

/** @param {string} key @param {number} score @param {string} member @param {{ db?: number }} [options] */
export function redisZAdd(key, score, member, options = {}) {
  redisCommand(["ZADD", key, String(score), member], options);
}

/** @param {string} pattern @param {{ db?: number }} [options] @returns {string[]} */
export function redisKeys(pattern, options = {}) {
  const out = redisCommand(["--raw", "KEYS", pattern], options);
  return out ? out.split("\n").filter(Boolean) : [];
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string[]} */
export function redisHKeys(key, options = {}) {
  const out = redisCommand(["HKEYS", key], options);
  return out ? out.split("\n") : [];
}

/** @param {string} stream @param {string} group @param {{ db?: number }} [options] @returns {number} */
export function redisXPendingCount(stream, group, options = {}) {
  const out = redisCommand(["XPENDING", stream, group], options);
  if (out === "0") return 0;
  const count = Number(out.split(/\s+/)[0]);
  if (!Number.isInteger(count)) {
    throw new Error(`XPENDING count must be parseable, got: ${out}`);
  }
  return count;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisXLen(key, options = {}) {
  return Number(redisCommand(["XLEN", key], options));
}

/**
 * @param {string} key
 * @param {Record<string, string | number>} fields
 * @param {{ db?: number, id?: string }} [options]
 * @returns {string}
 */
export function redisXAdd(key, fields, options = {}) {
  const id = options.id || "*";
  const args = Object.entries(fields)
    .flatMap(([field, value]) => [field, String(value)]);
  return redisCommand(["XADD", key, id, ...args], options);
}

/** @param {string} key @param {string} group @param {{ db?: number }} [options] */
export function redisXGroupCreate(key, group, options = {}) {
  redisCommand(["XGROUP", "CREATE", key, group, "0", "MKSTREAM"], options);
}

/** @param {string} key @param {string} group @param {{ db?: number }} [options] */
export function redisXGroupDestroy(key, group, options = {}) {
  redisCommand(["XGROUP", "DESTROY", key, group], options);
}

/** @param {string} key @param {string} group @param {string} consumer @param {{ db?: number, count?: number, id?: string }} [options] */
export function redisXReadGroup(key, group, consumer, options = {}) {
  const count = options.count ?? 1;
  const id = options.id || ">";
  redisCommand(
    ["XREADGROUP", "GROUP", group, consumer, "COUNT", String(count), "STREAMS", key, id],
    options
  );
}

/** @param {string} key @param {string} group @param {string} consumer @param {string} streamId @param {number} idleMs @param {{ db?: number }} [options] */
export function redisXClaimIdle(key, group, consumer, streamId, idleMs, options = {}) {
  redisCommand(
    ["XCLAIM", key, group, consumer, "0", streamId, "IDLE", String(idleMs)],
    options
  );
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string} */
export function redisXInfoGroups(key, options = {}) {
  try {
    return redisCommand(["XINFO", "GROUPS", key], options);
  } catch {
    return "missing";
  }
}

/** @param {string} key @param {string} [start] @param {string} [stop] @param {{ db?: number, count?: number }} [options] @returns {string} */
export function redisXRangeRaw(key, start = "-", stop = "+", options = {}) {
  const countArgs = options.count == null ? [] : ["COUNT", String(options.count)];
  return redisCommand(["XRANGE", key, start, stop, ...countArgs], options);
}

/** @param {string} key @param {{ db?: number }} [options] */
export function redisDel(key, options = {}) {
  redisCommand(["DEL", key], options);
}

/** @param {string} key @param {string} value @param {number} ttlSeconds @param {{ db?: number }} [options] */
export function redisSetEx(key, value, ttlSeconds, options = {}) {
  redisCommand(["SET", key, value, "EX", String(ttlSeconds)], options);
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisExpireTime(key, options = {}) {
  return Number(redisCommand(["EXPIRETIME", key], options));
}

/** @param {string} key @param {number} unixSeconds @param {{ db?: number }} [options] @returns {boolean} */
export function redisExpireAt(key, unixSeconds, options = {}) {
  return redisCommand(["EXPIREAT", key, String(unixSeconds)], options) === "1";
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisSIsMember(key, member, options = {}) {
  return redisCommand(["SISMEMBER", key, member], options) === "1";
}

/** @param {string} src @param {string} dst @param {{ db?: number, replace?: boolean }} [options] @returns {number} */
export function redisCopy(src, dst, options = {}) {
  const replace = options.replace ? ["REPLACE"] : [];
  return Number(redisCommand(["COPY", src, dst, ...replace], options));
}

export function redisFlushAll() {
  redisCommand(["FLUSHALL"]);
}

export function redisScriptFlush() {
  redisCommand(["SCRIPT", "FLUSH"]);
}

/** @param {string} command @returns {number} */
export function redisCommandCalls(command) {
  const commandLabel = command.toLowerCase().replaceAll(" ", "|");
  const prefix = `cmdstat_${commandLabel}:`;
  const line = redisCommand(["INFO", "commandstats"])
    .split(/\r?\n/)
    .find((entry) => entry.startsWith(prefix));
  if (line == null) return 0;
  const calls = /(?:^|,)calls=(\d+)(?:,|$)/.exec(line.slice(prefix.length));
  if (calls == null) throw new Error(`missing calls field for Redis command ${command}`);
  return Number(calls[1]);
}

/** @param {string} channel @param {string} message @returns {number} */
export function redisPublish(channel, message) {
  return Number(redisCommand(["PUBLISH", channel, message]));
}

/** @param {string} type */
export function redisClientKillType(type) {
  redisCommand(["CLIENT", "KILL", "TYPE", type]);
}

/** @param {number} seconds */
export function redisDebugSleep(seconds) {
  redisCommand(["DEBUG", "SLEEP", String(seconds)]);
}
