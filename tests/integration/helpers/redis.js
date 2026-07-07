// Typed Redis CLI wrappers for integration tests. Every command goes through
// composeExec("redis", ...) and returns a parsed value.
// Single-value raw strings from redis-cli are trimmed before returning.

import { composeExec } from "./compose.js";
import { parseJsonText } from "./json-payload.js";
import { shellQuote } from "./shell-quote.js";

/**
 * @param {string} args
 * @param {{ db?: number }} [options]
 * @returns {string}
 */
export function redisCommand(args, options = {}) {
  const dbArgs = options.db == null ? "" : `-n ${options.db} `;
  return composeExec("redis", `redis-cli ${dbArgs}${args}`).trim();
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string | null} */
export function redisGet(key, options = {}) {
  const val = redisCommand(`GET ${shellQuote(key)}`, options);
  return val === "" ? null : val;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {boolean} */
export function redisExists(key, options = {}) {
  return redisCommand(`EXISTS ${shellQuote(key)}`, options) === "1";
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string | null} */
export function redisGetRaw(key, options = {}) {
  const val = redisCommand(`--raw GET ${shellQuote(key)}`, options);
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
  redisCommand(`SET ${shellQuote(key)} ${shellQuote(value)}`, options);
}

/** @param {string} key @param {unknown} value @param {{ db?: number }} [options] */
export function redisSetJson(key, value, options = {}) {
  redisSet(key, JSON.stringify(value), options);
}

/** @param {string} key @param {{ db?: number }} [options] @returns {Record<string, string>} */
export function redisHGetAll(key, options = {}) {
  const out = redisCommand(`HGETALL ${shellQuote(key)}`, options);
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
  const val = redisCommand(`HGET ${shellQuote(key)} ${shellQuote(field)}`, options);
  return val === "" ? null : val;
}

/** @param {string} key @param {string[]} fields @param {{ db?: number }} [options] @returns {Array<string | null>} */
export function redisHMGet(key, fields, options = {}) {
  if (fields.length === 0) return [];
  const dbArgs = options.db == null ? "" : `-n ${options.db} `;
  const args = fields.map(shellQuote).join(" ");
  const out = composeExec("redis", `redis-cli ${dbArgs}--raw HMGET ${shellQuote(key)} ${args}`);
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

/** @param {string} key @param {Record<string, string>} fields @param {{ db?: number }} [options] */
export function redisHSet(key, fields, options = {}) {
  const args = Object.entries(fields).map(([k, v]) => `${shellQuote(k)} ${shellQuote(v)}`).join(" ");
  redisCommand(`HSET ${shellQuote(key)} ${args}`, options);
}

/** @param {string} key @param {string[]} fields @param {{ db?: number }} [options] @returns {number} */
export function redisHDel(key, fields, options = {}) {
  return Number(redisCommand(`HDEL ${shellQuote(key)} ${fields.map(shellQuote).join(" ")}`, options));
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string[]} */
export function redisSMembers(key, options = {}) {
  const out = redisCommand(`SMEMBERS ${shellQuote(key)}`, options);
  return out ? out.split("\n") : [];
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisSAdd(key, member, options = {}) {
  redisCommand(`SADD ${shellQuote(key)} ${shellQuote(member)}`, options);
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisSRem(key, member, options = {}) {
  redisCommand(`SREM ${shellQuote(key)} ${shellQuote(member)}`, options);
}

/** @param {string} key @param {number} [start] @param {number} [stop] @param {{ db?: number }} [options] @returns {string[]} */
export function redisZRange(key, start = 0, stop = -1, options = {}) {
  const out = redisCommand(`ZRANGE ${shellQuote(key)} ${start} ${stop}`, options);
  return out ? out.split("\n") : [];
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] @returns {string | null} */
export function redisZScore(key, member, options = {}) {
  const out = redisCommand(`ZSCORE ${shellQuote(key)} ${shellQuote(member)}`, options);
  return out === "" ? null : out;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisZCard(key, options = {}) {
  return Number(redisCommand(`ZCARD ${shellQuote(key)}`, options));
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisZRem(key, member, options = {}) {
  redisCommand(`ZREM ${shellQuote(key)} ${shellQuote(member)}`, options);
}

/** @param {string} key @param {number} score @param {string} member @param {{ db?: number }} [options] */
export function redisZAdd(key, score, member, options = {}) {
  redisCommand(`ZADD ${shellQuote(key)} ${score} ${shellQuote(member)}`, options);
}

/** @param {string} pattern @param {{ db?: number }} [options] @returns {string[]} */
export function redisKeys(pattern, options = {}) {
  const out = redisCommand(`--raw KEYS ${shellQuote(pattern)}`, options);
  return out ? out.split("\n").filter(Boolean) : [];
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string[]} */
export function redisHKeys(key, options = {}) {
  const out = redisCommand(`HKEYS ${shellQuote(key)}`, options);
  return out ? out.split("\n") : [];
}

/** @param {string} stream @param {string} group @param {{ db?: number }} [options] @returns {number} */
export function redisXPendingCount(stream, group, options = {}) {
  const out = redisCommand(`XPENDING ${shellQuote(stream)} ${shellQuote(group)}`, options);
  if (out === "0") return 0;
  const count = Number(out.split(/\s+/)[0]);
  if (!Number.isInteger(count)) {
    throw new Error(`XPENDING count must be parseable, got: ${out}`);
  }
  return count;
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisXLen(key, options = {}) {
  return Number(redisCommand(`XLEN ${shellQuote(key)}`, options));
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
    .map(([field, value]) => `${shellQuote(field)} ${shellQuote(String(value))}`)
    .join(" ");
  return redisCommand(`XADD ${shellQuote(key)} ${shellQuote(id)} ${args}`, options);
}

/** @param {string} key @param {string} group @param {string} consumer @param {{ db?: number, count?: number, id?: string }} [options] */
export function redisXReadGroup(key, group, consumer, options = {}) {
  const count = options.count ?? 1;
  const id = options.id || ">";
  redisCommand(
    `XREADGROUP GROUP ${shellQuote(group)} ${shellQuote(consumer)} COUNT ${count} STREAMS ${shellQuote(key)} ${shellQuote(id)}`,
    options
  );
}

/** @param {string} key @param {string} group @param {string} consumer @param {string} streamId @param {number} idleMs @param {{ db?: number }} [options] */
export function redisXClaimIdle(key, group, consumer, streamId, idleMs, options = {}) {
  redisCommand(
    `XCLAIM ${shellQuote(key)} ${shellQuote(group)} ${shellQuote(consumer)} 0 ${shellQuote(streamId)} IDLE ${idleMs}`,
    options
  );
}

/** @param {string} key @param {{ db?: number }} [options] @returns {string} */
export function redisXInfoGroups(key, options = {}) {
  return redisCommand(`XINFO GROUPS ${shellQuote(key)} 2>/dev/null || echo missing`, options);
}

/** @param {string} key @param {string} [start] @param {string} [stop] @param {{ db?: number, count?: number }} [options] @returns {string} */
export function redisXRangeRaw(key, start = "-", stop = "+", options = {}) {
  const countArg = options.count == null ? "" : ` COUNT ${options.count}`;
  return redisCommand(`XRANGE ${shellQuote(key)} ${shellQuote(start)} ${shellQuote(stop)}${countArg}`, options);
}

/** @param {string} key @param {{ db?: number }} [options] */
export function redisDel(key, options = {}) {
  redisCommand(`DEL ${shellQuote(key)}`, options);
}

/** @param {string} key @param {string} value @param {number} ttlSeconds @param {{ db?: number }} [options] */
export function redisSetEx(key, value, ttlSeconds, options = {}) {
  redisCommand(`SET ${shellQuote(key)} ${shellQuote(value)} EX ${ttlSeconds}`, options);
}

/** @param {string} key @param {{ db?: number }} [options] @returns {number} */
export function redisExpireTime(key, options = {}) {
  return Number(redisCommand(`EXPIRETIME ${shellQuote(key)}`, options));
}

/** @param {string} key @param {number} unixSeconds @param {{ db?: number }} [options] @returns {boolean} */
export function redisExpireAt(key, unixSeconds, options = {}) {
  return redisCommand(`EXPIREAT ${shellQuote(key)} ${unixSeconds}`, options) === "1";
}

/** @param {string} key @param {string} member @param {{ db?: number }} [options] */
export function redisSIsMember(key, member, options = {}) {
  return redisCommand(`SISMEMBER ${shellQuote(key)} ${shellQuote(member)}`, options) === "1";
}

/** @param {string} src @param {string} dst @param {{ db?: number, replace?: boolean }} [options] @returns {number} */
export function redisCopy(src, dst, options = {}) {
  const replace = options.replace ? " REPLACE" : "";
  return Number(redisCommand(`COPY ${shellQuote(src)} ${shellQuote(dst)}${replace}`, options));
}

export function redisFlushAll() {
  redisCommand("FLUSHALL");
}

/** @param {string} channel @param {string} message @returns {number} */
export function redisPublish(channel, message) {
  return Number(redisCommand(`PUBLISH ${shellQuote(channel)} ${shellQuote(message)}`));
}

/** @param {string} type */
export function redisClientKillType(type) {
  redisCommand(`CLIENT KILL TYPE ${shellQuote(type)}`);
}

/** @param {number} seconds */
export function redisDebugSleep(seconds) {
  redisCommand(`DEBUG SLEEP ${seconds}`);
}
