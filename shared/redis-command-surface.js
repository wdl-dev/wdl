import {
  buildHGetExArgs,
  buildHSetExArgs,
  buildHSetArgs,
  buildSetArgs,
  decodeBulk,
  decodeHashObject,
  decodeRedisTimeMs,
  decodeStringArray,
  utf8Decoder,
} from "shared-redis-resp";

/**
 * @typedef {import("shared-redis-resp").RedisArg} RedisArg
 * @typedef {import("shared-redis-resp").RedisCommand} RedisCommand
 * @typedef {import("shared-redis-resp").RedisReply} RedisReply
 * @typedef {import("shared-redis-resp").RedisHSetArg} RedisHSetArg
 * @typedef {import("shared-redis-resp").RedisSetOptions} RedisSetOptions
 * @typedef {import("shared-redis-resp").RedisCopyOptions} RedisCopyOptions
 */

// Typed command construction and decoding shared by fresh-socket clients and
// held sessions. Subclasses retain ownership of their distinct I/O lifetimes.
/** @template T */
export class RedisCommandSurface {
  /** @param {...RedisArg} _args @returns {Promise<RedisReply>} */
  async _exec(..._args) {
    throw new Error("Redis command transport is not implemented");
  }

  /** @param {string} _command @param {RedisCommand[]} _commands @returns {Promise<RedisReply[]>} */
  async _execPipeline(_command, _commands) {
    throw new Error("Redis pipeline transport is not implemented");
  }

  /** @param {string} key */
  async getWithTime(key) {
    const { values, nowMs } = await this.getManyWithTime([key]);
    return {
      value: values[0],
      nowMs,
    };
  }

  /** @param {string[]} _keys @returns {Promise<{ values: T[], nowMs: number }>} */
  async getManyWithTime(_keys) {
    throw new Error("Redis timed read is not implemented");
  }

  /** @param {string} key @param {RedisArg} value @param {RedisSetOptions} [opts] */
  async set(key, value, opts = {}) {
    const reply = await this._exec(...buildSetArgs(key, value, opts));
    return reply === null ? null : decodeBulk(reply);
  }

  /** @param {...string} keys */
  async del(...keys) {
    return /** @type {number} */ (await this._exec("DEL", ...keys));
  }

  /** @param {string} key @param {RedisArg} value */
  async delIfEq(key, value) {
    return /** @type {number} */ (await this._exec("DELIFEQ", key, value));
  }

  async time() {
    return decodeRedisTimeMs(await this._exec("TIME"));
  }

  /** @param {string} channel @param {RedisArg} message */
  async publish(channel, message) {
    return this._exec("PUBLISH", channel, message);
  }

  /** @param {string} cursor @param {string} match @param {number} [count] @returns {Promise<[string, string[]]>} */
  async scan(cursor, match, count = 100) {
    const result = /** @type {[Uint8Array, Uint8Array[]]} */ (
      await this._exec("SCAN", cursor, "MATCH", match, "COUNT", String(count))
    );
    return [utf8Decoder.decode(result[0]), result[1].map((key) => utf8Decoder.decode(key))];
  }

  /** @param {string} key @param {string} field */
  async hGet(key, field) {
    return decodeBulk(await this._exec("HGET", key, field));
  }

  /** @param {Array<[string, string]>} pairs */
  async hGetMany(pairs) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "HGET_PIPELINE",
      pairs.map(([key, field]) => ["HGET", key, field])
    ));
    return replies.map(decodeBulk);
  }

  /** @param {string} key @param {string[]} fields */
  async hMGet(key, fields) {
    if (fields.length === 0) return [];
    const arr = /** @type {unknown[] | null} */ (await this._exec("HMGET", key, ...fields));
    return arr ? arr.map(decodeBulk) : [];
  }

  /** @param {string} key @param {number} ttlSeconds @param {string[]} fields */
  async hGetEx(key, ttlSeconds, fields) {
    if (fields.length === 0) return [];
    const arr = /** @type {unknown[] | null} */ (
      await this._exec(...buildHGetExArgs(key, ttlSeconds, fields))
    );
    return arr ? arr.map(decodeBulk) : [];
  }

  /** @param {string} key */
  async hGetAll(key) {
    return decodeHashObject(/** @type {unknown[] | null} */ (await this._exec("HGETALL", key)));
  }

  /** @param {string[]} keys */
  async hGetAllMany(keys) {
    const replies = /** @type {(unknown[] | null)[]} */ (await this._execPipeline(
      "HGETALL_PIPELINE",
      keys.map((key) => ["HGETALL", key])
    ));
    return replies.map(decodeHashObject);
  }

  /** @param {string} hashKey @param {string} stringKey */
  async hGetAllAndGet(hashKey, stringKey) {
    const [hashReply, valueReply] = await this._execPipeline("HGETALL_GET_PIPELINE", [
      ["HGETALL", hashKey],
      ["GET", stringKey],
    ]);
    return {
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      value: decodeBulk(valueReply),
    };
  }

  /** @param {string} key @param {...RedisHSetArg} rest */
  async hSet(key, ...rest) {
    return /** @type {number} */ (await this._exec(...buildHSetArgs(key, rest)));
  }

  /** @param {string} key @param {number} ttlSeconds @param {Record<string, RedisArg>} fields */
  async hSetEx(key, ttlSeconds, fields) {
    return /** @type {number} */ (await this._exec(...buildHSetExArgs(key, ttlSeconds, fields)));
  }

  /** @param {string} key @param {...string} fields */
  async hDel(key, ...fields) {
    return /** @type {number} */ (await this._exec("HDEL", key, ...fields));
  }

  /** @param {string} key */
  async hKeys(key) {
    const arr = /** @type {unknown[] | null} */ (await this._exec("HKEYS", key));
    return decodeStringArray(arr);
  }

  /** @param {string} key */
  async hLen(key) {
    return /** @type {number} */ (await this._exec("HLEN", key));
  }

  /** @param {string} key @param {string} field */
  async hExists(key, field) {
    return (await this._exec("HEXISTS", key, field)) === 1;
  }

  /** @param {string[]} keys @param {string} field */
  async hExistsMany(keys, field) {
    const replies = /** @type {number[]} */ (await this._execPipeline(
      "HEXISTS_PIPELINE",
      keys.map((key) => ["HEXISTS", key, field])
    ));
    return replies.map((value) => value === 1);
  }

  /** @param {Array<[string, string]>} pairs */
  async hStrLenMany(pairs) {
    return /** @type {number[]} */ (await this._execPipeline(
      "HSTRLEN_PIPELINE",
      pairs.map(([key, field]) => ["HSTRLEN", key, field])
    ));
  }

  /** @param {string[]} keys */
  async sMembersMany(keys) {
    const replies = /** @type {(unknown[] | null)[]} */ (await this._execPipeline(
      "SMEMBERS_PIPELINE",
      keys.map((key) => ["SMEMBERS", key])
    ));
    return replies.map(decodeStringArray);
  }

  /** @param {string} key */
  async sCard(key) {
    return /** @type {number} */ (await this._exec("SCARD", key));
  }

  /** @param {string[]} keys */
  async sCardMany(keys) {
    return /** @type {number[]} */ (await this._execPipeline(
      "SCARD_PIPELINE",
      keys.map((key) => ["SCARD", key])
    ));
  }

  /** @param {string} key */
  async incr(key) {
    return /** @type {number} */ (await this._exec("INCR", key));
  }

  /** @param {...string} keys */
  async exists(...keys) {
    return /** @type {number} */ (await this._exec("EXISTS", ...keys));
  }

  /** @param {string[]} keys */
  async existsMany(keys) {
    const replies = /** @type {number[]} */ (await this._execPipeline(
      "EXISTS_PIPELINE",
      keys.map((key) => ["EXISTS", key])
    ));
    return replies.map((value) => value > 0);
  }

  /** @param {string} src @param {string} dst @param {RedisCopyOptions} [opts] */
  async copy(src, dst, opts = {}) {
    const args = ["COPY", src, dst];
    if (opts.REPLACE || opts.replace) args.push("REPLACE");
    return /** @type {number} */ (await this._exec(...args));
  }

  /** @param {...RedisArg} args */
  async xRead(...args) {
    return this._exec("XREAD", ...args);
  }

  /** @param {...RedisArg} args */
  async xRange(...args) {
    return /** @type {[Uint8Array, Uint8Array[]][]} */ (await this._exec("XRANGE", ...args));
  }
}
