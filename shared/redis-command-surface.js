import {
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
    return /** @type {number} */ (await this._exec("PUBLISH", channel, message));
  }

  /** @param {string} script @param {string[]} [keys] @param {RedisArg[]} [args] */
  async eval(script, keys = [], args = []) {
    return this._exec("EVAL", script, String(keys.length), ...keys, ...args);
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

  /**
   * @param {string[]} hashKeys
   * @param {string[]} keyListHashes
   */
  async hGetAllManyAndHKeysMany(hashKeys, keyListHashes) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "HGETALL_HKEYS_PIPELINE",
      [
        ...hashKeys.map((key) => ["HGETALL", key]),
        ...keyListHashes.map((key) => ["HKEYS", key]),
      ]
    ));
    return {
      hashes: replies.slice(0, hashKeys.length)
        .map((reply) => decodeHashObject(/** @type {unknown[] | null} */ (reply))),
      keyLists: replies.slice(hashKeys.length)
        .map((reply) => decodeStringArray(/** @type {unknown[] | null} */ (reply))),
    };
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

  /**
   * @param {string} hashKey
   * @param {string} field
   * @param {string} sortedSetKey
   * @param {number} start
   * @param {number} stop
   */
  async hGetAndZRange(hashKey, field, sortedSetKey, start, stop) {
    const [fieldReply, membersReply] = await this._execPipeline("HGET_ZRANGE_PIPELINE", [
      ["HGET", hashKey, field],
      ["ZRANGE", sortedSetKey, String(start), String(stop)],
    ]);
    return {
      field: decodeBulk(fieldReply),
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
    };
  }

  /**
   * @param {string[]} sortedSetKeys
   * @param {string[]} hashKeys
   * @param {number} start
   * @param {number} stop
   */
  async zRangeManyAndHGetAllMany(sortedSetKeys, hashKeys, start, stop) {
    const replies = await this._execPipeline("ZRANGE_HGETALL_PIPELINE", [
      ...sortedSetKeys.map((key) => ["ZRANGE", key, String(start), String(stop)]),
      ...hashKeys.map((key) => ["HGETALL", key]),
    ]);
    return {
      ranges: replies.slice(0, sortedSetKeys.length)
        .map((reply) => decodeStringArray(/** @type {unknown[] | null} */ (reply))),
      hashes: replies.slice(sortedSetKeys.length)
        .map((reply) => decodeHashObject(/** @type {unknown[] | null} */ (reply))),
    };
  }

  /**
   * @param {string[]} sortedSetKeys
   * @param {string[]} existenceKeys
   * @param {number} start
   * @param {number} stop
   */
  async zRangeManyAndExistsMany(sortedSetKeys, existenceKeys, start, stop) {
    const replies = await this._execPipeline("ZRANGE_EXISTS_PIPELINE", [
      ...sortedSetKeys.map((key) => ["ZRANGE", key, String(start), String(stop)]),
      ...existenceKeys.map((key) => ["EXISTS", key]),
    ]);
    return {
      ranges: replies.slice(0, sortedSetKeys.length)
        .map((reply) => decodeStringArray(/** @type {unknown[] | null} */ (reply))),
      exists: replies.slice(sortedSetKeys.length).map((reply) => reply === 1),
    };
  }

  /** @param {string} setKey @param {string} hashKey */
  async sMembersAndHGetAll(setKey, hashKey) {
    const [membersReply, hashReply] = await this._execPipeline(
      "SMEMBERS_HGETALL_PIPELINE",
      [
        ["SMEMBERS", setKey],
        ["HGETALL", hashKey],
      ]
    );
    return {
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
    };
  }

  /** @param {string} namespacesKey @param {string} hashKey @param {string} setKey */
  async sMembersHGetAllAndSMembers(namespacesKey, hashKey, setKey) {
    const [namespacesReply, hashReply, membersReply] = await this._execPipeline(
      "SMEMBERS_HGETALL_SMEMBERS_PIPELINE",
      [
        ["SMEMBERS", namespacesKey],
        ["HGETALL", hashKey],
        ["SMEMBERS", setKey],
      ]
    );
    return {
      namespaces: decodeStringArray(/** @type {unknown[] | null} */ (namespacesReply)),
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
    };
  }

  /** @param {string} hashKey @param {string} setKey */
  async hGetAllAndSMembers(hashKey, setKey) {
    const [hashReply, membersReply] = await this._execPipeline(
      "HGETALL_SMEMBERS_PIPELINE",
      [
        ["HGETALL", hashKey],
        ["SMEMBERS", setKey],
      ]
    );
    return {
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
    };
  }

  /** @param {string} key @param {...RedisHSetArg} rest */
  async hSet(key, ...rest) {
    return /** @type {number} */ (await this._exec(...buildHSetArgs(key, rest)));
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

  /** @param {string} key @param {string} field */
  async hExists(key, field) {
    return (await this._exec("HEXISTS", key, field)) === 1;
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

  /** @param {string} key @param {string|string[]} members */
  async sAdd(key, members) {
    const values = Array.isArray(members) ? members : [members];
    if (values.length === 0) return 0;
    return /** @type {number} */ (await this._exec("SADD", key, ...values));
  }

  /** @param {string} key @param {string|string[]} members */
  async sRem(key, members) {
    const values = Array.isArray(members) ? members : [members];
    if (values.length === 0) return 0;
    return /** @type {number} */ (await this._exec("SREM", key, ...values));
  }

  /** @param {string} key */
  async sMembers(key) {
    return decodeStringArray(
      /** @type {unknown[] | null} */ (await this._exec("SMEMBERS", key))
    );
  }

  /** @param {string} key @param {string} member */
  async sIsMember(key, member) {
    return (await this._exec("SISMEMBER", key, member)) === 1;
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

  /** @param {string} key */
  async zCard(key) {
    return /** @type {number} */ (await this._exec("ZCARD", key));
  }

  /** @param {string} key @param {number} start @param {number} stop */
  async zRange(key, start, stop) {
    return decodeStringArray(
      /** @type {unknown[] | null} */ (
        await this._exec("ZRANGE", key, String(start), String(stop))
      )
    );
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
