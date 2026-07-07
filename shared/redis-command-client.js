import { connect } from "cloudflare:sockets";
import { RedisSession } from "shared-redis-session";
import { errorMessage } from "./errors.js";
import {
  buildHSetArgs,
  buildSetArgs,
  concatBuffers,
  decodeBulk,
  decodeHashObject,
  decodeRedisTimeMs,
  decodeStringArray,
  encodeCommand,
  normalizeRedisDb,
  utf8Decoder,
  warnRedisCallback,
  RespReader,
} from "shared-redis-resp";

/**
 * @typedef {import("shared-redis-resp").RedisArg} RedisArg
 * @typedef {import("shared-redis-resp").RedisCommand} RedisCommand
 * @typedef {import("shared-redis-resp").RedisReply} RedisReply
 * @typedef {import("shared-redis-resp").RedisCommandEvent} RedisCommandEvent
 * @typedef {import("shared-redis-resp").RedisHSetArg} RedisHSetArg
 * @typedef {import("shared-redis-resp").RedisSetOptions} RedisSetOptions
 * @typedef {import("shared-redis-resp").RedisXAddOptions} RedisXAddOptions
 * @typedef {import("shared-redis-resp").RedisZRangeByScoreOptions} RedisZRangeByScoreOptions
 * @typedef {import("shared-redis-resp").RedisCopyOptions} RedisCopyOptions
 * @typedef {import("shared-redis-resp").RedisClientOptions} RedisClientOptions
 */

export class RedisClient {
  /** @param {string} address @param {RedisClientOptions} [opts] */
  constructor(address, opts = {}) {
    this.address = address;
    this.db = normalizeRedisDb(opts.db);
    this.onCommand = opts.onCommand || null;
    this._connect = opts.connect || connect;
  }

  // Socket-per-call by design: workerd's `cloudflare:sockets` I/O objects
  // cannot be reused across request / JSRPC handler contexts. Multi-command
  // helpers below batch work within this one socket.
  /** @template T @param {string} command @param {(writer: WritableStreamDefaultWriter<Uint8Array>, reader: ReadableStreamDefaultReader<Uint8Array>, parser: RespReader) => Promise<T>} fn @returns {Promise<T>} */
  async _withSocket(command, fn) {
    const startedAt = Date.now();
    const socket = this._connect(this.address);
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    try {
      const parser = new RespReader(reader);
      if (this.db > 0) {
        await writer.write(encodeCommand(["SELECT", String(this.db)]));
        await parser.parseOne();
        parser.compact();
      }
      const result = await fn(writer, reader, parser);
      this._emitCommand({ command, duration_ms: Date.now() - startedAt, ok: true });
      return result;
    } catch (err) {
      this._emitCommand({
        command,
        duration_ms: Date.now() - startedAt,
        ok: false,
        error_message: errorMessage(err),
      });
      throw err;
    } finally {
      writer.close();
      try { reader.releaseLock(); } catch { /* already released */ }
      socket.close?.();
    }
  }

  /** @param {...RedisArg} args @returns {Promise<RedisReply>} */
  async _exec(...args) {
    const command = String(args[0] || "UNKNOWN").toUpperCase();
    return this._withSocket(command, async (writer, _reader, parser) => {
      await writer.write(encodeCommand(args));
      return parser.parseOne();
    });
  }

  /** @param {string} command @param {RedisCommand[]} commands @returns {Promise<RedisReply[]>} */
  async _execPipeline(command, commands) {
    if (commands.length === 0) return [];
    return this._withSocket(command, async (writer, _reader, parser) => {
      await writer.write(concatBuffers(commands.map((args) => encodeCommand(args))));
      const replies = [];
      for (let i = 0; i < commands.length; i += 1) replies.push(await parser.parseOne());
      return replies;
    });
  }

  /** @param {RedisCommandEvent} event */
  _emitCommand(event) {
    if (!this.onCommand) return;
    try {
      this.onCommand(event);
    } catch (err) {
      // Observer bugs must not break command IO, but silent swallow hides
      // the bug. Warn once per client.
      if (!this._observerWarned) {
        this._observerWarned = true;
        warnRedisCallback("redis_observer_callback_threw", err);
      }
    }
  }

  // SMEMBERS returns a Set of strings: members are always plain text.
  /** @param {string} key */
  async smembers(key) {
    const arr = /** @type {Uint8Array[] | null} */ (await this._exec("SMEMBERS", key));
    return decodeStringArray(arr);
  }

  /** @param {string} key @param {string} member */
  async sismember(key, member) {
    return (await this._exec("SISMEMBER", key, member)) === 1;
  }

  // HGETALL returns { fieldName (string) -> valueBytes (Uint8Array) }.
  // Field names are UTF-8 decoded; values stay as bytes so binary modules
  // round-trip without loss.
  /** @param {string} key */
  async hgetall(key) {
    const arr = /** @type {Uint8Array[] | null} */ (await this._exec("HGETALL", key));
    /** @type {Record<string, Uint8Array>} */
    const obj = {};
    if (arr) {
      for (let i = 0; i < arr.length; i += 2) obj[utf8Decoder.decode(arr[i])] = arr[i + 1];
    }
    return obj;
  }

  /** @param {string} key */
  async get(key) {
    return /** @type {Uint8Array | null} */ (await this._exec("GET", key));
  }

  /** @param {string} key */
  async getWithTime(key) {
    const [value, time] = await this._execPipeline("GET_TIME_PIPELINE", [
      ["GET", key],
      ["TIME"],
    ]);
    return {
      value: /** @type {Uint8Array | null} */ (value),
      nowMs: decodeRedisTimeMs(time),
    };
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

  /** @param {string} key @param {string} value */
  async delIfEq(key, value) {
    return /** @type {number} */ (await this._exec("DELIFEQ", key, value));
  }

  /** @param {string} script @param {string[]} [keys] @param {RedisArg[]} [args] */
  async eval(script, keys = [], args = []) {
    return this._exec("EVAL", script, String(keys.length), ...keys, ...args);
  }

  /** @param {RedisCommand[]} cmdList */
  async multiExec(cmdList) {
    const parts = [encodeCommand(["MULTI"])];
    for (const cmd of cmdList) parts.push(encodeCommand(cmd));
    parts.push(encodeCommand(["EXEC"]));
    const buf = concatBuffers(parts);

    return this._withSocket("MULTI_EXEC", async (writer, _reader, resp) => {
      await writer.write(buf);
      await resp.parseOne();
      for (let i = 0; i < cmdList.length; i += 1) await resp.parseOne();
      return resp.parseOne();
    });
  }

  async ping() {
    return this._exec("PING");
  }

  async time() {
    return decodeRedisTimeMs(await this._exec("TIME"));
  }

  /** @param {string} channel @param {RedisArg} message */
  async publish(channel, message) {
    return this._exec("PUBLISH", channel, message);
  }

  /** @param {string} key @param {Record<string, RedisArg>} fields @param {RedisXAddOptions} [opts] */
  async xadd(key, fields, opts = {}) {
    /** @type {RedisCommand} */
    const args = ["XADD", key];
    if (opts.maxlen) args.push("MAXLEN", "~", String(opts.maxlen));
    args.push("*");
    for (const [field, value] of Object.entries(fields)) args.push(field, value);
    const result = await this._exec(...args);
    return utf8Decoder.decode(/** @type {Uint8Array} */ (result));
  }

  /** @param {string} key @param {number|string} min @param {number|string} max @param {RedisZRangeByScoreOptions} [opts] */
  async zrangebyscore(key, min, max, opts = {}) {
    const args = ["ZRANGEBYSCORE", key, String(min), String(max)];
    if (opts.limit) args.push("LIMIT", String(opts.limit[0]), String(opts.limit[1]));
    const result = /** @type {Uint8Array[] | null} */ (await this._exec(...args));
    if (!result) return [];
    return decodeStringArray(result);
  }

  /** @param {string} key @param {...string} members */
  async zrem(key, ...members) {
    return this._exec("ZREM", key, ...members);
  }

  /** @param {string} cursor @param {string} match @param {number} [count] @returns {Promise<[string, string[]]>} */
  async scan(cursor, match, count = 100) {
    const result = /** @type {[Uint8Array, Uint8Array[]]} */ (
      await this._exec("SCAN", cursor, "MATCH", match, "COUNT", String(count))
    );
    return [utf8Decoder.decode(result[0]), result[1].map((k) => utf8Decoder.decode(k))];
  }

  // --- camelCase surface: decodes bulk strings, matches node-redis.
  // Binary-safe runtime paths stay on the lowercase methods above.

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
      await this._exec("HGETEX", key, "EX", String(ttlSeconds), "FIELDS", String(fields.length), ...fields)
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

  /** @param {string} key @param {...RedisHSetArg} rest */
  async hSet(key, ...rest) {
    return /** @type {number} */ (await this._exec(...buildHSetArgs(key, rest)));
  }

  /** @param {string} key @param {number} ttlSeconds @param {Record<string, RedisArg>} fields */
  async hSetEx(key, ttlSeconds, fields) {
    /** @type {RedisCommand} */
    const args = ["HSETEX", key, "EX", String(ttlSeconds), "FIELDS"];
    const entries = Object.entries(fields);
    args.push(String(entries.length));
    for (const [field, value] of entries) args.push(field, value);
    return /** @type {number} */ (await this._exec(...args));
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

  /** @param {string} key @param {string|string[]} members */
  async sAdd(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    return /** @type {number} */ (await this._exec("SADD", key, ...arr));
  }

  /** @param {string} key @param {string|string[]} members */
  async sRem(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    return /** @type {number} */ (await this._exec("SREM", key, ...arr));
  }

  /** @param {string} key */
  async sMembers(key) { return this.smembers(key); }
  /** @param {string} key @param {string} member */
  async sIsMember(key, member) { return this.sismember(key, member); }

  /** @param {string} key */
  async incr(key) { return /** @type {number} */ (await this._exec("INCR", key)); }

  /** @param {string} key */
  async zCard(key) { return /** @type {number} */ (await this._exec("ZCARD", key)); }
  /** @param {string} key @param {number} start @param {number} stop */
  async zRange(key, start, stop) {
    const arr = /** @type {Uint8Array[] | null} */ (
      await this._exec("ZRANGE", key, String(start), String(stop))
    );
    if (!arr) return [];
    return decodeStringArray(arr);
  }

  /** @param {...string} keys */
  async exists(...keys) { return /** @type {number} */ (await this._exec("EXISTS", ...keys)); }

  /** @param {string} src @param {string} dst @param {RedisCopyOptions} [opts] */
  async copy(src, dst, opts = {}) {
    const args = ["COPY", src, dst];
    if (opts.REPLACE || opts.replace) args.push("REPLACE");
    return /** @type {number} */ (await this._exec(...args));
  }

  // Raw stream commands. RESP shape returned unchanged: decoding is caller's job.
  /** @param {...RedisArg} args */
  async xRead(...args) { return this._exec("XREAD", ...args); }
  /** @param {...RedisArg} args */
  async xRange(...args) {
    return /** @type {[Uint8Array, Uint8Array[]][]} */ (await this._exec("XRANGE", ...args));
  }

  /** @template T @param {(session: RedisSession) => Promise<T>} fn @returns {Promise<T>} */
  async session(fn) {
    const session = new RedisSession(this.address, {
      db: this.db,
      onCommand: this.onCommand,
      connect: this._connect,
    });
    await session.open();
    try {
      return await fn(session);
    } finally {
      await session.close();
    }
  }
}
