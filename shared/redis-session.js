import { connect } from "cloudflare:sockets";
import {
  WatchError,
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
import { errorMessage } from "./errors.js";

/**
 * @typedef {import("shared-redis-resp").RedisArg} RedisArg
 * @typedef {import("shared-redis-resp").RedisCommand} RedisCommand
 * @typedef {import("shared-redis-resp").RedisCommandEvent} RedisCommandEvent
 * @typedef {import("shared-redis-resp").RedisHSetArg} RedisHSetArg
 * @typedef {import("shared-redis-resp").RedisSetOptions} RedisSetOptions
 * @typedef {import("shared-redis-resp").RedisXAddOptions} RedisXAddOptions
 * @typedef {import("shared-redis-resp").RedisCopyOptions} RedisCopyOptions
 * @typedef {import("shared-redis-resp").RedisSocket} RedisSocket
 * @typedef {import("shared-redis-resp").RedisClientOptions} RedisClientOptions
 * @typedef {{ writer: WritableStreamDefaultWriter<Uint8Array>, parser: RespReader }} RedisSessionIo
 */

/** @param {RedisSession} session @returns {RedisSessionIo} */
function requireSessionIo(session) {
  if (!session.writer || !session.parser) throw new Error("Redis session not open");
  return { writer: session.writer, parser: session.parser };
}

// One socket held open across WATCH -> reads -> MULTI/EXEC -> UNWATCH.
// camelCase / string-decoded API: byte-safe runtime reads use RedisClient's
// lowercase methods on fresh sockets instead.
export class RedisSession {
  /** @param {string} address @param {RedisClientOptions} [opts] */
  constructor(address, opts = {}) {
    this.address = address;
    this.db = normalizeRedisDb(opts.db);
    this.onCommand = opts.onCommand || null;
    this._connect = opts.connect || connect;
    this._observerWarned = false;
    /** @type {RedisSocket | null} */
    this.socket = null;
    /** @type {WritableStreamDefaultWriter<Uint8Array> | null} */
    this.writer = null;
    /** @type {ReadableStreamDefaultReader<Uint8Array> | null} */
    this.reader = null;
    /** @type {RespReader | null} */
    this.parser = null;
    this._closed = false;
  }

  async open() {
    if (this._closed) throw new Error("Redis session closed");
    if (this.socket) return this;
    const socket = this._connect(this.address);
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    const parser = new RespReader(reader);
    this.socket = socket;
    this.writer = writer;
    this.reader = reader;
    this.parser = parser;
    if (this.db > 0) {
      await writer.write(encodeCommand(["SELECT", String(this.db)]));
      await parser.parseOne();
      parser.compact();
    }
    return this;
  }

  hasOpenResources() {
    return !this._closed && Boolean(this.socket || this.writer || this.reader || this.parser);
  }

  async close() {
    if (this._closed) return;
    this._closed = true;
    const writer = this.writer;
    const reader = this.reader;
    const socket = this.socket;
    this.writer = null;
    this.reader = null;
    this.parser = null;
    this.socket = null;
    try { writer?.close(); } catch { /* already closed */ }
    try { reader?.releaseLock(); } catch { /* already released */ }
    try { socket?.close?.(); } catch { /* already closed */ }
  }

  /** @param {RedisCommandEvent} event */
  _emitCommand(event) {
    if (!this.onCommand) return;
    try {
      this.onCommand(event);
    } catch (err) {
      if (!this._observerWarned) {
        this._observerWarned = true;
        warnRedisCallback("redis_session_observer_callback_threw", err);
      }
    }
  }

  // Single RESP command on the held socket. Caller handles typed decode.
  /** @param {...RedisArg} args */
  async _exec(...args) {
    if (this._closed) throw new Error("Redis session closed");
    const { writer, parser } = requireSessionIo(this);
    const command = String(args[0] || "UNKNOWN").toUpperCase();
    const startedAt = Date.now();
    try {
      await writer.write(encodeCommand(args));
      const reply = await parser.parseOne();
      parser.compact();
      this._emitCommand({ command, duration_ms: Date.now() - startedAt, ok: true });
      return reply;
    } catch (err) {
      this._emitCommand({
        command,
        duration_ms: Date.now() - startedAt,
        ok: false,
        error_message: errorMessage(err),
      });
      throw err;
    }
  }

  /** @param {string} command @param {RedisCommand[]} commands */
  async _execPipeline(command, commands) {
    if (this._closed) throw new Error("Redis session closed");
    if (commands.length === 0) return [];
    const { writer, parser } = requireSessionIo(this);
    const startedAt = Date.now();
    try {
      await writer.write(concatBuffers(commands.map((args) => encodeCommand(args))));
      const replies = [];
      for (let i = 0; i < commands.length; i += 1) replies.push(await parser.parseOne());
      parser.compact();
      this._emitCommand({
        command,
        duration_ms: Date.now() - startedAt,
        ok: true,
        count: commands.length,
      });
      return replies;
    } catch (err) {
      this._emitCommand({
        command,
        duration_ms: Date.now() - startedAt,
        ok: false,
        count: commands.length,
        error_message: errorMessage(err),
      });
      throw err;
    }
  }

  /** @param {...string} keys */
  async watch(...keys) { return this._exec("WATCH", ...keys); }
  async unwatch() { return this._exec("UNWATCH"); }

  /** @param {string} key @param {string} field */
  async hGet(key, field) { return decodeBulk(await this._exec("HGET", key, field)); }
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
  async hExists(key, field) { return (await this._exec("HEXISTS", key, field)) === 1; }

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
    return this._exec("SADD", key, ...arr);
  }
  /** @param {string} key @param {string|string[]} members */
  async sRem(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    return this._exec("SREM", key, ...arr);
  }
  /** @param {string} key */
  async sMembers(key) {
    const arr = /** @type {unknown[] | null} */ (await this._exec("SMEMBERS", key));
    return decodeStringArray(arr);
  }
  /** @param {string} key @param {string} member */
  async sIsMember(key, member) { return (await this._exec("SISMEMBER", key, member)) === 1; }
  /** @param {string} key @param {...string} members */
  async sMIsMember(key, ...members) {
    if (members.length === 0) return [];
    const arr = /** @type {number[]} */ (await this._exec("SMISMEMBER", key, ...members));
    return arr.map((x) => x === 1);
  }

  /** @param {string} key */
  async incr(key) { return /** @type {number} */ (await this._exec("INCR", key)); }
  /** @param {string} key */
  async get(key) { return decodeBulk(await this._exec("GET", key)); }
  /** @param {string[]} keys */
  async getMany(keys) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "GET_PIPELINE",
      keys.map((key) => ["GET", key])
    ));
    return replies.map(decodeBulk);
  }
  /** @param {string} key */
  async getWithTime(key) {
    const [value, time] = await this._execPipeline("GET_TIME_PIPELINE", [
      ["GET", key],
      ["TIME"],
    ]);
    return {
      value: decodeBulk(value),
      nowMs: decodeRedisTimeMs(time),
    };
  }
  async time() { return decodeRedisTimeMs(await this._exec("TIME")); }
  /** @param {...string} keys */
  async del(...keys) { return /** @type {number} */ (await this._exec("DEL", ...keys)); }
  /** @param {string} key @param {string} value */
  async delIfEq(key, value) { return /** @type {number} */ (await this._exec("DELIFEQ", key, value)); }
  /** @param {string} channel @param {RedisArg} message */
  async publish(channel, message) { return this._exec("PUBLISH", channel, message); }

  /** @param {string} key */
  async zCard(key) { return this._exec("ZCARD", key); }
  /** @param {string} key @param {number} start @param {number} stop */
  async zRange(key, start, stop) {
    const arr = /** @type {unknown[] | null} */ (
      await this._exec("ZRANGE", key, String(start), String(stop))
    );
    return decodeStringArray(arr);
  }

  /** @param {string[]} keys @param {number} start @param {number} stop */
  async zRangeMany(keys, start, stop) {
    const replies = /** @type {(unknown[] | null)[]} */ (await this._execPipeline(
      "ZRANGE_PIPELINE",
      keys.map((key) => ["ZRANGE", key, String(start), String(stop)])
    ));
    return replies.map(decodeStringArray);
  }

  /** @param {...string} keys */
  async exists(...keys) { return /** @type {number} */ (await this._exec("EXISTS", ...keys)); }

  /** @param {string[]} keys */
  async existsMany(keys) {
    const replies = /** @type {number[]} */ (await this._execPipeline(
      "EXISTS_PIPELINE",
      keys.map((key) => ["EXISTS", key])
    ));
    return replies.map((value) => value > 0);
  }

  // Raw stream commands. Same byte-preserving contract as RedisClient's
  // counterparts: caller decodes.
  /** @param {...RedisArg} args */
  async xRead(...args) { return this._exec("XREAD", ...args); }
  /** @param {...RedisArg} args */
  async xRange(...args) {
    return /** @type {[Uint8Array, Uint8Array[]][]} */ (await this._exec("XRANGE", ...args));
  }

  // Returns [nextCursor: string, keys: string[]].
  /** @param {string} cursor @param {string} match @param {number} [count] @returns {Promise<[string, string[]]>} */
  async scan(cursor, match, count = 100) {
    const result = /** @type {[Uint8Array, Uint8Array[]]} */ (
      await this._exec("SCAN", cursor, "MATCH", match, "COUNT", String(count))
    );
    return [utf8Decoder.decode(result[0]), result[1].map((k) => utf8Decoder.decode(k))];
  }

  /** @param {string} src @param {string} dst @param {RedisCopyOptions} [opts] */
  async copy(src, dst, opts = {}) {
    const args = ["COPY", src, dst];
    if (opts.REPLACE || opts.replace) args.push("REPLACE");
    return /** @type {number} */ (await this._exec(...args));
  }

  multi() { return new RedisMulti(this); }
}

export class RedisMulti {
  /** @param {RedisSession} session */
  constructor(session) {
    this._session = session;
    /** @type {RedisCommand[]} */
    this._commands = [];
  }

  /** @param {string} key @param {...RedisHSetArg} rest */
  hSet(key, ...rest) { this._commands.push(buildHSetArgs(key, rest)); return this; }
  /** @param {string} key @param {...string} fields */
  hDel(key, ...fields) { this._commands.push(["HDEL", key, ...fields]); return this; }
  /** @param {string} key @param {string|string[]} members */
  sAdd(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    this._commands.push(["SADD", key, ...arr]); return this;
  }
  /** @param {string} key @param {string|string[]} members */
  sRem(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    this._commands.push(["SREM", key, ...arr]); return this;
  }
  /** @param {...string} keys */
  del(...keys) { this._commands.push(["DEL", ...keys]); return this; }
  /** @param {string} key @param {RedisArg} value @param {RedisSetOptions} [opts] */
  set(key, value, opts = {}) {
    this._commands.push(buildSetArgs(key, value, opts));
    return this;
  }
  /** @param {string} channel @param {RedisArg} message */
  publish(channel, message) { this._commands.push(["PUBLISH", channel, message]); return this; }
  /** @param {string} key @param {number|string} score @param {string} member */
  zAdd(key, score, member) {
    this._commands.push(["ZADD", key, String(score), member]);
    return this;
  }
  /** @param {string} key @param {Record<string, RedisArg>} fields @param {RedisXAddOptions} [opts] */
  xAdd(key, fields, opts = {}) {
    /** @type {RedisCommand} */
    const args = ["XADD", key];
    if (opts.maxlen) args.push("MAXLEN", "~", String(opts.maxlen));
    args.push("*");
    for (const [field, value] of Object.entries(fields)) args.push(field, value);
    this._commands.push(args);
    return this;
  }
  /** @param {string} key @param {string|string[]} members */
  zRem(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    this._commands.push(["ZREM", key, ...arr]); return this;
  }
  /** @param {string} key @param {number} timestampSec */
  expireAt(key, timestampSec) {
    this._commands.push(["EXPIREAT", key, String(timestampSec)]); return this;
  }
  /** @param {string} src @param {string} dst @param {RedisCopyOptions} [opts] */
  copy(src, dst, opts = {}) {
    const args = ["COPY", src, dst];
    if (opts.REPLACE || opts.replace) args.push("REPLACE");
    this._commands.push(args);
    return this;
  }

  async exec() {
    const session = this._session;
    if (session._closed) throw new Error("Redis session closed");
    const { writer, parser } = requireSessionIo(session);
    const parts = [encodeCommand(["MULTI"])];
    for (const cmd of this._commands) parts.push(encodeCommand(cmd));
    parts.push(encodeCommand(["EXEC"]));
    const buf = concatBuffers(parts);

    const startedAt = Date.now();
    try {
      await writer.write(buf);
      await parser.parseOne(); parser.compact();
      for (let i = 0; i < this._commands.length; i += 1) {
        await parser.parseOne(); parser.compact();
      }
      const result = await parser.parseOne();
      parser.compact();
      if (result === null) {
        session._emitCommand({
          command: "MULTI_EXEC",
          duration_ms: Date.now() - startedAt,
          ok: false,
          error_message: "watch invalidation",
        });
        throw new WatchError();
      }
      session._emitCommand({ command: "MULTI_EXEC", duration_ms: Date.now() - startedAt, ok: true });
      return result;
    } catch (err) {
      if (!(err instanceof WatchError)) {
        session._emitCommand({
          command: "MULTI_EXEC",
          duration_ms: Date.now() - startedAt,
          ok: false,
          error_message: errorMessage(err),
        });
      }
      throw err;
    }
  }
}
