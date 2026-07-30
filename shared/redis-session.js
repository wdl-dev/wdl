import { connect } from "cloudflare:sockets";
import { RedisCommandSurface } from "shared-redis-command-surface";
import {
  WatchError,
  buildHSetArgs,
  buildSetArgs,
  decodeBulk,
  decodeHashObject,
  decodeRedisTimeMs,
  decodeStringArray,
  encodeCommand,
  writeRedisCommands,
  normalizeRedisDb,
  warnRedisCallback,
  RespReader,
  RedisReplyError,
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
/** @extends {RedisCommandSurface<string | null | undefined>} */
export class RedisSession extends RedisCommandSurface {
  /** @param {string} address @param {RedisClientOptions} [opts] */
  constructor(address, opts = {}) {
    super();
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
    try {
      const socket = this._connect(this.address);
      this.socket = socket;
      const writer = socket.writable.getWriter();
      this.writer = writer;
      const reader = socket.readable.getReader();
      this.reader = reader;
      const parser = new RespReader(reader);
      this.parser = parser;
      if (this.db > 0) {
        await writer.write(encodeCommand(["SELECT", String(this.db)]));
        await parser.parseOne();
        parser.compact();
      }
      return this;
    } catch (err) {
      await this.close();
      throw err;
    }
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
      if (err instanceof RedisReplyError) {
        parser.compact();
      } else {
        await this.close();
      }
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
      await writeRedisCommands(writer, commands);
      const replies = [];
      /** @type {RedisReplyError | null} */
      let firstReplyError = null;
      for (let i = 0; i < commands.length; i += 1) {
        try {
          replies.push(await parser.parseOne());
        } catch (err) {
          if (!(err instanceof RedisReplyError)) throw err;
          if (!firstReplyError) firstReplyError = err;
        }
      }
      parser.compact();
      if (firstReplyError) throw firstReplyError;
      this._emitCommand({
        command,
        duration_ms: Date.now() - startedAt,
        ok: true,
        count: commands.length,
      });
      return replies;
    } catch (err) {
      if (!(err instanceof RedisReplyError)) await this.close();
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

  /**
   * WATCH must be the first command so every following read is protected when
   * Redis evaluates it. The typed public helpers below keep reply decoding at
   * the same owner as the corresponding non-WATCH pipeline.
   *
   * @param {string} command
   * @param {string[]} watchKeys
   * @param {RedisCommand[]} readCommands
   */
  async _watchAndExecPipeline(command, watchKeys, readCommands) {
    if (watchKeys.length === 0) {
      throw new Error("WATCH snapshot requires at least one key");
    }
    if (readCommands.length === 0) {
      throw new Error("WATCH snapshot requires at least one read");
    }
    const replies = await this._execPipeline(command, [
      ["WATCH", ...watchKeys],
      ...readCommands,
    ]);
    return replies.slice(1);
  }

  /** @param {...string} keys */
  async watch(...keys) { return this._exec("WATCH", ...keys); }
  async unwatch() { return this._exec("UNWATCH"); }

  /**
   * @param {Array<[string, string]>} fieldPairs
   * @param {string[]} hashKeys
   */
  async hGetManyAndHGetAllMany(fieldPairs, hashKeys) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "HGET_HGETALL_PIPELINE",
      [
        ...fieldPairs.map(([key, field]) => ["HGET", key, field]),
        ...hashKeys.map((key) => ["HGETALL", key]),
      ]
    ));
    return {
      fields: replies.slice(0, fieldPairs.length).map(decodeBulk),
      hashes: replies.slice(fieldPairs.length)
        .map((reply) => decodeHashObject(/** @type {unknown[] | null} */ (reply))),
    };
  }

  /**
   * @param {Array<[string, string]>} fieldPairs
   * @param {string[]} setKeys
   */
  async hGetManyAndSMembersMany(fieldPairs, setKeys) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "HGET_SMEMBERS_PIPELINE",
      [
        ...fieldPairs.map(([key, field]) => ["HGET", key, field]),
        ...setKeys.map((key) => ["SMEMBERS", key]),
      ]
    ));
    return {
      fields: replies.slice(0, fieldPairs.length).map(decodeBulk),
      memberLists: replies.slice(fieldPairs.length)
        .map((reply) => decodeStringArray(/** @type {unknown[] | null} */ (reply))),
    };
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

  /**
   * @param {string[]} watchKeys
   * @param {string} hashKey
   * @param {string} stringKey
   */
  async watchAndHGetAllAndGet(watchKeys, hashKey, stringKey) {
    const [hashReply, valueReply] = await this._watchAndExecPipeline(
      "HGETALL_GET_PIPELINE",
      watchKeys,
      [
        ["HGETALL", hashKey],
        ["GET", stringKey],
      ]
    );
    return {
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      value: decodeBulk(valueReply),
    };
  }

  /** @param {string} hashKey @param {string} stringKey @param {string} setKey */
  async hGetAllGetSMembers(hashKey, stringKey, setKey) {
    const [hashReply, valueReply, membersReply] = await this._execPipeline(
      "HGETALL_GET_SMEMBERS_PIPELINE",
      [
        ["HGETALL", hashKey],
        ["GET", stringKey],
        ["SMEMBERS", setKey],
      ]
    );
    return {
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      value: decodeBulk(valueReply),
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
    };
  }

  /**
   * @param {string[]} watchKeys
   * @param {string} hashKey
   * @param {string} stringKey
   * @param {string} setKey
   */
  async watchAndHGetAllGetSMembers(watchKeys, hashKey, stringKey, setKey) {
    const [hashReply, valueReply, membersReply] = await this._watchAndExecPipeline(
      "HGETALL_GET_SMEMBERS_PIPELINE",
      watchKeys,
      [
        ["HGETALL", hashKey],
        ["GET", stringKey],
        ["SMEMBERS", setKey],
      ]
    );
    return {
      hash: decodeHashObject(/** @type {unknown[] | null} */ (hashReply)),
      value: decodeBulk(valueReply),
      members: decodeStringArray(/** @type {unknown[] | null} */ (membersReply)),
    };
  }

  /**
   * @param {string[]} watchKeys
   * @param {string[]} existenceKeys
   * @param {Array<[string, string]>} lengthPairs
   */
  async watchAndExistsAndHStrLenMany(watchKeys, existenceKeys, lengthPairs) {
    const existenceCommand = existenceKeys.length > 0
      ? [["EXISTS", ...existenceKeys]]
      : [];
    const replies = /** @type {number[]} */ (await this._watchAndExecPipeline(
      "EXISTS_HSTRLEN_PIPELINE",
      watchKeys,
      [
        ...existenceCommand,
        ...lengthPairs.map(([key, field]) => ["HSTRLEN", key, field]),
      ]
    ));
    return {
      existsCount: existenceKeys.length > 0 ? replies[0] : 0,
      lengths: replies.slice(existenceCommand.length),
    };
  }

  /** @param {string[]} watchKeys @param {string} key */
  async watchAndSMembers(watchKeys, key) {
    const [membersReply] = await this._watchAndExecPipeline(
      "SMEMBERS_PIPELINE",
      watchKeys,
      [["SMEMBERS", key]]
    );
    return decodeStringArray(/** @type {unknown[] | null} */ (membersReply));
  }
  /** @param {string} key @param {...string} members */
  async sMIsMember(key, ...members) {
    if (members.length === 0) return [];
    const arr = /** @type {number[]} */ (await this._exec("SMISMEMBER", key, ...members));
    return arr.map((x) => x === 1);
  }

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
  /**
   * Read string keys and hash fields in one pipeline flush. `values` aligns to
   * `getKeys`, `fields` aligns to `hgetPairs`; callers that batch several
   * independent hash lookups pass them concatenated and slice `fields` back.
   *
   * @param {string[]} getKeys
   * @param {Array<[string, string]>} hgetPairs
   */
  async getManyAndHGetMany(getKeys, hgetPairs) {
    const replies = /** @type {unknown[]} */ (await this._execPipeline(
      "GET_HGET_PIPELINE",
      [
        ...getKeys.map((key) => ["GET", key]),
        ...hgetPairs.map(([key, field]) => ["HGET", key, field]),
      ]
    ));
    return {
      values: replies.slice(0, getKeys.length).map(decodeBulk),
      fields: replies.slice(getKeys.length).map(decodeBulk),
    };
  }
  /**
   * @param {string[]} watchKeys
   * @param {string[]} getKeys
   * @param {Array<[string, string]>} hgetPairs
   */
  async watchAndGetManyAndHGetMany(watchKeys, getKeys, hgetPairs) {
    const replies = /** @type {unknown[]} */ (await this._watchAndExecPipeline(
      "GET_HGET_PIPELINE",
      watchKeys,
      [
        ...getKeys.map((key) => ["GET", key]),
        ...hgetPairs.map(([key, field]) => ["HGET", key, field]),
      ]
    ));
    return {
      values: replies.slice(0, getKeys.length).map(decodeBulk),
      fields: replies.slice(getKeys.length).map(decodeBulk),
    };
  }
  /** @param {string[]} keys */
  async getManyWithTime(keys) {
    if (keys.length === 0) throw new Error("getManyWithTime requires at least one key");
    const replies = await this._execPipeline("GET_TIME_PIPELINE", [
      ...keys.map((key) => /** @type {RedisCommand} */ (["GET", key])),
      ["TIME"],
    ]);
    return {
      values: replies.slice(0, -1).map(decodeBulk),
      nowMs: decodeRedisTimeMs(replies.at(-1)),
    };
  }
  /** @param {string[]} watchKeys @param {string[]} keys */
  async watchAndExistsMany(watchKeys, keys) {
    const replies = /** @type {number[]} */ (await this._watchAndExecPipeline(
      "EXISTS_PIPELINE",
      watchKeys,
      keys.map((key) => ["EXISTS", key])
    ));
    return replies.map((value) => value > 0);
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
    if (arr.length === 0) return this;
    this._commands.push(["SADD", key, ...arr]); return this;
  }
  /** @param {string} key @param {string|string[]} members */
  sRem(key, members) {
    const arr = Array.isArray(members) ? members : [members];
    if (arr.length === 0) return this;
    this._commands.push(["SREM", key, ...arr]); return this;
  }
  /** @param {...string} keys */
  del(...keys) { this._commands.push(["DEL", ...keys]); return this; }
  /** @param {string} key @param {RedisArg} value @param {RedisSetOptions} [opts] */
  set(key, value, opts = {}) {
    this._commands.push(buildSetArgs(key, value, opts));
    return this;
  }
  /** @param {string} key */
  incr(key) { this._commands.push(["INCR", key]); return this; }
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
    const commands = [["MULTI"], ...this._commands, ["EXEC"]];

    const startedAt = Date.now();
    try {
      await writeRedisCommands(writer, commands);
      const replyCount = this._commands.length + 2;
      /** @type {RedisReplyError | null} */
      let firstReplyError = null;
      /** @type {unknown} */
      let result;
      for (let i = 0; i < replyCount; i += 1) {
        try {
          const reply = await parser.parseOne();
          if (i === replyCount - 1) result = reply;
        } catch (err) {
          if (!(err instanceof RedisReplyError)) throw err;
          if (!firstReplyError) firstReplyError = err;
        }
        parser.compact();
      }
      if (firstReplyError) throw firstReplyError;
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
      if (!(err instanceof RedisReplyError) && !(err instanceof WatchError)) {
        await session.close();
      }
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
