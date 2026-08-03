import { connect } from "cloudflare:sockets";
import { RedisCommandSurface } from "shared-redis-command-surface";
import { RedisSession } from "shared-redis-session";
import { errorMessage } from "./errors.js";
import {
  decodeRedisTimeMs,
  decodeStringArray,
  encodeCommand,
  writeRedisCommands,
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
 * @typedef {import("shared-redis-resp").RedisXAddOptions} RedisXAddOptions
 * @typedef {import("shared-redis-resp").RedisZRangeByScoreOptions} RedisZRangeByScoreOptions
 * @typedef {import("shared-redis-resp").RedisClientOptions} RedisClientOptions
 */

/** @extends {RedisCommandSurface<Uint8Array | null>} */
export class RedisClient extends RedisCommandSurface {
  /** @param {string} address @param {RedisClientOptions} [opts] */
  constructor(address, opts = {}) {
    super();
    this.address = address;
    this.db = normalizeRedisDb(opts.db);
    this.onCommand = opts.onCommand || null;
    this._connect = opts.connect || connect;
    if (
      opts.commandTimeoutMs !== undefined &&
      (!Number.isSafeInteger(opts.commandTimeoutMs) || opts.commandTimeoutMs <= 0)
    ) {
      throw new RangeError("Redis command timeout must be a positive safe integer");
    }
    // This deadline owns socket-per-call commands only. session() fails fast
    // instead of silently opening a held connection without that guarantee.
    this.commandTimeoutMs = opts.commandTimeoutMs ?? null;
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
    const commandTimeoutMs = this.commandTimeoutMs;
    /** @type {ReturnType<typeof setTimeout> | null} */
    let timeout = null;
    try {
      const parser = new RespReader(reader);
      const operation = (async () => {
        if (this.db > 0) {
          await writer.write(encodeCommand(["SELECT", String(this.db)]));
          await parser.parseOne();
          parser.compact();
        }
        return await fn(writer, reader, parser);
      })();
      const result = commandTimeoutMs == null
        ? await operation
        : await Promise.race([
            operation,
            new Promise((_, reject) => {
              timeout = setTimeout(() => {
                try { socket.close?.(); } catch { /* timeout still rejects */ }
                reject(new RedisCommandTimeoutError(command, commandTimeoutMs));
              }, commandTimeoutMs);
            }),
          ]);
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
      if (timeout !== null) clearTimeout(timeout);
      try { void Promise.resolve(writer.close()).catch(() => {}); } catch { /* already closed */ }
      try { reader.releaseLock(); } catch { /* already released */ }
      try { socket.close?.(); } catch { /* already closed */ }
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
      await writeRedisCommands(writer, commands);
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

  /** @param {string[]} keys */
  async getManyWithTime(keys) {
    if (keys.length === 0) throw new Error("getManyWithTime requires at least one key");
    const replies = await this._execPipeline("GET_TIME_PIPELINE", [
      ...keys.map((key) => /** @type {RedisCommand} */ (["GET", key])),
      ["TIME"],
    ]);
    return {
      values: /** @type {Array<Uint8Array | null>} */ (replies.slice(0, keys.length)),
      nowMs: decodeRedisTimeMs(replies[keys.length]),
    };
  }

  /** @param {string[]} keys */
  async getMany(keys) {
    return /** @type {Array<Uint8Array | null>} */ (await this._execPipeline(
      "GET_PIPELINE",
      keys.map((key) => ["GET", key])
    ));
  }

  /** @param {Array<[string, RedisArg]>} entries */
  async delIfEqMany(entries) {
    return /** @type {number[]} */ (await this._execPipeline(
      "DELIFEQ_PIPELINE",
      entries.map(([key, value]) => ["DELIFEQ", key, value])
    ));
  }

  /** @param {string} script @param {string[]} [keys] @param {RedisArg[]} [args] */
  async eval(script, keys = [], args = []) {
    return this._exec("EVAL", script, String(keys.length), ...keys, ...args);
  }

  /** @param {RedisCommand[]} cmdList */
  async multiExec(cmdList) {
    return this._withSocket("MULTI_EXEC", async (writer, _reader, resp) => {
      await writeRedisCommands(writer, [["MULTI"], ...cmdList, ["EXEC"]]);
      await resp.parseOne();
      for (let i = 0; i < cmdList.length; i += 1) await resp.parseOne();
      return resp.parseOne();
    });
  }

  async ping() {
    return this._exec("PING");
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

  /** @param {string} key @param {string} start @param {string} end @param {number} count */
  async existsAndXRange(key, start, end, count) {
    const [exists, entries] = await this._execPipeline("EXISTS_XRANGE_PIPELINE", [
      ["EXISTS", key],
      ["XRANGE", key, start, end, "COUNT", String(count)],
    ]);
    return {
      exists: /** @type {number} */ (exists) > 0,
      entries: /** @type {[Uint8Array, Uint8Array[]][]} */ (entries),
    };
  }

  /** @template T @param {(session: RedisSession) => Promise<T>} fn @returns {Promise<T>} */
  async session(fn) {
    if (this.commandTimeoutMs !== null) {
      throw new Error("RedisClient.session() does not support commandTimeoutMs");
    }
    const session = new RedisSession(this.address, {
      db: this.db,
      onCommand: this.onCommand,
      connect: this._connect,
    });
    try {
      await session.open();
      return await fn(session);
    } finally {
      await session.close();
    }
  }
}

export class RedisCommandTimeoutError extends Error {
  /** @param {string} command @param {number} timeoutMs */
  constructor(command, timeoutMs) {
    super(`Redis ${command} command timed out after ${timeoutMs}ms`);
    this.name = "RedisCommandTimeoutError";
    this.command = command;
    this.timeoutMs = timeoutMs;
  }
}
