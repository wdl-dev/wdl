import { createLogger } from "shared-observability";
import { utf8ByteLength } from "shared-utf8";
import { errorMessage } from "./errors.js";

/**
 * @typedef {string | number | boolean | Uint8Array} RedisArg
 * @typedef {RedisArg[]} RedisCommand
 * @typedef {string | number | null | Uint8Array | unknown[]} RedisReply
 * @typedef {RedisArg | Record<string, RedisArg>} RedisHSetArg
 * @typedef {{ command: string, duration_ms: number, ok: boolean, count?: number, error_message?: string }} RedisCommandEvent
 * @typedef {{ readable: ReadableStream<Uint8Array>, writable: WritableStream<Uint8Array>, close?: () => void }} RedisSocket
 * @typedef {(address: string) => RedisSocket} RedisSocketFactory
 * @typedef {{ db?: string | number, onCommand?: ((event: RedisCommandEvent) => void) | null, connect?: RedisSocketFactory }} RedisClientOptions
 * @typedef {{ ttl?: number, exat?: number, nx?: boolean, xx?: boolean, ifeq?: RedisArg }} RedisSetOptions
 * @typedef {{ maxlen?: number }} RedisXAddOptions
 * @typedef {{ limit?: [number, number] }} RedisZRangeByScoreOptions
 * @typedef {{ REPLACE?: boolean, replace?: boolean }} RedisCopyOptions
 * @typedef {{ onMessage?: ((channel: string, message: Uint8Array) => void) | null, onConnect?: (() => void) | null, onDisconnect?: (() => void) | null, onError?: ((err: unknown) => void) | null, backoff?: (attempt: number) => number, sleep?: (ms: number) => Promise<void>, connect?: RedisSocketFactory }} RedisSubscriberOptions
 */

export const utf8Decoder = new TextDecoder();
const utf8Encoder = new TextEncoder();
const redisCallbackLog = createLogger("shared-redis");
const RESP_CR = 0x0d;
const RESP_LF = 0x0a;
const ASCII_ZERO = 0x30;
const ASCII_NINE = 0x39;
const ASCII_MINUS = 0x2d;
const RESP_RETAINED_BUFFER_LIMIT = 64 * 1024;
const REDIS_WRITE_BATCH_BYTES = 256 * 1024;
const SHORT_UTF8_ARG_MAX_CODE_UNITS = 512;

/**
 * @typedef {{ args: RedisCommand, byteLength: number, longUtf8: Array<Uint8Array | undefined> | null }} RedisCommandPlan
 */

/**
 * @param {string} event
 * @param {unknown} err
 */
export function warnRedisCallback(event, err) {
  redisCallbackLog("warn", event, {
    error_message: errorMessage(err),
  });
}

/**
 * @param {Uint8Array} target
 * @param {number} offset
 * @param {string} text
 */
function writeAscii(target, offset, text) {
  for (let i = 0; i < text.length; i += 1) {
    target[offset + i] = text.charCodeAt(i);
  }
  return offset + text.length;
}

/**
 * @param {Uint8Array} buf
 * @param {number} start
 * @param {number} end
 */
function decodeRespLine(buf, start, end) {
  return utf8Decoder.decode(buf.subarray(start, end));
}

/**
 * @param {Uint8Array} buf
 * @param {number} start
 * @param {number} end
 * @param {string} label
 */
function parseRespIntegerBytes(buf, start, end, label) {
  const invalid = () => {
    throw new Error(`Invalid RESP ${label}: ${decodeRespLine(buf, start, end)}`);
  };
  if (start >= end) invalid();
  let pos = start;
  let negative = false;
  if (buf[pos] === ASCII_MINUS) {
    negative = true;
    pos += 1;
    if (pos >= end) invalid();
  }
  const first = buf[pos];
  if (first < ASCII_ZERO || first > ASCII_NINE) invalid();
  if (first === ASCII_ZERO && (negative || pos + 1 !== end)) invalid();
  let value = 0;
  for (; pos < end; pos += 1) {
    const digit = buf[pos];
    if (digit < ASCII_ZERO || digit > ASCII_NINE) invalid();
    const numericDigit = digit - ASCII_ZERO;
    if (value > (Number.MAX_SAFE_INTEGER - numericDigit) / 10) invalid();
    value = value * 10 + numericDigit;
  }
  return negative ? -value : value;
}

/**
 * @param {string} text
 * @param {string} label
 */
function parseRedisTimeInteger(text, label) {
  const invalid = () => {
    throw new Error(`Invalid Redis TIME ${label}`);
  };
  if (text.length === 0 || (text.length > 1 && text.charCodeAt(0) === ASCII_ZERO)) invalid();
  let value = 0;
  for (let i = 0; i < text.length; i += 1) {
    const digit = text.charCodeAt(i);
    if (digit < ASCII_ZERO || digit > ASCII_NINE) invalid();
    const numericDigit = digit - ASCII_ZERO;
    if (value > (Number.MAX_SAFE_INTEGER - numericDigit) / 10) invalid();
    value = value * 10 + numericDigit;
  }
  return value;
}

/**
 * @param {Uint8Array} buf
 * @param {number} pos
 */
function requireCrlfAt(buf, pos) {
  if (buf[pos] !== RESP_CR || buf[pos + 1] !== RESP_LF) {
    throw new Error("Invalid RESP bulk terminator");
  }
}

/**
 * @param {string} name
 * @param {unknown} value
 */
function positiveIntegerSetOption(name, value) {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Redis SET ${name} must be a positive integer`);
  }
  return value;
}

// EXEC returns nil array when any WATCHed key changed: Redis's CAS signal.
export class WatchError extends Error {
  constructor(message = "WATCH invalidation: EXEC aborted") {
    super(message);
    this.name = "WatchError";
  }
}

export class RedisReplyError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(`Redis error: ${message}`);
    this.name = "RedisReplyError";
  }
}

/** @param {RedisCommand} args @returns {RedisCommandPlan} */
function planCommand(args) {
  const argc = String(args.length);
  let total = 1 + argc.length + 2;
  /** @type {Array<Uint8Array | undefined> | null} */
  let longUtf8 = null;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    let byteLength;
    if (arg instanceof Uint8Array) {
      byteLength = arg.byteLength;
    } else {
      const value = String(arg);
      if (value.length <= SHORT_UTF8_ARG_MAX_CODE_UNITS) {
        byteLength = utf8ByteLength(value);
      } else {
        const bytes = utf8Encoder.encode(value);
        if (!longUtf8) longUtf8 = [];
        longUtf8[i] = bytes;
        byteLength = bytes.byteLength;
      }
    }
    total += 1 + String(byteLength).length + 2 + byteLength + 2;
  }
  return { args, byteLength: total, longUtf8 };
}

/**
 * @param {RedisCommandPlan[]} plans
 * @param {number} total
 */
function encodeCommandPlans(plans, total) {
  const out = new Uint8Array(total);
  let off = 0;
  for (const { args, longUtf8 } of plans) {
    off = writeAscii(out, off, "*");
    off = writeAscii(out, off, String(args.length));
    off = writeAscii(out, off, "\r\n");
    for (let i = 0; i < args.length; i += 1) {
      const arg = args[i];
      const value = arg instanceof Uint8Array ? arg : String(arg);
      const bytes = arg instanceof Uint8Array ? arg : longUtf8?.[i];
      const byteLength = bytes?.byteLength ?? utf8ByteLength(/** @type {string} */ (value));
      off = writeAscii(out, off, "$");
      off = writeAscii(out, off, String(byteLength));
      off = writeAscii(out, off, "\r\n");
      if (bytes) {
        out.set(bytes, off);
      } else {
        const encoded = utf8Encoder.encodeInto(
          /** @type {string} */ (value),
          out.subarray(off, off + byteLength)
        );
        if (encoded.read !== value.length || encoded.written !== byteLength) {
          throw new Error("Failed to encode Redis argument");
        }
      }
      off += byteLength;
      off = writeAscii(out, off, "\r\n");
    }
  }
  return out;
}

/** @param {RedisCommand[]} commands */
export function encodeCommands(commands) {
  const plans = commands.map(planCommand);
  return encodeCommandPlans(plans, plans.reduce((sum, plan) => sum + plan.byteLength, 0));
}

/** @param {RedisCommand} args */
export function encodeCommand(args) {
  return encodeCommands([args]);
}

/**
 * Write complete RESP commands in bounded buffers. Command frames stay intact;
 * only the transport write boundary changes.
 *
 * @param {WritableStreamDefaultWriter<Uint8Array>} writer
 * @param {RedisCommand[]} commands
 */
export async function writeRedisCommands(writer, commands) {
  /** @type {RedisCommandPlan[]} */
  let plans = [];
  let total = 0;
  for (const command of commands) {
    const plan = planCommand(command);
    if (plans.length > 0 && total + plan.byteLength > REDIS_WRITE_BATCH_BYTES) {
      await writer.write(encodeCommandPlans(plans, total));
      plans = [];
      total = 0;
    }
    plans.push(plan);
    total += plan.byteLength;
    if (total >= REDIS_WRITE_BATCH_BYTES) {
      await writer.write(encodeCommandPlans(plans, total));
      plans = [];
      total = 0;
    }
  }
  if (plans.length > 0) await writer.write(encodeCommandPlans(plans, total));
}

// Stateful RESP parser. Separate from one-shot exec so the subscriber can
// drive many replies off the same stream.
export class RespReader {
  /** @param {ReadableStreamDefaultReader<Uint8Array>} reader */
  constructor(reader) {
    this.reader = reader;
    this._storage = new Uint8Array(0);
    this.buf = this._storage.subarray(0, 0);
    this.pos = 0;
  }

  /** @param {boolean} releaseOversized */
  _compactConsumed(releaseOversized) {
    if (this.pos === 0) return 0;
    const consumed = this.pos;
    if (this.pos >= this.buf.length) {
      if (releaseOversized && this._storage.length > RESP_RETAINED_BUFFER_LIMIT) {
        this._storage = new Uint8Array(0);
      }
      this.buf = this._storage.subarray(0, 0);
      this.pos = 0;
      return consumed;
    }
    if (releaseOversized && this._storage.length > RESP_RETAINED_BUFFER_LIMIT) {
      this._storage = this.buf.slice(this.pos);
      this.buf = this._storage;
    } else {
      const remaining = this.buf.length - this.pos;
      this._storage.copyWithin(0, this.pos, this.buf.length);
      this.buf = this._storage.subarray(0, remaining);
    }
    this.pos = 0;
    return consumed;
  }

  /** @param {number} needed */
  _ensureCapacity(needed) {
    if (this._storage.length >= needed) return;
    const nextCapacity = Math.max(needed, this._storage.length * 2, 1024);
    const next = new Uint8Array(nextCapacity);
    next.set(this.buf);
    this._storage = next;
    this.buf = this._storage.subarray(0, this.buf.length);
  }

  async _readMore() {
    const { value, done } = await this.reader.read();
    if (done) throw new Error("Redis connection closed");
    const consumed = this._compactConsumed(false);
    const length = this.buf.length;
    const nextLength = length + value.length;
    this._ensureCapacity(nextLength);
    this._storage.set(value, length);
    this.buf = this._storage.subarray(0, nextLength);
    return consumed;
  }

  /** @param {number} from */
  async _findCRLF(from) {
    let scanFrom = from;
    while (true) {
      const cr = this.buf.indexOf(RESP_CR, scanFrom);
      if (cr !== -1) {
        if (cr + 1 < this.buf.length) {
          if (this.buf[cr + 1] === RESP_LF) return cr;
          scanFrom = cr + 1;
          continue;
        }
        const consumed = await this._readMore();
        scanFrom = Math.max(0, cr - consumed);
        continue;
      }
      const previousLength = this.buf.length;
      const consumed = await this._readMore();
      scanFrom = Math.max(0, previousLength - 1 - consumed);
    }
  }

  /** @param {number} targetLength */
  async _ensureBufferedLength(targetLength) {
    let target = targetLength;
    while (this.buf.length < target) {
      const consumed = await this._readMore();
      target = Math.max(0, target - consumed);
    }
  }

  /** @param {boolean} deferErrors @returns {Promise<RedisReply | RedisReplyError>} */
  async _parseOne(deferErrors) {
    await this._ensureBufferedLength(this.pos + 1);
    const type = String.fromCharCode(this.buf[this.pos]);
    this.pos++;
    const lineEnd = await this._findCRLF(this.pos);
    const lineStart = this.pos;
    this.pos = lineEnd + 2;
    if (type === "+") return decodeRespLine(this.buf, lineStart, lineEnd);
    if (type === "-") {
      const message = decodeRespLine(this.buf, lineStart, lineEnd);
      const error = new RedisReplyError(message);
      if (deferErrors) return error;
      throw error;
    }
    if (type === ":") return parseRespIntegerBytes(this.buf, lineStart, lineEnd, "integer");
    if (type === "$") {
      const len = parseRespIntegerBytes(this.buf, lineStart, lineEnd, "bulk length");
      if (len === -1) return null;
      if (len < 0) throw new Error(`Invalid RESP bulk length: ${decodeRespLine(this.buf, lineStart, lineEnd)}`);
      await this._ensureBufferedLength(this.pos + len + 2);
      const value = this.buf.slice(this.pos, this.pos + len);
      requireCrlfAt(this.buf, this.pos + len);
      this.pos += len + 2;
      return value;
    }
    if (type === "*") {
      const count = parseRespIntegerBytes(this.buf, lineStart, lineEnd, "array length");
      if (count === -1) return null;
      if (count < 0) throw new Error(`Invalid RESP array length: ${decodeRespLine(this.buf, lineStart, lineEnd)}`);
      const arr = [];
      /** @type {RedisReplyError | null} */
      let firstError = null;
      for (let i = 0; i < count; i += 1) {
        const item = await this._parseOne(true);
        if (item instanceof RedisReplyError) {
          if (!firstError) firstError = item;
        } else {
          arr.push(item);
        }
      }
      if (firstError) {
        if (deferErrors) return firstError;
        throw firstError;
      }
      return arr;
    }
    throw new Error(`Unknown RESP type: ${type}`);
  }

  /** @returns {Promise<RedisReply>} */
  async parseOne() {
    const reply = await this._parseOne(false);
    if (reply instanceof RedisReplyError) {
      throw reply;
    }
    return reply;
  }

  // Called between replies on the subscriber path so `buf` doesn't grow
  // unbounded over the isolate's lifetime.
  compact() {
    this._compactConsumed(true);
  }
}

/** @param {unknown} value @param {number} [fallback] */
export function normalizeRedisDb(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 0) {
    throw new TypeError(`Redis DB must be a non-negative integer; got ${JSON.stringify(value)}`);
  }
  return n;
}

/** @param {Record<string, unknown> | undefined | null} env @param {string} name @param {number} [fallback] */
export function redisDbFromEnv(env, name, fallback = 0) {
  return normalizeRedisDb(env?.[name], fallback);
}

/** @param {unknown} v @returns {string | null | undefined} */
export function decodeBulk(v) {
  if (v === null) return null;
  if (v === undefined) return undefined;
  if (typeof v === "string") return v;
  if (v instanceof Uint8Array) return utf8Decoder.decode(v);
  return String(v);
}

/**
 * @param {unknown} value
 * @param {string} label
 * @returns {number}
 */
function decodeRedisTimePart(value, label) {
  const text = decodeBulk(value);
  if (text == null) {
    throw new Error(`Invalid Redis TIME ${label}`);
  }
  return parseRedisTimeInteger(text, label);
}

/**
 * @param {unknown} reply
 * @returns {number}
 */
export function decodeRedisTimeMs(reply) {
  if (!Array.isArray(reply) || reply.length !== 2) {
    throw new Error("Invalid Redis TIME reply");
  }
  const seconds = decodeRedisTimePart(reply[0], "seconds");
  const microseconds = decodeRedisTimePart(reply[1], "microseconds");
  if (microseconds > 999_999) throw new Error("Invalid Redis TIME microseconds");
  const nowMs = seconds * 1000 + Math.floor(microseconds / 1000);
  if (!Number.isSafeInteger(nowMs)) throw new Error("Invalid Redis TIME reply");
  return nowMs;
}

/** @param {unknown[] | null | undefined} arr @returns {string[]} */
export function decodeStringArray(arr) {
  if (!arr) return [];
  const out = [];
  for (const value of arr) {
    const text = decodeBulk(value);
    if (text != null) out.push(text);
  }
  return out;
}

/** @param {unknown[] | null | undefined} arr */
export function decodeHashObject(arr) {
  /** @type {Record<string, string | null | undefined>} */
  const out = {};
  if (!arr) return out;
  for (let i = 0; i < arr.length; i += 2) {
    const field = decodeBulk(arr[i]);
    if (field != null) out[field] = decodeBulk(arr[i + 1]);
  }
  return out;
}

// Accepts (field, value) OR (object). Raw RESP args HSET key f1 v1 f2 v2...
/** @param {string} key @param {RedisHSetArg[]} rest @returns {RedisCommand} */
export function buildHSetArgs(key, rest) {
  /** @type {RedisCommand} */
  const args = ["HSET", key];
  if (rest.length === 2) {
    args.push(/** @type {RedisArg} */ (rest[0]), /** @type {RedisArg} */ (rest[1]));
  } else if (
    rest.length === 1 &&
    rest[0] &&
    typeof rest[0] === "object" &&
    !(rest[0] instanceof Uint8Array)
  ) {
    for (const [field, value] of Object.entries(rest[0])) args.push(field, value);
  } else {
    throw new Error("hSet requires (key, field, value) or (key, object)");
  }
  return args;
}

/** @param {string} key @param {number} ttlSeconds @param {string[]} fields @returns {RedisCommand} */
export function buildHGetExArgs(key, ttlSeconds, fields) {
  return [
    "HGETEX",
    key,
    "EX",
    String(ttlSeconds),
    "FIELDS",
    String(fields.length),
    ...fields,
  ];
}

/** @param {string} key @param {number} ttlSeconds @param {Record<string, RedisArg>} fields @returns {RedisCommand} */
export function buildHSetExArgs(key, ttlSeconds, fields) {
  /** @type {RedisCommand} */
  const args = ["HSETEX", key, "EX", String(ttlSeconds), "FIELDS"];
  const entries = Object.entries(fields);
  args.push(String(entries.length));
  for (const [field, value] of entries) args.push(field, value);
  return args;
}

/**
 * @param {string} key
 * @param {RedisArg} value
 * @param {RedisSetOptions} [opts]
 * @returns {RedisCommand}
 */
export function buildSetArgs(key, value, opts = {}) {
  /** @type {RedisCommand} */
  const args = ["SET", key, value];
  if (opts.ttl !== undefined && opts.ttl !== null) {
    args.push("EX", String(positiveIntegerSetOption("ttl", opts.ttl)));
  } else if (opts.exat !== undefined && opts.exat !== null) {
    args.push("EXAT", String(positiveIntegerSetOption("exat", opts.exat)));
  }
  if (opts.nx) args.push("NX");
  if (opts.xx) args.push("XX");
  if (opts.ifeq != null) args.push("IFEQ", opts.ifeq);
  return args;
}
