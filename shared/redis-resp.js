import { createLogger } from "shared-observability";
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

/** @param {RedisCommand} args */
export function encodeCommand(args) {
  /** @type {Uint8Array[]} */
  const encodedArgs = [];
  const argc = String(args.length);
  let total = 1 + argc.length + 2;
  for (const arg of args) {
    const bytes = arg instanceof Uint8Array ? arg : utf8Encoder.encode(String(arg));
    encodedArgs.push(bytes);
    total += 1 + String(bytes.length).length + 2 + bytes.length + 2;
  }
  const out = new Uint8Array(total);
  let off = 0;
  off = writeAscii(out, off, "*");
  off = writeAscii(out, off, argc);
  off = writeAscii(out, off, "\r\n");
  for (const bytes of encodedArgs) {
    off = writeAscii(out, off, "$");
    off = writeAscii(out, off, String(bytes.length));
    off = writeAscii(out, off, "\r\n");
    out.set(bytes, off);
    off += bytes.length;
    off = writeAscii(out, off, "\r\n");
  }
  return out;
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

/** @param {Uint8Array[]} parts */
export function concatBuffers(parts) {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
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
