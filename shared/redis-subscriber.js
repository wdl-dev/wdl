import { connect } from "cloudflare:sockets";
import {
  encodeCommand,
  utf8Decoder,
  warnRedisCallback,
  RespReader,
} from "shared-redis-resp";

/**
 * @typedef {import("shared-redis-resp").RedisSocket} RedisSocket
 * @typedef {import("shared-redis-resp").RedisSubscriberOptions} RedisSubscriberOptions
 */

// Exponential backoff capped at 5 s. Exposed for tests.
/** @param {number} attempt */
export function defaultBackoff(attempt) {
  return Math.min(5000, 100 * (2 ** attempt));
}

// `onConnect` fires after every channel's SUBSCRIBE ack is received.
// `onMessage` receives raw payload bytes: callers decode as needed.
// `start()` does not resolve until `stop()` is called; callers typically feed
// it into `ctx.waitUntil` so the isolate keeps the reader alive.
export class RedisSubscriber {
  /** @param {string} address @param {string|string[]} channels @param {RedisSubscriberOptions} [opts] */
  constructor(address, channels, opts = {}) {
    this.address = address;
    this.channels = Array.isArray(channels) ? channels.slice() : [channels];
    this.onMessage = typeof opts.onMessage === "function" ? opts.onMessage : null;
    this.onConnect = typeof opts.onConnect === "function" ? opts.onConnect : null;
    this.onDisconnect = typeof opts.onDisconnect === "function" ? opts.onDisconnect : null;
    this.onError = typeof opts.onError === "function" ? opts.onError : null;
    this.backoff = typeof opts.backoff === "function" ? opts.backoff : defaultBackoff;
    this.sleep = typeof opts.sleep === "function"
      ? opts.sleep
      : ((/** @type {number} */ ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    this._connect = typeof opts.connect === "function" ? opts.connect : connect;
    this._running = false;
    /** @type {RedisSocket | null} */
    this._socket = null;
  }

  async start() {
    if (this._running) return;
    this._running = true;
    let attempt = 0;
    while (this._running) {
      try {
        await this._run();
      } catch (err) {
        this._safe(/** @type {((...args: unknown[]) => unknown) | null} */ (this.onError), err);
      }
      if (!this._running) return;
      const delay = this.backoff(attempt++);
      await this.sleep(delay);
    }
  }

  stop() {
    this._running = false;
    try { this._socket?.close?.(); } catch { /* closed */ }
  }

  async _run() {
    const socket = this._connect(this.address);
    this._socket = socket;
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    try {
      const parser = new RespReader(reader);
      await writer.write(encodeCommand(["SUBSCRIBE", ...this.channels]));
      // Drain the SUBSCRIBE ack(s) before firing onConnect: otherwise callers
      // cannot tell when the server has registered the subscription.
      for (let i = 0; i < this.channels.length; i += 1) {
        const ack = await parser.parseOne();
        parser.compact();
        if (!Array.isArray(ack)) {
          throw new Error(`Unexpected SUBSCRIBE reply: ${JSON.stringify(ack)}`);
        }
      }
      this._safe(/** @type {((...args: unknown[]) => unknown) | null} */ (this.onConnect));
      while (this._running) {
        const msg = await parser.parseOne();
        parser.compact();
        if (!Array.isArray(msg) || msg.length < 3) continue;
        const kind = utf8Decoder.decode(/** @type {Uint8Array} */ (msg[0]));
        if (kind !== "message") continue;
        const channel = utf8Decoder.decode(/** @type {Uint8Array} */ (msg[1]));
        this._safe(
          /** @type {((...args: unknown[]) => unknown) | null} */ (this.onMessage),
          channel,
          /** @type {Uint8Array} */ (msg[2])
        );
      }
    } finally {
      try { writer.releaseLock(); } catch { /* released */ }
      try { reader.releaseLock(); } catch { /* released */ }
      try { socket.close?.(); } catch { /* closed */ }
      this._socket = null;
      this._safe(/** @type {((...args: unknown[]) => unknown) | null} */ (this.onDisconnect));
    }
  }

  /** @param {((...args: unknown[]) => unknown) | null | undefined} fn @param {...unknown} args */
  _safe(fn, ...args) {
    if (!fn) return;
    try { fn(...args); } catch (err) {
      // Callback errors shouldn't tear down the reader loop.
      warnRedisCallback("redis_subscriber_callback_threw", err);
    }
  }
}
