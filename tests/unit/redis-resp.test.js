// Unit coverage for the RESP framing helpers and RedisSubscriber's
// reconnect/loop state machine. Socket IO can't run under node --test
// because it needs workerd's cloudflare:sockets binding, so subscriber tests
// replace that import and inject a fake `connect` through opts.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { parseJsonText } from "../helpers/json-payload.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { delay } from "../helpers/timing.js";

const observabilityUrl = repositoryFileUrl("shared/observability.js");
const errorsUrl = repositoryFileUrl("shared/errors.js");
const redisRespUrl = repositoryModuleDataUrl("shared/redis-resp.js", [
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "\.\/errors\.js";/, `from ${JSON.stringify(errorsUrl)};`],
]);
const respMod = await import(redisRespUrl);
const subscriberMod = await importRepositoryModule("shared/redis-subscriber.js", [
  [/import \{ connect \} from "cloudflare:sockets";/, "const connect = null;"],
  [/from "shared-redis-resp";/g, `from ${JSON.stringify(redisRespUrl)};`],
]);
const { decodeRedisTimeMs, encodeCommand, RespReader } = respMod;
const { RedisSubscriber, defaultBackoff } = subscriberMod;

/** @param {Uint8Array[]} chunks */
function mockReader(chunks) {
  let i = 0;
  return {
    async read() {
      if (i >= chunks.length) return { done: true, value: undefined };
      return { done: false, value: chunks[i++] };
    },
    releaseLock() {},
  };
}

/** @param {string} str */
function bytes(str) {
  return new TextEncoder().encode(str);
}

test("encodeCommand frames a simple SET", () => {
  const out = encodeCommand(["SET", "k", "v"]);
  const str = new TextDecoder().decode(out);
  assert.equal(str, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
});

test("encodeCommand preserves binary args as-is", () => {
  const binary = new Uint8Array([0, 1, 2, 255]);
  const out = encodeCommand(["SET", "k", binary]);
  // Recover the structure: framing is text, payload is raw bytes.
  const str = new TextDecoder("utf-8", { fatal: false }).decode(out);
  // Header portion up to the final "$4\r\n" before the binary payload.
  const headerEnd = str.indexOf("$4\r\n") + 4;
  assert.equal(str.slice(0, headerEnd), "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n");
  assert.deepEqual(Array.from(out.slice(headerEnd, headerEnd + 4)), [0, 1, 2, 255]);
});

test("RespReader parses multiple replies in sequence", async () => {
  const reader = mockReader([bytes("+OK\r\n:42\r\n$3\r\nfoo\r\n")]);
  const parser = new RespReader(reader);
  assert.equal(await parser.parseOne(), "OK");
  assert.equal(await parser.parseOne(), 42);
  const bulk = await parser.parseOne();
  assert.deepEqual(Array.from(bulk), [102, 111, 111]); // 'foo'
});

test("RespReader handles a reply split across chunks", async () => {
  const reader = mockReader([bytes("*2\r\n$3\r\nf"), bytes("oo\r\n:7\r\n")]);
  const parser = new RespReader(reader);
  const arr = await parser.parseOne();
  assert.equal(arr.length, 2);
  assert.deepEqual(Array.from(arr[0]), [102, 111, 111]);
  assert.equal(arr[1], 7);
});

test("RespReader handles CRLF split across chunks", async () => {
  const reader = mockReader([bytes("+OK\r"), bytes("\n")]);
  const parser = new RespReader(reader);
  assert.equal(await parser.parseOne(), "OK");
});

test("RespReader.compact reuses small buffers between replies", async () => {
  const reader = mockReader([bytes("+OK\r\n+OK\r\n"), bytes(":2\r\n")]);
  const parser = new RespReader(reader);
  await parser.parseOne();
  const storage = parser._storage;
  parser.compact();
  assert.equal(parser.pos, 0);
  assert.equal(parser.buf.length, 5); // "+OK\r\n" remaining
  assert.equal(parser._storage, storage);
  assert.deepEqual(Array.from(parser.buf), Array.from(bytes("+OK\r\n")));
  await parser.parseOne();
  parser.compact();
  assert.equal(parser.buf.length, 0);
  assert.equal(parser._storage, storage);
  assert.equal(parser._storage.length, 1024);
  assert.equal(await parser.parseOne(), 2);
  assert.equal(parser._storage, storage);
});

test("RespReader.compact releases an oversized consumed buffer", async () => {
  const payload = "x".repeat(65 * 1024);
  const reader = mockReader([bytes(`$${payload.length}\r\n${payload}\r\n`)]);
  const parser = new RespReader(reader);

  assert.equal((await parser.parseOne()).length, payload.length);
  assert.ok(parser._storage.length > 64 * 1024);
  parser.compact();
  assert.equal(parser.buf.length, 0);
  assert.equal(parser._storage.length, 0);
});

test("RespReader surfaces Redis -ERR replies as throws", async () => {
  const reader = mockReader([bytes("-NOAUTH required\r\n")]);
  const parser = new RespReader(reader);
  await assert.rejects(() => parser.parseOne(), /Redis error: NOAUTH required/);
});

test("RespReader consumes full arrays before surfacing inline Redis errors", async () => {
  const reader = mockReader([bytes("*3\r\n:1\r\n-WRONGTYPE wrong kind\r\n:2\r\n+OK\r\n")]);
  const parser = new RespReader(reader);
  await assert.rejects(() => parser.parseOne(), /Redis error: WRONGTYPE wrong kind/);
  assert.equal(await parser.parseOne(), "OK");
});

test("RespReader rejects malformed numeric RESP lengths", async () => {
  await assert.rejects(
    () => new RespReader(mockReader([bytes(":1abc\r\n")])).parseOne(),
    /Invalid RESP integer: 1abc/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes(":9007199254740992\r\n")])).parseOne(),
    /Invalid RESP integer: 9007199254740992/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes(":-0\r\n")])).parseOne(),
    /Invalid RESP integer: -0/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes("$nan\r\n")])).parseOne(),
    /Invalid RESP bulk length: nan/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes("$-0\r\n\r\n")])).parseOne(),
    /Invalid RESP bulk length: -0/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes("$-2\r\n")])).parseOne(),
    /Invalid RESP bulk length: -2/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes("*-0\r\n")])).parseOne(),
    /Invalid RESP array length: -0/
  );
  await assert.rejects(
    () => new RespReader(mockReader([bytes("*-2\r\n")])).parseOne(),
    /Invalid RESP array length: -2/
  );
});

test("RespReader rejects bulk payloads without CRLF terminators", async () => {
  const reader = mockReader([bytes("$3\r\nfooXY")]);
  const parser = new RespReader(reader);
  await assert.rejects(() => parser.parseOne(), /Invalid RESP bulk terminator/);
});

test("decodeRedisTimeMs accepts Redis TIME replies and rejects malformed values", () => {
  assert.equal(decodeRedisTimeMs([bytes("1700000000"), bytes("123456")]), 1_700_000_000_123);
  assert.equal(decodeRedisTimeMs(["1700000000", "999999"]), 1_700_000_000_999);
  assert.throws(() => decodeRedisTimeMs(null), /Invalid Redis TIME reply/);
  assert.throws(() => decodeRedisTimeMs([bytes("1700000000")]), /Invalid Redis TIME reply/);
  assert.throws(() => decodeRedisTimeMs([bytes("1700000000"), bytes("1000000")]), /Invalid Redis TIME microseconds/);
  assert.throws(() => decodeRedisTimeMs([bytes("-1"), bytes("0")]), /Invalid Redis TIME seconds/);
  assert.throws(() => decodeRedisTimeMs([bytes("1700000000"), bytes("-0")]), /Invalid Redis TIME microseconds/);
  assert.throws(() => decodeRedisTimeMs([bytes("9007199254740992"), bytes("0")]), /Invalid Redis TIME seconds/);
});

test("defaultBackoff grows exponentially and caps at 5s", () => {
  assert.equal(defaultBackoff(0), 100);
  assert.equal(defaultBackoff(1), 200);
  assert.equal(defaultBackoff(4), 1600);
  assert.equal(defaultBackoff(10), 5000);
  assert.equal(defaultBackoff(20), 5000);
});

// Fake socket whose readable stream emits a SUBSCRIBE ack plus one message
// then closes. Subscriber should: (1) send the SUBSCRIBE command, (2) fire
// onConnect after the ack, (3) fire onMessage for the message, (4) fire
// onDisconnect on close, (5) loop and retry (we stop it before the second
// attempt).
/** @param {Uint8Array[]} scriptedChunks */
function fakeSocket(scriptedChunks) {
  const writer = {
    /** @type {Uint8Array[]} */
    writes: [],
    /** @param {Uint8Array} buf */
    async write(buf) { this.writes.push(buf); },
    releaseLock() {},
    close() {},
  };
  let i = 0;
  const reader = {
    async read() {
      if (i >= scriptedChunks.length) {
        // Simulate peer closing after the scripted data.
        return { done: true };
      }
      return { done: false, value: scriptedChunks[i++] };
    },
    releaseLock() {},
  };
  const socket = {
    writable: { getWriter: () => writer },
    readable: { getReader: () => reader },
    close() {},
    _writer: writer,
  };
  return socket;
}

test("RedisSubscriber does not select a logical DB before subscribing", async () => {
  const socket = fakeSocket([
    bytes("*3\r\n$9\r\nsubscribe\r\n$5\r\nroute\r\n:1\r\n"),
  ]);
  const sub = new RedisSubscriber("x", ["route"], /** @type {any} */ ({
    db: 1,
    connect: () => socket,
    onConnect: () => sub.stop(),
    onError: () => {},
    sleep: async () => sub.stop(),
  }));

  await sub.start();

  assert.equal(socket._writer.writes.length, 1);
  assert.equal(
    new TextDecoder().decode(socket._writer.writes[0]),
    "*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\nroute\r\n"
  );
});

test("RedisSubscriber: SUBSCRIBE, ack, message, disconnect fire in order", async () => {
  /** @type {Array<string | [string, string, string]>} */
  const events = [];
  const scripted = [
    bytes("*3\r\n$9\r\nsubscribe\r\n$5\r\nroute\r\n:1\r\n"),
    bytes("*3\r\n$7\r\nmessage\r\n$5\r\nroute\r\n$3\r\nabc\r\n"),
  ];
  let stopRequested = false;
  const sub = new RedisSubscriber("x", ["route"], {
    // First connect serves the scripted bytes; any subsequent connect
    // attempt means the reconnect loop is live. The test stops the
    // subscriber once the single expected disconnect fires.
    connect: () => {
      if (stopRequested) {
        // Keep the loop from ever reconnecting after we've decided to stop.
        // Throwing here would cascade through onError; cleaner to return a
        // socket that immediately ends so the outer loop sees _running=false
        // on the next check.
        return fakeSocket([]);
      }
      return fakeSocket(scripted);
    },
    backoff: () => 1,
    sleep: delay,
    onConnect: () => events.push("connect"),
    onDisconnect: () => {
      events.push("disconnect");
      stopRequested = true;
      sub.stop();
    },
    onMessage: (/** @type {string} */ ch, /** @type {Uint8Array} */ payload) =>
      events.push(["message", ch, new TextDecoder().decode(payload)]),
    onError: () => {},
  });
  await sub.start();

  assert.equal(events[0], "connect");
  assert.deepEqual(events[1], ["message", "route", "abc"]);
  assert.equal(events[2], "disconnect");
});

test("RedisSubscriber: non-function backoff option falls back safely", async () => {
  /** @type {number[]} */
  const delays = [];
  const sub = new RedisSubscriber("x", ["route"], {
    connect: () => fakeSocket([]),
    backoff: /** @type {any} */ (100),
    sleep: async (/** @type {number} */ ms) => {
      delays.push(ms);
      sub.stop();
    },
    onError: () => {},
  });

  await sub.start();

  assert.deepEqual(delays, [defaultBackoff(0)]);
});

test("RedisSubscriber: callback errors do not tear down the reader loop", async () => {
  const scripted = [
    bytes("*3\r\n$9\r\nsubscribe\r\n$5\r\nroute\r\n:1\r\n"),
    bytes("*3\r\n$7\r\nmessage\r\n$5\r\nroute\r\n$1\r\na\r\n"),
    bytes("*3\r\n$7\r\nmessage\r\n$5\r\nroute\r\n$1\r\nb\r\n"),
  ];
  /** @type {string[]} */
  const received = [];
  // Quiet the intentional warn from _safe — the whole point of this test
  // is to verify we warn-and-continue; the visible warning is noise here.
  /** @type {unknown[][]} */
  const warnings = [];
  let stopRequested = false;
  await withMockedProperty(console, "log", (/** @type {unknown[]} */ ...args) => { warnings.push(args); }, async () => {
    const sub = new RedisSubscriber("x", ["route"], {
      connect: () => (stopRequested ? fakeSocket([]) : fakeSocket(scripted)),
      backoff: () => 1,
      sleep: () => Promise.resolve(),
      onDisconnect: () => {
        stopRequested = true;
        sub.stop();
      },
      onMessage: (/** @type {string} */ _ch, /** @type {Uint8Array} */ payload) => {
        const s = new TextDecoder().decode(payload);
        received.push(s);
        if (s === "a") throw new Error("boom");
      },
    });
    await sub.start();
  });
  assert.deepEqual(received, ["a", "b"]);
  assert.ok(warnings.length > 0, "expected a warn for the throwing callback");
  const warning = parseJsonText(String(warnings[0][0]), "Redis subscriber warning log");
  assert.equal(warning.service, "shared-redis");
  assert.equal(warning.level, "warn");
  assert.equal(warning.event, "redis_subscriber_callback_threw");
  assert.equal(warning.error_message, "boom");
});
