// RedisSession + RedisMulti unit coverage. cloudflare:sockets is
// workerd-only, so `opts.connect` receives a fake socket. Key invariants
// exercised: a single socket spans the whole session (WATCH breaks
// otherwise); MULTI's nil reply under WATCH surfaces as WatchError.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  repositoryModuleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const observabilityUrl = repositoryFileUrl("shared/observability.js");
const errorsUrl = repositoryFileUrl("shared/errors.js");
const redisRespUrl = repositoryModuleDataUrl("shared/redis-resp.js", [
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "\.\/errors\.js";/, `from ${JSON.stringify(errorsUrl)};`],
]);
const redisSessionUrl = repositoryModuleDataUrl("shared/redis-session.js", [
  [/import \{ connect \} from "cloudflare:sockets";/, "const connect = null;"],
  [/from "shared-redis-resp";/g, `from ${JSON.stringify(redisRespUrl)};`],
  [/from "\.\/errors\.js";/, `from ${JSON.stringify(errorsUrl)};`],
]);
const clientMod = await importRepositoryModule("shared/redis-command-client.js", [
  [/import \{ connect \} from "cloudflare:sockets";/, "const connect = null;"],
  [/from "shared-redis-resp";/g, `from ${JSON.stringify(redisRespUrl)};`],
  [/from "shared-redis-session";/g, `from ${JSON.stringify(redisSessionUrl)};`],
  [/from "\.\/errors\.js";/, `from ${JSON.stringify(errorsUrl)};`],
]);
const respMod = await import(redisRespUrl);
const sessionMod = await import(redisSessionUrl);
const { RedisClient } = clientMod;
const { RedisSession } = sessionMod;
const { RespReader, WatchError, decodeBulk } = respMod;

/** @param {string} str */
function bytes(str) { return new TextEncoder().encode(str); }
/** @param {Uint8Array} buf */
function decode(buf) { return new TextDecoder().decode(buf); }

test("decodeBulk normalizes Redis string and bulk replies", () => {
  assert.equal(decodeBulk(null), null);
  assert.equal(decodeBulk(undefined), undefined);
  assert.equal(decodeBulk("main"), "main");
  assert.equal(decodeBulk(bytes("main")), "main");
  assert.equal(decodeBulk(123), "123");
});

// Records every write, yields scripted reply chunks in order.
// `connectCount` lets tests assert a single socket per session.
/** @param {Uint8Array[]} scriptedChunks */
function makeFakeSocket(scriptedChunks) {
  /** @type {Uint8Array[]} */
  const writes = [];
  const writer = {
    /** @param {Uint8Array} buf */
    async write(buf) { writes.push(buf); },
    close() { writer.closed = true; },
    releaseLock() {},
    /** @type {boolean} */
    closed: false,
  };
  let i = 0;
  const reader = {
    async read() {
      if (i >= scriptedChunks.length) return { done: true };
      return { done: false, value: scriptedChunks[i++] };
    },
    releaseLock() { reader.released = true; },
    /** @type {boolean} */
    released: false,
  };
  const socket = {
    writable: { getWriter: () => writer },
    readable: { getReader: () => reader },
    close() { socket.closed = true; },
    /** @type {boolean} */
    closed: false,
    _reader: reader,
    _writer: writer,
    _writes: writes,
  };
  return socket;
}

/** @param {any} socket */
function scriptedConnect(socket) {
  /** @type {{ count: number, lastSocket: any }} */
  const state = { count: 0, lastSocket: null };
  return {
    state,
    connect: () => {
      state.count += 1;
      state.lastSocket = socket;
      return socket;
    },
  };
}

/** @param {string[]} chunks */
function makeChunkReader(chunks) {
  let i = 0;
  return {
    async read() {
      if (i >= chunks.length) return { done: true };
      return { done: false, value: bytes(chunks[i++]) };
    },
  };
}

test("RespReader parses split lines and nested arrays", async () => {
  const reader = new RespReader(makeChunkReader([
    "*2\r\n$3",
    "\r\nfoo\r\n:",
    "7\r\n",
  ]));

  const result = await reader.parseOne();
  assert.equal(decode(result[0]), "foo");
  assert.equal(result[1], 7);
});

test("RespReader drops consumed prefixes while reading the next reply", async () => {
  const reader = new RespReader(makeChunkReader([
    "$3\r\nfoo\r\n$3\r\n",
    "bar\r\n",
  ]));

  assert.equal(decode(await reader.parseOne()), "foo");
  assert.equal(decode(await reader.parseOne()), "bar");
  assert.equal(reader.buf.length, "bar\r\n".length, "consumed prefix should be discarded before appending");
  assert.equal(reader.pos, reader.buf.length);
});

test("RespReader compact keeps unread bytes for following replies", async () => {
  const reader = new RespReader(makeChunkReader(["+OK\r\n:2\r\n"]));

  assert.equal(await reader.parseOne(), "OK");
  reader.compact();
  assert.equal(reader.pos, 0);
  assert.equal(await reader.parseOne(), 2);
});

test("RedisSession: single socket carries HGET + HSET sequentially", async () => {
  // Replies: $3\r\nfoo\r\n ($foo) then :1\r\n (HSET returns int)
  const socket = makeFakeSocket([bytes("$3\r\nfoo\r\n:1\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const result = await client.session(async (/** @type {any} */ s) => {
    const got = await s.hGet("k", "f");
    const n = await s.hSet("k", "f2", "v2");
    return [got, n];
  });
  assert.deepEqual(result, ["foo", 1]);
  assert.equal(state.count, 1, "session must open exactly one socket");
  // Decode writes to validate RESP framing.
  assert.equal(decode(socket._writes[0]), "*3\r\n$4\r\nHGET\r\n$1\r\nk\r\n$1\r\nf\r\n");
  assert.equal(decode(socket._writes[1]), "*4\r\n$4\r\nHSET\r\n$1\r\nk\r\n$2\r\nf2\r\n$2\r\nv2\r\n");
  assert.ok(socket.closed, "socket closed after session");
  assert.ok(socket._reader.released, "session reader lock released after close");
});

test("RedisSession.hGetAll returns string-decoded object", async () => {
  // *4 $1 f $3 foo $1 g $3 bar
  const socket = makeFakeSocket([bytes("*4\r\n$1\r\nf\r\n$3\r\nfoo\r\n$1\r\ng\r\n$3\r\nbar\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const obj = await client.session((/** @type {any} */ s) => s.hGetAll("k"));
  assert.deepEqual(obj, { f: "foo", g: "bar" });
});

test("RedisClient.hMGet returns decoded values and null misses", async () => {
  const socket = makeFakeSocket([bytes("*3\r\n$1\r\na\r\n$-1\r\n$1\r\nc\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const values = await client.hMGet("h", ["a", "b", "c"]);

  assert.deepEqual(values, ["a", null, "c"]);
  assert.equal(
    decode(socket._writes[0]),
    "*5\r\n$5\r\nHMGET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  assert.ok(socket._reader.released, "per-call reader lock released after command");
});

test("RedisClient.hGetEx refreshes hash field TTLs while reading values", async () => {
  const socket = makeFakeSocket([bytes("*3\r\n$1\r\na\r\n$-1\r\n$1\r\nc\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const values = await client.hGetEx("h", 30, ["a", "b", "c"]);

  assert.deepEqual(values, ["a", null, "c"]);
  assert.equal(
    decode(socket._writes[0]),
    "*9\r\n$6\r\nHGETEX\r\n$1\r\nh\r\n$2\r\nEX\r\n$2\r\n30\r\n$6\r\nFIELDS\r\n$1\r\n3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  assert.ok(socket._reader.released, "per-call reader lock released after command");
});

test("RedisSession carries hash field TTL heartbeats on its held socket", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$1\r\n1\r\n$-1\r\n:9999\r\n:1\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const result = await client.session(async (/** @type {any} */ session) => {
    const active = await session.hGetEx("logs:tail:active", 30, ["demo:a", "demo:b"]);
    const count = await session.hLen("logs:tail:active");
    const written = await session.hSetEx("logs:tail:active", 30, { "demo:b": "1" });
    return { active, count, written };
  });

  assert.deepEqual(result, { active: ["1", null], count: 9999, written: 1 });
  assert.equal(state.count, 1, "heartbeat commands must reuse one Redis socket");
  assert.equal(
    decode(socket._writes[0]),
    "*8\r\n$6\r\nHGETEX\r\n$16\r\nlogs:tail:active\r\n$2\r\nEX\r\n$2\r\n30\r\n$6\r\nFIELDS\r\n$1\r\n2\r\n$6\r\ndemo:a\r\n$6\r\ndemo:b\r\n"
  );
  assert.equal(decode(socket._writes[1]), "*2\r\n$4\r\nHLEN\r\n$16\r\nlogs:tail:active\r\n");
  assert.equal(
    decode(socket._writes[2]),
    "*8\r\n$6\r\nHSETEX\r\n$16\r\nlogs:tail:active\r\n$2\r\nEX\r\n$2\r\n30\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$6\r\ndemo:b\r\n$1\r\n1\r\n"
  );
});

test("RedisSession.open fails explicitly after close", async () => {
  const socket = makeFakeSocket([]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();
  await session.close();

  await assert.rejects(() => session.open(), /Redis session closed/);
});

test("RedisSession.hGetAllMany batches independent HGETALL reads", async () => {
  const socket = makeFakeSocket([
    bytes("*4\r\n$4\r\nkind\r\n$2\r\nns\r\n$2\r\nns\r\n$6\r\ndemo-a\r\n*2\r\n$4\r\nkind\r\n$3\r\nops\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const records = await client.session((/** @type {any} */ s) =>
    s.hGetAllMany(["auth:token:a", "auth:token:b"]));

  assert.deepEqual(records, [
    { kind: "ns", ns: "demo-a" },
    { kind: "ops" },
  ]);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$7\r\nHGETALL\r\n$12\r\nauth:token:a\r\n" +
      "*2\r\n$7\r\nHGETALL\r\n$12\r\nauth:token:b\r\n"
  );
});

test("RedisSession batches bounded hash lengths and set cardinalities", async () => {
  const socket = makeFakeSocket([bytes(":2\r\n:0\r\n:7\r\n:3\r\n:0\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const result = await client.session(async (/** @type {any} */ session) => ({
    lengths: await session.hStrLenMany([["worker:a", "__meta__"], ["worker:b", "__meta__"]]),
    count: await session.sCard("do:objects:storage"),
    counts: await session.sCardMany(["hosts:alpha", "hosts:beta"]),
  }));

  assert.deepEqual(result, { lengths: [2, 0], count: 7, counts: [3, 0] });
  assert.equal(
    decode(socket._writes[0]),
    "*3\r\n$7\r\nHSTRLEN\r\n$8\r\nworker:a\r\n$8\r\n__meta__\r\n" +
      "*3\r\n$7\r\nHSTRLEN\r\n$8\r\nworker:b\r\n$8\r\n__meta__\r\n"
  );
  assert.equal(
    decode(socket._writes[1]),
    "*2\r\n$5\r\nSCARD\r\n$18\r\ndo:objects:storage\r\n"
  );
  assert.equal(
    decode(socket._writes[2]),
    "*2\r\n$5\r\nSCARD\r\n$11\r\nhosts:alpha\r\n" +
      "*2\r\n$5\r\nSCARD\r\n$10\r\nhosts:beta\r\n"
  );
});

test("RedisClient.sCardMany batches independent set cardinalities", async () => {
  const socket = makeFakeSocket([bytes(":3\r\n:0\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  assert.deepEqual(await client.sCardMany(["hosts:alpha", "hosts:beta"]), [3, 0]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$5\r\nSCARD\r\n$11\r\nhosts:alpha\r\n" +
      "*2\r\n$5\r\nSCARD\r\n$10\r\nhosts:beta\r\n"
  );
});

test("RedisClient.hGetAllMany batches independent HGETALL reads", async () => {
  const socket = makeFakeSocket([
    bytes("*4\r\n$4\r\nkind\r\n$2\r\nns\r\n$2\r\nns\r\n$6\r\ndemo-a\r\n*2\r\n$4\r\nkind\r\n$3\r\nops\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const records = await client.hGetAllMany(["auth:token:a", "auth:token:b"]);

  assert.deepEqual(records, [
    { kind: "ns", ns: "demo-a" },
    { kind: "ops" },
  ]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$7\r\nHGETALL\r\n$12\r\nauth:token:a\r\n" +
      "*2\r\n$7\r\nHGETALL\r\n$12\r\nauth:token:b\r\n"
  );
});

test("RedisSession.sMembersMany batches independent SMEMBERS reads", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n*1\r\n$5\r\nthree\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const members = await client.session((/** @type {any} */ session) =>
    session.sMembersMany(["set:a", "set:b"]));

  assert.deepEqual(members, [["one", "two"], ["three"]]);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$8\r\nSMEMBERS\r\n$5\r\nset:a\r\n" +
      "*2\r\n$8\r\nSMEMBERS\r\n$5\r\nset:b\r\n"
  );
});

test("RedisClient.sMembersMany batches independent SMEMBERS reads", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$3\r\none\r\n$3\r\ntwo\r\n*1\r\n$5\r\nthree\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const members = await client.sMembersMany(["set:a", "set:b"]);

  assert.deepEqual(members, [["one", "two"], ["three"]]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$8\r\nSMEMBERS\r\n$5\r\nset:a\r\n" +
      "*2\r\n$8\r\nSMEMBERS\r\n$5\r\nset:b\r\n"
  );
});

test("RedisClient.hGetAllAndGet reads a hash and string on one socket", async () => {
  const socket = makeFakeSocket([
    bytes("*4\r\n$10\r\ndatabaseId\r\n$7\r\nd1_main\r\n$5\r\nstate\r\n$5\r\nready\r\n$7\r\nd1_main\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const snapshot = await client.hGetAllAndGet("d1:database:demo:main", "d1:database-name:demo:main");

  assert.deepEqual(snapshot, {
    hash: { databaseId: "d1_main", state: "ready" },
    value: "d1_main",
  });
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$7\r\nHGETALL\r\n$21\r\nd1:database:demo:main\r\n" +
      "*2\r\n$3\r\nGET\r\n$26\r\nd1:database-name:demo:main\r\n"
  );
});

test("RedisClient.sMembersAndHGetAll reads a gateway snapshot on one socket", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$4\r\ndemo\r\n$6\r\nsystem\r\n*2\r\n$3\r\napp\r\n$2\r\nv3\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const snapshot = await client.sMembersAndHGetAll("namespaces", "routes:demo");

  assert.deepEqual(snapshot, {
    members: ["demo", "system"],
    hash: { app: "v3" },
  });
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$8\r\nSMEMBERS\r\n$10\r\nnamespaces\r\n" +
      "*2\r\n$7\r\nHGETALL\r\n$11\r\nroutes:demo\r\n"
  );
});

test("RedisSession.hGetAllAndGet preserves empty hash and missing string replies", async () => {
  const socket = makeFakeSocket([bytes("*0\r\n$-1\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const snapshot = await client.session((/** @type {any} */ session) =>
    session.hGetAllAndGet("d1:database:demo:missing", "d1:database-name:demo:missing"));

  assert.deepEqual(snapshot, { hash: {}, value: null });
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$7\r\nHGETALL\r\n$24\r\nd1:database:demo:missing\r\n" +
      "*2\r\n$3\r\nGET\r\n$29\r\nd1:database-name:demo:missing\r\n"
  );
});

test("RedisSession.hGetAllGetSMembers reads a delete snapshot in one write", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$5\r\nstate\r\n$5\r\nready\r\n$7\r\nd1_main\r\n*2\r\n$6\r\nworker\r\n$7\r\nversion\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const snapshot = await client.session((/** @type {any} */ session) =>
    session.hGetAllGetSMembers("database", "alias", "referrers"));

  assert.deepEqual(snapshot, {
    hash: { state: "ready" },
    value: "d1_main",
    members: ["worker", "version"],
  });
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$7\r\nHGETALL\r\n$8\r\ndatabase\r\n" +
      "*2\r\n$3\r\nGET\r\n$5\r\nalias\r\n" +
      "*2\r\n$8\r\nSMEMBERS\r\n$9\r\nreferrers\r\n"
  );
});

test("RedisSession.getMany batches independent GET reads", async () => {
  const socket = makeFakeSocket([bytes("$3\r\none\r\n$-1\r\n$5\r\nthree\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const values = await client.session((/** @type {any} */ s) =>
    s.getMany(["key:1", "key:2", "key:3"]));

  assert.deepEqual(values, ["one", null, "three"]);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$3\r\nGET\r\n$5\r\nkey:1\r\n" +
      "*2\r\n$3\r\nGET\r\n$5\r\nkey:2\r\n" +
      "*2\r\n$3\r\nGET\r\n$5\r\nkey:3\r\n"
  );
});

test("RedisSession.hGetMany batches independent HGET reads", async () => {
  const socket = makeFakeSocket([bytes("$2\r\nv1\r\n$-1\r\n$2\r\nv3\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const values = await client.session((/** @type {any} */ s) =>
    s.hGetMany([["hash:1", "field"], ["hash:2", "field"], ["hash:3", "other"]]));

  assert.deepEqual(values, ["v1", null, "v3"]);
  assert.equal(
    decode(socket._writes[0]),
    "*3\r\n$4\r\nHGET\r\n$6\r\nhash:1\r\n$5\r\nfield\r\n" +
      "*3\r\n$4\r\nHGET\r\n$6\r\nhash:2\r\n$5\r\nfield\r\n" +
      "*3\r\n$4\r\nHGET\r\n$6\r\nhash:3\r\n$5\r\nother\r\n"
  );
});

test("RedisClient.hGetMany batches independent HGET reads", async () => {
  const socket = makeFakeSocket([bytes("$2\r\nv1\r\n$-1\r\n$2\r\nv3\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const values = await client.hGetMany([["hash:1", "field"], ["hash:2", "field"], ["hash:3", "other"]]);

  assert.deepEqual(values, ["v1", null, "v3"]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*3\r\n$4\r\nHGET\r\n$6\r\nhash:1\r\n$5\r\nfield\r\n" +
      "*3\r\n$4\r\nHGET\r\n$6\r\nhash:2\r\n$5\r\nfield\r\n" +
      "*3\r\n$4\r\nHGET\r\n$6\r\nhash:3\r\n$5\r\nother\r\n"
  );
});

test("RedisClient.getWithTime batches GET and TIME on one socket", async () => {
  const socket = makeFakeSocket([bytes("$5\r\nowner\r\n*2\r\n$10\r\n1700000000\r\n$6\r\n123456\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const result = await client.getWithTime("owner-key");

  assert.equal(state.count, 1);
  assert.deepEqual({ value: decode(result.value), nowMs: result.nowMs }, {
    value: "owner",
    nowMs: 1_700_000_000_123,
  });
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$3\r\nGET\r\n$9\r\nowner-key\r\n" +
      "*1\r\n$4\r\nTIME\r\n"
  );
});

test("RedisClient.getManyWithTime batches raw GET replies and TIME on one socket", async () => {
  const socket = makeFakeSocket([
    bytes("$5\r\nowner\r\n$-1\r\n*2\r\n$10\r\n1700000000\r\n$6\r\n654321\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const result = await client.getManyWithTime(["owner-key", "storage-key"]);

  assert.equal(state.count, 1);
  assert.equal(decode(result.values[0]), "owner");
  assert.equal(result.values[1], null);
  assert.equal(result.nowMs, 1_700_000_000_654);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$3\r\nGET\r\n$9\r\nowner-key\r\n" +
      "*2\r\n$3\r\nGET\r\n$11\r\nstorage-key\r\n" +
      "*1\r\n$4\r\nTIME\r\n"
  );
});

test("RedisClient.getManyWithTime rejects an empty key set before opening IO", async () => {
  const client = new RedisClient("x", {
    connect() {
      throw new Error("unexpected connection");
    },
  });
  await assert.rejects(
    client.getManyWithTime([]),
    /getManyWithTime requires at least one key/
  );
});

test("RedisSession.getWithTime batches GET and TIME on its held socket", async () => {
  const socket = makeFakeSocket([bytes("$5\r\nowner\r\n*2\r\n$10\r\n1700000000\r\n$6\r\n654321\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const result = await client.session((/** @type {any} */ s) => s.getWithTime("owner-key"));

  assert.equal(state.count, 1);
  assert.deepEqual(result, { value: "owner", nowMs: 1_700_000_000_654 });
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$3\r\nGET\r\n$9\r\nowner-key\r\n" +
      "*1\r\n$4\r\nTIME\r\n"
  );
});

test("RedisSession.getManyWithTime batches related GETs and TIME on its held socket", async () => {
  const socket = makeFakeSocket([
    bytes("$5\r\nowner\r\n$11\r\nwhole:token\r\n*2\r\n$10\r\n1700000000\r\n$6\r\n654321\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const result = await client.session((/** @type {any} */ s) => (
    s.getManyWithTime(["owner-key", "delete-lock"])
  ));

  assert.equal(state.count, 1);
  assert.deepEqual(result, {
    values: ["owner", "whole:token"],
    nowMs: 1_700_000_000_654,
  });
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$3\r\nGET\r\n$9\r\nowner-key\r\n" +
      "*2\r\n$3\r\nGET\r\n$11\r\ndelete-lock\r\n" +
      "*1\r\n$4\r\nTIME\r\n"
  );
});

test("RedisSession.getManyWithTime rejects an empty key set before opening IO", async () => {
  const session = new RedisSession("x", {
    connect() {
      throw new Error("unexpected connection");
    },
  });
  await assert.rejects(
    session.getManyWithTime([]),
    /getManyWithTime requires at least one key/
  );
});

test("RedisSession.hSet supports object form", async () => {
  const socket = makeFakeSocket([bytes(":2\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session((/** @type {any} */ s) => s.hSet("k", { a: "1", b: "2" }));
  // HSET k a 1 b 2 → 6 args (command, key, 2 × (field, value))
  assert.equal(
    decode(socket._writes[0]),
    "*6\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
  );
});

test("RedisSession.set supports atomic lock options on the held socket", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n")]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();

  assert.equal(await session.set("lock", "token", { nx: true, ttl: 30 }), "OK");
  assert.match(decode(socket._writes[0]), /SET\r\n.*lock\r\n.*token\r\n.*EX\r\n.*30\r\n.*NX\r\n/s);
  await session.close();
});

test("RedisSession.sAdd/sRem accept scalar or array", async () => {
  const socket = makeFakeSocket([bytes(":1\r\n:2\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    await s.sAdd("k", "one");
    await s.sRem("k", ["a", "b"]);
  });
  assert.equal(decode(socket._writes[0]), "*3\r\n$4\r\nSADD\r\n$1\r\nk\r\n$3\r\none\r\n");
  assert.equal(decode(socket._writes[1]), "*4\r\n$4\r\nSREM\r\n$1\r\nk\r\n$1\r\na\r\n$1\r\nb\r\n");
});

test("RedisMulti.zAdd / zRem wire to ZADD / ZREM with correct arities", async () => {
  // MULTI +OK, 2× QUEUED, EXEC *2 [:1, :1]
  const socket = makeFakeSocket([
    bytes("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:1\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    return s.multi()
      .zAdd("z", 7, "v7")
      .zRem("z", ["v1", "v2"])
      .exec();
  });
  const pipeline = decode(socket._writes[0]);
  assert.ok(pipeline.includes("$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n7\r\n$2\r\nv7\r\n"));
  assert.ok(pipeline.includes("$4\r\nZREM\r\n$1\r\nz\r\n$2\r\nv1\r\n$2\r\nv2\r\n"));
});

test("RedisMulti.copy wires COPY with REPLACE inside MULTI", async () => {
  const socket = makeFakeSocket([
    bytes("+OK\r\n+QUEUED\r\n*1\r\n:1\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session((/** @type {any} */ s) =>
    s.multi()
      .copy("bundle:demo:worker:v1", "bundle:demo:worker:v2", { REPLACE: true })
      .exec());

  const pipeline = decode(socket._writes[0]);
  assert.ok(pipeline.startsWith("*1\r\n$5\r\nMULTI\r\n"));
  assert.ok(pipeline.includes(
    "$4\r\nCOPY\r\n$21\r\nbundle:demo:worker:v1\r\n$21\r\nbundle:demo:worker:v2\r\n$7\r\nREPLACE\r\n"
  ));
  assert.ok(pipeline.endsWith("*1\r\n$4\r\nEXEC\r\n"));
});

test("RedisMulti.incr wires revision changes inside the transaction", async () => {
  const socket = makeFakeSocket([
    bytes("+OK\r\n+QUEUED\r\n*1\r\n:2\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const replies = await client.session((/** @type {any} */ session) =>
    session.multi().incr("declared-hosts:revision").exec());

  assert.deepEqual(replies, [2]);
  assert.ok(decode(socket._writes[0]).includes(
    "$4\r\nINCR\r\n$23\r\ndeclared-hosts:revision\r\n"
  ));
});

test("RedisMulti.exec commits on success", async () => {
  // MULTI +OK, QUEUED × 2, EXEC *2 [:1, :1]
  const socket = makeFakeSocket([
    bytes("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n:1\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  const replies = await client.session(async (/** @type {any} */ s) => {
    return s.multi()
      .hSet("k", "f", "v")
      .sAdd("set", "m")
      .exec();
  });
  assert.deepEqual(replies, [1, 1]);
  // MULTI/EXEC goes as one pipelined write; command count is 4 RESP
  // arrays concatenated: MULTI, HSET, SADD, EXEC.
  const pipeline = decode(socket._writes[0]);
  assert.ok(pipeline.startsWith("*1\r\n$5\r\nMULTI\r\n"));
  assert.ok(pipeline.includes("$4\r\nHSET\r\n"));
  assert.ok(pipeline.includes("$4\r\nSADD\r\n"));
  assert.ok(pipeline.endsWith("*1\r\n$4\r\nEXEC\r\n"));
});

test("RedisMulti.exec surfaces WATCH invalidation as WatchError", async () => {
  // WATCH → +OK, MULTI → +OK, HSET queued → +QUEUED, EXEC aborted → *-1.
  const socket = makeFakeSocket([bytes("+OK\r\n+OK\r\n+QUEUED\r\n*-1\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await assert.rejects(
    client.session(async (/** @type {any} */ s) => {
      await s.watch("k");
      return s.multi().hSet("k", "f", "v").exec();
    }),
    (err) => err instanceof WatchError
  );
});

test("RedisMulti.exec consumes inline EXEC errors before later session reads", async () => {
  // MULTI +OK, two queued commands, EXEC commits with one inline WRONGTYPE,
  // then a later GET reply on the same held socket.
  const socket = makeFakeSocket([
    bytes("+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:1\r\n-WRONGTYPE wrong kind\r\n$3\r\nbar\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    await assert.rejects(
      s.multi()
        .hSet("hash", "field", "value")
        .sAdd("hash", "member")
        .exec(),
      /Redis error: WRONGTYPE wrong kind/
    );
    assert.equal(await s.get("k"), "bar");
  });

  assert.equal(state.count, 1);
  assert.equal(decode(socket._writes[1]), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
});

test("RedisMulti.exec drains queue-time errors before later session reads", async () => {
  // MULTI +OK, invalid SADD errors, SET queues, EXEC aborts, then GET succeeds.
  const socket = makeFakeSocket([
    bytes(
      "+OK\r\n" +
      "-ERR wrong number of arguments for 'sadd' command\r\n" +
      "+QUEUED\r\n" +
      "-EXECABORT Transaction discarded because of previous errors.\r\n" +
      "$3\r\nbar\r\n"
    ),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    await assert.rejects(
      s.multi()
        .sAdd("set", [])
        .set("k", "v")
        .exec(),
      /Redis error: ERR wrong number of arguments/
    );
    assert.equal(await s.get("k"), "bar");
  });

  assert.equal(state.count, 1);
  assert.equal(decode(socket._writes[1]), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
});

test("RedisMulti.exec closes the session after a malformed transaction reply", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n+QUEUED\r\n!invalid\r\n")]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();

  await assert.rejects(
    session.multi().set("k", "v").exec(),
    /Unknown RESP type/
  );
  assert.equal(session.hasOpenResources(), false);
  assert.ok(socket.closed);
  await assert.rejects(session.get("k"), /Redis session closed/);
});

test("RedisSession.watch reads +OK ack on the session socket", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n+OK\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    const a = await s.watch("k1", "k2");
    const b = await s.unwatch();
    assert.equal(a, "OK");
    assert.equal(b, "OK");
  });
  // WATCH k1 k2 then UNWATCH land on the same held socket.
  assert.equal(state.count, 1);
  assert.equal(decode(socket._writes[0]), "*3\r\n$5\r\nWATCH\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
  assert.equal(decode(socket._writes[1]), "*1\r\n$7\r\nUNWATCH\r\n");
});

test("RedisSession read pipelines batch independent ZRANGE and EXISTS commands", async () => {
  const socket = makeFakeSocket([
    bytes("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n*1\r\n$2\r\nv3\r\n:0\r\n:1\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });
  await client.session(async (/** @type {any} */ s) => {
    assert.deepEqual(
      await s.zRangeMany(["worker-versions:demo:a", "worker-versions:demo:b"], 0, -1),
      [["v1", "v2"], ["v3"]]
    );
    assert.deepEqual(await s.existsMany(["secrets:demo:a", "secrets:demo:b"]), [false, true]);
  });

  assert.equal(
    decode(socket._writes[0]),
    "*4\r\n$6\r\nZRANGE\r\n$22\r\nworker-versions:demo:a\r\n$1\r\n0\r\n$2\r\n-1\r\n" +
      "*4\r\n$6\r\nZRANGE\r\n$22\r\nworker-versions:demo:b\r\n$1\r\n0\r\n$2\r\n-1\r\n"
  );
  assert.equal(
    decode(socket._writes[1]),
    "*2\r\n$6\r\nEXISTS\r\n$14\r\nsecrets:demo:a\r\n" +
      "*2\r\n$6\r\nEXISTS\r\n$14\r\nsecrets:demo:b\r\n"
  );
});

test("RedisSession drains pipeline reply errors before later commands", async () => {
  const socket = makeFakeSocket([
    bytes(":1\r\n-WRONGTYPE bad set\r\n:3\r\n$3\r\nbar\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  await client.session(async (/** @type {any} */ session) => {
    await assert.rejects(
      session.sCardMany(["set:a", "set:b", "set:c"]),
      /Redis error: WRONGTYPE bad set/
    );
    assert.equal(await session.get("k"), "bar");
  });

  assert.equal(state.count, 1);
  assert.equal(decode(socket._writes[1]), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
});

test("RedisSession keeps single-command reply errors reusable", async () => {
  const socket = makeFakeSocket([
    bytes("-WRONGTYPE bad set\r\n$3\r\nbar\r\n"),
  ]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();

  await assert.rejects(session.sCard("set:a"), /Redis error: WRONGTYPE bad set/);
  assert.equal(await session.get("k"), "bar");
  await session.close();
});

test("RedisSession closes after a malformed pipeline reply", async () => {
  const socket = makeFakeSocket([bytes(":1\r\n!invalid\r\n:3\r\n")]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();

  await assert.rejects(
    session.sCardMany(["set:a", "set:b", "set:c"]),
    /Unknown RESP type/
  );
  assert.equal(session.hasOpenResources(), false);
  assert.ok(socket.closed);
  await assert.rejects(session.get("k"), /Redis session closed/);
});

test("RedisClient.existsMany batches independent existence reads", async () => {
  const socket = makeFakeSocket([bytes(":0\r\n:1\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  assert.deepEqual(await client.existsMany(["secrets:demo:a", "secrets:demo:b"]), [false, true]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$6\r\nEXISTS\r\n$14\r\nsecrets:demo:a\r\n" +
      "*2\r\n$6\r\nEXISTS\r\n$14\r\nsecrets:demo:b\r\n"
  );
});

test("RedisClient.existsAndXRange batches a stream resume snapshot", async () => {
  const socket = makeFakeSocket([
    bytes(":1\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\njson\r\n$2\r\n{}\r\n"),
  ]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  const result = await client.existsAndXRange("logs:demo:worker:s", "-", "+", 1);

  assert.equal(result.exists, true);
  assert.equal(decode(result.entries[0][0]), "1-0");
  assert.deepEqual(result.entries[0][1].map(decode), ["json", "{}"]);
  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*2\r\n$6\r\nEXISTS\r\n$18\r\nlogs:demo:worker:s\r\n" +
      "*6\r\n$6\r\nXRANGE\r\n$18\r\nlogs:demo:worker:s\r\n$1\r\n-\r\n$1\r\n+\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n"
  );
});

test("RedisClient batches independent HEXISTS checks on one socket", async () => {
  const socket = makeFakeSocket([bytes(":1\r\n:0\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect });

  assert.deepEqual(
    await client.hExistsMany(["worker:demo:api:v1", "worker:demo:api:v2"], "__meta__"),
    [true, false]
  );

  assert.equal(state.count, 1);
  assert.equal(
    decode(socket._writes[0]),
    "*3\r\n$7\r\nHEXISTS\r\n$18\r\nworker:demo:api:v1\r\n$8\r\n__meta__\r\n" +
      "*3\r\n$7\r\nHEXISTS\r\n$18\r\nworker:demo:api:v2\r\n$8\r\n__meta__\r\n"
  );
});

test("RedisClient ordinary commands use per-call sockets under workerd", async () => {
  const socketA = makeFakeSocket([bytes("$3\r\nfoo\r\n")]);
  const socketB = makeFakeSocket([bytes("$-1\r\n")]);
  const sockets = [socketA, socketB];
  let turn = 0;
  const connect = () => sockets[turn++];
  const client = new RedisClient("x", { connect });
  assert.equal(await client.hGet("k", "f"), "foo");
  assert.equal(await client.hGet("k", "g"), null);
  assert.equal(turn, 2, "ordinary commands must not reuse sockets across calls");
  assert.ok(socketA.closed && socketB.closed);
});

test("RedisClient selects configured DB before ordinary commands", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n$3\r\nfoo\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect, db: 1 });

  assert.equal(await client.hGet("k", "f"), "foo");
  assert.equal(decode(socket._writes[0]), "*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");
  assert.equal(decode(socket._writes[1]), "*3\r\n$4\r\nHGET\r\n$1\r\nk\r\n$1\r\nf\r\n");
});

test("RedisSession selects configured DB on its held socket", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n$3\r\nfoo\r\n")]);
  const { connect, state } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect, db: 1 });

  assert.equal(await client.session((/** @type {any} */ s) => s.hGet("k", "f")), "foo");
  assert.equal(state.count, 1);
  assert.equal(decode(socket._writes[0]), "*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");
  assert.equal(decode(socket._writes[1]), "*3\r\n$4\r\nHGET\r\n$1\r\nk\r\n$1\r\nf\r\n");
});

test("RedisSession reports resources while SELECT is still pending", async () => {
  let releaseSelect = () => {};
  const writer = {
    /** @param {Uint8Array} _buf */
    async write(_buf) {},
    close() { writer.closed = true; },
    /** @type {boolean} */
    closed: false,
  };
  const reader = {
    read() {
      return new Promise((resolve, reject) => {
        releaseSelect = () => {
          if (reader.released) {
            reject(new Error("reader released"));
          } else {
            resolve({ done: false, value: bytes("+OK\r\n") });
          }
        };
      });
    },
    releaseLock() { reader.released = true; },
    /** @type {boolean} */
    released: false,
  };
  const socket = {
    writable: { getWriter: () => writer },
    readable: { getReader: () => reader },
    close() { socket.closed = true; },
    /** @type {boolean} */
    closed: false,
  };
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect, db: 1 });

  assert.equal(session.hasOpenResources(), false);
  const openPromise = session.open();
  assert.equal(session.hasOpenResources(), true);
  await Promise.resolve();
  await session.close();
  assert.equal(session.hasOpenResources(), false);
  assert.equal(writer.closed, true);
  assert.equal(reader.released, true);
  assert.equal(socket.closed, true);
  releaseSelect();
  await assert.rejects(openPromise, /reader released/);
});

test("RedisSession closes partially opened resources when SELECT fails", async () => {
  const socket = makeFakeSocket([bytes("-ERR invalid DB index\r\n")]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect, db: 1 });

  await assert.rejects(session.open(), /invalid DB index/);

  assert.equal(session.hasOpenResources(), false);
  assert.equal(socket._writer.closed, true);
  assert.equal(socket._reader.released, true);
  assert.equal(socket.closed, true);
});

test("RedisClient.session does not enter its callback after SELECT failure", async () => {
  const socket = makeFakeSocket([bytes("-ERR invalid DB index\r\n")]);
  const { connect } = scriptedConnect(socket);
  const client = new RedisClient("x", { connect, db: 1 });
  let callbackCalled = false;

  await assert.rejects(
    client.session(async () => {
      callbackCalled = true;
    }),
    /invalid DB index/
  );

  assert.equal(callbackCalled, false);
  assert.equal(socket.closed, true);
});

test("RedisClient.set supports Valkey IFEQ and delIfEq", async () => {
  const socketA = makeFakeSocket([bytes("+OK\r\n")]);
  const socketB = makeFakeSocket([bytes(":1\r\n")]);
  const sockets = [socketA, socketB];
  let turn = 0;
  const connect = () => sockets[turn++];
  const client = new RedisClient("x", { connect });

  assert.equal(await client.set("lock", "token", { ttl: 600, ifeq: "token" }), "OK");
  assert.equal(await client.delIfEq("lock", "token"), 1);

  assert.equal(
    decode(socketA._writes[0]),
    "*7\r\n$3\r\nSET\r\n$4\r\nlock\r\n$5\r\ntoken\r\n$2\r\nEX\r\n$3\r\n600\r\n$4\r\nIFEQ\r\n$5\r\ntoken\r\n"
  );
  assert.equal(
    decode(socketB._writes[0]),
    "*3\r\n$7\r\nDELIFEQ\r\n$4\r\nlock\r\n$5\r\ntoken\r\n"
  );
});

test("RedisClient batches raw GET and DELIFEQ commands without decoding CAS values", async () => {
  const socketA = makeFakeSocket([bytes("$7\r\nowner-a\r\n$7\r\nowner-b\r\n")]);
  const socketB = makeFakeSocket([bytes(":1\r\n:0\r\n")]);
  const sockets = [socketA, socketB];
  let turn = 0;
  const client = new RedisClient("x", { connect: () => sockets[turn++] });

  const values = await client.getMany(["key-a", "key-b"]);
  assert.deepEqual(values.map(decode), ["owner-a", "owner-b"]);
  assert.deepEqual(await client.delIfEqMany([
    ["key-a", values[0]],
    ["key-b", values[1]],
  ]), [1, 0]);

  assert.equal(
    decode(socketA._writes[0]),
    "*2\r\n$3\r\nGET\r\n$5\r\nkey-a\r\n*2\r\n$3\r\nGET\r\n$5\r\nkey-b\r\n"
  );
  assert.equal(
    decode(socketB._writes[0]),
    "*3\r\n$7\r\nDELIFEQ\r\n$5\r\nkey-a\r\n$7\r\nowner-a\r\n" +
      "*3\r\n$7\r\nDELIFEQ\r\n$5\r\nkey-b\r\n$7\r\nowner-b\r\n"
  );
});

test("Redis SET helpers reject zero and malformed expiration options", async () => {
  const client = new RedisClient("x", {
    connect: () => {
      throw new Error("set validation should fail before opening a socket");
    },
  });
  await assert.rejects(
    () => client.set("lock", "token", { ttl: 0 }),
    /Redis SET ttl must be a positive integer/
  );
  await assert.rejects(
    () => client.set("lock", "token", { exat: "not-a-number" }),
    /Redis SET exat must be a positive integer/
  );
  await assert.rejects(
    () => client.set("lock", "token", { ttl: true }),
    /Redis SET ttl must be a positive integer/
  );
  await assert.rejects(
    () => client.set("lock", "token", { exat: "2" }),
    /Redis SET exat must be a positive integer/
  );

  const socket = makeFakeSocket([]);
  const { connect } = scriptedConnect(socket);
  const session = new RedisSession("x", { connect });
  await session.open();
  try {
    assert.throws(
      () => session.multi().set("lock", "token", { ttl: -1 }),
      /Redis SET ttl must be a positive integer/
    );
    assert.throws(
      () => session.multi().set("lock", "token", { exat: 0 }),
      /Redis SET exat must be a positive integer/
    );
  } finally {
    await session.close();
  }
});

test("RedisSession still owns one dedicated socket", async () => {
  const ordinarySocket = makeFakeSocket([bytes("$3\r\nhot\r\n")]);
  const sessionSocket = makeFakeSocket([bytes("$3\r\ncas\r\n")]);
  let turn = 0;
  const connect = () => (++turn === 1 ? ordinarySocket : sessionSocket);
  const client = new RedisClient("x", { connect });

  assert.equal(await client.hGet("k", "f"), "hot");
  assert.equal(await client.session((/** @type {any} */ s) => s.hGet("k", "f")), "cas");

  assert.equal(turn, 2, "session must open a separate connection");
  assert.equal(ordinarySocket.closed, true, "ordinary command closes its socket");
  assert.equal(sessionSocket.closed, true, "session closes its dedicated socket");
});

test("RedisSession.close is idempotent and blocks further commands", async () => {
  const socket = makeFakeSocket([bytes("+OK\r\n")]);
  const { connect } = scriptedConnect(socket);
  // Direct session (skip the RedisClient.session wrapper) so we can call
  // close() twice and assert the post-close command throws.
  const s = new RedisSession("x", { connect });
  await s.open();
  assert.equal(s.hasOpenResources(), true);
  await s.close();
  assert.equal(s.hasOpenResources(), false);
  await s.close(); // idempotent — no throw
  await assert.rejects(() => s.get("k"), /session closed/);
});
