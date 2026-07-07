// KV binding shim: put/get/delete/list + metadata. Assumes compose stack.
import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  delay,
  deployAndPromote,
  gatewayFetch,
  responseJson,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisHSet } from "./helpers/redis.js";
import { fnv1a32Utf8 } from "../../shared/fnv1a32.js";

setupIntegrationSuite();

const KV_HARNESS = readFileSync(
  new URL("../../test-workers/kv-harness/src/index.js", import.meta.url),
  "utf8"
);

/** @param {string} ns */
async function setup(ns) {
  await deployAndPromote(ns, "k", {
    mainModule: "worker.js",
    modules: { "worker.js": KV_HARNESS },
    bindings: { KV: { type: "kv", id: "test" } },
  });
}

/** @param {string} ns @param {Record<string, string>} params */
async function call(ns, params) {
  const qs = new URLSearchParams(params).toString();
  return gatewayFetch(ns, `/k?${qs}`);
}

/** @param {unknown} cursor */
function kvListCursor(cursor) {
  return `wdl2:${Buffer.from(JSON.stringify(cursor)).toString("base64url")}`;
}

/** @param {string} key */
function kvBucket(key) {
  return fnv1a32Utf8(key) % 32;
}

/** @param {string} ns @param {string} id @param {string} key */
function kvHashKey(ns, id, key) {
  return `kvh:${ns}:${id}:b:${kvBucket(key)}`;
}

/** @param {string} ns @param {string} id @param {Array<[string, string]>} entries */
function seedKvFields(ns, id, entries) {
  /** @type {Map<string, string[]>} */
  const byHash = new Map();
  for (const [key, value] of entries) {
    const hash = kvHashKey(ns, id, key);
    const fields = byHash.get(hash) || [];
    fields.push(`v:${key}`, value);
    byHash.set(hash, fields);
  }
  for (const [hash, fields] of byHash) {
    /** @type {Record<string, string>} */
    const hashFields = {};
    for (let i = 0; i < fields.length; i += 2) {
      hashFields[fields[i]] = fields[i + 1];
    }
    redisHSet(hash, hashFields, { db: 1 });
  }
}

/** @param {string} prefix @param {number} targetBucket @param {number} count */
function keysInBucket(prefix, targetBucket, count) {
  /** @type {string[]} */
  const keys = [];
  for (let i = 0; keys.length < count; i += 1) {
    const key = `${prefix}${String(i).padStart(6, "0")}`;
    if (kvBucket(key) === targetBucket) keys.push(key);
  }
  return keys;
}

test("put + get round-trip", async () => {
  await setup("kvns1");
  let r = await call("kvns1", { op: "put", key: "a", val: "alpha" });
  assert.equal(r.status, 200);
  r = await call("kvns1", { op: "get", key: "a" });
  assert.equal(await r.text(), "alpha");
});

test("get unknown key → null", async () => {
  await setup("kvns2");
  const r = await call("kvns2", { op: "get", key: "missing" });
  assert.equal(await r.text(), "__null__");
});

test("delete removes key", async () => {
  await setup("kvns3");
  await call("kvns3", { op: "put", key: "x", val: "1" });
  await call("kvns3", { op: "del", key: "x" });
  const r = await call("kvns3", { op: "get", key: "x" });
  assert.equal(await r.text(), "__null__");
});

test("metadata stored and retrieved", async () => {
  await setup("kvns4");
  await call("kvns4", {
    op: "put",
    key: "m",
    val: "v",
    meta: JSON.stringify({ author: "alice" }),
  });
  const r = await call("kvns4", { op: "getMeta", key: "m" });
  const json = await responseJson(r);
  assert.equal(json.value, "v");
  assert.deepEqual(json.metadata, { author: "alice" });
});

test("put without metadata clears prior metadata", async () => {
  await setup("kvns5");
  await call("kvns5", {
    op: "put",
    key: "k",
    val: "v1",
    meta: JSON.stringify({ tag: 1 }),
  });
  await call("kvns5", { op: "put", key: "k", val: "v2" });
  const r = await call("kvns5", { op: "getMeta", key: "k" });
  const json = await responseJson(r);
  assert.equal(json.value, "v2");
  assert.equal(json.metadata, null);
});

test("list by prefix", async () => {
  await setup("kvns6");
  await call("kvns6", { op: "put", key: "a1", val: "1" });
  await call("kvns6", { op: "put", key: "a2", val: "2" });
  await call("kvns6", { op: "put", key: "b1", val: "3" });
  const r = await call("kvns6", { op: "list", prefix: "a" });
  const json = await responseJson(r);
  const names = json.keys.map((/** @type {any} */ k) => k.name).toSorted();
  assert.deepEqual(names, ["a1", "a2"]);
  assert.equal(json.list_complete, true);
});

test("KV keys are isolated by namespace even for same binding id", async () => {
  await setup("kvns7a");
  await setup("kvns7b");
  await call("kvns7a", { op: "put", key: "shared", val: "A" });
  await call("kvns7b", { op: "put", key: "shared", val: "B" });
  const a = await call("kvns7a", { op: "get", key: "shared" });
  const b = await call("kvns7b", { op: "get", key: "shared" });
  assert.equal(await a.text(), "A");
  assert.equal(await b.text(), "B");
});

test("KV keys with 'v:' prefix don't collide with metadata keys", async () => {
  // Regression guard: user key literally "v:foo" must stay an ordinary field
  // name even though old KV storage used v:/m: internal sub-prefixes.
  await setup("kvns8");
  await call("kvns8", {
    op: "put",
    key: "v:foo",
    val: "user-value",
    meta: JSON.stringify({ owned: "by-user" }),
  });
  const r = await call("kvns8", { op: "getMeta", key: "v:foo" });
  const json = await responseJson(r);
  assert.equal(json.value, "user-value");
  assert.deepEqual(json.metadata, { owned: "by-user" });
});

test("KV keys with 'm:' prefix don't collide with metadata keys", async () => {
  // Mirror of the v: test: user key literally "m:foo" must not be confused
  // with storage metadata.
  await setup("kvns9");
  await call("kvns9", {
    op: "put",
    key: "m:foo",
    val: "real-value",
    meta: JSON.stringify({ label: "meta-prefixed" }),
  });
  const r = await call("kvns9", { op: "getMeta", key: "m:foo" });
  const json = await responseJson(r);
  assert.equal(json.value, "real-value");
  assert.deepEqual(json.metadata, { label: "meta-prefixed" });
});

test("get supports 'json' type", async () => {
  await setup("kvns10");
  await call("kvns10", { op: "put", key: "j", val: '{"n":42,"s":"x"}' });
  const r = await call("kvns10", { op: "get", key: "j", type: "json" });
  assert.deepEqual(await responseJson(r), { n: 42, s: "x" });
});

test("get supports 'arrayBuffer' type", async () => {
  await setup("kvns11");
  await call("kvns11", { op: "put", key: "b", val: "hi" });
  const r = await call("kvns11", { op: "get", key: "b", type: "arrayBuffer" });
  const buf = Buffer.from(await r.arrayBuffer());
  assert.equal(buf.toString("utf8"), "hi");
});

test("getWithMetadata supports 'json' type for value", async () => {
  await setup("kvns12");
  await call("kvns12", {
    op: "put",
    key: "k",
    val: '{"x":1}',
    meta: JSON.stringify({ tag: "t" }),
  });
  const r = await call("kvns12", { op: "getMeta", key: "k", type: "json" });
  const json = await responseJson(r);
  assert.deepEqual(json.value, { x: 1 });
  assert.deepEqual(json.metadata, { tag: "t" });
});

test("batch get and getWithMetadata return Maps", async () => {
  await setup("kvns12b");
  await call("kvns12b", { op: "put", key: "a", val: '{"n":1}', meta: JSON.stringify({ tag: "a" }) });
  await call("kvns12b", { op: "put", key: "b", val: '{"n":2}' });

  let r = await call("kvns12b", { op: "getBatch", keys: JSON.stringify(["a", "missing", "b"]), type: "json" });
  assert.deepEqual(await responseJson(r), [
    ["a", { n: 1 }],
    ["missing", null],
    ["b", { n: 2 }],
  ]);

  r = await call("kvns12b", { op: "getMetaBatch", keys: JSON.stringify(["a", "missing", "b"]), type: "json" });
  assert.deepEqual(await responseJson(r), [
    ["a", { value: { n: 1 }, metadata: { tag: "a" } }],
    ["missing", { value: null, metadata: null }],
    ["b", { value: { n: 2 }, metadata: null }],
  ]);
});

test("list honors limit and keeps pagination self-terminating", async () => {
  // The shim uses Redis SCAN with a fixed internal COUNT, so whether
  // pagination kicks in depends on total keys vs COUNT, not on `limit`
  // alone. The contract we *can* check: `limit` bounds keys per call, and
  // walking returned cursors always terminates with every key visible.
  await setup("kvns13");
  const total = 12;
  for (let i = 0; i < total; i++) {
    await call("kvns13", { op: "put", key: `k${i}`, val: String(i) });
  }

  const seen = new Set();
  let cursor = "";
  let guard = 0;
  for (;;) {
    if (guard++ > 50) throw new Error("pagination did not terminate");
    /** @type {Record<string, string>} */
    const params = { op: "list", prefix: "k", limit: "5" };
    if (cursor) params.cursor = cursor;
    const r = await call("kvns13", params);
    const page = await responseJson(r);
    // SCAN cursors mean "iteration not complete", not "more matching keys".
    // A final empty page is valid as long as walking cursors terminates and
    // every matching key is observed.
    for (const k of page.keys) seen.add(k.name);
    if (page.list_complete) break;
    assert.ok(page.keys.length > 0, "intermediate page must make progress");
    cursor = page.cursor;
    assert.ok(cursor, "expected cursor when list_complete=false");
  }
  assert.equal(seen.size, total);
  for (let i = 0; i < total; i++) assert.ok(seen.has(`k${i}`), `missing k${i}`);
});

test("list clamps oversized user limits to Cloudflare's 1000-key page cap", async () => {
  const ns = "kvns-list-cap";
  await setup(ns);
  const entries = keysInBucket("cap-", 0, 1005).map((/** @type {string} */ key) => /** @type {[string, string]} */ ([key, "x"]));
  seedKvFields(ns, "test", entries);

  const r = await call(ns, { op: "list", prefix: "cap-", limit: "5000" });
  assert.equal(r.status, 200);
  const page = await responseJson(r);
  assert.ok(
    page.keys.length <= 1000,
    `expected at most 1000 keys after runtime/proxy clamping, got ${page.keys.length}`
  );
});

test("list ignores forged cursor overflow outside the current binding page", async () => {
  const ns = "kvns-forged-cursor";
  await setup(ns);
  seedKvFields(ns, "test", [["safe-existing", "same-binding"]]);
  seedKvFields("otherns", "test", [["safe-cross-ns", "cross-ns"]]);

  const cursor = kvListCursor({
    bucket: kvBucket("safe-existing"),
    scan: "0",
    overflow: ["v:safe-missing", "v:safe-existing", "v:other-existing", "v:safe-cross-ns"],
  });
  const r = await call(ns, { op: "list", prefix: "safe-", cursor });
  assert.equal(r.status, 200);
  const page = await responseJson(r);
  assert.deepEqual(page.keys, [{ name: "safe-existing" }]);
  assert.equal(page.list_complete, true);
});

test("KV list includes metadata without fetching values", async () => {
  await setup("kvns-list-meta");
  await call("kvns-list-meta", {
    op: "put",
    key: "meta-a",
    val: "value-a",
    meta: JSON.stringify({ tag: "a" }),
  });
  await call("kvns-list-meta", { op: "put", key: "meta-b", val: "value-b" });
  const r = await call("kvns-list-meta", { op: "list", prefix: "meta-", metadata: "true" });
  assert.equal(r.status, 200);
  const page = await responseJson(r);
  assert.deepEqual(
    page.keys.toSorted((/** @type {any} */ a, /** @type {any} */ b) => a.name.localeCompare(b.name)),
    [
      { name: "meta-a", metadata: { tag: "a" } },
      { name: "meta-b", metadata: null },
    ]
  );
});

test("put with expirationTtl makes the key expire", async () => {
  await setup("kvns14");
  await call("kvns14", { op: "put", key: "ephemeral", val: "bye", ttl: "2" });
  let r = await call("kvns14", { op: "get", key: "ephemeral" });
  assert.equal(await r.text(), "bye");
  await waitUntil("KV TTL expires value", async () => {
    r = await call("kvns14", { op: "get", key: "ephemeral" });
    return await r.text() === "__null__";
  }, { timeoutMs: 6_000, intervalMs: 250 });
});

test("put without expiration clears a previous key expiration", async () => {
  await setup("kvns14b");
  await call("kvns14b", { op: "put", key: "sticky", val: "temporary", ttl: "2" });
  await call("kvns14b", { op: "put", key: "sticky", val: "permanent" });
  await delay(3500);
  const r = await call("kvns14b", { op: "get", key: "sticky" });
  assert.equal(await r.text(), "permanent");
});

test("put with metadata + expirationTtl expires both value and metadata", async () => {
  await setup("kvns15");
  await call("kvns15", {
    op: "put",
    key: "k",
    val: "v",
    meta: JSON.stringify({ tag: 1 }),
    ttl: "2",
  });
  let r = await call("kvns15", { op: "getMeta", key: "k" });
  let json = await responseJson(r);
  assert.equal(json.value, "v");
  assert.deepEqual(json.metadata, { tag: 1 });
  await waitUntil("KV TTL expires value and metadata", async () => {
    r = await call("kvns15", { op: "getMeta", key: "k" });
    json = await responseJson(r);
    return json.value === null && json.metadata === null;
  }, { timeoutMs: 6_000, intervalMs: 250 });
});
