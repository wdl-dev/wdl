import { test } from "node:test";
import assert from "node:assert/strict";
import { redisConformanceCases } from "../helpers/redis-conformance-cases.js";
import { createFakeRedis, FakeRedisWatchError } from "../helpers/mocks/fake-redis.js";

/**
 * @param {ReturnType<typeof createFakeRedis>} redis
 * @param {string} prefix
 * @returns {import("../helpers/redis-conformance-cases.js").RedisConformanceAdapter}
 */
function fakeRedisConformanceAdapter(redis, prefix) {
  return {
    key: (suffix) => `${prefix}:${suffix}`,
    del: (...keys) => redis.del(...keys),
    exists: async (key) => await redis.exists(key) === 1,
    hSet: (key, fields) => redis.hSet(key, fields),
    hGet: (key, field) => redis.hGet(key, field),
    hMGet: (key, fields) => redis.hMGet(key, fields),
    hGetAll: (key) => redis.hGetAll(key),
    hDel: (key, ...fields) => redis.hDel(key, ...fields),
    set: (key, value) => redis.set(key, value),
    sAdd: (key, member) => redis.sAdd(key, member),
    zAdd: (key, score, member) => redis.session((session) => session.multi().zAdd(key, score, member).exec()),
    zRange: (key, start, stop) => redis.zRange(key, start, stop),
    copy: async (src, dst, opts = {}) => Number(await redis.session((session) => session.copy(src, dst, opts))),
    expireAt: (key, unixSeconds) => redis.session((session) => session.multi().expireAt(key, unixSeconds).exec()),
    expireTime: async (key) => {
      const expiresAt = redis.expirations.get(key);
      // The fake stores expirations in JavaScript milliseconds; Redis EXPIRETIME
      // returns whole Unix seconds.
      return expiresAt == null ? -1 : Math.floor(expiresAt / 1000);
    },
  };
}

for (const conformanceCase of redisConformanceCases) {
  test(`fake redis conformance: ${conformanceCase.name}`, async () => {
    const redis = createFakeRedis();
    await conformanceCase.run(fakeRedisConformanceAdapter(
      redis,
      `fake-conformance:${conformanceCase.id}`
    ));
  });
}

test("fake redis client records batched hash reads", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("hash:a", { one: "1", two: "2" });
  redis.hashes.set("hash:b", { one: "3" });
  const inheritedHash = Object.create({ inherited: "bad" });
  inheritedHash.own = "ok";
  redis.hashes.set("hash:proto", inheritedHash);

  assert.deepEqual(await redis.hGetAllMany(["hash:a", "hash:b", "hash:c"]), [
    { one: "1", two: "2" },
    { one: "3" },
    {},
  ]);
  assert.deepEqual(await redis.hGetMany([
    ["hash:a", "two"],
    ["hash:b", "missing"],
  ]), ["2", null]);
  assert.deepEqual(await redis.hMGet("hash:a", ["one", "missing", "two"]), ["1", null, "2"]);
  assert.deepEqual(await redis.hMGet("hash:proto", ["inherited", "own"]), [null, "ok"]);
  assert.deepEqual(await redis.hMGet("hash:a", []), []);
  assert.deepEqual(redis.commands, [
    ["hGetAllMany", ["hash:a", "hash:b", "hash:c"]],
    ["hGetMany", [["hash:a", "two"], ["hash:b", "missing"]]],
    ["hMGet", "hash:a", ["one", "missing", "two"]],
    ["hMGet", "hash:proto", ["inherited", "own"]],
  ]);
});

test("fake redis session records single and batched hash reads", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("hash:a", { one: "1" });
  redis.hashes.set("hash:b", { two: "2" });

  await redis.session(async (session) => {
    assert.deepEqual(await session.hGetAll("hash:a"), { one: "1" });
    assert.deepEqual(await session.hGetAllMany(["hash:a", "hash:b"]), [
      { one: "1" },
      { two: "2" },
    ]);
    assert.equal(await session.hGet("hash:b", "two"), "2");
    assert.deepEqual(await session.hMGet("hash:b", ["two", "missing"]), ["2", null]);
    assert.deepEqual(await session.hMGet("hash:b", []), []);
  });
  assert.deepEqual(redis.commands, [
    ["hGetAll", "hash:a"],
    ["hGetAllMany", ["hash:a", "hash:b"]],
    ["hGet", "hash:b", "two"],
    ["hMGet", "hash:b", ["two", "missing"]],
  ]);
});

test("fake redis supports hash existence and key reads on clients and sessions", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("hash:a", { one: "1", two: "2" });

  assert.equal(await redis.hExists("hash:a", "one"), true);
  assert.equal(await redis.hExists("hash:a", "missing"), false);
  assert.deepEqual(await redis.hKeys("hash:a"), ["one", "two"]);

  await redis.session(async (session) => {
    assert.equal(await session.hExists("hash:a", "two"), true);
    assert.equal(await session.hExists("hash:missing", "two"), false);
    assert.deepEqual(await session.hKeys("hash:a"), ["one", "two"]);
  });

  assert.deepEqual(redis.commands, [
    ["hExists", "hash:a", "one"],
    ["hExists", "hash:a", "missing"],
    ["hKeys", "hash:a"],
    ["hExists", "hash:a", "two"],
    ["hExists", "hash:missing", "two"],
    ["hKeys", "hash:a"],
  ]);
});

test("fake redis zRange mirrors sorted-set ordering and inclusive indexes", async () => {
  const redis = createFakeRedis();
  redis.zsets.set("versions", new Map([
    ["v3", 3],
    ["v1", 1],
    ["v2b", 2],
    ["v2a", 2],
  ]));

  assert.equal(await redis.zCard("versions"), 4);
  assert.deepEqual(await redis.zRange("versions", 0, -1), ["v1", "v2a", "v2b", "v3"]);
  assert.deepEqual(await redis.zRange("versions", 1, 2), ["v2a", "v2b"]);

  await redis.session(async (session) => {
    assert.equal(await session.zCard("versions"), 4);
    assert.deepEqual(await session.zRange("versions", -2, -1), ["v2b", "v3"]);
    assert.deepEqual(await session.zRange("missing", 0, -1), []);
  });
});

test("fake redis sessions support batched zRange and exists reads", async () => {
  const redis = createFakeRedis();
  redis.zsets.set("versions:a", new Map([
    ["v2", 2],
    ["v1", 1],
  ]));
  redis.zsets.set("versions:b", new Map([["v3", 3]]));
  redis.hashes.set("secrets:b", { TOKEN: "value" });

  await redis.session(async (session) => {
    assert.deepEqual(await session.zRangeMany(["versions:a", "versions:b", "versions:c"], 0, -1), [
      ["v1", "v2"],
      ["v3"],
      [],
    ]);
    assert.deepEqual(await session.existsMany(["secrets:a", "secrets:b"]), [false, true]);
    assert.deepEqual(await session.zRangeMany(["versions:a", "versions:b"], -1, -1), [
      ["v2"],
      ["v3"],
    ]);
  });

  assert.deepEqual(redis.commands, [
    ["zRangeMany", ["versions:a", "versions:b", "versions:c"], 0, -1],
    ["existsMany", ["secrets:a", "secrets:b"]],
    ["zRangeMany", ["versions:a", "versions:b"], -1, -1],
  ]);
});

test("fake redis treats empty collection keys as absent", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("hash:empty", {});
  redis.sets.set("set:empty", new Set());
  redis.zsets.set("zset:empty", new Map());

  assert.equal(await redis.exists("hash:empty"), 0);
  await redis.session(async (session) => {
    assert.deepEqual(await session.existsMany(["set:empty", "zset:empty"]), [false, false]);
  });

  redis.hashes.set("hash:one", { value: "1" });
  assert.equal(await redis.exists("hash:one"), 1);
  assert.equal(await redis.hDel("hash:one", "value"), 1);
  assert.equal(await redis.exists("hash:one"), 0);

  await redis.session(async (session) => {
    await session.multi()
      .hSet("hash:two", { value: "2" })
      .hDel("hash:two", "value")
      .sAdd("set:one", "a")
      .sRem("set:one", "a")
      .zAdd("zset:one", 1, "a")
      .zRem("zset:one", "a")
      .exec();
  });

  await redis.session(async (session) => {
    assert.deepEqual(
      await session.existsMany(["hash:two", "set:one", "zset:one"]),
      [false, false, false]
    );
  });
});

test("fake redis multi copy records and honors replace option", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("src", { value: "new" });
  redis.hashes.set("dst", { value: "old" });

  await redis.session(async (session) => {
    assert.deepEqual(await session.multi().copy("src", "dst").exec(), [0]);
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "old" });

  await redis.session(async (session) => {
    assert.deepEqual(await session.multi().copy("src", "dst", { REPLACE: true }).exec(), [1]);
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "new" });
  assert.deepEqual(redis.ops.filter((op) => op[0] === "copy"), [
    ["copy", "src", "dst", {}],
    ["copy", "src", "dst", { REPLACE: true }],
  ]);
});

test("fake redis copy replace clears destination type and ttl remnants", async () => {
  const redis = createFakeRedis();
  redis.hashes.set("src", { value: "new" });
  redis.strings.set("dst", "old-string");
  redis.sets.set("dst", new Set(["old-set"]));
  redis.zsets.set("dst", new Map([["old-zset", 1]]));
  redis.expirations.set("dst", 9_999);

  await redis.session(async (session) => {
    assert.equal(await session.copy("src", "dst", { REPLACE: true }), 1);
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "new" });
  assert.equal(redis.strings.has("dst"), false);
  assert.equal(redis.sets.has("dst"), false);
  assert.equal(redis.zsets.has("dst"), false);
  assert.equal(redis.expirations.has("dst"), false);

  redis.strings.set("dst", "old-string-again");
  redis.sets.set("dst", new Set(["old-set-again"]));
  redis.zsets.set("dst", new Map([["old-zset-again", 1]]));
  redis.expirations.set("dst", 9_999);
  await redis.session(async (session) => {
    await session.multi().copy("src", "dst", { REPLACE: true }).exec();
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "new" });
  assert.equal(redis.strings.has("dst"), false);
  assert.equal(redis.sets.has("dst"), false);
  assert.equal(redis.zsets.has("dst"), false);
  assert.equal(redis.expirations.has("dst"), false);
});

test("fake redis copy preserves source expiration", async () => {
  let nowMs = 1_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });
  redis.hashes.set("src", { value: "new" });
  redis.expirations.set("src", 9_999_000);

  await redis.session(async (session) => {
    assert.equal(await session.copy("src", "dst"), 1);
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "new" });
  assert.equal(redis.expirations.get("dst"), 9_999_000);

  redis.hashes.set("other", { value: "other" });
  await redis.session(async (session) => {
    await session.multi().copy("other", "dst", { REPLACE: true }).exec();
  });
  assert.deepEqual(redis.hashes.get("dst"), { value: "other" });
  assert.equal(redis.expirations.has("dst"), false);

  nowMs = 10_000_000;
  assert.deepEqual(await redis.hGetAll("src"), {});
});

test("fake redis supports lock-style set and delIfEq with TTL expiry", async () => {
  let nowMs = 1_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });

  assert.equal(await redis.set("lock", "token-a", { nx: true, ttl: 1 }), "OK");
  assert.equal(await redis.set("lock", "token-b", { nx: true, ttl: 1 }), null);
  assert.equal(await redis.set("lock", "token-b", { ifeq: "token-a", ttl: 1 }), "OK");
  assert.equal(await redis.delIfEq("lock", "token-a"), 0);
  assert.equal(await redis.get("lock"), "token-b");
  nowMs += 1_001;
  assert.equal(await redis.get("lock"), null);
  assert.equal(await redis.set("lock", "token-c", { nx: true }), "OK");
  assert.equal(await redis.delIfEq("lock", "token-c"), 1);
  assert.equal(await redis.exists("lock"), 0);
});

test("fake redis set without ttl clears an existing expiration", async () => {
  let nowMs = 1_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });

  assert.equal(await redis.set("key", "a", { ttl: 1 }), "OK");
  assert.equal(await redis.set("key", "b"), "OK");
  nowMs += 1_001;
  assert.equal(await redis.get("key"), "b");
});

test("fake redis scan paginates matching keys", async () => {
  const redis = createFakeRedis();
  redis.strings.set("auth:token:a", "1");
  redis.strings.set("auth:token:b", "2");
  redis.hashes.set("auth:token:c", { value: "3" });
  redis.strings.set("worker:demo", "ignored");

  const first = await redis.scan("0", "auth:token:*", 2);
  const second = await redis.scan(first[0], "auth:token:*", 2);

  assert.deepEqual(first, ["2", ["auth:token:a", "auth:token:b"]]);
  assert.deepEqual(second, ["0", ["auth:token:c"]]);
});

test("fake redis multi injects watch conflicts", async () => {
  const redis = createFakeRedis();
  redis.execFailures = 1;

  await assert.rejects(
    () => redis.session((session) => session.multi().set("key", "value").exec()),
    (err) => err instanceof FakeRedisWatchError
  );
  assert.equal(redis.strings.has("key"), false);

  await redis.session((session) => session.multi().set("key", "value").exec());
  assert.equal(redis.strings.get("key"), "value");
});

test("fake redis multi set honors lock options", async () => {
  let nowMs = 1_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });

  await redis.session((session) => session.multi().set("lock", "token-a", { nx: true, ttl: 1 }).exec());
  await redis.session((session) => session.multi().set("lock", "token-b", { nx: true }).exec());
  assert.equal(redis.strings.get("lock"), "token-a");

  await redis.session((session) => session.multi().set("lock", "token-b", { ifeq: "token-a", ttl: 1 }).exec());
  assert.equal(redis.strings.get("lock"), "token-b");
  nowMs += 1_001;
  assert.equal(await redis.get("lock"), null);
});

test("fake redis multi set without ttl clears an existing expiration", async () => {
  let nowMs = 1_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });

  await redis.session((session) => session.multi().set("key", "a", { ttl: 1 }).exec());
  await redis.session((session) => session.multi().set("key", "b").exec());
  nowMs += 1_001;
  assert.equal(await redis.get("key"), "b");
});

test("fake redis multi expireAt uses Redis unix seconds", async () => {
  let nowMs = 1_000_000;
  const redis = createFakeRedis(undefined, { nowMs: () => nowMs });

  await redis.session(async (session) => {
    await session.multi()
      .sAdd("cron-slot", "demo/app/v1")
      .expireAt("cron-slot", Math.floor(nowMs / 1000) + 60)
      .exec();
  });
  assert.deepEqual(await redis.sMembers("cron-slot"), ["demo/app/v1"]);

  nowMs += 60_001;
  assert.deepEqual(await redis.sMembers("cron-slot"), []);
});
