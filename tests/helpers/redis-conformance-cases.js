import assert from "node:assert/strict";

/**
 * @typedef {{
 *   key: (suffix: string) => string,
 *   del: (...keys: string[]) => Promise<unknown>,
 *   exists: (key: string) => Promise<boolean>,
 *   existsCount: (...keys: string[]) => Promise<number>,
 *   hSet: (key: string, fields: Record<string, string>) => Promise<unknown>,
 *   hGet: (key: string, field: string) => Promise<string | null>,
 *   hMGet: (key: string, fields: string[]) => Promise<Array<string | null>>,
 *   hGetAll: (key: string) => Promise<Record<string, string>>,
 *   hStrLenMany: (pairs: Array<[string, string]>) => Promise<number[]>,
 *   hDel: (key: string, ...fields: string[]) => Promise<number>,
 *   set: (key: string, value: string) => Promise<unknown>,
 *   setIfEq: (key: string, value: string, expected: string) => Promise<boolean>,
 *   delIfEq: (key: string, expected: string) => Promise<number>,
 *   sAdd: (key: string, member: string) => Promise<unknown>,
 *   sMembers: (key: string) => Promise<string[]>,
 *   sCard: (key: string) => Promise<number>,
 *   sCardMany: (keys: string[]) => Promise<number[]>,
 *   zAdd: (key: string, score: number, member: string) => Promise<unknown>,
 *   zRange: (key: string, start: number, stop: number) => Promise<string[]>,
 *   copy: (src: string, dst: string, opts?: { REPLACE?: boolean }) => Promise<number>,
 *   expireAt: (key: string, unixSeconds: number) => Promise<unknown>,
 *   expireTime: (key: string) => Promise<number>,
 * }} RedisConformanceAdapter
 */

export const redisConformanceCases = [
  {
    id: "multi-exists",
    name: "multi-key existence counts present keys including duplicates",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const first = redis.key("exists-first");
      const second = redis.key("exists-second");
      const missing = redis.key("exists-missing");
      await redis.del(first, second, missing);
      await redis.set(first, "1");
      await redis.set(second, "2");

      assert.equal(await redis.existsCount(first, missing, second, first), 3);
    },
  },
  {
    id: "value-cas",
    name: "value CAS updates and deletes only the expected string",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const key = redis.key("value-cas");
      await redis.del(key);
      await redis.set(key, "owner-a");

      assert.equal(await redis.setIfEq(key, "owner-b", "stale-owner"), false);
      assert.equal(await redis.setIfEq(key, "owner-b", "owner-a"), true);
      assert.equal(await redis.delIfEq(key, "owner-a"), 0);
      assert.equal(await redis.delIfEq(key, "owner-b"), 1);
      assert.equal(await redis.exists(key), false);
      assert.equal(await redis.setIfEq(key, "owner-c", "owner-b"), false);
      assert.equal(await redis.delIfEq(key, "owner-b"), 0);
    },
  },
  {
    id: "hash-read",
    name: "hash reads return own fields, missing fields, and empty HMGET batches",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const hash = redis.key("hash");
      const missing = redis.key("missing-hash");
      await redis.del(hash, missing);

      await redis.hSet(hash, { one: "1", two: "2" });

      assert.deepEqual(await redis.hGetAll(missing), {});
      assert.equal(await redis.hGet(hash, "one"), "1");
      assert.equal(await redis.hGet(hash, "missing"), null);
      assert.deepEqual(
        await redis.hMGet(hash, ["one", "missing", "two", "trailing"]),
        ["1", null, "2", null]
      );
      // WDL helpers make empty batch reads a no-op even when Redis wire commands
      // would reject the corresponding zero-field HMGET.
      assert.deepEqual(await redis.hMGet(hash, []), []);
    },
  },
  {
    id: "hash-delete",
    name: "hash delete removes the key after the last field",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const hash = redis.key("hash-delete");
      await redis.del(hash);

      await redis.hSet(hash, { value: "1" });
      assert.equal(await redis.exists(hash), true);
      assert.equal(await redis.hDel(hash, "value"), 1);
      assert.equal(await redis.exists(hash), false);
      assert.deepEqual(await redis.hGetAll(hash), {});
    },
  },
  {
    id: "hash-length-set-cardinality",
    name: "hash byte lengths and set cardinalities preserve missing-key semantics",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const hash = redis.key("length-hash");
      const members = redis.key("cardinality-set");
      const missing = redis.key("cardinality-missing");
      await redis.del(hash, members, missing);
      await redis.hSet(hash, { ascii: "a", unicode: "\u00e9", empty: "" });
      await redis.sAdd(members, "one");
      await redis.sAdd(members, "two");

      assert.deepEqual(await redis.hStrLenMany([
        [hash, "ascii"],
        [hash, "unicode"],
        [hash, "empty"],
        [hash, "missing"],
        [missing, "missing"],
      ]), [1, 2, 0, 0, 0]);
      assert.equal(await redis.sCard(members), 2);
      assert.equal(await redis.sCard(missing), 0);
      assert.deepEqual(await redis.sCardMany([members, missing]), [2, 0]);
    },
  },
  {
    id: "zrange",
    name: "sorted ranges use score/member order and inclusive negative indexes",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const versions = redis.key("versions");
      await redis.del(versions);

      await redis.zAdd(versions, 3, "v3");
      await redis.zAdd(versions, 1, "v1");
      await redis.zAdd(versions, 2, "v2b");
      await redis.zAdd(versions, 2, "v2a");

      assert.deepEqual(await redis.zRange(versions, 0, -1), ["v1", "v2a", "v2b", "v3"]);
      assert.deepEqual(await redis.zRange(versions, 1, 2), ["v2a", "v2b"]);
      assert.deepEqual(await redis.zRange(versions, -2, -1), ["v2b", "v3"]);
      assert.deepEqual(await redis.zRange(redis.key("versions-missing"), 0, -1), []);
    },
  },
  {
    id: "copy-replace",
    name: "COPY REPLACE overwrites target type and preserves source expiration",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const src = redis.key("copy-src");
      const futureUnixSeconds = 4_102_444_800;
      await redis.del(
        src,
        redis.key("copy-dst-string"),
        redis.key("copy-dst-set"),
        redis.key("copy-dst-zset"),
        redis.key("copy-dst-hash")
      );

      await redis.hSet(src, { value: "new" });
      await redis.expireAt(src, futureUnixSeconds);

      const existingHash = redis.key("copy-dst-hash");
      await redis.hSet(existingHash, { value: "old" });
      assert.equal(await redis.copy(src, existingHash), 0);
      assert.deepEqual(await redis.hGetAll(existingHash), { value: "old" });

      /** @type {Array<[string, (key: string) => Promise<void>]>} */
      const targets = [
        ["copy-dst-string", async (key) => { await redis.set(key, "old-string"); }],
        ["copy-dst-set", async (key) => { await redis.sAdd(key, "old-set"); }],
        ["copy-dst-zset", async (key) => { await redis.zAdd(key, 1, "old-zset"); }],
        ["copy-dst-hash", async (key) => { await redis.hSet(key, { value: "old-hash" }); }],
      ];
      for (const [suffix, seed] of targets) {
        const dst = redis.key(suffix);
        await redis.del(dst);
        await seed(dst);
        await redis.expireAt(dst, 1_893_456_000);

        assert.equal(await redis.copy(src, dst, { REPLACE: true }), 1);
        assert.deepEqual(await redis.hGetAll(dst), { value: "new" });
        assert.equal(await redis.expireTime(dst), futureUnixSeconds);
      }
    },
  },
  {
    id: "copy-set-source",
    name: "COPY preserves Set values and source expiration",
    /** @param {RedisConformanceAdapter} redis */
    async run(redis) {
      const src = redis.key("copy-set-src");
      const dst = redis.key("copy-set-dst");
      const futureUnixSeconds = 4_102_444_800;
      await redis.del(src, dst);
      await redis.sAdd(src, "two");
      await redis.sAdd(src, "one");
      await redis.expireAt(src, futureUnixSeconds);
      await redis.hSet(dst, { old: "value" });

      assert.equal(await redis.copy(src, dst, { REPLACE: true }), 1);
      assert.deepEqual((await redis.sMembers(dst)).toSorted(), ["one", "two"]);
      assert.equal(await redis.expireTime(dst), futureUnixSeconds);
    },
  },
];
