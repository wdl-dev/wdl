import { test } from "node:test";
import assert from "node:assert/strict";
import { redisConformanceCases } from "../helpers/redis-conformance-cases.js";
import { setupIntegrationSuite, uniqueNs } from "./helpers/index.js";
import {
  redisCommand,
  redisCopy,
  redisDel,
  redisExists,
  redisExistsCount,
  redisExpireAt,
  redisExpireTime,
  redisHDel,
  redisHGet,
  redisHGetAll,
  redisHMGet,
  redisHSet,
  redisHStrLen,
  redisSAdd,
  redisSCard,
  redisSMembers,
  redisSet,
  redisSetIfEq,
  redisDelIfEq,
  redisZAdd,
  redisZRange,
} from "./helpers/redis.js";

setupIntegrationSuite();

/**
 * @param {string} prefix
 * @returns {import("../helpers/redis-conformance-cases.js").RedisConformanceAdapter}
 */
function realRedisConformanceAdapter(prefix) {
  return {
    key: (suffix) => `${prefix}:${suffix}`,
    del: async (...keys) => {
      for (const key of keys) redisDel(key);
    },
    exists: async (key) => redisExists(key),
    existsCount: async (...keys) => redisExistsCount(keys),
    hSet: async (key, fields) => redisHSet(key, fields),
    hGet: async (key, field) => redisHGet(key, field),
    hMGet: async (key, fields) => redisHMGet(key, fields),
    hGetAll: async (key) => redisHGetAll(key),
    hStrLenMany: async (pairs) => pairs.map(([key, field]) => redisHStrLen(key, field)),
    hDel: async (key, ...fields) => redisHDel(key, fields),
    set: async (key, value) => redisSet(key, value),
    setIfEq: async (key, value, expected) => redisSetIfEq(key, value, expected),
    delIfEq: async (key, expected) => redisDelIfEq(key, expected),
    sAdd: async (key, member) => redisSAdd(key, member),
    sMembers: async (key) => redisSMembers(key),
    sCard: async (key) => redisSCard(key),
    sCardMany: async (keys) => keys.map((key) => redisSCard(key)),
    zAdd: async (key, score, member) => redisZAdd(key, score, member),
    zRange: async (key, start, stop) => redisZRange(key, start, stop),
    copy: async (src, dst, opts = {}) => redisCopy(src, dst, { replace: opts.REPLACE === true }),
    expireAt: async (key, unixSeconds) => redisExpireAt(key, unixSeconds),
    expireTime: async (key) => redisExpireTime(key),
  };
}

for (const conformanceCase of redisConformanceCases) {
  test(`real redis conformance: ${conformanceCase.name}`, async () => {
    const prefix = uniqueNs(`redisconf-${conformanceCase.id}`);
    await conformanceCase.run(realRedisConformanceAdapter(prefix));
  });
}

test("real redis conformance: HSET clears an existing hash field TTL", () => {
  const key = uniqueNs("redisconf-hset-ttl");
  redisCommand(`HSETEX ${key} EX 60 FIELDS 1 value old`);
  assert.ok(Number(redisCommand(`HPTTL ${key} FIELDS 1 value`)) > 0);

  redisHSet(key, { value: "new" });

  assert.equal(redisHGet(key, "value"), "new");
  assert.equal(Number(redisCommand(`HPTTL ${key} FIELDS 1 value`)), -1);
});
