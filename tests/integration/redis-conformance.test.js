import { test } from "node:test";
import { redisConformanceCases } from "../helpers/redis-conformance-cases.js";
import { setupIntegrationSuite, uniqueNs } from "./helpers/index.js";
import {
  redisCopy,
  redisDel,
  redisExists,
  redisExpireAt,
  redisExpireTime,
  redisHDel,
  redisHGet,
  redisHGetAll,
  redisHMGet,
  redisHSet,
  redisSAdd,
  redisSet,
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
    hSet: async (key, fields) => redisHSet(key, fields),
    hGet: async (key, field) => redisHGet(key, field),
    hMGet: async (key, fields) => redisHMGet(key, fields),
    hGetAll: async (key) => redisHGetAll(key),
    hDel: async (key, ...fields) => redisHDel(key, fields),
    set: async (key, value) => redisSet(key, value),
    sAdd: async (key, member) => redisSAdd(key, member),
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
