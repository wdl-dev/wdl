// Public Redis surface for gateway + runtime + control over cloudflare:sockets.
//
// Keep this module as the stable import target. The implementation is split
// so each Redis shape owns its own lifecycle:
// - `RedisClient`     per-call sockets, byte-preserving lowercase reads.
// - `RedisSession`    one socket across WATCH -> reads -> MULTI/EXEC.
// - `RedisSubscriber` long-lived SUBSCRIBE loop with reconnect.

export {
  RedisReplyError,
  WatchError,
  RespReader,
  decodeBulk,
  decodeRedisTimeMs,
  encodeCommand,
  normalizeRedisDb,
  redisDbFromEnv,
} from "shared-redis-resp";
export { RedisClient, RedisCommandTimeoutError } from "shared-redis-command-client";
export { RedisSession, RedisMulti } from "shared-redis-session";
export { RedisSubscriber, defaultBackoff } from "shared-redis-subscriber";

/**
 * @typedef {import("shared-redis-resp").RedisArg} RedisArg
 * @typedef {import("shared-redis-resp").RedisCommand} RedisCommand
 * @typedef {import("shared-redis-resp").RedisReply} RedisReply
 * @typedef {import("shared-redis-resp").RedisHSetArg} RedisHSetArg
 * @typedef {import("shared-redis-resp").RedisCommandEvent} RedisCommandEvent
 * @typedef {import("shared-redis-resp").RedisSocket} RedisSocket
 * @typedef {import("shared-redis-resp").RedisSocketFactory} RedisSocketFactory
 * @typedef {import("shared-redis-resp").RedisClientOptions} RedisClientOptions
 * @typedef {import("shared-redis-resp").RedisSetOptions} RedisSetOptions
 * @typedef {import("shared-redis-resp").RedisXAddOptions} RedisXAddOptions
 * @typedef {import("shared-redis-resp").RedisZRangeByScoreOptions} RedisZRangeByScoreOptions
 * @typedef {import("shared-redis-resp").RedisCopyOptions} RedisCopyOptions
 * @typedef {import("shared-redis-resp").RedisSubscriberOptions} RedisSubscriberOptions
 */
