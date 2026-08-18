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
  decodeBulk,
  redisDbFromEnv,
} from "shared-redis-resp";
export { RedisClient } from "shared-redis-command-client";
export { RedisSession, RedisMulti } from "shared-redis-session";
export { RedisSubscriber, defaultBackoff } from "shared-redis-subscriber";

/**
 * @typedef {import("shared-redis-resp").RedisCommandEvent} RedisCommandEvent
 * @typedef {import("shared-redis-resp").RedisSetOptions} RedisSetOptions
 */
