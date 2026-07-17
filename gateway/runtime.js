// Runtime helpers for gateway ingress. This module owns Redis clients,
// route/pattern caches, subscriber invalidation, and gateway-local metrics/logs;
// gateway/index.js owns request dispatch and forwarding decisions.

import { RedisClient, RedisSubscriber } from "shared-redis";
import { decodePatternProjection } from "shared-route-projection";
import {
  MetricsRegistry,
  createLogger,
  formatError,
  recordRedisCommand,
} from "shared-observability";
import { isValidRouteNs } from "shared-ns-pattern";
import {
  DECLARED_HOSTS_KEY,
  NAMESPACES_KEY,
  PATTERNS_CHANNEL,
  ROUTES_CHANNEL,
  ROUTES_FLUSH_CHANNEL,
  patternsKey,
  routesKey,
} from "shared-worker-contract";
import { isCanonicalPatternHost, sortPatterns } from "gateway-lib";

/** @type {Set<string> | null} */
let knownNs = null;
/** @type {Set<string> | null} */
let knownPatternHosts = null;
const routeCache = new Map();
const patternCache = new Map();
/** @type {RedisSubscriber | null} */
let subscriber = null;
let subscriberConnected = 0;
let websocketProxyActiveConnections = 0;
let websocketProxyDetachedConnections = 0;
let websocketProxyBufferedMessages = 0;
const MAX_ROUTE_CACHE_ENTRIES = 10_000;
const MAX_PATTERN_CACHE_ENTRIES = 10_000;
const utf8Decoder = new TextDecoder();

export const metrics = new MetricsRegistry();
export const log = createLogger("gateway");

function clearRouteState() {
  routeCache.clear();
  knownNs = null;
}

function clearPatternState() {
  patternCache.clear();
  knownPatternHosts = null;
}

/** @param {import("shared-redis").RedisCommandEvent} event */
function onRedisCommand(event) {
  recordRedisCommand({ metrics, log, service: "gateway", event });
}

/** @param {string} redisAddr */
export function createGatewayRedis(redisAddr) {
  return new RedisClient(redisAddr, { onCommand: onRedisCommand });
}

/** @param {RedisClient} redis */
export async function ensureKnownNs(redis) {
  if (knownNs === null) knownNs = new Set(await redis.sMembers(NAMESPACES_KEY));
  return knownNs;
}

/** @param {RedisClient} redis */
export async function ensureKnownPatternHosts(redis) {
  if (knownPatternHosts === null) knownPatternHosts = new Set(await redis.sMembers(DECLARED_HOSTS_KEY));
  return knownPatternHosts;
}

/** @param {string} ns */
export function getCachedNsRoutes(ns) {
  const value = routeCache.get(ns);
  if (value) {
    routeCache.delete(ns);
    routeCache.set(ns, value);
  }
  return value;
}

/**
 * @param {RedisClient} redis
 * @param {string} ns
 */
export async function loadNsRoutes(redis, ns) {
  const entries = await redis.hGetAll(routesKey(ns));
  const map = new Map(
    Object.entries(entries).flatMap(([k, v]) => typeof v === "string" ? [[k, v]] : [])
  );
  setBoundedCacheEntry(routeCache, ns, map, MAX_ROUTE_CACHE_ENTRIES);
  return map;
}

/** @param {string} host */
export function getCachedPatterns(host) {
  const value = patternCache.get(host);
  if (value) {
    patternCache.delete(host);
    patternCache.set(host, value);
  }
  return value;
}

/**
 * @param {RedisClient} redis
 * @param {string} host
 * @param {string} requestId
 */
export async function loadPatternsForHost(redis, host, requestId) {
  const entries = await redis.hGetAll(patternsKey(host));
  const decodedEntries = Object.fromEntries(
    Object.entries(entries).flatMap(([k, v]) =>
      typeof v === "string" ? [[k, decodePatternProjection(v)]] : []
    )
  );
  const { sorted, errors } = sortPatterns(decodedEntries, isValidRouteNs);
  if (errors.length) {
    for (const e of errors) {
      metrics.increment("pattern_parse_errors", { service: "gateway", reason: e.reason });
    }
    log("warn", "pattern_parse_errors", {
      request_id: requestId,
      host,
      dropped: errors.length,
      sample: errors.slice(0, 5),
    });
  }
  setBoundedCacheEntry(patternCache, host, sorted, MAX_PATTERN_CACHE_ENTRIES);
  return sorted;
}

/**
 * @param {Map<unknown, unknown>} cache
 * @param {unknown} key
 * @param {unknown} value
 * @param {number} maxEntries
 */
function setBoundedCacheEntry(cache, key, value, maxEntries) {
  // Map preserves insertion order; delete+set on hits and writes gives this
  // tiny bounded cache LRU semantics without a second recency structure.
  if (cache.has(key)) cache.delete(key);
  cache.set(key, value);
  if (cache.size > maxEntries) {
    cache.delete(cache.keys().next().value);
  }
}

/** @param {string} redisAddr */
export function ensureGatewaySubscriber(redisAddr) {
  if (subscriber) return null;
  subscriber = new RedisSubscriber(
    redisAddr,
    [ROUTES_CHANNEL, ROUTES_FLUSH_CHANNEL, PATTERNS_CHANNEL],
    {
      onConnect: () => {
        subscriberConnected = 1;
        // Clear on first connect too: a request can warm caches before the
        // SUBSCRIBE ack, and that cache would otherwise miss an early PUBLISH.
        clearRouteState();
        clearPatternState();
        metrics.increment("subscriber_connects", { service: "gateway" });
        log("info", "subscriber_connected", {});
      },
      onDisconnect: () => {
        if (subscriberConnected === 0) return;
        subscriberConnected = 0;
        clearRouteState();
        clearPatternState();
        metrics.increment("subscriber_disconnects", { service: "gateway" });
        log("warn", "subscriber_disconnected", {});
      },
      onError: (err) => {
        log("error", "subscriber_error", formatError(err));
      },
      onMessage: (channel, payload) => {
        const value = utf8Decoder.decode(payload);
        if (channel === PATTERNS_CHANNEL) {
          if (value === "*") {
            clearPatternState();
          } else if (isCanonicalPatternHost(value)) {
            patternCache.delete(value);
            knownPatternHosts = null;
          } else {
            log("warn", "patterns_invalidation_ignored", {
              reason: "invalid_host",
              payload: value.slice(0, 128),
            });
            return;
          }
          metrics.increment("subscriber_invalidations", {
            service: "gateway",
            scope: "pattern",
          });
          log("info", "patterns_invalidated", { host: value });
          return;
        }
        if (channel === ROUTES_FLUSH_CHANNEL) {
          clearRouteState();
          metrics.increment("subscriber_invalidations", { service: "gateway", scope: "all" });
          log("info", "routes_invalidated_all", {});
          return;
        }
        // The route channel owns routeCache + knownNs only; the patterns cache
        // is invalidated exclusively via the pattern channel so channel
        // semantics stay orthogonal.
        if (!isValidRouteNs(value)) {
          log("warn", "routes_invalidation_ignored", {
            reason: "invalid_namespace",
            payload: value.slice(0, 128),
          });
          return;
        }
        routeCache.delete(value);
        // A brand-new namespace must pass the knownNs gate before the next full
        // resync; promote publishes this ns after making it active.
        if (knownNs) knownNs.add(value);
        metrics.increment("subscriber_invalidations", { service: "gateway", scope: "namespace" });
        log("info", "routes_invalidated", { namespace: value });
      },
    }
  );
  return subscriber.start();
}

export function gatewayHealthSnapshot() {
  return {
    subscriber_connected: subscriberConnected === 1,
    namespace_cache_size: knownNs ? knownNs.size : 0,
    pattern_host_cache_size: knownPatternHosts ? knownPatternHosts.size : 0,
    route_cache_size: routeCache.size,
    pattern_cache_size: patternCache.size,
  };
}

export function prepareGatewayMetrics() {
  metrics.setGauge("subscriber_connected", { service: "gateway" }, subscriberConnected);
  metrics.setGauge("websocket_proxy_connections", {
    service: "gateway",
    state: "active",
  }, websocketProxyActiveConnections);
  metrics.setGauge("websocket_proxy_connections", {
    service: "gateway",
    state: "detached",
  }, websocketProxyDetachedConnections);
  metrics.setGauge("websocket_proxy_buffered_messages", {
    service: "gateway",
  }, websocketProxyBufferedMessages);
}

/**
 * @param {string} stage
 * @param {string} outcome
 */
export function recordRoutingLookup(stage, outcome) {
  metrics.increment("routing_lookups", { service: "gateway", stage, outcome });
}

/**
 * @param {string} outcome
 * @param {number} comparisons
 */
export function recordPatternMatchComparisons(outcome, comparisons) {
  metrics.observe("pattern_match_comparisons", { service: "gateway", outcome }, comparisons);
}

// WS upgrades return 101 from the holder DO once the initial upstream
// handshake completes; the proxy then runs async on the DO actor, so this
// metric covers handshake only. Full WS session lifetime is reported by
// recordGatewayWebSocketSessionLifetime.
/**
 * @param {number} durationMs
 * @param {string} binding
 * @param {string} outcome
 */
export function recordRuntimeForwardDuration(durationMs, binding, outcome) {
  metrics.observe("runtime_forward_duration_ms", {
    service: "gateway",
    binding,
    outcome,
  }, durationMs);
}

/** @param {Response | null | undefined} response */
export function runtimeForwardOutcome(response) {
  // WebSocket upgrade responses are 101, which makes Response.ok false. Treat
  // all non-error HTTP statuses as successful forwards so upgrade traffic does
  // not inflate gateway error metrics.
  return response && response.status < 400 ? "ok" : "error";
}

/** @param {string} outcome */
export function recordGatewayWebSocketProxy(outcome) {
  metrics.increment("websocket_proxies", { service: "gateway", outcome });
}

/**
 * @param {number} durationMs
 * @param {string} outcome
 */
export function recordGatewayWebSocketSessionLifetime(durationMs, outcome) {
  metrics.observe("websocket_session_lifetime_ms", {
    service: "gateway",
    outcome,
  }, durationMs);
}

/**
 * @param {"active" | "detached"} state
 * @param {number} delta
 */
export function adjustGatewayWebSocketProxyConnections(state, delta) {
  if (state === "active") {
    websocketProxyActiveConnections = Math.max(0, websocketProxyActiveConnections + delta);
    metrics.setGauge("websocket_proxy_connections", {
      service: "gateway",
      state: "active",
    }, websocketProxyActiveConnections);
    return;
  }
  if (state === "detached") {
    websocketProxyDetachedConnections = Math.max(0, websocketProxyDetachedConnections + delta);
    metrics.setGauge("websocket_proxy_connections", {
      service: "gateway",
      state: "detached",
    }, websocketProxyDetachedConnections);
  }
}

/** @param {number} delta */
export function adjustGatewayWebSocketProxyBufferedMessages(delta) {
  websocketProxyBufferedMessages = Math.max(0, websocketProxyBufferedMessages + delta);
  metrics.setGauge("websocket_proxy_buffered_messages", {
    service: "gateway",
  }, websocketProxyBufferedMessages);
}
