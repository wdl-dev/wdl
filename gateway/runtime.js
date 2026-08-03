// Runtime helpers for gateway ingress. This module owns Redis clients,
// static routing-option memoization, route/pattern caches, subscriber
// invalidation, and gateway-local metrics/logs; gateway/index.js owns request
// dispatch and forwarding decisions.

import {
  decodeBulk,
  defaultBackoff,
  RedisClient,
  RedisReplyError,
  RedisSubscriber,
} from "shared-redis";
import { decodePatternProjection } from "shared-route-projection";
import {
  MetricsRegistry,
  createLogger,
  formatError,
  recordRedisCommand,
} from "shared-observability";
import {
  isValidRouteNs,
  isValidRuntimeLoadNs,
  isValidWorkerName,
  platformDomainFromEnv,
} from "shared-ns-pattern";
import {
  DECLARED_HOSTS_KEY,
  DURABLE_OBJECT_ROLLOUT_CHANNEL,
  NAMESPACES_KEY,
  PATTERNS_CHANNEL,
  ROUTES_CHANNEL,
  ROUTES_FLUSH_CHANNEL,
  WORKER_DELETE_CHANNEL,
  DURABLE_OBJECT_ROLLOUT_PRESERVE,
  DURABLE_OBJECT_ROLLOUT_RESTART,
  durableObjectRolloutKey,
  parseDurableObjectRolloutEvent,
  parseDurableObjectRolloutProjection,
  parseWorkerDeleteEvent,
  parseVersion,
  patternsKey,
  platformDomainDisabledKey,
  routesKey,
} from "shared-worker-contract";
import {
  isPatternInvalidationKey,
  normalizeRequestHost,
  sortPatterns,
} from "gateway-lib";

/**
 * @typedef {import("shared-route-projection").PatternProjection & { slot: string }} PatternEntry
 * @typedef {{ known: true, routes: Map<string, string>, cacheHit: boolean } | { known: false, routes: null, cacheHit: false }} NamespaceRouteResolution
 * @typedef {{ known: true, patterns: PatternEntry[], cacheHit: boolean } | { known: false, patterns: null, cacheHit: false }} HostPatternResolution
 * @typedef {{ epoch: number, readers: number }} InFlightReadState
 * @typedef {{ state: InFlightReadState, epoch: number }} InFlightRead
 * @typedef {{ PLATFORM_DOMAIN?: string, ADMIN_HOST?: string, [key: string]: unknown }} GatewayRoutingEnv
 * @typedef {{ platformDomain: string, normalizedAdminHost: string }} GatewayRoutingOptions
 * @typedef {{ kind: "active", version: string, mode: "preserve" | "restart", restartSequence: number }} ActiveWebSocketLifecycleSnapshot
 * @typedef {{ kind: "inactive" } | ActiveWebSocketLifecycleSnapshot} WebSocketLifecycleSnapshot
 * @typedef {{ restart: () => void, fail: () => void }} WebSocketLifecycleHandlers
 * @typedef {"restart" | "fail" | null} WebSocketLifecycleDisposition
 * @typedef {{ restartSequence: number, notify: (disposition: WebSocketLifecycleDisposition) => void }} WebSocketLifecycleSession
 * @typedef {{ ns: string, worker: string, sessions: Set<WebSocketLifecycleSession> }} WebSocketLifecycleGroup
 */

/** @type {WeakMap<GatewayRoutingEnv, GatewayRoutingOptions>} */
const routingOptionsByEnv = new WeakMap();
/** @type {Set<string> | null} */
let knownNs = null;
/** @type {Set<string> | null} */
let knownPatternHosts = null;
/** @type {Map<string, Map<string, string>>} */
const routeCache = new Map();
/** @type {Map<string, PatternEntry[]>} */
const patternCache = new Map();
let routeResetEpoch = 0;
let routeMembershipEpoch = 0;
let patternResetEpoch = 0;
let patternMembershipEpoch = 0;
/** @type {Map<string, InFlightReadState>} */
const routeReads = new Map();
/** @type {Map<string, InFlightReadState>} */
const patternReads = new Map();
/** @type {RedisSubscriber | null} */
let subscriber = null;
let subscriberConnected = 0;
let websocketProxyActiveConnections = 0;
let websocketProxyDetachedConnections = 0;
let websocketProxyBufferedMessages = 0;
/** @type {Map<string, WebSocketLifecycleGroup>} */
const webSocketLifecycleGroups = new Map();
/** @type {Set<WebSocketLifecycleGroup>} */
const webSocketLifecycleReconcilePending = new Set();
/** @type {Set<WebSocketLifecycleGroup>} */
const webSocketLifecycleReconcileRetryGroups = new Set();
/** @type {Promise<void> | null} */
let webSocketLifecycleReconcile = null;
/** @type {ReturnType<typeof setTimeout> | null} */
let webSocketLifecycleReconcileRetryTimer = null;
let webSocketLifecycleReconcileRetryAttempt = 0;
const MAX_ROUTE_CACHE_ENTRIES = 10_000;
const MAX_PATTERN_CACHE_ENTRIES = 10_000;
const MAX_ROUTING_SNAPSHOT_ATTEMPTS = 5;
const WEBSOCKET_LIFECYCLE_RECONCILE_CONCURRENCY = 8;
const GATEWAY_REDIS_COMMAND_TIMEOUT_MS = 2_000;
const TRANSIENT_REDIS_REPLY_CODES = new Set([
  "BUSY",
  "CLUSTERDOWN",
  "LOADING",
  "MASTERDOWN",
  "READONLY",
  "TRYAGAIN",
]);
const utf8Decoder = new TextDecoder();
const READ_WEBSOCKET_LIFECYCLE_SNAPSHOT_SCRIPT = `
return {
  redis.call("HGET", KEYS[1], ARGV[1]),
  redis.call("GET", KEYS[2])
}
`;

export const metrics = new MetricsRegistry();
export const log = createLogger("gateway");

/**
 * @param {GatewayRoutingEnv} env
 * @returns {GatewayRoutingOptions}
 */
export function gatewayRoutingOptionsFromEnv(env) {
  let options = routingOptionsByEnv.get(env);
  if (!options) {
    options = {
      platformDomain: platformDomainFromEnv(env),
      normalizedAdminHost: normalizeRequestHost(env.ADMIN_HOST || "").toLowerCase(),
    };
    routingOptionsByEnv.set(env, options);
  }
  return options;
}

export class GatewayRoutingUnavailableError extends Error {
  constructor() {
    super("Gateway routing state changed throughout the bounded lookup");
    this.name = "GatewayRoutingUnavailableError";
    this.status = 503;
    this.code = "gateway_routing_unavailable";
    this.publicMessage = "Gateway routing temporarily unavailable";
  }
}

function clearRouteState() {
  routeResetEpoch += 1;
  routeMembershipEpoch += 1;
  routeCache.clear();
  knownNs = null;
}

function clearPatternState() {
  patternResetEpoch += 1;
  patternMembershipEpoch += 1;
  patternCache.clear();
  knownPatternHosts = null;
}

/**
 * @param {Map<string, InFlightReadState>} reads
 * @param {string} key
 * @returns {InFlightRead}
 */
function beginKeyRead(reads, key) {
  let state = reads.get(key);
  if (!state) {
    state = { epoch: 0, readers: 0 };
    reads.set(key, state);
  }
  state.readers += 1;
  return { state, epoch: state.epoch };
}

/**
 * @param {Map<string, InFlightReadState>} reads
 * @param {string} key
 * @param {InFlightReadState} state
 */
function endKeyRead(reads, key, state) {
  state.readers -= 1;
  if (state.readers === 0) reads.delete(key);
}

/** @param {Map<string, InFlightReadState>} reads @param {string} key */
function invalidateKeyRead(reads, key) {
  const state = reads.get(key);
  if (state) state.epoch += 1;
}

/** @param {import("shared-redis").RedisCommandEvent} event */
function onRedisCommand(event) {
  recordRedisCommand({ metrics, log, service: "gateway", event });
}

/** @param {string} redisAddr */
export function createGatewayRedis(redisAddr) {
  return new RedisClient(redisAddr, { onCommand: onRedisCommand });
}

/** @param {string} redisAddr */
export function createGatewayLifecycleRedis(redisAddr) {
  return new RedisClient(redisAddr, {
    commandTimeoutMs: GATEWAY_REDIS_COMMAND_TIMEOUT_MS,
    onCommand: onRedisCommand,
  });
}

/** @param {unknown} err */
function isTransientRedisReplyError(err) {
  return err instanceof RedisReplyError && TRANSIENT_REDIS_REPLY_CODES.has(err.code);
}

/**
 * Read the active route and DO rollout projection at one Redis linearization
 * point. Gateway calls this only at WebSocket lifecycle and subscriber-recovery
 * boundaries.
 *
 * @param {RedisClient} redis
 * @param {string} ns
 * @param {string} worker
 * @returns {Promise<WebSocketLifecycleSnapshot>}
 */
export async function readWebSocketLifecycleSnapshot(redis, ns, worker) {
  let reply;
  try {
    reply = await redis.eval(
      READ_WEBSOCKET_LIFECYCLE_SNAPSHOT_SCRIPT,
      [routesKey(ns), durableObjectRolloutKey(ns, worker)],
      [worker]
    );
  } catch (err) {
    if (err instanceof RedisReplyError && !isTransientRedisReplyError(err)) {
      throw new GatewayRoutingUnavailableError();
    }
    throw err;
  }
  if (!Array.isArray(reply) || reply.length !== 2) {
    throw new GatewayRoutingUnavailableError();
  }
  const version = decodeBulk(reply[0]);
  const rawProjection = decodeBulk(reply[1]);
  if (version == null && rawProjection == null) return { kind: "inactive" };
  if (version == null || parseVersion(version) == null) {
    throw new GatewayRoutingUnavailableError();
  }
  if (rawProjection == null) {
    return {
      kind: "active",
      version,
      mode: DURABLE_OBJECT_ROLLOUT_PRESERVE,
      restartSequence: 0,
    };
  }
  let projection;
  try {
    projection = parseDurableObjectRolloutProjection(rawProjection);
  } catch {
    throw new GatewayRoutingUnavailableError();
  }
  if (!projection || projection.version !== version) {
    throw new GatewayRoutingUnavailableError();
  }
  return { kind: "active", ...projection };
}

/** @param {string} ns @param {string} worker */
function webSocketLifecycleKey(ns, worker) {
  return `${ns}:${worker}`;
}

/**
 * Register one live public WebSocket against the rollout generation observed
 * before its backend upgrade. The registration is process-local and exists
 * only for the lifetime of that connection.
 *
 * @param {string} ns
 * @param {string} worker
 * @param {{ restartSequence: number }} snapshot
 * @param {WebSocketLifecycleHandlers} handlers
 */
export function registerGatewayWebSocketLifecycle(ns, worker, snapshot, handlers) {
  const key = webSocketLifecycleKey(ns, worker);
  let group = webSocketLifecycleGroups.get(key);
  if (!group) {
    group = { ns, worker, sessions: new Set() };
    webSocketLifecycleGroups.set(key, group);
  }
  const { promise, resolve } = Promise.withResolvers();
  // Pub/sub runs in the subscriber request's IoContext. Settling this promise
  // schedules its continuation back on the WebSocket request's IoContext;
  // Gateway's compatibility date enables workerd's cross-request settlement.
  void promise.then((disposition) => {
    if (disposition === "restart") handlers.restart();
    if (disposition === "fail") handlers.fail();
  }).catch((err) => {
    log("error", "websocket_lifecycle_signal_failed", formatError(err));
  });
  const session = { restartSequence: snapshot.restartSequence, notify: resolve };
  group.sessions.add(session);
  return () => {
    const current = webSocketLifecycleGroups.get(key);
    if (current) {
      current.sessions.delete(session);
      if (current.sessions.size === 0) webSocketLifecycleGroups.delete(key);
    }
    resolve(null);
  };
}

/**
 * Remove before notifying so repeated pub/sub or reconciliation passes cannot
 * deliver the same lifecycle transition twice.
 *
 * @param {WebSocketLifecycleGroup} group
 * @param {WebSocketLifecycleSession} session
 * @param {Exclude<WebSocketLifecycleDisposition, null>} disposition
 */
function notifyWebSocketLifecycleSession(group, session, disposition) {
  if (!group.sessions.delete(session)) return;
  const key = webSocketLifecycleKey(group.ns, group.worker);
  if (group.sessions.size === 0 && webSocketLifecycleGroups.get(key) === group) {
    webSocketLifecycleGroups.delete(key);
  }
  session.notify(disposition);
}

/** @param {string} raw */
function parseWebSocketRolloutEvent(raw) {
  let event;
  try {
    event = parseDurableObjectRolloutEvent(raw);
  } catch {
    return null;
  }
  if (
    !isValidRuntimeLoadNs(event.ns) ||
    !isValidWorkerName(event.worker)
  ) return null;
  return event;
}

/** @param {string} raw */
function parseWebSocketDeleteEvent(raw) {
  let event;
  try {
    event = parseWorkerDeleteEvent(raw);
  } catch {
    return null;
  }
  if (!isValidRuntimeLoadNs(event.ns) || !isValidWorkerName(event.worker)) return null;
  return event;
}

/** @param {{ ns: string, worker: string, restartSequence: number }} event */
function webSocketLifecycleGroupForRolloutEvent(event) {
  const group = webSocketLifecycleGroups.get(webSocketLifecycleKey(event.ns, event.worker));
  if (!group) return null;
  for (const session of group.sessions) {
    if (event.restartSequence > session.restartSequence) return group;
  }
  return null;
}

/**
 * @param {string} redisAddr
 * @param {WebSocketLifecycleGroup[]} groups
 * @returns {Promise<WebSocketLifecycleGroup[]>} groups deferred by transport failures
 */
async function reconcileGatewayWebSocketLifecycles(redisAddr, groups) {
  if (groups.length === 0) return [];
  const redis = createGatewayLifecycleRedis(redisAddr);
  let nextGroup = 0;
  /** @type {WebSocketLifecycleGroup[]} */
  const deferredGroups = [];
  let firstTransportError = null;
  async function reconcileNextGroup() {
    while (true) {
      const index = nextGroup++;
      if (index >= groups.length) return;
      const group = groups[index];
      // Bind this Redis result only to sessions that existed when the read
      // started; newer sessions may already have observed a later sequence.
      const sessions = [...group.sessions];
      let current;
      try {
        current = await readWebSocketLifecycleSnapshot(redis, group.ns, group.worker);
      } catch (err) {
        if (err instanceof GatewayRoutingUnavailableError) {
          for (const session of sessions) {
            notifyWebSocketLifecycleSession(group, session, "fail");
          }
        } else {
          deferredGroups.push(group);
          firstTransportError ??= err;
        }
        continue;
      }
      if (current.kind === "inactive") {
        for (const session of sessions) {
          notifyWebSocketLifecycleSession(group, session, "restart");
        }
        continue;
      }
      for (const session of sessions) {
        if (current.restartSequence < session.restartSequence) {
          notifyWebSocketLifecycleSession(group, session, "fail");
        } else if (current.restartSequence > session.restartSequence) {
          if (current.mode === DURABLE_OBJECT_ROLLOUT_RESTART) {
            notifyWebSocketLifecycleSession(group, session, "restart");
          } else {
            session.restartSequence = current.restartSequence;
          }
        }
      }
    }
  }
  await Promise.all(
    Array.from(
      { length: Math.min(WEBSOCKET_LIFECYCLE_RECONCILE_CONCURRENCY, groups.length) },
      () => reconcileNextGroup()
    )
  );
  if (deferredGroups.length > 0) {
    log("warn", "websocket_lifecycle_reconcile_deferred", {
      deferred_groups: deferredGroups.length,
      ...(firstTransportError == null ? {} : formatError(firstTransportError)),
    });
  }
  return deferredGroups;
}

function cancelWebSocketLifecycleReconcileRetry() {
  if (webSocketLifecycleReconcileRetryTimer !== null) {
    clearTimeout(webSocketLifecycleReconcileRetryTimer);
    webSocketLifecycleReconcileRetryTimer = null;
  }
  webSocketLifecycleReconcileRetryGroups.clear();
  webSocketLifecycleReconcileRetryAttempt = 0;
}

/** @param {string} redisAddr */
function deferWebSocketLifecycleReconcile(redisAddr) {
  if (
    webSocketLifecycleReconcileRetryTimer !== null ||
    subscriberConnected === 0 ||
    webSocketLifecycleReconcileRetryGroups.size === 0
  ) return;
  const delayMs = defaultBackoff(webSocketLifecycleReconcileRetryAttempt++);
  webSocketLifecycleReconcileRetryTimer = setTimeout(() => {
    webSocketLifecycleReconcileRetryTimer = null;
    for (const group of webSocketLifecycleReconcileRetryGroups) {
      const key = webSocketLifecycleKey(group.ns, group.worker);
      if (group.sessions.size > 0 && webSocketLifecycleGroups.get(key) === group) {
        webSocketLifecycleReconcilePending.add(group);
      }
    }
    webSocketLifecycleReconcileRetryGroups.clear();
    scheduleWebSocketLifecycleReconcile(redisAddr, []);
  }, delayMs);
}

/**
 * @param {string} redisAddr
 * @param {Iterable<WebSocketLifecycleGroup>} [groups]
 */
function scheduleWebSocketLifecycleReconcile(
  redisAddr,
  groups = webSocketLifecycleGroups.values()
) {
  for (const group of groups) {
    const key = webSocketLifecycleKey(group.ns, group.worker);
    if (group.sessions.size > 0 && webSocketLifecycleGroups.get(key) === group) {
      webSocketLifecycleReconcilePending.add(group);
    }
  }
  if (webSocketLifecycleReconcilePending.size === 0) {
    if (!webSocketLifecycleReconcile && webSocketLifecycleReconcileRetryGroups.size === 0) {
      cancelWebSocketLifecycleReconcileRetry();
    }
    return;
  }
  if (webSocketLifecycleReconcile) return;
  /** @type {WebSocketLifecycleGroup[]} */
  let activeGroups = [];
  webSocketLifecycleReconcile = (async () => {
    while (webSocketLifecycleReconcilePending.size > 0) {
      activeGroups = [...webSocketLifecycleReconcilePending];
      webSocketLifecycleReconcilePending.clear();
      for (const group of activeGroups) webSocketLifecycleReconcileRetryGroups.delete(group);
      const deferred = await reconcileGatewayWebSocketLifecycles(redisAddr, activeGroups);
      for (const group of deferred) webSocketLifecycleReconcileRetryGroups.add(group);
    }
  })()
    .catch((err) => {
      for (const group of activeGroups) webSocketLifecycleReconcileRetryGroups.add(group);
      log("error", "websocket_lifecycle_reconcile_failed", formatError(err));
    })
    .finally(() => {
      webSocketLifecycleReconcile = null;
      if (webSocketLifecycleReconcilePending.size > 0) {
        scheduleWebSocketLifecycleReconcile(redisAddr, []);
      } else if (webSocketLifecycleReconcileRetryGroups.size > 0) {
        deferWebSocketLifecycleReconcile(redisAddr);
      } else {
        cancelWebSocketLifecycleReconcileRetry();
      }
    });
}

/**
 * Build the namespace subdomain route map, subtracting workers that opted out
 * of platform-domain routing. Reading both keys in one pipeline is transport
 * batching, not atomicity: an interleaved promote bumps the membership or
 * namespace generation, and the caller's post-read recheck discards that view.
 *
 * @param {string} ns
 * @param {Record<string, unknown>} entries
 * @param {Set<string>} disabled
 */
function cacheNsRoutes(ns, entries, disabled) {
  const map = new Map(
    Object.entries(entries).flatMap(([k, v]) =>
      typeof v === "string" && !disabled.has(k) ? [[k, v]] : []
    )
  );
  setBoundedCacheEntry(routeCache, ns, map, MAX_ROUTE_CACHE_ENTRIES);
  return map;
}

/**
 * @param {string} host
 * @param {string} requestId
 * @param {Record<string, unknown>} entries
 */
function cacheHostPatterns(host, requestId, entries) {
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
 * @template K, V
 * @param {Map<K, V>} cache
 * @param {K} key
 * @returns {V | null}
 */
function getCachedEntry(cache, key) {
  const value = cache.get(key);
  if (value === undefined) return null;
  cache.delete(key);
  cache.set(key, value);
  return value;
}

/**
 * @param {RedisClient} redis
 * @param {string} ns
 * @returns {Promise<NamespaceRouteResolution>}
 */
export async function resolveNamespaceRoutes(redis, ns) {
  for (let attempt = 0; attempt < MAX_ROUTING_SNAPSHOT_ATTEMPTS; attempt += 1) {
    const resetEpoch = routeResetEpoch;
    if (knownNs === null) {
      const membershipEpoch = routeMembershipEpoch;
      const snapshot = await redis.sMembersHGetAllAndSMembers(
        NAMESPACES_KEY,
        routesKey(ns),
        platformDomainDisabledKey(ns)
      );
      if (
        resetEpoch !== routeResetEpoch ||
        membershipEpoch !== routeMembershipEpoch
      ) continue;
      knownNs = new Set(snapshot.namespaces);
      if (!knownNs.has(ns)) return { known: false, routes: null, cacheHit: false };
      return {
        known: true,
        routes: cacheNsRoutes(ns, snapshot.hash, new Set(snapshot.members)),
        cacheHit: false,
      };
    }
    if (!knownNs.has(ns)) return { known: false, routes: null, cacheHit: false };

    const cached = getCachedEntry(routeCache, ns);
    if (cached) return { known: true, routes: cached, cacheHit: true };
    const read = beginKeyRead(routeReads, ns);
    try {
      const snapshot = await redis.hGetAllAndSMembers(
        routesKey(ns),
        platformDomainDisabledKey(ns)
      );
      if (
        resetEpoch !== routeResetEpoch ||
        read.epoch !== read.state.epoch
      ) continue;
      return {
        known: true,
        routes: cacheNsRoutes(ns, snapshot.hash, new Set(snapshot.members)),
        cacheHit: false,
      };
    } finally {
      endKeyRead(routeReads, ns, read.state);
    }
  }
  throw new GatewayRoutingUnavailableError();
}

/**
 * @param {RedisClient} redis
 * @param {string} host
 * @param {string} requestId
 * @returns {Promise<HostPatternResolution>}
 */
export async function resolveHostPatterns(redis, host, requestId) {
  for (let attempt = 0; attempt < MAX_ROUTING_SNAPSHOT_ATTEMPTS; attempt += 1) {
    const resetEpoch = patternResetEpoch;
    if (knownPatternHosts === null) {
      const membershipEpoch = patternMembershipEpoch;
      const snapshot = await redis.sMembersAndHGetAll(DECLARED_HOSTS_KEY, patternsKey(host));
      if (
        resetEpoch !== patternResetEpoch ||
        membershipEpoch !== patternMembershipEpoch
      ) continue;
      knownPatternHosts = new Set(snapshot.members);
      if (!knownPatternHosts.has(host)) {
        return { known: false, patterns: null, cacheHit: false };
      }
      return {
        known: true,
        patterns: cacheHostPatterns(host, requestId, snapshot.hash),
        cacheHit: false,
      };
    }
    if (!knownPatternHosts.has(host)) {
      return { known: false, patterns: null, cacheHit: false };
    }

    const cached = getCachedEntry(patternCache, host);
    if (cached) return { known: true, patterns: cached, cacheHit: true };
    const read = beginKeyRead(patternReads, host);
    try {
      const entries = await redis.hGetAll(patternsKey(host));
      if (
        resetEpoch !== patternResetEpoch ||
        read.epoch !== read.state.epoch
      ) continue;
      return {
        known: true,
        patterns: cacheHostPatterns(host, requestId, entries),
        cacheHit: false,
      };
    } finally {
      endKeyRead(patternReads, host, read.state);
    }
  }
  throw new GatewayRoutingUnavailableError();
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
    [
      ROUTES_CHANNEL,
      ROUTES_FLUSH_CHANNEL,
      PATTERNS_CHANNEL,
      DURABLE_OBJECT_ROLLOUT_CHANNEL,
      WORKER_DELETE_CHANNEL,
    ],
    {
      onConnect: () => {
        subscriberConnected = 1;
        // Clear on first connect too: a request can warm caches before the
        // SUBSCRIBE ack, and that cache would otherwise miss an early PUBLISH.
        clearRouteState();
        clearPatternState();
        metrics.increment("subscriber_connects", { service: "gateway" });
        log("info", "subscriber_connected", {});
        cancelWebSocketLifecycleReconcileRetry();
        scheduleWebSocketLifecycleReconcile(redisAddr);
      },
      onDisconnect: () => {
        if (subscriberConnected === 0) return;
        subscriberConnected = 0;
        cancelWebSocketLifecycleReconcileRetry();
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
        if (channel === DURABLE_OBJECT_ROLLOUT_CHANNEL) {
          const event = parseWebSocketRolloutEvent(value);
          if (!event) {
            log("warn", "websocket_rollout_invalidation_ignored", {
              reason: "invalid_payload",
              payload: value.slice(0, 128),
            });
            return;
          }
          const group = webSocketLifecycleGroupForRolloutEvent(event);
          const reconciliationRequested = group !== null;
          if (group) scheduleWebSocketLifecycleReconcile(redisAddr, [group]);
          metrics.increment("subscriber_invalidations", {
            service: "gateway",
            scope: "do_rollout",
          });
          log("info", "websocket_rollout_invalidated", {
            namespace: event.ns,
            worker: event.worker,
            version: event.version,
            restart_sequence: event.restartSequence,
            reconciliation_requested: reconciliationRequested,
          });
          return;
        }
        if (channel === WORKER_DELETE_CHANNEL) {
          const event = parseWebSocketDeleteEvent(value);
          if (!event) {
            log("warn", "worker_delete_invalidation_ignored", {
              reason: "invalid_payload",
              payload: value.slice(0, 128),
            });
            return;
          }
          const group = webSocketLifecycleGroups.get(
            webSocketLifecycleKey(event.ns, event.worker)
          );
          if (group) scheduleWebSocketLifecycleReconcile(redisAddr, [group]);
          metrics.increment("subscriber_invalidations", {
            service: "gateway",
            scope: "worker_delete",
          });
          log("info", "worker_delete_invalidated", {
            namespace: event.ns,
            worker: event.worker,
            reconciliation_requested: group !== undefined,
          });
          return;
        }
        if (channel === PATTERNS_CHANNEL) {
          if (value === "*") {
            clearPatternState();
          } else if (isPatternInvalidationKey(value)) {
            patternMembershipEpoch += 1;
            invalidateKeyRead(patternReads, value);
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
        routeMembershipEpoch += 1;
        invalidateKeyRead(routeReads, value);
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

// WS upgrades return 101 once the initial upstream handshake completes; the
// local proxy then runs asynchronously, so this metric covers handshake only.
// Full WS session lifetime is reported by recordGatewayWebSocketSessionLifetime.
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
