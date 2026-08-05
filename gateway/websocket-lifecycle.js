// Process-local WebSocket lifecycle admission and reconciliation. The Gateway
// subscriber supplies hints; Redis route/session-policy state remains authoritative.

import {
  decodeBulk,
  defaultBackoff,
  RedisClient,
  RedisReplyError,
} from "shared-redis";
import { formatError } from "shared-observability";
import {
  isValidRuntimeLoadNs,
  isValidWorkerName,
} from "shared-ns-pattern";
import { GatewayRoutingUnavailableError } from "gateway-lib";
import {
  SESSION_POLICY_PRESERVE,
  SESSION_POLICY_RESTART,
  sessionPolicyKey,
  parseSessionPolicyEvent,
  parseSessionPolicyProjection,
  parseWorkerDeleteEvent,
  parseVersion,
  routesKey,
} from "shared-worker-contract";

/**
 * @typedef {{ kind: "active", version: string, mode: "preserve" | "restart", restartSequence: number }} ActiveWebSocketLifecycleSnapshot
 * @typedef {{ kind: "inactive" } | ActiveWebSocketLifecycleSnapshot} WebSocketLifecycleSnapshot
 * @typedef {{ restart: () => void, fail: () => void }} WebSocketLifecycleHandlers
 * @typedef {"restart" | "fail" | null} WebSocketLifecycleDisposition
 * @typedef {{ restartSequence: number, notify: (disposition: WebSocketLifecycleDisposition) => void }} WebSocketLifecycleSession
 * @typedef {{ ns: string, worker: string, sessions: Set<WebSocketLifecycleSession> }} WebSocketLifecycleGroup
 * @typedef {(level: string, event: string, fields?: Record<string, unknown>) => void} GatewayLogger
 * @typedef {{ increment(name: string, labels?: Record<string, string | number | boolean>, delta?: number): void }} GatewayMetrics
 */

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
const READ_WEBSOCKET_LIFECYCLE_SNAPSHOT_SCRIPT = `
return {
  redis.call("HGET", KEYS[1], ARGV[1]),
  redis.call("GET", KEYS[2])
}
`;

/** @param {unknown} err */
function isTransientRedisReplyError(err) {
  return err instanceof RedisReplyError && TRANSIENT_REDIS_REPLY_CODES.has(err.code);
}

/** @param {string} ns @param {string} worker */
function webSocketLifecycleKey(ns, worker) {
  return `${ns}:${worker}`;
}

/** @param {string} raw */
function parseWebSocketSessionPolicyEvent(raw) {
  let event;
  try {
    event = parseSessionPolicyEvent(raw);
  } catch {
    return null;
  }
  if (!isValidRuntimeLoadNs(event.ns) || !isValidWorkerName(event.worker)) return null;
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

/**
 * @param {{
 *   metrics: GatewayMetrics,
 *   log: GatewayLogger,
 *   onRedisCommand: (event: import("shared-redis").RedisCommandEvent) => void,
 * }} options
 */
export function createGatewayWebSocketLifecycleManager({ metrics, log, onRedisCommand }) {
  /** @type {Map<string, WebSocketLifecycleGroup>} */
  const groups = new Map();
  /** @type {Set<WebSocketLifecycleGroup>} */
  const reconcilePending = new Set();
  /** @type {Set<WebSocketLifecycleGroup>} */
  const reconcileRetryGroups = new Set();
  /** @type {Promise<void> | null} */
  let reconciliation = null;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let retryTimer = null;
  let retryAttempt = 0;
  let subscriberConnected = false;

  /** @param {string} redisAddr */
  function createRedis(redisAddr) {
    return new RedisClient(redisAddr, {
      commandTimeoutMs: GATEWAY_REDIS_COMMAND_TIMEOUT_MS,
      onCommand: onRedisCommand,
    });
  }

  /**
   * Read the active route and session policy projection at one Redis linearization
   * point. Gateway calls this only at WebSocket lifecycle and subscriber-recovery
   * boundaries.
   *
   * @param {RedisClient} redis
   * @param {string} ns
   * @param {string} worker
   * @returns {Promise<WebSocketLifecycleSnapshot>}
   */
  async function readSnapshot(redis, ns, worker) {
    let reply;
    try {
      reply = await redis.eval(
        READ_WEBSOCKET_LIFECYCLE_SNAPSHOT_SCRIPT,
        [routesKey(ns), sessionPolicyKey(ns, worker)],
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
        mode: SESSION_POLICY_PRESERVE,
        restartSequence: 0,
      };
    }
    let projection;
    try {
      projection = parseSessionPolicyProjection(rawProjection);
    } catch {
      throw new GatewayRoutingUnavailableError();
    }
    if (!projection || projection.version !== version) {
      throw new GatewayRoutingUnavailableError();
    }
    return { kind: "active", ...projection };
  }

  /**
   * Register one live public WebSocket against the session-policy generation observed
   * before its backend upgrade. The registration is process-local and exists
   * only for the lifetime of that connection.
   *
   * @param {string} ns
   * @param {string} worker
   * @param {{ restartSequence: number }} snapshot
   * @param {WebSocketLifecycleHandlers} handlers
   */
  function register(ns, worker, snapshot, handlers) {
    const key = webSocketLifecycleKey(ns, worker);
    let group = groups.get(key);
    if (!group) {
      group = { ns, worker, sessions: new Set() };
      groups.set(key, group);
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
      const current = groups.get(key);
      if (current) {
        current.sessions.delete(session);
        if (current.sessions.size === 0) groups.delete(key);
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
  function notify(group, session, disposition) {
    if (!group.sessions.delete(session)) return;
    const key = webSocketLifecycleKey(group.ns, group.worker);
    if (group.sessions.size === 0 && groups.get(key) === group) groups.delete(key);
    session.notify(disposition);
  }

  /** @param {{ ns: string, worker: string, restartSequence: number }} event */
  function groupForSessionPolicyEvent(event) {
    const group = groups.get(webSocketLifecycleKey(event.ns, event.worker));
    if (!group) return null;
    for (const session of group.sessions) {
      if (event.restartSequence > session.restartSequence) return group;
    }
    return null;
  }

  /**
   * @param {string} redisAddr
   * @param {WebSocketLifecycleGroup[]} requestedGroups
   * @returns {Promise<WebSocketLifecycleGroup[]>} groups deferred by transport failures
   */
  async function reconcile(redisAddr, requestedGroups) {
    if (requestedGroups.length === 0) return [];
    const redis = createRedis(redisAddr);
    let nextGroup = 0;
    /** @type {WebSocketLifecycleGroup[]} */
    const deferredGroups = [];
    let firstTransportError = null;
    async function reconcileNextGroup() {
      while (true) {
        const index = nextGroup++;
        if (index >= requestedGroups.length) return;
        const group = requestedGroups[index];
        // Bind this Redis result only to sessions that existed when the read
        // started; newer sessions may already have observed a later sequence.
        const sessions = [...group.sessions];
        let current;
        try {
          current = await readSnapshot(redis, group.ns, group.worker);
        } catch (err) {
          if (err instanceof GatewayRoutingUnavailableError) {
            for (const session of sessions) notify(group, session, "fail");
          } else {
            deferredGroups.push(group);
            firstTransportError ??= err;
          }
          continue;
        }
        if (current.kind === "inactive") {
          for (const session of sessions) notify(group, session, "restart");
          continue;
        }
        for (const session of sessions) {
          if (current.restartSequence < session.restartSequence) {
            notify(group, session, "fail");
          } else if (current.restartSequence > session.restartSequence) {
            if (current.mode === SESSION_POLICY_RESTART) {
              notify(group, session, "restart");
            } else {
              session.restartSequence = current.restartSequence;
            }
          }
        }
      }
    }
    await Promise.all(
      Array.from(
        {
          length: Math.min(
            WEBSOCKET_LIFECYCLE_RECONCILE_CONCURRENCY,
            requestedGroups.length
          ),
        },
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

  function cancelRetry() {
    if (retryTimer !== null) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    reconcileRetryGroups.clear();
    retryAttempt = 0;
  }

  /** @param {string} redisAddr */
  function deferReconcile(redisAddr) {
    if (retryTimer !== null || !subscriberConnected || reconcileRetryGroups.size === 0) return;
    const delayMs = defaultBackoff(retryAttempt++);
    retryTimer = setTimeout(() => {
      retryTimer = null;
      for (const group of reconcileRetryGroups) {
        const key = webSocketLifecycleKey(group.ns, group.worker);
        if (group.sessions.size > 0 && groups.get(key) === group) {
          reconcilePending.add(group);
        }
      }
      reconcileRetryGroups.clear();
      scheduleReconcile(redisAddr, []);
    }, delayMs);
  }

  /**
   * @param {string} redisAddr
   * @param {Iterable<WebSocketLifecycleGroup>} [requestedGroups]
   */
  function scheduleReconcile(redisAddr, requestedGroups = groups.values()) {
    for (const group of requestedGroups) {
      const key = webSocketLifecycleKey(group.ns, group.worker);
      if (group.sessions.size > 0 && groups.get(key) === group) reconcilePending.add(group);
    }
    if (reconcilePending.size === 0) {
      if (!reconciliation && reconcileRetryGroups.size === 0) cancelRetry();
      return;
    }
    if (reconciliation) return;
    /** @type {WebSocketLifecycleGroup[]} */
    let activeGroups = [];
    reconciliation = (async () => {
      while (reconcilePending.size > 0) {
        activeGroups = [...reconcilePending];
        reconcilePending.clear();
        for (const group of activeGroups) reconcileRetryGroups.delete(group);
        const deferred = await reconcile(redisAddr, activeGroups);
        for (const group of deferred) reconcileRetryGroups.add(group);
      }
    })()
      .catch((err) => {
        for (const group of activeGroups) reconcileRetryGroups.add(group);
        log("error", "websocket_lifecycle_reconcile_failed", formatError(err));
      })
      .finally(() => {
        reconciliation = null;
        if (reconcilePending.size > 0) {
          scheduleReconcile(redisAddr, []);
        } else if (reconcileRetryGroups.size > 0) {
          deferReconcile(redisAddr);
        } else {
          cancelRetry();
        }
      });
  }

  /** @param {string} redisAddr */
  function onSubscriberConnect(redisAddr) {
    subscriberConnected = true;
    cancelRetry();
    scheduleReconcile(redisAddr);
  }

  function onSubscriberDisconnect() {
    subscriberConnected = false;
    cancelRetry();
  }

  /** @param {string} redisAddr @param {string} raw */
  function onSessionPolicyEvent(redisAddr, raw) {
    const event = parseWebSocketSessionPolicyEvent(raw);
    if (!event) {
      log("warn", "websocket_session_policy_invalidation_ignored", {
        reason: "invalid_payload",
        payload: raw.slice(0, 128),
      });
      return;
    }
    const group = groupForSessionPolicyEvent(event);
    if (group) scheduleReconcile(redisAddr, [group]);
    metrics.increment("subscriber_invalidations", {
      service: "gateway",
      scope: "session_policy",
    });
    log("info", "websocket_session_policy_invalidated", {
      namespace: event.ns,
      worker: event.worker,
      version: event.version,
      restart_sequence: event.restartSequence,
      reconciliation_requested: group !== null,
    });
  }

  /** @param {string} redisAddr @param {string} raw */
  function onWorkerDeleteEvent(redisAddr, raw) {
    const event = parseWebSocketDeleteEvent(raw);
    if (!event) {
      log("warn", "worker_delete_invalidation_ignored", {
        reason: "invalid_payload",
        payload: raw.slice(0, 128),
      });
      return;
    }
    const group = groups.get(webSocketLifecycleKey(event.ns, event.worker));
    if (group) scheduleReconcile(redisAddr, [group]);
    metrics.increment("subscriber_invalidations", {
      service: "gateway",
      scope: "worker_delete",
    });
    log("info", "worker_delete_invalidated", {
      namespace: event.ns,
      worker: event.worker,
      reconciliation_requested: group !== undefined,
    });
  }

  return {
    createRedis,
    onSessionPolicyEvent,
    onSubscriberConnect,
    onSubscriberDisconnect,
    onWorkerDeleteEvent,
    readSnapshot,
    register,
  };
}
