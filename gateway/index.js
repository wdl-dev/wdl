// Gateway worker: ingress routing + route resolution. Admin-host requests
// short-circuit to control; public data traffic falls into two host-shape
// branches:
//   <ns>.<PLATFORM_DOMAIN>/<worker>/<path>  -> routes:<ns>
//   any other host                          -> patterns:<host>
// Data branches forward to runtime with x-worker-id, x-worker-prefix, and
// x-request-id. Control is infrastructure and receives only x-request-id.

import {
  internalErrorResponse,
  jsonError,
  jsonResponse,
  prometheusResponse,
} from "shared-respond";
import {
  createLogLevelBinder,
} from "shared-observability";
import {
  createHttpRequestScope,
} from "shared-request-scope";
import {
  deleteGatewayInternalHeaders,
  isWebSocketUpgrade,
  normalizeRequestHost,
} from "gateway-lib";
import {
  resolveGatewayDispatch,
} from "gateway-dispatch";
import {
  GatewayRoutingUnavailableError,
  adjustGatewayWebSocketProxyBufferedMessages,
  adjustGatewayWebSocketProxyConnections,
  createGatewayLifecycleRedis,
  createGatewayRedis,
  ensureGatewaySubscriber,
  gatewayHealthSnapshot,
  gatewayRoutingOptionsFromEnv,
  log,
  metrics,
  prepareGatewayMetrics,
  readWebSocketLifecycleSnapshot,
  registerGatewayWebSocketLifecycle,
  recordGatewayWebSocketProxy,
  recordGatewayWebSocketSessionLifetime,
  recordRuntimeForwardDuration,
  runtimeForwardOutcome,
} from "gateway-runtime";
import {
  createGatewayWebSocketUpstreamFetch,
  proxyGatewayWebSocket,
  webSocketProxyOptionsFromEnv,
} from "gateway-websocket";
import { SESSION_POLICY_RESTART } from "shared-worker-contract";
import { formatWorkerId } from "shared-worker-id";

/**
 * @typedef {{
 *   REDIS_ADDR: string,
 *   LOG_LEVEL?: unknown,
 *   PLATFORM_DOMAIN?: string,
 *   ADMIN_HOST?: string,
 *   WEBSOCKET_MAX_BUFFERED_MESSAGES?: unknown,
 *   WEBSOCKET_RECONNECT_DELAYS_MS?: unknown,
 *   CONTROL: Fetcher,
 *   RUNTIME_SYSTEM: Fetcher,
 *   RUNTIME_USER: Fetcher,
 *   [key: string]: unknown,
 * }} GatewayEnv
 * @typedef {{ waitUntil: (promise: Promise<unknown>) => void }} GatewayContext
 */

function notFoundResponse() {
  return jsonError(404, "not_found", "Not found");
}

const bindLogLevel = createLogLevelBinder();

/**
 * @param {string} requestId
 * @param {string | null} namespace
 * @param {string | null} worker
 * @param {string | null} version
 * @param {string} binding
 */
function gatewayWebSocketObservability(requestId, namespace, worker, version, binding) {
  return {
    adjustBufferedMessages: adjustGatewayWebSocketProxyBufferedMessages,
    adjustConnections: adjustGatewayWebSocketProxyConnections,
    recordEvent(
      /** @type {string} */ level,
      /** @type {string} */ event,
      /** @type {Record<string, unknown>} */ fields = {}
    ) {
      log(level, event, {
        request_id: requestId,
        namespace,
        worker,
        version,
        binding,
        ...fields,
      });
    },
    recordSessionLifetime: recordGatewayWebSocketSessionLifetime,
  };
}

export default {
  /**
   * @param {Request} request
   * @param {GatewayEnv} env
   * @param {GatewayContext} ctx
   */
  async fetch(request, env, ctx) {
    bindLogLevel(env);
    const url = new URL(request.url);
    const redis = createGatewayRedis(env.REDIS_ADDR);
    /** @type {string | null} */
    let namespace = null;
    /** @type {string | null} */
    let worker = null;
    /** @type {string | null} */
    let version = null;
    const scope = createHttpRequestScope({
      request,
      service: "gateway",
      metrics,
      log,
      route: "worker_fetch",
      extras: () => ({ namespace, worker, version }),
      responseHeaderFilter: deleteGatewayInternalHeaders,
    });

    try {
      const subscriberStart = ensureGatewaySubscriber(env.REDIS_ADDR);
      if (subscriberStart) ctx.waitUntil(subscriberStart);

      if (url.pathname === "/healthz" && request.method === "GET") {
        scope.setRoute("healthz");
        return scope.respond(jsonResponse(200, {
          ok: true,
          service: "gateway",
          ...gatewayHealthSnapshot(),
        }));
      }

      if (url.pathname === "/_metrics" && request.method === "GET") {
        scope.setRoute("metrics");
        prepareGatewayMetrics();
        return scope.respond(prometheusResponse(metrics));
      }

      // After health/metrics so a malformed PLATFORM_DOMAIN cannot 502 probes.
      const { platformDomain, normalizedAdminHost } = gatewayRoutingOptionsFromEnv(env);

      // Admin host short-circuit runs before any ns / Redis lookup so
      // control stays reachable even mid-FLUSHALL / Redis outage.
      const normalizedHost = normalizeRequestHost(url.hostname).toLowerCase();
      const dispatch = await resolveGatewayDispatch({
        url,
        normalizedHost,
        normalizedAdminHost,
        platformDomain,
        redis,
        requestId: scope.requestId,
      });

      scope.setRoute(dispatch.route);
      namespace = dispatch.namespace;
      worker = dispatch.worker;
      version = dispatch.version;
      if (dispatch.kind === "not_found") {
        return scope.respond(notFoundResponse());
      }

      let forwardRequest;
      if (dispatch.forwardPath === url.pathname) {
        forwardRequest = new Request(request);
      } else {
        url.pathname = dispatch.forwardPath;
        forwardRequest = new Request(url.toString(), request);
      }
      deleteGatewayInternalHeaders(forwardRequest.headers);
      // Loader branches carry worker identity + prefix; control is
      // infrastructure and has no worker id to inject.
      if (dispatch.bindingName !== "CONTROL") {
        forwardRequest.headers.set("x-worker-id", formatWorkerId({ namespace, worker, version }));
        forwardRequest.headers.set("x-worker-prefix", dispatch.prefix);
      }
      forwardRequest.headers.set("x-request-id", scope.requestId);

      /** @type {number | null} */
      let forwardStartedAt = null;
      try {
        let response;
        if (isWebSocketUpgrade(request) && dispatch.bindingName !== "CONTROL") {
          const lifecycleRedis = createGatewayLifecycleRedis(env.REDIS_ADDR);
          const runtimeNamespace = dispatch.namespace;
          const runtimeWorker = dispatch.worker;
          const runtimeVersion = dispatch.version;
          const initialLifecycle = await readWebSocketLifecycleSnapshot(
            lifecycleRedis,
            runtimeNamespace,
            runtimeWorker
          );
          if (
            initialLifecycle.kind === "inactive" ||
            initialLifecycle.version !== runtimeVersion
          ) {
            throw new GatewayRoutingUnavailableError();
          }
          let observedLifecycle = initialLifecycle;
          const upstreamFetch = createGatewayWebSocketUpstreamFetch(
            forwardRequest,
            env[dispatch.bindingName]
          );
          forwardStartedAt = Date.now();
          const initial = await upstreamFetch();
          response = initial.status === 101 && initial.webSocket
            ? proxyGatewayWebSocket(
              initial,
              upstreamFetch,
              recordGatewayWebSocketProxy,
              gatewayWebSocketObservability(
                scope.requestId,
                runtimeNamespace,
                runtimeWorker,
                runtimeVersion,
                dispatch.bindingName
              ),
              {
                ...webSocketProxyOptionsFromEnv(env),
                registerLifecycle: (handlers) => registerGatewayWebSocketLifecycle(
                  runtimeNamespace,
                  runtimeWorker,
                  initialLifecycle,
                  handlers
                ),
                checkLifecycle: async () => {
                  let current;
                  try {
                    current = await readWebSocketLifecycleSnapshot(
                      lifecycleRedis,
                      runtimeNamespace,
                      runtimeWorker
                    );
                  } catch (err) {
                    if (err instanceof GatewayRoutingUnavailableError) throw err;
                    return "retry";
                  }
                  if (current.kind === "inactive") return "restart";
                  if (current.restartSequence < observedLifecycle.restartSequence) {
                    throw new GatewayRoutingUnavailableError();
                  }
                  if (current.version !== runtimeVersion) return "restart";
                  if (current.restartSequence > observedLifecycle.restartSequence) {
                    if (current.mode === SESSION_POLICY_RESTART) return "restart";
                    observedLifecycle = current;
                  }
                  return "continue";
                },
              }
            )
            : initial;
        } else {
          forwardStartedAt = Date.now();
          response = await env[dispatch.bindingName].fetch(forwardRequest);
        }
        recordRuntimeForwardDuration(
          Date.now() - forwardStartedAt,
          dispatch.bindingName,
          runtimeForwardOutcome(response)
        );
        return scope.respond(response);
      } catch (err) {
        if (forwardStartedAt !== null) {
          recordRuntimeForwardDuration(Date.now() - forwardStartedAt, dispatch.bindingName, "exception");
        }
        if (isWebSocketUpgrade(request)) recordGatewayWebSocketProxy("exception");
        throw err;
      }
    } catch (err) {
      scope.markError(err);
      if (err instanceof GatewayRoutingUnavailableError) {
        return scope.respond(internalErrorResponse(
          err.status,
          err.code,
          err.publicMessage,
          scope.requestId
        ));
      }
      return scope.respond(internalErrorResponse(502, "gateway_error", "Gateway error", scope.requestId));
    } finally {
      scope.complete();
    }
  },
};
