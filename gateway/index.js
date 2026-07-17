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
  createGatewayRedis,
  ensureGatewaySubscriber,
  gatewayHealthSnapshot,
  log,
  metrics,
  prepareGatewayMetrics,
  recordGatewayWebSocketProxy,
  recordRuntimeForwardDuration,
  runtimeForwardOutcome,
} from "gateway-runtime";
import { formatWorkerId } from "shared-worker-id";
import { platformDomainFromEnv } from "shared-ns-pattern";

// Re-exported so workerd's capnp `durableObjectNamespaces` entry resolves
// the class name against this worker's exports.
export { GatewayWsHolder } from "gateway-holder";

/**
 * @typedef {{
 *   REDIS_ADDR: string,
 *   LOG_LEVEL?: unknown,
 *   PLATFORM_DOMAIN?: string,
 *   ADMIN_HOST?: string,
 *   WS_HOLDER: DurableObjectNamespace,
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
      const platformDomain = platformDomainFromEnv(env);

      // Admin host short-circuit runs before any ns / Redis lookup so
      // control stays reachable even mid-FLUSHALL / Redis outage.
      const normalizedHost = normalizeRequestHost(url.hostname).toLowerCase();
      const normalizedAdminHost = normalizeRequestHost(env.ADMIN_HOST || "").toLowerCase();
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

      url.pathname = dispatch.forwardPath;

      const forwardRequest = new Request(url.toString(), request);
      deleteGatewayInternalHeaders(forwardRequest.headers);
      // Loader branches carry worker identity + prefix; control is
      // infrastructure and has no worker id to inject.
      if (dispatch.bindingName !== "CONTROL") {
        forwardRequest.headers.set("x-worker-id", formatWorkerId({ namespace, worker, version }));
        forwardRequest.headers.set("x-worker-prefix", dispatch.prefix);
      }
      forwardRequest.headers.set("x-request-id", scope.requestId);

      const forwardStartedAt = Date.now();
      try {
        let response;
        if (isWebSocketUpgrade(request) && dispatch.bindingName !== "CONTROL") {
          // Routed through a DO so the long-lived 101 lives on an actor
          // IoContext, which workerd's hang detector skips.
          forwardRequest.headers.set("x-wdl-upstream-binding", dispatch.bindingName);
          const holderId = env.WS_HOLDER.newUniqueId();
          response = await env.WS_HOLDER.get(holderId).fetch(forwardRequest);
        } else {
          response = await env[dispatch.bindingName].fetch(forwardRequest);
        }
        recordRuntimeForwardDuration(
          Date.now() - forwardStartedAt,
          dispatch.bindingName,
          runtimeForwardOutcome(response)
        );
        return scope.respond(response);
      } catch (err) {
        recordRuntimeForwardDuration(Date.now() - forwardStartedAt, dispatch.bindingName, "exception");
        if (isWebSocketUpgrade(request)) recordGatewayWebSocketProxy("exception");
        throw err;
      }
    } catch (err) {
      scope.markError(err);
      return scope.respond(internalErrorResponse(502, "gateway_error", "Gateway error", scope.requestId));
    } finally {
      scope.complete();
    }
  },
};
