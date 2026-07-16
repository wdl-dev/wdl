import {
  ensureRequestId,
  recordRequestComplete,
} from "shared-observability";
import { echoResponseWithRequestId } from "shared-respond";

/**
 * @typedef {import("shared-observability").MetricsRegistry} MetricsRegistry
 * @typedef {(level: string, event: string, fields?: Record<string, unknown>) => void} Logger
 */

/**
 * @param {{
 *   request: Request,
 *   service: string,
 *   metrics?: MetricsRegistry | null,
 *   log: Logger,
 *   route: string,
 *   probeRoutes?: string[] | undefined,
 *   extras?: Record<string, unknown> | (() => Record<string, unknown>) | null,
 * }} options
 */
export function createHttpRequestScope({
  request,
  service,
  metrics,
  log,
  route,
  probeRoutes = undefined,
  extras = null,
}) {
  const startedAt = Date.now();
  const requestId = ensureRequestId(request.headers);
  let currentRoute = route;
  let status = 500;
  /** @type {unknown} */
  let requestError = null;
  let hasRequestError = false;

  return {
    requestId,

    /**
     * @param {string} nextRoute
     */
    setRoute(nextRoute) {
      currentRoute = nextRoute;
    },

    /** @param {unknown} err */
    markError(err) {
      requestError = err;
      hasRequestError = true;
    },

    /**
     * @param {Response} response
     * @returns {Response}
     */
    respond(response) {
      status = response.status;
      return echoResponseWithRequestId(response, requestId);
    },

    /**
     * @returns {void}
     */
    complete() {
      const requestExtras = typeof extras === "function" ? extras() : extras;
      recordRequestComplete({
        service,
        metrics,
        log,
        method: request.method,
        requestId,
        route: currentRoute,
        status,
        startedAt,
        error: requestError,
        hasError: hasRequestError,
        extras: requestExtras,
        probeRoutes,
      });
    },
  };
}
