import { withInternalAuth } from "shared-internal-auth";
import { errorMessage } from "shared-errors";
import { validOwnerEndpointForService } from "shared-owner-endpoint";

export const MAX_OWNER_FORWARD_HOPS = 2;

/** @param {unknown} value */
export function parseForwardHopCount(value) {
  const parsed = Number(value ?? "0");
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : 0;
}

/** @param {unknown} status */
function normalizeStatus(status) {
  const parsed = Number(status ?? 500);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 500;
}

/** @param {{ status?: unknown } | null | undefined} response */
export function forwardOutcome(response) {
  const status = normalizeStatus(response?.status);
  return status < 400 ? "ok" : "error";
}

/** @param {HeadersInit} headers @param {string | null | undefined} requestId */
function withRequestId(headers, requestId) {
  if (typeof requestId !== "string" || !requestId) return headers;
  if (headers instanceof Headers) {
    headers.set("x-request-id", requestId);
    return headers;
  }
  return { ...headers, "x-request-id": requestId };
}

/**
 * @param {{
 *   env: Record<string, unknown>,
 *   endpoint?: string | null,
 *   endpointPort: number,
 *   endpointService: "d1-runtime" | "do-runtime",
 *   pathname: string,
 *   method?: string,
 *   requestId?: string | null,
 *   hopCount?: number,
 *   body?: BodyInit | null,
 *   signal?: AbortSignal,
 *   metrics: { increment(name: string, labels?: Record<string, unknown>, value?: number): void },
 *   metricName: string,
 *   service: string,
 *   log: (level: string, event: string, fields?: Record<string, unknown>) => void,
 *   logEvent: string,
 *   buildHeaders(nextHopCount: number): HeadersInit,
 *   logFields(): Record<string, unknown>,
 *   missingEndpointError(): Error,
 *   invalidEndpointError(): Error,
 *   hopExhaustedError(): Error,
 *   unavailableError(err: unknown): Error,
 *   isTimeoutError?: (err: unknown) => boolean,
 * }} options
 * @returns {Promise<Response>}
 */
export async function forwardOwnerRequest({
  env,
  endpoint,
  endpointPort,
  endpointService,
  pathname,
  method = "POST",
  requestId = null,
  hopCount = 0,
  body = null,
  signal = undefined,
  metrics,
  metricName,
  service,
  log,
  logEvent,
  buildHeaders,
  logFields,
  missingEndpointError,
  invalidEndpointError,
  hopExhaustedError,
  unavailableError,
  isTimeoutError = undefined,
}) {
  if (!endpoint) throw missingEndpointError();
  if (!validOwnerEndpointForService(endpoint, endpointPort, endpointService)) {
    throw invalidEndpointError();
  }
  const nextHopCount = hopCount + 1;
  if (nextHopCount > MAX_OWNER_FORWARD_HOPS) throw hopExhaustedError();
  try {
    const headers = withRequestId(buildHeaders(nextHopCount), requestId);
    const response = await fetch(`http://${endpoint}${pathname}`, {
      method,
      headers: withInternalAuth(headers, env),
      ...(body == null ? {} : { body }),
      ...(signal == null ? {} : { signal }),
    });
    const outcome = forwardOutcome(response);
    metrics.increment(metricName, { service, outcome });
    log(outcome === "ok" ? "info" : "warn", logEvent, {
      request_id: requestId || undefined,
      ...logFields(),
      status: response.status,
    });
    return response;
  } catch (err) {
    if (isTimeoutError?.(err)) {
      metrics.increment(metricName, { service, outcome: "timeout" });
      throw err;
    }
    metrics.increment(metricName, { service, outcome: "unavailable" });
    log("warn", logEvent.replace(/_complete$/, "_unavailable"), {
      request_id: requestId || undefined,
      ...logFields(),
      error_message: errorMessage(err),
    });
    throw unavailableError(err);
  }
}
