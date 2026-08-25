import {
  createLogger,
} from "shared-observability";
import { metrics as runtimeMetrics } from "runtime-metrics";

export const SERVICE = "do-runtime";

export const metrics = runtimeMetrics;
export const log = createLogger(SERVICE);
/** @type {Map<string, import("do-runtime-owner-registry").DoOwner>} */
export const ownedScopes = new Map();

let draining = false;
let inFlightRequests = 0;
/** @type {Set<(value?: unknown) => void>} */
const inFlightWaiters = new Set();

export function isDraining() {
  return draining;
}

export function setDraining(value = true) {
  draining = value === true;
}

export function prepareDoRuntimeMetrics() {
  metrics.setGauge("do_in_flight_requests", { service: SERVICE }, inFlightRequests);
}

const INVOKE_KINDS = new Set(["alarm", "fetch", "rpc"]);
/** @param {unknown} kind */
function normalizeInvokeKind(kind) {
  const value = String(kind);
  return INVOKE_KINDS.has(value) ? value : "fetch";
}

/** @param {unknown} response */
function outcomeForResponse(response) {
  const rawStatus = response && typeof response === "object" && "status" in response ? response.status : 500;
  const parsed = Number(rawStatus ?? 500);
  const status = Number.isFinite(parsed) && parsed > 0 ? parsed : 500;
  if (status < 400) return "ok";
  return status < 500 ? "client_error" : "server_error";
}

function outcomeForError() {
  return "server_error";
}

/**
 * @template {Response} T
 * @param {unknown} kind
 * @param {() => Promise<T>} fn
 */
export async function recordDoInvoke(kind, fn) {
  const normalizedKind = normalizeInvokeKind(kind);
  const startedAt = Date.now();
  try {
    const response = await fn();
    const outcome = outcomeForResponse(response);
    metrics.increment("do_invokes", { service: SERVICE, kind: normalizedKind, outcome });
    metrics.observe("do_invoke_duration_ms", { service: SERVICE, kind: normalizedKind, outcome }, Date.now() - startedAt);
    return response;
  } catch (err) {
    const outcome = outcomeForError();
    metrics.increment("do_invokes", { service: SERVICE, kind: normalizedKind, outcome });
    metrics.observe("do_invoke_duration_ms", { service: SERVICE, kind: normalizedKind, outcome }, Date.now() - startedAt);
    throw err;
  }
}

/**
 * @template {Response} T
 * @param {() => Promise<T>} fn
 */
export async function recordDoWebSocketUpgrade(fn) {
  const startedAt = Date.now();
  try {
    const response = await fn();
    const outcome = outcomeForResponse(response);
    metrics.increment("do_websocket_upgrades", { service: SERVICE, outcome });
    metrics.observe("do_websocket_upgrade_duration_ms", { service: SERVICE, outcome }, Date.now() - startedAt);
    return response;
  } catch (err) {
    const outcome = outcomeForError();
    metrics.increment("do_websocket_upgrades", { service: SERVICE, outcome });
    metrics.observe("do_websocket_upgrade_duration_ms", { service: SERVICE, outcome }, Date.now() - startedAt);
    throw err;
  }
}

function notifyInFlightWaiters() {
  if (inFlightRequests !== 0) return;
  for (const resolve of inFlightWaiters) resolve();
  inFlightWaiters.clear();
}

export function beginInFlightDispatch() {
  if (draining) return false;
  inFlightRequests += 1;
  return true;
}

export function endInFlightDispatch() {
  if (inFlightRequests > 0) inFlightRequests -= 1;
  notifyInFlightWaiters();
}

export function currentInFlightDispatches() {
  return inFlightRequests;
}

/** @param {number} timeoutMs */
export async function waitForInFlightDispatches(timeoutMs) {
  if (inFlightRequests === 0) {
    return { drained: true, inFlight: 0, waitedMs: 0 };
  }
  const started = Date.now();
  /** @type {ReturnType<typeof setTimeout> | null} */
  let timeout = null;
  const { promise: waiterPromise, resolve: waiter } = Promise.withResolvers();
  inFlightWaiters.add(waiter);
  await Promise.race([
    waiterPromise,
    new Promise((resolve) => {
      timeout = setTimeout(resolve, Math.max(1, timeoutMs));
    }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (waiter) inFlightWaiters.delete(waiter);
  return {
    drained: inFlightRequests === 0,
    inFlight: inFlightRequests,
    waitedMs: Date.now() - started,
  };
}
