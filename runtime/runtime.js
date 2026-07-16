// Runtime helpers for user-runtime and system-runtime. This module owns
// service-name binding, logger setup, metrics access, request-scope creation,
// and the per-process loaded-worker registry that drives historical-version
// eviction; runtime/load.js owns the shared workerLoader cold-load wrapper.

import {
  createLogLevelBinder,
  createLogger,
  formatError,
} from "shared-observability";
import { errorMessage } from "shared-errors";
import {
  createHttpRequestScope,
} from "shared-request-scope";
import { parseWorkerId } from "shared-worker-id";
import { metrics } from "runtime-metrics";

/** @type {((level: string, event: string, fields?: Record<string, unknown>) => void) | null} */
let log = null;
let serviceName = "runtime";
const bindLogLevel = createLogLevelBinder();
const USER_RUNTIME_SERVICE = "user-runtime";
const SYSTEM_RUNTIME_SERVICE = "system-runtime";
const SYSTEM_NAMESPACE = "__system__";

/** @param {string} namespace */
export function expectedRuntimeServiceForNamespace(namespace) {
  return namespace === SYSTEM_NAMESPACE ? SYSTEM_RUNTIME_SERVICE : USER_RUNTIME_SERVICE;
}

/** @param {string} service @param {string} namespace */
export function runtimeServiceAllowsNamespace(service, namespace) {
  return service === expectedRuntimeServiceForNamespace(namespace);
}

/** @param {Record<string, unknown>} env */
export function bindRuntime(env) {
  if (!log) {
    serviceName = typeof env.SERVICE_NAME === "string" && env.SERVICE_NAME ? env.SERVICE_NAME : "runtime";
    log = createLogger(serviceName);
  }
  bindLogLevel(env);
  const activeLog = log;

  return {
    metrics,
    log: activeLog,
    serviceName,

    /**
     * @param {Request} request
     * @param {{ route: string, extras?: Record<string, unknown> | (() => Record<string, unknown>) | null }} options
     */
    requestScope(request, { route, extras }) {
      return createHttpRequestScope({
        request,
        service: serviceName,
        metrics,
        log: activeLog,
        route,
        extras,
      });
    },
  };
}

// Per-process registry of workerLoader cache entries. Insert on factory
// invocation (cache miss); delete on successful abort. workerd's loader
// never evicts on its own — we use this Map to find historical versions
// of the same <ns>:<name> to abort when a new version cold-loads.
const loadedWorkers = new Map();
const loadedFamilies = new Map();

/** @param {string} ns @param {string} name */
function loadedFamilyKey(ns, name) {
  return `${ns}\0${name}`;
}

/** @param {string} workerId */
function forgetLoadedWorker(workerId) {
  const entry = loadedWorkers.get(workerId);
  loadedWorkers.delete(workerId);
  if (!entry?.familyKey) return;
  const family = loadedFamilies.get(entry.familyKey);
  if (!family) return;
  family.delete(workerId);
  if (family.size === 0) loadedFamilies.delete(entry.familyKey);
}

/** @param {string} workerId */
export function recordLoadedWorker(workerId) {
  forgetLoadedWorker(workerId);
  const parts = parseWorkerId(workerId);
  const familyKey = parts ? loadedFamilyKey(parts[0], parts[1]) : null;
  loadedWorkers.set(workerId, { loadedAt: Date.now(), familyKey });
  if (familyKey) {
    const family = loadedFamilies.get(familyKey) || new Set();
    family.add(workerId);
    loadedFamilies.set(familyKey, family);
  }
}

/** @lintignore data-URL unit tests import this helper from a rewritten module. */
export function loadedWorkerCount() {
  return loadedWorkers.size;
}

/** @lintignore data-URL unit tests import this helper from a rewritten module. */
export function loadedWorkerIds() {
  return Array.from(loadedWorkers.keys());
}

/** @lintignore data-URL unit tests import this hook from a rewritten module. */
export function _resetLoadedWorkersForTest() {
  loadedWorkers.clear();
  loadedFamilies.clear();
}

/** @param {string} workerId */
export function siblingsFor(workerId) {
  const parts = parseWorkerId(workerId);
  if (!parts) return [];
  const family = loadedFamilies.get(loadedFamilyKey(parts[0], parts[1]));
  if (!family) return [];
  return [...family].filter((id) => id !== workerId);
}

// Anchor on the literal prefix workerd's own abortIsolateDynamic test
// asserts. Other `internal error;` phrasings (abort signal,
// script_internal_error) are unrelated failures and must not look like
// successful eviction.
const ABORT_SUCCESS_PREFIX = "internal error; reference =";

// Factory throws to surface the case where workerd's cache somehow lost
// the entry between our record and this abort — we'd rather see the
// error than silently cold-load a doomed isolate.
/**
 * @param {{ env: Record<string, unknown>, workerId: string }} options
 */
export async function abortLoadedWorker({ env, workerId }) {
  const loader = env.LOADER;
  if (!loader || typeof loader !== "object" || !("get" in loader) || typeof loader.get !== "function") {
    throw new Error("runtime loader binding is missing");
  }
  const stub = /** @type {{ get(id: string, factory: () => unknown): { getEntrypoint(name: string): { abort(reason: string): Promise<unknown> } } }} */ (loader).get(workerId, () => {
    throw new Error("evict_factory_unexpected_call");
  });
  try {
    await stub.getEntrypoint("__WdlAbort__").abort("wdl-evict");
    return { aborted: false, reason: "no_internal_error" };
  } catch (err) {
    const msg = errorMessage(err);
    if (msg.startsWith(ABORT_SUCCESS_PREFIX)) {
      forgetLoadedWorker(workerId);
      return { aborted: true };
    }
    if (msg.includes("evict_factory_unexpected_call")) {
      forgetLoadedWorker(workerId);
      return { aborted: false, reason: "factory_called" };
    }
    return { aborted: false, reason: "unexpected", error: err };
  }
}

// Service-binding cold-loads must not call this — the version they pin
// may not be the active one, so aborting siblings would force gateway
// traffic into a needless cold-load.
/**
 * @param {{
 *   env: Record<string, unknown>,
 *   workerId: string,
 *   log?: ((level: string, event: string, fields?: Record<string, unknown>) => void) | null,
 * }} options
 */
export async function evictSiblings({ env, workerId, log: logger = null }) {
  const siblings = siblingsFor(workerId);
  if (siblings.length === 0) return { considered: 0, aborted: 0 };
  let aborted = 0;
  for (const old of siblings) {
    let result;
    try {
      result = await abortLoadedWorker({ env, workerId: old });
    } catch (err) {
      metrics.increment("loader_evictions", { service: serviceName, outcome: "failed" });
      if (logger) logger("warn", "evict_failed", {
        worker_id: old,
        triggered_by: workerId,
        ...formatError(err),
      });
      continue;
    }
    if (result.aborted) {
      aborted += 1;
      metrics.increment("loader_evictions", { service: serviceName, outcome: "aborted" });
      if (logger) logger("info", "evict_aborted", {
        worker_id: old,
        triggered_by: workerId,
      });
    } else {
      metrics.increment("loader_evictions", { service: serviceName, outcome: "skipped" });
      if (logger) logger("warn", "evict_skipped", {
        worker_id: old,
        triggered_by: workerId,
        reason: result.reason,
        ...(result.reason === "unexpected" ? formatError(result.error) : {}),
      });
    }
  }
  return { considered: siblings.length, aborted };
}
