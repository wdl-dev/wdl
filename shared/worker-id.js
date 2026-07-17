import { isValidRouteNs, isValidRuntimeLoadNs, WORKER_NAME_RE } from "./ns-pattern.js";
import { parseVersion } from "./worker-contract.js";

/** @param {{ namespace: unknown, worker: unknown, version: unknown }} id */
export function formatWorkerId({ namespace, worker, version }) {
  return `${namespace}:${worker}:${version}`;
}

/** @param {string | null | undefined} workerId */
export function parseWorkerId(workerId) {
  if (!workerId) return null;
  const parts = workerId.split(":");
  if (parts.length !== 3 || parts.some((part) => part === "")) return null;
  return parts;
}

/** @param {unknown} workerId */
export function parseWorkerIdObject(workerId) {
  const parts = parseWorkerId(typeof workerId === "string" ? workerId : null);
  if (!parts) {
    return { namespace: "", worker: "", version: "" };
  }
  return { namespace: parts[0], worker: parts[1], version: parts[2] };
}

/**
 * @param {string | null | undefined} workerId
 * @param {(ns: unknown) => boolean} nsValidator
 * @returns {{ namespace: string, worker: string, version: string } | null}
 */
function parseValidatedWorkerId(workerId, nsValidator) {
  const parts = parseWorkerId(workerId);
  if (!parts) return null;
  const [namespace, worker, version] = parts;
  if (!nsValidator(namespace)) return null;
  if (!WORKER_NAME_RE.test(worker)) return null;
  if (parseVersion(version) == null) return null;
  return { namespace, worker, version };
}

/**
 * Runtime dispatch entrypoints are route-selected traffic. They intentionally
 * reject platform-tier workers; those are reachable only through
 * [[platform_bindings]] service-binding cold loads.
 *
 * @param {string | null | undefined} workerId
 * @returns {{ namespace: string, worker: string, version: string } | null}
 */
export function parseDispatchWorkerId(workerId) {
  return parseValidatedWorkerId(workerId, isValidRouteNs);
}

/**
 * Runtime load may materialize workers that are not public routes, including
 * current platform-tier targets selected by the platform-binding linker.
 *
 * @param {string | null | undefined} workerId
 * @returns {{ namespace: string, worker: string, version: string } | null}
 */
export function parseRuntimeLoadWorkerId(workerId) {
  return parseValidatedWorkerId(workerId, isValidRuntimeLoadNs);
}
