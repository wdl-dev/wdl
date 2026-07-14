// Single point of truth for the `v<int>` version tag shape used in
// `routes:<ns>` values and worker-bundle keys. Enforcing it here prevents
// silent format drift between the INCR path (new deploys) and the HGET
// path (promote / versions listing).

const VERSION_RE = /^v([1-9][0-9]*)$/;

/**
 * @param {unknown} n
 * @returns {string}
 */
export function formatVersion(n) {
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1) {
    throw new Error(`invalid version number ${n}`);
  }
  return `v${n}`;
}

// Returns the integer or null — never throws, callers decide on 400 vs skip.
/**
 * @param {unknown} tag
 * @returns {number | null}
 */
export function parseVersion(tag) {
  if (typeof tag !== "string") return null;
  const m = tag.match(VERSION_RE);
  if (!m) return null;
  const parsed = Number.parseInt(m[1], 10);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

// `v:` infix separates the integer-indexed bundle namespace from sibling
// keys like `:next_version`, so there's no way a version tag can shadow
// another subkey even if someone ever writes Redis directly.
/**
 * @param {string} ns
 * @param {string} name
 * @param {unknown} version
 * @returns {string}
 */
export function bundleKey(ns, name, version) {
  const n = parseVersion(version);
  if (n == null) throw new Error(`invalid version tag ${JSON.stringify(version)}`);
  return `worker:${ns}:${name}:v:${n}`;
}

// Monotonic immutable-version allocator for one logical worker.
/** @param {string} ns @param {string} name */
export function nextVersionKey(ns, name) {
  return `worker:${ns}:${name}:next_version`;
}

// Active-route hash for a namespace: field=workerName, value=`v<int>`. Control
// is the sole writer; centralized here so cross-tier readers cannot drift from
// the key grammar (reader set in docs/redis-key-layout.md).
/** @param {string} ns @returns {string} */
export function routesKey(ns) {
  return `routes:${ns}`;
}

// Pattern-route hash for a declared custom host: field=path slot, value=compact
// route projection. Control writes it; gateway reads it for pattern routing.
/** @param {string} host @returns {string} */
export function patternsKey(host) {
  return `patterns:${host}`;
}

export const NAMESPACES_KEY = "namespaces";
export const DECLARED_HOSTS_KEY = "declared-hosts";
const HOSTS_PREFIX = "hosts:";
const NS_HOSTS_PREFIX = "ns-hosts:";
const HOST_DECLARATIONS_PREFIX = "host-declarations:";
export const HOSTS_SCAN_PATTERN = `${HOSTS_PREFIX}*`;
export const HOST_DECLARATIONS_SCAN_PATTERN = `${HOST_DECLARATIONS_PREFIX}*`;

/** @param {string} ns */
export function hostsKey(ns) {
  return `${HOSTS_PREFIX}${ns}`;
}

/** @param {string} key */
export function namespaceFromHostsKey(key) {
  return key.startsWith(HOSTS_PREFIX) ? key.slice(HOSTS_PREFIX.length) : "";
}

/** @param {string} ns */
export function nsHostsKey(ns) {
  return `${NS_HOSTS_PREFIX}${ns}`;
}

/** @param {string} host */
export function hostDeclarationsKey(host) {
  return `${HOST_DECLARATIONS_PREFIX}${host}`;
}

// Retained-version ZSET for a worker: score=int version, member=`v<int>`.
/** @param {string} ns @param {string} worker @returns {string} */
export function workerVersionsKey(ns, worker) {
  return `worker-versions:${ns}:${worker}`;
}

// Logical Worker -> Durable Object storage pointer. Control owns writes; DO
// runtime and workflows read it for owner/storage fencing.
/** @param {string} ns @param {string} worker */
export function doStorageIdKey(ns, worker) {
  return `worker:do-storage:${ns}:${worker}`;
}
