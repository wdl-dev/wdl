// Canonical JS contract for immutable worker versions and the control-plane
// Redis keys and channels shared across in-tree tiers.

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
 * @param {string} worker
 * @param {unknown} version
 * @returns {string}
 */
export function bundleKey(ns, worker, version) {
  const n = parseVersion(version);
  if (n == null) throw new Error(`invalid version tag ${JSON.stringify(version)}`);
  return `worker:${ns}:${worker}:v:${n}`;
}

// Monotonic immutable-version allocator for one logical worker.
/** @param {string} ns @param {string} worker */
export function nextVersionKey(ns, worker) {
  return `worker:${ns}:${worker}:next_version`;
}

// Cron generations below this epoch are reserved and never allocated by the
// permanent counter, so slot refs without allocator state cannot overlap
// permanent allocations.
export const CRON_GENERATION_EPOCH = 1024;

// Permanent high-water mark for Cron configuration generations. Like
// next_version, this survives whole-worker deletion so stale scheduled refs
// can never match a recreated Cron entry.
/** @param {string} ns @param {string} worker */
export function cronSequenceKey(ns, worker) {
  return `cron:seq:${ns}:${worker}`;
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
export const DECLARED_HOSTS_REVISION_KEY = "declared-hosts:revision";
export const ROUTES_CHANNEL = "routes:invalidate";
export const ROUTES_FLUSH_CHANNEL = "routes:flush";
export const PATTERNS_CHANNEL = "patterns:invalidate";
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

// DO runtime owns the records; Control uses the storage-scoped pattern during
// whole-worker cleanup. Keep both sides on the same encoded Redis prefix.
export const DO_OWNER_SCOPE_PREFIX = "do:owner:scope:";

/** @param {string} storageId */
export function doOwnerScopeScanPatternForStorage(storageId) {
  return `${DO_OWNER_SCOPE_PREFIX}${encodeURIComponent(`${storageId}:`)}*`;
}

// Per-worker lifecycle lock. Control owns acquisition/release; other tiers may
// WATCH it when creating state that whole-worker delete must discover.
export const WHOLE_DELETE_LOCK_KIND = "whole";
export const VERSION_DELETE_LOCK_KIND = "version";

/** @param {string} ns @param {string} worker */
export function deleteLockKey(ns, worker) {
  return `worker-delete-lock:${ns}:${worker}`;
}

/**
 * @param {"whole" | "version"} kind
 * @param {string} token
 */
export function formatDeleteLockToken(kind, token) {
  if (
    (kind !== WHOLE_DELETE_LOCK_KIND && kind !== VERSION_DELETE_LOCK_KIND) ||
    typeof token !== "string" || !token
  ) {
    throw new TypeError("invalid worker delete lock token");
  }
  return `${kind}:${token}`;
}

/** @param {unknown} value @returns {"whole" | "version" | null} */
export function parseDeleteLockKind(value) {
  if (typeof value !== "string") return null;
  const separator = value.indexOf(":");
  if (separator <= 0 || separator === value.length - 1) return null;
  const kind = value.slice(0, separator);
  return kind === WHOLE_DELETE_LOCK_KIND || kind === VERSION_DELETE_LOCK_KIND
    ? kind
    : null;
}
