// Canonical JS contract for immutable worker versions and the control-plane
// Redis keys and channels shared across in-tree tiers.

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

export const MAX_WORKER_VERSION_TAG = formatVersion(Number.MAX_SAFE_INTEGER);

// Returns the integer or null — never throws, callers decide on 400 vs skip.
/**
 * @param {unknown} tag
 * @returns {number | null}
 */
export function parseVersion(tag) {
  if (typeof tag !== "string" || tag.length < 2 || tag[0] !== "v" || tag[1] === "0") {
    return null;
  }
  let parsed = 0;
  for (let i = 1; i < tag.length; i += 1) {
    const digit = tag.charCodeAt(i) - 0x30;
    if (digit < 0 || digit > 9) return null;
    parsed = (parsed * 10) + digit;
    if (!Number.isSafeInteger(parsed)) return null;
  }
  return parsed;
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

// Workers explicitly hidden from the namespace platform-domain route. The
// active version remains in routes:<ns> for lifecycle, binding, and Workflows
// readers; Gateway subtracts this set when it builds its subdomain cache.
/** @param {string} ns @returns {string} */
export function platformDomainDisabledKey(ns) {
  return `platform-domain-disabled:${ns}`;
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
export const SESSION_POLICY_CHANNEL = "session-policy:restart";
export const WORKER_DELETE_CHANNEL = "worker:delete";

// Initial backend upgrades can opt one public WebSocket session out of
// Gateway's transparent backend replacement. Gateway strips this internal
// response header before returning the public 101.
export const WEBSOCKET_RECONNECT_POLICY_HEADER = "x-wdl-websocket-reconnect-policy";
export const WEBSOCKET_RECONNECT_POLICY_DISABLED = "disabled";
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

export const SESSION_POLICY_PRESERVE = "preserve";
export const SESSION_POLICY_RESTART = "restart";

/** @param {unknown} value */
export function isSessionPolicyMode(value) {
  return value === SESSION_POLICY_PRESERVE ||
    value === SESSION_POLICY_RESTART;
}

/**
 * @param {unknown} version
 * @param {unknown} mode
 * @param {unknown} restartSequence
 */
function isValidSessionPolicy(version, mode, restartSequence) {
  return parseVersion(version) != null &&
    isSessionPolicyMode(mode) &&
    Number.isSafeInteger(restartSequence) &&
    /** @type {number} */ (restartSequence) >= 0 &&
    (
      mode !== SESSION_POLICY_RESTART ||
      restartSequence !== 0
    );
}

// Active session policy projection. Control writes it in the same transaction as
// routes:<ns>; Gateway and do-runtime read it at lifecycle/fence boundaries.
/** @param {string} ns @param {string} worker */
export function sessionPolicyKey(ns, worker) {
  return `worker:session-policy:${ns}:${worker}`;
}

// Permanent restart-event allocator. It survives whole-worker deletion so a
// stale Gateway session can never confuse a recreated worker with an old event.
/** @param {string} ns @param {string} worker */
export function sessionPolicySequenceKey(ns, worker) {
  return `worker:session-policy-seq:${ns}:${worker}`;
}

/**
 * @param {{ version: string, mode: "preserve" | "restart", restartSequence: number }} projection
 */
export function encodeSessionPolicyProjection(projection) {
  if (!isValidSessionPolicy(
    projection.version,
    projection.mode,
    projection.restartSequence
  )) {
    throw new TypeError("invalid session policy projection");
  }
  return JSON.stringify({
    version: projection.version,
    mode: projection.mode,
    restartSequence: projection.restartSequence,
  });
}

/**
 * @param {{ ns: string, worker: string, version: string, restartSequence: number }} event
 */
export function encodeSessionPolicyEvent(event) {
  if (
    typeof event.ns !== "string" ||
    !event.ns ||
    typeof event.worker !== "string" ||
    !event.worker ||
    !isValidSessionPolicy(
      event.version,
      SESSION_POLICY_RESTART,
      event.restartSequence
    )
  ) {
    throw new TypeError("invalid session policy event");
  }
  return JSON.stringify({
    ns: event.ns,
    worker: event.worker,
    version: event.version,
    restartSequence: event.restartSequence,
  });
}

/**
 * @param {unknown} raw
 * @returns {{ ns: string, worker: string, version: string, restartSequence: number }}
 */
export function parseSessionPolicyEvent(raw) {
  let value;
  try {
    value = typeof raw === "string" ? JSON.parse(raw) : null;
  } catch {
    value = null;
  }
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    typeof value.ns !== "string" ||
    !value.ns ||
    typeof value.worker !== "string" ||
    !value.worker ||
    !isValidSessionPolicy(
      value.version,
      SESSION_POLICY_RESTART,
      value.restartSequence
    )
  ) {
    throw new TypeError("invalid session policy event");
  }
  return {
    ns: value.ns,
    worker: value.worker,
    version: value.version,
    restartSequence: value.restartSequence,
  };
}

/** @param {{ ns: string, worker: string }} event */
export function encodeWorkerDeleteEvent(event) {
  if (
    typeof event.ns !== "string" ||
    !event.ns ||
    typeof event.worker !== "string" ||
    !event.worker
  ) {
    throw new TypeError("invalid worker delete event");
  }
  return JSON.stringify({ ns: event.ns, worker: event.worker });
}

/**
 * @param {unknown} raw
 * @returns {{ ns: string, worker: string }}
 */
export function parseWorkerDeleteEvent(raw) {
  let value;
  try {
    value = typeof raw === "string" ? JSON.parse(raw) : null;
  } catch {
    value = null;
  }
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    typeof value.ns !== "string" ||
    !value.ns ||
    typeof value.worker !== "string" ||
    !value.worker
  ) {
    throw new TypeError("invalid worker delete event");
  }
  return { ns: value.ns, worker: value.worker };
}

/**
 * @param {unknown} raw
 * @returns {{ version: string, mode: "preserve" | "restart", restartSequence: number } | null}
 */
export function parseSessionPolicyProjection(raw) {
  if (raw == null) return null;
  let value;
  try {
    value = typeof raw === "string" ? JSON.parse(raw) : null;
  } catch {
    value = null;
  }
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    !isValidSessionPolicy(value.version, value.mode, value.restartSequence)
  ) {
    throw new TypeError("invalid session policy projection");
  }
  return {
    version: value.version,
    mode: value.mode,
    restartSequence: value.restartSequence,
  };
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
