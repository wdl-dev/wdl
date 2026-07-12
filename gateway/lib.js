// Pure request-routing helpers for the gateway worker. No bare specifier
// imports — this file is loaded both as a workerd embedded module (via the
// capnp "lib" name) and directly by node --test.

/**
 * @typedef {import("shared-route-projection").PatternProjection & { slot: string }} PatternEntry
 * @typedef {{ kind: string, value: string }} PatternMatchEntry
 * @typedef {{ slot: string, reason: string }} PatternError
 */

const INTERNAL_HEADER_PREFIX = "x-wdl-";
const INTERNAL_FORWARD_HEADERS = [
  "x-worker-id",
  "x-worker-prefix",
];

/** @param {Headers} headers */
export function deleteGatewayInternalHeaders(headers) {
  for (const name of INTERNAL_FORWARD_HEADERS) headers.delete(name);
  for (const name of [...headers.keys()]) {
    if (name.toLowerCase().startsWith(INTERNAL_HEADER_PREFIX)) headers.delete(name);
  }
}

/** @param {string} s */
export function escapeRegex(s) {
  return RegExp.escape(s);
}

// Strip trailing FQDN dot(s). WHATWG URL preserves them, so without this
// "example.com." would look up a different Redis key than "example.com"
// and bypass the platform-domain branch.
/** @param {string} hostname */
export function normalizeRequestHost(hostname) {
  return hostname.replace(/\.+$/, "");
}

/** @param {Request} request */
export function isWebSocketUpgrade(request) {
  return (request.headers.get("Upgrade") || "").toLowerCase() === "websocket";
}

// Must stay aligned with control/topology.js#normalizeHost output; gateway
// only validates pub/sub payloads here, while control remains the writer.
/** @param {unknown} host */
export function isCanonicalPatternHost(host) {
  return typeof host === "string" &&
    host.length > 0 &&
    host === host.toLowerCase() &&
    normalizeRequestHost(host) === host &&
    !host.includes(":") &&
    !host.includes("/") &&
    !/\s/.test(host);
}

const hostRegexCache = new Map();

/**
 * @param {string} platformDomain
 * @param {string} nsPattern
 */
function hostRegex(platformDomain, nsPattern) {
  const key = `${platformDomain}\n${nsPattern}`;
  let re = hostRegexCache.get(key);
  if (!re) {
    re = new RegExp(`^(${nsPattern})\\.${escapeRegex(platformDomain)}$`);
    hostRegexCache.set(key, re);
  }
  return re;
}

// Subdomain branch (<ns>.<PLATFORM_DOMAIN>) vs. pattern branch
// (everything else). Disjoint by host shape — never tried as fallback.
/**
 * @param {string} hostname
 * @param {string} platformDomain
 * @param {string} nsPattern
 */
export function classifyHost(hostname, platformDomain, nsPattern) {
  const normalized = normalizeRequestHost(hostname);
  const m = normalized.match(hostRegex(platformDomain, nsPattern));
  if (m) return { branch: "subdomain", namespace: m[1] };
  return { branch: "pattern", host: normalized };
}

// Returns { sorted, errors }. Malformed entries are dropped so one bad
// write can't black-hole the host; the caller surfaces `errors` via a
// metric / log instead of silent drop.
/**
 * @param {Record<string, unknown>} entries
 * @param {(ns: string) => boolean} isValidRouteNamespace
 */
export function sortPatterns(entries, isValidRouteNamespace) {
  if (typeof isValidRouteNamespace !== "function") {
    throw new TypeError("sortPatterns requires shared isValidRouteNs");
  }
  /** @type {PatternEntry[]} */
  const sorted = [];
  /** @type {PatternError[]} */
  const errors = [];
  for (const [slot, parsed] of Object.entries(entries)) {
    const record = parsed && typeof parsed === "object"
      ? /** @type {Record<string, unknown>} */ (parsed)
      : null;
    if (
      !record ||
      typeof record.ns !== "string" ||
      typeof record.worker !== "string" ||
      typeof record.version !== "string" ||
      (record.kind !== "exact" && record.kind !== "prefix") ||
      typeof record.value !== "string"
    ) {
      errors.push({ slot, reason: "bad_shape" });
      continue;
    }
    if (!isValidRouteNamespace(record.ns)) {
      errors.push({ slot, reason: "bad_namespace" });
      continue;
    }
    sorted.push({
      slot,
      kind: record.kind,
      value: record.value,
      ns: record.ns,
      worker: record.worker,
      version: record.version,
    });
  }
  const sortedRoutes = sorted.toSorted((a, b) => {
    const d = b.value.length - a.value.length;
    if (d !== 0) return d;
    if (a.kind !== b.kind) return a.kind === "exact" ? -1 : 1;
    return 0;
  });
  return { sorted: sortedRoutes, errors };
}

// CF matches against the full URL — exact must reject a non-empty query;
// prefix (trailing `*`) is wildcard by design so query passes through.
// value comes from parsePattern with the trailing `*` stripped — so
// "/api/*" → "/api/" (slash-bounded), "/foo*" → "/foo" (startsWith glob).
/**
 * @param {PatternMatchEntry} entry
 * @param {string} pathname
 * @param {string} [search]
 */
export function matchPatternEntry(entry, pathname, search = "") {
  if (entry.kind === "exact") return pathname === entry.value && search === "";
  return pathname.startsWith(entry.value);
}

/**
 * @param {PatternEntry[]} sorted
 * @param {string} pathname
 * @param {string} [search]
 * @returns {{ entry: PatternEntry | null, comparisons: number }}
 */
export function matchPatternWithStats(sorted, pathname, search = "") {
  let comparisons = 0;
  for (const entry of sorted) {
    comparisons += 1;
    if (matchPatternEntry(entry, pathname, search)) return { entry, comparisons };
  }
  return { entry: null, comparisons };
}
