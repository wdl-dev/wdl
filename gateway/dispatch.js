import {
  SUBDOMAIN_NS_PATTERN,
  WORKER_NAME_RE,
  isReservedNs,
  RESERVED_TENANT_NS,
  ROUTES_ALLOWED_RESERVED_NS,
} from "shared-ns-pattern";
import {
  classifyHost,
  matchPatternWithStats,
} from "gateway-lib";
import {
  ensureKnownNs,
  ensureKnownPatternHosts,
  getCachedNsRoutes,
  loadNsRoutes,
  getCachedPatterns,
  loadPatternsForHost,
  recordPatternMatchComparisons,
  recordRoutingLookup,
} from "gateway-runtime";
import { parseVersion } from "shared-worker-contract";

/**
 * @typedef {{
 *   kind: "not_found",
 *   route: string,
 *   namespace: string | null,
 *   worker: string | null,
 *   version: string | null,
 * }} GatewayNotFoundDispatch
 * @typedef {{
 *   kind: "forward",
 *   route: string,
 *   bindingName: "CONTROL" | "RUNTIME_SYSTEM" | "RUNTIME_USER",
 *   forwardPath: string,
 *   prefix: string,
 *   namespace: string | null,
 *   worker: string | null,
 *   version: string | null,
 * }} GatewayForwardDispatch
 * @typedef {GatewayNotFoundDispatch | GatewayForwardDispatch} GatewayDispatch
 */

/**
 * @param {string} route
 * @param {{ namespace?: string | null, worker?: string | null, version?: string | null }} [context]
 * @returns {GatewayNotFoundDispatch}
 */
export function notFoundDispatch(route, context = {}) {
  return {
    kind: "not_found",
    route,
    namespace: context.namespace ?? null,
    worker: context.worker ?? null,
    version: context.version ?? null,
  };
}

/**
 * @param {string} pathname
 * @returns {{ worker: string, prefix: string, forwardPath: string } | null}
 */
function subdomainPathParts(pathname) {
  const match = /^\/+([^/]+)(?=\/|$)/.exec(pathname);
  if (!match) return null;
  const worker = match[1];
  const suffix = pathname.slice(match[0].length);
  return {
    worker,
    prefix: `/${worker}`,
    forwardPath: suffix || "/",
  };
}

/**
 * @param {{
 *   url: URL,
 *   normalizedHost: string,
 *   normalizedAdminHost: string,
 *   platformDomain: string,
 *   redis: import("shared-redis").RedisClient,
 *   requestId: string,
 * }} options
 * @returns {Promise<GatewayDispatch>}
 */
export async function resolveGatewayDispatch({
  url,
  normalizedHost,
  normalizedAdminHost,
  platformDomain,
  redis,
  requestId,
}) {
  if (normalizedAdminHost && normalizedHost === normalizedAdminHost) {
    return {
      kind: "forward",
      route: "worker_fetch_admin_host",
      bindingName: "CONTROL",
      forwardPath: url.pathname,
      prefix: "/",
      namespace: null,
      worker: null,
      version: null,
    };
  }

  const classified = classifyHost(normalizedHost, platformDomain, SUBDOMAIN_NS_PATTERN);
  let namespace;
  let worker;
  let version;
  let forwardPath;
  let prefix;
  let route;

  if (classified.branch === "subdomain") {
    route = "worker_fetch_subdomain";
    namespace = /** @type {string} */ (classified.namespace);

    // Public data-plane misses intentionally return the same 404 body so
    // callers cannot enumerate namespaces or workers from response details.
    // This reserved-name gate must stay before Redis namespace lookup so
    // hostile reserved hosts don't depend on Redis availability.
    if (isReservedNs(namespace) || RESERVED_TENANT_NS.has(namespace)) {
      return notFoundDispatch(route, { namespace });
    }

    const nsSet = await ensureKnownNs(redis);
    if (!nsSet.has(namespace)) {
      recordRoutingLookup("namespace_gate", "miss");
      return notFoundDispatch(route, { namespace });
    }
    recordRoutingLookup("namespace_gate", "hit");

    const pathParts = subdomainPathParts(url.pathname);
    if (!pathParts) return notFoundDispatch(route, { namespace });

    ({ worker, forwardPath, prefix } = pathParts);
    if (!WORKER_NAME_RE.test(worker)) return notFoundDispatch(route, { namespace, worker });

    let nsRoutes = getCachedNsRoutes(namespace);
    if (nsRoutes) {
      recordRoutingLookup("route_cache", "hit");
    } else {
      recordRoutingLookup("route_cache", "miss");
      nsRoutes = await loadNsRoutes(redis, namespace);
    }
    version = nsRoutes.get(worker);
    if (!version) return notFoundDispatch(route, { namespace, worker });
    if (parseVersion(version) == null) {
      return notFoundDispatch(route, { namespace, worker, version });
    }
  } else {
    route = "worker_fetch_pattern";
    const patternHost = /** @type {string} */ (classified.host);
    const knownPatternHosts = await ensureKnownPatternHosts(redis);
    if (!knownPatternHosts.has(patternHost)) {
      recordRoutingLookup("pattern_host_gate", "miss");
      return notFoundDispatch(route);
    }
    recordRoutingLookup("pattern_host_gate", "hit");

    let sorted = getCachedPatterns(patternHost);
    if (sorted) {
      recordRoutingLookup("pattern_cache", "hit");
    } else {
      recordRoutingLookup("pattern_cache", "miss");
      sorted = await loadPatternsForHost(redis, patternHost, requestId);
    }
    const match = matchPatternWithStats(sorted, url.pathname, url.search);
    recordPatternMatchComparisons(match.entry ? "hit" : "miss", match.comparisons);
    const hit = match.entry;
    if (!hit) return notFoundDispatch(route);

    namespace = hit.ns;
    if ((isReservedNs(namespace) && !ROUTES_ALLOWED_RESERVED_NS.has(namespace)) ||
        RESERVED_TENANT_NS.has(namespace)) {
      return notFoundDispatch(route, {
        namespace,
        worker: hit.worker,
        version: hit.version,
      });
    }
    worker = hit.worker;
    if (!WORKER_NAME_RE.test(worker)) {
      return notFoundDispatch(route, { namespace, worker, version: hit.version });
    }
    if (parseVersion(hit.version) == null) {
      return notFoundDispatch(route, { namespace, worker, version: hit.version });
    }
    version = hit.version;
    forwardPath = url.pathname;
    prefix = hit.slot || "";
  }

  const bindingName = namespace === "__system__" ? "RUNTIME_SYSTEM" : "RUNTIME_USER";
  return {
    kind: "forward",
    route,
    bindingName,
    forwardPath,
    prefix,
    namespace,
    worker,
    version,
  };
}
