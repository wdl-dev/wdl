// Lifecycle Redis key helpers, bundle metadata parsing, referrer-index
// encode/decode, URL → {gate, ns} classifier, and secret-key validation.
// Pure data-shaping only.

import {
  NS_PATTERN,
  RESERVED_OBJECT_KEYS,
  isReservedNs,
  RESERVED_TENANT_NS,
  WDL_RESERVED_BINDING_RE,
} from "shared-ns-pattern";
import { decodePatternProjection } from "shared-route-projection";
import { PLATFORM_TIER_RESERVED_NS, ROLES } from "shared-auth-roles";
import { errorMessage } from "shared-errors";
export const NS_RE = new RegExp(`^${NS_PATTERN}$`);
export const MAX_QUEUE_DELAY_SECONDS = 86_400;
const WORKERD_DEPENDENCY_VERSION_RE = /^1\.(\d{4})(\d{2})(\d{2})\.(\d+)$/;

/**
 * @typedef {{ callerNs: string, callerWorker: string, callerVersion: string, binding: string }} ReferrerMember
 * @typedef {{ kind?: string, ns?: string }} AccessPrincipal
 * @typedef {{ referrers: ReferrerMember[], crossNamespaceReferrerCount?: number }} ReferrerBlocker
 * @typedef {{ namespace: string, worker: string, version: string, message: string, reason: string, cause: unknown }} BundleMetaFailure
 */

export class BundleMetaError extends Error {
  /** @param {{ namespace: string, worker: string, version: string, message: string, cause: unknown }} failure */
  constructor({ namespace, worker, version, message, cause }) {
    super(message, { cause });
    this.name = "BundleMetaError";
    this.status = 500;
    this.code = "corrupt_meta";
    this.details = { namespace, worker, version };
  }
}

/**
 * @param {unknown} raw
 * @param {{ ns: string, worker: string, version: string, makeError?: (failure: BundleMetaFailure) => Error }} options
 * @returns {Record<string, unknown>}
 */
export function parseBundleMeta(raw, { ns, worker, version, makeError }) {
  /** @param {unknown} cause @returns {never} */
  const fail = (cause) => {
    const failure = {
      namespace: ns,
      worker,
      version,
      message: `Corrupt __meta__ for ${ns}/${worker}/${version}`,
      reason: errorMessage(cause),
      cause,
    };
    throw makeError ? makeError(failure) : new BundleMetaError(failure);
  };

  if (typeof raw !== "string") {
    fail(new TypeError("__meta__ must be a JSON string"));
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    fail(new SyntaxError("__meta__ is not valid JSON"));
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    fail(new TypeError("__meta__ must be a JSON object"));
  }
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {unknown} raw
 * @param {{ host: string, slot: string, makeError: (details: { host: string, slot: string }) => Error }} options
 */
export function parsePatternProjection(raw, { host, slot, makeError }) {
  const projection = decodePatternProjection(raw);
  if (!projection) throw makeError({ host, slot });
  return projection;
}

/**
 * @param {Record<string, unknown>} meta
 * @returns {string | null}
 */
export function bundleAssetPrefix(meta) {
  const assets = meta.assets;
  if (!assets || typeof assets !== "object" || Array.isArray(assets)) return null;
  const prefix = /** @type {Record<string, unknown>} */ (assets).prefix;
  return typeof prefix === "string" ? prefix : null;
}

// Relaxes NS_PATTERN to also accept reserved `__<x>__` ns (so JSRPC-only
// reserved-ns flows can deploy / declare hosts), then re-tightens to
// reject RESERVED_TENANT_NS (which NS_RE would otherwise pass).
/** @param {unknown} ns */
export function isAdminAcceptableNs(ns) {
  if (typeof ns !== "string") return false;
  if (RESERVED_TENANT_NS.has(ns)) return false;
  return isReservedNs(ns) || NS_RE.test(ns);
}

/** @param {AccessPrincipal | null | undefined} principal */
export function projectAccessPrincipal(principal) {
  if (!principal || typeof principal.kind !== "string") return null;
  const role = ROLES[principal.kind];
  if (!role) return null;
  if (role.boundNsKind === "none") {
    return { kind: principal.kind };
  }
  if (
    (role.boundNsKind === "tenant" || role.boundNsKind === "platform-tier") &&
    typeof principal.ns === "string"
  ) {
    return { kind: principal.kind, ns: principal.ns };
  }
  return null;
}

/** @param {unknown} value */
export function configuredPublicUrl(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  let parsed;
  try {
    parsed = new URL(value.trim());
  } catch {
    return null;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
  if (parsed.username || parsed.password) return null;
  parsed.search = "";
  parsed.hash = "";
  return parsed.href.replace(/\/+$/, "");
}

/**
 * @param {string} source
 * @returns {{ version: string, year: number, month: number, day: number, patch: number } | null}
 */
export function parseWorkerdDependencyVersion(source) {
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    return null;
  }
  const raw = parsed?.dependencies?.workerd;
  if (typeof raw !== "string") return null;
  const version = raw.replace(/^[~^]/, "");
  const match = WORKERD_DEPENDENCY_VERSION_RE.exec(version);
  if (!match) return null;
  return {
    version,
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
    patch: Number(match[4]),
  };
}

/** @param {string} source */
export function platformVersionFromPackageJson(source) {
  const parsed = parseWorkerdDependencyVersion(source);
  return parsed ? `wdl.${parsed.version.slice(2)}` : "wdl.unknown";
}

// ─── Referrer index ────────────────────────────────────────────────

// Alphabetical key order so SADD / SREM of the same logical referrer
// produce identical bytes regardless of caller-side property-insertion
// order — critical for the reverse-index invariant.
/** @param {ReferrerMember} referrer */
export function encodeReferrerMember({ callerNs, callerWorker, callerVersion, binding }) {
  if (typeof callerNs !== "string" || !callerNs)
    throw new Error("encodeReferrerMember: callerNs required");
  if (typeof callerWorker !== "string" || !callerWorker)
    throw new Error("encodeReferrerMember: callerWorker required");
  if (typeof callerVersion !== "string" || !callerVersion)
    throw new Error("encodeReferrerMember: callerVersion required");
  if (typeof binding !== "string" || !binding)
    throw new Error("encodeReferrerMember: binding required");
  return JSON.stringify({
    binding,
    callerNs,
    callerVersion,
    callerWorker,
  });
}

// Returns null for malformed entries so callers can skip rather than
// crash on a single corrupt record.
/**
 * @param {unknown} raw
 * @returns {ReferrerMember | null}
 */
export function decodeReferrerMember(raw) {
  if (typeof raw !== "string") return null;
  let obj;
  try { obj = JSON.parse(raw); } catch { return null; }
  if (!obj || typeof obj !== "object") return null;
  if (typeof obj.callerNs !== "string") return null;
  if (typeof obj.callerWorker !== "string") return null;
  if (typeof obj.callerVersion !== "string") return null;
  if (typeof obj.binding !== "string") return null;
  return {
    callerNs: obj.callerNs,
    callerWorker: obj.callerWorker,
    callerVersion: obj.callerVersion,
    binding: obj.binding,
  };
}

// Cross-ns referrer details collapse into a count so a non-ops token can't
// enumerate other namespaces via the error surface. Platform principals
// get the full list ONLY when target ns matches their bound ns — the
// double pin (kind === "platform" AND targetNs === principal.ns).
/**
 * @param {Iterable<string>} rawMembers
 * @param {{ targetNs: string, principal?: AccessPrincipal | null }} options
 * @returns {ReferrerBlocker}
 */
export function formatReferrerBlocker(rawMembers, { targetNs, principal }) {
  const parsed = [];
  for (const raw of rawMembers) {
    const r = decodeReferrerMember(raw);
    if (r) parsed.push(r);
  }
  if (principal && principal.kind === "ops") {
    return { referrers: parsed };
  }
  if (principal && principal.kind === "platform" &&
      typeof principal.ns === "string" &&
      PLATFORM_TIER_RESERVED_NS.has(principal.ns) &&
      targetNs === principal.ns) {
    return { referrers: parsed };
  }
  const sameNs = [];
  let crossCount = 0;
  for (const r of parsed) {
    if (r.callerNs === targetNs) sameNs.push(r);
    else crossCount += 1;
  }
  /** @type {ReferrerBlocker} */
  const out = { referrers: sameNs };
  if (crossCount > 0) out.crossNamespaceReferrerCount = crossCount;
  return out;
}

/** @param {Iterable<string>} rawMembers */
export function formatD1ReferrerBlockers(rawMembers) {
  const blockers = [];
  let malformedReferrerCount = 0;
  for (const raw of rawMembers) {
    const ref = decodeReferrerMember(raw);
    if (!ref) {
      malformedReferrerCount += 1;
      continue;
    }
    blockers.push({
      worker: ref.callerWorker,
      version: ref.callerVersion,
      binding: ref.binding,
    });
  }
  const sortedBlockers = blockers.toSorted((a, b) =>
    a.worker.localeCompare(b.worker) ||
    a.version.localeCompare(b.version) ||
    a.binding.localeCompare(b.binding)
  );
  return { blockers: sortedBlockers, malformedReferrerCount };
}

// Same-ns entries ARE included: the referrer index is authoritative;
// same-ns bypass is a response-layer concern.
/**
 * @param {unknown} metaBindings
 * @param {string} callerNs
 */
export function extractOutgoingRefs(metaBindings, callerNs) {
  if (!metaBindings || typeof metaBindings !== "object") return [];
  const out = [];
  for (const [bindingName, spec] of Object.entries(metaBindings)) {
    if (!spec || typeof spec !== "object") continue;
    if (spec.type !== "service") continue;
    if (typeof spec.service !== "string" || !spec.service) continue;
    if (typeof spec.version !== "string" || !spec.version) continue;
    const targetNs = typeof spec.ns === "string" && spec.ns ? spec.ns : callerNs;
    out.push({
      targetNs,
      targetWorker: spec.service,
      targetVersion: spec.version,
      binding: bindingName,
    });
  }
  return out;
}

/** @param {unknown} metaBindings */
export function extractD1Refs(metaBindings) {
  if (!metaBindings || typeof metaBindings !== "object") return [];
  const out = [];
  for (const [bindingName, spec] of Object.entries(metaBindings)) {
    if (!spec || typeof spec !== "object") continue;
    if (spec.type !== "d1") continue;
    const databaseId = spec.databaseId;
    if (typeof databaseId !== "string" || !databaseId) continue;
    out.push({ binding: bindingName, databaseId });
  }
  return out;
}

// ─── Lifecycle Redis keys ──────────────────────────────────────────

/** @param {string} ns @param {string} worker @param {string} version */
export function referrersKey(ns, worker, version) {
  return `worker-version-referrers:${ns}:${worker}:${version}`;
}

/** @param {string} ns */
export function workersIndexKey(ns) {
  return `workers:${ns}`;
}

/** @param {string} ns @param {string} worker */
export function workflowDefsKey(ns, worker) {
  return `wf:defs:${ns}:${worker}`;
}

/** @param {string} ns */
export function d1DatabasesKey(ns) {
  return `d1:databases:${ns}`;
}

/** @param {string} ns @param {string} databaseId */
export function d1DatabaseKey(ns, databaseId) {
  return `d1:database:${ns}:${databaseId}`;
}

/** @param {string} ns @param {string} databaseName */
export function d1DatabaseNameKey(ns, databaseName) {
  return `d1:database-name:${ns}:${databaseName}`;
}

/** @param {string} ns @param {string} databaseId */
export function d1DatabaseReferrersKey(ns, databaseId) {
  return `d1:database-referrers:${ns}:${databaseId}`;
}

/** @param {string} ns @param {string} databaseId */
export function d1DatabaseTombstoneKey(ns, databaseId) {
  return `d1:database-tombstone:${ns}:${databaseId}`;
}

/** @param {string} ns */
export function d1DatabaseTombstonesKey(ns) {
  return `d1:database-tombstones:${ns}`;
}

/** @param {string} storageId */
export function doObjectRegistryKey(storageId) {
  return `do:objects:${encodeURIComponent(storageId)}`;
}

// ─── Control URL parser ────────────────────────────────────────────

/**
 * @template {Record<string, unknown> | null} T
 * @param {T} route
 * @param {string | null} action
 */
function withAction(route, action) {
  return action ? { ...route, action } : route;
}

// URL → canonical control route. `action` is intentionally absent unless
// length × method × verb match an authorized shape exactly; known dispatch
// prefixes may still return `kind`/params so ops reaches dispatcher/handler
// method gates while non-ops hit the unknown-action red line. Reserved-ns /
// RESERVED_TENANT_NS rules belong to evaluateAccess.
/** @param {string} pathname @param {string} method */
export function parseControlRoute(pathname, method) {
  const segs = pathname.split("/").filter(Boolean);

  if (pathname === "/reload") {
    return withAction({
      kind: "reload",
      scopeRoute: "reload",
    }, method === "POST" ? "system.reload" : null);
  }

  if (pathname === "/whoami") {
    return withAction({
      kind: "whoami",
      scopeRoute: "whoami",
    }, method === "GET" ? "diagnostic.whoami" : null);
  }

  // /auth/tokens, /auth/tokens/<id>
  if (segs[0] === "auth" && segs[1] === "tokens") {
    const route = {
      kind: "authTokens",
      scopeRoute: "auth_tokens",
      tokenId: segs.length === 3 ? segs[2] : undefined,
    };
    if (segs.length === 2) {
      if (method === "POST") return withAction(route, "auth.token.issue");
      if (method === "GET")  return withAction(route, "auth.token.list");
      return route;
    }
    if (segs.length === 3 && method === "DELETE") {
      return withAction(route, "auth.token.revoke");
    }
    return route;
  }

  // /auth/delegated-tokens
  if (segs[0] === "auth" && segs[1] === "delegated-tokens" && segs.length === 2) {
    const route = {
      kind: "authDelegatedTokens",
      scopeRoute: "auth_delegated_tokens",
    };
    if (method === "POST") {
      return withAction(route, "auth.delegated_token.issue");
    }
    return route;
  }

  // /ns/<ns>/...
  if (segs[0] === "ns" && segs[1]) {
    const ns = segs[1];

    // /ns/<ns>/workers
    if (segs.length === 3 && segs[2] === "workers") {
      return withAction({
        kind: "workers",
        scopeRoute: "workers",
        ns,
      }, method === "GET" ? "worker.list" : null);
    }

    // /ns/<ns>/workflows[/<worker>/<workflow>/instances[/<id>[/<action>]]]
    if (segs[2] === "workflows") {
      const route = {
        kind: "workflows",
        scopeRoute: "workflows",
        ns,
        subPath: segs.slice(3),
      };
      if (segs.length === 3) {
        return withAction(route, method === "GET" ? "workflow.list" : null);
      }
      if (segs.length === 6 && segs[5] === "instances") {
        return withAction(route, method === "GET" ? "workflow.read" : null);
      }
      if (segs.length === 7 && segs[5] === "instances") {
        return withAction(route, method === "GET" ? "workflow.read" : null);
      }
      if (segs.length === 8 && segs[5] === "instances" &&
          ["pause", "resume", "restart", "terminate"].includes(segs[7])) {
        return withAction(route, method === "POST" ? "workflow.write" : null);
      }
      return route;
    }

    // /ns/<ns>/secrets[/<key>] — only the three meaningful shapes.
    // GET /ns/<ns>/secrets/<key> doesn't exist (no read-secret-value API)
    // so it falls through to red line 3.
    if (segs[2] === "secrets") {
      const route = {
        kind: "nsSecrets",
        scopeRoute: "ns_secrets",
        ns,
        secretKey: segs.length === 4 ? segs[3] : undefined,
      };
      if (segs.length === 3 && method === "GET")    return withAction(route, "secret.read");
      if (segs.length === 4 && method === "PUT")    return withAction(route, "secret.write");
      if (segs.length === 4 && method === "DELETE") return withAction(route, "secret.delete");
      if (segs.length === 3 || segs.length === 4) return route;
    }

    if (segs.length === 3 && segs[2] === "hosts") {
      const route = {
        kind: "hosts",
        scopeRoute: "hosts",
        ns,
      };
      if (method === "GET")  return withAction(route, "host.read");
      if (method === "POST") return withAction(route, "host.write");
      return route;
    }

    // /ns/<ns>/logs/<verb> — keep the verb dispatch grouped so future
    // /logs/<other> shapes (e.g. list, stats) slot in here without
    // reshaping the whole `ns` branch.
    if (segs[2] === "logs") {
      if (segs.length === 4 && segs[3] === "tail") {
        return withAction({
          kind: "logsTail",
          scopeRoute: "logs_tail",
          ns,
        }, method === "GET" ? "worker.logs.tail" : null);
      }
    }

    // /ns/<ns>/d1[/databases[/<id>[/{query,migrations[/{status,apply}]}]]]
    if (segs[2] === "d1") {
      const route = {
        kind: "d1",
        scopeRoute: "d1",
        ns,
        subPath: segs.slice(3),
      };
      if (segs[3] !== "databases") {
        return route;
      }
      if (segs.length === 4) {
        if (method === "GET")  return withAction(route, "d1.list");
        if (method === "POST") return withAction(route, "d1.create");
        return route;
      }
      if (segs.length === 5 && method === "DELETE") {
        return withAction(route, "d1.delete");
      }
      if (segs.length === 6 && segs[5] === "query" && method === "POST") {
        return withAction(route, "d1.execute");
      }
      if (segs.length === 6 && segs[5] === "migrations" && method === "GET") {
        return withAction(route, "d1.migrate.read");
      }
      if (segs.length === 7 && segs[5] === "migrations" && method === "POST") {
        // status is read-only despite POST (handler reads body for the
        // migration manifest); apply is the actual write.
        if (segs[6] === "status") return withAction(route, "d1.migrate.read");
        if (segs[6] === "apply")  return withAction(route, "d1.migrate.write");
      }
      return route;
    }

    // /ns/<ns>/r2/buckets[/<bucket>/objects[/<key...>]]
    if (segs[2] === "r2") {
      const dispatch = parseR2DispatchPath(pathname);
      const route = dispatch
        ? {
            kind: "r2",
            scopeRoute: "r2",
            ns,
            subPath: dispatch.subPath,
          }
        : null;
      if (segs.length === 4 && segs[3] === "buckets" && method === "GET") {
        return withAction(route, "r2.bucket.list");
      }
      if (dispatch?.subPath[0] === "buckets" &&
          dispatch.subPath[1] &&
          dispatch.subPath[2] === "objects") {
        const hasObjectKey = dispatch.subPath.length > 3;
        if (method === "HEAD" && hasObjectKey) {
          return withAction(route, "r2.object.head");
        }
        if (method === "GET") {
          return withAction(route, hasObjectKey ? "r2.object.get" : "r2.object.list");
        }
        if (method === "DELETE" && hasObjectKey) {
          return withAction(route, "r2.object.delete");
        }
      }
      if (route) return route;
    }

    // /ns/<ns>/worker/<name>/<action>[/<v>]
    if (segs[2] === "worker" && segs[3]) {
      const verb = segs[4];
      const route = verb
        ? {
            kind: "worker",
            scopeRoute: verb,
            ns,
            worker: segs[3],
            workerAction: verb,
            subPath: segs.slice(5),
          }
        : null;
      if (segs.length === 5) {
        if (verb === "deploy"   && method === "POST") return withAction(route, "worker.deploy");
        if (verb === "promote"  && method === "POST") return withAction(route, "worker.promote");
        if (verb === "delete"   && method === "POST") return withAction(route, "worker.delete");
        if (verb === "versions" && method === "GET")  return withAction(route, "worker.versions.read");
        if (verb === "secrets"  && method === "GET")  return withAction(route, "secret.read");
        if (["deploy", "promote", "delete", "versions", "secrets"].includes(verb)) return route;
      }
      if (segs.length === 6 && verb === "versions" && method === "DELETE") {
        return withAction(route, "worker.versions.delete");
      }
      if (segs.length === 6 && verb === "secrets") {
        if (method === "PUT")    return withAction(route, "secret.write");
        if (method === "DELETE") return withAction(route, "secret.delete");
      }
      if (route) return route;
    }
  }

  return {};
}

/** @param {string} pathname */
export function parseR2DispatchPath(pathname) {
  const parts = String(pathname).split("/");
  if (parts[0] !== "" || parts[1] !== "ns" || !parts[2] || parts[3] !== "r2") {
    return null;
  }
  return {
    ns: parts[2],
    // Object keys are S3 keys, not URL paths. Preserve empty path segments so
    // admin get/delete can address keys like "a//b" and "a/" correctly.
    subPath: parts.slice(4),
  };
}

// ─── Log tail resume id ────────────────────────────────────────────

// Redis stream id grammar: `<ms>-<seq>`, both decimal integers ≥ 0.
// Sentinels (`$`, `+`, `-`, `>`) are intentionally rejected — they would
// either silently mean "from now" (defeating resume) or surface as Redis
// protocol errors. An explicit "from now" is the absence of any resume
// input, not a literal `$`.
// Redis stream ids are two decimal halves. Cap both components before
// compareStreamIds parses them as BigInt; 20 digits covers uint64 ids
// without allowing large decimal strings to burn CPU.
const RESUME_ID_RE = /^(0|[1-9][0-9]{0,19})-(0|[1-9][0-9]{0,19})$/;

/** @param {unknown} value */
export function isValidResumeId(value) {
  return typeof value === "string" && RESUME_ID_RE.test(value);
}

// Lexicographic Redis stream id compare — `<ms>-<seq>`. Use BigInt for the
// `<seq>` half so trim-detection stays correct past Number's 2^53
// precision bound (a single ms with > 9e15 entries is improbable in
// practice but the cost is one BigInt parse per id).
/** @param {string} a @param {string} b */
export function compareStreamIds(a, b) {
  const [ams, aseq] = a.split("-");
  const [bms, bseq] = b.split("-");
  const amsB = BigInt(ams);
  const bmsB = BigInt(bms);
  if (amsB !== bmsB) return amsB < bmsB ? -1 : 1;
  const aseqB = BigInt(aseq);
  const bseqB = BigInt(bseq);
  if (aseqB !== bseqB) return aseqB < bseqB ? -1 : 1;
  return 0;
}

// ─── Secret key grammar ────────────────────────────────────────────

const SECRET_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** @param {unknown} key */
export function validateSecretKey(key) {
  if (typeof key !== "string" || !SECRET_KEY_RE.test(key)) {
    throw new Error(
      `secret key must match ${SECRET_KEY_RE} (env-var grammar), got ${JSON.stringify(key)}`
    );
  }
  if (WDL_RESERVED_BINDING_RE.test(key)) {
    throw new Error("secret key is reserved for runtime-internal bindings");
  }
  if (RESERVED_OBJECT_KEYS.has(key)) {
    throw new Error("secret key is a reserved Object.prototype key");
  }
  if (key.length > 128) throw new Error("secret key too long (max 128)");
}
