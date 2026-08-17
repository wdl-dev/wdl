// Single source of truth for the auth role model. Imported by both auth/
// (which evaluates verify + issue against ROLES) and control/ (which calls
// validatePrincipalShape on the verify allow payload). Pure data + helpers,
// no IO, so unit tests exercise it directly.

import { isValidTenantNs, PLATFORM_TIER_RESERVED_NS } from "shared-ns-pattern";

/**
 * @typedef {"tenant" | "platform-tier" | "none"} BoundNsKind
 * @typedef {{ actions: string[], namespaces: string[], boundNsKind: BoundNsKind }} Role
 */

// Sentinel inside ROLES.<role>.namespaces — substituted at evaluateAccess
// time with the principal's bound ns (record.ns). Picked specifically to
// not match any legal ns / `*` literal so accidental string equality with
// a real ns name is impossible.
export const PRINCIPAL_NS_PLACEHOLDER = "$bound";

export { PLATFORM_TIER_RESERVED_NS };

// "ops-class" kinds that bypass red line 1 (reserved-ns gate). `ops-observer`
// only bypasses — it still falls through to ROLES.ops-observer for action
// + namespace check, where the read-only `actions` list rejects writes.
export const OPS_KINDS = new Set(["ops", "ops-observer"]);

// Authoritative action vocabulary. classifier output must be ∈ this set
// (red line 3 rejects unknowns); ROLES.<role>.actions string literals
// (non-wildcard) must be ∈ this set (sanity test asserts).
export const KNOWN_ACTIONS = new Set([
  "diagnostic.whoami",
  "worker.deploy", "worker.promote", "worker.delete",
  "worker.versions.read", "worker.versions.delete", "worker.list",
  "worker.logs.tail",
  "workflow.list", "workflow.read", "workflow.write",
  "secret.read", "secret.write", "secret.delete",
  "d1.list", "d1.create", "d1.delete",
  "d1.migrate.read", "d1.migrate.write", "d1.execute",
  "r2.bucket.list", "r2.object.list", "r2.object.head", "r2.object.get", "r2.object.delete",
  "ai.provider.read", "ai.provider.write", "ai.model.list",
  "host.read", "host.write",
  "auth.token.issue", "auth.token.revoke", "auth.token.list",
  "auth.delegated_token.issue",
  "system.reload",
]);

// Strict pattern matcher. Supports:
//   - "*"               → matches any action
//   - "<prefix>.*"      → matches "<prefix>.<single-segment>" (no nested dots)
//   - literal           → exact string compare
// Deliberately no infix glob (no "worker.*.read") — every cross-cutting
// read must be listed explicitly so adding `worker.deploy.dry-run` later
// doesn't silently flow into existing roles.
/**
 * @param {unknown} pattern
 * @param {unknown} action
 * @returns {boolean}
 */
export function matchesAction(pattern, action) {
  if (typeof pattern !== "string" || typeof action !== "string") return false;
  if (pattern === "*") return true;
  if (pattern.endsWith(".*")) {
    const prefix = pattern.slice(0, -2);
    if (!action.startsWith(prefix + ".")) return false;
    const rest = action.slice(prefix.length + 1);
    return !rest.includes(".");
  }
  return pattern === action;
}

// `boundNsKind` decides issue-time ns validation:
//   "tenant"        → ns must pass isValidTenantNs (record.ns is tenant)
//   "platform-tier" → ns must ∈ PLATFORM_TIER_RESERVED_NS
//   "none"          → ns must be absent (own key not present)
// And evaluateAccess uses ROLES.<role>.namespaces to decide allowed scope:
//   [PRINCIPAL_NS_PLACEHOLDER] → tenant/platform-tier (substituted at eval)
//   ["*"]                      → cross-ns (none-bound)
// The (namespaces, boundNsKind) pair must self-cohere — sanity test asserts.
/** @type {Record<string, Role>} */
export const ROLES = {
  ops: {
    actions: ["*"],
    namespaces: ["*"],
    boundNsKind: "none",
  },
  // Cross-ns observer. No secret.read (cross-tenant blast radius); no
  // auth.token.list (red line 4 prevails anyway, kept off the role for
  // representational consistency); no d1.execute (arbitrary SQL); no
  // writes. red line 1 lets it bypass reserved-ns then ROLES.actions
  // shrinks it to read-only.
  "ops-observer": {
    actions: [
      "worker.list", "worker.versions.read",
      "workflow.list",
      "d1.list", "d1.migrate.read",
      "r2.bucket.list", "r2.object.list",
      "ai.provider.read", "ai.model.list",
      "host.read",
    ],
    namespaces: ["*"],
    boundNsKind: "none",
  },
  // Platform-tier admin (deploy + secret + d1 within bound ns). Bound to
  // record.ns ∈ PLATFORM_TIER_RESERVED_NS at issue time. host.* deliberately
  // omitted — platform-tier ns are not in ROUTES_ALLOWED_RESERVED_NS, so
  // hosts have no semantics there today; promoting one in later requires
  // an explicit ROLES change to grant host.write.
  platform: {
    actions: [
      "worker.deploy", "worker.promote", "worker.delete",
      "worker.versions.read", "worker.versions.delete", "worker.list",
      "worker.logs.tail",
      "workflow.list", "workflow.read", "workflow.write",
      "secret.read", "secret.write", "secret.delete",
      "d1.list", "d1.create", "d1.delete",
      "d1.migrate.read", "d1.migrate.write",
      "d1.execute",
      "r2.bucket.list", "r2.object.list", "r2.object.head", "r2.object.get", "r2.object.delete",
      "ai.provider.read", "ai.provider.write", "ai.model.list",
    ],
    namespaces: [PRINCIPAL_NS_PLACEHOLDER],
    boundNsKind: "platform-tier",
  },
  "platform-observer": {
    actions: [
      "worker.versions.read", "worker.list", "worker.logs.tail",
      "workflow.list",
      "secret.read",
      "d1.list", "d1.migrate.read",
      "r2.bucket.list", "r2.object.list",
      "ai.provider.read", "ai.model.list",
    ],
    namespaces: [PRINCIPAL_NS_PLACEHOLDER],
    boundNsKind: "platform-tier",
  },
  // Tenant. Explicit list (not actions:["*"]) so future actions don't
  // auto-grant. host.write deliberately omitted — host declaration is
  // operator-driven (see CLAUDE.md), enforced as missing-from-list rather
  // than a hard red line so a future "network admin" role can opt in.
  ns: {
    actions: [
      "worker.deploy", "worker.promote", "worker.delete",
      "worker.versions.read", "worker.versions.delete", "worker.list",
      "worker.logs.tail",
      "workflow.list", "workflow.read", "workflow.write",
      "secret.read", "secret.write", "secret.delete",
      "d1.list", "d1.create", "d1.delete",
      "d1.migrate.read", "d1.migrate.write",
      "d1.execute",
      "r2.bucket.list", "r2.object.list", "r2.object.head", "r2.object.get", "r2.object.delete",
      "ai.provider.read", "ai.provider.write", "ai.model.list",
      "host.read",
    ],
    namespaces: [PRINCIPAL_NS_PLACEHOLDER],
    boundNsKind: "tenant",
  },
  // Narrow credential issuer. It cannot list/revoke/direct-issue tokens;
  // auth/index.js re-reads the issuer record and its issue_templates allowlist
  // before materializing any target token.
  "token-issuer": {
    actions: ["auth.delegated_token.issue"],
    namespaces: ["*"],
    boundNsKind: "none",
  },
};

// Action → log category. Full action also stays in log fields; the category is
// only a scan-friendly grouping for auth_verify records.
//
// `auth_token` is special-cased on the exact `auth.token.` prefix (not
// derived from the leading dot-segment like the others), so classifier
// drift like "auth.foo" lands in `unknown` instead of being silently
// absorbed into auth_token — the divergence is the signal.
/**
 * @param {unknown} action
 * @returns {string}
 */
export function actionCategory(action) {
  if (typeof action !== "string") return "unknown";
  if (action.startsWith("auth.token.")) return "auth_token";
  if (action.startsWith("auth.delegated_token.")) return "auth_delegated_token";
  const dot = action.indexOf(".");
  if (dot < 0) return "unknown";
  const prefix = action.slice(0, dot);
  return ["diagnostic", "worker", "workflow", "secret", "d1", "r2", "ai", "host", "system"].includes(prefix)
    ? prefix
    : "unknown";
}

// Last-mile shape check on the AUTH allow payload — formatReferrerBlocker's
// double-pin uses principal.ns directly as authority. `none`-bound
// principals must not even have an `ns` own key: JSRPC preserves own-key
// existence across the boundary, so {kind, ns: undefined} arrives with
// the key present and we want to reject that.
/**
 * @param {unknown} principal
 * @returns {boolean}
 */
export function validatePrincipalShape(principal) {
  if (!principal || typeof principal !== "object") return false;
  const record = /** @type {{ kind?: unknown, ns?: unknown }} */ (principal);
  if (typeof record.kind !== "string") return false;
  const role = ROLES[record.kind];
  if (!role) return false;
  switch (role.boundNsKind) {
    case "tenant":
      return isValidTenantNs(record.ns);
    case "platform-tier":
      return typeof record.ns === "string" &&
             PLATFORM_TIER_RESERVED_NS.has(record.ns);
    case "none":
      return !Object.hasOwn(principal, "ns");
    default:
      return false;
  }
}
