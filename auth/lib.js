// Pure helpers for auth/index.js. Stays workerd-IO-free so unit tests
// run under plain node without standing up an isolate.

import {
  isReservedNs,
  isValidTenantNs,
  NS_PATTERN,
  RESERVED_TENANT_NS,
} from "shared-ns-pattern";
import { MAX_TOKEN_HEADER_BYTES, extractToken } from "shared-auth-token";
import { bytesToHex } from "shared-hex";
import { randomHex } from "shared-random-id";
import {
  KNOWN_ACTIONS,
  OPS_KINDS,
  PLATFORM_TIER_RESERVED_NS,
  PRINCIPAL_NS_PLACEHOLDER,
  ROLES,
  matchesAction,
} from "shared-auth-roles";

// Reserved tokenId for the infra-managed bootstrap ops token. issue()
// must never generate or accept this id; revoke() must reject it.
export const BOOTSTRAP_TOKEN_ID = "bootstrap";
const utf8Encoder = new TextEncoder();

// Re-exported for tests that import through auth-lib; sanitizer itself lives in
// shared-auth-token to keep it identical to the copy control uses before handing
// tokens to AUTH.verify.
export { MAX_TOKEN_HEADER_BYTES, extractToken };

export { isValidTenantNs };
export { randomHex };

/**
 * @typedef {{ status: number, reason: string, details?: Record<string, unknown> }} AuthPolicyErrorFields
 * @typedef {{ ok: boolean, reason?: string }} AccessDecision
 * @typedef {{ kind?: unknown, ns?: unknown }} IssueInput
 * @typedef {{ kind?: unknown, ns?: unknown, [key: string]: unknown }} PrincipalRecord
 * @typedef {{
 *   hash: string,
 *   kind: string,
 *   ns?: string,
 *   label?: string,
 *   created_at?: string,
 *   created_by?: string,
 *   expires_at?: string,
 *   revoked_at?: string,
 *   expired_at?: string,
 *   issue_templates?: string,
 *   issue_template?: string,
 *   issue_template_version?: string,
 * }} TokenRecord
 */

// Mirrors the control/routing status + machine-code shape on purpose, but stays
// auth-local: enhanced_error_serialization preserves {status, reason}, and
// control maps that distinct policy contract without importing auth internals.
export class AuthPolicyError extends Error {
  /**
   * @param {number} status
   * @param {string} reason
   * @param {string} message
   * @param {Record<string, unknown>} [details]
   */
  constructor(status, reason, message, details) {
    super(message || reason);
    this.name = "AuthPolicyError";
    /** @type {number} */
    this.status = status;
    /** @type {string} */
    this.reason = reason;
    if (details) {
      /** @type {Record<string, unknown>} */
      this.details = details;
    }
  }
}

/** @param {Uint8Array} bytes */
function base64urlEncode(bytes) {
  return Buffer.from(bytes).toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");
}

// 32 bytes via WebCrypto — Math.random would be a security regression
// even for dev tokens.
export function generatePlaintextToken() {
  const buf = new Uint8Array(32);
  crypto.getRandomValues(buf);
  return base64urlEncode(buf);
}

// Independent randomness from the plaintext: a tokenId leaks via logs /
// list, so it must not narrow the search space for the plaintext.
export function generateTokenId() {
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  const id = base64urlEncode(buf);
  // Vanishingly unlikely 16-byte collision with the literal "bootstrap"
  // string, but if it ever happened the "issue() can't overwrite
  // bootstrap" invariant would silently break.
  if (id === BOOTSTRAP_TOKEN_ID) return generateTokenId();
  return id;
}

// Hex (not base64) so `auth:hash:<…>` keys stay grep-friendly and avoid
// `/` URL hazards if a key ever leaks into a path.
/** @param {string} plaintext */
export async function hashToken(plaintext) {
  const bytes = utf8Encoder.encode(plaintext);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return bytesToHex(new Uint8Array(digest));
}

// Strict ISO-8601 UTC: `YYYY-MM-DDTHH:MM:SS[.fff]Z`. Trailing `Z` is
// required (Date.parse alone silently accepts offsets / partial forms);
// fractional seconds capped at ms because JS Date can't represent finer
// and silent truncation would drift the canonical re-serialization.
const STRICT_ISO_UTC_RE =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,3})?Z$/;
const SHA256_HEX_RE = /^[0-9a-f]{64}$/;
const ISSUE_TEMPLATE_ID_RE = /^[a-z][a-z0-9-]{0,63}$/;
const ISSUE_TEMPLATE_VERSION_RE = /^[A-Za-z0-9._:-]{1,64}$/;
export const MAX_DELEGATED_TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60;
export const MAX_DELEGATED_TOKEN_ACTIVE_QUOTA = 10_000;

/**
 * @typedef {{
 *   id: string,
 *   targetKind: string,
 *   nsGenerator: { prefix: string, randomHexBytes: number },
 *   labelTemplate: string,
 *   ttlSeconds: number,
 *   activeQuota: number,
 *   version: string,
 *   disabled: boolean,
 * }} DelegatedIssueTemplate
 */

export const DELEGATED_ISSUE_TEMPLATES = Object.freeze([
  Object.freeze({
    id: "wdl-chat-ns-pool",
    targetKind: "ns",
    nsGenerator: Object.freeze({
      prefix: "tmp-",
      randomHexBytes: 4,
    }),
    labelTemplate: "workshop-pool {ns}",
    ttlSeconds: 6 * 60 * 60,
    activeQuota: 100,
    version: "1",
    disabled: false,
  }),
  Object.freeze({
    id: "wdl-cli-integration",
    targetKind: "ns",
    nsGenerator: Object.freeze({
      prefix: "cli-it-",
      randomHexBytes: 4,
    }),
    labelTemplate: "cli live integration {ns}",
    ttlSeconds: 60 * 60,
    activeQuota: 50,
    version: "1",
    disabled: false,
  }),
]);

/** @param {string} value */
export function isCanonicalStoredTimestamp(value) {
  if (!STRICT_ISO_UTC_RE.test(value)) return false;
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return false;
  return new Date(ms).toISOString() === value;
}

// Strict ISO-8601 UTC, must be in the future. Returns the canonical
// (re-serialized) string for storage uniformity.
/**
 * @param {unknown} value
 * @param {number} [now]
 */
export function validateExpiresAt(value, now = Date.now()) {
  if (value == null) return undefined;
  if (typeof value !== "string") {
    throw new AuthPolicyError(400, "invalid_expires_at", "expiresAt must be ISO-8601 UTC string");
  }
  const m = STRICT_ISO_UTC_RE.exec(value);
  if (!m) {
    throw new AuthPolicyError(
      400, "invalid_expires_at",
      `expiresAt must be ISO-8601 UTC ending in 'Z' (e.g. 2026-12-31T23:59:59Z), got: ${value}`
    );
  }
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) {
    throw new AuthPolicyError(400, "invalid_expires_at", `expiresAt is not a valid ISO-8601 timestamp: ${value}`);
  }
  // Date.parse silently rolls calendar-invalid values forward
  // (2099-04-31 → 2099-05-01, hour=24 → next day, …). Every UTC
  // component must round-trip; mismatch means the input was not a
  // real moment in time.
  const d = new Date(ms);
  const [yyyy, mm, dd, hh, mi, ss] = m.slice(1).map((s) => Number.parseInt(s, 10));
  if (
    d.getUTCFullYear() !== yyyy ||
    d.getUTCMonth() + 1 !== mm ||
    d.getUTCDate() !== dd ||
    d.getUTCHours() !== hh ||
    d.getUTCMinutes() !== mi ||
    d.getUTCSeconds() !== ss
  ) {
    throw new AuthPolicyError(
      400, "invalid_expires_at",
      `expiresAt is not a real calendar moment (${value} normalized to ${d.toISOString()})`
    );
  }
  if (ms <= now) {
    throw new AuthPolicyError(400, "expired_at_in_past", "expiresAt must be in the future");
  }
  return d.toISOString();
}

/** @param {unknown} value */
function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

/** @param {unknown} id */
function isIssueTemplateId(id) {
  return typeof id === "string" && ISSUE_TEMPLATE_ID_RE.test(id);
}

/** @param {unknown} id */
function assertIssueTemplateId(id) {
  if (!isIssueTemplateId(id)) {
    throw new AuthPolicyError(400, "invalid_template_request",
      "template id must match /^[a-z][a-z0-9-]{0,63}$/");
  }
}

/**
 * Public token-issuer creation accepts camelCase issueTemplates; Redis stores
 * snake_case issue_templates. Keep this parser narrow so API and storage shape
 * cannot be mixed accidentally.
 * @param {unknown} value
 * @returns {string[]}
 */
export function validateIssueTemplatesInput(value) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new AuthPolicyError(400, "invalid_template_request",
      "issueTemplates must be a non-empty array of template ids");
  }
  if (value.length > 32) {
    throw new AuthPolicyError(400, "invalid_template_request",
      "issueTemplates must contain at most 32 template ids");
  }
  /** @type {string[]} */
  const ids = [];
  for (const item of value) {
    assertIssueTemplateId(item);
    if (!ids.includes(item)) ids.push(item);
  }
  return ids;
}

/**
 * @param {unknown} stored
 * @returns {string[]}
 */
export function parseStoredIssueTemplates(stored) {
  if (typeof stored !== "string" || !stored) {
    throw new AuthPolicyError(503, "invalid_issue_templates",
      "issuer token record is missing issue_templates");
  }
  let parsed;
  try {
    parsed = JSON.parse(stored);
  } catch {
    throw new AuthPolicyError(503, "invalid_issue_templates",
      "issuer token record has invalid issue_templates");
  }
  try {
    return validateIssueTemplatesInput(parsed);
  } catch {
    throw new AuthPolicyError(503, "invalid_issue_templates",
      "issuer token record has invalid issue_templates");
  }
}

/**
 * @param {unknown} templates
 * @returns {Map<string, DelegatedIssueTemplate>}
 */
export function createDelegatedIssueTemplateMap(templates = DELEGATED_ISSUE_TEMPLATES) {
  if (!Array.isArray(templates) || templates.length === 0) {
    throw new AuthPolicyError(503, "delegated_issue_misconfigured",
      "delegated issue templates must be a non-empty array");
  }
  /** @type {Map<string, DelegatedIssueTemplate>} */
  const out = new Map();
  for (const item of templates) {
    if (!isPlainObject(item)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        "delegated issue templates must be objects");
    }
    const template = /** @type {Record<string, unknown>} */ (item);
    if (!isIssueTemplateId(template.id)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        "delegated issue template id is invalid");
    }
    const id = /** @type {string} */ (template.id);
    if (out.has(id)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `duplicate delegated issue template "${id}"`);
    }
    if (typeof template.targetKind !== "string" ||
        !Object.hasOwn(ROLES, template.targetKind) ||
        template.targetKind === "ops" ||
        template.targetKind === "ops-observer" ||
        template.targetKind === "token-issuer" ||
        ROLES[template.targetKind].boundNsKind !== "tenant") {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" has invalid targetKind`);
    }
    if (!isPlainObject(template.nsGenerator)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" must define nsGenerator`);
    }
    const generator = /** @type {Record<string, unknown>} */ (template.nsGenerator);
    if (typeof generator.prefix !== "string" || !generator.prefix) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" nsGenerator.prefix must be non-empty`);
    }
    const randomHexBytes = generator.randomHexBytes;
    if (typeof randomHexBytes !== "number" ||
        !Number.isInteger(randomHexBytes) ||
        randomHexBytes < 1 ||
        randomHexBytes > 16) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" nsGenerator.randomHexBytes must be 1..16`);
    }
    const sampleNs = generator.prefix + "0".repeat(randomHexBytes * 2);
    if (!isValidTenantNs(sampleNs)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" generated namespace shape is invalid`);
    }
    if (typeof template.labelTemplate !== "string" ||
        !template.labelTemplate ||
        template.labelTemplate.length > 128 ||
        !template.labelTemplate.includes("{ns}")) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" labelTemplate must include {ns}`);
    }
    const maxLabel = template.labelTemplate.replaceAll("{ns}", sampleNs);
    if (maxLabel.length > 128) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" labelTemplate renders over 128 chars`);
    }
    const ttlSeconds = template.ttlSeconds;
    if (typeof ttlSeconds !== "number" ||
        !Number.isInteger(ttlSeconds) ||
        ttlSeconds < 1 ||
        ttlSeconds > MAX_DELEGATED_TOKEN_TTL_SECONDS) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" ttlSeconds is outside the server limit`);
    }
    const activeQuota = template.activeQuota;
    if (typeof activeQuota !== "number" ||
        !Number.isInteger(activeQuota) ||
        activeQuota < 1 ||
        activeQuota > MAX_DELEGATED_TOKEN_ACTIVE_QUOTA) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" activeQuota is outside the server limit`);
    }
    const version = template.version === undefined ? "1" : template.version;
    if (typeof version !== "string" || !ISSUE_TEMPLATE_VERSION_RE.test(version)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        `template "${id}" version is invalid`);
    }
    out.set(id, {
      id,
      targetKind: template.targetKind,
      nsGenerator: {
        prefix: generator.prefix,
        randomHexBytes,
      },
      labelTemplate: template.labelTemplate,
      ttlSeconds,
      activeQuota,
      version,
      disabled: template.disabled === true,
    });
  }
  return out;
}

/**
 * @param {string} templateId
 * @param {Map<string, DelegatedIssueTemplate>} [configured]
 */
export function resolveDelegatedIssueTemplate(templateId, configured = createDelegatedIssueTemplateMap()) {
  const template = configured.get(templateId);
  if (!template) {
    throw new AuthPolicyError(400, "invalid_template",
      `template "${templateId}" does not exist`);
  }
  if (template.disabled) {
    throw new AuthPolicyError(400, "template_disabled",
      `template "${templateId}" is disabled`);
  }
  return template;
}

/**
 * @param {DelegatedIssueTemplate} template
 * @param {string} ns
 */
export function renderDelegatedIssueLabel(template, ns) {
  return template.labelTemplate.replaceAll("{ns}", ns);
}

/**
 * @param {TokenRecord | null | undefined} record
 * @param {number} [now]
 */
export function isExpired(record, now = Date.now()) {
  if (!record || !record.expires_at) return false;
  const ms = Date.parse(record.expires_at);
  // Unparseable persisted expiry is rejected by validateRecordShape().
  // Keep this predicate side-effect-free for list/revoke helpers.
  if (!Number.isFinite(ms)) return false;
  return ms <= now;
}

/** @param {TokenRecord | null | undefined} record */
export function isRevoked(record) {
  return Boolean(record && record.revoked_at);
}

// Fail-closed access decision. Red lines fire before ROLES lookup so a
// misconfigured kind / unknown action / drifted classifier output can't
// silently take a more permissive path.
//
// Red line ordering matters:
//   0  unknown_role       — kind ∉ ROLES (auth contract violation, 503)
//   1  reserved-ns gate   — non-PLATFORM_TIER reserved-ns: ops-class only
//   2  RESERVED_TENANT_NS — strict ops only
//   3  unknown action     — strict ops only (else dispatcher 404 leaks)
//   4  auth.token.*       — strict ops only (lifecycle is attack surface)
//   5  system.*           — strict ops only (full-platform write)
//   ROLES.namespaces × ROLES.actions — final scope check
/**
 * @param {{ action: unknown, ns: unknown, kind: unknown, principalNs?: unknown }} input
 * @returns {AccessDecision}
 */
export function evaluateAccess({ action, ns, kind, principalNs }) {
  // Red line 0: ordering matters — a bogus kind hitting __system__ must
  // surface as unknown_role (503), not be masked as reserved_ns_requires_ops.
  if (typeof kind !== "string" || !Object.hasOwn(ROLES, kind)) {
    return { ok: false, reason: "unknown_role" };
  }

  // Red line 1: PLATFORM_TIER members fall through (their ROLES entry pins
  // record.ns); ops-observer also falls through so its read-only actions
  // list narrows it (writes to __system__ etc. land at action_not_in_scope).
  if (typeof ns === "string" && isReservedNs(ns) && !PLATFORM_TIER_RESERVED_NS.has(ns)) {
    if (kind === "ops") return { ok: true };
    if (!OPS_KINDS.has(kind)) {
      return { ok: false, reason: "reserved_ns_requires_ops" };
    }
  }

  // Red line 2: RESERVED_TENANT_NS rejects ops-observer too — reserved
  // tenant names are naming-policy placeholders with no resources to observe.
  if (typeof ns === "string" && RESERVED_TENANT_NS.has(ns)) {
    return kind === "ops"
      ? { ok: true }
      : { ok: false, reason: "reserved_tenant_ns_requires_ops" };
  }

  // Red line 3: must precede prefix red lines below so a typo like
  // `auth.token.foo` doesn't get absorbed by red line 4.
  if (typeof action !== "string" || !KNOWN_ACTIONS.has(action)) {
    return kind === "ops"
      ? { ok: true }
      : { ok: false, reason: "unknown_action_requires_ops" };
  }

  // Self-introspection has no resource namespace. Any valid role may ask
  // who it is, but this action must not grant access to any tenant object.
  if (action === "diagnostic.whoami") {
    return { ok: true };
  }

  if (action.startsWith("auth.token.")) {
    return kind === "ops"
      ? { ok: true }
      : { ok: false, reason: "auth_lifecycle_requires_ops" };
  }

  // Red line 5: prefix (not literal "system.reload") so future system
  // actions auto-cover.
  if (action.startsWith("system.")) {
    return kind === "ops"
      ? { ok: true }
      : { ok: false, reason: "system_action_requires_ops" };
  }

  const role = ROLES[kind];
  // If principalNs is undefined and namespaces contains $bound, the
  // substituted allowedNs contains undefined and `.includes(<string>)`
  // returns false → ns_not_in_scope. Automatic fail-closed defense.
  const allowedNs = role.namespaces.map((s) =>
    s === PRINCIPAL_NS_PLACEHOLDER ? principalNs : s
  );
  const nsOk = allowedNs.includes("*") ||
               (typeof ns === "string" && allowedNs.includes(ns));
  if (!nsOk) return { ok: false, reason: "ns_not_in_scope" };

  const actionOk = role.actions.some((pat) => matchesAction(pat, action));
  if (!actionOk) return { ok: false, reason: "action_not_in_scope" };

  return { ok: true };
}

// `kind === undefined` defaults to "ns" (back-compat with body
// {ns, label}). `kind === null` is malformed — `=== undefined` (not
// `== null`) draws the line between "field absent" and "explicitly null".
/**
 * @param {IssueInput} input
 * @returns {{ kind: string, ns: string | undefined }}
 */
export function validateIssueInput({ kind, ns }) {
  const effectiveKind = kind === undefined ? "ns" : kind;

  if (effectiveKind === "ops") {
    throw new AuthPolicyError(400, "ops_not_issuable",
      "ops tokens are bootstrap-only");
  }
  if (typeof effectiveKind !== "string" ||
      !Object.hasOwn(ROLES, effectiveKind)) {
    throw new AuthPolicyError(400, "unknown_role",
      `unknown role "${effectiveKind}"`);
  }

  const role = ROLES[effectiveKind];

  switch (role.boundNsKind) {
    case "tenant":
      assertTenantNs(ns);
      return { kind: effectiveKind, ns: /** @type {string} */ (ns) };
    case "platform-tier":
      // Set check, not assertTenantNs — the latter rejects reserved-ns
      // shape outright; also catches `__system__`-shaped attempts.
      if (typeof ns !== "string" || !PLATFORM_TIER_RESERVED_NS.has(ns)) {
        throw new AuthPolicyError(400, "invalid_bound_ns",
          `role "${effectiveKind}" must bind to a platform-tier reserved ns ` +
          `(one of: ${[...PLATFORM_TIER_RESERVED_NS].join(", ")})`);
      }
      return { kind: effectiveKind, ns };
    case "none":
      // `!== undefined` not `Object.hasOwn` — handlers build {ns: body.ns, ...}
      // which creates an own key with undefined when body.ns is absent.
      if (ns !== undefined) {
        throw new AuthPolicyError(400, "role_no_ns",
          `role "${effectiveKind}" is not namespace-bound; ns must not be set`);
      }
      return { kind: effectiveKind, ns: undefined };
    default:
      // 503 (not 400/unknown_role) — this is a code bug, not user input drift.
      throw new AuthPolicyError(503, "invalid_role_config",
        `role "${effectiveKind}" has invalid boundNsKind ` +
        `"${role.boundNsKind}" (expected one of: tenant, platform-tier, none)`);
  }
}

// Defensive verify-side check on the persisted record. Catches drifted
// data (migrations, restored backups, buggy issuer) before it's promoted
// to a principal.
/**
 * @param {unknown} record
 * @returns {AccessDecision}
 */
export function validateRecordShape(record) {
  if (!record || typeof record !== "object") {
    return { ok: false, reason: "unknown_role" };
  }
  const persisted = /** @type {PrincipalRecord} */ (record);
  const role = typeof persisted.kind === "string" ? ROLES[persisted.kind] : undefined;
  if (!role) return { ok: false, reason: "unknown_role" };
  if (typeof persisted.hash !== "string" || !SHA256_HEX_RE.test(persisted.hash)) {
    return { ok: false, reason: "invalid_record_hash" };
  }
  for (const field of ["created_at", "expires_at", "revoked_at", "expired_at"]) {
    const value = /** @type {Record<string, unknown>} */ (persisted)[field];
    if (value !== undefined && (typeof value !== "string" || !isCanonicalStoredTimestamp(value))) {
      return { ok: false, reason: "invalid_record_timestamp" };
    }
  }

  switch (role.boundNsKind) {
    case "tenant":
      return isValidTenantNs(persisted.ns)
        ? { ok: true }
        : { ok: false, reason: "invalid_record_ns" };

    case "platform-tier":
      if (typeof persisted.ns !== "string" || !PLATFORM_TIER_RESERVED_NS.has(persisted.ns)) {
        return { ok: false, reason: "invalid_record_ns" };
      }
      return { ok: true };

    case "none":
      // parseTokenRecord normalizes "" / missing → undefined; any string
      // here means a none-bound record was issued with an ns.
      if (persisted.ns !== undefined) {
        return { ok: false, reason: "invalid_record_ns" };
      }
      if (persisted.kind === "token-issuer") {
        try {
          parseStoredIssueTemplates(persisted.issue_templates);
        } catch {
          return { ok: false, reason: "invalid_issue_templates" };
        }
      }
      return { ok: true };

    default:
      // Plain Error (not AuthPolicyError) so verify's catch maps it to
      // verify_threw rather than minting a new public reason.
      throw new Error(
        `validateRecordShape: invalid boundNsKind "${role.boundNsKind}" ` +
        `on role "${persisted.kind}"`
      );
  }
}

/** @param {unknown} ns */
export function assertTenantNs(ns) {
  if (isValidTenantNs(ns)) return;
  // Distinguish "field absent" (missing_ns) from "field present but wrong
  // type" (invalid_ns). Both 400, but the reason drives different operator
  // remediation: "you forgot to send ns" vs "your serializer sent a number".
  if (ns === undefined || ns === null || ns === "") {
    throw new AuthPolicyError(400, "missing_ns", "ns is required");
  }
  if (typeof ns !== "string") {
    throw new AuthPolicyError(400, "invalid_ns",
      `ns must be a string, got ${typeof ns}`);
  }
  if (isReservedNs(ns)) {
    throw new AuthPolicyError(400, "reserved_ns", `ns "${ns}" is reserved (ops-only)`);
  }
  if (RESERVED_TENANT_NS.has(ns)) {
    throw new AuthPolicyError(400, "reserved_tenant_ns",
      `ns "${ns}" is a reserved tenant name`);
  }
  throw new AuthPolicyError(400, "invalid_ns", `ns must match ${NS_PATTERN}, got "${ns}"`);
}

/**
 * @param {unknown} hash
 * @returns {TokenRecord | null}
 */
export function parseTokenRecord(hash) {
  if (!hash || typeof hash !== "object") return null;
  const record = /** @type {Record<string, unknown>} */ (hash);
  if (!record.kind) return null;
  return {
    hash: typeof record.hash === "string" ? record.hash : "",
    kind: String(record.kind),
    ns: typeof record.ns === "string" && record.ns ? record.ns : undefined,
    label: typeof record.label === "string" && record.label ? record.label : undefined,
    created_at: typeof record.created_at === "string" && record.created_at ? record.created_at : undefined,
    created_by: typeof record.created_by === "string" && record.created_by ? record.created_by : undefined,
    expires_at: typeof record.expires_at === "string" && record.expires_at ? record.expires_at : undefined,
    revoked_at: typeof record.revoked_at === "string" && record.revoked_at ? record.revoked_at : undefined,
    expired_at: typeof record.expired_at === "string" && record.expired_at ? record.expired_at : undefined,
    issue_templates: typeof record.issue_templates === "string" && record.issue_templates ? record.issue_templates : undefined,
    issue_template: typeof record.issue_template === "string" && record.issue_template ? record.issue_template : undefined,
    issue_template_version: typeof record.issue_template_version === "string" && record.issue_template_version ? record.issue_template_version : undefined,
  };
}

/**
 * @param {string} tokenId
 * @param {TokenRecord} record
 */
export function recordToListEntry(tokenId, record) {
  /** @type {{
   *   tokenId: string,
   *   kind: string,
   *   createdAt: string | null,
   *   ns?: string,
   *   label?: string,
   *   createdBy?: string,
   *   expiresAt?: string,
   *   revokedAt?: string,
   *   expiredAt?: string,
   *   issueTemplates?: string[],
   *   issueTemplatesInvalid?: true,
   *   issueTemplate?: string,
   *   issueTemplateVersion?: string,
   * }}
   */
  const out = {
    tokenId,
    kind: record.kind,
    createdAt: record.created_at || null,
  };
  if (record.ns) out.ns = record.ns;
  if (record.label) out.label = record.label;
  if (record.created_by) out.createdBy = record.created_by;
  if (record.expires_at) out.expiresAt = record.expires_at;
  if (record.revoked_at) out.revokedAt = record.revoked_at;
  if (record.expired_at) out.expiredAt = record.expired_at;
  if (record.kind === "token-issuer") {
    try {
      out.issueTemplates = parseStoredIssueTemplates(record.issue_templates);
    } catch {
      out.issueTemplatesInvalid = true;
    }
  }
  if (record.issue_template) out.issueTemplate = record.issue_template;
  if (record.issue_template_version) out.issueTemplateVersion = record.issue_template_version;
  return out;
}
