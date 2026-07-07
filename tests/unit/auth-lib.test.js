import { test } from "node:test";
import assert from "node:assert/strict";
import { loadAuthLib } from "../helpers/load-auth-lib.js";
import { RESERVED_NS } from "../../shared/ns-pattern.js";

const { authLib, sharedAuthRoles } = await loadAuthLib();
const {
  KNOWN_ACTIONS,
  PLATFORM_TIER_RESERVED_NS,
  PRINCIPAL_NS_PLACEHOLDER,
  ROLES,
  actionCategory,
  matchesAction,
  validatePrincipalShape,
} = sharedAuthRoles;
const {
  AuthPolicyError,
  BOOTSTRAP_TOKEN_ID,
  DELEGATED_ISSUE_TEMPLATES,
  MAX_TOKEN_HEADER_BYTES,
  assertTenantNs,
  createDelegatedIssueTemplateMap,
  isValidTenantNs,
  evaluateAccess,
  validateIssueInput,
  validateRecordShape,
  extractToken,
  generatePlaintextToken,
  generateTokenId,
  hashToken,
  isExpired,
  isRevoked,
  parseStoredIssueTemplates,
  parseTokenRecord,
  recordToListEntry,
  renderDelegatedIssueLabel,
  resolveDelegatedIssueTemplate,
  validateExpiresAt,
  validateIssueTemplatesInput,
} = authLib;

const VALID_TOKEN_HASH = "a".repeat(64);

/** @param {Record<string, unknown>} fields */
function persistedRecord(fields) {
  return { hash: VALID_TOKEN_HASH, ...fields };
}

/** @param {string} ns */
function addPlatformTierFixture(ns) {
  RESERVED_NS.add(ns);
  PLATFORM_TIER_RESERVED_NS.add(ns);
}

/** @param {string} ns */
function deletePlatformTierFixture(ns) {
  PLATFORM_TIER_RESERVED_NS.delete(ns);
  RESERVED_NS.delete(ns);
}

// --- token generation -------------------------------------------------------

test("generatePlaintextToken returns ~43-char base64url string", () => {
  const t = generatePlaintextToken();
  assert.equal(typeof t, "string");
  assert.ok(/^[A-Za-z0-9_-]+$/.test(t), `not base64url: ${t}`);
  // 32 bytes → 43 base64 chars (no padding).
  assert.equal(t.length, 43);
});

test("generatePlaintextToken is non-deterministic", () => {
  const set = new Set();
  for (let i = 0; i < 100; i++) set.add(generatePlaintextToken());
  assert.equal(set.size, 100);
});

test("generateTokenId returns ~22-char base64url and never \"bootstrap\"", () => {
  for (let i = 0; i < 200; i++) {
    const id = generateTokenId();
    assert.ok(/^[A-Za-z0-9_-]+$/.test(id));
    assert.equal(id.length, 22);
    assert.notEqual(id, BOOTSTRAP_TOKEN_ID);
  }
});

test("hashToken yields stable lowercase hex SHA-256", async () => {
  const h = await hashToken("abc");
  // RFC 6234 SHA-256("abc") well-known fixture.
  assert.equal(
    h,
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
  );
  assert.equal(h, await hashToken("abc"));
  assert.notEqual(h, await hashToken("abcd"));
});

// --- header hygiene ---------------------------------------------------------

test("extractToken accepts trimmed simple value", () => {
  const headers = new Headers({ "x-admin-token": "  hello-world  " });
  assert.equal(extractToken(headers), "hello-world");
});

test("extractToken returns null on missing / empty / control-char / multi-value / too long", () => {
  // Use plain objects so we can include values (\n, \t) that Node's
  // strict undici-backed Headers constructor rejects outright.
  const cases = [
    {}, // no header
    { "x-admin-token": "" },
    { "x-admin-token": "   " },
    { "x-admin-token": "valid,extra" },              // multi-value join
    { "x-admin-token": "tab\there" },                 // control char (\t)
    { "x-admin-token": "newline\nhere" },             // control char
    { "x-admin-token": "x".repeat(MAX_TOKEN_HEADER_BYTES + 1) },
  ];
  for (const c of cases) {
    assert.equal(extractToken(c), null,
      `case ${JSON.stringify(c)} should be dirty`);
  }
});

test("extractToken accepts plain object via bracket-access fallback", () => {
  assert.equal(extractToken({ "x-admin-token": "ok" }), "ok");
});

// --- expiresAt validation ---------------------------------------------------

test("validateExpiresAt rejects past + invalid input + non-string", () => {
  const future = new Date(Date.now() + 60_000).toISOString();
  assert.equal(validateExpiresAt(undefined), undefined);
  assert.equal(validateExpiresAt(null), undefined);

  // Round-trip canonical ISO.
  const canonical = validateExpiresAt(future);
  assert.equal(typeof canonical, "string");
  assert.equal(new Date(canonical).toISOString(), canonical);

  for (const bad of ["", "tomorrow", "2024-13-01T00:00:00Z", 12345, {}]) {
    assert.throws(() => validateExpiresAt(bad), AuthPolicyError);
  }
  assert.throws(
    () => validateExpiresAt(new Date(Date.now() - 60_000).toISOString()),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "expired_at_in_past"
  );
});

test("validateExpiresAt requires strict UTC (trailing Z) — rejects offset and naive forms", () => {
  // All of these are valid ISO-8601 datetimes Date.parse would accept,
  // but the doc contract says "ISO-8601 UTC". Strict-Z is the marker.
  for (const bad of [
    "2099-01-01T00:00:00+08:00",
    "2099-01-01T00:00:00-00:00",
    "2099-01-01T00:00:00",            // no timezone
    "2099-01-01",                     // date-only
    "2099-01-01T00:00:00.000+00:00",  // explicit zero offset, still not Z
    "2099-01-01T00:00:00z",           // lowercase z
  ]) {
    assert.throws(
      () => validateExpiresAt(bad),
      (err) =>
        err instanceof AuthPolicyError &&
        /** @type {any} */ (err).reason === "invalid_expires_at",
      `should reject: ${bad}`
    );
  }
  // Strict-Z accepted; sub-second precision up to ms allowed.
  assert.equal(typeof validateExpiresAt("2099-01-01T00:00:00Z"), "string");
  assert.equal(typeof validateExpiresAt("2099-01-01T00:00:00.123Z"), "string");

  // Sub-millisecond precision rejected — JS Date silently truncates to
  // ms, so accepting it would mean caller's `.1234Z` round-trips as
  // `.123Z` and the canonical form drifts.
  for (const tooPrecise of [
    "2099-01-01T00:00:00.1234Z",       // 4 digits
    "2099-01-01T00:00:00.123456Z",     // 6 digits (microsecond)
    "2099-01-01T00:00:00.123456789Z",  // 9 digits (nanosecond)
  ]) {
    assert.throws(
      () => validateExpiresAt(tooPrecise),
      (err) =>
        err instanceof AuthPolicyError &&
        /** @type {any} */ (err).reason === "invalid_expires_at",
      `should reject sub-ms precision: ${tooPrecise}`
    );
  }
});

test("validateExpiresAt rejects calendar-invalid moments that Date.parse silently rolls forward", () => {
  // Cases where the regex shape passes but Date.parse normalizes
  // forward — e.g. April only has 30 days, hour 24 = next day, etc.
  for (const bad of [
    "2099-04-31T00:00:00Z",  // April 31 → May 1
    "2099-02-30T00:00:00Z",  // Feb 30 → Mar 2
    "2099-13-01T00:00:00Z",  // Month 13 → next year Jan
    "2099-00-15T00:00:00Z",  // Month 00 → previous year Dec
    "2099-01-32T00:00:00Z",  // Day 32 → next month
    "2099-01-01T24:00:00Z",  // Hour 24 → next day
    "2099-01-01T00:60:00Z",  // Minute 60 → next hour
    "2099-01-01T00:00:60Z",  // Second 60 → next minute (also leap-second-like)
    "2099-02-29T00:00:00Z",  // 2099 is not a leap year → Mar 1
  ]) {
    assert.throws(
      () => validateExpiresAt(bad),
      (err) =>
        err instanceof AuthPolicyError &&
        /** @type {any} */ (err).reason === "invalid_expires_at",
      `should reject calendar-invalid: ${bad}`
    );
  }
  // 2096 IS a leap year — Feb 29 must pass.
  assert.equal(typeof validateExpiresAt("2096-02-29T00:00:00Z"), "string");
});

test("isExpired uses <= now", () => {
  const past = { expires_at: new Date(Date.now() - 1000).toISOString() };
  const future = { expires_at: new Date(Date.now() + 60_000).toISOString() };
  const none = {};
  assert.equal(isExpired(past), true);
  assert.equal(isExpired(future), false);
  assert.equal(isExpired(none), false);
});

// --- evaluateAccess: fixture matrix -----------------------------------------

// (kind, action, ns, principalNs) → {ok|reason} table. Each row exercises
// one red line or ROLES check exit. principalNs is implicit for kind=ns /
// platform-class (set to test ns) unless overridden.
const EXPECT_OK = { ok: true };
/** @param {string} reason */
function E(reason) { return { ok: false, reason }; }

const ACCESS_CASES = [
  // ops: blanket access
  { kind: "ops", action: "worker.deploy", ns: "tenant-foo", expect: EXPECT_OK },
  { kind: "ops", action: "diagnostic.whoami", ns: undefined, expect: EXPECT_OK },
  { kind: "ops", action: "auth.token.issue", ns: undefined, expect: EXPECT_OK },
  { kind: "ops", action: "auth.delegated_token.issue", ns: undefined, expect: EXPECT_OK },
  { kind: "ops", action: "system.reload", ns: undefined, expect: EXPECT_OK },
  { kind: "ops", action: "anything-not-known", ns: "tenant-foo", expect: EXPECT_OK },

  // ops-observer: cross-ns observer; reserved-ns red line bypasses but
  // ROLES.actions read-only list narrows it
  { kind: "ops-observer", action: "worker.versions.read", ns: "tenant-foo", expect: EXPECT_OK },
  { kind: "ops-observer", action: "diagnostic.whoami", ns: undefined, expect: EXPECT_OK },
  { kind: "ops-observer", action: "worker.versions.read", ns: "__platform__", expect: EXPECT_OK },
  { kind: "ops-observer", action: "worker.versions.read", ns: "__system__", expect: EXPECT_OK },
  { kind: "ops-observer", action: "workflow.list", ns: "tenant-foo", expect: EXPECT_OK },
  { kind: "ops-observer", action: "workflow.read", ns: "tenant-foo", expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "r2.bucket.list", ns: "tenant-foo", expect: EXPECT_OK },
  { kind: "ops-observer", action: "r2.object.head", ns: "tenant-foo", expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "r2.object.get", ns: "tenant-foo", expect: E("action_not_in_scope") },
  // KEY regression: ops-observer must NOT short-circuit red line 1 — falls
  // through to ROLES.actions, where worker.deploy is missing.
  { kind: "ops-observer", action: "worker.deploy", ns: "__system__", expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "worker.deploy", ns: "tenant-foo", expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "secret.read", ns: "tenant-foo", expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "auth.token.list", ns: undefined, expect: E("auth_lifecycle_requires_ops") },
  { kind: "ops-observer", action: "auth.delegated_token.issue", ns: undefined, expect: E("action_not_in_scope") },
  { kind: "ops-observer", action: "system.reload", ns: undefined, expect: E("system_action_requires_ops") },
  { kind: "ops-observer", action: "host.read", ns: "admin", expect: E("reserved_tenant_ns_requires_ops") },

  // platform: bound to record.ns; access only own ns
  { kind: "platform", action: "worker.deploy", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform", action: "diagnostic.whoami", ns: undefined, principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform", action: "worker.logs.tail", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform", action: "worker.deploy", ns: "tenant-foo", principalNs: "__platform__", expect: E("ns_not_in_scope") },
  { kind: "platform", action: "secret.write", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  // host.write is deliberately not in ROLES.platform → action_not_in_scope
  { kind: "platform", action: "host.write", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },
  { kind: "platform", action: "auth.token.issue", ns: undefined, principalNs: "__platform__", expect: E("auth_lifecycle_requires_ops") },
  { kind: "platform", action: "system.reload", ns: undefined, principalNs: "__platform__", expect: E("system_action_requires_ops") },

  // platform-observer: bound, read-only
  { kind: "platform-observer", action: "worker.versions.read", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform-observer", action: "diagnostic.whoami", ns: undefined, principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform-observer", action: "worker.logs.tail", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform-observer", action: "workflow.list", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform-observer", action: "workflow.read", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },
  { kind: "platform-observer", action: "r2.object.list", ns: "__platform__", principalNs: "__platform__", expect: EXPECT_OK },
  { kind: "platform-observer", action: "r2.object.head", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },
  { kind: "platform-observer", action: "r2.object.get", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },
  { kind: "platform-observer", action: "secret.write", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },
  { kind: "platform-observer", action: "d1.execute", ns: "__platform__", principalNs: "__platform__", expect: E("action_not_in_scope") },

  // ns: tenant token
  { kind: "ns", action: "worker.deploy", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "diagnostic.whoami", ns: undefined, principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "worker.logs.tail", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "r2.object.head", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "r2.object.get", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "r2.object.delete", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "host.read", ns: "tenant-foo", principalNs: "tenant-foo", expect: EXPECT_OK },
  { kind: "ns", action: "host.write", ns: "tenant-foo", principalNs: "tenant-foo", expect: E("action_not_in_scope") },
  { kind: "ns", action: "worker.deploy", ns: "tenant-bar", principalNs: "tenant-foo", expect: E("ns_not_in_scope") },
  // tenant ns hitting a platform-tier ns: red line 1 doesn't fire (it's
  // PLATFORM_TIER_RESERVED_NS member), falls into ROLES check where
  // principalNs ≠ ns → ns_not_in_scope.
  { kind: "ns", action: "worker.deploy", ns: "__platform__", principalNs: "tenant-foo", expect: E("ns_not_in_scope") },
  // Other reserved ns: red line 1 fires.
  { kind: "ns", action: "worker.deploy", ns: "__system__", principalNs: "tenant-foo", expect: E("reserved_ns_requires_ops") },
  { kind: "ns", action: "worker.deploy", ns: "admin", principalNs: "tenant-foo", expect: E("reserved_tenant_ns_requires_ops") },
  { kind: "ns", action: "auth.token.issue", ns: undefined, principalNs: "tenant-foo", expect: E("auth_lifecycle_requires_ops") },
  { kind: "ns", action: "auth.delegated_token.issue", ns: undefined, principalNs: "tenant-foo", expect: E("ns_not_in_scope") },
  { kind: "ns", action: "system.reload", ns: undefined, principalNs: "tenant-foo", expect: E("system_action_requires_ops") },
  { kind: "ns", action: "unknown-action", ns: "tenant-foo", principalNs: "tenant-foo", expect: E("unknown_action_requires_ops") },

  // token-issuer: delegated issue only.
  { kind: "token-issuer", action: "auth.delegated_token.issue", ns: undefined, expect: EXPECT_OK },
  { kind: "token-issuer", action: "auth.token.issue", ns: undefined, expect: E("auth_lifecycle_requires_ops") },
  { kind: "token-issuer", action: "worker.deploy", ns: "tenant-foo", expect: E("action_not_in_scope") },

  // unknown role: red line 0 must FIRE FIRST regardless of scenario
  { kind: "bogus", action: "worker.deploy", ns: "tenant-foo", expect: E("unknown_role") },
  { kind: "bogus", action: "worker.deploy", ns: "__system__", expect: E("unknown_role") },
  { kind: "bogus", action: "worker.deploy", ns: "admin", expect: E("unknown_role") },
  { kind: "bogus", action: "auth.token.list", ns: undefined, expect: E("unknown_role") },
  { kind: "bogus", action: "system.reload", ns: undefined, expect: E("unknown_role") },
  { kind: "bogus", action: "unknown-action", ns: "tenant-foo", expect: E("unknown_role") },
];

for (const c of ACCESS_CASES) {
  const principalNs = c.principalNs !== undefined ? c.principalNs : c.ns;
  const label = `evaluateAccess: kind=${c.kind} action=${c.action} ns=${c.ns ?? "-"} → ${c.expect.ok ? "ok" : Reflect.get(c.expect, "reason")}`;
  test(label, () => {
    const result = evaluateAccess({
      action: c.action,
      ns: c.ns,
      kind: c.kind,
      principalNs,
    });
    if (c.expect.ok) {
      assert.equal(result.ok, true,
        `expected ok, got reason=${Reflect.get(result, "reason")}`);
    } else {
      assert.equal(result.ok, false,
        `expected reject, got ok`);
      assert.equal(Reflect.get(result, "reason"), Reflect.get(c.expect, "reason"));
    }
  });
}

test("evaluateAccess: __pf2__ fixture — platform principal accesses own ns", () => {
  addPlatformTierFixture("__pf2__");
  try {
    const result = evaluateAccess({
      action: "worker.deploy", ns: "__pf2__", kind: "platform", principalNs: "__pf2__",
    });
    assert.equal(result.ok, true);
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("evaluateAccess: __pf2__ fixture — platform principal cannot reach __platform__", () => {
  addPlatformTierFixture("__pf2__");
  try {
    const result = evaluateAccess({
      action: "worker.deploy", ns: "__platform__", kind: "platform", principalNs: "__pf2__",
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, "ns_not_in_scope");
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

// --- matchesAction: pattern grammar -----------------------------------------

test("matchesAction: '*' matches anything", () => {
  assert.equal(matchesAction("*", "worker.deploy"), true);
  assert.equal(matchesAction("*", "anything"), true);
});

test("matchesAction: '<prefix>.*' matches single segment, NOT nested", () => {
  assert.equal(matchesAction("worker.*", "worker.deploy"), true);
  assert.equal(matchesAction("worker.*", "worker.list"), true);
  // Nested action: rejected (single-layer wildcard is intentional).
  assert.equal(matchesAction("worker.*", "worker.versions.read"), false);
  assert.equal(matchesAction("auth.token.*", "auth.token.issue"), true);
  assert.equal(matchesAction("auth.token.*", "auth.token.foo.bar"), false);
});

test("matchesAction: literal compare is exact", () => {
  assert.equal(matchesAction("worker.deploy", "worker.deploy"), true);
  assert.equal(matchesAction("worker.deploy", "worker.deployx"), false);
  assert.equal(matchesAction("worker.deploy", "worker.depl"), false);
});

// --- validateIssueInput matrix ----------------------------------------------

test("validateIssueInput: kind=undefined defaults to 'ns'", () => {
  const out = validateIssueInput({ kind: undefined, ns: "tenant-foo" });
  assert.deepEqual(out, { kind: "ns", ns: "tenant-foo" });
});

test("validateIssueInput: kind=null is invalid (NOT defaulted to 'ns')", () => {
  // Helper uses === undefined, NOT == null. Ensures explicit null fails
  // rather than silently defaulting to ns.
  assert.throws(
    () => validateIssueInput({ kind: null, ns: "tenant-foo" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "unknown_role"
  );
});

test("validateIssueInput: kind=ns + tenant ns ok", () => {
  const out = validateIssueInput({ kind: "ns", ns: "tenant-foo" });
  assert.deepEqual(out, { kind: "ns", ns: "tenant-foo" });
});

test("validateIssueInput: kind=ns + reserved-ns rejected (assertTenantNs path)", () => {
  assert.throws(
    () => validateIssueInput({ kind: "ns", ns: "__platform__" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "reserved_ns"
  );
});

test("validateIssueInput: kind=ns + admin rejected (RESERVED_TENANT_NS)", () => {
  assert.throws(
    () => validateIssueInput({ kind: "ns", ns: "admin" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "reserved_tenant_ns"
  );
});

test("validateIssueInput: kind=ns + missing ns rejected", () => {
  assert.throws(
    () => validateIssueInput({ kind: "ns", ns: undefined }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "missing_ns"
  );
});

test("validateIssueInput: kind=platform + __platform__ ok", () => {
  const out = validateIssueInput({ kind: "platform", ns: "__platform__" });
  assert.deepEqual(out, { kind: "platform", ns: "__platform__" });
});

test("validateIssueInput: kind=platform + tenant ns rejected (invalid_bound_ns)", () => {
  assert.throws(
    () => validateIssueInput({ kind: "platform", ns: "tenant-foo" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_bound_ns"
  );
});

test("validateIssueInput: kind=platform + __system__ rejected (set check, not just reserved ns)", () => {
  // Critical: __system__ IS reserved but is NOT in
  // PLATFORM_TIER_RESERVED_NS. If the helper used isReservedNs alone,
  // this would slip through.
  assert.throws(
    () => validateIssueInput({ kind: "platform", ns: "__system__" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_bound_ns"
  );
});

test("validateIssueInput: kind=platform + missing ns rejected", () => {
  assert.throws(
    () => validateIssueInput({ kind: "platform", ns: undefined }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_bound_ns"
  );
});

test("validateIssueInput: kind=platform + null ns rejected", () => {
  assert.throws(
    () => validateIssueInput({ kind: "platform", ns: null }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_bound_ns"
  );
});

test("validateIssueInput: kind=platform-observer + __platform__ ok", () => {
  const out = validateIssueInput({ kind: "platform-observer", ns: "__platform__" });
  assert.deepEqual(out, { kind: "platform-observer", ns: "__platform__" });
});

test("validateIssueInput: kind=platform with __pf2__ fixture ok (set-driven)", () => {
  addPlatformTierFixture("__pf2__");
  try {
    const out = validateIssueInput({ kind: "platform", ns: "__pf2__" });
    assert.deepEqual(out, { kind: "platform", ns: "__pf2__" });
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("validateIssueInput: kind=ops-observer + ns=undefined ok (handler literal {ns: body.ns} pattern)", () => {
  // Critical edge case: handler builds {kind: body.kind, ns: body.ns, ...}
  // — if body.ns is missing, ns is `undefined` BUT the own key exists.
  // validateIssueInput must use === undefined (NOT Object.hasOwn) so this
  // shape is accepted as "user did not specify ns".
  const out = validateIssueInput({ kind: "ops-observer", ns: undefined });
  assert.deepEqual(out, { kind: "ops-observer", ns: undefined });
});

test("validateIssueInput: kind=ops-observer + ns set rejected (role_no_ns)", () => {
  for (const ns of ["tenant-foo", "__platform__"]) {
    assert.throws(
      () => validateIssueInput({ kind: "ops-observer", ns }),
      (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "role_no_ns"
    );
  }
});

test("validateIssueInput: kind=ops-observer + null ns rejected (null !== undefined)", () => {
  assert.throws(
    () => validateIssueInput({ kind: "ops-observer", ns: null }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "role_no_ns"
  );
});

test("validateIssueInput: kind=token-issuer is none-bound", () => {
  assert.deepEqual(validateIssueInput({ kind: "token-issuer", ns: undefined }), {
    kind: "token-issuer",
    ns: undefined,
  });
  assert.throws(
    () => validateIssueInput({ kind: "token-issuer", ns: "tenant-foo" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "role_no_ns"
  );
});

test("validateIssueInput: kind=ops rejected (bootstrap-only)", () => {
  assert.throws(
    () => validateIssueInput({ kind: "ops" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "ops_not_issuable"
  );
});

test("validateIssueInput: unknown-kind rejected", () => {
  assert.throws(
    () => validateIssueInput({ kind: "unknown-role", ns: "x" }),
    (err) => err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "unknown_role"
  );
});

test("validateIssueInput: bogus boundNsKind triggers invalid_role_config (default branch)", () => {
  ROLES["__test_bogus__"] = { actions: [], namespaces: ["*"], boundNsKind: "bogus" };
  try {
    /** @type {any} */
    let threw;
    try {
      validateIssueInput({ kind: "__test_bogus__", ns: undefined });
    } catch (err) { threw = err; }
    assert.ok(threw instanceof AuthPolicyError, `expected AuthPolicyError, got ${threw}`);
    assert.equal(threw.status, 503);
    assert.equal(threw.reason, "invalid_role_config");
  } finally {
    delete ROLES["__test_bogus__"];
  }
});

test("delegated issue templates are code-defined and render labels", () => {
  const templates = createDelegatedIssueTemplateMap();
  const chat = templates.get("wdl-chat-ns-pool");
  const cli = templates.get("wdl-cli-integration");
  assert.ok(chat);
  assert.ok(cli);
  assert.deepEqual(
    DELEGATED_ISSUE_TEMPLATES.map((/** @type {{ id: string }} */ t) => t.id),
    ["wdl-chat-ns-pool", "wdl-cli-integration"]
  );
  assert.equal(chat.version, "1");
  assert.equal(chat.disabled, false);
  assert.equal(renderDelegatedIssueLabel(chat, "tmp-00112233"), "workshop-pool tmp-00112233");
  assert.equal(resolveDelegatedIssueTemplate("wdl-chat-ns-pool", templates), chat);
  assert.equal(cli.version, "1");
  assert.equal(cli.disabled, false);
  assert.equal(cli.ttlSeconds, 60 * 60);
  assert.equal(cli.activeQuota, 50);
  assert.deepEqual(cli.nsGenerator, { prefix: "cli-it-", randomHexBytes: 4 });
  assert.equal(renderDelegatedIssueLabel(cli, "cli-it-00112233"), "cli live integration cli-it-00112233");
  assert.equal(resolveDelegatedIssueTemplate("wdl-cli-integration", templates), cli);
});

test("delegated issue template resolver rejects missing and disabled templates consistently", () => {
  const disabled = createDelegatedIssueTemplateMap([{
    id: "disabled-template",
    targetKind: "ns",
    nsGenerator: { prefix: "tmp-", randomHexBytes: 4 },
    labelTemplate: "pool {ns}",
    ttlSeconds: 60,
    activeQuota: 1,
    disabled: true,
  }]);
  assert.throws(
    () => resolveDelegatedIssueTemplate("missing-template", disabled),
    (err) => err instanceof AuthPolicyError &&
      /** @type {any} */ (err).status === 400 &&
      /** @type {any} */ (err).reason === "invalid_template"
  );
  assert.throws(
    () => resolveDelegatedIssueTemplate("disabled-template", disabled),
    (err) => err instanceof AuthPolicyError &&
      /** @type {any} */ (err).status === 400 &&
      /** @type {any} */ (err).reason === "template_disabled"
  );
});

test("delegated issue template code-shape failures are server misconfiguration", () => {
  for (const templates of [
    null,
    "",
    {},
    [],
    [{ id: "Bad", targetKind: "ns" }],
    [{
      id: "bad-target",
      targetKind: "token-issuer",
      nsGenerator: { prefix: "tmp-", randomHexBytes: 4 },
      labelTemplate: "pool {ns}",
      ttlSeconds: 60,
      activeQuota: 1,
    }],
    [{
      id: "long-label",
      targetKind: "ns",
      nsGenerator: { prefix: "tmp-", randomHexBytes: 4 },
      labelTemplate: "{ns} ".repeat(12),
      ttlSeconds: 60,
      activeQuota: 1,
    }],
  ]) {
    assert.throws(
      () => createDelegatedIssueTemplateMap(templates),
      (err) => err instanceof AuthPolicyError &&
        /** @type {any} */ (err).status === 503 &&
        /** @type {any} */ (err).reason === "delegated_issue_misconfigured",
      `expected config failure for ${String(templates)}`
    );
  }
});

test("issueTemplates public input and persisted issue_templates are strict JSON arrays", () => {
  assert.deepEqual(validateIssueTemplatesInput(["wdl-chat-ns-pool", "wdl-chat-ns-pool"]), [
    "wdl-chat-ns-pool",
  ]);
  assert.deepEqual(parseStoredIssueTemplates("[\"wdl-chat-ns-pool\"]"), ["wdl-chat-ns-pool"]);
  for (const value of [undefined, "", "wdl-chat-ns-pool", "[bad]", "[]", "[123]"]) {
    assert.throws(
      () => parseStoredIssueTemplates(value),
      (err) => err instanceof AuthPolicyError &&
        /** @type {any} */ (err).status === 503 &&
        /** @type {any} */ (err).reason === "invalid_issue_templates",
      `stored issue_templates=${String(value)}`
    );
  }
  for (const value of [undefined, [], ["Bad"], [""]]) {
    assert.throws(
      () => validateIssueTemplatesInput(value),
      (err) => err instanceof AuthPolicyError &&
        /** @type {any} */ (err).reason === "invalid_template_request",
    );
  }
});

// --- validateRecordShape ----------------------------------------------------

test("validateRecordShape: ns kind + valid tenant ns ok", () => {
  assert.deepEqual(validateRecordShape(persistedRecord({ kind: "ns", ns: "tenant-foo" })), { ok: true });
});

test("validateRecordShape: ns kind + reserved-ns rejected", () => {
  // Critical: a buggy issuer could write {kind:"ns", ns:"__platform__"};
  // verify must reject, not promote it to a principal.
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "ns", ns: "__platform__" })),
    { ok: false, reason: "invalid_record_ns" }
  );
});

test("validateRecordShape: token-issuer requires stored issue_templates JSON array", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({
      kind: "token-issuer",
      issue_templates: "[\"wdl-chat-ns-pool\"]",
    })),
    { ok: true }
  );
  for (const issue_templates of [undefined, "", "wdl-chat-ns-pool", "[]", "[123]"]) {
    assert.deepEqual(
      validateRecordShape(persistedRecord({ kind: "token-issuer", issue_templates })),
      { ok: false, reason: "invalid_issue_templates" },
      `issue_templates=${String(issue_templates)}`
    );
  }
});

test("validateRecordShape: ns kind + admin rejected", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "ns", ns: "admin" })),
    { ok: false, reason: "invalid_record_ns" }
  );
});

test("validateRecordShape: platform kind + __platform__ ok", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "platform", ns: "__platform__" })),
    { ok: true }
  );
});

test("validateRecordShape: platform kind + tenant ns rejected (cross-team / wrong shape)", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "platform", ns: "tenant-foo" })),
    { ok: false, reason: "invalid_record_ns" }
  );
});

test("validateRecordShape: platform kind + __system__ rejected (not in PLATFORM_TIER_RESERVED_NS)", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "platform", ns: "__system__" })),
    { ok: false, reason: "invalid_record_ns" }
  );
});

test("validateRecordShape: ops kind + no ns ok", () => {
  assert.deepEqual(validateRecordShape(persistedRecord({ kind: "ops" })), { ok: true });
});

test("validateRecordShape: ops-observer + ns=undefined ok (parseTokenRecord normalizes)", () => {
  assert.deepEqual(validateRecordShape(persistedRecord({ kind: "ops-observer", ns: undefined })), { ok: true });
});

test("validateRecordShape: ops-observer + ns=string rejected", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "ops-observer", ns: "tenant-foo" })),
    { ok: false, reason: "invalid_record_ns" }
  );
});

test("validateRecordShape: unknown kind → unknown_role", () => {
  assert.deepEqual(
    validateRecordShape(persistedRecord({ kind: "bogus", ns: "x" })),
    { ok: false, reason: "unknown_role" }
  );
});

test("validateRecordShape: persisted hash must be lowercase SHA-256 hex", () => {
  for (const hash of ["", "abc", "A".repeat(64), "g".repeat(64), undefined]) {
    assert.deepEqual(
      validateRecordShape({ hash, kind: "ops" }),
      { ok: false, reason: "invalid_record_hash" },
      `hash=${JSON.stringify(hash)}`
    );
  }
});

test("validateRecordShape: persisted timestamp fields must be canonical UTC", () => {
  const good = persistedRecord({
    kind: "ops",
    created_at: "2026-04-25T00:00:00.000Z",
    expires_at: "2026-12-31T23:59:59.000Z",
    revoked_at: "2026-04-26T00:00:00.000Z",
    expired_at: "2026-04-27T00:00:00.000Z",
  });
  assert.deepEqual(validateRecordShape(good), { ok: true });

  for (const [field, value] of [
    ["created_at", "bad"],
    ["expires_at", "2026-12-31T23:59:59Z"],
    ["revoked_at", "2026-04-31T00:00:00.000Z"],
    ["expired_at", 123],
  ]) {
    assert.deepEqual(
      validateRecordShape(persistedRecord({ kind: "ops", [field]: value })),
      { ok: false, reason: "invalid_record_timestamp" },
      `${field}=${JSON.stringify(value)}`
    );
  }
});

// --- validatePrincipalShape -------------------------------------------------

test("validatePrincipalShape: ops with no ns key accepted", () => {
  assert.equal(validatePrincipalShape({ kind: "ops" }), true);
});

test("validatePrincipalShape: ops-observer with no ns key accepted", () => {
  assert.equal(validatePrincipalShape({ kind: "ops-observer" }), true);
});

test("validatePrincipalShape: ns + tenant ns accepted", () => {
  assert.equal(validatePrincipalShape({ kind: "ns", ns: "tenant-foo" }), true);
});

test("validatePrincipalShape: platform + __platform__ accepted", () => {
  assert.equal(validatePrincipalShape({ kind: "platform", ns: "__platform__" }), true);
});

test("validatePrincipalShape: ns missing ns key rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "ns" }), false);
});

test("validatePrincipalShape: platform missing ns key rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "platform" }), false);
});

test("validatePrincipalShape: contract violation — platform with tenant ns rejected", () => {
  // Models a buggy AUTH that returned a principal-shape mismatch; control
  // validator must reject before formatReferrerBlocker etc trust principal.ns.
  assert.equal(validatePrincipalShape({ kind: "platform", ns: "tenant-foo" }), false);
});

test("validatePrincipalShape: contract violation — platform with __system__ rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "platform", ns: "__system__" }), false);
});

test("validatePrincipalShape: contract violation — ns with reserved ns rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "ns", ns: "__platform__" }), false);
});

test("validatePrincipalShape: contract violation — ns with admin rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "ns", ns: "admin" }), false);
});

test("validatePrincipalShape: contract violation — ns with empty string rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "ns", ns: "" }), false);
});

test("validatePrincipalShape: ops-observer with ns key (own) rejected", () => {
  // Object literal {kind, ns: undefined} creates the own key — boundNsKind
  // "none" requires NO own key.
  assert.equal(validatePrincipalShape({ kind: "ops-observer", ns: undefined }), false);
  assert.equal(validatePrincipalShape({ kind: "ops-observer", ns: "x" }), false);
});

test("validatePrincipalShape: unknown role rejected", () => {
  assert.equal(validatePrincipalShape({ kind: "bogus" }), false);
});

test("validatePrincipalShape: null / non-object rejected", () => {
  assert.equal(validatePrincipalShape(null), false);
  assert.equal(validatePrincipalShape(undefined), false);
  assert.equal(validatePrincipalShape("ops"), false);
});

// --- isValidTenantNs ↔ assertTenantNs consistency ---------------------------

test("isValidTenantNs vs assertTenantNs: same verdict on every fixture", () => {
  const fixtures = [
    "tenant-foo", "a-b-c", "x", "abc-123",
    "a".repeat(63),
    "", "-", "-tenant", "tenant-", "a".repeat(64), "Foo", "FOO", "tenant.dot",
    "__platform__", "__system__", "admin",
    null, undefined, 123, {},
  ];
  for (const ns of fixtures) {
    let assertThrew = false;
    try { assertTenantNs(ns); } catch { assertThrew = true; }
    assert.equal(
      isValidTenantNs(ns), !assertThrew,
      `disagreement on ${JSON.stringify(ns)}`,
    );
  }
});

// --- actionCategory ---------------------------------------------------------

test("actionCategory: buckets cover all KNOWN_ACTIONS prefixes", () => {
  assert.equal(actionCategory("diagnostic.whoami"), "diagnostic");
  assert.equal(actionCategory("worker.deploy"), "worker");
  assert.equal(actionCategory("worker.deploy.dry-run"), "worker");
  assert.equal(actionCategory("worker.versions.read"), "worker");
  assert.equal(actionCategory("workflow.write"), "workflow");
  assert.equal(actionCategory("secret.read"), "secret");
  assert.equal(actionCategory("d1.execute"), "d1");
  assert.equal(actionCategory("r2.object.get"), "r2");
  assert.equal(actionCategory("host.write"), "host");
  assert.equal(actionCategory("auth.token.issue"), "auth_token");
  assert.equal(actionCategory("system.reload"), "system");
  assert.equal(actionCategory("system.flush_cache"), "system");
});

test("actionCategory: 'auth.foo' (drift) → unknown, NOT auth_token", () => {
  // Critical: auth.token.* uses startsWith on "auth.token." NOT "auth.".
  // Otherwise classifier drift produces auth.<x> that gets silently
  // absorbed into auth_token, weakening the drift-detection signal.
  assert.equal(actionCategory("auth.foo"), "unknown");
  assert.equal(actionCategory("auth"), "unknown");
});

test("actionCategory: delegated auth issue has its own bounded category", () => {
  assert.equal(actionCategory("auth.delegated_token.issue"), "auth_delegated_token");
});

test("actionCategory: bare prefix (no dot) → unknown", () => {
  assert.equal(actionCategory("worker"), "unknown");
});

test("actionCategory: type-bad input → unknown", () => {
  for (const v of [undefined, null, "", 123, {}]) {
    assert.equal(actionCategory(v), "unknown",
      `expected unknown for ${JSON.stringify(v)}`);
  }
});

// --- ROLES / KNOWN_ACTIONS contract drift tripwires -------------------------

test("ROLES contract: every literal action in ROLES.actions is in KNOWN_ACTIONS", () => {
  for (const [roleName, role] of Object.entries(ROLES)) {
    for (const pat of role.actions) {
      if (pat === "*" || pat.endsWith(".*")) continue;
      assert.ok(KNOWN_ACTIONS.has(pat),
        `role "${roleName}" has unknown action literal "${pat}"`);
    }
  }
});

test("ROLES contract: every namespaces and boundNsKind pair uses a legal form", () => {
  const PLACEHOLDER = PRINCIPAL_NS_PLACEHOLDER;
  for (const [roleName, role] of Object.entries(ROLES)) {
    const ns = JSON.stringify(role.namespaces);
    const bnk = role.boundNsKind;
    const ok =
      (ns === '["*"]' && bnk === "none") ||
      (ns === JSON.stringify([PLACEHOLDER]) && bnk === "tenant") ||
      (ns === JSON.stringify([PLACEHOLDER]) && bnk === "platform-tier");
    assert.ok(ok,
      `role "${roleName}" has invalid (namespaces=${ns}, boundNsKind=${bnk})`);
  }
});

test("KNOWN_ACTIONS contract: every member is role-covered or hard-policy ops-only", () => {
  // Either matched by a role's actions, self-introspection hard-policy,
  // auth.token.* / system.* (red line 4/5 owns it), OR host.write
  // (KNOWN_ACTIONS lists it because the classifier emits it; current
  // ROLES has no allowance).
  const HARD_POLICY_ACTION = (/** @type {string} */ a) =>
    a === "diagnostic.whoami" ||
    a.startsWith("auth.token.") ||
    a.startsWith("system.") ||
    a === "host.write";
  for (const action of KNOWN_ACTIONS) {
    let covered = HARD_POLICY_ACTION(action);
    if (!covered) {
      for (const role of /** @type {any[]} */ (Object.values(ROLES))) {
        if (role.actions.some((/** @type {string} */ p) => matchesAction(p, action))) {
          covered = true; break;
        }
      }
    }
    assert.ok(covered, `KNOWN_ACTIONS member "${action}" is unreachable`);
  }
});

test("ROLES contract: platform admin action set tracks ns except tenant-host read", () => {
  const nsActions = new Set(ROLES.ns.actions);
  const platformActions = new Set(ROLES.platform.actions);
  nsActions.delete("host.read");
  assert.deepEqual(
    [...platformActions].toSorted(),
    [...nsActions].toSorted(),
    "platform should mirror ns action grants except tenant host.read",
  );
});

// --- host.write default-deny dynamic test (locks "data-driven not granted") -

/** @type {Set<string>} */
const HOST_WRITE_ALLOWED_ROLES = new Set([]); // empty: no role grants host.write

/** @param {any} role */
function inScopeNsFor(role) {
  if (role.boundNsKind !== "none") {
    return { principalNs: "tenant-foo", ns: "tenant-foo" };
  }
  return { principalNs: undefined, ns: "tenant-foo" };
}

for (const kind of Object.keys(ROLES)) {
  if (kind === "ops") continue;
  if (HOST_WRITE_ALLOWED_ROLES.has(kind)) continue;
  test(`host.write default-deny: role "${kind}" rejects host.write with action_not_in_scope`, () => {
    const { principalNs, ns } = inScopeNsFor(ROLES[kind]);
    const result = evaluateAccess({
      action: "host.write", ns, kind, principalNs,
    });
    assert.equal(result.ok, false);
    // CRITICAL: must be action_not_in_scope, NOT ns_not_in_scope (latter
    // means namespace check rejected first, so action check is skipped
    // and the test is vacuous).
    assert.equal(result.reason, "action_not_in_scope",
      `role "${kind}" rejected host.write but on the wrong axis (got ${result.reason})`);
  });
}

// --- role mutation cleanup tripwire -----------------------------------------

test("ROLES cleanup tripwire has exactly the baseline 6 roles", () => {
  assert.deepEqual(
    Object.keys(ROLES).toSorted(),
    ["ns", "ops", "ops-observer", "platform", "platform-observer", "token-issuer"]
  );
});

test("PLATFORM_TIER_RESERVED_NS cleanup tripwire is exactly {__platform__}", () => {
  for (const ns of PLATFORM_TIER_RESERVED_NS) {
    assert.ok(RESERVED_NS.has(ns),
      `platform-tier member "${ns}" must also be listed in RESERVED_NS`);
  }
  assert.deepEqual([...PLATFORM_TIER_RESERVED_NS].toSorted(), ["__platform__"]);
});

// --- assertTenantNs ---------------------------------------------------------

test("assertTenantNs accepts valid tenant ns, rejects reserved / invalid", () => {
  assertTenantNs("foo");
  assertTenantNs("a-b-c");
  // Genuinely missing values → missing_ns ("you forgot to send ns").
  assert.throws(() => assertTenantNs(""), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "missing_ns");
  assert.throws(() => assertTenantNs(undefined), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "missing_ns");
  assert.throws(() => assertTenantNs(null), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "missing_ns");
  // Wrong type → invalid_ns ("your serializer sent a number"), NOT
  // missing_ns. Operators need the distinction to chase the right bug.
  assert.throws(() => assertTenantNs(123), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_ns");
  assert.throws(() => assertTenantNs(true), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_ns");
  assert.throws(() => assertTenantNs({}), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_ns");
  assert.throws(() => assertTenantNs("__system__"), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "reserved_ns");
  assert.throws(() => assertTenantNs("Foo"), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "invalid_ns");
  assert.throws(() => assertTenantNs("admin"), (err) =>
    err instanceof AuthPolicyError && /** @type {any} */ (err).reason === "reserved_tenant_ns");
});

// --- record marshalling -----------------------------------------------------

test("parseTokenRecord normalizes empty strings to undefined", () => {
  const rec = parseTokenRecord({
    hash: "abc",
    kind: "ns",
    ns: "foo",
    label: "",
    revoked_at: "",
    created_at: "2026-01-01T00:00:00Z",
  });
  assert.equal(rec.label, undefined);
  assert.equal(rec.revoked_at, undefined);
  assert.equal(rec.kind, "ns");
});

test("parseTokenRecord preserves delegated issue metadata", () => {
  const rec = parseTokenRecord({
    hash: "abc",
    kind: "ns",
    ns: "tmp-00112233",
    issue_template: "wdl-chat-ns-pool",
    issue_template_version: "1",
  });
  assert.equal(rec.issue_template, "wdl-chat-ns-pool");
  assert.equal(rec.issue_template_version, "1");
});

test("parseTokenRecord returns null for empty / kindless hashes", () => {
  assert.equal(parseTokenRecord({}), null);
  assert.equal(parseTokenRecord({ hash: "abc" }), null); // no kind
  assert.equal(parseTokenRecord(null), null);
});

test("isRevoked checks revoked_at presence", () => {
  assert.equal(isRevoked({}), false);
  assert.equal(isRevoked({ revoked_at: "2026-01-01T00:00:00Z" }), true);
});

test("recordToListEntry omits absent optional fields, never includes hash/plaintext", () => {
  const entry = recordToListEntry("tok-123", {
    kind: "ns",
    hash: "do-not-leak",
    ns: "foo",
    created_at: "2026-01-01T00:00:00Z",
  });
  assert.deepEqual(entry, {
    tokenId: "tok-123",
    kind: "ns",
    createdAt: "2026-01-01T00:00:00Z",
    ns: "foo",
  });
  assert.equal(Object.hasOwn(entry, "hash"), false);
});

test("recordToListEntry exposes issuer and delegated template metadata in camelCase", () => {
  const issuer = recordToListEntry("issuer-1", {
    kind: "token-issuer",
    hash: "do-not-leak",
    created_at: "2026-01-01T00:00:00Z",
    issue_templates: "[\"wdl-chat-ns-pool\"]",
  });
  assert.deepEqual(issuer.issueTemplates, ["wdl-chat-ns-pool"]);
  assert.equal(Object.hasOwn(issuer, "issue_templates"), false);

  const delegated = recordToListEntry("tok-1", {
    kind: "ns",
    hash: "do-not-leak",
    ns: "tmp-00112233",
    created_at: "2026-01-01T00:00:00Z",
    issue_template: "wdl-chat-ns-pool",
    issue_template_version: "1",
  });
  assert.equal(delegated.issueTemplate, "wdl-chat-ns-pool");
  assert.equal(delegated.issueTemplateVersion, "1");
});

test("recordToListEntry marks malformed token-issuer issue_templates for repair", () => {
  const malformed = recordToListEntry("issuer-bad", {
    kind: "token-issuer",
    hash: "do-not-leak",
    created_at: "2026-01-01T00:00:00Z",
    issue_templates: "not-json",
  });
  assert.equal(malformed.issueTemplatesInvalid, true);

  const missing = recordToListEntry("issuer-missing", {
    kind: "token-issuer",
    hash: "do-not-leak",
    created_at: "2026-01-01T00:00:00Z",
  });
  assert.equal(missing.issueTemplatesInvalid, true);
});
