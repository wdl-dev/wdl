// Worker-level harness for auth/index.js. Covers verify error paths
// (verify_threw, unknown_role, invalid_record_ns) and lifecycle
// invalid_role_config — integration tests can't reach these because
// rolesPatch from a separate Node process can't mutate ROLES inside the
// workerd AUTH isolate, and forcing Redis to throw mid-verify is not
// deterministic against a real socket.

import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  loadAuthIndex,
  authMockState,
  lastAuthLog,
  resetAuthMockState,
  seedBootstrap,
} from "../helpers/load-auth-index.js";
import { withMockedProperty } from "../helpers/mock-global.js";

const ENV = {
  BOOTSTRAP_TOKEN: "test-bootstrap-token-for-unit-harness-1234567890",
  REDIS_ADDR: "redis://mock",
  SERVICE_NAME: "auth",
  LOG_LEVEL: "info",
};

// One harness load per test, so a rolesPatch fixture in one test can't
// pollute the ROLES table in the next. Cheap enough at our scale.
async function freshAuth(opts = {}) {
  resetAuthMockState();
  const { AuthClass, authLib } = await loadAuthIndex(opts);
  const env = { ...ENV, ...(/** @type {any} */ (opts).env || {}) };
  await seedBootstrap(authLib, env);
  const auth = new AuthClass({}, env);
  return { auth, authLib, state: authMockState() };
}

const QUOTA_TWO_TEMPLATE = { activeQuota: 2 };
const TINY_RANDOM_TEMPLATE = {
  randomHexBytes: 1,
  ttlSeconds: 60,
  activeQuota: 10,
};
const TINY_RANDOM_HIGH_QUOTA_TEMPLATE = {
  randomHexBytes: 1,
  ttlSeconds: 60,
  activeQuota: 1000,
};

/** @param {string} issuerTokenId @param {string} [templateId] */
function delegatedIssueLockKey(issuerTokenId, templateId = "wdl-chat-ns-pool") {
  return `auth:delegated-issue-lock:${encodeURIComponent(issuerTokenId)}:${encodeURIComponent(templateId)}`;
}

/** @param {{ strings: Map<string, unknown> }} state */
function delegatedIssueLockKeys(state) {
  return [...state.strings.keys()].filter((/** @type {string} */ key) =>
    key.startsWith("auth:delegated-issue-lock:"));
}


beforeEach(() => {
  // Belt-and-suspenders — freshAuth resets too, but a beforeEach reset
  // covers any test that forgets to call freshAuth (none should).
  resetAuthMockState();
});

// --- verify: error paths ----------------------------------------------------

test("verify: unknown_role on persisted record → 503 outcome=error log", async () => {
  const { auth, authLib } = await freshAuth();

  // Bypass validateIssueInput by writing the bogus record directly —
  // simulates a migrated record or buggy issuer.
  const plaintext = "phantom-token-plaintext";
  const tokenId = "phantom-id-x";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "phantom", ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "unknown_role");
  assert.equal(res.tokenId, tokenId);

  const logged = lastAuthLog("auth_verify");
  assert.ok(logged, "auth_verify log must fire");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "unknown_role");
  assert.equal(logged.principal_kind, "phantom");
  assert.equal(logged.target_ns, "tenant-a");
  assert.equal(logged.action, "worker.list");

  // Action/target details are log fields, not metric labels.
  assert.equal(logged.category, "worker");
});

test("verify: request id follows shared sanitizer before logging", async () => {
  const { auth, authLib } = await freshAuth();
  const plaintext = "tenant-token-for-request-id-test";
  const tokenId = "tenant-token-rid";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash,
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  await auth.verify({
    token: plaintext,
    action: "worker.list",
    ns: "tenant-a",
    requestId: "bad id",
  });

  const logged = lastAuthLog("auth_verify");
  assert.ok(logged, "auth_verify log must fire");
  assert.equal(Object.hasOwn(logged, "request_id"), false);
});

test("verify: invalid_record_ns when persisted ns shape conflicts with role.boundNsKind", async () => {
  const { auth, authLib } = await freshAuth();

  // kind=ns (boundNsKind="tenant") but no ns persisted — parseTokenRecord
  // normalizes "" → undefined, which fails isValidTenantNs(undefined).
  const plaintext = "ns-no-ns-plaintext";
  const tokenId = "tid-ns-nons";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ns",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "invalid_record_ns");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "invalid_record_ns");

  assert.equal(logged.category, "worker");
});

test("verify: malformed persisted token record fails closed", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "malformed-token-record";
  const tokenId = "tid-malformed";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash,
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    expires_at: "not-a-date",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "invalid_record_timestamp");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "invalid_record_timestamp");
});

test("verify: malformed revoked persisted token record fails contract-closed", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "malformed-revoked-token-record";
  const tokenId = "tid-malformed-revoked";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash,
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    revoked_at: "bad",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "invalid_record_timestamp");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "invalid_record_timestamp");
});

test("verify: hash index and token record hash must agree", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "mismatched-token-record";
  const tokenId = "tid-hash-mismatch";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash: "b".repeat(64),
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "invalid_record_hash");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "invalid_record_hash");
});

test("verify: invalid_record_ns when platform-tier record carries a non-platform-tier ns", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "platform-bad-ns-plaintext";
  const tokenId = "tid-plat-bad";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  // kind=platform requires ns ∈ PLATFORM_TIER_RESERVED_NS; "tenant-foo"
  // is a valid tenant ns but NOT platform-tier → invalid_record_ns.
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "platform", ns: "tenant-foo",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-foo",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 503);
  assert.equal(res.reason, "invalid_record_ns");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.principal_kind, "platform");
});

test("verify: verify_threw when Redis throws mid-flight", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "throw-token";
  const tokenId = "tid-throw";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ns", ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);
  // Force the GET on auth:hash:<sha> to throw — verify catches and re-raises.
  state.getThrows.add(`auth:hash:${hash}`);

  await assert.rejects(
    () => auth.verify({ token: plaintext, action: "worker.list", ns: "tenant-a" }),
    /forced get throw/,
  );

  const logged = lastAuthLog("auth_verify");
  assert.ok(logged, "verify_threw must still log auth_verify");
  assert.equal(logged.level, "error");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "verify_threw");
  // Action still rendered for log (not a label) so triage can group by it.
  assert.equal(logged.action, "worker.list");
  assert.equal(logged.category, "worker");
  assert.ok(typeof logged.error_message === "string" && logged.error_message.length > 0,
    "verify_threw log must carry error_message");

  assert.equal(logged.category, "worker");
});

// --- verify: outcome split ↔ log level mapping ------------------------------

test("verify: valid token emits outcome=ok and info log", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "ok-token-plaintext";
  const tokenId = "tid-ok";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ns", ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, true);
  assert.equal(res.status, 200);
  assert.deepEqual(res.principal, { kind: "ns", ns: "tenant-a" });

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "info");
  assert.equal(logged.outcome, "ok");
  assert.equal(logged.reason, "allow");
  assert.equal(authMockState().sessions, 1);
  assert.deepEqual(
    authMockState().commands.map((/** @type {any} */ entry) => entry.command),
    ["HGET", "GET", "HGETALL"],
  );

  assert.equal(logged.category, "worker");
});

test("verify: skips bootstrap HGET after first ensure and re-ensures bootstrap token after wipe", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "cached-bootstrap-path-token";
  const tokenId = "tid-cache";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ns", ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const first = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(first.ok, true);

  state.commands = [];
  const second = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(second.ok, true);
  assert.deepEqual(
    state.commands.map((/** @type {any} */ entry) => entry.command),
    ["GET", "HGETALL"],
  );

  state.commands = [];
  state.hashes.clear();
  state.strings.clear();
  const recovered = await auth.verify({
    token: ENV.BOOTSTRAP_TOKEN, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(recovered.ok, true);
  assert.equal(recovered.tokenId, "bootstrap");
  assert.deepEqual(
    state.commands.map((/** @type {any} */ entry) => entry.command),
    ["GET", "HGET", "HGET", "HSET", "SET", "GET", "HGETALL"],
  );
});

test("verify: bootstrap token rotates a stale bootstrap record on first ensure", async () => {
  const { auth, authLib } = await freshAuth();

  const oldHash = await authLib.hashToken("old-bootstrap-token");
  const desiredHash = await authLib.hashToken(ENV.BOOTSTRAP_TOKEN);
  const state = authMockState();
  state.hashes.set("auth:token:bootstrap", {
    hash: oldHash,
    kind: "ops",
    created_at: "2026-04-24T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.clear();
  state.strings.set(`auth:hash:${oldHash}`, "bootstrap");
  state.commands = [];

  const res = await auth.verify({
    token: ENV.BOOTSTRAP_TOKEN, action: "worker.list", ns: "tenant-a",
  });

  assert.equal(res.ok, true);
  assert.equal(res.tokenId, "bootstrap");
  assert.equal(state.hashes.get("auth:token:bootstrap").hash, desiredHash);
  assert.equal(state.strings.has(`auth:hash:${oldHash}`), false);
  assert.equal(state.strings.get(`auth:hash:${desiredHash}`), "bootstrap");
  assert.deepEqual(
    state.commands.map((/** @type {any} */ entry) => entry.command),
    ["HGET", "HGET", "DEL", "HSET", "SET", "GET", "HGETALL"],
  );
});

test("verify: policy reject (action_not_in_scope) → outcome=reject, log level=warn", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "reject-token";
  const tokenId = "tid-reject";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ns", ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  // host.write is NOT in ROLES.ns.actions → action_not_in_scope.
  const res = await auth.verify({
    token: plaintext, action: "host.write", ns: "tenant-a",
  });
  assert.equal(res.ok, false);
  assert.equal(res.status, 403);
  assert.equal(res.reason, "action_not_in_scope");

  const logged = lastAuthLog("auth_verify");
  assert.equal(logged.level, "warn");
  assert.equal(logged.outcome, "reject");

  assert.equal(logged.category, "host");
});

test("verify: principal for none-bound role has NO own ns key (Object.hasOwn discipline)", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "obs-tok";
  const tokenId = "tid-obs";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash, kind: "ops-observer",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.verify({
    token: plaintext, action: "worker.list", ns: "tenant-a",
  });
  assert.equal(res.ok, true);
  // control-side validatePrincipalShape uses Object.hasOwn — JSRPC
  // preserves own-key existence, so {kind, ns: undefined} would still
  // arrive with `ns` present and break downstream shape checks.
  assert.equal(Object.hasOwn(res.principal, "ns"), false,
    `none-bound principal must omit ns key, got: ${JSON.stringify(res.principal)}`);
});

// --- lifecycle: invalid_role_config (rolesPatch) ----------------------------
// validateIssueInput's default branch (malformed boundNsKind) is unreachable
// through public input — the only way to exercise it is mutating ROLES
// in-process via rolesPatch.

test("issue: invalid_role_config when ROLES[kind].boundNsKind is malformed → 503 outcome=error log", async () => {
  const { auth } = await freshAuth({
    rolesPatch: {
      broken: {
        actions: ["*"],
        namespaces: ["*"],
        boundNsKind: "garbage",
      },
    },
  });

  await assert.rejects(
    () => auth.issue({
      kind: "broken", ns: "tenant-a",
      issuerTokenId: "bootstrap",
    }),
    (err) => {
      const e = /** @type {any} */ (err);
      return e.status === 503 && e.reason === "invalid_role_config";
    },
  );

  const logged = lastAuthLog("auth_issue");
  assert.ok(logged, "auth_issue log must fire on invalid_role_config");
  assert.equal(logged.level, "error",
    "5xx AuthPolicyError must log at error level (operator alerting)");
  assert.equal(logged.outcome, "error");
  assert.equal(logged.reason, "invalid_role_config");
});

test("issue: 4xx AuthPolicyError → outcome=reject, log level=warn (rethrowPolicy 4xx/5xx split)", async () => {
  // Counterpart to the test above — confirms the rethrowPolicy split is
  // status-driven, not class-driven. unknown_role is also AuthPolicyError
  // but its 400 status puts it in outcome=reject + level=warn.
  const { auth } = await freshAuth();

  await assert.rejects(
    () => auth.issue({
      kind: "no-such-role", ns: "tenant-a",
      issuerTokenId: "bootstrap",
    }),
    (err) => {
      const e = /** @type {any} */ (err);
      return e.status === 400 && e.reason === "unknown_role";
    },
  );

  const logged = lastAuthLog("auth_issue");
  assert.equal(logged.level, "warn");
  assert.equal(logged.outcome, "reject");
  assert.equal(logged.reason, "unknown_role");
});

test("issue: token-issuer stores validated issueTemplates as issue_templates", async () => {
  const { auth } = await freshAuth();

  const result = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  assert.equal(typeof result.token, "string");
  assert.equal(typeof result.tokenId, "string");

  const state = authMockState();
  const record = state.hashes.get(`auth:token:${result.tokenId}`);
  assert.equal(record.kind, "token-issuer");
  assert.equal(record.issue_templates, "[\"wdl-chat-ns-pool\"]");
  assert.equal(Object.hasOwn(record, "ns"), false);
  assert.equal(Object.hasOwn(record, "issueTemplates"), false);
});

test("issue: token-issuer rejects snake_case storage field and unknown templates", async () => {
  const { auth } = await freshAuth();

  await assert.rejects(
    () => auth.issue({
      kind: "token-issuer",
      issue_templates: ["wdl-chat-ns-pool"],
      issuerTokenId: "bootstrap",
    }),
    (err) => /** @type {any} */ (err).reason === "invalid_template_request"
  );
  await assert.rejects(
    () => auth.issue({
      kind: "token-issuer",
      issueTemplates: ["missing-template"],
      issuerTokenId: "bootstrap",
    }),
    (err) => /** @type {any} */ (err).reason === "invalid_template"
  );
});

test("delegatedIssue: issuer token creates a bounded ns token from template", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });

  const result = await auth.delegatedIssue({
    issuerTokenId: issuer.tokenId,
    template: "wdl-chat-ns-pool",
    requestId: "rid-delegated",
  });

  assert.equal(typeof result.token, "string");
  assert.equal(result.kind, "ns");
  assert.match(result.ns, /^tmp-[0-9a-f]{8}$/);
  assert.equal(result.label, `workshop-pool ${result.ns}`);
  assert.equal(result.issueTemplate, "wdl-chat-ns-pool");
  assert.equal(result.issueTemplateVersion, "1");
  assert.equal(new Date(result.expiresAt).toISOString(), result.expiresAt);

  const state = authMockState();
  const record = state.hashes.get(`auth:token:${result.tokenId}`);
  assert.equal(record.created_by, issuer.tokenId);
  assert.equal(record.issue_template, "wdl-chat-ns-pool");
  assert.equal(record.issue_template_version, "1");
  assert.equal(record.ns, result.ns);
  assert.equal(state.strings.get(`auth:hash:${record.hash}`), result.tokenId);
  assert.deepEqual(delegatedIssueLockKeys(state), []);

  const verify = await auth.verify({
    token: result.token,
    action: "worker.list",
    ns: result.ns,
  });
  assert.equal(verify.ok, true);
  assert.deepEqual(verify.principal, { kind: "ns", ns: result.ns });
});

test("delegatedIssue: CLI integration template creates a short-lived ns token", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-cli-integration"],
    issuerTokenId: "bootstrap",
  });

  const result = await auth.delegatedIssue({
    issuerTokenId: issuer.tokenId,
    template: "wdl-cli-integration",
  });

  assert.equal(result.kind, "ns");
  assert.match(result.ns, /^cli-it-[0-9a-f]{8}$/);
  assert.equal(result.label, `cli live integration ${result.ns}`);
  assert.equal(result.issueTemplate, "wdl-cli-integration");

  const state = authMockState();
  const record = state.hashes.get(`auth:token:${result.tokenId}`);
  assert.equal(record.created_by, issuer.tokenId);
  assert.equal(record.issue_template, "wdl-cli-integration");
  assert.equal(record.issue_template_version, "1");
  assert.equal(record.ns, result.ns);
});

test("delegatedIssue: rejects direct issue fields and disallowed templates", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });

  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "wdl-chat-ns-pool",
      ns: "attacker",
    }),
    (err) => /** @type {any} */ (err).reason === "invalid_template_request"
  );
  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "wdl-chat-ns-pool",
      foo: "bar",
    }),
    (err) => /** @type {any} */ (err).reason === "invalid_template_request"
  );
  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "missing-template",
    }),
    (err) => /** @type {any} */ (err).reason === "invalid_template"
  );
});

test("delegatedIssue: active non-token-issuer callers are rejected with specific reason", async () => {
  const { auth } = await freshAuth();

  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: "bootstrap",
      template: "wdl-chat-ns-pool",
    }),
    (err) => {
      const policy = /** @type {any} */ (err);
      assert.equal(policy.status, 403);
      assert.equal(policy.reason, "issuer_not_token_issuer");
      return true;
    }
  );
});

test("delegatedIssue: malformed issuer record is a storage contract error", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  state.hashes.get(`auth:token:${issuer.tokenId}`).issue_templates = "not-json";

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => {
      const policy = /** @type {any} */ (err);
      assert.equal(policy.status, 503);
      assert.equal(policy.reason, "invalid_issue_templates");
      return true;
    }
  );
});

test("delegatedIssue: active quota counts expires_at directly and fails closed on malformed records", async () => {
  const { auth } = await freshAuth({ delegatedTemplatePatch: QUOTA_TWO_TEMPLATE });
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });

  await auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" });
  await auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" });
  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => {
      const policy = /** @type {any} */ (err);
      assert.equal(policy.reason, "active_quota_exceeded");
      assert.deepEqual(policy.details, {
        active: 2,
        quota: 2,
        available: 0,
      });
      return true;
    }
  );

  const state = authMockState();
  for (const record of state.hashes.values()) {
    if (record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool") {
      record.revoked_at = "2026-04-25T00:00:00.000Z";
    }
  }
  state.hashes.set("auth:token:malformed-delegated", {
    hash: "b".repeat(64),
    kind: "ns",
    ns: "tmp-bad00000",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: issuer.tokenId,
    issue_template: "wdl-chat-ns-pool",
  });
  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => /** @type {any} */ (err).reason === "delegated_issue_misconfigured"
  );
});

test("delegatedIssue: active quota de-duplicates repeated SCAN keys", async () => {
  const { auth } = await freshAuth({ delegatedTemplatePatch: QUOTA_TWO_TEMPLATE });
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  await auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" });
  const state = authMockState();
  const activeTokenKey = [...state.hashes.keys()].find((/** @type {string} */ key) => {
    const record = state.hashes.get(key);
    return record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool";
  });
  assert.ok(activeTokenKey, "expected delegated token record");
  state.scanPages = [
    { next: "1", keys: ["auth:token:bootstrap", activeTokenKey] },
    { next: "0", keys: [activeTokenKey] },
  ];

  const second = await auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" });

  assert.equal(second.kind, "ns");
  const activeRecords = [...state.hashes.values()].filter((/** @type {any} */ record) =>
    record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool");
  assert.equal(activeRecords.length, 2);
});

test("delegatedIssue: expired issue lock prevents writing after slow quota scan", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  const originalGet = state.strings.get.bind(state.strings);
  state.strings.get = (/** @type {string} */ key) => key === lockKey ? "stale-lock-token" : originalGet(key);

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
  );
  assert.equal(
    [...state.hashes.values()].filter((/** @type {any} */ record) =>
      record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool").length,
    0
  );
});

test("delegatedIssue: local lock budget is measured from lock acquisition", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);

  await withMockedProperty(Date, "now", () => state.strings.has(lockKey) ? 29_000 : 0, async () => {
    await assert.rejects(
      () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
      (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
    );
    assert.equal(
      [...state.hashes.values()].filter((/** @type {any} */ record) =>
        record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool").length,
      0
    );
  });
});

test("delegatedIssue: leaves an uncertain applied lock for TTL expiry after a lost SET reply", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  let lostReplyInjected = false;
  let nowMs = 1_750_000_000_000;
  state.afterSetAppliedBeforeReply = (/** @type {string} */ key) => {
    if (lostReplyInjected || key !== lockKey) return;
    lostReplyInjected = true;
    throw new Error("lost delegated issue lock SET reply");
  };

  await withMockedProperty(Date, "now", () => nowMs, async () => {
    await assert.rejects(
      () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
      /lost delegated issue lock SET reply/
    );
    assert.equal(lostReplyInjected, true);
    assert.equal(state.strings.has(lockKey), true);
    assert.equal(state.expirations.get(lockKey), nowMs + 30_000);
    assert.equal(state.commands.at(-1)?.command, "SET");
    assert.equal(state.commands.at(-1)?.ok, false);
    assert.equal(state.commands.some((/** @type {any} */ event) => event.command === "DELIFEQ"), false);

    nowMs += 29_999;
    await assert.rejects(
      () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
      (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
    );

    nowMs += 2;
    const issued = await auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "wdl-chat-ns-pool",
    });
    assert.equal(issued.kind, "ns");
    assert.equal(state.strings.has(lockKey), false);
    assert.equal(state.expirations.has(lockKey), false);
  });
});

test("delegatedIssue: leaves its lock for TTL expiry when the final EXEC reply is lost", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  state.afterMultiExecAppliedBeforeReply = () => {
    throw new Error("lost delegated issue EXEC reply");
  };

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    /lost delegated issue EXEC reply/
  );
  const committed = [...state.hashes.values()].filter((/** @type {any} */ record) =>
    record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool");
  assert.equal(committed.length, 1);
  assert.equal(state.strings.has(lockKey), true);
  assert.equal(state.commands.at(-1)?.command, "MULTI_EXEC");
  assert.equal(state.commands.at(-1)?.ok, false);
  assert.equal(state.commands.some((/** @type {any} */ event) => event.command === "DELIFEQ"), false);
});

test("delegatedIssue: issue lock release failure warns without failing issued token", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  state.delIfEqThrows.add(lockKey);

  const issued = await auth.delegatedIssue({
    issuerTokenId: issuer.tokenId,
    template: "wdl-chat-ns-pool",
    requestId: "rid-release-warn",
  });

  assert.equal(issued.kind, "ns");
  assert.deepEqual(state.logs.filter((/** @type {any} */ entry) =>
    entry.event === "auth_delegated_issue_lock_release_failed"), [{
    level: "warn",
    event: "auth_delegated_issue_lock_release_failed",
    service: "auth",
    request_id: "rid-release-warn",
    error_name: "Error",
    error_message: `forced delIfEq throw on ${lockKey}`,
  }]);
});

test("delegatedIssue: issue lock WATCH prevents writing if the lock changes before EXEC", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  let invalidated = false;
  state.beforeMultiExec = (/** @type {unknown[][]} */ cmds) => {
    if (invalidated || !cmds.some((cmd) => cmd[0] === "HSET" && String(cmd[1]).startsWith("auth:token:"))) {
      return;
    }
    invalidated = true;
    state.strings.set(lockKey, "stolen-lock-token");
    state.keyVersions.set(lockKey, (state.keyVersions.get(lockKey) || 0) + 1);
  };

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
  );
  assert.equal(invalidated, true);
  assert.deepEqual(state.logs.filter((/** @type {any} */ entry) =>
    entry.event === "redis_watch_invalidation"), [{
    level: "warn",
    event: "redis_watch_invalidation",
    service: "auth",
    command: "MULTI_EXEC",
    duration_ms: 0,
  }]);
  assert.equal(
    [...state.hashes.values()].filter((/** @type {any} */ record) =>
      record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool").length,
    0
  );
});

test("delegatedIssue: issuer revoke invalidates the final token write", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const issuerKey = `auth:token:${issuer.tokenId}`;
  let revoked = false;
  state.beforeMultiExec = (/** @type {unknown[][]} */ cmds) => {
    if (revoked || !cmds.some((cmd) => cmd[0] === "HSET" && cmd[1] !== issuerKey)) return;
    revoked = true;
    state.hashes.get(issuerKey).revoked_at = "2026-07-16T00:00:00.000Z";
    state.keyVersions.set(issuerKey, (state.keyVersions.get(issuerKey) || 0) + 1);
  };

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
  );
  assert.equal(revoked, true);
  assert.equal(state.hashes.get(issuerKey).revoked_at, "2026-07-16T00:00:00.000Z");
  assert.equal(
    [...state.hashes.values()].filter((/** @type {any} */ record) =>
      record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool").length,
    0
  );
});

test("delegatedIssue: namespace collision retries against namespaces set", async () => {
  const { auth } = await freshAuth({ delegatedTemplatePatch: TINY_RANDOM_TEMPLATE });
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  // Only a small subset: this verifies the collision branch without making
  // the test depend on every random value.
  state.sets.set("namespaces", new Set(["tmp-00", "tmp-01", "tmp-02"]));

  const result = await auth.delegatedIssue({
    issuerTokenId: issuer.tokenId,
    template: "wdl-chat-ns-pool",
  });
  assert.match(result.ns, /^tmp-[0-9a-f]{2}$/);
  assert.equal(state.sets.get("namespaces").has(result.ns), false);
});

test("delegatedIssue: namespace collision checks delegated token namespace claims", async () => {
  const { auth } = await freshAuth({ delegatedTemplatePatch: TINY_RANDOM_HIGH_QUOTA_TEMPLATE });
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const expiresAt = new Date(Date.now() + 60_000).toISOString();
  for (let i = 0; i < 256; i += 1) {
    const suffix = i.toString(16).padStart(2, "0");
    state.hashes.set(`auth:token:delegated-collision-${suffix}`, {
      hash: suffix.padStart(64, "0"),
      kind: "ns",
      ns: `tmp-${suffix}`,
      created_at: "2026-04-25T00:00:00.000Z",
      created_by: `other-issuer-${suffix}`,
      expires_at: expiresAt,
      issue_template: `other-template-${suffix}`,
      issue_template_version: "1",
    });
  }

  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "wdl-chat-ns-pool",
    }),
    (err) => /** @type {any} */ (err).reason === "namespace_collision"
  );
});

test("delegatedIssue: generated namespace cannot reuse expired or revoked direct token namespace claims", async () => {
  const { auth } = await freshAuth({ delegatedTemplatePatch: TINY_RANDOM_HIGH_QUOTA_TEMPLATE });
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  for (let i = 0; i < 256; i += 1) {
    const suffix = i.toString(16).padStart(2, "0");
    /** @type {any} */
    const record = {
      hash: suffix.padStart(64, "0"),
      kind: "ns",
      ns: `tmp-${suffix}`,
      created_at: "2026-04-25T00:00:00.000Z",
      created_by: "bootstrap",
    };
    if (i % 2 === 0) {
      record.expires_at = "2026-04-25T00:00:00.000Z";
    } else {
      record.revoked_at = "2026-04-25T00:00:00.000Z";
    }
    state.hashes.set(`auth:token:direct-claim-${suffix}`, record);
  }

  await assert.rejects(
    () => auth.delegatedIssue({
      issuerTokenId: issuer.tokenId,
      template: "wdl-chat-ns-pool",
    }),
    (err) => /** @type {any} */ (err).reason === "namespace_collision"
  );
});

test("delegatedIssue: issuer/template issue lock serializes quota checks", async () => {
  const { auth } = await freshAuth();
  const issuer = await auth.issue({
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issuerTokenId: "bootstrap",
  });
  const state = authMockState();
  const lockKey = delegatedIssueLockKey(issuer.tokenId);
  state.strings.set(lockKey, "other-inflight-issue");

  await assert.rejects(
    () => auth.delegatedIssue({ issuerTokenId: issuer.tokenId, template: "wdl-chat-ns-pool" }),
    (err) => /** @type {any} */ (err).reason === "delegated_issue_busy"
  );
  assert.equal(state.strings.get(lockKey), "other-inflight-issue");
  assert.equal(
    [...state.hashes.values()].filter((/** @type {any} */ record) =>
      record.created_by === issuer.tokenId && record.issue_template === "wdl-chat-ns-pool").length,
    0
  );
});

test("list: scans token records through one Redis session", async () => {
  const { auth } = await freshAuth();

  const state = authMockState();
  state.hashes.set("auth:token:tenant-token", {
    hash: "tenant-hash",
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.hashes.set("auth:token:other-token", {
    hash: "other-hash",
    kind: "ns",
    ns: "tenant-b",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });

  const res = await auth.list({ ns: "tenant-a" });

  assert.deepEqual(res.tokens.map((/** @type {any} */ t) => t.tokenId), ["tenant-token"]);
  assert.equal(state.sessions, 1);
  assert.deepEqual(
    state.commands.map((/** @type {any} */ entry) => entry.command),
    ["HGET", "SCAN", "HGETALL_PIPELINE"],
  );
});

test("list: malformed issue_templates is visible without breaking token list", async () => {
  const { auth } = await freshAuth();
  const state = authMockState();
  state.hashes.set("auth:token:bad-issuer", {
    hash: "b".repeat(64),
    kind: "token-issuer",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
    issue_templates: "not-json",
  });
  state.hashes.set("auth:token:missing-issuer", {
    hash: "c".repeat(64),
    kind: "token-issuer",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });

  const res = await auth.list({});
  const bad = res.tokens.find((/** @type {any} */ token) => token.tokenId === "bad-issuer");
  const missing = res.tokens.find((/** @type {any} */ token) => token.tokenId === "missing-issuer");

  assert.ok(bad, "malformed issuer should remain listable");
  assert.equal(bad.issueTemplatesInvalid, true);
  assert.ok(missing, "issuer missing issue_templates should remain listable");
  assert.equal(missing.issueTemplatesInvalid, true);
});

test("revoke: reads and tombstones through one Redis session", async () => {
  const { auth, authLib } = await freshAuth();

  const plaintext = "revocable-token";
  const tokenId = "tid-revoke";
  const hash = await authLib.hashToken(plaintext);
  const state = authMockState();
  state.hashes.set(`auth:token:${tokenId}`, {
    hash,
    kind: "ns",
    ns: "tenant-a",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${hash}`, tokenId);

  const res = await auth.revoke({ tokenId });

  assert.deepEqual(res, { revoked: true });
  assert.equal(state.sessions, 1);
  assert.equal(state.strings.has(`auth:hash:${hash}`), false);
  assert.ok(state.hashes.get(`auth:token:${tokenId}`).revoked_at);
  assert.deepEqual(
    state.commands.map((/** @type {any} */ entry) => entry.command),
    ["HGET", "HGETALL", "GET", "DEL", "HSET"],
  );
});

// (Note: rethrowPolicy's `lifecycle_threw` else-branch is defensive — every
// validate* function inside issue's try/catch throws AuthPolicyError, so a
// generic throw is unreachable through public input. The invalid_role_config
// + 4xx tests above cover the log-level split that matters for operator
// alerting.)
