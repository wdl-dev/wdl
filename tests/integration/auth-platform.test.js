// @wdl-cli-integration
//
// Locks the wire-level role behavior introduced by the ROLES table
// rewrite. Existing auth-worker.test.js still covers ops + tenant ns
// flows; this file is the platform / platform-observer / ops-observer
// matrix plus the new reason taxonomy on classifier paths.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminGet,
  adminPost,
  deployAndPromote,
  fetchWithToken,
  parseJsonText,
  responseJson,
  sh,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisHGetAll } from "./helpers/redis.js";

setupIntegrationSuite();

// --- issue: kind validation -------------------------------------------------

test("issue: {kind:'platform', ns:'__platform__'} → 201 with ns field", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__", label: "platform-team",
  });
  assert.equal(r.status, 201);
  assert.equal(typeof r.json.token, "string");
  assert.equal(typeof r.json.tokenId, "string");
});

test("issue: {kind:'platform-observer', ns:'__platform__'} → 201", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "platform-observer", ns: "__platform__",
  });
  assert.equal(r.status, 201);
});

test("issue: {kind:'ops-observer'} (no ns) → 201", async () => {
  const r = await adminPost("/auth/tokens", { kind: "ops-observer" });
  assert.equal(r.status, 201);
});

test("issue: {kind:'token-issuer', issueTemplates:[...]} → 201 and list shows allowlist", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
  });
  assert.equal(r.status, 201);

  const list = await adminGet("/auth/tokens");
  assert.equal(list.status, 200);
  const entry = list.json.tokens.find((/** @type {any} */ t) => t.tokenId === r.json.tokenId);
  assert.ok(entry, "token-issuer must be listed");
  assert.equal(entry.kind, "token-issuer");
  assert.deepEqual(entry.issueTemplates, ["wdl-chat-ns-pool"]);
  assert.equal(Object.hasOwn(entry, "ns"), false);
});

test("issue: {kind:'platform'} (no ns) → 400 invalid_bound_ns", async () => {
  const r = await adminPost("/auth/tokens", { kind: "platform" });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "invalid_bound_ns");
});

test("issue: {kind:'platform', ns:'tenant-foo'} → 400 invalid_bound_ns", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "platform", ns: "tenant-foo",
  });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "invalid_bound_ns");
});

test("issue: {kind:'platform', ns:'__system__'} → 400 invalid_bound_ns (set check, not isReservedNs)", async () => {
  // __system__ is reserved but NOT in PLATFORM_TIER_RESERVED_NS.
  // If validateIssueInput used isReservedNs alone, this would slip through.
  const r = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__system__",
  });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "invalid_bound_ns");
});

test("issue: {kind:'ops-observer', ns:'tenant-foo'} → 400 role_no_ns", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "ops-observer", ns: "tenant-foo",
  });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "role_no_ns");
});

test("issue: {kind:'token-issuer'} requires camelCase issueTemplates", async () => {
  const missing = await adminPost("/auth/tokens", { kind: "token-issuer" });
  assert.equal(missing.status, 400);
  assert.equal(missing.json.error, "invalid_template_request");

  const snake = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issue_templates: ["wdl-chat-ns-pool"],
  });
  assert.equal(snake.status, 400);
  assert.equal(snake.json.error, "invalid_template_request");
});

test("issue: {kind:'unknown-role'} → 400 unknown_role", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "unknown-role", ns: "tenant-foo",
  });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "unknown_role");
});

test("issue: {kind:'ops'} → 400 ops_not_issuable (bootstrap-only)", async () => {
  const r = await adminPost("/auth/tokens", { kind: "ops" });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "ops_not_issuable");
});

// --- list: token records reflect kind + ns shape ----------------------------

test("list: platform token entry has ns field; ops-observer entry does NOT", async () => {
  const a = await adminPost("/auth/tokens", { kind: "platform", ns: "__platform__" });
  const b = await adminPost("/auth/tokens", { kind: "ops-observer" });
  assert.equal(a.status, 201);
  assert.equal(b.status, 201);

  const list = await adminGet("/auth/tokens");
  assert.equal(list.status, 200);
  const platform = list.json.tokens.find((/** @type {any} */ t) => t.tokenId === a.json.tokenId);
  const observer = list.json.tokens.find((/** @type {any} */ t) => t.tokenId === b.json.tokenId);
  assert.ok(platform, "platform token must be listed");
  assert.ok(observer, "ops-observer token must be listed");
  assert.equal(platform.kind, "platform");
  assert.equal(platform.ns, "__platform__");
  // Critical: none-bound role must NOT have an ns key in the list entry.
  // Otherwise the issue() splat-undefined regression (HSET ns "undefined")
  // could leak as a string-valued ns through to list responses.
  assert.equal(observer.kind, "ops-observer");
  assert.equal(Object.hasOwn(observer, "ns"), false,
    `ops-observer list entry must omit ns, got ${JSON.stringify(observer)}`);
});

// --- platform: works inside __platform__, denied elsewhere ------------------

test("platform token: deploy to __platform__ ok", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__",
  });
  assert.equal(issued.status, 201);
  const tok = issued.json.token;

  const dep = await fetchWithToken(tok, "/ns/__platform__/worker/p/deploy", {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('p')} };" },
  });
  assert.equal(dep.status, 201, `deploy: ${dep.status} ${dep.text}`);
});

test("platform token: deploy to tenant ns → 403 ns_not_in_scope", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__",
  });
  const tok = issued.json.token;
  const ns = uniqueNs("plat-deny");

  const dep = await fetchWithToken(tok, `/ns/${ns}/worker/x/deploy`, {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(dep.status, 403);
  assert.equal(dep.json.error, "ns_not_in_scope");
});

test("platform token: POST /auth/tokens → 403 auth_lifecycle_requires_ops", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__",
  });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/auth/tokens", {
    method: "POST",
    body: { ns: "tenant-foo" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "auth_lifecycle_requires_ops");
});

test("platform token: POST /reload → 403 system_action_requires_ops", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__",
  });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/reload", { method: "POST", body: "{}" });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "system_action_requires_ops");
});

test("platform token: POST /ns/__platform__/hosts → 403 action_not_in_scope (host.write not in ROLES.platform)", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__",
  });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__platform__/hosts", {
    method: "POST",
    body: { hosts: [] },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

// --- platform-observer: read-only ------------------------------------------

test("platform-observer: GET /ns/__platform__/workers → 200", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform-observer", ns: "__platform__",
  });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__platform__/workers");
  assert.equal(r.status, 200, `${r.status} ${r.text}`);
});

test("platform-observer: deploy → 403 action_not_in_scope (no write)", async () => {
  const issued = await adminPost("/auth/tokens", {
    kind: "platform-observer", ns: "__platform__",
  });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__platform__/worker/p/deploy", {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('p')} };" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

// --- ops-observer: cross-ns read, no writes, no auth lifecycle -------------

test("ops-observer: GET /ns/<tenant>/workers → 200 (cross-ns read)", async () => {
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;
  const ns = uniqueNs("obs-read");

  const r = await fetchWithToken(tok, `/ns/${ns}/workers`);
  assert.equal(r.status, 200);
});

test("ops-observer: GET /ns/__system__/workers → 200 (red line 1 bypass + ROLES allows)", async () => {
  // CRITICAL: ops-observer bypasses red line 1 for __system__ because
  // its actions list permits worker.list. If red line 1 short-circuited
  // (only let ops bypass), this would 403.
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__system__/workers");
  assert.equal(r.status, 200);
});

test("ops-observer: deploy to tenant ns → 403 action_not_in_scope (writes denied)", async () => {
  // CRITICAL regression: if red line 1 short-circuited ops-observer
  // (not just bypassed it), this could fall through to ROLES check and
  // potentially permit; the test pins that the read-only actions list
  // narrows it.
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;
  const ns = uniqueNs("obs-write");

  const r = await fetchWithToken(tok, `/ns/${ns}/worker/x/deploy`, {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

test("ops-observer: deploy to __system__ → 403 action_not_in_scope (NOT reserved_ns_requires_ops)", async () => {
  // The reason MATTERS here: we want to lock that ops-observer falls
  // through red line 1 into ROLES (action_not_in_scope), NOT that it
  // gets stopped by red line 1 (reserved_ns_requires_ops). The latter
  // would mean the bypass logic is wrong.
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__system__/worker/x/deploy", {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

test("ops-observer: POST /reload → 403 system_action_requires_ops", async () => {
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/reload", { method: "POST", body: "{}" });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "system_action_requires_ops");
});

test("ops-observer: POST /auth/tokens → 403 auth_lifecycle_requires_ops", async () => {
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/auth/tokens", {
    method: "POST",
    body: { ns: "tenant-foo" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "auth_lifecycle_requires_ops");
});

test("ops-observer: GET /auth/tokens → 403 auth_lifecycle_requires_ops (token list is attack-surface enumeration)", async () => {
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/auth/tokens");
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "auth_lifecycle_requires_ops");
});

test("ops-observer: GET /ns/admin/workers → 403 reserved_tenant_ns_requires_ops (red line 2 strict ops)", async () => {
  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/admin/workers");
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "reserved_tenant_ns_requires_ops");
});

test("ops-observer: GET /ns/<tenant>/secrets → 403 action_not_in_scope (cross-tenant enumeration denied)", async () => {
  const ns = uniqueNs("obs-secret");

  // secret.read is intentionally absent from ROLES["ops-observer"] to block
  // cross-tenant secret enumeration, even for read-only observer tokens.
  // Seed a real secret first so the 403 is from auth, not "not found".
  const put = await adminFetch(`/ns/${ns}/secrets/MY_KEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });
  assert.equal(put.status, 200, `seed: ${put.status}`);

  const issued = await adminPost("/auth/tokens", { kind: "ops-observer" });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, `/ns/${ns}/secrets`);
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

// --- ns / new reason names ---------------------------------------------------

test("ns token POST /ns/<own>/hosts → 403 action_not_in_scope (was ops_gate_requires_ops)", async () => {
  // Locks the reason rename: with the role rewrite, host.write goes
  // through ROLES check instead of a synthetic ops gate.
  const ns = uniqueNs("ns-host-deny");
  const issued = await adminPost("/auth/tokens", { kind: "ns", ns });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, `/ns/${ns}/hosts`, {
    method: "POST",
    body: { hosts: [] },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "action_not_in_scope");
});

test("ns token deploy to __platform__ → 403 ns_not_in_scope (was reserved_ns_requires_ops)", async () => {
  // Red line 1 lets PLATFORM_TIER_RESERVED_NS members fall through to
  // ROLES; tenant principalNs ≠ __platform__ → ns_not_in_scope.
  const ns = uniqueNs("ns-platform-deny");
  const issued = await adminPost("/auth/tokens", { kind: "ns", ns });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__platform__/worker/x/deploy", {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "ns_not_in_scope");
});

test("ns token deploy to __system__ returns 403 reserved_ns_requires_ops", async () => {
  // Red line 1 still fires for non-PLATFORM_TIER reserved ns: __system__.
  const ns = uniqueNs("ns-sys-deny");
  const issued = await adminPost("/auth/tokens", { kind: "ns", ns });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/ns/__system__/worker/x/deploy", {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "reserved_ns_requires_ops");
});

test("any non-ops token: unknown shape → 403 unknown_action_requires_ops", async () => {
  const ns = uniqueNs("ns-unknown");
  const issued = await adminPost("/auth/tokens", { kind: "ns", ns });
  const tok = issued.json.token;

  const r = await fetchWithToken(tok, "/totally-unknown-path", {
    method: "POST", body: "{}",
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "unknown_action_requires_ops");
});

// --- HGETALL Redis token shape: lock the wire layout ------------------------
// Direct HGETALL is defense-in-depth: a write-side bug stuffing `ns:"undefined"`
// into Redis and a list-side bug stripping it before output would mask each
// other; the list-end test alone can't catch that pair.

test("HGETALL auth:token:<id>: ns-bound token records ns; ops-observer omits ns entirely", async () => {
  const ns = uniqueNs("hgetall");
  const a = await adminPost("/auth/tokens", { kind: "ns", ns, label: "a" });
  const b = await adminPost("/auth/tokens", { kind: "ops-observer", label: "b" });
  assert.equal(a.status, 201);
  assert.equal(b.status, 201);

  const aHash = redisHGetAll(`auth:token:${a.json.tokenId}`);
  assert.equal(aHash.kind, "ns");
  assert.equal(aHash.ns, ns);
  assert.equal(aHash.label, "a");
  assert.equal(aHash.created_by, "bootstrap");
  assert.equal(typeof aHash.hash, "string");
  assert.match(aHash.hash, /^[0-9a-f]{64}$/);
  assert.match(aHash.created_at, /^\d{4}-\d{2}-\d{2}T.*Z$/);

  const bHash = redisHGetAll(`auth:token:${b.json.tokenId}`);
  assert.equal(bHash.kind, "ops-observer");
  // buildHsetCmd must skip the undefined ns for none-bound roles —
  // if it ever wrote ns="undefined" this catches it at the wire layout.
  assert.equal(Object.hasOwn(bHash, "ns"), false,
    `ops-observer Redis hash must omit ns key, got: ${JSON.stringify(bHash)}`);
  assert.equal(bHash.label, "b");
  assert.equal(bHash.created_by, "bootstrap");
  assert.match(bHash.hash, /^[0-9a-f]{64}$/);

  // Plaintext token must never be persisted.
  assert.equal(Object.hasOwn(aHash, "token"), false);
  assert.equal(Object.hasOwn(bHash, "token"), false);
});

// --- referrer cross-ns: platform double-pin (the only allowed cross-ns read)
// The platform branch in formatReferrerBlocker is the only path in the
// system that lets a non-ops principal read another tenant's identifiers.
// A regression that drops the `targetNs === principal.ns` half-check would
// silently widen this surface.

const PLATFORM_TARGET_SRC = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Echo extends WorkerEntrypoint { async ping() { return "ok"; } }
export default { fetch() { return new Response("404", { status: 404 }) } };
`;

test("referrer cross-ns DELETE blocker: ops + platform-double-pin see full caller; other-tenant blocked at auth", async () => {
  const callerNs = uniqueNs("ref-pf");

  await deployAndPromote("__platform__", "p", {
    code: PLATFORM_TARGET_SRC,
    exports: [{
      entrypoint: "Echo", as: "PTEST",
      allowedCallers: ["*"], requiredCallerSecrets: [],
    }],
  });

  // [[platform_bindings]] → linker expands to {ns:"__platform__", ...},
  // writing the cross-ns referrer to worker-version-referrers:__platform__:p:v1.
  await deployAndPromote(callerNs, "app", {
    code: `export default { async fetch(req, env) { return Response.json(await env.DEMO.ping()); } };`,
    platformBindings: [{ binding: "DEMO", platform: "PTEST" }],
  });

  // Promote v2 so v1 is retained-but-non-active and eligible for DELETE.
  await deployAndPromote("__platform__", "p", {
    code: PLATFORM_TARGET_SRC,
    exports: [{
      entrypoint: "Echo", as: "PTEST",
      allowedCallers: ["*"], requiredCallerSecrets: [],
    }],
  });

  const ops = await adminFetch(`/ns/__platform__/worker/p/versions/v1`, { method: "DELETE" });
  assert.equal(ops.status, 409);
  const opsBody = await responseJson(ops);
  assert.equal(opsBody.error, "version_referenced");
  assert.equal(opsBody.referrers.length, 1);
  assert.equal(opsBody.referrers[0].callerNs, callerNs);
  assert.equal(opsBody.referrers[0].callerWorker, "app");
  assert.equal(opsBody.crossNamespaceReferrerCount, undefined);

  // Platform double-pin: kind=platform AND targetNs===principal.ns → full list.
  const platIssue = await adminPost("/auth/tokens", {
    kind: "platform", ns: "__platform__", label: "ref-test",
  });
  assert.equal(platIssue.status, 201);
  const platTok = platIssue.json.token;
  const plat = await fetchWithToken(platTok, `/ns/__platform__/worker/p/versions/v1`, {
    method: "DELETE",
  });
  assert.equal(plat.status, 409);
  assert.equal(plat.json.error, "version_referenced");
  assert.equal(plat.json.referrers.length, 1,
    "platform double-pin must surface the cross-ns caller in full");
  assert.equal(plat.json.referrers[0].callerNs, callerNs);
  assert.equal(plat.json.referrers[0].callerWorker, "app");
  assert.equal(plat.json.crossNamespaceReferrerCount, undefined,
    "double-pin must NOT collapse cross-ns into a count");

  // A different tenant's ns-token can't reach the endpoint at all (auth
  // returns ns_not_in_scope before the handler runs) — pins that the
  // platform branch is exclusive to platform principals.
  const otherNs = uniqueNs("ref-other");
  const otherIssue = await adminPost("/auth/tokens", { kind: "ns", ns: otherNs });
  const otherTok = otherIssue.json.token;
  const other = await fetchWithToken(otherTok, `/ns/__platform__/worker/p/versions/v1`, {
    method: "DELETE",
  });
  assert.equal(other.status, 403);
  assert.equal(other.json.error, "ns_not_in_scope");
});

test("referrer cross-ns DELETE blocker: same-tenant ns sees only same-ns details + crossNamespaceReferrerCount (no platform leakage)", async () => {
  // Mirror image of the platform test: deploy a tenant target, have
  // BOTH a same-ns caller and a __platform__ caller (via service binding),
  // delete the target version with the tenant's own ns-token. Pins that
  // ns principals never get the platform-style full-list view, even
  // when the cross-ns referrer is __platform__.
  const targetNs = uniqueNs("ref-tn");

  await deployAndPromote(targetNs, "api", {
    code: "export default {fetch(){return new Response('api v1')}};",
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });
  // Same-ns caller (visible to ns-token).
  await deployAndPromote(targetNs, "self-caller", {
    code: "export default {fetch(){return new Response('self')}};",
    bindings: { API: { type: "service", service: "api" } },
  });
  // Cross-ns __platform__ caller (must be redacted from ns-token's view).
  await deployAndPromote("__platform__", "platform-caller", {
    code: "export default {fetch(){return new Response('pf')}};",
    bindings: { API: { type: "service", ns: targetNs, service: "api" } },
  });
  // Promote v2 so v1 retained.
  await deployAndPromote(targetNs, "api", {
    code: "export default {fetch(){return new Response('api v2')}};",
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });

  const issued = await adminPost("/auth/tokens", { kind: "ns", ns: targetNs });
  const nsTok = issued.json.token;
  const r = await fetchWithToken(nsTok, `/ns/${targetNs}/worker/api/versions/v1`, {
    method: "DELETE",
  });
  assert.equal(r.status, 409);
  assert.equal(r.json.error, "version_referenced");
  assert.equal(r.json.referrers.length, 1);
  assert.equal(r.json.referrers[0].callerNs, targetNs);
  assert.equal(r.json.referrers[0].callerWorker, "self-caller");
  assert.equal(r.json.crossNamespaceReferrerCount, 1,
    "ns principal must not see the __platform__ caller's identifiers");
  assert.ok(!r.text.includes("__platform__"),
    "no __platform__ literal anywhere in the response body");
  assert.ok(!r.text.includes("platform-caller"),
    "no cross-ns worker name leak");
});

// --- observability contract: auth_verify + auth_issue log payload ---------
// Scrapes system-runtime stdout for structured auth_verify / auth_issue
// records on ok + reject paths and pins the `gate` → `action` rename
// (no `gate` key in any auth log) plus lifecycle signal isolation.
// Auth is a socket-less JSRPC worker, so these logs are the observable product
// contract; it does not maintain a private metrics registry.

/** @param {string} servicesGrep @param {string} eventName */
function findStructuredLogLines(servicesGrep, eventName) {
  const raw = sh(`docker compose logs --no-color --tail=2000 ${servicesGrep}`);
  /** @type {any[]} */
  const out = [];
  for (const line of raw.split("\n")) {
    // workerd console.* surfaces both raw + structured; we want the
    // structured form (single-line JSON object). Quick filter then parse.
    const idx = line.indexOf("{");
    if (idx < 0) continue;
    const candidate = line.slice(idx);
    if (!candidate.includes(`"event":"${eventName}"`)) continue;
    try {
      const obj = parseJsonText(candidate, `${eventName} structured log line`);
      if (obj.event === eventName) out.push(obj);
    } catch { /* not JSON, skip */ }
  }
  return out;
}

test("observability contract: auth_verify ok + reject log payload shape, no gate field, request_id propagation", async () => {
  // Use unique request IDs so we can pinpoint the exact log lines this
  // test produced — system-runtime stdout is shared across the whole run.
  const okReqId = `obs-ok-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const rejectReqId = `obs-reject-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const ns = uniqueNs("obs-auth");

  // ok path: bootstrap (ops) GET /ns/<ns>/workers — authorized → outcome=ok.
  const okRes = await adminFetch(`/ns/${ns}/workers`, {
    method: "GET",
    headers: { "x-request-id": okReqId },
  });
  assert.equal(okRes.status, 200);

  // reject path: ns-token attempting host.write on its own ns →
  // action_not_in_scope (host.write deliberately omitted from ROLES.ns).
  const issued = await adminPost("/auth/tokens", { kind: "ns", ns });
  const nsTok = issued.json.token;
  const rejectRes = await fetchWithToken(nsTok, `/ns/${ns}/hosts`, {
    method: "POST",
    body: { hosts: [] },
    headers: { "x-request-id": rejectReqId },
  });
  assert.equal(rejectRes.status, 403);
  assert.equal(rejectRes.json.error, "action_not_in_scope");

  // workerd console output is async w.r.t. the fetch return.
  /** @type {any} */ let okLog = null;
  /** @type {any} */ let rejectLog = null;
  await waitUntil("auth_verify log lines surfaced", () => {
    const lines = findStructuredLogLines("system-runtime", "auth_verify");
    okLog = lines.find((l) => l.request_id === okReqId);
    rejectLog = lines.find((l) => l.request_id === rejectReqId);
    return Boolean(okLog && rejectLog);
  }, { timeoutMs: 8000, intervalMs: 250 });
  assert.ok(okLog && rejectLog, "waitUntil should have populated both logs");

  assert.equal(okLog.outcome, "ok");
  assert.equal(okLog.reason, "allow");
  assert.equal(okLog.action, "worker.list");
  assert.equal(okLog.category, "worker");
  assert.equal(okLog.principal_kind, "ops");
  assert.equal(okLog.target_ns, ns);
  assert.equal(typeof okLog.duration_ms, "number");
  // Drift sentinel: any `gate` field would mean an emit site got missed
  // during the gate → action rename.
  assert.equal(Object.hasOwn(okLog, "gate"), false,
    `auth_verify log must NOT carry "gate" field, got: ${JSON.stringify(okLog)}`);

  assert.equal(rejectLog.outcome, "reject");
  assert.equal(rejectLog.reason, "action_not_in_scope");
  assert.equal(rejectLog.action, "host.write");
  assert.equal(rejectLog.category, "host");
  assert.equal(rejectLog.principal_kind, "ns");
  assert.equal(rejectLog.principal_ns, ns);
  assert.equal(rejectLog.target_ns, ns);
  assert.equal(Object.hasOwn(rejectLog, "gate"), false);

  // Log fields are derived from the record, never the input token.
  const tokSubstr = nsTok.slice(0, 20);
  assert.ok(!JSON.stringify(rejectLog).includes(tokSubstr),
    "plaintext token prefix must not leak into auth_verify log");
});

test("observability contract: auth_issue lifecycle log carries no verify-side fields (signal isolation)", async () => {
  const reqId = `obs-issue-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  const r = await adminFetch("/auth/tokens", {
    method: "POST",
    body: JSON.stringify({ kind: "platform", ns: "__platform__", label: "obs-iso" }),
    headers: { "x-request-id": reqId },
  });
  assert.equal(r.status, 201);
  const issuedBody = await responseJson(r);
  const issuedPlaintext = issuedBody && issuedBody.token;

  /** @type {any} */ let issueLog = null;
  await waitUntil("auth_issue log line surfaced", () => {
    const lines = findStructuredLogLines("system-runtime", "auth_issue");
    issueLog = lines.find((l) => l.request_id === reqId);
    return Boolean(issueLog);
  }, { timeoutMs: 8000, intervalMs: 250 });
  assert.ok(issueLog, "waitUntil should have populated issueLog");

  // lifecycle event isolation: keep the verify-side dimensions out of
  // lifecycle logs so on-call can grep cleanly per signal source.
  assert.equal(issueLog.outcome, "ok");
  assert.equal(typeof issueLog.token_id, "string");
  assert.ok(issueLog.token_id.length > 0);
  assert.equal(issueLog.principal_kind, "platform");
  assert.equal(issueLog.target_ns, "__platform__");
  // Drift sentinels — these must NOT appear on lifecycle events.
  assert.equal(Object.hasOwn(issueLog, "action"), false,
    `auth_issue must not carry verify-side "action" field, got: ${JSON.stringify(issueLog)}`);
  assert.equal(Object.hasOwn(issueLog, "category"), false,
    `auth_issue must not carry verify-side "category" field`);
  assert.equal(Object.hasOwn(issueLog, "gate"), false);
  // Plaintext token (returned exactly once in HTTP body) must not appear
  // in the log payload — token_id is fine, plaintext is not.
  if (issuedPlaintext) {
    assert.ok(!JSON.stringify(issueLog).includes(issuedPlaintext),
      "auth_issue log must not echo the issued plaintext token");
  }
});
