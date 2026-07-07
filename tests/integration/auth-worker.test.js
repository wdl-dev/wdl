// auth worker E2E. Requires the docker compose stack (see helpers/).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  ADMIN_HOST_HEADER,
  ADMIN_TOKEN,
  ASSETS_CDN_BASE,
  assertStatus,
  adminGet,
  adminPost,
  fetchWithToken,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";

const packageJson = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8"));
const workerdVersion = packageJson.dependencies.workerd.replace(/^[~^]/, "");
const expectedPlatformVersion = `wdl.${workerdVersion.slice(2)}`;

setupIntegrationSuite();

test("bootstrap upserts the dev token as ops on cold start", async () => {
  // FLUSHALL in resetStack wipes auth state; the lazy upsert re-seeds.
  const r = await adminGet("/ns/foo/worker/x/versions");
  assert.equal(r.status, 200);
});

test("missing X-Admin-Token → 401", async () => {
  const r = await fetchWithToken(null, "/reload", { method: "POST" });
  assert.equal(r.status, 401);
  assert.equal(r.json?.error, "missing_token");
  assert.equal(r.json?.message, "unauthorized");
});

test("garbage X-Admin-Token → 401 (header hygiene + unknown token both unauth)", async () => {
  for (const t of ["", "  ", "tok\twith\ttab", "x".repeat(300), "a,b"]) {
    const r = await fetchWithToken(t, "/reload", { method: "POST" });
    assertStatus(r, 401, `case ${JSON.stringify(t)}`);
  }
});

test("wrong but well-shaped token → 401", async () => {
  const r = await fetchWithToken("not-the-real-token", "/reload", { method: "POST" });
  assert.equal(r.status, 401);
});

test("whoami returns current principal without exposing token material", async () => {
  const ops = await adminGet("/whoami");
  assert.equal(ops.status, 200);
  assert.equal(ops.json.ok, true);
  assert.deepEqual(ops.json.principal, { kind: "ops" });
  assert.equal(ops.json.tokenId, "bootstrap");
  assert.equal(typeof ops.json.requestId, "string");
  assert.equal(ops.json.platformVersion, expectedPlatformVersion);
  assert.equal(ops.json.minCliVersion, "0.11.0");
  assert.equal(Object.hasOwn(ops.json, "workerdVersion"), false);
  assert.deepEqual(ops.json.urls, {
    control: `http://${ADMIN_HOST_HEADER}`,
    assets: ASSETS_CDN_BASE,
  });
  assert.equal(Object.hasOwn(ops.json, "token"), false);
  assert.equal(Object.hasOwn(ops.json, "hash"), false);

  const ns = uniqueNs("auth-whoami");
  const issued = await adminPost("/auth/tokens", { ns, label: "whoami" });
  assert.equal(issued.status, 201);
  const tenant = await fetchWithToken(issued.json.token, "/whoami");
  assert.equal(tenant.status, 200);
  assert.equal(tenant.json.ok, true);
  assert.deepEqual(tenant.json.principal, { kind: "ns", ns });
  assert.equal(tenant.json.tokenId, issued.json.tokenId);
  assert.equal(tenant.json.platformVersion, expectedPlatformVersion);
  assert.equal(tenant.json.minCliVersion, "0.11.0");
  assert.deepEqual(tenant.json.urls, {
    control: `http://${ADMIN_HOST_HEADER}`,
    namespace: `http://${ns}.workers.local`,
    assets: ASSETS_CDN_BASE,
  });
  assert.equal(Object.hasOwn(tenant.json, "token"), false);
  assert.equal(Object.hasOwn(tenant.json, "hash"), false);
  assert.equal(JSON.stringify(tenant.json).includes(issued.json.token), false);

  const proxied = await fetchWithToken(issued.json.token, "/whoami", {
    headers: { "x-forwarded-proto": "https" },
  });
  assert.equal(proxied.status, 200);
  assert.deepEqual(proxied.json.urls, {
    control: `https://${ADMIN_HOST_HEADER}`,
    namespace: `https://${ns}.workers.local`,
    assets: ASSETS_CDN_BASE,
  });
});

test("issue → verify → revoke round-trip on a tenant ns", async () => {
  const ns = uniqueNs("auth-rt");
  const issued = await adminPost("/auth/tokens", { ns, label: "ci" });
  assert.equal(issued.status, 201);
  assert.equal(typeof issued.json.token, "string");
  assert.equal(typeof issued.json.tokenId, "string");
  assert.notEqual(issued.json.tokenId, "bootstrap");
  assert.equal(issued.json.token.length, 43);

  const tenantTok = issued.json.token;
  const dep = await fetchWithToken(tenantTok, `/ns/${ns}/worker/hello/deploy`, {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('ok')} };" },
  });
  assert.equal(dep.status, 201, `deploy status: ${dep.status} ${dep.text}`);

  const revoked = await fetchWithToken(ADMIN_TOKEN, `/auth/tokens/${issued.json.tokenId}`, {
    method: "DELETE",
  });
  assert.equal(revoked.status, 200);
  assert.equal(revoked.json.revoked, true);

  const again = await fetchWithToken(ADMIN_TOKEN, `/auth/tokens/${issued.json.tokenId}`, {
    method: "DELETE",
  });
  assert.equal(again.status, 200);
  assert.equal(again.json.revoked, false);

  const post = await fetchWithToken(tenantTok, `/ns/${ns}/worker/hello/versions`);
  assert.equal(post.status, 401);

  const whoami = await fetchWithToken(tenantTok, "/whoami");
  assert.equal(whoami.status, 401);
});

test("token-issuer creates a delegated tenant ns token from server template", async () => {
  const bootstrapDelegated = await adminPost("/auth/delegated-tokens", {
    template: "wdl-chat-ns-pool",
  });
  assert.equal(bootstrapDelegated.status, 403);
  assert.equal(bootstrapDelegated.json.error, "issuer_not_token_issuer");

  const malformedPath = await adminPost("/auth/delegated-tokens/extra", {
    template: "wdl-chat-ns-pool",
  });
  assert.equal(malformedPath.status, 400);
  assert.equal(malformedPath.json.error, "invalid_path");

  const issuer = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    label: "chat issuer",
  });
  assert.equal(issuer.status, 201);

  const delegated = await fetchWithToken(issuer.json.token, "/auth/delegated-tokens", {
    method: "POST",
    body: { template: "wdl-chat-ns-pool" },
  });
  assert.equal(delegated.status, 201, delegated.text);
  assert.equal(typeof delegated.json.token, "string");
  assert.equal(delegated.json.kind, "ns");
  assert.match(delegated.json.ns, /^tmp-[0-9a-f]{8}$/);
  assert.equal(delegated.json.label, `workshop-pool ${delegated.json.ns}`);
  assert.equal(delegated.json.issueTemplate, "wdl-chat-ns-pool");
  assert.equal(delegated.json.issueTemplateVersion, "1");

  const ownNs = await fetchWithToken(delegated.json.token, `/ns/${delegated.json.ns}/workers`);
  assert.equal(ownNs.status, 200, ownNs.text);

  const directLifecycle = await fetchWithToken(issuer.json.token, "/auth/tokens", {
    method: "GET",
  });
  assert.equal(directLifecycle.status, 403);
  assert.equal(directLifecycle.json.error, "auth_lifecycle_requires_ops");

  const directIssue = await fetchWithToken(issuer.json.token, "/auth/tokens", {
    method: "POST",
    body: { kind: "ns", ns: uniqueNs("auth-delegated-direct") },
  });
  assert.equal(directIssue.status, 403);
  assert.equal(directIssue.json.error, "auth_lifecycle_requires_ops");
});

test("token issue rejects storage-shaped issue_templates in HTTP body", async () => {
  const r = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
    issue_templates: ["wdl-chat-ns-pool"],
  });
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "invalid_template_request");
});

test("delegated issue rejects caller-provided direct token fields", async () => {
  const issuer = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
  });
  assert.equal(issuer.status, 201);

  for (const body of [
    { template: "wdl-chat-ns-pool", ns: uniqueNs("auth-delegated") },
    { template: "wdl-chat-ns-pool", foo: "bar" },
  ]) {
    const r = await fetchWithToken(issuer.json.token, "/auth/delegated-tokens", {
      method: "POST",
      body,
    });
    assert.equal(r.status, 400);
    assert.equal(r.json.error, "invalid_template_request");
  }
});

test("delegated issue quota response preserves additive details through AUTH binding", async () => {
  const issuer = await adminPost("/auth/tokens", {
    kind: "token-issuer",
    issueTemplates: ["wdl-chat-ns-pool"],
  });
  assert.equal(issuer.status, 201);

  let quotaExceeded;
  for (let i = 0; i <= 100; i += 1) {
    const r = await fetchWithToken(issuer.json.token, "/auth/delegated-tokens", {
      method: "POST",
      body: { template: "wdl-chat-ns-pool" },
    });
    if (r.status === 409) {
      quotaExceeded = r;
      break;
    }
    assert.equal(r.status, 201, `delegated issue ${i} failed: ${r.status} ${r.text}`);
  }

  assert.ok(quotaExceeded, "expected active quota to be exhausted");
  assert.equal(quotaExceeded.json.error, "active_quota_exceeded");
  assert.equal(quotaExceeded.json.active, 100);
  assert.equal(quotaExceeded.json.quota, 100);
  assert.equal(quotaExceeded.json.available, 0);
});

test("expired token: verify lazily DELs the hash index and tombstones the record with expiredAt", async () => {
  const ns = uniqueNs("auth-exp");
  // Wait below is computed off expiresAt so a slow test head can't eat
  // into the cushion and let the assertion fire before expiry.
  const expiresAt = new Date(Date.now() + 5000).toISOString();
  const issued = await adminPost("/auth/tokens", { ns, label: "exp", expiresAt });
  assert.equal(issued.status, 201);
  const tok = issued.json.token;
  const tokenId = issued.json.tokenId;

  const pre = await fetchWithToken(tok, `/ns/${ns}/workers`);
  assert.equal(pre.status, 200, `pre-expiry list must succeed: ${pre.status} ${pre.text}`);

  /** @type {{ value: Awaited<ReturnType<typeof fetchWithToken>> | null }} */
  const expiredCall = { value: null };
  await waitUntil("auth token expires in system-runtime", async () => {
    expiredCall.value = await fetchWithToken(tok, `/ns/${ns}/workers`);
    return expiredCall.value.status === 401;
  }, { timeoutMs: 15000, intervalMs: 500 });
  if (!expiredCall.value) throw new Error("expiry wait completed without a token check");
  assert.equal(expiredCall.value.status, 401, "first post-expiry call must 401");

  const listed = await adminGet("/auth/tokens");
  assert.equal(listed.status, 200);
  const entry = listed.json.tokens.find((/** @type {any} */ t) => t.tokenId === tokenId);
  assert.ok(entry, "expired token record must remain visible in list");
  assert.equal(entry.expiresAt, expiresAt);
  assert.ok(typeof entry.expiredAt === "string" && entry.expiredAt.endsWith("Z"),
    `expected expiredAt ISO string, got ${JSON.stringify(entry.expiredAt)}`);

  // Hash index DEL'd → second call short-circuits before HGETALL.
  const repeat = await fetchWithToken(tok, `/ns/${ns}/workers`);
  assert.equal(repeat.status, 401, "subsequent calls must continue to 401");

  // Revoke after expiry-GC: `revoked: false` means "no state mutation",
  // not an error — the hash was already gone.
  const rev = await fetchWithToken(ADMIN_TOKEN, `/auth/tokens/${tokenId}`, {
    method: "DELETE",
  });
  assert.equal(rev.status, 200);
  assert.equal(rev.json.revoked, false);
});

test("issue rejects reserved ns + reserved tenant name + missing ns + past expiresAt", async () => {
  const a = await adminPost("/auth/tokens", { ns: "__system__" });
  assert.equal(a.status, 400);
  assert.equal(a.json.error, "reserved_ns");

  const adm = await adminPost("/auth/tokens", { ns: "admin" });
  assert.equal(adm.status, 400);
  assert.equal(adm.json.error, "reserved_tenant_ns");

  const b = await adminPost("/auth/tokens", {});
  assert.equal(b.status, 400);
  assert.equal(b.json.error, "missing_ns");

  const c = await adminPost("/auth/tokens", {
    ns: uniqueNs("auth-past"),
    expiresAt: new Date(Date.now() - 60000).toISOString(),
  });
  assert.equal(c.status, 400);
  assert.equal(c.json.error, "expired_at_in_past");
});

test("list rejects reserved tenant name filter (parallel to reserved ns filter)", async () => {
  const r = await adminGet("/auth/tokens?ns=admin");
  assert.equal(r.status, 400);
  assert.equal(r.json.error, "reserved_tenant_ns");
});

test("revoke(\"bootstrap\") → 403", async () => {
  const r = await fetchWithToken(ADMIN_TOKEN, "/auth/tokens/bootstrap", {
    method: "DELETE",
  });
  assert.equal(r.status, 403);
  assert.equal(r.json.error, "bootstrap_protected");
});

test("list returns metadata without plaintext and filters by namespace", async () => {
  const ns = uniqueNs("auth-list");
  const issued = await adminPost("/auth/tokens", { ns, label: "first" });
  assert.equal(issued.status, 201);

  const all = await adminGet("/auth/tokens");
  assert.equal(all.status, 200);
  const ids = all.json.tokens.map((/** @type {any} */ t) => t.tokenId);
  assert.ok(ids.includes("bootstrap"), "bootstrap record should be visible");
  assert.ok(ids.includes(issued.json.tokenId));
  for (const t of all.json.tokens) {
    assert.equal(Object.hasOwn(t, "token"), false,
      "list must never return plaintext");
    assert.equal(Object.hasOwn(t, "hash"), false);
  }

  const filtered = await adminGet(`/auth/tokens?ns=${ns}`);
  assert.equal(filtered.status, 200);
  assert.deepEqual(filtered.json.tokens.map((/** @type {any} */ t) => t.tokenId), [issued.json.tokenId]);

  const bad = await adminGet("/auth/tokens?ns=__system__");
  assert.equal(bad.status, 400);
  assert.equal(bad.json.error, "reserved_ns");
});

test("ns token: same-ns deploy passes, cross-ns deploy denied 403", async () => {
  const owned = uniqueNs("auth-ns-own");
  const other = uniqueNs("auth-ns-other");
  const issued = await adminPost("/auth/tokens", { ns: owned });
  assert.equal(issued.status, 201);
  const tok = issued.json.token;

  const ok = await fetchWithToken(tok, `/ns/${owned}/worker/hi/deploy`, {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('ok')} };" },
  });
  assert.equal(ok.status, 201);

  const bad = await fetchWithToken(tok, `/ns/${other}/worker/hi/deploy`, {
    method: "POST",
    body: { code: "export default { fetch(){return new Response('x')} };" },
  });
  assert.equal(bad.status, 403);
});

test("ns token cannot reach ops endpoints (/reload, /auth/*, POST /hosts, reserved ns)", async () => {
  const owned = uniqueNs("auth-ns-only");
  const issued = await adminPost("/auth/tokens", { ns: owned });
  const tok = issued.json.token;

  for (const [path, method] of [
    ["/reload", "POST"],
    ["/auth/tokens", "POST"],
    ["/auth/tokens", "GET"],
    [`/ns/${owned}/hosts`, "POST"],
    ["/ns/__system__/worker/x/deploy", "POST"],
  ]) {
    const r = await fetchWithToken(tok, path, {
      method,
      body: method === "POST" ? "{}" : undefined,
    });
    assert.equal(r.status, 403,
      `${method} ${path} should 403 for ns token, got ${r.status}`);
  }
});

test("ns token reads GET /ns/<own>/hosts diagnostic endpoint", async () => {
  const owned = uniqueNs("auth-ns-diag");
  const issued = await adminPost("/auth/tokens", { ns: owned });
  const tok = issued.json.token;
  const r = await fetchWithToken(tok, `/ns/${owned}/hosts`);
  assert.equal(r.status, 200);
  assert.equal(r.json.namespace, owned);
});

test("concurrent revoke: exactly one call returns { revoked: true }", async () => {
  const ns = uniqueNs("auth-revoke-race");
  const issued = await adminPost("/auth/tokens", { ns });
  assert.equal(issued.status, 201);
  const tokenId = issued.json.tokenId;

  // Both calls race past the pre-MULTI GET; only one's DEL collapses
  // the hash index, the other reports false.
  const path = `/auth/tokens/${tokenId}`;
  const [a, b] = await Promise.all([
    fetchWithToken(ADMIN_TOKEN, path, { method: "DELETE" }),
    fetchWithToken(ADMIN_TOKEN, path, { method: "DELETE" }),
  ]);
  assert.equal(a.status, 200);
  assert.equal(b.status, 200);
  const wins = [a.json.revoked, b.json.revoked].filter((x) => x === true).length;
  assert.equal(wins, 1, `exactly one revoke: true expected, got a=${a.json.revoked} b=${b.json.revoked}`);
});
