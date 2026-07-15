// GET /ns/<ns>/workers, DELETE /ns/<ns>/worker/<name>/versions/<v>,
// POST /ns/<ns>/worker/<name>/delete (with ?dry_run=1).
//
// Each test FLUSHALLs.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminPost,
  adminGet,
  deployAndPromote,
  fetchWithToken,
  gatewayFetch,
  readMeta,
  uniqueNs,
  withServiceStopped,
  setupIntegrationSuite,
  parseBase64Json,
  responseJson,
} from "./helpers/index.js";
import {
  redisCommand,
  redisDel,
  redisExists,
  redisGet,
  redisHGet,
  redisHGetAll,
  redisHSet,
  redisSIsMember,
  redisSMembers,
  redisSetEx,
  redisZRange,
} from "./helpers/redis.js";
import {
  S3_CLEANUP_QUEUE_NAME,
} from "../../shared/s3-cleanup-lifecycle.js";
import { queueStreamKey } from "../../shared/queue-keys.js";

setupIntegrationSuite();

function queueCleanupIntents() {
  const stream = queueStreamKey("__system__", S3_CLEANUP_QUEUE_NAME);
  const raw = redisCommand(`--raw XRANGE ${stream} - +`, { db: 1 });
  const lines = raw.split("\n").map((l) => l.trim()).filter(Boolean);
  /** @type {Array<{ streamId: string, fields: Record<string, string>, body: any }>} */
  const out = [];
  for (let i = 0; i < lines.length;) {
    const streamId = lines[i++];
    /** @type {Record<string, string>} */
    const fields = {};
    while (i + 1 < lines.length && !/^\d+-\d+$/.test(lines[i])) {
      fields[lines[i++]] = lines[i++];
    }
    if (fields.body_b64) {
      out.push({
        streamId,
        fields,
        body: parseBase64Json(fields.body_b64, "S3 cleanup queue body"),
      });
    }
  }
  return out;
}

// ───── GET /ns/<ns>/workers ─────

test("GET /workers lists deploy-only + active + secret-only entries", async () => {
  const ns = uniqueNs("wlist");
  await adminPost(`/ns/${ns}/worker/d-only/deploy`, {
    code: "export default { fetch() { return new Response('d-only'); } };",
  });
  await deployAndPromote(ns, "active", {
    code: "export default { fetch() { return new Response('active'); } };",
  });
  await adminFetch(`/ns/${ns}/worker/secret-only/secrets/KEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });

  const r = await adminGet(`/ns/${ns}/workers`);
  assert.equal(r.status, 200);
  assert.equal(r.json.namespace, ns);
  const byName = Object.fromEntries(r.json.workers.map((/** @type {any} */ w) => [w.name, w]));
  assert.ok(byName["d-only"]);
  assert.equal(byName["d-only"].activeVersion, null);
  assert.deepEqual(byName["d-only"].versions, ["v1"]);
  assert.equal(byName["d-only"].hasSecrets, false);

  assert.ok(byName["active"]);
  assert.equal(byName["active"].activeVersion, "v1");
  assert.deepEqual(byName["active"].versions, ["v1"]);

  assert.ok(byName["secret-only"]);
  assert.equal(byName["secret-only"].activeVersion, null);
  assert.deepEqual(byName["secret-only"].versions, []);
  assert.equal(byName["secret-only"].hasSecrets, true);
});

// ───── DELETE /versions/<v> ─────

test("DELETE /versions/<non-active> removes bundle, indexes, referrers and creates cleanup task", async () => {
  const ns = uniqueNs("vdel");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v1'); } };",
  });
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v2'); } };",
    assets: { "x.txt": Buffer.from("hello").toString("base64") },
  });

  const r = await adminFetch(`/ns/${ns}/worker/api/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.deleted, true);
  assert.equal(body.version, "v1");

  assert.equal(redisExists(`worker:${ns}:api:v:1`), false);
  assert.deepEqual(redisZRange(`worker-versions:${ns}:api`), ["v2"]);
  assert.equal(redisExists(`worker-version-referrers:${ns}:api:v1`), false);
  assert.equal("cleanupTaskId" in body.assets, false);
  assert.equal(body.assets.skippedSharedPrefix, false);
});

test("DELETE /versions/<active> → 409 active_version", async () => {
  const ns = uniqueNs("vdelact");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  const r = await adminFetch(`/ns/${ns}/worker/api/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 409);
  assert.equal((await responseJson(r)).error, "active_version");
});

test("DELETE /versions referenced by a retained caller → 409 version_referenced with ops-principal details", async () => {
  const ns = uniqueNs("vdelref");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v1'); } };",
  });
  await adminPost(`/ns/${ns}/worker/web/deploy`, {
    code: "export default { fetch() { return new Response('web'); } };",
    bindings: { API: { type: "service", service: "api" } },
  });
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v2'); } };",
  });

  const r = await adminFetch(`/ns/${ns}/worker/api/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 409);
  const body = await responseJson(r);
  assert.equal(body.error, "version_referenced");
  assert.equal(body.referrers.length, 1);
  assert.deepEqual(body.referrers[0], {
    binding: "API",
    callerNs: ns,
    callerWorker: "web",
    callerVersion: "v1",
  });
});

test("DELETE /versions/<v> idempotent-404 on non-existent version", async () => {
  const ns = uniqueNs("vdel404");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  const r = await adminFetch(`/ns/${ns}/worker/api/versions/v99`, { method: "DELETE" });
  assert.equal(r.status, 404);
  assert.equal((await responseJson(r)).error, "version_not_found");
});

test("DELETE /versions of last retained version (deploy-only, no secrets) clears worker-versions + SREMs workers", async () => {
  const ns = uniqueNs("vdellast");
  await adminPost(`/ns/${ns}/worker/drafty/deploy`, {
    code: "export default { fetch() { return new Response('d'); } };",
  });
  assert.deepEqual(redisSMembers(`workers:${ns}`), ["drafty"]);
  assert.deepEqual(redisZRange(`worker-versions:${ns}:drafty`), ["v1"]);

  const r = await adminFetch(`/ns/${ns}/worker/drafty/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 200);
  assert.equal((await responseJson(r)).deleted, true);

  assert.equal(redisExists(`worker-versions:${ns}:drafty`), false);
  assert.deepEqual(redisSMembers(`workers:${ns}`), []);
});

test("DELETE /versions last-retained-no-active WITH secrets leaves workers:<ns> + secrets intact", async () => {
  const ns = uniqueNs("vdellast2");
  await adminPost(`/ns/${ns}/worker/latent/deploy`, {
    code: "export default { fetch() { return new Response('x'); } };",
  });
  await adminFetch(`/ns/${ns}/worker/latent/secrets/FOO`, {
    method: "PUT",
    body: JSON.stringify({ value: "bar" }),
  });

  const r = await adminFetch(`/ns/${ns}/worker/latent/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 200);

  // secret-only worker must stay visible in workers:<ns>; secrets untouched.
  assert.equal(redisExists(`worker-versions:${ns}:latent`), false);
  assert.deepEqual(redisSMembers(`workers:${ns}`), ["latent"]);
  assert.equal(redisExists(`secrets:${ns}:latent`), true);
});

test("DELETE /versions with shared assets prefix (secret-bump sibling) skips cleanup task", async () => {
  const ns = uniqueNs("vdelshared");
  // v1 ships assets; secret-bump COPYs v1 → v2 (both pin the same prefix);
  // v3 gets a fresh assets upload so its prefix differs. Deleting v1 must
  // NOT schedule a cleanup task because v2 still references that prefix.
  await deployAndPromote(ns, "w", {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export default { fetch(_, env) { return new Response(env.ASSETS.url('x')); } };",
    },
    assets: { "x.txt": Buffer.from("A").toString("base64") },
  });
  await adminFetch(`/ns/${ns}/worker/w/secrets/K`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });
  await deployAndPromote(ns, "w", {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export default { fetch(_, env) { return new Response(env.ASSETS.url('x')); } };",
    },
    assets: { "x.txt": Buffer.from("B").toString("base64") },
  });
  const r = await adminFetch(`/ns/${ns}/worker/w/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.deleted, true);
  assert.equal(body.assets.skippedSharedPrefix, true);
  assert.equal("cleanupTaskId" in body.assets, false);
});

// ───── POST /delete ─────

test("POST /delete on fully active worker removes projection + bundles + cleanup task", async () => {
  const ns = uniqueNs("wdel");
  const v = await deployAndPromote(ns, "api", {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export default { fetch(_, env) { return new Response(env.ASSETS.url('x')); } };",
    },
    assets: { "x.txt": Buffer.from("A").toString("base64") },
  });
  const activeMeta = readMeta(ns, "api", v);

  // Pre-state: gateway should route <ns>/api.
  const g1 = await gatewayFetch(ns, "/api");
  assert.equal(g1.status, 200);

  await withServiceStopped("scheduler", async () => {
    const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
    assert.equal(r.status, 200);
    const body = await responseJson(r);
    assert.equal(body.deleted, true);
    assert.equal(body.activeDeleted, v);
    assert.deepEqual(body.versionsDeleted, [v]);
    assert.equal("cleanupTaskId" in body.assets, false);
    assert.equal(body.assets.queueHint, "queued", JSON.stringify(body.assets));
    assert.deepEqual(body.assets.warnings, []);

    assert.deepEqual(redisSMembers(`workers:${ns}`), []);
    assert.equal(redisExists(`worker-versions:${ns}:api`), false);
    assert.equal(redisHGet(`routes:${ns}`, "api"), null);
    assert.equal(redisExists(`worker:${ns}:api:v:${v.slice(1)}`), false);
    // next_version survives whole-delete — no id reuse on future deploys.
    assert.notEqual(redisExists(`worker:${ns}:api:next_version`), false);

    const intents = queueCleanupIntents();
    assert.equal(intents.length, 1, "expected one cleanup queue intent");
    assert.notEqual(intents[0].fields.id, intents[0].body.taskId);
    assert.match(intents[0].body.taskId, /^s3cleanup:/);
    assert.deepEqual(intents[0].body.prefixes, [activeMeta.assets.prefix]);
    assert.equal(intents[0].body.source.kind, "delete-worker");
    assert.equal(intents[0].body.source.ns, ns);
    assert.equal(intents[0].body.source.worker, "api");
    assert.deepEqual(intents[0].body.source.versions, [v]);
    assert.equal(typeof intents[0].body.source.requestId, "string");
  });
});

test("POST /delete idempotent on never-existed worker", async () => {
  const ns = uniqueNs("widem");
  const r = await adminFetch(`/ns/${ns}/worker/ghost/delete`, { method: "POST" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.deleted, false);
  assert.deepEqual(body.versionsDeleted, []);
});

test("POST /delete blocked by retained caller referrers → 409 version_referenced", async () => {
  const ns = uniqueNs("wdelref");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  await deployAndPromote(ns, "web", {
    code: "export default { fetch() { return new Response('web'); } };",
    bindings: { API: { type: "service", service: "api" } },
  });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 409);
  const body = await responseJson(r);
  assert.equal(body.error, "version_referenced");
  assert.equal(body.blockers.length, 1);
  assert.equal(body.blockers[0].version, "v1");
  assert.ok(body.blockers[0].referrers.some((/** @type {any} */ r2) => r2.callerWorker === "web"));
});

test("POST /delete deletes worker secrets but preserves ns secrets", async () => {
  const ns = uniqueNs("wdelsec");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  await adminFetch(`/ns/${ns}/worker/api/secrets/WKEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });
  await adminFetch(`/ns/${ns}/secrets/NSKEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "x" }),
  });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 200);

  assert.equal(redisExists(`secrets:${ns}:api`), false);
  assert.equal(redisExists(`secrets:${ns}`), true);
});

test("POST /delete preserves next_version counter (no id reuse)", async () => {
  const ns = uniqueNs("wdelnv");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('v1'); } };",
  });
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('v2'); } };",
  });
  assert.equal(redisGet(`worker:${ns}:api:next_version`), "2");

  await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(redisGet(`worker:${ns}:api:next_version`), "2");
  const d = await adminPost(`/ns/${ns}/worker/api/deploy`, {
    code: "export default { fetch() { return new Response('new'); } };",
  });
  assert.equal(d.json.version, "v3");
});

test("POST /delete?dry_run=1 returns impact without side effects", async () => {
  const ns = uniqueNs("wdrun");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });

  const pre = {
    workers: redisSMembers(`workers:${ns}`),
    versions: redisZRange(`worker-versions:${ns}:api`),
    routes: redisHGetAll(`routes:${ns}`),
  };
  const r = await adminFetch(`/ns/${ns}/worker/api/delete?dry_run=1`, { method: "POST" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.dryRun, true);
  assert.equal(body.deleted, true);
  assert.deepEqual(body.versionsDeleted, ["v1"]);

  assert.deepEqual(redisSMembers(`workers:${ns}`), pre.workers);
  assert.deepEqual(redisZRange(`worker-versions:${ns}:api`), pre.versions);
  assert.deepEqual(redisHGetAll(`routes:${ns}`), pre.routes);
  assert.equal(queueCleanupIntents().length, 0);
});

test("POST /delete?dry_run=1 surfaces referrer blockers without state change", async () => {
  const ns = uniqueNs("wdrunblk");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  await deployAndPromote(ns, "web", {
    code: "export default { fetch() { return new Response('web'); } };",
    bindings: { API: { type: "service", service: "api" } },
  });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete?dry_run=1`, { method: "POST" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.dryRun, true);
  assert.equal(body.deleted, false);
  assert.ok(Array.isArray(body.blockers));
  assert.equal(body.blockers[0].referrers[0].callerWorker, "web");
});

test("POST /delete: second concurrent delete gets 409 deleting until lock releases", async () => {
  const ns = uniqueNs("wdellock");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api'); } };",
  });
  // Simulate another in-flight delete by holding the lock externally.
  redisSetEx(`worker-delete-lock:${ns}:api`, "other-token", 30);
  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 409);
  assert.equal((await responseJson(r)).error, "deleting");
  redisDel(`worker-delete-lock:${ns}:api`);
});

test("POST /delete clears pattern routes + publishes invalidations so gateway re-resolves", async () => {
  const ns = uniqueNs("wdelpat");
  await adminFetch(`/ns/${ns}/hosts`, {
    method: "POST",
    body: JSON.stringify({ hosts: ["acme.workers.example"] }),
  });
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('ok'); } };",
    routes: ["acme.workers.example/api/*"],
  });

  assert.ok(redisHGetAll("patterns:acme.workers.example")["/api/*"]);

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.deepEqual(body.affectedHosts, ["acme.workers.example"]);

  assert.equal(redisHGetAll("patterns:acme.workers.example")["/api/*"], undefined);
  // Host had no other ns-owned pattern → ns-hosts loses it.
  assert.equal(redisSIsMember(`ns-hosts:${ns}`, "acme.workers.example"), false);
});

test("POST /delete fail-closes when active route metadata is malformed", async () => {
  const ns = uniqueNs("wdelbadrt");
  await adminFetch(`/ns/${ns}/hosts`, {
    method: "POST",
    body: JSON.stringify({ hosts: ["bad-route.workers.example"] }),
  });
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('ok'); } };",
    routes: ["bad-route.workers.example/api/*"],
  });
  const meta = readMeta(ns, "api", "v1");
  meta.routes = [{ host: "bad-route.workers.example", slot: 42 }];
  redisHSet(`worker:${ns}:api:v:1`, { __meta__: JSON.stringify(meta) });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 500);
  const body = await responseJson(r);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.message, "Internal error");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);

  assert.ok(redisHGetAll("patterns:bad-route.workers.example")["/api/*"]);
  assert.equal(redisSIsMember(`ns-hosts:${ns}`, "bad-route.workers.example"), true);
});

// ───── Principal-aware referrer redaction ─────

async function seedCrossNsReferrerScenario() {
  const targetNs = uniqueNs("redact-t");
  const crossNs = uniqueNs("redact-c");

  await deployAndPromote(targetNs, "api", {
    code: "export default {fetch(){return new Response('api v1')}};",
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });
  await deployAndPromote(targetNs, "same-caller", {
    code: "export default {fetch(){return new Response('same')}};",
    bindings: { API: { type: "service", service: "api" } },
  });
  await deployAndPromote(crossNs, "cross-caller", {
    code: "export default {fetch(){return new Response('cross')}};",
    bindings: { API: { type: "service", ns: targetNs, service: "api" } },
  });
  // Promote api v2 so v1 is retained-but-non-active (eligible for delete).
  await deployAndPromote(targetNs, "api", {
    code: "export default {fetch(){return new Response('api v2')}};",
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });

  return { targetNs, crossNs };
}

test("DELETE /versions 409 redacts cross-ns callers for ns-principal; ops sees full list", async () => {
  const { targetNs, crossNs } = await seedCrossNsReferrerScenario();

  // Ops token (default adminFetch) → full list.
  const opsRes = await adminFetch(`/ns/${targetNs}/worker/api/versions/v1`, { method: "DELETE" });
  assert.equal(opsRes.status, 409);
  const opsBody = await responseJson(opsRes);
  assert.equal(opsBody.error, "version_referenced");
  assert.equal(opsBody.referrers.length, 2);
  const opsPairs = opsBody.referrers.map((/** @type {any} */ r) => `${r.callerNs}/${r.callerWorker}`).toSorted();
  assert.deepEqual(
    opsPairs,
    [`${targetNs}/same-caller`, `${crossNs}/cross-caller`].toSorted()
  );
  assert.equal(opsBody.crossNamespaceReferrerCount, undefined);

  // Issue an ns-scoped token for the target ns.
  const issued = await adminPost("/auth/tokens", { ns: targetNs, label: "redact-test" });
  assert.equal(issued.status, 201);
  const nsToken = issued.json.token;

  const nsRes = await fetchWithToken(
    nsToken,
    `/ns/${targetNs}/worker/api/versions/v1`,
    { method: "DELETE" }
  );
  assert.equal(nsRes.status, 409);
  assert.equal(nsRes.json.error, "version_referenced");
  assert.equal(nsRes.json.referrers.length, 1);
  assert.equal(nsRes.json.referrers[0].callerNs, targetNs);
  assert.equal(nsRes.json.referrers[0].callerWorker, "same-caller");
  assert.equal(nsRes.json.crossNamespaceReferrerCount, 1);
  // No leakage of the other ns's identifiers anywhere in the body.
  assert.ok(!nsRes.text.includes(crossNs), "cross-ns name must not leak");
  assert.ok(!nsRes.text.includes("cross-caller"), "cross-ns worker name must not leak");
});

test("POST /delete blockers redacted for ns-principal; ops sees full blockers", async () => {
  const { targetNs, crossNs } = await seedCrossNsReferrerScenario();

  const ops = await adminFetch(`/ns/${targetNs}/worker/api/delete`, { method: "POST" });
  assert.equal(ops.status, 409);
  const opsBody = await responseJson(ops);
  assert.equal(opsBody.error, "version_referenced");
  assert.equal(opsBody.blockers.length, 1);
  assert.equal(opsBody.blockers[0].version, "v1");
  assert.equal(opsBody.blockers[0].referrers.length, 2);
  assert.equal(opsBody.blockers[0].crossNamespaceReferrerCount, undefined);

  const issued = await adminPost("/auth/tokens", { ns: targetNs, label: "redact-post" });
  const nsToken = issued.json.token;
  const nsRes = await fetchWithToken(
    nsToken, `/ns/${targetNs}/worker/api/delete`, { method: "POST" }
  );
  assert.equal(nsRes.status, 409);
  assert.equal(nsRes.json.error, "version_referenced");
  assert.equal(nsRes.json.blockers.length, 1);
  assert.equal(nsRes.json.blockers[0].referrers.length, 1);
  assert.equal(nsRes.json.blockers[0].referrers[0].callerNs, targetNs);
  assert.equal(nsRes.json.blockers[0].crossNamespaceReferrerCount, 1);
  assert.ok(!nsRes.text.includes(crossNs));
});

test("POST /delete?dry_run=1 blockers also redact cross-ns referrers for ns-principal", async () => {
  const { targetNs, crossNs } = await seedCrossNsReferrerScenario();
  const issued = await adminPost("/auth/tokens", { ns: targetNs, label: "redact-dry" });
  const nsToken = issued.json.token;

  const nsRes = await fetchWithToken(
    nsToken,
    `/ns/${targetNs}/worker/api/delete?dry_run=1`,
    { method: "POST" }
  );
  assert.equal(nsRes.status, 200);
  assert.equal(nsRes.json.dryRun, true);
  assert.equal(nsRes.json.deleted, false);
  assert.equal(nsRes.json.blockers.length, 1);
  assert.equal(nsRes.json.blockers[0].referrers.length, 1);
  assert.equal(nsRes.json.blockers[0].referrers[0].callerNs, targetNs);
  assert.equal(nsRes.json.blockers[0].crossNamespaceReferrerCount, 1);
  assert.ok(!nsRes.text.includes(crossNs));
});

// ───── Deploy abort cleanup ─────

test("deploy aborted by target_deleting creates a durable deploy-abort S3 cleanup task for the orphan prefix", async () => {
  const ns = uniqueNs("depabort");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('api')}};",
  });
  // Foreign lock trips caller's WATCH at commit; S3 upload has already
  // happened by then, so the orphan prefix needs a durable cleanup task.
  redisSetEx(`worker-delete-lock:${ns}:api`, "externtok", 30);

  const dep = await adminPost(`/ns/${ns}/worker/web/deploy`, {
    mainModule: "worker.js",
    modules: {
      "worker.js":
        "export default { fetch(_, env) { return new Response(env.ASSETS.url('x.txt')); } };",
    },
    bindings: { API: { type: "service", service: "api" } },
    assets: { "x.txt": Buffer.from("orphan").toString("base64") },
  });
  assert.equal(dep.status, 409);
  assert.equal(dep.json.error, "target_deleting");

  const intents = queueCleanupIntents();
  assert.equal(intents.length, 1, "exactly one deploy-abort cleanup intent");
  const payload = intents[0].body;
  assert.equal(payload.source.kind, "deploy-abort");
  assert.equal(payload.source.ns, ns);
  assert.equal(payload.source.worker, "web");
  assert.equal(payload.source.reason, "target_deleting");
  assert.equal(payload.prefixes.length, 1);
  assert.match(payload.prefixes[0], new RegExp(`^assets/${RegExp.escape(ns)}/web/[0-9a-f]{28}/$`));

  redisDel(`worker-delete-lock:${ns}:api`);
});

// ───── Delete-lock serializes every writer ─────

test("pre-held delete-lock blocks deploy / promote / worker-secret mutations on same worker", async () => {
  const ns = uniqueNs("lockser");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v1')}};",
  });
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v2')}};",
  });

  redisSetEx(`worker-delete-lock:${ns}:api`, "holder-tok", 30);

  const dep = await adminPost(`/ns/${ns}/worker/api/deploy`, {
    code: "export default {fetch(){return new Response('v3')}};",
  });
  assert.equal(dep.status, 409);
  assert.equal(dep.json.error, "caller_deleting");

  const prom = await adminPost(`/ns/${ns}/worker/api/promote`, { version: "v1" });
  assert.equal(prom.status, 409);
  assert.equal(prom.json.error, "deleting");

  const putSec = await adminFetch(`/ns/${ns}/worker/api/secrets/KEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "x" }),
  });
  assert.equal(putSec.status, 409);
  assert.equal((await responseJson(putSec)).error, "deleting");

  const delSec = await adminFetch(`/ns/${ns}/worker/api/secrets/KEY`, { method: "DELETE" });
  assert.equal(delSec.status, 409);
  assert.equal((await responseJson(delSec)).error, "deleting");

  redisDel(`worker-delete-lock:${ns}:api`);
  const dep2 = await adminPost(`/ns/${ns}/worker/api/deploy`, {
    code: "export default {fetch(){return new Response('v3')}};",
  });
  assert.equal(dep2.status, 201, `deploy should unblock after lock release, got ${dep2.status}`);
});

// ───── Corrupt __meta__: whole-delete fails closed ─────

test("POST /delete on a worker whose active bundle has corrupt __meta__ → 500 corrupt_meta with no partial mutations", async () => {
  const ns = uniqueNs("crupactv");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('x')}};",
    routes: [],
  });
  redisHSet(`worker:${ns}:api:v:1`, { __meta__: "not-json{" });

  const preActive = redisHGet(`routes:${ns}`, "api");
  const preWorkers = redisSMembers(`workers:${ns}`);

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 500);
  const body = await responseJson(r);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.version, "v1");
  assert.equal(body.message, "Internal error");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);

  assert.equal(redisHGet(`routes:${ns}`, "api"), preActive);
  assert.deepEqual(redisSMembers(`workers:${ns}`), preWorkers);
  assert.equal(redisExists(`worker:${ns}:api:v:1`), true);
});

test("POST /delete on a worker whose retained (non-active) bundle has corrupt __meta__ → 500 corrupt_meta", async () => {
  const ns = uniqueNs("crupret");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v1')}};",
  });
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v2')}};",
  });
  redisHSet(`worker:${ns}:api:v:1`, { __meta__: "garbage" });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete`, { method: "POST" });
  assert.equal(r.status, 500);
  const body = await responseJson(r);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.version, "v1");
  assert.equal(body.message, "Internal error");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);

  // Active pointer must survive — no partial tear-down.
  assert.equal(redisHGet(`routes:${ns}`, "api"), "v2");
  assert.equal(redisExists(`worker:${ns}:api:v:2`), true);
});

test("POST /delete?dry_run=1 also fail-closes on corrupt meta", async () => {
  const ns = uniqueNs("crupdry");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('x')}};",
  });
  redisHSet(`worker:${ns}:api:v:1`, { __meta__: "not-json" });

  const r = await adminFetch(`/ns/${ns}/worker/api/delete?dry_run=1`, { method: "POST" });
  assert.equal(r.status, 500);
  const body = await responseJson(r);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.dryRun, true);
});

test("DELETE /versions/<v> with corrupt sibling meta during shared-prefix scan → 500 corrupt_meta", async () => {
  const ns = uniqueNs("crupsib");
  // v1 + v2 share an assets prefix via secret-bump COPY; both retained.
  await deployAndPromote(ns, "w", {
    mainModule: "worker.js",
    modules: { "worker.js": "export default {fetch(_,e){return new Response(e.ASSETS.url('x'))}};" },
    assets: { "x.txt": Buffer.from("A").toString("base64") },
  });
  await adminFetch(`/ns/${ns}/worker/w/secrets/K`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });
  await deployAndPromote(ns, "w", {
    mainModule: "worker.js",
    modules: { "worker.js": "export default {fetch(_,e){return new Response(e.ASSETS.url('y'))}};" },
    assets: { "y.txt": Buffer.from("B").toString("base64") },
  });
  // v1 + v2 share a prefix; corrupting v2 must abort v1's delete before
  // a cleanup task is staged (otherwise v2's objects would be deleted).
  redisHSet(`worker:${ns}:w:v:2`, { __meta__: "corrupt{" });

  const r = await adminFetch(`/ns/${ns}/worker/w/versions/v1`, { method: "DELETE" });
  assert.equal(r.status, 500);
  const body = await responseJson(r);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.version, "v2");
  assert.equal(body.message, "Internal error");
  assert.equal(body.stage, undefined);
  assert.equal(body.detail, undefined);

  assert.equal(redisExists(`worker:${ns}:w:v:1`), true);
  assert.equal(queueCleanupIntents().length, 0);
});

test("delete-lock WATCH-races: a racing promote retries and eventually 409s deleting once the lock is observed", async () => {
  const ns = uniqueNs("lockrace");
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v1')}};",
  });
  await deployAndPromote(ns, "api", {
    code: "export default {fetch(){return new Response('v2')}};",
  });

  redisSetEx(`worker-delete-lock:${ns}:api`, "h", 30);
  const p = await adminPost(`/ns/${ns}/worker/api/promote`, { version: "v1" });
  assert.equal(p.status, 409);
  assert.equal(p.json.error, "deleting");
  redisDel(`worker-delete-lock:${ns}:api`);
});
