// Admin HTTP API end-to-end. Requires docker-compose stack (see helpers/).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminGet,
  adminPost,
  delay,
  deployAndPromote,
  gatewayFetch,
  gatewayUrl,
  responseJson,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisHSet, redisSet, redisSRem, redisSMembers } from "./helpers/redis.js";

setupIntegrationSuite();

test("deploy + versions round-trip", async () => {
  const d = await adminPost("/ns/test1/worker/hello/deploy", {
    code: "export default { fetch() { return new Response('ok'); } };",
  });
  assert.equal(d.status, 201);
  assert.equal(d.json.namespace, "test1");
  assert.equal(d.json.name, "hello");
  assert.equal(d.json.version, "v1");
  assert.equal(d.json.active, false);

  const v = await adminGet("/ns/test1/worker/hello/versions");
  assert.equal(v.status, 200);
  assert.deepEqual(v.json.versions, [{ version: "v1", active: false }]);
});

test("deploy increments version", async () => {
  await adminPost("/ns/test2/worker/x/deploy", { code: "export default { fetch(){return new Response('a')} };" });
  const d2 = await adminPost("/ns/test2/worker/x/deploy", { code: "export default { fetch(){return new Response('b')} };" });
  assert.equal(d2.json.version, "v2");
});

test("deploy rejects malformed body", async () => {
  const res = await adminFetch("/ns/test3/worker/x/deploy", {
    method: "POST",
    body: "{not json",
  });
  assert.equal(res.status, 400);
});

test("deploy rejects missing fields", async () => {
  const d = await adminPost("/ns/test3/worker/x/deploy", {});
  assert.equal(d.status, 400);
  assert.match(d.json.message, /Body must have/);
});

test("deploy rejects malformed optional metadata instead of silently dropping it", async () => {
  /** @type {Array<[string, unknown, RegExp]>} */
  const cases = [
    ["bindings", [], /bindings must be an object/],
    ["vars", "", /\[vars\] must be an object/],
    ["workflows", false, /workflows must be an array/],
  ];
  for (const [field, value, pattern] of cases) {
    const d = await adminPost("/ns/test3/worker/x/deploy", {
      code: "export default { fetch() { return new Response('ok'); } };",
      [field]: value,
    });
    assert.equal(d.status, 400, `${field} should be rejected`);
    assert.match(d.json.message, pattern);
  }
});

test("deploy rejects workerd 0701 unsupported bundle metadata before cold-load", async () => {
  const experimental = await adminPost("/ns/test3/worker/unsupported/deploy", {
    code: "export default { fetch() { return new Response('ok'); } };",
    compatibilityFlags: ["unsafe_module"],
  });
  assert.equal(experimental.status, 400);
  assert.equal(experimental.json.error, "experimental_compat_flag_unsupported");

  const python = await adminPost("/ns/test3/worker/unsupported/deploy", {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export default {};",
      "mod.py": { py: "print(1)" },
    },
  });
  assert.equal(python.status, 400);
  assert.equal(python.json.error, "python_workers_unsupported");
});

test("deploy rejects invalid namespace", async () => {
  const d = await adminPost("/ns/Bad_NS/worker/x/deploy", { code: "x" });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /Invalid namespace/);
});

test("deploy rejects reserved tenant name `admin` (reserved naming policy)", async () => {
  const d = await adminPost("/ns/admin/worker/x/deploy", { code: "x" });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /Invalid namespace.*admin/);

  const h = await adminPost("/ns/admin/hosts", { hosts: ["workers.example"] });
  assert.equal(h.status, 400);

  const sb = await adminPost("/ns/foo/worker/x/deploy", {
    code: "x",
    bindings: { TARGET: { type: "service", service: "y", ns: "admin" } },
  });
  assert.equal(sb.status, 400);

  const ac = await adminPost("/ns/foo/worker/x/deploy", {
    code: "x",
    exports: [{ entrypoint: "default", allowedCallers: ["admin"] }],
  });
  assert.equal(ac.status, 400);
  assert.match(ac.json.message, /reserved tenant name/);
});

test("deploy rejects mainModule absent from modules", async () => {
  const d = await adminPost("/ns/test4/worker/x/deploy", {
    mainModule: "missing.js",
    modules: { "worker.js": "x" },
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /not present in modules/);
});

test("versions returns empty list for unknown worker", async () => {
  const v = await adminGet("/ns/test5/worker/nothing/versions");
  assert.equal(v.status, 200);
  assert.deepEqual(v.json.versions, []);
});

test("versions hides incomplete bundles missing __meta__", async () => {
  redisSet("worker:test5b:x:next_version", "1");
  redisHSet("worker:test5b:x:v:1", { "worker.js": "partial" });
  const v = await adminGet("/ns/test5b/worker/x/versions");
  assert.equal(v.status, 200);
  assert.deepEqual(v.json.versions, []);
});

test("promote requires existing version", async () => {
  await adminPost("/ns/test6/worker/x/deploy", { code: "export default {fetch(){return new Response('a')}};" });
  const p = await adminPost("/ns/test6/worker/x/promote", { version: "v99" });
  assert.equal(p.status, 404);
});

test("promote rejects incomplete bundles missing __meta__", async () => {
  redisHSet("worker:test6b:x:v:1", { "worker.js": "partial" });
  const p = await adminPost("/ns/test6b/worker/x/promote", { version: "v1" });
  assert.equal(p.status, 404);
});

test("promote requires body.version", async () => {
  await adminPost("/ns/test7/worker/x/deploy", { code: "export default {fetch(){return new Response('a')}};" });
  const p = await adminPost("/ns/test7/worker/x/promote", {});
  assert.equal(p.status, 400);
  assert.match(p.json.message, /Missing 'version'/);
});

test("promote flips active flag in /versions", async () => {
  await deployAndPromote("test8", "x", { code: "export default {fetch(){return new Response('a')}};" });
  const v = await adminGet("/ns/test8/worker/x/versions");
  assert.deepEqual(v.json.versions, [{ version: "v1", active: true }]);
});

test("namespaces set: deploy alone does not register the ns", async () => {
  // Without promote, no active worker exists — the gate must treat the
  // subdomain as unknown so deploy-without-promote stays cold.
  await adminPost("/ns/test8b/worker/x/deploy", { code: "export default {fetch(){return new Response('a')}};" });
  const members = redisSMembers("namespaces");
  assert.ok(!members.includes("test8b"), `ns should not be in namespaces yet, got ${members.join(",")}`);
});

test("namespaces set: promote registers the ns atomically with the route flip", async () => {
  await deployAndPromote("test8c", "x", { code: "export default {fetch(){return new Response('a')}};" });
  const members = redisSMembers("namespaces");
  assert.ok(members.includes("test8c"), `ns should be in namespaces post-promote, got ${members.join(",")}`);
});

test("namespaces set: secret bump heals a drifted-out ns", async () => {
  // Bump path goes through bumpActiveAndPromote, not promoteWithRoutes —
  // separate test because the namespaces SADD lives on both paths.
  const ns = "test8d";
  await deployAndPromote(ns, "hi", {
    code: "export default { fetch(_, env) { return new Response(env.K || ''); } };",
  });
  redisSRem("namespaces", ns);
  let members = redisSMembers("namespaces");
  assert.ok(!members.includes(ns), `precondition: ${ns} should be drifted out, got ${members.join(",")}`);

  const r = await adminFetch(`/ns/${ns}/worker/hi/secrets/K`, {
    method: "PUT",
    body: JSON.stringify({ value: "v" }),
  });
  assert.equal(r.status, 200);
  members = redisSMembers("namespaces");
  assert.ok(members.includes(ns), `bump should heal namespaces membership, got ${members.join(",")}`);
});

test("invalid path shape → 400", async () => {
  const res = await adminFetch("/totally/wrong");
  assert.equal(res.status, 400);
});

test("/reload PUBLISHes full resync to gateway subscribers", async () => {
  const res = await adminFetch("/reload", { method: "POST" });
  assert.equal(res.status, 200);
  const body = await responseJson(res);
  assert.equal(body.reload.ok, true);
  // With the stack warm, the gateway's subscriber is attached on both
  // channels — each PUBLISH should report at least one receiver.
  assert.ok(
    body.reload.routes.receivers >= 1 && body.reload.patterns.receivers >= 1,
    `expected >=1 subscriber per channel, got ${JSON.stringify(body.reload)}`
  );
});

test("/reload records success telemetry (visible on gateway /_metrics)", async () => {
  // Control itself doesn't expose /metrics (system-runtime-static.md
  // §observability). The invalidations it publishes are observed via
  // gateway's subscriber counters, which are the survivable signal.
  await adminFetch("/reload", { method: "POST" });
  await delay(100);
  const metricsRes = await fetch(gatewayUrl("/_metrics"));
  const body = await metricsRes.text();
  const line = body
    .split("\n")
    .find((s) => s.startsWith("wdl_subscriber_invalidations_total{") && s.includes('scope="all"'));
  assert.ok(line, "expected scope=all subscriber_invalidations counter on gateway");
});

test("promote makes worker reachable through gateway", async () => {
  await deployAndPromote("test9", "hi", {
    code: "export default { fetch() { return new Response('routed'); } };",
  });
  const res = await gatewayFetch("test9", "/hi");
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "routed");
});

test("promote PUBLISH is fire-and-forget — receivers count ≥ 0, not an error condition", async () => {
  // `receivers` is a number (can be 0), never an error condition.
  const r = await adminPost("/reload", {});
  assert.equal(r.status, 200);
  assert.equal(r.json.reload.ok, true);
  assert.equal(typeof r.json.reload.routes.receivers, "number");
  assert.equal(typeof r.json.reload.patterns.receivers, "number");
});

test("deploy: routes on a reserved ns other than __system__ → 400", async () => {
  const res = await adminPost("/ns/__platform__/worker/rogue/deploy", {
    code: "export default { fetch(){return new Response('x')} };",
    routes: ["public.workers.example/*"],
  });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /reserved.*may not declare routes/);
});

test("deploy: routes on __system__ accepted (whitelist)", async () => {
  const res = await adminPost("/ns/__system__/hosts", { hosts: ["sys.workers.example"] });
  assert.equal(res.status, 200);
  const dep = await adminPost("/ns/__system__/worker/dash/deploy", {
    code: "export default { fetch(){return new Response('dash')} };",
    routes: ["sys.workers.example/*"],
  });
  assert.equal(dep.status, 201);
});

test("promote: reserved ns bundle with meta.routes is rejected even when deploy gate is bypassed", async () => {
  // Simulates a bundle committed around admin (direct Redis, or a
  // pre-gate version). promote must still reject — the rule is a
  // promote-time invariant, not a deploy-time convention.
  const meta = JSON.stringify({
    mainModule: "worker.js",
    modules: { "worker.js": { type: "module" } },
    routes: [{ host: "rogue.workers.example", slot: "/*", kind: "prefix", value: "/" }],
  });
  // bundleKey uses the integer tag — "worker:<ns>:<name>:v:1" not "v:v1".
  redisHSet("worker:__platform__:rogue:v:1", {
    __meta__: meta,
    "worker.js": `export default { fetch(){ return new Response("rogue"); } };`,
  });
  redisSet("worker:__platform__:rogue:next_version", "1");

  const prom = await adminPost("/ns/__platform__/worker/rogue/promote", { version: "v1" });
  assert.equal(prom.status, 400);
  assert.match(prom.json.message, /reserved.*may not declare routes/);
});

test("deploy rejects worker name containing ':' — would break x-worker-id parsing", async () => {
  const d = await adminPost("/ns/demo/worker/a:b/deploy", {
    code: "export default { fetch() { return new Response('ok'); } };",
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /Invalid worker name/);
});

test("deploy rejects binding name __proto__ — would shadow Object.prototype on env", async () => {
  // Raw body: JSON.stringify({__proto__: x}) drops the key (literal sets
  // prototype, not own property).
  const rawBody = `{
    "code": "export default { fetch() { return new Response('ok'); } };",
    "bindings": { "__proto__": { "type": "kv", "id": "x" } }
  }`;
  const res = await adminFetch(`/ns/demo/worker/w/deploy`, {
    method: "POST",
    body: rawBody,
  });
  assert.equal(res.status, 400);
  const json = await responseJson(res);
  assert.equal(json.error, "invalid_request");
  assert.match(json.message, /reserved Object\.prototype key/);
});

test("deploy rejects queue id containing ':' — would corrupt queue:<ns>:<id>:s key", async () => {
  const d = await adminPost("/ns/demo/worker/w/deploy", {
    code: "export default { fetch() { return new Response('ok'); } };",
    bindings: { MY_Q: { type: "queue", id: "bad:queue" } },
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /queue id must match/);
});

test("deploy rejects KV id containing ':' — preserves KV Redis key-shape boundaries", async () => {
  const d = await adminPost("/ns/demo/worker/w/deploy", {
    code: "export default { fetch() { return new Response('ok'); } };",
    bindings: { CACHE: { type: "kv", id: "foo:v" } },
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /kv id must match/);
});

test("deploy rejects module path '__proto__' — would pollute control in-proc meta.modules", async () => {
  const rawBody = `{
    "mainModule": "worker.js",
    "modules": { "worker.js": "export default { fetch() { return new Response('ok'); } };", "__proto__": "x" }
  }`;
  const res = await adminFetch(`/ns/demo/worker/w/deploy`, {
    method: "POST",
    body: rawBody,
  });
  assert.equal(res.status, 400);
  const json = await responseJson(res);
  assert.equal(json.error, "invalid_request");
  assert.match(json.message, /is reserved|must match/);
});

test("deploy rejects module path with '..' segment — path traversal", async () => {
  const d = await adminPost("/ns/demo/worker/w/deploy", {
    mainModule: "worker.js",
    modules: { "worker.js": "export default { fetch() { return new Response('ok'); } };", "../escape.js": "x" },
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /must match|invalid segment/);
});

test("deploy rejects queueConsumers with camelCase queue name", async () => {
  const d = await adminPost("/ns/demo/worker/w/deploy", {
    code: "export default { fetch() { return new Response('ok'); }, async queue() {} };",
    queueConsumers: [{ queue: "badName", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /queue must match/);
});
