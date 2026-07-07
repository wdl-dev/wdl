// Every writer (deploy / secret bump / promote) must populate the
// worker-owned-lifecycle indexes in the same EXEC. Tests hit Redis
// through typed integration helpers because no HTTP endpoint reads these yet.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminPost,
  deployAndPromote,
  readMeta,
  responseJson,
  uniqueNs,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisJsonMember, redisJsonMembers, redisSMembers, redisZRange } from "./helpers/redis.js";

setupIntegrationSuite();

test("deploy: SADDs workers:<ns> and ZADDs worker-versions:<ns>:<name>", async () => {
  const ns = uniqueNs("pr1-idx");
  const d1 = await adminPost(`/ns/${ns}/worker/hello/deploy`, {
    code: "export default { fetch() { return new Response('ok'); } };",
  });
  assert.equal(d1.status, 201);
  assert.equal(d1.json.version, "v1");

  assert.deepEqual(redisSMembers(`workers:${ns}`), ["hello"]);
  assert.deepEqual(redisZRange(`worker-versions:${ns}:hello`), ["v1"]);

  const d2 = await adminPost(`/ns/${ns}/worker/hello/deploy`, {
    code: "export default { fetch() { return new Response('ok2'); } };",
  });
  assert.equal(d2.json.version, "v2");
  assert.deepEqual(redisZRange(`worker-versions:${ns}:hello`), ["v1", "v2"]);

  const d3 = await adminPost(`/ns/${ns}/worker/other/deploy`, {
    code: "export default { fetch() { return new Response('other'); } };",
  });
  assert.equal(d3.status, 201);
  const members = redisSMembers(`workers:${ns}`).toSorted();
  assert.deepEqual(members, ["hello", "other"]);
});

test("deploy: reverses service binding into worker-version-referrers of target version", async () => {
  const ns = uniqueNs("pr1-ref");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v1'); } };",
  });
  const callerDeploy = await adminPost(`/ns/${ns}/worker/web/deploy`, {
    code: "export default { fetch() { return new Response('web'); } };",
    bindings: {
      API: { type: "service", service: "api" },
    },
  });
  assert.equal(callerDeploy.status, 201);
  const callerVersion = callerDeploy.json.version;

  const refKey = `worker-version-referrers:${ns}:api:v1`;
  const members = redisSMembers(refKey);
  assert.equal(members.length, 1);
  const parsed = redisJsonMember(members[0], `${refKey} member`);
  assert.deepEqual(parsed, {
    binding: "API",
    callerNs: ns,
    callerVersion,
    callerWorker: "web",
  });
});

test("secret bump (PUT on active worker) grows worker-versions and copies referrers forward", async () => {
  const ns = uniqueNs("pr1-sec");
  await deployAndPromote(ns, "api", {
    code: "export default { fetch() { return new Response('api v1'); } };",
  });
  await deployAndPromote(ns, "web", {
    code: "export default { fetch() { return new Response('web'); } };",
    bindings: { API: { type: "service", service: "api" } },
  });
  assert.deepEqual(redisZRange(`worker-versions:${ns}:web`), ["v1"]);

  const r = await adminFetch(`/ns/${ns}/worker/web/secrets/HELLO`, {
    method: "PUT",
    body: JSON.stringify({ value: "world" }),
  });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.set, true);
  assert.ok(body.version, `expected bump to return a new version, got ${JSON.stringify(body)}`);
  assert.notEqual(body.version, "v1");

  const versions = redisZRange(`worker-versions:${ns}:web`);
  assert.deepEqual(versions.toSorted(), ["v1", body.version].toSorted());

  // Secret bump COPYs bindings, so the target now has two retained
  // caller versions pointing at it.
  const members = redisSMembers(`worker-version-referrers:${ns}:api:v1`);
  assert.equal(members.length, 2);
  const parsed = redisJsonMembers(members, `worker-version-referrers:${ns}:api:v1 member`);
  const versionsSeen = parsed.map((p) => p.callerVersion).toSorted();
  assert.deepEqual(versionsSeen.toSorted(), ["v1", body.version].toSorted());
});

test("secret PUT on never-deployed worker: SADDs workers:<ns> as secret-only entry", async () => {
  const ns = uniqueNs("pr1-secpre");
  const r = await adminFetch(`/ns/${ns}/worker/future/secrets/FOO`, {
    method: "PUT",
    body: JSON.stringify({ value: "bar" }),
  });
  assert.equal(r.status, 200);
  const body = await responseJson(r);
  assert.equal(body.set, true);
  assert.match(body.note || "", /stored; will apply on next load or deploy/);

  assert.deepEqual(redisSMembers(`workers:${ns}`), ["future"]);
  assert.deepEqual(redisZRange(`worker-versions:${ns}:future`), []);
});

test("secret DELETE of last key on undeployed worker SREMs workers:<ns>", async () => {
  const ns = uniqueNs("pr1-secclean");
  await adminFetch(`/ns/${ns}/worker/ghost/secrets/ONLY`, {
    method: "PUT",
    body: JSON.stringify({ value: "x" }),
  });
  assert.deepEqual(redisSMembers(`workers:${ns}`), ["ghost"]);

  const del = await adminFetch(`/ns/${ns}/worker/ghost/secrets/ONLY`, {
    method: "DELETE",
  });
  assert.equal(del.status, 200);
  assert.deepEqual(redisSMembers(`workers:${ns}`), []);
});

test("deploy writes __meta__.assets = {token, prefix} and uploads to tokenized S3 path", async () => {
  const ns = uniqueNs("pr1-assets");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    mainModule: "worker.js",
    modules: {
      "worker.js":
        "export default { fetch(_, env) { return new Response(env.ASSETS.url('x.txt')); } };",
    },
    assets: { "x.txt": Buffer.from("hello").toString("base64") },
  });
  assert.equal(d.status, 201);

  const meta = readMeta(ns, "w", d.json.version);
  assert.ok(meta.assets, "meta.assets must be set");
  assert.match(meta.assets.token, /^[0-9a-f]{28}$/);
  assert.equal(meta.assets.prefix, `assets/${ns}/w/${meta.assets.token}/`);
});
