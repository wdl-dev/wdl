// Gateway routing + versioning + pub/sub invalidation. Assumes compose stack.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminPost,
  adminFetch,
  ADMIN_HOST_HEADER,
  assertIntegrationJson,
  delay,
  deployAndPromote,
  gatewayFetch,
  gatewayUrl,
  hostFetch,
  sh,
  readIntegrationJson,
  waitForGateway,
  waitForGatewayCacheState,
  waitForGatewaySubscriber,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { parseCounters } from "./helpers/prometheus.js";
import {
  redisClientKillType,
  redisDebugSleep,
  redisHSet,
  redisPublish,
  redisSAdd,
} from "./helpers/redis.js";

setupIntegrationSuite();

/**
 * @param {string} ns
 * @param {string} path
 * @param {string} expected
 * @param {string} label
 */
async function waitForGatewayText(ns, path, expected, label) {
  let last = null;
  await waitUntil(label, async () => {
    const res = await gatewayFetch(ns, path).catch((err) => {
      last = err?.message ?? String(err);
      return null;
    });
    if (!res) return false;
    const text = await res.text();
    last = `${res.status} ${text}`;
    return res.status === 200 && text === expected;
  }, { timeoutMs: 5000, intervalMs: 50 });
  assert.equal(last, `200 ${expected}`);
}

test("unknown host (no subdomain) → 404 via pattern branch", async () => {
  // Any host that doesn't match <ns>.<PLATFORM_DOMAIN> falls into the
  // pattern branch; with no patterns:<host> declared this returns 404.
  const res = await fetch(gatewayUrl("/anything"), {
    headers: { Host: "workers.local" },
  });
  await assertIntegrationJson(res, 404, { error: "not_found", message: "Not found" });
});

test("unknown namespace → 404", async () => {
  const res = await gatewayFetch("nonexistent", "/anything");
  await assertIntegrationJson(res, 404, { error: "not_found", message: "Not found" });
});

test("reserved ns subdomain (__platform__, __system__, __community__) → 404 before any Redis lookup", async () => {
  for (const ns of ["__platform__", "__system__", "__community__"]) {
    const res = await gatewayFetch(ns, "/whatever");
    const body = await readIntegrationJson(res, 404, `reserved ns ${ns}`);
    assert.deepEqual(body, { error: "not_found", message: "Not found" });
  }
});

test("reserved ns pattern slot → 404 even if Redis contains a matching pattern", async () => {
  const host = "reserved-pattern.workers.example";
  redisSAdd("declared-hosts", host);
  redisSAdd(`host-declarations:${host}`, "platform-test");
  redisHSet(`patterns:${host}`, { "/api/*": "v2\t__platform__\tghost\tv1\tprefix\t/api/" });

  const res = await hostFetch(host, "/api/anything");
  await assertIntegrationJson(res, 404, { error: "not_found", message: "Not found" });
});

test("admin host with trailing FQDN dot still short-circuits to control", async () => {
  const res = await adminFetch("/reload", {
    method: "POST",
    headers: { Host: `${ADMIN_HOST_HEADER}.` },
  });
  const body = await readIntegrationJson(res, 200, "reload");
  assert.equal(body.reload.ok, true);
});

test("reserved tenant name `admin` subdomain → 404 (reserved naming policy)", async () => {
  const res = await gatewayFetch("admin", "/foo");
  const body = await readIntegrationJson(res, 404, "reserved admin tenant");
  assert.deepEqual(body, { error: "not_found", message: "Not found" });
});

test("hostname-level rejects do not leak namespace or path details", async () => {
  for (const ns of ["__system__", "__platform__", "admin"]) {
    const res = await gatewayFetch(ns, "/");
    const body = await readIntegrationJson(res, 404, `${ns}.workers.local/ hostname classification`);
    assert.deepEqual(body, { error: "not_found", message: "Not found" });
    assert.equal(Object.hasOwn(body, "example"), false,
      "404 must not carry the path-validation example field");
  }
});

test("known namespace, unknown worker → 404", async () => {
  await deployAndPromote("gwns1", "real", {
    code: "export default {fetch(){return new Response('ok')}};",
  });
  const res = await gatewayFetch("gwns1", "/ghost");
  await assertIntegrationJson(res, 404, { error: "not_found", message: "Not found" });
});

test("known namespace root path → 404 without internal example", async () => {
  await deployAndPromote("gwns-root", "real", {
    code: "export default {fetch(){return new Response('ok')}};",
  });
  const res = await gatewayFetch("gwns-root", "/");
  const body = await readIntegrationJson(res, 404, "known namespace root path");
  assert.deepEqual(body, { error: "not_found", message: "Not found" });
  assert.equal(Object.hasOwn(body, "example"), false);
});

test("multiple workers in same ns route correctly", async () => {
  await deployAndPromote("gwns2", "a", {
    code: "export default {fetch(){return new Response('A')}};",
  });
  await deployAndPromote("gwns2", "b", {
    code: "export default {fetch(){return new Response('B')}};",
  });
  const a = await gatewayFetch("gwns2", "/a");
  const b = await gatewayFetch("gwns2", "/b");
  assert.equal(await a.text(), "A");
  assert.equal(await b.text(), "B");
});

test("gateway passes x-worker-prefix + strips prefix from path + preserves query string", async () => {
  await deployAndPromote("gwns3", "echo", {
    code: `export default {
      async fetch(request) {
        const url = new URL(request.url);
        return new Response(JSON.stringify({
          path: url.pathname,
          search: url.search,
          prefix: request.headers.get("x-worker-prefix") || "",
        }), { headers: { "content-type": "application/json" }});
      }
    };`,
  });
  const res = await gatewayFetch("gwns3", "/echo/sub/path?q=1");
  const body = await readIntegrationJson(res, 200, "gateway prefix echo");
  assert.equal(body.prefix, "/echo");
  assert.equal(body.path, "/sub/path");
  assert.equal(body.search, "?q=1");
});

test("gateway strips client-supplied internal forwarding headers", async () => {
  await deployAndPromote("gwns-internal-headers", "echo", {
    code: `export default {
      async fetch(request) {
        return Response.json({
          workerId: request.headers.get("x-worker-id"),
          prefix: request.headers.get("x-worker-prefix"),
          upstreamBinding: request.headers.get("x-wdl-upstream-binding"),
          internalAuth: request.headers.get("x-wdl-internal-auth"),
          doOwnerHint: request.headers.get("x-wdl-do-owner-hint"),
          d1OwnerEndpoint: request.headers.get("x-wdl-d1-owner-endpoint"),
        });
      }
    };`,
  });
  const res = await gatewayFetch("gwns-internal-headers", "/echo", {
    headers: {
      "x-worker-id": "__platform__:platform-api:v1",
      "x-worker-prefix": "/forged",
      "x-wdl-upstream-binding": "RUNTIME_SYSTEM",
      "x-wdl-internal-auth": "spoofed",
      "x-wdl-do-owner-hint": "1",
      "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    },
  });
  await assertIntegrationJson(res, 200, {
    workerId: "gwns-internal-headers:echo:v1",
    prefix: "/echo",
    upstreamBinding: null,
    internalAuth: null,
    doOwnerHint: null,
    d1OwnerEndpoint: null,
  });
});

test("gateway strips tenant response internal headers", async () => {
  await deployAndPromote("gwns-response-headers", "echo", {
    code: `export default {
      fetch() {
        return new Response("ok", {
          headers: {
            "x-worker-id": "forged:worker:v1",
            "x-worker-prefix": "/forged",
            "x-wdl-internal-auth": "secret",
            "x-wdl-future-private": "hidden",
            "x-public": "visible",
          },
        });
      }
    };`,
  });

  const res = await gatewayFetch("gwns-response-headers", "/echo");
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "ok");
  assert.equal(res.headers["x-worker-id"], undefined);
  assert.equal(res.headers["x-worker-prefix"], undefined);
  assert.equal(res.headers["x-wdl-internal-auth"], undefined);
  assert.equal(res.headers["x-wdl-future-private"], undefined);
  assert.equal(res.headers["x-public"], "visible");
  assert.ok(res.headers["x-request-id"]);
});

test("promoting a new version replaces routed code (pub/sub invalidates cache)", async () => {
  await deployAndPromote("gwns4", "v", {
    code: "export default {fetch(){return new Response('one')}};",
  });
  const before = await gatewayFetch("gwns4", "/v");
  assert.equal(await before.text(), "one");

  await deployAndPromote("gwns4", "v", {
    code: "export default {fetch(){return new Response('two')}};",
  });
  await waitForGatewayText("gwns4", "/v", "two", "gateway observes promoted v2");
});

test("promoting an older version rolls back", async () => {
  const v1 = await deployAndPromote("gwns5", "v", {
    code: "export default {fetch(){return new Response('one')}};",
  });
  await deployAndPromote("gwns5", "v", {
    code: "export default {fetch(){return new Response('two')}};",
  });
  await waitForGatewayText("gwns5", "/v", "two", "gateway observes promoted v2 before rollback");

  await adminPost(`/ns/gwns5/worker/v/promote`, { version: v1 });
  await waitForGatewayText("gwns5", "/v", "one", "gateway observes rollback to v1");
});

test("old control port 8082 is not bound (regression guard)", async () => {
  const res = await fetch("http://localhost:8082/anything", { method: "POST" }).catch(
    (e) => ({ error: e.message, status: 0 })
  );
  assert.notEqual(res.status, 200, "port 8082 must not accept HTTP");
});

test("data plane /_metrics is now published on :8080", async () => {
  const res = await fetch(gatewayUrl("/_metrics"));
  assert.equal(res.status, 200);
  const body = await res.text();
  assert.match(body, /wdl_requests_total\{[^}]*service="gateway"/);
});

test("admin host does not route runtime internal endpoint names to runtime", async () => {
  const res = await adminFetch("/_scheduled", {
    method: "POST",
    headers: { Host: ADMIN_HOST_HEADER, "content-type": "application/json" },
    body: JSON.stringify({ scheduledTime: Date.now(), cron: "* * * * *" }),
  });
  await assertIntegrationJson(res, 400, {
    error: "invalid_path",
    message: "Invalid path. Use /ns/<ns>/worker/<name>/<action>",
  });
});

test("subdomain data plane uses paths reserved on the runtime internal socket", async () => {
  await deployAndPromote("gwns-internal", "app", {
    code: `export default {
      fetch() { return new Response("user fetch"); },
      scheduled() { return "scheduled"; },
      queue() { return "queued"; }
    };`,
  });

  for (const path of ["/app/_healthz", "/app/_metrics"]) {
    const res = await gatewayFetch("gwns-internal", path);
    assert.equal(res.status, 200, `${path} should be tenant fetch`);
    assert.equal(await res.text(), "user fetch");
  }

  for (const path of ["/app/_scheduled", "/app/_queued", "/app/internal/workflows/notify", "/app/internal/workflows/run"]) {
    const res = await gatewayFetch("gwns-internal", path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ scheduledTime: Date.now(), cron: "* * * * *", queue: "q", messages: [] }),
    });
    assert.equal(res.status, 200, `${path} should be tenant fetch`);
    assert.equal(await res.text(), "user fetch");
  }
});

test("POST admin /reload PUBLISHes route flush and gateway drops caches", async () => {
  await deployAndPromote("gwns-reload", "w", {
    code: "export default {fetch(){return new Response('warm')}};",
  });
  const warm = await gatewayFetch("gwns-reload", "/w");
  assert.equal(await warm.text(), "warm");

  const r = await adminPost("/reload", {});
  assert.equal(r.status, 200);
  assert.equal(r.json.reload.ok, true);
  // Both channels must reach at least one subscriber; receivers==0 on either
  // means gateway isn't fully attached and the pub/sub premise is broken.
  assert.ok(
    r.json.reload.routes.receivers >= 1 && r.json.reload.patterns.receivers >= 1,
    `expected subscribers on both channels, got ${JSON.stringify(r.json.reload)}`
  );
  await waitForGatewayCacheState(
    "gateway route caches clear after admin reload",
    (health) => health.namespace_cache_size === 0 && health.pattern_cache_size === 0
  );
  const after = await gatewayFetch("gwns-reload", "/w");
  assert.equal(after.status, 200);
  assert.equal(await after.text(), "warm");
});

test("subscriber ignores malformed route invalidation payloads", async () => {
  await gatewayFetch("missing-ns", "/anything");
  let health = await readIntegrationJson(await fetch(gatewayUrl("/healthz")), 200, "healthz before malformed invalidation");
  assert.equal(health.namespace_cache_size, 0);

  redisPublish("routes:invalidate", "Bad_NS");
  await delay(100);

  health = await readIntegrationJson(await fetch(gatewayUrl("/healthz")), 200, "healthz after malformed invalidation");
  assert.equal(health.namespace_cache_size, 0);
});

test("subscriber uses routes:flush, not routes:invalidate wildcard, for full route state flush", async () => {
  await deployAndPromote("gwns-flush", "w", {
    code: "export default {fetch(){return new Response('ok')}};",
  });
  const warm = await gatewayFetch("gwns-flush", "/w");
  assert.equal(await warm.text(), "ok");

  let health = await readIntegrationJson(await fetch(gatewayUrl("/healthz")), 200, "healthz before wildcard invalidation");
  assert.ok(health.namespace_cache_size >= 1,
    `expected namespace cache to be warm, got ${JSON.stringify(health)}`);

  redisPublish("routes:invalidate", "*");
  await delay(100);
  health = await readIntegrationJson(await fetch(gatewayUrl("/healthz")), 200, "healthz after wildcard invalidation");
  assert.ok(health.namespace_cache_size >= 1,
    `routes:invalidate '*' must not flush route state, got ${JSON.stringify(health)}`);

  redisPublish("routes:flush", "ignored-payload");
  await waitForGatewayCacheState(
    "gateway route cache clears after routes:flush",
    (health) => health.namespace_cache_size === 0
  );
  health = await readIntegrationJson(await fetch(gatewayUrl("/healthz")), 200, "healthz after routes flush");
  assert.equal(health.namespace_cache_size, 0);
});

test("subscriber accepts whitelisted reserved route invalidation payloads", async () => {
  const before = parseCounters(await (await fetch(gatewayUrl("/_metrics"))).text());
  redisPublish("routes:invalidate", "__system__");
  await delay(100);

  const after = parseCounters(await (await fetch(gatewayUrl("/_metrics"))).text());
  const key = `wdl_subscriber_invalidations_total{scope="namespace",service="gateway"}`;
  assert.equal((after.get(key) ?? 0) - (before.get(key) ?? 0), 1);
});

test("subscriber reconnects after Redis restart and resumes invalidations", async () => {
  await deployAndPromote("gwns-reco", "w", {
    code: "export default {fetch(){return new Response('pre')}};",
  });
  const pre = await gatewayFetch("gwns-reco", "/w");
  assert.equal(await pre.text(), "pre");

  redisDebugSleep(0);
  try {
    redisClientKillType("pubsub");
  } catch {
    // CLIENT KILL may race the subscriber detecting closure; reconnect
    // kicks in either way.
  }
  await waitForGatewaySubscriber();

  // Invalidation must travel over the post-reconnect subscriber.
  await deployAndPromote("gwns-reco", "w", {
    code: "export default {fetch(){return new Response('post')}};",
  });
  await waitForGatewayText("gwns-reco", "/w", "post", "gateway observes post-reconnect invalidation");
});

test("boot race: promote during pre-subscribe window still converges", async () => {
  await deployAndPromote("gwns-boot", "w", {
    code: "export default {fetch(){return new Response('v1')}};",
  });

  // Fresh isolate so onConnect (not onDisconnect) is the only path that
  // can clear a pre-attach cache warm.
  sh("docker compose restart gateway");
  await waitForGateway();

  // Warm cache before SUBSCRIBE ack, then promote — v2's PUBLISH may be
  // dropped while subscriber handshake is in flight.
  await gatewayFetch("gwns-boot", "/w");
  await deployAndPromote("gwns-boot", "w", {
    code: "export default {fetch(){return new Response('v2')}};",
  });

  await waitForGatewaySubscriber();
  await waitForGatewayText(
    "gwns-boot",
    "/w",
    "v2",
    "PUBLISH dropped during boot window must still converge via onConnect clear"
  );
});
