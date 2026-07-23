// End-to-end pattern routing: admin deploys → gateway dispatches by Host
// + path, longest-prefix wins, invalidation propagates.

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import {
  GATEWAY_HOST,
  GATEWAY_PORT,
  adminPost,
  assertStatus,
  delay,
  deployAndPromote,
  gatewayUrl,
  readIntegrationJson,
  responseJson,
  setupIntegrationSuite,
  uniqueNs,
  withResponseJsonAccessors,
} from "./helpers/index.js";
import { redisDel } from "./helpers/redis.js";

setupIntegrationSuite();

// gatewayFetch in _helpers hardcodes ns.workers.local; the pattern
// branch needs arbitrary Host headers.
/**
 * @param {string} host
 * @param {string} p
 * @param {{ method?: string, headers?: Record<string, string> }} [init]
 */
function fetchWithHost(host, p, init = {}) {
  const method = init.method || "GET";
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method,
        path: p,
        headers: { Host: host, ...(init.headers || {}) },
      },
      (res) => {
        /** @type {Buffer[]} */
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          resolve(withResponseJsonAccessors({
            status: res.statusCode,
            body,
            text: () => body,
          }, "routing gateway response body"));
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

// Echo worker that reflects the request path + a worker-identifying tag.
/** @param {string} tag */
function echoWorker(tag) {
  const code = `export default {
    fetch(req) {
      const u = new URL(req.url);
      return new Response(JSON.stringify({
        tag: ${JSON.stringify(tag)},
        path: u.pathname,
        host: u.hostname,
      }), { headers: { "content-type": "application/json" } });
    }
  };`;
  return { code };
}

test("pattern branch: custom host + path reach the worker, path not stripped", async () => {
  const ns = uniqueNs("gw-basic");
  const host = "basic.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "app", {
    ...echoWorker("app"),
    routes: [`${host}/*`],
  });

  const res = await fetchWithHost(host, "/some/path");
  assertStatus(res, 200, "basic route fetch");
  const body = res.json();
  assert.equal(body.tag, "app");
  assert.equal(body.path, "/some/path");
});

test("pattern branch: runtime-looking paths remain tenant fetch paths", async () => {
  const ns = uniqueNs("gw-internal");
  const host = "internal.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "app", {
    ...echoWorker("app"),
    routes: [`${host}/*`],
  });

  for (const path of ["/_queued", "/_scheduled", "/internal/workflows/notify", "/internal/workflows/run"]) {
    const res = await fetchWithHost(host, path, {
      method: "POST",
      headers: { "content-type": "application/json" },
    });
    assertStatus(res, 200, `${path} tenant fetch`);
    const body = await responseJson(res);
    assert.equal(body.tag, "app");
    assert.equal(body.path, path);
  }
});

test("pattern branch: longest-prefix match picks the right worker", async () => {
  const ns = uniqueNs("gw-prefix");
  const host = "prefix.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "root", {
    ...echoWorker("root"),
    routes: [`${host}/*`],
  });
  await deployAndPromote(ns, "api", {
    ...echoWorker("api"),
    routes: [`${host}/api/*`],
  });

  const rootHit = await fetchWithHost(host, "/anything");
  assert.equal(rootHit.json().tag, "root");

  const apiHit = await fetchWithHost(host, "/api/users");
  assert.equal(apiHit.json().tag, "api");
});

test("pattern branch: unknown host → 404", async () => {
  const res = await fetchWithHost("nothing.workers.example", "/whatever");
  assertStatus(res, 404, "unknown host fetch");
  assert.deepEqual(res.json(), { error: "not_found", message: "Not found" });
  const health = await readIntegrationJson(
    await fetch(gatewayUrl("/healthz")),
    200,
    "gateway health after unknown host"
  );
  assert.equal(health.pattern_cache_size, 0);
});

test("pattern branch: CF exact-match semantics — /mcp doesn't match /mcphello", async () => {
  const ns = uniqueNs("gw-exact");
  const host = "gwexact.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "exact", {
    ...echoWorker("exact"),
    routes: [`${host}/mcp`],
  });
  await deployAndPromote(ns, "sub", {
    ...echoWorker("sub"),
    routes: [`${host}/mcp/*`],
  });

  assert.equal(responseJson(await fetchWithHost(host, "/mcp")).tag, "exact");
  assert.equal(responseJson(await fetchWithHost(host, "/mcp/v1")).tag, "sub");
  // CF: bare `host/mcp` does NOT match `/mcphello` (no over-match)
  assertStatus(await fetchWithHost(host, "/mcphello"), 404, "over-match guard fetch");
});

test("pattern branch: promote invalidates gateway cache (next request sees new version)", async () => {
  const ns = uniqueNs("gw-invalidate");
  const host = "invalidate.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "app", {
    ...echoWorker("v1"),
    routes: [`${host}/*`],
  });

  let res = await fetchWithHost(host, "/x");
  assert.equal(res.json().tag, "v1");

  await deployAndPromote(ns, "app", {
    ...echoWorker("v2"),
    routes: [`${host}/*`],
  });

  // Pub/sub invalidation is fire-and-forget; give it a beat before asserting v2.
  await delay(100);
  res = await fetchWithHost(host, "/x");
  assert.equal(res.json().tag, "v2");
});

test("subdomain routing dispatches without custom-host branch", async () => {
  const ns = uniqueNs("gw-subdomain");
  await deployAndPromote(ns, "hello", {
    code: `export default { fetch() { return new Response("hello-subdomain"); } };`,
  });
  const res = await fetchWithHost(`${ns}.workers.local`, "/hello");
  assertStatus(res, 200, "subdomain fetch");
  assert.equal(res.text(), "hello-subdomain");
});

test("rollback flips pattern routing", async () => {
  const ns = uniqueNs("gw-rollback");
  const host = "rb.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });

  const v1 = await deployAndPromote(ns, "app", {
    ...echoWorker("v1"),
    routes: [`${host}/*`],
  });
  await deployAndPromote(ns, "app", {
    ...echoWorker("v2"),
    routes: [`${host}/*`],
  });

  await delay(100);
  let res = await fetchWithHost(host, "/");
  assert.equal(res.json().tag, "v2");

  const p = await adminPost(`/ns/${ns}/worker/app/promote`, { version: v1 });
  assertStatus(p, 200, "rollback promote");
  await delay(100);
  res = await fetchWithHost(host, "/");
  assert.equal(res.json().tag, "v1");
});

// --- cache rebuild ----------------------------------------------------------

test("POST /reload rebuilds declared-host gate from existing host declarations", async () => {
  const ns = uniqueNs("gw-reload-hosts");
  const host = "reload-host.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await deployAndPromote(ns, "app", {
    ...echoWorker("reload"),
    routes: [`${host}/*`],
  });

  redisDel("declared-hosts");
  redisDel(`host-declarations:${host}`);

  const stale = await fetchWithHost(host, "/some/path");
  assertStatus(stale, 404, "stale declared host fetch");

  const reload = await adminPost("/reload", {});
  assertStatus(reload, 200, "reload");
  assert.equal(reload.json.reload.declarations.ok, true);
  assert.ok(reload.json.reload.declarations.declaredHosts >= 1);

  const healed = await fetchWithHost(host, "/some/path");
  assertStatus(healed, 200, "healed declared host fetch");
  const body = healed.json();
  assert.equal(body.tag, "reload");
  assert.equal(body.path, "/some/path");
});
