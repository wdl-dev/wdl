import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminGet,
  delay,
  deployAndPromote,
  gatewayFetch,
  gatewayUrl,
  gatewayWorkerId,
  responseJson,
  runtimeInternalGet,
  runtimeInternalPost,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { parseCounters } from "./helpers/prometheus.js";

setupIntegrationSuite();

// Control has no /metrics or /healthz; its ingress traffic surfaces via
// gateway's /_metrics under route=worker_fetch_admin_host.
test("control-plane requests surface on gateway /_metrics under route=worker_fetch_admin_host", async () => {
  await adminGet("/ns/obs-admin/worker/missing/versions");
  const res = await fetch(gatewayUrl("/_metrics"));
  assert.equal(res.status, 200);
  const body = await res.text();
  assert.match(
    body,
    /wdl_requests_total\{[^}]*route="worker_fetch_admin_host"[^}]*service="gateway"/
  );
});

test("gateway exposes metrics on the data plane (/_metrics on :8080)", async () => {
  await deployAndPromote("obs-gw", "hello", {
    code: "export default { fetch() { return new Response('ok'); } };",
  });
  const routed = await gatewayFetch("obs-gw", "/hello");
  assert.equal(routed.status, 200);

  const res = await fetch(gatewayUrl("/_metrics"));
  assert.equal(res.status, 200);
  const body = await res.text();
  assert.match(body, /wdl_requests_total\{[^}]*service="gateway"/);
  assert.match(
    body,
    /wdl_routing_lookups_total\{[^}]*outcome="hit"[^}]*service="gateway"[^}]*stage="namespace_gate"/
  );
  assert.match(
    body,
    /wdl_routing_lookups_total\{[^}]*outcome="miss"[^}]*service="gateway"[^}]*stage="route_cache"/
  );
  assert.match(body, /wdl_runtime_forward_duration_ms_count\{[^}]*binding="RUNTIME_USER"[^}]*outcome="ok"[^}]*service="gateway"/);
  assert.match(body, /wdl_subscriber_connected\{[^}]*service="gateway"[^}]*\}\s+1/);
});

test("gateway exposes PUBLISH invalidation counters after a promote", async () => {
  await deployAndPromote("obs-inv", "hello", {
    code: "export default { fetch() { return new Response('v1'); } };",
  });
  await gatewayFetch("obs-inv", "/hello");
  await deployAndPromote("obs-inv", "hello", {
    code: "export default { fetch() { return new Response('v2'); } };",
  });
  await delay(100);

  const body = await (await fetch(gatewayUrl("/_metrics"))).text();
  assert.match(
    body,
    /wdl_subscriber_invalidations_total\{[^}]*scope="namespace"[^}]*\}\s+[1-9]/
  );
});

test("user-runtime exposes loader and bundle timing metrics", async () => {
  await deployAndPromote("obs-rt", "hello", {
    code: "export default { fetch() { return new Response('runtime'); } };",
  });
  const routed = await gatewayFetch("obs-rt", "/hello");
  assert.equal(routed.status, 200);

  const body = runtimeInternalGet("/_metrics");
  assert.match(body, /wdl_requests_total\{[^}]*service="user-runtime"/);
  assert.match(body, /wdl_loader_misses_total\{[^}]*service="user-runtime"/);
  assert.match(body, /wdl_bundle_load_duration_ms_count\{[^}]*service="user-runtime"/);
});

test("user-runtime exposes KV, R2, and queue binding operation metrics", async () => {
  // user-runtime counters are process-local singletons, no per-file reset;
  // assert deltas, not absolutes.
  const before = parseCounters(runtimeInternalGet("/_metrics"));

  const version = await deployAndPromote("obs-bindings", "ops", {
    code: `
export default {
  async fetch(req, env) {
    await env.KV.put("k", "v");
    await env.KV.get("k");
    await env.BUCKET.put("r2-key", "r2");
    await env.BUCKET.get("r2-key");
    await env.MY_Q.send({ ok: true });
    return new Response("ok");
  }
};`,
    bindings: {
      KV: { type: "kv", id: "test" },
      BUCKET: { type: "r2", bucketName: "obs-bindings" },
      MY_Q: { type: "queue", id: "obs-q" },
    },
  });

  const res = runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId("obs-bindings", "ops", version),
  }, "");
  assert.equal(res.status, 200, res.body);

  const after = parseCounters(runtimeInternalGet("/_metrics"));

  const putKey = `wdl_binding_operations_total{binding="kv",operation="put",outcome="ok",service="user-runtime"}`;
  const getKey = `wdl_binding_operations_total{binding="kv",operation="get",outcome="ok",service="user-runtime"}`;
  const r2PutKey = `wdl_binding_operations_total{binding="r2",operation="put",outcome="ok",service="user-runtime"}`;
  const r2GetKey = `wdl_binding_operations_total{binding="r2",operation="get",outcome="ok",service="user-runtime"}`;
  const sendKey = `wdl_binding_operations_total{binding="queue",operation="send",outcome="ok",service="user-runtime"}`;
  const putDurKey = `wdl_binding_operation_duration_ms_count{binding="kv",operation="put",service="user-runtime"}`;
  const r2PutDurKey = `wdl_binding_operation_duration_ms_count{binding="r2",operation="put",service="user-runtime"}`;

  assert.equal((after.get(putKey) ?? 0) - (before.get(putKey) ?? 0), 1, "kv put delta");
  assert.equal((after.get(getKey) ?? 0) - (before.get(getKey) ?? 0), 1, "kv get delta");
  assert.equal((after.get(r2PutKey) ?? 0) - (before.get(r2PutKey) ?? 0), 1, "r2 put delta");
  assert.equal((after.get(r2GetKey) ?? 0) - (before.get(r2GetKey) ?? 0), 1, "r2 get delta");
  assert.equal((after.get(sendKey) ?? 0) - (before.get(sendKey) ?? 0), 1, "queue send delta");
  assert.equal((after.get(putDurKey) ?? 0) - (before.get(putDurKey) ?? 0), 1, "kv put duration count delta");
  assert.equal((after.get(r2PutDurKey) ?? 0) - (before.get(r2PutDurKey) ?? 0), 1, "r2 put duration count delta");
});

test("gateway /healthz reports subscriber + cache state", async () => {
  const res = await fetch(gatewayUrl("/healthz"));
  assert.equal(res.status, 200);
  const body = await responseJson(res);
  assert.equal(body.ok, true);
  assert.equal(body.service, "gateway");
  assert.equal(body.subscriber_connected, true);
  assert.equal(typeof body.namespace_cache_size, "number");
  assert.equal(typeof body.route_cache_size, "number");
});

test("user-runtime internal health check returns SERVICE_NAME binding", async () => {
  const body = responseJson({ body: runtimeInternalGet("/_healthz") });
  assert.deepEqual(body, {
    ok: true,
    service: "user-runtime",
  });
});

test("gateway mints and echoes x-request-id when client omits it", async () => {
  await deployAndPromote("obs-reqid-mint", "hello", {
    code: "export default { fetch() { return new Response('ok'); } };",
  });
  const res = await gatewayFetch("obs-reqid-mint", "/hello");
  assert.equal(res.status, 200);
  const id = res.headers["x-request-id"];
  assert.ok(id && /^[0-9a-f]{16}$/.test(String(id)), `expected 16-hex id, got ${id}`);
});

test("gateway honors upstream x-request-id across tiers", async () => {
  await deployAndPromote("obs-reqid-honor", "hello", {
    code: "export default { fetch(req) { return new Response(req.headers.get('x-request-id') || ''); } };",
  });
  const sent = "fixed-upstream-id-0123";
  const res = await gatewayFetch("obs-reqid-honor", "/hello", {
    headers: { "x-request-id": sent },
  });
  assert.equal(res.status, 200);
  assert.equal(res.headers["x-request-id"], sent);
  assert.equal(await res.text(), sent);
});

test("control echoes x-request-id on its responses (via gateway admin-host path)", async () => {
  // No /healthz on control; probe a read-only endpoint with no side effects.
  const res = await adminFetch("/ns/obs-reqid-control/worker/missing/versions");
  assert.equal(res.status, 200);
  const id = res.headers["x-request-id"];
  assert.ok(id && /^[0-9a-f]{16}$/.test(String(id)), `expected 16-hex id, got ${id}`);
});

test("prometheus output declares TYPE lines and splits _max into its own gauge family", async () => {
  const body = runtimeInternalGet("/_metrics");
  assert.match(body, /^# TYPE wdl_requests_total counter$/m);
  assert.match(body, /^# TYPE wdl_request_duration_ms summary$/m);
  assert.match(body, /^# TYPE wdl_request_duration_ms_max gauge$/m);
  const summaryBlock = body.match(
    /# TYPE wdl_request_duration_ms summary\n(?:wdl_request_duration_ms_(?:count|sum)[^\n]*\n)+/
  );
  assert.ok(summaryBlock, "expected a summary block with only _count/_sum");
});
