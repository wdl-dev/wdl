// Loaded workers must not be able to reach the platform's internal
// service mesh. System-pool controls prove the same Node clients can reach
// the internal targets. Public Internet probes would be a flaky proxy for
// workerd's own allow=public classifier (upstream's job, not ours).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readRepositoryModuleSource } from "../helpers/load-shared-module.js";
import {
  adminPost,
  assertStatus,
  deployAndPromote,
  gatewayFetch,
  hostFetch,
  readIntegrationJson,
  responseJson,
  uniqueNs,
  setupIntegrationSuite,
} from "./helpers/index.js";

setupIntegrationSuite();

test("Node HTTP and TCP clients retain the tenant outbound network boundary", async () => {
  const ns = uniqueNs("node-netbnd");
  const bundle = {
    code: readRepositoryModuleSource("test-workers/workerd-compat/src/node-io.js"),
    compatibilityDate: "2026-09-15",
  };
  await deployAndPromote(ns, "probe", bundle);
  const host = `${ns}.test`;
  assertStatus(await adminPost("/ns/__system__/hosts", { hosts: [host] }), 200);
  await deployAndPromote("__system__", "node-probe", { ...bundle, routes: [`${host}/*`] });
  for (const [mode, target] of [
    ["http", "http://user-runtime:8088/_healthz"],
    ["tcp", "tcp://redis:6379"],
  ]) {
    const path = `/network/${mode}?target=${encodeURIComponent(target)}`;
    const allowed = await readIntegrationJson(await hostFetch(host, path), 200);
    assert.deepEqual(allowed, mode === "http" ? { outcome: "ok", status: 200 } : { outcome: "ok" });
    const result = await readIntegrationJson(
      await gatewayFetch(ns, `/probe${path}`), 200
    );
    assert.equal(result.outcome, "threw", JSON.stringify(result));
    assert.notEqual(result.message, "network probe timed out");
  }
});

// Worker reflects the outcome of one fetch attempt as JSON. Lets the test
// distinguish "worker code threw" (allowlist working) from "worker got a
// real HTTP response" (allowlist failed open).
const FETCH_PROBE = `
export default {
  async fetch(req) {
    const url = new URL(req.url).searchParams.get("target");
    try {
      const res = await fetch(url);
      return Response.json({ outcome: "ok", status: res.status });
    } catch (err) {
      return Response.json({
        outcome: "threw",
        name: err.name,
        message: err.message,
      });
    }
  }
};`;

const CONNECT_PROBE = `
import { connect } from "cloudflare:sockets";
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const target = u.searchParams.get("target");
    try {
      const sock = connect(target);
      // Force the connection to actually be established.
      await sock.opened;
      sock.close();
      return Response.json({ outcome: "ok" });
    } catch (err) {
      return Response.json({
        outcome: "threw",
        name: err.name,
        message: err.message,
      });
    }
  }
};`;

// Tenant workers must not reach runtime loader/internal sockets,
// control's socket, s3mock, or redis. __system__ ns workers are a
// different trust boundary — see system-pool-auth.test.js.
const INTERNAL_HTTP_TARGETS = [
  "http://user-runtime:8081/",
  "http://user-runtime:8088/_healthz",
  "http://system-runtime:8081/",
  "http://system-runtime:8088/_healthz",
  "http://system-runtime:8082/",
  "http://s3mock:9090/",
];

// Note: redis isn't HTTP, so a fetch() to it would fail at the protocol
// layer even without our allowlist. The connect() probe below is the
// load-bearing test for raw-TCP coverage of redis.
const INTERNAL_TCP_TARGETS = [
  "redis:6379",
  "user-runtime:8081",
  "user-runtime:8088",
  "system-runtime:8081",
  "system-runtime:8088",
  "system-runtime:8082",
];

for (const target of INTERNAL_HTTP_TARGETS) {
  test(`fetch() to internal target ${target} is rejected by workerd outbound allowlist`, async () => {
    const ns = uniqueNs("netbnd");
    await deployAndPromote(ns, "probe", { code: FETCH_PROBE });

    const res = await gatewayFetch(ns, `/probe/?target=${encodeURIComponent(target)}`);
    assert.equal(res.status, 200);
    const body = await responseJson(res);
    assert.equal(body.outcome, "threw",
      `expected fetch(${target}) to throw, but got status=${body.status}`);
  });
}

for (const target of INTERNAL_TCP_TARGETS) {
  test(`connect() to internal target ${target} is rejected by workerd outbound allowlist`, async () => {
    const ns = uniqueNs("netbnd");
    await deployAndPromote(ns, "probe", { code: CONNECT_PROBE });

    const res = await gatewayFetch(ns, `/probe/?target=${encodeURIComponent(target)}`);
    assert.equal(res.status, 200);
    const body = await responseJson(res);
    assert.equal(body.outcome, "threw",
      `expected connect(${target}) to throw, but got outcome=${body.outcome}`);
    // Distinguish a deny-list rejection from a TCP timeout/refused. The
    // former proves the allowlist is doing its job; the latter would mean
    // the connect was attempted (allowlist failed open) and hit a real
    // network problem.
    const msg = String(body.message || "").toLowerCase();
    const looksLikeDeny = /not allowed|disallowed|proxy|denied|refused by/.test(msg);
    assert.ok(looksLikeDeny,
      `connect(${target}) threw but error doesn't look like an allowlist deny: ${body.name}: ${body.message}`);
  });
}
