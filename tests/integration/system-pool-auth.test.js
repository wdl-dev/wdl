// __system__ workers get private+public outbound by design
// (config-system.capnp). The auth boundary is therefore control's
// X-Admin-Token, not TCP reachability — this suite pins that.
// Gateway's subdomain branch 404s reserved ns, so these tests reach
// the caller via a declared-host pattern route.

import http from "node:http";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  ADMIN_TOKEN,
  adminPost,
  assertStatus,
  GATEWAY_HOST,
  GATEWAY_PORT,
  uniqueNs,
  setupIntegrationSuite,
  withResponseJsonAccessors,
} from "./helpers/index.js";

setupIntegrationSuite();

// Driver: fetch() from inside __system__ pool, report status + body
// so tests can tell 401 / 200 / network error apart.
const CONTROL_CALLER = `
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const token = u.searchParams.get("token");
    const path = u.searchParams.get("path") || "/ns/probe/worker/missing/versions";
    const init = token ? { headers: { "x-admin-token": token } } : {};
    try {
      const res = await fetch("http://system-runtime:8082" + path, init);
      const body = await res.text();
      return Response.json({ outcome: "http", status: res.status, body: body.slice(0, 200) });
    } catch (err) {
      return Response.json({ outcome: "threw", name: err.name, message: err.message });
    }
  }
};`;

/** @param {string} uniqueHost @param {string} uniqueRoute */
async function deployCaller(uniqueHost, uniqueRoute) {
  const ns = "__system__";
  const decl = await adminPost("/ns/__system__/hosts", { hosts: [uniqueHost] });
  assertStatus(decl, 200, "system pool host declare");
  const dep = await adminPost(`/ns/${ns}/worker/caller/deploy`, {
    code: CONTROL_CALLER,
    routes: [`${uniqueHost}${uniqueRoute}`],
  });
  assertStatus(dep, 201, "system pool caller deploy");
  const prom = await adminPost(`/ns/${ns}/worker/caller/promote`, {
    version: dep.json.version,
  });
  assertStatus(prom, 200, "system pool caller promote");
}

/** @param {string} host @param {string} pathWithQuery */
function hostFetch(host, pathWithQuery) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method: "GET",
        path: pathWithQuery,
        headers: { Host: host },
        agent: false,
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
          }, "system pool host response body"));
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

test("__system__ worker reaches control's TCP socket through privileged outbound", async () => {
  const host = `${uniqueNs("sys").replaceAll("-", "")}.test`;
  await deployCaller(host, "/caller/*");

  const res = await hostFetch(host, `/caller/?token=${encodeURIComponent(ADMIN_TOKEN)}`);
  assert.equal(res.status, 200);
  const body = res.json();
  assert.equal(body.outcome, "http", `expected http outcome, got ${JSON.stringify(body)}`);
  assert.equal(body.status, 200, "with valid token control must return 200");
});

test("__system__ worker without token gets 401 from control (app-layer boundary)", async () => {
  const host = `${uniqueNs("sys").replaceAll("-", "")}.test`;
  await deployCaller(host, "/caller/*");

  const res = await hostFetch(host, `/caller/`);
  assert.equal(res.status, 200);
  const body = res.json();
  assert.equal(body.outcome, "http", `TCP must reach control; outcome=${body.outcome}`);
  assert.equal(body.status, 401, "without token control must 401");
});

test("__system__ worker with wrong token gets 401 from control", async () => {
  const host = `${uniqueNs("sys").replaceAll("-", "")}.test`;
  await deployCaller(host, "/caller/*");

  const res = await hostFetch(host, `/caller/?token=not-the-real-token`);
  assert.equal(res.status, 200);
  const body = res.json();
  assert.equal(body.outcome, "http");
  assert.equal(body.status, 401, "bad token must 401");
});

test("control via admin-host rejects missing token with 401 (host-path parity)", async () => {
  // Direct admin-host ingress — same app-layer gate, different shape.
  const res = await adminFetch("/ns/probe/worker/missing/versions", {
    method: "GET",
    headers: { "x-admin-token": "" },
  });
  assert.equal(res.status, 401);
});
