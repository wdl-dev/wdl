import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { performance } from "node:perf_hooks";
import {
  ADMIN_TOKEN,
  GATEWAY_HOST,
  GATEWAY_PORT,
  assertStatus,
  adminPost,
  deployAndPromote,
  gatewayFetch,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
  withResponseJsonAccessors,
} from "./helpers/index.js";

setupIntegrationSuite();

const ABORT_WORKER = readFileSync(
  new URL("../../test-workers/http-abort/src/index.js", import.meta.url),
  "utf8"
);

test("client disconnect response stream behavior is bounded on current workerd", async () => {
  const ns = uniqueNs("abort");
  await deployAndPromote(ns, "w", {
    mainModule: "worker.js",
    modules: { "worker.js": ABORT_WORKER },
    bindings: { MARKER: { type: "kv", id: "abort-test" } },
  });
  const key = `k-${Date.now()}`;

  await new Promise((resolve, reject) => {
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: `/w?key=${encodeURIComponent(key)}`,
      headers: { Host: `${ns}.workers.local` },
      agent: false,
    }, (res) => {
      assert.equal(res.statusCode, 200);
      res.once("data", () => {
        // Wait until a chunk lands so the worker is mid-loop, then drop.
        res.socket.destroy();
        resolve(undefined);
      });
      res.once("error", reject);
    });
    req.on("error", reject);
    req.end();
  });

  await waitUntil("disconnect marker written", async () => {
    const r = await gatewayFetch(ns, `/w/poll?key=${encodeURIComponent(key)}`);
    const text = await r.text();
    // workerd #6832 means 2026-06-19+ no longer calls ReadableStream.cancel()
    // for this async response-body pattern on client disconnect. If "cancel"
    // appears again, upstream likely fixed the bug and WDL should restore the
    // strict cancel assertion and re-evaluate log-tail watchdog/documentation.
    if (text === "cancel") {
      throw new Error("upstream workerd #6832 appears fixed; restore strict cancel assertion and re-evaluate WDL stream watchdogs");
    }
    if (text === "enqueue-threw" || text === "ended-normally") return true;
    if (text === "__null__") return false;
    throw new Error(`unexpected marker value ${JSON.stringify(text)}`);
  }, { timeoutMs: 30000, intervalMs: 250 });
});

const STREAM_WORKER = `
  export default {
    async fetch() {
      const enc = new TextEncoder();
      const stream = new ReadableStream({
        async start(controller) {
          for (let i = 0; i < 5; i++) {
            controller.enqueue(enc.encode(\`chunk:\${i}:\${Date.now()}\\n\`));
            await new Promise((r) => setTimeout(r, 150));
          }
          controller.close();
        },
      });
      return new Response(stream, {
        headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
      });
    },
  };
`;

test("streaming response is not buffered by gateway or runtime", async () => {
  const ns = uniqueNs("stream");
  await deployAndPromote(ns, "s", { code: STREAM_WORKER });

  /** @type {number[]} */
  const arrivals = [];
  await new Promise((resolve, reject) => {
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: `/s`,
      headers: { Host: `${ns}.workers.local` },
      agent: false,
    }, (res) => {
      assert.equal(res.statusCode, 200);
      assert.match(res.headers["content-type"] || "", /text\/event-stream/);
      const startedAt = performance.now();
      // TCP both coalesces and splits; consume complete \n-terminated
      // lines, keep the trailing partial for the next data event.
      let pending = "";
      res.on("data", (c) => {
        pending += c.toString("utf8");
        let idx;
        while ((idx = pending.indexOf("\n")) !== -1) {
          const line = pending.slice(0, idx);
          pending = pending.slice(idx + 1);
          if (line.startsWith("chunk:")) arrivals.push(performance.now() - startedAt);
        }
      });
      res.on("end", () => {
        if (pending.startsWith("chunk:")) arrivals.push(performance.now() - startedAt);
        resolve(undefined);
      });
      res.on("error", reject);
    });
    req.on("error", reject);
    req.end();
  });

  assert.equal(arrivals.length, 5, `expected 5 chunks, got ${arrivals.length}: ${arrivals}`);
  // Worker drips 5 × 150ms ≈ 600ms. Buffering would cluster arrivals
  // together; 300ms is generous headroom on the spread invariant.
  const lastArrival = arrivals.at(-1) ?? 0;
  const firstArrival = arrivals[0];
  assert.ok(lastArrival - firstArrival >= 300,
    `expected chunk spread ≥ 300ms, got ${lastArrival - firstArrival}ms (arrivals: ${arrivals})`);
});

const CONNECT_WORKER = `
  import { connect } from "cloudflare:sockets";
  export default {
    async fetch() {
      const sock = connect("redis:6379");
      try {
        const writer = sock.writable.getWriter();
        await writer.write(new TextEncoder().encode("*1\\r\\n$4\\r\\nPING\\r\\n"));
        writer.releaseLock();
        const reader = sock.readable.getReader();
        const { value } = await reader.read();
        reader.releaseLock();
        return Response.json({ reply: new TextDecoder().decode(value) });
      } finally {
        try { await sock.close(); } catch {}
      }
    },
  };
`;

/** @param {string} host @param {string} pathWithQuery */
function hostFetch(host, pathWithQuery) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: pathWithQuery,
      headers: { Host: host, "x-admin-token": ADMIN_TOKEN },
      agent: false,
    }, (res) => {
      /** @type {Buffer[]} */
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        resolve(withResponseJsonAccessors({
          status: res.statusCode,
          body,
          text: () => body,
        }, "system host response body"));
      });
    });
    req.on("error", reject);
    req.end();
  });
}

test("__system__ worker reaches redis:6379 via cloudflare:sockets connect()", async () => {
  // Reserved-ns subdomains 404 at gateway; __system__ reaches loaded
  // workers via pattern routing (see ROUTES_ALLOWED_RESERVED_NS).
  const host = `${uniqueNs("sys").replaceAll("-", "")}.test`;
  const decl = await adminPost("/ns/__system__/hosts", { hosts: [host] });
  assertStatus(decl, 200, "system host declare");
  const dep = await adminPost(`/ns/__system__/worker/redis-probe/deploy`, {
    mainModule: "worker.js",
    modules: { "worker.js": CONNECT_WORKER },
    routes: [`${host}/probe`],
  });
  assertStatus(dep, 201, "redis probe deploy");
  const prom = await adminPost(`/ns/__system__/worker/redis-probe/promote`, {
    version: dep.json.version,
  });
  assertStatus(prom, 200, "redis probe promote");

  const res = await hostFetch(host, "/probe");
  assert.equal(res.status, 200);
  const body = res.json();
  // Match on prefix — kernel could in theory split even +PONG\r\n.
  assert.match(body.reply, /^\+PONG/,
    `expected +PONG from redis, got ${JSON.stringify(body.reply)}`);
});
