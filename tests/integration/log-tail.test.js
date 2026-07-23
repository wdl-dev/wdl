// @wdl-cli-integration
//
// End-to-end coverage for `wdl tail`: control SSE handler ↔ Rust proxy
// ↔ tail-worker ↔ loaded worker. The runtime activation gate is the
// most distinctive piece of this feature, so each test pins both the
// "events flow" and "no events flow" sides.

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import {
  ADMIN_HOST_HEADER,
  ADMIN_TOKEN,
  GATEWAY_HOST,
  GATEWAY_PORT,
  adminFetch,
  delay,
  deployAndPromote,
  gatewayWorkerId,
  gatewayFetch,
  runtimeDispatchPost,
  uniqueNs,
  waitForActivation,
  setupIntegrationSuite,
  responseJson,
  parseJsonText,
} from "./helpers/index.js";
import {
  redisCommand,
  redisDel,
  redisScriptFlush,
  redisXLen,
  redisXRangeRaw,
} from "./helpers/redis.js";

setupIntegrationSuite();

// Direct admin-API deploy of a raw-code worker; bypasses wrangler bundling
// to keep each test under ~5s (the CLI deploy path runs ~25s/test, fine
// for cli-smoke but burns this file's 180s suite budget across 5 cases).
const TAIL_WORKER_CODE = `
  export default {
    async fetch(request) {
      const url = new URL(request.url);
      const tag = url.searchParams.get("tag") || "default";
      console.log("hello tag=" + tag);
      if (url.searchParams.get("throw") === "1") throw new Error("boom " + tag);
      return new Response("ok " + tag + "\\n");
    },
  };
`;

const INVOCATION_WORKER_CODE = `
  export default {
    fetch() {
      return new Response("ok");
    },
    scheduled() {},
    queue(batch) {
      batch.ackAll();
    },
  };
`;

const FETCH_INVOCATION_WORKER_CODE = `
  export default {
    fetch(request) {
      const url = new URL(request.url);
      if (url.searchParams.get("fail") === "1") throw new Error("fetch failed");
      return new Response("", { status: 204 });
    },
  };
`;

const SERVICE_BINDING_TARGET_CODE = `
  export default {
    fetch() {
      return new Response("target ok", { status: 202 });
    },
  };
`;

const SERVICE_BINDING_CALLER_CODE = `
  export default {
    async fetch(request, env) {
      return await env.TARGET.fetch(new Request("https://svc/internal", {
        headers: request.headers,
      }));
    },
  };
`;

/** @param {string} ns @param {string} [name] @param {string} [code] */
async function deployTailWorker(ns, name = "tail-target", code = TAIL_WORKER_CODE) {
  const dep = await adminFetch(`/ns/${ns}/worker/${name}/deploy`, {
    method: "POST",
    body: JSON.stringify({ code }),
  });
  if (!dep.ok) throw new Error(`deploy failed: ${dep.status} ${await dep.text()}`);
  const { version } = await responseJson(dep);
  const prom = await adminFetch(`/ns/${ns}/worker/${name}/promote`, {
    method: "POST",
    body: JSON.stringify({ version }),
  });
  if (!prom.ok) throw new Error(`promote failed: ${prom.status} ${await prom.text()}`);
  return version;
}

// Spec-style SSE collector. Opens `GET /ns/<ns>/logs/tail?<query>`,
// drains for `durationMs`, returns { status, events[] }. Treats abort
// errors after our own req.destroy() as clean shutdown — the SSE body
// is a long-lived stream, server doesn't end-of-stream on its own.
/**
 * @typedef {{ event: string, id: string | null, data: string }} SseEvent
 * @typedef {{ status: number, headers: import("node:http").IncomingHttpHeaders, body?: string, events: SseEvent[] }} SseResult
 */

/**
 * @param {{ ns: string, query: string, headers?: Record<string, string>, durationMs?: number }} args
 * @returns {Promise<SseResult>}
 */
function openSseAndCollect({ ns, query, headers = {}, durationMs = 4000 }) {
  return new Promise((resolve, reject) => {
    const reqHeaders = {
      Host: ADMIN_HOST_HEADER,
      "x-admin-token": ADMIN_TOKEN,
      Accept: "text/event-stream",
      ...headers,
    };
    /** @type {Array<{ event: string, id: string | null, data: string }>} */
    const events = [];
    let aborted = false;
    const { promise: connectedPromise, resolve: onConnected } = Promise.withResolvers();
    /** @type {{ event: string, id: string | null, data: string[] }} */
    let current = { event: "message", id: null, data: [] };
    let buf = "";
    const dispatch = () => {
      if (current.data.length === 0) {
        current = { event: "message", id: current.id, data: [] };
        return;
      }
      events.push({ event: current.event, id: current.id, data: current.data.join("\n") });
      current = { event: "message", id: current.id, data: [] };
    };
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: `/ns/${ns}/logs/tail?${query}`,
      headers: reqHeaders,
      agent: false,
    }, (res) => {
      const status = res.statusCode || 0;
      onConnected({ status, headers: res.headers });
      if (status < 200 || status >= 300) {
        /** @type {Buffer[]} */
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          resolve({ status, headers: res.headers, body: text, events });
        });
        return;
      }
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        buf += chunk;
        let nl;
        while ((nl = buf.indexOf("\n")) >= 0) {
          let line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (line.endsWith("\r")) line = line.slice(0, -1);
          if (line === "") { dispatch(); continue; }
          if (line.startsWith(":")) continue;
          const colon = line.indexOf(":");
          const field = colon < 0 ? line : line.slice(0, colon);
          let value = colon < 0 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "event") current.event = value;
          else if (field === "id") current.id = value;
          else if (field === "data") current.data.push(value);
        }
      });
      res.on("end", () => resolve({ status, headers: res.headers, events }));
      res.on("error", (err) => {
        if (aborted) return resolve({ status, headers: res.headers, events });
        reject(err);
      });
      setTimeout(() => { aborted = true; req.destroy(); }, durationMs);
    });
    req.on("error", (err) => {
      if (aborted) return;
      reject(err);
    });
    req.end();
    // Surface connect errors via the rejection above.
    connectedPromise.catch(() => {});
  });
}

/** @param {string} ns @param {string} worker */
function streamLen(ns, worker) {
  return redisXLen(`logs:${ns}:${worker}:s`, { db: 1 });
}

/** @param {Array<{ event: string, id: string | null, data: string }>} events */
function parseEventJson(events) {
  return events
    .filter((e) => e.event !== "tail_warning")
    .map((e) => {
      try { return logTailEventJson(e); } catch { return null; }
    })
    .filter((/** @type {any} */ p) => p !== null);
}

/** @param {{ event: string, data: string }} event */
function logTailEventJson(event) {
  return parseJsonText(event.data, `log-tail ${event.event} event`);
}

test("wdl tail: console.* + uncaught exception flow end-to-end", async () => {
  const ns = uniqueNs("wdl-tail-flow");
  await deployTailWorker(ns);

  // Open SSE first; poll the proxy until the activation heartbeat has
  // landed so the first XADD afterward is already gated through.
  const ssePromise = openSseAndCollect({
    ns, query: "worker=tail-target", durationMs: 5000,
  });
  await waitForActivation(ns, "tail-target");
  redisScriptFlush();

  for (const tag of ["one", "two"]) {
    const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
    assert.equal(res.status, 200);
  }
  const failRes = await gatewayFetch(ns, `/tail-target/?tag=oops&throw=1`);
  assert.equal(failRes.status, 502);

  const { status, events } = await ssePromise;
  assert.equal(status, 200, "SSE handshake should be 200 for valid request");

  const payloads = parseEventJson(events);
  const consoleEvents = payloads.filter((p) => p.event === "worker_console");
  const exceptionEvents = payloads.filter((p) => p.event === "worker_exception");

  // tag values from console.log; one console.log per request hit.
  const tags = consoleEvents.map((p) => {
    const m = (Array.isArray(p.message) ? p.message[0] : p.message);
    return typeof m === "string" ? m.replace("hello tag=", "") : null;
  });
  assert.ok(tags.includes("one"), `expected tag "one" in ${JSON.stringify(tags)}`);
  assert.ok(tags.includes("two"), `expected tag "two" in ${JSON.stringify(tags)}`);
  assert.ok(tags.includes("oops"), `expected tag "oops" in ${JSON.stringify(tags)}`);

  // `>= 1` (not `=== 1`) future-proofs against workerd starting to
  // surface unhandled rejections via event.exceptions; the content
  // match pins it to our throw.
  assert.ok(exceptionEvents.length >= 1,
    `expected ≥1 exception, got ${exceptionEvents.length}`);
  const exc = exceptionEvents.find((e) => /boom oops/.test(String(e.message)));
  assert.ok(exc, `expected an exception event matching /boom oops/, got ${
    JSON.stringify(exceptionEvents.map((e) => e.message))}`);
  assert.ok(exc.worker_id, "exception event should carry worker_id from forwarded headers");

  // Every stream-backed event has SSE id; sanity-check the ids are valid
  // Redis stream ids so the resume contract is honored.
  for (const e of events) {
    if (e.event === "tail_warning") continue;
    assert.match(e.id || "", /^\d+-\d+$/, `bad id ${e.id}`);
  }

  // worker name injected by control before forwarding
  for (const p of payloads) {
    assert.equal(p.worker, "tail-target");
  }
});

test("wdl tail: fetch invocation events cover no-console success and failure", async () => {
  const ns = uniqueNs("wdl-tail-fetch");
  const worker = "fetch-target";
  const version = await deployTailWorker(ns, worker, FETCH_INVOCATION_WORKER_CODE);
  const workerId = gatewayWorkerId(ns, worker, version);

  const sse = openSseAndCollect({
    ns,
    query: `worker=${worker}`,
    durationMs: 6000,
  });
  await waitForActivation(ns, worker);

  const ok = await gatewayFetch(ns, `/${worker}/`);
  assert.equal(ok.status, 204);
  const failed = await gatewayFetch(ns, `/${worker}/?fail=1`);
  assert.equal(failed.status, 502);

  const collected = await sse;
  const payloads = parseEventJson(collected.events);
  const fetchEvents = payloads.filter((p) => p.event === "worker_fetch");

  const startEvents = fetchEvents.filter((p) => p.phase === "start");
  const finishEvents = fetchEvents.filter((p) => p.phase === "finish");
  assert.equal(startEvents.length, 2);
  assert.equal(finishEvents.length, 2);
  assert.deepEqual(fetchEvents.map((p) => p.worker_id), [workerId, workerId, workerId, workerId]);
  assert.deepEqual(startEvents.map((p) => p.method), ["GET", "GET"]);
  assert.deepEqual(startEvents.map((p) => p.path), ["/", "/"]);
  assert.ok(finishEvents.some((p) => p.outcome === "ok" && p.status === 204));
  assert.ok(finishEvents.some((p) => p.outcome === "error" && /fetch failed/.test(String(p.error))));
  assert.ok(finishEvents.every((p) => typeof p.duration_ms === "number"));
});

test("wdl tail: service-binding fetch emits target worker_fetch events", async () => {
  const ns = uniqueNs("wdl-tail-sbfetch");
  const target = "target";
  const targetVersion = await deployAndPromote(ns, target, { code: SERVICE_BINDING_TARGET_CODE });
  await deployAndPromote(ns, "caller", {
    code: SERVICE_BINDING_CALLER_CODE,
    bindings: { TARGET: { type: "service", service: target } },
  });
  const targetWorkerId = gatewayWorkerId(ns, target, targetVersion);

  const sse = openSseAndCollect({
    ns,
    query: `worker=${target}`,
    durationMs: 6000,
  });
  await waitForActivation(ns, target);

  const res = await gatewayFetch(ns, "/caller/");
  assert.equal(res.status, 202);

  const collected = await sse;
  const payloads = parseEventJson(collected.events);
  const fetchEvents = payloads.filter((p) => p.event === "worker_fetch");
  assert.deepEqual(fetchEvents.map((p) => p.phase), ["start", "finish"]);
  assert.equal(fetchEvents[0].worker_id, targetWorkerId);
  assert.equal(fetchEvents[0].method, "GET");
  assert.equal(fetchEvents[0].path, "/internal");
  assert.equal(fetchEvents[1].status, 202);
  assert.equal(fetchEvents[1].outcome, "ok");
});

test("wdl tail: new subscriber for B is visible after active-set miss cache refresh", async () => {
  // Misses are cached briefly to keep no-tailer cost low. Once that
  // cache expires, a newly subscribed worker must be discovered.
  const ns = uniqueNs("wdl-tail-newsub");
  await deployTailWorker(ns, "tail-a");
  await deployTailWorker(ns, "tail-b");

  const sseA = openSseAndCollect({
    ns, query: "worker=tail-a", durationMs: 8000,
  });
  await waitForActivation(ns, "tail-a");
  const aWarm = await gatewayFetch(ns, `/tail-a/?tag=Awarm`);
  assert.equal(aWarm.status, 200);
  // Let the short miss cache expire before opening B; the contract is
  // live-debug after the operator opens tail, not lossless capture for
  // events racing the activation signal.
  await delay(2200);

  const sseB = openSseAndCollect({
    ns, query: "worker=tail-b", durationMs: 4000,
  });
  await waitForActivation(ns, "tail-b");
  const bRes = await gatewayFetch(ns, `/tail-b/?tag=Bfirst`);
  assert.equal(bRes.status, 200);

  const [resA, resB] = await Promise.all([sseA, sseB]);
  const aTags = parseEventJson(resA.events)
    .filter((p) => p.event === "worker_console" && p.worker === "tail-a")
    .map((p) => {
      const m = Array.isArray(p.message) ? p.message[0] : p.message;
      return typeof m === "string" ? m.replace("hello tag=", "") : null;
    });
  const bTags = parseEventJson(resB.events)
    .filter((p) => p.event === "worker_console" && p.worker === "tail-b")
    .map((p) => {
      const m = Array.isArray(p.message) ? p.message[0] : p.message;
      return typeof m === "string" ? m.replace("hello tag=", "") : null;
    });
  assert.ok(aTags.includes("Awarm"),
    `tail-a session must see its own warmup event; got ${JSON.stringify(aTags)}`);
  assert.ok(bTags.includes("Bfirst"),
    `tail-b event must arrive after active-set refresh; got ${JSON.stringify(bTags)}`);
});

test("wdl tail: events after activation miss-cache expiry land in SSE", async () => {
  const ns = uniqueNs("wdl-tail-fresh-sub");
  await deployTailWorker(ns);

  // Warm the worker isolate without a subscriber so the active-set
  // cache populates as empty.
  for (const tag of ["warm1", "warm2"]) {
    const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
    assert.equal(res.status, 200);
  }
  await delay(200);
  assert.equal(streamLen(ns, "tail-target"), 0,
    "warmup must NOT write to Redis (no subscriber yet)");

  const ssePromise = openSseAndCollect({
    ns, query: "worker=tail-target", durationMs: 8000,
  });
  await waitForActivation(ns, "tail-target");
  await delay(2200);
  for (const tag of ["live1", "live2"]) {
    const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
    assert.equal(res.status, 200);
  }

  const { events } = await ssePromise;
  const tags = parseEventJson(events)
    .filter((p) => p.event === "worker_console")
    .map((p) => {
      const m = Array.isArray(p.message) ? p.message[0] : p.message;
      return typeof m === "string" ? m.replace("hello tag=", "") : null;
    });
  assert.ok(tags.includes("live1"),
    `live1 must arrive after activation miss-cache expiry; got ${JSON.stringify(tags)}`);
  assert.ok(tags.includes("live2"), `expected live2 in ${JSON.stringify(tags)}`);
});

// Cache-eviction (positive hit max-age + re-fetch verdict flip) is
// unit-tested in tests/unit/runtime-tail-worker.test.js where fetch
// can be mocked and call sites counted; the integration suite would
// need either a 30s activation-TTL wait or a proxy side-channel.

test("wdl tail: no subscriber → zero Redis writes", async () => {
  const ns = uniqueNs("wdl-tail-zero");
  await deployTailWorker(ns);

  // uniqueNs guarantees a never-seen key, so no prior activation
  // can leak into this test.
  for (const tag of ["a", "b", "c"]) {
    const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
    assert.equal(res.status, 200);
  }
  // Give ctx.waitUntil(POST /logs/tail/append) time to flush.
  await delay(800);

  assert.equal(streamLen(ns, "tail-target"), 0,
    "no Redis stream entries should be written when no tailer is subscribed");
});

test("wdl tail: invalid selector + invalid resume id rejected at handler", async () => {
  const ns = uniqueNs("wdl-tail-bad");
  await deployTailWorker(ns);

  // Empty selector — no worker= query parameter.
  const empty = await openSseAndCollect({ ns, query: "", durationMs: 1000 });
  assert.equal(empty.status, 400);
  const emptyBody = responseJson(empty);
  assert.equal(emptyBody.error, "missing_worker");

  const allSelector = await openSseAndCollect({
    ns, query: "all=1", durationMs: 1000,
  });
  assert.equal(allSelector.status, 400);
  assert.equal(responseJson(allSelector).error, "unsupported_selector");

  // Multi-worker `?since=` is rejected up front (explicit user intent
  // can't be silently misinterpreted across N stream cursors).
  const multiSince = await openSseAndCollect({
    ns, query: "worker=tail-target&worker=tail-other&since=1700000000000-0", durationMs: 1000,
  });
  assert.equal(multiSince.status, 400);
  assert.equal(responseJson(multiSince).error, "since_single_worker_only");

  // Multi-worker `Last-Event-ID` is silently IGNORED (not 400). Browser
  // EventSource reattaches it on every reconnect; failing those would
  // defeat reconnect entirely for legitimate multi-worker sessions.
  // The header makes no difference to the response shape (200 SSE).
  const multiHeader = await openSseAndCollect({
    ns, query: "worker=tail-target&worker=tail-other",
    headers: { "last-event-id": "1700000000000-0" },
    durationMs: 1000,
  });
  assert.equal(multiHeader.status, 200,
    "multi-worker session must accept Last-Event-ID without 400");

  // Sentinel `$` is rejected (would silently mean "from now")
  const dollarSince = await openSseAndCollect({
    ns, query: "worker=tail-target&since=%24", durationMs: 1000,
  });
  assert.equal(dollarSince.status, 400);
  assert.equal(responseJson(dollarSince).error, "invalid_resume_id");

  const hugeSince = await openSseAndCollect({
    ns, query: `worker=tail-target&since=1-${"9".repeat(21)}`, durationMs: 1000,
  });
  assert.equal(hugeSince.status, 400);
  assert.equal(responseJson(hugeSince).error, "invalid_resume_id");

  const tooManyExplicit = await openSseAndCollect({
    ns,
    query: Array.from({ length: 51 }, (_, i) => `worker=w${i}`).join("&"),
    durationMs: 1000,
  });
  assert.equal(tooManyExplicit.status, 400);
  assert.equal(responseJson(tooManyExplicit).error, "too_many_workers");
});

test("wdl tail: reserved ns rejected with 404 after auth (ops principal)", async () => {
  const reserved = await openSseAndCollect({
    ns: "__system__", query: "worker=anything", durationMs: 1000,
  });
  assert.equal(reserved.status, 404);
  assert.equal(responseJson(reserved).error, "not_found");
});

test("wdl tail: platform token opens its bound platform-tier namespace", async () => {
  const issued = await adminFetch("/auth/tokens", {
    method: "POST",
    body: JSON.stringify({ kind: "platform", ns: "__platform__", label: "tail-platform" }),
  });
  assert.equal(issued.status, 201);
  const { token } = await responseJson(issued);

  const platformTail = await openSseAndCollect({
    ns: "__platform__", query: "worker=anything", durationMs: 1000,
    headers: { "x-admin-token": token },
  });
  assert.equal(platformTail.status, 200);
});

test("wdl tail: scheduled and queue dispatches emit invocation events", async () => {
  const ns = uniqueNs("wdl-tail-dispatch");
  const worker = "dispatch-target";
  const version = await deployTailWorker(ns, worker, INVOCATION_WORKER_CODE);
  const workerId = gatewayWorkerId(ns, worker, version);

  const sse = openSseAndCollect({
    ns,
    query: `worker=${worker}`,
    durationMs: 6000,
  });
  await waitForActivation(ns, worker);

  const scheduled = runtimeDispatchPost("/_scheduled", {
    "x-worker-id": workerId,
    "x-request-id": "rid-tail-cron",
  }, {
    scheduledTime: 123456789,
    cron: "*/5 * * * *",
  });
  assert.equal(scheduled.status, 200, scheduled.body);

  const queued = runtimeDispatchPost("/_queued", {
    "x-worker-id": workerId,
    "x-request-id": "rid-tail-queue",
  }, {
    queue: "jobs",
    messages: [{
      id: "m1",
      first_seen_ms: "123456789",
      attempts: "0",
      content_type: "text",
      body_b64: Buffer.from("hello").toString("base64"),
    }],
  });
  assert.equal(queued.status, 200, queued.body);

  const collected = await sse;
  const payloads = parseEventJson(collected.events);
  const scheduledEvents = payloads.filter((p) => p.event === "worker_scheduled");
  const queueEvents = payloads.filter((p) => p.event === "worker_queue");

  assert.deepEqual(scheduledEvents.map((p) => p.phase), ["start", "finish"]);
  assert.deepEqual(queueEvents.map((p) => p.phase), ["start", "finish"]);
  assert.deepEqual(scheduledEvents.map((p) => p.request_id), ["rid-tail-cron", "rid-tail-cron"]);
  assert.deepEqual(queueEvents.map((p) => p.request_id), ["rid-tail-queue", "rid-tail-queue"]);
  assert.equal(scheduledEvents[0].worker_id, workerId);
  assert.equal(scheduledEvents[0].scheduled_time, 123456789);
  assert.equal(scheduledEvents[0].cron, "*/5 * * * *");
  assert.equal(scheduledEvents[1].outcome, "ok");
  assert.equal(queueEvents[0].worker_id, workerId);
  assert.equal(queueEvents[0].queue, "jobs");
  assert.equal(queueEvents[0].batch_size, 1);
  assert.equal(queueEvents[1].outcome, "ok");
});

test("wdl tail: Last-Event-ID wins over ?since= when both present", async () => {
  const ns = uniqueNs("wdl-tail-precedence");
  await deployTailWorker(ns);

  // Phase A: warm up — 2 events with SSE open so we have real ids.
  const phaseA = await (async () => {
    const sse = openSseAndCollect({ ns, query: "worker=tail-target", durationMs: 4000 });
    await waitForActivation(ns, "tail-target");
    for (const tag of ["preA", "preB"]) {
      const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
      assert.equal(res.status, 200);
    }
    return await sse;
  })();
  const aEvents = phaseA.events.filter((e) => e.event !== "tail_warning");
  assert.ok(aEvents.length >= 2);
  const olderId = aEvents[0].id ?? "";
  const newerId = aEvents.at(-1)?.id ?? "";
  assert.notEqual(olderId, newerId, "test fixture: need 2 distinct ids");

  // No new traffic between phases — both ids remain in-stream (well
  // under MAXLEN ~ 500 trim).

  // Phase B: send `?since=<older>` AND `Last-Event-ID: <newer>`. The
  // header must win — server should resume from `newer`, not `older`.
  // If the older one wins, we'd see the events between older and
  // newer replayed. Since no new events were produced, both versions
  // would be silent — instead we add ONE new request after connect.
  const phaseB = await (async () => {
    const sse = openSseAndCollect({
      ns,
      query: `worker=tail-target&since=${encodeURIComponent(olderId)}`,
      headers: { "last-event-id": newerId },
      durationMs: 3500,
    });
    await waitForActivation(ns, "tail-target");
    const res = await gatewayFetch(ns, `/tail-target/?tag=postNew`);
    assert.equal(res.status, 200);
    return await sse;
  })();
  const bPayloads = parseEventJson(phaseB.events);
  const bTags = bPayloads
    .filter((p) => p.event === "worker_console")
    .map((p) => {
      const m = Array.isArray(p.message) ? p.message[0] : p.message;
      return typeof m === "string" ? m.replace("hello tag=", "") : null;
    });
  assert.ok(bTags.includes("postNew"),
    `expected postNew in phase B, got ${JSON.stringify(bTags)}`);
  // If `?since=<older>` won, preB's events between older and newer would
  // re-stream. They must NOT.
  assert.ok(!bTags.includes("preB"),
    `preB must not replay (Last-Event-ID should win over ?since=); got ${JSON.stringify(bTags)}`);
});

test("wdl tail: Last-Event-ID resume returns only events after the cursor", async () => {
  const ns = uniqueNs("wdl-tail-resume");
  await deployTailWorker(ns);

  // Phase A: open SSE, fire 2 requests, capture last id.
  const phaseA = await (async () => {
    const sse = openSseAndCollect({
      ns, query: "worker=tail-target", durationMs: 4000,
    });
    await waitForActivation(ns, "tail-target");
    for (const tag of ["pre1", "pre2"]) {
      const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
      assert.equal(res.status, 200);
    }
    return await sse;
  })();
  assert.equal(phaseA.status, 200);
  const phaseAEvents = phaseA.events.filter((e) => e.event !== "tail_warning");
  assert.ok(phaseAEvents.length >= 2,
    `phase A expected ≥2 events, got ${phaseAEvents.length}`);
  const lastId = phaseAEvents.at(-1)?.id ?? "";
  assert.match(lastId, /^\d+-\d+$/);

  // Write mid1+mid2 under their own short-lived SSE so the activation
  // gate is hot during the writes — keeps the test independent of the
  // 30 s activation grace.
  const midSse = openSseAndCollect({
    ns, query: "worker=tail-target", durationMs: 2000,
  });
  await waitForActivation(ns, "tail-target");
  for (const tag of ["mid1", "mid2"]) {
    const res = await gatewayFetch(ns, `/tail-target/?tag=${tag}`);
    assert.equal(res.status, 200);
  }
  await midSse;
  assert.ok(streamLen(ns, "tail-target") >= 4,
    "stream should hold pre1+pre2+mid1+mid2 ids before resume");

  // Phase B: resume from lastId via Last-Event-ID; expect mid1 + mid2,
  // never the pre1/pre2 already-seen entries.
  const phaseB = await openSseAndCollect({
    ns,
    query: "worker=tail-target",
    headers: { "last-event-id": lastId },
    durationMs: 3000,
  });
  assert.equal(phaseB.status, 200);
  const phaseBPayloads = parseEventJson(phaseB.events);
  const phaseBTags = phaseBPayloads
    .filter((p) => p.event === "worker_console")
    .map((p) => {
      const m = Array.isArray(p.message) ? p.message[0] : p.message;
      return typeof m === "string" ? m.replace("hello tag=", "") : null;
    });
  assert.ok(phaseBTags.includes("mid1"), `expected mid1 in ${JSON.stringify(phaseBTags)}`);
  assert.ok(phaseBTags.includes("mid2"), `expected mid2 in ${JSON.stringify(phaseBTags)}`);
  assert.ok(!phaseBTags.includes("pre1"),
    `pre1 must not reappear (resume cursor advanced); got ${JSON.stringify(phaseBTags)}`);
  assert.ok(!phaseBTags.includes("pre2"),
    `pre2 must not reappear (resume cursor advanced); got ${JSON.stringify(phaseBTags)}`);
});

// Force the "stream key gone" condition by DEL'ing the stream
// behind the proxy's back; checkResumePoint must surface
// resume_stream_expired before XREAD blocks.
test("wdl tail: resume against an evicted stream surfaces tail_warning{resume_stream_expired}", async () => {
  const ns = uniqueNs("wdl-tail-expired");
  await deployTailWorker(ns);

  const seedSse = openSseAndCollect({
    ns, query: "worker=tail-target", durationMs: 2000,
  });
  await waitForActivation(ns, "tail-target");
  await gatewayFetch(ns, `/tail-target/?tag=seed`);
  const seedRes = await seedSse;
  const seedEvents = seedRes.events.filter((e) => e.event !== "tail_warning");
  assert.ok(seedEvents.length >= 1, "seed must produce at least one event");
  const seedId = seedEvents.at(-1)?.id ?? "";

  // The proxy's XADD on the next event would recreate the key, so
  // do NOT generate events between DEL and the resume open.
  redisDel(`logs:${ns}:tail-target:s`, { db: 1 });
  assert.equal(streamLen(ns, "tail-target"), 0,
    "DEL should have removed the stream key before resume");

  // Resume from seedId — checkResumePoint pre-check sees EXISTS=0 and
  // emits the warning before XREAD BLOCK.
  const expired = await openSseAndCollect({
    ns,
    query: "worker=tail-target",
    headers: { "last-event-id": seedId },
    durationMs: 2000,
  });
  assert.equal(expired.status, 200,
    "expired-stream resume still opens the SSE; the warning rides the body");
  const warning = expired.events.find((e) => e.event === "tail_warning");
  assert.ok(warning, "expected a tail_warning event after expired-stream resume");
  const warningPayload = logTailEventJson(warning);
  assert.equal(warningPayload.code, "resume_stream_expired",
    `expected resume_stream_expired, got ${warningPayload.code}`);
  // Warning carries no SSE id (would corrupt Last-Event-ID).
  assert.equal(warning.id, null,
    "tail_warning must not carry an SSE id (would poison resume cursor)");
});

// Anchor id captured under a hot tailer, then 600 synthetic XADDs
// trim it past MAXLEN ~ 500; resume against the anchor must surface
// the trimmed warning.
test("wdl tail: resume past the MAXLEN trim surfaces tail_warning{resume_point_trimmed}", async () => {
  const ns = uniqueNs("wdl-tail-trimmed");
  await deployTailWorker(ns);

  const seedSse = openSseAndCollect({
    ns, query: "worker=tail-target", durationMs: 30000,
  });
  await waitForActivation(ns, "tail-target");

  await gatewayFetch(ns, `/tail-target/?tag=trim-anchor`);
  // Poll Redis for the anchor — the SSE collector accumulates lazily.
  await /** @type {Promise<void>} */ (new Promise((res, rej) => {
    const start = Date.now();
    const tick = () => {
      const len = streamLen(ns, "tail-target");
      if (len >= 1) return res();
      if (Date.now() - start > 4000) return rej(new Error("anchor never appeared"));
      setTimeout(tick, 50);
    };
    tick();
  }));
  const anchorIdRaw = redisXRangeRaw(`logs:${ns}:tail-target:s`, "-", "+", { db: 1, count: 1 });
  const anchorId = anchorIdRaw.split(/\s/)[0].trim();
  assert.match(anchorId, /^\d+-\d+$/,
    `anchor id must look like a stream id; got ${JSON.stringify(anchorIdRaw)}`);

  // 600 synthetic XADDs straight to Redis (bypasses runtime; going
  // through gatewayFetch 600× would be ~30s and brittle).
  for (let i = 0; i < 12; i++) {
    redisCommand(
      `EVAL ${JSON.stringify(
        `for i=1,50 do redis.call('XADD', KEYS[1], 'MAXLEN', '~', '500', '*', 'json', '{}') end return 1`
      )} 1 ${JSON.stringify(`logs:${ns}:tail-target:s`)}`,
      { db: 1 }
    );
  }

  // Close the warmer so the resume open doesn't double-PUBLISH.
  await seedSse;

  const trimmed = await openSseAndCollect({
    ns,
    query: "worker=tail-target",
    headers: { "last-event-id": anchorId },
    durationMs: 2000,
  });
  assert.equal(trimmed.status, 200);
  const warning = trimmed.events.find((e) => e.event === "tail_warning");
  assert.ok(warning, "expected a tail_warning event after trim");
  const warningPayload = logTailEventJson(warning);
  assert.equal(warningPayload.code, "resume_point_trimmed",
    `expected resume_point_trimmed, got ${warningPayload.code}`);
  assert.equal(warning.id, null,
    "tail_warning must not carry an SSE id");
});
