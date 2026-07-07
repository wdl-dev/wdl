// Worker- and ns-level secrets. Assumes compose stack.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminGet,
  adminPost,
  assertStatus,
  composeRestart,
  cronId,
  delay,
  deployAndPromote,
  gatewayFetch,
  gatewayWorkerId,
  responseJson,
  runtimeInternalPost,
  uniqueNs,
  waitForCurrentSlotFixtureWindow,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisHGetAll, redisHashJsonField, redisSAdd } from "./helpers/redis.js";

setupIntegrationSuite();

// Worker that reflects env — only stringable keys pass through so the
// assertion side can do plain equality.
const ENV_HARNESS = `
export default {
  fetch(_, env) {
    const out = {};
    for (const [k, v] of Object.entries(env)) {
      if (typeof v === "string") out[k] = v;
    }
    return new Response(JSON.stringify(out), { headers: { "content-type": "application/json" } });
  }
};`;

const QUEUE_PRODUCER = `
export default {
  async fetch(req, env) {
    await env.MY_Q.send(await req.json());
    return new Response("ok");
  },
};`;

/** @param {string} ns @param {string} worker @param {string} key @param {string} value */
async function setWorkerSecret(ns, worker, key, value) {
  const res = await adminFetch(`/ns/${ns}/worker/${worker}/secrets/${key}`, {
    method: "PUT",
    body: JSON.stringify({ value }),
  });
  const body = await responseJson(res);
  return { status: res.status, body };
}

/** @param {string} ns @param {string} key @param {string} value */
async function setNsSecret(ns, key, value) {
  const res = await adminFetch(`/ns/${ns}/secrets/${key}`, {
    method: "PUT",
    body: JSON.stringify({ value }),
  });
  const body = await responseJson(res);
  return { status: res.status, body };
}

/** @param {string} ns @param {string} worker */
async function envOf(ns, worker) {
  const res = await gatewayFetch(ns, `/${worker}/`);
  assert.equal(res.status, 200, "worker fetch failed");
  return res.json();
}

test("reserved namespace delimiter injection is rejected before secret writes", async () => {
  const injectedNs = "__platform__:platform-demo";
  const res = await adminFetch(`/ns/${injectedNs}/secrets/API_KEY`, {
    method: "PUT",
    body: JSON.stringify({ value: "should-not-write" }),
  });
  assert.equal(res.status, 400);
  assert.equal((await responseJson(res)).error, "invalid_namespace");
  assert.deepEqual(redisHGetAll("secrets:__platform__:platform-demo"), {});
});

test("worker secret: PUT on active worker bumps version and takes effect immediately", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS, vars: { MODE: "prod" } });

  const { status, body } = await setWorkerSecret(ns, "echo", "API_KEY", "sk_123");
  assert.equal(status, 200);
  assert.equal(body.set, true);
  assert.equal(body.previousVersion, "v1");
  assert.equal(body.version, "v2");

  const env = await envOf(ns, "echo");
  assert.equal(env.API_KEY, "sk_123");
  assert.equal(env.MODE, "prod", "vars survive secret bump");
});

test("worker secret: PUT without active version just stores; first deploy picks it up", async () => {
  const ns = uniqueNs("sec");
  // No deploy yet — pre-deploy set.
  const { status, body } = await setWorkerSecret(ns, "pending", "PRE", "preset");
  assert.equal(status, 200);
  assert.equal(body.set, true);
  assert.ok(/next load or deploy/i.test(body.note), `note should describe deferred load: ${body.note}`);
  assert.equal(body.version, undefined, "no version bump without active worker");

  // Pre-deploy PUT must not burn version numbers — first real deploy
  // should land as v1, not v2+. Multiple pre-deploy PUTs compound this.
  await setWorkerSecret(ns, "pending", "PRE2", "preset2");
  await setWorkerSecret(ns, "pending", "PRE3", "preset3");
  const firstDeploy = await adminPost(`/ns/${ns}/worker/pending/deploy`, { code: ENV_HARNESS });
  assert.equal(firstDeploy.json.version, "v1", "first deploy must start at v1 despite 3 pre-deploy PUTs");
  await adminPost(`/ns/${ns}/worker/pending/promote`, { version: "v1" });

  const env = await envOf(ns, "pending");
  assert.equal(env.PRE, "preset");
  assert.equal(env.PRE2, "preset2");
  assert.equal(env.PRE3, "preset3");
});

test("worker secret: DELETE on active worker bumps version", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });
  await setWorkerSecret(ns, "echo", "TO_DELETE", "goodbye");

  const res = await adminFetch(`/ns/${ns}/worker/echo/secrets/TO_DELETE`, { method: "DELETE" });
  const body = await responseJson(res);
  assert.equal(res.status, 200);
  assert.equal(body.deleted, true);
  assert.ok(body.version, "delete should return new version when worker is active");

  const env = await envOf(ns, "echo");
  assert.equal(env.TO_DELETE, undefined);
});

test("worker secret: DELETE on absent key is a noop", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });
  const before = await adminGet(`/ns/${ns}/worker/echo/versions`);
  const activeBefore = before.json.versions.find((/** @type {any} */ v) => v.active).version;

  const res = await adminFetch(`/ns/${ns}/worker/echo/secrets/NEVER_SET`, { method: "DELETE" });
  const body = await responseJson(res);
  assert.equal(res.status, 200);
  assert.equal(body.deleted, false);

  const after = await adminGet(`/ns/${ns}/worker/echo/versions`);
  const activeAfter = after.json.versions.find((/** @type {any} */ v) => v.active).version;
  assert.equal(activeBefore, activeAfter, "no-op delete must not bump version");
});

test("worker secret: empty string is a distinct, defined value (not a delete)", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });

  const { status, body } = await setWorkerSecret(ns, "echo", "FEATURE_FLAG", "");
  assert.equal(status, 200);
  assert.equal(body.set, true);

  const env = await envOf(ns, "echo");
  assert.equal(env.FEATURE_FLAG, "", "empty-string secret must reach worker env as ''");
  assert.ok("FEATURE_FLAG" in env, "key must be present (empty string ≠ unset)");
});

test("ns secret: shared across all workers in the ns, no bump", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "one", { code: ENV_HARNESS });
  await deployAndPromote(ns, "two", { code: ENV_HARNESS });

  const { status, body } = await setNsSecret(ns, "SHARED", "across_both");
  assert.equal(status, 200);
  assert.equal(body.set, true);
  assert.equal(body.version, undefined, "ns secrets do not bump versions");

  // New worker deploy → natural cold-load reads ns secret.
  await deployAndPromote(ns, "three", { code: ENV_HARNESS });
  const env = await envOf(ns, "three");
  assert.equal(env.SHARED, "across_both");
});

test("override order: vars < ns secrets < worker secrets", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", {
    code: ENV_HARNESS,
    vars: { KEY: "from_var" },
  });

  await setNsSecret(ns, "KEY", "from_ns");
  // Trigger cold-load by touching worker secrets (unrelated key).
  await setWorkerSecret(ns, "echo", "TRIGGER", "x");
  let env = await envOf(ns, "echo");
  assert.equal(env.KEY, "from_ns", "ns secret should shadow var");

  await setWorkerSecret(ns, "echo", "KEY", "from_worker");
  env = await envOf(ns, "echo");
  assert.equal(env.KEY, "from_worker", "worker secret should shadow ns secret");
});

test("secret list hides values, returns sorted keys only", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });
  await setWorkerSecret(ns, "echo", "B_KEY", "b");
  await setWorkerSecret(ns, "echo", "A_KEY", "a");

  const res = await adminGet(`/ns/${ns}/worker/echo/secrets`);
  assert.equal(res.status, 200);
  assert.deepEqual(res.json.keys, ["A_KEY", "B_KEY"]);
  // Body shouldn't contain either value.
  const raw = JSON.stringify(res.json);
  assert.ok(!raw.includes('"a"') && !raw.includes('"b"'), "list must not leak values");
});

test("invalid key grammar is rejected 400", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });

  const res = await adminFetch(`/ns/${ns}/worker/echo/secrets/kebab-case`, {
    method: "PUT",
    body: JSON.stringify({ value: "x" }),
  });
  assert.equal(res.status, 400);
});

test("value size limit (64 KiB) enforced by utf-8 byte count, not char count", async () => {
  const ns = uniqueNs("sec");
  await deployAndPromote(ns, "echo", { code: ENV_HARNESS });

  const asciiOversized = "a".repeat(64 * 1024 + 1);
  const r1 = await adminFetch(`/ns/${ns}/worker/echo/secrets/BIG`, {
    method: "PUT",
    body: JSON.stringify({ value: asciiOversized }),
  });
  assert.equal(r1.status, 400, "ascii oversized must reject");

  // 30 KiB CJK chars × 3 bytes/char > 64 KiB, but char count is under —
  // a char-count check would let this through, byte-count must reject.
  const cjkOversized = "中".repeat(30 * 1024);
  assert.ok(cjkOversized.length < 64 * 1024);
  assert.ok(Buffer.byteLength(cjkOversized, "utf8") > 64 * 1024);
  const r2 = await adminFetch(`/ns/${ns}/worker/echo/secrets/CJK`, {
    method: "PUT",
    body: JSON.stringify({ value: cjkOversized }),
  });
  assert.equal(r2.status, 400, "cjk oversized by bytes must reject");
});

test("concurrent secret PUT + promote: neither loses the other", async () => {
  // Regression: without the WATCH/retry, the secret path COPYs a stale
  // activeVersion and promotes it, silently rolling code back.
  const CODE_V1 = `export default { fetch(_, env) { return new Response(JSON.stringify({tag:"v1",SECRET:env.SECRET||null})); } };`;
  const CODE_V2 = `export default { fetch(_, env) { return new Response(JSON.stringify({tag:"v2",SECRET:env.SECRET||null})); } };`;

  for (let i = 0; i < 20; i++) {
    const ns = uniqueNs("race");
    await deployAndPromote(ns, "race", { code: CODE_V1 });
    const d = await adminPost(`/ns/${ns}/worker/race/deploy`, { code: CODE_V2 });
    assert.equal(d.status, 201);

    const [promoteRes, secretRes] = await Promise.all([
      adminPost(`/ns/${ns}/worker/race/promote`, { version: d.json.version }),
      adminFetch(`/ns/${ns}/worker/race/secrets/SECRET`, {
        method: "PUT",
        body: JSON.stringify({ value: `iter-${i}` }),
      }).then(async (r) => ({ ok: r.ok, json: await responseJson(r) })),
    ]);
    assert.ok(promoteRes.ok, `iter ${i}: promote failed`);
    assert.ok(secretRes.ok, `iter ${i}: secret PUT failed`);

    const envRes = await gatewayFetch(ns, `/race/`);
    assert.equal(envRes.status, 200);
    const envJson = await responseJson(envRes);
    assert.equal(envJson.tag, "v2", `iter ${i}: code rolled back to v1 — race fix regressed`);
    assert.equal(envJson.SECRET, `iter-${i}`, `iter ${i}: secret missing`);
  }
});

test("worker secret bump rewrites cron meta.version and scheduler fires the bumped version", async () => {
  const ns = uniqueNs("sec-cron");
  const worker = "cronrec";
  const code = `
  let last = null;
  export default {
    async fetch() {
      return new Response(JSON.stringify(last), {
        headers: { "content-type": "application/json" },
      });
    },
    async scheduled(_controller, env) {
      last = { secret: env.API_KEY ?? null };
    },
  };`;

  const d1 = await adminPost(`/ns/${ns}/worker/${worker}/deploy`, {
    code,
    crons: [{ cron: "0 0 1 1 *", timezone: "UTC" }],
  });
  assertStatus(d1, 201, "d1");
  const p1 = await adminPost(`/ns/${ns}/worker/${worker}/promote`, { version: d1.json.version });
  assertStatus(p1, 200, "p1");

  // Warm v1 into runtime cache before the secret bump. If scheduler later
  // targets the stale id, it'll execute this cached env without API_KEY.
  const warm = await gatewayFetch(ns, `/${worker}/`);
  assert.equal(warm.status, 200);

  const secretPut = await setWorkerSecret(ns, worker, "API_KEY", "cron-secret");
  assert.equal(secretPut.status, 200);
  assert.equal(secretPut.body.previousVersion, "v1");
  assert.equal(secretPut.body.version, "v2");

  const cronHash = redisHGetAll(`crons:${ns}:${worker}`);
  const meta = redisHashJsonField(cronHash, "__meta__", `crons:${ns}:${worker} __meta__`);
  assert.equal(meta.version, "v2", "cron meta.version must follow the bumped active version");

  const id = cronId("0 0 1 1 *", "UTC");
  const entry = redisHashJsonField(cronHash, id, `crons:${ns}:${worker} ${id}`);
  const ref = `${ns}:${worker}:${id}:${entry.gen}`;
  await waitForCurrentSlotFixtureWindow();
  const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
  redisSAdd(`cron-slot:${currentSlot}`, ref);
  composeRestart("scheduler");

  await waitUntil("scheduler to fire bumped cron version", async () => {
    const res = await gatewayFetch(ns, `/${worker}/`);
    if (res.status !== 200) return false;
    const body = await responseJson(res);
    return body && body.secret === "cron-secret";
  }, { timeoutMs: 15_000, intervalMs: 500 });
});

test("worker secret bump rewrites queue-consumer version and scheduler delivers to the bumped version", async () => {
  const ns = uniqueNs("sec-queue");
  const consumer = "consumer";
  const queue = "orders";
  const consumerCode = `
  let last = null;
  export default {
    async fetch() {
      return new Response(JSON.stringify(last), {
        headers: { "content-type": "application/json" },
      });
    },
    async queue(batch, env) {
      for (const msg of batch.messages) {
        last = { secret: env.API_KEY ?? null, body: msg.body };
        msg.ack();
      }
    },
  };`;

  const d1 = await adminPost(`/ns/${ns}/worker/${consumer}/deploy`, {
    code: consumerCode,
    queueConsumers: [{ queue, maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 }],
  });
  assertStatus(d1, 201, "d1");
  const p1 = await adminPost(`/ns/${ns}/worker/${consumer}/promote`, { version: d1.json.version });
  assertStatus(p1, 200, "p1");

  // Warm v1 into runtime cache before the bump.
  const warm = await gatewayFetch(ns, `/${consumer}/`);
  assert.equal(warm.status, 200);

  const secretPut = await setWorkerSecret(ns, consumer, "API_KEY", "queue-secret");
  assert.equal(secretPut.status, 200);
  assert.equal(secretPut.body.previousVersion, "v1");
  assert.equal(secretPut.body.version, "v2");

  const registry = redisHGetAll(`queue-consumer:${ns}:${queue}`);
  assert.equal(
    registry.version,
    "v2",
    "queue consumer registry must point scheduler at the bumped active version"
  );
  // Scheduler owns an in-memory consumer registry and refreshes it from Redis
  // on QUEUE_RECONCILE_MS; wait one reconcile window before producing so this
  // test pins the bumped-version contract rather than racing the old snapshot.
  await delay(2500);

  const producerVersion = await deployAndPromote(ns, "producer", {
    code: QUEUE_PRODUCER,
    bindings: { MY_Q: { type: "queue", id: queue } },
  });
  const send = runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "producer", producerVersion),
    "content-type": "application/json",
  }, { hello: "queue" });
  assertStatus(send, 200, "send");

  await waitUntil("scheduler to deliver to bumped queue consumer version", async () => {
    const res = await gatewayFetch(ns, `/${consumer}/`);
    if (res.status !== 200) return false;
    const body = await responseJson(res);
    return body && body.secret === "queue-secret" && body.body?.hello === "queue";
  }, { timeoutMs: 30_000, intervalMs: 1_000 });
});
