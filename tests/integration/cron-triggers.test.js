// Cron triggers end-to-end: deploy → promote materialization →
// runtime /_scheduled → scheduler sidecar fire.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminFetch,
  adminPost,
  assertStatus,
  composeRestart,
  composeScale,
  cronId,
  delay,
  deployAndPromote,
  gatewayWorkerId,
  readMeta,
  runtimeDispatchPost,
  runtimeInternalGetWithHeaders,
  runtimeInternalPost,
  schedulerMetricsText,
  sh,
  uniqueNs,
  waitUntil,
  waitForCurrentSlotFixtureWindow,
  withServiceStopped,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import {
  redisCommandCalls,
  redisDel,
  redisExpireTime,
  redisGet,
  redisHGetAll,
  redisHashJsonField,
  redisHSet,
  redisKeys,
  redisSAdd,
  redisScriptFlush,
  redisSMembers,
} from "./helpers/redis.js";

setupIntegrationSuite();

const NOP = `export default { fetch() { return new Response('ok'); }, scheduled() {} };`;

test("deploy stores crons in __meta__.crons, defaulting timezone to UTC", async () => {
  const ns = uniqueNs("cron");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: NOP,
    crons: [{ cron: "*/5 * * * *" }, { cron: "0 0 * * *", timezone: "UTC" }],
  });
  assert.equal(d.status, 201);

  const meta = readMeta(ns, "w", d.json.version);
  assert.deepEqual(meta.crons, [
    { cron: "*/5 * * * *", timezone: "UTC" },
    { cron: "0 0 * * *", timezone: "UTC" },
  ]);
});

test("deploy preserves per-entry timezone", async () => {
  const ns = uniqueNs("cron");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: NOP,
    crons: [{ cron: "0 9 * * 1-5", timezone: "Asia/Shanghai" }],
  });
  assert.equal(d.status, 201);
  const meta = readMeta(ns, "w", d.json.version);
  assert.deepEqual(meta.crons, [{ cron: "0 9 * * 1-5", timezone: "Asia/Shanghai" }]);
});

test("deploy without crons leaves __meta__.crons unset", async () => {
  const ns = uniqueNs("cron");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, { code: NOP });
  assert.equal(d.status, 201);
  const meta = readMeta(ns, "w", d.json.version);
  assert.equal(meta.crons, undefined);
});

test("deploy rejects invalid cron expression with 400", async () => {
  const ns = uniqueNs("cron");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: NOP,
    crons: [{ cron: "not a cron" }],
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /invalid expression/);
});

test("deploy rejects unknown timezone with 400", async () => {
  const ns = uniqueNs("cron");
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: NOP,
    crons: [{ cron: "*/5 * * * *", timezone: "Mars/Olympus" }],
  });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /unknown timezone/);
});

test("deploy rejects >10 cron entries with 400", async () => {
  const ns = uniqueNs("cron");
  const many = Array.from({ length: 11 }, (_, i) => ({ cron: `${i} * * * *` }));
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, { code: NOP, crons: many });
  assert.equal(d.status, 400);
  assert.match(d.json.message, /max 10/);
});

/** @param {string} ns @param {string} name @param {Array<{ cron: string, timezone?: string }>} crons */
async function deployAndPromoteWithCrons(ns, name, crons) {
  const d = await adminPost(`/ns/${ns}/worker/${name}/deploy`, { code: NOP, crons });
  if (!d.ok) throw new Error(`deploy failed: ${d.status} ${JSON.stringify(d.json)}`);
  const p = await adminPost(`/ns/${ns}/worker/${name}/promote`, { version: d.json.version });
  if (!p.ok) throw new Error(`promote failed: ${p.status} ${JSON.stringify(p.json)}`);
  return d.json.version;
}

/** @param {string} ns @param {string} field */
function cronHashJsonField(ns, field) {
  return redisHashJsonField(
    redisHGetAll(`crons:${ns}:w`),
    field,
    `crons:${ns}:w ${field}`,
  );
}

test("promote materializes crons hash + cron-slot bucket refs", async () => {
  const ns = uniqueNs("cron");
  const v = await deployAndPromoteWithCrons(ns, "w", [
    { cron: "*/5 * * * *", timezone: "UTC" },
  ]);

  const id = cronId("*/5 * * * *", "UTC");
  const hash = redisHGetAll(`crons:${ns}:w`);
  assert.ok(hash.__meta__, "expected __meta__ field");
  assert.ok(
    redisSMembers("cron:index:workers").includes(`crons:${ns}:w`),
    "expected cron worker discovery index to include promoted worker"
  );
  const meta = redisHashJsonField(hash, "__meta__", `crons:${ns}:w __meta__`);
  assert.deepEqual(meta, { version: v });
  assert.ok(hash[id], `expected entry for cron_id ${id}`);
  const entry = redisHashJsonField(hash, id, `crons:${ns}:w ${id}`);
  assert.equal(entry.cron, "*/5 * * * *");
  assert.equal(entry.timezone, "UTC");
  assert.equal(typeof entry.gen, "number");
  assert.equal(entry.gen, 1024);
  assert.equal(redisGet(`cron:seq:${ns}:w`), "1024");

  const ref = `${ns}:w:${id}:${entry.gen}`;
  const slotKeys = redisKeys("cron-slot:*");
  let foundIn = null;
  for (const k of slotKeys) {
    if (redisSMembers(k).includes(ref)) { foundIn = k; break; }
  }
  assert.ok(foundIn, `expected "${ref}" in some cron-slot:* set`);

  // 10min grace is well past tick lookback (60s) and sweep cycle (5min);
  // bounds orphans if advance_ref ever crashes mid-SREM.
  const slotMs = Number(foundIn.split(":")[1]);
  assert.equal(redisExpireTime(foundIn), Math.floor(slotMs / 1000) + 600);
});

test("promote with no crons clears prior hash entries", async () => {
  const ns = uniqueNs("cron");
  await deployAndPromoteWithCrons(ns, "w", [{ cron: "*/5 * * * *" }]);
  assert.ok(redisHGetAll(`crons:${ns}:w`).__meta__);

  await deployAndPromoteWithCrons(ns, "w", []);
  const hash = redisHGetAll(`crons:${ns}:w`);
  assert.deepEqual(hash, {}, "expected crons hash fully cleared");
  assert.equal(
    redisSMembers("cron:index:workers").includes(`crons:${ns}:w`),
    false,
    "cron worker discovery index must drop workers with no crons"
  );
  assert.equal(redisGet(`cron:seq:${ns}:w`), "1024");
});

test("cron generation survives clear and whole-worker delete", async () => {
  const ns = uniqueNs("cronseq");
  const cron = { cron: "*/5 * * * *", timezone: "UTC" };
  const id = cronId(cron.cron, cron.timezone);

  await deployAndPromoteWithCrons(ns, "w", [cron]);
  const first = redisHashJsonField(
    redisHGetAll(`crons:${ns}:w`), id, `crons:${ns}:w ${id}`
  );
  await deployAndPromoteWithCrons(ns, "w", []);
  await deployAndPromoteWithCrons(ns, "w", [cron]);
  const afterClear = redisHashJsonField(
    redisHGetAll(`crons:${ns}:w`), id, `crons:${ns}:w ${id}`
  );
  assert.equal(afterClear.gen, first.gen + 1);

  const deleted = await adminFetch(`/ns/${ns}/worker/w/delete`, { method: "POST" });
  assertStatus(deleted, 200, "whole-worker delete");
  assert.equal(redisGet(`cron:seq:${ns}:w`), String(afterClear.gen));

  await deployAndPromoteWithCrons(ns, "w", [cron]);
  const afterDelete = redisHashJsonField(
    redisHGetAll(`crons:${ns}:w`), id, `crons:${ns}:w ${id}`
  );
  assert.equal(afterDelete.gen, afterClear.gen + 1);
});

test("whole-worker delete without an allocator recreates Cron at the generation epoch", async () => {
  const ns = uniqueNs("cronreserveddelete");
  const cron = { cron: "*/5 * * * *", timezone: "UTC" };
  const id = cronId(cron.cron, cron.timezone);
  const version = await deployAndPromoteWithCrons(ns, "w", []);

  redisHSet(`crons:${ns}:w`, {
    __meta__: JSON.stringify({ version, seq: 7 }),
    [id]: JSON.stringify({ ...cron, gen: 7 }),
  });

  const deleted = await adminFetch(`/ns/${ns}/worker/w/delete`, { method: "POST" });
  assertStatus(deleted, 200, "whole-worker delete without allocator");
  assert.equal(redisGet(`cron:seq:${ns}:w`), null);

  await deployAndPromoteWithCrons(ns, "w", [cron]);
  const recreated = redisHashJsonField(
    redisHGetAll(`crons:${ns}:w`), id, `crons:${ns}:w ${id}`
  );
  assert.equal(recreated.gen, 1024);
});

test("promote preserves gen for unchanged cron across versions (no reschedule churn)", async () => {
  const ns = uniqueNs("cron");
  const v1 = await deployAndPromoteWithCrons(ns, "w", [
    { cron: "*/5 * * * *", timezone: "UTC" },
  ]);
  const id = cronId("*/5 * * * *", "UTC");
  const before = cronHashJsonField(ns, id);

  const v2 = await deployAndPromoteWithCrons(ns, "w", [
    { cron: "*/5 * * * *", timezone: "UTC" },
  ]);
  assert.notEqual(v1, v2);
  const after = cronHashJsonField(ns, id);
  assert.equal(after.gen, before.gen, "gen must be preserved for kept crons");
  const meta = cronHashJsonField(ns, "__meta__");
  assert.equal(meta.version, v2, "meta.version follows currently-active version");
});

test("promote HDEL's removed cron entries; added entries get new gen", async () => {
  const ns = uniqueNs("cron");
  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "*/5 * * * *", timezone: "UTC" },
  ]);
  const oldId = cronId("*/5 * * * *", "UTC");

  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "0 0 * * *", timezone: "UTC" },
  ]);
  const hash = redisHGetAll(`crons:${ns}:w`);
  assert.equal(hash[oldId], undefined, "old cron entry must be HDEL'd");
  const newId = cronId("0 0 * * *", "UTC");
  assert.ok(hash[newId], "new cron entry must be present");
});

// Isolate-local state stands in for KV so the test doesn't need a binding.
const SCHEDULED_RECORDER = `
let last = null;
export default {
  async fetch() {
    return new Response(JSON.stringify(last), {
      headers: { "content-type": "application/json" },
    });
  },
  async scheduled(controller) {
    last = {
      scheduledTime: controller.scheduledTime,
      cron: controller.cron,
    };
  },
};`;

const SCHEDULED_COUNTER = `
let events = [];
export default {
  async fetch() {
    return Response.json({ count: events.length, events });
  },
  async scheduled(controller) {
    events.push({
      scheduledTime: controller.scheduledTime,
      cron: controller.cron,
    });
  },
};`;

const SCHEDULED_THROWER = `
export default {
  fetch() { return new Response("ok"); },
  async scheduled() {
    throw new Error("synthetic handler failure");
  },
};`;

test("runtime POST /_scheduled invokes scheduled() and returns outcome:ok", async () => {
  const ns = uniqueNs("sched");
  const version = await deployAndPromote(ns, "w", { code: SCHEDULED_RECORDER });

  const scheduledTime = 1_800_000_000_000;
  const res = runtimeDispatchPost("/_scheduled", {
    "x-worker-id": gatewayWorkerId(ns, "w", version),
  }, { scheduledTime, cron: "*/5 * * * *" });

  assert.equal(res.status, 200, res.body);
  const parsed = responseJson(res);
  assert.equal(parsed.outcome, "ok");
  assert.equal(typeof parsed.duration_ms, "number");
});

test("runtime POST /_scheduled: controller carries scheduledTime + cron through to handler", async () => {
  const ns = uniqueNs("sched");
  const version = await deployAndPromote(ns, "w", { code: SCHEDULED_RECORDER });

  const scheduledTime = 1_800_000_060_000;
  const cron = "0 9 * * *";
  const fire = runtimeDispatchPost("/_scheduled", {
    "x-worker-id": gatewayWorkerId(ns, "w", version),
  }, { scheduledTime, cron });
  assert.equal(fire.status, 200);

  const read = runtimeInternalPost("/", {
    "x-worker-id": gatewayWorkerId(ns, "w", version),
  }, "");
  assert.equal(read.status, 200);
  const recorded = responseJson(read);
  assert.equal(recorded.scheduledTime, scheduledTime);
  assert.equal(recorded.cron, cron);
});

test("runtime POST /_scheduled: handler throw surfaces as 200 outcome:error (not 5xx)", async () => {
  const ns = uniqueNs("sched");
  const version = await deployAndPromote(ns, "w", { code: SCHEDULED_THROWER });

  const res = runtimeDispatchPost("/_scheduled", {
    "x-worker-id": gatewayWorkerId(ns, "w", version),
  }, { scheduledTime: Date.now(), cron: "*/5 * * * *" });

  assert.equal(res.status, 200, "5xx would cause sidecar retry, breaking CF semantics");
  const parsed = responseJson(res);
  assert.equal(parsed.outcome, "error");
  // With service_binding_extra_handlers, scheduled() returns
  // { outcome: "exception" } instead of throwing, so the runtime
  // can't always extract the original error message. Accept either
  // the original text or the generic fallback.
  assert.ok(parsed.error, "error field must be non-empty");
});

test("runtime POST /_scheduled rejects malformed body with 400", async () => {
  const ns = uniqueNs("sched");
  const version = await deployAndPromote(ns, "w", { code: SCHEDULED_RECORDER });

  const res = runtimeDispatchPost("/_scheduled", {
    "x-worker-id": gatewayWorkerId(ns, "w", version),
  }, { scheduledTime: "not a number", cron: "*/5 * * * *" });
  assert.equal(res.status, 400);
});

test("scheduler: ref in current bucket is fired, pre-advanced, and reaches worker.scheduled()", async () => {
  const ns = uniqueNs("schsidecar");
  // Cron whose natural next-fire is distant so promote doesn't put the
  // ref in the current slot; the test seeds it there explicitly.
  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "0 0 1 1 *", timezone: "UTC" },
  ]);

  // Swap the NOP for SCHEDULED_RECORDER; same cron, gen is preserved.
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: SCHEDULED_RECORDER,
    crons: [{ cron: "0 0 1 1 *", timezone: "UTC" }],
  });
  assert.ok(d.ok);
  const p = await adminPost(`/ns/${ns}/worker/w/promote`, { version: d.json.version });
  assert.ok(p.ok);
  const activeVersion = d.json.version;

  const id = cronId("0 0 1 1 *", "UTC");
  const hash = redisHGetAll(`crons:${ns}:w`);
  const entry = redisHashJsonField(hash, id, `crons:${ns}:w ${id}`);
  const ref = `${ns}:w:${id}:${entry.gen}`;

  await waitForCurrentSlotFixtureWindow();
  const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
  redisSAdd(`cron-slot:${currentSlot}`, ref);
  redisScriptFlush();
  const evalshaBefore = redisCommandCalls("evalsha");
  const scriptLoadsBefore = redisCommandCalls("script load");
  composeRestart("scheduler");

  await waitUntil("scheduler to fire the seeded ref", async () => {
    const res = runtimeInternalGetWithHeaders("/", {
      "x-worker-id": gatewayWorkerId(ns, "w", activeVersion),
    });
    if (res.status !== 200) return false;
    if (res.body === "null") return false;
    const recorded = responseJson(res);
    return recorded && recorded.scheduledTime === currentSlot;
  }, { timeoutMs: 15_000, intervalMs: 500 });

  const remainingInCurrent = redisSMembers(`cron-slot:${currentSlot}`);
  assert.equal(remainingInCurrent.includes(ref), false,
    "ref must have been SREM'd from current slot during pre-advance");

  const nextSlotKey = redisKeys("cron-slot:*").find((k) => redisSMembers(k).includes(ref));
  assert.ok(nextSlotKey, "ref must have been advanced into some future slot");
  const nextSlotMs = Number(nextSlotKey.split(":")[1]);
  assert.equal(redisExpireTime(nextSlotKey), Math.floor(nextSlotMs / 1000) + 600,
    "advance_ref must EXPIREAT the new slot");
  assert.ok(redisCommandCalls("evalsha") > evalshaBefore);
  assert.ok(redisCommandCalls("script load") > scriptLoadsBefore);
});

test("scheduler: stranded ref (slotMs < currentSlot) is advanced without firing — CF 'skip missed' semantics", async () => {
  const ns = uniqueNs("cronstrand");
  // Swap the NOP for SCHEDULED_RECORDER so we can assert the handler
  // did NOT run. Distant cron so promote doesn't naturally seed the
  // current bucket.
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: SCHEDULED_RECORDER,
    crons: [{ cron: "0 0 1 1 *", timezone: "UTC" }],
  });
  assert.ok(d.ok);
  const p = await adminPost(`/ns/${ns}/worker/w/promote`, { version: d.json.version });
  assert.ok(p.ok);
  const activeVersion = d.json.version;

  const id = cronId("0 0 1 1 *", "UTC");
  const entry = cronHashJsonField(ns, id);
  const ref = `${ns}:w:${id}:${entry.gen}`;

  // Directly seed the prior slot, then restart scheduler so its startup
  // immediate tick visits the ref without waiting for the next minute boundary.
  const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
  const pastSlot = currentSlot - 60_000;
  redisSAdd(`cron-slot:${pastSlot}`, ref);
  composeRestart("scheduler");

  await waitUntil("scheduler to SREM stranded ref from past slot", async () => {
    return !redisSMembers(`cron-slot:${pastSlot}`).includes(ref);
  }, { timeoutMs: 15_000, intervalMs: 500 });

  // SCHEDULED_RECORDER leaves `last = null` unless scheduled() ran.
  // CF semantics: a ref whose slot lies in the past is dropped, not fired retroactively.
  const res = runtimeInternalGetWithHeaders("/", {
    "x-worker-id": gatewayWorkerId(ns, "w", activeVersion),
  });
  assert.equal(res.status, 200);
  assert.equal(res.body, "null", "stranded ref must NOT invoke scheduled()");
});

test("scheduler: invalid persisted cron ref is removed without firing", async () => {
  const ns = uniqueNs("cronbadnext");
  const cron = "0 0 1 1 *";
  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: SCHEDULED_RECORDER,
    crons: [{ cron, timezone: "UTC" }],
  });
  assertStatus(d, 201, "invalid persisted cron fixture deploy");
  const p = await adminPost(`/ns/${ns}/worker/w/promote`, { version: d.json.version });
  assertStatus(p, 200, "invalid persisted cron fixture promote");

  const id = cronId(cron, "UTC");
  const entry = cronHashJsonField(ns, id);
  const ref = `${ns}:w:${id}:${entry.gen}`;
  redisHSet(`crons:${ns}:w`, {
    [id]: JSON.stringify({ ...entry, timezone: "Mars/Olympus" }),
  });

  await waitForCurrentSlotFixtureWindow();
  const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
  redisSAdd(`cron-slot:${currentSlot}`, ref);
  composeRestart("scheduler");

  await waitUntil("scheduler to remove invalid persisted cron ref", async () => {
    return !redisSMembers(`cron-slot:${currentSlot}`).includes(ref);
  }, { timeoutMs: 15_000, intervalMs: 500 });

  const res = runtimeInternalGetWithHeaders("/", {
    "x-worker-id": gatewayWorkerId(ns, "w", d.json.version),
  });
  assert.equal(res.status, 200);
  assert.equal(res.body, "null", "invalid persisted cron must NOT invoke scheduled()");
});

test("scheduler: ref with mismatched gen is SREM'd from bucket without firing", async () => {
  const ns = uniqueNs("schstale");
  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "0 0 1 1 *", timezone: "UTC" },
  ]);
  const id = cronId("0 0 1 1 *", "UTC");

  await waitForCurrentSlotFixtureWindow();
  const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
  const bogusRef = `${ns}:w:${id}:99999`;
  redisSAdd(`cron-slot:${currentSlot}`, bogusRef);
  composeRestart("scheduler");

  await waitUntil("scheduler to SREM the stale ref", async () => {
    const members = redisSMembers(`cron-slot:${currentSlot}`);
    return !members.includes(bogusRef);
  }, { timeoutMs: 15_000, intervalMs: 500 });
});

test("scheduler replicas: due cron ref is claimed by only one replica", async () => {
  const ns = uniqueNs("schreplica");
  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "0 0 1 1 *", timezone: "UTC" },
  ]);

  const d = await adminPost(`/ns/${ns}/worker/w/deploy`, {
    code: SCHEDULED_COUNTER,
    crons: [{ cron: "0 0 1 1 *", timezone: "UTC" }],
  });
  assertStatus(d, 201, "replica cron deploy");
  const p = await adminPost(`/ns/${ns}/worker/w/promote`, { version: d.json.version });
  assertStatus(p, 200, "replica cron promote");
  const activeVersion = d.json.version;

  const id = cronId("0 0 1 1 *", "UTC");
  const entry = cronHashJsonField(ns, id);
  const ref = `${ns}:w:${id}:${entry.gen}`;

  await withServiceStopped("scheduler", async () => {
    await waitForCurrentSlotFixtureWindow();
    const currentSlot = Math.floor(Date.now() / 60_000) * 60_000;
    redisSAdd(`cron-slot:${currentSlot}`, ref);

    composeScale("scheduler", 2);

    const workerId = gatewayWorkerId(ns, "w", activeVersion);
    await waitUntil("one scheduler replica to fire the seeded ref", async () => {
      const res = runtimeInternalGetWithHeaders("/", { "x-worker-id": workerId });
      if (res.status !== 200) return false;
      const snap = responseJson(res);
      return snap.count === 1 && snap.events[0]?.scheduledTime === currentSlot;
    }, { timeoutMs: 20_000, intervalMs: 500 });

    await delay(2_000);
    const finalRes = runtimeInternalGetWithHeaders("/", { "x-worker-id": workerId });
    assert.equal(finalRes.status, 200, finalRes.body);
    const finalSnap = responseJson(finalRes);
    assert.equal(finalSnap.count, 1, `cron ref must fire once across replicas: ${finalRes.body}`);
    assert.equal(
      redisSMembers(`cron-slot:${currentSlot}`).includes(ref),
      false,
      "claimed ref must be removed from the due slot"
    );
  });
});

// sweep() recovers bucket refs from the crons hash after missed ticks /
// bucket loss. It runs once on boot for every scheduler replica; the Redis
// SADD/EXPIREAT shape must stay idempotent when two replicas start together.
test("scheduler replicas: sweep() re-seeds cron-slot buckets from the crons hash on boot", async () => {
  const ns = uniqueNs("cronsweep");
  await deployAndPromoteWithCrons(ns, "w", [
    { cron: "0 0 1 1 *", timezone: "UTC" },
  ]);

  const id = cronId("0 0 1 1 *", "UTC");
  const entry = cronHashJsonField(ns, id);
  const ref = `${ns}:w:${id}:${entry.gen}`;

  const beforeSlots = redisKeys("cron-slot:*");
  let originalSlot = null;
  for (const k of beforeSlots) {
    if (redisSMembers(k).includes(ref)) { originalSlot = k; break; }
  }
  assert.ok(originalSlot, `ref "${ref}" must be in some bucket post-promote`);

  await withServiceStopped("scheduler", async () => {
    // Simulate bucket loss (redis outage, accidental FLUSHDB of slots, etc).
    for (const k of beforeSlots) redisDel(k);
    redisDel("cron:index:workers");
    redisDel("cron:index:workers:backfilled");
    assert.equal(
      redisKeys("cron-slot:*").length,
      0,
      "all cron-slot:* sets must be gone before restart"
    );

    composeScale("scheduler", 2);

    await waitUntil("sweep() to re-SADD ref from crons hash", async () => {
      const slots = redisKeys("cron-slot:*");
      for (const k of slots) {
        if (redisSMembers(k).includes(ref)) return true;
      }
      return false;
    }, { timeoutMs: 20_000, intervalMs: 500 });

    const reseededKey = redisKeys("cron-slot:*").find((k) => redisSMembers(k).includes(ref));
    assert.ok(reseededKey, "ref must be reseeded into some bucket");
    assert.ok(
      redisSMembers("cron:index:workers").includes(`crons:${ns}:w`),
      "sweep() must backfill the cron worker discovery index"
    );
    assert.equal(redisGet("cron:index:workers:backfilled"), "1");
    const reseededSlotMs = Number(reseededKey.split(":")[1]);
    assert.equal(redisExpireTime(reseededKey), Math.floor(reseededSlotMs / 1000) + 600,
      "sweep() must EXPIREAT the re-seeded slot");
  });
});

// sweep must continue past per-entry next_fire_ms failures — a single bad
// entry whose iteration order beats the valid ones would otherwise stop
// every other worker's crons from being re-seeded on boot.
test("scheduler sweep(): one bad entry must not block re-seeding the valid ones", async () => {
  const ns = uniqueNs("sweeptol");
  const validCrons = [
    { cron: "0 0 1 1 *", timezone: "UTC" },
    { cron: "0 0 2 1 *", timezone: "UTC" },
    { cron: "0 0 3 1 *", timezone: "UTC" },
  ];
  await deployAndPromoteWithCrons(ns, "w", validCrons);

  const hash = redisHGetAll(`crons:${ns}:w`);
  const expectedRefs = validCrons.map((c) => {
    const id = cronId(c.cron, c.timezone);
    const entry = redisHashJsonField(hash, id, `crons:${ns}:w ${id}`);
    return `${ns}:w:${id}:${entry.gen}`;
  });

  // promote rejects unknown timezones at deploy, but raw HSET is happy.
  // 5 poisoned entries vs 3 good means HashMap iteration nearly always
  // hits a poisoned one first (P(all 3 good first) = 1/C(8,3) ≈ 1.8%).
  const badEntry = JSON.stringify({
    cron: "* * * * *",
    timezone: "Mars/Olympus",
    gen: 999,
  });
  for (let i = 0; i < 5; i++) {
    redisHSet(`crons:${ns}:w`, { [`bad-tz-${i}`]: badEntry });
  }

  // Wipe slot buckets so sweep is the only path that can re-seed.
  for (const k of redisKeys("cron-slot:*")) redisDel(k);

  sh("docker compose restart scheduler", { stdio: "pipe" });

  await waitUntil("sweep to re-seed every valid ref despite bad neighbors", async () => {
    const slots = redisKeys("cron-slot:*");
    const seen = new Set();
    for (const k of slots) {
      for (const m of redisSMembers(k)) seen.add(m);
    }
    return expectedRefs.every((r) => seen.has(r));
  }, { timeoutMs: 25_000, intervalMs: 500 });

  for (const k of redisKeys("cron-slot:*")) {
    for (const m of redisSMembers(k)) {
      assert.ok(
        !m.includes(":bad-tz-"),
        `bad-tz ref must not appear in a slot bucket: ${m} in ${k}`
      );
    }
  }

  // Deterministic backstop: this counter was introduced with the per-entry
  // tolerance code, so it can't even be parsed on a pre-fix scheduler.
  const metrics = schedulerMetricsText();
  const skipMatch = metrics.match(
    /wdl_cron_sweep_entries_skipped_total\b[^\n]*?\s+(\d+(?:\.\d+)?)\s*$/m
  );
  assert.ok(
    skipMatch,
    `expected wdl_cron_sweep_entries_skipped_total in scheduler metrics:\n${metrics}`
  );
  assert.ok(
    Number(skipMatch[1]) >= 5,
    `expected >= 5 skipped entries, observed ${skipMatch[1]} (sweep bailed early?)`
  );
});
