// Admin-only integration tests for route patterns: hosts reconcile +
// promote-with-routes. Gateway-side dispatch is covered in Phase 3.

import { before, test } from "node:test";
import assert from "node:assert/strict";
import {
  adminGet,
  adminPost,
  assertStatus,
  ensureStackUp,
  readMeta,
  resetStack,
  uniqueNs,
} from "./helpers/index.js";
import { redisHGet, redisHGetAll, redisHKeys, redisSMembers } from "./helpers/redis.js";

before(async () => {
  await ensureStackUp();
  await resetStack();
});

/** @param {string[]} [routes] */
function trivialDeploy(routes) {
  const code = `export default { fetch() { return new Response("ok"); } };`;
  return { code, ...(routes ? { routes } : {}) };
}

/** @param {string} raw */
function decodePatternProjection(raw) {
  const parts = raw.split("\t");
  assert.equal(parts.length, 6, raw);
  assert.equal(parts[0], "v2", raw);
  const [, ns, worker, version, kind, value] = parts;
  return { ns, worker, version, kind, value };
}

test("POST /ns/<ns>/hosts: additive, normalized, idempotent", async () => {
  const ns = uniqueNs("hosts-add");
  let res = await adminPost(`/ns/${ns}/hosts`, { hosts: ["Workers.Example:8080", "api.workers.example"] });
  assert.equal(res.status, 200);
  assert.deepEqual(res.json.hosts, ["api.workers.example", "workers.example"]);

  // Resubmit with same (different case/port) → still the normalized set
  res = await adminPost(`/ns/${ns}/hosts`, { hosts: ["WORKERS.example", "api.workers.example"] });
  assert.equal(res.status, 200);
  assert.deepEqual(res.json.hosts, ["api.workers.example", "workers.example"]);
});

test("POST /ns/<ns>/hosts: preserves literal ASCII xn-- labels", async () => {
  const ns = uniqueNs("hosts-alabel");
  const res = await adminPost(`/ns/${ns}/hosts`, {
    hosts: ["XN--pokxncvks.Example"],
  });
  assert.equal(res.status, 200);
  assert.deepEqual(res.json.hosts, ["xn--pokxncvks.example"]);
});

test("POST /ns/<ns>/hosts: full-set replacement (removes hosts not in body)", async () => {
  const ns = uniqueNs("hosts-replace");
  await adminPost(`/ns/${ns}/hosts`, { hosts: ["a.workers.example", "b.workers.example"] });
  const res = await adminPost(`/ns/${ns}/hosts`, { hosts: ["a.workers.example"] });
  assert.equal(res.status, 200);
  assert.deepEqual(res.json.hosts, ["a.workers.example"]);
});

test("POST /ns/<ns>/hosts: rejects platform-domain entries", async () => {
  const ns = uniqueNs("hosts-pd");
  const res = await adminPost(`/ns/${ns}/hosts`, { hosts: ["demo.workers.local"] });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /platform domain/);
});

test("POST /ns/<ns>/hosts: rejects invalid URL authority spellings", async () => {
  const ns = uniqueNs("hosts-authority");
  for (const host of ["user@workers.example", "127.1"]) {
    const res = await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
    assert.equal(res.status, 400);
    assert.match(res.json.message, /invalid host/);
  }
});

test("POST /ns/<ns>/hosts: remove blocked by live pattern", async () => {
  const ns = uniqueNs("hosts-blockrm");
  const host = "blockrm.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/*`])
  ).then(async (d) => {
    assert.equal(d.status, 201);
    const p = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
    assert.equal(p.status, 200);
  });

  // Now try to remove the host — must 409
  const res = await adminPost(`/ns/${ns}/hosts`, { hosts: [] });
  assert.equal(res.status, 409);
  assert.equal(res.json.error, "host_in_use");
  assert.match(res.json.message, /live pattern/);

  // GET still returns the host
  const g = await adminGet(`/ns/${ns}/hosts`);
  assert.deepEqual(g.json.hosts, [host]);
});

test("Deploy stores routes in version meta; promote without host declaration → 403", async () => {
  const ns = uniqueNs("promote-403");
  // No POST /hosts yet.
  const d = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy(["undeclared.workers.example/*"])
  );
  assert.equal(d.status, 201);

  const p = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
  assert.equal(p.status, 403);
  assert.equal(p.json.error, "host_not_declared");
  assert.equal(Object.hasOwn(p.json, "reason"), false);
  assert.match(p.json.message, /not declared/);
});

test("Promote with declaration → writes patterns:<host>", async () => {
  const ns = uniqueNs("promote-ok");
  const host = "ok.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  const d = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/api/*`, `${host}/*`])
  );
  assert.equal(d.status, 201);
  const p = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
  assert.equal(p.status, 200);

  const redisOut = Object.entries(redisHGetAll(`patterns:${host}`))
    .flat()
    .join("\n");
  assert.match(redisOut, /\/api\//);
  assert.match(redisOut, new RegExp(RegExp.escape(`\t${ns}\tapp\tv1\t`)));
});

test("Cross-worker (host, slot) conflict → 409 on second promote", async () => {
  const ns = uniqueNs("conflict");
  const host = "conflict.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });

  // worker-a claims /api/
  const da = await adminPost(
    `/ns/${ns}/worker/worker-a/deploy`,
    trivialDeploy([`${host}/api/*`])
  );
  const pa = await adminPost(`/ns/${ns}/worker/worker-a/promote`, { version: da.json.version });
  assert.equal(pa.status, 200);

  // worker-b tries to claim same slot
  const db = await adminPost(
    `/ns/${ns}/worker/worker-b/deploy`,
    trivialDeploy([`${host}/api/*`])
  );
  const pb = await adminPost(`/ns/${ns}/worker/worker-b/promote`, { version: db.json.version });
  assert.equal(pb.status, 409);
  assert.equal(pb.json.error, "route_conflict");
  assert.match(pb.json.message, /already owned by/);
});

test("Repeat promote of same version is idempotent", async () => {
  const ns = uniqueNs("idempotent");
  const host = "idem.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  const d = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/*`])
  );
  const p1 = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
  assert.equal(p1.status, 200);
  const p2 = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
  assert.equal(p2.status, 200);
});

test("Rollback: promoting an earlier version reverts the route set", async () => {
  const ns = uniqueNs("rollback");
  const host = "rb.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });

  // v1 declares /old/*
  const d1 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/old/*`])
  );
  await adminPost(`/ns/${ns}/worker/app/promote`, { version: d1.json.version });

  // v2 swaps to /new/*
  const d2 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/new/*`])
  );
  const p2 = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d2.json.version });
  assert.equal(p2.status, 200);

  let out = redisHKeys(`patterns:${host}`).join("\n");
  assert.ok(out.includes("/new/"), `expected /new/ after v2 promote, got ${out}`);
  assert.ok(!out.includes("/old/"), `expected /old/ removed after v2, got ${out}`);

  const pRoll = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d1.json.version });
  assert.equal(pRoll.status, 200);

  out = redisHKeys(`patterns:${host}`).join("\n");
  assert.ok(out.includes("/old/"), `expected /old/ restored after rollback, got ${out}`);
  assert.ok(!out.includes("/new/"), `expected /new/ gone after rollback, got ${out}`);
});

test("patterns:<host> slot value embeds version; ns-hosts:<ns> reverse index tracks active hosts", async () => {
  const ns = uniqueNs("nshosts");
  const hostA = "a.workers.example";
  const hostB = "b.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [hostA, hostB] });

  // Promote v1 with routes on both hosts
  const d1 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${hostA}/api/*`, `${hostB}/*`])
  );
  await adminPost(`/ns/${ns}/worker/app/promote`, { version: d1.json.version });

  // Slot value carries version
  const slotA = redisHGet(`patterns:${hostA}`, "/api/*");
  assert.ok(slotA, "expected /api/* pattern projection");
  assert.equal(decodePatternProjection(slotA).version, "v1");

  // ns-hosts tracks both
  const nsHosts1 = new Set(redisSMembers(`ns-hosts:${ns}`));
  assert.ok(nsHosts1.has(hostA), [...nsHosts1].join("\n"));
  assert.ok(nsHosts1.has(hostB), [...nsHosts1].join("\n"));

  // v2 drops hostB; hostA's slot should now carry version v2
  const d2 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${hostA}/api/*`])
  );
  await adminPost(`/ns/${ns}/worker/app/promote`, { version: d2.json.version });

  const slotA2 = redisHGet(`patterns:${hostA}`, "/api/*");
  assert.ok(slotA2, "expected pattern slot to be set");
  assert.equal(decodePatternProjection(slotA2).version, "v2");

  const nsHosts2 = new Set(redisSMembers(`ns-hosts:${ns}`));
  assert.ok(nsHosts2.has(hostA), [...nsHosts2].join("\n"));
  assert.ok(!nsHosts2.has(hostB), `expected hostB dropped, got ${[...nsHosts2].join("\n")}`);

  // Now hostB can be reconciled away cleanly (reverse index says ns no longer
  // owns anything there, so 409 fast-path doesn't trigger)
  const rm = await adminPost(`/ns/${ns}/hosts`, { hosts: [hostA] });
  assert.equal(rm.status, 200);
});

test("Deploy routes stored in version meta", async () => {
  const ns = uniqueNs("meta-routes");
  const d = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy(["WORKERS.EXAMPLE/api/*", "api.workers.example/v1/*"])
  );
  assert.equal(d.status, 201);

  const meta = readMeta(ns, "app", d.json.version);
  assert.ok(Array.isArray(meta.routes));
  assert.equal(meta.routes.length, 2);
  assert.equal(meta.routes[0].host, "workers.example");
  assert.equal(meta.routes[0].slot, "/api/*");
  assert.equal(meta.routes[0].kind, "prefix");
  assert.equal(meta.routes[0].value, "/api/");
});

test("Concurrent promotes on same (ns, worker): WATCH serializes, final state is one of the inputs", async () => {
  const ns = uniqueNs("promote-race");
  const host = "race.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });

  // Deploy two versions with disjoint routes so we can tell from patterns:<host>
  // which one won. Each version has a single slot only it declares.
  const d1 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/one/*`])
  );
  assert.equal(d1.status, 201);
  const d2 = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/two/*`])
  );
  assert.equal(d2.status, 201);

  // Fire both promotes in parallel. WATCH/MULTI/EXEC makes them serializable,
  // so both must return 200 (retry budget is 5, contention is 1) and the final
  // routes:<ns> value is whichever committed last.
  const [p1, p2] = await Promise.all([
    adminPost(`/ns/${ns}/worker/app/promote`, { version: d1.json.version }),
    adminPost(`/ns/${ns}/worker/app/promote`, { version: d2.json.version }),
  ]);
  assertStatus(p1, 200, "parallel promote p1");
  assertStatus(p2, 200, "parallel promote p2");

  // Final state reflects exactly one of the two versions: routes:<ns>[app]
  // equals one of v1/v2 and patterns:<host> carries only that version's slot.
  const activeVersion = redisHGet(`routes:${ns}`, "app");
  assert.ok(
    activeVersion === d1.json.version || activeVersion === d2.json.version,
    `unexpected active version: ${activeVersion}`
  );
  const patternSlots = redisHKeys(`patterns:${host}`).join("\n");
  if (activeVersion === d1.json.version) {
    assert.ok(patternSlots.includes("/one/*"), patternSlots);
    assert.ok(!patternSlots.includes("/two/*"), patternSlots);
  } else {
    assert.ok(patternSlots.includes("/two/*"), patternSlots);
    assert.ok(!patternSlots.includes("/one/*"), patternSlots);
  }
});

test("Concurrent promotes on same (host, slot): one owner wins and one conflicts", async () => {
  const ns = uniqueNs("promote-slot-race");
  const host = "slot-race.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });

  const d1 = await adminPost(
    `/ns/${ns}/worker/app-a/deploy`,
    trivialDeploy([`${host}/api/*`])
  );
  assert.equal(d1.status, 201);
  const d2 = await adminPost(
    `/ns/${ns}/worker/app-b/deploy`,
    trivialDeploy([`${host}/api/*`])
  );
  assert.equal(d2.status, 201);

  const results = await Promise.all([
    adminPost(`/ns/${ns}/worker/app-a/promote`, { version: d1.json.version }),
    adminPost(`/ns/${ns}/worker/app-b/promote`, { version: d2.json.version }),
  ]);
  const statuses = results.map((r) => r.status).toSorted();
  assert.deepEqual(statuses, [200, 409], JSON.stringify(results.map((r) => r.json)));

  const conflict = results.find((r) => r.status === 409);
  assert.equal(conflict?.json.error, "route_conflict");

  const slot = redisHGet(`patterns:${host}`, "/api/*");
  assert.ok(slot, "expected pattern slot to be set");
  const parsed = decodePatternProjection(slot);
  assert.ok(["app-a", "app-b"].includes(parsed.worker), slot);
  assert.equal(results.filter((r) => r.status === 200).length, 1);
});

test("CF idiom: /mcp + /mcp/* on same host → exact + prefix slots, both stored", async () => {
  const ns = uniqueNs("mcp");
  const host = "mcp.workers.example";
  await adminPost(`/ns/${ns}/hosts`, { hosts: [host] });
  const d = await adminPost(
    `/ns/${ns}/worker/app/deploy`,
    trivialDeploy([`${host}/mcp`, `${host}/mcp/*`])
  );
  const p = await adminPost(`/ns/${ns}/worker/app/promote`, { version: d.json.version });
  assert.equal(p.status, 200);
  const out = redisHKeys(`patterns:${host}`).join("\n");
  assert.ok(out.includes("/mcp"), out);
  assert.ok(out.includes("/mcp/*"), out);
});
