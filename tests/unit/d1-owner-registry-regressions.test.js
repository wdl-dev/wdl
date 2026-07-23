import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";

import { parseStoredJson } from "../helpers/json-payload.js";
import {
  D1_OWNER_REGISTRY_TEST_STATE as D1_TEST_STATE,
  assertCurrentD1Owner as assertCurrentOwner,
  assertCurrentOwnerWithLeaseBudget,
  drainOwnedDbs,
  drainConcurrency,
  normalizeDatabases,
  ownerGenerationKeyOf,
  ownerKeyOf,
  rebalanceDatabase,
  renewOwnedDbs,
  resetD1OwnerRegistryTestState,
  resolveDbOwner,
  takeoverExpiredOwner,
} from "../helpers/load-d1-owner-registry.js";

beforeEach(resetD1OwnerRegistryTestState);

test("D1 owner registry: records stored under another database scope fail closed", async () => {
  const [requested, misplaced] = normalizeDatabases([
    { namespace: "tenant-a", databaseId: "db-a" },
    { namespace: "tenant-b", databaseId: "db-b" },
  ]);
  D1_TEST_STATE.registryStore.set(ownerKeyOf(requested.dbKey), JSON.stringify({
    ...misplaced,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  }));

  await assert.rejects(
    () => resolveDbOwner({ REDIS_ADDR: "redis:6379" }, requested),
    /D1 owner record is invalid/
  );
  assert.deepEqual(D1_TEST_STATE.setCommands, []);
});

test("D1 owner registry: same-task reclaim repairs a stale generation counter without advancing owner generation", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify({
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 11,
    leaseExpiresAt: Date.now() + 60_000,
  }));
  D1_TEST_STATE.registryStore.set(generationKey, "7");

  const owner = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.equal(owner.generation, 11);
  assert.equal(D1_TEST_STATE.registryStore.get(generationKey), "11");
  assert.deepEqual(
    D1_TEST_STATE.setCommands.map((/** @type {any} */ command) => command[1]),
    [generationKey]
  );
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_generation_repairs", labels: { service: "d1-runtime", outcome: "ok" } },
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "local" } },
  ]);
});

test("D1 owner registry: corrupt generation counter fails closed", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  D1_TEST_STATE.registryStore.set(
    ownerGenerationKeyOf(identity.dbKey),
    "not-a-generation"
  );

  await assert.rejects(
    () => resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity),
    /Owner generation counter is corrupt/
  );
});

test("D1 owner registry: draining task errors do not expose task identity", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  D1_TEST_STATE.draining = true;

  await assert.rejects(
    () => resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity),
    (err) => {
      assert.ok(err instanceof Error);
      assert.match(err.message, /D1 task is draining/);
      assert.doesNotMatch(err.message, /\btask-a\b/);
      return true;
    }
  );
});

test("D1 owner registry: same-task hot path does not refresh the owner lease", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 11,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(generationKey, "11");
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);

  const resolved = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.deepEqual(resolved, owner);
  assert.deepEqual(D1_TEST_STATE.setCommands, []);
  assert.deepEqual(parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKey), "D1 owner record"), owner);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "local" } },
  ]);
});

test("D1 owner registry: expired same-task owner is reclaimed before local dispatch", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const expiredOwner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 11,
    leaseExpiresAt: redisNow - 1,
  };
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(expiredOwner));
  D1_TEST_STATE.registryStore.set(generationKey, "11");
  D1_TEST_STATE.ownedDbs.set(expiredOwner.dbKey, expiredOwner);
  D1_TEST_STATE.observedOwners.set(expiredOwner.dbKey, {
    owner: expiredOwner,
    expiresAt: Date.now() + 60_000,
  });

  const resolved = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);
  const stored = parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKey), "D1 owner record");

  assert.equal(resolved.taskId, "task-a");
  assert.equal(resolved.generation, 12);
  assert.ok(resolved.leaseExpiresAt > Date.now());
  assert.deepEqual(stored, resolved);
  assert.deepEqual(D1_TEST_STATE.ownedDbs.get(identity.dbKey), resolved);
  assert.deepEqual(D1_TEST_STATE.observedOwners.get(identity.dbKey).owner, resolved);
  assert.equal(D1_TEST_STATE.registryStore.get(generationKey), "12");
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_takeovers", labels: { service: "d1-runtime", outcome: "ok" } },
  ]);
});

test("D1 owner registry: lost owner state cannot validate an old owner from another task", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  D1_TEST_STATE.taskIdentity = { taskId: "task-b", endpoint: "d1-runtime-b:8787" };

  const owner = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.equal(owner.taskId, "task-b");
  assert.equal(owner.generation, 1);
  await assert.rejects(
    assertCurrentOwner({ REDIS_ADDR: "redis:6379" }, {
      ...identity,
      taskId: "task-a",
      generation: 1,
    }),
    (err) => {
      assert.ok(err instanceof Error);
      assert.match(err.message, /owned by another task/);
      assert.doesNotMatch(err.message, /\btask-[ab]\b/);
      return true;
    }
  );
});

test("D1 owner registry: same task reclaims generation one after owner state loss", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  D1_TEST_STATE.taskIdentity = { taskId: "task-b", endpoint: "d1-runtime-b:8787" };

  const owner = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.equal(owner.taskId, "task-b");
  assert.equal(owner.generation, 1);
  assert.equal(owner.endpoint, "d1-runtime-b:8787");
  assert.deepEqual(await assertCurrentOwner({ REDIS_ADDR: "redis:6379" }, owner), owner);
  assert.deepEqual(
    D1_TEST_STATE.redisState.commands.filter((/** @type {any[]} */ command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", [ownerKey, generationKey]]]
  );
});

test("D1 owner registry: current-owner check rejects expired matching leases", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: redisNow - 1,
  };
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  await assert.rejects(
    assertCurrentOwner({ REDIS_ADDR: "redis:6379" }, owner),
    /owner lease has expired/
  );
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.equal(D1_TEST_STATE.observedOwners.has(identity.dbKey), false);
});

test("D1 owner registry: current-owner check renews leases inside the guard window", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: redisNow + 250,
  };
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(ownerGenerationKeyOf(identity.dbKey), String(owner.generation));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  const renewed = await assertCurrentOwner({ REDIS_ADDR: "redis:6379", D1_OWNER_LEASE_GUARD_MS: "1000" }, owner);

  assert.equal(renewed.taskId, owner.taskId);
  assert.equal(renewed.generation, owner.generation);
  assert.ok(Number(renewed.leaseExpiresAt) > Number(owner.leaseExpiresAt));
  assert.deepEqual(D1_TEST_STATE.ownedDbs.get(identity.dbKey), renewed);
  assert.equal(D1_TEST_STATE.observedOwners.has(identity.dbKey), true);
});

test("D1 owner registry: current-owner budget result carries Redis-time remaining lease", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: redisNow + 5000,
  };
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  const assertion = await assertCurrentOwnerWithLeaseBudget({ REDIS_ADDR: "redis:6379", D1_OWNER_LEASE_GUARD_MS: "1200" }, owner);

  assert.deepEqual(assertion.owner, owner);
  assert.equal(assertion.leaseRemainingMs, 5000);
  assert.equal(assertion.guardMs, 1200);
});

test("D1 owner registry: current-owner check rejects renewals that still miss the guard budget", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: redisNow + 250,
  };
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(ownerGenerationKeyOf(identity.dbKey), String(owner.generation));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  await assert.rejects(
    assertCurrentOwner({
      REDIS_ADDR: "redis:6379",
      D1_OWNER_TTL_SECONDS: "1",
      D1_OWNER_LEASE_GUARD_MS: "2000",
    }, owner),
    /insufficient remaining budget/
  );
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.equal(D1_TEST_STATE.observedOwners.has(identity.dbKey), false);
});

test("D1 owner registry: post-renew guard budget uses the renew Redis time", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const redisNow = Date.now();
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: redisNow + 250,
  };
  D1_TEST_STATE.redisTimeSequence = [redisNow, redisNow + 200];
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(ownerGenerationKeyOf(identity.dbKey), String(owner.generation));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  await assert.rejects(
    assertCurrentOwner({
      REDIS_ADDR: "redis:6379",
      D1_OWNER_TTL_SECONDS: "1",
      D1_OWNER_LEASE_GUARD_MS: "1100",
    }, owner),
    /insufficient remaining budget/
  );
});

test("D1 owner registry: current-owner check uses Redis server time as lease authority", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const leaseExpiresAt = Date.now() + 60_000;
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt,
  };
  D1_TEST_STATE.redisTimeMs = leaseExpiresAt + 1;
  D1_TEST_STATE.registryStore.set(ownerKeyOf(identity.dbKey), JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, owner);
  D1_TEST_STATE.observedOwners.set(identity.dbKey, { owner, expiresAt: Date.now() + 60_000 });

  await assert.rejects(
    assertCurrentOwner({ REDIS_ADDR: "redis:6379" }, owner),
    /owner lease has expired/
  );
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.equal(D1_TEST_STATE.observedOwners.has(identity.dbKey), false);
});

test("D1 owner registry: stale generation repair failure is best-effort", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 11,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(generationKey, "7");
  D1_TEST_STATE.watchExecFailures = 3;

  const resolved = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.deepEqual(resolved, owner);
  assert.ok(D1_TEST_STATE.ownedDbs.has(identity.dbKey));
  assert.equal(D1_TEST_STATE.registryStore.get(generationKey), "7");
  assert.equal(D1_TEST_STATE.logEntries.length, 1);
  assert.equal(D1_TEST_STATE.logEntries[0].event, "d1_owner_generation_repair_failed");
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_generation_repairs", labels: { service: "d1-runtime", outcome: "error" } },
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "local" } },
  ]);
});

test("D1 owner registry: claim race falls back to the winner owner", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const winner = {
    ...identity,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(generationKey, "3");
  D1_TEST_STATE.watchExecFailures = 3;
  D1_TEST_STATE.onWatchExecFailure = (/** @type {any} */ _commands, /** @type {number} */ remainingFailures) => {
    if (remainingFailures === 0) {
      D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(winner));
      D1_TEST_STATE.registryStore.set(generationKey, "4");
    }
  };

  const owner = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.deepEqual(owner, winner);
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "race_resolved" } },
  ]);
});

test("D1 owner registry: claim race waits briefly for a winner owner", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const winner = {
    ...identity,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(generationKey, "3");
  D1_TEST_STATE.watchExecFailures = 3;
  D1_TEST_STATE.onWatchExecFailure = (/** @type {any} */ _commands, /** @type {number} */ remainingFailures) => {
    if (remainingFailures === 0) {
      setTimeout(() => {
        D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(winner));
        D1_TEST_STATE.registryStore.set(generationKey, "4");
      }, 20);
    }
  };

  const owner = await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity);

  assert.deepEqual(owner, winner);
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "race_resolved" } },
  ]);
});

test("D1 owner registry: observed remote owner avoids repeated registry reads", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const owner = {
    ...identity,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(identity.dbKey, { ...identity, taskId: "task-a" });

  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity), owner);
  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity), owner);

  assert.equal(D1_TEST_STATE.redisGets, 1);
  assert.equal(D1_TEST_STATE.redisTimes, 1);
  assert.equal(D1_TEST_STATE.ownedDbs.has(identity.dbKey), false);
  assert.deepEqual(D1_TEST_STATE.forgottenStorageSizes, [identity.dbKey, identity.dbKey]);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "remote" } },
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "cached_remote" } },
  ]);
});

test("D1 owner registry: observed local owner avoids repeated registry reads", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const generationKey = ownerGenerationKeyOf(identity.dbKey);
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 4,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.registryStore.set(generationKey, "4");

  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity), owner);
  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity), owner);

  assert.equal(D1_TEST_STATE.redisGets, 1);
  assert.equal(D1_TEST_STATE.redisTimes, 1);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "local" } },
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "cached_local" } },
  ]);
});

test("D1 owner registry: forced refresh bypasses observed owner cache", async () => {
  const identity = { namespace: "tenant-a", databaseId: "db1", dbKey: "tenant-a:db1", slot: 7 };
  const ownerKey = ownerKeyOf(identity.dbKey);
  const firstOwner = {
    ...identity,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const nextOwner = {
    ...identity,
    taskId: "task-c",
    endpoint: "d1-runtime-c:8787",
    generation: 5,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(firstOwner));
  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity), firstOwner);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(nextOwner));

  assert.deepEqual(await resolveDbOwner({ REDIS_ADDR: "redis:6379" }, identity, { refresh: true }), nextOwner);

  assert.equal(D1_TEST_STATE.redisGets, 2);
  assert.deepEqual(D1_TEST_STATE.metricIncrements, [
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "remote" } },
    { name: "d1_owner_resolutions", labels: { service: "d1-runtime", outcome: "remote" } },
  ]);
});

test("D1 owner registry: takeover bumps generation monotonically even when the stored counter is stale", async () => {
  const redisNow = Date.now();
  const staleOwner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 11,
    leaseExpiresAt: redisNow - 1,
  };
  const ownerKey = ownerKeyOf(staleOwner.dbKey);
  const generationKey = ownerGenerationKeyOf(staleOwner.dbKey);
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(staleOwner));
  D1_TEST_STATE.registryStore.set(generationKey, "7");

  const owner = await takeoverExpiredOwner({ REDIS_ADDR: "redis:6379" }, staleOwner);

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 12);
  assert.equal(D1_TEST_STATE.registryStore.get(generationKey), "12");
  assert.ok(D1_TEST_STATE.watchedKeys.includes(ownerKey));
  assert.ok(D1_TEST_STATE.watchedKeys.includes(generationKey));
  assert.deepEqual(
    D1_TEST_STATE.redisState.commands.filter((/** @type {any[]} */ command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", [ownerKey, generationKey]]]
  );
});

test("D1 owner registry: drain waits for in-flight queries before releasing owners", async () => {
  const owner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(owner.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  D1_TEST_STATE.pendingQueries = 1;

  setTimeout(() => {
    D1_TEST_STATE.pendingQueries = 0;
  }, 40);

  const started = Date.now();
  const result = await drainOwnedDbs({ REDIS_ADDR: "redis:6379", D1_DRAIN_TIMEOUT_MS: "250" });

  assert.equal(result.released, 1);
  assert.equal(result.pending, 0);
  assert.ok(result.pendingObservedMax >= 1);
  assert.ok(result.waitedMs >= 25);
  assert.ok(Date.now() - started >= 25);
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKey), false);
  assert.deepEqual(D1_TEST_STATE.forgottenStorageSizes, [owner.dbKey]);
});

test("D1 owner registry: batched drain releases peers without deleting a reclaimed owner", async () => {
  const owners = ["db1", "db2"].map((databaseId, index) => ({
    namespace: "tenant-a",
    databaseId,
    dbKey: `tenant-a:${databaseId}`,
    slot: index,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: index + 3,
    leaseExpiresAt: Date.now() + 60_000,
  }));
  for (const owner of owners) {
    D1_TEST_STATE.registryStore.set(ownerKeyOf(owner.dbKey), JSON.stringify(owner));
    D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  }
  const replacement = {
    ...owners[1],
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 9,
  };
  D1_TEST_STATE.beforeDelIfEqMany = () => {
    D1_TEST_STATE.registryStore.set(ownerKeyOf(owners[1].dbKey), JSON.stringify(replacement));
  };

  const result = await drainOwnedDbs({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.released, 1);
  assert.equal(result.alreadyLost, 1);
  assert.deepEqual(result.errors, []);
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKeyOf(owners[0].dbKey)), false);
  assert.deepEqual(
    parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKeyOf(owners[1].dbKey))),
    replacement
  );
});

test("D1 owner registry: drain retries a concurrent same-fence lease renewal", async () => {
  const owner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(owner.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  let renewed = false;
  D1_TEST_STATE.beforeDelIfEqMany = () => {
    if (renewed) return;
    renewed = true;
    D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify({
      ...owner,
      leaseExpiresAt: owner.leaseExpiresAt + 60_000,
    }));
  };

  const result = await drainOwnedDbs({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.released, 1);
  assert.equal(result.alreadyLost, 0);
  assert.deepEqual(result.errors, []);
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKey), false);
  assert.equal(D1_TEST_STATE.ownedDbs.has(owner.dbKey), false);
});

test("D1 owner registry: drain retains ownership after persistent same-fence release races", async () => {
  const owner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(owner.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  let leaseExpiresAt = owner.leaseExpiresAt;
  D1_TEST_STATE.beforeDelIfEqMany = () => {
    leaseExpiresAt += 60_000;
    D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify({ ...owner, leaseExpiresAt }));
  };

  const result = await drainOwnedDbs({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.released, 0);
  assert.equal(result.alreadyLost, 0);
  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0].error, /owner release raced/i);
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKey), true);
  assert.equal(D1_TEST_STATE.ownedDbs.has(owner.dbKey), true);
});

test("D1 owner registry: drain concurrency is configurable and bounded", () => {
  assert.equal(drainConcurrency({}), 16);
  assert.equal(drainConcurrency({ D1_DRAIN_CONCURRENCY: "2" }), 2);
  assert.equal(drainConcurrency({ D1_DRAIN_CONCURRENCY: "1000" }), 64);
  assert.equal(drainConcurrency({ D1_DRAIN_CONCURRENCY: "0" }), 16);
  assert.equal(drainConcurrency({ D1_DRAIN_CONCURRENCY: "bad" }), 16);
});

test("D1 owner registry: drain releases idle databases even when one actor times out", async () => {
  const owners = ["slow", "idle"].map((databaseId, idx) => ({
    namespace: "tenant-a",
    databaseId,
    dbKey: `tenant-a:${databaseId}`,
    slot: idx,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  }));
  for (const owner of owners) {
    D1_TEST_STATE.registryStore.set(ownerKeyOf(owner.dbKey), JSON.stringify(owner));
    D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  }

  const result = await drainOwnedDbs({
    REDIS_ADDR: "redis:6379",
    D1_DRAIN_TIMEOUT_MS: "30",
    D1_DATABASES: {
      /** @param {string} dbKey */
      idFromName(dbKey) {
        return dbKey;
      },
      /** @param {string} dbKey */
      get(dbKey) {
        return {
          /** @param {string} _url @param {{ signal?: AbortSignal }} [init] */
          fetch(_url, init = {}) {
            if (dbKey === "tenant-a:slow") {
              return new Promise((resolve, reject) => {
                const timer = setTimeout(() => resolve(Response.json({ idle: true })), 1_000);
                init.signal?.addEventListener("abort", () => {
                  clearTimeout(timer);
                  reject(new DOMException("Aborted", "AbortError"));
                }, { once: true });
              });
            }
            return Response.json({ idle: true });
          },
        };
      },
    },
  });

  assert.equal(result.released, 1);
  assert.equal(result.alreadyLost, 0);
  assert.equal(result.errors.length, 1);
  assert.equal(result.errors[0].dbKey, "tenant-a:slow");
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKeyOf("tenant-a:idle")), false);
  assert.equal(D1_TEST_STATE.registryStore.has(ownerKeyOf("tenant-a:slow")), true);
});

test("D1 owner registry: takeover retries WatchError before surfacing an ownership race", async () => {
  const redisNow = Date.now();
  const staleOwner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
    leaseExpiresAt: redisNow - 1,
  };
  const ownerKey = ownerKeyOf(staleOwner.dbKey);
  const generationKey = ownerGenerationKeyOf(staleOwner.dbKey);
  D1_TEST_STATE.redisTimeMs = redisNow;
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(staleOwner));
  D1_TEST_STATE.registryStore.set(generationKey, "4");
  D1_TEST_STATE.watchExecFailures = 1;

  const owner = await takeoverExpiredOwner({ REDIS_ADDR: "redis:6379" }, staleOwner);

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 5);
});

test("D1 owner registry: rebalance retries WatchError before moving ownership", async () => {
  const database = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
  };
  const currentOwner = {
    ...database,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 6,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(database.dbKey);
  const generationKey = ownerGenerationKeyOf(database.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(currentOwner));
  D1_TEST_STATE.registryStore.set(generationKey, "6");
  D1_TEST_STATE.ownedDbs.set(database.dbKey, currentOwner);
  D1_TEST_STATE.watchExecFailures = 1;

  const result = await rebalanceDatabase(
    { REDIS_ADDR: "redis:6379" },
    database,
    { taskId: "task-b", endpoint: "d1-runtime-b:8787" }
  );

  assert.equal(result.outcome, "moved");
  assert.equal(result.owner.taskId, "task-b");
  assert.equal(result.owner.generation, 7);
  assert.deepEqual(D1_TEST_STATE.forgottenStorageSizes, [database.dbKey]);
  assert.deepEqual(
    D1_TEST_STATE.redisState.commands.filter((/** @type {any[]} */ command) => command[0] === "getManyWithTime"),
    [
      ["getManyWithTime", [ownerKey, generationKey]],
      ["getManyWithTime", [ownerKey, generationKey]],
    ]
  );
});

test("D1 owner registry: rebalance to the current owner is a no-op", async () => {
  const database = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
  };
  const currentOwner = {
    ...database,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 6,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(database.dbKey);
  const generationKey = ownerGenerationKeyOf(database.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(currentOwner));
  D1_TEST_STATE.registryStore.set(generationKey, "6");
  D1_TEST_STATE.ownedDbs.set(database.dbKey, currentOwner);

  const result = await rebalanceDatabase(
    { REDIS_ADDR: "redis:6379" },
    database,
    { taskId: "task-a", endpoint: "d1-runtime-a:8787" }
  );

  assert.equal(result.outcome, "unchanged");
  assert.equal(result.owner.taskId, "task-a");
  assert.equal(result.owner.generation, 6);
  assert.equal(D1_TEST_STATE.registryStore.get(generationKey), "6");
  assert.deepEqual(parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKey), "D1 owner record"), currentOwner);
  assert.equal(D1_TEST_STATE.ownedDbs.get(database.dbKey), result.owner);
  assert.equal(D1_TEST_STATE.ownedDbs.get(database.dbKey).generation, 6);
});

test("D1 owner registry: rebalance to same task with a different endpoint refreshes owner", async () => {
  const database = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
  };
  const currentOwner = {
    ...database,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 6,
    leaseExpiresAt: Date.now() + 60_000,
  };
  D1_TEST_STATE.registryStore.set(ownerKeyOf(database.dbKey), JSON.stringify(currentOwner));
  D1_TEST_STATE.registryStore.set(ownerGenerationKeyOf(database.dbKey), "6");
  D1_TEST_STATE.ownedDbs.set(database.dbKey, currentOwner);

  const result = await rebalanceDatabase(
    { REDIS_ADDR: "redis:6379" },
    database,
    { taskId: "task-a", endpoint: "d1-runtime-a-new:8787" }
  );

  assert.equal(result.outcome, "moved");
  assert.equal(result.owner.taskId, "task-a");
  assert.equal(result.owner.endpoint, "d1-runtime-a-new:8787");
  assert.equal(result.owner.generation, 7);
  assert.equal(D1_TEST_STATE.ownedDbs.has(database.dbKey), false);
  assert.deepEqual(D1_TEST_STATE.forgottenStorageSizes, [database.dbKey]);
});

test("D1 owner registry: renew retries raw IFEQ after a same-fence race", async () => {
  const owner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const ownerKey = ownerKeyOf(owner.dbKey);
  const stored = `${JSON.stringify(owner)}\n`;
  const concurrentRenewal = { ...owner, leaseExpiresAt: owner.leaseExpiresAt + 1_000 };
  D1_TEST_STATE.registryStore.set(ownerKey, stored);
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  D1_TEST_STATE.beforeSetIfEq = (
    /** @type {string} */ _key,
    /** @type {string} */ _value,
    /** @type {{ ifeq?: string | Uint8Array }} */ options,
    /** @type {number} */ attempt
  ) => {
    if (attempt !== 1) return;
    assert.ok(options.ifeq instanceof Uint8Array);
    assert.equal(new TextDecoder().decode(options.ifeq), stored);
    D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(concurrentRenewal));
  };

  const result = await renewOwnedDbs({
    REDIS_ADDR: "redis:6379",
    D1_OWNER_TTL_SECONDS: "120",
  });

  assert.equal(result.renewed, 1);
  assert.equal(result.lost, 0);
  assert.deepEqual(result.errors, []);
  assert.equal(D1_TEST_STATE.ifEqAttempts, 2);
  assert.equal(D1_TEST_STATE.watchedKeys.includes(ownerKey), false);
  assert.ok(parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKey)).leaseExpiresAt > owner.leaseExpiresAt);
});

test("D1 owner registry: renew does not overwrite a peer that wins the IFEQ race", async () => {
  const owner = {
    namespace: "tenant-a",
    databaseId: "db1",
    dbKey: "tenant-a:db1",
    slot: 7,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const replacement = {
    ...owner,
    taskId: "task-b",
    endpoint: "d1-runtime-b:8787",
    generation: 4,
  };
  const ownerKey = ownerKeyOf(owner.dbKey);
  D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(owner));
  D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
  D1_TEST_STATE.beforeSetIfEq = () => {
    D1_TEST_STATE.registryStore.set(ownerKey, JSON.stringify(replacement));
    D1_TEST_STATE.beforeSetIfEq = null;
  };

  const result = await renewOwnedDbs({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.renewed, 0);
  assert.equal(result.lost, 1);
  assert.deepEqual(result.errors, []);
  assert.equal(D1_TEST_STATE.ifEqAttempts, 1);
  assert.equal(D1_TEST_STATE.ownedDbs.has(owner.dbKey), false);
  assert.deepEqual(parseStoredJson(D1_TEST_STATE.registryStore.get(ownerKey)), replacement);
});

test("D1 owner registry: renew uses bounded concurrency instead of strict serial execution", async () => {
  D1_TEST_STATE.ifEqDelayMs = 25;
  for (let idx = 0; idx < 4; idx += 1) {
    const owner = {
      namespace: "tenant-a",
      databaseId: `db${idx}`,
      dbKey: `tenant-a:db${idx}`,
      slot: idx,
      taskId: "task-a",
      endpoint: "d1-runtime-a:8787",
      generation: 1,
      leaseExpiresAt: Date.now() + 60_000,
    };
    D1_TEST_STATE.ownedDbs.set(owner.dbKey, owner);
    D1_TEST_STATE.registryStore.set(ownerKeyOf(owner.dbKey), JSON.stringify(owner));
  }

  const result = await renewOwnedDbs({
    REDIS_ADDR: "redis:6379",
    D1_RENEW_CONCURRENCY: "2",
    D1_OWNER_TTL_SECONDS: "120",
  });

  assert.equal(result.renewed, 4);
  assert.equal(result.lost, 0);
  assert.equal(result.errors.length, 0);
  assert.equal(D1_TEST_STATE.ifEqConcurrencyMax, 2);
});
