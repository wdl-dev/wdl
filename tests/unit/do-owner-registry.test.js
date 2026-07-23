import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";

import {
  assertCurrentOwnerWithLeaseBudget,
  DO_OWNER_REGISTRY_TEST_STATE,
  doOwnerRegistryWriteCommands,
  drainOwnedScopes,
  ownerGenerationKeyOf,
  ownerLeaseGuardMs,
  ownerKeyOf,
  parseOwner,
  renewOwnedScopes,
  resetDoOwnerRegistryTestState,
  resolveDoOwner,
  shouldRenewOwnerLease,
} from "../helpers/load-do-owner-registry.js";
import { parseStoredJson } from "../helpers/json-payload.js";

// Simulated Redis clock advancement between owner check and renew-time reads.
const REDIS_TIME_INCREMENT_AFTER_RENEW_MS = 200;
const LEASE_DURATION_MS = 90_000;
const HEALTHY_LEASE_MS = 60_000;
const SHORT_LEASE_MS = 1_000;
const LEASE_GUARD_MS = 1_000;
const NEAR_EXPIRY_LEASE_MS = 250;
const EXPIRED_LEASE_AGE_MS = 1_000;
const EXPECTED_LEASE_REMAINING_MS = 1234;
const VALID_RENEW_TIME_BASES = [0, 1, 100];
const INVALID_RENEW_TIME_BASES = [1234.5, NaN, Infinity, -Infinity, -1];

const DO_STORAGE_ID = "do_0123456789abcdef0123456789abcdef";
const OWNER_KEY = `${DO_STORAGE_ID}:Room:shard0`;
const STORAGE_POINTER_KEY = "worker:do-storage:tenant:chat";
const DELETE_LOCK_KEY = "worker-delete-lock:tenant:chat";

function ownerSnapshotKeys() {
  return [
    ownerKeyOf(OWNER_KEY),
    ownerGenerationKeyOf(OWNER_KEY),
    DELETE_LOCK_KEY,
    STORAGE_POINTER_KEY,
  ];
}

beforeEach(resetDoOwnerRegistryTestState);

function invoke() {
  return {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: DO_STORAGE_ID,
    hostId: OWNER_KEY,
    className: "Room",
  };
}

/**
 * @param {Record<string, string>} env
 * @param {{ ownerKey: string, taskId: string, generation: number }} owner
 */
async function assertCurrentOwnerForTest(env, owner) {
  const result = await assertCurrentOwnerWithLeaseBudget(env, owner, {
    storageScope: invoke(),
  });
  return result.owner;
}

function setStoragePointer(value = DO_STORAGE_ID) {
  DO_OWNER_REGISTRY_TEST_STATE.store.set(STORAGE_POINTER_KEY, value);
}

function ownerRecord(overrides = {}) {
  return {
    ownerKey: OWNER_KEY,
    hostId: OWNER_KEY,
    className: "Room",
    ns: "tenant",
    worker: "chat",
    doStorageId: DO_STORAGE_ID,
    taskId: "task-a",
    endpoint: "do-runtime-a:8788",
    generation: 7,
    leaseExpiresAt: Date.now() + LEASE_DURATION_MS,
    ...overrides,
  };
}

test("DO owner registry rejects malformed persisted owner endpoints", () => {
  const owner = ownerRecord();
  assert.deepEqual(parseOwner(JSON.stringify(owner), OWNER_KEY), owner);
  assert.equal(parseOwner(null, OWNER_KEY), null);
  assert.throws(
    () => parseOwner(JSON.stringify(ownerRecord({ endpoint: "8.8.8.8:8788" })), OWNER_KEY),
    /DO owner record is invalid/
  );
  assert.throws(
    () => parseOwner(JSON.stringify(ownerRecord({ ownerKey: `${DO_STORAGE_ID}:Room:shard1` })), OWNER_KEY),
    /DO owner record is invalid/
  );
  for (const invalidScope of [
    { ns: "tenant:other" },
    { worker: "chat:other" },
  ]) {
    assert.throws(
      () => parseOwner(JSON.stringify(ownerRecord(invalidScope)), OWNER_KEY),
      /DO owner record is invalid/
    );
  }
  assert.throws(() => parseOwner("{not-json", OWNER_KEY), /DO owner record is invalid/);
});

test("DO owner registry: keys encode owner scope and generation separately", () => {
  assert.equal(ownerKeyOf(OWNER_KEY), "do:owner:scope:do_0123456789abcdef0123456789abcdef%3ARoom%3Ashard0");
  assert.equal(
    ownerGenerationKeyOf(OWNER_KEY),
    "do:owner:scope:do_0123456789abcdef0123456789abcdef%3ARoom%3Ashard0:generation"
  );
});

test("DO owner registry: records stored under another owner scope fail closed", async () => {
  const misplacedStorageId = "do_fedcba9876543210fedcba9876543210";
  const misplacedOwnerKey = `${misplacedStorageId}:Room:shard1`;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(OWNER_KEY), JSON.stringify(ownerRecord({
    ownerKey: misplacedOwnerKey,
    hostId: misplacedOwnerKey,
    doStorageId: misplacedStorageId,
  })));

  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    /DO owner record is invalid/
  );
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: records for another bundle scope fail closed before pointer lookup", async () => {
  const misplacedPointerKey = "worker:do-storage:other:worker";
  DO_OWNER_REGISTRY_TEST_STATE.store.set(misplacedPointerKey, DO_STORAGE_ID);
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(OWNER_KEY), JSON.stringify(ownerRecord({
    ns: "other",
    worker: "worker",
  })));

  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    /DO owner record is invalid/
  );
  assert.equal(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.some((command) => (
      command[0] === "get" && command[1] === misplacedPointerKey
    )),
    false
  );
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: lease guard config bounds values and permits test disable", () => {
  assert.equal(ownerLeaseGuardMs({}), 1000);
  assert.equal(ownerLeaseGuardMs({ DO_OWNER_LEASE_GUARD_MS: "0" }), 0);
  assert.equal(ownerLeaseGuardMs({ DO_OWNER_LEASE_GUARD_MS: "250" }), 250);
  assert.equal(ownerLeaseGuardMs({ DO_OWNER_LEASE_GUARD_MS: "-1" }), 1000);
});

test("DO owner registry: claim writes owner generation counter", async () => {
  setStoragePointer();
  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 1);
  assert.equal(owner.ns, "tenant");
  assert.equal(owner.worker, "chat");
  assert.equal(owner.doStorageId, DO_STORAGE_ID);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerGenerationKeyOf(owner.ownerKey)), "1");
  assert.equal(
    parseStoredJson(
      DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerKeyOf(owner.ownerKey)),
      "DO owner record"
    ).generation,
    1
  );
  assert.deepEqual(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => command[0] === "getManyWithTime"),
    [
      ["getManyWithTime", ownerSnapshotKeys()],
      ["getManyWithTime", ownerSnapshotKeys()],
    ]
  );
  assert.equal(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.some((command) => command[0] === "get"),
    false
  );
});

test("DO owner registry: corrupt generation counter fails closed", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(
    ownerGenerationKeyOf(OWNER_KEY),
    "not-a-generation"
  );

  await assert.rejects(
    () => resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    /Owner generation counter is corrupt/
  );
});

test("DO owner registry: claim retries WatchError before surfacing owner", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisState.execFailures = 1;

  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.redisState.execFailures, 0);
  assert.equal(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.watchBatches.filter((keys) =>
      keys.includes(ownerKeyOf(OWNER_KEY))
    ).length,
    2
  );
});

test("DO owner registry: first claim requires the active worker storage pointer", async () => {
  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    (err) => {
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "stale_owner_storage");
      return true;
    }
  );
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: first claim refuses a worker under whole-delete lock", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(DELETE_LOCK_KEY, "whole:delete-token");

  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    (err) => {
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "stale_owner_storage");
      return true;
    }
  );
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.watchedKeys, []);
  assert.deepEqual(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", ownerSnapshotKeys()]]
  );
  assert.equal(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.some((command) => (
      command[0] === "get" && command[1] === DELETE_LOCK_KEY
    )),
    false
  );
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: version delete lock does not interrupt active storage", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(DELETE_LOCK_KEY, "version:delete-token");

  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 1);
  assert.ok(DO_OWNER_REGISTRY_TEST_STATE.watchedKeys.includes(DELETE_LOCK_KEY));
});

test("DO owner registry: delete lock appearing before claim commit aborts the claim", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisState.execFailures = 1;
  DO_OWNER_REGISTRY_TEST_STATE.onExecFailure = () => {
    DO_OWNER_REGISTRY_TEST_STATE.store.set(DELETE_LOCK_KEY, "whole:delete-token");
  };

  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    (err) => {
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "stale_owner_storage");
      return true;
    }
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(ownerKeyOf(OWNER_KEY)), false);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(ownerGenerationKeyOf(OWNER_KEY)), false);
});

test("DO owner registry: lost owner state cannot validate an old owner from another task", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.taskIdentity = { taskId: "task-b", endpoint: "do-runtime-b:8788" };

  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-b");
  assert.equal(owner.generation, 1);
  await assert.rejects(
    assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, {
      ownerKey: OWNER_KEY,
      taskId: "task-a",
      generation: 1,
    }),
    /owner generation is stale/
  );
});

test("DO owner registry: same task reclaims generation one after owner state loss", async () => {
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.taskIdentity = { taskId: "task-b", endpoint: "do-runtime-b:8788" };

  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-b");
  assert.equal(owner.generation, 1);
  assert.equal(owner.endpoint, "do-runtime-b:8788");
  assert.deepEqual(await assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, owner), owner);
});

test("DO owner registry: same-task hot path does not refresh every request", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({ ownerKey, hostId: ownerKey });
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "7");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);

  const resolved = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.deepEqual(resolved, owner);
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.redisState.watchBatches, []);
  assert.deepEqual(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", ownerSnapshotKeys()]]
  );
  assert.equal(
    shouldRenewOwnerLease(
      { DO_OWNER_TTL_SECONDS: "120" },
      owner,
      DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs
    ),
    false
  );
});

test("DO owner registry: renewal time base validation rejects invalid values", () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({ ownerKey, hostId: ownerKey });
  const fixedNowMs = 1_700_000_000_000;

  for (const validTimeBase of VALID_RENEW_TIME_BASES) {
    assert.doesNotThrow(() => shouldRenewOwnerLease({ DO_OWNER_TTL_SECONDS: "120" }, owner, validTimeBase));
  }
  assert.doesNotThrow(() => shouldRenewOwnerLease({ DO_OWNER_TTL_SECONDS: "120" }, owner, fixedNowMs));
  for (const invalidTimeBase of INVALID_RENEW_TIME_BASES) {
    assert.throws(
      () => shouldRenewOwnerLease({ DO_OWNER_TTL_SECONDS: "120" }, owner, invalidTimeBase),
      /Owner lease renewal time base is invalid/
    );
  }
});

test("DO owner registry: draining task returns a healthy remote owner", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
  });
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "7");
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;

  const resolved = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.deepEqual(resolved, owner);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.redisState.watchBatches, []);
  assert.deepEqual(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", ownerSnapshotKeys()]]
  );
});

test("DO owner registry: draining task refuses local ownership or takeover", async () => {
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;

  await assert.rejects(
    resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke()),
    (err) => {
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "task_draining");
      assert.match(/** @type {Error} */ (err).message, /DO task is draining/);
      assert.doesNotMatch(/** @type {Error} */ (err).message, /task-a/);
      return true;
    }
  );
});

test("DO owner registry: expired takeover bumps generation monotonically", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  const staleOwner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
    generation: 11,
    leaseExpiresAt: redisNow - EXPIRED_LEASE_AGE_MS,
  });
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(staleOwner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "7");

  const owner = await resolveDoOwner({ REDIS_ADDR: "redis:6379" }, invoke());

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 12);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerGenerationKeyOf(ownerKey)), "12");
  assert.ok(DO_OWNER_REGISTRY_TEST_STATE.watchedKeys.includes(ownerKeyOf(ownerKey)));
  assert.ok(DO_OWNER_REGISTRY_TEST_STATE.watchedKeys.includes(ownerGenerationKeyOf(ownerKey)));
});

test("DO owner registry: actor fence rejects stale generations", async () => {
  const ownerKey = OWNER_KEY;
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  })));

  await assert.rejects(
    assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, { ownerKey, taskId: "task-a", generation: 7 }),
    /owner generation is stale/
  );
});

test("DO owner registry: actor fence rejects expired local leases", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow - EXPIRED_LEASE_AGE_MS,
  })));

  await assert.rejects(
    assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, { ownerKey, taskId: "task-a", generation: 8 }),
    /owner lease has expired/
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
});

test("DO owner registry: actor fence uses Redis server time as lease authority", async () => {
  const ownerKey = OWNER_KEY;
  const leaseExpiresAt = Date.now() + HEALTHY_LEASE_MS;
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = leaseExpiresAt + 1;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt,
  })));

  await assert.rejects(
    assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, { ownerKey, taskId: "task-a", generation: 8 }),
    /owner lease has expired/
  );
});

test("DO owner registry: actor fence renews leases inside the guard window", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow + NEAR_EXPIRY_LEASE_MS,
  })));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  const { owner, leaseRemainingMs } = await assertCurrentOwnerWithLeaseBudget(
    { REDIS_ADDR: "redis:6379", DO_OWNER_LEASE_GUARD_MS: String(LEASE_GUARD_MS) },
    { ownerKey, taskId: "task-a", generation: 8 },
    { storageScope: invoke() }
  );

  assert.equal(owner.taskId, "task-a");
  assert.equal(owner.generation, 8);
  assert.ok(Number(owner.leaseExpiresAt) > redisNow + NEAR_EXPIRY_LEASE_MS);
  assert.ok(leaseRemainingMs >= LEASE_GUARD_MS);
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.get(ownerKey), owner);
});

test("DO owner registry: actor fence rejects renewals that still miss the guard budget", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow + NEAR_EXPIRY_LEASE_MS,
  })));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  await assert.rejects(
    assertCurrentOwnerWithLeaseBudget(
      {
        REDIS_ADDR: "redis:6379",
        DO_OWNER_TTL_SECONDS: "1",
        DO_OWNER_LEASE_GUARD_MS: "2000",
      },
      { ownerKey, taskId: "task-a", generation: 8 },
      { storageScope: invoke() }
    ),
    /insufficient remaining budget/
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
});

test("DO owner registry: post-renew guard budget uses the renew Redis time", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  setStoragePointer();
  // The owner check consumes the first Redis time; renewal consumes the second.
  // This verifies the post-renew guard budget uses the renew-time clock.
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeSequence = [redisNow, redisNow + REDIS_TIME_INCREMENT_AFTER_RENEW_MS];
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow + NEAR_EXPIRY_LEASE_MS,
  })));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  await assert.rejects(
    assertCurrentOwnerWithLeaseBudget(
      {
        REDIS_ADDR: "redis:6379",
        DO_OWNER_TTL_SECONDS: "1",
        DO_OWNER_LEASE_GUARD_MS: "1100",
      },
      { ownerKey, taskId: "task-a", generation: 8 },
      { storageScope: invoke() }
    ),
    /insufficient remaining budget/
  );
});

test("DO owner registry: actor fence reports stale storage before lease-guard renew", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  setStoragePointer("do_new_storage");
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow + NEAR_EXPIRY_LEASE_MS,
  })));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  await assert.rejects(
    assertCurrentOwnerWithLeaseBudget(
      { REDIS_ADDR: "redis:6379", DO_OWNER_LEASE_GUARD_MS: String(LEASE_GUARD_MS) },
      { ownerKey, taskId: "task-a", generation: 8 },
      { storageScope: invoke() }
    ),
    /no longer matches active worker storage/
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: lease-budget watchdog rejects near-expiry leases without renewing", async () => {
  const ownerKey = OWNER_KEY;
  const redisNow = Date.now();
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = redisNow;
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt: redisNow + NEAR_EXPIRY_LEASE_MS,
  })));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  await assert.rejects(
    assertCurrentOwnerWithLeaseBudget(
      { REDIS_ADDR: "redis:6379", DO_OWNER_LEASE_GUARD_MS: String(LEASE_GUARD_MS) },
      { ownerKey, taskId: "task-a", generation: 8 },
      { renewNearExpiry: false, storageScope: invoke() }
    ),
    /insufficient remaining budget/
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
  assert.deepEqual(doOwnerRegistryWriteCommands(), []);
});

test("DO owner registry: actor fence returns Redis-time lease budget", async () => {
  const ownerKey = OWNER_KEY;
  const leaseExpiresAt = Date.now() + HEALTHY_LEASE_MS;
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.redisTimeMs = leaseExpiresAt - EXPECTED_LEASE_REMAINING_MS;
  const owner = ownerRecord({
    ownerKey,
    generation: 8,
    leaseExpiresAt,
  });
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));

  assert.deepEqual(
    await assertCurrentOwnerWithLeaseBudget(
      { REDIS_ADDR: "redis:6379" },
      { ownerKey, taskId: "task-a", generation: 8 },
      { storageScope: invoke() }
    ),
    { owner, leaseRemainingMs: EXPECTED_LEASE_REMAINING_MS }
  );
  assert.deepEqual(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => command[0] === "getManyWithTime"),
    [["getManyWithTime", [ownerKeyOf(ownerKey), STORAGE_POINTER_KEY]]]
  );
  assert.equal(
    DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.some((command) => command[0] === "get"),
    false
  );
});

test("DO owner registry: renewOwnedScopes extends matching owned leases", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);

  const result = await renewOwnedScopes({ REDIS_ADDR: "redis:6379", DO_OWNER_TTL_SECONDS: "120" });

  assert.equal(result.renewed, 1);
  assert.equal(result.lost, 0);
  assert.equal(result.errors.length, 0);
  const renewed = parseStoredJson(
    DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerKeyOf(ownerKey)),
    "DO owner record"
  );
  assert.equal(renewed.generation, 8);
  assert.equal(renewed.taskId, "task-a");
  assert.ok(renewed.leaseExpiresAt > owner.leaseExpiresAt);
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.get(ownerKey), renewed);
});

test("DO owner registry: renewal retries an exact-byte same-fence owner race", async () => {
  const owner = ownerRecord({
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  const initialRaw = `${JSON.stringify(owner)}\n`;
  const concurrentOwner = {
    ...owner,
    leaseExpiresAt: owner.leaseExpiresAt + SHORT_LEASE_MS,
  };
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(OWNER_KEY), initialRaw);
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(OWNER_KEY, owner);
  DO_OWNER_REGISTRY_TEST_STATE.beforeRenewEval = ({ attempt }) => {
    if (attempt === 1) {
      DO_OWNER_REGISTRY_TEST_STATE.store.set(
        ownerKeyOf(OWNER_KEY),
        JSON.stringify(concurrentOwner)
      );
    }
  };

  const result = await renewOwnedScopes({
    REDIS_ADDR: "redis:6379",
    DO_OWNER_TTL_SECONDS: "120",
  });

  assert.equal(result.renewed, 1);
  assert.equal(result.lost, 0);
  assert.equal(result.errors.length, 0);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.renewEvalCalls.length, 2);
  assert.equal(
    new TextDecoder().decode(
      /** @type {Uint8Array} */ (DO_OWNER_REGISTRY_TEST_STATE.renewEvalCalls[0].args[0])
    ),
    initialRaw
  );
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.watchedKeys.includes(ownerKeyOf(OWNER_KEY)), false);
  assert.ok(
    parseStoredJson(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerKeyOf(OWNER_KEY)))
      .leaseExpiresAt > concurrentOwner.leaseExpiresAt
  );
});

test("DO owner registry: renewal preserves a concurrently replaced storage pointer", async () => {
  const owner = ownerRecord({
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  const initialRaw = JSON.stringify(owner);
  const replacementStorageId = "do_replaced0123456789abcdef01234567";
  setStoragePointer();
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(OWNER_KEY), initialRaw);
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(OWNER_KEY, owner);
  DO_OWNER_REGISTRY_TEST_STATE.beforeRenewEval = () => {
    setStoragePointer(replacementStorageId);
  };

  const result = await renewOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.renewed, 0);
  assert.equal(result.lost, 1);
  assert.equal(result.errors.length, 0);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.renewEvalCalls.length, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerKeyOf(OWNER_KEY)), initialRaw);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.get(STORAGE_POINTER_KEY), replacementStorageId);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(OWNER_KEY), false);
});

test("DO owner registry: renewOwnedScopes forgets owners lost to another task", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  const remoteOwner = {
    ...owner,
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
    generation: 9,
  };
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(remoteOwner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "9");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);

  const result = await renewOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.renewed, 0);
  assert.equal(result.lost, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
  assert.deepEqual(DO_OWNER_REGISTRY_TEST_STATE.metricIncrements.at(-1), {
    name: "do_lease_renew_failures",
    labels: { service: "do-runtime", reason: "lost_owner" },
  });
});

test("DO owner registry: renewOwnedScopes logs lease loss with in-flight dispatches", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  const remoteOwner = {
    ...owner,
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
    generation: 9,
  };
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(remoteOwner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "9");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);
  DO_OWNER_REGISTRY_TEST_STATE.inFlightDispatches = 1;

  const result = await renewOwnedScopes({ REDIS_ADDR: "redis:6379" });
  const lastLogEntry = DO_OWNER_REGISTRY_TEST_STATE.logEntries.at(-1);

  assert.equal(result.renewed, 0);
  assert.equal(result.lost, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.draining, false);
  assert.ok(lastLogEntry);
  assert.equal(lastLogEntry.event, "do_owner_lease_lost_with_in_flight_dispatches");
  assert.equal(lastLogEntry.fields.reason, "lost_owner");
  assert.equal(lastLogEntry.fields.in_flight, 1);
});

test("DO owner registry: renewOwnedScopes forgets owners after worker storage pointer changes", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + SHORT_LEASE_MS,
  });
  setStoragePointer("do_replaced0123456789abcdef01234567");
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);

  const result = await renewOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.renewed, 0);
  assert.equal(result.lost, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(ownerKey), false);
});

test("DO owner registry: actor fence rejects owners after worker storage pointer changes", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  });
  setStoragePointer("do_replaced0123456789abcdef01234567");
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");

  await assert.rejects(
    assertCurrentOwnerForTest({ REDIS_ADDR: "redis:6379" }, { ownerKey, taskId: "task-a", generation: 8 }),
    /no longer matches active worker storage/
  );
});

test("DO owner registry: drain releases only matching owned generations", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  });
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);

  DO_OWNER_REGISTRY_TEST_STATE.draining = true;
  const result = await drainOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.released, 1);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(ownerKeyOf(ownerKey)), false);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerGenerationKeyOf(ownerKey)), "8");
});

test("DO owner registry: drainOwnedScopes reports release counts", async () => {
  const ownerKey = OWNER_KEY;
  const owner = ownerRecord({
    ownerKey,
    hostId: ownerKey,
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  });
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(ownerKey), JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerGenerationKeyOf(ownerKey), "8");
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(ownerKey, owner);
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;

  const result = await drainOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.deepEqual(result, {
    draining: true,
    owned: 0,
    released: 1,
    alreadyLost: 0,
    errors: [],
  });
});

test("DO owner registry: batched drain preserves a scope reclaimed before DELIFEQ", async () => {
  const first = ownerRecord({ generation: 8 });
  const secondOwnerKey = `${DO_STORAGE_ID}:Room:shard1`;
  const second = ownerRecord({
    ownerKey: secondOwnerKey,
    hostId: secondOwnerKey,
    generation: 9,
  });
  const replacement = {
    ...second,
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
    generation: 10,
  };
  for (const owner of [first, second]) {
    DO_OWNER_REGISTRY_TEST_STATE.store.set(ownerKeyOf(owner.ownerKey), JSON.stringify(owner));
    DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(owner.ownerKey, owner);
  }
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;
  DO_OWNER_REGISTRY_TEST_STATE.beforeDelIfEqMany = () => {
    DO_OWNER_REGISTRY_TEST_STATE.store.set(
      ownerKeyOf(secondOwnerKey),
      JSON.stringify(replacement)
    );
  };

  const result = await drainOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.deepEqual(result, {
    draining: true,
    owned: 0,
    released: 1,
    alreadyLost: 1,
    errors: [],
  });
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(ownerKeyOf(first.ownerKey)), false);
  assert.deepEqual(
    parseStoredJson(DO_OWNER_REGISTRY_TEST_STATE.store.get(ownerKeyOf(secondOwnerKey))),
    replacement
  );
});

test("DO owner registry: drain retries a concurrent same-fence lease renewal", async () => {
  const owner = ownerRecord({
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  });
  const key = ownerKeyOf(owner.ownerKey);
  DO_OWNER_REGISTRY_TEST_STATE.store.set(key, JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(owner.ownerKey, owner);
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;
  let renewed = false;
  DO_OWNER_REGISTRY_TEST_STATE.beforeDelIfEqMany = () => {
    if (renewed) return;
    renewed = true;
    DO_OWNER_REGISTRY_TEST_STATE.store.set(key, JSON.stringify({
      ...owner,
      leaseExpiresAt: owner.leaseExpiresAt + HEALTHY_LEASE_MS,
    }));
  };

  const result = await drainOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.deepEqual(result, {
    draining: true,
    owned: 0,
    released: 1,
    alreadyLost: 0,
    errors: [],
  });
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(key), false);
});

test("DO owner registry: drain retains ownership after persistent same-fence release races", async () => {
  const owner = ownerRecord({
    generation: 8,
    leaseExpiresAt: Date.now() + HEALTHY_LEASE_MS,
  });
  const key = ownerKeyOf(owner.ownerKey);
  DO_OWNER_REGISTRY_TEST_STATE.store.set(key, JSON.stringify(owner));
  DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.set(owner.ownerKey, owner);
  DO_OWNER_REGISTRY_TEST_STATE.draining = true;
  let leaseExpiresAt = owner.leaseExpiresAt;
  DO_OWNER_REGISTRY_TEST_STATE.beforeDelIfEqMany = () => {
    leaseExpiresAt += HEALTHY_LEASE_MS;
    DO_OWNER_REGISTRY_TEST_STATE.store.set(key, JSON.stringify({ ...owner, leaseExpiresAt }));
  };

  const result = await drainOwnedScopes({ REDIS_ADDR: "redis:6379" });

  assert.equal(result.released, 0);
  assert.equal(result.alreadyLost, 0);
  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0].error, /owner release raced/i);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.store.has(key), true);
  assert.equal(DO_OWNER_REGISTRY_TEST_STATE.ownedScopes.has(owner.ownerKey), true);
});
