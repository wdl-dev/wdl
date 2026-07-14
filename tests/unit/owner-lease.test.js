import assert from "node:assert/strict";
import { test } from "node:test";

import { sharedModuleDataUrl } from "../helpers/load-shared-module.js";

const {
  boundedPositiveIntEnv,
  currentOwnerGenerationCounter,
  nextOwnerGeneration,
  ownerGenerationScopedKey,
  ownerLeaseExpiresAt,
  ownerLeaseExpired,
  ownerScopedKey,
  parseOwnerRecord,
  redisServerTimeMs,
  withOwnerWatchRetries,
} = await import(sharedModuleDataUrl("shared/owner-lease.js"));

test("owner lease helpers encode owner scope keys", () => {
  assert.equal(ownerScopedKey("d1:owner:db:", "tenant-a:d1_main"), "d1:owner:db:tenant-a%3Ad1_main");
  assert.equal(
    ownerGenerationScopedKey("do:owner:scope:", "do_abc:Room:shard0"),
    "do:owner:scope:do_abc%3ARoom%3Ashard0:generation"
  );
});

test("owner lease helpers parse Redis owner records", () => {
  const owner = { taskId: "task-a", generation: 7, leaseExpiresAt: 123 };
  assert.deepEqual(parseOwnerRecord(JSON.stringify(owner)), owner);
  assert.deepEqual(parseOwnerRecord(new TextEncoder().encode(JSON.stringify(owner))), owner);
  assert.equal(parseOwnerRecord(null), null);
  assert.equal(parseOwnerRecord("{not-json"), null);
  assert.equal(parseOwnerRecord(JSON.stringify({ taskId: "missing-generation" })), null);
  assert.equal(parseOwnerRecord(JSON.stringify({ taskId: "zero", generation: 0 })), null);
  assert.equal(parseOwnerRecord(JSON.stringify({ taskId: "fractional", generation: 1.5 })), null);
  assert.equal(parseOwnerRecord(JSON.stringify({ taskId: "unsafe", generation: Number.MAX_SAFE_INTEGER + 1 })), null);
  assert.equal(parseOwnerRecord(JSON.stringify({ taskId: "bad-lease", generation: 1, leaseExpiresAt: "soon" })), null);
});

test("owner lease helpers classify lease expiry", () => {
  assert.equal(ownerLeaseExpired(null, 1000), true);
  assert.equal(ownerLeaseExpired({ leaseExpiresAt: "not-a-number" }, 1000), true);
  assert.equal(ownerLeaseExpired({ leaseExpiresAt: 999 }, 1000), true);
  assert.equal(ownerLeaseExpired({ leaseExpiresAt: 1001 }, 1000), false);
});

test("owner lease helpers derive lease expiry from Redis server time", async () => {
  assert.equal(await redisServerTimeMs({ async time() { return 1_700_000_000_123; } }), 1_700_000_000_123);
  await assert.rejects(
    () => redisServerTimeMs({ async time() { return 1.5; } }),
    /Redis server time is invalid/
  );
  await assert.rejects(
    () => redisServerTimeMs({ async time() { return -1; } }),
    /Redis server time is invalid/
  );

  assert.equal(ownerLeaseExpiresAt(10_000, 30), 40_000);
  assert.equal(ownerLeaseExpiresAt(Number.MAX_SAFE_INTEGER - 500, 1), Number.MAX_SAFE_INTEGER);
  assert.throws(() => ownerLeaseExpiresAt(1.5, 30), /Owner lease time base is invalid/);
  assert.throws(() => ownerLeaseExpiresAt(10_000, 0.5), /Owner lease TTL is invalid/);
  assert.throws(() => ownerLeaseExpiresAt(10_000, 0), /Owner lease TTL is invalid/);
});

test("owner lease helpers derive monotonic owner generations from Redis counters", async () => {
  /** @type {Map<string, string | Uint8Array>} */
  const values = new Map();
  values.set("normal", "7");
  values.set("bytes", new TextEncoder().encode("12"));
  values.set("fractional", "3.5");
  values.set("bad", "not-a-number");
  values.set("negative", "-1");
  values.set("near-max", String(Number.MAX_SAFE_INTEGER - 1));
  values.set("max", String(Number.MAX_SAFE_INTEGER));
  values.set("huge", "9007199254740992");
  const session = {
    /** @param {string} key */
    async get(key) {
      return values.get(key) ?? null;
    },
  };

  assert.equal(await currentOwnerGenerationCounter(session, "normal"), 7);
  assert.equal(await currentOwnerGenerationCounter(session, "bytes"), 12);
  await assert.rejects(
    () => currentOwnerGenerationCounter(session, "fractional"),
    /Owner generation counter is corrupt: fractional/
  );
  await assert.rejects(
    () => currentOwnerGenerationCounter(session, "bad"),
    /Owner generation counter is corrupt: bad/
  );
  await assert.rejects(
    () => currentOwnerGenerationCounter(session, "negative"),
    /Owner generation counter is corrupt: negative/
  );
  await assert.rejects(
    () => currentOwnerGenerationCounter(session, "huge"),
    /Owner generation counter is corrupt: huge/
  );
  assert.equal(await nextOwnerGeneration(session, "normal", 3), 8);
  assert.equal(await nextOwnerGeneration(session, "normal", 10), 11);
  assert.equal(await nextOwnerGeneration(session, "missing", 0), 1);
  assert.equal(await nextOwnerGeneration(session, "near-max", 0), Number.MAX_SAFE_INTEGER);
  assert.equal(await currentOwnerGenerationCounter(session, "max"), Number.MAX_SAFE_INTEGER);
  await assert.rejects(
    () => nextOwnerGeneration(session, "max", 0),
    /Owner generation counter is exhausted: max/
  );
  await assert.rejects(
    () => nextOwnerGeneration(session, "missing", Number.MAX_SAFE_INTEGER),
    /Owner generation counter is exhausted: missing/
  );
  await assert.rejects(
    () => nextOwnerGeneration(session, "bad", 0),
    /Owner generation counter is corrupt: bad/
  );
});

test("owner lease helpers bound positive integer env values", () => {
  assert.equal(boundedPositiveIntEnv({}, "LIMIT", 8, 64), 8);
  assert.equal(boundedPositiveIntEnv({ LIMIT: "0" }, "LIMIT", 8, 64), 8);
  assert.equal(boundedPositiveIntEnv({ LIMIT: 0 }, "LIMIT", 8, 64), 8);
  assert.equal(boundedPositiveIntEnv({ LIMIT: "" }, "LIMIT", 8, 64), 8);
  assert.equal(boundedPositiveIntEnv({ LIMIT: "0.5" }, "LIMIT", 8, 64), 1);
  assert.equal(boundedPositiveIntEnv({ LIMIT: "3.9" }, "LIMIT", 8, 64), 3);
  assert.equal(boundedPositiveIntEnv({ LIMIT: "1000" }, "LIMIT", 8, 64), 64);
});

test("owner lease helpers retry WATCH races and emit tier errors on exhaustion", async () => {
  class FakeWatchError extends Error {}
  let attempts = 0;
  const result = await withOwnerWatchRetries(async () => {
    attempts += 1;
    if (attempts < 2) throw new FakeWatchError("race");
    return "ok";
  }, {
    retries: 3,
    isWatchError: (/** @type {unknown} */ err) => err instanceof FakeWatchError,
    createError: (/** @type {number} */ status, /** @type {string} */ code, /** @type {string} */ message) =>
      Object.assign(new Error(message), { status, code }),
    exhaustedCode: "owner-raced",
    exhaustedMessage: "owner raced",
  });

  assert.equal(result, "ok");
  assert.equal(attempts, 2);

  await assert.rejects(
    withOwnerWatchRetries(async () => {
      throw new FakeWatchError("race");
    }, {
      retries: 2,
      isWatchError: (/** @type {unknown} */ err) => err instanceof FakeWatchError,
      createError: (/** @type {number} */ status, /** @type {string} */ code, /** @type {string} */ message) =>
        Object.assign(new Error(message), { status, code }),
      exhaustedCode: "owner-raced",
      exhaustedMessage: "owner raced",
    }),
    (err) => err instanceof Error &&
      Reflect.get(err, "status") === 503 &&
      Reflect.get(err, "code") === "owner-raced" &&
      err.message === "owner raced"
  );
});

test("owner lease helpers do not retry non-WATCH failures", async () => {
  let attempts = 0;
  await assert.rejects(
    withOwnerWatchRetries(async () => {
      attempts += 1;
      throw new Error("boom");
    }, {
      retries: 3,
      isWatchError: () => false,
      createError: (/** @type {number} */ status, /** @type {string} */ code, /** @type {string} */ message) =>
        Object.assign(new Error(message), { status, code }),
      exhaustedCode: "owner-raced",
      exhaustedMessage: "owner raced",
    }),
    /boom/
  );
  assert.equal(attempts, 1);
});
