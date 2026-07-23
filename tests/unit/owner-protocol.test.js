import assert from "node:assert/strict";
import { test } from "node:test";

import { repositoryModuleDataUrl, sharedModuleDataUrl } from "../helpers/load-shared-module.js";

const SHARED_OWNER_LEASE_URL = sharedModuleDataUrl("shared/owner-lease.js");
const {
  ownerFenceMatches,
  ownerProtocolKeys,
  releaseOwnerRecords,
  readOwnerRecord,
  readOwnerRecordWithRedisTime,
  readOwnerSnapshotWithRedisTime,
  stageOwnerClaim,
  stageOwnerRelease,
  stageOwnerRenew,
} = await import(repositoryModuleDataUrl("shared/owner-protocol.js", [
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
]));

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

test("owner protocol helpers derive owner and generation keys together", () => {
  assert.deepEqual(ownerProtocolKeys("d1:owner:db:", "tenant-a:d1_main"), {
    ownerKey: "d1:owner:db:tenant-a%3Ad1_main",
    generationKey: "d1:owner:db:tenant-a%3Ad1_main:generation",
  });
});

test("owner protocol helpers compare generation fences without accepting partial records", () => {
  assert.equal(ownerFenceMatches({ taskId: "task-a", generation: 3 }, { taskId: "task-a", generation: 3 }), true);
  assert.equal(ownerFenceMatches({ taskId: "task-b", generation: 3 }, { taskId: "task-a", generation: 3 }), false);
  assert.equal(ownerFenceMatches({ taskId: "task-a", generation: 4 }, { taskId: "task-a", generation: 3 }), false);
  assert.equal(ownerFenceMatches({ taskId: "task-a" }, { taskId: "task-a", generation: 3 }), false);
  assert.equal(ownerFenceMatches(null, { taskId: "task-a", generation: 3 }), false);
});

test("owner protocol helpers read owner records through caller-supplied parser", async () => {
  const parsed = await readOwnerRecord({
    /** @param {string} key */
    async get(key) {
      assert.equal(key, "owner-key");
      return "raw-owner";
    },
  }, "owner-key", (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => ({ raw }));
  assert.deepEqual(parsed, { raw: "raw-owner" });
});

test("owner protocol helpers read owner records and Redis time together", async () => {
  const result = await readOwnerRecordWithRedisTime({
    /** @param {string} key */
    async getWithTime(key) {
      assert.equal(key, "owner-key");
      return { value: "raw-owner", nowMs: 1_700_000_000_123 };
    },
  }, "owner-key", (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => ({ raw }));

  assert.deepEqual(result, {
    owner: { raw: "raw-owner" },
    rawOwner: "raw-owner",
    nowMs: 1_700_000_000_123,
  });
});

test("owner protocol helpers reject invalid Redis time from combined owner reads", async () => {
  await assert.rejects(
    () => readOwnerRecordWithRedisTime({
      async getWithTime() {
        return { value: "raw-owner", nowMs: -1 };
      },
    }, "owner-key", (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => ({ raw })),
    /Redis server time is invalid/
  );
});

test("owner protocol helpers read related owner state in the timed snapshot", async () => {
  const result = await readOwnerSnapshotWithRedisTime({
    /** @param {string[]} keys */
    async getManyWithTime(keys) {
      assert.deepEqual(keys, ["owner-key", "delete-lock"]);
      return {
        values: ["raw-owner", "whole:token"],
        nowMs: 1_700_000_000_456,
      };
    },
  }, "owner-key", ["delete-lock"], (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => ({ raw }));

  assert.deepEqual(result, {
    owner: { raw: "raw-owner" },
    rawOwner: "raw-owner",
    relatedValues: ["whole:token"],
    nowMs: 1_700_000_000_456,
  });
});

test("owner protocol release preserves raw bytes and rereads CAS races", async () => {
  const firstRaw = textEncoder.encode('{"taskId":"task-a","generation":3,"padding":"  "}');
  const secondRaw = textEncoder.encode('{"taskId":"task-a","generation":4}');
  const replacementRaw = textEncoder.encode('{"taskId":"task-b","generation":5}');
  let readCount = 0;
  const results = await releaseOwnerRecords({
    /** @param {string[]} keys */
    async getMany(keys) {
      readCount += 1;
      if (readCount === 1) {
        assert.deepEqual(keys, ["owner-a", "owner-b"]);
        return [firstRaw, secondRaw];
      }
      assert.deepEqual(keys, ["owner-b"]);
      return [replacementRaw];
    },
    /** @param {Array<[string, string | Uint8Array]>} entries */
    async delIfEqMany(entries) {
      assert.equal(entries[0][1], firstRaw, "release must compare the exact stored bytes");
      assert.equal(entries[1][1], secondRaw, "release must not reserialize owner records");
      return [1, 0];
    },
  }, [
    { ownerKey: "owner-a", expected: { taskId: "task-a", generation: 3 } },
    { ownerKey: "owner-b", expected: { taskId: "task-a", generation: 4 } },
  ], (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => (
    raw == null ? null : JSON.parse(typeof raw === "string" ? raw : textDecoder.decode(raw))
  ));

  assert.deepEqual(results, [
    { released: true, owner: null },
    { released: false, owner: { taskId: "task-b", generation: 5 } },
  ]);
});

test("owner protocol release retries exact-byte races while the owner fence still matches", async () => {
  const initialRaw = JSON.stringify({ taskId: "task-a", generation: 3, leaseExpiresAt: 1000 });
  const renewedRaw = JSON.stringify({ taskId: "task-a", generation: 3, leaseExpiresAt: 2000 });
  /** @type {string | null} */
  let currentRaw = initialRaw;
  let deleteAttempts = 0;
  const results = await releaseOwnerRecords({
    /** @param {string[]} keys */
    async getMany(keys) {
      assert.deepEqual(keys, ["owner-a"]);
      return [currentRaw];
    },
    /** @param {Array<[string, string | Uint8Array]>} entries */
    async delIfEqMany(entries) {
      deleteAttempts += 1;
      assert.deepEqual(entries, [["owner-a", deleteAttempts === 1 ? initialRaw : renewedRaw]]);
      if (deleteAttempts === 1) {
        currentRaw = renewedRaw;
        return [0];
      }
      currentRaw = null;
      return [1];
    },
  }, [
    { ownerKey: "owner-a", expected: { taskId: "task-a", generation: 3 } },
  ], (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => (
    raw == null ? null : JSON.parse(String(raw))
  ));

  assert.equal(deleteAttempts, 2);
  assert.deepEqual(results, [{ released: true, owner: null }]);
});

test("owner protocol release reports persistent same-fence races as errors", async () => {
  let leaseExpiresAt = 1000;
  let deleteAttempts = 0;
  const results = await releaseOwnerRecords({
    async getMany() {
      return [JSON.stringify({ taskId: "task-a", generation: 3, leaseExpiresAt })];
    },
    async delIfEqMany() {
      deleteAttempts += 1;
      if (deleteAttempts > 3) throw new Error("owner release retry budget exceeded");
      leaseExpiresAt += 1000;
      return [0];
    },
  }, [
    { ownerKey: "owner-a", expected: { taskId: "task-a", generation: 3 } },
  ], (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => (
    raw == null ? null : JSON.parse(String(raw))
  ));

  assert.ok(deleteAttempts > 1 && deleteAttempts <= 3);
  assert.equal(results[0].released, false);
  assert.equal(results[0].owner, null);
  assert.match(String(results[0].error), /owner release raced/i);
});

test("owner protocol release preserves completed results when a retry fails", async () => {
  const rawA = JSON.stringify({ taskId: "task-a", generation: 1 });
  const rawB = JSON.stringify({ taskId: "task-b", generation: 2, leaseExpiresAt: 1000 });
  const renewedB = JSON.stringify({ taskId: "task-b", generation: 2, leaseExpiresAt: 2000 });
  let deleteAttempts = 0;
  const results = await releaseOwnerRecords({
    /** @param {string[]} keys */
    async getMany(keys) {
      if (keys.length === 2) return [rawA, rawB];
      assert.deepEqual(keys, ["owner-b"]);
      return [renewedB];
    },
    async delIfEqMany() {
      deleteAttempts += 1;
      if (deleteAttempts === 1) return [1, 0];
      throw new Error("retry transport failed");
    },
  }, [
    { ownerKey: "owner-a", expected: { taskId: "task-a", generation: 1 } },
    { ownerKey: "owner-b", expected: { taskId: "task-b", generation: 2 } },
  ], (/** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw) => (
    raw == null ? null : JSON.parse(String(raw))
  ));

  assert.equal(deleteAttempts, 2);
  assert.deepEqual(results[0], { released: true, owner: null });
  assert.equal(results[1].released, false);
  assert.equal(results[1].owner, null);
  assert.match(String(results[1].error), /retry transport failed/);
});

test("owner protocol release bounds batches and isolates transport failures", async () => {
  const entries = Array.from({ length: 513 }, (_, index) => ({
    ownerKey: `owner-${index}`,
    expected: { taskId: "task-a", generation: index },
  }));
  /** @type {number[]} */
  const getBatchSizes = [];
  /** @type {number[]} */
  const deleteBatchSizes = [];
  let snapshotBatch = 0;
  const results = await releaseOwnerRecords({
    /** @param {string[]} keys */
    async getMany(keys) {
      getBatchSizes.push(keys.length);
      snapshotBatch += 1;
      if (snapshotBatch === 2) throw new Error("middle batch unavailable");
      return keys.map((key) => {
        const generation = Number(key.slice("owner-".length));
        return JSON.stringify({ taskId: "task-a", generation });
      });
    },
    /** @param {Array<[string, string | Uint8Array]>} batch */
    async delIfEqMany(batch) {
      deleteBatchSizes.push(batch.length);
      return batch.map(() => 1);
    },
  }, entries, (
    /** @type {string | Uint8Array | ArrayBuffer | null | undefined} */ raw
  ) => raw == null ? null : /** @type {{ taskId: string, generation: number }} */ (
    JSON.parse(String(raw))
  ));

  assert.deepEqual(getBatchSizes, [256, 256, 1]);
  assert.deepEqual(deleteBatchSizes, [256, 1]);
  assert.equal(results.length, entries.length);
  assert.equal(results.slice(0, 256).every(
    (/** @type {{ released: boolean }} */ result) => result.released
  ), true);
  assert.equal(results.slice(256, 512).every(
    (/** @type {{ error?: unknown }} */ result) => result.error instanceof Error
  ), true);
  assert.equal(results[512].released, true);
});

test("owner protocol helpers stage claim, renew, and release writes", () => {
  /** @type {Array<unknown[]>} */
  const commands = [];
  const multi = {
    /**
     * @param {string} key
     * @param {string} value
     * @param {{ ttl?: number }} [options]
     */
    set(key, value, options = undefined) {
      commands.push(["set", key, value, options]);
      return this;
    },
    /** @param {...string} keys */
    del(...keys) {
      commands.push(["del", ...keys]);
      return this;
    },
  };
  const owner = { taskId: "task-a", generation: 5 };

  assert.equal(stageOwnerClaim(multi, { ownerKey: "owner", generationKey: "owner:generation" }, owner, 120), multi);
  assert.equal(stageOwnerRenew(multi, "owner", owner, 60), multi);
  assert.equal(stageOwnerRelease(multi, "owner"), multi);
  assert.deepEqual(commands, [
    ["set", "owner:generation", "5", undefined],
    ["set", "owner", JSON.stringify(owner), { ttl: 120 }],
    ["set", "owner", JSON.stringify(owner), { ttl: 60 }],
    ["del", "owner"],
  ]);
});
