import assert from "node:assert/strict";
import { test } from "node:test";

import { repositoryModuleDataUrl, sharedModuleDataUrl } from "../helpers/load-shared-module.js";

const SHARED_OWNER_LEASE_URL = sharedModuleDataUrl("shared/owner-lease.js");
const {
  ownerFenceMatches,
  ownerProtocolKeys,
  readOwnerRecord,
  readOwnerRecordWithRedisTime,
  readOwnerSnapshotWithRedisTime,
  stageOwnerClaim,
  stageOwnerRelease,
  stageOwnerRenew,
} = await import(repositoryModuleDataUrl("shared/owner-protocol.js", [
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
]));

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
    relatedValues: ["whole:token"],
    nowMs: 1_700_000_000_456,
  });
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
