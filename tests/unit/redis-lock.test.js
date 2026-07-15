import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const {
  acquireTokenLock,
  createTokenLock,
  releaseTokenLock,
  renewTokenLock,
} = await importRepositoryModule("shared/redis-lock.js", importSpecifierReplacements({
  "shared-random-id": repositoryFileUrl("shared/random-id.js"),
}));

test("token lock owner acquires and renews with the canonical Redis options", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const client = {
    /** @param {unknown[]} args */
    async set(...args) {
      calls.push(["set", ...args]);
      return "OK";
    },
    async delIfEq() { return 1; },
  };
  const lock = createTokenLock("lock:demo");

  assert.match(lock.token, /^[0-9a-f]{32}$/);
  assert.equal(await acquireTokenLock(client, lock, { ttlSeconds: 30 }), true);
  assert.equal(await renewTokenLock(client, lock, 60), true);
  assert.deepEqual(calls, [
    ["set", "lock:demo", lock.token, { nx: true, ttl: 30 }],
    ["set", "lock:demo", lock.token, { ttl: 60, ifeq: lock.token }],
  ]);
});

test("token lock release is token-scoped and never masks the primary outcome", async () => {
  /** @type {unknown[][]} */
  const seen = [];
  const releaseError = new Error("redis unavailable");
  const client = {
    async set() { return "OK"; },
    /** @param {string} key @param {string} token */
    async delIfEq(key, token) {
      seen.push([key, token]);
      throw releaseError;
    },
  };

  await assert.doesNotReject(() => releaseTokenLock(
    client,
    { key: "lock:demo", token: "token" },
    {
      onError(/** @type {unknown} */ err) {
        assert.equal(err, releaseError);
        throw new Error("logger unavailable");
      },
    }
  ));
  assert.deepEqual(seen, [["lock:demo", "token"]]);
});
