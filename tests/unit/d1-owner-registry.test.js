import { test } from "node:test";
import assert from "node:assert/strict";

import {
  normalizeDatabases,
  normalizeTarget,
  ownerGenerationKeyOf,
  ownerLeaseGuardMs,
  ownerKeyOf,
  ownerTtlSeconds,
  observedOwnerMaxEntries,
  observedOwnerTtlMs,
  parseOwner,
  probeTimeoutMs,
  renewConcurrency,
} from "../helpers/load-d1-owner-registry.js";

test("D1 owner registry: config helpers keep bounded defaults", () => {
  assert.equal(ownerTtlSeconds({}), 120);
  assert.equal(ownerTtlSeconds({ D1_OWNER_TTL_SECONDS: "0" }), 120);
  assert.equal(ownerTtlSeconds({ D1_OWNER_TTL_SECONDS: "45" }), 45);
  assert.equal(ownerLeaseGuardMs({}), 1000);
  assert.equal(ownerLeaseGuardMs({ D1_OWNER_LEASE_GUARD_MS: "0" }), 0);
  assert.equal(ownerLeaseGuardMs({ D1_OWNER_LEASE_GUARD_MS: "250" }), 250);
  assert.equal(ownerLeaseGuardMs({ D1_OWNER_LEASE_GUARD_MS: "-1" }), 1000);
  assert.equal(probeTimeoutMs({}), 500);
  assert.equal(probeTimeoutMs({ D1_PROBE_TIMEOUT_MS: "-1" }), 500);
  assert.equal(probeTimeoutMs({ D1_PROBE_TIMEOUT_MS: "75" }), 75);
  assert.equal(observedOwnerTtlMs({}), 30_000);
  assert.equal(observedOwnerTtlMs({ D1_OBSERVED_OWNER_TTL_MS: "-1" }), 30_000);
  assert.equal(observedOwnerTtlMs({ D1_OBSERVED_OWNER_TTL_MS: "0" }), 0);
  assert.equal(observedOwnerTtlMs({ D1_OBSERVED_OWNER_TTL_MS: 0 }), 0);
  assert.equal(observedOwnerTtlMs({ D1_OBSERVED_OWNER_TTL_MS: "" }), 30_000);
  assert.equal(observedOwnerMaxEntries({}), 10_000);
  assert.equal(observedOwnerMaxEntries({ D1_OBSERVED_OWNER_MAX_ENTRIES: "-1" }), 10_000);
  assert.equal(observedOwnerMaxEntries({ D1_OBSERVED_OWNER_MAX_ENTRIES: "0" }), 0);
  assert.equal(observedOwnerMaxEntries({ D1_OBSERVED_OWNER_MAX_ENTRIES: 0 }), 0);
  assert.equal(observedOwnerMaxEntries({ D1_OBSERVED_OWNER_MAX_ENTRIES: "" }), 10_000);
  assert.equal(renewConcurrency({}), 8);
  assert.equal(renewConcurrency({ D1_RENEW_CONCURRENCY: "0" }), 8);
  assert.equal(renewConcurrency({ D1_RENEW_CONCURRENCY: "0.5" }), 1);
  assert.equal(renewConcurrency({ D1_RENEW_CONCURRENCY: "3.9" }), 3);
  assert.equal(renewConcurrency({ D1_RENEW_CONCURRENCY: "1000" }), 64);
});

test("D1 owner registry: owner keys encode database keys safely", () => {
  assert.equal(ownerKeyOf("tenant-a:d1_main"), "d1:owner:db:tenant-a%3Ad1_main");
  assert.equal(ownerGenerationKeyOf("tenant-a:d1_main"), "d1:owner:db:tenant-a%3Ad1_main:generation");
});

test("D1 owner registry: parseOwner accepts Redis strings and bulk bytes", () => {
  const [identity] = normalizeDatabases([{ namespace: "tenant-a", databaseId: "d1_main" }]);
  const owner = {
    ...identity,
    taskId: "task-a",
    endpoint: "d1-runtime-a:8787",
    generation: 3,
    leaseExpiresAt: Date.now() + 1000,
  };
  assert.deepEqual(parseOwner(JSON.stringify(owner), identity.dbKey), owner);
  assert.deepEqual(parseOwner(new TextEncoder().encode(JSON.stringify(owner)), identity.dbKey), owner);
  assert.equal(parseOwner(null, identity.dbKey), null);
  assert.throws(
    () => parseOwner(JSON.stringify({ ...owner, endpoint: "8.8.8.8:8787" }), identity.dbKey),
    /D1 owner record is invalid/
  );
  assert.throws(
    () => parseOwner(JSON.stringify({ ...owner, dbKey: "tenant-b:db-b" }), identity.dbKey),
    /D1 owner record is invalid/
  );
  assert.throws(() => parseOwner("{not-json", identity.dbKey), /D1 owner record is invalid/);
});

test("D1 owner registry: rebalance request normalization validates shape", () => {
  const databases = normalizeDatabases([{ namespace: "tenant-a", databaseId: "d1_main" }]);
  assert.equal(databases[0].dbKey, "tenant-a:d1_main");
  assert.ok(databases[0].slot >= 0);

  assert.deepEqual(normalizeTarget(null), null);
  assert.deepEqual(
    normalizeTarget({ taskId: "task-b", endpoint: "10.0.0.2:8787" }),
    { taskId: "task-b", endpoint: "10.0.0.2:8787" }
  );
  assert.throws(() => normalizeDatabases([]), /databases must be a non-empty array/);
  assert.throws(
    () => normalizeDatabases([{ namespace: "tenant-a", databaseId: "db:bad" }]),
    /databaseId is invalid/
  );
  assert.throws(
    () => normalizeDatabases([{ namespace: "admin", databaseId: "main" }]),
    /namespace is invalid/
  );
  assert.throws(() => normalizeTarget({ taskId: "task-b" }), /target.endpoint is required/);
  assert.throws(
    () => normalizeTarget({ taskId: "task-b", endpoint: "8.8.8.8:8787" }),
    /target.endpoint is invalid/
  );
});
