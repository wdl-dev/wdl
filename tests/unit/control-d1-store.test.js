import { afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { createFakeRedis, sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";

function makeRedis() {
  return createFakeRedis();
}

/** @type {{ redis: ReturnType<typeof makeRedis> | null }} */
const CONTROL_D1_STORE_TEST_STATE = { redis: null };
/** @type {typeof globalThis & { __d1StoreTestState?: typeof CONTROL_D1_STORE_TEST_STATE }} */
const d1StoreGlobal = globalThis;
d1StoreGlobal.__d1StoreTestState = CONTROL_D1_STORE_TEST_STATE;

const controlSharedUrl = controlSharedStubUrl(`
export const state = {
  redis: {
    async get(...args) { return /** @type {any} */ (globalThis).__d1StoreTestState.redis.get(...args); },
    async hGetAll(...args) { return /** @type {any} */ (globalThis).__d1StoreTestState.redis.hGetAll(...args); },
    async hSet(...args) { return /** @type {any} */ (globalThis).__d1StoreTestState.redis.hSet(...args); },
    async session(...args) { return /** @type {any} */ (globalThis).__d1StoreTestState.redis.session(...args); },
  },
};
`);

const controlLibUrl = moduleDataUrl(`
export function d1DatabasesKey(ns) { return "d1:databases:" + ns; }
export function d1DatabaseKey(ns, id) { return "d1:database:" + ns + ":" + id; }
export function d1DatabaseNameKey(ns, name) { return "d1:database-name:" + ns + ":" + name; }
export function d1DatabaseReferrersKey(ns, id) { return "d1:database-referrers:" + ns + ":" + id; }
export function d1DatabaseTombstoneKey(ns, id) { return "d1:database-tombstone:" + ns + ":" + id; }
export function d1DatabaseTombstonesKey(ns) { return "d1:database-tombstones:" + ns; }
export function formatD1ReferrerBlockers() { return { blockers: [], malformedReferrerCount: 0 }; }
`);

const sharedRedisUrl = sharedRedisStubUrl();

const modelUrl = repositoryModuleDataUrl("control/d1-model.js", [
  [
    /export \{ splitSqlStatements \} from "shared-sql-splitter";/,
    `export { splitSqlStatements } from ${JSON.stringify(repositoryFileUrl("shared/sql-splitter.js"))};`
  ],
  [/from "shared-hex";/, `from ${JSON.stringify(repositoryFileUrl("shared/hex.js"))};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(repositoryFileUrl("shared/ns-pattern.js"))};`],
]);
const src = applyModuleReplacements(readRepositoryFile("control/d1-store.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(controlLibUrl)};`],
  [/from "shared-redis";/, `from ${JSON.stringify(sharedRedisUrl)};`],
  [/from "control-d1-model";/, `from ${JSON.stringify(modelUrl)};`],
]);

const {
  commitDatabaseMetadata,
  deleteDatabaseMetadata,
  markDatabaseReady,
  resolveDatabaseRefFrom,
  rollbackProvisionalDatabaseMetadata,
} = await import(moduleDataUrl(src));

afterEach(() => {
  CONTROL_D1_STORE_TEST_STATE.redis = null;
});

test("D1 store: create writes provisional metadata then flips ready", async () => {
  const redis = makeRedis();
  CONTROL_D1_STORE_TEST_STATE.redis = redis;
  const created = await commitDatabaseMetadata("demo", "main", "d1_main", "2026-01-01T00:00:00.000Z");
  assert.deepEqual(created, { ok: true, databaseId: "d1_main" });
  const provisional = redis.hashes.get("d1:database:demo:d1_main");
  assert.ok(provisional);
  assert.equal(provisional.state, "provisional");
  assert.equal(await resolveDatabaseRefFrom(redis, "demo", "main"), null);

  const ready = await markDatabaseReady("demo", { databaseId: "d1_main", databaseName: "main" }, "2026-01-01T00:00:01.000Z");
  assert.deepEqual(ready, { ok: true });
  const readyRecord = redis.hashes.get("d1:database:demo:d1_main");
  assert.ok(readyRecord);
  assert.equal("provisionalUntil" in readyRecord, false);
  const resolved = await resolveDatabaseRefFrom(redis, "demo", "main");
  assert.equal(resolved.databaseId, "d1_main");
  assert.equal(resolved.state, "ready");
});

test("D1 store: direct non-ready id does not fall through to name alias", async () => {
  const redis = makeRedis();
  CONTROL_D1_STORE_TEST_STATE.redis = redis;
  await commitDatabaseMetadata("demo", "main", "d1_main", "2026-01-01T00:00:00.000Z");
  await commitDatabaseMetadata("demo", "d1_main", "d1_other", "2026-01-01T00:00:00.000Z");
  await markDatabaseReady("demo", { databaseId: "d1_other", databaseName: "d1_main" }, "2026-01-01T00:00:01.000Z");

  assert.equal(await resolveDatabaseRefFrom(redis, "demo", "d1_main"), null);
});

test("D1 store: rollback removes only provisional metadata", async () => {
  const redis = makeRedis();
  CONTROL_D1_STORE_TEST_STATE.redis = redis;
  await commitDatabaseMetadata("demo", "main", "d1_main", "2026-01-01T00:00:00.000Z");
  const rolledBack = await rollbackProvisionalDatabaseMetadata("demo", { databaseId: "d1_main", databaseName: "main" });
  assert.deepEqual(rolledBack, { rolledBack: true });
  assert.equal(redis.hashes.has("d1:database:demo:d1_main"), false);
  assert.equal(redis.strings.has("d1:database-name:demo:main"), false);
});

test("D1 store: delete writes tombstone before removing active metadata", async () => {
  const redis = makeRedis();
  CONTROL_D1_STORE_TEST_STATE.redis = redis;
  await commitDatabaseMetadata("demo", "main", "d1_main", "2026-01-01T00:00:00.000Z");
  await markDatabaseReady("demo", { databaseId: "d1_main", databaseName: "main" }, "2026-01-01T00:00:01.000Z");

  const deleted = await deleteDatabaseMetadata(
    "demo",
    { databaseId: "d1_main", databaseName: "main", state: "ready" },
    "2026-01-01T00:01:00.000Z",
    "rid-delete"
  );

  assert.equal(deleted.deleted, true);
  assert.equal(redis.hashes.has("d1:database:demo:d1_main"), false);
  assert.equal(redis.strings.has("d1:database-name:demo:main"), false);
  assert.equal(redis.sets.has("d1:databases:demo"), false);
  assert.deepEqual(redis.hashes.get("d1:database-tombstone:demo:d1_main"), {
    namespace: "demo",
    databaseId: "d1_main",
    databaseName: "main",
    dbKey: "demo:d1_main",
    storageNamespace: "wdl-d1-storage-v1",
    state: "tombstoned",
    deletedAt: "2026-01-01T00:01:00.000Z",
    updatedAt: "2026-01-01T00:01:00.000Z",
    requestId: "rid-delete",
    ownerReleaseState: "pending",
    ownerReleaseError: "",
  });
  const tombstoneSet = redis.sets.get("d1:database-tombstones:demo");
  assert.ok(tombstoneSet);
  assert.equal(tombstoneSet.has("d1_main"), true);
});
