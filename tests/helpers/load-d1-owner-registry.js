import { d1ProtocolDataUrl } from "./load-d1-protocol.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  repositoryModuleDataUrl,
  sharedModuleDataUrl,
} from "./load-shared-module.js";
import {
  createFakeRedisState,
  resetFakeRedisState,
  sharedRedisStubUrl,
} from "./mocks/fake-redis.js";
import { delay } from "./timing.js";

const FAKE_REDIS_URL = repositoryFileUrl("tests/helpers/mocks/fake-redis.js");
const SHARED_ENV_URL = repositoryFileUrl("shared/env.js");
const SHARED_OWNER_LEASE_URL = sharedModuleDataUrl("shared/owner-lease.js");
const SHARED_OWNER_PROTOCOL_URL = repositoryModuleDataUrl("shared/owner-protocol.js", [
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
]);
const redisState = createFakeRedisState();

/** @type {any} */
export const D1_OWNER_REGISTRY_TEST_STATE =
  /** @type {any} */ (globalThis).__d1OwnerRegistryTestState = {
    redisState,
    registryStore: redisState.strings,
    ownedDbs: new Map(),
    observedOwners: new Map(),
    watchedKeys: redisState.watched,
    get setCommands() {
      return redisState.commands.filter((command) => command[0] === "set");
    },
    get watchExecFailures() {
      return redisState.execFailures;
    },
    set watchExecFailures(value) {
      redisState.execFailures = Number(value) || 0;
    },
  };

export function resetD1OwnerRegistryTestState() {
  resetFakeRedisState(D1_OWNER_REGISTRY_TEST_STATE.redisState);
  D1_OWNER_REGISTRY_TEST_STATE.ownedDbs.clear();
  D1_OWNER_REGISTRY_TEST_STATE.observedOwners.clear();
  D1_OWNER_REGISTRY_TEST_STATE.forgottenStorageSizes = [];
  D1_OWNER_REGISTRY_TEST_STATE.pendingQueries = 0;
  D1_OWNER_REGISTRY_TEST_STATE.draining = false;
  D1_OWNER_REGISTRY_TEST_STATE.taskIdentity = { taskId: "task-a", endpoint: "d1-runtime-a:8787" };
  D1_OWNER_REGISTRY_TEST_STATE.onWatchExecFailure = null;
  D1_OWNER_REGISTRY_TEST_STATE.logEntries = [];
  D1_OWNER_REGISTRY_TEST_STATE.metricIncrements = [];
  D1_OWNER_REGISTRY_TEST_STATE.sessionDelayMs = 0;
  D1_OWNER_REGISTRY_TEST_STATE.sessionConcurrency = 0;
  D1_OWNER_REGISTRY_TEST_STATE.sessionConcurrencyMax = 0;
  D1_OWNER_REGISTRY_TEST_STATE.redisGets = 0;
  D1_OWNER_REGISTRY_TEST_STATE.redisTimes = 0;
  D1_OWNER_REGISTRY_TEST_STATE.redisTimeMs = Date.now();
  D1_OWNER_REGISTRY_TEST_STATE.redisTimeSequence = [];
  D1_OWNER_REGISTRY_TEST_STATE.delay = delay;
}

resetD1OwnerRegistryTestState();

const stateUrl = moduleDataUrl(`
const D1_TEST_STATE = /** @type {any} */ (globalThis).__d1OwnerRegistryTestState;
export function isDraining() { return D1_TEST_STATE.draining === true; }
export function setDraining(value) { D1_TEST_STATE.draining = value === true; }
export function log(level, event, fields = {}) {
  D1_TEST_STATE.logEntries.push({ level, event, fields });
}
export const metrics = {
  increment(name, labels) {
    D1_TEST_STATE.metricIncrements.push({ name, labels });
  },
};
export const ownedDbs = D1_TEST_STATE.ownedDbs;
export const observedD1Owners = D1_TEST_STATE.observedOwners;
export function forgetStorageSize(dbKey) {
  D1_TEST_STATE.forgottenStorageSizes.push(dbKey);
}
export function pendingQueryCount() { return Number(D1_TEST_STATE.pendingQueries || 0); }
export const SERVICE = "d1-runtime";
`);

const taskIdentityUrl = moduleDataUrl(`
export async function resolveTaskIdentity() {
  return /** @type {any} */ (globalThis).__d1OwnerRegistryTestState.taskIdentity;
}
`);

const redisUrl = sharedRedisStubUrl();

const redisClientUrl = moduleDataUrl(`
import { createFakeRedisClient, createFakeRedisSession } from ${JSON.stringify(FAKE_REDIS_URL)};

function testState() { return /** @type {any} */ (globalThis).__d1OwnerRegistryTestState; }

function redisNow() {
  const sequence = testState().redisTimeSequence;
  if (Array.isArray(sequence) && sequence.length) return sequence.shift();
  return testState().redisTimeMs;
}

function fakeRedisOptions() {
  return {
    nowMs: redisNow,
    onExecFailure(commands, remainingFailures) {
      const state = testState();
      if (typeof state.onWatchExecFailure === "function") {
        state.onWatchExecFailure(commands, remainingFailures);
      }
    },
  };
}

function wrapRedis(redis, { countGets = true } = {}) {
  return {
    ...redis,
    async get(key) {
      if (countGets) testState().redisGets += 1;
      return await redis.get(key);
    },
    async getWithTime(key) {
      return {
        value: await this.get(key),
        nowMs: await this.time(),
      };
    },
    async time() {
      testState().redisTimes += 1;
      return await redis.time();
    },
  };
}

function d1RedisClient() {
  const state = testState();
  const redisState = state.redisState;
  const client = wrapRedis(createFakeRedisClient(redisState, fakeRedisOptions()));
  return {
    ...client,
    async session(fn) {
      state.sessionConcurrency += 1;
      state.sessionConcurrencyMax = Math.max(
        state.sessionConcurrencyMax,
        state.sessionConcurrency
      );
      try {
        if (state.sessionDelayMs > 0) await state.delay(state.sessionDelayMs);
        return await fn(wrapRedis(createFakeRedisSession(redisState, fakeRedisOptions()), {
          countGets: false,
        }));
      } finally {
        state.sessionConcurrency -= 1;
      }
    },
  };
}

export function createRequiredRedisClient(env, ErrorClass, code, message) {
  if (!env.REDIS_ADDR) throw new ErrorClass(503, code, message);
  return d1RedisClient();
}
`);


const src = applyModuleReplacements(readRepositoryFile("d1-runtime/owner-registry.js"), [
  [/from "d1-runtime-protocol";/, `from ${JSON.stringify(d1ProtocolDataUrl())};`],
  [/from "d1-runtime-task-identity";/, `from ${JSON.stringify(taskIdentityUrl)};`],
  [/from "shared-redis";/, `from ${JSON.stringify(redisUrl)};`],
  [/from "shared-env";/, `from ${JSON.stringify(SHARED_ENV_URL)};`],
  [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  [/from "shared-owner-endpoint";/, `from ${JSON.stringify(repositoryFileUrl("shared/owner-endpoint.js"))};`],
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
  [/from "shared-owner-protocol";/, `from ${JSON.stringify(SHARED_OWNER_PROTOCOL_URL)};`],
  [/from "shared-redis-client";/, `from ${JSON.stringify(redisClientUrl)};`],
  [/from "d1-runtime-state";/, `from ${JSON.stringify(stateUrl)};`],
]);

const registry = await import(moduleDataUrl(src));

export const {
  assertCurrentOwnerWithLeaseBudget,
  drainOwnedDbs,
  drainConcurrency,
  normalizeDatabases,
  normalizeTarget,
  ownerGenerationKeyOf,
  ownerKeyOf,
  ownerLeaseGuardMs,
  ownerTtlSeconds,
  observedOwnerMaxEntries,
  observedOwnerTtlMs,
  parseOwner,
  probeTimeoutMs,
  rebalanceDatabase,
  renewConcurrency,
  renewOwnedDbs,
  resolveDbOwner,
  takeoverExpiredOwner,
} = registry;

/** @param {Record<string, unknown>} env @param {Record<string, unknown>} owner */
export async function assertCurrentD1Owner(env, owner) {
  return (await assertCurrentOwnerWithLeaseBudget(env, owner)).owner;
}
