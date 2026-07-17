import { doProtocolDataUrl } from "./load-do-protocol.js";
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

const PROTOCOL_URL = doProtocolDataUrl();
const FAKE_REDIS_URL = repositoryFileUrl("tests/helpers/mocks/fake-redis.js");
const SHARED_ENV_URL = repositoryFileUrl("shared/env.js");
const SHARED_OWNER_LEASE_URL = sharedModuleDataUrl("shared/owner-lease.js");
const WORKER_CONTRACT_URL = sharedModuleDataUrl("shared/worker-contract.js");
const SHARED_OWNER_PROTOCOL_URL = repositoryModuleDataUrl("shared/owner-protocol.js", [
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
]);

const redisState = createFakeRedisState();

/**
 * @typedef {object} DoOwnerRegistryTestState
 * @property {ReturnType<typeof createFakeRedisState>} redisState
 * @property {Map<string, string>} store
 * @property {Map<string, unknown>} ownedScopes
 * @property {{ taskId: string, endpoint: string }} taskIdentity
 * @property {string[]} watchedKeys
 * @property {unknown[]} metricIncrements
 * @property {Array<{ level: string, event: string, fields: Record<string, unknown> }>} logEntries
 * @property {number} redisTimeMs
 * @property {number[]} redisTimeSequence
 * @property {(() => void) | null} onExecFailure
 * @property {boolean} draining
 * @property {number} inFlightDispatches
 */
/** @type {DoOwnerRegistryTestState} */
export const DO_OWNER_REGISTRY_TEST_STATE = {
  redisState,
  store: redisState.strings,
  ownedScopes: new Map(),
  taskIdentity: { taskId: "task-a", endpoint: "do-runtime-a:8788" },
  watchedKeys: redisState.watched,
  metricIncrements: [],
  logEntries: [],
  redisTimeMs: Date.now(),
  redisTimeSequence: [],
  onExecFailure: null,
  draining: false,
  inFlightDispatches: 0,
};

/** @type {typeof globalThis & { __doOwnerRegistryTestState?: typeof DO_OWNER_REGISTRY_TEST_STATE }} */
const doOwnerRegistryGlobal = globalThis;
doOwnerRegistryGlobal.__doOwnerRegistryTestState = DO_OWNER_REGISTRY_TEST_STATE;

export function resetDoOwnerRegistryTestState() {
  const testState = DO_OWNER_REGISTRY_TEST_STATE;
  resetFakeRedisState(testState.redisState);
  testState.ownedScopes.clear();
  testState.taskIdentity = { taskId: "task-a", endpoint: "do-runtime-a:8788" };
  testState.metricIncrements = [];
  testState.logEntries = [];
  testState.redisTimeMs = Date.now();
  testState.redisTimeSequence = [];
  testState.onExecFailure = null;
  testState.draining = false;
  testState.inFlightDispatches = 0;
}

export function doOwnerRegistryWriteCommands() {
  return DO_OWNER_REGISTRY_TEST_STATE.redisState.commands.filter((command) => (
    ["set", "del", "hSet", "hDel", "sAdd", "sRem", "zAdd", "zRem", "copy", "publish", "expireAt"]
      .includes(/** @type {string} */ (command[0]))
  ));
}

const stateUrl = moduleDataUrl(`
function testState() { return /** @type {any} */ (globalThis).__doOwnerRegistryTestState; }
export const ownedScopes = testState().ownedScopes;
export const SERVICE = "do-runtime";
export function isDraining() { return testState().draining === true; }
export function setDraining(value = true) { testState().draining = value === true; }
export function currentInFlightDispatches() { return testState().inFlightDispatches || 0; }
export function log(level, event, fields = {}) {
  testState().logEntries.push({ level, event, fields });
}
export const metrics = {
  increment(name, labels) {
    testState().metricIncrements.push({ name, labels });
  },
};
`);

const taskIdentityUrl = moduleDataUrl(`
function testState() { return /** @type {any} */ (globalThis).__doOwnerRegistryTestState; }
export async function resolveTaskIdentity() {
  return testState().taskIdentity;
}
`);

const sharedRedisUrl = sharedRedisStubUrl();

const redisUrl = moduleDataUrl(`
import { createFakeRedisClient } from ${JSON.stringify(FAKE_REDIS_URL)};
function testState() { return /** @type {any} */ (globalThis).__doOwnerRegistryTestState; }
export function createRedisClient() {
  return createFakeRedisClient(testState().redisState, {
    encodeGet: true,
    nowMs: () => {
      const sequence = testState().redisTimeSequence;
      if (Array.isArray(sequence) && sequence.length) return sequence.shift();
      return testState().redisTimeMs;
    },
    onExecFailure: () => testState().onExecFailure?.(),
  });
}
`);

const src = applyModuleReplacements(readRepositoryFile("do-runtime/owner-registry.js"), [
  [/from "do-runtime-protocol";/, `from ${JSON.stringify(PROTOCOL_URL)};`],
  [/from "do-runtime-task-identity";/, `from ${JSON.stringify(taskIdentityUrl)};`],
  [/from "shared-redis";/, `from ${JSON.stringify(sharedRedisUrl)};`],
  [/from "do-runtime-redis";/, `from ${JSON.stringify(redisUrl)};`],
  [/from "shared-env";/, `from ${JSON.stringify(SHARED_ENV_URL)};`],
  [/from "shared-owner-lease";/, `from ${JSON.stringify(SHARED_OWNER_LEASE_URL)};`],
  [/from "shared-owner-protocol";/, `from ${JSON.stringify(SHARED_OWNER_PROTOCOL_URL)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(WORKER_CONTRACT_URL)};`],
  [/from "do-runtime-state";/, `from ${JSON.stringify(stateUrl)};`],
  [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(repositoryFileUrl("shared/ns-pattern.js"))};`],
  [/from "shared-owner-endpoint";/, `from ${JSON.stringify(repositoryFileUrl("shared/owner-endpoint.js"))};`],
]);

const registry = await import(moduleDataUrl(src));

export const {
  assertCurrentOwner,
  assertCurrentOwnerWithLeaseBudget,
  drainOwnedScopes,
  ownerGenerationKeyOf,
  ownerLeaseGuardMs,
  ownerKeyOf,
  parseOwner,
  renewOwnedScopes,
  releaseOwner,
  resolveDoOwner,
  shouldRenewOwnerLease,
} = registry;
