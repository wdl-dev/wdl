import {
  applyModuleReplacements,
  readRepositoryFile,
  moduleDataUrl,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { doProtocolDataUrl } from "./load-do-protocol.js";
import { OBSERVABILITY_NOOP_URL } from "./mocks/observability.js";

/**
 * @typedef {{
 *   assertResponses: any[],
 *   assertArguments: Array<{ owner: unknown, options: unknown }>,
 *   actorInvokes: any[],
 *   remembered: any[],
 *   deletedFacets: string[],
 *   aborts: any[],
 *   draining: boolean,
 *   inFlight: number,
 *   logs: any[],
 *   gaugeSamples: any[],
 *   assertCalls: number,
 *   forgottenOwners: string[],
 *   registryError?: unknown,
 *   registryWait?: Promise<void>,
 *   registryWaitStarted?: (() => void),
 *   abortReject?: ((reason: unknown) => void),
 * }} DoHostActorHarnessState
 */
/** @type {typeof globalThis & { __doActorTestState?: DoHostActorHarnessState }} */
const doActorGlobal = globalThis;
/** @type {DoHostActorHarnessState} */
const DO_ACTOR_TEST_STATE = {
  assertResponses: [],
  assertArguments: [],
  actorInvokes: [],
  remembered: [],
  deletedFacets: [],
  aborts: [],
  draining: false,
  inFlight: 0,
  logs: [],
  gaugeSamples: [],
  assertCalls: 0,
  forgottenOwners: [],
};
doActorGlobal.__doActorTestState = DO_ACTOR_TEST_STATE;

const durableObjectStub = "class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }";
const productionProtocolUrl = doProtocolDataUrl();

const loadUrl = moduleDataUrl(`
export function loadDoWorkerCode() {
  throw new Error("loadDoWorkerCode should not be called");
}
`);

const objectRegistryUrl = moduleDataUrl(`
export function objectRegistryMember(invoke) {
  return [invoke.className, invoke.objectName].join(":");
}
export async function rememberDoObject(env, invoke) {
  if (globalThis.__doActorTestState.registryWait) {
    globalThis.__doActorTestState.registryWaitStarted?.();
    await globalThis.__doActorTestState.registryWait;
  }
  if (globalThis.__doActorTestState.registryError) throw globalThis.__doActorTestState.registryError;
  globalThis.__doActorTestState.remembered.push({ env, invoke });
}
`);

const protocolUrl = moduleDataUrl(`
export {
  DO_OWNERSHIP_CODE,
  DO_OWNERSHIP_ERROR_CONTROL_HEADER,
  DoRuntimeError,
  buildRpcRequest,
  doPlatformErrorResponse,
} from ${JSON.stringify(productionProtocolUrl)};
export function buildFacetName(invoke) {
  return [invoke.className, invoke.objectName].join(":");
}
export function buildAlarmRequest() { throw new Error("unexpected alarm request"); }
export function buildForwardRequest() { throw new Error("unexpected forward request"); }
export function normalizeDoConnectRequest() { throw new Error("unexpected connect normalize"); }
export async function readLocalActorInvokeRequest() {
  const next = globalThis.__doActorTestState.actorInvokes.shift();
  if (!next) throw new Error("missing actor invoke request");
  return next;
}
`);

const ownerRegistryUrl = moduleDataUrl(`
export async function assertCurrentOwner() {
  globalThis.__doActorTestState.assertCalls += 1;
  const next = globalThis.__doActorTestState.assertResponses.shift();
  if (next instanceof Error) throw next;
  if (!next) throw new Error("missing assertCurrentOwner response");
  return await next;
}
export async function assertCurrentOwnerWithLeaseBudget(_env, ownerFence, options) {
  globalThis.__doActorTestState.assertArguments.push({ owner: ownerFence, options });
  const owner = await assertCurrentOwner();
  return {
    owner,
    leaseRemainingMs: Number(owner.leaseRemainingMs ?? (Number(owner.leaseExpiresAt ?? 0) - Date.now())),
  };
}
export function ownerLeaseGuardMs(env) {
  const raw = Number(env?.DO_OWNER_LEASE_GUARD_MS ?? 1000);
  return Number.isFinite(raw) && raw >= 0 ? Math.trunc(raw) : 1000;
}
export function forgetOwnedScope(ownerKey) {
  globalThis.__doActorTestState.forgottenOwners.push(ownerKey);
}
`);

const stateUrl = moduleDataUrl(`
export const SERVICE = "do-runtime";
export function beginInFlightDispatch() {
  if (globalThis.__doActorTestState.draining) return false;
  globalThis.__doActorTestState.inFlight += 1;
  return true;
}
export function endInFlightDispatch() {
  if (globalThis.__doActorTestState.inFlight > 0) globalThis.__doActorTestState.inFlight -= 1;
}
export function setDraining(value = true) {
  globalThis.__doActorTestState.draining = value === true;
}
export function log(level, event, fields = {}) {
  globalThis.__doActorTestState.logs.push({ level, event, fields });
}
export const metrics = {
  setGauge(name, labels, value) {
    globalThis.__doActorTestState.gaugeSamples.push({ name, labels, value });
  },
};
`);

const source = applyModuleReplacements(readRepositoryFile("do-runtime/actor.js"), [
  [/import \{ DurableObject \} from "cloudflare:workers";/g, durableObjectStub],
  [/from "do-runtime-load";/g, `from ${JSON.stringify(loadUrl)};`],
  [/from "do-runtime-object-registry";/g, `from ${JSON.stringify(objectRegistryUrl)};`],
  [/from "do-runtime-protocol";/g, `from ${JSON.stringify(protocolUrl)};`],
  [/from "do-runtime-owner-registry";/g, `from ${JSON.stringify(ownerRegistryUrl)};`],
  [/from "do-runtime-state";/g, `from ${JSON.stringify(stateUrl)};`],
  [/from "shared-errors";/g, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  [/from "shared-observability";/g, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
  [/from "shared-respond";/g, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
]);

/** @returns {DoHostActorHarnessState} */
export function doHostActorHarnessState() {
  return DO_ACTOR_TEST_STATE;
}

export function resetDoHostActorHarness() {
  const state = DO_ACTOR_TEST_STATE;
  state.assertResponses = [];
  state.assertArguments = [];
  state.actorInvokes = [];
  state.remembered = [];
  state.deletedFacets = [];
  state.aborts = [];
  state.draining = false;
  state.inFlight = 0;
  state.logs = [];
  state.gaugeSamples = [];
  state.assertCalls = 0;
  state.forgottenOwners = [];
  delete state.registryError;
  delete state.registryWait;
  delete state.registryWaitStarted;
  delete state.abortReject;
}

/** @returns {Promise<any>} */
export async function loadDoHostActor() {
  return await import(moduleDataUrl(source));
}
