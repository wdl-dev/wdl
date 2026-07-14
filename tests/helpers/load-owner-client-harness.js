import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
  repositoryModuleDataUrl,
  sharedModuleDataUrl,
} from "./load-shared-module.js";
import { sharedInternalAuthUrl } from "./runtime-proxy-stub.js";

/**
 * @typedef {{
 *   fetches: any[],
 *   logs: any[],
 *   metrics: any[],
 * }} OwnerClientHarnessState
 */

/**
 * @param {string} globalName
 * @param {string} service
 */
export function createOwnerClientHarness(globalName, service) {
  /** @type {OwnerClientHarnessState} */
  const state = { fetches: [], logs: [], metrics: [] };
  Object.defineProperty(globalThis, globalName, {
    value: state,
    configurable: true,
    writable: true,
  });
  const stateKey = JSON.stringify(globalName);
  const stateUrl = moduleDataUrl(`
export const SERVICE = ${JSON.stringify(service)};
export const metrics = {
  increment(name, labels) {
    globalThis[${stateKey}].metrics.push({ name, labels });
  },
};
export function log(level, event, fields) {
  globalThis[${stateKey}].logs.push({ level, event, fields });
}
`);
  const internalAuthUrl = sharedInternalAuthUrl();
  const errorsUrl = repositoryFileUrl("shared/errors.js");
  const ownerEndpointUrl = repositoryFileUrl("shared/owner-endpoint.js");
  const ownerLeaseUrl = sharedModuleDataUrl("shared/owner-lease.js");
  const ownerForwarderUrl = repositoryModuleDataUrl("shared/owner-forwarder.js", [
    [/from "shared-internal-auth";/, `from ${JSON.stringify(internalAuthUrl)};`],
    [/from "shared-errors";/, `from ${JSON.stringify(errorsUrl)};`],
    [/from "shared-owner-endpoint";/, `from ${JSON.stringify(ownerEndpointUrl)};`],
  ]);

  return {
    state,
    stateUrl,
    errorsUrl,
    internalAuthUrl,
    ownerForwarderUrl,
    ownerEndpointUrl,
    ownerLeaseUrl,
    reset() {
      state.fetches = [];
      state.logs = [];
      state.metrics = [];
    },
  };
}

/**
 * @param {string} relativePath
 * @param {Record<string, string>} replacements
 */
export async function importOwnerClientModule(relativePath, replacements) {
  return await importRepositoryModule(relativePath, importSpecifierReplacements(replacements));
}
