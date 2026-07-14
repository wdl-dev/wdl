import { doProtocolDataUrl, loadDoProtocol } from "./load-do-protocol.js";
import {
  createOwnerClientHarness,
  importOwnerClientModule,
} from "./load-owner-client-harness.js";

export const protocolUrl = doProtocolDataUrl();
const { DO_INVOKE_CONTENT_TYPE: invokeContentType } = await loadDoProtocol();
export const DO_INVOKE_CONTENT_TYPE = invokeContentType;

const ownerHarness = createOwnerClientHarness("__doOwnerClientTestState", "do-runtime");

const doOwnerClientModule = await importOwnerClientModule("do-runtime/owner-client.js", {
  "do-runtime-protocol": protocolUrl,
  "shared-internal-auth": ownerHarness.internalAuthUrl,
  "shared-owner-forwarder": ownerHarness.ownerForwarderUrl,
  "shared-owner-lease": ownerHarness.ownerLeaseUrl,
  "do-runtime-state": ownerHarness.stateUrl,
});

/** @returns {import("./load-owner-client-harness.js").OwnerClientHarnessState} */
export function doOwnerClientHarnessState() {
  return ownerHarness.state;
}

export function resetDoOwnerClientHarness() {
  ownerHarness.reset();
}

/** @returns {any} */
export function loadDoOwnerClient() {
  return doOwnerClientModule;
}
