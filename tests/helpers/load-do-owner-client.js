import { doProtocolDataUrl, loadDoProtocol } from "./load-do-protocol.js";
import {
  createOwnerClientHarness,
  importOwnerClientModule,
} from "./load-owner-harness.js";
import { repositoryFileUrl } from "./load-shared-module.js";

export const protocolUrl = doProtocolDataUrl();
const { DO_INVOKE_CONTENT_TYPE: invokeContentType } = await loadDoProtocol();
export const DO_INVOKE_CONTENT_TYPE = invokeContentType;
const { DO_FORWARD_HEADERS: forwardHeaders } = await import(
  repositoryFileUrl("runtime/_wdl-do-scoped-request.js")
);
export const DO_FORWARD_HEADERS = forwardHeaders;

const ownerHarness = createOwnerClientHarness("__doOwnerClientTestState", "do-runtime");

const doOwnerClientModule = await importOwnerClientModule("do-runtime/owner-client.js", {
  "do-runtime-protocol": protocolUrl,
  "_wdl-do-scoped-request.js": repositoryFileUrl("runtime/_wdl-do-scoped-request.js"),
  "shared-internal-auth": ownerHarness.internalAuthUrl,
  "shared-owner-forwarder": ownerHarness.ownerForwarderUrl,
  "shared-owner-lease": ownerHarness.ownerLeaseUrl,
  "do-runtime-state": ownerHarness.stateUrl,
});

/** @returns {import("./load-owner-harness.js").OwnerHarnessState} */
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
