import { loadD1QueryWire, d1QueryWireDataUrl } from "./load-d1-protocol.js";
import { moduleDataUrl } from "./load-shared-module.js";
import {
  createOwnerClientHarness,
  importOwnerClientModule,
} from "./load-owner-client-harness.js";

const wire = await loadD1QueryWire();
export const {
  decodeD1QueryRequest,
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
} = wire;

const ownerHarness = createOwnerClientHarness("__d1OwnerClientTestState", "d1-runtime");

const protocolUrl = moduleDataUrl(`
export class D1ProtocolError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = "D1ProtocolError";
    this.status = status;
    this.code = code;
  }
}
`);
const timeoutUrl = moduleDataUrl(`
export function createD1QueryDeadline() {
  return { signal: new AbortController().signal, clear() {} };
}
export function isD1QueryTimeoutError() { return false; }
`);
const ownerRegistryUrl = moduleDataUrl(`
export function probeTimeoutMs() { return 1000; }
`);

const d1OwnerClientModule = await importOwnerClientModule("d1-runtime/owner-client.js", {
  "d1-runtime-protocol": protocolUrl,
  "shared-d1-query-wire": d1QueryWireDataUrl(),
  "shared-d1-timeout": timeoutUrl,
  "shared-errors": ownerHarness.errorsUrl,
  "d1-runtime-owner-registry": ownerRegistryUrl,
  "shared-internal-auth": ownerHarness.internalAuthUrl,
  "shared-owner-forwarder": ownerHarness.ownerForwarderUrl,
  "shared-owner-endpoint": ownerHarness.ownerEndpointUrl,
  "shared-owner-lease": ownerHarness.ownerLeaseUrl,
  "d1-runtime-state": ownerHarness.stateUrl,
});

/** @returns {import("./load-owner-client-harness.js").OwnerClientHarnessState} */
export function d1OwnerClientHarnessState() {
  return ownerHarness.state;
}

export function resetD1OwnerClientHarness() {
  ownerHarness.reset();
}

/** @returns {any} */
export function loadD1OwnerClient() {
  return d1OwnerClientModule;
}
