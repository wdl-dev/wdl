import {
  repositoryFileUrl,
  repositoryModuleDataUrl,
  sharedModuleDataUrl,
} from "./load-shared-module.js";

const PARAMS_URL = repositoryFileUrl("shared/d1-params.js");
const DATA_FIELD_URL = repositoryFileUrl("shared/d1-data-field.js");
const FNV_URL = repositoryFileUrl("shared/fnv1a32.js");
const BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const NS_PATTERN_URL = repositoryFileUrl("shared/ns-pattern.js");
const D1_TIMEOUT_URL = sharedModuleDataUrl("shared/d1-timeout.js");
const RESPOND_URL = repositoryFileUrl("shared/respond.js");
const INTERNAL_AUTH_URL = repositoryFileUrl("shared/internal-auth.js");
const OWNER_ENDPOINT_URL = repositoryFileUrl("shared/owner-endpoint.js");

export function d1ProtocolDataUrl() {
  const queryWireUrl = d1QueryWireDataUrl();
  return repositoryModuleDataUrl("d1-runtime/protocol.js", [
    [/import \{ normalizeD1Param \} from "shared-d1-params";/g,
      `import { normalizeD1Param } from ${JSON.stringify(PARAMS_URL)};`],
    [/import \{ fnv1a32CodeUnits \} from "shared-fnv1a32";/g,
      `import { fnv1a32CodeUnits } from ${JSON.stringify(FNV_URL)};`],
    [/from "shared-bounded-body";/g, `from ${JSON.stringify(BOUNDED_BODY_URL)};`],
    [/from "shared-errors";/g, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
    [/from "shared-ns-pattern";/g, `from ${JSON.stringify(NS_PATTERN_URL)};`],
    [/from "shared-d1-query-wire";/g, `from ${JSON.stringify(queryWireUrl)};`],
  ]);
}

export function d1QueryWireDataUrl() {
  return repositoryModuleDataUrl("shared/d1-query-wire.js", [
    [/from "shared-d1-params";/, `from ${JSON.stringify(PARAMS_URL)};`],
    [/from "shared-d1-data-field";/, `from ${JSON.stringify(DATA_FIELD_URL)};`],
  ]);
}

export function d1TransportDataUrl() {
  return repositoryModuleDataUrl("shared/d1-transport.js", [
    [/from "shared-d1-data-field";/, `from ${JSON.stringify(DATA_FIELD_URL)};`],
  ]);
}

export function controlD1RuntimeClientDataUrl() {
  return repositoryModuleDataUrl("control/d1-runtime-client.js", [
    [/from "shared-d1-timeout";/g, `from ${JSON.stringify(D1_TIMEOUT_URL)};`],
    [/from "shared-d1-transport";/g, `from ${JSON.stringify(d1TransportDataUrl())};`],
    [/from "shared-d1-query-wire";/g, `from ${JSON.stringify(d1QueryWireDataUrl())};`],
    [/from "shared-respond";/g, `from ${JSON.stringify(RESPOND_URL)};`],
    [/from "shared-internal-auth";/g, `from ${JSON.stringify(INTERNAL_AUTH_URL)};`],
    [/from "shared-owner-endpoint";/g, `from ${JSON.stringify(OWNER_ENDPOINT_URL)};`],
  ]);
}

export async function loadD1Protocol() {
  return await import(d1ProtocolDataUrl());
}

export async function loadD1QueryWire() {
  return await import(d1QueryWireDataUrl());
}

export async function loadControlD1RuntimeClient() {
  return await import(controlD1RuntimeClientDataUrl());
}
