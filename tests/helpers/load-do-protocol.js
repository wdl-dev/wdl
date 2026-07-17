import {
  importSpecifierReplacements,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "./load-shared-module.js";

const SHARED_FNV_URL = repositoryFileUrl("shared/fnv1a32.js");
const SHARED_WORKER_ID_URL = repositoryFileUrl("shared/worker-id.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_INTERNAL_AUTH_URL = repositoryFileUrl("shared/internal-auth.js");
const SHARED_NS_PATTERN_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");
const DO_WIRE_GRAMMAR_URL = repositoryFileUrl("do-runtime/protocol/wire-grammar.js");
const DO_ERRORS_URL = repositoryModuleDataUrl("do-runtime/protocol/errors.js", [
  [/from "shared-respond";/g, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
]);
const DO_IDENTITY_URL = repositoryModuleDataUrl("do-runtime/protocol/identity.js", [
  [/from "do-runtime-protocol-wire-grammar";/g, `from ${JSON.stringify(DO_WIRE_GRAMMAR_URL)};`],
  [/from "do-runtime-protocol-errors";/g, `from ${JSON.stringify(DO_ERRORS_URL)};`],
  [/from "shared-fnv1a32";/g, `from ${JSON.stringify(SHARED_FNV_URL)};`],
]);

export function doProtocolDataUrl() {
  return repositoryModuleDataUrl("do-runtime/protocol.js", [
    ...importSpecifierReplacements({
      "do-runtime-protocol-wire-grammar": DO_WIRE_GRAMMAR_URL,
      "do-runtime-protocol-errors": DO_ERRORS_URL,
      "do-runtime-protocol-identity": DO_IDENTITY_URL,
      "shared-worker-id": SHARED_WORKER_ID_URL,
      "shared-bounded-body": SHARED_BOUNDED_BODY_URL,
      "shared-internal-auth": SHARED_INTERNAL_AUTH_URL,
      "shared-ns-pattern": SHARED_NS_PATTERN_URL,
      "shared-worker-contract": WORKER_CONTRACT_URL,
    }),
  ]);
}

/** @returns {Promise<typeof import("../../do-runtime/protocol.js")>} */
export async function loadDoProtocol() {
  return await import(doProtocolDataUrl());
}
