import {
  importSpecifierReplacements,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "./load-shared-module.js";

const SHARED_FNV_URL = repositoryFileUrl("shared/fnv1a32.js");
const SHARED_WORKER_ID_URL = repositoryFileUrl("shared/worker-id.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_INTERNAL_AUTH_URL = repositoryFileUrl("shared/internal-auth.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_WORKERD_COMPAT_FLAGS_URL = repositoryFileUrl("shared/workerd-compat-flags.js");
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
      "shared-workerd-compat-flags": SHARED_WORKERD_COMPAT_FLAGS_URL,
      "shared-bounded-body": SHARED_BOUNDED_BODY_URL,
      "shared-internal-auth": SHARED_INTERNAL_AUTH_URL,
    }),
  ]);
}

/** @returns {Promise<typeof import("../../do-runtime/protocol.js")>} */
export async function loadDoProtocol() {
  return await import(doProtocolDataUrl());
}
