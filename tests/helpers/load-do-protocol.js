import { repositoryFileUrl, repositoryModuleDataUrl } from "./load-shared-module.js";

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
    [/from "do-runtime-protocol-wire-grammar";/g, `from ${JSON.stringify(DO_WIRE_GRAMMAR_URL)};`],
    [/from "do-runtime-protocol-errors";/g, `from ${JSON.stringify(DO_ERRORS_URL)};`],
    [/from "do-runtime-protocol-identity";/g, `from ${JSON.stringify(DO_IDENTITY_URL)};`],
    [/from "shared-worker-id";/g, `from ${JSON.stringify(SHARED_WORKER_ID_URL)};`],
    [/from "shared-workerd-compat-flags";/g, `from ${JSON.stringify(SHARED_WORKERD_COMPAT_FLAGS_URL)};`],
    [/from "shared-bounded-body";/g, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)};`],
    [/from "shared-internal-auth";/g, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
  ]);
}

/** @returns {Promise<typeof import("../../do-runtime/protocol.js")>} */
export async function loadDoProtocol() {
  return await import(doProtocolDataUrl());
}
