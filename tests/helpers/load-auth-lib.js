import {
  applyModuleReplacements,
  freshModuleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { compileSharedAuthRoles } from "./load-auth-roles.js";

const SHARED_NS_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_HEX_URL = repositoryFileUrl("shared/hex.js");
const SHARED_RANDOM_ID_URL = repositoryFileUrl("shared/random-id.js");

/** @param {{ rolesPatch?: Record<string, unknown>, authLibReplacements?: Array<[RegExp, string]> }} [opts] */
export async function loadAuthLib(opts = {}) {
  const { sharedAuthRolesUrl, sharedAuthRoles } = await compileSharedAuthRoles(opts);
  /** @type {Array<[RegExp | string, string]>} */
  const authLibReplacements = [
    [/from "shared-ns-pattern"/g, `from ${JSON.stringify(SHARED_NS_URL)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-hex"/g, `from ${JSON.stringify(SHARED_HEX_URL)}`],
    [/from "shared-random-id"/g, `from ${JSON.stringify(SHARED_RANDOM_ID_URL)}`],
    ...(opts.authLibReplacements || []),
  ];
  let authLibSource = readRepositoryFile("auth/lib.js");
  for (const [pattern, replacement] of authLibReplacements) {
    const replaced = applyModuleReplacements(authLibSource, [[pattern, replacement]]);
    if (replaced === authLibSource) {
      throw new Error(`auth-lib harness replacement did not match: ${String(pattern)}`);
    }
    authLibSource = replaced;
  }
  const authLibUrl = freshModuleDataUrl(authLibSource);
  const authLib = await import(authLibUrl);
  return { authLib, sharedAuthRoles };
}
