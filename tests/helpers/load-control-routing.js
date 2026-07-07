import {
  applyModuleReplacements,
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryFile,
} from "./load-shared-module.js";
import { controlSharedStubUrl } from "./control-shared-stub.js";
import { compileControlGraph } from "./load-control-lib.js";

const {
  sharedNsUrl,
  sharedAuthRolesUrl,
  sharedQueueKeysUrl,
  sharedVersionUrl,
  sharedErrorsUrl,
  sharedRouteProjectionUrl,
  libUrl: controlLibUrl,
  topologyUrl,
  lifecycleIndexesUrl,
  cronIndexUrl,
  routePlanUrl,
} = await compileControlGraph();

const controlSharedUrl = controlSharedStubUrl(`
export const DECLARED_HOSTS_KEY = "declared-hosts";
export const HOST_DECLARATIONS_PREFIX = "host-declarations:";
`);

export const CONTROL_ROUTING_TEST_URL = moduleDataUrl(applyModuleReplacements(readRepositoryFile("control/routing.js"), [
  ...importSpecifierReplacements({
    "control-shared": controlSharedUrl,
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "control-topology": topologyUrl,
    "shared-errors": sharedErrorsUrl,
    "shared-route-projection": sharedRouteProjectionUrl,
    "shared-version": sharedVersionUrl,
    "control-cron-index": cronIndexUrl,
    "shared-ns-pattern": sharedNsUrl,
    "shared-auth-roles": sharedAuthRolesUrl,
    "shared-queue-keys": sharedQueueKeysUrl,
    "control-routing-route-plan": routePlanUrl,
  }),
]));

export async function loadControlRouting() {
  return await import(CONTROL_ROUTING_TEST_URL);
}
