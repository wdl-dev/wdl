import {
  applyModuleReplacements,
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryFile,
} from "./load-shared-module.js";
import { controlSharedStubUrl } from "./control-shared-stub.js";
import { compileControlGraph } from "./load-control-lib.js";
import { sharedRedisStubUrl } from "./mocks/fake-redis.js";

const {
  sharedNsUrl,
  sharedAuthRolesUrl,
  sharedQueueKeysUrl,
  workerContractUrl,
  sharedErrorsUrl,
  sharedRouteProjectionUrl,
  libUrl: controlLibUrl,
  topologyUrl,
  lifecycleIndexesUrl,
  cronIndexUrl,
  routePlanUrl,
} = await compileControlGraph();

const controlSharedUrl = controlSharedStubUrl();
const sharedRedisUrl = sharedRedisStubUrl();

export const CONTROL_ROUTING_TEST_URL = moduleDataUrl(applyModuleReplacements(readRepositoryFile("control/routing.js"), [
  ...importSpecifierReplacements({
    "control-shared": controlSharedUrl,
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "control-topology": topologyUrl,
    "shared-errors": sharedErrorsUrl,
    "shared-route-projection": sharedRouteProjectionUrl,
    "shared-worker-contract": workerContractUrl,
    "control-cron-index": cronIndexUrl,
    "shared-ns-pattern": sharedNsUrl,
    "shared-auth-roles": sharedAuthRolesUrl,
    "shared-queue-keys": sharedQueueKeysUrl,
    "shared-redis": sharedRedisUrl,
    "control-routing-route-plan": routePlanUrl,
  }),
]));

export async function loadControlRouting() {
  return await import(CONTROL_ROUTING_TEST_URL);
}
