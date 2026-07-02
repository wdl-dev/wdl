// Each downstream rewrite targets the same URL so cross-module imports share
// one instance — `_resetWorkflowReplayCacheForTest` must reset the same cache
// `dispatch.js` sees.

import {
  importRepositoryModule,
  moduleDataUrl,
  repositoryFileUrl,
  repositoryModuleDataUrl,
  runtimeLibModuleDataUrl,
} from "./load-shared-module.js";
import { runtimeProxyBindingStubUrl, sharedInternalAuthUrl } from "./runtime-proxy-stub.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const RESPOND_URL = repositoryFileUrl("shared/respond.js");
const BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const RUNTIME_LIB_URL = runtimeLibModuleDataUrl();
const METRICS_MOCK_URL = moduleDataUrl(`
export const metrics = {
  increment() {},
  setGauge() {},
};
`);

/** @type {Promise<Record<string, any>> | null} */
let dispatchPromise = null;

/** @returns {Promise<Record<string, any>>} */
export async function loadRuntimeDispatch() {
  if (dispatchPromise) return dispatchPromise;
  dispatchPromise = (async () => {
    const workflowJsonUrl = repositoryModuleDataUrl("runtime/dispatch/workflow-json.js");
    const workflowReplayCacheUrl = repositoryModuleDataUrl("runtime/dispatch/workflow-replay-cache.js", [
      [/from "runtime-metrics";/, `from ${JSON.stringify(METRICS_MOCK_URL)};`],
    ]);
    const workflowStepUrl = repositoryModuleDataUrl("runtime/dispatch/workflow-step.js", [
      [/from "runtime-dispatch-workflow-json";/g, `from ${JSON.stringify(workflowJsonUrl)};`],
      [/from "runtime-dispatch-workflow-replay-cache";/g, `from ${JSON.stringify(workflowReplayCacheUrl)};`],
    ]);
    const tailForwarderUrl = repositoryModuleDataUrl("runtime/tail-forwarder.js", [
      [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
      [/from "shared-internal-auth";/, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
    ]);

    const [runtimeDispatchWorkflowStep, runtimeDispatch] = await Promise.all([
      import(workflowStepUrl),
      importRepositoryModule("runtime/dispatch.js", [
        [/from "shared-respond";/, `from ${JSON.stringify(RESPOND_URL)};`],
        [/from "shared-bounded-body";/, `from ${JSON.stringify(BOUNDED_BODY_URL)};`],
        [/from "shared-errors";/, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
        [/from "shared-internal-auth";/, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
        [/from "runtime-lib";/, `from ${JSON.stringify(RUNTIME_LIB_URL)};`],
        [/from "runtime-metrics";/, `from ${JSON.stringify(METRICS_MOCK_URL)};`],
        [/from "runtime-dispatch-workflow-json";/g, `from ${JSON.stringify(workflowJsonUrl)};`],
        [/from "runtime-dispatch-workflow-replay-cache";/g, `from ${JSON.stringify(workflowReplayCacheUrl)};`],
        [/from "runtime-dispatch-workflow-step";/g, `from ${JSON.stringify(workflowStepUrl)};`],
        [/from "runtime-tail-forwarder";/, `from ${JSON.stringify(tailForwarderUrl)};`],
      ]),
    ]);
    return { runtimeDispatch, runtimeDispatchWorkflowStep };
  })();
  return dispatchPromise;
}
