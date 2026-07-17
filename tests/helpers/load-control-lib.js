import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  freshRepositoryModuleDataUrl,
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { compileSharedAuthRoles } from "./load-auth-roles.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Resolve croner against the root package so tests pick up the hoisted
// version used by control.
const moduleRequire = createRequire(path.resolve(__dirname, "../../package.json"));
const SHARED_NS_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_QUEUE_KEYS_URL = repositoryFileUrl("shared/queue-keys.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const SHARED_ROUTE_PROJECTION_URL = repositoryFileUrl("shared/route-projection.js");
const SHARED_WORKERD_COMPAT_FLAGS_URL = repositoryFileUrl("shared/workerd-compat-flags.js");

/**
 * Compile the control/* module graph against shared/* deps. Returns URLs so
 * callers can rewrite imports in other source files (e.g. control/handlers/*.js)
 * without re-stubbing control-lib's surface. Each control module is compiled
 * once and the downstream URL is reused across siblings, so cross-module
 * imports share one instance.
 *
 * @param {{ rolesPatch?: Record<string, unknown> }} [opts]
 */
export async function compileControlGraph(opts = {}) {
  const { sharedAuthRolesUrl, sharedAuthRoles } = await compileSharedAuthRoles(opts);
  const cronerUrl = pathToFileURL(moduleRequire.resolve("croner")).href;
  const sharedCronTimeUrl = freshRepositoryModuleDataUrl("shared/cron-time.js", [
    [/from "croner"/g, `from ${JSON.stringify(cronerUrl)}`],
  ]);

  // Dependency order: lib → (bindings, topology, indexes, routing helpers) → bundle.
  const libUrl = freshRepositoryModuleDataUrl("control/lib.js", [
    [/from "shared-ns-pattern"/g, `from ${JSON.stringify(SHARED_NS_URL)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-errors"/g, `from ${JSON.stringify(SHARED_ERRORS_URL)}`],
    [/from "shared-route-projection"/g, `from ${JSON.stringify(SHARED_ROUTE_PROJECTION_URL)}`],
  ]);

  const bindingsUrl = freshRepositoryModuleDataUrl("control/bindings.js", [
    [/from "shared-ns-pattern"/g, `from ${JSON.stringify(SHARED_NS_URL)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-errors"/g, `from ${JSON.stringify(SHARED_ERRORS_URL)}`],
    [/from "control-lib"/g, `from ${JSON.stringify(libUrl)}`],
  ]);

  const topologyUrl = freshRepositoryModuleDataUrl("control/topology.js", [
    [/from "shared-ns-pattern"/g, `from ${JSON.stringify(SHARED_NS_URL)}`],
    [/from "shared-errors"/g, `from ${JSON.stringify(SHARED_ERRORS_URL)}`],
    [/from "control-lib"/g, `from ${JSON.stringify(libUrl)}`],
    [/from "shared-cron-time"/g, `from ${JSON.stringify(sharedCronTimeUrl)}`],
  ]);

  const lifecycleIndexesUrl = freshRepositoryModuleDataUrl("control/lifecycle-indexes.js", [
    [/from "control-lib"/g, `from ${JSON.stringify(libUrl)}`],
    [/from "shared-queue-keys"/g, `from ${JSON.stringify(SHARED_QUEUE_KEYS_URL)}`],
    [/from "shared-worker-contract"/g, `from ${JSON.stringify(WORKER_CONTRACT_URL)}`],
  ]);

  const cronIndexUrl = freshRepositoryModuleDataUrl("control/cron-index.js", [
    [/from "shared-cron-time"/g, `from ${JSON.stringify(sharedCronTimeUrl)}`],
  ]);

  const routePlanUrl = freshRepositoryModuleDataUrl("control/routing/route-plan.js", [
    [/from "shared-route-projection"/g, `from ${JSON.stringify(SHARED_ROUTE_PROJECTION_URL)}`],
  ]);

  const packageJsonSourceUrl = moduleDataUrl(
    `export default ${JSON.stringify(readRepositoryFile("package.json"))};`
  );
  const bundleUrl = freshRepositoryModuleDataUrl("control/bundle.js", [
    ...importSpecifierReplacements({
      "shared-ns-pattern": SHARED_NS_URL,
      "shared-workerd-compat-flags": SHARED_WORKERD_COMPAT_FLAGS_URL,
      "control-lib": libUrl,
    }),
    [/from "control-bindings"/g, `from ${JSON.stringify(bindingsUrl)}`],
    [/from "wdl-package-json-source"/g, `from ${JSON.stringify(packageJsonSourceUrl)}`],
  ]);

  return {
    sharedNsUrl: SHARED_NS_URL,
    sharedAuthRolesUrl,
    sharedAuthRoles,
    sharedQueueKeysUrl: SHARED_QUEUE_KEYS_URL,
    workerContractUrl: WORKER_CONTRACT_URL,
    sharedErrorsUrl: SHARED_ERRORS_URL,
    sharedRouteProjectionUrl: SHARED_ROUTE_PROJECTION_URL,
    sharedCronTimeUrl,
    libUrl,
    bindingsUrl,
    topologyUrl,
    lifecycleIndexesUrl,
    cronIndexUrl,
    routePlanUrl,
    bundleUrl,
  };
}

/** @param {{ rolesPatch?: Record<string, unknown> }} [opts] */
export async function loadControlLib(opts = {}) {
  const { sharedAuthRoles, libUrl, bindingsUrl, topologyUrl, bundleUrl } =
    await compileControlGraph(opts);
  const [controlLib, controlBindings, controlTopology, controlBundle] = await Promise.all([
    import(libUrl),
    import(bindingsUrl),
    import(topologyUrl),
    import(bundleUrl),
  ]);
  return { controlLib, controlBindings, controlTopology, controlBundle, sharedAuthRoles };
}
