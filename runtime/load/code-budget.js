import {
  WDL_RESERVED_ENTRYPOINT_RE,
  isValidJsClassDeclarationName,
} from "shared-ns-pattern";
import {
  HOST_BINDING_RESERVED_MODULES,
  HOST_BINDING_RESERVED_MODULE_NAMES,
  WORKFLOWS_MODULE_NAME,
  WORKFLOWS_MODULE_SOURCE,
  rewriteCloudflareWorkflowsImports,
} from "runtime-load-module-rewrite";
import {
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "runtime-load-wrapper-generate";

// Mirrors workerd v1.20260701.1
// src/workerd/api/worker-loader.c++ MAX_DYNAMIC_WORKER_CODE_SIZE.
export const WORKER_LOADER_CODE_MAX_BYTES = 64 * 1024 * 1024;

const D1_DATA_FIELD_MODULE_NAME = "_wdl-d1-data-field.js";
const utf8Decoder = new TextDecoder();

/**
 * @typedef {string | Uint8Array} NormalizedModuleBody
 * @typedef {[name: string, body: NormalizedModuleBody]} NormalizedModule
 * @typedef {string | { cjs: string } | { text: string } | { json: unknown } | { wasm: Uint8Array } | { data: Uint8Array }} WorkerModuleValue
 * @typedef {{ modules: Record<string, WorkerModuleValue>, mainModule: string, [key: string]: unknown }} WorkerCodeShape
 * @typedef {Record<string, unknown> & { type?: string, className?: unknown }} RuntimeBindingSpec
 * @typedef {{ binding?: unknown, className?: unknown }} RuntimeWorkflowSpec
 * @typedef {{ entrypoint?: unknown }} RuntimeExportSpec
 * @typedef {{ bindings?: Record<string, RuntimeBindingSpec> | null, workflows?: RuntimeWorkflowSpec[] | null, exports?: RuntimeExportSpec[] | null, modules?: Record<string, { type?: unknown }> | null }} RuntimeBundleMeta
 * @typedef {{
 *   bindingEntries: Array<[string, RuntimeBindingSpec]>,
 *   workflows: RuntimeWorkflowSpec[],
 *   d1Bindings: string[],
 *   r2Bindings: string[],
 *   doBindings: string[],
 *   workflowBindings: Record<string, unknown>,
 *   hostWrappedClassNames: string[],
 *   needsDoBackend: boolean,
 *   needsWorkflowsBackend: boolean,
 *   needsHostBindingWrapper: boolean,
 * }} RuntimeMetaPlan
 * @typedef {{
 *   d1ClientSource: string,
 *   d1DataFieldSource: string,
 *   d1ParamsSource: string,
 *   sqlSplitterSource: string,
 *   d1TransportSource: string,
 *   r2ClientSource: string,
 *   r2UtilsSource: string,
 *   doClientSource: string,
 *   doTransportSource: string,
 *   ownerEndpointSource: string,
 *   ownerHintCacheSource: string,
 *   requestIdSource: string,
 *   workflowsClientSource: string,
 * }} RuntimeInjectionSources
 * @typedef {[name: string, source: string]} RuntimeModuleInjection
 */

/** @param {string | Uint8Array} body */
function moduleBodyByteLength(body) {
  return typeof body === "string" ? Buffer.byteLength(body, "utf8") : body.byteLength;
}

/** @param {RuntimeInjectionSources} sources */
function runtimeModuleInjections(sources) {
  /** @type {RuntimeModuleInjection} */
  const requestIdModuleInjection = ["_wdl-request-id.js", sources.requestIdSource];
  const d1TransportInjectedSource = sources.d1TransportSource.replace(
    /from "shared-d1-data-field";/,
    `from "./${D1_DATA_FIELD_MODULE_NAME}";`
  );
  /** @type {RuntimeModuleInjection[]} */
  const d1ModuleInjections = [
    requestIdModuleInjection,
    [D1_DATA_FIELD_MODULE_NAME, sources.d1DataFieldSource],
    ["_wdl-d1-params.js", sources.d1ParamsSource],
    ["_wdl-sql-splitter.js", sources.sqlSplitterSource],
    ["_wdl-d1-transport.js", d1TransportInjectedSource],
    ["_wdl-d1-client.js", sources.d1ClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const r2ModuleInjections = [
    requestIdModuleInjection,
    ["_wdl-r2-utils.js", sources.r2UtilsSource],
    ["_wdl-r2-client.js", sources.r2ClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const doModuleInjections = [
    requestIdModuleInjection,
    ["_wdl-do-transport.js", sources.doTransportSource],
    ["_wdl-owner-endpoint.js", sources.ownerEndpointSource],
    ["_wdl-owner-hint-cache.js", sources.ownerHintCacheSource],
    ["_wdl-do-client.js", sources.doClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const workflowsModuleInjections = [
    requestIdModuleInjection,
    ["_wdl-workflows-client.js", sources.workflowsClientSource],
  ];
  return {
    d1ModuleInjections,
    r2ModuleInjections,
    doModuleInjections,
    workflowsModuleInjections,
  };
}

/**
 * @typedef {{
 *   modules: RuntimeModuleInjection[],
 *   bindingNames(plan: Pick<RuntimeMetaPlan, "d1Bindings" | "r2Bindings" | "doBindings">): string[],
 * }} HostFacadeBindingDefinition
 */

/**
 * @param {ReturnType<typeof runtimeModuleInjections>} injections
 * @returns {HostFacadeBindingDefinition[]}
 */
function hostFacadeBindingDefinitions(injections) {
  const {
    d1ModuleInjections,
    r2ModuleInjections,
    doModuleInjections,
  } = injections;
  return [
    {
      modules: d1ModuleInjections,
      bindingNames(plan) { return plan.d1Bindings; },
    },
    {
      modules: r2ModuleInjections,
      bindingNames(plan) { return plan.r2Bindings; },
    },
    {
      modules: doModuleInjections,
      bindingNames(plan) { return plan.doBindings; },
    },
  ];
}

/**
 * @param {RuntimeMetaPlan} plan
 * @param {RuntimeBindingSpec} spec
 * @param {string} name
 */
function addHostFacadeBinding(plan, spec, name) {
  switch (spec?.type) {
    case "d1":
      plan.d1Bindings.push(name);
      return;
    case "r2":
      plan.r2Bindings.push(name);
      return;
    case "do":
      plan.doBindings.push(name);
      return;
  }
}

/** @param {RuntimeMetaPlan} plan */
function hasHostFacadeBindings(plan) {
  return plan.d1Bindings.length > 0 || plan.r2Bindings.length > 0 || plan.doBindings.length > 0;
}

/** @param {RuntimeBundleMeta} meta */
function d1ExportedEntrypointNames(meta) {
  /** @type {string[]} */
  const out = [];
  for (const entry of meta.exports || []) {
    const name = entry?.entrypoint;
    if (!name || name === "default") continue;
    if (typeof name !== "string") {
      throw new Error(
        `Host binding wrapper requires exported entrypoint names to be strings, got ${JSON.stringify(name)}`
      );
    }
    if (!isValidJsClassDeclarationName(name)) {
      throw new Error(
        `Host binding wrapper requires exported entrypoint names to be valid JS class declaration names, got ${JSON.stringify(name)}`
      );
    }
    if (WDL_RESERVED_ENTRYPOINT_RE.test(name)) {
      throw new Error(`Exported entrypoint targets reserved runtime entrypoint "${name}" (redeploy worker)`);
    }
    out.push(name);
  }
  return out;
}

/**
 * @param {RuntimeBundleMeta} meta
 * @param {Array<[string, RuntimeBindingSpec]>} bindingEntries
 * @param {RuntimeWorkflowSpec[]} workflows
 */
function hostWrappedClassNames(meta, bindingEntries, workflows) {
  const out = new Set(d1ExportedEntrypointNames(meta));
  for (const [, spec] of bindingEntries) {
    if (spec?.type === "do" && typeof spec.className === "string" && spec.className) {
      if (!isValidJsClassDeclarationName(spec.className)) {
        throw new Error(
          `Host binding wrapper requires Durable Object class names to be valid JS class declaration names, got ${JSON.stringify(spec.className)}`
        );
      }
      if (WDL_RESERVED_ENTRYPOINT_RE.test(spec.className)) {
        throw new Error(`Durable Object binding targets reserved runtime entrypoint "${spec.className}" (redeploy worker)`);
      }
      out.add(spec.className);
    }
  }
  for (const workflow of workflows) {
    const className = workflow?.className;
    if (typeof className === "string" && className) {
      if (!isValidJsClassDeclarationName(className)) {
        throw new Error(
          `Host binding wrapper requires Workflow class names to be valid JS class declaration names, got ${JSON.stringify(className)}`
        );
      }
      if (WDL_RESERVED_ENTRYPOINT_RE.test(className)) {
        throw new Error(`Workflow binding targets reserved runtime entrypoint "${className}" (redeploy worker)`);
      }
      out.add(className);
    }
  }
  return [...out];
}

/** @param {RuntimeBundleMeta} meta @returns {RuntimeMetaPlan} */
export function analyzeRuntimeMeta(meta) {
  const bindingEntries = Object.entries(meta.bindings || {});
  const workflows = Array.isArray(meta.workflows) ? meta.workflows : [];
  /** @type {RuntimeMetaPlan} */
  const plan = {
    bindingEntries,
    workflows,
    d1Bindings: [],
    r2Bindings: [],
    doBindings: [],
    workflowBindings: Object.create(null),
    hostWrappedClassNames: [],
    needsDoBackend: false,
    needsWorkflowsBackend: false,
    needsHostBindingWrapper: false,
  };
  for (const [name, spec] of bindingEntries) {
    addHostFacadeBinding(plan, spec, name);
  }
  for (const workflow of workflows) {
    if (typeof workflow?.binding === "string" && workflow.binding) plan.workflowBindings[workflow.binding] = workflow;
  }
  plan.needsDoBackend = plan.doBindings.length > 0;
  plan.needsWorkflowsBackend = Object.keys(plan.workflowBindings).length > 0;
  plan.needsHostBindingWrapper = hasHostFacadeBindings(plan) || plan.needsWorkflowsBackend;
  if (plan.needsHostBindingWrapper) {
    plan.hostWrappedClassNames = hostWrappedClassNames(meta, bindingEntries, workflows);
  }
  return plan;
}

/**
 * @param {string} mainModule
 * @param {RuntimeBundleMeta} meta
 * @param {RuntimeInjectionSources} runtimeSources
 * @param {RuntimeMetaPlan} [plan]
 */
function runtimeInjectedModuleSources(mainModule, meta, runtimeSources, plan = analyzeRuntimeMeta(meta)) {
  const injections = runtimeModuleInjections(runtimeSources);
  /** @type {Map<string, string>} */
  const out = new Map();
  /** @param {RuntimeModuleInjection[]} modules */
  const addModules = (modules) => {
    for (const [name, source] of modules) out.set(name, source);
  };
  for (const definition of hostFacadeBindingDefinitions(injections)) {
    if (definition.bindingNames(plan).length > 0) addModules(definition.modules);
  }
  if (plan.needsWorkflowsBackend) {
    addModules(injections.workflowsModuleInjections);
  }
  out.set(WORKFLOWS_MODULE_NAME, WORKFLOWS_MODULE_SOURCE);
  // `_wdl-wrapper.js` is always injected: host bindings use the larger wrapper,
  // and otherwise the abort shim still rewrites the user main module.
  out.set(
    "_wdl-wrapper.js",
    plan.needsHostBindingWrapper
      ? generateHostBindingWrapperModule(
          mainModule,
          plan.d1Bindings,
          plan.r2Bindings,
          plan.doBindings,
          plan.workflowBindings,
          plan.hostWrappedClassNames
        )
      : generateAbortShimWrapperModule(mainModule)
  );
  return [...out];
}

/**
 * @param {WorkerCodeShape} workerCode
 * @param {RuntimeBundleMeta} meta
 * @param {RuntimeInjectionSources} runtimeSources
 * @param {RuntimeMetaPlan} [plan]
 */
export function injectRuntimeModulesForHostBindings(
  workerCode,
  meta,
  runtimeSources,
  plan = analyzeRuntimeMeta(meta)
) {
  const originalMain = workerCode.mainModule;
  if (typeof originalMain !== "string" || !originalMain) {
    throw new Error("Host binding wrapper requires a string mainModule");
  }
  rewriteCloudflareWorkflowsImports(workerCode);
  if (
    HOST_BINDING_RESERVED_MODULES.has(originalMain) ||
    [...HOST_BINDING_RESERVED_MODULES].some((name) => Object.hasOwn(workerCode.modules, name))
  ) {
    throw new Error(
      `Host binding wrapper requires reserved module names ${HOST_BINDING_RESERVED_MODULE_NAMES.join(", ")}`
    );
  }
  for (const [name, source] of runtimeInjectedModuleSources(originalMain, meta, runtimeSources, plan)) {
    workerCode.modules[name] = source;
  }
  workerCode.mainModule = "_wdl-wrapper.js";
  return workerCode;
}

/** @param {Record<string, { type?: unknown }> | null | undefined} modules @param {string} name */
function moduleType(modules, name) {
  const type = modules?.[name]?.type;
  return typeof type === "string" ? type : "";
}

/**
 * @param {{
 *   mainModule: string,
 *   normalized: NormalizedModule[],
 *   meta: RuntimeBundleMeta,
 *   runtimeSources: RuntimeInjectionSources,
 * }} args
 */
export function estimateFinalWorkerLoaderCodeBytes({ mainModule, normalized, meta, runtimeSources }) {
  /** @type {Record<string, string>} */
  const jsModules = Object.create(null);
  /** @type {Map<string, number>} */
  const userModuleBytes = new Map();
  for (const [name, body] of normalized) {
    const type = moduleType(meta.modules, name);
    if (type === "module") {
      jsModules[name] = typeof body === "string" ? body : utf8Decoder.decode(body);
    } else {
      userModuleBytes.set(name, moduleBodyByteLength(body));
    }
  }
  rewriteCloudflareWorkflowsImports({ modules: jsModules });
  for (const [name, source] of Object.entries(jsModules)) {
    userModuleBytes.set(name, Buffer.byteLength(source, "utf8"));
  }
  let total = 0;
  for (const bytes of userModuleBytes.values()) total += bytes;
  for (const [, source] of runtimeInjectedModuleSources(mainModule, meta, runtimeSources)) {
    total += Buffer.byteLength(source, "utf8");
  }
  return total;
}
