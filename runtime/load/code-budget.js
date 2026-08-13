import {
  WDL_RESERVED_ENTRYPOINT_RE,
  isValidJsClassDeclarationName,
} from "shared-ns-pattern";
import {
  HOST_BINDING_MODULE_NAMES,
  HOST_BINDING_RESERVED_MODULES,
  HOST_BINDING_RESERVED_MODULE_NAMES,
  WORKFLOWS_MODULE_NAME,
  WORKFLOWS_MODULE_SOURCE,
  rewriteCloudflareWorkflowsImports,
} from "runtime-load-module-rewrite";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "runtime-load-wrapper-generate";

const nodeBuffer = /** @type {{ Buffer: WdlNodeBufferConstructor }} */ (
  /** @type {unknown} */ (globalThis)
).Buffer;
// Mirrors workerd v1.20260718.1
// src/workerd/api/worker-loader.c++ MAX_DYNAMIC_WORKER_CODE_SIZE.
export const WORKER_LOADER_CODE_MAX_BYTES = 64 * 1024 * 1024;

const WORKFLOWS_IMPORT_MARKER = "cloudflare:workflows";
const WORKFLOWS_IMPORT_MARKER_BYTES = nodeBuffer.from(WORKFLOWS_IMPORT_MARKER, "utf8");
const utf8Decoder = new TextDecoder();

/**
 * @typedef {string | Uint8Array} NormalizedModuleBody
 * @typedef {[name: string, body: NormalizedModuleBody]} NormalizedModule
 * @typedef {string | { cjs: string } | { text: string } | { json: unknown } | { wasm: Uint8Array } | { data: Uint8Array }} WorkerModuleValue
 * @typedef {{ modules: Record<string, WorkerModuleValue>, mainModule: string, [key: string]: unknown }} WorkerCodeShape
 * @typedef {Record<string, unknown> & { type?: string, className?: unknown }} RuntimeBindingSpec
 * @typedef {{ binding?: unknown, className?: unknown }} RuntimeWorkflowSpec
 * @typedef {{ entrypoint?: unknown }} RuntimeExportSpec
 * @typedef {{ bindings?: Record<string, RuntimeBindingSpec> | null, workflows?: RuntimeWorkflowSpec[] | null, exports?: RuntimeExportSpec[] | null, modules?: Record<string, { type?: unknown }> | null, compatibilityFlags?: unknown }} RuntimeBundleMeta
 * @typedef {{
 *   bindingEntries: Array<[string, RuntimeBindingSpec]>,
 *   workflows: RuntimeWorkflowSpec[],
 *   d1Bindings: string[],
 *   r2Bindings: string[],
 *   doBindings: string[],
 *   aiBindings: string[],
 *   workflowBindings: Record<string, unknown>,
 *   hostWrappedClassNames: string[],
 *   needsHostBindingWrapper: boolean,
 * }} RuntimeMetaPlan
 * @typedef {{
 *   d1ClientSource: string,
 *   d1DataFieldSource: string,
 *   d1ParamsSource: string,
 *   utf8Source: string,
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
 *   aiClientSource: string,
 * }} RuntimeInjectionSources
 * @typedef {[name: string, source: string]} RuntimeModuleInjection
 * @typedef {"d1Bindings" | "r2Bindings" | "doBindings" | "aiBindings"} HostFacadePlanKey
 * @typedef {"d1ModuleInjections" | "r2ModuleInjections" | "doModuleInjections" | "aiModuleInjections"} HostFacadeInjectionKey
 * @typedef {{ planKey: HostFacadePlanKey, injectionKey: HostFacadeInjectionKey }} HostFacadeBindingDefinition
 */

/** @type {Readonly<Record<string, HostFacadeBindingDefinition>>} */
const HOST_FACADE_BINDING_DEFINITIONS = Object.freeze({
  d1: { planKey: "d1Bindings", injectionKey: "d1ModuleInjections" },
  r2: { planKey: "r2Bindings", injectionKey: "r2ModuleInjections" },
  do: { planKey: "doBindings", injectionKey: "doModuleInjections" },
  ai: { planKey: "aiBindings", injectionKey: "aiModuleInjections" },
});

/** @param {string | Uint8Array} body */
function moduleBodyByteLength(body) {
  return typeof body === "string" ? nodeBuffer.byteLength(body, "utf8") : body.byteLength;
}

/** @param {NormalizedModuleBody} body */
function decodedTextModuleByteLength(body) {
  if (typeof body === "string") return nodeBuffer.byteLength(body, "utf8");
  const hasUtf8Bom = (
    body.byteLength >= 3 &&
    body[0] === 0xef &&
    body[1] === 0xbb &&
    body[2] === 0xbf
  );
  return body.byteLength - (hasUtf8Bom ? 3 : 0);
}

/** @param {NormalizedModuleBody} body */
function containsWorkflowsImportMarker(body) {
  if (typeof body === "string") return body.includes(WORKFLOWS_IMPORT_MARKER);
  const bytes = nodeBuffer.from(body.buffer, body.byteOffset, body.byteLength);
  return bytes.indexOf(WORKFLOWS_IMPORT_MARKER_BYTES) !== -1;
}

/** @param {RuntimeInjectionSources} sources */
function runtimeModuleInjections(sources) {
  /** @type {RuntimeModuleInjection} */
  const requestIdModuleInjection = [HOST_BINDING_MODULE_NAMES.requestId, sources.requestIdSource];
  const d1ParamsInjectedSource = sources.d1ParamsSource.replace(
    '"./utf8.js"',
    `"./${HOST_BINDING_MODULE_NAMES.utf8}"`
  );
  const d1TransportInjectedSource = sources.d1TransportSource.replace(
    /from "shared-d1-data-field";/,
    `from "./${HOST_BINDING_MODULE_NAMES.d1DataField}";`
  );
  /** @type {RuntimeModuleInjection[]} */
  const d1ModuleInjections = [
    requestIdModuleInjection,
    [HOST_BINDING_MODULE_NAMES.d1DataField, sources.d1DataFieldSource],
    [HOST_BINDING_MODULE_NAMES.utf8, sources.utf8Source],
    [HOST_BINDING_MODULE_NAMES.d1Params, d1ParamsInjectedSource],
    [HOST_BINDING_MODULE_NAMES.sqlSplitter, sources.sqlSplitterSource],
    [HOST_BINDING_MODULE_NAMES.d1Transport, d1TransportInjectedSource],
    [HOST_BINDING_MODULE_NAMES.d1Client, sources.d1ClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const r2ModuleInjections = [
    requestIdModuleInjection,
    [HOST_BINDING_MODULE_NAMES.r2Utils, sources.r2UtilsSource],
    [HOST_BINDING_MODULE_NAMES.r2Client, sources.r2ClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const doModuleInjections = [
    requestIdModuleInjection,
    [HOST_BINDING_MODULE_NAMES.doTransport, sources.doTransportSource],
    [HOST_BINDING_MODULE_NAMES.ownerEndpoint, sources.ownerEndpointSource],
    [HOST_BINDING_MODULE_NAMES.ownerHintCache, sources.ownerHintCacheSource],
    [HOST_BINDING_MODULE_NAMES.doClient, sources.doClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const workflowsModuleInjections = [
    requestIdModuleInjection,
    [HOST_BINDING_MODULE_NAMES.workflowsClient, sources.workflowsClientSource],
  ];
  /** @type {RuntimeModuleInjection[]} */
  const aiModuleInjections = [
    requestIdModuleInjection,
    [HOST_BINDING_MODULE_NAMES.aiClient, sources.aiClientSource],
  ];
  return {
    d1ModuleInjections,
    r2ModuleInjections,
    doModuleInjections,
    workflowsModuleInjections,
    aiModuleInjections,
  };
}

/**
 * @param {RuntimeMetaPlan} plan
 * @param {RuntimeBindingSpec} spec
 * @param {string} name
 */
function addHostFacadeBinding(plan, spec, name) {
  if (typeof spec?.type !== "string") return;
  const definition = HOST_FACADE_BINDING_DEFINITIONS[spec.type];
  if (definition) plan[definition.planKey].push(name);
}

/** @param {RuntimeMetaPlan} plan */
function hasHostFacadeBindings(plan) {
  return Object.values(HOST_FACADE_BINDING_DEFINITIONS).some(
    ({ planKey }) => plan[planKey].length > 0
  );
}

/** @param {RuntimeBundleMeta} meta */
function exportedEntrypointNames(meta) {
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
  const out = new Set(exportedEntrypointNames(meta));
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
    aiBindings: [],
    workflowBindings: Object.create(null),
    hostWrappedClassNames: [],
    needsHostBindingWrapper: false,
  };
  for (const [name, spec] of bindingEntries) {
    if (spec?.type === "ai") {
      if (Object.keys(spec).length !== 1) {
        throw new Error(
          `AI binding ${JSON.stringify(name)} has invalid persisted shape (redeploy worker)`
        );
      }
      if (plan.aiBindings.length > 0) {
        throw new Error("Persisted metadata contains more than one AI binding (redeploy worker)");
      }
    }
    addHostFacadeBinding(plan, spec, name);
  }
  for (const workflow of workflows) {
    if (typeof workflow?.binding === "string" && workflow.binding) plan.workflowBindings[workflow.binding] = workflow;
  }
  plan.needsHostBindingWrapper =
    hasHostFacadeBindings(plan) || Object.keys(plan.workflowBindings).length > 0;
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
function runtimeInjectedModuleSources(
  mainModule,
  meta,
  runtimeSources,
  plan = analyzeRuntimeMeta(meta)
) {
  const injections = runtimeModuleInjections(runtimeSources);
  /** @type {Map<string, string>} */
  const out = new Map();
  /** @param {RuntimeModuleInjection[]} modules */
  const addModules = (modules) => {
    for (const [name, source] of modules) out.set(name, source);
  };
  for (const { planKey, injectionKey } of Object.values(HOST_FACADE_BINDING_DEFINITIONS)) {
    if (plan[planKey].length > 0) addModules(injections[injectionKey]);
  }
  if (Object.keys(plan.workflowBindings).length > 0) {
    addModules(injections.workflowsModuleInjections);
  }
  out.set(WORKFLOWS_MODULE_NAME, WORKFLOWS_MODULE_SOURCE);
  if (plan.needsHostBindingWrapper) {
    out.set(HOST_BINDING_RUNTIME_MODULE_NAME, HOST_BINDING_RUNTIME_SOURCE);
  }
  // `_wdl-wrapper.js` is always injected: host bindings use the larger wrapper,
  // and otherwise the abort shim still rewrites the user main module.
  out.set(
    "_wdl-wrapper.js",
    plan.needsHostBindingWrapper
      ? generateHostBindingWrapperModule(
          mainModule,
          {
            d1Bindings: plan.d1Bindings,
            r2Bindings: plan.r2Bindings,
            doBindings: plan.doBindings,
            workflowBindings: plan.workflowBindings,
            entrypointNames: plan.hostWrappedClassNames,
            aiBindings: plan.aiBindings,
            importableEnvDisabled: Array.isArray(meta.compatibilityFlags) &&
              meta.compatibilityFlags.includes("disallow_importable_env"),
          }
        )
      : generateAbortShimWrapperModule(mainModule)
  );
  return [...out];
}

/**
 * @param {WorkerCodeShape} workerCode
 * @param {RuntimeBundleMeta} meta
 * @param {RuntimeInjectionSources} runtimeSources
 * @param {{ plan?: RuntimeMetaPlan }} [options]
 */
export function injectRuntimeModulesForHostBindings(
  workerCode,
  meta,
  runtimeSources,
  options = {}
) {
  const plan = options.plan || analyzeRuntimeMeta(meta);
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
  for (const [name, source] of runtimeInjectedModuleSources(
    originalMain,
    meta,
    runtimeSources,
    plan
  )) {
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
 *   normalized: NormalizedModule[],
 *   meta: RuntimeBundleMeta,
 * }} args
 */
export function estimateWorkerLoaderUserCodeBytes({ normalized, meta }) {
  /** @type {Record<string, string>} */
  const jsModules = Object.create(null);
  let total = 0;
  for (const [name, body] of normalized) {
    const type = moduleType(meta.modules, name);
    if (type !== "module" || !containsWorkflowsImportMarker(body)) {
      total += type === "module" || type === "cjs" || type === "text"
        ? decodedTextModuleByteLength(body)
        : moduleBodyByteLength(body);
      continue;
    }
    jsModules[name] = typeof body === "string" ? body : utf8Decoder.decode(body);
  }
  rewriteCloudflareWorkflowsImports({ modules: jsModules });
  for (const source of Object.values(jsModules)) total += nodeBuffer.byteLength(source, "utf8");
  return total;
}

/**
 * @param {{
 *   mainModule: string,
 *   normalized: NormalizedModule[],
 *   meta: RuntimeBundleMeta,
 *   runtimeSources: RuntimeInjectionSources,
 *   userCodeBytes?: number,
 * }} args
 */
export function estimateFinalWorkerLoaderCodeBytes({
  mainModule,
  normalized,
  meta,
  runtimeSources,
  userCodeBytes = estimateWorkerLoaderUserCodeBytes({ normalized, meta }),
}) {
  let total = userCodeBytes;
  for (const [, source] of runtimeInjectedModuleSources(
    mainModule,
    meta,
    runtimeSources,
    analyzeRuntimeMeta(meta)
  )) {
    total += nodeBuffer.byteLength(source, "utf8");
  }
  return total;
}
