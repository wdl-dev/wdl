export const DO_RUNTIME_RESERVED_MODULE = "_wdl-do-runtime-wrapper.js";
export const DO_ALARM_SHIM_MODULE = "_wdl-do-alarm-shim.js";

/**
 * @typedef {Record<string, unknown> & { type?: string, className?: unknown }} WorkerBindingSpec
 * @typedef {Record<string, unknown> & { bindings?: Record<string, WorkerBindingSpec> | null }} WorkerMeta
 * @typedef {[name: string, source: string]} DoRuntimeModuleInjection
 */

/** @param {WorkerMeta} meta */
function doRuntimeClassNames(meta) {
  /** @type {Set<string>} */
  const out = new Set();
  for (const spec of Object.values(meta.bindings || {})) {
    if (spec?.type === "do" && typeof spec.className === "string" && spec.className) {
      out.add(spec.className);
    }
  }
  return [...out];
}

/** @param {WorkerMeta} meta */
export function hasDoRuntimeInjectedModules(meta) {
  return doRuntimeClassNames(meta).length > 0;
}

/**
 * @param {string} userMainSpecifier
 * @param {string[]} classNames
 */
function generateDoRuntimeWrapperModule(userMainSpecifier, classNames) {
  const userMain = JSON.stringify(`./${userMainSpecifier}`);
  const alarmShim = JSON.stringify(`./${DO_ALARM_SHIM_MODULE}`);
  const wrappedClasses = classNames.map((name) => `
export class ${name} extends wrapDurableObjectClass(user.${name}, ${JSON.stringify(name)}) {}
`).join("");
  return `
import * as user from ${userMain};
export * from ${userMain};

import { wrapDurableObjectClass } from ${alarmShim};

${wrappedClasses}
`;
}

/**
 * @param {string} mainModule
 * @param {WorkerMeta} meta
 * @param {string} alarmShimSource
 * @returns {DoRuntimeModuleInjection[]}
 */
export function doRuntimeInjectedModuleSources(mainModule, meta, alarmShimSource) {
  const classNames = doRuntimeClassNames(meta);
  if (!classNames.length) return [];
  return [
    [DO_ALARM_SHIM_MODULE, alarmShimSource],
    [DO_RUNTIME_RESERVED_MODULE, generateDoRuntimeWrapperModule(mainModule, classNames)],
  ];
}

/**
 * @param {string} mainModule
 * @param {WorkerMeta} meta
 * @param {string} alarmShimSource
 */
export function estimateDoRuntimeInjectedCodeBytes(mainModule, meta, alarmShimSource) {
  let total = 0;
  for (const [, source] of doRuntimeInjectedModuleSources(mainModule, meta, alarmShimSource)) {
    total += Buffer.byteLength(source, "utf8");
  }
  return total;
}
