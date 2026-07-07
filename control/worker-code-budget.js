import {
  WORKER_LOADER_CODE_MAX_BYTES,
  estimateFinalWorkerLoaderCodeBytes,
} from "runtime-load-code-budget";
import { RUNTIME_INJECTION_SOURCES } from "runtime-load-injection-sources";
import { HOST_BINDING_RESERVED_MODULE_NAMES } from "runtime-load-module-rewrite";
import {
  DO_ALARM_SHIM_MODULE,
  DO_RUNTIME_RESERVED_MODULE,
  estimateDoRuntimeInjectedCodeBytes,
  hasDoRuntimeInjectedModules,
} from "do-runtime-load-code-budget";
import { errorMessage } from "shared-errors";
import { DO_ALARM_SHIM_SOURCE } from "do-runtime-alarm-shim-source";

export { WORKER_LOADER_CODE_MAX_BYTES };

export class WorkerCodeBudgetError extends Error {
  /**
   * @param {string} message
   * @param {Record<string, unknown>} [details]
   * @param {{ status?: number, code?: string }} [options]
   */
  constructor(message, details = {}, options = {}) {
    super(message);
    this.status = options.status ?? 413;
    this.code = options.code ?? "worker_code_too_large";
    this.details = details;
  }
}

/**
 * @param {string} message
 * @param {Record<string, unknown>} details
 */
function workerCodeInvalid(message, details) {
  return new WorkerCodeBudgetError(message, details, {
    status: 400,
    code: "worker_code_invalid",
  });
}

/**
 * @param {unknown} err
 * @param {Record<string, unknown>} details
 */
function wrapWorkerCodeInvalid(err, details) {
  if (err instanceof WorkerCodeBudgetError) return err;
  const message = errorMessage(err);
  return workerCodeInvalid(`final WorkerCode is invalid: ${message}`, details);
}

/**
 * @param {{ mainModule?: unknown, [key: string]: unknown }} meta
 */
function reservedModuleNamesForMeta(meta) {
  const names = [...HOST_BINDING_RESERVED_MODULE_NAMES];
  if (hasDoRuntimeInjectedModules(meta)) {
    names.push(DO_ALARM_SHIM_MODULE, DO_RUNTIME_RESERVED_MODULE);
  }
  return names;
}

/**
 * @param {{
 *   mainModule: unknown,
 *   meta: { mainModule?: unknown, [key: string]: unknown },
 *   normalized: Array<[string, string | Uint8Array]>,
 *   ns?: string,
 *   worker?: string,
 *   version?: string,
 * }} args
 */
function assertFinalWorkerCodeShape({ mainModule, meta, normalized, ns, worker, version }) {
  const label = ns && worker ? `${ns}/${worker}${version ? `@${version}` : ""}` : "worker";
  const reservedModuleNames = reservedModuleNamesForMeta(meta);
  const reservedModules = new Set(reservedModuleNames);
  if (typeof mainModule !== "string" || !mainModule) {
    throw workerCodeInvalid(`final WorkerCode for ${label} requires prepared meta.mainModule`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
    });
  }
  if (reservedModules.has(mainModule)) {
    throw workerCodeInvalid(`final WorkerCode for ${label} uses reserved mainModule ${mainModule}`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
      module: mainModule,
      reserved_modules: reservedModuleNames,
    });
  }
  for (const [name] of normalized) {
    if (!reservedModules.has(name)) continue;
    throw workerCodeInvalid(`final WorkerCode for ${label} uses reserved module name ${name}`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
      module: name,
      reserved_modules: reservedModuleNames,
    });
  }
}

/**
 * @param {{
 *   meta: { mainModule?: unknown, modules?: Record<string, { type?: unknown }> | null, [key: string]: unknown },
 *   normalized: Array<[string, string | Uint8Array]>,
 * }} bundle
 * @param {{ ns?: string, worker?: string, version?: string }} [context]
 */
function estimateWorkerLoaderCodeBytesWithContext(bundle, context = {}) {
  const details = {
    ...(context.ns ? { namespace: context.ns } : {}),
    ...(context.worker ? { worker: context.worker } : {}),
    ...(context.version ? { version: context.version } : {}),
  };
  try {
    const mainModule = bundle.meta.mainModule;
    assertFinalWorkerCodeShape({
      mainModule,
      meta: bundle.meta,
      normalized: bundle.normalized,
      ns: context.ns,
      worker: context.worker,
      version: context.version,
    });
    const runtimeBytes = estimateFinalWorkerLoaderCodeBytes({
      mainModule: /** @type {string} */ (mainModule),
      normalized: bundle.normalized,
      meta: bundle.meta,
      runtimeSources: RUNTIME_INJECTION_SOURCES,
    });
    // do-runtime cold-loads the same bundle after the generic runtime wrapper has
    // made `_wdl-wrapper.js` the main module, then adds its alarm-storage wrapper
    // around exported DO classes. The stock workerLoader 64 MiB cap applies to
    // that second WorkerCode too.
    return runtimeBytes + estimateDoRuntimeInjectedCodeBytes(
      "_wdl-wrapper.js",
      bundle.meta,
      DO_ALARM_SHIM_SOURCE
    );
  } catch (err) {
    throw wrapWorkerCodeInvalid(err, details);
  }
}

/**
 * @param {{
 *   ns: string,
 *   worker: string,
 *   version?: string,
 *   meta: { mainModule?: unknown, modules?: Record<string, { type?: unknown }> | null, [key: string]: unknown },
 *   normalized: Array<[string, string | Uint8Array]>,
 * }} args
 */
export function assertWorkerLoaderCodeBudget({ ns, worker, version = undefined, meta, normalized }) {
  const bytes = estimateWorkerLoaderCodeBytesWithContext({ meta, normalized }, { ns, worker, version });
  if (bytes <= WORKER_LOADER_CODE_MAX_BYTES) return bytes;
  const label = version ? `${ns}/${worker}@${version}` : `${ns}/${worker}`;
  throw new WorkerCodeBudgetError(
    `final WorkerCode for ${label} totals ${bytes} bytes, ` +
      `exceeding workerd workerLoader code limit ${WORKER_LOADER_CODE_MAX_BYTES} bytes`,
    {
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
      code_bytes: bytes,
      max_code_bytes: WORKER_LOADER_CODE_MAX_BYTES,
    }
  );
}
