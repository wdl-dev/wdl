import {
  WORKER_LOADER_CODE_MAX_BYTES,
  estimateFinalWorkerLoaderCodeBytes,
  estimateWorkerLoaderUserCodeBytes,
} from "runtime-load-code-budget";
import { RUNTIME_INJECTION_SOURCES } from "runtime-load-injection-sources";
import {
  RUNTIME_WRAPPER_MODULE_NAME,
  WDL_RESERVED_MODULE_PREFIX,
  isWdlReservedModuleName,
} from "runtime-load-module-rewrite";
import { estimateDoRuntimeInjectedCodeBytes } from "do-runtime-load-code-budget";
import { errorMessage } from "shared-errors";
import { DO_ALARM_SHIM_SOURCE } from "do-runtime-alarm-shim-source";

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
 * @param {{
 *   mainModule: unknown,
 *   normalized: Array<[string, string | Uint8Array]>,
 *   ns?: string,
 *   worker?: string,
 *   version?: string,
 * }} args
 */
function assertFinalWorkerCodeShape({ mainModule, normalized, ns, worker, version }) {
  const label = ns && worker ? `${ns}/${worker}${version ? `@${version}` : ""}` : "worker";
  if (typeof mainModule !== "string" || !mainModule) {
    throw workerCodeInvalid(`final WorkerCode for ${label} requires prepared meta.mainModule`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
    });
  }
  if (isWdlReservedModuleName(mainModule)) {
    throw workerCodeInvalid(`final WorkerCode for ${label} uses WDL-reserved mainModule ${mainModule}`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
      module: mainModule,
      reserved_module_prefix: WDL_RESERVED_MODULE_PREFIX,
    });
  }
  for (const [name] of normalized) {
    if (!isWdlReservedModuleName(name)) continue;
    throw workerCodeInvalid(`final WorkerCode for ${label} uses WDL-reserved module name ${name}`, {
      ...(ns ? { namespace: ns } : {}),
      ...(worker ? { worker } : {}),
      ...(version ? { version } : {}),
      module: name,
      reserved_module_prefix: WDL_RESERVED_MODULE_PREFIX,
    });
  }
}

/**
 * @param {{
 *   meta: { mainModule?: unknown, modules?: Record<string, { type?: unknown }> | null, [key: string]: unknown },
 *   normalized: Array<[string, string | Uint8Array]>,
 *   userCodeBytes?: number,
 * }} bundle
 * @param {{ ns: string, worker: string, version?: string }} context
 */
function estimateWorkerLoaderCodeBytesWithContext(bundle, context) {
  const details = {
    ...(context.ns ? { namespace: context.ns } : {}),
    ...(context.worker ? { worker: context.worker } : {}),
    ...(context.version ? { version: context.version } : {}),
  };
  try {
    const mainModule = bundle.meta.mainModule;
    assertFinalWorkerCodeShape({
      mainModule,
      normalized: bundle.normalized,
      ns: context.ns,
      worker: context.worker,
      version: context.version,
    });
    const userCodeBytes = bundle.userCodeBytes ??
      estimateWorkerLoaderUserCodeBytes({ normalized: bundle.normalized, meta: bundle.meta });
    const runtimeBytes = estimateFinalWorkerLoaderCodeBytes({
      mainModule: /** @type {string} */ (mainModule),
      normalized: bundle.normalized,
      meta: bundle.meta,
      runtimeSources: RUNTIME_INJECTION_SOURCES,
      userCodeBytes,
    });
    // do-runtime cold-loads the same bundle after the generic runtime wrapper has
    // made `_wdl-wrapper.js` the main module, then adds its alarm-storage wrapper
    // around exported DO classes. The stock workerLoader 64 MiB cap applies to
    // that second WorkerCode too.
    return {
      bytes: runtimeBytes + estimateDoRuntimeInjectedCodeBytes(
        RUNTIME_WRAPPER_MODULE_NAME,
        bundle.meta,
        DO_ALARM_SHIM_SOURCE
      ),
      userCodeBytes,
    };
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
 *   userCodeBytes?: number,
 * }} args
 * @returns {{ bytes: number, userCodeBytes: number }}
 */
export function assertWorkerLoaderCodeBudget({
  ns,
  worker,
  version = undefined,
  meta,
  normalized,
  userCodeBytes = undefined,
}) {
  const estimate = estimateWorkerLoaderCodeBytesWithContext(
    { meta, normalized, userCodeBytes },
    { ns, worker, version }
  );
  if (estimate.bytes <= WORKER_LOADER_CODE_MAX_BYTES) return estimate;
  const label = version ? `${ns}/${worker}@${version}` : `${ns}/${worker}`;
  throw new WorkerCodeBudgetError(
    `final WorkerCode for ${label} totals ${estimate.bytes} bytes, ` +
      `exceeding workerd workerLoader code limit ${WORKER_LOADER_CODE_MAX_BYTES} bytes`,
    {
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
      code_bytes: estimate.bytes,
      max_code_bytes: WORKER_LOADER_CODE_MAX_BYTES,
    }
  );
}
