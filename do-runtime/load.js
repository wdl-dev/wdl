import { bundleToWorkerCode } from "runtime-lib";
import {
  buildWorkerEnv,
  decodeRuntimeLoadPayload,
  internalAuthBackend,
  runtimeLoadContentTypeMatches,
  wrapWorkerCodeForHostBindings,
} from "runtime-load";
import { DoRuntimeError } from "do-runtime-protocol";
import { formatError, logStructured } from "shared-observability";
import { withInternalAuth } from "shared-internal-auth";
import { discardResponseBody } from "shared-respond";
import { DO_ALARM_SHIM_SOURCE } from "do-runtime-alarm-shim-source";
import {
  DO_RUNTIME_RESERVED_MODULE,
  doRuntimeInjectedModuleSources,
} from "do-runtime-load-code-budget";

const REDIS_PROXY_LOAD_TIMEOUT_MS = 5000;
const REDIS_PROXY_LOAD_RETRIES = 3;
const NATIVE_DELETE_ALL_DELETES_ALARM_FLAG = "delete_all_deletes_alarm";
const NATIVE_DELETE_ALL_PRESERVES_ALARM_FLAG = "delete_all_preserves_alarm";

/**
 * @typedef {{ REDIS_PROXY_URL?: unknown, LOG_LOAD_FAILURES?: unknown, ASSETS_CDN_BASE?: string | null, DO_BACKEND?: unknown, WORKFLOWS_BACKEND?: unknown, PUBLIC_NETWORK?: unknown }} DoEnv
 * @typedef {import("do-runtime-protocol").DoInvoke & { ns: string, worker: string, version: string, doStorageId: string, workerId: string }} DoInvoke
 * @typedef {Record<string, unknown> & { type?: string, className?: unknown }} WorkerBindingSpec
 * @typedef {{ bindings?: Record<string, WorkerBindingSpec> | null }} WorkerMeta
 * @typedef {Record<string, unknown> & { doStorageId?: unknown, className?: unknown }} RuntimeBindingSpec
 * @typedef {{ exports: Record<string, (options: { props: Record<string, unknown> }) => unknown> & { KV(options: { props: Record<string, unknown> }): unknown, Assets(options: { props: Record<string, unknown> }): unknown, QueueProducer(options: { props: Record<string, unknown> }): unknown, D1Database(options: { props: Record<string, unknown> }): unknown, R2Bucket(options: { props: Record<string, unknown> }): unknown, ServiceBinding(options: { props: Record<string, unknown> }): unknown, DurableObjectNamespace(options: { props: Record<string, unknown> }): unknown, DoAlarmBinding(options: { props: Record<string, unknown> }): unknown, InternalAuthBackend(options: { props: Record<string, unknown> }): unknown } }} DoRuntimeContext
 * @typedef {{ name: string, spec: RuntimeBindingSpec }} DoBindingInput
 * @typedef {string | { cjs: string } | { text: string } | { json: unknown } | { wasm: Uint8Array } | { data: Uint8Array }} WorkerModuleValue
 * @typedef {{ mainModule: string, modules: Record<string, WorkerModuleValue>, compatibilityFlags?: string[], compatibilityDate?: string, env?: Record<string, unknown>, globalOutbound?: unknown }} WorkerCode
 */

/** @param {DoEnv} env */
function redisProxyUrl(env) {
  if (!env.REDIS_PROXY_URL) {
    throw new DoRuntimeError(503, "bundle_loader_unavailable", "do-runtime requires REDIS_PROXY_URL");
  }
  return String(env.REDIS_PROXY_URL).replace(/\/+$/, "");
}

/** @param {unknown} err */
function isRetryableLoadError(err) {
  if (err instanceof DoRuntimeError) {
    return err.status >= 500 && (
      err.code === "bundle_load_failed" ||
      err.code === "bundle_load_timeout"
    );
  }
  return true;
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 */
async function loadViaProxyOnce(env, invoke) {
  const url = new URL(`${redisProxyUrl(env)}/runtime/load`);
  url.searchParams.set("ns", invoke.ns);
  url.searchParams.set("worker", invoke.worker);
  url.searchParams.set("version", invoke.version);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REDIS_PROXY_LOAD_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      headers: withInternalAuth(undefined, env),
      signal: controller.signal,
    });
    if (!response.ok) {
      await discardResponseBody(response);
      if (response.status === 404) {
        throw new DoRuntimeError(
          404,
          "bundle_not_found",
          "DO worker bundle was not found",
          { upstreamStatus: response.status }
        );
      }
      throw new DoRuntimeError(
        response.status >= 500 ? 503 : response.status,
        "bundle_load_failed",
        "Redis proxy runtime load failed",
        { upstreamStatus: response.status }
      );
    }
    const contentType = response.headers.get("content-type") || "";
    if (!runtimeLoadContentTypeMatches(contentType)) {
      await discardResponseBody(response);
      throw new DoRuntimeError(
        503,
        "bundle_load_invalid_content_type",
        `Redis proxy runtime load returned unsupported content-type ${contentType || "<missing>"}`
      );
    }
    return decodeRuntimeLoadPayload(await response.arrayBuffer());
  } catch (err) {
    if (controller.signal.aborted) {
      throw new DoRuntimeError(
        503,
        "bundle_load_timeout",
        `Redis proxy runtime load timed out after ${REDIS_PROXY_LOAD_TIMEOUT_MS}ms`
      );
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * @param {DoEnv} env
 * @param {DoInvoke} invoke
 * @param {string | null} [requestId]
 */
export async function loadViaProxy(env, invoke, requestId = null) {
  let lastErr;
  for (let attempt = 1; attempt <= REDIS_PROXY_LOAD_RETRIES; attempt += 1) {
    try {
      return await loadViaProxyOnce(env, invoke);
    } catch (err) {
      lastErr = err;
      if (attempt === REDIS_PROXY_LOAD_RETRIES || !isRetryableLoadError(err)) break;
      if (env.LOG_LOAD_FAILURES !== "0") {
        logStructured("do-runtime", "warn", "do_bundle_load_proxy_retry", {
          request_id: requestId,
          worker_id: invoke.workerId,
          attempt,
          ...formatError(err),
        });
      }
      await new Promise((resolve) => setTimeout(resolve, 100 * (2 ** (attempt - 1))));
    }
  }
  throw lastErr;
}

/**
 * @param {WorkerCode} workerCode
 * @param {WorkerMeta} meta
 */
function wrapWorkerCodeForDoRuntime(workerCode, meta) {
  const originalMain = workerCode.mainModule;
  const injections = doRuntimeInjectedModuleSources(originalMain, meta, DO_ALARM_SHIM_SOURCE);
  if (!injections.length) return workerCode;
  const collision = injections.find(([moduleName]) => Object.hasOwn(workerCode.modules, moduleName));
  if (collision) {
    throw new DoRuntimeError(400, "reserved_module_name", `do-runtime requires reserved module name ${collision[0]}`);
  }
  for (const [moduleName, source] of injections) workerCode.modules[moduleName] = source;
  workerCode.mainModule = DO_RUNTIME_RESERVED_MODULE;
  return workerCode;
}

/** @param {WorkerCode} workerCode */
function forceNativeDeleteAllPreservesAlarm(workerCode) {
  const flags = Array.isArray(workerCode.compatibilityFlags)
    ? workerCode.compatibilityFlags.filter((flag) => flag !== NATIVE_DELETE_ALL_DELETES_ALARM_FLAG)
    : [];
  if (!flags.includes(NATIVE_DELETE_ALL_PRESERVES_ALARM_FLAG)) {
    flags.push(NATIVE_DELETE_ALL_PRESERVES_ALARM_FLAG);
  }
  workerCode.compatibilityFlags = flags;
}

/**
 * @param {WorkerMeta} meta
 * @param {Record<string, string>} nsSecrets
 * @param {Record<string, string>} workerSecrets
 * @param {DoEnv} env
 * @param {DoRuntimeContext} ctx
 * @param {DoInvoke} invoke
 */
export function buildDoEnv(meta, nsSecrets, workerSecrets, env, ctx, invoke) {
  const out = buildWorkerEnv(
    meta,
    nsSecrets,
    workerSecrets,
    invoke.ns,
    invoke.worker,
    invoke.version,
    env.ASSETS_CDN_BASE,
    ctx,
    internalAuthBackend(ctx, env, "DO_BACKEND"),
    {
      workflowsBackend: internalAuthBackend(ctx, env, "WORKFLOWS_BACKEND"),
      doBindingFactory(binding) {
        const { name, spec } = /** @type {DoBindingInput} */ (binding);
        return ctx.exports.DurableObjectNamespace({
          props: {
            ns: invoke.ns,
            worker: invoke.worker,
            version: invoke.version,
            doStorageId: spec.doStorageId,
            binding: name,
            className: spec.className,
          },
        });
      },
    }
  );
  out.__WDL_DO_ALARMS__ = ctx.exports.DoAlarmBinding({
    props: {
      ns: invoke.ns,
      worker: invoke.worker,
      version: invoke.version,
      doStorageId: invoke.doStorageId,
    },
  });
  return out;
}

/**
 * @param {DoEnv} env
 * @param {unknown} ctx
 * @param {DoInvoke} invoke
 * @param {string | null} [requestId]
 */
export async function loadDoWorkerCode(env, ctx, invoke, requestId = null) {
  const startedAt = Date.now();
  try {
    const loaded = await loadViaProxy(env, invoke, requestId);
    const { meta, ...codeBase } = bundleToWorkerCode(loaded.bundle);
    const workerCode = {
      ...codeBase,
      env: buildDoEnv(
        meta,
        loaded.ns_secrets,
        loaded.worker_secrets,
        env,
        /** @type {DoRuntimeContext} */ (/** @type {unknown} */ (ctx)),
        invoke
      ),
      globalOutbound: env.PUBLIC_NETWORK,
    };
    // Keep this order. The generic runtime wrapper may replace mainModule with
    // _wdl-wrapper.js so DO classes get host-binding facades first; the
    // do-runtime wrapper then imports that module and layers only the alarm
    // storage proxy on top.
    wrapWorkerCodeForHostBindings(workerCode, meta);
    // The shim owns DO alarm semantics. Keep native SQLite deleteAll() away
    // from workerd's alarm scheduler, which is not valid for facet-backed DOs.
    forceNativeDeleteAllPreservesAlarm(workerCode);
    wrapWorkerCodeForDoRuntime(workerCode, meta);
    return workerCode;
  } catch (err) {
    if (env.LOG_LOAD_FAILURES !== "0") {
      logStructured("do-runtime", "error", "do_bundle_load_failed", {
        request_id: requestId,
        worker_id: invoke.workerId,
        duration_ms: Date.now() - startedAt,
        ...formatError(err),
      });
    }
    throw err;
  }
}
