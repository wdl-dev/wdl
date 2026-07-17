// Bundle + secrets cold-load, factored out of index.js so
// bindings/service.js can reuse it without an import cycle.

import { bundleToWorkerCode } from "runtime-lib";
import { formatError } from "shared-observability";
import { withInternalAuth } from "shared-internal-auth";
import { discardResponseBody } from "shared-respond";
import { formatWorkerId, parseRuntimeLoadWorkerId } from "shared-worker-id";
import { evictSiblings, recordLoadedWorker } from "runtime-state";
import {
  analyzeRuntimeMeta,
  injectRuntimeModulesForHostBindings,
} from "runtime-load-code-budget";
import { RUNTIME_INJECTION_SOURCES } from "runtime-load-injection-sources";
import { buildWorkerEnv } from "runtime-load-env-build";
export { buildWorkerEnv, doAlarmBindingProps } from "runtime-load-env-build";

const REDIS_PROXY_LOAD_TIMEOUT_MS = 8000;

const RUNTIME_LOAD_MAGIC = "WDLLOAD!";
const RUNTIME_LOAD_CONTENT_TYPE = "application/vnd.wdl.runtime-load";
const MAX_RUNTIME_LOAD_HEADER_BYTES = 1024 * 1024;
const utf8Decoder = new TextDecoder();

/**
 * @typedef {{ bundle: Record<string, Uint8Array>, ns_secrets: Record<string, string>, worker_secrets: Record<string, string> }} RuntimeLoadPayload
 * @typedef {string | { cjs: string } | { text: string } | { json: unknown } | { wasm: Uint8Array } | { data: Uint8Array }} WorkerModuleValue
 * @typedef {{ modules: Record<string, WorkerModuleValue>, mainModule: string, [key: string]: unknown }} WorkerCodeShape
 * @typedef {Record<string, unknown> & { type?: string, className?: unknown }} RuntimeBindingSpec
 * @typedef {{ binding?: unknown, className?: unknown }} RuntimeWorkflowSpec
 * @typedef {{ entrypoint?: unknown }} RuntimeExportSpec
 * @typedef {{ bindings?: Record<string, RuntimeBindingSpec> | null, workflows?: RuntimeWorkflowSpec[] | null, exports?: RuntimeExportSpec[] | null }} RuntimeBundleMeta
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
 *   SERVICE_NAME?: unknown,
 *   REDIS_PROXY_URL?: unknown,
 *   ASSETS_CDN_BASE?: string | null,
 *   DO_BACKEND?: unknown,
 *   DO_OWNER_NETWORK?: unknown,
 *   WORKFLOWS_BACKEND?: unknown,
 *   PUBLIC_NETWORK?: unknown,
 *   TAIL_WORKER?: unknown,
 *   [key: string]: unknown,
 * }} RuntimeLoaderEnv
 * @typedef {{
 *   increment(name: string, labels?: Record<string, unknown>, value?: number): void,
 *   observe(name: string, labels: Record<string, string | number | boolean> | null | undefined, value: number): void,
 * }} RuntimeLoaderMetrics
 * @typedef {(options: { props: Record<string, unknown> }) => unknown} RuntimeEntrypointFactory
 * @typedef {{ exports: Record<string, RuntimeEntrypointFactory> & { KV: RuntimeEntrypointFactory, Assets: RuntimeEntrypointFactory, QueueProducer: RuntimeEntrypointFactory, D1Database: RuntimeEntrypointFactory, R2Bucket: RuntimeEntrypointFactory, ServiceBinding: RuntimeEntrypointFactory, DurableObjectNamespace: RuntimeEntrypointFactory, InternalAuthBackend: RuntimeEntrypointFactory } }} RuntimeContext
 */

/**
 * Keep the internal auth token in the host loader realm. Generated tenant
 * facades receive a cloneable WorkerEntrypoint stub, not a plain object with a
 * function property, so workerd can pass it through workerLoader env cloning.
 *
 * @param {RuntimeContext} ctx
 * @param {RuntimeLoaderEnv} env
 * @param {"DO_BACKEND" | "DO_OWNER_NETWORK" | "WORKFLOWS_BACKEND"} binding
 * @returns {unknown}
 */
export function internalAuthBackend(ctx, env, binding) {
  const backend = env[binding];
  if (!backend || typeof /** @type {{ fetch?: unknown }} */ (backend).fetch !== "function") {
    throw new Error(`${binding} service binding is not configured`);
  }
  if (typeof ctx.exports.InternalAuthBackend !== "function") {
    throw new Error("InternalAuthBackend runtime binding adapter is not configured");
  }
  return ctx.exports.InternalAuthBackend({ props: { binding } });
}

/** @param {unknown} contentType */
export function runtimeLoadContentTypeMatches(contentType) {
  if (typeof contentType !== "string") return false;
  return contentType.split(";", 1)[0].trim().toLowerCase() === RUNTIME_LOAD_CONTENT_TYPE;
}

/** @param {Uint8Array} bytes */
function hasRuntimeLoadMagic(bytes) {
  if (bytes.length < RUNTIME_LOAD_MAGIC.length) return false;
  for (let i = 0; i < RUNTIME_LOAD_MAGIC.length; i++) {
    if (bytes[i] !== RUNTIME_LOAD_MAGIC.charCodeAt(i)) return false;
  }
  return true;
}

/** @param {DataView} view @param {number} offset */
function readU32(view, offset) {
  if (offset + 4 > view.byteLength) {
    throw new Error("Redis proxy runtime load payload is truncated");
  }
  return view.getUint32(offset, false);
}

/** @param {ArrayBuffer} buffer @returns {RuntimeLoadPayload} */
export function decodeRuntimeLoadPayload(buffer) {
  const bytes = new Uint8Array(buffer);
  if (!hasRuntimeLoadMagic(bytes)) {
    throw new Error("Redis proxy runtime load payload has invalid magic");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = RUNTIME_LOAD_MAGIC.length;

  const headerLen = readU32(view, offset);
  offset += 4;
  if (headerLen > MAX_RUNTIME_LOAD_HEADER_BYTES) {
    throw new Error(`Redis proxy runtime load payload header exceeds ${MAX_RUNTIME_LOAD_HEADER_BYTES} bytes`);
  }
  if (offset + headerLen > bytes.length) {
    throw new Error("Redis proxy runtime load payload header is truncated");
  }
  const header = /** @type {{ ns_secrets?: Record<string, string>, worker_secrets?: Record<string, string> }} */ (headerLen === 0
    ? {}
    : JSON.parse(utf8Decoder.decode(bytes.subarray(offset, offset + headerLen))));
  offset += headerLen;

  const entryCount = readU32(view, offset);
  offset += 4;
  /** @type {Record<string, Uint8Array>} */
  const bundle = {};
  for (let i = 0; i < entryCount; i++) {
    const keyLen = readU32(view, offset);
    offset += 4;
    const valueLen = readU32(view, offset);
    offset += 4;
    if (offset + keyLen + valueLen > bytes.length) {
      throw new Error("Redis proxy runtime load payload entry is truncated");
    }
    const key = utf8Decoder.decode(bytes.subarray(offset, offset + keyLen));
    offset += keyLen;
    // Copy each module value so user-visible wasm/data modules cannot expose
    // the full cold-load envelope, which also contains secrets.
    Object.defineProperty(bundle, key, {
      value: bytes.slice(offset, offset + valueLen),
      enumerable: true,
      configurable: true,
      writable: true,
    });
    offset += valueLen;
  }
  if (offset !== bytes.length) {
    throw new Error("Redis proxy runtime load payload has trailing bytes");
  }

  return {
    bundle,
    ns_secrets: header.ns_secrets || {},
    worker_secrets: header.worker_secrets || {},
  };
}

/** @param {Record<string, unknown>} env */
function redisProxyUrl(env) {
  if (!env.REDIS_PROXY_URL) {
    throw new Error("Runtime requires REDIS_PROXY_URL");
  }
  return String(env.REDIS_PROXY_URL).replace(/\/+$/, "");
}

/** @param {WorkerCodeShape} workerCode @param {RuntimeBundleMeta} meta @param {RuntimeMetaPlan} [plan] */
export function wrapWorkerCodeForHostBindings(workerCode, meta, plan = analyzeRuntimeMeta(meta)) {
  return injectRuntimeModulesForHostBindings(workerCode, meta, RUNTIME_INJECTION_SOURCES, plan);
}

/** @param {Record<string, unknown>} env @param {string} ns @param {string} worker @param {string} version */
async function loadViaProxy(env, ns, worker, version) {
  const url = new URL(`${redisProxyUrl(env)}/runtime/load`);
  url.searchParams.set("ns", ns);
  url.searchParams.set("worker", worker);
  url.searchParams.set("version", version);
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    REDIS_PROXY_LOAD_TIMEOUT_MS
  );
  try {
    const res = await fetch(url, {
      headers: withInternalAuth(undefined, env),
      signal: controller.signal,
    });
    if (!res.ok) {
      await discardResponseBody(res);
      throw new Error(`Redis proxy runtime load failed with ${res.status}`);
    }
    const contentType = res.headers.get("content-type") || "";
    if (!runtimeLoadContentTypeMatches(contentType)) {
      await discardResponseBody(res);
      throw new Error(`Redis proxy runtime load returned unsupported content-type ${contentType || "<missing>"}`);
    }
    return decodeRuntimeLoadPayload(await res.arrayBuffer());
  } catch (err) {
    if (controller.signal.aborted) {
      throw new Error(`Redis proxy runtime load timed out after ${REDIS_PROXY_LOAD_TIMEOUT_MS}ms`, {
        cause: err,
      });
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

// Factory for the workerLoader cold-load callback. `metrics` and `log` are
// optional — the service-binding path leaves them null so nested cold-load
// noise doesn't outlive the caller's own request_complete.
/**
 * @param {{
 *   requestId: string | null,
 *   env: RuntimeLoaderEnv,
 *   ctx: unknown,
 *   ns: string,
 *   worker: string,
 *   version: string,
 *   workerId: string,
 *   metrics?: RuntimeLoaderMetrics | null,
 *   log?: ((level: string, event: string, fields?: Record<string, unknown>) => void) | null,
 * }} options
 */
export function createLoaderCallback({ requestId, env, ctx, ns, worker, version, workerId, metrics = null, log = null }) {
  /** @param {(metrics: RuntimeLoaderMetrics) => void} fn */
  const maybeMetric = (fn) => { if (metrics) fn(metrics); };
  /** @param {string} level @param {string} event @param {Record<string, unknown>} fields */
  const maybeLog = (level, event, fields) => { if (log) log(level, event, fields); };
  const serviceName = typeof env.SERVICE_NAME === "string" && env.SERVICE_NAME
    ? env.SERVICE_NAME
    : "runtime";

  return async () => {
    const parsedIdentity = parseRuntimeLoadWorkerId(workerId);
    if (
      !parsedIdentity ||
      parsedIdentity.namespace !== ns ||
      parsedIdentity.worker !== worker ||
      parsedIdentity.version !== version
    ) {
      throw new Error(`Invalid runtime load worker identity ${workerId}`);
    }
    const loadStartedAt = Date.now();
    maybeMetric((m) => m.increment("loader_misses", { service: serviceName }));
    maybeLog("info", "loader_miss", { request_id: requestId, worker_id: workerId });

    const redisStartedAt = Date.now();
    let loaded;
    let lastErr;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        loaded = await loadViaProxy(env, ns, worker, version);
        break;
      } catch (err) {
        lastErr = err;
        if (attempt === 3) break;
        maybeLog("warn", "bundle_load_proxy_retry", {
          request_id: requestId, worker_id: workerId, attempt,
          ...formatError(err),
        });
        await new Promise((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
    }
    maybeMetric((m) => m.observe("bundle_load_stage_duration_ms", {
      service: serviceName, stage: "redis_proxy_load",
    }, Date.now() - redisStartedAt));
    if (!loaded) throw lastErr;

    const hash = loaded.bundle;
    const nsSecrets = loaded.ns_secrets || {};
    const workerSecrets = loaded.worker_secrets || {};
    if (!hash.__meta__) {
      throw new Error(`Bundle for ${workerId} not found in Redis`);
    }

    const decodeStartedAt = Date.now();
    const { meta, ...codeBase } = bundleToWorkerCode(hash);
    const metaPlan = analyzeRuntimeMeta(meta);
    maybeMetric((m) => m.observe("bundle_load_stage_duration_ms", {
      service: serviceName, stage: "decode_bundle",
    }, Date.now() - decodeStartedAt));

    const envStartedAt = Date.now();
    const runtimeCtx = /** @type {RuntimeContext} */ (ctx);
    const doBackend = metaPlan.needsDoBackend ? internalAuthBackend(runtimeCtx, env, "DO_BACKEND") : null;
    const doOwnerNetwork = metaPlan.needsDoBackend
      ? internalAuthBackend(runtimeCtx, env, "DO_OWNER_NETWORK")
      : null;
    if (metaPlan.needsDoBackend && typeof runtimeCtx.exports.DurableObjectNamespace !== "function") {
      throw new Error("DurableObjectNamespace runtime binding adapter is not configured");
    }
    const workflowsBackend = metaPlan.needsWorkflowsBackend
      ? internalAuthBackend(runtimeCtx, env, "WORKFLOWS_BACKEND")
      : null;
    const workerEnv = buildWorkerEnv(
      meta, nsSecrets, workerSecrets, ns, worker, version,
      env.ASSETS_CDN_BASE, runtimeCtx, doBackend,
      {
        doOwnerNetwork,
        workflowsBackend,
        bindingEntries: metaPlan.bindingEntries,
        workflows: metaPlan.workflows,
        doBindingFactory({ name, spec, ns: bindingNs, worker: bindingWorker, version: bindingVersion }) {
          const hostProxy = runtimeCtx.exports.DurableObjectNamespace({
            props: {
              ns: bindingNs,
              worker: bindingWorker,
              version: bindingVersion,
              doStorageId: spec.doStorageId,
              binding: name,
              className: spec.className,
            },
          });
          return {
            ns: bindingNs,
            worker: bindingWorker,
            version: bindingVersion,
            doStorageId: spec.doStorageId,
            binding: name,
            className: spec.className,
            hostProxy,
          };
        },
      }
    );
    maybeMetric((m) => m.observe("bundle_load_stage_duration_ms", {
      service: serviceName, stage: "build_env",
    }, Date.now() - envStartedAt));

    // Loaded workers must not inherit runtime's private-reaching
    // outbound; pin them to PUBLIC_NETWORK.
    const workerCode = {
      ...codeBase,
      env: workerEnv,
      globalOutbound: env.PUBLIC_NETWORK,
      ...(env.TAIL_WORKER ? { tails: [env.TAIL_WORKER] } : {}),
    };
    wrapWorkerCodeForHostBindings(workerCode, meta, metaPlan);
    maybeMetric((m) => m.observe("bundle_load_duration_ms", { service: serviceName },
      Date.now() - loadStartedAt));
    return workerCode;
  };
}

/**
 * @template TStub
 * @param {{
 *   requestId: string | null,
 *   env: RuntimeLoaderEnv & { LOADER: { get(id: string, factory: () => Promise<WorkerCodeShape>): TStub } },
 *   ctx: { waitUntil(promise: Promise<unknown>): void },
 *   ns: string,
 *   worker: string,
 *   version: string,
 *   workerId?: string,
 *   metrics?: RuntimeLoaderMetrics | null,
 *   log?: ((level: string, event: string, fields?: Record<string, unknown>) => void) | null,
 *   evictOnLoad?: boolean,
 * }} options
 * @returns {{ workerId: string, stub: TStub }}
 */
export function getLoadedWorkerStub({
  requestId,
  env,
  ctx,
  ns,
  worker,
  version,
  workerId = formatWorkerId({ namespace: ns, worker, version }),
  metrics = null,
  log = null,
  evictOnLoad = false,
}) {
  const baseCallback = createLoaderCallback({
    requestId,
    env,
    ctx,
    ns,
    worker,
    version,
    workerId,
    metrics,
    log,
  });
  const stub = env.LOADER.get(workerId, async () => {
    const code = await baseCallback();
    recordLoadedWorker(workerId);
    if (evictOnLoad) {
      ctx.waitUntil(
        evictSiblings({ env, workerId, log })
          .catch(() => {})
      );
    }
    return code;
  });
  return { workerId, stub };
}
