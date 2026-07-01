import { SecretEnvelopeError, decryptSecretValue } from "shared-secret-envelope";
import { bundleKey } from "shared-version";

const DO_BACKEND_BINDING = "__WDL_DO_BACKEND__";
const DO_OWNER_NETWORK_BINDING = "__WDL_DO_OWNER_NETWORK__";
const DO_ALARMS_BINDING = "__WDL_DO_ALARMS__";
const WORKFLOWS_BACKEND_BINDING = "__WDL_WORKFLOWS_BACKEND__";
const ESTIMATED_ASSETS_CDN_BASE = "https://assets.invalid";
const ESTIMATED_VERSION = "v0000000000";
const ESTIMATED_DO_STORAGE_ID = "do_00000000000000000000000000000000";
const ESTIMATED_WORKFLOW_KEY = "wf_00000000000000000000000000000000";

export const WORKER_LOADER_ENV_VERSION_PLACEHOLDER = ESTIMATED_VERSION;
export const UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES = 1024 * 1024;
export const WORKER_LOADER_ENV_HEADROOM_BYTES = 8 * 1024;
export const WORKER_LOADER_ENV_MAX_BYTES =
  UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES - WORKER_LOADER_ENV_HEADROOM_BYTES;

export class WorkerEnvBudgetError extends Error {
  /**
   * @param {string} message
   * @param {Record<string, unknown>} [details]
   */
  constructor(message, details = {}) {
    super(message);
    this.status = 400;
    this.code = "worker_env_too_large";
    this.details = details;
  }
}

/** @param {Record<string, unknown> | null | undefined} source */
function stringRecord(source) {
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (const [key, value] of Object.entries(source || {})) {
    if (typeof value === "string") out[key] = value;
  }
  return out;
}

/** @param {unknown} value @returns {Record<string, unknown> | null} */
function objectRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? /** @type {Record<string, unknown>} */ (value)
    : null;
}

/** @param {unknown} value */
function stringOrFallback(value, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

/**
 * @param {{
 *   requiredCallerSecrets?: unknown,
 *   nsSecrets: Record<string, string>,
 *   workerSecrets: Record<string, string>,
 * }} args
 */
function callerSecretsForBinding({ requiredCallerSecrets, nsSecrets, workerSecrets }) {
  if (!Array.isArray(requiredCallerSecrets) || requiredCallerSecrets.length === 0) return undefined;
  /** @type {Record<string, string>} */
  const callerSecrets = {};
  for (const key of requiredCallerSecrets) {
    if (typeof key !== "string") continue;
    if (Object.hasOwn(workerSecrets, key)) {
      callerSecrets[key] = workerSecrets[key];
    } else if (Object.hasOwn(nsSecrets, key)) {
      callerSecrets[key] = nsSecrets[key];
    }
  }
  return callerSecrets;
}

/**
 * @param {{
 *   name: string,
 *   spec: Record<string, unknown>,
 *   meta: Record<string, unknown>,
 *   ns: string,
 *   worker: string,
 *   version: string,
 *   assetsCdnBase: string,
 *   nsSecrets: Record<string, string>,
 *   workerSecrets: Record<string, string>,
 * }} args
 */
function estimatedBindingEnvValue({ name, spec, meta, ns, worker, version, assetsCdnBase, nsSecrets, workerSecrets }) {
  switch (spec.type) {
    case "kv":
      return {
        __wdlBinding: "kv",
        props: { ns, id: stringOrFallback(spec.id) },
      };
    case "assets":
      return {
        __wdlBinding: "assets",
        props: {
          cdnBase: assetsCdnBase,
          prefix: stringOrFallback(objectRecord(meta.assets)?.prefix),
        },
      };
    case "queue":
      return {
        __wdlBinding: "queue",
        props: {
          ns,
          id: stringOrFallback(spec.id),
          deliveryDelaySeconds: spec.deliveryDelaySeconds ?? 0,
        },
      };
    case "d1":
      return {
        __wdlBinding: "d1",
        props: {
          ns,
          databaseId: stringOrFallback(spec.databaseId),
          binding: name,
        },
      };
    case "r2":
      return {
        __wdlBinding: "r2",
        props: {
          ns,
          bucketName: stringOrFallback(spec.bucketName),
          binding: name,
        },
      };
    case "do": {
      const props = {
        ns,
        worker,
        version,
        doStorageId: stringOrFallback(spec.doStorageId, ESTIMATED_DO_STORAGE_ID),
        binding: name,
        className: stringOrFallback(spec.className),
      };
      return {
        __wdlBinding: "do",
        ...props,
        hostProxy: { __wdlBinding: "do-host-proxy", props },
      };
    }
    case "service": {
      const callerSecrets = callerSecretsForBinding({
        requiredCallerSecrets: spec.requiredCallerSecrets,
        nsSecrets,
        workerSecrets,
      });
      return {
        __wdlBinding: "service",
        props: {
          targetNs: stringOrFallback(spec.ns, ns),
          targetWorker: stringOrFallback(spec.service),
          targetVersion: stringOrFallback(spec.version),
          targetEntrypoint: typeof spec.entrypoint === "string" ? spec.entrypoint : null,
          callerNs: ns,
          ...(callerSecrets ? { callerSecrets } : {}),
        },
      };
    }
    default:
      return { __wdlBinding: stringOrFallback(spec.type, "unknown") };
  }
}

/**
 * @param {{
 *   ns: string,
 *   worker?: string,
 *   version?: string,
 *   vars?: Record<string, unknown> | null,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 *   meta?: Record<string, unknown> | null,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export function estimatedWorkerLoaderEnv({
  ns,
  worker = "",
  version = ESTIMATED_VERSION,
  vars = null,
  nsSecrets = null,
  workerSecrets = null,
  meta = null,
  assetsCdnBase = ESTIMATED_ASSETS_CDN_BASE,
}) {
  const nsSecretStrings = stringRecord(nsSecrets);
  const workerSecretStrings = stringRecord(workerSecrets);
  /** @type {Record<string, unknown>} */
  const env = {
    ...stringRecord(vars),
    ...nsSecretStrings,
    ...workerSecretStrings,
  };
  const metaRecord = objectRecord(meta);
  if (!metaRecord || !worker) return env;

  let hasDoBinding = false;
  let doAlarmStorageId = ESTIMATED_DO_STORAGE_ID;
  let hasWorkflowBinding = false;
  const workflows = Array.isArray(metaRecord.workflows) ? metaRecord.workflows : [];
  for (const workflow of workflows) {
    const record = objectRecord(workflow);
    if (!record) continue;
    const binding = stringOrFallback(record.binding);
    if (!binding) continue;
    hasWorkflowBinding = true;
    env[binding] = {
      ns,
      worker,
      version,
      name: stringOrFallback(record.name),
      binding,
      className: stringOrFallback(record.className),
      workflowKey: stringOrFallback(record.workflowKey, ESTIMATED_WORKFLOW_KEY),
    };
  }

  const bindings = objectRecord(metaRecord.bindings);
  if (bindings) {
    for (const [name, rawSpec] of Object.entries(bindings)) {
      const spec = objectRecord(rawSpec);
      if (!spec) continue;
      env[name] = estimatedBindingEnvValue({
        name,
        spec,
        meta: metaRecord,
        ns,
        worker,
        version,
        assetsCdnBase: typeof assetsCdnBase === "string" && assetsCdnBase
          ? assetsCdnBase
          : ESTIMATED_ASSETS_CDN_BASE,
        nsSecrets: nsSecretStrings,
        workerSecrets: workerSecretStrings,
      });
      if (spec.type === "do") {
        hasDoBinding = true;
        doAlarmStorageId = stringOrFallback(spec.doStorageId, ESTIMATED_DO_STORAGE_ID);
      }
    }
  }
  if (hasDoBinding) {
    env[DO_BACKEND_BINDING] = { __wdlBinding: "internal", name: "DO_BACKEND" };
    env[DO_OWNER_NETWORK_BINDING] = { __wdlBinding: "internal", name: "DO_OWNER_NETWORK" };
    env[DO_ALARMS_BINDING] = {
      __wdlBinding: "do-alarms",
      props: { ns, worker, version, doStorageId: doAlarmStorageId },
    };
  }
  if (hasWorkflowBinding) {
    env[WORKFLOWS_BACKEND_BINDING] = { __wdlBinding: "internal", name: "WORKFLOWS_BACKEND" };
  }
  return env;
}

/**
 * @param {{
 *   ns: string,
 *   worker?: string,
 *   version?: string,
 *   vars?: Record<string, unknown> | null,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 *   meta?: Record<string, unknown> | null,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export function assertWorkerLoaderUserEnvBudget({
  ns,
  worker = undefined,
  version = ESTIMATED_VERSION,
  vars = null,
  nsSecrets = null,
  workerSecrets = null,
  meta = null,
  assetsCdnBase = ESTIMATED_ASSETS_CDN_BASE,
}) {
  // workerd enforces the full workerLoader env as a Frankenvalue estimate. Control
  // mirrors the user strings plus runtime-injected binding/workflow env shapes as
  // JSON and leaves headroom for native facade-object overhead and estimator drift.
  const bytes = Buffer.byteLength(JSON.stringify(estimatedWorkerLoaderEnv({
    ns,
    worker,
    version,
    vars,
    nsSecrets,
    workerSecrets,
    meta,
    assetsCdnBase,
  })), "utf8");
  if (bytes > WORKER_LOADER_ENV_MAX_BYTES) {
    const label = worker ? `${ns}/${worker}` : ns;
    throw new WorkerEnvBudgetError(
      `estimated workerLoader env for ${label} serializes to ${bytes} bytes, exceeding WDL workerLoader env budget ${WORKER_LOADER_ENV_MAX_BYTES} bytes`,
      {
        namespace: ns,
        ...(worker ? { worker } : {}),
        env_bytes: bytes,
        max_env_bytes: WORKER_LOADER_ENV_MAX_BYTES,
        upstream_max_env_bytes: UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES,
        headroom_bytes: WORKER_LOADER_ENV_HEADROOM_BYTES,
      }
    );
  }
  return bytes;
}

/**
 * @param {{
 *   encrypted: Record<string, string | null | undefined>,
 *   env: Record<string, string | undefined>,
 *   hashKey: string,
 *   ignoreSecretEnvelopeErrors?: boolean,
 * }} args
 */
export async function decryptSecretHash({ encrypted, env, hashKey, ignoreSecretEnvelopeErrors = false }) {
  const entries = await Promise.all(
    Object.entries(encrypted || {})
      .filter((entry) => typeof entry[1] === "string")
      .map(async ([fieldName, value]) => {
        try {
          return [
            fieldName,
            await decryptSecretValue(/** @type {string} */ (value), { env, hashKey, fieldName }),
          ];
        } catch (err) {
          if (ignoreSecretEnvelopeErrors && err instanceof SecretEnvelopeError) return null;
          throw err;
        }
      })
  );
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (const entry of entries) {
    if (!entry) continue;
    const [fieldName, value] = entry;
    out[fieldName] = value;
  }
  return out;
}

/**
 * @param {{
 *   redis: { hGet(key: string, field: string): Promise<string | null | undefined> },
 *   ns: string,
 *   worker: string,
 *   versions: Iterable<string>,
 *   versionEstimates?: Iterable<{ sourceVersion: string, estimatedVersion: string }>,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export async function assertWorkerVersionsUserEnvBudget({
  redis,
  ns,
  worker,
  versions,
  versionEstimates = [],
  nsSecrets = null,
  workerSecrets = null,
  assetsCdnBase = ESTIMATED_ASSETS_CDN_BASE,
}) {
  const checks = [
    ...[...versions]
      .filter((version) => typeof version === "string" && version)
      .map((version) => ({ sourceVersion: version, estimatedVersion: version })),
    ...[...versionEstimates]
      .filter((entry) =>
        entry &&
        typeof entry.sourceVersion === "string" &&
        entry.sourceVersion &&
        typeof entry.estimatedVersion === "string" &&
        entry.estimatedVersion
      ),
  ];
  const uniqueChecks = [...new Map(
    checks.map((entry) => [`${entry.sourceVersion}\0${entry.estimatedVersion}`, entry])
  ).values()];
  if (uniqueChecks.length === 0) {
    assertWorkerLoaderUserEnvBudget({ ns, worker, nsSecrets, workerSecrets, assetsCdnBase });
    return;
  }

  // Keep bundle metadata reads sequential: callers may pass a RedisSession,
  // whose command protocol is single-flight even though secret decryption is not.
  for (const { sourceVersion, estimatedVersion } of uniqueChecks) {
    const rawMeta = await redis.hGet(bundleKey(ns, worker, sourceVersion), "__meta__");
    /** @type {Record<string, unknown>} */
    let meta = {};
    if (typeof rawMeta === "string") {
      try {
        const parsed = JSON.parse(rawMeta);
        meta = parsed && typeof parsed === "object" && !Array.isArray(parsed)
          ? /** @type {Record<string, unknown>} */ (parsed)
          : {};
      } catch (err) {
        throw new Error(
          `invalid bundle metadata for ${ns}/${worker}@${sourceVersion}: ${err instanceof Error ? err.message : String(err)}`,
          { cause: err }
        );
      }
    }
    assertWorkerLoaderUserEnvBudget({
      ns,
      worker,
      version: estimatedVersion,
      vars: meta.vars && typeof meta.vars === "object" && !Array.isArray(meta.vars)
        ? /** @type {Record<string, unknown>} */ (meta.vars)
        : null,
      nsSecrets,
      workerSecrets,
      meta,
      assetsCdnBase,
    });
  }
}
