import { decryptSecretValue } from "shared-secret-envelope";
import { errorMessage } from "shared-errors";
import { bundleKey } from "shared-version";
import { WatchError } from "shared-redis";

const DO_BACKEND_BINDING = "__WDL_DO_BACKEND__";
const DO_OWNER_NETWORK_BINDING = "__WDL_DO_OWNER_NETWORK__";
const DO_ALARMS_BINDING = "__WDL_DO_ALARMS__";
const WORKFLOWS_BACKEND_BINDING = "__WDL_WORKFLOWS_BACKEND__";
const ESTIMATED_ASSETS_CDN_BASE = "https://assets.invalid";
// Pessimistic placeholder for version strings that will be allocated after a
// secret mutation. Redis INCR results are parsed as JS numbers today, so this
// uses the longest safe integer-shaped `v<int>` tag.
export const WORKER_LOADER_ENV_VERSION_PLACEHOLDER = "v9007199254740991";
const ESTIMATED_DO_STORAGE_ID = "do_00000000000000000000000000000000";
const ESTIMATED_WORKFLOW_KEY = "wf_00000000000000000000000000000000";

// Mirrors workerd v1.20260701.1
// src/workerd/api/worker-loader.c++ MAX_DYNAMIC_WORKER_ENV_SIZE.
export const UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES = 1024 * 1024;
// Absorbs WDL's JSON-vs-V8 estimator noise and small platform-injected fields
// while keeping Control fail-closed before workerd's authoritative limit.
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
  const callerSecrets = Object.create(null);
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
      // DO bindings intentionally mirror runtime's default/factory output shape:
      // props are top-level fields, while hostProxy carries the JSRPC namespace.
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
  version = WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
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

/** @param {string} value */
function hasNonLatin1(value) {
  for (let i = 0; i < value.length; i += 1) {
    if (value.charCodeAt(i) > 0xff) return true;
  }
  return false;
}

/** @param {string} value */
function v8TwoByteStringPenalty(value) {
  if (!hasNonLatin1(value)) return 0;
  return Math.max(0, (2 * value.length) - Buffer.byteLength(value, "utf8"));
}

/** @param {unknown} value */
function v8StringPenalty(value) {
  if (typeof value === "string") return v8TwoByteStringPenalty(value);
  if (!value || typeof value !== "object") return 0;
  let bytes = 0;
  for (const [key, child] of Object.entries(value)) {
    bytes += v8TwoByteStringPenalty(key);
    bytes += v8StringPenalty(child);
  }
  return bytes;
}

/** @param {unknown} value */
export function estimatedWorkerLoaderEnvBytes(value) {
  const json = JSON.stringify(value) ?? "null";
  return Buffer.byteLength(json, "utf8") + v8StringPenalty(value);
}

/**
 * @param {{
 *   ns: string,
 *   worker?: string,
 *   version?: string,
 *   sourceVersion?: string | null,
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
  version = WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
  sourceVersion = null,
  vars = null,
  nsSecrets = null,
  workerSecrets = null,
  meta = null,
  assetsCdnBase = ESTIMATED_ASSETS_CDN_BASE,
}) {
  // workerd enforces the full workerLoader env as a Frankenvalue estimate. Control
  // mirrors the user strings plus runtime-injected binding/workflow env shapes as
  // JSON, then accounts for V8's two-byte representation of non-Latin-1 strings.
  const bytes = estimatedWorkerLoaderEnvBytes(estimatedWorkerLoaderEnv({
    ns,
    worker,
    version,
    vars,
    nsSecrets,
    workerSecrets,
    meta,
    assetsCdnBase,
  }));
  if (bytes > WORKER_LOADER_ENV_MAX_BYTES) {
    const labelVersion = sourceVersion ||
      (version !== WORKER_LOADER_ENV_VERSION_PLACEHOLDER ? version : "");
    const label = worker
      ? `${ns}/${worker}${labelVersion ? `@${labelVersion}` : ""}`
      : ns;
    throw new WorkerEnvBudgetError(
      `estimated workerLoader env for ${label} serializes to ${bytes} bytes, ` +
        `exceeding WDL workerLoader env budget ${WORKER_LOADER_ENV_MAX_BYTES} bytes`,
      {
        namespace: ns,
        ...(worker ? { worker } : {}),
        ...(sourceVersion ? { source_version: sourceVersion } : {}),
        ...(sourceVersion && version ? { estimated_version: version } : {}),
        ...(!sourceVersion && version !== WORKER_LOADER_ENV_VERSION_PLACEHOLDER ? { version } : {}),
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
 * }} args
 */
export async function decryptSecretHash({ encrypted, env, hashKey }) {
  const entries = await Promise.all(
    Object.entries(encrypted || {})
      .filter((entry) => typeof entry[1] === "string")
      .map(async ([fieldName, value]) => [
        fieldName,
        await decryptSecretValue(/** @type {string} */ (value), { env, hashKey, fieldName }),
      ])
  );
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (const entry of entries) {
    const [fieldName, value] = entry;
    out[fieldName] = value;
  }
  return out;
}

/**
 * Budget checks model the post-mutation plaintext hash. DELETE excludes the
 * target before decrypting so corrupt target envelopes remain repairable;
 * other corrupt envelopes still fail closed.
 * @param {{
 *   encrypted: Record<string, string | null | undefined>,
 *   env: Record<string, string | undefined>,
 *   hashKey: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   plaintext?: string | null,
 * }} args
 */
export async function decryptMutatedSecretHashForBudget({
  encrypted,
  env,
  hashKey,
  key,
  method,
  plaintext = null,
}) {
  const budgetEncrypted = { ...encrypted };
  delete budgetEncrypted[key];
  const secrets = await decryptSecretHash({
    encrypted: budgetEncrypted,
    env,
    hashKey,
  });
  if (method === "PUT") {
    if (typeof plaintext !== "string") throw new Error("PUT secret plaintext missing");
    secrets[key] = plaintext;
  }
  return secrets;
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
 *   retryMissingVersions?: boolean,
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
  retryMissingVersions = false,
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
    if (typeof rawMeta !== "string") {
      if (retryMissingVersions) throw new WatchError();
      throw new Error(`bundle metadata missing for ${ns}/${worker}@${sourceVersion}`);
    }
    /** @type {unknown} */
    let parsed;
    try {
      parsed = JSON.parse(rawMeta);
    } catch (err) {
      throw new Error(
        `invalid bundle metadata for ${ns}/${worker}@${sourceVersion}: ${errorMessage(err)}`,
        { cause: err }
      );
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(
        `invalid bundle metadata for ${ns}/${worker}@${sourceVersion}: ` +
          "__meta__ must be a JSON object"
      );
    }
    const meta = /** @type {Record<string, unknown>} */ (parsed);
    assertWorkerLoaderUserEnvBudget({
      ns,
      worker,
      version: estimatedVersion,
      sourceVersion,
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
