import { decryptSecretValue } from "shared-secret-envelope";
import { BundleMetaError, parseBundleMeta } from "control-lib";
import { buildWorkerEnv, doAlarmBindingProps } from "runtime-load-env-build";
import { bundleKey } from "shared-worker-contract";
import { WatchError } from "shared-redis";

export { BundleMetaError };

const DO_ALARMS_BINDING = "__WDL_DO_ALARMS__";
const ESTIMATED_ASSETS_CDN_BASE = "https://assets.invalid";
const WORKER_ENV_META_READ_BATCH_SIZE = 32;
// Pessimistic placeholder for version strings that will be allocated after a
// secret mutation. Redis INCR results are parsed as JS numbers today, so this
// uses the longest safe integer-shaped `v<int>` tag.
export const WORKER_LOADER_ENV_VERSION_PLACEHOLDER = "v9007199254740991";
const ESTIMATED_DO_BACKEND = Object.freeze({ __wdlBinding: "internal", name: "DO_BACKEND" });
const ESTIMATED_DO_OWNER_NETWORK = Object.freeze({
  __wdlBinding: "internal",
  name: "DO_OWNER_NETWORK",
});
const ESTIMATED_WORKFLOWS_BACKEND = Object.freeze({
  __wdlBinding: "internal",
  name: "WORKFLOWS_BACKEND",
});
/** @type {Parameters<typeof buildWorkerEnv>[7]} */
const ESTIMATED_RUNTIME_CONTEXT = Object.freeze({
  exports: Object.freeze({
    KV: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "kv", props }),
    Assets: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "assets", props }),
    QueueProducer: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "queue", props }),
    D1Database: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "d1", props }),
    R2Bucket: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "r2", props }),
    ServiceBinding: (/** @type {{ props: Record<string, unknown> }} */ { props }) => ({ __wdlBinding: "service", props }),
  }),
});

// Mirrors workerd v1.20260718.1
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

/** @param {string} ns @param {string} worker @param {string} version @param {unknown} cause */
function bundleMaterializationError(ns, worker, version, cause) {
  return new BundleMetaError({
    namespace: ns,
    worker,
    version,
    message: `Corrupt __meta__ for ${ns}/${worker}/${version}`,
    cause,
  });
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

/** @param {{ name: string, spec: Record<string, unknown>, ns: string, worker: string, version: string }} input */
function estimatedDoBinding({ name, spec, ns, worker, version }) {
  const props = {
    ns,
    worker,
    version,
    doStorageId: spec.doStorageId,
    binding: name,
    className: spec.className,
  };
  return {
    __wdlBinding: "do",
    ...props,
    hostProxy: { __wdlBinding: "do-host-proxy", props },
  };
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

  const estimated = buildWorkerEnv(
    { ...metaRecord, vars: stringRecord(vars) },
    nsSecretStrings,
    workerSecretStrings,
    ns,
    worker,
    version,
    typeof assetsCdnBase === "string" && assetsCdnBase
      ? assetsCdnBase
      : ESTIMATED_ASSETS_CDN_BASE,
    ESTIMATED_RUNTIME_CONTEXT,
    ESTIMATED_DO_BACKEND,
    {
      doOwnerNetwork: ESTIMATED_DO_OWNER_NETWORK,
      doBindingFactory: estimatedDoBinding,
      workflowsBackend: ESTIMATED_WORKFLOWS_BACKEND,
    }
  );

  let doAlarmStorageId = null;
  for (const rawSpec of Object.values(objectRecord(metaRecord.bindings) || {})) {
    const spec = objectRecord(rawSpec);
    if (spec?.type === "do") doAlarmStorageId = spec.doStorageId;
  }
  if (typeof doAlarmStorageId === "string" && doAlarmStorageId) {
    estimated[DO_ALARMS_BINDING] = {
      __wdlBinding: "do-alarms",
      props: doAlarmBindingProps({ ns, worker, version, doStorageId: doAlarmStorageId }),
    };
  }
  return estimated;
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
 * @param {Iterable<string>} versions
 * @param {Iterable<{ sourceVersion: string, estimatedVersion: string }>} versionEstimates
 */
function versionBudgetChecks(versions, versionEstimates) {
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
  return [...new Map(
    checks.map((entry) => [`${entry.sourceVersion}\0${entry.estimatedVersion}`, entry])
  ).values()];
}

/**
 * @typedef {{
 *   worker: string,
 *   versions: Iterable<string>,
 *   versionEstimates?: Iterable<{ sourceVersion: string, estimatedVersion: string }>,
 *   workerSecrets?: Record<string, unknown> | null,
 * }} WorkerVersionBudgetInput
 */

/**
 * @param {{
 *   redis: { hGetMany(pairs: Array<[string, string]>): Promise<Array<string | null | undefined>> },
 *   ns: string,
 *   workers: Iterable<WorkerVersionBudgetInput>,
 *   nsSecrets?: Record<string, unknown> | null,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export async function assertWorkersVersionsUserEnvBudget({
  redis,
  ns,
  workers,
  nsSecrets = null,
  assetsCdnBase = ESTIMATED_ASSETS_CDN_BASE,
}) {
  /** @type {Array<WorkerVersionBudgetInput & { sourceVersion: string, estimatedVersion: string }>} */
  const checks = [];
  /** @type {Map<string, [string, string]>} */
  const metadataReads = new Map();

  for (const input of workers) {
    const workerChecks = versionBudgetChecks(input.versions, input.versionEstimates || []);
    if (workerChecks.length === 0) {
      assertWorkerLoaderUserEnvBudget({
        ns,
        worker: input.worker,
        nsSecrets,
        workerSecrets: input.workerSecrets,
        assetsCdnBase,
      });
      continue;
    }
    for (const check of workerChecks) {
      checks.push({ ...input, ...check });
      const identity = `${input.worker}\0${check.sourceVersion}`;
      if (!metadataReads.has(identity)) {
        metadataReads.set(identity, [bundleKey(ns, input.worker, check.sourceVersion), "__meta__"]);
      }
    }
  }

  /** @type {Map<string, string | null | undefined>} */
  const rawMetadata = new Map();
  const reads = [...metadataReads];
  for (let offset = 0; offset < reads.length; offset += WORKER_ENV_META_READ_BATCH_SIZE) {
    const batch = reads.slice(offset, offset + WORKER_ENV_META_READ_BATCH_SIZE);
    const replies = await redis.hGetMany(batch.map(([, pair]) => pair));
    for (let index = 0; index < batch.length; index += 1) {
      rawMetadata.set(batch[index][0], replies[index]);
    }
  }

  /** @type {Map<string, ReturnType<typeof parseBundleMeta>>} */
  const parsedMetadata = new Map();
  for (const {
    worker,
    workerSecrets = null,
    sourceVersion,
    estimatedVersion,
  } of checks) {
    const identity = `${worker}\0${sourceVersion}`;
    const rawMeta = rawMetadata.get(identity);
    if (typeof rawMeta !== "string") throw new WatchError();
    let meta = parsedMetadata.get(identity);
    if (!meta) {
      meta = parseBundleMeta(rawMeta, {
        ns,
        worker,
        version: sourceVersion,
      });
      parsedMetadata.set(identity, meta);
    }
    try {
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
    } catch (err) {
      if (err instanceof WorkerEnvBudgetError || err instanceof BundleMetaError) throw err;
      throw bundleMaterializationError(ns, worker, sourceVersion, err);
    }
  }
}

/**
 * @param {{
 *   redis: { hGetMany(pairs: Array<[string, string]>): Promise<Array<string | null | undefined>> },
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
  await assertWorkersVersionsUserEnvBudget({
    redis,
    ns,
    workers: [{ worker, versions, versionEstimates, workerSecrets }],
    nsSecrets,
    assetsCdnBase,
  });
}
