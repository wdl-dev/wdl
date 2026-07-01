import { decryptSecretValue } from "shared-secret-envelope";
import { bundleKey } from "shared-version";

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

/**
 * @param {{
 *   vars?: Record<string, unknown> | null,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 * }} args
 */
export function mergedUserEnvStrings({ vars = null, nsSecrets = null, workerSecrets = null }) {
  return {
    ...stringRecord(vars),
    ...stringRecord(nsSecrets),
    ...stringRecord(workerSecrets),
  };
}

/** @param {Record<string, string>} envStrings */
export function userEnvSerializedBytes(envStrings) {
  return Buffer.byteLength(JSON.stringify(envStrings), "utf8");
}

/**
 * @param {{
 *   ns: string,
 *   worker?: string,
 *   vars?: Record<string, unknown> | null,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 * }} args
 */
export function assertWorkerLoaderUserEnvBudget({
  ns,
  worker = undefined,
  vars = null,
  nsSecrets = null,
  workerSecrets = null,
}) {
  const merged = mergedUserEnvStrings({ vars, nsSecrets, workerSecrets });
  // workerd enforces the full workerLoader env as a Frankenvalue estimate. Control
  // checks the user-controlled string env as JSON and leaves headroom for runtime
  // facade objects and estimator drift.
  const bytes = userEnvSerializedBytes(merged);
  if (bytes > WORKER_LOADER_ENV_MAX_BYTES) {
    const label = worker ? `${ns}/${worker}` : ns;
    throw new WorkerEnvBudgetError(
      `vars and secrets for ${label} serialize to ${bytes} bytes, exceeding WDL workerLoader env budget ${WORKER_LOADER_ENV_MAX_BYTES} bytes`,
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
  for (const [fieldName, value] of entries) out[fieldName] = value;
  return out;
}

/**
 * @param {{
 *   redis: { hGet(key: string, field: string): Promise<string | null | undefined> },
 *   ns: string,
 *   worker: string,
 *   versions: Iterable<string>,
 *   nsSecrets?: Record<string, unknown> | null,
 *   workerSecrets?: Record<string, unknown> | null,
 * }} args
 */
export async function assertWorkerVersionsUserEnvBudget({
  redis,
  ns,
  worker,
  versions,
  nsSecrets = null,
  workerSecrets = null,
}) {
  const uniqueVersions = [...new Set([...versions].filter((version) => typeof version === "string" && version))];
  if (uniqueVersions.length === 0) {
    assertWorkerLoaderUserEnvBudget({ ns, worker, nsSecrets, workerSecrets });
    return;
  }

  // Keep bundle metadata reads sequential: callers may pass a RedisSession,
  // whose command protocol is single-flight even though secret decryption is not.
  for (const version of uniqueVersions) {
    const rawMeta = await redis.hGet(bundleKey(ns, worker, version), "__meta__");
    const meta = typeof rawMeta === "string" ? JSON.parse(rawMeta) : {};
    assertWorkerLoaderUserEnvBudget({
      ns,
      worker,
      vars: meta && typeof meta === "object" ? meta.vars : null,
      nsSecrets,
      workerSecrets,
    });
  }
}
