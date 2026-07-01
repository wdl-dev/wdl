import { decryptSecretValue } from "shared-secret-envelope";

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
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (const [fieldName, value] of Object.entries(encrypted || {})) {
    if (typeof value !== "string") continue;
    out[fieldName] = await decryptSecretValue(value, { env, hashKey, fieldName });
  }
  return out;
}
