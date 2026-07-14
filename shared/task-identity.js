import { envValueOr } from "shared-env";

const DEFAULT_IDENTITY_TIMEOUT_MS = 1000;

/**
 * @typedef {{ IPv4Addresses?: unknown, IPv4Address?: unknown }} EcsNetwork
 * @typedef {{ Name?: unknown, Networks?: unknown }} EcsContainer
 * @typedef {{ TaskARN?: unknown, Networks?: unknown, Containers?: unknown }} EcsTaskMetadata
 * @typedef {{ taskId: string, endpoint: string, source: "env" | "ecs-metadata" }} TaskIdentity
 * @typedef {(status: number, code: string, message: string) => Error} ErrorFactory
 * @typedef {{
 *   envPrefix: string,
 *   defaultPort: number,
 *   defaultContainerName: string,
 *   serviceLabel: string,
 *   unavailableCode: string,
 *   createError: ErrorFactory,
 *   validateEndpoint?: (endpoint: string, port: number) => boolean,
 * }} TaskIdentityResolverOptions
 * @typedef {Record<string, unknown>} EnvLike
 */

/**
 * @param {unknown} value
 * @returns {string | null}
 */
function nonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/**
 * @param {unknown} networks
 * @returns {string | null}
 */
function firstIpv4FromNetworks(networks) {
  if (!Array.isArray(networks)) return null;
  for (const network of networks) {
    const record = /** @type {EcsNetwork | null | undefined} */ (network);
    const addresses = record?.IPv4Addresses;
    if (Array.isArray(addresses)) {
      const found = addresses.find((addr) => typeof addr === "string" && addr);
      if (found) return found;
    }
    if (typeof record?.IPv4Address === "string" && record.IPv4Address) return record.IPv4Address;
  }
  return null;
}

/**
 * @param {TaskIdentityResolverOptions} options
 */
export function createTaskIdentityResolver({
  envPrefix,
  defaultPort,
  defaultContainerName,
  serviceLabel,
  unavailableCode,
  createError,
  validateEndpoint = undefined,
}) {
  /** @type {TaskIdentity | null} */
  let cachedIdentity = null;
  /** @type {Promise<TaskIdentity> | null} */
  let identityPromise = null;

  /**
   * @param {string} message
   * @returns {Error}
   */
  function error(message) {
    return createError(503, unavailableCode, message);
  }

  /** @param {TaskIdentity} identity */
  function requireValidEndpoint(identity) {
    if (validateEndpoint?.(identity.endpoint, defaultPort) === false) {
      throw error(`${serviceLabel} task endpoint is invalid`);
    }
    return identity;
  }

  /**
   * @param {EnvLike} env
   * @returns {number}
   */
  function identityTimeoutMs(env) {
    const raw = Number(envValueOr(env[`${envPrefix}_TASK_IDENTITY_TIMEOUT_MS`], DEFAULT_IDENTITY_TIMEOUT_MS));
    return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_IDENTITY_TIMEOUT_MS;
  }

  /**
   * @param {EnvLike} env
   * @returns {string}
   */
  function containerName(env) {
    return nonEmptyString(env[`${envPrefix}_TASK_CONTAINER_NAME`]) || defaultContainerName;
  }

  /**
   * @param {unknown} taskMetadata
   * @param {EnvLike} [env]
   * @returns {string | null}
   */
  function firstPrivateIpv4(taskMetadata, env = {}) {
    const metadata = /** @type {EcsTaskMetadata | null | undefined} */ (taskMetadata);
    const topLevel = firstIpv4FromNetworks(metadata?.Networks);
    if (topLevel) return topLevel;
    const preferredName = containerName(env);
    const containers = Array.isArray(metadata?.Containers) ? metadata.Containers : [];
    for (const container of containers) {
      const record = /** @type {EcsContainer | null | undefined} */ (container);
      if (record?.Name === preferredName) {
        const found = firstIpv4FromNetworks(record?.Networks);
        if (found) return found;
      }
    }
    for (const container of containers) {
      const record = /** @type {EcsContainer | null | undefined} */ (container);
      const found = firstIpv4FromNetworks(record?.Networks);
      if (found) return found;
    }
    return null;
  }

  /**
   * @returns {void}
   */
  function resetTaskIdentityForTests() {
    cachedIdentity = null;
    identityPromise = null;
  }

  /**
   * @param {EnvLike} env
   * @returns {TaskIdentity | null}
   */
  function taskIdentityFromEnv(env) {
    const taskId = nonEmptyString(env[`${envPrefix}_TASK_ID`]);
    const endpoint = nonEmptyString(env[`${envPrefix}_TASK_ENDPOINT`]);
    if (!taskId && !endpoint) return null;
    if (!taskId || !endpoint) {
      throw error(`${envPrefix}_TASK_ID and ${envPrefix}_TASK_ENDPOINT must be configured together`);
    }
    return requireValidEndpoint({ taskId, endpoint, source: "env" });
  }

  /**
   * @param {unknown} taskMetadata
   * @param {EnvLike} [env]
   * @returns {TaskIdentity}
   */
  function taskIdentityFromEcsMetadata(taskMetadata, env = {}) {
    const metadata = /** @type {EcsTaskMetadata | null | undefined} */ (taskMetadata);
    const taskId = nonEmptyString(metadata?.TaskARN);
    if (!taskId) throw error("ECS task metadata did not include TaskARN");
    const privateIpv4 = firstPrivateIpv4(metadata, env);
    if (!privateIpv4) throw error("ECS task metadata did not include a private IPv4 address");
    return requireValidEndpoint({
      taskId,
      endpoint: `${privateIpv4}:${defaultPort}`,
      source: "ecs-metadata",
    });
  }

  /**
   * @param {EnvLike} env
   * @param {(input: string, init?: { signal?: AbortSignal }) => Promise<Response>} fetchImpl
   * @returns {Promise<unknown>}
   */
  async function fetchEcsTaskMetadata(env, fetchImpl) {
    const base = nonEmptyString(env.ECS_CONTAINER_METADATA_URI_V4);
    if (!base) {
      throw error(
        `${serviceLabel} task identity unavailable: configure ` +
        `${envPrefix}_TASK_ID/${envPrefix}_TASK_ENDPOINT or ECS_CONTAINER_METADATA_URI_V4`
      );
    }
    const res = await fetchImpl(`${base.replace(/\/+$/, "")}/task`, {
      signal: AbortSignal.timeout(identityTimeoutMs(env)),
    });
    if (!res.ok) throw error(`ECS task metadata request failed with ${res.status}`);
    return await res.json();
  }

  /**
   * @param {EnvLike} env
   * @returns {TaskIdentity | null}
   */
  function peekTaskIdentity(env) {
    if (cachedIdentity) return cachedIdentity;
    try {
      return taskIdentityFromEnv(env);
    } catch {
      return null;
    }
  }

  /**
   * @param {EnvLike} env
   * @param {(input: string, init?: { signal?: AbortSignal }) => Promise<Response>} [fetchImpl]
   * @returns {Promise<TaskIdentity>}
   */
  async function resolveTaskIdentity(env, fetchImpl = fetch) {
    const fromEnv = taskIdentityFromEnv(env);
    if (fromEnv) {
      cachedIdentity = fromEnv;
      return fromEnv;
    }
    if (cachedIdentity) return cachedIdentity;
    if (identityPromise) return await identityPromise;

    identityPromise = (async () => {
      const taskMetadata = await fetchEcsTaskMetadata(env, fetchImpl);
      const identity = taskIdentityFromEcsMetadata(taskMetadata, env);
      cachedIdentity = identity;
      return identity;
    })();
    try {
      return await identityPromise;
    } finally {
      identityPromise = null;
    }
  }

  return {
    peekTaskIdentity,
    resetTaskIdentityForTests,
    resolveTaskIdentity,
    taskIdentityFromEcsMetadata,
    taskIdentityFromEnv,
  };
}
