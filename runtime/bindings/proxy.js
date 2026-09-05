import { withInternalAuth } from "shared-internal-auth";
import { discardResponseBody } from "shared-respond";

/**
 * @typedef {{ SERVICE_NAME?: unknown, REDIS_PROXY_URL?: unknown, WDL_INTERNAL_AUTH_TOKEN?: unknown }} RuntimeProxyEnv
 */

/** @param {RuntimeProxyEnv | null | undefined} env */
export function serviceNameFromEnv(env) {
  return typeof env?.SERVICE_NAME === "string" && env.SERVICE_NAME
    ? env.SERVICE_NAME
    : "runtime";
}

/**
 * @param {RuntimeProxyEnv | null | undefined} env
 * @returns {string | null}
 */
export function optionalRedisProxyBaseUrl(env) {
  if (!env?.REDIS_PROXY_URL) return null;
  return String(env.REDIS_PROXY_URL).replace(/\/+$/, "");
}

/**
 * @param {RuntimeProxyEnv | null | undefined} env
 * @param {string} capability
 */
export function requireRedisProxyBaseUrl(env, capability) {
  const base = optionalRedisProxyBaseUrl(env);
  if (!base) {
    throw new Error(`${capability} requires REDIS_PROXY_URL`);
  }
  return base;
}

/**
 * @param {string} base
 * @param {string} path
 * @param {Record<string, unknown>} [params]
 */
export function proxyEndpoint(base, path, params = {}) {
  const url = new URL(`${base}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  return url;
}

/**
 * @param {URL | string} url
 * @param {RequestInit | undefined} init
 * @param {{
 *   env: RuntimeProxyEnv | null | undefined,
 *   failurePrefix: string,
 *   okStatuses?: readonly number[],
 *   transportError?: (error: unknown) => unknown,
 *   statusError?: (status: number) => unknown,
 * }} opts
 */
export async function proxyFetch(url, init, opts) {
  const env = opts.env;
  let res;
  try {
    res = await fetch(url, {
      ...init,
      headers: withInternalAuth(init?.headers, env),
    });
  } catch (error) {
    if (opts.transportError) throw opts.transportError(error);
    throw error;
  }
  if (!res.ok && !opts.okStatuses?.includes(res.status)) {
    await discardResponseBody(res);
    if (opts.statusError) throw opts.statusError(res.status);
    throw new Error(`${opts.failurePrefix} failed with ${res.status}`);
  }
  return res;
}
