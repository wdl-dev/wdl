import { WorkerEntrypoint } from "cloudflare:workers";
import {
  DO_CONNECT_URL,
  DO_INVOKE_URL,
  connectHeaders,
  doOwnerHintCacheKey,
  dispatchDoConnectWithHintCache,
  dispatchDoInvokeWithHintCache,
  fetchInvokeInit,
  isWebSocketUpgrade,
  replayOwnerUnavailableForFetch,
  rpcInvokeInit,
  rpcResultFromResponse,
} from "runtime-do-transport";
import { createOwnerHintCache } from "runtime-owner-hint-cache";
import { withInternalAuth } from "shared-internal-auth";

/**
 * @typedef {{ ns: string, worker: string, version: string, doStorageId: string, className: string }} DoBindingProps
 * @typedef {{ DO_BACKEND: { fetch(url: string, init?: RequestInit): Promise<Response> }, WDL_INTERNAL_AUTH_TOKEN?: unknown }} DoBindingEnv
 * @typedef {{ ctx: { props: DoBindingProps }, env: DoBindingEnv }} DoBinding
 */

const ownerHintCache = createOwnerHintCache();

/** @param {DurableObjectNamespace} binding @returns {DoBinding} */
function doBinding(binding) {
  return /** @type {DoBinding} */ (/** @type {unknown} */ (binding));
}

/** @param {DurableObjectNamespace} binding */
function propsOf(binding) {
  const view = doBinding(binding);
  const props = view.ctx.props || /** @type {DoBindingProps} */ ({});
  return {
    ns: props.ns,
    worker: props.worker,
    version: props.version,
    doStorageId: props.doStorageId,
    className: props.className,
  };
}

/**
 * @param {DurableObjectNamespace} binding
 * @param {RequestInit} init
 * @param {string} objectName
 * @param {{ replayOwnerUnavailable?: boolean }} [options]
 */
async function dispatchInvokeWithOwnerHint(binding, init, objectName, { replayOwnerUnavailable = false } = {}) {
  const backend = doBinding(binding).env.DO_BACKEND;
  const props = propsOf(binding);
  const hintKey = doOwnerHintCacheKey(props, objectName);
  return await dispatchDoInvokeWithHintCache({
    routerFetch: (url, requestInit) => backend.fetch(url, requestInit),
    routerUrl: DO_INVOKE_URL,
    ownerFetch: fetch,
    ownerPath: "/internal/do/invoke",
    init,
    cache: ownerHintCache,
    hintKey,
    replayOwnerUnavailable,
  });
}

export class DurableObjectNamespace extends WorkerEntrypoint {
  /**
   * @param {string} objectName
   * @param {Request} request
   * @param {string | null} [requestId]
   * @returns {Promise<Response>}
   */
  async fetchObject(objectName, request, requestId = null) {
    const props = propsOf(this);
    // This facade is only used inside do-runtime for DO-to-DO calls. The
    // self-service calls intentionally start fresh internal requests; owner
    // forwarding, if needed, will add and enforce hop-count headers.
    if (isWebSocketUpgrade(request)) {
      const init = {
        method: "GET",
        headers: withInternalAuth(connectHeaders(props, objectName, request, requestId), doBinding(this).env),
      };
      const hintKey = doOwnerHintCacheKey(props, objectName);
      return await dispatchDoConnectWithHintCache({
        routerFetch: (url, requestInit) => doBinding(this).env.DO_BACKEND.fetch(url, requestInit),
        routerUrl: DO_CONNECT_URL,
        ownerFetch: fetch,
        ownerPath: "/internal/do/connect",
        init,
        cache: ownerHintCache,
        hintKey,
      });
    }
    const init = await fetchInvokeInit(props, objectName, request, requestId);
    init.headers = withInternalAuth(init.headers, doBinding(this).env);
    return await dispatchInvokeWithOwnerHint(this, init, objectName, {
      replayOwnerUnavailable: replayOwnerUnavailableForFetch(request),
    });
  }

  /**
   * @param {string} objectName
   * @param {string} method
   * @param {unknown[]} args
   * @param {string | null} [requestId]
   */
  async rpcObject(objectName, method, args, requestId = null) {
    const props = propsOf(this);
    const init = rpcInvokeInit(props, objectName, method, args, requestId);
    init.headers = withInternalAuth(init.headers, doBinding(this).env);
    const routed = await dispatchInvokeWithOwnerHint(this, init, objectName);
    return await rpcResultFromResponse(routed);
  }
}
