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
  scopedDoRequest,
} from "./_wdl-do-transport.js";
import { createOwnerHintCache } from "./_wdl-owner-hint-cache.js";
import { requestIdFromOptions } from "./_wdl-request-id.js";

const ownerHintCache = createOwnerHintCache();
const intrinsicObjectHasOwn = Object.hasOwn;
const intrinsicReflectApply = Reflect.apply;
const intrinsicStringIsWellFormed = String.prototype.isWellFormed;

/** @param {object} object @param {PropertyKey} key */
function objectHasOwn(object, key) {
  return intrinsicReflectApply(intrinsicObjectHasOwn, undefined, [object, key]);
}

/** @param {string} value */
function isWellFormedUnicodeString(value) {
  return intrinsicReflectApply(intrinsicStringIsWellFormed, value, []);
}

/** @param {unknown} value @param {string} method */
function requireObjectIdString(value, method) {
  if (typeof value !== "string" || !value) {
    throw new TypeError(`DurableObjectNamespace.${method}() requires a non-empty string`);
  }
  if (!isWellFormedUnicodeString(value)) {
    throw new TypeError(`DurableObjectNamespace.${method}() requires well-formed Unicode`);
  }
  return value;
}

/**
 * @typedef {{ fetch(url: string, init?: RequestInit): Promise<Response> }} DoBackend
 * @typedef {{
 *   ns?: string,
 *   worker?: string,
 *   version?: string,
 *   doStorageId?: string,
 *   binding?: string,
 *   className?: string,
 * }} DurableObjectBindingProps
 * @typedef {{
 *   fetch?(request: Request): unknown,
 *   rpcObject?(objectName: string, method: string, args: unknown[], requestId: string | null): unknown,
 * }} DurableObjectBindingProxy
 * @typedef {{
 *   backend?: DoBackend,
 *   ownerNetwork?: DoBackend,
 *   requestId?: string,
 *   requestIdProvider?: () => string | null,
 * }} DurableObjectNamespaceOptions
 */

export function clearDoOwnerHintsForTest() {
  ownerHintCache.clearForTest();
}

/** @param {number} maxEntries */
export function setDoOwnerHintMaxEntriesForTest(maxEntries) {
  ownerHintCache.setMaxEntriesForTest(maxEntries);
}

class DurableObjectId {
  /** @param {string} name */
  constructor(name) {
    this.name = name;
  }

  toString() {
    return this.name;
  }
}

class DurableObjectStub {
  /**
   * @param {DurableObjectNamespace} namespace
   * @param {DurableObjectId} id
   */
  constructor(namespace, id) {
    this.namespace = namespace;
    this.id = id;
    return new Proxy(this, {
      get(target, prop, receiver) {
        if (typeof prop !== "string") return Reflect.get(target, prop, receiver);
        // Avoid making stubs look like thenables or JSON-serializable RPC
        // objects; all other unknown string properties are DO RPC methods.
        if (prop === "then" || prop === "toJSON") return undefined;
        const real = Reflect.get(target, prop, receiver);
        if (real !== undefined) return real;
        /** @param {...unknown} args */
        const method = (...args) => target.namespace.rpcObject(target.id.name, prop, args);
        return method;
      },
    });
  }

  /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
  async fetch(input, init = undefined) {
    const request = new Request(input, init);
    return await this.namespace.fetchObject(this.id.name, request);
  }
}

/** @param {DurableObjectBindingProps | undefined} props */
function requireBindingProps(props) {
  if (!props ||
      typeof props.ns !== "string" ||
      typeof props.worker !== "string" ||
      typeof props.version !== "string" ||
      typeof props.doStorageId !== "string" ||
      typeof props.className !== "string") {
    throw new Error("Durable Object binding metadata is not configured");
  }
  return /** @type {import("./_wdl-do-transport.js").DoBindingProps} */ (props);
}

export class DurableObjectNamespace {
  /** @type {DoBackend | undefined} */
  #backend;
  /** @type {null | ((objectName: string, request: Request, requestId: string | null) => Promise<Response>)} */
  #bindingFetch = null;
  /** @type {null | ((objectName: string, method: string, args: unknown[], requestId: string | null) => Promise<unknown>)} */
  #bindingRpcObject = null;
  /** @type {DoBackend | undefined} */
  #ownerNetwork;
  /** @type {DurableObjectBindingProps | undefined} */
  #props;
  /** @type {unknown} */
  #requestIdOptions = null;

  /** @param {DurableObjectBindingProxy} proxy */
  #setBindingProxy(proxy) {
    const fetch = proxy.fetch;
    this.#bindingFetch = typeof fetch === "function"
      ? async (objectName, request, requestId) => {
        const response = await intrinsicReflectApply(fetch, proxy, [
          scopedDoRequest(objectName, request, requestId),
        ]);
        if (!(response instanceof Response)) {
          throw new Error("Durable Object binding fetch returned a non-Response value");
        }
        return response;
      }
      : null;
    this.#bindingRpcObject = typeof proxy.rpcObject === "function"
      ? async (objectName, method, args, requestId) => await proxy.rpcObject?.(objectName, method, args, requestId)
      : null;
  }

  /**
   * @param {DurableObjectBindingProps | DurableObjectBindingProxy | null | undefined} binding
   * @param {DurableObjectNamespaceOptions | string} [options]
   */
  constructor(binding, options = {}) {
    const isMetadataBinding = binding && typeof binding === "object" && (
      objectHasOwn(binding, "ns") ||
      objectHasOwn(binding, "worker") ||
      objectHasOwn(binding, "version") ||
      objectHasOwn(binding, "doStorageId") ||
      objectHasOwn(binding, "binding") ||
      objectHasOwn(binding, "className")
    );
    if (binding && !isMetadataBinding) {
      this.#setBindingProxy(/** @type {DurableObjectBindingProxy} */ (binding));
    } else {
      const props = /** @type {DurableObjectBindingProps | null | undefined} */ (binding);
      this.#props = {
        ns: props?.ns,
        worker: props?.worker,
        version: props?.version,
        doStorageId: props?.doStorageId,
        binding: props?.binding,
        className: props?.className,
      };
    }
    if (typeof options === "string") {
      this.#requestIdOptions = { requestId: options };
    } else {
      this.#backend = options?.backend;
      this.#ownerNetwork = options?.ownerNetwork;
      this.#requestIdOptions = options || null;
    }
  }

  #currentRequestId() {
    return requestIdFromOptions(this.#requestIdOptions);
  }

  /** @param {string} objectName @param {Request} request */
  async fetchObject(objectName, request) {
    const requestId = this.#currentRequestId();
    if (this.#bindingFetch) {
      return await this.#bindingFetch(objectName, request, requestId);
    }
    // Explicit backend options are the low-level transport construction path;
    // runtime materialization supplies binding-scoped methods above.
    const backend = this.#backend;
    if (!backend || typeof backend.fetch !== "function") {
      throw new Error("Durable Object backend is not configured");
    }
    const props = requireBindingProps(this.#props);
    if (isWebSocketUpgrade(request)) {
      const init = {
        method: "GET",
        headers: connectHeaders(props, objectName, request, requestId),
      };
      return await this.#dispatchWithOwnerHint(
        dispatchDoConnectWithHintCache,
        DO_CONNECT_URL,
        "/internal/do/connect",
        init,
        doOwnerHintCacheKey(props, objectName),
        false
      );
    }
    const init = await fetchInvokeInit(props, objectName, request, requestId);
    return await this.#dispatchWithOwnerHint(
      dispatchDoInvokeWithHintCache,
      DO_INVOKE_URL,
      "/internal/do/invoke",
      init,
      doOwnerHintCacheKey(props, objectName),
      replayOwnerUnavailableForFetch(request)
    );
  }

  /** @param {string} objectName @param {string} method @param {unknown[]} args */
  async rpcObject(objectName, method, args) {
    const requestId = this.#currentRequestId();
    if (this.#bindingRpcObject) {
      return await this.#bindingRpcObject(objectName, method, args, requestId);
    }
    const backend = this.#backend;
    if (!backend || typeof backend.fetch !== "function") {
      throw new Error("Durable Object backend is not configured");
    }
    const props = requireBindingProps(this.#props);
    const init = rpcInvokeInit(props, objectName, method, args, requestId);
    const response = await this.#dispatchWithOwnerHint(
      dispatchDoInvokeWithHintCache,
      DO_INVOKE_URL,
      "/internal/do/invoke",
      init,
      doOwnerHintCacheKey(props, objectName),
      false
    );
    return await rpcResultFromResponse(response);
  }

  /**
   * @param {typeof dispatchDoInvokeWithHintCache | typeof dispatchDoConnectWithHintCache} dispatch
   * @param {string} routerUrl
   * @param {string} ownerPath
   * @param {RequestInit} init
   * @param {string} hintKey
   * @param {boolean} replayOwnerUnavailable
   */
  async #dispatchWithOwnerHint(dispatch, routerUrl, ownerPath, init, hintKey, replayOwnerUnavailable) {
    const backend = this.#backend;
    const ownerNetwork = this.#ownerNetwork;
    if (!backend || typeof backend.fetch !== "function") {
      throw new Error("Durable Object backend is not configured");
    }
    /** @type {import("./_wdl-do-transport.js").DoFetch | null} */
    const ownerFetch = typeof ownerNetwork?.fetch === "function"
      ? (url, requestInit) => ownerNetwork.fetch(url, requestInit)
      : null;
    /** @type {import("./_wdl-do-transport.js").DoFetch} */
    const routerFetch = (url, requestInit) => backend.fetch(url, requestInit);
    return await dispatch({
      routerFetch,
      routerUrl,
      ownerFetch,
      ownerPath,
      init,
      cache: ownerHintCache,
      hintKey,
      replayOwnerUnavailable,
    });
  }

  /** @param {string} name */
  idFromName(name) {
    return new DurableObjectId(requireObjectIdString(name, "idFromName"));
  }

  /** @param {string} value */
  idFromString(value) {
    return new DurableObjectId(requireObjectIdString(value, "idFromString"));
  }

  newUniqueId() {
    return new DurableObjectId(crypto.randomUUID());
  }

  /** @param {DurableObjectId} id */
  get(id) {
    if (!(id instanceof DurableObjectId)) {
      throw new TypeError("DurableObjectNamespace.get() requires an id returned by this namespace");
    }
    return new DurableObjectStub(this, id);
  }
}
