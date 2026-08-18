import { scopedDoRequest } from "./_wdl-do-transport.js";
import { requestIdFromOptions } from "./_wdl-request-id.js";

const intrinsicReflectApply = Reflect.apply;
const intrinsicStringIsWellFormed = String.prototype.isWellFormed;

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
 * @typedef {{
 *   fetch?(request: Request): unknown,
 *   rpcObject?(objectName: string, method: string, args: unknown[], requestId: string | null): unknown,
 * }} DurableObjectBindingProxy
 * @typedef {{
 *   requestId?: string,
 *   requestIdProvider?: () => string | null,
 * }} DurableObjectNamespaceOptions
 */

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

export class DurableObjectNamespace {
  /** @type {null | ((objectName: string, request: Request, requestId: string | null) => Promise<Response>)} */
  #bindingFetch = null;
  /** @type {null | ((objectName: string, method: string, args: unknown[], requestId: string | null) => Promise<unknown>)} */
  #bindingRpcObject = null;
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
    const rpcObject = proxy.rpcObject;
    this.#bindingRpcObject = typeof rpcObject === "function"
      ? async (objectName, method, args, requestId) => await intrinsicReflectApply(
        rpcObject,
        proxy,
        [objectName, method, args, requestId]
      )
      : null;
  }

  /**
   * @param {DurableObjectBindingProxy} binding
   * @param {DurableObjectNamespaceOptions} [options]
   */
  constructor(binding, options = {}) {
    this.#setBindingProxy(binding);
    this.#requestIdOptions = options || null;
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
    throw new Error("Durable Object binding fetch is not configured");
  }

  /** @param {string} objectName @param {string} method @param {unknown[]} args */
  async rpcObject(objectName, method, args) {
    const requestId = this.#currentRequestId();
    if (this.#bindingRpcObject) {
      return await this.#bindingRpcObject(objectName, method, args, requestId);
    }
    throw new Error("Durable Object binding RPC is not configured");
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
