// KV binding shim — quacks like Cloudflare's KVNamespace.
//
// This is a sidecar-style capability adapter: the loader puts dynamic code
// into workerLoader, and this module supplies one of the platform
// capabilities that loaded code finds on its `env`. New capabilities
// (R2, D1, secrets, …) should land as sibling files in this directory and
// be wired into runtime/load/env-build.js.
//
// The KV class is exported as a named entrypoint. Runtime instantiates
// one per binding via `ctx.exports.KV({ props: { ns, id } })` — that
// indirection is how workerd gives the loaded isolate an RPC-callable
// shim (plain JS objects would be structured-cloned without the RPC
// handler). Method calls on the stub cross back to the runtime isolate
// via JSRPC and execute through the Redis proxy sidecar.

import { WorkerEntrypoint } from "cloudflare:workers";
import { toBytes } from "runtime-lib";
import { recordBindingOperation } from "runtime-metrics";
import { base64ToBytes, bytesToBase64 } from "shared-base64";
import { readBoundedStreamBytes } from "shared-bounded-body";
import { discardResponseBody } from "shared-respond";
import {
  proxyEndpoint as buildProxyEndpoint,
  proxyFetch as fetchProxy,
  requireRedisProxyBaseUrl,
  serviceNameFromEnv,
} from "runtime-bindings-proxy";

export const KV_VALUE_MAX_BYTES = 25 * 1024 * 1024;
export const KV_LIST_LIMIT_MAX = 1000;
const KV_LIST_LIMIT_DEFAULT = 1000;
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

/**
 * @typedef {{ ns: string, id: string }} KVBindingProps
 * @typedef {{ REDIS_PROXY_URL?: unknown, SERVICE_NAME?: string, WDL_INTERNAL_AUTH_TOKEN?: unknown }} KVBindingEnv
 * @typedef {{ ctx: { props: KVBindingProps }, env: KVBindingEnv }} KVBinding
 * @typedef {string | { type?: string }} KVGetType
 * @typedef {{ expirationTtl?: unknown, expiration?: unknown, metadata?: unknown }} KVPutOptions
 * @typedef {{ prefix?: unknown, cursor?: unknown, limit?: unknown, metadata?: unknown }} KVListOptions
 * @typedef {{ key: string, value_b64?: string | null, metadata?: unknown }} KVBatchEntry
 */

/** @param {KV} kv @returns {KVBinding} */
function kvBinding(kv) {
  return /** @type {KVBinding} */ (/** @type {unknown} */ (kv));
}

/**
 * @param {number} size
 */
function assertKvValueSize(size) {
  if (size > KV_VALUE_MAX_BYTES) {
    throw new TypeError(`KV put: value exceeds ${KV_VALUE_MAX_BYTES} byte limit`);
  }
}

/**
 * @param {unknown} value
 * @param {string} name
 */
function requirePositiveInteger(value, name) {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(
      `KV put: ${name} must be a positive integer no greater than ${Number.MAX_SAFE_INTEGER}`
    );
  }
  return value;
}

/** @param {unknown} value */
function stringifyKvMetadata(value) {
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new TypeError("KV put: metadata must be JSON-serializable");
  }
  return json;
}

/**
 * @param {unknown} value
 * @returns {number}
 */
function normalizeListLimit(value) {
  const raw = value == null ? KV_LIST_LIMIT_DEFAULT : value;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1) {
    throw new TypeError(`KV list: limit must be an integer in [1, ${KV_LIST_LIMIT_MAX}]`);
  }
  return Math.min(n, KV_LIST_LIMIT_MAX);
}

/**
 * @param {Uint8Array} bytes
 * @param {KVGetType | undefined} typeOrOpts
 * @returns {unknown}
 */
function coerceValue(bytes, typeOrOpts) {
  const type =
    typeof typeOrOpts === "string" ? typeOrOpts : typeOrOpts?.type || "text";
  if (type === "text") return utf8Decoder.decode(bytes);
  if (type === "json") return JSON.parse(utf8Decoder.decode(bytes));
  if (type === "arrayBuffer") {
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  }
  if (type === "stream") return new Response(/** @type {BodyInit} */ (bytes)).body;
  throw new Error(`KV: unsupported type "${type}"`);
}

/**
 * @param {string | null | undefined} valueB64
 * @param {KVGetType | undefined} typeOrOpts
 * @returns {unknown}
 */
function coerceBatchValue(valueB64, typeOrOpts) {
  if (valueB64 == null) return null;
  assertBatchType(typeOrOpts);
  return coerceValue(base64ToBytes(valueB64), typeOrOpts);
}

/**
 * @param {KVGetType | undefined} typeOrOpts
 */
function assertBatchType(typeOrOpts) {
  const type =
    typeof typeOrOpts === "string" ? typeOrOpts : typeOrOpts?.type || "text";
  if (type === "arrayBuffer" || type === "stream") {
    throw new Error(`KV: unsupported batch type "${type}"`);
  }
}

/**
 * @param {KVBinding} kv
 * @returns {string}
 */
function serviceName(kv) {
  return serviceNameFromEnv(kv.env);
}

/**
 * @param {KVBinding} kv
 * @returns {string}
 */
function proxyUrl(kv) {
  return requireRedisProxyBaseUrl(kv.env, "KV binding");
}

/**
 * @param {KVBinding} kv
 * @param {string} path
 * @param {Record<string, unknown>} [params]
 * @returns {URL}
 */
function proxyEndpoint(kv, path, params = {}) {
  const { ns, id } = kv.ctx.props;
  return buildProxyEndpoint(proxyUrl(kv), path, { ns, id, ...params });
}

/**
 * @param {KVBinding} kv
 * @param {string} path
 * @param {RequestInit | undefined} init
 * @param {Record<string, unknown>} [params]
 * @returns {Promise<Response>}
 */
async function proxyFetch(kv, path, init, params) {
  return fetchProxy(proxyEndpoint(kv, path, params), init, {
    env: kv.env,
    failurePrefix: `KV proxy ${path}`,
    // 404 is load-bearing on /kv/get (missing key -> null); no other route
    // returns it. Cancel the body without reading so proxy-side error text
    // doesn't land verbatim in the Error surfaced to user code.
    okStatuses: [404],
  });
}

export class KV extends WorkerEntrypoint {
  /**
   * @param {string | string[]} key
   * @param {KVGetType} [typeOrOpts]
   */
  async get(key, typeOrOpts) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "get", async () => {
      if (Array.isArray(key)) {
        assertBatchType(typeOrOpts);
        const res = await proxyFetch(kv, "/kv/get-batch", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ keys: key }),
        });
        const body = await res.json();
        return new Map(
          body.entries.map(/** @param {KVBatchEntry} entry */ (entry) => [
            entry.key,
            coerceBatchValue(entry.value_b64 ?? null, typeOrOpts),
          ])
        );
      }
      const res = await proxyFetch(kv, "/kv/get", undefined, { key });
      if (res.status === 404) {
        await discardResponseBody(res);
        return null;
      }
      return coerceValue(new Uint8Array(await res.arrayBuffer()), typeOrOpts);
    });
  }

  /**
   * @param {string | string[]} key
   * @param {KVGetType} [typeOrOpts]
   */
  async getWithMetadata(key, typeOrOpts) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "getWithMetadata", async () => {
      if (Array.isArray(key)) {
        assertBatchType(typeOrOpts);
        const res = await proxyFetch(kv, "/kv/get-batch", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ keys: key, metadata: true }),
        });
        const body = await res.json();
        return new Map(
          body.entries.map(/** @param {KVBatchEntry} entry */ (entry) => [
            entry.key,
            {
              value: coerceBatchValue(entry.value_b64 ?? null, typeOrOpts),
              metadata: entry.metadata ?? null,
            },
          ])
        );
      }
      const res = await proxyFetch(kv, "/kv/get-with-metadata", undefined, { key });
      const body = await res.json();
      const value = body.value_b64 ? coerceValue(base64ToBytes(body.value_b64), typeOrOpts) : null;
      return { value, metadata: body.metadata ?? null };
    });
  }

  /**
   * @param {string} key
   * @param {unknown} value
   * @param {KVPutOptions} [opts]
   */
  async put(key, value, opts = {}) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "put", async () => {
      let bytes;
      if (value instanceof ReadableStream) {
        bytes = await readBoundedStreamBytes(
          value,
          KV_VALUE_MAX_BYTES,
          () => new TypeError(`KV put: value exceeds ${KV_VALUE_MAX_BYTES} byte limit`)
        );
      } else {
        bytes = toBytes(value);
        assertKvValueSize(bytes.byteLength);
      }
      const setOpts = {};
      if (opts.expirationTtl != null) {
        setOpts.ttl = requirePositiveInteger(opts.expirationTtl, "expirationTtl");
      } else if (opts.expiration != null) {
        setOpts.exat = requirePositiveInteger(opts.expiration, "expiration");
      }

      /** @type {Record<string, string>} */
      const headers = {};
      if (opts.metadata !== undefined) {
        // Metadata rides a base64 header because the body slot is taken by
        // the raw value bytes and HTTP headers need ASCII-safe transport.
        headers["x-kv-metadata-b64"] = bytesToBase64(
          utf8Encoder.encode(stringifyKvMetadata(opts.metadata))
        );
      }
      await proxyFetch(kv, "/kv/put", {
        method: "PUT",
        headers,
        body: /** @type {BodyInit} */ (bytes),
      }, { key, ttl: setOpts.ttl, exat: setOpts.exat });
    });
  }

  /**
   * @param {string} key
   */
  async delete(key) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "delete", async () => {
      await proxyFetch(kv, "/kv/delete", { method: "DELETE" }, { key });
    });
  }

  /**
   * @param {KVListOptions} [opts]
   */
  async list(opts = {}) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "list", async () => {
      const { prefix = "", cursor, metadata } = opts;
      const limit = normalizeListLimit(opts.limit);
      const res = await proxyFetch(kv, "/kv/list", undefined, { prefix, limit, cursor, metadata: metadata === true ? "true" : undefined });
      return res.json();
    });
  }
}
