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
import {
  acquireKvReadLease,
  kvReadCapacityError,
  kvReadTimeoutError,
} from "runtime-bindings-kv-capacity";
import { recordBindingOperation } from "runtime-metrics";
import {
  isRuntimeInfrastructureError,
  runtimeInfrastructureError,
} from "runtime-infrastructure-error";
import {
  canonicalBase64ToBytes,
  bytesToBase64,
  prepareCanonicalBase64Values,
} from "shared-base64";
import { readBoundedStreamBytes } from "shared-bounded-body";
import { discardResponseBody } from "shared-respond";
import {
  proxyEndpoint as buildProxyEndpoint,
  proxyFetch as fetchProxy,
  requireRedisProxyBaseUrl,
  serviceNameFromEnv,
} from "runtime-bindings-proxy";

export const KV_VALUE_MAX_BYTES = 25 * 1024 * 1024;
export const KV_READ_RESPONSE_MAX_BYTES = 36 * 1024 * 1024;
export const KV_LIST_LIMIT_MAX = 1000;
const KV_LIST_LIMIT_DEFAULT = 1000;
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const strictUtf8Decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * @typedef {{ ns: string, id: string }} KVBindingProps
 * @typedef {{ REDIS_PROXY_URL?: unknown, SERVICE_NAME?: string, WDL_INTERNAL_AUTH_TOKEN?: unknown }} KVBindingEnv
 * @typedef {{ ctx: { props: KVBindingProps, waitUntil(promise: Promise<unknown>): void }, env: KVBindingEnv }} KVBinding
 * @typedef {string | { type?: string }} KVGetType
 * @typedef {{ expirationTtl?: unknown, expiration?: unknown, metadata?: unknown }} KVPutOptions
 * @typedef {{ prefix?: unknown, cursor?: unknown, limit?: unknown, metadata?: unknown }} KVListOptions
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
    if (
      bytes.buffer instanceof ArrayBuffer &&
      bytes.byteOffset === 0 &&
      bytes.byteLength === bytes.buffer.byteLength
    ) {
      return bytes.buffer;
    }
    return bytes.slice().buffer;
  }
  if (type === "stream") return new Response(/** @type {BodyInit} */ (bytes)).body;
  throw new Error(`KV: unsupported type "${type}"`);
}

/**
 * @param {Uint8Array | null} value
 * @param {KVGetType | undefined} typeOrOpts
 * @returns {unknown}
 */
function coerceBatchValue(value, typeOrOpts) {
  if (value === null) return null;
  assertBatchType(typeOrOpts);
  return coerceValue(value, typeOrOpts);
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
    okStatuses: path === "/kv/get" ? [404] : undefined,
  });
}

/**
 * @template T
 * @param {KVBinding} kv
 * @param {Response} response
 * @param {(bytes: Uint8Array) => T} consume
 * @param {string | null} [infrastructureInvocationId]
 * @returns {Promise<T>}
 */
async function consumeReadResponse(
  kv,
  response,
  consume,
  infrastructureInvocationId = null
) {
  const aborter = new AbortController();
  const lease = acquireKvReadLease(kv, response, () => {
    aborter.abort(kvReadTimeoutError(infrastructureInvocationId));
  });
  if (!lease) {
    await discardResponseBody(response);
    throw kvReadCapacityError(infrastructureInvocationId);
  }
  try {
    let bytes;
    try {
      if (!response.body && lease.contentLength !== null && lease.contentLength > 0) {
        throw new TypeError("KV response body is missing despite a non-zero Content-Length");
      }
      bytes = response.body
        ? await readBoundedStreamBytes(
          response.body,
          KV_READ_RESPONSE_MAX_BYTES,
          () => runtimeInfrastructureError(
            "KV read response is too large",
            "Runtime KV response exceeded its wire byte limit",
            infrastructureInvocationId
          ),
          aborter.signal,
          lease.contentLength
        )
        : new Uint8Array();
    } catch (error) {
      if (isRuntimeInfrastructureError(error)) throw error;
      throw runtimeInfrastructureError(
        "KV read response failed",
        `Runtime KV response body failed: ${error instanceof Error ? error.message : String(error)}`,
        infrastructureInvocationId
      );
    }
    return consume(bytes);
  } finally {
    lease.release(aborter.signal.aborted ? "deadline" : "completed");
  }
}

/** @param {unknown} value */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {string} diagnostic @param {string | null} infrastructureInvocationId */
function invalidKvEnvelope(diagnostic, infrastructureInvocationId) {
  return runtimeInfrastructureError(
    "KV read response is invalid",
    diagnostic,
    infrastructureInvocationId
  );
}

/**
 * @param {Uint8Array} bytes
 * @param {string | null} infrastructureInvocationId
 * @param {string} diagnostic
 * @returns {Record<string, unknown>}
 */
function parseKvEnvelopeRecord(bytes, infrastructureInvocationId, diagnostic) {
  let parsed;
  try {
    parsed = /** @type {unknown} */ (JSON.parse(strictUtf8Decoder.decode(bytes)));
  } catch {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  if (!isRecord(parsed)) throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {string} value
 * @param {string | null} infrastructureInvocationId
 * @param {string} diagnostic
 */
function decodeKvEnvelopeBase64(value, infrastructureInvocationId, diagnostic) {
  try {
    return canonicalBase64ToBytes(value);
  } catch {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
}

/**
 * @param {Array<{ value_b64: string | null }>} entries
 * @param {string | null} infrastructureInvocationId
 */
function prepareKvBatchValues(entries, infrastructureInvocationId) {
  const diagnostic = "Runtime KV batch response envelope is invalid";
  try {
    return prepareCanonicalBase64Values(entries.map((entry) => entry.value_b64));
  } catch {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
}

/**
 * @param {Uint8Array} bytes
 * @param {string | null} infrastructureInvocationId
 * @param {string[]} requestedKeys
 * @returns {Array<{ key: string, value_b64: string | null, metadata: unknown }>}
 */
function parseKvBatchEnvelope(bytes, infrastructureInvocationId, requestedKeys) {
  const diagnostic = "Runtime KV batch response envelope is invalid";
  const body = parseKvEnvelopeRecord(bytes, infrastructureInvocationId, diagnostic);
  if (!Array.isArray(body.entries) || body.entries.length !== requestedKeys.length) {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  for (let index = 0; index < body.entries.length; index += 1) {
    const entry = body.entries[index];
    if (
      !isRecord(entry) ||
      typeof entry.key !== "string" ||
      entry.key !== requestedKeys[index] ||
      !Object.hasOwn(entry, "value_b64") ||
      (entry.value_b64 !== null && typeof entry.value_b64 !== "string") ||
      !Object.hasOwn(entry, "metadata") ||
      (entry.value_b64 === null && entry.metadata !== null)
    ) {
      throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
    }
  }
  return /** @type {Array<{ key: string, value_b64: string | null, metadata: unknown }>} */ (
    body.entries
  );
}

/**
 * @param {Uint8Array} bytes
 * @param {string | null} infrastructureInvocationId
 */
function parseKvMetadataEnvelope(bytes, infrastructureInvocationId) {
  const diagnostic = "Runtime KV metadata response envelope is invalid";
  const body = parseKvEnvelopeRecord(bytes, infrastructureInvocationId, diagnostic);
  const valueB64 = body.value_b64;
  if (!Object.hasOwn(body, "value_b64") || !Object.hasOwn(body, "metadata")) {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  if (valueB64 === null && body.metadata !== null) {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  let value;
  if (valueB64 === null) {
    value = null;
  } else if (typeof valueB64 === "string") {
    value = decodeKvEnvelopeBase64(valueB64, infrastructureInvocationId, diagnostic);
  } else {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  return {
    value,
    metadata: body.metadata,
  };
}

/**
 * @param {Uint8Array} bytes
 * @param {string | null} infrastructureInvocationId
 * @param {boolean} includeMetadata
 */
function parseKvListEnvelope(bytes, infrastructureInvocationId, includeMetadata) {
  const diagnostic = "Runtime KV list response envelope is invalid";
  const body = parseKvEnvelopeRecord(bytes, infrastructureInvocationId, diagnostic);
  if (!Array.isArray(body.keys) || typeof body.list_complete !== "boolean") {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  /** @type {Array<{ name: string, metadata?: unknown }>} */
  const keys = [];
  for (const entry of body.keys) {
    if (
      !isRecord(entry) ||
      typeof entry.name !== "string" ||
      (includeMetadata && !Object.hasOwn(entry, "metadata"))
    ) {
      throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
    }
    keys.push(includeMetadata
      ? { name: entry.name, metadata: entry.metadata }
      : { name: entry.name });
  }
  if (body.list_complete) return { keys, list_complete: true };
  if (typeof body.cursor !== "string" || body.cursor.length === 0) {
    throw invalidKvEnvelope(diagnostic, infrastructureInvocationId);
  }
  return { keys, list_complete: false, cursor: body.cursor };
}

export class KV extends WorkerEntrypoint {
  /**
   * @param {string | string[]} key
   * @param {KVGetType} [typeOrOpts]
   * @param {string | null} [infrastructureInvocationId]
   */
  async get(key, typeOrOpts, infrastructureInvocationId = null) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "get", async () => {
      if (Array.isArray(key)) {
        assertBatchType(typeOrOpts);
        const res = await proxyFetch(kv, "/kv/get-batch", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ keys: key }),
        });
        return consumeReadResponse(
          kv,
          res,
          (bytes) => {
            const entries = parseKvBatchEnvelope(bytes, infrastructureInvocationId, key);
            const values = prepareKvBatchValues(entries, infrastructureInvocationId);
            return new Map(
              values.map((value, index) => [
                entries[index].key,
                coerceBatchValue(value, typeOrOpts),
              ])
            );
          },
          infrastructureInvocationId
        );
      }
      const res = await proxyFetch(kv, "/kv/get", undefined, { key });
      if (res.status === 404) {
        await discardResponseBody(res);
        return null;
      }
      return consumeReadResponse(
        kv,
        res,
        (bytes) => coerceValue(bytes, typeOrOpts),
        infrastructureInvocationId
      );
    });
  }

  /**
   * @param {string | string[]} key
   * @param {KVGetType} [typeOrOpts]
   * @param {string | null} [infrastructureInvocationId]
   */
  async getWithMetadata(key, typeOrOpts, infrastructureInvocationId = null) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "getWithMetadata", async () => {
      if (Array.isArray(key)) {
        assertBatchType(typeOrOpts);
        const res = await proxyFetch(kv, "/kv/get-batch", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ keys: key, metadata: true }),
        });
        return consumeReadResponse(
          kv,
          res,
          (bytes) => {
            const entries = parseKvBatchEnvelope(bytes, infrastructureInvocationId, key);
            const values = prepareKvBatchValues(entries, infrastructureInvocationId);
            return new Map(
              values.map((value, index) => [
                entries[index].key,
                {
                  value: coerceBatchValue(value, typeOrOpts),
                  metadata: entries[index].metadata,
                },
              ])
            );
          },
          infrastructureInvocationId
        );
      }
      const res = await proxyFetch(kv, "/kv/get-with-metadata", undefined, { key });
      return consumeReadResponse(
        kv,
        res,
        (bytes) => {
          const body = parseKvMetadataEnvelope(bytes, infrastructureInvocationId);
          const value = body.value === null ? null : coerceValue(body.value, typeOrOpts);
          return { value, metadata: body.metadata };
        },
        infrastructureInvocationId
      );
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
   * @param {string | null} [infrastructureInvocationId]
   */
  async list(opts = {}, infrastructureInvocationId = null) {
    const kv = kvBinding(this);
    return recordBindingOperation(serviceName(kv), "kv", "list", async () => {
      const { prefix = "", cursor, metadata } = opts;
      const limit = normalizeListLimit(opts.limit);
      const res = await proxyFetch(kv, "/kv/list", undefined, { prefix, limit, cursor, metadata: metadata === true ? "true" : undefined });
      return consumeReadResponse(
        kv,
        res,
        (bytes) => parseKvListEnvelope(bytes, infrastructureInvocationId, metadata === true),
        infrastructureInvocationId
      );
    });
  }
}
