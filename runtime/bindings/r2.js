import { WorkerEntrypoint } from "cloudflare:workers";
import { SigV4Client } from "@wdl-dev/aws-sigv4";
import { recordBindingOperation } from "runtime-metrics";
import { serviceNameFromEnv } from "runtime-bindings-proxy";
import { discardResponseBody } from "shared-respond";
import { bytesToBase64 } from "shared-base64";
import { S3_TRANSIENT_RETRIES, fetchRetryableS3Post } from "shared-s3-retry";
import {
  assertR2BufferSize,
  encodeS3KeyPath,
  encodeS3Query,
  normalizeR2ListLimit,
  normalizeR2ObjectKey,
  r2PhysicalKey,
  r2PhysicalPrefix,
  stripR2PhysicalPrefix,
} from "runtime-r2-utils";
import {
  applyCustomMetadata,
  applyGetOptions,
  applyHttpMetadata,
  applyOnlyIf,
  headersWithRequestId,
  metaFromHeaders,
  metaFromPutResponse,
} from "runtime-bindings-r2-metadata";
import { parseListObjects, xmlEscape, xmlUnescape } from "runtime-bindings-r2-xml";

const DELETE_OBJECTS_BATCH_SIZE = 1000;
const LIST_INCLUDE_HEAD_CONCURRENCY = 16;
const S3_CLIENT_CACHE_MAX_ENTRIES = 128;
const utf8Encoder = new TextEncoder();
const s3Cache = new Map();
const s3ByBucket = new WeakMap();

/**
 * @typedef {{ ns: string, bucketName: string }} R2BindingProps
 * @typedef {Record<string, unknown>} R2BindingEnv
 * @typedef {{ ctx: { props: R2BindingProps }, env: R2BindingEnv }} R2BucketBinding
 * @typedef {{ client: { fetch(url: string, init?: RequestInit): Promise<Response> }, endpoint: string, bucket: string }} S3Binding
 * @typedef {import("runtime-bindings-r2-metadata").R2Options} R2Options
 * @typedef {import("runtime-bindings-r2-metadata").R2RequestMeta} R2RequestMeta
 */

/** @param {R2Bucket} bucket @returns {R2BucketBinding} */
function r2Binding(bucket) {
  return /** @type {R2BucketBinding} */ (/** @type {unknown} */ (bucket));
}

/** @param {BufferSource} bytes */
async function sha256Base64(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return bytesToBase64(new Uint8Array(digest));
}

/**
 * @param {Record<string, unknown>} env
 * @param {string} key
 * @returns {string}
 */
function requireEnv(env, key) {
  if (!env[key]) throw new Error(`R2 binding requires ${key}`);
  return String(env[key]);
}

/**
 * @param {Record<string, unknown>} env
 */
function s3ClientConfig(env) {
  return {
    accessKeyId: requireEnv(env, "R2_S3_ACCESS_KEY_ID"),
    secretAccessKey: requireEnv(env, "R2_S3_SECRET_ACCESS_KEY"),
    endpoint: requireEnv(env, "R2_S3_ENDPOINT").replace(/\/+$/, ""),
    bucket: requireEnv(env, "R2_S3_BUCKET"),
    region: typeof env.R2_S3_REGION === "string" && env.R2_S3_REGION
      ? env.R2_S3_REGION
      : "us-east-1",
  };
}

/** @param {Record<string, string>} config */
function s3CacheKey(config) {
  return [
    config.endpoint,
    config.bucket,
    config.region,
    config.accessKeyId,
    config.secretAccessKey,
  ].join("\n");
}

/**
 * @template T,U
 * @param {T[]} items
 * @param {number} concurrency
 * @param {(item: T) => Promise<U>} fn
 * @returns {Promise<U[]>}
 */
async function mapWithConcurrency(items, concurrency, fn) {
  const out = new Array(items.length);
  let next = 0;
  async function worker() {
    for (;;) {
      const index = next++;
      if (index >= items.length) return;
      out[index] = await fn(items[index]);
    }
  }
  const count = Math.min(concurrency, items.length);
  await Promise.all(Array.from({ length: count }, worker));
  return out;
}

/** @param {R2BucketBinding} bucket */
function serviceName(bucket) {
  return serviceNameFromEnv(bucket.env);
}

/**
 * @param {unknown} value
 * @returns {Uint8Array}
 */
function putBodyFromUnknown(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new TypeError("R2 put: value must be ArrayBuffer or ArrayBufferView");
}

/** @param {R2BucketBinding} bucket */
function s3ForBucket(bucket) {
  const existing = s3ByBucket.get(bucket);
  if (existing) return existing;
  const config = s3ClientConfig(bucket.env);
  const key = s3CacheKey(config);
  let cached = s3Cache.get(key);
  if (cached) {
    s3Cache.delete(key);
    s3Cache.set(key, cached);
  } else {
    cached = {
      client: new SigV4Client({
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
        service: "s3",
        region: config.region,
        retries: S3_TRANSIENT_RETRIES,
      }),
      endpoint: config.endpoint,
      bucket: config.bucket,
    };
    s3Cache.set(key, cached);
    if (s3Cache.size > S3_CLIENT_CACHE_MAX_ENTRIES) {
      const oldestKey = s3Cache.keys().next().value;
      s3Cache.delete(oldestKey);
    }
  }
  const local = {
    client: cached.client,
    endpoint: cached.endpoint,
    bucket: cached.bucket,
  };
  s3ByBucket.set(bucket, local);
  return local;
}

/**
 * @param {R2BucketBinding} bucket
 * @param {S3Binding} s3
 * @param {unknown} userKey
 */
function objectUrl(bucket, s3, userKey) {
  const key = r2PhysicalKey(bucket.ctx.props, userKey);
  return `${s3.endpoint}/${s3.bucket}/${encodeS3KeyPath(key)}`;
}

/**
 * @param {R2BucketBinding} bucket
 * @param {S3Binding} s3
 * @param {string} key
 * @param {R2RequestMeta} [requestMeta]
 */
async function headRaw(bucket, s3, key, requestMeta = {}) {
  const res = await s3.client.fetch(objectUrl(bucket, s3, key), {
    method: "HEAD",
    headers: headersWithRequestId(requestMeta),
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`R2 HEAD failed with ${res.status}`);
  return metaFromHeaders(key, res.headers);
}

/**
 * @param {R2BucketBinding} bucket
 * @param {S3Binding} s3
 * @param {string[]} keys
 * @param {R2RequestMeta} [requestMeta]
 */
async function deleteBatch(bucket, s3, keys, requestMeta = {}) {
  const prefix = r2PhysicalPrefix(bucket.ctx.props);
  const body = [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<Delete>",
    ...keys.map((key) => {
      const physicalKey = `${prefix}${normalizeR2ObjectKey(key)}`;
      return `<Object><Key>${xmlEscape(physicalKey)}</Key></Object>`;
    }),
    "</Delete>",
  ].join("");
  const bodyBytes = utf8Encoder.encode(body);
  const headers = headersWithRequestId(requestMeta);
  headers.set("content-type", "application/xml");
  headers.set("x-amz-checksum-sha256", await sha256Base64(bodyBytes));
  const res = await fetchRetryableS3Post(s3.client, `${s3.endpoint}/${s3.bucket}?delete`, {
    method: "POST",
    headers,
    body,
  });
  if (!res.ok) {
    await discardResponseBody(res);
    throw new Error(`R2 DELETE failed with ${res.status}`);
  }
  const xml = await res.text();
  const errorBlocks = [...xml.matchAll(/<Error>([\s\S]*?)<\/Error>/g)];
  if (errorBlocks.length > 0) {
    const details = errorBlocks.slice(0, 5).map((m) => {
      const blk = m[1];
      const rawKey = xmlUnescape((/<Key>([^<]*)<\/Key>/.exec(blk) || [])[1] || "?");
      let k = "?";
      try {
        k = stripR2PhysicalPrefix(bucket.ctx.props, rawKey);
      } catch {}
      return k;
    }).join("; ");
    const shown = Math.min(5, errorBlocks.length);
    throw new Error(
      `R2 DELETE partial failure (${errorBlocks.length} errors, showing ${shown}): ${details}`
    );
  }
}

export class R2Bucket extends WorkerEntrypoint {
  /**
   * @param {string} key
   * @param {R2RequestMeta} [requestMeta]
   */
  async head(key, requestMeta = {}) {
    const bucket = r2Binding(this);
    return recordBindingOperation(serviceName(bucket), "r2", "head", async () => {
      return headRaw(bucket, s3ForBucket(bucket), key, requestMeta);
    });
  }

  /**
   * @param {string} key
   * @param {R2Options} [options]
   * @param {R2RequestMeta} [requestMeta]
   */
  async get(key, options = {}, requestMeta = {}) {
    const bucket = r2Binding(this);
    return recordBindingOperation(serviceName(bucket), "r2", "get", async () => {
      const s3 = s3ForBucket(bucket);
      const headers = headersWithRequestId(requestMeta);
      applyGetOptions(headers, options);
      const res = await s3.client.fetch(objectUrl(bucket, s3, key), { method: "GET", headers });
      if (res.status === 404) {
        await discardResponseBody(res);
        return null;
      }
      if (!res.ok && res.status !== 206 && res.status !== 304 && res.status !== 412) {
        await discardResponseBody(res);
        throw new Error(`R2 GET failed with ${res.status}`);
      }
      if (res.status === 304 || res.status === 412) {
        await discardResponseBody(res);
        return { meta: await headRaw(bucket, s3, key, requestMeta) || metaFromHeaders(key, res.headers) };
      }
      return {
        meta: metaFromHeaders(key, res.headers),
        body: res.body || new ReadableStream({ start(controller) { controller.close(); } }),
      };
    });
  }

  /**
   * @param {string} key
   * @param {unknown} value
   * @param {R2Options} [options]
   * @param {R2RequestMeta} [requestMeta]
   */
  async put(key, value, options = {}, requestMeta = {}) {
    const bucket = r2Binding(this);
    return recordBindingOperation(serviceName(bucket), "r2", "put", async () => {
      const body = putBodyFromUnknown(value);
      assertR2BufferSize(body.byteLength, "put");
      const s3 = s3ForBucket(bucket);
      const headers = headersWithRequestId(requestMeta);
      applyHttpMetadata(headers, options.httpMetadata);
      applyCustomMetadata(headers, options.customMetadata);
      if (options.storageClass) headers.set("x-amz-storage-class", String(options.storageClass));
      applyOnlyIf(headers, options.onlyIf);
      const res = await s3.client.fetch(objectUrl(bucket, s3, key), {
        method: "PUT",
        headers,
        body,
      });
      if (res.status === 412) {
        await discardResponseBody(res);
        return null;
      }
      if (!res.ok) {
        await discardResponseBody(res);
        throw new Error(`R2 PUT failed with ${res.status}`);
      }
      return metaFromPutResponse(key, res.headers, body.byteLength, options);
    });
  }

  /**
   * @param {string | string[]} keys
   * @param {R2RequestMeta} [requestMeta]
   */
  async delete(keys, requestMeta = {}) {
    const bucket = r2Binding(this);
    return recordBindingOperation(serviceName(bucket), "r2", "delete", async () => {
      const list = Array.isArray(keys) ? keys : [keys];
      const s3 = s3ForBucket(bucket);
      if (Array.isArray(keys)) {
        // CF accepts empty batch delete as a no-op; keep it explicit.
        if (list.length === 0) return;
        for (let i = 0; i < list.length; i += DELETE_OBJECTS_BATCH_SIZE) {
          await deleteBatch(bucket, s3, list.slice(i, i + DELETE_OBJECTS_BATCH_SIZE), requestMeta);
        }
        return;
      }
      for (const key of list) {
        const res = await s3.client.fetch(objectUrl(bucket, s3, key), {
          method: "DELETE",
          headers: headersWithRequestId(requestMeta),
        });
        if (!res.ok && res.status !== 404) {
          await discardResponseBody(res);
          throw new Error(`R2 DELETE failed with ${res.status}`);
        }
      }
    });
  }

  /**
   * @param {R2Options} [options]
   * @param {R2RequestMeta} [requestMeta]
   */
  async list(options = {}, requestMeta = {}) {
    const bucket = r2Binding(this);
    return recordBindingOperation(serviceName(bucket), "r2", "list", async () => {
      const s3 = s3ForBucket(bucket);
      const prefix = r2PhysicalPrefix(bucket.ctx.props);
      const listPrefix = options.prefix ? normalizeR2ObjectKey(options.prefix) : "";
      const startAfter = options.startAfter ? normalizeR2ObjectKey(options.startAfter) : "";
      const query = encodeS3Query({
        "list-type": "2",
        prefix: `${prefix}${listPrefix}`,
        delimiter: options.delimiter,
        "continuation-token": options.cursor,
        "start-after": startAfter ? `${prefix}${startAfter}` : undefined,
        "max-keys": options.limit == null ? undefined : String(normalizeR2ListLimit(options.limit)),
      });
      const res = await s3.client.fetch(`${s3.endpoint}/${s3.bucket}?${query}`, {
        method: "GET",
        headers: headersWithRequestId(requestMeta),
      });
      if (!res.ok) {
        await discardResponseBody(res);
        throw new Error(`R2 LIST failed with ${res.status}`);
      }
      const xml = await res.text();
      const listed = parseListObjects(xml, bucket.ctx.props);
      const include = new Set(Array.isArray(options.include) ? options.include : []);
      if (include.has("httpMetadata") || include.has("customMetadata")) {
        listed.objects = await mapWithConcurrency(
          listed.objects,
          LIST_INCLUDE_HEAD_CONCURRENCY,
          async (meta) => {
            const head = await headRaw(bucket, s3, meta.key, requestMeta);
            if (!head) return meta;
            return {
              ...meta,
              ...(include.has("httpMetadata") ? { httpMetadata: head.httpMetadata } : {}),
              ...(include.has("customMetadata") ? { customMetadata: head.customMetadata } : {}),
            };
          }
        );
      }
      return listed;
    });
  }
}
