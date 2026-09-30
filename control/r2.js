import { SigV4Client } from "@wdl-dev/aws-sigv4";
import { discardResponseBody } from "shared-respond";
import { S3_TRANSIENT_RETRIES } from "shared-s3-retry";
import { S3_ERROR_BODY_MAX_BYTES, streamS3Response, withS3Request } from "shared-s3-request";
import { readBoundedText } from "shared-bounded-body";
import {
  encodeS3KeyPath,
  encodeS3Query,
  normalizeR2ListLimit,
  normalizeR2ObjectKey,
  r2PhysicalKey,
  r2PhysicalPrefix,
  stripR2PhysicalPrefix,
  validateR2BucketName,
} from "runtime-r2-utils";
import { collectXmlFields, listXmlTagValues, xmlTagValueIsTrue } from "shared-s3-xml";

const DEFAULT_LIST_LIMIT = 1000;

/**
 * @typedef {{ client: SigV4Client, endpoint: string, bucket: string }} R2Admin
 * @typedef {{ ns: string, bucketName: string }} R2ObjectScope
 * @typedef {{ waitUntil(promise: Promise<unknown>): void }} R2RequestContext
 */

/** @param {string} etag */
function stripEtag(etag) {
  if (!etag) return "";
  return etag.startsWith('"') && etag.endsWith('"') ? etag.slice(1, -1) : etag;
}

/** @param {Record<string, string | undefined>} env @param {string} key */
function requireR2Env(env, key) {
  if (!env[key]) throw new Error(`R2 admin API requires ${key}`);
  return env[key];
}

/** @param {Record<string, string | undefined>} env */
export function makeR2AdminClient(env) {
  const endpoint = env.R2_S3_ENDPOINT;
  const bucket = env.R2_S3_BUCKET;
  if (!endpoint || !bucket) return null;
  const client = new SigV4Client({
    accessKeyId: requireR2Env(env, "R2_S3_ACCESS_KEY_ID"),
    secretAccessKey: requireR2Env(env, "R2_S3_SECRET_ACCESS_KEY"),
    service: "s3",
    region: env.R2_S3_REGION || "us-east-1",
    retries: S3_TRANSIENT_RETRIES,
  });
  return { client, endpoint: endpoint.replace(/\/+$/, ""), bucket };
}

/** @param {R2Admin} r2 @param {R2ObjectScope} props @param {string} key */
function r2ObjectUrl(r2, props, key) {
  const physicalKey = r2PhysicalKey(props, key);
  return `${r2.endpoint}/${r2.bucket}/${encodeS3KeyPath(physicalKey)}`;
}

/** @param {string | undefined} requestId */
function requestHeaders(requestId) {
  const headers = new Headers();
  if (requestId) headers.set("x-request-id", String(requestId));
  return headers;
}

/**
 * @param {{ r2: R2Admin, ns: string, bucketName: string, key: string, requestId?: string, ctx?: R2RequestContext | null, method: "GET" | "HEAD" | "DELETE", notFound?: "error" | "null" | "ok" }} args
 * @returns {Promise<Response | null>}
 */
async function fetchR2AdminObject({
  r2,
  ns,
  bucketName,
  key,
  requestId,
  method,
  ctx,
  notFound = "error",
}) {
  return withS3Request(async (aborter) => {
    const res = await r2.client.fetch(r2ObjectUrl(r2, { ns, bucketName }, key), {
      method,
      headers: requestHeaders(requestId),
      signal: aborter.signal,
    });
    if (res.status === 404 && notFound !== "error") {
      await discardResponseBody(res);
      return notFound === "null" ? null : res;
    }
    if (!res.ok) {
      const detail = await readBoundedText(res, S3_ERROR_BODY_MAX_BYTES, aborter.signal).catch(async () => {
        await discardResponseBody(res);
        aborter.signal.throwIfAborted();
        return "";
      });
      throw new Error(`R2 admin ${method} failed with ${res.status}: ${detail.slice(0, 200)}`);
    }
    if (method === "GET" && res.body) {
      return new Response(streamS3Response(res, aborter, ctx), res);
    }
    await discardResponseBody(res);
    return res;
  });
}

/** @param {unknown} value */
function limitFrom(value) {
  if (value == null || value === "") return DEFAULT_LIST_LIMIT;
  return normalizeR2ListLimit(value) ?? DEFAULT_LIST_LIMIT;
}

/** @param {string} value */
function isoFromS3Date(value) {
  if (!value) return "";
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? new Date(ms).toISOString() : "";
}

const OBJECT_LIST_TAGS = new Set(["Key", "Size", "ETag", "LastModified", "StorageClass"]);
const PREFIX_LIST_TAGS = new Set(["Prefix"]);

/** @param {string} xml @param {R2ObjectScope} props */
function parseObjectList(xml, props) {
  const objects = [];
  for (const block of xml.matchAll(/<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?Contents)>([\s\S]*?)<\/\1>/g)) {
    const fields = collectXmlFields(block[2], OBJECT_LIST_TAGS);
    const physicalKey = fields.Key || "";
    if (!physicalKey) continue;
    const size = fields.Size ?? "0";
    objects.push({
      key: stripR2PhysicalPrefix(props, physicalKey),
      size: Number(size),
      etag: stripEtag(fields.ETag || ""),
      uploaded: isoFromS3Date(fields.LastModified || ""),
      version: "",
      storageClass: fields.StorageClass || "Standard",
    });
  }
  const cursor = listXmlTagValues(xml, "NextContinuationToken")[0];
  const prefix = r2PhysicalPrefix(props);
  const delimitedPrefixes = [...xml.matchAll(/<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?CommonPrefixes)>([\s\S]*?)<\/\1>/g)]
    .map((block) => collectXmlFields(block[2], PREFIX_LIST_TAGS).Prefix || "")
    .filter((p) => p.startsWith(prefix))
    .map((p) => stripR2PhysicalPrefix(props, p));
  return {
    objects,
    truncated: xmlTagValueIsTrue(xml, "IsTruncated"),
    ...(cursor ? { cursor } : {}),
    delimitedPrefixes,
  };
}

/** @param {string} xml @param {string} ns */
function parseBucketList(xml, ns) {
  const nsPrefix = `r2/${ns}/`;
  const buckets = [...xml.matchAll(/<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?CommonPrefixes)>([\s\S]*?)<\/\1>/g)]
    .map((block) => collectXmlFields(block[2], PREFIX_LIST_TAGS).Prefix || "")
    .filter((prefix) => prefix.startsWith(nsPrefix))
    .map((prefix) => prefix.slice(nsPrefix.length).replace(/\/$/, ""))
    .filter(Boolean)
    .toSorted();
  const cursor = listXmlTagValues(xml, "NextContinuationToken")[0];
  return {
    buckets: [...new Set(buckets)].map((name) => ({ name })),
    truncated: xmlTagValueIsTrue(xml, "IsTruncated"),
    ...(cursor ? { cursor } : {}),
  };
}

/** @param {R2Admin} r2 @param {{ prefix: string, delimiter?: string, cursor?: string, limit?: unknown, requestId?: string, signal: AbortSignal }} options */
async function listS3(r2, { prefix, delimiter, cursor, limit, requestId, signal }) {
  const query = encodeS3Query({
    "list-type": "2",
    prefix,
    delimiter,
    "continuation-token": cursor,
    "max-keys": String(limitFrom(limit)),
  });
  const res = await r2.client.fetch(`${r2.endpoint}/${r2.bucket}?${query}`, {
    method: "GET",
    headers: requestHeaders(requestId),
    signal,
  });
  if (!res.ok) {
    const detail = await readBoundedText(res, S3_ERROR_BODY_MAX_BYTES, signal).catch(async () => {
      await discardResponseBody(res);
      signal.throwIfAborted();
      return "";
    });
    throw new Error(`R2 admin LIST failed with ${res.status}: ${detail.slice(0, 200)}`);
  }
  return readBoundedText(res, Infinity, signal);
}

/** @param {{ r2: R2Admin, ns: string, cursor?: string, limit?: unknown, requestId?: string }} args */
export async function listR2Buckets({ r2, ns, cursor, limit, requestId }) {
  return withS3Request(async (aborter) => {
    const xml = await listS3(r2, {
      prefix: `r2/${ns}/`,
      delimiter: "/",
      cursor,
      limit,
      requestId,
      signal: aborter.signal,
    });
    return { namespace: ns, ...parseBucketList(xml, ns) };
  });
}

/** @param {{ r2: R2Admin, ns: string, bucketName: string, prefix?: string, delimiter?: string, cursor?: string, limit?: unknown, requestId?: string }} args */
export async function listR2Objects({
  r2,
  ns,
  bucketName,
  prefix = "",
  delimiter,
  cursor,
  limit,
  requestId,
}) {
  return withS3Request(async (aborter) => {
    validateR2BucketName(bucketName);
    const normalizedPrefix = prefix ? normalizeR2ObjectKey(prefix) : "";
    const props = { ns, bucketName };
    const xml = await listS3(r2, {
      prefix: `${r2PhysicalPrefix(props)}${normalizedPrefix}`,
      delimiter,
      cursor,
      limit,
      requestId,
      signal: aborter.signal,
    });
    return {
      namespace: ns,
      bucket: bucketName,
      prefix: normalizedPrefix,
      ...parseObjectList(xml, props),
    };
  });
}

/** @param {{ r2: R2Admin, ns: string, bucketName: string, key: string, requestId?: string, ctx?: R2RequestContext | null }} args */
export async function getR2Object({ r2, ns, bucketName, key, requestId, ctx }) {
  return fetchR2AdminObject({
    r2,
    ns,
    bucketName,
    key,
    requestId,
    method: "GET",
    ctx,
    notFound: "null",
  });
}

/** @param {{ r2: R2Admin, ns: string, bucketName: string, key: string, requestId?: string }} args */
export async function headR2Object({ r2, ns, bucketName, key, requestId }) {
  return fetchR2AdminObject({
    r2,
    ns,
    bucketName,
    key,
    requestId,
    method: "HEAD",
    notFound: "null",
  });
}

/** @param {{ r2: R2Admin, ns: string, bucketName: string, key: string, requestId?: string }} args */
export async function deleteR2Object({ r2, ns, bucketName, key, requestId }) {
  const res = await fetchR2AdminObject({
    r2,
    ns,
    bucketName,
    key,
    requestId,
    method: "DELETE",
    notFound: "ok",
  });
  if (!res) throw new Error("R2 admin DELETE unexpectedly returned no response");
  await discardResponseBody(res);
  return { namespace: ns, bucket: bucketName, key, status: "ok" };
}
