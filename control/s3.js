// PutObject-only S3 helper. Bucket creation is out-of-band (terraform /
// s3mock boot-time initial buckets) — keeps the S3 IAM surface minimal.

import { SigV4Client } from "@wdl-dev/aws-sigv4";
import { encodeS3KeyPath } from "runtime-r2-utils";
import { S3_TRANSIENT_RETRIES } from "shared-s3-retry";

const TYPE_BY_EXT = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
  ".txt": "text/plain; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
  ".xml": "application/xml",
  ".wasm": "application/wasm",
  ".map": "application/json",
};
/**
 * @typedef {{ client: SigV4Client, endpoint: string, bucket: string }} S3Client
 */

/** @param {string} filePath */
export function inferContentType(filePath) {
  const dot = filePath.lastIndexOf(".");
  if (dot < 0) return "application/octet-stream";
  const ext = /** @type {keyof typeof TYPE_BY_EXT} */ (filePath.slice(dot).toLowerCase());
  return TYPE_BY_EXT[ext] || "application/octet-stream";
}

// Returns null when S3 isn't configured — worker-only deploys still
// work; deploys carrying `assets` error clearly at the call site.
/** @param {Record<string, string | undefined>} env */
export function makeS3Client(env) {
  const endpoint = env.S3_ENDPOINT;
  const bucket = env.S3_BUCKET;
  if (!endpoint || !bucket) return null;
  const accessKeyId = env.S3_ACCESS_KEY_ID;
  const secretAccessKey = env.S3_SECRET_ACCESS_KEY;
  const explicitLocal = env.S3_ALLOW_TEST_CREDENTIALS === "1" || env.S3_ALLOW_TEST_CREDENTIALS === "true";
  const localEndpoint = /^https?:\/\/(?:localhost|127\.0\.0\.1|s3mock)(?::\d+)?(?:\/|$)/.test(endpoint);
  const allowTestCredentials = explicitLocal || localEndpoint;
  if ((!accessKeyId || !secretAccessKey) && !allowTestCredentials) {
    throw new Error("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required when S3_ENDPOINT/S3_BUCKET are configured");
  }
  const client = new SigV4Client({
    accessKeyId: accessKeyId || "test",
    secretAccessKey: secretAccessKey || "test",
    service: "s3",
    region: env.S3_REGION || "us-east-1",
    retries: S3_TRANSIENT_RETRIES,
  });
  return { client, endpoint: endpoint.replace(/\/+$/, ""), bucket };
}

/** @param {S3Client} s3 @param {string} key @param {BodyInit | Uint8Array} body @param {string} contentType */
export async function putAsset(s3, key, body, contentType) {
  const url = `${s3.endpoint}/${s3.bucket}/${encodeS3KeyPath(key)}`;
  const res = await s3.client.fetch(url, {
    method: "PUT",
    body: /** @type {BodyInit} */ (body),
    headers: { "content-type": contentType },
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`S3 PUT ${key} → ${res.status} ${detail.slice(0, 200)}`);
  }
}
