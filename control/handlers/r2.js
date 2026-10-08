import {
  deleteR2Object,
  getR2Object,
  headR2Object,
  listR2Buckets,
  listR2Objects,
} from "control-r2";
import { errorMessage, getControlR2, jsonError, jsonResponse, requireControlLog } from "control-shared";

/**
 * @param {string} value
 * @param {string} label
 */
function decodePathValue(value, label) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new Error(`invalid percent-encoding in ${label}`);
  }
}

/** @param {{ subPath: string[] }} requestParts */
function objectKeyFromRequest({ subPath }) {
  if (subPath.length === 0) return "";
  return decodePathValue(subPath.join("/"), "R2 object key");
}

/** @param {Headers} headers */
function copyObjectHeaders(headers) {
  const out = new Headers();
  for (const name of [
    "content-type",
    "content-language",
    "content-disposition",
    "content-encoding",
    "cache-control",
    "expires",
    "etag",
    "last-modified",
    "content-length",
  ]) {
    const value = headers.get(name);
    if (value) out.set(name, value);
  }
  for (const [name, value] of headers) {
    if (name.startsWith("x-amz-meta-")) out.set(name, value);
  }
  return out;
}

/** @param {{ method: string, url: URL, ns: string, subPath: string[], requestId: string, ctx?: import("control-r2").R2RequestContext | null }} args */
export async function handle({ method, url, ns, subPath, requestId, ctx }) {
  try {
    return await handleInner({ method, url, ns, subPath, requestId, ctx });
  } catch (err) {
    const message = errorMessage(err);
    if (err instanceof TypeError ||
        /^invalid percent-encoding /.test(message) ||
        /r2 bucket_name must match/.test(message)) {
      return jsonError(400, "invalid_request", message);
    }
    throw err;
  }
}

/** @param {{ method: string, url: URL, ns: string, subPath: string[], requestId: string, ctx?: import("control-r2").R2RequestContext | null }} args */
async function handleInner({ method, url, ns, subPath, requestId, ctx }) {
  const r2 = getControlR2();
  const log = requireControlLog();
  if (!r2) {
    return jsonError(503, "r2_not_configured", "R2 admin API is not configured");
  }

  if (method === "GET" && subPath.length === 1 && subPath[0] === "buckets") {
    const body = await listR2Buckets({
      r2,
      ns,
      cursor: url.searchParams.get("cursor") || undefined,
      limit: url.searchParams.get("limit") || undefined,
      requestId,
    });
    log("info", "r2_buckets_listed", {
      request_id: requestId,
      namespace: ns,
      count: body.buckets.length,
      truncated: body.truncated,
    });
    return jsonResponse(200, body);
  }

  if (subPath.length >= 3 && subPath[0] === "buckets" && subPath[2] === "objects") {
    const bucketName = decodePathValue(subPath[1], "R2 bucket name");
    if (method === "GET" && subPath.length === 3) {
      const body = await listR2Objects({
        r2,
        ns,
        bucketName,
        prefix: url.searchParams.get("prefix") || "",
        delimiter: url.searchParams.get("delimiter") || undefined,
        cursor: url.searchParams.get("cursor") || undefined,
        limit: url.searchParams.get("limit") || undefined,
        requestId,
      });
      log("info", "r2_objects_listed", {
        request_id: requestId,
        namespace: ns,
        bucket: bucketName,
        count: body.objects.length,
        truncated: body.truncated,
      });
      return jsonResponse(200, body);
    }

    if (subPath.length === 3) {
      return jsonError(405, "method_not_allowed", "Method not allowed");
    }

    if (method === "HEAD") {
      const key = objectKeyFromRequest({ subPath: subPath.slice(3) });
      const res = await headR2Object({ r2, ns, bucketName, key, requestId });
      if (!res) return new Response(null, { status: 404 });
      log("info", "r2_object_head", {
        request_id: requestId,
        namespace: ns,
        bucket: bucketName,
        key,
      });
      return new Response(null, {
        status: 200,
        headers: copyObjectHeaders(res.headers),
      });
    }

    if (method === "GET") {
      const key = objectKeyFromRequest({ subPath: subPath.slice(3) });
      const res = await getR2Object({ r2, ns, bucketName, key, requestId, ctx });
      if (!res) return jsonError(404, "r2_object_not_found", "R2 object not found");
      log("info", "r2_object_read", {
        request_id: requestId,
        namespace: ns,
        bucket: bucketName,
        key,
      });
      return new Response(res.body, {
        status: 200,
        headers: copyObjectHeaders(res.headers),
      });
    }

    if (method === "DELETE") {
      const key = objectKeyFromRequest({ subPath: subPath.slice(3) });
      const body = await deleteR2Object({ r2, ns, bucketName, key, requestId });
      log("info", "r2_object_deleted", {
        request_id: requestId,
        namespace: ns,
        bucket: bucketName,
        key,
        status: body.status,
      });
      return jsonResponse(200, body);
    }
  }

  return jsonError(404, "not_found", "Not found");
}
