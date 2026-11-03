import http from "node:http";

import { GATEWAY_HOST, GATEWAY_PORT, ADMIN_HOST_HEADER, ADMIN_TOKEN } from "./env.js";
import { bufferedResponseBody, responseJson, responseJsonOrNull } from "./http-response.js";

/**
 * @typedef {{
 *   status: number,
 *   ok: boolean,
 *   headers: import("node:http").IncomingHttpHeaders,
 *   text: () => Promise<string>,
 *   json: () => Promise<any>,
 *   jsonOrNull: () => Promise<any>,
 *   arrayBuffer: () => Promise<ArrayBuffer>,
 * }} AdminResponse
 */

// Admin-API transport. undici fetch() rewrites Host; gateway's
// admin-host short-circuit reads url.hostname, so we set Host ourselves.
/**
 * @param {string} pathSuffix
 * @param {{ method?: string, headers?: Record<string, string>, body?: string | Buffer | Record<string, unknown> }} [init]
 * @returns {Promise<AdminResponse>}
 */
export async function adminFetch(pathSuffix, init = {}) {
  const method = init.method || "GET";
  /** @type {Record<string, string>} */
  const headers = {
    Host: ADMIN_HOST_HEADER,
    "x-admin-token": ADMIN_TOKEN,
    ...(init.headers || {}),
  };
  const body = init.body;
  if (body && !("content-type" in headers) && !("Content-Type" in headers)) {
    headers["content-type"] = "application/json";
  }

  /** @type {import("node:http").IncomingMessage} */
  const res = await new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method,
        path: pathSuffix,
        headers,
        agent: false,
      },
      resolve
    );
    req.on("error", reject);
    if (body) req.write(typeof body === "string" ? body : JSON.stringify(body));
    req.end();
  });
  /** @type {Buffer[]} */
  const chunks = [];
  for await (const chunk of res) chunks.push(Buffer.from(chunk));
  const responseBody = bufferedResponseBody(Buffer.concat(chunks));
  const status = res.statusCode ?? 0;
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: res.headers,
    text: responseBody.text,
    json: async () => responseJson(responseBody, "admin response body"),
    jsonOrNull: async () => responseJsonOrNull(responseBody, "admin response body"),
    arrayBuffer: responseBody.arrayBuffer,
  };
}

/** @param {string} pathSuffix @param {unknown} body */
export async function adminPost(pathSuffix, body) {
  const res = await adminFetch(pathSuffix, { method: "POST", body: JSON.stringify(body) });
  const json = await res.jsonOrNull();
  return { status: res.status, ok: res.ok, json };
}

/** @param {string} pathSuffix @param {unknown} body */
export async function adminPut(pathSuffix, body) {
  const res = await adminFetch(pathSuffix, { method: "PUT", body: JSON.stringify(body) });
  const json = await res.jsonOrNull();
  return { status: res.status, ok: res.ok, json };
}

/** @param {string} pathSuffix */
export async function adminGet(pathSuffix) {
  const res = await adminFetch(pathSuffix, { method: "GET" });
  const json = await res.jsonOrNull().catch(() => null);
  return { status: res.status, ok: res.ok, json };
}

// Every adminFetch opens a fresh socket; this export exists for
// call-sites that want to signal "no pooling" at their use.
/** @param {string} pathSuffix */
export function adminGetFresh(pathSuffix) {
  return adminGet(pathSuffix);
}

/** @param {string} ns @param {string} name @param {unknown} body */
export async function deployAndPromote(ns, name, body) {
  const d = await adminPost(`/ns/${ns}/worker/${name}/deploy`, body);
  if (!d.ok) throw new Error(`deploy failed: ${d.status} ${JSON.stringify(d.json)}`);
  const p = await adminPost(`/ns/${ns}/worker/${name}/promote`, { version: d.json.version });
  if (!p.ok) throw new Error(`promote failed: ${p.status} ${JSON.stringify(p.json)}`);
  return d.json.version;
}

// Like adminFetch but with an explicit token parameter so auth tests can
// exercise non-admin / bootstrap / expired token scenarios.
// Returns a simplified { status, text, json } shape matching the inline
// versions it replaces — auth tests access .json as a property, not .json().
/**
 * @param {string | null} token
 * @param {string} pathSuffix
 * @param {{ method?: string, headers?: Record<string, string>, body?: string | Buffer | Record<string, unknown> }} [init]
 * @returns {Promise<{ status: number | undefined, text: string, json: any }>}
 */
export async function fetchWithToken(token, pathSuffix, init = {}) {
  const method = init.method || "GET";
  /** @type {Record<string, string>} */
  const headers = {
    Host: ADMIN_HOST_HEADER,
    ...(init.headers || {}),
  };
  if (token != null) headers["x-admin-token"] = token;
  const body = init.body;
  if (body && !("content-type" in headers) && !("Content-Type" in headers)) {
    headers["content-type"] = "application/json";
  }

  /** @type {import("node:http").IncomingMessage} */
  const res = await new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method,
        path: pathSuffix,
        headers,
        agent: false,
      },
      resolve
    );
    req.on("error", reject);
    if (body) req.write(typeof body === "string" ? body : JSON.stringify(body));
    req.end();
  });
  /** @type {Buffer[]} */
  const chunks = [];
  for await (const chunk of res) chunks.push(Buffer.from(chunk));
  const text = Buffer.concat(chunks).toString("utf8");
  let json = null;
  try { json = responseJsonOrNull({ body: text }, "token-auth admin response body"); } catch { /* leave null */ }
  return { status: res.statusCode, text, json };
}
