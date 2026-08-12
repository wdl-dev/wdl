import http from "node:http";
import { formatWorkerId } from "../../../shared/worker-id.js";

import { GATEWAY_HOST, GATEWAY_PORT } from "./env.js";
import { bufferedResponseBody, responseJson } from "./http-response.js";

export function gatewayUrl(pathWithQuery = "/") {
  return `http://${GATEWAY_HOST}:${GATEWAY_PORT}${pathWithQuery}`;
}

/** @param {string} namespace @param {string} worker @param {string} version */
export function gatewayWorkerId(namespace, worker, version) {
  return formatWorkerId({ namespace, worker, version });
}

// Unique suffix per node process: workerd's workerLoader caches failed
// loads by worker id, so if a prior test run tried to load ns:name:v1 and
// the bundle wasn't there (race / interrupt), the failure sticks until the
// runtime isolate restarts. Fresh suffix per run = fresh ids = no poisoning.
const RUN_SUFFIX = Date.now().toString(36);
let nsCounter = 0;
/** @param {string} prefix */
export function uniqueNs(prefix) {
  nsCounter++;
  return `${prefix}-${RUN_SUFFIX}-${nsCounter}`.toLowerCase();
}

// agent:false — pooled sockets FIN'd mid-idle surface as "socket hang
// up" on the next reuse.
/**
 * @typedef {{
 *   status: number | undefined,
 *   headers: { get: (name: string) => string | string[] | null },
 *   text: () => Promise<string>,
 *   arrayBuffer: () => Promise<ArrayBuffer>,
 * }} RawHttpResponse
 */

/**
 * @param {string} url
 * @returns {Promise<RawHttpResponse>}
 */
export function rawHttpGet(url) {
  const u = new URL(url);
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: u.hostname,
        port: Number(u.port) || (u.protocol === "https:" ? 443 : 80),
        method: "GET",
        path: u.pathname + u.search,
        agent: false,
      },
      (res) => {
        /** @type {Buffer[]} */
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          const body = bufferedResponseBody(buf);
          resolve({
            status: res.statusCode,
            headers: { get: (/** @type {string} */ name) => res.headers[name.toLowerCase()] ?? null },
            text: body.text,
            arrayBuffer: body.arrayBuffer,
          });
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

// undici fetch() rewrites Host; use plain http so gateway sees what we set.
/**
 * @typedef {{
 *   status: number | undefined,
 *   headers: import("node:http").IncomingHttpHeaders,
 *   arrayBuffer: () => Promise<ArrayBuffer>,
 *   text: () => Promise<string>,
 *   json: () => Promise<any>,
 * }} GatewayResponse
 */

/**
 * @typedef {{
 *   status: number | undefined,
 *   headers: import("node:http").IncomingHttpHeaders,
 *   body: import("node:http").IncomingMessage,
 * }} GatewayStreamResponse
 */

/**
 * @param {string} ns
 * @param {string} p
 * @param {{ method?: string, headers?: Record<string, string>, body?: string | Buffer }} [init]
 * @returns {Promise<GatewayResponse>}
 */
export function gatewayFetch(ns, p, init = {}) {
  const method = init.method || "GET";
  const headers = { Host: `${ns}.workers.local`, ...(init.headers || {}) };
  const body = init.body;

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method,
        path: p,
        headers,
        agent: false,
      },
      (res) => {
        /** @type {Buffer[]} */
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          const body = bufferedResponseBody(buf);
          resolve({
            status: res.statusCode,
            headers: res.headers,
            arrayBuffer: body.arrayBuffer,
            text: body.text,
            json: async () => responseJson(body, "gateway response body"),
          });
        });
      }
    );
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

// Return on response headers so tests can observe a body that intentionally
// remains open. Callers own and must destroy the returned body.
/**
 * @param {string} ns
 * @param {string} p
 * @param {{ method?: string, headers?: Record<string, string>, body?: string | Buffer }} [init]
 * @returns {Promise<GatewayStreamResponse>}
 */
export function gatewayStream(ns, p, init = {}) {
  const method = init.method || "GET";
  const headers = { Host: `${ns}.workers.local`, ...(init.headers || {}) };
  const body = init.body;

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: GATEWAY_HOST,
        port: GATEWAY_PORT,
        method,
        path: p,
        headers,
        agent: false,
      },
      (res) => resolve({ status: res.statusCode, headers: res.headers, body: res })
    );
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

// Same raw-http reason as gatewayFetch — keep Host intact.
/** @param {string} host @param {string} [p] */
export function hostFetch(host, p = "/") {
  return localHttpGet(GATEWAY_HOST, GATEWAY_PORT, p, { Host: host });
}

/**
 * @typedef {{
 *   status: number | undefined,
 *   text: () => Promise<string>,
 *   json: () => Promise<any>,
 *   headers: import("node:http").IncomingHttpHeaders,
 * }} LocalHttpResponse
 */

/**
 * @param {string} host @param {number} port @param {string} path
 * @param {Record<string, string>} [headers]
 * @returns {Promise<LocalHttpResponse>}
 */
function localHttpGet(host, port, path, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host,
        port,
        method: "GET",
        path,
        headers,
        agent: false,
      },
      (res) => {
        /** @type {Buffer[]} */
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const body = bufferedResponseBody(Buffer.concat(chunks));
          resolve({
            status: res.statusCode,
            text: body.text,
            json: async () => responseJson(body, "local HTTP response body"),
            headers: res.headers,
          });
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}
