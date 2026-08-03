import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { adminPost, deployAndPromote } from "./admin-http.js";
import { composeProfileUp, composeUp } from "./compose.js";
import { ensureD1SingleRuntime, recreateD1MultiRuntimes } from "./runtimes.js";
import { gatewayFetch } from "./gateway-http.js";
import {
  serviceInternalGet,
  serviceInternalPost,
  serviceInternalPostAsync,
} from "./internal-http.js";
import { sh } from "./cli.js";
import { loadD1Protocol, loadD1QueryWire } from "../../helpers/load-d1-protocol.js";
import { responseJson } from "./http-response.js";
import { assertStatusIn } from "./assertions.js";
import { redisSet, redisSetJson } from "./redis.js";

export const { dbKeyOf, slotOf } = await loadD1Protocol();
export const {
  D1_QUERY_CONTENT_TYPE,
  decodeD1QueryResponse,
  encodeD1QueryRequest,
} = await loadD1QueryWire();
export const D1_COMPAT_WORKER = readFileSync(
  new URL("../../../test-workers/d1-compat/src/index.js", import.meta.url),
  "utf8"
);

export const D1_HARNESS = readFileSync(
  new URL("../../../test-workers/d1-harness/src/index.js", import.meta.url),
  "utf8"
);

export const D1_NAMED_TARGET = readFileSync(
  new URL("../../../test-workers/d1-named-target/src/index.js", import.meta.url),
  "utf8"
);

export const D1_NAMED_CALLER = readFileSync(
  new URL("../../../test-workers/d1-named-caller/src/index.js", import.meta.url),
  "utf8"
);

/** @param {string} ns @param {string} worker @param {string} [databaseId] */
export async function setup(ns, worker, databaseId = "shared-main") {
  const created = await adminPost(`/ns/${ns}/d1/databases`, {
    databaseName: databaseId,
  });
  assertStatusIn(created, [201, 409], "D1 database create");
  await deployAndPromote(ns, worker, {
    mainModule: "worker.js",
    modules: { "worker.js": D1_HARNESS },
    bindings: { DB: { type: "d1", databaseId } },
  });
}

/** @param {string} ns @param {string} worker @param {Record<string, string>} params */
export async function call(ns, worker, params) {
  const qs = new URLSearchParams(params).toString();
  const res = await gatewayFetch(ns, `/${worker}?${qs}`);
  const text = await res.text();
  assert.equal(res.status, 200, text);
  return responseJson({ body: text });
}

/** @typedef {Parameters<typeof encodeD1QueryRequest>[0]} D1QueryRequestInput */

/** @param {string} service @param {D1QueryRequestInput} body */
export function d1RuntimeQuery(service, body) {
  const res = serviceInternalPost(
    service,
    8787,
    "/internal/d1/query",
    encodeD1QueryRequest(body),
    { "content-type": D1_QUERY_CONTENT_TYPE }
  );
  return {
    status: res.status,
    body: normalizeD1QueryBody(decodeD1QueryResponse(Buffer.from(res.bodyB64, "base64"))),
  };
}

/** @param {string} service @param {D1QueryRequestInput} body */
export function d1RuntimeQueryRaw(service, body) {
  const res = serviceInternalPost(
    service,
    8787,
    "/internal/d1/query",
    encodeD1QueryRequest(body),
    { "content-type": D1_QUERY_CONTENT_TYPE }
  );
  return {
    status: res.status,
    body: decodeD1QueryResponse(Buffer.from(res.bodyB64, "base64")),
  };
}

/** @param {string} service @param {D1QueryRequestInput} body */
export async function d1RuntimeTestHookQueryAsync(service, body) {
  const res = await serviceInternalPostAsync(service, 8787, "/internal/d1/test-hook/query", body);
  return {
    status: res.status,
    body: decodeD1QueryResponse(Buffer.from(res.bodyB64, "base64")),
  };
}

/** @param {string} service */
export function d1RuntimeDrain(service) {
  const res = serviceInternalPost(service, 8787, "/internal/d1/drain", {});
  return {
    status: res.status,
    body: responseJson(res),
  };
}

/** @param {string} service */
export async function d1RuntimeDrainAsync(service) {
  const res = await serviceInternalPostAsync(service, 8787, "/internal/d1/drain", {});
  return {
    status: res.status,
    body: responseJson(res),
  };
}

/** @param {string} service @param {D1QueryRequestInput} body */
export async function d1RuntimeQueryAsync(service, body) {
  const res = await serviceInternalPostAsync(
    service,
    8787,
    "/internal/d1/query",
    encodeD1QueryRequest(body),
    { "content-type": D1_QUERY_CONTENT_TYPE }
  );
  return {
    status: res.status,
    body: normalizeD1QueryBody(decodeD1QueryResponse(Buffer.from(res.bodyB64, "base64"))),
  };
}

/**
 * @template T
 * @param {unknown} value
 * @returns {T}
 */
export function normalizeD1QueryBody(value) {
  if (Array.isArray(value)) return /** @type {T} */ (value.map(normalizeD1QueryBody));
  if (!value || typeof value !== "object" || !("results" in value) || Array.isArray(value.results)) {
    return /** @type {T} */ (value);
  }
  const { columns, rows } = /** @type {{ columns?: unknown, rows?: unknown }} */ (value.results || {});
  if (!Array.isArray(columns) || !Array.isArray(rows)) return /** @type {T} */ (value);
  return /** @type {T} */ ({
    ...value,
    results: rows.map((/** @type {unknown[]} */ row) => Object.fromEntries(columns.map((/** @type {string} */ column, /** @type {number} */ idx) => [column, row[idx]]))),
  });
}

/** @param {string} service @param {Record<string, unknown>} body */
export function d1RuntimeRebalance(service, body) {
  const res = serviceInternalPost(service, 8787, "/internal/d1/rebalance", body);
  return {
    status: res.status,
    body: responseJson(res),
  };
}

/** @param {string} service @param {string} dbKey */
export function d1RuntimeProbe(service, dbKey) {
  const res = serviceInternalGet(service, 8787, `/internal/d1/probe?dbKey=${encodeURIComponent(dbKey)}`);
  assert.equal(res.status, 200, res.body);
  return responseJson(res);
}

/** @param {string} service @param {string} dbKey */
export function d1RuntimeProbeRaw(service, dbKey) {
  const res = serviceInternalGet(service, 8787, `/internal/d1/probe?dbKey=${encodeURIComponent(dbKey)}`);
  return {
    status: res.status,
    body: responseJson(res),
  };
}

export const D1_LEASE_WAIT_MS = 7_000;

/** @param {string} ns @param {string} firstId */
export function databaseIdOnSameSlot(ns, firstId) {
  const firstSlot = slotOf(ns, firstId);
  for (let idx = 0; idx < 50_000; idx += 1) {
    const candidate = `same-${idx}`;
    if (candidate !== firstId && slotOf(ns, candidate) === firstSlot) return candidate;
  }
  throw new Error("failed to find database id on the same D1 slot");
}

/** @param {{ ownerLeaseGuardMs?: number }} [options] */
export function recreateD1MultiTasks(options = {}) {
  recreateD1MultiRuntimes(options);
}

export function restoreD1MultiTasks() {
  recreateD1MultiRuntimes();
}

export function startD1EnvoyOwnerPair() {
  return composeProfileUp(
    "d1-multi",
    ["--force-recreate", "--wait", "d1-runtime-a"],
    { stdio: "pipe" }
  );
}

export function stopD1EnvoyOwnerPair() {
  return sh(["docker", "compose", "rm", "-sf", "d1-runtime-a"], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "d1-multi" },
  });
}

export function stopD1Router() {
  return sh(["docker", "compose", "stop", "d1-runtime"], { stdio: "pipe" });
}

export function startD1Router() {
  return composeUp(["--wait", "d1-runtime"], { stdio: "pipe" });
}

export function restoreD1SingleRuntime() {
  ensureD1SingleRuntime();
}

/** @param {string} dbKey */
function ownerKeyOf(dbKey) {
  return `d1:owner:db:${encodeURIComponent(dbKey)}`;
}

/**
 * @typedef {{
 *   generation: number | string,
 *   namespace?: string,
 *   databaseId?: string,
 *   dbKey?: string,
 *   slot?: number,
 *   taskId?: string,
 *   endpoint?: string,
 *   leaseExpiresAt?: number,
 *   [key: string]: unknown,
 * }} D1OwnerFixture
 */

/** @param {string} dbKey @param {D1OwnerFixture} owner */
export function redisSetOwner(dbKey, owner) {
  const key = ownerKeyOf(dbKey);
  redisSetJson(key, owner);
  redisSet(`${key}:generation`, String(owner.generation));
}
