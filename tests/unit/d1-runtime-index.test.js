import { test } from "node:test";
import assert from "node:assert/strict";
import { loadD1QueryWire } from "../helpers/load-d1-protocol.js";
import { applyModuleReplacements, moduleDataUrl, readRepositoryFile } from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const { D1_QUERY_RESPONSE_CONTENT_TYPE } = await loadD1QueryWire();
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

const protocolUrl = moduleDataUrl(`
export function classifyD1Error(err) {
  return {
    status: Number.isInteger(err?.status) ? err.status : 503,
    code: err?.code || "stub-error",
    category: "internal",
    retryable: false,
    message: err instanceof Error ? err.message : String(err),
  };
}
export function d1ErrorPayload(err) {
  const classified = classifyD1Error(err);
  return {
    success: false,
    error: classified.code,
    message: classified.message,
    category: classified.category,
    retryable: classified.retryable,
  };
}
export function d1ErrorResponse(err) {
  return new Response("binary:" + (err instanceof Error ? err.message : String(err)), {
    status: Number.isInteger(err?.status) ? err.status : 503,
    headers: { "content-type": ${JSON.stringify(D1_QUERY_RESPONSE_CONTENT_TYPE)} },
  });
}
`);

const observabilityUrl = moduleDataUrl(`
export const calls = { ensure: 0, requestComplete: [] };
export function ensureRequestId() {
  calls.ensure += 1;
  return "rid-index";
}
export function recordRequestComplete(fields) {
  calls.requestComplete.push(fields);
}
export function createLogLevelBinder() {
  return function bindLogLevel() {};
}
`);

const opsUrl = moduleDataUrl(`
const state = /** @type {any} */ (globalThis).__d1IndexOpsState ||= { throwProbe: false };
export async function handleDrain() { return Response.json({ ok: true }); }
export async function handleHealth() { return Response.json({ ok: true }); }
export async function handleProbe() {
  if (state.throwProbe) {
    const err = new Error("probe exploded");
    err.status = 503;
    err.code = "probe_failed";
    throw err;
  }
  return Response.json({ ok: true });
}
export async function handleRebalance() { return Response.json({ ok: true }); }
export async function handleRenew() { return Response.json({ ok: true }); }
export function handleMetrics() { return new Response(""); }
`);

const actorUrl = moduleDataUrl(`
export class D1DatabaseActor {}
`);

const httpUrl = moduleDataUrl(`
export function json(data, init = {}) {
  return Response.json(data, init);
}
export function jsonError(status, error, message, extra = {}) {
  return json({ ...extra, error, message }, { status });
}
`);

const routerUrl = moduleDataUrl(`
export const calls = [];
const state = /** @type {any} */ (globalThis).__d1IndexRouterState ||= { throwQuery: false };
export async function handleQuery(_request, _env, requestId) {
  if (state.throwQuery) {
    const err = new Error("query exploded");
    err.status = 503;
    err.code = "query_failed";
    throw err;
  }
  calls.push({ route: "query", requestId });
  return Response.json({ success: true, requestId });
}
export async function handleTestHookQuery(_request, _env, requestId) {
  calls.push({ route: "test-hook", requestId });
  return Response.json({ success: true, requestId });
}
`);

const stateUrl = moduleDataUrl(`
export function log() {}
export const metrics = { increment() {}, observe() {} };
export const SERVICE = "d1-runtime";
`);

const respondUrl = moduleDataUrl(`
export function echoResponseWithRequestId(response, requestId) {
  const out = new Response(response.body, response);
  out.headers.set("x-request-id", requestId);
  return out;
}
`);

const requestScopeUrl = moduleDataUrl(`
import { ensureRequestId, recordRequestComplete } from ${JSON.stringify(observabilityUrl)};
import { echoResponseWithRequestId } from ${JSON.stringify(respondUrl)};
export function createHttpRequestScope({ request, service, metrics, log, route, probeRoutes }) {
  const requestId = ensureRequestId(request.headers);
  const startedAt = Date.now();
  let status = 500;
  let requestError = null;
  let hasRequestError = false;
  return {
    requestId,
    markError(err) {
      requestError = err;
      hasRequestError = true;
      return err;
    },
    respond(response) {
      status = response.status;
      return echoResponseWithRequestId(response, requestId);
    },
    complete() {
      recordRequestComplete({
        service,
        metrics,
        log,
        method: request.method,
        requestId,
        route,
        status,
        startedAt,
        error: requestError,
        hasError: hasRequestError,
        probeRoutes,
      });
    },
  };
}
`);

const src = applyModuleReplacements(readRepositoryFile("d1-runtime/index.js"), [
  [
    /import \{\n {2}classifyD1Error,\n {2}d1ErrorPayload,\n {2}d1ErrorResponse,\n\} from "d1-runtime-protocol";/,
    `import { classifyD1Error, d1ErrorPayload, d1ErrorResponse } from ${JSON.stringify(protocolUrl)};`,
  ],
  [
    /import \{\n {2}createLogLevelBinder,\n\} from "shared-observability";/,
    `import { createLogLevelBinder } from ${JSON.stringify(observabilityUrl)};`,
  ],
  [
    /import \{\n {2}createHttpRequestScope,\n\} from "shared-request-scope";/,
    `import { createHttpRequestScope } from ${JSON.stringify(requestScopeUrl)};`,
  ],
  [
    /import \{\n {2}handleDrain,\n {2}handleHealth,\n {2}handleProbe,\n {2}handleRebalance,\n {2}handleRenew,\n {2}handleMetrics,\n\} from "d1-runtime-ops";/,
    `import { handleDrain, handleHealth, handleProbe, handleRebalance, handleRenew, handleMetrics } from ${JSON.stringify(opsUrl)};`,
  ],
  [/import \{ D1DatabaseActor \} from "d1-runtime-actor";/, `import { D1DatabaseActor } from ${JSON.stringify(actorUrl)};`],
  [/import \{ jsonError \} from "d1-runtime-http";/, `import { jsonError } from ${JSON.stringify(httpUrl)};`],
  [
    /import \{\n {2}internalAuthFailureResponse,\n {2}verifyInternalAuthHeaders,\n\} from "shared-internal-auth";/,
    `import { internalAuthFailureResponse, verifyInternalAuthHeaders } from ${JSON.stringify(sharedInternalAuthUrl())};`,
  ],
  [
    /import \{\n {2}handleQuery,\n {2}handleTestHookQuery,\n\} from "d1-runtime-router";/,
    `import { handleQuery, handleTestHookQuery } from ${JSON.stringify(routerUrl)};`,
  ],
  [
    /import \{\n {2}log,\n {2}metrics,\n {2}SERVICE,\n\} from "d1-runtime-state";/,
    `import { log, metrics, SERVICE } from ${JSON.stringify(stateUrl)};`,
  ],
]);

const [{ default: d1Runtime }, observability, router] = await Promise.all([
  import(moduleDataUrl(src)),
  import(observabilityUrl),
  import(routerUrl),
]);

/** @param {string} url @param {RequestInit} [init] */
function internalRequest(url, init = {}) {
  const headers = new Headers(init.headers);
  headers.set("x-wdl-internal-auth", TEST_INTERNAL_AUTH_TOKEN);
  return new Request(url, { ...init, headers });
}

function env() {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
}

test("D1 runtime rejects private routes without valid internal auth token", async () => {
  for (const headers of [new Headers(), new Headers({ "x-wdl-internal-auth": "wrong" })]) {
    const response = await d1Runtime.fetch(
      new Request("http://d1-runtime/internal/d1/query", {
        method: "POST",
        headers,
        body: JSON.stringify({ namespace: "tenant-a", databaseId: "db1", mode: "all", statements: [] }),
      }),
      env()
    );

    await assertJsonResponse(response, 401, {
      error: "internal_auth_failed",
      message: "Internal authentication failed",
    });
  }
  observability.calls.ensure = 0;
  observability.calls.requestComplete.length = 0;
});

test("D1 runtime query uses one request id for response, request log, and query router", async () => {
  /** @type {any} */ (globalThis).__d1IndexRouterState.throwQuery = false;
  const response = await d1Runtime.fetch(
    internalRequest("http://d1-runtime/internal/d1/query", {
      method: "POST",
      body: JSON.stringify({ namespace: "tenant-a", databaseId: "db1", mode: "all", statements: [] }),
    }),
    env()
  );

  assert.equal(response.headers.get("x-request-id"), "rid-index");
  await assertJsonResponse(response, 200, { success: true, requestId: "rid-index" });
  assert.equal(observability.calls.ensure, 1);
  assert.equal(router.calls[0].route, "query");
  assert.equal(router.calls[0].requestId, "rid-index");
  assert.equal(observability.calls.requestComplete[0].requestId, "rid-index");
});

test("D1 runtime test-hook query route uses the scoped request id", async () => {
  /** @type {any} */ (globalThis).__d1IndexRouterState.throwQuery = false;
  router.calls.length = 0;
  const response = await d1Runtime.fetch(
    internalRequest("http://d1-runtime/internal/d1/test-hook/query", {
      method: "POST",
      body: JSON.stringify({}),
    }),
    env()
  );

  assert.equal(response.headers.get("x-request-id"), "rid-index");
  await assertJsonResponse(response, 200, { success: true, requestId: "rid-index" });
  assert.equal(router.calls[0].route, "test-hook");
  assert.equal(router.calls[0].requestId, "rid-index");
});

test("D1 runtime non-query route errors remain JSON", async () => {
  /** @type {any} */ (globalThis).__d1IndexOpsState.throwProbe = true;
  try {
    const response = await d1Runtime.fetch(
      internalRequest("http://d1-runtime/internal/d1/probe"),
      env()
    );

    assert.match(response.headers.get("content-type") || "", /application\/json/);
    await assertJsonResponse(response, 503, {
      success: false,
      error: "probe_failed",
      message: "probe exploded",
      category: "internal",
      retryable: false,
    });
  } finally {
    /** @type {any} */ (globalThis).__d1IndexOpsState.throwProbe = false;
  }
});

test("D1 runtime query route errors remain binary D1 query responses", async () => {
  /** @type {any} */ (globalThis).__d1IndexRouterState.throwQuery = true;
  try {
    const response = await d1Runtime.fetch(
      internalRequest("http://d1-runtime/internal/d1/query", {
        method: "POST",
        body: JSON.stringify({}),
      }),
      env()
    );

    assert.equal(response.status, 503);
    assert.equal(response.headers.get("content-type"), D1_QUERY_RESPONSE_CONTENT_TYPE);
    assert.equal(await response.text(), "binary:query exploded");
  } finally {
    /** @type {any} */ (globalThis).__d1IndexRouterState.throwQuery = false;
  }
});
