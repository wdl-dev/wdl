import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

/** @type {any} */ (globalThis).__controlIndexState = null;

const sharedNsPatternUrl = repositoryFileUrl("shared/ns-pattern.js");
const controlLibUrl = moduleDataUrl(`
export const NS_RE = /^[a-z0-9-]+$/;
export function configuredPublicUrl() { return null; }
export function platformVersionFromPackageJson() { return "wdl.test"; }
export function projectAccessPrincipal(principal) { return principal || null; }
export function isAdminAcceptableNs(ns) { return typeof ns === "string" && !ns.includes("_"); }
function withAction(route, action) { return action ? { ...route, action } : route; }
export function parseControlRoute(pathname, method) {
  if (pathname === "/reload") return withAction({ kind: "reload", scopeRoute: "reload" }, method === "POST" ? "system.reload" : null);
  if (pathname === "/whoami") return withAction({ kind: "whoami", scopeRoute: "whoami" }, method === "GET" ? "diagnostic.whoami" : null);
  if (pathname === "/auth/delegated-tokens") return withAction({ kind: "authDelegatedTokens", scopeRoute: "auth_delegated_tokens" }, method === "POST" ? "auth.delegated_token.issue" : null);
  const nsMatch = /^\\/ns\\/([^/]+)\\/workers$/.exec(pathname);
  if (nsMatch) return withAction({ kind: "workers", ns: nsMatch[1], scopeRoute: "ns_workers" }, method === "GET" ? "worker.list" : null);
  return {};
}
`);

const controlSharedUrl = moduleDataUrl(`
export const state = { service: "control-test" };
export function ensureInit() {}
export async function authorizeControlRequest(_request, _env, routeInfo, requestId) {
  globalThis.__controlIndexState.authCalls.push({ routeInfo, requestId });
  return globalThis.__controlIndexState.authResult;
}
export function jsonResponse(status, data, extraHeaders = {}) {
  return new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json", ...extraHeaders } });
}
export function jsonError(status, error, message, details = {}, extraHeaders = {}) {
  return jsonResponse(status, { ...details, error, message }, extraHeaders);
}
export function internalErrorResponse(status, error, message, requestId) {
  return jsonError(status, error, message, { request_id: requestId });
}
export function requireControlLog() {
  return () => {};
}
`);

const requestScopeUrl = moduleDataUrl(`
export function createHttpRequestScope() {
  return {
    requestId: "rid-control-index",
    setRoute(route) { globalThis.__controlIndexState.routes.push(route); },
    respond(response) { return response; },
    markError(err) { globalThis.__controlIndexState.errors.push(err); },
    complete() { globalThis.__controlIndexState.completed += 1; },
  };
}
`);

const packageJsonUrl = moduleDataUrl(`export default "{}";`);
const handlerUrl = moduleDataUrl(`
export async function handle(args) {
  globalThis.__controlIndexState.handlerCalls.push(args);
  return Response.json({ ok: true });
}
`);

const controlIndex = (await importRepositoryModule("control/index.js", importSpecifierReplacements({
  "control-lib": controlLibUrl,
  "control-shared": controlSharedUrl,
  "shared-ns-pattern": sharedNsPatternUrl,
  "shared-request-scope": requestScopeUrl,
  "wdl-package-json-source": packageJsonUrl,
  "control-handlers-reload": handlerUrl,
  "control-handlers-auth-tokens": handlerUrl,
  "control-handlers-ns-secrets": handlerUrl,
  "control-handlers-hosts": handlerUrl,
  "control-handlers-worker-secrets": handlerUrl,
  "control-handlers-versions": handlerUrl,
  "control-handlers-deploy": handlerUrl,
  "control-handlers-promote": handlerUrl,
  "control-handlers-workers": handlerUrl,
  "control-handlers-delete": handlerUrl,
  "control-handlers-d1": handlerUrl,
  "control-handlers-r2": handlerUrl,
  "control-handlers-logs-tail": handlerUrl,
  "control-handlers-workflows": handlerUrl,
}))).default;

function resetControlIndexState() {
  /** @type {any} */ (globalThis).__controlIndexState = {
    authCalls: [],
    routes: [],
    errors: [],
    completed: 0,
    handlerCalls: [],
    authResult: {
      ok: true,
      principal: { kind: "ops" },
      tokenId: "ops-token",
      status: 200,
    },
  };
  return /** @type {any} */ (globalThis).__controlIndexState;
}

test("control dispatcher returns 405 for ops wrong-method /reload", async () => {
  const state = resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("https://ctl.example/reload", { method: "GET" }),
    {},
    /** @type {ExecutionContext} */ ({})
  );

  await assertJsonResponse(response, 405, {
    error: "method_not_allowed",
    message: "Method not allowed",
  });
  assert.deepEqual(state.authCalls, [{
    routeInfo: {},
    requestId: "rid-control-index",
  }]);
  assert.deepEqual(state.handlerCalls, []);
  assert.equal(state.completed, 1);
});

test("control dispatcher sends delegated auth route to auth token handler", async () => {
  const state = resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("https://ctl.example/auth/delegated-tokens", {
      method: "POST",
      body: JSON.stringify({ template: "wdl-chat-ns-pool" }),
      headers: { "content-type": "application/json" },
    }),
    {},
    /** @type {ExecutionContext} */ ({})
  );

  assert.equal(response.status, 200);
  assert.deepEqual(state.authCalls, [{
    routeInfo: { action: "auth.delegated_token.issue", ns: undefined },
    requestId: "rid-control-index",
  }]);
  assert.equal(state.handlerCalls.length, 1);
  assert.equal(state.handlerCalls[0].routeKind, "delegatedTokens");
  assert.equal(state.handlerCalls[0].auth.tokenId, "ops-token");
});

test("control whoami uses sanitized forwarded proto for public URL hints", async () => {
  const state = resetControlIndexState();
  state.authResult.principal = { kind: "ns", ns: "tenant-a" };

  const response = await controlIndex.fetch(
    new Request("http://control.example/whoami", {
      headers: { "x-forwarded-proto": "https" },
    }),
    { PLATFORM_DOMAIN: "workers.example" },
    /** @type {ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 200);
  assert.equal(body.ok, true);
  assert.deepEqual(body.urls, {
    control: "https://control.example",
    namespace: "https://tenant-a.workers.example",
  });
});

test("control whoami omits namespace URL when no public platform domain is configured", async () => {
  const state = resetControlIndexState();
  state.authResult.principal = { kind: "ns", ns: "tenant-a" };

  const response = await controlIndex.fetch(
    new Request("http://control.example/whoami"),
    {},
    /** @type {ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 200);
  assert.deepEqual(body.urls, { control: "http://control.example" });
});

test("control whoami ignores malformed forwarded proto values", async () => {
  resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("http://control.example/whoami", {
      headers: { "x-forwarded-proto": "https,http" },
    }),
    {},
    /** @type {ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 200);
  assert.equal(body.ok, true);
  assert.deepEqual(body.urls, {
    control: "http://control.example",
  });
});

test("control namespace routes include grammar details for invalid namespaces", async () => {
  resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("https://ctl.example/ns/Bad_NS/workers"),
    {},
    /** @type {ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 400);
  assert.equal(body.error, "invalid_namespace");
  assert.match(body.message, /Must match/);
});
