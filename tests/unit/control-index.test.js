import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { jsonRequest } from "../helpers/request-body.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

/** @type {any} */ (globalThis).__controlIndexState = null;

const { libUrl: controlLibUrl, sharedNsUrl: sharedNsPatternUrl } = await compileControlGraph();

const controlSharedUrl = controlSharedStubUrl(`
export const state = { service: "control-test", log() {} };
export function ensureInit() {}
export async function authorizeControlRequest(_request, _env, routeInfo, requestId) {
  globalThis.__controlIndexState.authCalls.push({ routeInfo, requestId });
  return globalThis.__controlIndexState.authResult;
}
export function internalErrorResponse(status, error, message, requestId) {
  return jsonError(status, error, message, { request_id: requestId });
}
export function authPolicyResponse(err) {
  throw err;
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

const versionUrl = moduleDataUrl(`export default "wdl.20990101.2\\n";`);
const handlerUrl = moduleDataUrl(`
export async function handle(args) {
  globalThis.__controlIndexState.handlerCalls.push(args);
  return Response.json({ ok: true });
}
`);
const authTokensHandlerUrl = repositoryModuleDataUrl(
  "control/handlers/auth-tokens.js",
  importSpecifierReplacements({ "control-shared": controlSharedUrl })
);

const authBinding = {
  /** @param {Record<string, unknown>} input */
  async issue(input) {
    /** @type {any} */ (globalThis).__controlIndexState.authLifecycleCalls.push({ method: "issue", input });
    return { tokenId: "issued-token" };
  },
  /** @param {Record<string, unknown>} input */
  async delegatedIssue(input) {
    /** @type {any} */ (globalThis).__controlIndexState.authLifecycleCalls.push({ method: "delegatedIssue", input });
    return { tokenId: "delegated-token" };
  },
  /** @param {Record<string, unknown>} input */
  async list(input) {
    /** @type {any} */ (globalThis).__controlIndexState.authLifecycleCalls.push({ method: "list", input });
    return { tokens: [] };
  },
  /** @param {Record<string, unknown>} input */
  async revoke(input) {
    /** @type {any} */ (globalThis).__controlIndexState.authLifecycleCalls.push({ method: "revoke", input });
    return { revoked: true };
  },
};

const controlIndex = (await importRepositoryModule("control/index.js", importSpecifierReplacements({
  "control-lib": controlLibUrl,
  "control-shared": controlSharedUrl,
  "shared-ns-pattern": sharedNsPatternUrl,
  "shared-request-scope": requestScopeUrl,
  "wdl-version-source": versionUrl,
  "control-handlers-reload": handlerUrl,
  "control-handlers-auth-tokens": authTokensHandlerUrl,
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
  "control-handlers-ai": handlerUrl,
  "control-handlers-logs-tail": handlerUrl,
  "control-handlers-workflows": handlerUrl,
}))).default;

function resetControlIndexState() {
  /** @type {any} */ (globalThis).__controlIndexState = {
    authCalls: [],
    authLifecycleCalls: [],
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
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
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

for (const action of ["deploy", "promote", "delete"]) {
  test(`control dispatcher rejects ops ${action} suffixes without calling a handler`, async () => {
    for (const suffix of ["extra", "extra/more"]) {
      const state = resetControlIndexState();
      const request = jsonRequest(`https://ctl.example/ns/demo/worker/api/${action}/${suffix}`, {
        code: "export default {};",
        version: "v1",
      }, { method: "POST" });

      const response = await controlIndex.fetch(request, {}, {});

      await assertJsonResponse(response, 404, { error: "not_found", message: "Not found" });
      assert.deepEqual(state.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
      assert.deepEqual(state.handlerCalls, []);
      assert.equal(request.bodyUsed, false);
    }
  });

  test(`control dispatcher accepts exact ${action} POST and preserves wrong-method 404`, async () => {
    const state = resetControlIndexState();
    const request = jsonRequest(`https://ctl.example/ns/demo/worker/api/${action}`, {}, { method: "POST" });

    const response = await controlIndex.fetch(request, {}, {});

    assert.equal(response.status, 200);
    assert.deepEqual(state.authCalls, [{
      routeInfo: { action: `worker.${action}`, ns: "demo" },
      requestId: "rid-control-index",
    }]);
    assert.equal(state.handlerCalls.length, 1);
    assert.equal(state.handlerCalls[0].request, request);
    assert.equal(state.handlerCalls[0].ns, "demo");
    assert.equal(state.handlerCalls[0].name, "api");

    const wrongMethodState = resetControlIndexState();
    const wrongMethod = await controlIndex.fetch(new Request(request.url), {}, {});
    assert.equal(wrongMethod.status, 404);
    assert.deepEqual(wrongMethodState.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
    assert.deepEqual(wrongMethodState.handlerCalls, []);
  });
}

test("control dispatcher rejects ops auth token suffixes without AUTH lifecycle calls", async () => {
  for (const method of ["GET", "POST", "DELETE"]) {
    for (const suffix of ["extra", "extra/more"]) {
      const state = resetControlIndexState();
      const url = `https://ctl.example/auth/tokens/token-id/${suffix}`;
      const request = method === "POST"
        ? jsonRequest(url, { kind: "ns", ns: "demo" }, { method })
        : new Request(url, { method });

      const response = await controlIndex.fetch(request, { AUTH: authBinding }, {});

      const body = await readJsonResponse(response, 400);
      assert.equal(body.error, "invalid_path");
      assert.equal(body.message, "Invalid control API path");
      assert.deepEqual(state.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
      assert.deepEqual(state.handlerCalls, []);
      assert.deepEqual(state.authLifecycleCalls, []);
      assert.equal(request.bodyUsed, false);
    }
  }
});

test("control dispatcher keeps exact auth token lifecycle routes", async () => {
  for (const [method, path, operation] of [
    ["POST", "/auth/tokens", "issue"],
    ["GET", "/auth/tokens", "list"],
    ["DELETE", "/auth/tokens/token-id", "revoke"],
  ]) {
    const state = resetControlIndexState();
    const url = `https://ctl.example${path}`;
    const request = method === "POST"
      ? jsonRequest(url, { kind: "ns", ns: "demo" }, { method })
      : new Request(url, { method });

    const response = await controlIndex.fetch(request, { AUTH: authBinding }, {});

    assert.equal(response.status, operation === "issue" ? 201 : 200);
    assert.deepEqual(state.authCalls, [{
      routeInfo: { action: `auth.token.${operation}`, ns: undefined },
      requestId: "rid-control-index",
    }]);
    assert.equal(state.authLifecycleCalls.length, 1);
    assert.equal(state.authLifecycleCalls[0].method, operation);
    assert.equal(state.authLifecycleCalls[0].input.requestId, "rid-control-index");
    if (operation === "issue") {
      assert.equal(state.authLifecycleCalls[0].input.ns, "demo");
      assert.equal(state.authLifecycleCalls[0].input.issuerTokenId, "ops-token");
    } else if (operation === "revoke") {
      assert.equal(state.authLifecycleCalls[0].input.tokenId, "token-id");
    }
  }
});

test("control dispatcher keeps ops auth token wrong-method 405 without AUTH lifecycle calls", async () => {
  for (const [method, path] of [
    ["PUT", "/auth/tokens"],
    ["DELETE", "/auth/tokens"],
    ["GET", "/auth/tokens/token-id"],
    ["POST", "/auth/tokens/token-id"],
  ]) {
    const state = resetControlIndexState();
    const response = await controlIndex.fetch(
      new Request(`https://ctl.example${path}`, { method }),
      { AUTH: authBinding },
      {}
    );

    assert.equal(response.status, 405);
    assert.deepEqual(state.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
    assert.deepEqual(state.authLifecycleCalls, []);
  }
});

test("control dispatcher preserves non-ops rejection before suffix dispatch", async () => {
  for (const path of [
    "/ns/demo/worker/api/deploy/extra",
    "/ns/demo/worker/api/promote/extra",
    "/ns/demo/worker/api/delete/extra",
    "/auth/tokens/token-id/extra",
  ]) {
    const state = resetControlIndexState();
    state.authResult = {
      ok: false,
      status: 403,
      error: "unknown_action_requires_ops",
      message: "forbidden",
    };
    const request = jsonRequest(`https://ctl.example${path}`, {}, { method: "POST" });

    const response = await controlIndex.fetch(request, { AUTH: authBinding }, {});

    await assertJsonResponse(response, 403, { error: "unknown_action_requires_ops", message: "forbidden" });
    assert.deepEqual(state.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
    assert.deepEqual(state.handlerCalls, []);
    assert.deepEqual(state.authLifecycleCalls, []);
    assert.equal(request.bodyUsed, false);
  }
});

test("control dispatcher rejects non-ops log-tail methods before handler dispatch", async () => {
  for (const method of ["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]) {
    const state = resetControlIndexState();
    state.authResult = {
      ok: false,
      status: 403,
      error: "unknown_action_requires_ops",
      message: "forbidden",
    };
    const response = await controlIndex.fetch(
      new Request("https://ctl.example/ns/demo/logs/tail?worker=foo", { method }),
      {},
      {}
    );

    assert.equal(response.status, 403, method);
    if (method === "HEAD") {
      // The HTTP transport strips HEAD bodies; this direct fetch has no pump.
      await response.body?.cancel();
    } else {
      await assertJsonResponse(response, 403, {
        error: "unknown_action_requires_ops",
        message: "forbidden",
      });
    }
    assert.deepEqual(state.authCalls, [{ routeInfo: {}, requestId: "rid-control-index" }]);
    assert.deepEqual(state.handlerCalls, []);
    assert.deepEqual(state.authLifecycleCalls, []);
  }
});

test("control dispatcher sends delegated auth route to auth token handler", async () => {
  const state = resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("https://ctl.example/auth/delegated-tokens", {
      method: "POST",
      body: JSON.stringify({ template: "wdl-chat-ns-pool" }),
      headers: { "content-type": "application/json" },
    }),
    { AUTH: authBinding },
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
  );

  assert.equal(response.status, 201);
  assert.deepEqual(state.authCalls, [{
    routeInfo: { action: "auth.delegated_token.issue", ns: undefined },
    requestId: "rid-control-index",
  }]);
  assert.deepEqual(state.authLifecycleCalls, [{
    method: "delegatedIssue",
    input: {
      issuerTokenId: "ops-token",
      template: "wdl-chat-ns-pool",
      requestId: "rid-control-index",
    },
  }]);
});

test("control whoami uses sanitized forwarded proto for public URL hints", async () => {
  const state = resetControlIndexState();
  state.authResult.principal = { kind: "ns", ns: "tenant-a" };

  const response = await controlIndex.fetch(
    new Request("http://control.example/whoami", {
      headers: { "x-forwarded-proto": "https" },
    }),
    { PLATFORM_DOMAIN: "workers.example" },
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 200);
  assert.equal(body.ok, true);
  assert.equal(body.platformVersion, "wdl.20990101.2");
  assert.equal(body.minCliVersion, "1.9.0");
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
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
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
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
  );

  const body = await readJsonResponse(response, 200);
  assert.equal(body.ok, true);
  assert.deepEqual(body.urls, {
    control: "http://control.example",
  });
});

test("control namespace routes include grammar details for invalid namespaces", async () => {
  for (const ns of ["Bad_NS", "BadName", "-invalid", "admin"]) {
    const state = resetControlIndexState();

    const response = await controlIndex.fetch(
      new Request(`https://ctl.example/ns/${ns}/workers`),
      {},
      /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
    );

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "invalid_namespace");
    assert.match(body.message, /Must match/);
    assert.deepEqual(state.handlerCalls, []);
  }
});

test("control dispatcher preserves reserved namespaces for authorized ops requests", async () => {
  const state = resetControlIndexState();

  const response = await controlIndex.fetch(
    new Request("https://ctl.example/ns/__system__/workers"),
    {},
    /** @type {import("@cloudflare/workers-types").ExecutionContext} */ ({})
  );

  await assertJsonResponse(response, 200, { ok: true });
  assert.equal(state.handlerCalls.length, 1);
  assert.equal(state.handlerCalls[0].nsName, "__system__");
  assert.deepEqual(state.authCalls, [{
    routeInfo: { action: "worker.list", ns: "__system__" },
    requestId: "rid-control-index",
  }]);
});
