import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createControlHandlerState,
  importControlHandler,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import {
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const WORKFLOWS_HANDLER_GLOBAL = "__workflowsHandlerState";
const { libUrl: productionControlLibUrl } = await compileControlGraph();
const sharedVersionUrl = repositoryFileUrl("shared/version.js");

const { handle } = await importControlHandler("control/handlers/workflows.js", {
  globalName: WORKFLOWS_HANDLER_GLOBAL,
  replacements: {
    "control-lib": productionControlLibUrl,
    "shared-internal-auth": sharedInternalAuthUrl(),
    "shared-version": sharedVersionUrl,
  },
});

function resetWorkflowsHandlerState() {
  const meta = JSON.stringify({
    workflows: [{
      name: "orders",
      binding: "ORDERS",
      className: "OrderWorkflow",
      workflowKey: "wf_1234",
    }],
  });
  const state = createControlHandlerState({
    env: { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN },
  });
  const redis = /** @type {any} */ (state.redis);
  redis.hashes.set("routes:demo", { api: "v2" });
  redis.hashes.set("worker:demo:api:v:2", { "__meta__": meta });
  state.workflows = {
    /** @param {string} url @param {{ body: string, headers?: HeadersInit, signal?: AbortSignal | null }} init */
    async fetch(url, init) {
      assert.equal(init.signal, undefined);
      assert.equal(new Headers(init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
      redis.commands.push(["fetch", url, parseJsonObjectRequestBody(init, "workflows backend request body")]);
      return new Response(JSON.stringify({ id: "order-1", status: "paused" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  };
  installControlHandlerState(WORKFLOWS_HANDLER_GLOBAL, state);
  return /** @type {any} */ (state);
}

test("workflows handler lists active workflow definitions from bundle metadata", async () => {
  const state = resetWorkflowsHandlerState();

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-list",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    workflows: [{
      namespace: "demo",
      worker: "api",
      activeVersion: "v2",
      name: "orders",
      binding: "ORDERS",
      className: "OrderWorkflow",
      workflowKey: "wf_1234",
    }],
  });
  assert.deepEqual(state.redis.commands, [
    ["hGetAll", "routes:demo"],
    ["hGetMany", [["worker:demo:api:v:2", "__meta__"]]],
    ["hGetAllMany", ["wf:defs:demo:api"]],
  ]);
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workflows_listed",
    fields: { request_id: "rid-list", namespace: "demo", count: 1 },
  }]);
});

for (const [label, rawMeta] of [
  ["missing", null],
  ["empty", ""],
  ["malformed", "{bad"],
  ["non-object", "[]"],
]) {
  test(`workflows handler fails closed on ${label} active bundle metadata`, async () => {
    const state = resetWorkflowsHandlerState();
    state.redis.hashes.set(
      "worker:demo:api:v:2",
      rawMeta === null ? {} : { "__meta__": rawMeta }
    );

    const response = await handle({
      method: "GET",
      url: new URL("http://control/ns/demo/workflows"),
      ns: "demo",
      subPath: [],
      requestId: `rid-${label}-meta`,
    });

    await assertJsonResponse(response, 500, {
      error: "corrupt_meta",
      message: "Corrupt __meta__ for demo/api/v2",
    });
  });
}

test("workflows handler lists empty namespaces without batch reads", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("routes:demo", {});

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-empty-list",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    workflows: [],
  });
  assert.deepEqual(state.redis.commands, [["hGetAll", "routes:demo"]]);
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workflows_listed",
    fields: { request_id: "rid-empty-list", namespace: "demo", count: 0 },
  }]);
});

test("workflows handler preserves metadata-unavailable error shape for batched reads", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hGetAll = async (/** @type {string} */ key) => {
    state.redis.commands.push(["hGetAll", key]);
    if (key === "routes:demo") return { api: "v2", billing: "v5" };
    return {};
  };
  state.redis.hGetMany = async (/** @type {Array<[string, string]>} */ pairs) => {
    state.redis.commands.push(["hGetMany", pairs]);
    throw new Error("redis unavailable");
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-meta-fail",
  });

  await assertJsonResponse(response, 500, {
    error: "workflow_metadata_unavailable",
    message: "Workflow metadata is unavailable",
    namespace: "demo",
    worker_count: 2,
  });
  assert.deepEqual(state.logs, [{
    level: "error",
    event: "workflow_metadata_unavailable",
    fields: {
      namespace: "demo",
      worker_count: 2,
      error_message: "redis unavailable",
    },
  }]);
});

test("workflows handler wraps workflow definition batch read failures", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hGetAll = async (/** @type {string} */ key) => {
    state.redis.commands.push(["hGetAll", key]);
    if (key === "routes:demo") return { api: "v2", billing: "v5" };
    return {};
  };
  state.redis.hGetAllMany = async (/** @type {string[]} */ keys) => {
    state.redis.commands.push(["hGetAllMany", keys]);
    throw new Error("defs unavailable");
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-defs-fail",
  });

  await assertJsonResponse(response, 500, {
    error: "workflow_metadata_unavailable",
    message: "Workflow metadata is unavailable",
    namespace: "demo",
    worker_count: 2,
  });
  assert.deepEqual(state.logs, [{
    level: "error",
    event: "workflow_metadata_unavailable",
    fields: {
      namespace: "demo",
      worker_count: 2,
      error_message: "defs unavailable",
    },
  }]);
});

test("workflows handler resolves retired workflow definitions from wf:defs", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("routes:demo", { api: "v3" });
  state.redis.hashes.set("worker:demo:api:v:3", { "__meta__": JSON.stringify({ workflows: [] }) });
  state.redis.hashes.set("wf:defs:demo:api", {
    orders: JSON.stringify({
      workflowKey: "wf_retired",
      className: "OldOrderWorkflow",
    }),
  });

  const response = await handle({
    method: "POST",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1/terminate"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1", "terminate"],
    requestId: "rid-retired",
  });

  assert.equal(response.status, 200);
  assert.deepEqual(state.redis.commands.at(-1), ["fetch", "http://workflows/internal/workflows/terminate", {
    ns: "demo",
    worker: "api",
    frozenVersion: "v3",
    workflowName: "orders",
    workflowKey: "wf_retired",
    className: "OldOrderWorkflow",
    instanceId: "order-1",
    requestId: "rid-retired",
  }]);
});

test("workflows handler rejects restart for retired workflow definitions", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("routes:demo", { api: "v3" });
  state.redis.hashes.set("worker:demo:api:v:3", { "__meta__": JSON.stringify({ workflows: [] }) });
  state.redis.hashes.set("wf:defs:demo:api", {
    orders: JSON.stringify({
      workflowKey: "wf_retired",
      className: "OldOrderWorkflow",
    }),
  });

  const response = await handle({
    method: "POST",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1/restart"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1", "restart"],
    requestId: "rid-retired-restart",
  });

  await assertJsonResponse(response, 409, {
    error: "workflow_not_exported",
    message: "Workflow demo/api/orders is not exported by the active worker version",
  });
  assert.equal(state.redis.commands.some((/** @type {any} */ call) => call[0] === "fetch"), false);
});

test("workflows handler resolves retired workflow defs with own-property discipline", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("routes:demo", { api: "v3" });
  state.redis.hashes.set("worker:demo:api:v:3", { "__meta__": JSON.stringify({ workflows: [] }) });
  state.redis.hashes.set("wf:defs:demo:api", {});

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/constructor/instances/order-1"),
    ns: "demo",
    subPath: ["api", "constructor", "instances", "order-1"],
    requestId: "rid-constructor",
  });

  await assertJsonResponse(response, 404, {
    error: "workflow_not_found",
    message: "Workflow demo/api/constructor is not exported",
  });
  assert.equal(state.redis.commands.some((/** @type {any} */ call) => call[0] === "fetch"), false);
});

test("workflows handler resolves active workflow identity before lifecycle proxy", async () => {
  const state = resetWorkflowsHandlerState();

  const response = await handle({
    method: "POST",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1/resume"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1", "resume"],
    requestId: "rid-resume",
  });

  await assertJsonResponse(response, 200, { id: "order-1", status: "paused" });
  assert.deepEqual(state.redis.commands, [
    ["hGet", "routes:demo", "api"],
    ["hGet", "worker:demo:api:v:2", "__meta__"],
    ["fetch", "http://workflows/internal/workflows/resume", {
      ns: "demo",
      worker: "api",
      frozenVersion: "v2",
      workflowName: "orders",
      workflowKey: "wf_1234",
      className: "OrderWorkflow",
      instanceId: "order-1",
      requestId: "rid-resume",
    }],
  ]);
});

test("workflows handler forwards status includeSteps options", async () => {
  const state = resetWorkflowsHandlerState();

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1?includeSteps=true&stepLimit=10"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-status",
  });

  assert.equal(response.status, 200);
  assert.deepEqual(state.redis.commands.at(-1), ["fetch", "http://workflows/internal/workflows/status", {
    ns: "demo",
    worker: "api",
    frozenVersion: "v2",
    workflowName: "orders",
    workflowKey: "wf_1234",
    className: "OrderWorkflow",
    instanceId: "order-1",
    options: { includeSteps: true, stepLimit: 10 },
    requestId: "rid-status",
  }]);
});

test("workflows handler fails closed when workflows backend is unavailable", async () => {
  const state = resetWorkflowsHandlerState();
  state.workflows = null;

  const response = await handle({
    method: "POST",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1/terminate"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1", "terminate"],
    requestId: "rid-down",
  });

  await assertJsonResponse(response, 503, {
    error: "workflow_internal_dispatch_failed",
    message: "Workflow backend is unavailable",
  });
  assert.deepEqual(state.redis.commands, [
    ["hGet", "routes:demo", "api"],
    ["hGet", "worker:demo:api:v:2", "__meta__"],
  ]);
});

test("workflows handler hides backend 5xx messages but logs diagnostics", async () => {
  const state = resetWorkflowsHandlerState();
  state.workflows = {
    /** @param {string} url @param {{ body: string, headers?: HeadersInit }} init */
    async fetch(url, init) {
      assert.equal(new Headers(init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
      state.redis.commands.push(["fetch", url, parseJsonObjectRequestBody(init, "workflows backend request body")]);
      return Response.json({
        error: "redis_error",
        message: "READONLY replica cannot accept writes",
        shard: "s1",
      }, { status: 500 });
    },
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-upstream-500",
  });

  await assertJsonResponse(response, 500, {
    upstream_status: 500,
    error: "redis_error",
    message: "Workflow backend request failed",
  });
  assert.deepEqual(state.logs.at(-1), {
    level: "error",
    event: "workflow_backend_error",
    fields: {
      request_id: "rid-upstream-500",
      endpoint: "status",
      upstream_status: 500,
      error: "redis_error",
      error_message: "READONLY replica cannot accept writes",
    },
  });
});

test("workflows handler hides backend fetch exceptions but logs diagnostics", async () => {
  const state = resetWorkflowsHandlerState();
  state.workflows = {
    async fetch() {
      throw new Error("connect ECONNREFUSED workflows");
    },
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-fetch-fail",
  });

  await assertJsonResponse(response, 503, {
    error: "workflow_internal_dispatch_failed",
    message: "Workflow backend request failed",
  });
  assert.deepEqual(state.logs.at(-1), {
    level: "error",
    event: "workflow_backend_request_failed",
    fields: {
      request_id: "rid-fetch-fail",
      endpoint: "status",
      error_message: "connect ECONNREFUSED workflows",
    },
  });
});

test("workflows handler rejects invalid status query options before backend dispatch", async () => {
  const state = resetWorkflowsHandlerState();

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1?includeSteps=maybe"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-bad-query",
  });

  await assertJsonResponse(response, 400, {
    error: "invalid_request",
    message: "includeSteps must be true or false",
  });
  assert.deepEqual(state.redis.commands, [
    ["hGet", "routes:demo", "api"],
    ["hGet", "worker:demo:api:v:2", "__meta__"],
  ]);
});

test("workflows handler rejects snake_case status query options", async () => {
  const state = resetWorkflowsHandlerState();

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1?include_steps=true&step_limit=10"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-snake-query",
  });

  await assertJsonResponse(response, 400, {
    error: "invalid_request",
    message: "workflow status query options use camelCase",
  });
  assert.deepEqual(state.redis.commands, [
    ["hGet", "routes:demo", "api"],
    ["hGet", "worker:demo:api:v:2", "__meta__"],
  ]);
});
