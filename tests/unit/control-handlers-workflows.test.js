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
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const WORKFLOWS_HANDLER_GLOBAL = "__workflowsHandlerState";
const ACTIVE_WORKFLOW_KEY = `wf_${"1".repeat(32)}`;
const NEW_WORKFLOW_KEY = `wf_${"2".repeat(32)}`;
const RETIRED_WORKFLOW_KEY = `wf_${"3".repeat(32)}`;
const { libUrl: productionControlLibUrl } = await compileControlGraph();
const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");
const sharedNsPatternUrl = repositoryFileUrl("shared/ns-pattern.js");

const { handle } = await importControlHandler("control/handlers/workflows.js", {
  globalName: WORKFLOWS_HANDLER_GLOBAL,
  replacements: {
    "control-lib": productionControlLibUrl,
    "shared-internal-auth": sharedInternalAuthUrl(),
    "shared-ns-pattern": sharedNsPatternUrl,
    "shared-worker-contract": workerContractUrl,
  },
});

function resetWorkflowsHandlerState() {
  const meta = JSON.stringify({
    workflows: [{
      name: "orders",
      binding: "ORDERS",
      className: "OrderWorkflow",
      workflowKey: ACTIVE_WORKFLOW_KEY,
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
      workflowKey: ACTIVE_WORKFLOW_KEY,
    }],
  });
  assert.deepEqual(state.redis.commands, [
    ["hGetAll", "routes:demo"],
    ["hGetMany", [["worker:demo:api:v:2", "__meta__"]]],
    ["hGetAllMany", ["wf:defs:demo:api"]],
    ["hGetAll", "routes:demo"],
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
  ["malformed", "SECRET_TOKEN_ABC"],
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
      namespace: "demo",
      worker: "api",
      version: "v2",
      error: "corrupt_meta",
      message: "Internal error",
    });
    const rejection = state.logs.find((/** @type {any} */ entry) =>
      entry.event === "workflow_request_rejected"
    );
    assert.ok(rejection);
    assert.equal(rejection.level, "error");
    assert.equal(rejection.fields.request_id, `rid-${label}-meta`);
    assert.equal(rejection.fields.namespace, "demo");
    assert.equal(rejection.fields.worker, "api");
    assert.equal(rejection.fields.status, 500);
    assert.equal(rejection.fields.reason, "corrupt_meta");
    assert.equal(rejection.fields.error_message, "Corrupt __meta__ for demo/api/v2");
    assert.equal(rejection.fields.metadata_version, "v2");
    assert.equal(rejection.fields.stage, "bundle_meta_parse");
    assert.equal(typeof rejection.fields.error_detail, "string");
    if (label === "malformed") {
      assert.equal(rejection.fields.error_detail, "__meta__ is not valid JSON");
      assert.equal(JSON.stringify(state.logs).includes(String(rawMeta)), false);
    }
  });
}

test("workflows handler fails closed on malformed active workflow entries", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("worker:demo:api:v:2", {
    "__meta__": JSON.stringify({
      workflows: [{
        name: "orders",
        binding: "ORDERS",
        className: "OrderWorkflow",
      }],
    }),
  });

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-malformed-workflow-entry",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.stage, undefined);
  const rejection = state.logs.find((/** @type {any} */ entry) =>
    entry.event === "workflow_request_rejected"
  );
  assert.equal(rejection.fields.stage, "workflow_entries_parse");
});

test("workflows handler rejects active workflows that share a workflow key", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("worker:demo:api:v:2", {
    "__meta__": JSON.stringify({
      workflows: [
        {
          name: "orders",
          binding: "ORDERS",
          className: "OrderWorkflow",
          workflowKey: ACTIVE_WORKFLOW_KEY,
        },
        {
          name: "billing",
          binding: "BILLING",
          className: "BillingWorkflow",
          workflowKey: ACTIVE_WORKFLOW_KEY,
        },
      ],
    }),
  });

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-duplicate-active-workflow-key",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  const rejection = state.logs.find((/** @type {any} */ entry) =>
    entry.event === "workflow_request_rejected"
  );
  assert.equal(rejection.fields.stage, "workflow_entries_parse");
});

test("workflows handler fails closed on malformed persisted workflow definitions", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("wf:defs:demo:api", { retired: "not-json" });

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-malformed-workflow-def",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  const rejection = state.logs.find((/** @type {any} */ entry) =>
    entry.event === "workflow_request_rejected"
  );
  assert.equal(rejection.fields.stage, "workflow_defs_parse");
});

test("workflows handler retries a list snapshot split by whole-worker delete", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  redis.hGetMany = async (/** @type {Array<[string, string]>} */ pairs) => {
    redis.commands.push(["hGetMany", pairs]);
    redis.hashes.set("routes:demo", {});
    redis.hashes.set("worker:demo:api:v:2", {});
    return pairs.map(() => null);
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-list-delete-race",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    workflows: [],
  });
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workflows_listed",
    fields: { request_id: "rid-list-delete-race", namespace: "demo", count: 0 },
  }]);
});

test("workflows handler returns worker_not_found when whole-delete wins metadata resolution", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  const originalHGet = redis.hGet.bind(redis);
  redis.hGet = async (/** @type {string} */ key, /** @type {string} */ field) => {
    if (key === "worker:demo:api:v:2" && field === "__meta__") {
      redis.commands.push(["hGet", key, field]);
      redis.hashes.set("routes:demo", {});
      redis.hashes.set("worker:demo:api:v:2", {});
      return null;
    }
    return await originalHGet(key, field);
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-resolve-delete-race",
  });

  await assertJsonResponse(response, 404, {
    error: "worker_not_found",
    message: "Worker demo/api is not active",
  });
  assert.equal(redis.commands.some((/** @type {unknown[]} */ command) => command[0] === "fetch"), false);
});

test("workflows handler returns contention when workflow resolution never stabilizes", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  const originalHGet = redis.hGet.bind(redis);
  const nextVersions = new Map([
    ["worker:demo:api:v:2", "v3"],
    ["worker:demo:api:v:3", "v4"],
  ]);
  redis.hGet = async (/** @type {string} */ key, /** @type {string} */ field) => {
    const nextVersion = field === "__meta__" ? nextVersions.get(key) : undefined;
    if (nextVersion) {
      redis.commands.push(["hGet", key, field]);
      redis.hashes.set("routes:demo", { api: nextVersion });
      return null;
    }
    return await originalHGet(key, field);
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-resolve-contention",
  });

  await assertJsonResponse(response, 503, {
    error: "workflow_metadata_contention",
    message: "Internal error",
    namespace: "demo",
    worker: "api",
  });
  assert.equal(redis.commands.filter((/** @type {unknown[]} */ command) =>
    command[0] === "hGet" && String(command[1]).startsWith("worker:demo:api:v:")
  ).length, 2);
  assert.equal(redis.commands.some((/** @type {unknown[]} */ command) => command[0] === "fetch"), false);
});

for (const [label, rawMeta] of [
  ["missing", null],
  ["empty", ""],
  ["malformed", "SECRET_TOKEN_ABC"],
  ["non-object", "[]"],
]) {
  test(`workflows handler reports stable ${label} metadata despite sibling route churn`, async () => {
    const state = resetWorkflowsHandlerState();
    const redis = /** @type {any} */ (state.redis);
    const emptyMeta = JSON.stringify({ workflows: [] });
    redis.hashes.set("worker:demo:api:v:2", rawMeta == null ? {} : { "__meta__": rawMeta });
    redis.hashes.set("worker:demo:billing:v:1", { "__meta__": emptyMeta });
    redis.hashes.set("worker:demo:billing:v:2", { "__meta__": emptyMeta });
    const routeSnapshots = [
      { api: "v2", billing: "v1" },
      { api: "v2", billing: "v2" },
      { api: "v2", billing: "v2" },
      { api: "v2", billing: "v3" },
    ];
    redis.hGetAll = async (/** @type {string} */ key) => {
      redis.commands.push(["hGetAll", key]);
      if (key === "routes:demo") return routeSnapshots.shift() ?? routeSnapshots.at(-1) ?? {};
      return redis.hashes.get(key) ?? {};
    };

    const response = await handle({
      method: "GET",
      url: new URL("http://control/ns/demo/workflows"),
      ns: "demo",
      subPath: [],
      requestId: `rid-stable-${label}-with-sibling-churn`,
    });

    await assertJsonResponse(response, 500, {
      namespace: "demo",
      worker: "api",
      version: "v2",
      error: "corrupt_meta",
      message: "Internal error",
    });
    assert.equal(routeSnapshots.length, 2);
  });
}

test("workflows handler returns contention when list routes never stabilize", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  const meta = redis.hashes.get("worker:demo:api:v:2")["__meta__"];
  redis.hashes.set("worker:demo:api:v:3", { "__meta__": meta });
  const routeSnapshots = [
    { api: "v2" },
    { api: "v3" },
    { api: "v3" },
    { api: "v4" },
  ];
  redis.hGetAll = async (/** @type {string} */ key) => {
    redis.commands.push(["hGetAll", key]);
    if (key === "routes:demo") return routeSnapshots.shift() ?? {};
    return redis.hashes.get(key) ?? {};
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-list-contention",
  });

  await assertJsonResponse(response, 503, {
    error: "workflow_metadata_contention",
    message: "Internal error",
    namespace: "demo",
  });
  assert.equal(routeSnapshots.length, 0);
});

test("workflows handler does not combine active metadata with redeployed workflow defs", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  redis.hashes.set("worker:demo:api:v:2", { "__meta__": JSON.stringify({ workflows: [] }) });
  const originalHGet = redis.hGet.bind(redis);
  let redeployed = false;
  redis.hGet = async (/** @type {string} */ key, /** @type {string} */ field) => {
    if (key === "wf:defs:demo:api" && field === "orders" && !redeployed) {
      redeployed = true;
      redis.hashes.set("routes:demo", { api: "v3" });
      redis.hashes.set("worker:demo:api:v:3", { "__meta__": JSON.stringify({ workflows: [] }) });
      redis.hashes.set(key, {
        orders: JSON.stringify({ workflowKey: NEW_WORKFLOW_KEY, className: "NewOrderWorkflow" }),
      });
    }
    return await originalHGet(key, field);
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances/order-1"),
    ns: "demo",
    subPath: ["api", "orders", "instances", "order-1"],
    requestId: "rid-redeploy-defs-race",
  });

  assert.equal(response.status, 200);
  assert.deepEqual(redis.commands.at(-1), ["fetch", "http://workflows/internal/workflows/status", {
    ns: "demo",
    worker: "api",
    frozenVersion: "v3",
    workflowName: "orders",
    workflowKey: NEW_WORKFLOW_KEY,
    className: "NewOrderWorkflow",
    instanceId: "order-1",
    options: {},
    requestId: "rid-redeploy-defs-race",
  }]);
});

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
    message: "Internal error",
    namespace: "demo",
    worker_count: 2,
  });
  assert.deepEqual(state.logs, [
    {
      level: "error",
      event: "workflow_metadata_unavailable",
      fields: {
        namespace: "demo",
        worker_count: 2,
        error_message: "redis unavailable",
      },
    },
    {
      level: "error",
      event: "workflow_request_rejected",
      fields: {
        request_id: "rid-meta-fail",
        namespace: "demo",
        status: 500,
        reason: "workflow_metadata_unavailable",
        error_message: "Workflow metadata is unavailable",
      },
    },
  ]);
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
    message: "Internal error",
    namespace: "demo",
    worker_count: 2,
  });
  assert.deepEqual(state.logs, [
    {
      level: "error",
      event: "workflow_metadata_unavailable",
      fields: {
        namespace: "demo",
        worker_count: 2,
        error_message: "defs unavailable",
      },
    },
    {
      level: "error",
      event: "workflow_request_rejected",
      fields: {
        request_id: "rid-defs-fail",
        namespace: "demo",
        status: 500,
        reason: "workflow_metadata_unavailable",
        error_message: "Workflow metadata is unavailable",
      },
    },
  ]);
});

test("workflows handler resolves retired workflow definitions from wf:defs", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("routes:demo", { api: "v3" });
  state.redis.hashes.set("worker:demo:api:v:3", { "__meta__": JSON.stringify({ workflows: [] }) });
  state.redis.hashes.set("wf:defs:demo:api", {
    orders: JSON.stringify({
      workflowKey: RETIRED_WORKFLOW_KEY,
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
    workflowKey: RETIRED_WORKFLOW_KEY,
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
      workflowKey: RETIRED_WORKFLOW_KEY,
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
    ["hGet", "routes:demo", "api"],
    ["fetch", "http://workflows/internal/workflows/resume", {
      ns: "demo",
      worker: "api",
      frozenVersion: "v2",
      workflowName: "orders",
      workflowKey: ACTIVE_WORKFLOW_KEY,
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
    workflowKey: ACTIVE_WORKFLOW_KEY,
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
    message: "Internal error",
  });
  assert.deepEqual(state.redis.commands, [
    ["hGet", "routes:demo", "api"],
    ["hGet", "worker:demo:api:v:2", "__meta__"],
    ["hGet", "routes:demo", "api"],
  ]);
  assert.deepEqual(state.logs, [{
    level: "error",
    event: "workflow_request_rejected",
    fields: {
      request_id: "rid-down",
      namespace: "demo",
      worker: "api",
      workflow: "orders",
      status: 503,
      reason: "workflow_internal_dispatch_failed",
      error_message: "Workflow backend is unavailable",
    },
  }]);
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
    message: "Internal error",
  });
  assert.deepEqual(state.logs.find((/** @type {any} */ entry) =>
    entry.event === "workflow_backend_error"
  ), {
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
    message: "Internal error",
  });
  assert.deepEqual(state.logs.find((/** @type {any} */ entry) =>
    entry.event === "workflow_backend_request_failed"
  ), {
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
    ["hGet", "routes:demo", "api"],
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
    ["hGet", "routes:demo", "api"],
  ]);
});
