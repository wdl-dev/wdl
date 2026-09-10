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
import { withMockedProperty } from "../helpers/mock-global.js";
import { workflowDefinitionsUrl, workflowDefinitionRedisEval } from "../helpers/workflow-definitions.js";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const WORKFLOWS_HANDLER_GLOBAL = "__workflowsHandlerState";
const ACTIVE_WORKFLOW_KEY = `wf_${"1".repeat(32)}`;
const NEW_WORKFLOW_KEY = `wf_${"2".repeat(32)}`;
const RETIRED_WORKFLOW_KEY = `wf_${"3".repeat(32)}`;
const { libUrl: productionControlLibUrl } = await compileControlGraph();
const { WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT, WORKFLOW_ROUTE_PAGE_SCRIPT } = await import(workflowDefinitionsUrl);
const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");
const sharedNsPatternUrl = repositoryFileUrl("shared/ns-pattern.js");

const { handle } = await importControlHandler("control/handlers/workflows.js", {
  globalName: WORKFLOWS_HANDLER_GLOBAL,
  replacements: {
    "control-lib": productionControlLibUrl,
    "shared-internal-auth": sharedInternalAuthUrl(),
    "shared-ns-pattern": sharedNsPatternUrl,
    "shared-worker-contract": workerContractUrl,
    "control-workflow-definitions": workflowDefinitionsUrl,
    "base64.js": repositoryFileUrl("shared/base64.js"),
    "shared-utf8": repositoryFileUrl("shared/utf8.js"),
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
    redis: createFakeRedis(undefined, { eval: workflowDefinitionRedisEval }),
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

/**
 * @param {any} state
 * @param {(session: any) => void} configure
 */
function configureWorkflowListSession(state, configure) {
  const redis = /** @type {any} */ (state.redis);
  const originalSession = redis.session.bind(redis);
  redis.session = async (/** @type {(session: unknown) => Promise<unknown>} */ fn) =>
    originalSession(async (/** @type {any} */ session) => {
      configure(session);
      return await fn(session);
    });
}

test("workflows handler lists active workflow definitions from bundle metadata", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  const originalSession = redis.session.bind(redis);
  let sessionCalls = 0;
  redis.session = async (/** @type {(session: unknown) => Promise<unknown>} */ fn) => {
    sessionCalls += 1;
    return await originalSession(fn);
  };

  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows"),
    ns: "demo",
    subPath: [],
    requestId: "rid-list",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    cursor: null,
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
  assert.equal(state.redis.commands.filter((/** @type {unknown[]} */ command) => command[0] === "eval" && command[1] === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT).length, 1);
  assert.equal(state.redis.commands.some((/** @type {unknown[]} */ command) => command[0] === "hGetAll"), false);
  assert.equal(sessionCalls, 1);
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workflows_listed",
    fields: { request_id: "rid-list", namespace: "demo", count: 1 },
  }]);
});

test("definition lists sort names and prefer active metadata over retained definitions", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("worker:demo:api:v:2", { __meta__: JSON.stringify({
    workflows: [{ name: "zeta", binding: "ZETA", className: "CurrentWorkflow", workflowKey: ACTIVE_WORKFLOW_KEY }],
  }) });
  state.redis.hashes.set("wf:defs:demo:api", {
    zeta: JSON.stringify({ workflowKey: ACTIVE_WORKFLOW_KEY, className: "PreviousWorkflow" }),
    alpha: JSON.stringify({ workflowKey: RETIRED_WORKFLOW_KEY, className: "RetiredWorkflow" }),
  });
  const response = await handle({ method: "GET", url: new URL("http://control/ns/demo/workflows"), ns: "demo", subPath: [], requestId: "rid-definition-order" });
  await assertJsonResponse(response, 200, {
    namespace: "demo",
    cursor: null,
    workflows: [
      { namespace: "demo", worker: "api", activeVersion: "v2", name: "alpha", binding: null, className: "RetiredWorkflow", workflowKey: RETIRED_WORKFLOW_KEY, retired: true },
      { namespace: "demo", worker: "api", activeVersion: "v2", name: "zeta", binding: "ZETA", className: "CurrentWorkflow", workflowKey: ACTIVE_WORKFLOW_KEY },
    ],
  });
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
  configureWorkflowListSession(state, (session) => {
    const originalRead = session.eval.bind(session);
    session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
      const snapshot = await originalRead(script, keys, args);
      if (script === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT) {
        redis.hashes.set("routes:demo", {});
        redis.hashes.set("worker:demo:api:v:2", {});
      }
      return snapshot;
    };
  });

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
    cursor: null,
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
    redis.hashes.set("routes:demo", { api: "v2", billing: "v1" });
    let scans = 0;
    configureWorkflowListSession(state, (session) => {
      const originalRead = session.eval.bind(session);
      session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
        const snapshot = await originalRead(script, keys, args);
        if (script === WORKFLOW_ROUTE_PAGE_SCRIPT) {
          scans += 1;
          redis.hashes.set("routes:demo", { api: "v2", billing: `v${scans + 1}` });
        }
        return snapshot;
      };
    });

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
    assert.equal(scans, 1);
  });
}

test("workflows handler returns contention when list routes never stabilize", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  const meta = redis.hashes.get("worker:demo:api:v:2")["__meta__"];
  redis.hashes.set("worker:demo:api:v:3", { "__meta__": meta });
  let snapshots = 0;
  configureWorkflowListSession(state, (session) => {
    const originalRead = session.eval.bind(session);
    session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
      const snapshot = await originalRead(script, keys, args);
      if (script === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT) {
        snapshots += 1;
        redis.hashes.set("routes:demo", { api: `v${snapshots + 2}` });
      }
      return snapshot;
    };
  });

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
  assert.equal(snapshots, 2);
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
    cursor: null,
  });
  assert.equal(state.redis.commands.some((/** @type {unknown[]} */ command) => command[0] === "eval" && command[1] === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT), false);
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workflows_listed",
    fields: { request_id: "rid-empty-list", namespace: "demo", count: 0 },
  }]);
});

test("definition pagination covers retired names and bounded worker batches without duplicates", async () => {
  const state = resetWorkflowsHandlerState();
  const meta = state.redis.hashes.get("worker:demo:api:v:2").__meta__;
  state.redis.hashes.set("wf:defs:demo:api", {
    orders: JSON.stringify({ workflowKey: ACTIVE_WORKFLOW_KEY, className: "OrderWorkflow" }),
    retired: JSON.stringify({ workflowKey: RETIRED_WORKFLOW_KEY, className: "OldOrderWorkflow" }),
  });
  const routes = state.redis.hashes.get("routes:demo");
  for (let index = 0; index < 150; index += 1) {
    const worker = `worker-${String(index).padStart(3, "0")}`;
    routes[worker] = "v1";
    state.redis.hashes.set(`worker:demo:${worker}:v:1`, { __meta__: index === 149 ? meta : '{"workflows":[]}' });
  }
  /** @type {string[]} */
  const seen = [];
  let cursor = "";
  let pages = 0;
  do {
    const commandsBefore = state.redis.commands.length;
    const url = new URL("http://control/ns/demo/workflows");
    url.searchParams.set("limit", "1");
    if (cursor) url.searchParams.set("cursor", cursor);
    const response = await handle({ method: "GET", url, ns: "demo", subPath: [], requestId: "rid-definition-pages" });
    const body = await readJsonResponse(response, 200);
    const snapshots = state.redis.commands.slice(commandsBefore).filter((/** @type {unknown[]} */ entry) => entry[0] === "eval" && entry[1] === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT);
    assert.ok(snapshots.length <= 16);
    assert.ok(body.workflows.length <= 1);
    for (const entry of body.workflows) seen.push(`${entry.worker}/${entry.name}/${entry.retired === true}`);
    cursor = body.cursor;
    pages += 1;
    assert.ok(pages < 30);
  } while (cursor !== null);
  assert.deepEqual(seen, ["api/orders/false", "api/retired/true", "worker-149/orders/false"]);
  assert.ok(pages > 3);
});

test("definition cursors reject snapshot drift rather than splicing changed definitions", async () => {
  const state = resetWorkflowsHandlerState();
  state.redis.hashes.set("wf:defs:demo:api", {
    orders: JSON.stringify({ workflowKey: ACTIVE_WORKFLOW_KEY, className: "OrderWorkflow" }),
    retired: JSON.stringify({ workflowKey: RETIRED_WORKFLOW_KEY, className: "OldOrderWorkflow" }),
  });
  const first = await handle({ method: "GET", url: new URL("http://control/ns/demo/workflows?limit=1"), ns: "demo", subPath: [], requestId: "rid-first" });
  const cursor = (await readJsonResponse(first, 200)).cursor;
  assert.equal(typeof cursor, "string");
  state.redis.hashes.get("wf:defs:demo:api").retired = JSON.stringify({ workflowKey: RETIRED_WORKFLOW_KEY, className: "ChangedWorkflow" });
  const url = new URL("http://control/ns/demo/workflows?limit=1");
  url.searchParams.set("cursor", cursor);
  const changed = await handle({ method: "GET", url, ns: "demo", subPath: [], requestId: "rid-next" });
  assert.equal((await readJsonResponse(changed, 503)).error, "workflow_metadata_contention");
});

test("definition query rejects invalid limits and cursors before Redis reads", async () => {
  for (const query of ["limit=1001", "limit=0", "cursor=not-a-cursor", `cursor=${"x".repeat(2049)}`]) {
    const state = resetWorkflowsHandlerState();
    const response = await handle({ method: "GET", url: new URL(`http://control/ns/demo/workflows?${query}`), ns: "demo", subPath: [], requestId: "rid-invalid-definition-page" });
    assert.equal((await readJsonResponse(response, 400)).error, "invalid_request");
    assert.equal(state.redis.commands.length, 0);
  }
});

test("workflows handler preserves metadata-unavailable error shape for batched reads", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  redis.hashes.set("routes:demo", { api: "v2", billing: "v5" });
  configureWorkflowListSession(state, (session) => {
    const originalRead = session.eval.bind(session);
    session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
      if (script === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT) throw new Error("redis unavailable");
      return originalRead(script, keys, args);
    };
  });

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

test("workflows handler wraps combined metadata batch read failures", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = /** @type {any} */ (state.redis);
  redis.hashes.set("routes:demo", { api: "v2", billing: "v5" });
  configureWorkflowListSession(state, (session) => {
    const originalRead = session.eval.bind(session);
    session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
      if (script === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT) throw new Error("defs unavailable");
      return originalRead(script, keys, args);
    };
  });

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

test("workflows handler preserves instance list cursor and payloads under a finite deadline", async () => {
  const state = resetWorkflowsHandlerState();
  const result = {
    instances: [{ id: "order-1", status: "completed", output: { text: "\u4e2d" }, error: null }],
    cursor: "17",
  };
  state.workflows = {
    async fetch(/** @type {string} */ url, /** @type {RequestInit} */ init) {
      assert.ok(init.signal instanceof AbortSignal);
      assert.equal(new Headers(init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
      assert.equal(new Headers(init.headers).get("x-request-id"), "rid-instances");
      assert.equal(url, "http://workflows/internal/workflows/instances");
      assert.deepEqual(parseJsonObjectRequestBody(init, "workflow list").options, { limit: 1000, cursor: "12" });
      return Response.json(result);
    },
  };
  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances?limit=1000&cursor=12"),
    ns: "demo",
    subPath: ["api", "orders", "instances"],
    requestId: "rid-instances",
  });
  await assertJsonResponse(response, 200, result);
});

test("workflows instance list deadline cancels a stalled body and hides the failure", async () => {
  const state = resetWorkflowsHandlerState();
  const controller = new AbortController();
  let cancelled = false;
  state.workflows = {
    async fetch(/** @type {string} */ _url, /** @type {RequestInit} */ init) {
      assert.equal(init.signal, controller.signal);
      return new Response(new ReadableStream({
        cancel() { cancelled = true; },
      }));
    },
  };
  await withMockedProperty(AbortSignal, "timeout", (delay) => {
    assert.equal(delay, 5_000);
    return controller.signal;
  }, async () => {
    const pending = handle({
      method: "GET",
      url: new URL("http://control/ns/demo/workflows/api/orders/instances"),
      ns: "demo",
      subPath: ["api", "orders", "instances"],
      requestId: "rid-list-deadline",
    });
    await new Promise((resolve) => setImmediate(resolve));
    controller.abort(new Error("private stalled Workflow transport"));
    await assertJsonResponse(await pending, 503, {
      error: "workflow_internal_dispatch_failed",
      message: "Internal error",
    });
  });
  assert.equal(cancelled, true);
});

test("workflows instance lists preserve the bounded JSON bytes without number re-encoding", async () => {
  const state = resetWorkflowsHandlerState();
  const wire = '{"instances":[{"id":"order-1","status":"completed","output":1e20,"error":null}],"cursor":null}';
  state.workflows = { fetch: async () => new Response(wire) };
  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances"),
    ns: "demo",
    subPath: ["api", "orders", "instances"],
    requestId: "rid-list-wire",
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "application/json");
  assert.equal(await response.text(), wire);
});

test("workflows instance list preserves sanitized backend errors", async () => {
  const state = resetWorkflowsHandlerState();
  state.workflows = { fetch: async () => Response.json({
    error: "workflow_payload_missing", message: "private missing payload reference",
  }, { status: 500 }) };
  const response = await handle({
    method: "GET",
    url: new URL("http://control/ns/demo/workflows/api/orders/instances"),
    ns: "demo",
    subPath: ["api", "orders", "instances"],
    requestId: "rid-list-payload-missing",
  });
  await assertJsonResponse(response, 500, {
    error: "workflow_payload_missing",
    message: "Internal error",
    upstream_status: 500,
  });
});

test("definition snapshots retry route drift before reading stale metadata", async () => {
  const state = resetWorkflowsHandlerState();
  const redis = state.redis;
  const meta = redis.hashes.get("worker:demo:api:v:2").__meta__;
  redis.hashes.set("worker:demo:api:v:3", { __meta__: meta });
  let snapshots = 0;
  configureWorkflowListSession(state, (session) => {
    const originalRead = session.eval.bind(session);
    session.eval = async (/** @type {string} */ script, /** @type {string[]} */ keys, /** @type {unknown[]} */ args) => {
      if (script === WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT && snapshots++ === 0) {
        redis.hashes.set("routes:demo", { api: "v3" });
        redis.hashes.set("worker:demo:api:v:2", { __meta__: "stale invalid metadata" });
      }
      return originalRead(script, keys, args);
    };
  });
  const response = await handle({ method: "GET", url: new URL("http://control/ns/demo/workflows"), ns: "demo", subPath: [], requestId: "rid-atomic-snapshot" });
  const body = await readJsonResponse(response, 200);
  assert.equal(body.workflows.length, 1);
  assert.equal(body.workflows[0].activeVersion, "v3");
  assert.equal(snapshots, 2);
});

test("a full definition page needs at most one snapshot call per worker plus two route scans", async () => {
  const state = resetWorkflowsHandlerState();
  const meta = state.redis.hashes.get("worker:demo:api:v:2").__meta__;
  const routes = state.redis.hashes.get("routes:demo");
  for (let index = 0; index < 15; index += 1) {
    const worker = `worker-${index}`;
    routes[worker] = "v1";
    state.redis.hashes.set(`worker:demo:${worker}:v:1`, { __meta__: meta });
  }
  const response = await handle({ method: "GET", url: new URL("http://control/ns/demo/workflows"), ns: "demo", subPath: [], requestId: "rid-snapshot-budget" });
  const body = await readJsonResponse(response, 200);
  assert.equal(body.workflows.length, 16);
  assert.equal(body.cursor, null);
  const reads = state.redis.commands.filter((/** @type {unknown[]} */ command) => ["eval", "hGet", "hGetAll"].includes(String(command[0])));
  assert.ok(reads.length <= 18, `definition page used ${reads.length} Redis calls`);
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
