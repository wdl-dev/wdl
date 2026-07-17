import { test } from "node:test";
import assert from "node:assert/strict";

import { Workflow } from "../../runtime/workflows-client.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { withMockedProperty, withMockedPropertyDescriptor } from "../helpers/mock-global.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";

const workflowLimits = /** @type {{ createBatchMax: number }} */ (
  readRepositoryJson("tests/fixtures/workflow-limits.json")
);

/**
 * @param {Record<string, unknown>} [runtimeOptions]
 * @param {Record<string, unknown>} [metadataOverrides]
 */
function createWorkflowForTest(runtimeOptions = {}, metadataOverrides = {}) {
  return new Workflow({
    ns: "tenant-a",
    worker: "shop",
    version: "v7",
    name: "orders",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
    className: "OrderWorkflow",
    ...metadataOverrides,
  }, runtimeOptions);
}

test("Workflow facade does not expose private backend caller", () => {
  const workflow = createWorkflowForTest();

  assert.equal(Object.hasOwn(Workflow.prototype, "_call"), false);
  assert.equal("_call" in workflow, false);
  assert.equal(typeof /** @type {any} */ (workflow)._call, "undefined");
  assert.equal(Object.hasOwn(Workflow.prototype, "create"), true);
  assert.equal(typeof workflow.create, "function");
});

test("Workflow instances do not expose the private caller through patched Function.prototype.bind", async () => {
  /** @type {string[]} */
  const endpoints = [];
  const workflow = createWorkflowForTest({
    backend: {
      /** @param {string} url */
      async fetch(url) {
        endpoints.push(new URL(url).pathname);
        return Response.json({ id: "inst-1", status: "running" });
      },
    },
  });
  const originalBind = Function.prototype.bind;
  let privateBindCalls = 0;
  await withMockedProperty(Function.prototype, "bind", function (/** @type {any[]} */ ...args) {
    if (this.name === "#call") privateBindCalls += 1;
    return Reflect.apply(originalBind, this, args);
  }, async () => {
    const instance = await workflow.get("inst-1");
    await instance.status();
  });

  assert.equal(privateBindCalls, 0);
  assert.deepEqual(endpoints, [
    "/internal/workflows/get",
    "/internal/workflows/status",
  ]);
});

test("Workflow.create preserves runtime metadata from params override", async () => {
  /** @type {Record<string, unknown> | undefined} */
  let capturedRequestBody;
  let capturedUrl;
  /** @type {RequestInit | undefined} */
  let capturedInit;
  let fetchCalls = 0;
  const workflow = createWorkflowForTest({
    requestId: "runtime-request",
    backend: {
      /** @param {string} url @param {RequestInit} init */
      async fetch(url, init) {
        fetchCalls += 1;
        capturedUrl = url;
        capturedInit = init;
        capturedRequestBody = parseJsonObjectRequestBody(init, "workflow create request body");
        return Response.json({ id: String(capturedRequestBody.instanceId) });
      },
    },
  });

  await workflow.create({
    id: "inst-1",
    callback: { kind: "do", binding: "ROOMS", idFromName: "room-a" },
    params: {
      ns: "victim",
      worker: "victim-worker",
      workflowName: "evil",
      workflowKey: "wf_ffffffffffffffffffffffffffffffff",
    },
  });

  assert.equal(fetchCalls, 1);
  assert.equal(capturedUrl, "http://workflows/internal/workflows/create");
  assert.equal(capturedInit?.method, "POST");
  assert.equal(new Headers(capturedInit?.headers).get("x-request-id"), "runtime-request");
  assert.equal(typeof capturedInit?.body, "string");
  assert.deepEqual(capturedRequestBody, {
    ns: "tenant-a",
    worker: "shop",
    frozenVersion: "v7",
    workflowName: "orders",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
    className: "OrderWorkflow",
    requestId: "runtime-request",
    instanceId: "inst-1",
    params: {
      ns: "victim",
      worker: "victim-worker",
      workflowName: "evil",
      workflowKey: "wf_ffffffffffffffffffffffffffffffff",
    },
    retention: null,
    callback: { kind: "do", binding: "ROOMS", idFromName: "room-a" },
  });
});

test("Workflow request identity ignores tenant-patched JSON.stringify", async () => {
  let capturedBody = "";
  const workflow = createWorkflowForTest({
    backend: {
      /** @param {string} _url @param {RequestInit} init */
      async fetch(_url, init) {
        capturedBody = String(init.body);
        return new Response('{"id":"inst-1"}', {
          headers: { "content-type": "application/json" },
        });
      },
    },
  });
  let patchedCalls = 0;
  await withMockedProperty(JSON, "stringify", () => {
    patchedCalls += 1;
    return '{"ns":"victim","worker":"victim-worker"}';
  }, async () => {
    await workflow.create({ id: "inst-1" });
  });

  assert.equal(patchedCalls, 0);
  assert.deepEqual(JSON.parse(capturedBody), {
    instanceId: "inst-1",
    params: null,
    retention: null,
    callback: null,
    ns: "tenant-a",
    worker: "shop",
    frozenVersion: "v7",
    workflowName: "orders",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
    className: "OrderWorkflow",
    requestId: null,
  });
});

test("Workflow request identity ignores inherited Object.prototype.toJSON", async () => {
  let capturedBody = "";
  const workflow = createWorkflowForTest({
    backend: {
      /** @param {string} _url @param {RequestInit} init */
      async fetch(_url, init) {
        capturedBody = String(init.body);
        return new Response('{"id":"inst-2"}', {
          headers: { "content-type": "application/json" },
        });
      },
    },
  });
  let inheritedCalls = 0;
  await withMockedPropertyDescriptor(/** @type {any} */ (Object.prototype), "toJSON", {
    configurable: true,
    value() {
      inheritedCalls += 1;
      return { ns: "victim", worker: "victim-worker" };
    },
  }, async () => {
    await workflow.create({ id: "inst-2" });
  });

  assert.equal(inheritedCalls, 0);
  const body = JSON.parse(capturedBody);
  assert.equal(body.ns, "tenant-a");
  assert.equal(body.worker, "shop");
  assert.equal(body.frozenVersion, "v7");
  assert.equal(body.workflowKey, "wf_0123456789abcdef0123456789abcdef");
  assert.equal(body.className, "OrderWorkflow");
});

test("Workflow.create forwards explicit non-null retention", async () => {
  /** @type {Record<string, unknown> | undefined} */
  let capturedBody;
  let capturedUrl;
  const workflow = createWorkflowForTest({
    requestId: "runtime-request",
    backend: {
      /** @param {string} url @param {RequestInit} init */
      async fetch(url, init) {
        capturedUrl = url;
        capturedBody = parseJsonObjectRequestBody(init, "workflow create request body");
        return Response.json({ id: String(capturedBody.instanceId) });
      },
    },
  });

  await workflow.create({
    id: "inst-2",
    retention: "30d",
  });
  assert.equal(capturedUrl, "http://workflows/internal/workflows/create");
  assert.ok(capturedBody, "workflow backend request body should be captured");
  assert.equal(capturedBody.retention, "30d");
});

test("Workflow.createBatch rejects backend response entries without ids", async () => {
  const workflow = createWorkflowForTest({
    backend: {
      async fetch() {
        return Response.json({ instances: [{}] });
      },
    },
  });

  await assert.rejects(
    () => workflow.createBatch([{ id: "inst-1" }]),
    /** @param {unknown} error */
    (error) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /Workflow instance id must be a string/);
      assert.match(error.message, /entry 0/);
      return true;
    }
  );
});

test("Workflow.createBatch rejects backend response entries with non-string ids", async () => {
  const invalidIds = [123, null, false, { value: "inst-1" }, ["inst-1"]];
  for (const id of invalidIds) {
    const workflow = createWorkflowForTest({
      backend: {
        async fetch() {
          return Response.json({ instances: [{ id }] });
        },
      },
    });

    await assert.rejects(
      () => workflow.createBatch([{ id: "inst-1" }]),
      /Workflow instance id must be a string/,
      `backend id ${JSON.stringify(id)} should be rejected`
    );
  }
});

test("Workflow.createBatch rejects missing instances array", async () => {
  const invalidResponses = [
    { name: "missing instances field", response: {} },
    { name: "instances is null", response: { instances: null } },
    { name: "instances is not an array", response: { instances: "not-an-array" } },
    { name: "instances is a number", response: { instances: 123 } },
    { name: "instances is an object", response: { instances: { id: "inst-1" } } },
  ];

  for (const { name, response } of invalidResponses) {
    const workflow = createWorkflowForTest({
      backend: {
        async fetch() {
          return Response.json(response);
        },
      },
    });

    await assert.rejects(
      () => workflow.createBatch([{ id: "inst-1" }]),
      /Workflow createBatch response must include instances/,
      name
    );
  }
});

test("Workflow.createBatch accepts valid instances array", async () => {
  const workflow = createWorkflowForTest({
    backend: {
      async fetch() {
        return Response.json({
          instances: [{ id: "inst-1" }, { id: "inst-2" }],
        });
      },
    },
  });

  const instances = await workflow.createBatch([{ id: "inst-1" }, { id: "inst-2" }]);
  assert.deepEqual(instances.map((instance) => instance.id), ["inst-1", "inst-2"]);
});

test("Workflow.create rejects non-object success responses", async () => {
  const workflow = createWorkflowForTest({
    backend: {
      async fetch() {
        return Response.json([]);
      },
    },
  });

  await assert.rejects(
    () => workflow.create({ id: "inst-1" }),
    /Workflows backend returned an invalid response/
  );
});

test("Workflow.create supports omitted requestId in context", async () => {
  let capturedUrl;
  /** @type {Record<string, unknown> | undefined} */
  let capturedBody;
  const workflow = createWorkflowForTest({
    backend: {
      /** @param {string} url @param {RequestInit} init */
      async fetch(url, init) {
        capturedUrl = url;
        capturedBody = parseJsonObjectRequestBody(init, "workflow create request body");
        return Response.json({ id: String(capturedBody.instanceId) });
      },
    },
  });

  const created = await workflow.create({ id: "inst-3" });
  assert.equal(capturedUrl, "http://workflows/internal/workflows/create");
  assert.equal(created.id, "inst-3");
  assert.ok(capturedBody, "workflow backend request body should be captured");
  assert.equal(capturedBody.requestId, null);
});

test("Workflow.createBatch limit matches the cross-language fixture", async () => {
  const workflow = createWorkflowForTest();
  await assert.rejects(
    () => workflow.createBatch(Array.from({ length: workflowLimits.createBatchMax + 1 }, (_, index) => ({
      id: `inst-${index}`,
    }))),
    new RegExp(`exceeds ${workflowLimits.createBatchMax} item limit`),
  );
});

test("Workflow.createBatch accepts backend-skipped ids", async () => {
  /** @type {Record<string, unknown> | undefined} */
  let capturedBody;
  const workflow = createWorkflowForTest({
    backend: {
      /** @param {string} _url @param {RequestInit} init */
      async fetch(_url, init) {
        capturedBody = parseJsonObjectRequestBody(init, "workflow createBatch request body");
        return Response.json({ instances: [{ id: "inst-1" }] });
      },
    },
  });

  const instances = await workflow.createBatch([{ id: "inst-1" }, { id: "inst-2" }]);
  assert.ok(capturedBody, "workflow backend request body should be captured");
  const sentIds = /** @type {Array<{ instanceId: string }>} */ (capturedBody.entries)
    .map((entry) => entry.instanceId);
  assert.deepEqual(sentIds, ["inst-1", "inst-2"]);
  assert.equal(instances.length, 1);
  const instanceIds = instances.map((instance) => instance.id);
  assert.deepEqual(instanceIds, ["inst-1"]);
});
