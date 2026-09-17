import { test } from "node:test";
import assert from "node:assert/strict";

import { Workflow, WorkflowInstance } from "../../runtime/workflows-client.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { withMockedProperty, withMockedPropertyDescriptor } from "../helpers/mock-global.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";

const workflowLimits = /** @type {{ createBatchMax: number }} */ (
  readRepositoryJson("tests/fixtures/workflow-limits.json")
);

/**
 * @param {Record<string, unknown>} [runtimeOptions]
 */
function createWorkflowForTest(runtimeOptions = {}) {
  const { backend, ...options } = runtimeOptions;
  return new Workflow(/** @type {any} */ (backend), options);
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

test("Workflow.create sends only public operation fields to its scoped backend", async () => {
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

test("Workflow operation fields ignore tenant-patched JSON.stringify", async () => {
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
  });
});

test("Workflow operation fields ignore inherited Object.prototype.toJSON", async () => {
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
  assert.deepEqual(JSON.parse(capturedBody), {
    instanceId: "inst-2",
    params: null,
    retention: null,
    callback: null,
  });
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

test("Workflow create APIs reject unsupported location hints before backend dispatch", async () => {
  let fetchCalls = 0;
  let getters = 0;
  const workflow = createWorkflowForTest({
    backend: {
      async fetch() {
        fetchCalls += 1;
        return Response.json({ id: "unreachable" });
      },
    },
  });

  for (const options of [
    { id: "inst-1", locationHint: "weur" },
    Object.create({ locationHint: "weur" }),
    new class {
      get locationHint() { getters += 1; throw new Error("must not evaluate locationHint"); }
    }(),
  ]) {
    await assert.rejects(
      () => workflow.create(options),
      /Workflow create options locationHint is not supported by WDL/
    );
    await assert.rejects(
      () => workflow.createBatch([options]),
      /Workflow createBatch entry locationHint is not supported by WDL/
    );
  }
  assert.equal(fetchCalls, 0);
  assert.equal(getters, 0);
});

test("Workflow lifecycle rejects unsupported options without invoking getters or the backend", async () => {
  let calls = 0;
  let getters = 0;
  const instance = new WorkflowInstance("inst-1", async () => {
    calls += 1;
    return {};
  });
  for (const [method, field] of /** @type {const} */ ([["terminate", "rollback"], ["restart", "from"]])) {
    for (const value of [true, false, undefined, { name: "charge", count: 1, type: "do" }]) {
      for (const options of [{ [field]: value }, Object.create({ [field]: value })]) {
        await assert.rejects(() => instance[method](options), {
          name: "TypeError",
          message: `Workflow ${method} options ${field} is not supported by WDL`,
        });
      }
    }
    await assert.rejects(() => instance[method]({
      get [field]() { getters += 1; throw new Error("must not evaluate an unsupported option"); },
    }), /is not supported by WDL/);
    await assert.rejects(() => instance[method](new class {
      get [field]() { getters += 1; throw new Error("must not evaluate an inherited option"); }
    }()), /is not supported by WDL/);
    for (const options of [true, "invalid", []]) {
      await assert.rejects(() => instance[method](options), /options must be an object/);
    }
  }
  assert.equal(calls, 0);
  assert.equal(getters, 0);
});

test("omitted Workflow lifecycle options do not inherit ambient option fields", async () => {
  let calls = 0;
  let getters = 0;
  const instance = new WorkflowInstance("inst-1", async () => {
    calls += 1;
    return {};
  });
  for (const [method, field] of /** @type {const} */ ([["terminate", "rollback"], ["restart", "from"]])) {
    await withMockedPropertyDescriptor(/** @type {any} */ (Object.prototype), field, {
      get() { getters += 1; throw new Error("must not evaluate ambient options"); },
    }, async () => {
      assert.equal(await instance[method](), instance);
      assert.equal(await instance[method](null), instance);
      await assert.rejects(() => instance[method]({}), /is not supported by WDL/);
    });
  }
  assert.equal(calls, 4);
  assert.equal(getters, 0);
});

test("Workflow lifecycle preserves ordinary calls with absent or empty options", async () => {
  /** @type {unknown[]} */
  const calls = [];
  const instance = new WorkflowInstance("inst-1", async (endpoint, fields) => {
    calls.push({ endpoint, fields });
    return {};
  });
  for (const method of /** @type {const} */ (["terminate", "restart"])) {
    for (const options of [undefined, null, {}]) {
      assert.equal(await instance[method](options), instance);
    }
  }
  assert.deepEqual(calls, ["terminate", "terminate", "terminate", "restart", "restart", "restart"].map(
    (endpoint) => ({ endpoint, fields: { instanceId: "inst-1" } })
  ));
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
  assert.equal(Object.hasOwn(capturedBody, "requestId"), false);
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
