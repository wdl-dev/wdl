// Workflows facade must be available inside Durable Object classes loaded by do-runtime.
import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  deployAndPromote,
  gatewayFetch,
  responseJson,
  uniqueNs,
  waitUntil,
  setupIntegrationSuite,
} from "./helpers/index.js";

setupIntegrationSuite();

const DO_WORKFLOW_WORKER = readFileSync(
  new URL("../../test-workers/do-workflow/src/index.js", import.meta.url),
  "utf8"
);

test("Durable Object classes create same-worker workflow instances", async () => {
  const ns = uniqueNs("wfdo");
  await deployAndPromote(ns, "shop", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKFLOW_WORKER },
    vars: { LABEL: "do-workflow-ok" },
    bindings: {
      LAUNCHER: { type: "do", className: "Launcher" },
      PROGRESS: { type: "do", className: "Progress" },
    },
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });

  const created = await gatewayFetch(ns, "/shop/create?id=do-order-1");
  assert.equal(created.status, 200, await created.text());
  const createdBody = await responseJson(created);
  assert.equal(createdBody.id, "do-order-1");
  assert.deepEqual({
    id: createdBody.status.id,
    output: createdBody.status.output,
    error: createdBody.status.error,
  }, {
    id: "do-order-1",
    output: null,
    error: null,
  });
  assert.ok(
    createdBody.status.status === "queued" || createdBody.status.status === "running",
    `expected created workflow to be queued or running, got ${createdBody.status.status}`
  );

  /** @type {any} */
  let completed;
  await waitUntil("workflow created from Durable Object completes", async () => {
    const status = await gatewayFetch(ns, "/shop/status?id=do-order-1");
    assert.equal(status.status, 200, await status.text());
    completed = await responseJson(status);
    return completed.status === "completed";
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(completed);
  assert.deepEqual(completed.output, {
    createdBy: "durable-object",
    id: "do-order-1",
    fromEnv: "do-workflow-ok",
  });
  assert.deepEqual(completed.steps.entries.map((/** @type {any} */ entry) => ({
    ordinal: entry.ordinal,
    name: entry.name,
    status: entry.status,
  })), [
    { ordinal: 0, name: "record", status: "completed" },
  ]);

  /** @type {any} */
  let progress;
  await waitUntil("workflow progress callback reaches same-worker Durable Object", async () => {
    const res = await gatewayFetch(ns, "/shop/progress/events");
    assert.equal(res.status, 200, await res.text());
    progress = await responseJson(res);
    return progress.events.includes("workflow_instance_created") &&
      progress.events.includes("workflow_step_completed") &&
      progress.events.includes("workflow_instance_completed");
  }, { timeoutMs: 60000, intervalMs: 250 });
  assert.ok(progress.events.includes("workflow_instance_created"));
  assert.ok(progress.events.includes("workflow_step_completed"));
});
