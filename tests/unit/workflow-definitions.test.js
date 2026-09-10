import { test } from "node:test";
import assert from "node:assert/strict";
import { workflowDefinitionsUrl, workflowDefinitionRedisEval } from "../helpers/workflow-definitions.js";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";
import { withMockedProperty } from "../helpers/mock-global.js";

const definitions = await import(workflowDefinitionsUrl);

test("Workflow definition budgets account replacement bytes and cumulative retired names", () => {
  const old = '{"workflowKey":"wf_old","className":"Flow"}';
  const snapshot = { count: definitions.WORKFLOW_DEFINITIONS_MAX_COUNT, bytes: definitions.WORKFLOW_DEFINITIONS_MAX_BYTES, values: [old] };
  assert.equal(definitions.workflowDefinitionUpdatesFit(snapshot, [["flow", old]]), true);
  assert.equal(definitions.workflowDefinitionUpdatesFit(snapshot, [["flow", `${old} `]]), false);
  assert.equal(definitions.workflowDefinitionUpdatesFit({ ...snapshot, values: [null] }, [["new", old]]), false);
});

test("Workflow declaration limits apply after materialized keys and to UTF-8 bytes", () => {
  assert.equal(definitions.workflowDeclarationsFit(Array(definitions.WORKFLOW_DEFINITIONS_MAX_COUNT + 1).fill({})), false);
  assert.equal(definitions.workflowDeclarationsFit([{ name: "flow", binding: "FLOW", className: "Flow" }]), true);
  assert.equal(definitions.workflowDeclarationsFit([{ className: "\u4e2d".repeat(definitions.WORKFLOW_DEFINITIONS_MAX_BYTES / 2) }]), false);
});

test("bounded Workflow snapshots expose only requested admission values", async () => {
  const redis = createFakeRedis(undefined, { eval: workflowDefinitionRedisEval });
  redis.hashes.set("defs", { selected: "value", unrelated: "not-json" });
  const result = await definitions.readWorkflowDefinitionAdmission(redis, "defs", ["selected", "constructor"]);
  assert.equal(result.status, 1);
  assert.equal(result.count, 2);
  assert.deepEqual(result.values, ["value", null]);
  redis.hashes.set("meta", { __meta__: "x".repeat(definitions.WORKFLOW_LIST_META_MAX_BYTES + 1) });
  redis.hashes.set("routes", { worker: "v1" });
  assert.equal((await definitions.readWorkflowDefinitionSnapshot(redis, "defs", "meta", { key: "routes", worker: "worker", version: "v1" })).status, -1);
});

test("Workflow snapshots return Lua byte accounting without re-encoding decoded strings", async () => {
  const redis = createFakeRedis(undefined, { eval: workflowDefinitionRedisEval });
  const meta = JSON.stringify({ note: "\u4e2d".repeat(2048) });
  const value = JSON.stringify({ className: "\u6587".repeat(2048) });
  redis.hashes.set("routes", { worker: "v1" });
  redis.hashes.set("defs", { flow: value });
  redis.hashes.set("meta", { __meta__: meta });
  const expectedBytes = Buffer.byteLength(meta) + Buffer.byteLength("flow") + Buffer.byteLength(value);
  await withMockedProperty(TextEncoder.prototype, "encode", () => {
    throw new Error("snapshot must not re-encode strings to count bytes");
  }, async () => {
    const snapshot = await definitions.readWorkflowDefinitionSnapshot(redis, "defs", "meta", { key: "routes", worker: "worker", version: "v1" });
    assert.equal(snapshot.status, 1);
    assert.equal(snapshot.bytes, expectedBytes);
    assert.equal(snapshot.metaRaw, meta);
    assert.deepEqual({ ...snapshot.defs }, { flow: value });
  });
});

test("Workflow snapshots report a changed route before reading its old version", async () => {
  const redis = createFakeRedis(undefined, { eval: workflowDefinitionRedisEval });
  redis.hashes.set("routes", { worker: "v2" });
  redis.hashes.set("meta", { __meta__: "x".repeat(definitions.WORKFLOW_LIST_META_MAX_BYTES + 1) });
  const snapshot = await definitions.readWorkflowDefinitionSnapshot(redis, "defs", "meta", { key: "routes", worker: "worker", version: "v1" });
  assert.equal(snapshot.status, 2);
  assert.equal(snapshot.bytes, 0);
  assert.equal(snapshot.metaRaw, null);
  assert.deepEqual({ ...snapshot.defs }, {});
});
