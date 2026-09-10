// WDL Workflows metadata path: deploy-time parsing and Redis persistence.
// Execution is covered by the Workflow runtime suites.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminPost,
  adminFetch,
  readIntegrationJson,
  assertStatus,
  readMeta,
  uniqueNs,
  setupIntegrationSuite,
} from "./helpers/index.js";
import { redisHGetJson, redisEval, redisHSet, redisHGetAll, redisHStrLen, redisJsonMember } from "./helpers/redis.js";
import { bundleKey, routesKey } from "../../shared/worker-contract.js";
import { workflowDefinitionsUrl } from "../helpers/workflow-definitions.js";

const definitions = await import(workflowDefinitionsUrl);

setupIntegrationSuite();

const WORKER_CODE = `
export class OrderWorkflow {}
export class ReplacementWorkflow {}
export default { fetch() { return new Response("ok"); } };
`;

/** @param {string} ns @param {string} worker @param {string} workflowName */
function readWorkflowDef(ns, worker, workflowName) {
  return redisHGetJson(`wf:defs:${ns}:${worker}`, workflowName, {
    label: `wf:defs:${ns}:${worker} ${workflowName}`,
  });
}

test("deploy stores workflow metadata and wf:defs with stable workflow keys", async () => {
  const ns = uniqueNs("wfmeta");
  const first = await adminPost(`/ns/${ns}/worker/shop/deploy`, {
    code: WORKER_CODE,
    workflows: [
      { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    ],
  });
  assertStatus(first, 201, "initial workflow deploy");

  const firstMeta = readMeta(ns, "shop", first.json.version);
  assert.equal(firstMeta.workflows.length, 1);
  assert.deepEqual(
    {
      name: firstMeta.workflows[0].name,
      binding: firstMeta.workflows[0].binding,
      className: firstMeta.workflows[0].className,
    },
    { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
  );
  assert.match(firstMeta.workflows[0].workflowKey, /^wf_[0-9a-f]{32}$/);

  const firstDef = readWorkflowDef(ns, "shop", "orders");
  assert.deepEqual(firstDef, {
    workflowKey: firstMeta.workflows[0].workflowKey,
    className: "OrderWorkflow",
  });

  const second = await adminPost(`/ns/${ns}/worker/shop/deploy`, {
    code: WORKER_CODE,
    workflows: [
      { name: "orders", binding: "ORDERS", className: "ReplacementWorkflow" },
    ],
  });
  assertStatus(second, 201, "replacement workflow deploy");

  const secondMeta = readMeta(ns, "shop", second.json.version);
  assert.equal(secondMeta.workflows[0].workflowKey, firstMeta.workflows[0].workflowKey);
  assert.equal(secondMeta.workflows[0].className, "ReplacementWorkflow");

  const secondDef = readWorkflowDef(ns, "shop", "orders");
  assert.deepEqual(secondDef, {
    workflowKey: firstMeta.workflows[0].workflowKey,
    className: "ReplacementWorkflow",
  });
});

test("deploy rejects Cloudflare script_name workflows with a stable code", async () => {
  const ns = uniqueNs("wfscript");
  const res = await adminPost(`/ns/${ns}/worker/shop/deploy`, {
    code: WORKER_CODE,
    workflows: [
      {
        name: "orders",
        binding: "ORDERS",
        className: "OrderWorkflow",
        script_name: "other-worker",
      },
    ],
  });
  assert.equal(res.status, 400);
  assert.equal(res.json.error, "workflow_script_name_unsupported");
});

test("definition snapshot atomically checks the route and returns stored byte totals", async () => {
  const ns = uniqueNs("wfmetasnapshot");
  const deployed = await adminPost(`/ns/${ns}/worker/shop/deploy`, {
    code: WORKER_CODE,
    vars: { LABEL: "\u4e2d\u6587" },
    workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }],
  });
  assertStatus(deployed, 201);
  const version = deployed.json.version;
  assertStatus(await adminPost(`/ns/${ns}/worker/shop/promote`, { version }), 200);
  const defsKey = `wf:defs:${ns}:shop`;
  const metaKey = bundleKey(ns, "shop", version);
  const expectedBytes = redisHStrLen(metaKey, "__meta__") + Object.entries(redisHGetAll(defsKey))
    .reduce((total, [field, value]) => total + Buffer.byteLength(field) + Buffer.byteLength(value), 0);
  const script = `local snapshot = (function()\n${definitions.WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT}\nend)()
return cjson.encode({status=snapshot[1], bytes=snapshot[4], hasMeta=type(snapshot[2])=='string', hasDefs=next(snapshot[3])~=nil})`;
  const args = [String(definitions.WORKFLOW_DEFINITIONS_MAX_COUNT), String(definitions.WORKFLOW_DEFINITIONS_MAX_BYTES), String(definitions.WORKFLOW_LIST_META_MAX_BYTES), "shop", version];
  const keys = [defsKey, metaKey, routesKey(ns)];
  const snapshot = redisJsonMember(redisEval(script, keys, args), "Workflow snapshot");
  assert.deepEqual(snapshot, { status: 1, bytes: expectedBytes, hasMeta: true, hasDefs: true });
  args[4] = "v999";
  const changed = redisJsonMember(redisEval(script, keys, args), "changed Workflow snapshot");
  assert.deepEqual(changed, { status: 2, bytes: 0, hasMeta: false, hasDefs: false });
});

test("Workflow definition listing pages retained names and deploy admission bounds their growth", async () => {
  const ns = uniqueNs("wfdefpage");
  const first = await adminPost(`/ns/${ns}/worker/shop/deploy`, { code: WORKER_CODE,
    workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }],
  });
  assertStatus(first, 201);
  const promoted = await adminPost(`/ns/${ns}/worker/shop/promote`, { version: first.json.version });
  assertStatus(promoted, 200);
  const key = `wf:defs:${ns}:shop`;
  redisEval(`
for i = 1, 1023 do
  redis.call('HSET', KEYS[1], 'retired-' .. i,
    cjson.encode({workflowKey='wf_' .. string.format('%032x', i), className='OrderWorkflow'}))
end
return 1`, [key], []);
  const names = new Set();
  let cursor = "";
  let pages = 0;
  do {
    const path = `/ns/${ns}/workflows?limit=200${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
    const body = await readIntegrationJson(await adminFetch(path), 200);
    assert.ok(body.workflows.length <= 200);
    for (const entry of body.workflows) {
      assert.equal(names.has(entry.name), false);
      names.add(entry.name);
      assert.equal(entry.retired === true, entry.name !== "orders");
    }
    cursor = body.cursor;
    pages += 1;
    assert.ok(pages < 10);
  } while (cursor !== null);
  assert.equal(names.size, 1024);
  assert.equal(pages, 6);
  const rejected = await adminPost(`/ns/${ns}/worker/shop/deploy`, { code: WORKER_CODE,
    workflows: [{ name: "extra", binding: "EXTRA", className: "OrderWorkflow" }],
  });
  assertStatus(rejected, 413);
  assert.equal(rejected.json.error, "workflow_definitions_too_large");
  assert.equal(Number(redisEval("return redis.call('HLEN', KEYS[1])", [key], [])), 1024);
  const replaced = await adminPost(`/ns/${ns}/worker/shop/deploy`, { code: WORKER_CODE,
    workflows: [{ name: "orders", binding: "ORDERS", className: "ReplacementWorkflow" }],
  });
  assertStatus(replaced, 201);
  assert.equal(Number(redisEval("return redis.call('HLEN', KEYS[1])", [key], [])), 1024);
});

test("Workflow definition readers bound stored bytes before returning them from Valkey", async () => {
  const ns = uniqueNs("wfdefbytes");
  const first = await adminPost(`/ns/${ns}/worker/shop/deploy`, { code: WORKER_CODE,
    workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }],
  });
  assertStatus(first, 201);
  assertStatus(await adminPost(`/ns/${ns}/worker/shop/promote`, { version: first.json.version }), 200);
  const defs = `wf:defs:${ns}:shop`;
  redisEval("redis.call('HSET', KEYS[1], 'oversized', string.rep('x', 1048577)); return 1", [defs], []);
  const invalid = await readIntegrationJson(await adminFetch(`/ns/${ns}/workflows`), 500);
  assert.equal(invalid.error, "corrupt_meta");
  redisEval("redis.call('HDEL', KEYS[1], 'oversized'); return 1", [defs], []);
  const bundle = bundleKey(ns, "shop", first.json.version);
  const meta = readMeta(ns, "shop", first.json.version);
  try {
    redisEval("redis.call('HSET', KEYS[1], '__meta__', string.rep('x', 8388609)); return 1", [bundle], []);
    const tooLarge = await readIntegrationJson(await adminFetch(`/ns/${ns}/workflows`), 413);
    assert.equal(tooLarge.error, "workflow_listing_metadata_too_large");
  } finally {
    redisHSet(bundle, { __meta__: JSON.stringify(meta) });
  }
});
