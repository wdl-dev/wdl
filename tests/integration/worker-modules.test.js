// Multi-module bundles: text/json/wasm/data round-trip through Redis +
// workerLoader without corruption. Assumes compose stack.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  deployAndPromote,
  gatewayFetch,
  responseJson,
  setupIntegrationSuite,
  uniqueNs,
} from "./helpers/index.js";

setupIntegrationSuite();

const WORKERD_COMPAT_WORKER = readFileSync(
  new URL("../../test-workers/workerd-compat/src/index.js", import.meta.url),
  "utf8"
);

/**
 * @typedef {{
 *   moduleClock: { dateNow: number, dateValue: number, performanceNow: number },
 *   requestClock: { dateNow: number, dateValue: number, performanceNow: number },
 *   abortType: string,
 *   tracing: { startSpanType: string, setAttributeChained: boolean, setAttributesChained: boolean },
 *   nodeGlobals: Record<string, string>,
 * }} WorkerdCompatResult
 */

test("text + json + data bundled together", async () => {
  const pngBytes = [137, 80, 78, 71, 13, 10, 26, 10]; // PNG magic
  await deployAndPromote("modns1", "multi", {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        import config from "./config.json";
        import greeting from "./greeting.txt";
        import icon from "./icon.png";
        export default {
          async fetch(request) {
            const url = new URL(request.url);
            if (url.pathname === "/config") return Response.json(config);
            if (url.pathname === "/greeting") return new Response(greeting);
            if (url.pathname === "/icon") {
              return new Response(icon, { headers: { "content-type": "image/png" }});
            }
            return new Response("not found", { status: 404 });
          }
        };
      `,
      "config.json": { json: { name: "multi", version: 1 } },
      "greeting.txt": { text: "hello world" },
      "icon.png": { data_b64: Buffer.from(pngBytes).toString("base64") },
    },
  });

  const cfg = await gatewayFetch("modns1", "/multi/config");
  assert.equal(cfg.status, 200);
  assert.deepEqual(await responseJson(cfg), { name: "multi", version: 1 });

  const g = await gatewayFetch("modns1", "/multi/greeting");
  assert.equal(await g.text(), "hello world");

  const icon = await gatewayFetch("modns1", "/multi/icon");
  const iconBytes = new Uint8Array(await icon.arrayBuffer());
  assert.deepEqual(Array.from(iconBytes), pngBytes);
});

test("compatibilityDate and vars propagated", async () => {
  await deployAndPromote("modns2", "v", {
    mainModule: "worker.js",
    compatibilityDate: "2026-04-24",
    vars: { GREETING: "hi-from-vars" },
    modules: {
      "worker.js": `export default {
        fetch(request, env) {
          return new Response(env.GREETING || "(no var)");
        }
      };`,
    },
  });
  const res = await gatewayFetch("modns2", "/v");
  assert.equal(await res.text(), "hi-from-vars");
});

test("bundled workerd tenant runtime defaults and execution context APIs", async () => {
  const ns = uniqueNs("workerd-compat");
  const variants = [
    { name: "before-node-default", compatibilityDate: "2026-04-24", compatibilityFlags: [] },
    { name: "node-default", compatibilityDate: "2026-08-04", compatibilityFlags: [] },
    { name: "single-node-optout", compatibilityDate: "2026-08-04", compatibilityFlags: ["no_nodejs_compat"] },
    {
      name: "full-node-optout",
      compatibilityDate: "2026-08-04",
      compatibilityFlags: ["no_nodejs_compat", "no_nodejs_compat_v2"],
    },
  ];

  for (const variant of variants) {
    await deployAndPromote(ns, variant.name, {
      mainModule: "worker.js",
      modules: { "worker.js": WORKERD_COMPAT_WORKER },
      compatibilityDate: variant.compatibilityDate,
      compatibilityFlags: variant.compatibilityFlags,
    });
  }

  /** @type {Record<string, WorkerdCompatResult>} */
  const results = {};
  for (const variant of variants) {
    const response = await gatewayFetch(ns, `/${variant.name}`);
    assert.equal(response.status, 200);
    results[variant.name] = /** @type {WorkerdCompatResult} */ (await responseJson(response));
  }

  for (const result of Object.values(results)) {
    assert.deepEqual(result.moduleClock, {
      dateNow: 0,
      dateValue: 0,
      performanceNow: 0,
    });
    assert.ok(result.requestClock.dateNow > 0);
    assert.ok(result.requestClock.dateValue > 0);
    assert.ok(result.requestClock.performanceNow > 0);
    assert.equal(result.abortType, "function");
    assert.deepEqual(result.tracing, {
      startSpanType: "function",
      setAttributeChained: true,
      setAttributesChained: true,
    });
  }

  const disabledGlobals = {
    Buffer: "undefined",
    process: "undefined",
    global: "undefined",
    setImmediate: "undefined",
  };
  const enabledGlobals = {
    Buffer: "function",
    process: "object",
    global: "object",
    setImmediate: "function",
  };
  assert.deepEqual(results["before-node-default"].nodeGlobals, disabledGlobals);
  assert.deepEqual(results["node-default"].nodeGlobals, enabledGlobals);
  assert.deepEqual(results["single-node-optout"].nodeGlobals, enabledGlobals);
  assert.deepEqual(results["full-node-optout"].nodeGlobals, disabledGlobals);

  const aborted = await gatewayFetch(ns, "/node-default?abort=1");
  assert.equal(aborted.status, 502);
  assert.equal((await responseJson(aborted)).error, "runtime_error");

  const healthy = await gatewayFetch(ns, "/node-default");
  assert.equal(healthy.status, 200);
});

test("Workflow identity stays out of Node-compatible process.env", async () => {
  const ns = uniqueNs("workflow-process-env");
  await deployAndPromote(ns, "probe", {
    mainModule: "worker.js",
    compatibilityDate: "2026-08-04",
    modules: {
      "worker.js": `
        import { WorkflowEntrypoint } from "cloudflare:workers";
        export class ProbeWorkflow extends WorkflowEntrypoint {
          async run() { return null; }
        }
        export default {
          fetch(_request, env) {
            return Response.json({
              processEnvBinding: process.env.FLOW ?? null,
              facadeCreate: typeof env.FLOW?.create,
            });
          },
        };
      `,
    },
    workflows: [{ name: "probe", binding: "FLOW", className: "ProbeWorkflow" }],
  });

  const response = await gatewayFetch(ns, "/probe");
  assert.equal(response.status, 200);
  assert.deepEqual(await responseJson(response), {
    processEnvBinding: null,
    facadeCreate: "function",
  });
});
