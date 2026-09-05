// Multi-module bundles: text/json/wasm/data round-trip through Redis +
// workerLoader without corruption. Assumes compose stack.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import http from "node:http";
import {
  GATEWAY_HOST,
  GATEWAY_PORT,
  deployAndPromote,
  gatewayFetch,
  responseJson,
  setupIntegrationSuite,
  uniqueNs,
  withResponseJsonAccessors,
} from "./helpers/index.js";

setupIntegrationSuite();

const WORKERD_COMPAT_WORKER = readFileSync(
  new URL("../../test-workers/workerd-compat/src/index.js", import.meta.url),
  "utf8"
);
const HOST_BINDING_ENV_WORKER = readFileSync(
  new URL("../../test-workers/host-binding-env/src/index.js", import.meta.url),
  "utf8"
);
const SHARED_BASE64_WORKER = readFileSync(
  new URL("../../shared/base64.js", import.meta.url),
  "utf8"
);
const BASE64_PROBE_WORKER = `
  import { base64ToBytes, canonicalBase64ToBytes } from "./base64.js";
  function probe(decode, value) {
    try {
      return { value, ok: true, bytes: Array.from(decode(value)) };
    } catch {
      return { value, ok: false };
    }
  }
  export default {
    fetch() {
      return Response.json({
        bufferType: typeof Buffer,
        forgiving: ["Zg==", "Zg", "Z g ==", "Zh==", "Zm9=", "%%%", "Zg=", "-_8="].map(
          (value) => probe(base64ToBytes, value)
        ),
        canonical: ["", "Zg==", "Zm8=", "Zm9v", "Zg", "Z g==", "Zh==", "Zm9=", "-_8="].map(
          (value) => probe(canonicalBase64ToBytes, value)
        ),
      });
    },
  };
`;

/**
 * @typedef {{
 *   listeners: string[],
 *   reports: boolean[],
 *   threw: boolean,
 *   caughtSameError: boolean,
 *   dispatchResult: boolean | null,
 * }} ListenerExceptionResult
 *
 * @typedef {{
 *   moduleClock: { dateNow: number, dateValue: number, performanceNow: number },
 *   requestClock: { dateNow: number, dateValue: number, performanceNow: number },
 *   abortType: string,
 *   tracing: {
 *     invocationSpanPresent: boolean,
 *     startSpanType: string,
 *     setAttributeChained: boolean,
 *     setAttributesChained: boolean,
 *     startSpanPreservesActive: boolean,
 *     activeSpan: {
 *       beforeAwait: boolean,
 *       afterAwait: boolean,
 *       recordException: {
 *         errorReturnedVoid: boolean,
 *         stringReturnedVoid: boolean,
 *         codeZeroReturnedVoid: boolean,
 *       },
 *       callerPreservedWhilePending: boolean,
 *       callerRestoredAfterAwait: boolean,
 *     },
 *   },
 *   listenerExceptions: {
 *     eventTarget: ListenerExceptionResult,
 *     abortSignal: ListenerExceptionResult & { aborted: boolean, reasonPreserved: boolean },
 *   },
 *   htmlRewriter: { uppercaseAttributeMatches: number },
 *   byob: { firstDone: boolean, firstBytes: number[], finalDone: boolean, finalBytes: number[] },
 *   importMetaPathHelpers: { dirname: string, filename: string },
 *   nodeGlobals: Record<string, string>,
 *   urlParsing: { nonUts46XnLabel: string },
 * }} WorkerdCompatResult
 */

/**
 * @typedef {{ value: string, ok: boolean, bytes?: number[] }} Base64ProbeEntry
 * @typedef {{
 *   bufferType: string,
 *   forgiving: Base64ProbeEntry[],
 *   canonical: Base64ProbeEntry[],
 * }} Base64ProbeResult
 */

/** @param {string} ns @param {string} path @param {Buffer} body */
function pendingByobGatewayPost(ns, path, body) {
  return new Promise((resolve, reject) => {
    const request = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "POST",
      path,
      headers: {
        Host: `${ns}.workers.local`,
        "content-type": "application/octet-stream",
        "content-length": String(body.byteLength),
      },
      agent: false,
    }, (response) => {
      if (response.headers["x-byob-read-pending"] !== "1") {
        response.resume();
        request.destroy();
        reject(new Error("worker did not confirm a pending BYOB read before request body delivery"));
        return;
      }
      /** @type {Buffer[]} */
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(withResponseJsonAccessors({
          status: response.statusCode,
          body: text,
          text: () => text,
        }, "pending BYOB gateway response body"));
      });
      response.on("error", reject);
      response.on("close", () => {
        if (!response.complete) {
          reject(new Error("pending BYOB gateway response closed before completion"));
        }
      });
      request.end(body);
    });
    request.on("error", reject);
    request.flushHeaders();
  });
}

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
    {
      name: "spec-compliant-dispatch-exceptions",
      compatibilityDate: "2026-04-24",
      compatibilityFlags: ["spec_compliant_dispatch_exceptions"],
    },
    { name: "node-default", compatibilityDate: "2026-08-04", compatibilityFlags: [] },
    { name: "single-node-optout", compatibilityDate: "2026-08-04", compatibilityFlags: ["no_nodejs_compat"] },
    {
      name: "full-node-optout",
      compatibilityDate: "2026-08-04",
      compatibilityFlags: ["no_nodejs_compat", "no_nodejs_compat_v2"],
    },
    {
      name: "byob-pending-read",
      compatibilityDate: "2026-04-24",
      compatibilityFlags: ["streams_byob_reader_does_not_detach_buffer"],
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
      invocationSpanPresent: true,
      startSpanType: "function",
      setAttributeChained: true,
      setAttributesChained: true,
      startSpanPreservesActive: true,
      activeSpan: {
        beforeAwait: true,
        afterAwait: true,
        recordException: {
          errorReturnedVoid: true,
          stringReturnedVoid: true,
          codeZeroReturnedVoid: true,
        },
        callerPreservedWhilePending: true,
        callerRestoredAfterAwait: true,
      },
    });
    assert.deepEqual(result.htmlRewriter, {
      uppercaseAttributeMatches: 1,
    });
    assert.deepEqual(result.byob, {
      firstDone: false,
      firstBytes: [1, 2, 3, 4],
      finalDone: true,
      finalBytes: [],
    });
    assert.deepEqual(result.importMetaPathHelpers, {
      dirname: "undefined",
      filename: "undefined",
    });
    assert.deepEqual(result.urlParsing, {
      nonUts46XnLabel: "xn--pokxncvks",
    });
  }

  for (const variant of variants) {
    const reportsExceptions = variant.compatibilityFlags.includes("spec_compliant_dispatch_exceptions");
    const expected = {
      listeners: reportsExceptions ? ["first", "second"] : ["first"],
      reports: reportsExceptions ? [true] : [],
      threw: !reportsExceptions,
      caughtSameError: !reportsExceptions,
      dispatchResult: null,
    };
    assert.deepEqual(results[variant.name].listenerExceptions, {
      eventTarget: { ...expected, dispatchResult: reportsExceptions ? true : null },
      abortSignal: { ...expected, aborted: true, reasonPreserved: true },
    }, variant.name);
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
  assert.deepEqual(results["spec-compliant-dispatch-exceptions"].nodeGlobals, disabledGlobals);
  assert.deepEqual(results["byob-pending-read"].nodeGlobals, disabledGlobals);
  assert.deepEqual(results["node-default"].nodeGlobals, enabledGlobals);
  assert.deepEqual(results["single-node-optout"].nodeGlobals, enabledGlobals);
  assert.deepEqual(results["full-node-optout"].nodeGlobals, disabledGlobals);

  const resizeResponse = await pendingByobGatewayPost(
    ns,
    "/byob-pending-read?pendingByob=resize",
    Buffer.from([1, 2])
  );
  assert.equal(resizeResponse.status, 200);
  assert.deepEqual(await responseJson(resizeResponse), {
    done: false,
    bytes: [1, 2],
    resultByteOffset: 4,
    resultBufferByteLength: 6,
    originalBufferByteLength: 6,
    transferredByteLength: null,
  });

  const transferResponse = await pendingByobGatewayPost(
    ns,
    "/byob-pending-read?pendingByob=transfer",
    Buffer.from([1, 2, 3, 4])
  );
  assert.equal(transferResponse.status, 200);
  assert.deepEqual(await responseJson(transferResponse), {
    done: false,
    bytes: [],
    resultByteOffset: 0,
    resultBufferByteLength: 0,
    originalBufferByteLength: 0,
    transferredByteLength: 16,
  });

  const aborted = await gatewayFetch(ns, "/node-default?abort=1");
  assert.equal(aborted.status, 502);
  assert.equal((await responseJson(aborted)).error, "runtime_error");

  const healthy = await gatewayFetch(ns, "/node-default");
  assert.equal(healthy.status, 200);
});

test("shared base64 grammar agrees across workerd web and Node branches", async () => {
  const ns = uniqueNs("base64-grammar");
  for (const variant of [
    { name: "web", compatibilityDate: "2026-04-24" },
    { name: "node", compatibilityDate: "2026-08-04" },
  ]) {
    await deployAndPromote(ns, variant.name, {
      mainModule: "worker.js",
      modules: {
        "worker.js": BASE64_PROBE_WORKER,
        "base64.js": SHARED_BASE64_WORKER,
      },
      compatibilityDate: variant.compatibilityDate,
    });
  }

  /** @type {Record<string, Base64ProbeResult>} */
  const results = {};
  for (const name of ["web", "node"]) {
    const response = await gatewayFetch(ns, `/${name}`);
    assert.equal(response.status, 200);
    results[name] = /** @type {Base64ProbeResult} */ (await responseJson(response));
  }
  assert.equal(results.web.bufferType, "undefined");
  assert.equal(results.node.bufferType, "function");
  const { bufferType: _webBufferType, ...webResult } = results.web;
  const { bufferType: _nodeBufferType, ...nodeResult } = results.node;
  assert.deepEqual(webResult, nodeResult);
  assert.deepEqual(webResult, {
    forgiving: [
      { value: "Zg==", ok: true, bytes: [102] },
      { value: "Zg", ok: true, bytes: [102] },
      { value: "Z g ==", ok: true, bytes: [102] },
      { value: "Zh==", ok: true, bytes: [102] },
      { value: "Zm9=", ok: true, bytes: [102, 111] },
      { value: "%%%", ok: false },
      { value: "Zg=", ok: false },
      { value: "-_8=", ok: false },
    ],
    canonical: [
      { value: "", ok: true, bytes: [] },
      { value: "Zg==", ok: true, bytes: [102] },
      { value: "Zm8=", ok: true, bytes: [102, 111] },
      { value: "Zm9v", ok: true, bytes: [102, 111, 111] },
      { value: "Zg", ok: false },
      { value: "Z g==", ok: false },
      { value: "Zh==", ok: false },
      { value: "Zm9=", ok: false },
      { value: "-_8=", ok: false },
    ],
  });
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

test("host facades respect importable-env flags without exposing generic backends", async () => {
  const ns = uniqueNs("host-binding-env");
  for (const { worker, compatibilityFlags } of [
    { worker: "enabled", compatibilityFlags: [] },
    { worker: "disabled", compatibilityFlags: ["disallow_importable_env"] },
  ]) {
    await deployAndPromote(ns, worker, {
      mainModule: "worker.js",
      modules: { "worker.js": HOST_BINDING_ENV_WORKER },
      compatibilityDate: "2026-08-11",
      compatibilityFlags,
      bindings: {
        ROOM: { type: "do", className: "Room" },
      },
      workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
    });
  }

  const hidden = {
    doBackend: "undefined",
    ownerNetwork: "undefined",
    workflowsBackend: "undefined",
  };
  const positional = { room: "function", flow: "function" };
  const enabledModuleScope = {
    roomFacade: "function",
    roomTransport: "function",
    flowFacade: "function",
    flowTransport: "function",
  };

  const enabledHandlerResponse = await gatewayFetch(ns, "/enabled");
  assert.equal(enabledHandlerResponse.status, 200);
  const enabledHandler = await responseJson(enabledHandlerResponse);
  assert.deepEqual(enabledHandler, {
    positional,
    imported: positional,
    moduleScope: { ...enabledModuleScope, alarmTransport: "undefined" },
    moduleScopeCalls: { room: "rejected", flow: "rejected" },
    hidden,
  });
  const enabledDoResponse = await gatewayFetch(ns, "/enabled/do");
  assert.equal(enabledDoResponse.status, 200);
  const enabledDo = await responseJson(enabledDoResponse);
  assert.deepEqual(enabledDo, {
    positional,
    imported: positional,
    moduleScope: { ...enabledModuleScope, alarmTransport: "function" },
    moduleScopeCalls: { room: "rejected", flow: "rejected" },
    hidden,
  });

  /** @type {[string, Record<string, unknown>][]} */
  const abortCases = [
    ["/enabled/abort-do", { name: "AbortError", message: "binding caller aborted", dispatches: 0 }],
    ["/enabled/abort-do-nested", { name: "AbortError", message: "binding caller aborted", dispatches: 0 }],
    ["/enabled/abort-workflow", { name: "AbortError", message: "binding caller aborted" }],
  ];
  for (const [path, expected] of abortCases) {
    const response = await gatewayFetch(ns, path);
    assert.equal(response.status, 200, path);
    assert.deepEqual(await responseJson(response), expected, path);
  }

  const disabled = {
    positional,
    imported: { room: "undefined", flow: "undefined" },
    moduleScope: {
      roomFacade: "undefined",
      roomTransport: "undefined",
      flowFacade: "undefined",
      flowTransport: "undefined",
      alarmTransport: "undefined",
    },
    moduleScopeCalls: { room: "missing", flow: "missing" },
    hidden,
  };
  const disabledHandlerResponse = await gatewayFetch(ns, "/disabled");
  assert.equal(disabledHandlerResponse.status, 200);
  assert.deepEqual(await responseJson(disabledHandlerResponse), disabled);
  const disabledDoResponse = await gatewayFetch(ns, "/disabled/do");
  assert.equal(disabledDoResponse.status, 200);
  assert.deepEqual(await responseJson(disabledDoResponse), disabled);
});
