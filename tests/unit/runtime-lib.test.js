import { test } from "node:test";
import assert from "node:assert/strict";
import { parseBase64Json } from "../helpers/json-payload.js";
import { runtimeLibModuleDataUrl } from "../helpers/load-shared-module.js";

const {
  toBytes,
  bundleToWorkerCode,
  buildAssetUrl,
  buildQueueEnvelope,
  decodeQueueBody,
  decodeQueuedDispatchMessages,
  normalizeWorkflowNotifyBody,
  normalizeWorkflowRunBody,
  normalizeQueueDelaySeconds,
  normalizeQueuedDispatchBody,
  normalizeScheduledDispatchBody,
} = await import(runtimeLibModuleDataUrl());

const enc = new TextEncoder();

test("toBytes: string → Uint8Array utf8", () => {
  const r = toBytes("hi");
  assert.ok(r instanceof Uint8Array);
  assert.deepEqual(Array.from(r), [104, 105]);
});

test("toBytes: Uint8Array passes through", () => {
  const u = new Uint8Array([1, 2, 3]);
  assert.strictEqual(toBytes(u), u);
});

test("toBytes: ArrayBuffer wrapped", () => {
  const ab = new Uint8Array([9, 8]).buffer;
  const r = toBytes(ab);
  assert.deepEqual(Array.from(r), [9, 8]);
});

test("toBytes: DataView slice", () => {
  const backing = new Uint8Array([0, 1, 2, 3, 4]);
  const view = new DataView(backing.buffer, 1, 3);
  const r = toBytes(view);
  assert.deepEqual(Array.from(r), [1, 2, 3]);
});

test("toBytes: Int16Array", () => {
  const ia = new Int16Array([256]);
  const r = toBytes(ia);
  assert.equal(r.length, 2);
});

test("toBytes: unsupported throws", () => {
  assert.throws(() => toBytes(42), /KV put: value must be/);
  assert.throws(() => toBytes({}), /KV put: value must be/);
  assert.throws(() => toBytes(null), /KV put: value must be/);
});

test("normalizeScheduledDispatchBody validates and returns scheduler payload", () => {
  assert.deepEqual(
    normalizeScheduledDispatchBody({ scheduledTime: 123, cron: "* * * * *" }),
    { scheduledTime: 123, cron: "* * * * *" }
  );
  assert.throws(
    () => normalizeScheduledDispatchBody({ scheduledTime: "nope", cron: "* * * * *" }),
    { message: "scheduledTime must be a finite number" }
  );
  assert.throws(
    () => normalizeScheduledDispatchBody({ scheduledTime: 123, cron: "" }),
    { message: "cron must be a non-empty string" }
  );
});

test("normalizeQueuedDispatchBody validates and returns queue payload", () => {
  const messages = [{ id: "m1" }];
  assert.deepEqual(
    normalizeQueuedDispatchBody({ queue: "jobs", messages }),
    { queueName: "jobs", messages }
  );
  assert.throws(
    () => normalizeQueuedDispatchBody({ queue: "", messages }),
    { message: "queue must be a non-empty string" }
  );
  assert.throws(
    () => normalizeQueuedDispatchBody({ queue: "jobs", messages: {} }),
    { message: "messages must be an array" }
  );
});

function workflowBody(overrides = {}) {
  return {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "Order",
    workflowKey: "demo:shop:Order",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 1,
    createdAtMs: 1,
    runToken: "run-token",
    event: { payload: { ok: true } },
    callback: { url: "https://callback.workers.example" },
    progress: { event: "step" },
    ...overrides,
  };
}

test("normalizeWorkflowRunBody validates persisted identity integers", () => {
  assert.deepEqual(normalizeWorkflowRunBody(workflowBody()), {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "Order",
    workflowKey: "demo:shop:Order",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 1,
    createdAtMs: 1,
    runToken: "run-token",
    event: { payload: { ok: true } },
  });
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ generation: "0" })), /generation/);
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ generation: 0 })), /generation/);
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ generation: -1 })), /generation/);
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ createdAtMs: 0 })), /createdAtMs/);
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ createdAtMs: "123" })), /createdAtMs/);
});

test("normalizeWorkflowRunBody validates route worker identity grammar", () => {
  assert.equal(normalizeWorkflowRunBody(workflowBody({ ns: "__system__" })).ns, "__system__");
  for (const badNs of ["admin", "__platform__", "__community__", "bad:ns", "bad_ns", "-bad"]) {
    assert.throws(() => normalizeWorkflowRunBody(workflowBody({ ns: badNs })), /ns must be a route namespace/);
  }
  for (const badWorker of ["bad:name", "-bad", "bad.name"]) {
    assert.throws(() => normalizeWorkflowRunBody(workflowBody({ worker: badWorker })), /worker must be a worker name/);
  }
  assert.throws(() => normalizeWorkflowRunBody(workflowBody({ worker: "" })), /worker must be a non-empty string/);
  for (const badVersion of ["1", "v0", "v01", "latest"]) {
    assert.throws(
      () => normalizeWorkflowRunBody(workflowBody({ frozenVersion: badVersion })),
      /frozenVersion must be an immutable worker version/
    );
  }
  assert.throws(
    () => normalizeWorkflowRunBody(workflowBody({ frozenVersion: "" })),
    /frozenVersion must be a non-empty string/
  );
});

test("normalizeWorkflowNotifyBody validates persisted identity integers", () => {
  assert.deepEqual(normalizeWorkflowNotifyBody(workflowBody()), {
    ns: "demo",
    worker: "shop",
    frozenVersion: "v1",
    workflowName: "Order",
    workflowKey: "demo:shop:Order",
    className: "OrderWorkflow",
    instanceId: "inst-1",
    generation: 1,
    callback: { url: "https://callback.workers.example" },
    progress: { event: "step" },
  });
  assert.throws(() => normalizeWorkflowNotifyBody(workflowBody({ generation: "0" })), /generation/);
  assert.throws(() => normalizeWorkflowNotifyBody(workflowBody({ generation: 0 })), /generation/);
  assert.throws(() => normalizeWorkflowNotifyBody(workflowBody({ generation: 0.5 })), /generation/);
});

test("normalizeWorkflowNotifyBody validates route worker identity grammar", () => {
  assert.equal(normalizeWorkflowNotifyBody(workflowBody({ ns: "__system__" })).ns, "__system__");
  assert.throws(() => normalizeWorkflowNotifyBody(workflowBody({ ns: "__platform__" })), /ns must be a route namespace/);
  assert.throws(() => normalizeWorkflowNotifyBody(workflowBody({ worker: "bad:name" })), /worker must be a worker name/);
  assert.throws(
    () => normalizeWorkflowNotifyBody(workflowBody({ frozenVersion: "v0" })),
    /frozenVersion must be an immutable worker version/
  );
});

test("decodeQueuedDispatchMessages decodes runtime queue wire messages", () => {
  const messages = decodeQueuedDispatchMessages([
    {
      id: "m1",
      first_seen_ms: "123",
      attempts: "2",
      body_b64: btoa(JSON.stringify({ ok: true })),
      content_type: "json",
    },
  ]);
  assert.equal(messages[0].id, "m1");
  assert.equal(messages[0].timestamp.getTime(), 123);
  assert.equal(messages[0].attempts, 3);
  assert.deepEqual(messages[0].body, { ok: true });
});

test("decodeQueuedDispatchMessages maps internal retry count to worker attempts", () => {
  const messages = decodeQueuedDispatchMessages([
    {
      id: "first",
      first_seen_ms: "123",
      attempts: "0",
      body_b64: btoa("text"),
      content_type: "text",
    },
    {
      id: "bad-attempts",
      first_seen_ms: "123",
      attempts: "bogus",
      body_b64: btoa("text"),
      content_type: "text",
    },
  ]);
  assert.equal(messages[0].attempts, 1);
  assert.equal(messages[1].attempts, 1);
});

test("normalizeQueueDelaySeconds preserves explicit zero over defaults", () => {
  assert.equal(normalizeQueueDelaySeconds(undefined, 7), 7);
  assert.equal(normalizeQueueDelaySeconds(0, 7), 0);
  assert.equal(normalizeQueueDelaySeconds(86_400, 7), 86_400);
  assert.throws(() => normalizeQueueDelaySeconds(86_401, 0), /delaySeconds/);
});

test("buildQueueEnvelope writes the internal Redis queue envelope", () => {
  const built = buildQueueEnvelope({ ok: true }, "json", 123);
  assert.equal(built.entry.content_type, "json");
  assert.equal(built.entry.attempts, "0");
  assert.equal(built.entry.first_seen_ms, "123");
  assert.ok(built.entry.id);
  assert.deepEqual(
    parseBase64Json(built.entry.body_b64, "queue envelope body"),
    { ok: true }
  );
});

/**
 * @param {any} meta
 * @param {Record<string, Uint8Array | undefined>} files
 * @returns {Record<string, Uint8Array | undefined>}
 */
function mkBundle(meta, files) {
  /** @type {Record<string, Uint8Array | undefined>} */
  const out = { __meta__: enc.encode(JSON.stringify(meta)) };
  for (const [k, v] of Object.entries(files)) out[k] = v;
  return out;
}

test("bundleToWorkerCode: module + data + wasm + json + text + cjs", () => {
  const meta = {
    mainModule: "worker.js",
    modules: {
      "worker.js": { type: "module" },
      "icon.png": { type: "data" },
      "lib.wasm": { type: "wasm" },
      "config.json": { type: "json" },
      "readme.txt": { type: "text" },
      "commonjs.cjs": { type: "cjs" },
    },
  };
  const code = bundleToWorkerCode(
    mkBundle(meta, {
      "worker.js": enc.encode("export default {}"),
      "icon.png": new Uint8Array([1, 2]),
      "lib.wasm": new Uint8Array([0, 97, 115, 109]),
      "config.json": enc.encode('{"k":1}'),
      "readme.txt": enc.encode("hello"),
      "commonjs.cjs": enc.encode("module.exports = 1"),
    })
  );
  assert.equal(code.mainModule, "worker.js");
  assert.equal(code.compatibilityDate, "2026-04-24");
  assert.equal(code.modules["worker.js"], "export default {}");
  const dataModule = /** @type {{ data: Uint8Array }} */ (code.modules["icon.png"]);
  const wasmModule = /** @type {{ wasm: Uint8Array }} */ (code.modules["lib.wasm"]);
  const jsonModule = /** @type {{ json: unknown }} */ (code.modules["config.json"]);
  const textModule = /** @type {{ text: string }} */ (code.modules["readme.txt"]);
  const cjsModule = /** @type {{ cjs: string }} */ (code.modules["commonjs.cjs"]);
  assert.deepEqual(Array.from(dataModule.data), [1, 2]);
  assert.deepEqual(Array.from(wasmModule.wasm), [0, 97, 115, 109]);
  assert.deepEqual(jsonModule.json, { k: 1 });
  assert.equal(textModule.text, "hello");
  assert.equal(cjsModule.cjs, "module.exports = 1");
});

test("bundleToWorkerCode: py modules fail closed with WDL error", () => {
  assert.throws(
    () => bundleToWorkerCode(
      mkBundle(
        { mainModule: "worker.js", modules: { "worker.js": { type: "module" }, "mod.py": { type: "py" } } },
        { "worker.js": enc.encode("export default {}"), "mod.py": enc.encode("x = 1") }
      )
    ),
    /Module "mod\.py": Python Workers modules are not supported by WDL/
  );
});

test("bundleToWorkerCode: uses meta.compatibilityDate when set", () => {
  const code = bundleToWorkerCode(
    mkBundle(
      {
        mainModule: "w.js",
        compatibilityDate: "2024-01-01",
        modules: { "w.js": { type: "module" } },
      },
      { "w.js": enc.encode("x") }
    )
  );
  assert.equal(code.compatibilityDate, "2024-01-01");
});

test("bundleToWorkerCode: compatibilityFlags merge user-declared with old-date platform floor, meta is frozen", () => {
  const code = bundleToWorkerCode(
    mkBundle(
      {
        mainModule: "w.js",
        compatibilityDate: "2026-04-20",
        compatibilityFlags: ["nodejs_compat"],
        bindings: { KV: { type: "kv", id: "x" } },
        vars: { G: "hi" },
        modules: { "w.js": { type: "module" } },
      },
      { "w.js": enc.encode("x") }
    )
  );
  assert.deepEqual(code.compatibilityFlags, ["nodejs_compat", "enhanced_error_serialization"]);
  assert.deepEqual(/** @type {any} */ (code.meta.bindings), { KV: { type: "kv", id: "x" } });
  assert.deepEqual(code.meta.vars, { G: "hi" });
  assert.ok(Object.isFrozen(code.meta), "meta must be frozen");
  assert.ok(Object.isFrozen(code.meta.bindings), "nested bindings must be frozen");
  assert.ok(Object.isFrozen(code.meta.bindings.KV), "nested binding spec must be frozen");
  assert.throws(() => { /** @type {any} */ (code.meta.bindings).KV.id = "mutated"; }, TypeError);
});

test("bundleToWorkerCode: default compatibilityDate does not inject defaulted platform flag", () => {
  const code = bundleToWorkerCode(
    mkBundle(
      { mainModule: "w.js", modules: { "w.js": { type: "module" } } },
      { "w.js": enc.encode("x") }
    )
  );
  assert.equal(code.compatibilityDate, "2026-04-24");
  assert.deepEqual(code.compatibilityFlags, []);
});

test("bundleToWorkerCode: old compatibilityDate absent flags → floor only", () => {
  const code = bundleToWorkerCode(
    mkBundle(
      {
        mainModule: "w.js",
        compatibilityDate: "2026-04-20",
        modules: { "w.js": { type: "module" } },
      },
      { "w.js": enc.encode("x") }
    )
  );
  assert.deepEqual(code.compatibilityFlags, ["enhanced_error_serialization"]);
});

test("bundleToWorkerCode: compatibilityFlags already includes floor → no dup", () => {
  const code = bundleToWorkerCode(
    mkBundle(
      {
        mainModule: "w.js",
        compatibilityDate: "2026-04-20",
        compatibilityFlags: ["enhanced_error_serialization", "nodejs_compat"],
        modules: { "w.js": { type: "module" } },
      },
      { "w.js": enc.encode("x") }
    )
  );
  assert.deepEqual(code.compatibilityFlags, ["enhanced_error_serialization", "nodejs_compat"]);
});

test("bundleToWorkerCode: throws (doesn't silently drop) on malformed compatibilityFlags in bundle bytes", () => {
  assert.throws(
    () => bundleToWorkerCode(
      mkBundle(
        {
          mainModule: "w.js",
          compatibilityFlags: "nodejs_compat",
          modules: { "w.js": { type: "module" } },
        },
        { "w.js": enc.encode("x") }
      )
    ),
    /compatibilityFlags must be an array/
  );
  assert.throws(
    () => bundleToWorkerCode(
      mkBundle(
        {
          mainModule: "w.js",
          compatibilityFlags: [42],
          modules: { "w.js": { type: "module" } },
        },
        { "w.js": enc.encode("x") }
      )
    ),
    /entries must be non-empty strings/
  );
});

test("bundleToWorkerCode: experimental workerd compatibility flags fail closed", () => {
  assert.throws(
    () => bundleToWorkerCode(
      mkBundle(
        {
          mainModule: "w.js",
          compatibilityFlags: ["nodejs_compat", "unsafe_module"],
          modules: { "w.js": { type: "module" } },
        },
        { "w.js": enc.encode("x") }
      )
    ),
    /meta\.compatibilityFlags contains experimental workerd flag "unsafe_module"/
  );
});

test("bundleToWorkerCode: missing __meta__ throws", () => {
  assert.throws(
    () => bundleToWorkerCode({ "worker.js": enc.encode("x") }),
    /missing __meta__/
  );
});

test("bundleToWorkerCode: malformed metadata shape throws before loading modules", () => {
  for (const meta of [
    null,
    [],
    { mainModule: "w.js" },
    { mainModule: "", modules: { "w.js": { type: "module" } } },
    { mainModule: "w.js", modules: [] },
  ]) {
    assert.throws(
      () => bundleToWorkerCode(mkBundle(meta, { "w.js": enc.encode("x") })),
      /Bundle metadata is invalid/,
      `expected malformed metadata rejection for ${JSON.stringify(meta)}`
    );
  }
});

test("bundleToWorkerCode: missing module field throws", () => {
  assert.throws(
    () =>
      bundleToWorkerCode(
        mkBundle(
          { mainModule: "w.js", modules: { "w.js": { type: "module" }, "other.txt": { type: "text" } } },
          { "w.js": enc.encode("x") }
        )
      ),
    /missing module "other.txt"/
  );
});

test("bundleToWorkerCode: unknown module type throws", () => {
  assert.throws(
    () =>
      bundleToWorkerCode(
        mkBundle(
          { mainModule: "w.js", modules: { "w.js": { type: "unknown" } } },
          { "w.js": enc.encode("x") }
        )
      ),
    /unknown type "unknown"/
  );
  assert.throws(
    () =>
      bundleToWorkerCode(
        mkBundle(
          { mainModule: "w.js", modules: { "w.js": { type: "constructor" } } },
          { "w.js": enc.encode("x") }
        )
      ),
    /unknown type "constructor"/
  );
});

const PX = "assets/acme/my-worker/abc123/";
const PXS = "assets/acme/w/abc/";

test("buildAssetUrl: composes cdnBase + prefix + path", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PX, "logo.png"),
    `https://assets.example/${PX}logo.png`
  );
});

test("buildAssetUrl: strips trailing slashes from cdnBase", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example///", PXS, "a.css"),
    `https://assets.example/${PXS}a.css`
  );
});

test("buildAssetUrl: strips leading slashes from path", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "/css/app.css"),
    `https://assets.example/${PXS}css/app.css`
  );
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "///x"),
    `https://assets.example/${PXS}x`
  );
});

test("buildAssetUrl: nested paths pass through", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "fonts/inter/regular.woff2"),
    `https://assets.example/${PXS}fonts/inter/regular.woff2`
  );
});

test("buildAssetUrl: empty path yields prefix directory URL", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, ""),
    `https://assets.example/${PXS}`
  );
});

test("buildAssetUrl: missing cdnBase throws", () => {
  assert.throws(
    () => buildAssetUrl("", PXS, "x"),
    /cdnBase is not configured/
  );
  assert.throws(
    () => buildAssetUrl(undefined, PXS, "x"),
    /cdnBase is not configured/
  );
});

test("buildAssetUrl: missing or malformed prefix throws", () => {
  assert.throws(
    () => buildAssetUrl("https://assets.example", "", "x"),
    /prefix must be a non-empty string/
  );
  assert.throws(
    () => buildAssetUrl("https://assets.example", "assets/demo/app/abc", "x"),
    /prefix must be a non-empty string ending in '\/'/
  );
});

test("buildAssetUrl: non-string path throws", () => {
  assert.throws(
    () => buildAssetUrl("https://assets.example", PXS, 42),
    /path must be a string/
  );
});

test("buildAssetUrl: rejects traversal and empty middle segments", () => {
  for (const bad of ["..", "../x", "a/../b", "./x", "a//b", "a/./b"]) {
    assert.throws(
      () => buildAssetUrl("https://assets.example", PXS, bad),
      /invalid path segment/,
      `expected rejection for ${JSON.stringify(bad)}`
    );
  }
});

test("buildAssetUrl: percent-encodes query/fragment/space/unicode", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "foo?x=1"),
    `https://assets.example/${PXS}foo%3Fx%3D1`
  );
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "foo#bar"),
    `https://assets.example/${PXS}foo%23bar`
  );
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "has space.txt"),
    `https://assets.example/${PXS}has%20space.txt`
  );
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "日本語.txt"),
    `https://assets.example/${PXS}${encodeURIComponent("日本語.txt")}`
  );
});

test("buildAssetUrl: slashes separate segments, each encoded individually", () => {
  assert.strictEqual(
    buildAssetUrl("https://assets.example", PXS, "a b/c d.txt"),
    `https://assets.example/${PXS}a%20b/c%20d.txt`
  );
});

// encodeBody in runtime/bindings/queue.js uses String.fromCharCode + btoa
// over raw bytes — mirror that here so the round-trip is actually tested,
// not just Buffer.from("utf8") which skips the binary-safe path.
/** @param {Uint8Array} bytes */
function b64FromBytes(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}
/** @param {string} str */
function b64FromUtf8(str) {
  return b64FromBytes(new TextEncoder().encode(str));
}

test("decodeQueueBody: text contentType returns string", () => {
  assert.equal(decodeQueueBody(b64FromUtf8("hello"), "text"), "hello");
});

test("decodeQueueBody: json contentType parses the value", () => {
  const decoded = decodeQueueBody(b64FromUtf8(JSON.stringify({ k: 1, arr: [2, 3] })), "json");
  assert.deepEqual(decoded, { k: 1, arr: [2, 3] });
});

test("decodeQueueBody: bytes contentType preserves binary payload", () => {
  const payload = new Uint8Array([0, 1, 2, 255, 128, 0x0a]);
  const decoded = decodeQueueBody(b64FromBytes(payload), "bytes");
  assert.ok(decoded instanceof Uint8Array);
  assert.deepEqual(Array.from(decoded), Array.from(payload));
});

test("decodeQueueBody: unknown contentType throws (v8 path must not silently pass)", () => {
  assert.throws(
    () => decodeQueueBody(b64FromUtf8("x"), "v8"),
    /unsupported contentType/
  );
  assert.throws(
    () => decodeQueueBody(b64FromUtf8("x"), "avro"),
    /unsupported contentType/
  );
});

test("decodeQueueBody: empty/missing body_b64 yields empty output per contentType", () => {
  assert.equal(decodeQueueBody("", "text"), "");
  assert.equal(decodeQueueBody(/** @type {any} */ (undefined), "text"), "");
  const emptyBytes = decodeQueueBody("", "bytes");
  assert.equal(emptyBytes.length, 0);
});
