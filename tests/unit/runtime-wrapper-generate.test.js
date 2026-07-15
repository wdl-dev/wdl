import { test } from "node:test";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "../../runtime/load/wrapper-generate.js";
import { applyModuleReplacements, moduleDataUrl } from "../helpers/load-shared-module.js";
import { installMockProperty } from "../helpers/mock-global.js";

const hostBindingRuntime = await import(moduleDataUrl(HOST_BINDING_RUNTIME_SOURCE));

function generatedWrappers() {
  return {
    abortOnly: generateAbortShimWrapperModule("worker.js"),
    hostBindings: generateHostBindingWrapperModule("worker.js", [], [], [], {}, []),
  };
}

/**
 * @param {string} source
 * @param {string} startMarker
 * @param {string} endMarker
 */
function sourceFragment(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing generated source marker: ${startMarker}`);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(end, -1, `missing generated source marker: ${endMarker}`);
  return source.slice(start, end);
}

test("generated wrapper flavors share the exact abort shim", () => {
  const { abortOnly, hostBindings } = generatedWrappers();
  const start = "// Reserved name (WDL_RESERVED_ENTRYPOINT_RE)";
  const end = "\n\nexport class __WdlWorkflowNotify__";

  assert.equal(
    sourceFragment(abortOnly, start, end),
    sourceFragment(hostBindings, start, end)
  );
});

test("generated wrapper flavors preserve default-export class detection", () => {
  const { abortOnly, hostBindings } = generatedWrappers();
  const sourceLine = "const source = Function.prototype.toString.call(raw);";
  const classTest = "/^\\s*class\\b/.test(source)";

  assert.equal(abortOnly.split(sourceLine).length - 1, 1);
  assert.match(abortOnly, new RegExp(`if \\(!${RegExp.escape(classTest)}\\)`));
  assert.match(hostBindings, /const source = __WdlHostRuntime__\.functionSource\(raw\);/);
  assert.match(hostBindings, /if \(__WdlHostRuntime__\.regexpTest\(\/\^\\s\*class\\b\/, source\)\)/);
});

test("host wrapper runtime captures platform intrinsics before user module evaluation", () => {
  const source = generateHostBindingWrapperModule("worker.js", [], [], ["ROOM"], {}, []);
  assert.ok(
    source.indexOf(`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";`) <
      source.indexOf('import * as __WdlUserModule__ from "./worker.js";')
  );
  assert.match(source, /import \* as __WdlHostRuntime__/);
  assert.doesNotMatch(source, /__wdlRunWithRequestContext/);
  for (const intrinsic of [
    "Array.prototype.forEach",
    "Function.prototype.toString",
    "Object.defineProperty",
    "Object.entries",
    "Object.keys",
    "AsyncLocalStorage.prototype.getStore",
    "AsyncLocalStorage.prototype.run",
    "Reflect.apply",
    "Reflect.get",
    "RegExp.prototype.test",
  ]) {
    assert.match(HOST_BINDING_RUNTIME_SOURCE, new RegExp(RegExp.escape(intrinsic)));
  }
  assert.doesNotMatch(source, /Object\.(?:defineProperty|entries|keys)\(/);
  assert.doesNotMatch(source, /for \(const .* of /);
  assert.doesNotMatch(source, /Function\.prototype\.toString\.call/);
});

test("host wrapper request context is invocation-local and preserves return identity", async () => {
  const firstStarted = Promise.withResolvers();
  const releaseFirst = Promise.withResolvers();
  const firstResult = hostBindingRuntime.runWithRequestContext("rid-first", () => {
    const result = (async () => {
      assert.equal(hostBindingRuntime.currentRequestId(), "rid-first");
      firstStarted.resolve(undefined);
      await releaseFirst.promise;
      return hostBindingRuntime.currentRequestId();
    })();
    assert.equal(hostBindingRuntime.currentRequestId(), "rid-first");
    return result;
  });

  await firstStarted.promise;
  assert.equal(hostBindingRuntime.currentRequestId(), null);
  const secondResult = hostBindingRuntime.runWithRequestContext("rid-second", async () => {
    await Promise.resolve();
    return hostBindingRuntime.currentRequestId();
  });
  assert.equal(await secondResult, "rid-second");
  releaseFirst.resolve(undefined);
  assert.equal(await firstResult, "rid-first");
  assert.equal(hostBindingRuntime.currentRequestId(), null);

  const nativePromise = Promise.resolve("same");
  assert.equal(
    hostBindingRuntime.runWithRequestContext("rid-identity", () => nativePromise),
    nativePromise
  );
});

test("host wrapper request context inherits when a nested call has no new id", () => {
  const nested = hostBindingRuntime.runWithRequestContext("rid-outer", () =>
    hostBindingRuntime.runWithRequestContext(null, () => hostBindingRuntime.currentRequestId()));

  assert.equal(nested, "rid-outer");
  assert.equal(hostBindingRuntime.currentRequestId(), null);
});

test("generated host wrappers alias legal entrypoint names without declaration collisions", async () => {
  const entrypointNames = [
    "user",
    "WorkerEntrypoint",
    "abortIsolate",
    "withRequestContext",
    "wrapEnv",
    "wrapClassInstance",
    "D1Database",
    "R2Bucket",
    "DurableObjectNamespace",
    "Workflow",
  ];
  const userUrl = moduleDataUrl(`
    ${entrypointNames.map((name) => `export class ${name} {}`).join("\n")}
    export default {};
  `);
  const cloudflareUrl = moduleDataUrl(`
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
  `);
  const d1Url = moduleDataUrl("export class D1Database {}");
  const r2Url = moduleDataUrl("export class R2Bucket {}");
  const doUrl = moduleDataUrl("export class DurableObjectNamespace {}");
  const workflowUrl = moduleDataUrl("export class Workflow {}");
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule(
      "worker.js",
      ["DB"],
      ["BUCKET"],
      ["ROOM"],
      { FLOW: { className: "Workflow" } },
      entrypointNames
    ),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_SOURCE))}`],
      ['from "./_wdl-d1-client.js"', `from ${JSON.stringify(d1Url)}`],
      ['from "./_wdl-r2-client.js"', `from ${JSON.stringify(r2Url)}`],
      ['from "./_wdl-do-client.js"', `from ${JSON.stringify(doUrl)}`],
      ['from "./_wdl-workflows-client.js"', `from ${JSON.stringify(workflowUrl)}`],
      ['from "./worker.js"', `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  const userModule = await import(userUrl);

  for (const name of entrypointNames) {
    assert.equal(wrapped[name].name, name);
    assert.ok(new wrapped[name]({}, {}) instanceof userModule[name]);
  }
});

test("host wrapper context uses captured AsyncLocalStorage intrinsics", async () => {
  const restoreRun = installMockProperty(AsyncLocalStorage.prototype, "run", () => {
    throw new Error("live AsyncLocalStorage.run must not run");
  });
  const restoreGetStore = installMockProperty(AsyncLocalStorage.prototype, "getStore", () => {
    throw new Error("live AsyncLocalStorage.getStore must not run");
  });
  let result;
  try {
    result = hostBindingRuntime.runWithRequestContext("rid-captured", async () => {
      await Promise.resolve();
      return hostBindingRuntime.currentRequestId();
    });
  } finally {
    restoreGetStore();
    restoreRun();
  }

  assert.equal(await result, "rid-captured");
});

test("host wrapper does not inspect or replace tenant return values", () => {
  let thenReads = 0;
  const value = {};
  Object.defineProperty(value, "then", {
    get() {
      thenReads += 1;
      return undefined;
    },
  });

  const result = hostBindingRuntime.runWithRequestContext("rid-object", () => value);

  assert.equal(result, value);
  assert.equal(thenReads, 0);
  assert.equal(hostBindingRuntime.currentRequestId(), null);
});
