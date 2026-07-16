import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "../../runtime/load/wrapper-generate.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const HOST_BINDING_RUNTIME_TEST_SOURCE = applyModuleReplacements(HOST_BINDING_RUNTIME_SOURCE, [
  [
    'from "./_wdl-request-id.js"',
    `from ${JSON.stringify(repositoryFileUrl("runtime/_wdl-request-id.js"))}`,
  ],
]);

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

test("host wrapper runtime evaluates before the tenant module", () => {
  const source = generateHostBindingWrapperModule("worker.js", [], [], ["ROOM"], {}, []);
  assert.ok(
    source.indexOf(`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";`) <
      source.indexOf('import * as __WdlUserModule__ from "./worker.js";')
  );
  assert.match(source, /import \* as __WdlHostRuntime__/);
  assert.match(HOST_BINDING_RUNTIME_SOURCE, /import \{ sanitizeRequestId \} from "\.\/_wdl-request-id\.js"/);
  assert.doesNotMatch(HOST_BINDING_RUNTIME_SOURCE, /AsyncLocalStorage|node:async_hooks/);
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
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
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
