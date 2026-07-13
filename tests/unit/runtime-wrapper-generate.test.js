import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "../../runtime/load/wrapper-generate.js";
import { moduleDataUrl } from "../helpers/load-shared-module.js";
import { withMockedProperty } from "../helpers/mock-global.js";

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
  assert.match(hostBindings, /const source = functionSource\(raw\);/);
  assert.match(hostBindings, /if \(regexpTest\(\/\^\\s\*class\\b\/, source\)\)/);
});

test("host wrapper runtime captures platform intrinsics before user module evaluation", () => {
  const source = generateHostBindingWrapperModule("worker.js", [], [], ["ROOM"], {}, []);
  assert.ok(
    source.indexOf(`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";`) <
      source.indexOf('import * as user from "./worker.js";')
  );
  for (const intrinsic of [
    "Array.prototype.forEach",
    "Function.prototype.toString",
    "Object.defineProperty",
    "Object.entries",
    "Object.keys",
    "Promise.prototype.then",
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

test("host wrapper cleanup ignores tenant-patched Promise.then", async () => {
  let cleaned = false;
  await withMockedProperty(
    Promise.prototype,
    "then",
    () => {
      throw new Error("tenant Promise.then must not run");
    },
    async () => {
      const result = await hostBindingRuntime.settleWithFinally(
        Promise.resolve("settled"),
        () => {
          cleaned = true;
        }
      );
      assert.equal(result, "settled");
      assert.equal(cleaned, true);

      cleaned = false;
      const failure = new Error("rejected");
      let rejection;
      try {
        await hostBindingRuntime.settleWithFinally(
          Promise.reject(failure),
          () => {
            cleaned = true;
          }
        );
      } catch (error) {
        rejection = error;
      }
      assert.equal(rejection, failure);
      assert.equal(cleaned, true);
    }
  );
});
