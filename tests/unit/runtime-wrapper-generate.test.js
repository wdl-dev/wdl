import { test } from "node:test";
import assert from "node:assert/strict";
import {
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "../../runtime/load/wrapper-generate.js";

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

test("generated wrapper flavors share default-export class detection", () => {
  const { abortOnly, hostBindings } = generatedWrappers();
  const sourceLine = "const source = Function.prototype.toString.call(raw);";
  const classTest = "/^\\s*class\\b/.test(source)";

  assert.equal(abortOnly.split(sourceLine).length - 1, 1);
  assert.equal(hostBindings.split(sourceLine).length - 1, 1);
  assert.match(abortOnly, new RegExp(`if \\(!${RegExp.escape(classTest)}\\)`));
  assert.match(hostBindings, new RegExp(`if \\(${RegExp.escape(classTest)}\\)`));
});
