import { test } from "node:test";
import assert from "node:assert/strict";

import { extractExperimentalCompatFlags } from "../../scripts/extract-workerd-experimental-compat-flags.mjs";

test("workerd experimental compat flag extractor mirrors only experimental enable flags", () => {
  const flags = extractExperimentalCompatFlags(`
struct CompatibilityFlags {
  gaFlag @1 :Bool $compatEnableFlag("unique_ctx_per_invocation");
  experimentalFlag @2 :Bool
      $experimental
      $compatEnableFlag("experimental_one")
      $compatDisableFlag("no_experimental_one");
  spacedExperimentalFlag @3   :   Bool
      $compatEnableFlag("experimental_two")
      $experimental;
}
`);
  assert.deepEqual(flags, ["experimental_one", "experimental_two"]);
});
