import { test } from "node:test";
import assert from "node:assert/strict";

import { integrationChildEnv, sh } from "../integration/helpers/cli.js";

test("integration sh does not leak node:test worker env into child processes", () => {
  const env = integrationChildEnv(
    { NODE_TEST_CONTEXT: "ctx", NODE_TEST_WORKER_ID: "worker", TEST_VALUE: "base" },
    { TEST_VALUE: "override" }
  );
  assert.equal("NODE_TEST_CONTEXT" in env, false);
  assert.equal("NODE_TEST_WORKER_ID" in env, false);
  assert.equal(env.TEST_VALUE, "override");
});

test("integration sh forwards argv and env without shell parsing", () => {
  const output = sh(
    [
      process.execPath,
      "-e",
      "process.stdout.write(process.env.TEST_SH_VALUE + ':' + JSON.stringify(process.argv.slice(1)))",
      "hello world",
      "",
      "left && right",
    ],
    { env: { TEST_SH_VALUE: "inline" } }
  );
  assert.equal(output, 'inline:["hello world","","left && right"]');
});
