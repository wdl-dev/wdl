import assert from "node:assert/strict";
import { chmodSync, existsSync, writeFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";

import {
  ROOT,
  composeNoBuildArgs,
  resolveWdlCliBin,
} from "../../scripts/integration-environment.js";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { makeTempDir, removeTempDir } from "../helpers/temp-dir.js";

const composeHelper = await importRepositoryModule(
  "tests/integration/helpers/compose.js",
  importSpecifierReplacements({
    "../../../scripts/integration-environment.js": repositoryFileUrl(
      "scripts/integration-environment.js"
    ),
    "./env.js": moduleDataUrl('export const ROOT = ".";'),
    "./cli.js": moduleDataUrl("export function sh(argv, opts = {}) { return { argv, opts }; }"),
  })
);

test("compose no-build args follow the integration environment", () => {
  assert.deepEqual(composeNoBuildArgs({ WDL_INTEGRATION_NO_BUILD: "1" }), ["--no-build"]);
  assert.deepEqual(composeNoBuildArgs({}), []);
});

test("compose helpers consume the shared preflight no-build flag", async () => {
  await withMockedProperty(process.env, "WDL_INTEGRATION_NO_BUILD", "1", () => {
    assert.deepEqual(
      composeHelper.composeUp(["--wait", "gateway"]),
      { argv: ["docker", "compose", "up", "-d", "--no-build", "--wait", "gateway"], opts: {} }
    );
    assert.deepEqual(
      composeHelper.composeProfileUp("d1-multi", ["--wait", "d1-runtime-a"]),
      {
        argv: ["docker", "compose", "up", "-d", "--no-build", "--wait", "d1-runtime-a"],
        opts: { env: { COMPOSE_PROFILES: "d1-multi" } },
      }
    );
  });
  await withMockedProperty(process.env, "WDL_INTEGRATION_NO_BUILD", "0", () => {
    assert.deepEqual(
      composeHelper.composeUp(["--wait", "gateway"]),
      { argv: ["docker", "compose", "up", "-d", "--wait", "gateway"], opts: {} }
    );
    assert.deepEqual(
      composeHelper.composeProfileUp("d1-multi", ["--wait", "d1-runtime-a"]),
      {
        argv: ["docker", "compose", "up", "-d", "--wait", "d1-runtime-a"],
        opts: { env: { COMPOSE_PROFILES: "d1-multi" } },
      }
    );
  });
});

test("resolveWdlCliBin uses the supplied PATH environment", () => {
  assert.equal(resolveWdlCliBin({ PATH: "" }), "wdl");
});

test("resolveWdlCliBin resolves executable wdl commands on PATH", () => {
  const dir = makeTempDir("wdl-cli-path-");
  try {
    const bin = path.join(dir, "wdl");
    writeFileSync(bin, "#!/usr/bin/env node\n");
    chmodSync(bin, 0o755);
    assert.equal(resolveWdlCliBin({ PATH: dir }), bin);
  } finally {
    removeTempDir(dir);
  }
});

test("resolveWdlCliBin ignores non-executable wdl commands on PATH", () => {
  const dir = makeTempDir("wdl-cli-path-");
  try {
    const bin = path.join(dir, "wdl");
    writeFileSync(bin, "#!/usr/bin/env node\n");
    chmodSync(bin, 0o644);
    assert.equal(resolveWdlCliBin({ PATH: dir }), "wdl");
  } finally {
    removeTempDir(dir);
  }
});

test("resolveWdlCliBin resolves repository-relative overrides", () => {
  assert.equal(
    resolveWdlCliBin({ WDL_CLI_BIN: "local-cli/bin/wdl.js" }),
    path.join(ROOT, "local-cli/bin/wdl.js")
  );
});

test("resolveWdlCliBin preserves absolute overrides", () => {
  const absoluteBin = path.join(ROOT, "local-cli/bin/wdl.js");
  assert.equal(resolveWdlCliBin({ WDL_CLI_BIN: absoluteBin }), absoluteBin);
});

test("ROOT points to the repository root directory", () => {
  assert.equal(existsSync(path.join(ROOT, "package.json")), true);
});
