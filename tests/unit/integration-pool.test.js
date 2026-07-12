import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { withTempDir } from "../helpers/temp-dir.js";

import {
  buildDurationReport,
  formatIntegrationSummary,
  makeSlotEnv,
  persistDurationFile,
} from "../../scripts/_integration-pool.js";
import {
  LOCAL_ADMIN_TOKEN,
  LOCAL_CONNECT_HOST,
  localAssetsCdnBase,
  localControlUrl,
} from "../../scripts/integration-environment.js";
import { installStreamWriteCapture } from "../helpers/output-capture.js";

test("integration shard env ignores host control credentials", () => {
  const env = makeSlotEnv(2, "wdl-it", 18080, 29500, {
    ADMIN_TOKEN: "staging-token",
    CONTROL_URL: "https://ctl.staging.example",
    CONTROL_CONNECT_HOST: "ctl.remote.example",
    WDL_NS: "prod-ns",
    CLOUDFLARE_ENV: "production",
  });

  assert.equal(env.COMPOSE_PROJECT_NAME, "wdl-it-2");
  assert.equal(env.WDL_GATEWAY_HOST_PORT, "18082");
  assert.equal(env.ADMIN_TOKEN, LOCAL_ADMIN_TOKEN);
  assert.equal(env.CONTROL_URL, localControlUrl(18082));
  assert.equal(env.CONTROL_CONNECT_HOST, LOCAL_CONNECT_HOST);
  assert.equal(env.ASSETS_CDN_BASE, localAssetsCdnBase(29502));
  assert.equal(env.WDL_WORKERD_CONFIG_VARIANT, "local");
  assert.equal("WDL_NS" in env, false);
  assert.equal("CLOUDFLARE_ENV" in env, false);
});

test("buildDurationReport merges observed integration file durations", () => {
  const report = buildDurationReport(
    {
      files: {
        "tests/integration/old.test.js": {
          durationMs: 10,
          status: "passed",
          updatedAt: "2026-05-17T00:00:00.000Z",
        },
      },
    },
    {
      runDurationMs: 123,
      fileDurations: [
        {
          file: "tests/integration/new.test.js",
          slot: 1,
          durationMs: 42,
          status: "passed",
        },
      ],
    },
    new Date("2026-05-18T00:00:00.000Z")
  );

  assert.deepEqual(report, {
    updatedAt: "2026-05-18T00:00:00.000Z",
    runDurationMs: 123,
    files: {
      "tests/integration/old.test.js": {
        durationMs: 10,
        status: "passed",
        updatedAt: "2026-05-17T00:00:00.000Z",
      },
      "tests/integration/new.test.js": {
        durationMs: 42,
        status: "passed",
        updatedAt: "2026-05-18T00:00:00.000Z",
      },
    },
  });
});

test("persistDurationFile reports filesystem failures without throwing", () => {
  const file = path.join(os.tmpdir(), `wdl-missing-${process.pid}`, "durations.json");
  /** @type {string[]} */
  const writes = [];
  const restoreWrite = installStreamWriteCapture(process.stderr, writes);

  try {
    assert.equal(
      persistDurationFile(file, {
        runDurationMs: 123,
        fileDurations: [
          {
            file: "tests/integration/new.test.js",
            slot: 0,
            durationMs: 42,
            status: "passed",
          },
        ],
      }),
      false
    );
  } finally {
    restoreWrite();
  }
  assert.match(writes.join(""), /warning: unable to update integration duration file/);
});

test("persistDurationFile writes observed durations when the file is writable", async () => {
  await withTempDir("wdl-duration-", async (dir) => {
    const file = path.join(dir, "durations.json");
    assert.equal(
      persistDurationFile(file, {
        runDurationMs: 123,
        fileDurations: [
          {
            file: "tests/integration/new.test.js",
            slot: 0,
            durationMs: 42,
            status: "passed",
          },
        ],
      }),
      true
    );
    const parsed = JSON.parse(readFileSync(file, "utf8"));
    assert.equal(parsed.runDurationMs, 123);
    assert.equal(parsed.files["tests/integration/new.test.js"].durationMs, 42);
  });
});

test("formatIntegrationSummary reports wall, shard, and file durations", () => {
  assert.equal(
    formatIntegrationSummary({
      shardCount: 2,
      runDurationMs: 100,
      fileDurations: [
        { file: "tests/integration/fast.test.js", slot: 0, durationMs: 10, status: "passed" },
        { file: "tests/integration/slow.test.js", slot: 1, durationMs: 90, status: "failed" },
      ],
    }),
    [
      "integration summary: files=2 test_wall_ms=100",
      "integration shard durations:",
      "  s0 files=1 duration_ms=10",
      "  s1 files=1 duration_ms=90",
      "integration file durations:",
      "  90ms s1 failed tests/integration/slow.test.js",
      "  10ms s0 passed tests/integration/fast.test.js",
      "",
    ].join("\n")
  );
});
