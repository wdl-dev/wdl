import { test } from "node:test";
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";

import {
  CLI_INTEGRATION_MARKER,
  durationPriorityNames,
  hasCliIntegrationMarker,
  prioritizeDefaultFiles,
  readIntegrationDurationRecords,
  readIntegrationDurationReport,
} from "../../scripts/integration-test-plan.js";
import { installStreamWriteCapture } from "../helpers/output-capture.js";
import { withTempDir } from "../helpers/temp-dir.js";

test("prioritizeDefaultFiles runs configured slow files first and preserves the rest", () => {
  assert.deepEqual(
    prioritizeDefaultFiles([
      "z.test.js",
      "log-tail.test.js",
      "d1-ownership-multi-runtime.test.js",
      "d1-storage-shared-localdisk.test.js",
      "workflows-runtime-retention.test.js",
      "workflows-runtime-scheduler.test.js",
      "workflows-runtime-core.test.js",
      "durable-objects-ownership.test.js",
      "durable-objects-websocket.test.js",
      "durable-objects-alarms.test.js",
      "queues-orphan-and-control.test.js",
      "queues-retry-and-delay.test.js",
      "queues-delivery.test.js",
      "queues-batch-and-isolation.test.js",
      "cron-triggers.test.js",
      "a.test.js",
    ]),
    [
      "d1-storage-shared-localdisk.test.js",
      "durable-objects-ownership.test.js",
      "d1-ownership-multi-runtime.test.js",
      "durable-objects-alarms.test.js",
      "cron-triggers.test.js",
      "log-tail.test.js",
      "durable-objects-websocket.test.js",
      "workflows-runtime-retention.test.js",
      "queues-orphan-and-control.test.js",
      "queues-retry-and-delay.test.js",
      "workflows-runtime-scheduler.test.js",
      "queues-delivery.test.js",
      "queues-batch-and-isolation.test.js",
      "workflows-runtime-core.test.js",
      "z.test.js",
      "a.test.js",
    ]
  );
});

test("prioritizeDefaultFiles ignores missing priority entries", () => {
  assert.deepEqual(
    prioritizeDefaultFiles(["b.test.js", "a.test.js"], ["missing.test.js", "a.test.js"]),
    ["a.test.js", "b.test.js"]
  );
});

test("prioritizeDefaultFiles prefers recorded durations before the fallback slow list", () => {
  const durations = {
    "tests/integration/a.test.js": { durationMs: 50, updatedAt: "2026-05-18T00:00:00.000Z" },
    "tests/integration/queues-delivery.test.js": { durationMs: 100, updatedAt: "2026-05-18T00:00:00.000Z" },
  };

  assert.deepEqual(
    prioritizeDefaultFiles(
      ["a.test.js", "queues-delivery.test.js", "log-tail.test.js", "z.test.js"],
      { durationRecords: durations }
    ),
    [
      "queues-delivery.test.js",
      "a.test.js",
      "log-tail.test.js",
      "z.test.js",
    ]
  );
});

test("durationPriorityNames ignores unavailable and malformed duration records", () => {
  assert.deepEqual(
    durationPriorityNames(["a.test.js", "b.test.js"], {
      "tests/integration/a.test.js": { durationMs: 2 },
      "tests/integration/missing.test.js": { durationMs: 100 },
      "tests/integration/b.test.js": { durationMs: 200, status: "failed" },
      "tests/integration/c.test.js": { durationMs: "not-a-number" },
    }),
    ["a.test.js"]
  );
});

test("hasCliIntegrationMarker only accepts line-start marker comments", () => {
  assert.equal(hasCliIntegrationMarker(`// ${CLI_INTEGRATION_MARKER}\n`), true);
  assert.equal(hasCliIntegrationMarker(`  // ${CLI_INTEGRATION_MARKER}\n`), false);
  assert.equal(hasCliIntegrationMarker(`const marker = "// ${CLI_INTEGRATION_MARKER}";\n`), false);
  assert.equal(hasCliIntegrationMarker(`/* ${CLI_INTEGRATION_MARKER} */\n`), false);
});

test("integration duration reader owns the full tolerant report schema", async () => {
  await withTempDir("wdl-duration-reader-", async (dir) => {
    const file = `${dir}/durations.json`;
    const report = {
      updatedAt: "2026-07-10T00:00:00.000Z",
      runDurationMs: 123,
      files: {
        "tests/integration/a.test.js": {
          durationMs: 42,
          status: "passed",
          updatedAt: "2026-07-10T00:00:00.000Z",
        },
      },
    };
    writeFileSync(file, JSON.stringify(report));

    assert.deepEqual(readIntegrationDurationReport(file), report);
    assert.deepEqual(readIntegrationDurationRecords(file), report.files);
  });
});

test("integration duration reader warns once and returns null for unreadable JSON", async () => {
  await withTempDir("wdl-duration-reader-", async (dir) => {
    const file = `${dir}/durations.json`;
    writeFileSync(file, "not-json");
    /** @type {string[]} */
    const writes = [];
    const restoreWrite = installStreamWriteCapture(process.stderr, writes);
    try {
      assert.equal(readIntegrationDurationReport(file), null);
    } finally {
      restoreWrite();
    }
    assert.match(writes.join(""), /warning: ignoring unreadable integration duration file/);
  });
});
