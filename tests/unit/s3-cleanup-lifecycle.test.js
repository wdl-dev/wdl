import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { parseBase64Json } from "../helpers/json-payload.js";
import {
  S3_CLEANUP_QUEUE_NAME,
  S3_CLEANUP_TABLE,
  S3_CLEANUP_TASK_FIELDS,
  S3_CLEANUP_TASK_ID_PREFIX,
  S3_CLEANUP_TASK_STATUS,
  s3CleanupQueueFields,
} from "../../shared/s3-cleanup-lifecycle.js";

const CANONICAL_ASSETS_PREFIX = "assets/n/w/019dd83e7d1c345302f9b0f3b4f6/";

test("s3CleanupQueueFields builds the canonical queue intent", () => {
  const taskId = `${S3_CLEANUP_TASK_ID_PREFIX}unit`;
  const source = { kind: "delete-worker", ns: "n", worker: "w" };
  const fields = s3CleanupQueueFields({
    taskId,
    prefixes: [CANONICAL_ASSETS_PREFIX],
    source,
    nowMs: 1234,
  });
  const body = parseBase64Json(fields.body_b64, "S3 cleanup queue body");

  assert.equal(S3_CLEANUP_QUEUE_NAME, "worker-delete-s3-cleanup");
  assert.equal(S3_CLEANUP_TABLE, "s3_cleanup_task");
  assert.equal(S3_CLEANUP_TASK_FIELDS.STATE, "state");
  assert.equal(S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON, "checkpoint_json");
  assert.equal(S3_CLEANUP_TASK_STATUS.PENDING, "pending");
  assert.notEqual(fields.id, taskId);
  assert.match(fields.id, /^[0-9a-f-]{36}$/);
  assert.deepEqual(fields, {
    id: fields.id,
    body_b64: fields.body_b64,
    content_type: "json",
    attempts: "0",
    first_seen_ms: "1234",
  });
  assert.deepEqual(body, {
    taskId,
    prefixes: [CANONICAL_ASSETS_PREFIX],
    source,
    createdAt: 1234,
    nextAttemptAt: 1234,
  });
});

test("s3CleanupQueueFields rejects non-canonical cleanup tasks", () => {
  assert.throws(
    () => s3CleanupQueueFields({
      taskId: "bad",
      prefixes: [CANONICAL_ASSETS_PREFIX],
      source: { kind: "delete-worker" },
    }, "unit"),
    /unit: taskId must start with 's3cleanup:'/
  );
  assert.throws(
    () => s3CleanupQueueFields({
      taskId: `${S3_CLEANUP_TASK_ID_PREFIX}unit`,
      prefixes: [],
      source: { kind: "delete-worker" },
    }, "unit"),
    /unit: prefixes must be non-empty array/
  );
  for (const prefixes of [
    [""],
    ["assets/"],
    ["r2/n/b/key"],
    ["assets/n/w/not-a-token/"],
    ["assets/./w/019dd83e7d1c345302f9b0f3b4f6/"],
    ["assets/n/../019dd83e7d1c345302f9b0f3b4f6/"],
  ]) {
    assert.throws(
      () => s3CleanupQueueFields({
        taskId: `${S3_CLEANUP_TASK_ID_PREFIX}unit`,
        prefixes,
        source: { kind: "delete-worker" },
      }, "unit"),
      /unit: prefixes must be canonical ASSETS prefixes/
    );
  }
  assert.throws(
    () => s3CleanupQueueFields(/** @type {any} */ ({
      taskId: `${S3_CLEANUP_TASK_ID_PREFIX}unit`,
      prefixes: [CANONICAL_ASSETS_PREFIX],
      source: null,
    }), "unit"),
    /unit: source object required/
  );
});

test("s3-cleanup wrangler consumer queue matches the shared queue name", () => {
  const toml = readFileSync(
    new URL("../../system-workers/s3-cleanup/wrangler.toml", import.meta.url),
    "utf8"
  );
  const migration = readFileSync(
    new URL("../../system-workers/s3-cleanup/migrations/0001_s3_cleanup_task.sql", import.meta.url),
    "utf8"
  );
  const checkpointMigration = readFileSync(
    new URL("../../system-workers/s3-cleanup/migrations/0002_s3_cleanup_checkpoint.sql", import.meta.url),
    "utf8"
  );
  assert.match(toml, new RegExp(`\\bqueue\\s*=\\s*"${RegExp.escape(S3_CLEANUP_QUEUE_NAME)}"`));
  assert.match(toml, /\bbinding\s*=\s*"S3_CLEANUP_DB"/);
  assert.match(toml, /\bmigrations_dir\s*=\s*"migrations"/);
  assert.match(migration, new RegExp(`CREATE TABLE IF NOT EXISTS ${RegExp.escape(S3_CLEANUP_TABLE)}`));
  assert.match(migration, new RegExp(`\\b${RegExp.escape(S3_CLEANUP_TASK_FIELDS.ID)}\\s+TEXT PRIMARY KEY`));
  assert.match(migration, /idx_s3_cleanup_pending_due/);
  assert.match(
    checkpointMigration,
    new RegExp(`ADD COLUMN ${RegExp.escape(S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON)} TEXT`)
  );
});
