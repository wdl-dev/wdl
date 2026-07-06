// @wdl-cli-integration
// scheduler → system-runtime dispatch + the s3-cleanup worker drain
// path. One deploy is shared across cases; tests clean only their own
// task/prefix keys.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  ASSETS_CDN_BASE,
  CONTROL_URL,
  adminPost,
  assertStatus,
  delay,
  rawHttpGet,
  runWdlCli,
  setupIntegrationSuite,
  systemRuntimeInternalPost,
  waitUntil,
  responseJson,
} from "./helpers/index.js";
import { redisFlushAll, redisXAdd } from "./helpers/redis.js";
import {
  S3_CLEANUP_QUEUE_NAME,
  S3_CLEANUP_TABLE,
  S3_CLEANUP_TASK_FIELDS,
  S3_CLEANUP_TASK_ID_PREFIX,
  S3_CLEANUP_TASK_STATUS,
  s3CleanupQueueFields,
} from "../../shared/s3-cleanup-lifecycle.js";
import { queueStreamKey } from "../../shared/queue-keys.js";
import http from "node:http";

const S3MOCK_BASE = ASSETS_CDN_BASE;
const CLEANUP_DB = "s3-cleanup-state";
const S3_CLEANUP_WORKER_ID = "__system__:s3-cleanup:v1";
const CLEANUP_MIGRATIONS = [
  ["0001_s3_cleanup_task.sql", "0001_s3_cleanup_task"],
  ["0002_s3_cleanup_checkpoint.sql", "0002_s3_cleanup_checkpoint"],
].map(([id, name]) => ({
  id,
  name,
  sql: readFileSync(
    new URL(`../../system-workers/s3-cleanup/migrations/${id}`, import.meta.url),
    "utf8"
  ),
}));

setupIntegrationSuite({
  reset: false,
  async afterStackUp() {
    // One-time FLUSHALL + deploy. We do NOT resetStack between tests in this file.
    redisFlushAll();

    const cliEnv = {
      ...process.env,
      ADMIN_TOKEN: "local-dev-token",
      CONTROL_URL,
      CONTROL_CONNECT_HOST: "localhost",
    };

    const created = await adminPost("/ns/__system__/d1/databases", {
      databaseName: CLEANUP_DB,
    });
    assertStatus(created, 201, "created");

    const migrated = await adminPost(`/ns/__system__/d1/databases/${CLEANUP_DB}/migrations/apply`, {
      migrations: CLEANUP_MIGRATIONS,
    });
    assertStatus(migrated, 200, "migrated");

    // S3 creds are worker-level secrets. Pre-deploy puts are HSET-only
    // because there is no active version to bump yet.
    const secrets = {
      S3_ENDPOINT: "http://s3mock:9090",
      S3_REGION: "us-east-1",
      S3_BUCKET: "wdl-assets",
      S3_ACCESS_KEY_ID: "foo",
      S3_SECRET_ACCESS_KEY: "bar",
    };
    for (const [k, v] of Object.entries(secrets)) {
      const put = runWdlCli(
        ["secret", "put", "--ns", "__system__", "--worker", "s3-cleanup", k],
        { env: cliEnv, input: v }
      );
      assert.equal(put.status, 0, put.stderr || put.stdout);
    }

    const deploy = runWdlCli(["deploy", "system-workers/s3-cleanup", "--ns", "__system__"], {
      env: cliEnv,
    });
    assert.equal(deploy.status, 0, deploy.stderr || deploy.stdout);

    // Let scheduler reconcile pick up the new queue consumer.
    await delay(2000);
  },
});

/**
 * @param {{ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs: number }} args
 */
function enqueueCleanupIntent({ taskId, prefixes, source, nowMs }) {
  const fields = s3CleanupQueueFields({
    taskId,
    prefixes,
    source,
    nowMs,
  }, "s3-cleanup integration");
  redisXAdd(queueStreamKey("__system__", S3_CLEANUP_QUEUE_NAME), fields, { db: 1 });
}

/** @param {{ sql: string, params?: unknown[], mode?: string }} args */
async function d1Query({ sql, params = [], mode = "all" }) {
  const res = await adminPost(`/ns/__system__/d1/databases/${CLEANUP_DB}/query`, {
    mode,
    sql,
    params,
  });
  assertStatus(res, 200, "s3 cleanup D1 query");
  return res.json.result;
}

/** @param {string} taskId */
async function cleanupRow(taskId) {
  const result = await d1Query({
    sql: `SELECT * FROM ${S3_CLEANUP_TABLE} WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?1`,
    params: [taskId],
  });
  return result.results[0] || null;
}

/**
 * @param {{ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs: number }} args
 */
async function seedCleanupRow({ taskId, prefixes, source, nowMs }) {
  await d1Query({
    mode: "run",
    sql: `
      INSERT OR REPLACE INTO ${S3_CLEANUP_TABLE}
        (${S3_CLEANUP_TASK_FIELDS.ID},
         ${S3_CLEANUP_TASK_FIELDS.SOURCE_JSON},
         ${S3_CLEANUP_TASK_FIELDS.PREFIXES_JSON},
         ${S3_CLEANUP_TASK_FIELDS.STATE},
         ${S3_CLEANUP_TASK_FIELDS.ATTEMPTS},
         ${S3_CLEANUP_TASK_FIELDS.CREATED_AT},
         ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT},
         ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT})
      VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7)
    `,
    params: [
      taskId,
      JSON.stringify(source),
      JSON.stringify(prefixes),
      S3_CLEANUP_TASK_STATUS.PENDING,
      nowMs,
      nowMs,
      nowMs,
    ],
  });
}

/** @param {string} prefix @param {string} relPath @param {string} body */
async function s3Put(prefix, relPath, body) {
  const key = `${prefix}${relPath}`;
  const url = `${S3MOCK_BASE}/${key}`;
  const u = new URL(url);
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: u.hostname, port: u.port, path: u.pathname + u.search,
      method: "PUT",
      headers: { "content-type": "text/plain", "content-length": Buffer.byteLength(body) },
    }, (res) => {
      /** @type {Buffer[]} */
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode }));
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

function randomTaskId() {
  return `${S3_CLEANUP_TASK_ID_PREFIX}test-${Math.random().toString(36).slice(2, 10)}`;
}

/** @param {(() => Promise<boolean | undefined>) & { _label?: string }} labelFn */
function runUntil(labelFn, timeoutMs = 15_000, intervalMs = 500) {
  return waitUntil(labelFn._label || "s3-cleanup state", labelFn, { timeoutMs, intervalMs });
}

test("scheduler queue path: __system__ dispatch → system-runtime → s3-cleanup drains task", async () => {
  const ns = "scdrain";
  const worker = "w";
  const token = "1111111111112222222222223333"; // 28 hex
  const prefix = `assets/${ns}/${worker}/${token}/`;

  await s3Put(prefix, "a.txt", "hello-a");
  await s3Put(prefix, "b.txt", "hello-b");
  const pre = await rawHttpGet(`${S3MOCK_BASE}/${prefix}a.txt`);
  assert.equal(pre.status, 200);

  const taskId = randomTaskId();
  const nowMs = Date.now();
  enqueueCleanupIntent({
    taskId,
    prefixes: [prefix],
    source: { kind: "delete-worker", ns, worker, versions: ["v1"], requestId: "test-drain" },
    nowMs,
  });

  await runUntil(
    Object.assign(
      async () => (await rawHttpGet(`${S3MOCK_BASE}/${prefix}a.txt`)).status === 404,
      { _label: `task ${taskId} deleted first object` }
    )
  );

  const post2 = await rawHttpGet(`${S3MOCK_BASE}/${prefix}b.txt`);
  assert.equal(post2.status, 404);

  assert.equal(await cleanupRow(taskId), null, "task row should be deleted after success");
});

test("cron drains D1 due rows without a queue delivery", async () => {
  const ns = "sccron";
  const worker = "w";
  const token = "ddddddddddddeeeeeeeeeeeeffff";
  const prefix = `assets/${ns}/${worker}/${token}/`;

  await s3Put(prefix, "cron.txt", "hello-cron");
  const pre = await rawHttpGet(`${S3MOCK_BASE}/${prefix}cron.txt`);
  assert.equal(pre.status, 200);

  const taskId = randomTaskId();
  const nowMs = Date.now();
  await seedCleanupRow({
    taskId,
    prefixes: [prefix],
    source: { kind: "delete-worker", ns, worker, versions: ["v1"], requestId: "cron-drain" },
    nowMs,
  });

  const res = systemRuntimeInternalPost("/_scheduled", {
    "x-worker-id": S3_CLEANUP_WORKER_ID,
  }, { scheduledTime: nowMs, cron: "*/1 * * * *" });
  assertStatus(res, 200, "s3 cleanup scheduled dispatch");
  assert.equal(responseJson(res).outcome, "ok");

  await runUntil(
    Object.assign(
      async () => (await rawHttpGet(`${S3MOCK_BASE}/${prefix}cron.txt`)).status === 404,
      { _label: `cron task ${taskId} deleted object` }
    )
  );
  assert.equal(await cleanupRow(taskId), null, "cron success should delete the task row");
});

test("XML-escaped keys in ListObjectsV2 are unescaped before re-send; the real object is deleted (regression: & in asset filename)", async () => {
  // Before the unescape fix, <Key>a&amp;b.txt</Key> got re-escaped on
  // the Delete request as `a&amp;amp;b.txt`, S3 "succeeded" deleting a
  // non-existent key, and the real object lingered.
  const ns = "ampkey";
  const worker = "w";
  const token = "aaaaaaaaaaaabbbbbbbbbbbbcccc"; // 28 hex
  const prefix = `assets/${ns}/${worker}/${token}/`;
  const trickyKey = "has&amp.txt";

  await s3Put(prefix, trickyKey, "gotcha");
  const pre = await rawHttpGet(`${S3MOCK_BASE}/${prefix}${encodeURIComponent(trickyKey)}`);
  assert.equal(pre.status, 200, `fixture upload must succeed; got ${pre.status}`);

  const taskId = randomTaskId();
  const nowMs = Date.now();
  enqueueCleanupIntent({
    taskId,
    prefixes: [prefix],
    source: { kind: "delete-worker", ns, worker, versions: ["v1"], requestId: "xml-unescape" },
    nowMs,
  });

  await runUntil(
    Object.assign(
      async () => (await rawHttpGet(`${S3MOCK_BASE}/${prefix}${encodeURIComponent(trickyKey)}`)).status === 404,
      { _label: `task ${taskId} deleted escaped key` }
    )
  );

  // The real key — not a phantom double-escaped variant — must be gone.
  const post = await rawHttpGet(`${S3MOCK_BASE}/${prefix}${encodeURIComponent(trickyKey)}`);
  assert.equal(post.status, 404, `real key should be deleted, got ${post.status}`);

  assert.equal(await cleanupRow(taskId), null, "task row should be deleted after success");
});
