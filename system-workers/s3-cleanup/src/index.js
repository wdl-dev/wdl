// Drains durable S3 cleanup tasks. Queue delivery creates D1 task rows;
// cron drains missed processing attempts and retries. Keep the ordering
// load-bearing: the queue is only the ingress durability boundary until the
// D1 row exists; once persisted, S3/delete failures are represented in D1 and
// cron owns replay, so the queue message must be acked instead of retrying.

// Vendored aws-sigv4 — wrangler bundler inlines it from the shared/
// copy so the worker doesn't need its own npm install.
import { SigV4Client } from "../../../shared/vendor/aws-sigv4.js";
import {
  createLogLevelBinder,
  logStructured as emitStructuredLog,
} from "../../../shared/observability.js";
import { discardResponseBody } from "../../../shared/respond.js";
import { errorMessage } from "../../../shared/errors.js";
import { bytesToBase64 } from "../../../shared/base64.js";
import {
  S3_CLEANUP_ERROR,
  S3_CLEANUP_OUTCOME,
  S3_CLEANUP_TABLE,
  S3_CLEANUP_TASK_FIELDS,
  S3_CLEANUP_TASK_STATUS,
  validateS3CleanupTaskInput,
} from "../../../shared/s3-cleanup-lifecycle.js";
import {
  listXmlTagValues,
  xmlEscape,
  xmlTagValueIsTrue,
  xmlUnescape,
} from "../../../shared/s3-xml.js";
import { encodeS3Query } from "../../../shared/s3-query.js";

const MAX_ATTEMPTS = 10;
const BACKOFF_MAX_MS = 30 * 60_000;
const BACKOFF_BASE_MS = 60_000;
const CRON_BATCH = 100;
const MAX_DELETE_PAGES_PER_RUN = 1;
const MAX_LIST_PAGES = 1000;
const PROCESSING_LEASE_MS = 30 * 60_000;
const S3_TRANSIENT_RETRIES = 10;
const S3_RETRY_BASE_MS = 50;
const S3_RETRY_MAX_MS = 5_000;
const DB_BINDING = "S3_CLEANUP_DB";
const SERVICE = "s3-cleanup";
const utf8Encoder = new TextEncoder();
const bindLogLevel = createLogLevelBinder();

/**
 * @typedef {import("../../../shared/s3-cleanup-lifecycle.js").S3CleanupIntent} S3CleanupIntent
 * @typedef {Record<string, unknown> & { S3_ACCESS_KEY_ID: string, S3_SECRET_ACCESS_KEY: string, S3_ENDPOINT: string, S3_BUCKET: string, S3_REGION?: string, LOG_LEVEL?: unknown, S3_CLEANUP_DB: CleanupDb }} S3CleanupEnv
 * @typedef {{ prepare(sql: string): { bind(...values: unknown[]): { run(): Promise<{ meta?: { changes?: number } }>, first(): Promise<Record<string, unknown> | null>, all(): Promise<{ results?: Record<string, unknown>[] }> } } }} CleanupDb
 * @typedef {{ aws: SigV4Client, endpoint: string, bucket: string }} S3Client
 * @typedef {{ prefixIndex: number, continuationToken: string | null, pageCount: number, deletedCount: number }} CleanupCheckpoint
 * @typedef {{ id: string, source: Record<string, unknown> | null, prefixes: string[] | null, state: string, attempts: number, createdAt: number, updatedAt: number, nextAttemptAt: number | null, lastError: string | null, checkpoint: CleanupCheckpoint | null }} CleanupTask
 */

/** @param {number} attempts */
export function nextBackoffMs(attempts) {
  return Math.min(BACKOFF_BASE_MS * 2 ** (attempts - 1), BACKOFF_MAX_MS);
}

/** @param {number} ms */
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** @param {number} attempt */
function s3RetryDelayMs(attempt) {
  return Math.min(S3_RETRY_BASE_MS * 2 ** attempt, S3_RETRY_MAX_MS);
}

/** @param {Response} response */
function isTransientS3Response(response) {
  return response.status === 429 || response.status >= 500;
}

/**
 * DeleteObjects is a POST, but deleting the same key set is safe to retry.
 * Keep this scoped to S3 POST calls with known idempotent semantics.
 * @param {SigV4Client} client
 * @param {string} url
 * @param {RequestInit} init
 */
async function fetchRetryableS3Post(client, url, init) {
  for (let attempt = 0; attempt <= S3_TRANSIENT_RETRIES; attempt += 1) {
    let response;
    try {
      response = await client.fetch(url, init);
    } catch (err) {
      if (attempt === S3_TRANSIENT_RETRIES) throw err;
      await delay(Math.random() * s3RetryDelayMs(attempt));
      continue;
    }
    if (attempt === S3_TRANSIENT_RETRIES || !isTransientS3Response(response)) {
      return response;
    }
    await discardResponseBody(response);
    await delay(Math.random() * s3RetryDelayMs(attempt));
  }
  throw new Error("unreachable S3 retry loop exit");
}

/**
 * @param {"debug" | "info" | "warn" | "error"} level
 * @param {string} event
 * @param {Record<string, unknown>} [fields]
 */
function logStructured(level, event, fields = {}) {
  emitStructuredLog(SERVICE, level, event, fields);
}

/**
 * @param {S3CleanupEnv} env
 * @param {string} key
 */
function requireEnv(env, key) {
  if (!env[key]) throw new Error(`s3-cleanup: missing required worker secret ${key}`);
}

/** @param {S3CleanupEnv} env */
function requireDb(env) {
  if (!env[DB_BINDING]) throw new Error(`s3-cleanup: missing required D1 binding ${DB_BINDING}`);
  return env[DB_BINDING];
}

/** @param {string} value */
function trimTrailingSlashes(value) {
  let end = value.length;
  while (end > 0 && value.charCodeAt(end - 1) === 47) end -= 1;
  return value.slice(0, end);
}

/** @param {S3CleanupEnv} env */
function buildS3Client(env) {
  for (const key of [
    "S3_ENDPOINT", "S3_REGION", "S3_BUCKET",
    "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY",
  ]) requireEnv(env, key);
  return {
    aws: new SigV4Client({
      accessKeyId: env.S3_ACCESS_KEY_ID,
      secretAccessKey: env.S3_SECRET_ACCESS_KEY,
      service: "s3",
      region: env.S3_REGION,
      retries: S3_TRANSIENT_RETRIES,
    }),
    endpoint: trimTrailingSlashes(env.S3_ENDPOINT),
    bucket: env.S3_BUCKET,
  };
}

/** @param {unknown} raw */
function safeJsonParse(raw) {
  if (typeof raw !== "string" || raw === "") return null;
  try { return JSON.parse(raw); } catch { return null; }
}

/** @param {unknown} raw */
function normalizeCheckpoint(raw) {
  const parsed = safeJsonParse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const checkpoint = /** @type {Record<string, unknown>} */ (parsed);
  const prefixIndex = Number(checkpoint.prefixIndex);
  if (!Number.isInteger(prefixIndex) || prefixIndex < 0) return null;
  const continuationToken = checkpoint.continuationToken;
  const pageCount = Number(checkpoint.pageCount ?? 0);
  const deletedCount = Number(checkpoint.deletedCount ?? 0);
  if (!Number.isInteger(pageCount) || pageCount < 0) return null;
  if (!Number.isInteger(deletedCount) || deletedCount < 0) return null;
  return {
    prefixIndex,
    continuationToken: typeof continuationToken === "string" && continuationToken
      ? continuationToken
      : null,
    pageCount,
    deletedCount,
  };
}

/** @param {Record<string, unknown> | null | undefined} row */
function normalizeTaskRow(row) {
  if (!row) return null;
  const prefixes = safeJsonParse(row[S3_CLEANUP_TASK_FIELDS.PREFIXES_JSON]);
  const source = safeJsonParse(row[S3_CLEANUP_TASK_FIELDS.SOURCE_JSON]);
  const lastError = row[S3_CLEANUP_TASK_FIELDS.LAST_ERROR];
  const checkpoint = normalizeCheckpoint(row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON]);
  return {
    id: String(row[S3_CLEANUP_TASK_FIELDS.ID] || ""),
    source,
    prefixes,
    state: String(row[S3_CLEANUP_TASK_FIELDS.STATE] || ""),
    attempts: Number(row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS] ?? 0),
    createdAt: Number(row[S3_CLEANUP_TASK_FIELDS.CREATED_AT] ?? 0),
    updatedAt: Number(row[S3_CLEANUP_TASK_FIELDS.UPDATED_AT] ?? 0),
    nextAttemptAt: row[S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT] == null
      ? null
      : Number(row[S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT]),
    lastError: typeof lastError === "string"
      ? lastError
      : null,
    checkpoint,
  };
}

/**
 * @param {CleanupDb} db
 * @param {S3CleanupIntent} intent
 * @param {number} [now]
 */
async function insertIntent(db, intent, now = Date.now()) {
  validateS3CleanupTaskInput(intent, "s3-cleanup queue");
  const createdAt = Number.isFinite(intent.createdAt) ? Math.trunc(intent.createdAt) : now;
  const nextAttemptAt = Number.isFinite(intent.nextAttemptAt)
    ? Math.trunc(intent.nextAttemptAt)
    : createdAt;
  await db.prepare(`
    INSERT OR IGNORE INTO ${S3_CLEANUP_TABLE}
      (${S3_CLEANUP_TASK_FIELDS.ID},
       ${S3_CLEANUP_TASK_FIELDS.SOURCE_JSON},
       ${S3_CLEANUP_TASK_FIELDS.PREFIXES_JSON},
       ${S3_CLEANUP_TASK_FIELDS.STATE},
       ${S3_CLEANUP_TASK_FIELDS.ATTEMPTS},
       ${S3_CLEANUP_TASK_FIELDS.CREATED_AT},
       ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT},
       ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT})
    VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7)
  `).bind(
    intent.taskId,
    JSON.stringify(intent.source),
    JSON.stringify(intent.prefixes),
    S3_CLEANUP_TASK_STATUS.PENDING,
    createdAt,
    now,
    nextAttemptAt
  ).run();
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 */
async function loadTask(db, id) {
  const row = await db.prepare(
    `SELECT * FROM ${S3_CLEANUP_TABLE} WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?1`
  ).bind(id).first();
  return normalizeTaskRow(row);
}

/**
 * @param {CleanupDb} db
 * @param {number} now
 * @param {number} limit
 */
async function listDueTasks(db, now, limit) {
  const result = await db.prepare(`
    SELECT * FROM ${S3_CLEANUP_TABLE}
    WHERE ${S3_CLEANUP_TASK_FIELDS.STATE} = ?1
      AND ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} <= ?2
    ORDER BY ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} ASC
    LIMIT ?3
  `).bind(S3_CLEANUP_TASK_STATUS.PENDING, now, limit).all();
  return (result.results || []).map(normalizeTaskRow).filter((task) => task !== null);
}

/**
 * @param {CleanupDb} db
 * @param {CleanupTask} task
 * @param {number} [now]
 */
async function claimTask(db, task, now = Date.now()) {
  const result = await db.prepare(`
    UPDATE ${S3_CLEANUP_TABLE}
    SET ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT} = ?1,
        ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} = ?2
    WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?3
      AND ${S3_CLEANUP_TASK_FIELDS.STATE} = ?4
      AND ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} <= ?5
  `).bind(
    now,
    now + PROCESSING_LEASE_MS,
    task.id,
    S3_CLEANUP_TASK_STATUS.PENDING,
    now
  ).run();
  return (result?.meta?.changes || 0) > 0;
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 * @param {string} message
 * @param {number} [now]
 */
async function markMalformed(db, id, message, now = Date.now()) {
  await db.prepare(`
    UPDATE ${S3_CLEANUP_TABLE}
    SET ${S3_CLEANUP_TASK_FIELDS.STATE} = ?1,
        ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT} = ?2,
        ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} = NULL,
        ${S3_CLEANUP_TASK_FIELDS.LAST_ERROR} = ?3
    WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?4
  `).bind(S3_CLEANUP_TASK_STATUS.MALFORMED, now, message.slice(0, 500), id).run();
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 */
async function deleteTaskRow(db, id) {
  await db.prepare(
    `DELETE FROM ${S3_CLEANUP_TABLE} WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?1`
  ).bind(id).run();
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 * @param {number} attempts
 * @param {string} message
 * @param {number} [now]
 */
async function markFailure(db, id, attempts, message, now = Date.now()) {
  await db.prepare(`
    UPDATE ${S3_CLEANUP_TABLE}
    SET ${S3_CLEANUP_TASK_FIELDS.STATE} = ?1,
        ${S3_CLEANUP_TASK_FIELDS.ATTEMPTS} = ?2,
        ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT} = ?3,
        ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} = NULL,
        ${S3_CLEANUP_TASK_FIELDS.LAST_ERROR} = ?4
    WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?5
  `).bind(S3_CLEANUP_TASK_STATUS.FAILED, attempts, now, message.slice(0, 500), id).run();
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 * @param {number} attempts
 * @param {string} message
 * @param {number} [now]
 */
async function scheduleRetry(db, id, attempts, message, now = Date.now()) {
  await db.prepare(`
    UPDATE ${S3_CLEANUP_TABLE}
    SET ${S3_CLEANUP_TASK_FIELDS.ATTEMPTS} = ?1,
        ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT} = ?2,
        ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} = ?3,
        ${S3_CLEANUP_TASK_FIELDS.LAST_ERROR} = ?4
    WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?5
  `).bind(attempts, now, now + nextBackoffMs(attempts), message.slice(0, 500), id).run();
}

/**
 * @param {CleanupDb} db
 * @param {string} id
 * @param {CleanupCheckpoint} checkpoint
 * @param {number} [now]
 */
async function saveProgress(db, id, checkpoint, now = Date.now()) {
  await db.prepare(`
    UPDATE ${S3_CLEANUP_TABLE}
    SET ${S3_CLEANUP_TASK_FIELDS.UPDATED_AT} = ?1,
        ${S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT} = ?2,
        ${S3_CLEANUP_TASK_FIELDS.ATTEMPTS} = 0,
        ${S3_CLEANUP_TASK_FIELDS.LAST_ERROR} = NULL,
        ${S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON} = ?3
    WHERE ${S3_CLEANUP_TASK_FIELDS.ID} = ?4
  `).bind(now, now, JSON.stringify(checkpoint), id).run();
}

/**
 * @param {S3Client} s3
 * @param {string} prefix
 * @param {string | null} [continuationToken]
 */
export async function deletePrefixPage(s3, prefix, continuationToken = null) {
  let deleted = 0;
  const query = encodeS3Query({
    "list-type": "2",
    prefix,
    "continuation-token": continuationToken,
  });
  const listRes = await s3.aws.fetch(`${s3.endpoint}/${s3.bucket}?${query}`, { method: "GET" });
  if (!listRes.ok) {
    const body = await listRes.text().catch(() => "");
    throw new Error(`s3 list ${prefix} → ${listRes.status}: ${body}`);
  }
  const xml = await listRes.text();
  // ListObjectsV2 returns <Key>a&amp;b.txt</Key> for key `a&b.txt` —
  // if we don't unescape before the subsequent xmlEscape() on send,
  // `a&amp;amp;b.txt` lands in the Delete body, S3 "successfully"
  // removes a non-existent key, and the real object is silently
  // orphaned with status=done.
  const keys = listXmlTagValues(xml, "Key");
  const truncated = xmlTagValueIsTrue(xml, "IsTruncated");
  if (keys.length > 0) {
    const body = [
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
      "<Delete>",
      ...keys.map((k) => `<Object><Key>${xmlEscape(k)}</Key></Object>`),
      "</Delete>",
    ].join("");
    // S3 DeleteObjects requires a body integrity header — Content-MD5 or
    // x-amz-checksum-*. workerd ships SHA-256 but not MD5, so we use the
    // sha256 checksum.
    const bodyBytes = utf8Encoder.encode(body);
    const sha = await crypto.subtle.digest("SHA-256", bodyBytes);
    const sha_b64 = bytesToBase64(new Uint8Array(sha));
    const delUrl = `${s3.endpoint}/${s3.bucket}?delete`;
    const delRes = await fetchRetryableS3Post(s3.aws, delUrl, {
      method: "POST",
      headers: {
        "content-type": "application/xml",
        "x-amz-checksum-sha256": sha_b64,
      },
      body,
    });
    const delXml = await delRes.text();
    if (!delRes.ok) {
      throw new Error(`s3 delete ${prefix} → ${delRes.status}: ${delXml}`);
    }
    // DeleteObjects reports partial failure in the body — the HTTP
    // status stays 200 even when every key failed (AccessDenied etc).
    const errorBlocks = [...delXml.matchAll(/<Error>([\s\S]*?)<\/Error>/g)];
    if (errorBlocks.length > 0) {
      const details = errorBlocks.slice(0, 5).map((m) => {
        const blk = m[1];
        const k = (/<Key>([^<]*)<\/Key>/.exec(blk) || [])[1] || "?";
        const c = (/<Code>([^<]*)<\/Code>/.exec(blk) || [])[1] || "?";
        const msg = (/<Message>([^<]*)<\/Message>/.exec(blk) || [])[1] || "";
        return `${xmlUnescape(k)}[${c}]: ${xmlUnescape(msg)}`;
      }).join("; ");
      throw new Error(
        `s3 delete ${prefix} partial-fail (${errorBlocks.length} errors): ${details}`
      );
    }
    // Count <Deleted> blocks rather than assume keys.length, in case
    // any keys we requested already disappeared before we got here.
    const deletedBlocks = [...delXml.matchAll(/<Deleted>([\s\S]*?)<\/Deleted>/g)];
    deleted += deletedBlocks.length > 0 ? deletedBlocks.length : keys.length;
  }
  if (!truncated) return { deletedCount: deleted, nextContinuationToken: null };
  const continuationTokens = listXmlTagValues(xml, "NextContinuationToken");
  if (!continuationTokens[0]) {
    throw new Error(`s3 list ${prefix} is truncated without NextContinuationToken`);
  }
  return { deletedCount: deleted, nextContinuationToken: continuationTokens[0] };
}

/**
 * @param {CleanupDb} db
 * @param {S3Client} s3
 * @param {CleanupTask | string} taskOrId
 */
export async function processTask(db, s3, taskOrId) {
  const task = typeof taskOrId === "string" ? await loadTask(db, taskOrId) : taskOrId;
  if (!task) {
    return S3_CLEANUP_OUTCOME.MISSING_TASK;
  }
  if (
    task.state === S3_CLEANUP_TASK_STATUS.FAILED ||
    task.state === S3_CLEANUP_TASK_STATUS.MALFORMED
  ) {
    return S3_CLEANUP_OUTCOME.ALREADY_FAILED;
  }
  if (task.nextAttemptAt != null && task.nextAttemptAt > Date.now()) {
    return S3_CLEANUP_OUTCOME.RETRY;
  }
  if (!(await claimTask(db, task))) {
    return S3_CLEANUP_OUTCOME.RETRY;
  }
  try {
    validateS3CleanupTaskInput(
      { taskId: task.id, prefixes: task.prefixes, source: task.source },
      "s3-cleanup task"
    );
  } catch (err) {
    await markMalformed(
      db,
      task.id,
      err instanceof Error ? err.message : S3_CLEANUP_ERROR.MALFORMED_PAYLOAD
    );
    return S3_CLEANUP_OUTCOME.MALFORMED;
  }

  let totalDeleted = task.checkpoint?.deletedCount ?? 0;
  try {
    const prefixes = /** @type {string[]} */ (task.prefixes);
    let prefixIndex = task.checkpoint?.prefixIndex ?? 0;
    let continuationToken = task.checkpoint?.continuationToken ?? null;
    let pageCount = task.checkpoint?.pageCount ?? 0;
    if (prefixIndex > prefixes.length) {
      throw new Error(
        `s3 cleanup checkpoint prefixIndex ${prefixIndex} exceeds prefix count ${prefixes.length}`
      );
    }
    for (let page = 0; page < MAX_DELETE_PAGES_PER_RUN && prefixIndex < prefixes.length; page += 1) {
      const prefix = prefixes[prefixIndex];
      const r = await deletePrefixPage(s3, prefix, continuationToken);
      // If checkpoint persistence fails after this delete, retry may re-delete
      // the page; S3 DELETE is idempotent and deleted_count is best-effort.
      totalDeleted += r.deletedCount;
      const nextPageCount = pageCount + 1;
      if (r.nextContinuationToken) {
        if (nextPageCount >= MAX_LIST_PAGES) {
          throw new Error(`s3 list ${prefix} exceeded ${MAX_LIST_PAGES} pages`);
        }
        await saveProgress(db, task.id, {
          prefixIndex,
          continuationToken: r.nextContinuationToken,
          pageCount: nextPageCount,
          deletedCount: totalDeleted,
        });
        logStructured("info", "s3_cleanup_task_progress", {
          task_id: task.id,
          prefix_index: prefixIndex,
          deleted_count: totalDeleted,
          has_more_pages: true,
        });
        return S3_CLEANUP_OUTCOME.RETRY;
      }
      prefixIndex += 1;
      continuationToken = null;
      pageCount = 0;
    }
    if (prefixIndex < prefixes.length) {
      await saveProgress(db, task.id, {
        prefixIndex,
        continuationToken: null,
        pageCount: 0,
        deletedCount: totalDeleted,
      });
      logStructured("info", "s3_cleanup_task_progress", {
        task_id: task.id,
        prefix_index: prefixIndex,
        deleted_count: totalDeleted,
        has_more_prefixes: true,
      });
      return S3_CLEANUP_OUTCOME.RETRY;
    }
    logStructured("info", "s3_cleanup_task_done", {
      task_id: task.id, deleted_count: totalDeleted,
    });
    await deleteTaskRow(db, task.id);
    return S3_CLEANUP_OUTCOME.DONE;
  } catch (err) {
    const attempts = (Number.isFinite(task.attempts) ? task.attempts : 0) + 1;
    const msg = errorMessage(err);
    logStructured(attempts >= MAX_ATTEMPTS ? "error" : "warn", "s3_cleanup_task_failed", {
      task_id: task.id,
      attempts,
      final: attempts >= MAX_ATTEMPTS,
      error_message: msg,
    });
    if (attempts >= MAX_ATTEMPTS) {
      await markFailure(db, task.id, attempts, msg);
      return S3_CLEANUP_OUTCOME.FAILED;
    }
    await scheduleRetry(db, task.id, attempts, msg);
    return S3_CLEANUP_OUTCOME.RETRY;
  }
}

export default {
  async fetch() {
    return new Response("s3-cleanup is JSRPC/queue/cron only", { status: 404 });
  },

  /**
   * @param {unknown} _controller
   * @param {S3CleanupEnv} env
   * @param {ExecutionContext} ctx
   */
  async scheduled(_controller, env, ctx) {
    bindLogLevel(env);
    const db = requireDb(env);
    const s3 = buildS3Client(env);
    const now = Date.now();
    const due = await listDueTasks(db, now, CRON_BATCH);
    const results = { scanned: due.length, done: 0, retry: 0, failed: 0, skipped: 0 };
    for (const task of due) {
      const status = await processTask(db, s3, task);
      if (status === S3_CLEANUP_OUTCOME.DONE) results.done++;
      else if (status === S3_CLEANUP_OUTCOME.RETRY) results.retry++;
      else if (
        status === S3_CLEANUP_OUTCOME.FAILED ||
        status === S3_CLEANUP_OUTCOME.MALFORMED
      ) results.failed++;
      else results.skipped++;
    }
    void ctx;
    logStructured("info", "s3_cleanup_cron_tick", { at: now, ...results });
  },

  /**
   * @param {MessageBatch<unknown>} batch
   * @param {S3CleanupEnv} env
   */
  async queue(batch, env) {
    bindLogLevel(env);
    const db = requireDb(env);
    const s3 = buildS3Client(env);
    for (const msg of batch.messages) {
      const body = msg.body;
      if (!body || typeof body !== "object") {
        msg.ack();
        continue;
      }
      const intent = /** @type {S3CleanupIntent} */ (body);
      try {
        validateS3CleanupTaskInput(intent, "s3-cleanup queue");
      } catch (err) {
        logStructured("warn", "s3_cleanup_intent_malformed", {
          error_message: errorMessage(err),
        });
        msg.ack();
        continue;
      }
      try {
        await insertIntent(db, intent);
      } catch (err) {
        logStructured("warn", "s3_cleanup_intent_persist_failed", {
          task_id: intent.taskId,
          error_message: errorMessage(err),
        });
        msg.retry({ delaySeconds: 60 });
        continue;
      }
      try {
        const status = await processTask(db, s3, intent.taskId);
        logStructured("info", "s3_cleanup_queue_consumed", { task_id: intent.taskId, status });
      } catch (err) {
        // Once the D1 row exists, cron owns replay. Ack queue delivery so
        // one transient S3 failure cannot retry-storm the Redis Stream.
        logStructured("warn", "s3_cleanup_queue_error", {
          task_id: intent.taskId,
          error_message: errorMessage(err),
        });
      }
      msg.ack();
    }
  },
};
