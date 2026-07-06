import { textToBase64 } from "./base64.js";
import { ASSETS_TOKEN_RE } from "./assets-token.js";

export const S3_CLEANUP_TASK_ID_PREFIX = "s3cleanup:";
export const S3_CLEANUP_QUEUE_NAME = "worker-delete-s3-cleanup";
export const S3_CLEANUP_TABLE = "s3_cleanup_task";

export const S3_CLEANUP_TASK_STATUS = Object.freeze({
  PENDING: "pending",
  FAILED: "failed",
  MALFORMED: "malformed",
});

export const S3_CLEANUP_OUTCOME = Object.freeze({
  DONE: "done",
  RETRY: "retry",
  FAILED: "failed",
  MALFORMED: "malformed",
  MISSING_TASK: "missing_task",
  ALREADY_FAILED: "already_failed",
});

export const S3_CLEANUP_TASK_FIELDS = Object.freeze({
  ID: "id",
  SOURCE_JSON: "source_json",
  PREFIXES_JSON: "prefixes_json",
  STATE: "state",
  ATTEMPTS: "attempts",
  CREATED_AT: "created_at",
  UPDATED_AT: "updated_at",
  NEXT_ATTEMPT_AT: "next_attempt_at",
  LAST_ERROR: "last_error",
  CHECKPOINT_JSON: "checkpoint_json",
});

export const S3_CLEANUP_ERROR = Object.freeze({
  MALFORMED_PAYLOAD: "malformed_payload",
});

/** @param {string} segment */
function isCanonicalPrefixSegment(segment) {
  return segment !== "" && segment !== "." && segment !== "..";
}

/** @param {string} prefix */
function isCanonicalAssetsPrefix(prefix) {
  const parts = prefix.split("/");
  return parts.length === 5 &&
    parts[0] === "assets" &&
    isCanonicalPrefixSegment(parts[1]) &&
    isCanonicalPrefixSegment(parts[2]) &&
    ASSETS_TOKEN_RE.test(parts[3]) &&
    parts[4] === "";
}

/**
 * @typedef {{ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs?: number }} S3CleanupIntentInput
 * @typedef {{ taskId: string, prefixes: string[], source: Record<string, unknown>, createdAt: number, nextAttemptAt: number }} S3CleanupIntent
 */

/**
 * @param {{ taskId?: unknown, prefixes?: unknown, source?: unknown }} input
 * @param {string} [caller]
 * @returns {void}
 */
export function validateS3CleanupTaskInput({ taskId, prefixes, source }, caller = "s3CleanupTask") {
  if (typeof taskId !== "string" || !taskId.startsWith(S3_CLEANUP_TASK_ID_PREFIX)) {
    throw new Error(`${caller}: taskId must start with '${S3_CLEANUP_TASK_ID_PREFIX}'`);
  }
  if (!Array.isArray(prefixes) || prefixes.length === 0) {
    throw new Error(`${caller}: prefixes must be non-empty array`);
  }
  for (const prefix of prefixes) {
    if (typeof prefix !== "string" || !isCanonicalAssetsPrefix(prefix)) {
      throw new Error(`${caller}: prefixes must be canonical ASSETS prefixes`);
    }
  }
  if (!source || typeof source !== "object") {
    throw new Error(`${caller}: source object required`);
  }
}

/**
 * @param {S3CleanupIntentInput} input
 * @param {string} [caller]
 * @returns {S3CleanupIntent}
 */
export function s3CleanupIntent(
  { taskId, prefixes, source, nowMs = Date.now() },
  caller = "s3CleanupTask"
) {
  validateS3CleanupTaskInput({ taskId, prefixes, source }, caller);
  return {
    taskId,
    prefixes,
    source,
    createdAt: nowMs,
    nextAttemptAt: nowMs,
  };
}

/**
 * Queue stream field values are string-typed, so numeric queue fields are
 * intentionally encoded here instead of leaving callers to coerce them.
 *
 * @param {S3CleanupIntentInput} input
 * @param {string} [caller]
 * @returns {{ id: string, body_b64: string, content_type: "json", attempts: "0", first_seen_ms: string }}
 */
export function s3CleanupQueueFields(input, caller = "s3CleanupTask") {
  const intent = s3CleanupIntent(input, caller);
  return {
    id: crypto.randomUUID(),
    body_b64: textToBase64(JSON.stringify(intent)),
    content_type: "json",
    attempts: "0",
    first_seen_ms: String(intent.createdAt),
  };
}
