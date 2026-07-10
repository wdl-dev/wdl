// Shared state + helpers for control. Handlers import from here; the
// dispatcher in index.js calls ensureInit(env) before route dispatch.

import { RedisClient, WatchError, redisDbFromEnv } from "shared-redis";
import { makeS3Client } from "control-s3";
import { makeR2AdminClient } from "control-r2";
import { extractToken } from "shared-auth-token";
import { validatePrincipalShape } from "shared-auth-roles";
import { BodyTooLargeError, readBoundedText } from "shared-bounded-body";
import { queueStreamKey } from "shared-queue-keys";
import { internalErrorResponse, jsonError, jsonResponse, sanitizeJsonErrorDetails } from "shared-respond";
import { withInternalAuth } from "shared-internal-auth";
import { errorMessage } from "shared-errors";
import { randomHex } from "shared-random-id";
import { deleteLockKey } from "control-lib";
import {
  S3_CLEANUP_QUEUE_NAME,
  S3_CLEANUP_TASK_ID_PREFIX,
  s3CleanupQueueFields,
} from "shared-s3-cleanup-lifecycle";
import {
  createLogLevelBinder,
  createLogger,
  formatError,
  recordRedisCommand,
} from "shared-observability";

export const ROUTES_CHANNEL = "routes:invalidate";
export const ROUTES_FLUSH_CHANNEL = "routes:flush";
export const PATTERNS_CHANNEL = "patterns:invalidate";
export const DEFAULT_JSON_BODY_MAX_BYTES = 1024 * 1024;
export const DECLARED_HOSTS_KEY = "declared-hosts";
export const HOST_DECLARATIONS_PREFIX = "host-declarations:";
const HOSTS_PREFIX = "hosts:";
const DO_ALARM_CLEANUP_TIMEOUT_MS = 5_000;

/**
 * @typedef {import("shared-observability").RedisCommandEvent} RedisCommandEvent
 * @typedef {(level: string, event: string, fields?: Record<string, unknown>) => void} ControlLogger
 * @typedef {{ verify(input: { token: string, action?: string, ns?: string, requestId: string }): Promise<unknown> }} AuthBinding
 * @typedef {{ fetch: typeof fetch }} WorkflowBackend
 * @typedef {{ del(key: string): void, hSet(key: string, field: string, value: Uint8Array | string): void }} BundleCommitMulti
 * @typedef {{
 *   env: Record<string, unknown> | null,
 *   log: ControlLogger | null,
 *   redis: RedisClient | null,
 *   dataRedis: RedisClient | null,
 *   workflows: WorkflowBackend | null,
 *   s3: ReturnType<typeof makeS3Client> | null,
 *   r2: ReturnType<typeof makeR2AdminClient> | null,
 *   service: string,
 * }} ControlState
 */

/** @type {ControlState} */
export const state = {
  env: null,
  log: null,
  redis: null,
  dataRedis: null,
  workflows: null,
  s3: null,
  r2: null,
  service: "control",
};

const bindLogLevel = createLogLevelBinder();
let s3Initialized = false;
let r2Initialized = false;

export { formatError, internalErrorResponse, jsonError, jsonResponse };

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {Record<string, unknown>} env */
export function stringEnv(env) {
  /** @type {Record<string, string | undefined>} */
  const out = {};
  for (const [key, value] of Object.entries(env)) {
    if (typeof value === "string") out[key] = value;
    else if (value === undefined) out[key] = undefined;
  }
  return out;
}

/** @param {unknown} err */
export function errMessage(err) {
  return errorMessage(err);
}

export { randomHex };

/**
 * @param {string} prefix
 * @param {number} [bytes]
 */
export function prefixedId(prefix, bytes = 16) {
  return `${prefix}${randomHex(bytes)}`;
}

/**
 * @template T, S
 * @param {{ session: <U>(fn: (session: S) => Promise<U>) => Promise<U> }} redis
 * @param {{ attempts?: number, onExhausted: () => unknown | Promise<unknown>, onWatchError?: (err: unknown, attempt: number) => void, shouldRetryResult?: (result: T, attempt: number) => boolean }} options
 * @param {(session: S, attempt: number) => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function runOptimistic(redis, { attempts = 5, onExhausted, onWatchError, shouldRetryResult }, fn) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const result = await redis.session((session) => fn(session, attempt));
      if (shouldRetryResult?.(result, attempt)) continue;
      return result;
    } catch (err) {
      if (err instanceof WatchError) {
        onWatchError?.(err, attempt);
        continue;
      }
      throw err;
    }
  }
  return /** @type {T} */ (await onExhausted());
}

/** @param {Record<string, unknown>} env */
export function ensureInit(env) {
  state.env = env;
  if (!state.log) {
    state.service = typeof env.SERVICE_NAME === "string" && env.SERVICE_NAME ? env.SERVICE_NAME : "control";
    state.log = createLogger(state.service);
  }
  bindLogLevel(env);
  const redisAddr = typeof env.REDIS_ADDR === "string" ? env.REDIS_ADDR : "";
  if (!state.redis) {
    state.redis = new RedisClient(redisAddr, {
      db: redisDbFromEnv(env, "REDIS_DB"),
      onCommand: onRedisCommand,
    });
  }
  if (!state.dataRedis) {
    const dataRedisAddr = typeof env.DATA_REDIS_ADDR === "string" && env.DATA_REDIS_ADDR
      ? env.DATA_REDIS_ADDR
      : redisAddr;
    state.dataRedis = new RedisClient(dataRedisAddr, {
      db: redisDbFromEnv(env, "DATA_REDIS_DB"),
      onCommand: onRedisCommand,
    });
  }
  if (!state.workflows) {
    const workflows = env.WORKFLOWS_BACKEND;
    state.workflows = workflows && typeof workflows === "object" && "fetch" in workflows && typeof workflows.fetch === "function"
      ? /** @type {WorkflowBackend} */ (workflows)
      : null;
  }
  const envStrings = stringEnv(env);
  if (!s3Initialized) {
    state.s3 = makeS3Client(envStrings);
    s3Initialized = true;
  }
  if (!r2Initialized) {
    state.r2 = makeR2AdminClient(envStrings);
    r2Initialized = true;
  }
}

/** @returns {ControlLogger} @throws {Error} If control initialization has not created the logger. */
export function requireControlLog() {
  if (!state.log) throw new Error("control log is not initialized");
  return state.log;
}

/** @returns {RedisClient} @throws {Error} If control initialization has not created the primary Redis client. */
export function requireControlRedis() {
  if (!state.redis) throw new Error("control redis is not initialized");
  return state.redis;
}

/** @returns {RedisClient} @throws {Error} If control initialization has not created the data Redis client. */
export function requireControlDataRedis() {
  if (!state.dataRedis) throw new Error("control data redis is not initialized");
  return state.dataRedis;
}

/** @returns {RedisClient} */
export function controlTailRedis() {
  return state.dataRedis || requireControlRedis();
}

/** @returns {ReturnType<typeof makeS3Client> | null} */
export function getControlS3() {
  return state.s3;
}

/** @returns {ReturnType<typeof makeR2AdminClient> | null} */
export function getControlR2() {
  return state.r2;
}

/** @returns {WorkflowBackend | null} */
export function getControlWorkflows() {
  return state.workflows;
}

export function controlInternalJsonHeaders() {
  if (!state.env) {
    throw new Error("control shared state has not been initialized");
  }
  return withInternalAuth({ "content-type": "application/json" }, state.env);
}

/** @param {RedisCommandEvent} event */
function onRedisCommand(event) {
  recordRedisCommand({ metrics: null, log: state.log, service: state.service, event });
}

// Intentionally local, despite looking like routing/deploy/auth errors. Each
// layer owns a slightly different machine-readable wire shape and catch boundary.
export class ControlAbort extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {{ message?: string, [key: string]: unknown }} [details]
   */
  constructor(status, code, details = {}) {
    super(details?.message || code);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

// Domain errors still flow through jsonError() so details cannot shadow the
// top-level wire contract.
/**
 * @param {unknown} err
 * @param {string} [fallbackCode]
 * @param {Record<string, unknown>} [extraDetails]
 */
export function codedErrorResponse(err, fallbackCode = "internal_error", extraDetails = {}) {
  const record = isRecord(err) ? err : {};
  const details = isRecord(record.details) ? record.details : {};
  const status = typeof record.status === "number" ? record.status : 500;
  const code = typeof record.code === "string" && record.code ? record.code : fallbackCode;
  const recordMessage = typeof record.message === "string" && record.message ? record.message : undefined;
  const message = err instanceof ControlAbort
    ? (typeof err.details.message === "string" && err.details.message) || err.message || code
    : recordMessage || (typeof details.message === "string" && details.message) || code;
  return jsonError(
    status,
    code,
    message,
    { ...details, ...extraDetails },
  );
}

// ControlAbort always carries code; the separate wrapper keeps abort call sites
// explicit while ordinary coded errors still get a fallback code.
/**
 * @param {ControlAbort} err
 * @param {Record<string, unknown>} [extraDetails]
 */
export function controlAbortResponse(err, extraDetails = {}) {
  return codedErrorResponse(err, err.code, extraDetails);
}

/**
 * @param {Request} request
 * @param {{ requireObject?: boolean, allowEmpty?: boolean, maxBytes?: number }} [opts]
 */
export async function readJsonBody(
  request,
  { requireObject = false, allowEmpty = false, maxBytes = DEFAULT_JSON_BODY_MAX_BYTES } = {},
) {
  let body;
  try {
    const limited = await readTextBody(request, maxBytes);
    if ("response" in limited) return { response: limited.response };
    const text = limited.text;
    if (text === "") {
      if (!allowEmpty) {
        return {
          response: jsonError(400, "invalid_json", "Body must be valid JSON"),
        };
      }
      body = {};
    } else {
      body = JSON.parse(text);
    }
  } catch {
    return {
      response: jsonError(400, "invalid_json", "Body must be valid JSON"),
    };
  }
  if (requireObject && (!body || typeof body !== "object" || Array.isArray(body))) {
    return {
      response: jsonError(400, "invalid_json_object", "Body must be a JSON object"),
    };
  }
  return { body };
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 * @returns {Promise<{ text: string } | { response: Response }>}
 */
async function readTextBody(request, maxBytes) {
  try {
    return { text: await readBoundedText(request, maxBytes) };
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      return { response: jsonError(413, "request_body_too_large", `Body must be at most ${maxBytes} bytes`) };
    }
    throw err;
  }
}

// AuthPolicyError → HTTP body shape. 4xx messages are user-actionable;
// 5xx messages stay generic so internal auth diagnostics do not leak.
/** @param {unknown} err */
export function authErrorBody(err) {
  const record = isRecord(err) ? err : {};
  const status = typeof record.status === "number" ? record.status : null;
  const reason = typeof record.reason === "string" ? record.reason : undefined;
  const message = typeof record.message === "string" ? record.message : "rejected";
  if (status && status >= 400 && status < 500) {
    const details = isRecord(record.details)
      ? /** @type {Record<string, unknown> | undefined} */ (sanitizeJsonErrorDetails(record.details))
      : undefined;
    return {
      status,
      body: {
        ...(details || {}),
        error: reason || "rejected",
        message,
      },
    };
  }
  if (status && status >= 500 && reason) {
    return { status, body: { error: reason, message: "auth error" } };
  }
  return { status: 503, body: { error: "auth_unavailable", message: "auth unavailable" } };
}

// AuthPolicyError from auth's JSRPC arrives with {status, reason} preserved
// by enhanced_error_serialization.
/**
 * @param {unknown} err
 * @param {string} requestId
 * @param {string} command
 */
export function authPolicyResponse(err, requestId, command) {
  const { status, body } = authErrorBody(err);
  const record = isRecord(err) ? err : {};
  const reason = typeof record.reason === "string" ? record.reason : undefined;
  const isServerError = status >= 500;
  requireControlLog()(isServerError ? "error" : "warn",
    isServerError ? "auth_lifecycle_error" : "auth_lifecycle_rejected", {
      request_id: requestId,
      command,
      status,
      ...formatError(err),
      ...(reason ? { reason } : {}),
    });
  return jsonResponse(status, body);
}

// `receivers` is Redis's subscriber count at PUBLISH time — visibility
// only, not a processing ack.
/**
 * @param {string} channel
 * @param {string} payload
 * @param {string} scope
 * @param {string} requestId
 */
export async function publishOne(channel, payload, scope, requestId) {
  const startedAt = Date.now();
  const log = requireControlLog();
  try {
    const receivers = await requireControlRedis().publish(channel, payload);
    const durationMs = Date.now() - startedAt;
    log("info", "invalidation_published", {
      request_id: requestId,
      channel,
      scope,
      payload,
      receivers,
      duration_ms: durationMs,
    });
    return { ok: true, channel, receivers, duration_ms: durationMs };
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    log("error", "invalidation_publish_failed", {
      request_id: requestId,
      channel,
      scope,
      payload,
      duration_ms: durationMs,
      ...formatError(err),
    });
    return {
      ok: false,
      channel,
      error: errMessage(err),
      duration_ms: durationMs,
    };
  }
}

/**
 * @param {RedisClient} redis
 * @param {string} pattern
 */
async function scanKeys(redis, pattern) {
  /** @type {string[]} */
  const keys = [];
  let cursor = "0";
  do {
    const [next, found] = await redis.scan(cursor, pattern, 100);
    keys.push(...found);
    cursor = next;
  } while (cursor !== "0");
  return keys;
}

/** @param {string} key */
function namespaceFromHostsKey(key) {
  return key.startsWith(HOSTS_PREFIX) ? key.slice(HOSTS_PREFIX.length) : "";
}

/**
 * Rebuild the global declared-host gate from namespace-owned `hosts:<ns>` sets.
 * This is an ops reload repair path, not the ordinary writer; `/hosts` reconcile
 * owns day-to-day declaration mutations.
 *
 * @param {RedisClient} [redis]
 */
export async function rebuildDeclaredHostIndexes(redis = requireControlRedis()) {
  const hostKeys = await scanKeys(redis, `${HOSTS_PREFIX}*`);
  const oldDeclarationKeys = await scanKeys(redis, `${HOST_DECLARATIONS_PREFIX}*`);
  /** @type {Map<string, Set<string>>} */
  const declarationsByHost = new Map();
  for (const key of hostKeys) {
    const ns = namespaceFromHostsKey(key);
    if (!ns) continue;
    for (const host of await redis.sMembers(key)) {
      if (!host) continue;
      const declarations = declarationsByHost.get(host) || new Set();
      declarations.add(ns);
      declarationsByHost.set(host, declarations);
    }
  }

  await redis.session(async (iso) => {
    const multi = iso.multi();
    multi.del(DECLARED_HOSTS_KEY, ...oldDeclarationKeys);
    for (const [host, namespaces] of declarationsByHost) {
      multi.sAdd(DECLARED_HOSTS_KEY, host);
      multi.sAdd(`${HOST_DECLARATIONS_PREFIX}${host}`, [...namespaces]);
    }
    await multi.exec();
  });
  return {
    declaredHosts: declarationsByHost.size,
    declarationKeysRemoved: oldDeclarationKeys.length,
  };
}

/** @param {string} requestId */
async function repairDeclaredHostsForReload(requestId) {
  const startedAt = Date.now();
  const log = requireControlLog();
  try {
    const result = await rebuildDeclaredHostIndexes();
    const durationMs = Date.now() - startedAt;
    log("info", "declared_hosts_rebuilt", {
      request_id: requestId,
      declared_hosts: result.declaredHosts,
      declaration_keys_removed: result.declarationKeysRemoved,
      duration_ms: durationMs,
    });
    return { ok: true, ...result, duration_ms: durationMs };
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    log("error", "declared_hosts_rebuild_failed", {
      request_id: requestId,
      duration_ms: durationMs,
      ...formatError(err),
    });
    return {
      ok: false,
      error: errMessage(err),
      duration_ms: durationMs,
    };
  }
}

// Route namespace invalidation and full route-state flush are separate
// channels: routes:invalidate payloads are always namespace names.
/** @param {string} requestId */
export async function publishReload(requestId) {
  const declarations = await repairDeclaredHostsForReload(requestId);
  if (!declarations.ok) {
    return { ok: false, declarations };
  }
  const [routes, patterns] = await Promise.all([
    publishOne(ROUTES_FLUSH_CHANNEL, "", "all", requestId),
    publishOne(PATTERNS_CHANNEL, "*", "all", requestId),
  ]);
  return { ok: routes.ok && patterns.ok, declarations, routes, patterns };
}

/**
 * @param {BundleCommitMulti} multi
 * @param {string} key
 * @param {{ meta: unknown, normalized: Iterable<[string, Uint8Array | string]> }} bundle
 */
export function stageBundleCommit(multi, key, { meta, normalized }) {
  multi.del(key);
  for (const [path, bytes] of normalized) multi.hSet(key, path, bytes);
  multi.hSet(key, "__meta__", JSON.stringify(meta));
}

// NEVER use x-request-id as the cleanup-task id: two retries with the same
// request id would overwrite each other's task row.
export function buildS3CleanupTaskId() {
  return `${S3_CLEANUP_TASK_ID_PREFIX}${crypto.randomUUID()}`;
}

export function generateLockToken() {
  return crypto.randomUUID();
}

// TTL only matters when a handler crashes mid-flow (normal path releases
// in a finally block); 30s covers the Redis critical section and no more.
const DELETE_LOCK_TTL_SECONDS = 30;
/**
 * @param {RedisClient} redis
 * @param {string} ns
 * @param {string} worker
 */
export async function acquireDeleteLock(redis, ns, worker) {
  const token = generateLockToken();
  const key = deleteLockKey(ns, worker);
  const reply = await redis.set(key, token, { nx: true, ttl: DELETE_LOCK_TTL_SECONDS });
  return reply === "OK" ? token : null;
}

// Compare-and-delete so we don't free a token a TTL-expired-then-reacquired
// lock holder is depending on.
/**
 * @param {RedisClient} redis
 * @param {string} ns
 * @param {string} worker
 * @param {string | null} token
 * @param {string} requestId
 */
export async function releaseDeleteLock(redis, ns, worker, token, requestId) {
  if (!token) return;
  const key = deleteLockKey(ns, worker);
  try {
    await redis.delIfEq(key, token);
  } catch (err) {
    requireControlLog()("warn", "delete_lock_release_failed", {
      request_id: requestId,
      namespace: ns, worker,
      ...formatError(err),
    });
  }
}

// Standalone intent write for deploy aborts, where the cleanup follows a
// failed commit path rather than sharing a delete transaction.
/**
 * @param {{ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs?: number }} intent
 */
export async function recordS3CleanupIntent({ taskId, prefixes, source, nowMs = Date.now() }) {
  const fields = s3CleanupQueueFields(
    { taskId, prefixes, source, nowMs },
    "recordS3CleanupIntent"
  );
  await requireControlDataRedis().xadd(queueStreamKey("__system__", S3_CLEANUP_QUEUE_NAME), fields);
}

/**
 * @param {{
 *   cleanupIntent: ({ taskId: string, prefixes: string[], source: Record<string, unknown>, nowMs?: number } | null),
 *   cleanupTaskId: string | null,
 *   warningMessage: string,
 *   logEvent: string,
 *   logFields: Record<string, unknown>,
 *   log: ControlLogger,
 * }} args
 */
export async function recordCleanupIntentOrWarn({
  cleanupIntent,
  cleanupTaskId,
  warningMessage,
  logEvent,
  logFields,
  log,
}) {
  const warnings = [];
  let queueHintStatus = cleanupTaskId ? "queued" : "none";
  if (cleanupIntent) {
    try {
      await recordS3CleanupIntent(cleanupIntent);
    } catch (err) {
      queueHintStatus = "failed";
      warnings.push({
        code: "cleanup_queue_failed",
        message: warningMessage,
      });
      log("warn", logEvent, {
        ...logFields,
        task_id: cleanupTaskId,
        error_message: errMessage(err),
      });
    }
  }
  return { queueHintStatus, warnings };
}

/**
 * @param {{ ns: string, worker: string, version?: string, allowCleanup?: boolean }} args
 */
export async function assertWorkflowDeleteAllowed({ ns, worker, version = undefined, allowCleanup = false }) {
  if (!state.workflows || typeof state.workflows.fetch !== "function") {
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow lifecycle check is unavailable",
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
    });
  }
  let response;
  let body;
  try {
    response = await state.workflows.fetch(
      "http://workflows/internal/workflows/lifecycle/check-delete",
      {
        method: "POST",
        headers: controlInternalJsonHeaders(),
        body: JSON.stringify({
          ns,
          worker,
          ...(version ? { version } : {}),
          ...(allowCleanup ? { allowCleanup: true } : {}),
        }),
      },
    );
    body = await response.json().catch(() => null);
  } catch (err) {
    state.log?.("error", "workflow_lifecycle_check_failed", {
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
      error_message: errMessage(err),
    });
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow lifecycle check failed",
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
    });
  }
  if (!response.ok) {
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow lifecycle check failed",
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
      upstream_status: response.status,
      upstream_error: isRecord(body) && typeof body.error === "string" ? body.error : null,
    });
  }
  if (!isRecord(body) || typeof body.allowed !== "boolean") {
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow lifecycle check returned an invalid response",
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
    });
  }
  if (body.allowed !== true) {
    throw new ControlAbort(409, "workflow_instances_active", {
      message: version
        ? `${ns}/${worker}/${version} has active workflow instances`
        : `${ns}/${worker} has active workflow instances`,
      namespace: ns,
      worker,
      ...(version ? { version } : {}),
      count: Number.isFinite(body.count) ? body.count : 0,
      blockers: Array.isArray(body.blockers) ? body.blockers : [],
    });
  }
}

/**
 * @param {{ ns: string, worker: string, doStorageId: string }} args
 */
export async function cleanupDoAlarmsForWorker({ ns, worker, doStorageId }) {
  if (!state.workflows || typeof state.workflows.fetch !== "function") {
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow DO alarm cleanup is unavailable",
      namespace: ns,
      worker,
    });
  }
  let response;
  let body;
  try {
    response = await state.workflows.fetch(
      "http://workflows/internal/workflows/do-alarms/cleanup-worker",
      {
        method: "POST",
        headers: controlInternalJsonHeaders(),
        body: JSON.stringify({ ns, worker, doStorageId }),
        signal: AbortSignal.timeout(DO_ALARM_CLEANUP_TIMEOUT_MS),
      },
    );
    body = await response.json().catch(() => null);
  } catch (err) {
    state.log?.("error", "workflow_do_alarm_cleanup_failed", {
      namespace: ns,
      worker,
      error_message: errMessage(err),
    });
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow DO alarm cleanup failed",
      namespace: ns,
      worker,
    });
  }
  if (!response.ok || !isRecord(body) || body.ok !== true) {
    throw new ControlAbort(503, "workflow_internal_dispatch_failed", {
      message: "Workflow DO alarm cleanup failed",
      namespace: ns,
      worker,
      upstream_status: response.status,
      upstream_error: isRecord(body) && typeof body.error === "string" ? body.error : null,
    });
  }
}

// Fail-closed 503 on AUTH throw / Redis explosion / missing binding.
/**
 * @param {Request} request
 * @param {Record<string, unknown>} env
 * @param {{ action?: string, ns?: string }} routeInfo
 * @param {string} requestId
 */
export async function authorizeControlRequest(request, env, routeInfo, requestId) {
  const log = requireControlLog();
  const auth = env.AUTH;
  if (!auth || typeof auth !== "object" || !("verify" in auth) || typeof auth.verify !== "function") {
    log("error", "auth_misconfigured", { request_id: requestId });
    return {
      ok: false,
      status: 503,
      error: "auth_misconfigured",
      message: "Control is misconfigured (AUTH binding missing)",
    };
  }
  const token = extractToken(request.headers);
  if (!token) {
    return { ok: false, status: 401, error: "missing_token", message: "unauthorized" };
  }
  let result;
  try {
    result = await /** @type {AuthBinding} */ (auth).verify({
      token,
      action: routeInfo.action,
      ns: routeInfo.ns,
      requestId,
    });
  } catch (err) {
    log("error", "auth_verify_threw", {
      request_id: requestId,
      action: routeInfo.action,
      target_ns: routeInfo.ns,
      ...formatError(err),
    });
    return { ok: false, status: 503, error: "auth_unavailable", message: "auth unavailable" };
  }
  if (!isRecord(result) || typeof result.ok !== "boolean") {
    log("error", "auth_verify_bad_shape", { request_id: requestId, stage: "envelope" });
    return { ok: false, status: 503, error: "auth_invalid_shape", message: "auth returned invalid shape" };
  }
  if (!result.ok) {
    // 5xx must not get "unauthorized" — text would contradict the status
    // (auth contract failure, not a token problem).
    const status = typeof result.status === "number" ? result.status : 401;
    let error;
    if (status >= 500) error = "auth error";
    else if (status === 403) error = "forbidden";
    else error = "unauthorized";
    return {
      ok: false,
      status,
      error: typeof result.reason === "string" && result.reason ? result.reason : error,
      message: error,
    };
  }
  // Bad allow shape → 503 (auth contract break), not a downstream policy
  // error that would mask the real cause.
  const principal = result.principal;
  const validPrincipal = validatePrincipalShape(principal);
  const validTokenId = typeof result.tokenId === "string" && result.tokenId.length > 0;
  if (!validPrincipal || !validTokenId) {
    log("error", "auth_verify_bad_shape", {
      request_id: requestId,
      stage: "allow_payload",
      action: routeInfo.action,
      has_token_id: validTokenId,
      principal_kind: isRecord(principal) ? principal.kind : null,
    });
    return { ok: false, status: 503, error: "auth_invalid_shape", message: "auth returned invalid shape" };
  }
  return {
    ok: true,
    status: 200,
    principal,
    tokenId: result.tokenId,
  };
}
