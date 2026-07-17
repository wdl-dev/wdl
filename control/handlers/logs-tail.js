// SSE handler for `wdl tail`. Pull-based ReadableStream + pre-registered
// ctx.waitUntil(cancelPromise) keeps cleanup outside the cancel callback.
// workerd >= 2026-06-19 no longer reliably calls cancel() on client
// disconnect, so max-session and idle-pull cleanup use independent watchdogs.

import { RedisSession, redisDbFromEnv } from "shared-redis";
import { envValueOr } from "shared-env";
import { PLATFORM_TIER_RESERVED_NS } from "shared-auth-roles";
import { isReservedNs, isValidWorkerName, WORKER_NAME_RE } from "shared-ns-pattern";
import { compareStreamIds, isValidResumeId } from "control-lib";
import { controlTailRedis, errMessage, jsonError, requireControlLog } from "control-shared";

const TAIL_ACTIVATION_CHANNEL = "logs:tail:active";
const TAIL_ACTIVATION_TTL_SECONDS = 30;
const TAIL_ACTIVATION_MAX_ENTRIES = 10_000;
const XREAD_BLOCK_MS = 10_000;
const SSE_KEEPALIVE_MS = 5_000;
const LOG_TAIL_IDLE_PULL_GRACE_FACTOR = 3;
const LOG_TAIL_IDLE_PULL_MS = SSE_KEEPALIVE_MS * LOG_TAIL_IDLE_PULL_GRACE_FACTOR;
export const LOG_TAIL_MAX_SESSION_MS_DEFAULT = 15 * 60 * 1000;
const MAX_WORKERS_PER_TAIL_SESSION = 50;
const JSON_FIELD_BYTES = [0x6a, 0x73, 0x6f, 0x6e]; // "json"
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
/**
 * @param {string} ns
 * @param {string} name
 */
const TAIL_STREAM_KEY = (ns, name) => `logs:${ns}:${name}:s`;

const SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache, no-transform",
  // Hint to nginx / Envoy / similar proxies: do not buffer the response.
  // Without this some intermediaries hold the entire body until close.
  "x-accel-buffering": "no",
};

/**
 * @typedef {{
 *   exists(key: string): Promise<number>,
 *   xRange(key: string, start: string, end: string, countKeyword: string, count: string): Promise<Array<[Uint8Array, Uint8Array[]]>>,
 *   hLen(key: string): Promise<number>,
 *   hGetEx(key: string, ttlSeconds: number, fields: string[]): Promise<Array<Uint8Array | string | null | undefined>>,
 *   hSetEx(key: string, ttlSeconds: number, fields: Record<string, string>): Promise<unknown>,
 * }} TailRedis
 * @typedef {{ xRead(...args: string[]): Promise<unknown> }} TailSession
 * @typedef {[Uint8Array, Uint8Array[]]} TailStreamItem
 * @typedef {[Uint8Array, TailStreamItem[]]} TailStreamBatch
 */

/** @param {URL} url */
function parseSelector(url) {
  if (url.searchParams.has("all")) {
    return { error: jsonError(400, "unsupported_selector",
      "?all is not supported; specify one or more ?worker=<name> parameters.") };
  }
  const workers = url.searchParams.getAll("worker");
  if (workers.length === 0) {
    return { error: jsonError(400, "missing_worker",
      "Must specify ?worker=<name> (one or more).") };
  }
  const uniqueCount = new Set(workers).size;
  if (uniqueCount > MAX_WORKERS_PER_TAIL_SESSION) {
    return { error: jsonError(400, "too_many_workers",
      `A tail session can subscribe to at most ${MAX_WORKERS_PER_TAIL_SESSION} workers.`) };
  }
  for (const w of workers) {
    if (!isValidWorkerName(w)) {
      return { error: jsonError(400, "invalid_worker_name",
        `Invalid worker name "${w}". Must match ${WORKER_NAME_RE}.`) };
    }
  }
  return { workers };
}

// Resume id is single-worker only — a single SSE Last-Event-ID / since
// param cannot represent N independent stream cursors.
//
// Precedence rule: when both are present, `Last-Event-ID` (set fresh by
// the SSE client on every reconnect) wins over `?since=` (frozen on the
// initial CLI invocation). Otherwise an in-process auto-reconnect would
// re-replay from the original --since cursor every time, duplicating
// every event between --since and the most recent id.
//
// Multi-worker handling:
//   - `?since=<id>` rejected up front (explicit user intent, surface
//     the error rather than silently misinterpreting it).
//   - `Last-Event-ID` header silently ignored (browser SSE clients
//     auto-attach it on every reconnect; failing those would defeat
//     reconnect entirely for the legitimate multi-worker session).
/** @param {{ url: URL, headers: Headers, isMultiWorker: boolean }} args */
function parseResume({ url, headers, isMultiWorker }) {
  const sinceParam = url.searchParams.get("since");
  const lastEventId = headers.get("last-event-id");
  if (isMultiWorker) {
    if (sinceParam) {
      return { error: jsonError(400, "since_single_worker_only",
        "--since is only valid for single-worker subscriptions.") };
    }
    return { resumeId: null };
  }
  const candidate = lastEventId || sinceParam || null;
  if (candidate === null) return { resumeId: null };
  if (!isValidResumeId(candidate)) {
    return { error: jsonError(400, "invalid_resume_id",
      `Resume id must match Redis stream id grammar <ms>-<seq>; got ${JSON.stringify(candidate)}.`) };
  }
  return { resumeId: candidate };
}

/** @param {{ workers: string[] }} args */
function resolveWorkers({ workers }) {
  return [...new Set(workers)].toSorted();
}

/** @param {Record<string, unknown> | null | undefined} env */
export function tailMaxSessionMs(env) {
  const raw = envValueOr(env?.LOG_TAIL_MAX_SESSION_MS, LOG_TAIL_MAX_SESSION_MS_DEFAULT);
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : LOG_TAIL_MAX_SESSION_MS_DEFAULT;
}

// Pre-check that the resume id can still be honored. If the stream key
// expired since last XADD, no first id exists for comparison so we
// distinguish "expired" from "trimmed" with separate codes (see doc).
/** @param {{ redis: TailRedis, streamKey: string, resumeId: string }} args */
async function checkResumePoint({ redis, streamKey, resumeId }) {
  const exists = await redis.exists(streamKey);
  if (exists === 0) {
    return { warning: { code: "resume_stream_expired",
      message: "Resume stream has expired (no recent activity)." } };
  }
  // XRANGE … COUNT 1 returns [[idBuf, [k1Buf,v1Buf,...]]] or [].
  const reply = await redis.xRange(streamKey, "-", "+", "COUNT", "1");
  const firstIdRaw = reply && reply[0] && reply[0][0];
  if (!firstIdRaw) return { warning: null };
  const firstId = utf8Decoder.decode(firstIdRaw);
  if (compareStreamIds(resumeId, firstId) < 0) {
    return { warning: { code: "resume_point_trimmed",
      message: "Resume id is older than the stream's current first entry (MAXLEN trim)." } };
  }
  return { warning: null };
}

/**
 * @param {Uint8Array} field
 */
function isJsonField(field) {
  return field.length === JSON_FIELD_BYTES.length &&
    field[0] === JSON_FIELD_BYTES[0] &&
    field[1] === JSON_FIELD_BYTES[1] &&
    field[2] === JSON_FIELD_BYTES[2] &&
    field[3] === JSON_FIELD_BYTES[3];
}

/**
 * @param {Uint8Array[]} fields
 */
function findJsonField(fields) {
  for (let i = 0; i + 1 < fields.length; i += 2) {
    if (isJsonField(fields[i])) return utf8Decoder.decode(fields[i + 1]);
  }
  return null;
}

/**
 * @param {TailRedis} redis
 * @param {string[]} keys
 */
export async function activateTailWorkers(redis, keys) {
  if (keys.length === 0) return;
  const active = await redis.hGetEx(TAIL_ACTIVATION_CHANNEL, TAIL_ACTIVATION_TTL_SECONDS, keys);
  const missing = [];
  for (let i = 0; i < keys.length; i++) {
    if (active[i] == null) missing.push(keys[i]);
  }
  let allowedMissing = missing;
  if (missing.length > 0) {
    const count = await redis.hLen(TAIL_ACTIVATION_CHANNEL);
    allowedMissing = missing.slice(0, Math.max(0, TAIL_ACTIVATION_MAX_ENTRIES - count));
  }
  /** @type {Record<string, string>} */
  const fields = {};
  for (const key of allowedMissing) fields[key] = "1";
  if (Object.keys(fields).length > 0) {
    await redis.hSetEx(TAIL_ACTIVATION_CHANNEL, TAIL_ACTIVATION_TTL_SECONDS, fields);
  }
}

/**
 * @param {string} ns
 * @param {string[]} workers
 */
export function tailActivationKeys(ns, workers) {
  return workers.map((w) => `${ns}:${w}`);
}

export class RedisTailCursor {
  /** @param {{ ns: string, workers: string[] }} args */
  constructor({ ns, workers }) {
    this.streamKeys = workers.map((w) => TAIL_STREAM_KEY(ns, w));
    this.ids = this.streamKeys.map(() => "$");
    this.streamByKey = new Map(this.streamKeys.map((key, idx) => [
      key,
      { idx, workerName: workers[idx] },
    ]));
  }

  /** @param {string} resumeId */
  setResumeId(resumeId) {
    this.ids[0] = resumeId;
  }

  /**
   * @param {TailSession} session
   * @param {number} blockMs
   */
  async readBatch(session, blockMs) {
    const reply = await session.xRead(
      "BLOCK", String(blockMs), "STREAMS",
      ...this.streamKeys, ...this.ids,
    );
    if (!reply) return null;
    return this.decode(reply);
  }

  /** @param {unknown} reply */
  decode(reply) {
    const batches = /** @type {TailStreamBatch[]} */ (Array.isArray(reply) ? reply : []);
    const events = [];
    const parseFailures = [];
    for (const entry of batches) {
      const keyBytes = entry[0];
      const items = entry[1];
      const streamKey = utf8Decoder.decode(keyBytes);
      const streamMeta = this.streamByKey.get(streamKey);
      if (streamMeta === undefined) continue; // Redis returned a stream we didn't ask for — skip
      const { idx, workerName } = streamMeta;
      for (const [idBuf, fields] of items) {
        const id = utf8Decoder.decode(idBuf);
        this.ids[idx] = id;
        const jsonStr = findJsonField(fields);
        if (jsonStr === null) continue;
        // Inject worker name so multi-worker CLI output does not depend
        // on every stream-backed payload carrying worker_id.
        /** @type {unknown} */
        let payloadObj;
        try { payloadObj = JSON.parse(jsonStr); }
        catch {
          parseFailures.push({ id, workerName });
          continue;
        }
        if (!payloadObj || typeof payloadObj !== "object" || Array.isArray(payloadObj)) continue;
        const payloadRecord = /** @type {Record<string, unknown>} */ (payloadObj);
        payloadRecord.worker = workerName;
        const eventName = typeof payloadRecord.event === "string"
          ? payloadRecord.event
          : "worker_event";
        events.push({ id, workerName, eventName, payloadObj: payloadRecord });
      }
    }
    return { events, parseFailures };
  }
}

/**
 * @param {{ event?: string, id?: string, data: string }} opts
 */
function sseEvent(opts) {
  // No `id:` line means the client's Last-Event-ID resume cursor is
  // untouched — required for control-generated events that don't have a
  // Redis stream id (would corrupt the cursor on next reconnect).
  const lines = [];
  if (opts.event) lines.push(`event: ${opts.event}`);
  if (opts.id) lines.push(`id: ${opts.id}`);
  lines.push(`data: ${opts.data}`);
  lines.push("", "");
  return lines.join("\n");
}

/**
 * @param {{ request: Request, env: Record<string, unknown>, ctx: { waitUntil(promise: Promise<unknown>): void } | null, ns: string, requestId: string }} args
 */
export async function handle({ request, env, ctx, ns, requestId }) {
  const log = requireControlLog();
  const redis = controlTailRedis();
  const maxSessionMs = tailMaxSessionMs(env);
  // Non-platform reserved-ns 404 — after auth (caller's auth result is
  // already validated by control's dispatcher). Platform-tier namespaces
  // are resource-shaped but still need their own live-debug surface.
  if (isReservedNs(ns) && !PLATFORM_TIER_RESERVED_NS.has(ns)) {
    return jsonError(404, "not_found", "Not found");
  }

  const url = new URL(request.url);
  const sel = parseSelector(url);
  if (sel.error) return sel.error;

  const isMultiWorker = sel.workers.length > 1;
  const resume = parseResume({ url, headers: request.headers, isMultiWorker });
  if (resume.error) return resume.error;

  const workers = resolveWorkers({ workers: sel.workers });
  if (workers.length === 0) {
    return jsonError(400, "no_workers_subscribed", "No worker names provided.");
  }

  const tailCursor = new RedisTailCursor({ ns, workers });

  /** @type {{ code: string, message: string } | null} */
  let pendingWarning = null;
  if (!isMultiWorker && resume.resumeId !== null) {
    const { warning } = await checkResumePoint({
      redis, streamKey: tailCursor.streamKeys[0], resumeId: resume.resumeId,
    });
    if (warning) {
      pendingWarning = warning;
      // fall back to fresh start; the warning event prefixes the stream.
    } else {
      tailCursor.setResumeId(resume.resumeId);
    }
  }

  if (!ctx || typeof ctx.waitUntil !== "function") {
    log("error", "tail_ctx_unavailable", { request_id: requestId });
    return jsonError(503, "ctx_unavailable", "Streaming response requires ctx.waitUntil");
  }

  const redisAddress = typeof env.DATA_REDIS_ADDR === "string" && env.DATA_REDIS_ADDR
    ? env.DATA_REDIS_ADDR
    : typeof env.REDIS_ADDR === "string"
      ? env.REDIS_ADDR
      : "";
  const session = new RedisSession(redisAddress, {
    db: redisDbFromEnv(env, "DATA_REDIS_DB"),
  });
  let sessionOpen = false;
  /** @type {Promise<void> | null} */
  let sessionClosePromise = null;
  let cancelled = false;
  /** @type {ReadableStreamDefaultController<Uint8Array> | null} */
  let streamController = null;
  let lastPullAtMs = Date.now();
  const { promise: cancelPromise, resolve: resolveCancel } =
    /** @type {PromiseWithResolvers<void>} */ (Promise.withResolvers());

  async function closeSessionIfOpen() {
    if (!sessionOpen && !session.hasOpenResources()) return;
    sessionClosePromise ??= (async () => {
      try {
        await session.close();
      } catch (err) {
        log("warn", "tail_session_close_failed", {
          request_id: requestId, namespace: ns,
          error_message: errMessage(err),
        });
      }
    })();
    await sessionClosePromise;
  }

  /**
   * @param {ReadableStreamDefaultController<Uint8Array> | null} controller
   * @param {{ code: string, message: string, logEvent: string, logFields: Record<string, unknown> }} warning
   */
  function closeWithWarning(controller, warning) {
    if (cancelled) return;
    cancelled = true;
    log("info", warning.logEvent, warning.logFields);
    if (controller) {
      try {
        controller.enqueue(utf8Encoder.encode(sseEvent({
          event: "tail_warning",
          data: JSON.stringify({
            event: "tail_warning",
            code: warning.code,
            message: warning.message,
          }),
        })));
      } catch {}
      try { controller.close(); } catch {}
    }
    resolveCancel();
  }

  /** @param {ReadableStreamDefaultController<Uint8Array> | null} controller */
  function expireSession(controller) {
    closeWithWarning(controller, {
      code: "session_expired",
      message: "Tail session reached its maximum lifetime; reconnecting for reauthorization.",
      logEvent: "tail_session_expired",
      logFields: {
        request_id: requestId, namespace: ns, worker_count: workers.length,
        max_session_ms: maxSessionMs,
      },
    });
  }

  /** @param {ReadableStreamDefaultController<Uint8Array> | null} controller */
  function idleSession(controller) {
    closeWithWarning(controller, {
      code: "session_idle",
      message: "Tail session stopped receiving client reads; reconnecting closes the abandoned session.",
      logEvent: "tail_session_idle",
      logFields: {
        request_id: requestId, namespace: ns, worker_count: workers.length,
        idle_pull_ms: Date.now() - lastPullAtMs,
        idle_limit_ms: LOG_TAIL_IDLE_PULL_MS,
      },
    });
  }

  const expiryTimer = setTimeout(() => expireSession(streamController), maxSessionMs);
  if (typeof expiryTimer === "object" && typeof expiryTimer.unref === "function") {
    expiryTimer.unref();
  }
  /** @type {ReturnType<typeof setTimeout> | null} */
  let idleTimer = null;
  function scheduleIdleWatchdog() {
    const delayMs = Math.max(1, LOG_TAIL_IDLE_PULL_MS - (Date.now() - lastPullAtMs));
    idleTimer = setTimeout(() => {
      if (cancelled) return;
      if (Date.now() - lastPullAtMs >= LOG_TAIL_IDLE_PULL_MS) {
        idleSession(streamController);
        return;
      }
      scheduleIdleWatchdog();
    }, delayMs);
    if (typeof idleTimer === "object" && typeof idleTimer.unref === "function") {
      idleTimer.unref();
    }
  }
  scheduleIdleWatchdog();

  // Pre-register cleanup. Per CLAUDE.md gotcha: scheduling waitUntil from
  // inside cancel races IoContext teardown — cancel only resolves the
  // promise, the actual close happens here.
  ctx.waitUntil(cancelPromise.then(async () => {
    clearTimeout(expiryTimer);
    if (idleTimer) clearTimeout(idleTimer);
    await closeSessionIfOpen();
    log("info", "tail_session_close", {
      request_id: requestId, namespace: ns, worker_count: workers.length,
    });
  }));

  log("info", "tail_session_open", {
    request_id: requestId, namespace: ns, worker_count: workers.length,
    resume: resume.resumeId !== null ? "yes" : "no",
    selector: "explicit",
  });

  const sessionStartedAtMs = Date.now();
  let bootstrapped = false;
  let openNoticeSent = false;

  const stream = new ReadableStream({
    start(controller) {
      streamController = controller;
    },
    async pull(controller) {
      streamController = controller;
      lastPullAtMs = Date.now();
      if (cancelled) {
        try { controller.close(); } catch {}
        return;
      }
      try {
        if (!bootstrapped) {
          // Open BEFORE flipping bootstrapped so a session.open() throw
          // lets the next pull retry the whole bootstrap, not skip past
          // it and fail on the first Redis stream read.
          await session.open();
          sessionOpen = true;
          if (cancelled) {
            await closeSessionIfOpen();
            try { controller.close(); } catch {}
            return;
          }
          bootstrapped = true;
        }

        const remainingMs = maxSessionMs - (Date.now() - sessionStartedAtMs);
        if (remainingMs <= 0) {
          expireSession(controller);
          return;
        }

        // Heartbeat subscribed workers. Write the Valkey HFE active hash
        // directly so `tail-open` means the runtime's active-set probe can
        // observe the subscriber; pub/sub delivery was too racy for that
        // contract. Failures don't block the read.
        try {
          await activateTailWorkers(redis, tailActivationKeys(ns, workers));
        } catch (err) {
          log("warn", "tail_heartbeat_activate_failed", {
            request_id: requestId, namespace: ns, worker_count: workers.length,
            error_message: errMessage(err),
          });
        }
        if (!openNoticeSent) {
          // Comment line forces the first body bytes to leave only after the
          // initial activation write attempt has run.
          controller.enqueue(utf8Encoder.encode(": tail-open\n\n"));
          openNoticeSent = true;
          if (pendingWarning) {
            const data = JSON.stringify({ event: "tail_warning", ...pendingWarning });
            // Per doc: control-generated events MUST NOT carry an SSE id.
            controller.enqueue(utf8Encoder.encode(sseEvent({
              event: "tail_warning", data,
            })));
            pendingWarning = null;
          }
        }

        // The Redis block timeout doubles as SSE keepalive cadence. Keep it
        // comfortably below common 10s proxy idle windows so sparse tails do
        // not flap between events.
        const batch = await tailCursor.readBatch(
          session,
          Math.min(XREAD_BLOCK_MS, SSE_KEEPALIVE_MS, remainingMs),
        );
        if (cancelled) {
          try { controller.close(); } catch {}
          return;
        }

        if (!batch) {
          // Timed out with no events → SSE comment so intermediaries know
          // the connection is alive even when traffic is sparse.
          controller.enqueue(utf8Encoder.encode(`: hb ${Date.now()}\n\n`));
          return;
        }

        for (const failure of batch.parseFailures) {
          log("warn", "tail_event_parse_failed", {
            request_id: requestId, namespace: ns, worker: failure.workerName, stream_id: failure.id,
          });
        }
        for (const event of batch.events) {
          controller.enqueue(utf8Encoder.encode(sseEvent({
            event: event.eventName, id: event.id, data: JSON.stringify(event.payloadObj),
          })));
        }
      } catch (err) {
        if (cancelled) {
          try { controller.close(); } catch {}
          return;
        }
        log("error", "tail_pull_failed", {
          request_id: requestId, namespace: ns,
          error_message: errMessage(err),
        });
        cancelled = true;
        resolveCancel();
        try { controller.close(); } catch { /* already closed */ }
      }
    },
    cancel() {
      // CLAUDE.md gotcha: do real cleanup in the pre-registered waitUntil
      // promise, not here — IoContext teardown can race anything scheduled
      // from inside the cancel callback itself.
      cancelled = true;
      resolveCancel();
    },
  });

  return new Response(stream, { status: 200, headers: SSE_HEADERS });
}
