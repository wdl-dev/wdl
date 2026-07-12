// Shared runtime-side forwarding for `wdl tail` stream-backed events.
// This is intentionally best-effort: stdout / handler responses stay
// canonical, and inactive workers do not pay per-event append calls.

import {
  optionalRedisProxyBaseUrl,
  proxyEndpoint,
} from "runtime-bindings-proxy";
import { errorMessage } from "shared-errors";
import { withInternalAuth } from "shared-internal-auth";

// Active-set cache. Keep the timing constants beside the implementation;
// higher-level docs should describe behavior without restating these numbers.
const ACTIVE_HIT_MAX_AGE_MS = 500;
const ACTIVE_MISS_MAX_AGE_MS = 2_000;
// 150 ms is generous headroom for the loopback hop while still keeping
// a hung proxy from blocking request-side observability work.
const ACTIVE_FETCH_TIMEOUT_MS = 150;
const POST_EVENT_TIMEOUT_MS = 150;
const MAX_TAIL_PATH_CHARS = 1024;
export const TAIL_EVENT_MAX_BYTES = 5 * 1024;

const utf8Encoder = new TextEncoder();

/**
 * @typedef {{ REDIS_PROXY_URL?: unknown, [key: string]: unknown }} RuntimeTailEnv
 */

// performance.now() is wall-clock-skew-safe (a container clock jump
// won't poison the cache). Date.now() stays for on-wire `ts` fields.
function nowMonotonic() {
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

// activeSet / missSet: Map<key, observedAtMonoMs>. A key is a
// "positive hit" only when lastConfirmedAt is fresher than
// ACTIVE_HIT_MAX_AGE_MS; a miss suppresses only that same key.
//
// Cost shape:
//   - Subscribed + cache-fresh: 0 active-set fetches per event.
//   - Subscribed + cache stale (every ACTIVE_HIT_MAX_AGE_MS): 1 fetch.
//   - Unsubscribed + cache-fresh miss: 0 active-set fetches per event.
//   - Unsubscribed + miss cache stale: 1 fetch.
export function makeActiveSetCache() {
  /** @type {Map<string, number>} */
  let activeSet = new Map();
  /** @type {Map<string, number>} */
  const missSet = new Map();
  /** @type {Promise<void> | null} */
  let activeFetchInflight = null;

  /** @param {RuntimeTailEnv} env */
  async function performActiveSetFetch(env) {
    const base = optionalRedisProxyBaseUrl(env);
    if (!base) return;
    if (activeFetchInflight) return await activeFetchInflight;
    activeFetchInflight = (async () => {
      try {
        const res = await fetch(proxyEndpoint(base, "/logs/tail/active"), {
          headers: withInternalAuth(undefined, env),
          signal: AbortSignal.timeout(ACTIVE_FETCH_TIMEOUT_MS),
        });
        if (res.ok) {
          const body = await res.json();
          const list = Array.isArray(body?.active) ? body.active : [];
          const now = nowMonotonic();
          const next = new Map();
          for (const k of list) next.set(k, now);
          activeSet = next;
          for (const k of list) missSet.delete(k);
        }
      } catch {
        // Keep stale cache on failure. Avoids a flapping proxy turning
        // every event into a stalled fetch.
      } finally {
        activeFetchInflight = null;
      }
    })();
    await activeFetchInflight;
  }

  /** @param {string} key */
  function isFreshMiss(key) {
    const seenAt = missSet.get(key);
    return seenAt !== undefined && nowMonotonic() - seenAt < ACTIVE_MISS_MAX_AGE_MS;
  }

  /** @param {string} key */
  function markMiss(key) {
    missSet.set(key, nowMonotonic());
  }

  /**
   * @param {RuntimeTailEnv} env
   * @param {string} key
   */
  async function refreshIfStale(env, key) {
    if (isFreshMiss(key)) return false;
    await performActiveSetFetch(env);
    if (!activeSet.has(key)) markMiss(key);
    return true;
  }

  /** @param {string} key */
  function isPositiveHit(key) {
    const seenAt = activeSet.get(key);
    return seenAt !== undefined && nowMonotonic() - seenAt < ACTIVE_HIT_MAX_AGE_MS;
  }

  /** @param {string} key */
  function snapshotHas(key) {
    return activeSet.has(key);
  }

  return { isPositiveHit, snapshotHas, refreshIfStale, markMiss };
}

const activeSetCache = makeActiveSetCache();

/** @param {Request} request */
export function fetchTailFields(request) {
  const { pathname } = new URL(request.url);
  if (pathname.length <= MAX_TAIL_PATH_CHARS) {
    return { method: request.method, path: pathname };
  }
  return {
    method: request.method,
    path: pathname.slice(0, MAX_TAIL_PATH_CHARS),
    path_truncated: true,
  };
}

// Best-effort POST. Network drop / 5xx / timeout -> silently swallowed.
// Timeout matters even under waitUntil: a hung proxy under high QPS would
// otherwise pile up unresolved background promises until the IoContext
// closes them en masse.
/**
 * @param {RuntimeTailEnv} env
 * @param {string} ns
 * @param {string} worker
 * @param {unknown} payload
 */
async function postEvent(env, ns, worker, payload) {
  const base = optionalRedisProxyBaseUrl(env);
  if (!base) return;
  const json = serializeTailPayload(payload);
  try {
    await fetch(proxyEndpoint(base, "/logs/tail/append"), {
      method: "POST",
      headers: withInternalAuth({ "content-type": "application/json" }, env),
      body: JSON.stringify({ ns, worker, json }),
      signal: AbortSignal.timeout(POST_EVENT_TIMEOUT_MS),
    });
  } catch {
    // swallow - see comment above.
  }
}

/** @param {string} json */
export function tailEventByteLength(json) {
  return utf8Encoder.encode(json).byteLength;
}

/** @param {unknown} payload */
export function tailEventTooLargePayload(payload) {
  /** @type {Record<string, unknown>} */
  const out = {
    event: "tail_warning",
    code: "event_too_large",
    message: "tail event dropped because it exceeds the size limit",
    limit_bytes: TAIL_EVENT_MAX_BYTES,
    ts: Date.now(),
  };
  const record = /** @type {Record<string, unknown> | null} */ (
    payload && typeof payload === "object" ? payload : null
  );
  if (typeof record?.event === "string") out.dropped_event = record.event;
  if (typeof record?.worker_id === "string") out.worker_id = record.worker_id;
  if (typeof record?.request_id === "string") out.request_id = record.request_id;
  if (typeof record?.phase === "string") out.phase = record.phase;
  return out;
}

/** @param {unknown} payload */
export function serializeTailPayload(payload) {
  let json;
  try {
    json = JSON.stringify(payload);
  } catch {
    json = JSON.stringify(tailEventTooLargePayload(payload));
  }
  if (tailEventByteLength(json) <= TAIL_EVENT_MAX_BYTES) return json;
  const warning = JSON.stringify(tailEventTooLargePayload(payload));
  if (tailEventByteLength(warning) <= TAIL_EVENT_MAX_BYTES) return warning;
  return JSON.stringify({
    event: "tail_warning",
    code: "event_too_large",
    limit_bytes: TAIL_EVENT_MAX_BYTES,
    ts: Date.now(),
  });
}

/** @param {{ ns: string, worker: string }} entry */
function entryKey(entry) {
  return `${entry.ns}:${entry.worker}`;
}

/**
 * @param {RuntimeTailEnv} env
 * @param {string} ns
 * @param {string} worker
 */
async function isTailActive(env, ns, worker) {
  const key = `${ns}:${worker}`;
  if (activeSetCache.isPositiveHit(key)) return true;
  await activeSetCache.refreshIfStale(env, key);
  return activeSetCache.snapshotHas(key);
}

/**
 * Caller must run the returned promise under ctx.waitUntil or another
 * background lifetime; direct await on the response path would make tail
 * transport latency user-visible.
 */
/**
 * @param {RuntimeTailEnv} env
 * @param {{ ns?: string, worker?: string, payload?: unknown }} entry
 */
export async function forwardTailEntry(env, entry) {
  if (!entry?.ns || !entry?.worker || !entry?.payload) return;
  if (await isTailActive(env, entry.ns, entry.worker)) {
    await postEvent(env, entry.ns, entry.worker, entry.payload);
  }
}

/**
 * @param {RuntimeTailEnv} env
 * @param {{ waitUntil?: (promise: Promise<unknown>) => void }} ctx
 * @param {Array<{ ns?: string, worker?: string, payload?: unknown }>} entries
 */
export async function forwardTailEntries(env, ctx, entries) {
  if (!ctx?.waitUntil || !Array.isArray(entries) || entries.length === 0) return;
  const decided = new Map();
  let fetchedThisBatch = false;
  for (const entry of entries) {
    if (!entry?.ns || !entry?.worker || !entry?.payload) continue;
    const key = entryKey(/** @type {{ ns: string, worker: string }} */ (entry));
    let active = decided.get(key);
    if (active === undefined) {
      if (activeSetCache.isPositiveHit(key)) {
        active = true;
      } else if (fetchedThisBatch) {
        active = activeSetCache.snapshotHas(key);
      } else {
        fetchedThisBatch = await activeSetCache.refreshIfStale(env, key);
        active = activeSetCache.snapshotHas(key);
      }
      if (!active && fetchedThisBatch) activeSetCache.markMiss(key);
      decided.set(key, active);
    }
    if (active) {
      ctx.waitUntil(postEvent(env, entry.ns, entry.worker, entry.payload));
    }
  }
}

/**
 * @param {{
 *   env: RuntimeTailEnv,
 *   ctx: { waitUntil?: (promise: Promise<unknown>) => void },
 *   identity: { namespace?: string, workerName?: string, workerId?: string, requestId?: string | null },
 *   event: string,
 *   phase: string,
 *   fields?: Record<string, unknown>,
 *   after?: Promise<unknown> | null,
 * }} options
 */
export function emitRuntimeTailEvent({ env, ctx, identity, event, phase, fields = {}, after = null }) {
  // Scheduled/queue/fetch events without a loaded-worker identity are not
  // tenant tail events. They still go through normal runtime logs; tail append
  // stays scoped to a concrete ns/worker stream key.
  if (!ctx?.waitUntil || !identity?.namespace || !identity?.workerName) return null;
  const task = Promise.resolve(after)
    .catch(() => {})
    .then(() => forwardTailEntry(env, {
      ns: identity.namespace,
      worker: identity.workerName,
      payload: {
        event,
        phase,
        ts: Date.now(),
        worker_id: identity.workerId,
        request_id: identity.requestId,
        ...fields,
      },
    }));
  ctx.waitUntil(task);
  return task;
}

/**
 * @param {{
 *   env: RuntimeTailEnv,
 *   ctx: { waitUntil?: (promise: Promise<unknown>) => void },
 *   identity: { namespace?: string, workerName?: string, workerId?: string, requestId?: string | null },
 *   event: string,
 *   fields: Record<string, unknown>,
 * }} options
 */
export function startTailEnvelope({ env, ctx, identity, event, fields }) {
  const startedAt = Date.now();
  const startTailEvent = emitRuntimeTailEvent({
    env, ctx, identity,
    event,
    phase: "start",
    fields,
  });

  /** @param {Record<string, unknown>} extraFields */
  function finish(extraFields) {
    const durationMs = Date.now() - startedAt;
    emitRuntimeTailEvent({
      env, ctx, identity,
      event,
      phase: "finish",
      after: startTailEvent,
      fields: {
        ...fields,
        ...extraFields,
        duration_ms: durationMs,
      },
    });
    return durationMs;
  }

  return {
    finish,
    /** @param {unknown} err @param {Record<string, unknown>} [extraFields] */
    finishError(err, extraFields = {}) {
      return finish({
        ...extraFields,
        outcome: "error",
        error: errorMessage(err),
      });
    },
  };
}
