import { createLogLevelBinder, createLogger } from "shared-observability";
import { utf8ByteLength } from "shared-utf8";
import {
  TAIL_EVENT_MAX_BYTES,
  forwardTailEntries,
  tailEventTooLargePayload,
} from "runtime-tail-forwarder";

// Logger name is bound to the capnp SERVICE_NAME binding
// ("user-runtime-tail" / "system-runtime-tail") so tail lines from the two
// pools don't collapse into a single "runtime-tail" service stream. Lazy
// inited on first `tail(events, env, ctx)` — env isn't in scope at module load.
/** @type {null | ((level: string, event: string, fields?: Record<string, unknown>) => void)} */
let log = null;
let serviceNameChecked = false;
const bindLogLevel = createLogLevelBinder();

// workerd hardcodes scriptName=none for workerLoader-loaded workers, so
// identity comes from the forwarded request headers gateway + runtime
// already set. scheduled() / queue() are JSRPC with no request shape, so
// their console tail events carry no id and stay stdout-only. runtime/dispatch
// emits separate invocation events for `wdl tail`.
/** @param {unknown} event */
function readHeaders(event) {
  const eventRecord = /** @type {{ event?: { request?: { headers?: Record<string, unknown> } } }} */ (
    event && typeof event === "object" ? event : {}
  );
  const headers = eventRecord.event?.request?.headers;
  return {
    workerId: typeof headers?.["x-worker-id"] === "string" ? headers["x-worker-id"] : null,
    requestId: typeof headers?.["x-request-id"] === "string" ? headers["x-request-id"] : null,
  };
}

/** @param {unknown} consoleLevel */
function loggerLevel(consoleLevel) {
  if (consoleLevel === "warn") return "warn";
  if (consoleLevel === "error") return "error";
  return "info";
}

const TAIL_MESSAGE_MAX_DEPTH = 64;

class TailEventTooLarge extends Error {}

/** @param {{ remaining: number }} budget @param {unknown} value */
function chargeBudget(budget, value) {
  budget.remaining -= utf8ByteLength(String(value));
  if (budget.remaining < 0) throw new TailEventTooLarge("tail event too large");
}

/** @param {unknown} value */
function safeString(value) {
  try { return String(value); } catch { return "[unserializable]"; }
}

// Bounded clone of TraceLog.message. createLogger does JSON.stringify(payload);
// without this, BigInt/cycles throw and large objects burn unbounded CPU/heap
// before LOG_LEVEL suppression can return.
/**
 * @param {unknown} value
 * @param {{ remaining: number }} [budget]
 * @param {WeakSet<object>} [seen]
 * @returns {unknown}
 */
function safeMessage(value, budget = { remaining: TAIL_EVENT_MAX_BYTES }, seen = new WeakSet()) {
  try {
    return safeMessageInner(value, budget, seen, 0);
  } catch (err) {
    if (err instanceof TailEventTooLarge) throw err;
    const out = safeString(value);
    chargeBudget(budget, out);
    return out;
  }
}

/**
 * @param {unknown} value
 * @param {{ remaining: number }} budget
 * @param {WeakSet<object>} seen
 * @param {number} depth
 * @returns {unknown}
 */
function safeMessageInner(value, budget, seen, depth) {
  if (depth > TAIL_MESSAGE_MAX_DEPTH) {
    throw new TailEventTooLarge("tail event too deep");
  }
  if (value === null) {
    chargeBudget(budget, "null");
    return null;
  }
  if (typeof value === "string") {
    chargeBudget(budget, value);
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    chargeBudget(budget, String(value));
    return value;
  }
  if (typeof value === "bigint") {
    const out = value.toString() + "n";
    chargeBudget(budget, out);
    return out;
  }
  if (typeof value === "undefined" || typeof value === "function" || typeof value === "symbol") {
    const out = safeString(value);
    chargeBudget(budget, out);
    return out;
  }
  if (typeof value !== "object") {
    const out = safeString(value);
    chargeBudget(budget, out);
    return out;
  }
  if (seen.has(value)) {
    chargeBudget(budget, "[Circular]");
    return "[Circular]";
  }
  seen.add(value);
  if (Array.isArray(value)) {
    chargeBudget(budget, "[]");
    const out = [];
    for (const item of value) {
      chargeBudget(budget, ",");
      out.push(safeMessageInner(item, budget, seen, depth + 1));
    }
    return out;
  }
  chargeBudget(budget, "{}");
  /** @type {Record<string, unknown>} */
  const out = {};
  const record = /** @type {Record<string, unknown>} */ (value);
  for (const key of Object.keys(record)) {
    chargeBudget(budget, ":,");
    chargeBudget(budget, key);
    Object.defineProperty(out, key, {
      value: safeMessageInner(record[key], budget, seen, depth + 1),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }
  return out;
}

/** @param {{ droppedEvent: string, workerId?: string | null, requestId?: string | null, consoleLevel?: string | null }} options */
function droppedFields({ droppedEvent, workerId, requestId, consoleLevel = null }) {
  return {
    code: "event_too_large",
    dropped_event: droppedEvent,
    limit_bytes: TAIL_EVENT_MAX_BYTES,
    ...(consoleLevel ? { console_level: consoleLevel } : {}),
    ...(workerId ? { worker_id: workerId } : {}),
    ...(requestId ? { request_id: requestId } : {}),
  };
}

/**
 * @param {Array<{ ns: string, worker: string, payload: unknown }>} forwardEntries
 * @param {{ ns: string, name: string } | null} nsName
 * @param {Record<string, unknown>} fields
 */
function pushDroppedWarning(forwardEntries, nsName, fields) {
  if (!nsName) return;
  forwardEntries.push({
    ns: nsName.ns,
    worker: nsName.name,
    payload: tailEventTooLargePayload({
      event: typeof fields.dropped_event === "string" ? fields.dropped_event : undefined,
      worker_id: typeof fields.worker_id === "string" ? fields.worker_id : undefined,
      request_id: typeof fields.request_id === "string" ? fields.request_id : undefined,
    }),
  });
}

// `<ns>:<name>:<version>` per CLAUDE.md / shared/worker-id.js#parseWorkerId.
// We split rather than re-import the helper to keep tail-worker's module
// graph minimal — it must boot even if other runtime modules drift.
/** @param {unknown} workerId */
function parseNsName(workerId) {
  if (typeof workerId !== "string") return null;
  const idx1 = workerId.indexOf(":");
  if (idx1 < 0) return null;
  const idx2 = workerId.indexOf(":", idx1 + 1);
  if (idx2 < 0) return null;
  const ns = workerId.slice(0, idx1);
  const name = workerId.slice(idx1 + 1, idx2);
  if (!ns || !name) return null;
  return { ns, name };
}

export default {
  /**
   * @param {unknown[]} events
   * @param {Record<string, unknown>} env
   * @param {{ waitUntil?: (promise: Promise<unknown>) => void }} ctx
   */
  async tail(events, env, ctx) {
    // Tail can be invoked during deployment drift without env; wait until a
    // real LOG_LEVEL appears before consuming the shared one-shot binder.
    if (env?.LOG_LEVEL) bindLogLevel(env);
    // capnp is the only writer of SERVICE_NAME; missing = deployment drift
    // that would collapse user-runtime-tail + system-runtime-tail into one
    // stream. Log once and keep going — tail is not a failable path.
    if (!serviceNameChecked) {
      serviceNameChecked = true;
      if (!env?.SERVICE_NAME) {
        const fallback = createLogger("runtime-tail");
        fallback("error", "tail_service_name_missing", {});
      }
    }
    const serviceName = typeof env?.SERVICE_NAME === "string" && env.SERVICE_NAME ? env.SERVICE_NAME : "runtime-tail";
    if (!log) log = createLogger(serviceName);
    const nowMs = Date.now();

    // ── Phase 1: structured stdout, unconditional ───────────────────
    // stdout is the source of truth (CloudWatch / audit). Run it first
    // and synchronously so a hung proxy on the active-set fetch below
    // can never block the log lines from emitting. forwardEntries holds
    // the events we'd send to a tailer if active.
    /** @type {Array<{ ns: string, worker: string, payload: unknown }>} */
    const forwardEntries = [];
    for (const event of events || []) {
      if (!event) continue;
      const { workerId, requestId } = readHeaders(event);
      const nsName = parseNsName(workerId);

      const tailEvent = /** @type {{ logs?: Array<{ level?: unknown, message?: unknown }>, exceptions?: Array<{ message?: unknown, stack?: unknown, name?: unknown }> }} */ (
        event && typeof event === "object" ? event : {}
      );
      for (const entry of tailEvent.logs || []) {
        const consoleLevel = typeof entry.level === "string" ? entry.level : "info";
        /** @type {Record<string, unknown>} */
        const fields = { console_level: consoleLevel };
        try {
          fields.message = safeMessage(entry.message);
        } catch (err) {
          if (!(err instanceof TailEventTooLarge)) throw err;
          const dropped = droppedFields({
            droppedEvent: "worker_console",
            workerId,
            requestId,
            consoleLevel,
          });
          log("warn", "worker_console_dropped", dropped);
          pushDroppedWarning(forwardEntries, nsName, dropped);
          continue;
        }
        if (workerId) fields.worker_id = workerId;
        if (requestId) fields.request_id = requestId;
        log(loggerLevel(consoleLevel), "worker_console", fields);
        if (nsName) {
          forwardEntries.push({
            ns: nsName.ns,
            worker: nsName.name,
            payload: {
              event: "worker_console",
              console_level: consoleLevel,
              message: fields.message,
              ts: nowMs,
              ...(workerId ? { worker_id: workerId } : {}),
              ...(requestId ? { request_id: requestId } : {}),
            },
          });
        }
      }

      // Uncaught fetch-handler exceptions. The shape varies across workerd
      // tail event surfaces (some carry .stack/.name, some only .message),
      // so we copy the populated subset and omit the rest rather than
      // emitting null sentinels.
      for (const exc of tailEvent.exceptions || []) {
        /** @type {Record<string, unknown>} */
        const fields = {};
        try {
          const budget = { remaining: TAIL_EVENT_MAX_BYTES };
          if (exc?.message != null) fields.message = safeMessage(exc.message, budget);
          if (exc?.stack != null)   fields.stack = safeMessage(exc.stack, budget);
          if (exc?.name != null)    fields.name = safeMessage(exc.name, budget);
        } catch (err) {
          if (!(err instanceof TailEventTooLarge)) throw err;
          const dropped = droppedFields({
            droppedEvent: "worker_exception",
            workerId,
            requestId,
          });
          log("warn", "worker_exception_dropped", dropped);
          pushDroppedWarning(forwardEntries, nsName, dropped);
          continue;
        }
        if (workerId)  fields.worker_id = workerId;
        if (requestId) fields.request_id = requestId;
        log("error", "worker_exception", fields);
        if (nsName) {
          forwardEntries.push({
            ns: nsName.ns,
            worker: nsName.name,
            payload: { event: "worker_exception", ts: nowMs, ...fields },
          });
        }
      }
    }

    // ── Phase 2: SSE forward, gated by active set ──────────────────
    await forwardTailEntries(env, ctx, forwardEntries);
  },
};
