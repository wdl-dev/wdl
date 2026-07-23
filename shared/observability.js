// Shared JS observability primitives for gateway, runtime, d1-runtime,
// control, and auth. Scheduler has a Rust-native implementation with the
// same JSON/Prometheus conventions.
// Label discipline + cardinality rules: see CLAUDE.md.

import { bytesToHex } from "./hex.js";
import { errorMessage } from "./errors.js";

const CARDINALITY_WARN_LIMIT = 100;
/**
 * @typedef {Record<string, string | number | boolean>} Labels
 * @typedef {(level: string, event: string, fields?: Record<string, unknown>) => void} Logger
 * @typedef {{
 *   increment(name: string, labels?: Labels, delta?: number): void,
 *   observe(name: string, labels: Labels, value: number): void,
 * }} RequestMetrics
 * @typedef {{
 *   increment(name: string, labels?: Labels, delta?: number): void,
 *   observe(name: string, labels: Labels, value: number): void,
 * }} RedisMetrics
 * @typedef {{ command: unknown, ok: boolean, duration_ms: number, error_message?: unknown }} RedisCommandEvent
 * @typedef {{
 *   service: string,
 *   metrics?: RequestMetrics | null,
 *   log: Logger,
 *   method: string,
 *   requestId: string,
 *   route: string,
 *   status: number,
 *   startedAt: number,
 *   error?: unknown,
 *   hasError?: boolean,
 *   extras?: Record<string, unknown> | null,
 *   probeRoutes?: string[],
 * }} RequestCompleteOptions
 * @typedef {{ name: string, labels: Labels, value: number }} CounterMetric
 * @typedef {{ name: string, labels: Labels, value: number }} GaugeMetric
 * @typedef {{ name: string, labels: Labels, count: number, sum: number, max: number }} SummaryMetric
 */
/**
 * @param {Labels} labels
 * @returns {[string, string | number | boolean][]}
 */
function stableLabelEntries(labels) {
  return Object.entries(labels).toSorted(([a], [b]) => a.localeCompare(b));
}

/**
 * @param {string} name
 * @param {Labels} labels
 * @returns {string}
 */
function metricKey(name, labels) {
  return `${name}|${stableLabelEntries(labels)
    .map(([k, v]) => `${k}=${v}`)
    .join(",")}`;
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function escapeLabelValue(value) {
  return String(value)
    .replaceAll("\\", "\\\\")
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\"", '\\"');
}

/**
 * @param {Labels} labels
 * @returns {string}
 */
function formatLabels(labels) {
  const entries = stableLabelEntries(labels);
  if (!entries.length) return "";
  return `{${entries.map(([k, v]) => `${k}="${escapeLabelValue(v)}"`).join(",")}}`;
}

function nowIso() {
  return new Date().toISOString();
}

/** @param {unknown} value */
function safeJsonStringify(value) {
  const seen = new WeakSet();
  return JSON.stringify(value, (_key, fieldValue) => {
    if (typeof fieldValue === "bigint") return fieldValue.toString();
    if (fieldValue && typeof fieldValue === "object") {
      if (seen.has(fieldValue)) return "[Circular]";
      seen.add(fieldValue);
    }
    return fieldValue;
  });
}

/**
 * @param {string} service
 * @param {string} level
 * @param {string} event
 * @param {Record<string, unknown>} [fields]
 * @returns {void}
 */
function emitStructuredLogLine(service, level, event, fields = {}) {
  const payload = {
    ts: nowIso(),
    service,
    level,
    event,
    ...fields,
  };
  const line = safeJsonStringify(payload);
  if (level === "error") console.error(line);
  else console.log(line);
}

export function generateRequestId() {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}

/** @param {string} value */
function trimAsciiWhitespace(value) {
  let start = 0;
  let end = value.length;
  /** @param {number} code */
  const isWhitespace = (code) => code === 0x20 || code === 0x09 || code === 0x0a || code === 0x0c || code === 0x0d;
  while (start < end && isWhitespace(value.charCodeAt(start))) start += 1;
  while (end > start && isWhitespace(value.charCodeAt(end - 1))) end -= 1;
  return value.slice(start, end);
}

// An inbound id flows into log fields AND response headers AND downstream
// subrequests. Header adapters may join repeated values with commas, so only
// the first token is considered; non-visible-ASCII and quote/backslash bytes
// are rejected before pass-through.
// Dirty → mint fresh; preserving a maybe-poisoned upstream id defeats the
// "single correlation token" goal.
/**
 * @param {unknown} raw
 * @returns {string | null}
 */
export function sanitizeRequestId(raw) {
  if (Array.isArray(raw)) raw = raw[0];
  if (typeof raw !== "string") return null;
  const first = trimAsciiWhitespace(raw.split(",")[0]);
  if (!first || first.length > 128) return null;
  for (let i = 0; i < first.length; i++) {
    const code = first.charCodeAt(i);
    if (code < 0x21 || code > 0x7e || code === 0x22 || code === 0x5c) return null;
  }
  return first;
}

/**
 * @param {{ get(name: string): string | null } | Record<string, unknown> | null | undefined} headersLike
 * @returns {string}
 */
export function ensureRequestId(headersLike) {
  if (!headersLike) return generateRequestId();
  const raw =
    typeof headersLike.get === "function"
      ? headersLike.get("x-request-id")
      : /** @type {Record<string, unknown>} */ (headersLike)["x-request-id"];
  return sanitizeRequestId(raw) || generateRequestId();
}

const DEFAULT_PROBE_ROUTES = ["healthz", "metrics"];

// extras schema: {namespace?, worker?, version?}. Null/undefined keys are
// stripped so throw-path logs don't carry "namespace":null noise.
/**
 * @param {Record<string, unknown> | null | undefined} extras
 * @returns {Record<string, unknown> | null}
 */
function pruneExtras(extras) {
  if (!extras) return null;
  /** @type {Record<string, unknown> | null} */
  let out = null;
  for (const [k, v] of Object.entries(extras)) {
    if (v == null) continue;
    if (!out) out = {};
    out[k] = v;
  }
  return out;
}

/**
 * @param {RequestCompleteOptions} options
 * @returns {void}
 */
export function recordRequestComplete({
  service,
  metrics,
  log,
  method,
  requestId,
  route,
  status,
  startedAt,
  error = null,
  hasError = false,
  extras = null,
  probeRoutes = DEFAULT_PROBE_ROUTES,
}) {
  const durationMs = Date.now() - startedAt;
  const requestHasError = hasError || error !== null;
  const requestLabels = { service, route };
  const statusLabel = String(status);
  if (metrics) {
    metrics.increment("requests", { ...requestLabels, status: statusLabel });
    metrics.observe("request_duration_ms", requestLabels, durationMs);
    if (status >= 500) metrics.increment("request_errors", { ...requestLabels, status: statusLabel });
  }
  if (!probeRoutes.includes(route) || requestHasError || status >= 500) {
    const pruned = pruneExtras(extras);
    log(requestHasError || status >= 500 ? "error" : "info", "request_complete", {
      request_id: requestId,
      method,
      route,
      status,
      duration_ms: durationMs,
      ...(pruned || {}),
      ...(requestHasError ? formatError(error) : {}),
    });
  }
}

/**
 * @param {unknown} err
 * @returns {Record<string, string>}
 */
export function formatError(err) {
  if (err == null) return { error_message: "Unknown error" };
  if (isErrorObject(err)) {
    /** @type {Record<string, string>} */
    const out = {
      error_name: errorStringField(err, "name") ?? "Error",
      error_message: errorMessage(err),
    };
    const code = errorStringField(err, "code") ?? errorStringField(err, "reason");
    if (code !== null) out.error_code = code;
    return out;
  }
  return { error_message: errorMessage(err) };
}

/** @param {unknown} value @returns {value is Error} */
function isErrorObject(value) {
  try {
    return value instanceof Error;
  } catch {
    return false;
  }
}

/** @param {Error} err @param {string} field */
function errorStringField(err, field) {
  try {
    const value = /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (err))[field];
    return typeof value === "string" ? value : null;
  } catch {
    return null;
  }
}

// Metrics bypass this gate entirely (in-memory registry, separate scrape
// path), so LOG_LEVEL=warn silences per-request access logs without
// costing any Prometheus signal.
/** @type {Record<string, number>} */
const LOG_LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

/**
 * @param {unknown} name
 * @returns {number | null}
 */
function parseLogLevel(name) {
  if (typeof name !== "string") return null;
  const v = LOG_LEVELS[name.toLowerCase()];
  return typeof v === "number" ? v : null;
}

let currentLogLevel =
  parseLogLevel(typeof process !== "undefined" ? process?.env?.LOG_LEVEL : null) ??
  LOG_LEVELS.info;

/**
 * @param {unknown} name
 * @returns {void}
 */
export function setLogLevel(name) {
  const v = parseLogLevel(name);
  if (v != null) currentLogLevel = v;
}

/**
 * @returns {(env?: { LOG_LEVEL?: unknown } | null) => void}
 */
export function createLogLevelBinder() {
  let logLevelSet = false;
  return function bindLogLevel(env) {
    if (logLevelSet) return;
    setLogLevel(env?.LOG_LEVEL);
    logLevelSet = true;
  };
}

/**
 * @param {string} service
 * @returns {Logger}
 */
export function createLogger(service) {
  return function log(level, event, fields = {}) {
    const threshold = LOG_LEVELS[level] ?? LOG_LEVELS.info;
    if (threshold < currentLogLevel) return;
    emitStructuredLogLine(service, level, event, fields);
  };
}

/**
 * @param {string} service
 * @param {string} level
 * @param {string} event
 * @param {Record<string, unknown>} [fields]
 * @returns {void}
 */
export function logStructured(service, level, event, fields = {}) {
  return createLogger(service)(level, event, fields);
}

const REDIS_COMMAND_LABELS = new Set([
  "COPY",
  "DEL",
  "DELIFEQ",
  "DELIFEQ_PIPELINE",
  "EVAL",
  "EXEC",
  "EXISTS",
  "EXISTS_PIPELINE",
  "EXISTS_XRANGE_PIPELINE",
  "EXPIREAT",
  "GET",
  "GET_PIPELINE",
  "GET_TIME_PIPELINE",
  "HDEL",
  "HEXISTS",
  "HEXISTS_PIPELINE",
  "HGET",
  "HGETALL",
  "HGETALL_GET_PIPELINE",
  "HGETALL_GET_SMEMBERS_PIPELINE",
  "HGETALL_PIPELINE",
  "HGETEX",
  "HGET_PIPELINE",
  "HKEYS",
  "HLEN",
  "HMGET",
  "HSET",
  "HSETEX",
  "HSTRLEN_PIPELINE",
  "INCR",
  "MGET",
  "MULTI_EXEC",
  "PING",
  "PUBLISH",
  "SADD",
  "SCARD",
  "SCARD_PIPELINE",
  "SCAN",
  "SET",
  "SISMEMBER",
  "SMEMBERS",
  "SMEMBERS_HGETALL_PIPELINE",
  "SMEMBERS_PIPELINE",
  "SMISMEMBER",
  "SREM",
  "TIME",
  "UNWATCH",
  "WATCH",
  "XADD",
  "XRANGE",
  "XREAD",
  "ZADD",
  "ZCARD",
  "ZRANGE",
  "ZRANGE_PIPELINE",
  "ZRANGEBYSCORE",
  "ZREM",
]);

/**
 * @param {unknown} command
 * @returns {string}
 */
function redisCommandLabel(command) {
  const normalized = typeof command === "string" ? command.toUpperCase() : "";
  return REDIS_COMMAND_LABELS.has(normalized) ? normalized : "OTHER";
}

/**
 * @param {RedisCommandEvent} event
 * @returns {boolean}
 */
function isWatchInvalidation(event) {
  return typeof event.command === "string"
    && event.command.toUpperCase() === "MULTI_EXEC"
    && event.error_message === "watch invalidation";
}

/**
 * @param {{ metrics?: RedisMetrics | null, log?: Logger | null, service: string, event: RedisCommandEvent }} options
 * @returns {void}
 */
export function recordRedisCommand({ metrics, log, service, event }) {
  const command = redisCommandLabel(event.command);
  if (metrics) {
    metrics.increment("redis_commands", {
      service,
      command,
      outcome: event.ok ? "ok" : "error",
    });
    metrics.observe("redis_command_duration_ms", {
      service,
      command,
    }, event.duration_ms);
  }
  if (!event.ok && log) {
    if (isWatchInvalidation(event)) {
      log("warn", "redis_watch_invalidation", {
        command: event.command,
        duration_ms: event.duration_ms,
      });
      return;
    }
    log("error", "redis_command_failed", {
      command: event.command,
      duration_ms: event.duration_ms,
      error_message: event.error_message,
    });
  }
}

// Summary latency metrics here carry count+sum+max only — no quantile
// estimation; dashboards compute rate(_sum)/rate(_count) for averages.
// Histogram buckets are a deliberate follow-up.
export class MetricsRegistry {
  constructor(prefix = "wdl") {
    this.prefix = prefix;
    /** @type {Map<string, CounterMetric>} */
    this.counters = new Map();
    /** @type {Map<string, GaugeMetric>} */
    this.gauges = new Map();
    /** @type {Map<string, SummaryMetric>} */
    this.summaries = new Map();
    // Per-name counts so the tripwire blames the actual offender, not
    // whichever metric happened to be written after the global total
    // crossed the limit.
    /** @type {Map<string, number>} */
    this._seriesByName = new Map();
    /** @type {Set<string>} */
    this._cardinalityWarned = new Set();
  }

  /**
   * @param {string} name
   * @returns {boolean}
   */
  _trackSeries(name) {
    const current = this._seriesByName.get(name) || 0;
    if (current >= CARDINALITY_WARN_LIMIT) {
      if (!this._cardinalityWarned.has(name)) {
        this._cardinalityWarned.add(name);
        emitStructuredLogLine("observability", "warn", "metric_cardinality_warning", {
          metric: name,
          series: current,
          limit: CARDINALITY_WARN_LIMIT,
        });
      }
      return false;
    }
    const next = current + 1;
    this._seriesByName.set(name, next);
    if (next >= CARDINALITY_WARN_LIMIT && !this._cardinalityWarned.has(name)) {
      this._cardinalityWarned.add(name);
      emitStructuredLogLine("observability", "warn", "metric_cardinality_warning", {
        metric: name,
        series: next,
        limit: CARDINALITY_WARN_LIMIT,
      });
    }
    return true;
  }

  /**
   * @param {string} name
   * @param {Labels} [labels]
   * @param {number} [delta]
   * @returns {void}
   */
  increment(name, labels = {}, delta = 1) {
    const key = metricKey(name, labels);
    const existing = this.counters.get(key);
    if (existing) existing.value += delta;
    else if (this._trackSeries(name)) {
      this.counters.set(key, { name, labels: { ...labels }, value: delta });
    }
  }

  /**
   * @param {string} name
   * @param {Labels | null | undefined} labels
   * @param {number} value
   * @returns {void}
   */
  setGauge(name, labels, value) {
    const metricLabels = labels || {};
    const key = metricKey(name, metricLabels);
    const existed = this.gauges.has(key);
    if (!existed && !this._trackSeries(name)) return;
    this.gauges.set(key, { name, labels: { ...metricLabels }, value });
  }

  /**
   * @param {string} name
   * @param {Labels | null | undefined} labels
   * @param {number} value
   * @returns {void}
   */
  observe(name, labels, value) {
    const metricLabels = labels || {};
    const key = metricKey(name, metricLabels);
    const existing = this.summaries.get(key);
    if (existing) {
      existing.count += 1;
      existing.sum += value;
      existing.max = Math.max(existing.max, value);
      return;
    }
    if (this._trackSeries(name)) {
      this.summaries.set(key, {
        name,
        labels: { ...metricLabels },
        count: 1,
        sum: value,
        max: value,
      });
    }
  }

  /**
   * @returns {string}
   */
  renderPrometheus() {
    /** @type {string[]} */
    const lines = [];
    const emittedTypes = new Set();
    /**
     * @param {string} suffix
     * @param {string} type
     */
    const emitType = (suffix, type) => {
      if (emittedTypes.has(suffix)) return;
      emittedTypes.add(suffix);
      lines.push(`# TYPE ${suffix} ${type}`);
    };

    for (const metric of [...this.counters.values()].toSorted((a, b) => a.name.localeCompare(b.name))) {
      const suffix = `${this.prefix}_${metric.name}_total`;
      emitType(suffix, "counter");
      lines.push(`${suffix}${formatLabels(metric.labels)} ${metric.value}`);
    }

    for (const metric of [...this.gauges.values()].toSorted((a, b) => a.name.localeCompare(b.name))) {
      const suffix = `${this.prefix}_${metric.name}`;
      emitType(suffix, "gauge");
      lines.push(`${suffix}${formatLabels(metric.labels)} ${metric.value}`);
    }

    // Prometheus summary TYPE only admits _count / _sum / {quantile="…"};
    // _max ships as its own gauge family so strict scrapers don't reject
    // the output.
    const sortedSummaries = [...this.summaries.values()].toSorted((a, b) =>
      a.name.localeCompare(b.name)
    );
    for (const metric of sortedSummaries) {
      const base = `${this.prefix}_${metric.name}`;
      emitType(base, "summary");
      lines.push(`${base}_count${formatLabels(metric.labels)} ${metric.count}`);
      lines.push(`${base}_sum${formatLabels(metric.labels)} ${metric.sum}`);
    }
    for (const metric of sortedSummaries) {
      const maxName = `${this.prefix}_${metric.name}_max`;
      emitType(maxName, "gauge");
      lines.push(`${maxName}${formatLabels(metric.labels)} ${metric.max}`);
    }

    return lines.join("\n") + "\n";
  }
}
