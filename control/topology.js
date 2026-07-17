// Operator-visible deploy ingress shapes: route patterns, host lists,
// cron expressions, queue consumer declarations. Pure parse + validate;
// Redis-facing writes that consume these live in control/routing.js.

import { parseCron } from "shared-cron-time";
import { isValidQueueName, QUEUE_NAME_RE } from "shared-ns-pattern";
import { errorMessage } from "shared-errors";
import { MAX_QUEUE_DELAY_SECONDS } from "control-lib";

/**
 * @typedef {import("shared-route-projection").PatternProjection} PatternProjection
 * @typedef {Pick<PatternProjection, "kind" | "value"> & { host: string, slot: string }} RoutePattern
 * @typedef {{ cron: string, timezone: string }} CronSpec
 * @typedef {{ queue: string, maxBatchSize: number, maxBatchTimeoutMs: number, maxRetries: number, retryDelaySeconds?: number, deadLetterQueue?: string }} QueueConsumer
 */

// Lowercase + strip :port + strip trailing FQDN dots — otherwise the
// platform-domain check is bypassable via "demo.workers.local.".
/** @param {unknown} raw */
export function normalizeHost(raw) {
  if (typeof raw !== "string") throw new Error("host must be a string");
  const trimmed = raw.trim();
  if (!trimmed) throw new Error("host must not be empty");
  let host = trimmed;
  const colon = host.lastIndexOf(":");
  if (colon >= 0 && /^\d+$/.test(host.slice(colon + 1))) {
    host = host.slice(0, colon);
  }
  host = host.replace(/\.+$/, "");
  host = host.toLowerCase();
  if (!host) throw new Error("host must not be empty after normalization");
  if (host.includes(":") || host.includes("/") || /\s/.test(host)) {
    throw new Error(`invalid host ${JSON.stringify(raw)}`);
  }
  return host;
}

/** @param {string} normalizedHost @param {string} platformDomain */
export function isPlatformDomainHost(normalizedHost, platformDomain) {
  const pd = platformDomain.toLowerCase();
  return normalizedHost === pd || normalizedHost.endsWith(`.${pd}`);
}

// Wrangler-style route pattern. `slot` is the Redis field key (raw
// pattern), `value` is what gateway matches against. CF: no `*` → exact.
//   "host/api/*" → { slot: "/api/*", kind: "prefix", value: "/api/" }
//   "host/mcp"   → { slot: "/mcp",   kind: "exact",  value: "/mcp"  }
/**
 * @param {unknown} raw
 * @param {string} platformDomain
 * @returns {RoutePattern}
 */
export function parsePattern(raw, platformDomain) {
  if (typeof raw !== "string") throw new Error("pattern must be a string");
  const s = raw.trim();
  if (!s) throw new Error("pattern must not be empty");
  // CF's scheme prefix scopes matching; we don't implement that, so
  // rejecting avoids silently broadening http-only to https.
  if (/^https?:\/\//i.test(s)) {
    throw new Error(
      `pattern ${JSON.stringify(raw)}: scheme prefix not supported — ` +
        `omit "http://" / "https://" (scheme-restricted matching is not implemented)`
    );
  }
  // Gateway matches pathname + search against known shapes — a stored
  // `?`/`#` pattern would deploy but never match.
  if (s.includes("?") || s.includes("#")) {
    throw new Error(
      `pattern ${JSON.stringify(raw)}: query ("?") and fragment ("#") are not supported`
    );
  }
  const slash = s.indexOf("/");
  if (slash < 0) {
    throw new Error(
      `pattern ${JSON.stringify(raw)}: path segment required — write "${s}/*" or "${s}/"`
    );
  }
  const hostRaw = s.slice(0, slash);
  const pathPart = s.slice(slash);
  if (!hostRaw) throw new Error(`pattern ${JSON.stringify(raw)}: host missing`);
  if (/\s/.test(pathPart)) {
    throw new Error(`pattern ${JSON.stringify(raw)}: path must not contain whitespace`);
  }
  if (hostRaw.includes("*")) {
    throw new Error(`pattern ${JSON.stringify(raw)}: wildcard hosts are not supported`);
  }
  // `*` is only allowed as the very last character. "/foo*" and "/foo/*"
  // are both fine (CF: trailing `*` = startsWith glob); "/a/*/b" or
  // "/*foo" are mid-pattern wildcards and rejected.
  const starIdx = pathPart.indexOf("*");
  if (starIdx !== -1 && starIdx !== pathPart.length - 1) {
    throw new Error(
      `pattern ${JSON.stringify(raw)}: "*" only allowed as the trailing character`
    );
  }
  const host = normalizeHost(hostRaw);
  if (isPlatformDomainHost(host, platformDomain)) {
    throw new Error(
      `pattern ${JSON.stringify(raw)}: host "${host}" is inside the platform domain ` +
        `(${platformDomain}); use <ns>.${platformDomain} subdomain routing instead`
    );
  }
  if (pathPart.endsWith("*")) {
    return { host, slot: pathPart, kind: "prefix", value: pathPart.slice(0, -1) };
  }
  return { host, slot: pathPart, kind: "exact", value: pathPart };
}

/**
 * @param {unknown} rawList
 * @param {string} platformDomain
 * @returns {RoutePattern[]}
 */
export function parseRoutes(rawList, platformDomain) {
  if (rawList == null) return [];
  if (!Array.isArray(rawList)) throw new Error("routes must be an array of strings");
  const seen = new Set();
  const out = [];
  for (const raw of rawList) {
    const parsed = parsePattern(raw, platformDomain);
    const key = `${parsed.host}|${parsed.slot}`;
    if (seen.has(key)) {
      throw new Error(`routes: duplicate pattern ${JSON.stringify(raw)}`);
    }
    seen.add(key);
    out.push(parsed);
  }
  return out;
}

/**
 * @param {unknown} rawList
 * @param {string} platformDomain
 * @returns {string[]}
 */
export function parseHostList(rawList, platformDomain) {
  if (!Array.isArray(rawList)) throw new Error("hosts must be an array of strings");
  const out = new Set();
  for (const raw of rawList) {
    const host = normalizeHost(raw);
    if (isPlatformDomainHost(host, platformDomain)) {
      throw new Error(
        `host ${JSON.stringify(host)} is inside the platform domain (${platformDomain})`
      );
    }
    out.add(host);
  }
  return [...out];
}

// Match CF's per-worker cap so portable configs hit the same limit on both.
export const MAX_CRONS_PER_WORKER = 10;

// Collapses duplicates on (cron, tz); enforces MAX_CRONS_PER_WORKER.
/**
 * @param {unknown} raw
 * @returns {CronSpec[]}
 */
export function parseCronList(raw) {
  if (raw == null) return [];
  if (!Array.isArray(raw)) throw new Error("crons must be an array");
  const seen = new Set();
  const out = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`crons entry must be an object, got ${JSON.stringify(entry)}`);
    }
    if (typeof entry.cron !== "string" || !entry.cron.trim()) {
      throw new Error(`crons entry needs non-empty string "cron"`);
    }
    const cron = entry.cron.trim();
    const timezone = entry.timezone == null ? "UTC" : entry.timezone;
    if (typeof timezone !== "string" || !timezone) {
      throw new Error(`crons entry "timezone" must be a non-empty string`);
    }
    try {
      // Throws RangeError on unknown IANA zone.
      new Intl.DateTimeFormat("en-US", { timeZone: timezone });
    } catch {
      throw new Error(`crons: unknown timezone ${JSON.stringify(timezone)}`);
    }
    try {
      parseCron(cron, timezone);
    } catch (err) {
      const message = errorMessage(err);
      throw new Error(`crons: invalid expression ${JSON.stringify(cron)}: ${message}`, { cause: err });
    }
    const key = `${cron}|${timezone}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ cron, timezone });
  }
  if (out.length > MAX_CRONS_PER_WORKER) {
    throw new Error(`crons: max ${MAX_CRONS_PER_WORKER} per worker, got ${out.length}`);
  }
  return out;
}

// CF's documented caps — portable wrangler configs should hit the same
// deploy-time bounce.
export const MAX_BATCH_SIZE = 100;
export const MAX_BATCH_TIMEOUT_MS = 60_000;
export const MAX_RETRIES = 100;

// Shape-check the CLI-normalized queue consumer entries (seconds → ms
// + renames already applied there).
/**
 * @param {unknown} raw
 * @returns {QueueConsumer[]}
 */
export function parseQueueConsumers(raw) {
  if (raw == null) return [];
  if (!Array.isArray(raw)) throw new Error("queueConsumers must be an array");
  const seenQueues = new Set();
  const out = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`queueConsumers entry must be an object, got ${JSON.stringify(entry)}`);
    }
    if (!isValidQueueName(entry.queue)) {
      throw new Error(
        `queueConsumers[].queue must match ${QUEUE_NAME_RE}, got ${JSON.stringify(entry.queue)}`
      );
    }
    const queue = entry.queue;
    if (seenQueues.has(queue)) {
      throw new Error(`queueConsumers: duplicate consumer for queue ${JSON.stringify(queue)}`);
    }
    seenQueues.add(queue);
    const maxBatchSize = entry.maxBatchSize == null ? 10 : entry.maxBatchSize;
    const maxBatchTimeoutMs = entry.maxBatchTimeoutMs == null ? 5000 : entry.maxBatchTimeoutMs;
    const maxRetries = entry.maxRetries == null ? 3 : entry.maxRetries;
    if (!Number.isInteger(maxBatchSize) || maxBatchSize < 1 || maxBatchSize > MAX_BATCH_SIZE) {
      throw new Error(
        `queueConsumers[${queue}].maxBatchSize must be integer in [1, ${MAX_BATCH_SIZE}]`
      );
    }
    if (!Number.isInteger(maxBatchTimeoutMs) || maxBatchTimeoutMs < 0 || maxBatchTimeoutMs > MAX_BATCH_TIMEOUT_MS) {
      throw new Error(
        `queueConsumers[${queue}].maxBatchTimeoutMs must be integer in [0, ${MAX_BATCH_TIMEOUT_MS}]`
      );
    }
    if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > MAX_RETRIES) {
      throw new Error(
        `queueConsumers[${queue}].maxRetries must be integer in [0, ${MAX_RETRIES}]`
      );
    }
    /** @type {QueueConsumer} */
    const normalized = { queue, maxBatchSize, maxBatchTimeoutMs, maxRetries };
    if (entry.retryDelaySeconds != null) {
      const retryDelaySeconds = entry.retryDelaySeconds;
      if (!Number.isInteger(retryDelaySeconds) || retryDelaySeconds < 0 || retryDelaySeconds > MAX_QUEUE_DELAY_SECONDS) {
        throw new Error(
          `queueConsumers[${queue}].retryDelaySeconds must be integer in [0, ${MAX_QUEUE_DELAY_SECONDS}]`
        );
      }
      normalized.retryDelaySeconds = retryDelaySeconds;
    }
    if (entry.deadLetterQueue != null) {
      if (!isValidQueueName(entry.deadLetterQueue)) {
        throw new Error(
          `queueConsumers[${queue}].deadLetterQueue must match ${QUEUE_NAME_RE}, got ${JSON.stringify(entry.deadLetterQueue)}`
        );
      }
      if (entry.deadLetterQueue === queue) {
        throw new Error(`queueConsumers[${queue}].deadLetterQueue must differ from source queue`);
      }
      normalized.deadLetterQueue = entry.deadLetterQueue;
    }
    out.push(normalized);
  }
  return out;
}
