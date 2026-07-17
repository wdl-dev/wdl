import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const ROOT = path.resolve(import.meta.dirname, "..");
export const DEFAULT_INTEGRATION_DURATIONS_FILE = path.join(ROOT, ".integration-test-durations.json");

// Update when integration runs reveal new slow tests; ordering affects
// scheduling latency, not correctness. Last calibrated 2026-07-17 against
// .integration-test-durations.json (descending durationMs). The recorded
// duration file, when present, takes precedence over this fallback list.
export const SLOW_FIRST_FILES = [
  "d1-storage-shared-localdisk.test.js",
  "durable-objects-ownership.test.js",
  "d1-ownership-multi-runtime.test.js",
  "durable-objects-alarms.test.js",
  "cron-triggers.test.js",
  "log-tail.test.js",
  "durable-objects-websocket.test.js",
  "workflows-runtime-retention.test.js",
  "queues-orphan-and-control.test.js",
  "queues-retry-and-delay.test.js",
  "gateway-websocket.test.js",
  "workflows-runtime-scheduler.test.js",
  "d1-read-cache.test.js",
  "delete-api.test.js",
  "queues-delivery.test.js",
  "queues-batch-and-isolation.test.js",
  "gateway.test.js",
  "d1-binding.test.js",
  "auth-platform.test.js",
  "durable-objects-storage.test.js",
  "secrets.test.js",
  "kv-binding.test.js",
  "admin-api.test.js",
  "scheduler-shutdown-drain.test.js",
  "auth-worker.test.js",
  "workflows-runtime-pausing.test.js",
  "workflows-runtime-core.test.js",
  "r2-cli-binding.test.js",
  "service-bindings-rpc.test.js",
  "d1-storage-localdisk.test.js",
  "redis-conformance.test.js",
  "platform-bindings.test.js",
  "cli-smoke.test.js",
  "cli-multi-env.test.js",
  "http-features.test.js",
  "s3-cleanup.test.js",
  "d1-lifecycle.test.js",
  "observability.test.js",
  "delete-indexes.test.js",
  "network-boundary.test.js",
  "service-bindings.test.js",
  "assets-binding.test.js",
  "pages-assets-demo.test.js",
  "route-demo.test.js",
  "routing-gateway.test.js",
  "runtime-eviction.test.js",
  "queue-native-dispatch.test.js",
  "durable-objects-core.test.js",
  "routing-admin.test.js",
  "system-pool-auth.test.js",
  "workflows-metadata.test.js",
  "worker-modules.test.js",
  "workflows-service.test.js",
  "workflows-durable-objects.test.js",
];

export const CLI_INTEGRATION_MARKER = "@wdl-cli-integration";

// Keep marker detection line-anchored so strings and doc blocks do not opt a
// file into the CLI integration runner.
const CLI_INTEGRATION_MARKER_RE = new RegExp(`^//\\s*${RegExp.escape(CLI_INTEGRATION_MARKER)}\\b`, "m");

/**
 * @typedef {{ durationMs?: unknown, status?: unknown, updatedAt?: unknown }} DurationRecord
 * @typedef {Record<string, DurationRecord>} DurationRecords
 * @typedef {{ updatedAt?: unknown, runDurationMs?: unknown, files?: unknown }} DurationReport
 * @typedef {{ priority?: string[], durationRecords?: DurationRecords | null }} PrioritizeOptions
 */

/** @param {unknown} err */
function errorMessage(err) {
  return err instanceof Error ? err.message : String(err);
}

/** @param {DurationReport | null} report @returns {DurationRecords | null} */
function durationRecordsFromReport(report) {
  const files = report?.files;
  return files && typeof files === "object"
    ? /** @type {DurationRecords} */ (files)
    : null;
}

/** @param {string} [file] @returns {DurationReport | null} */
export function readIntegrationDurationReport(file = DEFAULT_INTEGRATION_DURATIONS_FILE) {
  if (!file || !existsSync(file)) return null;
  try {
    const parsed = JSON.parse(readFileSync(file, "utf8"));
    return parsed && typeof parsed === "object"
      ? /** @type {DurationReport} */ (parsed)
      : null;
  } catch (err) {
    process.stderr.write(
      `warning: ignoring unreadable integration duration file ${file}: ${errorMessage(err)}\n`
    );
    return null;
  }
}

/** @param {string} [file] @returns {DurationRecords | null} */
export function readIntegrationDurationRecords(file = DEFAULT_INTEGRATION_DURATIONS_FILE) {
  return durationRecordsFromReport(readIntegrationDurationReport(file));
}

/** @param {string[]} names @param {DurationRecords | null | undefined} durationRecords */
export function durationPriorityNames(names, durationRecords) {
  if (!durationRecords) return [];
  const available = new Set(names);
  return Object.entries(durationRecords)
    .map(([file, record]) => ({
      name: path.basename(file),
      durationMs: Number(record?.durationMs),
      status: typeof record?.status === "string" ? record.status : "",
      updatedAt: typeof record?.updatedAt === "string" ? record.updatedAt : "",
    }))
    .filter((entry) =>
      available.has(entry.name) &&
      Number.isFinite(entry.durationMs) &&
      entry.status !== "failed"
    )
    .toSorted((a, b) =>
      b.durationMs - a.durationMs ||
      b.updatedAt.localeCompare(a.updatedAt) ||
      a.name.localeCompare(b.name)
    )
    .map((entry) => entry.name);
}

/** @param {string[]} names @param {string[] | PrioritizeOptions} [options] */
export function prioritizeDefaultFiles(names, options = {}) {
  const normalized = Array.isArray(options)
    ? { priority: options }
    : options;
  const priority = normalized.priority ?? SLOW_FIRST_FILES;
  const durationRecords = normalized.durationRecords ?? null;
  const available = new Set(names);
  const durationPriority = durationPriorityNames(names, durationRecords);
  const durationSet = new Set(durationPriority);
  const prioritized = [
    ...durationPriority,
    ...priority.filter((name) => available.has(name) && !durationSet.has(name)),
  ];
  const prioritizedSet = new Set(prioritized);
  return [
    ...prioritized,
    ...names.filter((name) => !prioritizedSet.has(name)),
  ];
}

/** @param {string} source */
export function hasCliIntegrationMarker(source) {
  return CLI_INTEGRATION_MARKER_RE.test(source);
}
