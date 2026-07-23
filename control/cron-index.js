// Pure cron helpers — diff two __meta__.crons lists and compute fire-slot
// placements. No Redis / IO so trivially unit-testable.
import { createHash } from "node:crypto";
// Re-exported from shared/ where control promote + scheduler advance both consume it.
export { nextFireMs, slotMsFor } from "shared-cron-time";

// Stable id for a (cron, timezone) pair. sha1 truncated to 10 hex chars —
// collision space (>10^12) dwarfs the 10-entries-per-worker limit, and a
// short id keeps Redis keys + log lines readable.
/**
 * @typedef {{ cron: string, timezone: string }} CronEntry
 * @typedef {{ cron: string, timezone: string, gen: string | number }} ExistingCronEntry
 */

/**
 * @param {string} cron
 * @param {string} timezone
 */
export function cronId(cron, timezone) {
  return createHash("sha1").update(`${cron}|${timezone}`).digest("hex").slice(0, 10);
}

// Diff an existing cron hash (parsed, __meta__ removed) against a new
// __meta__.crons list. Returns the minimum set of ops needed at promote:
//   added:   fresh entries — caller allocates `gen` from cron:seq high-water
//            and computes the initial bucket slot
//   removed: stale entries — HDEL from hash; bucket refs decay lazily
//   kept:    unchanged — preserve `gen`; no hash or bucket writes
//
// `oldById` shape: { id → { cron, timezone, gen } }
/**
 * @param {Record<string, ExistingCronEntry>} oldById
 * @param {CronEntry[]} newList
 */
export function diffCrons(oldById, newList) {
  const newById = new Map();
  for (const entry of newList) {
    const id = cronId(entry.cron, entry.timezone);
    newById.set(id, { id, cron: entry.cron, timezone: entry.timezone });
  }

  const added = [];
  const kept = [];
  for (const [id, e] of newById) {
    const prev = oldById[id];
    if (prev) {
      kept.push({ ...e, gen: prev.gen });
    } else {
      added.push({ ...e });
    }
  }

  const removed = [];
  for (const [id, prev] of Object.entries(oldById)) {
    if (!newById.has(id)) removed.push({ id, gen: prev.gen });
  }

  return { added, removed, kept };
}
