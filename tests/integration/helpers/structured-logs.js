import { sh } from "./cli.js";
import { parseJsonText } from "./json-payload.js";

/**
 * @param {string} service
 * @param {string} event
 * @param {{ tail?: number }} [options]
 * @returns {Record<string, unknown>[]}
 */
export function structuredServiceLogEvents(service, event, options = {}) {
  const tail = options.tail ?? 500;
  const raw = sh(["docker", "compose", "logs", "--no-color", `--tail=${tail}`, service]);
  /** @type {Record<string, unknown>[]} */
  const events = [];
  for (const line of raw.split("\n")) {
    const jsonStart = line.indexOf("{");
    if (jsonStart < 0) continue;
    const candidate = line.slice(jsonStart);
    if (!candidate.includes(`"event":${JSON.stringify(event)}`)) continue;
    try {
      const entry = parseJsonText(candidate, `${service} structured log`);
      if (entry?.event === event) events.push(entry);
    } catch {
      // Docker may interleave non-JSON service output with structured lines.
    }
  }
  return events;
}
