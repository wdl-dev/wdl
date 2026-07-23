// Canonical Redis key grammar. A stray colon in a queue name would
// corrupt parseStreamKey's anchor, so inline construction is a drift
// risk — every tier imports from here.

import { isValidRouteNs, isValidRuntimeLoadNs, QUEUE_NAME_RE } from "./ns-pattern.js";

/**
 * @typedef {{ ns: string, queue: string }} QueueKeyParts
 */

/** @param {string} ns @param {string} queue @returns {string} */
export function queueStreamKey(ns, queue)   { return `queue:${ns}:${queue}:s`; }
/** @param {string} ns @param {string} queue @returns {string} */
export function queueDelayedKey(ns, queue)  { return `queue-delayed:${ns}:${queue}`; }
/** @param {string} ns @param {string} queue @returns {string} */
export function queueDlqKey(ns, queue)      { return `queue:${ns}:${queue}:dlq`; }
/** @param {string} ns @param {string} queue @returns {string} */
export function queueOrphanedKey(ns, queue) { return `queue-orphaned:${ns}:${queue}`; }
/** @param {string} ns @param {string} queue @returns {string} */
export function queueConsumerKey(ns, queue) { return `queue-consumer:${ns}:${queue}`; }

export const QUEUE_CONSUMER_INDEX_KEY = "queue:index:consumers";
export const QUEUE_STREAM_INDEX_KEY = "queue:index:streams";
export const QUEUE_DELAYED_INDEX_KEY = "queue:index:delayed";
export const QUEUE_DELAYED_WAKE_STREAM = "queue-delayed-wake";

// Partial key for SCAN MATCH; sharing the grammar matters even here.
/** @param {string} ns @returns {string} */
export function queueConsumerScanPrefix(ns) { return `queue-consumer:${ns}:`; }

/**
 * @param {string} rest
 * @param {(ns: string) => boolean} isValidNs
 * @returns {QueueKeyParts | null}
 */
function parseQueueKeyRest(rest, isValidNs) {
  const separator = rest.indexOf(":");
  if (separator <= 0) return null;
  const ns = rest.slice(0, separator);
  const queue = rest.slice(separator + 1);
  if (!isValidNs(ns) || !QUEUE_NAME_RE.test(queue)) return null;
  return { ns, queue };
}

/**
 * @param {string} key
 * @returns {QueueKeyParts | null}
 */
export function parseStreamKey(key) {
  if (!key.startsWith("queue:") || !key.endsWith(":s")) return null;
  return parseQueueKeyRest(key.slice("queue:".length, -":s".length), isValidRuntimeLoadNs);
}

/**
 * @param {string} key
 * @returns {QueueKeyParts | null}
 */
export function parseDelayedKey(key) {
  if (!key.startsWith("queue-delayed:")) return null;
  return parseQueueKeyRest(key.slice("queue-delayed:".length), isValidRuntimeLoadNs);
}

/**
 * @param {string} key
 * @returns {QueueKeyParts | null}
 */
export function parseConsumerKey(key) {
  if (!key.startsWith("queue-consumer:")) return null;
  return parseQueueKeyRest(key.slice("queue-consumer:".length), isValidRouteNs);
}
