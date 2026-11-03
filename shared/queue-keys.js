// Queue key builders for Control and integration helpers. Callers validate
// namespace and queue names; Rust owns parsing persisted queue keys.

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
