import {
  queueDelayedKey,
  QUEUE_DELAYED_WAKE_STREAM,
  queueDlqKey,
  queueConsumerKey,
  QUEUE_CONSUMER_INDEX_KEY,
  QUEUE_DELAYED_INDEX_KEY,
  QUEUE_STREAM_INDEX_KEY,
  queueOrphanedKey,
  queueStreamKey,
} from "../../../shared/queue-keys.js";

/**
 * @typedef {"json" | "text" | "bytes"} QueueMessageContentType
 * @typedef {{
 *   id: string,
 *   body: unknown,
 *   contentType: QueueMessageContentType,
 *   attempts?: number,
 *   firstSeenMs?: number,
 * }} QueueMessageInput
 * @typedef {{
 *   id: string,
 *   body_b64: string,
 *   content_type: QueueMessageContentType,
 *   attempts: number,
 *   first_seen_ms: number,
 * }} QueueStreamMessage
 */

/** @param {unknown} value @param {QueueMessageContentType} contentType */
export function encodeQueueMessageBody(value, contentType) {
  if (contentType === "text") return Buffer.from(String(value), "utf8").toString("base64");
  if (contentType === "json") return Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  if (contentType === "bytes") {
    const bytes = value instanceof Uint8Array
      ? value
      : new Uint8Array(/** @type {ArrayBufferLike} */ (value));
    return Buffer.from(bytes).toString("base64");
  }
  throw new Error(`Unsupported queue message content type: ${contentType}`);
}

/** @param {QueueMessageInput} input @returns {QueueStreamMessage} */
export function queueStreamMessage({
  id,
  body,
  contentType,
  attempts = 0,
  firstSeenMs = Date.now(),
}) {
  return {
    id,
    body_b64: encodeQueueMessageBody(body, contentType),
    content_type: contentType,
    attempts,
    first_seen_ms: firstSeenMs,
  };
}

/** @param {QueueMessageInput} input @returns {Record<string, string | number>} */
export function queueStreamMessageFields(input) {
  return queueStreamMessage(input);
}

/** @param {QueueMessageInput} input @returns {string} */
export function queueDelayedMessageMember(input) {
  const message = queueStreamMessage(input);
  return JSON.stringify({
    id: message.id,
    body_b64: message.body_b64,
    content_type: message.content_type,
    attempts: String(message.attempts),
    first_seen_ms: String(message.first_seen_ms),
  });
}

export {
  queueDelayedKey,
  QUEUE_DELAYED_WAKE_STREAM,
  queueDlqKey,
  queueConsumerKey,
  QUEUE_CONSUMER_INDEX_KEY,
  QUEUE_DELAYED_INDEX_KEY,
  QUEUE_STREAM_INDEX_KEY,
  queueOrphanedKey,
  queueStreamKey,
};
