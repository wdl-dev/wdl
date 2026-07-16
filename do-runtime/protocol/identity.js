import {
  CLASS_NAME_RE,
  DO_HOST_SHARD_COUNT,
  HOST_ID_RE,
  MAX_ID_BYTES,
  STORAGE_ID_RE,
} from "do-runtime-protocol-wire-grammar";
import { DoRuntimeError } from "do-runtime-protocol-errors";
import { fnv1a32Utf8 } from "shared-fnv1a32";

const utf8Encoder = new TextEncoder();

/** @param {unknown} value */
export function isWellFormedUnicodeString(value) {
  return typeof value === "string" && value.isWellFormed();
}

/** @param {string} value */
function byteLength(value) {
  return utf8Encoder.encode(value).byteLength;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @param {RegExp | null} pattern
 */
function requireIdentityString(value, field, pattern) {
  if (typeof value !== "string" || value.length === 0) {
    throw new DoRuntimeError(400, "invalid_request", `${field} must be a non-empty string`);
  }
  if (!isWellFormedUnicodeString(value)) {
    throw new DoRuntimeError(400, "invalid_request", `${field} must contain well-formed Unicode`);
  }
  if (byteLength(value) > MAX_ID_BYTES) {
    throw new DoRuntimeError(400, "invalid_request", `${field} is too large`);
  }
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) {
      throw new DoRuntimeError(400, "invalid_request", `${field} must not contain control characters`);
    }
  }
  if (pattern && !pattern.test(value)) {
    throw new DoRuntimeError(400, "invalid_request", `${field} is not valid`);
  }
}

/**
 * @param {string} objectName
 * @param {number} [shardCount]
 */
export function shardForObjectName(objectName, shardCount = DO_HOST_SHARD_COUNT) {
  requireIdentityString(objectName, "objectName", null);
  const count = Number(shardCount);
  if (!Number.isInteger(count) || count <= 0) {
    throw new DoRuntimeError(500, "invalid_shard_count", "DO host shard count must be a positive integer");
  }
  return fnv1a32Utf8(objectName) % count;
}

/**
 * @param {string} doStorageId
 * @param {string} className
 * @param {number} shard
 */
export function hostIdForShard(doStorageId, className, shard) {
  const parsed = Number(shard);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed >= DO_HOST_SHARD_COUNT) {
    throw new DoRuntimeError(400, "invalid_request", "host shard is not valid");
  }
  requireIdentityString(doStorageId, "doStorageId", STORAGE_ID_RE);
  requireIdentityString(className, "className", CLASS_NAME_RE);
  const hostId = `${doStorageId}:${className}:shard${parsed}`;
  requireIdentityString(hostId, "hostId", HOST_ID_RE);
  return hostId;
}

/**
 * @param {string} doStorageId
 * @param {string} className
 * @param {string} objectName
 */
export function hostIdForObject(doStorageId, className, objectName) {
  return hostIdForShard(doStorageId, className, shardForObjectName(objectName));
}
