import { fnv1a32Utf8 } from "shared-fnv1a32";

export const MAX_ID_BYTES = 512;
export const CLASS_NAME_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
export const METHOD_NAME_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
export const HEADER_NAME_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
export const STORAGE_ID_RE = /^[a-z0-9_-]+$/;
export const HOST_ID_RE = /^[a-z0-9_-]+:[A-Za-z_$][A-Za-z0-9_$]*:shard(?:0|[1-9][0-9]*)$/;
export const DO_HOST_SHARD_COUNT = 16;
export const DO_OWNERSHIP_CODE = Object.freeze({
  OWNER_CLAIM_RACED: "owner_claim_raced",
  OWNER_FENCE_MISSING: "owner_fence_missing",
  STALE_OWNER_GENERATION: "stale_owner_generation",
  OWNER_LEASE_EXPIRED: "owner_lease_expired",
  STALE_OWNER_STORAGE: "stale_owner_storage",
  OWNER_LEASE_TOO_SHORT: "owner_lease_too_short",
  OWNER_RENEW_RACED: "owner_renew_raced",
  OWNER_RELEASE_RACED: "owner_release_raced",
  OWNER_UNAVAILABLE: "owner_unavailable",
  OWNER_ENDPOINT_MISSING: "owner_endpoint_missing",
  FORWARD_HOP_EXHAUSTED: "forward_hop_exhausted",
  TASK_DRAINING: "task_draining",
});
export const DO_OWNER_RACE_RETRY_CODES = Object.freeze([
  DO_OWNERSHIP_CODE.STALE_OWNER_GENERATION,
  DO_OWNERSHIP_CODE.OWNER_CLAIM_RACED,
  DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING,
  DO_OWNERSHIP_CODE.OWNER_LEASE_EXPIRED,
  DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE,
  DO_OWNERSHIP_CODE.OWNER_LEASE_TOO_SHORT,
  DO_OWNERSHIP_CODE.OWNER_RENEW_RACED,
  DO_OWNERSHIP_CODE.TASK_DRAINING,
]);

/**
 * Pure cross-tier shard projection. Callers own identity validation at their
 * trust boundary before using the result as authority.
 * @param {string} objectName
 * @param {number} [shardCount]
 */
export function doHostShardForObjectName(objectName, shardCount = DO_HOST_SHARD_COUNT) {
  return fnv1a32Utf8(objectName) % shardCount;
}

/**
 * @param {string} doStorageId
 * @param {string} className
 * @param {number} shard
 */
export function formatDoOwnerShardKey(doStorageId, className, shard) {
  return `${doStorageId}:${className}:shard${shard}`;
}
