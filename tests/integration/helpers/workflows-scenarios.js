// WDL Workflows integration scenario helpers.
import { readFileSync } from "node:fs";
import { adminFetch, deployAndPromote } from "./admin-http.js";
import { composeRestart, composeScale, withServiceStopped } from "./compose.js";
import { gatewayFetch, gatewayWorkerId, uniqueNs } from "./gateway-http.js";
import { readIntegrationJson, responseJson } from "./http-response.js";
import {
  runtimeDispatchPost,
  serviceInternalGet,
  serviceInternalPost,
  serviceInternalPostLarge,
} from "./internal-http.js";
import { readMeta } from "./misc.js";
import {
  redisCommandCalls,
  redisDel,
  redisEval,
  redisHDel,
  redisHGet,
  redisHSet,
  redisSAdd,
  redisSIsMember,
  redisScriptFlush,
  redisSMembers,
  redisSetEx,
  redisZAdd,
  redisZScore,
} from "./redis.js";
import { delay, setupIntegrationSuite, waitUntil } from "./stack.js";
import { workflowTickCount } from "./workflow-tick.js";
import { fnv1a32Utf8 } from "../../../shared/fnv1a32.js";

export {
  adminFetch,
  composeRestart,
  composeScale,
  delay,
  deployAndPromote,
  gatewayFetch,
  gatewayWorkerId,
  redisDel,
  redisCommandCalls,
  redisSAdd,
  redisSIsMember,
  redisScriptFlush,
  redisSMembers,
  redisSetEx,
  redisZAdd,
  redisZScore,
  runtimeDispatchPost,
  serviceInternalGet,
  serviceInternalPost,
  serviceInternalPostLarge,
  setupIntegrationSuite,
  uniqueNs,
  waitUntil,
  workflowTickCount,
  withServiceStopped,
  readIntegrationJson,
  responseJson,
};

export const WORKER_CODE = readFileSync(
  new URL("../../../test-workers/workflows-demo/src/index.js", import.meta.url),
  "utf8"
);

/** @param {string} ns @param {string} worker */
export function workflowByWorkerKey(ns, worker) {
  return `wf:by-worker:${ns}:${worker}`;
}

/** @param {string} ns @param {string} worker @param {string} version */
export function workflowByVersionKey(ns, worker, version) {
  return `wf:by-version:${ns}:${worker}:${version}`;
}

/** @param {string} ns @param {string} worker @param {string} version */
export function workflowPendingVersionKey(ns, worker, version) {
  return `wf:pending-version:${ns}:${worker}:${version}`;
}


/** @param {string} ns @param {string} workflowKey @param {string} version @param {number} pending @param {number} stale */
export function seedWorkflowLifecycleMembers(ns, workflowKey, version, pending, stale) {
  return redisEval(`
for i = 1, tonumber(ARGV[4]) do
  local id = 'pending-' .. i
  local key = 'wf:instance:{' .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. id .. '}:state'
  redis.call('HSET', key, 'ns', ARGV[1], 'worker', 'shop', 'workflowKey', ARGV[2],
    'instanceId', id, 'frozenVersion', ARGV[3], 'workflowName', 'orders',
    'className', 'OrderWorkflow', 'status', 'pending_create', 'generation', '1',
    'createdAtMs', '1', 'pendingCreateToken', id, 'pendingExpiresAtMs', '1')
  redis.call('SADD', KEYS[1], ARGV[2] .. '\\t' .. id)
  redis.call('SADD', KEYS[2], ARGV[2] .. '\\t' .. id)
end
for i = 1, tonumber(ARGV[5]) do
  redis.call('SADD', KEYS[1], ARGV[2] .. '\\tstale-' .. i)
end
return 1`, [workflowByWorkerKey(ns, "shop"), workflowByVersionKey(ns, "shop", version)],
  [ns, workflowKey, version, String(pending), String(stale)], { db: 2 });
}

/** @param {string} ns @param {string} worker @param {string} version */
export function workerMeta(ns, worker, version) {
  return readMeta(ns, worker, version);
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId */
export function workflowInstanceStateKey(ns, workflowKey, instanceId) {
  return `wf:instance:{${ns}:${workflowKey}:${instanceId}}:state`;
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId */
export function workflowEventTypeIndexKey(ns, workflowKey, instanceId) {
  return `wf:instance:{${ns}:${workflowKey}:${instanceId}}:events-by-type`;
}

export function workflowRetentionKey() {
  return "wf:retention";
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId */
export function workflowReadyToken(ns, workflowKey, instanceId) {
  return `${ns}\t${workflowKey}\t${instanceId}`;
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId */
export function workflowReadyShard(ns, workflowKey, instanceId) {
  return fnv1a32Utf8(`${ns}:${workflowKey}:${instanceId}`) % 32;
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId @param {string} field */
export function redisWorkflowStateHGet(ns, workflowKey, instanceId, field) {
  return redisHGet(workflowInstanceStateKey(ns, workflowKey, instanceId), field, { db: 2 }) ?? "";
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId @param {string[]} fieldValuePairs */
export function redisWorkflowStateHSet(ns, workflowKey, instanceId, fieldValuePairs) {
  /** @type {Record<string, string>} */
  const fields = {};
  for (let i = 0; i < fieldValuePairs.length; i += 2) {
    fields[fieldValuePairs[i]] = fieldValuePairs[i + 1];
  }
  redisHSet(workflowInstanceStateKey(ns, workflowKey, instanceId), fields, { db: 2 });
}

/**
 * @param {string} ns
 * @param {string} workflowKey
 * @param {string} instanceId
 * @param {string} runToken
 * @param {number} [leaseMs]
 */
export function setWorkflowRunningState(ns, workflowKey, instanceId, runToken, leaseMs = 60_000) {
  redisWorkflowStateHSet(ns, workflowKey, instanceId, [
    "status",
    "running",
    "updatedAtMs",
    String(Date.now()),
    "runToken",
    runToken,
    "runLeaseExpiresAtMs",
    String(Date.now() + leaseMs),
  ]);
}

/** @param {string} ns @param {string} workflowKey @param {string} instanceId @param {string[]} fields */
export function redisWorkflowStateHDel(ns, workflowKey, instanceId, fields) {
  redisHDel(workflowInstanceStateKey(ns, workflowKey, instanceId), fields, { db: 2 });
}

/**
 * @param {string} ns
 * @param {string} workflowKey
 * @param {string} version
 * @param {string} instanceId
 * @param {string} runToken
 * @param {Record<string, unknown>} params
 */
export function dispatchWorkflowReplay(ns, workflowKey, version, instanceId, runToken, params) {
  const createdAtMs = Number(redisWorkflowStateHGet(ns, workflowKey, instanceId, "createdAtMs"));
  return runtimeDispatchPost(
    "/internal/workflows/run",
    { "x-worker-id": gatewayWorkerId(ns, "shop", version) },
    {
      ns,
      worker: "shop",
      frozenVersion: version,
      workflowName: "orders",
      workflowKey,
      className: "OrderWorkflow",
      instanceId,
      generation: 1,
      createdAtMs,
      runToken,
      dispatchDeadlineMs: Date.now() + 60_000,
      params,
    }
  );
}
