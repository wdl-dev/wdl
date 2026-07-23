import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import { fnv1a32CodeUnits } from "../../../shared/fnv1a32.js";
import { doStorageIdKey } from "../../../shared/worker-contract.js";
import { doAlarmJobIdForStorage } from "../../helpers/do-alarm-job-id.js";
import { serviceInternalPost, serviceInternalPostAsync } from "./internal-http.js";
import {
  redisDel,
  redisEval,
  redisGet,
  redisGetJson,
  redisGetRaw,
  redisHGetAll,
  redisHSet,
  redisKeys,
  redisSAdd,
  redisSIsMember,
  redisSMembers,
  redisSet,
  redisSetJson,
  redisZAdd,
  redisZRange,
  redisZRem,
} from "./redis.js";
import { delay } from "./stack.js";
import { loadDoProtocol } from "../../helpers/load-do-protocol.js";

const {
  DO_INVOKE_CONTENT_TYPE,
  encodeDoInvokeRequest,
  hostIdForObject,
  normalizeDoInvokeRequest,
} = await loadDoProtocol();

const DO_ALARM_READY_SHARDS = 32;
const WORKFLOWS_REDIS_DB = 2;
const WORKFLOWS_REDIS = { db: WORKFLOWS_REDIS_DB };
const SEED_DO_ALARM_CLEANUP_JOBS_SCRIPT = `
local storage_id = ARGV[1]
for index = 2, #KEYS do
  local job_id = ARGV[index]
  redis.call("HSET", KEYS[index], "doStorageId", storage_id)
  redis.call("SADD", KEYS[1], job_id)
end
return #KEYS - 1
`;

/** @param {string} ownerKey */
export function doOwnerRedisKey(ownerKey) {
  return `do:owner:scope:${encodeURIComponent(ownerKey)}`;
}

/** @param {string} ns @param {string} worker @param {string} className @param {string} objectName */
export function doHostId(ns, worker, className, objectName) {
  return hostIdForObject(redisGetDoStorageId(ns, worker), className, objectName);
}

/** @param {string} ns @param {string} worker @param {string} className @param {string} objectName */
export function doAlarmJobId(ns, worker, className, objectName) {
  const doStorageId = redisGetDoStorageId(ns, worker);
  return doAlarmJobIdForStorage(ns, worker, doStorageId, className, objectName);
}

export { doAlarmJobIdForStorage };

/** @param {string} jobId */
export function doAlarmStateKey(jobId) {
  return `wf:internal:do-alarm:{${jobId}}:state`;
}

/** @param {string} jobId */
export function doAlarmShard(jobId) {
  return fnv1a32CodeUnits(jobId) % DO_ALARM_READY_SHARDS;
}

/** @param {string} jobId */
export function doAlarmDueKey(jobId) {
  return `wf:internal:do-alarm:due:${doAlarmShard(jobId)}`;
}

/** @param {string} jobId */
export function doAlarmReadyKey(jobId) {
  return `wf:internal:do-alarm:ready:${doAlarmShard(jobId)}`;
}

/** @param {string} ns @param {string} worker */
export function doAlarmByWorkerKey(ns, worker) {
  return `wf:internal:do-alarm:by-worker:${ns}:${worker}`;
}

/** @param {string} ns @param {string} worker @param {string} className @param {string} objectName */
export function redisGetDoAlarmJob(ns, worker, className, objectName) {
  return redisHGetAll(doAlarmStateKey(doAlarmJobId(ns, worker, className, objectName)), WORKFLOWS_REDIS);
}

/** @param {string} jobId */
export function redisGetDoAlarmJobById(jobId) {
  return redisHGetAll(doAlarmStateKey(jobId), WORKFLOWS_REDIS);
}

/** @param {string} ns @param {string} worker @param {string} className @param {string} objectName */
export function redisDoAlarmJobExists(ns, worker, className, objectName) {
  return Object.keys(redisGetDoAlarmJob(ns, worker, className, objectName)).length > 0;
}

/** @param {string} ns @param {string} worker */
export function redisDoAlarmJobIdsForWorker(ns, worker) {
  return redisSMembers(doAlarmByWorkerKey(ns, worker), WORKFLOWS_REDIS);
}

/** @param {string} ns @param {string} worker */
export function redisDoAlarmStateKeysForWorker(ns, worker) {
  return redisDoAlarmJobIdsForWorker(ns, worker).flatMap((jobId) =>
    redisKeys(doAlarmStateKey(jobId), WORKFLOWS_REDIS)
  );
}

/** @param {string} jobId */
export function redisDoAlarmDueIncludes(jobId) {
  return redisZRange(doAlarmDueKey(jobId), 0, -1, WORKFLOWS_REDIS).includes(jobId);
}

/** @param {string} jobId */
export function redisDoAlarmReadyIncludes(jobId) {
  return redisSIsMember(doAlarmReadyKey(jobId), jobId, WORKFLOWS_REDIS);
}

/** @param {string} jobId @param {Record<string, string>} value */
export function redisSetDoAlarmJob(jobId, value) {
  redisHSet(doAlarmStateKey(jobId), value, WORKFLOWS_REDIS);
}

/** @param {string} jobId */
export function redisDeleteDoAlarmJob(jobId) {
  redisDel(doAlarmStateKey(jobId), WORKFLOWS_REDIS);
}

/** @param {number} score @param {string} jobId */
export function redisAddDoAlarmDue(score, jobId) {
  redisZAdd(doAlarmDueKey(jobId), score, jobId, WORKFLOWS_REDIS);
}

/** @param {string} jobId */
export function redisAddDoAlarmReady(jobId) {
  redisSAdd(doAlarmReadyKey(jobId), jobId, WORKFLOWS_REDIS);
  redisSAdd("wf:internal:do-alarm:ready:active", String(doAlarmShard(jobId)), WORKFLOWS_REDIS);
}

/** @param {string} jobId */
export function redisRemoveDoAlarmDue(jobId) {
  redisZRem(doAlarmDueKey(jobId), jobId, WORKFLOWS_REDIS);
}

/** @param {string} ns @param {string} worker @param {string} jobId */
export function redisAddDoAlarmByWorker(ns, worker, jobId) {
  redisSAdd(doAlarmByWorkerKey(ns, worker), jobId, WORKFLOWS_REDIS);
}

/**
 * Seed enough minimal jobs for cleanup batching without one Docker exec per job.
 * @param {string} ns
 * @param {string} worker
 * @param {string} doStorageId
 * @param {string[]} jobIds
 */
export function redisSeedDoAlarmCleanupJobs(ns, worker, doStorageId, jobIds) {
  const seeded = Number(
    redisEval(
      SEED_DO_ALARM_CLEANUP_JOBS_SCRIPT,
      [doAlarmByWorkerKey(ns, worker), ...jobIds.map(doAlarmStateKey)],
      [doStorageId, ...jobIds],
      WORKFLOWS_REDIS
    )
  );
  assert.equal(seeded, jobIds.length);
}

/** @param {string} ns @param {string} worker */
export function redisGetDoStorageId(ns, worker) {
  return redisGetRaw(doStorageIdKey(ns, worker)) ?? "";
}

/**
 * @typedef {{
 *   generation: number | string,
 *   ownerKey?: string,
 *   taskId?: string,
 *   endpoint?: string,
 *   leaseExpiresAt?: number,
 *   [key: string]: unknown,
 * }} DoOwnerFixture
 */

/** @param {string} ownerKey @param {DoOwnerFixture} owner */
export function redisSetDoOwner(ownerKey, owner) {
  const key = doOwnerRedisKey(ownerKey);
  redisSetJson(key, owner);
  redisSet(`${key}:generation`, String(owner.generation));
}

/** @param {string} ownerKey */
export function redisGetDoOwnerGeneration(ownerKey) {
  return Number(redisGet(`${doOwnerRedisKey(ownerKey)}:generation`) ?? "");
}

/** @param {string} ownerKey */
export function redisGetDoOwner(ownerKey) {
  return redisGetJson(doOwnerRedisKey(ownerKey));
}

/**
 * @param {unknown} input
 * @param {Buffer | Uint8Array | string | null} [body]
 */
function encodeDoInvokeForTest(input, body = null) {
  const invoke = normalizeDoInvokeRequest(input);
  if (body == null) return encodeDoInvokeRequest(invoke);
  assert.ok("request" in invoke);
  return encodeDoInvokeRequest({
    .../** @type {any} */ (invoke),
    request: {
      .../** @type {any} */ (invoke).request,
      bodyBytes: Buffer.isBuffer(body) || body instanceof Uint8Array ? body : Buffer.from(String(body), "utf8"),
    },
  });
}

/** @param {string} service @param {unknown} input @param {{ body?: Buffer | Uint8Array | string | null, headers?: Record<string, string> }} [opts] */
export function doInternalInvoke(service, input, { body = null, headers = {} } = {}) {
  return serviceInternalPost(
    service,
    8788,
    "/internal/do/invoke",
    encodeDoInvokeForTest(input, body),
    { "content-type": DO_INVOKE_CONTENT_TYPE, ...headers }
  );
}

/** @param {string} service @param {unknown} input @param {{ body?: Buffer | Uint8Array | string | null, headers?: Record<string, string> }} [opts] */
export function doInternalInvokeAsync(service, input, { body = null, headers = {} } = {}) {
  return serviceInternalPostAsync(
    service,
    8788,
    "/internal/do/invoke",
    encodeDoInvokeForTest(input, body),
    { "content-type": DO_INVOKE_CONTENT_TYPE, ...headers }
  );
}

/**
 * @param {string} description
 * @param {() => Promise<any>} fn
 * @param {(last: any) => boolean} predicate
 * @param {number} [timeoutMs]
 */
export async function waitForJson(description, fn, predicate, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await fn();
    if (predicate(last)) return last;
    await delay(100);
  }
  assert.fail(`${description} timed out; last=${JSON.stringify(last)}`);
}

export const DO_WORKER = readFileSync(
  new URL("../../../test-workers/do-counter/src/index.js", import.meta.url),
  "utf8"
);

export const DO_SLOW_WORKER = readFileSync(
  new URL("../../../test-workers/do-slow/src/index.js", import.meta.url),
  "utf8"
);

export const DO_ALARM_WORKER = readFileSync(
  new URL("../../../test-workers/do-alarm/src/index.js", import.meta.url),
  "utf8"
);

export const DO_BINDINGS_WORKER = readFileSync(
  new URL("../../../test-workers/do-bindings/src/index.js", import.meta.url),
  "utf8"
);

export const DO_BINARY_BODY_WORKER = readFileSync(
  new URL("../../../test-workers/do-binary-body/src/index.js", import.meta.url),
  "utf8"
);

export const DO_RPC_WORKER = readFileSync(
  new URL("../../../test-workers/do-rpc/src/index.js", import.meta.url),
  "utf8"
);

/** @param {string} label */
export function doVersionWorker(label) {
  return `
import { DurableObject } from "cloudflare:workers";

export class Versioned extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.memory = 0;
  }

  async fetch() {
    this.memory += 1;
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS versions (name TEXT PRIMARY KEY, value INTEGER NOT NULL)");
    const row = [...sql.exec("SELECT value FROM versions WHERE name = ?", "main")][0];
    const storage = (row?.value ?? 0) + 1;
    sql.exec(
      "INSERT INTO versions (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
      "main",
      storage
    );
    return Response.json({ label: ${JSON.stringify(label)}, memory: this.memory, storage });
  }
}

export default {
  async fetch(_request, env) {
    const id = env.VERSIONED.idFromName("main");
    return await env.VERSIONED.get(id).fetch("https://do.internal/versioned");
  },
};
`;
}
