// Runtime helpers for the socket-less auth worker. This module owns Redis IO,
// bootstrap reflection, and observability; auth/index.js owns the JSRPC method
// flow and auth policy decisions.

import { RedisClient, WatchError, decodeBulk } from "shared-redis";
import {
  createLogLevelBinder,
  createLogger,
  formatError,
  recordRedisCommand,
  sanitizeRequestId,
} from "shared-observability";
import {
  AuthPolicyError,
  BOOTSTRAP_TOKEN_ID,
  hashToken,
  parseTokenRecord,
} from "auth-lib";
import {
  actionCategory,
} from "shared-auth-roles";
import { withOptimisticRetries } from "shared-optimistic-retry";

/**
 * @typedef {import("shared-redis").RedisSession} RedisSession
 * @typedef {import("shared-redis").RedisClient} RedisClientType
 * @typedef {(level: string, event: string, requestId: string | null, fields?: Record<string, unknown>) => void} AuthLogger
 * @typedef {{ desiredHash: string, cached: boolean }} BootstrapState
 * @typedef {{ ok: boolean, status: number, reason?: string, [key: string]: unknown }} AuthReturn
 * @typedef {{ ret: AuthReturn, reason?: string, tokenId?: string, principalKind?: string, principalNs?: string }} VerifyFinalizeInput
 * @typedef {{ finalize(input: VerifyFinalizeInput): AuthReturn }} VerifyFinalizer
 * @typedef {import("auth-lib").TokenRecord} AuthTokenRecord
 * @typedef {{
 *   hGet(key: string, field: string): Promise<string | null | undefined>,
 *   hGetAll(key: string): Promise<Record<string, string | null | undefined>>,
 *   session?: (fn: (session: RedisSession) => Promise<boolean>) => Promise<boolean>,
 * }} RedisAuthReader
 * @typedef {RedisAuthReader & {
 *   hGetAllMany(keys: string[]): Promise<Array<Record<string, string | null | undefined>>>,
 * }} RedisAuthBatchReader
 * @typedef {RedisAuthReader & {
 *   eval(script: string, keys?: string[], args?: Array<string | number | boolean | Uint8Array>): Promise<unknown>,
 * }} RedisAuthVerifier
 * @typedef {RedisAuthReader & {
 *   watch(key: string): Promise<unknown>,
 *   unwatch(): Promise<unknown>,
 *   multi(): import("shared-redis").RedisMulti,
 * }} RedisAuthSession
 * @typedef {{
 *   newRedis(): RedisClientType,
 *   logAuth: AuthLogger,
 *   ensureBootstrap(redis: RedisAuthReader, requestId: string | null): Promise<BootstrapState>,
 *   ensureBootstrapForVerify(redis: RedisAuthReader, requestId: string | null): Promise<BootstrapState>,
 *   readRecord(redis: RedisAuthReader, tokenId: string): Promise<AuthTokenRecord | null>,
 *   readRecordByHash(redis: RedisAuthVerifier, hash: string): Promise<{ tokenId: string, record: AuthTokenRecord | null } | null>,
 *   readRecords(redis: RedisAuthBatchReader, tokenIds: string[]): Promise<Array<{ tokenId: string, record: AuthTokenRecord | null }>>,
 *   rethrowPolicy(requestId: string | null, command: string, err: unknown): never,
 *   beginVerify(requestId: string | null, startedAt: number, action: unknown, ns: string | undefined): VerifyFinalizer,
 *   recordVerifyThrow(requestId: string | null, action: unknown, ns: string | undefined, err: unknown): void,
 *   recordLifecycleOk(command: string, requestId: string | null, fields?: Record<string, unknown>): void,
 * }} AuthRuntime
 */

export const TOKEN_KEY_PREFIX = "auth:token:";
export const HASH_KEY_PREFIX = "auth:hash:";
const READ_TOKEN_BY_HASH_SCRIPT = `
local token_id = redis.call("GET", KEYS[1])
if not token_id then
  return {0}
end
local record = redis.call("HGETALL", ARGV[1] .. token_id)
local out = {1, token_id}
for i = 1, #record do
  table.insert(out, record[i])
end
return out
`;
export { formatError };

/** @param {string} tokenId */
export function tokenKey(tokenId) { return `${TOKEN_KEY_PREFIX}${tokenId}`; }
/** @param {string} hash */
export function hashKey(hash) { return `${HASH_KEY_PREFIX}${hash}`; }

/** @type {null | ((level: string, event: string, fields?: Record<string, unknown>) => void)} */
let log = null;
const bindLogLevel = createLogLevelBinder();
let service = "auth";
let bootstrapMissingLogged = false;
/** @type {string | null} */
let verifyBootstrapHash = null;

/** @param {Record<string, unknown>} env */
function ensureInit(env) {
  if (!log) {
    service = typeof env.SERVICE_NAME === "string" && env.SERVICE_NAME ? env.SERVICE_NAME : "auth";
    log = createLogger(service);
  }
  bindLogLevel(env);
}

/** @param {import("shared-redis").RedisCommandEvent} event */
function onRedisCommand(event) {
  recordRedisCommand({ metrics: null, log, service, event });
}

/** @param {unknown} raw */
export function normalizeRequestId(raw) {
  // workerd JSRPC does not carry AsyncContextFrame across isolate boundaries;
  // the request id is only what control explicitly passes in.
  return sanitizeRequestId(raw);
}

/** @param {unknown} err */
export function isRedisWatchError(err) {
  return err instanceof WatchError;
}

/** @param {string} key @param {Record<string, unknown>} fields */
export function buildHsetCmd(key, fields) {
  const cmd = ["HSET", key];
  for (const [f, v] of Object.entries(fields)) {
    if (v === undefined) continue;
    cmd.push(f, String(v));
  }
  return cmd;
}

/** @param {Record<string, unknown>} env @returns {AuthRuntime} */
export function bindAuthRuntime(env) {
  ensureInit(env);
  /** @type {AuthLogger} */
  const logAuth = (level, event, requestId, fields) => {
    const payload = requestId ? { request_id: requestId, ...fields } : { ...fields };
    log?.(level, event, payload);
  };
  return {
    newRedis() {
      return new RedisClient(String(env.REDIS_ADDR), { onCommand: onRedisCommand });
    },

    logAuth,

    /** @param {RedisAuthReader} redis @param {string | null} requestId */
    async ensureBootstrap(redis, requestId) {
      return ensureBootstrap(env, redis, requestId, logAuth);
    },

    /** @param {RedisAuthReader} redis @param {string | null} requestId */
    async ensureBootstrapForVerify(redis, requestId) {
      return ensureBootstrapForVerify(env, redis, requestId, logAuth);
    },

    /** @param {RedisAuthReader} redis @param {string} tokenId */
    async readRecord(redis, tokenId) {
      const hash = await redis.hGetAll(tokenKey(tokenId));
      return parseTokenRecord(hash);
    },

    /** @param {RedisAuthVerifier} redis @param {string} hash */
    async readRecordByHash(redis, hash) {
      const reply = await redis.eval(
        READ_TOKEN_BY_HASH_SCRIPT,
        [hashKey(hash)],
        [TOKEN_KEY_PREFIX],
      );
      return parseIndexedTokenReply(reply);
    },

    /** @param {RedisAuthBatchReader} redis @param {string[]} tokenIds */
    async readRecords(redis, tokenIds) {
      if (tokenIds.length === 0) return [];
      const hashes = await redis.hGetAllMany(tokenIds.map(tokenKey));
      return tokenIds.map((tokenId, idx) => ({
        tokenId,
        record: parseTokenRecord(hashes[idx]),
      }));
    },

    /** @param {string | null} requestId @param {string} command @param {unknown} err */
    rethrowPolicy(requestId, command, err) {
      return rethrowPolicy(logAuth, requestId, command, err);
    },

    /** @param {string | null} requestId @param {number} startedAt @param {unknown} action @param {string | undefined} ns */
    beginVerify(requestId, startedAt, action, ns) {
      return beginVerify(logAuth, requestId, startedAt, action, ns);
    },

    /** @param {string | null} requestId @param {unknown} action @param {string | undefined} ns @param {unknown} err */
    recordVerifyThrow(requestId, action, ns, err) {
      logAuth("error", "auth_verify", requestId, {
        outcome: "error",
        reason: "verify_threw",
        action: typeof action === "string" ? action : undefined,
        category: actionCategory(action),
        target_ns: ns,
        ...formatError(err),
      });
    },

    /** @param {string} command @param {string | null} requestId @param {Record<string, unknown>} [fields] */
    recordLifecycleOk(command, requestId, fields = {}) {
      const event = `auth_${command}`;
      logAuth("info", event, requestId, {
        outcome: "ok",
        ...fields,
      });
    },
  };
}

/** @param {unknown} reply @returns {{ tokenId: string, record: AuthTokenRecord | null } | null} */
function parseIndexedTokenReply(reply) {
  if (!Array.isArray(reply) || reply.length === 0) {
    throw new Error("invalid auth token lookup reply");
  }
  if (reply[0] === 0) {
    if (reply.length !== 1) throw new Error("invalid auth token lookup miss reply");
    return null;
  }
  if (reply[0] !== 1 || reply.length < 2 || reply.length % 2 !== 0) {
    throw new Error("invalid auth token lookup record reply");
  }
  const tokenId = decodeBulk(reply[1]);
  if (!tokenId) throw new Error("invalid auth token lookup id");
  /** @type {Record<string, string | null | undefined>} */
  const fields = {};
  for (let i = 2; i < reply.length; i += 2) {
    const field = decodeBulk(reply[i]);
    if (!field) throw new Error("invalid auth token lookup field");
    fields[field] = decodeBulk(reply[i + 1]);
  }
  return { tokenId, record: parseTokenRecord(fields) };
}

/** @param {Record<string, unknown>} env @param {string | null} requestId @param {AuthLogger} logAuth */
async function desiredBootstrapHash(env, requestId, logAuth) {
  const token = env.BOOTSTRAP_TOKEN;
  // Fail closed: a missing infra seed is a 503 through control, never "use
  // whatever happens to be in Redis".
  if (!token || typeof token !== "string" || token.trim() === "") {
    if (!bootstrapMissingLogged) {
      bootstrapMissingLogged = true;
      logAuth("error", "auth_bootstrap", requestId, {
        outcome: "error",
        reason: "bootstrap_missing",
      });
    }
    throw new Error("BOOTSTRAP_TOKEN missing");
  }
  return await hashToken(token);
}

/** @param {Record<string, unknown>} env @param {RedisAuthReader} redis @param {string | null} requestId @param {AuthLogger} logAuth @returns {Promise<BootstrapState>} */
async function ensureBootstrapForVerify(env, redis, requestId, logAuth) {
  if (verifyBootstrapHash) {
    return { desiredHash: verifyBootstrapHash, cached: true };
  }
  const desiredHash = await desiredBootstrapHash(env, requestId, logAuth);
  await ensureBootstrapHash(redis, requestId, logAuth, desiredHash);
  return { desiredHash, cached: false };
}

/** @param {Record<string, unknown>} env @param {RedisAuthReader} redis @param {string | null} requestId @param {AuthLogger} logAuth @returns {Promise<BootstrapState>} */
async function ensureBootstrap(env, redis, requestId, logAuth) {
  const desiredHash = await desiredBootstrapHash(env, requestId, logAuth);
  await ensureBootstrapHash(redis, requestId, logAuth, desiredHash);
  return { desiredHash, cached: false };
}

/** @param {RedisAuthReader} redis @param {string | null} requestId @param {AuthLogger} logAuth @param {string} desiredHash */
async function ensureBootstrapHash(redis, requestId, logAuth, desiredHash) {
  const existingHash = await redis.hGet(tokenKey(BOOTSTRAP_TOKEN_ID), "hash");
  if (existingHash === desiredHash) {
    bootstrapMissingLogged = false;
    verifyBootstrapHash = desiredHash;
    return;
  }

  const rotatedFromExisting = Boolean(existingHash && existingHash !== desiredHash);
  /** @param {RedisAuthSession} session */
  const rotateBootstrap = async (session) => {
    // Bounded WATCH retry handles parallel auth instances during rolling
    // deploy; last-writer-wins is the documented rotation contract.
    await session.watch(tokenKey(BOOTSTRAP_TOKEN_ID));
    const oldHash = await session.hGet(tokenKey(BOOTSTRAP_TOKEN_ID), "hash");
    if (oldHash === desiredHash) {
      await session.unwatch();
      return true;
    }
    const m = session.multi();
    if (oldHash) m.del(hashKey(oldHash));
    m.hSet(tokenKey(BOOTSTRAP_TOKEN_ID), {
      hash: desiredHash,
      kind: "ops",
      created_at: new Date().toISOString(),
      created_by: BOOTSTRAP_TOKEN_ID,
      // HSET is unconditional inside MULTI; "" clears prior revoked_at and
      // parseTokenRecord coerces it back to undefined.
      revoked_at: "",
    });
    // The hash -> tokenId index lands in the same EXEC so verify never
    // observes "record updated, index not yet written".
    m.set(hashKey(desiredHash), BOOTSTRAP_TOKEN_ID);
    await m.exec();
    return true;
  };

  await withOptimisticRetries(
    async () => {
      if (typeof redis.session === "function") await redis.session(rotateBootstrap);
      else await rotateBootstrap(/** @type {RedisAuthSession} */ (redis));
      return true;
    },
    {
      attempts: 5,
      isRetryableError: (err) => err instanceof WatchError,
      onExhausted: () => {
        logAuth("error", "auth_bootstrap", requestId, {
          outcome: "error",
          reason: "bootstrap_watch_exhausted",
        });
        throw new Error("bootstrap upsert WATCH retry exhausted");
      },
    }
  );
  bootstrapMissingLogged = false;
  verifyBootstrapHash = desiredHash;
  logAuth("info", "auth_bootstrap", requestId, {
    outcome: "ok",
    token_id: BOOTSTRAP_TOKEN_ID,
    rotated: rotatedFromExisting,
  });
}

/** @param {AuthLogger} logAuth @param {string | null} requestId @param {string} command @param {unknown} err @returns {never} */
function rethrowPolicy(logAuth, requestId, command, err) {
  // Bucket thrown errors by HTTP status so 503 AuthPolicyError (contract/config
  // violation) lands in outcome=error alongside lifecycle_threw.
  const event = `auth_${command}`;
  if (err instanceof AuthPolicyError) {
    if (err.status >= 500) {
      logAuth("error", event, requestId, {
        outcome: "error",
        reason: err.reason,
        ...formatError(err),
      });
    } else {
      logAuth("warn", event, requestId, {
        outcome: "reject",
        reason: err.reason,
      });
    }
  } else {
    logAuth("error", event, requestId, {
      outcome: "error",
      reason: "lifecycle_threw",
      ...formatError(err),
    });
  }
  throw err;
}

/** @param {AuthLogger} logAuth @param {string | null} requestId @param {number} startedAt @param {unknown} action @param {string | undefined} ns */
function beginVerify(logAuth, requestId, startedAt, action, ns) {
  return {
    /** @param {VerifyFinalizeInput} input */
    finalize(input) {
      return finalizeVerify(logAuth, requestId, startedAt, action, ns, input);
    },
  };
}

/** @param {AuthLogger} logAuth @param {string | null} requestId @param {number} startedAt @param {unknown} action @param {string | undefined} ns @param {VerifyFinalizeInput} input */
function finalizeVerify(logAuth, requestId, startedAt, action, ns, input) {
  const { ret, reason, tokenId, principalKind, principalNs } = input;
  const duration = Date.now() - startedAt;
  // outcome values: ok | reject | error. error captures auth contract
  // violations; reject is policy denial; ok is allow.
  let outcome;
  if (ret.ok) outcome = "ok";
  else if (ret.status >= 500) outcome = "error";
  else outcome = "reject";
  logAuth(outcome === "ok" ? "info" : (outcome === "error" ? "error" : "warn"),
    "auth_verify", requestId, {
      outcome,
      reason,
      action: typeof action === "string" ? action : undefined,
      category: actionCategory(action),
      target_ns: ns,
      principal_kind: principalKind || "unknown",
      principal_ns: principalNs,
      token_id: tokenId,
      duration_ms: duration,
    });
  return ret;
}
