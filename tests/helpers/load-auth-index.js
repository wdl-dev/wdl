// Inline shared-redis / shared-observability mocks record commands into a
// harness-owned state object so tests can drive ensureBootstrap, WATCH
// conflicts, and expired-tombstone branches deterministically.

import {
  applyModuleReplacements,
  freshModuleDataUrl,
  freshRepositoryModuleDataUrl,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { compileSharedAuthRoles } from "./load-auth-roles.js";
import { CLOUDFLARE_WORKERS_URL } from "./mocks/cloudflare-workers.js";
import { sharedRedisStubUrl } from "./mocks/fake-redis.js";

const SHARED_NS_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_AUTH_TOKEN_URL = repositoryFileUrl("shared/auth-token.js");
const SHARED_HEX_URL = repositoryFileUrl("shared/hex.js");
const SHARED_RANDOM_ID_URL = repositoryFileUrl("shared/random-id.js");
const SHARED_REDIS_LOCK_URL = freshRepositoryModuleDataUrl("shared/redis-lock.js", [
  [/from "shared-random-id"/g, `from ${JSON.stringify(SHARED_RANDOM_ID_URL)}`],
]);
const SHARED_OBSERVABILITY_URL = repositoryFileUrl("shared/observability.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");

const SHARED_REDIS_MOCK = `
function ensureState() {
  if (!globalThis.__authMockState) {
    throw new Error("auth-index harness: globalThis.__authMockState not initialized");
  }
  return globalThis.__authMockState;
}

function recordCommand(command, ok, error_message) {
  const s = ensureState();
  const event = { command, ok, error_message, duration_ms: 0 };
  s.commands.push(event);
  if (s.onCommand) s.onCommand(event);
}

function bumpKeyVersion(key) {
  const s = ensureState();
  s.keyVersions.set(key, (s.keyVersions.get(key) || 0) + 1);
}

function expireStringIfNeeded(key) {
  const s = ensureState();
  const expiresAt = s.expirations.get(key);
  if (expiresAt == null || expiresAt > Date.now()) return;
  s.expirations.delete(key);
  if (s.strings.delete(key)) bumpKeyVersion(key);
}

function keyVersion(key) {
  expireStringIfNeeded(key);
  const s = ensureState();
  return s.keyVersions.get(key) || 0;
}

export class RedisClient {
  constructor(addr, opts = {}) {
    this.opts = opts;
    ensureState().onCommand = opts.onCommand;
  }
  async get(key) {
    const s = ensureState();
    expireStringIfNeeded(key);
    if (s.getThrows && s.getThrows.has(key)) {
      const err = new Error("forced get throw on " + key);
      recordCommand("GET", false, err.message);
      throw err;
    }
    recordCommand("GET", true);
    return s.strings.has(key) ? s.strings.get(key) : null;
  }
  async hGet(key, field) {
    const s = ensureState();
    recordCommand("HGET", true);
    const h = s.hashes.get(key);
    return h ? (h[field] ?? null) : null;
  }
  async hGetAll(key) {
    const s = ensureState();
    if (s.hGetAllThrows && s.hGetAllThrows.has(key)) {
      const err = new Error("forced hGetAll throw on " + key);
      recordCommand("HGETALL", false, err.message);
      throw err;
    }
    recordCommand("HGETALL", true);
    const h = s.hashes.get(key);
    return h ? { ...h } : {};
  }
  async hGetAllMany(keys) {
    const s = ensureState();
    recordCommand("HGETALL_PIPELINE", true);
    return keys.map((key) => {
      const h = s.hashes.get(key);
      return h ? { ...h } : {};
    });
  }
  async eval(script, keys = [], args = []) {
    const s = ensureState();
    const indexKey = keys[0];
    if (s.evalThrows && s.evalThrows.has(indexKey)) {
      const err = new Error("forced eval throw on " + indexKey);
      recordCommand("EVAL", false, err.message);
      throw err;
    }
    s.evalCalls.push([script, [...keys], [...args]]);
    recordCommand("EVAL", true);
    const tokenId = s.strings.get(indexKey);
    if (!tokenId) return [0];
    const record = s.hashes.get(String(args[0] || "") + tokenId) || {};
    return [1, tokenId, ...Object.entries(record).flat()];
  }
  async del(key) {
    const s = ensureState();
    expireStringIfNeeded(key);
    recordCommand("DEL", true);
    const had = s.strings.delete(key) || s.hashes.delete(key);
    s.expirations.delete(key);
    if (had) bumpKeyVersion(key);
    return had ? 1 : 0;
  }
  async delIfEq(key, value) {
    const s = ensureState();
    expireStringIfNeeded(key);
    if (s.delIfEqThrows && s.delIfEqThrows.has(key)) {
      const err = new Error("forced delIfEq throw on " + key);
      recordCommand("DELIFEQ", false, err.message);
      throw err;
    }
    recordCommand("DELIFEQ", true);
    if (s.strings.get(key) !== value) return 0;
    s.strings.delete(key);
    s.expirations.delete(key);
    bumpKeyVersion(key);
    return 1;
  }
  async set(key, value, options = {}) {
    const s = ensureState();
    expireStringIfNeeded(key);
    if (options.nx && s.strings.has(key)) {
      recordCommand("SET", true);
      return null;
    }
    if (options.ifeq != null && s.strings.get(key) !== options.ifeq) {
      recordCommand("SET", true);
      return null;
    }
    s.strings.set(key, value);
    if (typeof options.ttl === "number") {
      s.expirations.set(key, Date.now() + options.ttl * 1000);
    } else {
      s.expirations.delete(key);
    }
    bumpKeyVersion(key);
    try {
      if (s.afterSetAppliedBeforeReply) s.afterSetAppliedBeforeReply(key, value, options);
    } catch (err) {
      recordCommand("SET", false, err instanceof Error ? err.message : String(err));
      throw err;
    }
    recordCommand("SET", true);
    return "OK";
  }
  async scan(_cursor, pattern, _count) {
    const s = ensureState();
    recordCommand("SCAN", true);
    if (s.scanPages && s.scanPages.length) {
      const page = s.scanPages.shift();
      return [page.next, page.keys];
    }
    const prefix = String(pattern || "").replace(/\\*$/, "");
    return ["0", [...s.hashes.keys()].filter((key) => key.startsWith(prefix))];
  }
  async sIsMember(key, member) {
    const s = ensureState();
    recordCommand("SISMEMBER", true);
    const set = s.sets.get(key);
    return set ? set.has(member) : false;
  }
  async multiExec(cmds, watched = new Map()) {
    const s = ensureState();
    if (s.beforeMultiExec) s.beforeMultiExec(cmds);
    for (const [key, version] of watched) {
      if (keyVersion(key) !== version) {
        recordCommand("MULTI_EXEC", false, "watch invalidation");
        throw new WatchError("watch invalidation");
      }
    }
    const out = [];
    for (const cmd of cmds) {
      const [op, key, ...rest] = cmd;
      if (op === "DEL") {
        expireStringIfNeeded(key);
        const had = s.strings.delete(key) || s.hashes.delete(key);
        s.expirations.delete(key);
        if (had) bumpKeyVersion(key);
        out.push(had ? 1 : 0);
        recordCommand("DEL", true);
      } else if (op === "HSET") {
        const h = s.hashes.get(key) || {};
        for (let i = 0; i < rest.length; i += 2) {
          h[rest[i]] = rest[i + 1];
        }
        s.hashes.set(key, h);
        bumpKeyVersion(key);
        out.push(rest.length / 2);
        recordCommand("HSET", true);
      } else if (op === "SET") {
        expireStringIfNeeded(key);
        const options = rest.slice(1).map((arg) => String(arg));
        const upperOptions = options.map((arg) => arg.toUpperCase());
        if (upperOptions.includes("NX") && s.strings.has(key)) {
          out.push(null);
        } else {
          s.strings.set(key, rest[0]);
          const exIndex = upperOptions.indexOf("EX");
          if (exIndex >= 0) {
            s.expirations.set(key, Date.now() + Number(options[exIndex + 1]) * 1000);
          } else {
            s.expirations.delete(key);
          }
          bumpKeyVersion(key);
          out.push("OK");
        }
        recordCommand("SET", true);
      } else {
        throw new Error("multiExec mock: unsupported " + op);
      }
    }
    try {
      if (s.afterMultiExecAppliedBeforeReply) s.afterMultiExecAppliedBeforeReply(cmds);
    } catch (err) {
      recordCommand("MULTI_EXEC", false, err instanceof Error ? err.message : String(err));
      throw err;
    }
    return out;
  }
  async session(fn) {
    ensureState().sessions += 1;
    const self = this;
    const watched = new Map();
    const session = {
      async watch(...keys) {
        for (const key of keys) watched.set(key, keyVersion(key));
      },
      async unwatch() { watched.clear(); },
      async get(key) { return self.get(key); },
      async del(key) { return self.del(key); },
      async delIfEq(key, value) { return self.delIfEq(key, value); },
      async set(key, value, options) { return self.set(key, value, options); },
      async hGet(key, field) { return self.hGet(key, field); },
      async hGetAll(key) { return self.hGetAll(key); },
      async hGetAllMany(keys) { return self.hGetAllMany(keys); },
      async scan(cursor, pattern, count) { return self.scan(cursor, pattern, count); },
      async sIsMember(key, member) { return self.sIsMember(key, member); },
      multi() {
        const ops = [];
        const m = {
          del(key) { ops.push(["DEL", key]); return m; },
          hSet(key, fields) {
            const args = ["HSET", key];
            for (const [f, v] of Object.entries(fields)) {
              args.push(f, String(v));
            }
            ops.push(args);
            return m;
          },
          set(key, val, opts = {}) {
            const args = ["SET", key, val];
            if (opts.ttl !== undefined && opts.ttl !== null) args.push("EX", String(opts.ttl));
            if (opts.nx) args.push("NX");
            ops.push(args);
            return m;
          },
          async exec() {
            try {
              return await self.multiExec(ops, watched);
            } finally {
              watched.clear();
            }
          },
        };
        return m;
      },
    };
    return fn(session);
  }
}
`;

const SHARED_OBSERVABILITY_MOCK = String.raw`
export {
  formatError,
  recordRedisCommand,
  sanitizeRequestId,
} from ${JSON.stringify(SHARED_OBSERVABILITY_URL)};

export function createLogger(service) {
  return function (level, event, fields) {
    globalThis.__authMockState.logs.push({ level, event, service, ...fields });
  };
}

export function createLogLevelBinder() {
  return function bindLogLevel(_env) {};
}

`;

/**
 * @param {{
 *   rolesPatch?: Record<string, unknown>,
 *   authLibReplacements?: Array<[RegExp, string]>,
 *   delegatedTemplatePatch?: {
 *     templateId?: string,
 *     activeQuota?: number,
 *     ttlSeconds?: number,
 *     randomHexBytes?: number,
 *   },
 * }} [opts]
 * @returns {Promise<{ AuthClass: any, sharedAuthRoles: any, authLib: any }>}
 */
export async function loadAuthIndex(opts = {}) {
  const { sharedAuthRolesUrl, sharedAuthRoles } = await compileSharedAuthRoles(opts);
  /** @type {Array<[RegExp | string, string]>} */
  const authLibReplacements = [
    [/from "shared-ns-pattern"/g, `from ${JSON.stringify(SHARED_NS_URL)}`],
    [/from "shared-auth-token"/g, `from ${JSON.stringify(SHARED_AUTH_TOKEN_URL)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-hex"/g, `from ${JSON.stringify(SHARED_HEX_URL)}`],
    [/from "shared-random-id"/g, `from ${JSON.stringify(SHARED_RANDOM_ID_URL)}`],
    ...(opts.authLibReplacements || []),
  ];
  let authLibSource = readRepositoryFile("auth/lib.js");
  for (const [pattern, replacement] of authLibReplacements) {
    const replaced = applyModuleReplacements(authLibSource, [[pattern, replacement]]);
    if (replaced === authLibSource) {
      throw new Error(`auth-index harness replacement did not match: ${String(pattern)}`);
    }
    authLibSource = replaced;
  }
  const authLibBaseUrl = freshModuleDataUrl(authLibSource);
  const authLibUrl = opts.delegatedTemplatePatch
    ? freshModuleDataUrl(`
import * as base from ${JSON.stringify(authLibBaseUrl)};
export * from ${JSON.stringify(authLibBaseUrl)};

const PATCH = ${JSON.stringify(opts.delegatedTemplatePatch)};
function patchTemplate(template) {
  if (template.id !== (PATCH.templateId || "wdl-chat-ns-pool")) return template;
  return {
    ...template,
    ...(PATCH.activeQuota === undefined ? {} : { activeQuota: PATCH.activeQuota }),
    ...(PATCH.ttlSeconds === undefined ? {} : { ttlSeconds: PATCH.ttlSeconds }),
    nsGenerator: {
      ...template.nsGenerator,
      ...(PATCH.randomHexBytes === undefined ? {} : { randomHexBytes: PATCH.randomHexBytes }),
    },
  };
}

export const DELEGATED_ISSUE_TEMPLATES = Object.freeze(
  base.DELEGATED_ISSUE_TEMPLATES.map((template) => Object.freeze(patchTemplate(template)))
);
export function createDelegatedIssueTemplateMap(templates = DELEGATED_ISSUE_TEMPLATES) {
  return base.createDelegatedIssueTemplateMap(templates);
}
export function resolveDelegatedIssueTemplate(templateId, configured = createDelegatedIssueTemplateMap()) {
  return base.resolveDelegatedIssueTemplate(templateId, configured);
}
`)
    : authLibBaseUrl;
  const authLib = await import(authLibUrl);

  // Mocks have no module-level state; cache them across loads.
  const sharedRedisUrl = sharedRedisStubUrl(SHARED_REDIS_MOCK);
  const sharedObservabilityUrl = moduleDataUrl(SHARED_OBSERVABILITY_MOCK);

  const authRuntimeUrl = freshRepositoryModuleDataUrl("auth/runtime.js", [
    [/from "shared-redis"/g, `from ${JSON.stringify(sharedRedisUrl)}`],
    [/from "shared-observability"/g, `from ${JSON.stringify(sharedObservabilityUrl)}`],
    [/from "auth-lib"/g, `from ${JSON.stringify(authLibUrl)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-optimistic-retry"/g, `from ${JSON.stringify(repositoryFileUrl("shared/optimistic-retry.js"))}`],
  ]);

  const indexUrl = freshRepositoryModuleDataUrl("auth/index.js", [
    [/from "cloudflare:workers"/g, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)}`],
    [/from "auth-lib"/g, `from ${JSON.stringify(authLibUrl)}`],
    [/from "auth-runtime"/g, `from ${JSON.stringify(authRuntimeUrl)}`],
    [/from "shared-auth-roles"/g, `from ${JSON.stringify(sharedAuthRolesUrl)}`],
    [/from "shared-redis-lock"/g, `from ${JSON.stringify(SHARED_REDIS_LOCK_URL)}`],
    [/from "shared-worker-contract"/g, `from ${JSON.stringify(WORKER_CONTRACT_URL)}`],
  ]);
  const indexMod = await import(indexUrl);

  return {
    AuthClass: indexMod.default,
    sharedAuthRoles,
    authLib,
  };
}

export function resetAuthMockState() {
  /** @type {any} */ (globalThis).__authMockState = {
    strings: new Map(),
    expirations: new Map(),
    hashes: new Map(),
    sets: new Map(),
    getThrows: new Set(),
    evalThrows: new Set(),
    evalCalls: [],
    hGetAllThrows: new Set(),
    delIfEqThrows: new Set(),
    scanPages: [],
    keyVersions: new Map(),
    afterSetAppliedBeforeReply: null,
    afterMultiExecAppliedBeforeReply: null,
    beforeMultiExec: null,
    onCommand: null,
    commands: [],
    sessions: 0,
    logs: [],
  };
  return authMockState();
}

export function authMockState() {
  if (!/** @type {any} */ (globalThis).__authMockState) {
    throw new Error("auth-index harness state is not initialized");
  }
  return /** @type {any} */ (globalThis).__authMockState;
}

/** @param {string} event */
export function authLogs(event) {
  return authMockState().logs.filter((/** @type {any} */ entry) => entry.event === event);
}

/** @param {string} event */
export function lastAuthLog(event) {
  return authLogs(event).at(-1);
}

// Pre-populate the bootstrap record so ensureBootstrap's fast path returns
// immediately; otherwise every call would exercise the WATCH/MULTI/EXEC
// branch and clutter the test's state assertions.
/**
 * @param {{ hashToken: (token: string) => Promise<string> }} authLib
 * @param {{ BOOTSTRAP_TOKEN: string }} env
 */
export async function seedBootstrap(authLib, env) {
  const desiredHash = await authLib.hashToken(env.BOOTSTRAP_TOKEN);
  const state = authMockState();
  state.hashes.set("auth:token:bootstrap", {
    hash: desiredHash,
    kind: "ops",
    created_at: "2026-04-25T00:00:00.000Z",
    created_by: "bootstrap",
  });
  state.strings.set(`auth:hash:${desiredHash}`, "bootstrap");
}
