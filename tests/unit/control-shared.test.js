// Pure-function tests for control/shared.js exports. shared.js carries
// lazily-inited singletons (redis/s3/r2/log) that don't matter for
// pure helpers; bare specifiers are stubbed via data: URLs so the file
// imports without standing up any real client.

import { test } from "node:test";
import assert from "node:assert/strict";
import { moduleDataUrl, readRepositoryJson } from "../helpers/load-shared-module.js";
import { compileControlSharedGraph } from "../helpers/load-control-shared.js";
import {
  createFakeRedis,
  createFakeRedisState,
  sharedRedisStubUrl,
} from "../helpers/mocks/fake-redis.js";
import { installMockProperty, withMockedProperty } from "../helpers/mock-global.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

const sharedRedisUrl = sharedRedisStubUrl(`
  export class RedisClient {}
  export function redisDbFromEnv() { return 0; }
`);
const controlS3Url = moduleDataUrl(`export function makeS3Client() { return null; }`);
const controlR2Url = moduleDataUrl(`export function makeR2AdminClient() { return null; }`);
const sharedAuthTokenUrl = moduleDataUrl(`export function extractToken() { return null; }`);
const sharedAuthRolesUrl = moduleDataUrl(`export function validatePrincipalShape() { return false; }`);
const sharedQueueKeysUrl = moduleDataUrl(`export function queueStreamKey() { return ""; }`);
const { controlSharedUrl, controlWorkflowsClientUrl } = compileControlSharedGraph({
  sharedRedisUrl,
  controlS3Url,
  controlR2Url,
  sharedAuthTokenUrl,
  sharedAuthRolesUrl,
  sharedQueueKeysUrl,
});
const {
  WORKFLOWS_INTERNAL_TIMEOUT_MS,
  MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES,
  WORKFLOW_LIFECYCLE_MAX_PAGES,
  WORKFLOW_LIFECYCLE_TIMEOUT_MS,
  WORKFLOW_LIFECYCLE_RESPONSE_MAX_BYTES,
  postWorkflowsInternalRequest,
  readWorkflowInstancesResponse,
  readWorkflowLifecycleResponse,
} = await import(controlWorkflowsClientUrl);
// Use the same stub constructor imported by control/shared.js so
// runOptimistic's `instanceof WatchError` check observes the test error.
const { WatchError: ControlSharedWatchError } = await import(sharedRedisUrl);

const {
  authErrorBody,
  authPolicyResponse,
  acquireDeleteLock,
  assertWorkflowDeleteAllowed,
  cleanupDoAlarmsForWorker,
  codedErrorLogFields,
  codedErrorResponse,
  ControlAbort,
  controlAbortResponse,
  jsonError,
  readJsonBody,
  rebuildDeclaredHostIndexes,
  releaseDeleteLock,
  runOptimistic,
  secretEnvelopeErrorResponse,
  state,
} = await import(controlSharedUrl);

/**
 * @param {import("node:test").TestContext} t
 */
function restoreControlSharedStateAfter(t) {
  const previous = {
    redis: state.redis,
    env: state.env,
    log: state.log,
    workflows: state.workflows,
  };
  t.after(() => {
    state.redis = previous.redis;
    state.env = previous.env;
    state.log = previous.log;
    state.workflows = previous.workflows;
  });
}

// Plain-object stand-in for AuthPolicyError — the body shape function only
// reads .status / .reason / .message off the error.
/**
 * @param {number} status
 * @param {string | undefined} reason
 * @param {string} message
 * @param {Record<string, unknown>} [details]
 */
function policyError(status, reason, message, details) {
  const err = new Error(message);
  /** @type {any} */ (err).status = status;
  /** @type {any} */ (err).reason = reason;
  if (details) /** @type {any} */ (err).details = details;
  return err;
}

test("authErrorBody: 4xx ships {error: reason, message}", () => {
  const out = authErrorBody(policyError(400, "invalid_label", "label too long"));
  assert.deepEqual(out, {
    status: 400,
    body: { error: "invalid_label", message: "label too long" },
  });
});

test("authErrorBody: 4xx details are additive and cannot override top-level fields", () => {
  const out = authErrorBody(policyError(409, "active_quota_exceeded", "active token quota exceeded", {
    active: 100,
    quota: 100,
    available: 0,
    error: "wrong",
    message: "wrong",
    reason: "wrong",
  }));
  assert.deepEqual(out, {
    status: 409,
    body: {
      active: 100,
      quota: 100,
      available: 0,
      error: "active_quota_exceeded",
      message: "active token quota exceeded",
    },
  });
});

test("authErrorBody: 5xx with reason ships machine error and generic message", () => {
  const out = authErrorBody(policyError(503, "invalid_role_config",
    "role X has invalid boundNsKind 'bogus'"));
  assert.equal(out.status, 503);
  assert.equal(out.body.error, "invalid_role_config");
  assert.equal(out.body.message, "auth error");
  // 5xx err.message is internal diagnostics, must NOT leak to wire.
  assert.ok(!String(out.body.message).includes("boundNsKind"),
    `body.message leaked err.message: ${out.body.message}`);
  assert.ok(!String(out.body.message).includes("bogus"));
});

test("authErrorBody: generic Error → {status:503, error:'auth_unavailable'}", () => {
  const out = authErrorBody(new Error("redis explosion"));
  assert.deepEqual(out, {
    status: 503,
    body: { error: "auth_unavailable", message: "auth unavailable" },
  });
});

test("authErrorBody: 5xx without reason → generic 'auth unavailable'", () => {
  const err = policyError(500, undefined, "x");
  const out = authErrorBody(err);
  assert.equal(out.status, 503);
  assert.equal(out.body.error, "auth_unavailable");
  assert.equal(out.body.message, "auth unavailable");
});

test("authPolicyResponse logs reason from the error, not the wire body", async (t) => {
  restoreControlSharedStateAfter(t);
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const logs = [];
  state.log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    logs.push({ level, event, fields });
  const res = authPolicyResponse(policyError(400, "invalid_label", "label too long"), "req-1", "issue");
  await assertJsonResponse(res, 400, {
    error: "invalid_label",
    message: "label too long",
  });
  assert.equal(logs[0].fields.reason, "invalid_label");
});

test("jsonError prevents details from overriding error or message", async () => {
  const res = jsonError(400, "invalid_request", "bad body", {
    error: "wrong",
    message: "wrong message",
    reason: "old_reason",
    field: "x",
  });
  await assertJsonResponse(res, 400, {
    field: "x",
    error: "invalid_request",
    message: "bad body",
  });
});

test("runOptimistic retries WatchError and succeeds on a later attempt", async () => {
  let sessions = 0;
  const redis = {
    async session(/** @type {(session: { id: number }) => Promise<string>} */ fn) {
      sessions += 1;
      const result = await fn({ id: sessions });
      if (sessions === 1) throw new ControlSharedWatchError("watched key changed");
      return result;
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 3,
    onWatchError: () => {},
    onExhausted: () => "exhausted",
  }, async (/** @type {{ id: number }} */ session) => `session-${session.id}`);

  assert.equal(result, "session-2");
});

test("runOptimistic reports watch retry attempts via onWatchError", async () => {
  /** @type {number[]} */
  const watchAttempts = [];
  let sessions = 0;
  const redis = {
    async session(/** @type {(session: { id: number }) => Promise<string>} */ fn) {
      sessions += 1;
      const result = await fn({ id: sessions });
      if (sessions === 1) throw new ControlSharedWatchError("watched key changed");
      return result;
    },
  };

  await runOptimistic(redis, {
    attempts: 3,
    onWatchError: (/** @type {unknown} */ _err, /** @type {number} */ attempt) => watchAttempts.push(attempt),
    onExhausted: () => "exhausted",
  }, async (/** @type {{ id: number }} */ session) => `session-${session.id}`);

  assert.deepEqual(watchAttempts, [0]);
});

test("runOptimistic passes callback attempt number on success", async () => {
  /** @type {number[]} */
  const callbackAttempts = [];
  let sessions = 0;
  const redis = {
    async session(/** @type {(session: { id: number }) => Promise<string>} */ fn) {
      sessions += 1;
      const result = await fn({ id: sessions });
      if (sessions === 1) throw new ControlSharedWatchError("watched key changed");
      return result;
    },
  };

  await runOptimistic(redis, {
    attempts: 3,
    onWatchError: () => {},
    onExhausted: () => "exhausted",
  }, async (/** @type {{ id: number }} */ session, /** @type {number} */ attempt) => {
    callbackAttempts.push(attempt);
    return `session-${session.id}`;
  });

  assert.deepEqual(callbackAttempts, [0, 1]);
});

test("runOptimistic passes callback attempt=0 when successful on first try", async () => {
  /** @type {number[]} */
  const callbackAttempts = [];
  let sessions = 0;
  const redis = {
    async session(/** @type {(session: { id: number }) => Promise<string>} */ fn) {
      sessions += 1;
      return await fn({ id: sessions });
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 3,
    onExhausted: () => "exhausted",
  }, async (/** @type {{ id: number }} */ session, /** @type {number} */ attempt) => {
    callbackAttempts.push(attempt);
    return `session-${session.id}`;
  });

  assert.equal(result, "session-1");
  assert.deepEqual(callbackAttempts, [0]);
});

test("runOptimistic handles missing onWatchError callback", async () => {
  /** @type {number[]} */
  const callbackAttempts = [];
  let sessions = 0;
  const redis = {
    async session(/** @type {(session: { id: number }) => Promise<string>} */ fn) {
      sessions += 1;
      if (sessions === 1) throw new ControlSharedWatchError("watched key changed");
      return await fn({ id: sessions });
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 3,
    onExhausted: () => "exhausted",
  }, async (/** @type {{ id: number }} */ session, /** @type {number} */ attempt) => {
    callbackAttempts.push(attempt);
    return `session-${session.id}`;
  });

  assert.equal(result, "session-2");
  assert.deepEqual(callbackAttempts, [1]);
});

test("runOptimistic calls onExhausted after WatchError retries are exhausted", async () => {
  /** @type {number[]} */
  const watchAttempts = [];
  let exhaustedCalls = 0;
  const redis = {
    async session(/** @type {(session: {}) => Promise<unknown>} */ _fn) {
      throw new ControlSharedWatchError("watched key changed");
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 2,
    onWatchError: (/** @type {unknown} */ _err, /** @type {number} */ attempt) => watchAttempts.push(attempt),
    onExhausted: () => {
      exhaustedCalls += 1;
      return "exhausted";
    },
  }, async () => "ok");

  assert.deepEqual(watchAttempts, [0, 1]);
  assert.equal(exhaustedCalls, 1);
  assert.equal(result, "exhausted");
});

test("runOptimistic lets results request retry before exhausted fallback", async () => {
  /** @type {number[]} */
  const attempts = [];
  /** @type {number[]} */
  const retryChecks = [];
  const redis = {
    async session(/** @type {(session: {}) => Promise<{ retry: boolean, attempt: number }>} */ fn) {
      return await fn({});
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 2,
    onExhausted: () => ({ retry: false, attempt: -1 }),
    shouldRetryResult: (/** @type {{ retry: boolean }} */ out, /** @type {number} */ attempt) => {
      retryChecks.push(attempt);
      return out.retry;
    },
  }, async (/** @type {unknown} */ _session, /** @type {number} */ attempt) => {
    attempts.push(attempt);
    return { retry: true, attempt };
  });

  assert.deepEqual(attempts, [0, 1]);
  assert.deepEqual(retryChecks, [0, 1]);
  assert.deepEqual(result, { retry: false, attempt: -1 });
});

test("runOptimistic returns a result when shouldRetryResult does not request retry", async () => {
  /** @type {number[]} */
  const attempts = [];
  /** @type {number[]} */
  const retryChecks = [];
  let exhausted = false;
  const redis = {
    async session(/** @type {(session: {}) => Promise<{ retry: boolean, attempt: number }>} */ fn) {
      return await fn({});
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 3,
    onExhausted: () => {
      exhausted = true;
      return { retry: false, attempt: -1 };
    },
    shouldRetryResult: (/** @type {{ retry: boolean }} */ out, /** @type {number} */ attempt) => {
      retryChecks.push(attempt);
      return out.retry;
    },
  }, async (/** @type {unknown} */ _session, /** @type {number} */ attempt) => {
    attempts.push(attempt);
    return { retry: false, attempt };
  });

  assert.deepEqual(attempts, [0]);
  assert.deepEqual(retryChecks, [0]);
  assert.equal(exhausted, false);
  assert.deepEqual(result, { retry: false, attempt: 0 });
});

test("runOptimistic stops retrying when shouldRetryResult eventually returns false", async () => {
  /** @type {number[]} */
  const attempts = [];
  /** @type {number[]} */
  const retryChecks = [];
  let exhausted = false;
  const redis = {
    async session(/** @type {(session: {}) => Promise<{ retry: boolean, attempt: number }>} */ fn) {
      return await fn({});
    },
  };

  const result = await runOptimistic(redis, {
    attempts: 4,
    onExhausted: () => {
      exhausted = true;
      return { retry: false, attempt: -1 };
    },
    shouldRetryResult: (/** @type {{ retry: boolean }} */ out, /** @type {number} */ attempt) => {
      retryChecks.push(attempt);
      return out.retry;
    },
  }, async (/** @type {unknown} */ _session, /** @type {number} */ attempt) => {
    attempts.push(attempt);
    return { retry: attempt < 1, attempt };
  });

  assert.deepEqual(attempts, [0, 1]);
  assert.deepEqual(retryChecks, [0, 1]);
  assert.equal(exhausted, false);
  assert.deepEqual(result, { retry: false, attempt: 1 });
});

test("runOptimistic rethrows non-WatchError failures", async () => {
  const failure = new Error("not a watch conflict");
  const redis = {
    async session() {
      throw failure;
    },
  };

  await assert.rejects(
    () => runOptimistic(redis, {
      attempts: 3,
      onExhausted: () => "exhausted",
    }, async () => "unreachable"),
    failure
  );
});

test("rebuildDeclaredHostIndexes rebuilds global host declaration gate from namespace sets", async () => {
  const redis = createFakeRedis();
  redis.state.sets.set("hosts:alpha", new Set(["app.workers.example", "shared.workers.example"]));
  redis.state.sets.set("hosts:beta", new Set(["shared.workers.example"]));
  redis.state.sets.set("declared-hosts", new Set(["stale.workers.example"]));
  redis.state.sets.set("host-declarations:stale.workers.example", new Set(["old"]));

  const result = await rebuildDeclaredHostIndexes(redis);

  assert.deepEqual(result, {
    declaredHosts: 2,
    declarationKeysRemoved: 1,
  });
  assert.deepEqual(
    redis.state.commands.filter(([command]) => command === "sCardMany"),
    [["sCardMany", ["hosts:alpha", "hosts:beta"]]]
  );
  assert.deepEqual(
    redis.state.commands.filter(([command]) => command === "sMembersMany"),
    [["sMembersMany", ["hosts:alpha", "hosts:beta"]]]
  );
  assert.ok(redis.state.watchBatches.some((keys) => keys.includes("declared-hosts:revision")));
  assert.deepEqual(redis.state.sets.get("declared-hosts"), new Set(["app.workers.example", "shared.workers.example"]));
  assert.deepEqual(redis.state.sets.get("host-declarations:app.workers.example"), new Set(["alpha"]));
  assert.deepEqual(redis.state.sets.get("host-declarations:shared.workers.example"), new Set(["alpha", "beta"]));
  assert.equal(redis.state.sets.has("host-declarations:stale.workers.example"), false);
});

test("rebuildDeclaredHostIndexes bounds cardinality and member-read pipelines", async () => {
  const redis = createFakeRedis();
  for (let index = 0; index < 65; index += 1) {
    redis.state.sets.set(
      `hosts:tenant-${index}`,
      new Set([`host-${index}.workers.example`])
    );
  }

  const result = await rebuildDeclaredHostIndexes(redis);

  assert.equal(result.declaredHosts, 65);
  assert.deepEqual(
    redis.state.commands
      .filter(([command]) => command === "sCardMany")
      .map(([, keys]) => /** @type {string[]} */ (keys).length),
    [64, 1]
  );
  assert.deepEqual(
    redis.state.commands
      .filter(([command]) => command === "sMembersMany")
      .map(([, keys]) => /** @type {string[]} */ (keys).length),
    [64, 1]
  );
});

test("rebuildDeclaredHostIndexes retries from the current source after revision drift", async () => {
  const state = createFakeRedisState();
  state.sets.set("hosts:alpha", new Set(["alpha.workers.example"]));
  const redis = createFakeRedis(state);
  const session = redis.session.bind(redis);
  let injectedDrift = false;
  redis.session = async (fn) => await session(async (iso) => {
    const sMembersMany = iso.sMembersMany.bind(iso);
    iso.sMembersMany = async (keys) => {
      const members = await sMembersMany(keys);
      if (!injectedDrift) {
        injectedDrift = true;
        // Model the ordinary writer committing a source update and its revision
        // after reload captured the old source snapshot.
        state.sets.set("hosts:beta", new Set(["beta.workers.example"]));
        await redis.incr("declared-hosts:revision");
      }
      return members;
    };
    return await fn(iso);
  });

  const result = await rebuildDeclaredHostIndexes(redis);

  assert.deepEqual(result, { declaredHosts: 2, declarationKeysRemoved: 0 });
  assert.deepEqual(
    state.sets.get("declared-hosts"),
    new Set(["alpha.workers.example", "beta.workers.example"])
  );
  assert.equal(
    state.watchBatches.filter((keys) => keys.includes("declared-hosts:revision")).length,
    2
  );
});

test("rebuildDeclaredHostIndexes deduplicates SCAN results before counting and reading", async () => {
  const state = createFakeRedisState();
  state.sets.set("hosts:alpha", new Set(["app.workers.example"]));
  state.sets.set("host-declarations:app.workers.example", new Set(["alpha"]));
  const redis = createFakeRedis(state);
  const session = redis.session.bind(redis);
  redis.session = async (fn) => await session(async (iso) => {
    const scan = iso.scan.bind(iso);
    iso.scan = async (cursor, pattern, count) => {
      const [next, keys] = await scan(cursor, pattern, count);
      return [next, [...keys, ...keys]];
    };
    return await fn(iso);
  });

  const result = await rebuildDeclaredHostIndexes(redis);

  assert.deepEqual(result, { declaredHosts: 1, declarationKeysRemoved: 1 });
  assert.deepEqual(
    state.commands.filter(([command]) => command === "sMembersMany"),
    [["sMembersMany", ["hosts:alpha"]]]
  );
});

test("rebuildDeclaredHostIndexes rejects oversized scans before reading members or mutating", async () => {
  const hostKeys = Array.from(
    { length: 5_001 },
    (_, index) => `hosts:tenant-${index}`
  );
  const oldDeclarationKeys = Array.from(
    { length: 5_000 },
    (_, index) => `host-declarations:host-${index}.workers.example`
  );
  let scanCalls = 0;
  let cardinalityReadCalls = 0;
  let memberReadCalls = 0;
  let multiCalls = 0;
  const session = {
    async watch() {},
    /** @param {string} cursor @param {string} pattern */
    async scan(cursor, pattern) {
      scanCalls += 1;
      if (cursor !== "0") throw new Error("scan continued after exceeding the entry budget");
      return [
        pattern === "host-declarations:*" ? "next" : "0",
        pattern === "host-declarations:*" ? oldDeclarationKeys : hostKeys,
      ];
    },
    async sCardMany() { cardinalityReadCalls += 1; return []; },
    async sMembersMany() { memberReadCalls += 1; return []; },
    multi() {
      multiCalls += 1;
      throw new Error("must fail before building a transaction");
    },
  };
  const redis = {
    /** @param {(value: typeof session) => Promise<unknown>} fn */
    async session(fn) { return await fn(session); },
  };

  await assert.rejects(
    rebuildDeclaredHostIndexes(redis),
    /declared host rebuild exceeds 10000 entries/
  );
  assert.equal(scanCalls, 2);
  assert.equal(cardinalityReadCalls, 0);
  assert.equal(memberReadCalls, 0);
  assert.equal(multiCalls, 0);
});

test("rebuildDeclaredHostIndexes rejects oversized declarations before reading members or mutating", async () => {
  const redis = createFakeRedis();
  redis.state.sets.set(
    "hosts:tenant",
    new Set(Array.from({ length: 10_000 }, (_, index) => `host-${index}.workers.example`))
  );

  await assert.rejects(
    rebuildDeclaredHostIndexes(redis),
    /declared host rebuild exceeds 10000 entries/
  );
  assert.deepEqual(
    redis.state.commands.filter(([command]) => command === "sCardMany"),
    [["sCardMany", ["hosts:tenant"]]]
  );
  assert.deepEqual(
    redis.state.commands.filter(([command]) => command === "sMembersMany"),
    []
  );
  assert.deepEqual(redis.state.ops, []);
});

test("controlAbortResponse keeps abort errors on the shared response path", async () => {
  const err = new ControlAbort(409, "version_referenced", {
      message: "Version is still referenced",
      blockerCount: 2,
      dryRun: false,
      error: "wrong",
      reason: "wrong",
  });
  const res = controlAbortResponse(err, { dryRun: true });
  await assertJsonResponse(res, 409, {
    dryRun: true,
    blockerCount: 2,
    error: "version_referenced",
    message: "Version is still referenced",
  });
});

test("assertWorkflowDeleteAllowed fails malformed successful workflow response as internal error", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  state.workflows = {
    async fetch() {
      return new Response("not json", { status: 200 });
    },
  };

  await assert.rejects(
    () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api" }),
    (err) => {
      assert.ok(err instanceof ControlAbort);
      const abort = /** @type {InstanceType<typeof ControlAbort>} */ (err);
      assert.equal(abort.status, 503);
      assert.equal(abort.code, "workflow_internal_dispatch_failed");
      assert.equal(abort.details.message, "Workflow lifecycle check failed");
      return true;
    }
  );
});

test("assertWorkflowDeleteAllowed hides transport diagnostics from response details", async (t) => {
  restoreControlSharedStateAfter(t);
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const logs = [];
  state.log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    logs.push({ level, event, fields });
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  state.workflows = {
    async fetch() {
      throw new Error("connect ECONNREFUSED workflows");
    },
  };

  await assert.rejects(
    () => assertWorkflowDeleteAllowed({
      ns: "demo",
      worker: "api",
      version: "v2",
      requestId: "rid-lifecycle-failure",
    }),
    (err) => {
      assert.ok(err instanceof ControlAbort);
      const abort = /** @type {InstanceType<typeof ControlAbort>} */ (err);
      assert.equal(abort.status, 503);
      assert.equal(abort.code, "workflow_internal_dispatch_failed");
      assert.equal(abort.details.message, "Workflow lifecycle check failed");
      assert.equal(Object.hasOwn(abort.details, "error_message"), false);
      return true;
    }
  );
  assert.deepEqual(logs.at(-1), {
    level: "error",
    event: "workflow_lifecycle_check_failed",
    fields: {
      namespace: "demo",
      worker: "api",
      version: "v2",
      request_id: "rid-lifecycle-failure",
      error_message: "connect ECONNREFUSED workflows",
    },
  });
});

test("assertWorkflowDeleteAllowed preserves active workflow blockers", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  state.workflows = {
    async fetch() {
      return new Response(JSON.stringify({
        allowed: false,
        count: 1,
        blockers: [{ workflowKey: "wf_1", instanceId: "inst-1" }],
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  };

  await assert.rejects(
    () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", version: "v2" }),
    (err) => {
      assert.ok(err instanceof ControlAbort);
      const abort = /** @type {InstanceType<typeof ControlAbort>} */ (err);
      assert.equal(abort.status, 409);
      assert.equal(abort.code, "workflow_instances_active");
      assert.equal(abort.details.count, 1);
      assert.deepEqual(abort.details.blockers, [{ workflowKey: "wf_1", instanceId: "inst-1" }]);
      assert.equal(abort.details.version, "v2");
      return true;
    }
  );
});

test("shared workflows calls preserve endpoint-specific timeout behavior", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  /** @type {number[]} */
  const timeoutMs = [];
  /** @type {AbortSignal[]} */
  const timeoutSignals = [];
  /** @type {ReturnType<typeof setTimeout>[]} */
  const timeoutHandles = [];
  /** @type {AbortSignal[]} */
  const fetchSignals = [];
  /** @type {Record<string, unknown>} */
  const requestBodies = {};
  const restoreTimeout = installMockProperty(AbortSignal, "timeout", (ms) => {
    timeoutMs.push(ms);
    const controller = new AbortController();
    const handle = setTimeout(() => {
      controller.abort(new DOMException("The operation timed out.", "TimeoutError"));
    }, ms);
    timeoutHandles.push(handle);
    timeoutSignals.push(controller.signal);
    return controller.signal;
  });
  state.workflows = {
    /**
     * @param {RequestInfo | URL} url
     * @param {RequestInit | undefined} init
    */
    async fetch(url, init) {
      assert.equal(new Headers(init?.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
      const endpoint = String(url);
      if (endpoint.endsWith("/lifecycle/check-delete")) {
        assert.equal(new Headers(init?.headers).get("x-request-id"), "rid-lifecycle");
        assert.ok(init?.signal instanceof AbortSignal);
        fetchSignals.push(init.signal);
        requestBodies.lifecycle = parseJsonObjectRequestBody(init, "workflow lifecycle request body");
        return Response.json({ allowed: true });
      }
      assert.ok(init?.signal instanceof AbortSignal);
      assert.equal(new Headers(init?.headers).get("x-request-id"), "rid-cleanup");
      fetchSignals.push(init.signal);
      requestBodies.cleanup = parseJsonObjectRequestBody(init, "DO alarm cleanup request body");
      return Response.json({
        ok: true,
        jobId: null,
        changed: true,
        deleted: 2,
      });
    },
  };

  try {
    await assertWorkflowDeleteAllowed({
      ns: "demo", worker: "api", version: "v2", requestId: "rid-lifecycle",
    });
    await cleanupDoAlarmsForWorker({
      ns: "demo", worker: "api", doStorageId: "do_old", requestId: "rid-cleanup",
    });
  } finally {
    for (const handle of timeoutHandles) clearTimeout(handle);
    restoreTimeout();
  }

  assert.deepEqual(timeoutMs, [5_000, 5_000]);
  assert.equal(timeoutSignals.length, 2);
  assert.deepEqual(timeoutSignals, fetchSignals);
  assert.deepEqual(requestBodies, {
    lifecycle: { ns: "demo", worker: "api", version: "v2" },
    cleanup: { ns: "demo", worker: "api", doStorageId: "do_old" },
  });
});

const lifecycleContract = /** @type {{ limits: { responseBytesMax: number, controlPagesMax: number, controlTimeoutMs: number }, request: Record<string, unknown>, responses: { complete: Record<string, unknown>, blocked: Record<string, unknown>, continuation: Record<string, unknown>, rescanRequired: Record<string, unknown> } }} */ (
  readRepositoryJson("tests/fixtures/workflow-lifecycle-check.json")
);

test("Workflow lifecycle reader limits match the cross-language fixture", async () => {
  assert.equal(WORKFLOW_LIFECYCLE_MAX_PAGES, lifecycleContract.limits.controlPagesMax);
  assert.equal(WORKFLOW_LIFECYCLE_TIMEOUT_MS, lifecycleContract.limits.controlTimeoutMs);
  assert.equal(WORKFLOW_LIFECYCLE_RESPONSE_MAX_BYTES, lifecycleContract.limits.responseBytesMax);
  for (const body of Object.values(lifecycleContract.responses)) {
    assert.deepEqual(await readWorkflowLifecycleResponse(Response.json(body)), body);
  }
});

test("Workflow lifecycle pages resume internally and renew the same delete lock", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  const redis = createFakeRedis();
  state.redis = redis;
  const token = await acquireDeleteLock(redis, "demo", "api", "whole");
  assert.ok(token);
  /** @type {Record<string, unknown>[]} */
  const bodies = [];
  state.workflows = {
    async fetch(/** @type {string} */ _url, /** @type {RequestInit} */ init) {
      bodies.push(parseJsonObjectRequestBody(init, "lifecycle page"));
      return Response.json(bodies.length === 1 ? lifecycleContract.responses.continuation : lifecycleContract.responses.complete);
    },
  };
  await assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", version: "v2", allowCleanup: true, lockToken: token });
  assert.deepEqual(bodies, [
    { ns: "demo", worker: "api", version: "v2", allowCleanup: true },
    lifecycleContract.request,
  ]);
  assert.equal(redis.commands.filter((command) => command[0] === "set" && /** @type {{ifeq?: string}} */ (command[3] ?? {}).ifeq === token).length, 2);
});

test("Workflow lifecycle pagination stops when the delete lock is lost", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  const redis = createFakeRedis();
  state.redis = redis;
  const token = await acquireDeleteLock(redis, "demo", "api", "whole");
  assert.ok(token);
  let calls = 0;
  state.workflows = {
    async fetch() {
      calls += 1;
      await releaseDeleteLock(redis, "demo", "api", token);
      return Response.json(lifecycleContract.responses.continuation);
    },
  };
  await assert.rejects(
    () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", allowCleanup: true, lockToken: token }),
    (error) => error instanceof ControlAbort && /** @type {{code?: string}} */ (error).code === "deleting",
  );
  assert.equal(calls, 1);
});

test("Workflow lifecycle rescan responses require a new request instead of reporting active instances", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  for (const afterContinuation of [false, true]) {
    let calls = 0;
    state.workflows = {
      async fetch() {
        calls += 1;
        return Response.json(afterContinuation && calls === 1
          ? lifecycleContract.responses.continuation
          : lifecycleContract.responses.rescanRequired);
      },
    };
    await assert.rejects(
      () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", allowCleanup: true }),
      (error) => error instanceof ControlAbort &&
        /** @type {{status?: number, code?: string}} */ (error).status === 503 &&
        /** @type {{code?: string}} */ (error).code === "workflow_lifecycle_check_incomplete",
    );
    assert.equal(calls, afterContinuation ? 2 : 1);
  }
});

for (const phase of ["fetch", "body", "materialization"]) {
  test(`Workflow lifecycle total deadline during ${phase} returns incomplete`, async (t) => {
    restoreControlSharedStateAfter(t);
    state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
    let now = 1_000;
    const deadline = now + WORKFLOW_LIFECYCLE_TIMEOUT_MS;
    let calls = 0;
    let cancelled = false;
    /** @type {number[]} */
    const timeouts = [];
    /** @type {AbortController[]} */
    const controllers = [];
    /** @type {Array<{level: string, event: string}>} */
    const logs = [];
    state.log = (/** @type {string} */ level, /** @type {string} */ event) => logs.push({ level, event });
    const expire = () => {
      now = deadline;
      const controller = controllers.at(-1);
      assert.ok(controller);
      controller.abort(new DOMException("deadline expired", "TimeoutError"));
    };
    state.workflows = {
      async fetch(/** @type {string} */ _url, /** @type {RequestInit} */ init) {
        calls += 1;
        if (calls < 3) {
          now += WORKFLOWS_INTERNAL_TIMEOUT_MS - 1;
          return Response.json({ ...lifecycleContract.responses.continuation, cursor: String(calls) });
        }
        if (phase === "body") {
          return new Response(new ReadableStream({
            pull() { expire(); },
            cancel() { cancelled = true; },
          }, { highWaterMark: 0 }));
        }
        now = deadline;
        if (phase === "fetch") {
          expire();
          throw init.signal?.reason;
        }
        return Response.json(lifecycleContract.responses.complete);
      },
    };
    await withMockedProperty(Date, "now", () => now, async () => {
      await withMockedProperty(AbortSignal, "timeout", (ms) => {
        timeouts.push(ms);
        const controller = new AbortController();
        controllers.push(controller);
        return controller.signal;
      }, async () => {
        await assert.rejects(
          () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", allowCleanup: true }),
          (error) => error instanceof ControlAbort &&
            /** @type {{status?: number, code?: string}} */ (error).status === 503 &&
            /** @type {{code?: string}} */ (error).code === "workflow_lifecycle_check_incomplete",
        );
      });
    });
    assert.equal(calls, 3);
    assert.deepEqual(timeouts, [WORKFLOWS_INTERNAL_TIMEOUT_MS, WORKFLOWS_INTERNAL_TIMEOUT_MS, 2]);
    assert.equal(cancelled, phase === "body");
    assert.deepEqual(logs, [{ level: "warn", event: "workflow_lifecycle_check_failed" }]);
  });
}

test("Workflow lifecycle preserves per-call timeouts and backend errors", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  for (const scenario of ["per-call timeout", "late success", "late transport failure", "malformed reply"]) {
    let now = 1_000;
    const controller = new AbortController();
    /** @type {string[]} */
    const levels = [];
    state.log = (/** @type {string} */ level) => levels.push(level);
    state.workflows = { async fetch() {
      if (scenario === "per-call timeout") {
        now += WORKFLOWS_INTERNAL_TIMEOUT_MS;
        controller.abort(new DOMException("per-call timeout", "TimeoutError"));
        throw controller.signal.reason;
      }
      now += WORKFLOW_LIFECYCLE_TIMEOUT_MS;
      if (scenario === "late success") return Response.json(lifecycleContract.responses.complete);
      if (scenario === "malformed reply") return new Response("{broken");
      throw new Error("backend connection failed");
    } };
    await withMockedProperty(Date, "now", () => now, async () => {
      await withMockedProperty(AbortSignal, "timeout", () => controller.signal, async () => {
        await assert.rejects(
          () => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api" }),
          (error) => error instanceof ControlAbort &&
            /** @type {{code?: string}} */ (error).code === "workflow_internal_dispatch_failed",
        );
      });
    });
    assert.deepEqual(levels, ["error"]);
  }
});

for (const phase of ["fetch", "body"]) {
  test(`Workflow lifecycle preserves a per-call timeout after delayed ${phase} rejection`, async (t) => {
    restoreControlSharedStateAfter(t);
    state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
    const controller = new AbortController();
    const started = Promise.withResolvers();
    let now = 1_000;
    let calls = 0;
    /** @type {string[]} */
    const levels = [];
    state.log = (/** @type {string} */ level) => levels.push(level);
    state.workflows = { async fetch() {
      calls += 1;
      if (phase === "body") {
        return new Response(new ReadableStream({
          pull() { started.resolve(undefined); },
        }, { highWaterMark: 0 }));
      }
      return await new Promise((_, reject) => {
        controller.signal.addEventListener("abort", () => reject(controller.signal.reason), { once: true });
        started.resolve(undefined);
      });
    } };
    await withMockedProperty(Date, "now", () => now, async () => {
      await withMockedProperty(AbortSignal, "timeout", (ms) => {
        assert.equal(ms, WORKFLOWS_INTERNAL_TIMEOUT_MS);
        return controller.signal;
      }, async () => {
        const pending = assertWorkflowDeleteAllowed({ ns: "demo", worker: "api" });
        const rejected = assert.rejects(pending, (error) => error instanceof ControlAbort &&
          /** @type {{code?: string}} */ (error).code === "workflow_internal_dispatch_failed");
        await started.promise;
        now = 1_000 + WORKFLOWS_INTERNAL_TIMEOUT_MS;
        controller.abort(new DOMException("per-call timeout", "TimeoutError"));
        // Deliver the rejection only after both deadlines have passed.
        now = 1_000 + WORKFLOW_LIFECYCLE_TIMEOUT_MS + 1;
        await rejected;
      });
    });
    assert.equal(calls, 1);
    assert.deepEqual(levels, ["error"]);
  });
}

test("Workflow lifecycle bounds a hung renewal by the remaining check budget", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  const redis = createFakeRedis();
  state.redis = redis;
  const token = await acquireDeleteLock(redis, "demo", "api", "whole");
  assert.ok(token);
  const set = redis.set.bind(redis);
  const renewalStarted = Promise.withResolvers();
  const lateRenewal = Promise.withResolvers();
  let now = 1_000;
  let renewals = 0;
  let backendCalls = 0;
  /** @type {Array<{callback: () => void, delay: number, cleared: boolean}>} */
  const timers = [];
  const setTimeoutMock = /** @type {typeof setTimeout} */ ((
    /** @type {() => void} */ callback, /** @type {number} */ delay
  ) => {
    const timer = { callback, delay, cleared: false };
    timers.push(timer);
    return /** @type {ReturnType<typeof setTimeout>} */ (/** @type {unknown} */ (timer));
  });
  const clearTimeoutMock = /** @type {typeof clearTimeout} */ ((timer) => {
    /** @type {{cleared: boolean}} */ (/** @type {unknown} */ (timer)).cleared = true;
  });
  state.workflows = {
    async fetch() {
      backendCalls += 1;
      now += 2_000;
      return Response.json(lifecycleContract.responses.continuation);
    },
  };
  await withMockedProperty(Date, "now", () => now, async () => {
    await withMockedProperty(globalThis, "setTimeout", setTimeoutMock, async () => {
      await withMockedProperty(globalThis, "clearTimeout", clearTimeoutMock, async () => {
        await withMockedProperty(redis, "set", (/** @type {Parameters<typeof set>} */ ...args) => {
          renewals += 1;
          if (renewals === 1) return set(...args);
          renewalStarted.resolve(undefined);
          return lateRenewal.promise;
        }, async () => {
          const check = assertWorkflowDeleteAllowed({ ns: "demo", worker: "api", lockToken: token });
          const rejected = assert.rejects(check, (error) => error instanceof ControlAbort &&
            /** @type {{code?: string}} */ (error).code === "workflow_lifecycle_check_incomplete");
          await renewalStarted.promise;
          assert.equal(timers[0].cleared, true);
          assert.equal(timers[1].delay, lifecycleContract.limits.controlTimeoutMs - 2_000);
          now = 1_000 + lifecycleContract.limits.controlTimeoutMs;
          timers[1].callback();
          await rejected;
          assert.equal(timers[1].cleared, true);
          assert.equal(backendCalls, 1);
          lateRenewal.reject(new Error("late Redis rejection"));
          await Promise.resolve();
        });
      });
    });
  });
});

test("Workflow lifecycle continuation cannot turn an incomplete or blocked scan into permission", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
  for (const scenario of ["budget", "blocked", "stalled", "contradictory"]) {
    let calls = 0;
    state.workflows = {
      async fetch() {
        calls += 1;
        if (scenario === "blocked" && calls === 2) return Response.json(lifecycleContract.responses.blocked);
        if (scenario === "contradictory") return Response.json({ ...lifecycleContract.responses.complete, cursor: "128" });
        return Response.json({ ...lifecycleContract.responses.continuation, cursor: scenario === "stalled" ? "128" : String(calls) });
      },
    };
    const expected = scenario === "budget" ? "workflow_lifecycle_check_incomplete"
      : scenario === "blocked" ? "workflow_instances_active" : "workflow_internal_dispatch_failed";
    await assert.rejects(() => assertWorkflowDeleteAllowed({ ns: "demo", worker: "api" }),
      (error) => error instanceof ControlAbort && /** @type {{code?: string}} */ (error).code === expected);
    assert.equal(calls, scenario === "budget" ? WORKFLOW_LIFECYCLE_MAX_PAGES : scenario === "contradictory" ? 1 : 2);
  }
});

test("Workflow lifecycle reader rejects oversized replies without draining the body", async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({ cancel() { cancelled = true; } }), {
    headers: { "content-length": String(WORKFLOW_LIFECYCLE_RESPONSE_MAX_BYTES + 1) },
  });
  await assert.rejects(() => readWorkflowLifecycleResponse(response), /exceeds/);
  assert.equal(cancelled, true);
});

test("DO alarm cleanup rejects malformed or oversized success envelopes", async (t) => {
  restoreControlSharedStateAfter(t);
  state.env = { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };

  for (const response of [
    Response.json({ ok: true }),
    Response.json({ ok: true, jobId: "doa-wrong", changed: true, deleted: 1 }),
    Response.json({ ok: true, jobId: null, changed: true, deleted: 1, extra: true }),
    new Response("not-json", { headers: { "content-type": "application/json" } }),
  ]) {
    state.workflows = { fetch: async () => response };
    await assert.rejects(
      () => cleanupDoAlarmsForWorker({ ns: "demo", worker: "api", doStorageId: "do_old" }),
      (error) => error instanceof Error && /** @type {any} */ (error).status === 503
    );
  }

  let cancelled = false;
  state.workflows = {
    fetch: async () => new Response(new ReadableStream({
      cancel() { cancelled = true; },
    }), {
      headers: { "content-length": String(16 * 1024 + 1) },
    }),
  };
  await assert.rejects(
    () => cleanupDoAlarmsForWorker({ ns: "demo", worker: "api", doStorageId: "do_old" }),
    (error) => error instanceof Error && /** @type {any} */ (error).status === 503
  );
  await Promise.resolve();
  assert.equal(cancelled, true);
});

test("workflows transport requires an explicit timeout selection at runtime", async () => {
  let fetchCalls = 0;
  for (const deadlineMs of [undefined, Date.now() + 5_000, Date.now() - 1]) {
    await assert.rejects(
      postWorkflowsInternalRequest({
        workflows: {
          async fetch() {
            fetchCalls += 1;
            return Response.json({ ok: true });
          },
        },
        headers: () => ({ "content-type": "application/json" }),
        endpoint: "workflows/test",
        body: {},
        logEvent: "workflow_test_failed",
        timeoutMs: /** @type {any} */ (undefined),
        deadlineMs,
        makeError: (/** @type {import("../../control/workflows-client.js").WorkflowTransportFailure} */ failure) => new Error(failure),
      }),
      /request_failed/
    );
  }
  assert.equal(fetchCalls, 0);
});

test("workflows transport handles explicit null and tied caps without trusting upstream timeouts", async () => {
  for (const { timeoutMs, outcome } of [null, 1_000].flatMap((timeoutMs) =>
    ["success", "upstream timeout", "clock expiry"].map((outcome) => ({ timeoutMs, outcome })))) {
    let now = 1_000;
    const controller = new AbortController();
    await withMockedProperty(Date, "now", () => now, async () => {
      await withMockedProperty(AbortSignal, "timeout", (ms) => {
        assert.equal(ms, 1_000);
        return controller.signal;
      }, async () => {
        const pending = postWorkflowsInternalRequest({
          workflows: { async fetch() {
            if (outcome !== "success") now = 3_000;
            if (outcome === "upstream timeout") {
              throw new DOMException("upstream timeout", "TimeoutError");
            }
            return Response.json({ ok: true });
          } },
          headers: () => ({}),
          endpoint: "workflows/test",
          body: {},
          logEvent: "workflow_test_failed",
          timeoutMs,
          deadlineMs: 2_000,
          makeError: (/** @type {import("../../control/workflows-client.js").WorkflowTransportFailure} */ failure) => new Error(failure),
        });
        if (outcome === "upstream timeout") await assert.rejects(pending, /request_failed/);
        else if (outcome === "clock expiry") await assert.rejects(pending, /deadline/);
        else assert.deepEqual((await pending).body, { ok: true });
      });
    });
  }
});

test("Workflow instances reader shares its byte ceiling with the Rust fixture", () => {
  const contract = /** @type {{ instancesResponseBytesMax: number }} */ (
    readRepositoryJson("tests/fixtures/workflow-limits.json")
  );
  assert.equal(MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES, contract.instancesResponseBytesMax);
});

for (const character of ["x", "\u4e2d"]) {
  test(`Workflow instances reader accepts exact-limit UTF-8 (${character})`, async () => {
    const empty = JSON.stringify({ instances: [{ output: "" }], cursor: null });
    const available = MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES - Buffer.byteLength(empty);
    const width = Buffer.byteLength(character);
    const output = character.repeat(Math.floor(available / width)) + "x".repeat(available % width);
    const body = JSON.stringify({ instances: [{ output }], cursor: null });
    assert.equal(Buffer.byteLength(body), MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES);
    const response = await readWorkflowInstancesResponse(new Response(body));
    assert.equal(response.body.instances[0].output, output);
    assert.equal(response.body.cursor, null);
    assert.equal(response.bytes.byteLength, MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES);
  });
}

test("Workflow instances reader cancels an oversized declared body without waiting for cancel", async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({
    cancel() { cancelled = true; return new Promise(() => {}); },
  }), { headers: { "content-length": String(MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES + 1) } });
  await assert.rejects(() => readWorkflowInstancesResponse(response), /exceeds/);
  assert.equal(cancelled, true);
});

for (const headers of /** @type {HeadersInit[]} */ ([{}, { "content-encoding": "gzip", "content-length": "1" }])) {
  test(`Workflow instances reader caps streamed bytes with ${JSON.stringify(headers)}`, async () => {
    let cancelled = false;
    const response = new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES));
        controller.enqueue(new Uint8Array(1));
      },
      cancel() { cancelled = true; },
    }), { headers });
    await assert.rejects(() => readWorkflowInstancesResponse(response), /exceeds/);
    assert.equal(cancelled, true);
  });
}

test("Workflow instances reader rejects malformed JSON and UTF-8", async () => {
  for (const body of ["not-json", new Uint8Array([0xff])]) {
    await assert.rejects(() => readWorkflowInstancesResponse(new Response(body)));
  }
});

test("finite Workflow transport deadlines include synchronous response materialization", async () => {
  let now = 1_000;
  await withMockedProperty(Date, "now", () => now, async () => {
    await assert.rejects(() => postWorkflowsInternalRequest({
      workflows: { fetch: async () => Response.json({ instances: [], cursor: null }) },
      headers: () => ({}),
      endpoint: "workflows/instances",
      body: {},
      logEvent: "workflow_test_failed",
      timeoutMs: 5_000,
      readBody: async (/** @type {Response} */ response) => {
        const body = await response.json();
        now += 5_000;
        return body;
      },
      makeError: (/** @type {string} */ failure) => new Error(failure),
    }), /request_failed/);
  });
});

test("codedErrorResponse preserves semantic status/code with a fallback code", async () => {
  const err = Object.assign(new Error("route is already owned"), {
    status: 409,
    details: { host: "demo.workers.example" },
  });
  const res = codedErrorResponse(err, "routing_error");
  await assertJsonResponse(res, 409, {
    host: "demo.workers.example",
    error: "routing_error",
    message: "route is already owned",
  });
});

test("codedErrorResponse hides diagnostic messages on coded server errors", async () => {
  const err = {
    status: 503,
    code: "workflow_backend_invalid_response",
    message: null,
    details: {
      message: "Workflow backend returned a malformed response",
      upstreamStatus: 200,
    },
  };
  const res = codedErrorResponse(err, "workflow_backend_error");
  await assertJsonResponse(res, 503, {
    upstreamStatus: 200,
    error: "workflow_backend_invalid_response",
    message: "Internal error",
  });
});

test("codedErrorResponse keeps safe server context but strips diagnostic detail fields", async () => {
  const err = new ControlAbort(500, "corrupt_meta", {
    namespace: "demo",
    worker: "api",
    stage: "retained_meta_parse",
    detail: "Unexpected token near secret bytes",
    error_detail: "provider diagnostic",
  });
  const res = controlAbortResponse(err);

  await assertJsonResponse(res, 500, {
    namespace: "demo",
    worker: "api",
    error: "corrupt_meta",
    message: "Internal error",
  });
});

test("codedErrorLogFields preserves bounded server diagnostics at error level callers", () => {
  const err = new ControlAbort(500, "corrupt_meta", {
    message: "Corrupt __meta__ for demo/api/v2",
    version: "v2",
    stage: "bundle_meta_parse",
    detail: "__meta__ is not valid JSON",
  });

  assert.deepEqual(codedErrorLogFields(err), {
    status: 500,
    reason: "corrupt_meta",
    error_message: "Corrupt __meta__ for demo/api/v2",
    metadata_version: "v2",
    stage: "bundle_meta_parse",
    error_detail: "__meta__ is not valid JSON",
  });
});

test("codedErrorLogFields bounds structured diagnostic strings", () => {
  const longValue = "x".repeat(4096);
  const err = new ControlAbort(500, longValue, {
    message: longValue,
    version: longValue,
    stage: longValue,
    detail: longValue,
  });

  const fields = codedErrorLogFields(err, err.code, {
    context: { ...err.details, safe_context: "kept" },
  });
  assert.equal(fields.safe_context, "kept");
  for (const alias of ["message", "detail", "version"]) {
    assert.equal(Object.hasOwn(fields, alias), false, alias);
  }
  for (const key of ["reason", "error_message", "metadata_version", "stage", "error_detail"]) {
    assert.equal(/** @type {string} */ (fields[key]).length, 2048, key);
    assert.match(/** @type {string} */ (fields[key]), /\.\.\.$/, key);
  }
});

test("secretEnvelopeErrorResponse bounds the final structured log diagnostics", async () => {
  const longValue = "x".repeat(4096);
  /** @type {Array<{ level: string, event: string, fields: Record<string, unknown> }>} */
  const logs = [];
  const err = Object.assign(new Error(longValue), { code: "secret_provider_error" });

  const response = secretEnvelopeErrorResponse({
    err: /** @type {any} */ (err),
    log(/** @type {string} */ level, /** @type {string} */ event, /** @type {Record<string, unknown>} */ fields) {
      logs.push({ level, event, fields });
    },
    event: "secret_mutation_rejected",
    fields: { request_id: "rid-long-diagnostic" },
  });

  await assertJsonResponse(response, 503, {
    error: "secret_provider_error",
    message: "Internal error",
  });
  assert.equal(logs.length, 1);
  const fields = logs[0].fields;
  assert.equal(/** @type {string} */ (fields.error_message).length, 2048);
  assert.equal(/** @type {string} */ (fields.error_detail).length, 2048);
  assert.match(/** @type {string} */ (fields.error_message), /\.\.\.$/);
  assert.match(/** @type {string} */ (fields.error_detail), /\.\.\.$/);
});

test("codedErrorResponse strips only top-level wire-reserved detail fields", async () => {
  const err = Object.assign(new Error("secret nope"), {
    status: 403,
    code: "secret_denied",
    details: {
      error: "wrong",
      message: "wrong",
      reason: "wrong",
      nested: { error: "inner", safe: "kept" },
    },
  });
  const res = codedErrorResponse(err, "fallback", {
    reason: "extra wrong",
    visible: true,
  });
  await assertJsonResponse(res, 403, {
    nested: { error: "inner", safe: "kept" },
    visible: true,
    error: "secret_denied",
    message: "secret nope",
  });
});

test("readJsonBody: invalid JSON returns machine-code error plus message", async () => {
  const out = await readJsonBody(new Request("http://x", {
    method: "POST",
    body: "{",
  }));
  assert.ok(out.response);
  assert.equal(out.response.status, 400);
  assert.deepEqual(await out.response.json(), {
    error: "invalid_json",
    message: "Body must be valid JSON",
  });
});

test("readJsonBody: empty body is invalid", async () => {
  const rejected = await readJsonBody(new Request("http://x", {
    method: "POST",
  }));
  assert.ok(rejected.response);
  assert.deepEqual(await rejected.response.json(), {
    error: "invalid_json",
    message: "Body must be valid JSON",
  });
});

test("readJsonBody: requireObject rejects scalar JSON", async () => {
  const out = await readJsonBody(new Request("http://x", {
    method: "POST",
    body: "1",
  }), { requireObject: true });
  assert.ok(out.response);
  assert.deepEqual(await out.response.json(), {
    error: "invalid_json_object",
    message: "Body must be a JSON object",
  });
});

test("readJsonBody: content-length over maxBytes fails before parsing", async () => {
  const out = await readJsonBody(new Request("http://x", {
    method: "POST",
    headers: { "content-length": "12" },
    body: "{}",
  }), { maxBytes: 4 });
  assert.ok(out.response);
  assert.equal(out.response.status, 413);
  assert.deepEqual(await out.response.json(), {
    error: "request_body_too_large",
    message: "Body must be at most 4 bytes",
  });
});

test("readJsonBody: streamed body over maxBytes fails while reading", async () => {
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode("{\"x\":"));
      controller.enqueue(new TextEncoder().encode("\"too long\"}"));
      controller.close();
    },
  });
  const out = await readJsonBody(new Request("http://x", /** @type {RequestInit} */ ({
    method: "POST",
    body,
    duplex: "half",
  })), { maxBytes: 6 });
  assert.ok(out.response);
  assert.equal(out.response.status, 413);
  assert.deepEqual(await out.response.json(), {
    error: "request_body_too_large",
    message: "Body must be at most 6 bytes",
  });
});

test("acquireDeleteLock stores a kind-prefixed random token", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const token = await acquireDeleteLock({
    /** @param {unknown[]} args */
    async set(...args) {
      calls.push(args);
      return "OK";
    },
  }, "demo", "api", "version");

  assert.match(token || "", /^version:[0-9a-f]{32}$/);
  assert.deepEqual(calls, [[
    "worker-delete-lock:demo:api",
    token,
    { nx: true, ttl: 30 },
  ]]);
});

test("releaseDeleteLock uses token-scoped DELIFEQ", async (t) => {
  restoreControlSharedStateAfter(t);
  /** @type {unknown[][]} */
  const calls = [];
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const logs = [];
  state.log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    logs.push({ level, event, fields });
  await releaseDeleteLock({
    /** @param {unknown[]} args */
    async delIfEq(...args) {
      calls.push(args);
      return 1;
    },
  }, "demo", "api", "whole:token-a", "rid-delete");

  assert.deepEqual(calls, [["worker-delete-lock:demo:api", "whole:token-a"]]);
  assert.deepEqual(logs, []);
});
