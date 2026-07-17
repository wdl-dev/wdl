// Pure-function tests for control/shared.js exports. shared.js carries
// lazily-inited singletons (redis/s3/r2/log) that don't matter for
// pure helpers; bare specifiers are stubbed via data: URLs so the file
// imports without standing up any real client.

import { test } from "node:test";
import assert from "node:assert/strict";
import { moduleDataUrl } from "../helpers/load-shared-module.js";
import { compileControlSharedGraph } from "../helpers/load-control-shared.js";
import { sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { installMockProperty } from "../helpers/mock-global.js";
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
const { postWorkflowsInternalRequest } = await import(controlWorkflowsClientUrl);
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
    env: state.env,
    log: state.log,
    workflows: state.workflows,
  };
  t.after(() => {
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
  const sets = new Map([
    ["hosts:alpha", new Set(["app.workers.example", "shared.workers.example"])],
    ["hosts:beta", new Set(["shared.workers.example"])],
    ["declared-hosts", new Set(["stale.workers.example"])],
    ["host-declarations:stale.workers.example", new Set(["old"])],
  ]);
  const redis = {
    /** @param {string} _cursor @param {string} match */
    async scan(_cursor, match) {
      const keys = [...sets.keys()].filter((key) => {
        if (match.endsWith("*")) return key.startsWith(match.slice(0, -1));
        return key === match;
      });
      return ["0", keys];
    },
    /** @param {string} key */
    async sMembers(key) {
      return [...(sets.get(key) || new Set())];
    },
    /** @param {(session: { multi(): { del(...keys: string[]): unknown, sAdd(key: string, members: string | string[]): unknown, exec(): Promise<unknown> } }) => Promise<unknown>} fn */
    async session(fn) {
      /** @type {Array<() => void>} */
      const operations = [];
      let execCalled = false;
      const multi = {
        /** @param {string[]} keys */
        del(...keys) {
          operations.push(() => {
            for (const key of keys) sets.delete(key);
          });
          return multi;
        },
        /** @param {string} key @param {string | string[]} members */
        sAdd(key, members) {
          operations.push(() => {
            const set = sets.get(key) || new Set();
            for (const member of Array.isArray(members) ? members : [members]) set.add(member);
            sets.set(key, set);
          });
          return multi;
        },
        async exec() {
          execCalled = true;
          for (const apply of operations) apply();
        },
      };
      const result = await fn({ multi: () => multi });
      if (!execCalled) {
        throw new Error("Mock Redis transaction was not executed: expected multi().exec() to be called");
      }
      return result;
    },
  };

  const result = await rebuildDeclaredHostIndexes(redis);

  assert.deepEqual(result, {
    declaredHosts: 2,
    declarationKeysRemoved: 1,
  });
  assert.deepEqual(sets.get("declared-hosts"), new Set(["app.workers.example", "shared.workers.example"]));
  assert.deepEqual(sets.get("host-declarations:app.workers.example"), new Set(["alpha"]));
  assert.deepEqual(sets.get("host-declarations:shared.workers.example"), new Set(["alpha", "beta"]));
  assert.equal(sets.has("host-declarations:stale.workers.example"), false);
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
      assert.equal(abort.details.message, "Workflow lifecycle check returned an invalid response");
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
        assert.equal(init?.signal, undefined);
        requestBodies.lifecycle = parseJsonObjectRequestBody(init, "workflow lifecycle request body");
        return Response.json({ allowed: true });
      }
      assert.ok(init?.signal instanceof AbortSignal);
      assert.equal(new Headers(init?.headers).get("x-request-id"), "rid-cleanup");
      fetchSignals.push(init.signal);
      requestBodies.cleanup = parseJsonObjectRequestBody(init, "DO alarm cleanup request body");
      return Response.json({ ok: true });
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

  assert.deepEqual(timeoutMs, [5_000]);
  assert.equal(timeoutSignals.length, 1);
  assert.deepEqual(timeoutSignals, fetchSignals);
  assert.deepEqual(requestBodies, {
    lifecycle: { ns: "demo", worker: "api", version: "v2" },
    cleanup: { ns: "demo", worker: "api", doStorageId: "do_old" },
  });
});

test("workflows transport requires an explicit timeout selection at runtime", async () => {
  let fetchCalls = 0;
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
      makeError: (/** @type {"unavailable" | "request_failed"} */ failure) => new Error(failure),
    }),
    /request_failed/
  );
  assert.equal(fetchCalls, 0);
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

test("readJsonBody: empty body is invalid unless explicitly allowed", async () => {
  const rejected = await readJsonBody(new Request("http://x", {
    method: "POST",
  }));
  assert.ok(rejected.response);
  assert.deepEqual(await rejected.response.json(), {
    error: "invalid_json",
    message: "Body must be valid JSON",
  });

  const allowed = await readJsonBody(new Request("http://x", {
    method: "POST",
  }), { allowEmpty: true });
  assert.deepEqual(allowed, { body: {} });

  const malformedWithAllowEmpty = await readJsonBody(new Request("http://x", {
    method: "POST",
    body: "{",
  }), { allowEmpty: true });
  assert.ok(malformedWithAllowEmpty.response);
  assert.equal(malformedWithAllowEmpty.response.status, 400);
  assert.deepEqual(await malformedWithAllowEmpty.response.json(), {
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
