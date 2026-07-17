import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createControlHandlerState,
  controlSharedHarnessUrl,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import { decryptSecretValue, encryptSecretValue, isSecretEnvelope } from "../../shared/secret-envelope.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { createFakeRedis, sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { CONTROL_ROUTING_TEST_URL } from "../helpers/load-control-routing.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { readJsonResponse } from "../helpers/response-json.js";

const SECRET_ENVELOPE_URL = repositoryFileUrl("shared/secret-envelope.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");
const SHARED_SECRET_KEYS_URL = repositoryFileUrl("shared/secret-keys.js");
const RUNTIME_ENV_BUILD_URL = repositoryModuleDataUrl("runtime/load/env-build.js", [
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(repositoryFileUrl("shared/ns-pattern.js"))};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(WORKER_CONTRACT_URL)};`],
]);
const { libUrl: PRODUCTION_CONTROL_LIB_URL } = await compileControlGraph();
const env = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
  SECRET_ENVELOPE_KID: "local:test:secret-envelope:v1",
};

/**
 * @typedef {ReturnType<import("../helpers/mocks/fake-redis.js").createFakeRedis>} TestFakeRedis
 */

/** @type {WeakMap<TestFakeRedis, unknown[][]>} */
const failedOpsByRedis = new WeakMap();

/** @returns {TestFakeRedis} */
function createRecordingRedis() {
  /** @type {unknown[][]} */
  const failedOps = [];
  const redis = createFakeRedis(undefined, {
    onExecFailure(ops) {
      failedOps.push(...ops.map((op) => [...op]));
    },
  });
  failedOpsByRedis.set(redis, failedOps);
  return redis;
}

/** @param {TestFakeRedis} redis */
function redisAttemptedOps(redis) {
  return [...(failedOpsByRedis.get(redis) || []), ...redis.ops];
}

/** @param {TestFakeRedis} redis @param {string} key */
function redisHSetAttempts(redis, key) {
  return redisAttemptedOps(redis)
    .filter((op) => op[0] === "hSet" && op[1] === key)
    .flatMap((op) => Object.entries(/** @type {Record<string, string>} */ (op[2]))
      .map(([field, value]) => ({ key, field, value })));
}

/** @param {TestFakeRedis} redis @param {string} [version] */
function seedWorkerSecretActive(redis, version = "v1") {
  redis.hashes.set("routes:demo", { api: version });
  redis.hashes.set(`worker:demo:api:v:${version.slice(1)}`, { __meta__: "{}" });
}

/** @param {TestFakeRedis} redis @param {string[]} versions */
function seedWorkerSecretVersions(redis, versions) {
  redis.zsets.set("worker-versions:demo:api", new Map(
    versions.map((version) => [version, Number(version.slice(1))])
  ));
}

/**
 * @param {{ redis: TestFakeRedis }} state
 * @param {(redis: TestFakeRedis) => void | Promise<void>} setup
 * @param {(redis: TestFakeRedis, sessionCalls: () => number) => unknown | Promise<unknown>} callback
 */
async function withWorkerSecretRedis(state, setup, callback) {
  const redis = createRecordingRedis();
  seedWorkerSecretActive(redis);
  redis.strings.set("worker:demo:api:next_version", "1");
  await setup(redis);
  let calls = 0;
  const previousRedis = state.redis;
  const session = redis.session.bind(redis);
  redis.session = /** @type {TestFakeRedis["session"]} */ (async (fn) => {
    calls += 1;
    return await session(fn);
  });
  state.redis = redis;
  try {
    return await callback(redis, () => calls);
  } finally {
    state.redis = previousRedis;
  }
}

/**
 * @param {{ redis: TestFakeRedis }} state
 * @param {(redis: TestFakeRedis) => void | Promise<void>} setup
 * @param {(redis: TestFakeRedis) => unknown | Promise<unknown>} callback
 */
async function withNamespaceSecretRedis(state, setup, callback) {
  const redis = createRecordingRedis();
  await setup(redis);
  const previousRedis = state.redis;
  state.redis = redis;
  try {
    return await callback(redis);
  } finally {
    state.redis = previousRedis;
  }
}

const validateSecretKeyStubSource = `
import { RESERVED_OBJECT_KEYS } from ${JSON.stringify(repositoryFileUrl("shared/ns-pattern.js"))};
const SECRET_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const WDL_RESERVED_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;
export function validateSecretKey(key) {
  if (typeof key !== "string" || !SECRET_KEY_RE.test(key)) throw new Error("bad key");
  if (WDL_RESERVED_BINDING_RE.test(key)) throw new Error("reserved key");
  if (RESERVED_OBJECT_KEYS.has(key)) throw new Error("reserved object key");
  if (key.length > 128) throw new Error("key too long");
}
`;

/**
 * @param {string} controlSharedUrl
 * @param {string} controlLibUrl
 */
function secretPutUrl(controlSharedUrl, controlLibUrl) {
  const source = applyModuleReplacements(readRepositoryFile("control/handlers/secret-put.js"), [
    [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
    [/from "control-lib";/, `from ${JSON.stringify(controlLibUrl)};`],
    [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
  ]);
  return moduleDataUrl(source);
}

function envBudgetUrl() {
  const sharedRedisUrl = sharedRedisStubUrl();
  const source = applyModuleReplacements(readRepositoryFile("control/env-budget.js"), [
    [/from "control-lib";/, `from ${JSON.stringify(PRODUCTION_CONTROL_LIB_URL)};`],
    [/from "runtime-load-env-build";/, `from ${JSON.stringify(RUNTIME_ENV_BUILD_URL)};`],
    [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
    [/from "shared-worker-contract";/, `from ${JSON.stringify(WORKER_CONTRACT_URL)};`],
    [/from "shared-redis";/, `from ${JSON.stringify(sharedRedisUrl)};`],
  ]);
  return moduleDataUrl(source);
}

const NAMESPACE_SECRET_STATE_GLOBAL = "__controlSecretNamespaceState";
const namespaceSecretState = /** @type {ReturnType<typeof createControlHandlerState> & { redis: TestFakeRedis }} */ (
  installControlHandlerState(NAMESPACE_SECRET_STATE_GLOBAL, createControlHandlerState({
    redis: createRecordingRedis(),
    logs: [],
  }))
);
const controlSharedUrl = controlSharedHarnessUrl(NAMESPACE_SECRET_STATE_GLOBAL);
const controlLibStubUrl = moduleDataUrl(`
${validateSecretKeyStubSource}
export const workersIndexKey = (ns) => \`workers:\${ns}\`;
`);
const src = applyModuleReplacements(readRepositoryFile("control/handlers/ns-secrets.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(controlLibStubUrl)};`],
  [/from "control-handlers-secret-put";/, `from ${JSON.stringify(secretPutUrl(controlSharedUrl, controlLibStubUrl))};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(WORKER_CONTRACT_URL)};`],
  [/from "control-env-budget";/, `from ${JSON.stringify(envBudgetUrl())};`],
  [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
  [/from "shared-secret-keys";/, `from ${JSON.stringify(SHARED_SECRET_KEYS_URL)};`],
]);

const { handle } = await import(moduleDataUrl(src));
const {
  WORKER_LOADER_ENV_MAX_BYTES,
  WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
  estimatedWorkerLoaderEnv,
  estimatedWorkerLoaderEnvBytes,
} = await import(envBudgetUrl());

const WORKFLOW_BUDGET_META = Object.freeze({
  workflows: Object.freeze([{
    binding: "FLOW",
    name: "flow",
    className: "Flow",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
  }]),
});

/**
 * @param {{
 *   padLength: number,
 *   version: string,
 *   nsSecrets?: Record<string, string> | null,
 *   workerSecrets?: Record<string, string> | null,
 * }} args
 */
function workflowBudgetEnvBytes({ padLength, version, nsSecrets = null, workerSecrets = null }) {
  return estimatedWorkerLoaderEnvBytes(estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "api",
    version,
    vars: { PAD: "x".repeat(padLength) },
    nsSecrets,
    workerSecrets,
    meta: WORKFLOW_BUDGET_META,
  }));
}

/** @param {number} padLength */
function workflowBudgetMetaWithPad(padLength) {
  return {
    ...WORKFLOW_BUDGET_META,
    vars: { PAD: "x".repeat(padLength) },
  };
}

/**
 * @param {string} version
 * @param {{ nsSecrets?: Record<string, string> | null, workerSecrets?: Record<string, string> | null }} [secrets]
 */
function maxWorkflowBudgetPadFor(version, secrets = {}) {
  let lo = 0;
  let hi = WORKER_LOADER_ENV_MAX_BYTES;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (workflowBudgetEnvBytes({ padLength: mid, version, ...secrets }) <= WORKER_LOADER_ENV_MAX_BYTES) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }
  return lo;
}

test("namespace secret PUT stores an envelope instead of plaintext", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, () => {}, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret",
    });

    assert.equal(response.status, 200);
    const attempts = redisHSetAttempts(redis, "secrets:demo");
    assert.equal(attempts.length, 1);
    assert.equal(attempts[0].field, "TOKEN");
    assert.equal(redis.hashes.get("secrets:demo")?.TOKEN, attempts[0].value);
    assert.equal(isSecretEnvelope(attempts[0].value), true);
    assert.equal(attempts[0].value.includes("plain-secret"), false);
    assert.equal(
      await decryptSecretValue(attempts[0].value, {
        env,
        hashKey: "secrets:demo",
        fieldName: "TOKEN",
      }),
      "plain-secret"
    );
  });
});

test("namespace secret PUT hides envelope configuration details and logs them", async () => {
  const logStart = namespaceSecretState.logs.length;
  await withNamespaceSecretRedis(namespaceSecretState, () => {}, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env: {},
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-unconfigured",
    });

    const body = await readJsonResponse(response, 503);
    assert.deepEqual(body, {
      error: "secret_encryption_unconfigured",
      message: "Internal error",
    });
    assert.deepEqual(namespaceSecretState.logs.slice(logStart), [{
      level: "error",
      event: "ns_secret_mutation_rejected",
      fields: {
        request_id: "rid-secret-unconfigured",
        namespace: "demo",
        key: "TOKEN",
        method: "PUT",
        status: 503,
        reason: "secret_encryption_unconfigured",
        error_message: "SECRET_ENVELOPE_KID must be a canonical local provider kid",
        error_detail: "SECRET_ENVELOPE_KID must be a canonical local provider kid",
      },
    }]);
    assert.equal(redis.ops.length, 0);
  });
});

test("namespace secret mutation rejects invalid keys through shared validator", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, () => {}, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/bad-key", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "bad-key",
      requestId: "rid-secret",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "invalid_request");
    assert.equal(redis.ops.length, 0);
  });
});

test("namespace secret mutation rejects Object.prototype keys before persistence", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, () => {}, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/constructor", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "constructor",
      requestId: "rid-secret",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "invalid_request");
    assert.equal(redis.ops.length, 0);
  });
});

test("namespace secret PUT accepts lowercase secret keys like production", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, () => {}, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/lowercase", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "lowercase",
      requestId: "rid-secret",
    });

    assert.equal(response.status, 200);
    assert.equal(redisHSetAttempts(redis, "secrets:demo").at(-1)?.field, "lowercase");
  });
});

test("namespace secret PUT runs as a WATCH/MULTI mutation and retries contention", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.execFailures = 1;
    redis.sets.set("workers:demo", new Set(["api"]));
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.hashes.set("worker:demo:api:v:1", { __meta__: "{}" });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/RETRY_TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "RETRY_TOKEN",
      requestId: "rid-secret-retry",
    });

    assert.equal(response.status, 200);
    const attempts = redisHSetAttempts(redis, "secrets:demo");
    assert.equal(attempts.length, 2);
    assert.equal(attempts.at(-1)?.field, "RETRY_TOKEN");
    assert.ok(redis.watched.includes("secrets:demo"));
    assert.ok(redis.watched.includes("routes:demo"));
    assert.ok(redis.watched.includes("workers:demo"));
    assert.ok(redis.watched.includes("worker-versions:demo:api"));
    assert.ok(redis.watched.includes("secrets:demo:api"));
  });
});

test("namespace secret PUT checks retained worker versions before storing", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.sets.set("workers:demo", new Set(["api"]));
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.hashes.set("worker:demo:api:v:1", {
      __meta__: JSON.stringify({ vars: { BIG: "x".repeat(1024 * 1024) } }),
    });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-retained",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(redis.ops.some((op) => op[0] === "hSet"), false);
  });
});

test("namespace secret PUT returns corrupt_meta for invalid retained bundle metadata", async () => {
  const logStart = namespaceSecretState.logs.length;
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.sets.set("workers:demo", new Set(["api"]));
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.hashes.set("worker:demo:api:v:1", { __meta__: "[]" });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-ns-secret-corrupt-meta",
    });

    const body = await readJsonResponse(response, 500);
    assert.equal(body.error, "corrupt_meta");
    assert.equal(body.namespace, "demo");
    assert.equal(body.worker, "api");
    assert.equal(body.version, "v1");
    assert.equal(body.detail, undefined);
    assert.deepEqual(namespaceSecretState.logs.slice(logStart).find((entry) =>
      entry.event === "ns_secret_mutation_rejected"
    ), {
      level: "error",
      event: "ns_secret_mutation_rejected",
      fields: {
        request_id: "rid-ns-secret-corrupt-meta",
        namespace: "demo",
        key: "TOKEN",
        method: "PUT",
        status: 500,
        reason: "corrupt_meta",
        error_message: "Corrupt __meta__ for demo/api/v1",
        error_detail: "__meta__ must be a JSON object",
      },
    });
    assert.deepEqual(redisHSetAttempts(redis, "secrets:demo"), []);
  });
});

test("namespace secret PUT budgets active worker versions with their real active version", async () => {
  const nsSecrets = { TOKEN: "plain-secret" };
  const padLength = WORKER_LOADER_ENV_MAX_BYTES -
    workflowBudgetEnvBytes({
      padLength: 0,
      version: WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
      nsSecrets,
    }) +
    1;
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v1", nsSecrets }) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(
    workflowBudgetEnvBytes({ padLength, version: WORKER_LOADER_ENV_VERSION_PLACEHOLDER, nsSecrets }) >
      WORKER_LOADER_ENV_MAX_BYTES
  );

  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    seedWorkerSecretActive(redis, "v1");
    redis.sets.set("workers:demo", new Set(["api"]));
    redis.hashes.set("worker:demo:api:v:1", {
      __meta__: JSON.stringify(workflowBudgetMetaWithPad(padLength)),
    });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-active-placeholder",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.namespace, "demo");
    assert.equal(body.key, "TOKEN");
    assert.equal(body.set, true);
    assert.equal(redis.hashes.get("secrets:demo")?.TOKEN, redisHSetAttempts(redis, "secrets:demo").at(-1)?.value);
  });
});

test("namespace secret PUT deduplicates retained and active version checks", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    seedWorkerSecretActive(redis, "v1");
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.sets.set("workers:demo", new Set(["api"]));
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-version-dedupe",
    });

    assert.equal(response.status, 200);
    assert.equal(
      redis.commands.filter((op) => op[0] === "hGet" && op[1] === "worker:demo:api:v:1" && op[2] === "__meta__").length,
      1
    );
  });
});

test("namespace secret DELETE checks env revealed by removing a namespace secret", async () => {
  const encrypted = await encryptSecretValue("small", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
  });

  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { TOKEN: encrypted });
    redis.sets.set("workers:demo", new Set(["api"]));
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.hashes.set("worker:demo:api:v:1", {
      __meta__: JSON.stringify({ vars: { TOKEN: "x".repeat(1024 * 1024) } }),
    });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-delete-budget",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("namespace secret DELETE removes a corrupt target envelope", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { TOKEN: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-delete-corrupt",
    });

    assert.equal(response.status, 200);
    assert.deepEqual(redis.ops.find((op) => op[0] === "hDel"), ["hDel", "secrets:demo", "TOKEN"]);
    assert.equal(Object.hasOwn(redis.hashes.get("secrets:demo") || {}, "TOKEN"), false);
  });
});

test("namespace secret DELETE fails closed on other corrupt namespace envelopes", async () => {
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
  });

  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { TOKEN: encrypted, BAD: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-delete-other-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("namespace secret DELETE fails closed on unknown-kid namespace envelopes", async () => {
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
  });
  const unknownKid = await encryptSecretValue("plain", {
    env: { ...env, SECRET_ENVELOPE_KID: "local:test:secret-envelope:v2" },
    hashKey: "secrets:demo",
    fieldName: "BAD",
  });

  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { TOKEN: encrypted, BAD: unknownKid });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-delete-unknown-kid",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "unknown_kid");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("namespace secret DELETE fails closed on corrupt worker envelopes", async () => {
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
  });

  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { TOKEN: encrypted });
    redis.hashes.set("secrets:demo:api", { BAD: "WDL-ENC:not-json" });
    redis.sets.set("workers:demo", new Set(["api"]));
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-delete-worker-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("namespace secret PUT still fails closed on other corrupt namespace envelopes", async () => {
  await withNamespaceSecretRedis(namespaceSecretState, (redis) => {
    redis.hashes.set("secrets:demo", { BAD: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await handle({
      request: new Request("http://control.test/ns/demo/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      nsName: "demo",
      secretKey: "TOKEN",
      requestId: "rid-secret-put-other-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hSet"), false);
  });
});

const WORKER_SECRET_STATE_GLOBAL = "__controlSecretWorkerState";
const workerSecretState = /** @type {ReturnType<typeof createControlHandlerState> & { redis: TestFakeRedis }} */ (
  installControlHandlerState(WORKER_SECRET_STATE_GLOBAL, createControlHandlerState({
    redis: createRecordingRedis(),
    logs: [],
  }))
);

/** @param {number} start */
function workerSecretRejectionLogsSince(start) {
  return workerSecretState.logs.slice(start).filter((entry) => entry.event === "secret_mutation_rejected");
}

const workerControlSharedUrl = controlSharedHarnessUrl(WORKER_SECRET_STATE_GLOBAL);
const workerLibStubUrl = moduleDataUrl(`
${validateSecretKeyStubSource}
export { workflowDefsKey } from ${JSON.stringify(PRODUCTION_CONTROL_LIB_URL)};
export const workersIndexKey = (ns) => \`workers:\${ns}\`;
`);
const lifecycleStubUrl = moduleDataUrl(`
export function stageWorkerHidden(multi, ns, name) {
  multi.sRem(\`workers:\${ns}\`, name);
}
export function stageWorkerVisible(multi, ns, name) {
  multi.sAdd(\`workers:\${ns}\`, name);
}
`);
const workerSecretPutUrl = secretPutUrl(workerControlSharedUrl, workerLibStubUrl);
const workerSrc = applyModuleReplacements(readRepositoryFile("control/handlers/worker-secrets.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(workerControlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(workerLibStubUrl)};`],
  [/from "control-handlers-secret-put";/, `from ${JSON.stringify(workerSecretPutUrl)};`],
  [/from "control-lifecycle-indexes";/, `from ${JSON.stringify(lifecycleStubUrl)};`],
  [/from "control-routing";/, `from ${JSON.stringify(CONTROL_ROUTING_TEST_URL)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(WORKER_CONTRACT_URL)};`],
  [/from "control-env-budget";/, `from ${JSON.stringify(envBudgetUrl())};`],
  [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
  [/from "shared-secret-keys";/, `from ${JSON.stringify(SHARED_SECRET_KEYS_URL)};`],
]);
const { handle: workerHandle } = await import(moduleDataUrl(workerSrc));

test("worker secret PUT hides envelope configuration details and logs them", async () => {
  const state = workerSecretState;
  const logStart = state.logs.length;
  await withWorkerSecretRedis(state, () => {}, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env: {},
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-unconfigured",
    });

    const body = await readJsonResponse(response, 503);
    assert.deepEqual(body, {
      error: "secret_encryption_unconfigured",
      message: "Internal error",
    });
    assert.deepEqual(workerSecretRejectionLogsSince(logStart), [{
      level: "error",
      event: "secret_mutation_rejected",
      fields: {
        request_id: "rid-worker-secret-unconfigured",
        namespace: "demo",
        worker: "api",
        key: "TOKEN",
        method: "PUT",
        status: 503,
        reason: "secret_encryption_unconfigured",
        error_message: "SECRET_ENVELOPE_KID must be a canonical local provider kid",
        error_detail: "SECRET_ENVELOPE_KID must be a canonical local provider kid",
      },
    }]);
    assert.equal(redis.ops.length, 0);
  });
});

test("worker secret PUT encrypts before WATCH retries and reuses the envelope", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, (redis) => {
    redis.execFailures = 1;
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret",
    });

    assert.equal(response.status, 200);
    assert.deepEqual([...new Set(redis.watched)].sort(), [
      "crons:demo:api",
      "hosts:demo",
      "routes:demo",
      "secrets:demo",
      "secrets:demo:api",
      "worker-delete-lock:demo:api",
      "worker-versions:demo:api",
      "worker:demo:api:v:1",
      "worker:demo:api:v:2",
    ]);
    const attempts = redisHSetAttempts(redis, "secrets:demo:api");
    assert.equal(attempts.length, 2);
    assert.equal(attempts[0].field, "TOKEN");
    assert.equal(attempts[0].value, attempts[1].value);
    assert.equal(redis.hashes.get("secrets:demo:api")?.TOKEN, attempts[1].value);
    assert.equal(isSecretEnvelope(attempts[1].value), true);
    assert.equal(attempts[1].value.includes("plain-secret"), false);
    assert.equal(
      await decryptSecretValue(attempts[1].value, {
        env,
        hashKey: "secrets:demo:api",
        fieldName: "TOKEN",
      }),
      "plain-secret"
    );
  });
});

test("worker secret PUT returns corrupt_meta for invalid active bundle metadata", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("worker:demo:api:v:1", { __meta__: "[]" });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-corrupt-meta",
    });

    const body = await readJsonResponse(response, 500);
    assert.equal(body.error, "corrupt_meta");
    assert.equal(body.message, "Internal error");
    assert.equal(body.detail, undefined);
    assert.deepEqual(redisHSetAttempts(redis, "secrets:demo:api"), []);
  });
});

test("worker secret PUT logs retained metadata diagnostics without exposing them", async () => {
  const state = workerSecretState;
  const logStart = state.logs.length;
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.delete("routes:demo");
    seedWorkerSecretVersions(redis, ["v1"]);
    redis.hashes.set("worker:demo:api:v:1", { __meta__: "[]" });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-retained-corrupt-meta",
    });

    const body = await readJsonResponse(response, 500);
    assert.equal(body.error, "corrupt_meta");
    assert.equal(body.detail, undefined);
    assert.deepEqual(state.logs.slice(logStart).find((entry) =>
      entry.event === "secret_mutation_rejected"
    ), {
      level: "error",
      event: "secret_mutation_rejected",
      fields: {
        request_id: "rid-worker-secret-retained-corrupt-meta",
        namespace: "demo",
        worker: "api",
        key: "TOKEN",
        method: "PUT",
        status: 500,
        reason: "corrupt_meta",
        error_message: "Corrupt __meta__ for demo/api/v1",
        error_detail: "__meta__ must be a JSON object",
      },
    });
    assert.deepEqual(redisHSetAttempts(redis, "secrets:demo:api"), []);
  });
});

test("worker secret mutation rejects invalid keys through shared validator", async () => {
  const state = workerSecretState;
  const opsBefore = state.redis.ops.length;
  const response = await workerHandle({
    request: new Request("http://control.test/ns/demo/workers/api/secrets/bad-key", {
      method: "PUT",
      body: JSON.stringify({ value: "plain-secret" }),
    }),
    env,
    method: "PUT",
    ns: "demo",
    name: "api",
    subPath: ["bad-key"],
    requestId: "rid-worker-secret",
  });

  const body = await readJsonResponse(response, 400);
  assert.equal(body.error, "invalid_request");
  assert.equal(state.redis.ops.length, opsBefore);
});

test("worker secret PUT active precheck reads the shared fake Redis state", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.delete("routes:demo");
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-precheck-fake",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.note, "stored; will apply on next load or deploy (no active version to promote)");
    assert.equal(body.version, undefined);
    assert.equal(redis.commands.some((op) => op[0] === "incr"), false);
    assert.equal(redis.ops.some((op) => op[0] === "hSet" && op[1] === "secrets:demo:api"), true);
    assert.deepEqual([...new Set(redis.watched)].sort(), [
      "routes:demo",
      "secrets:demo",
      "secrets:demo:api",
      "worker-delete-lock:demo:api",
      "worker-versions:demo:api",
    ]);
  });
});

test("worker secret DELETE keeps a definitions-only worker discoverable", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.delete("routes:demo");
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted });
    redis.hashes.set("wf:defs:demo:api", { flow: "{}" });
    redis.sets.set("workers:demo", new Set(["api"]));
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-definitions-only",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.deleted, true);
    assert.equal(redis.hashes.has("secrets:demo:api"), false);
    assert.equal(redis.sets.get("workers:demo")?.has("api"), true);
    assert.equal(redis.watched.includes("wf:defs:demo:api"), true);
  });
});

test("worker secret PUT maps active bump delete-lock errors to deleting", async () => {
  const state = workerSecretState;
  const logStart = state.logs.length;
  await withWorkerSecretRedis(state, (redis) => {
    redis.strings.set("worker-delete-lock:demo:api", "holder-token");
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/KEY", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["KEY"],
      requestId: "rid-worker-secret-delete-lock",
    });

    const body = await readJsonResponse(response, 409);
    assert.equal(body.error, "deleting");
    assert.equal(redis.ops.length, 0);
    assert.deepEqual(workerSecretRejectionLogsSince(logStart), [{
      level: "warn",
      event: "secret_mutation_rejected",
      fields: {
        request_id: "rid-worker-secret-delete-lock",
        namespace: "demo",
        worker: "api",
        key: "KEY",
        method: "PUT",
        status: 409,
        reason: "deleting",
        error_message: "deleting",
      },
    }]);
  });
});

test("worker secret PUT maps active bump contention to secret mutation contention", async () => {
  const state = workerSecretState;
  const logStart = state.logs.length;
  await withWorkerSecretRedis(state, (redis) => {
    redis.execFailures = 5;
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/KEY", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["KEY"],
      requestId: "rid-worker-secret-bump-contention",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "secret_mutation_contention");
    assert.equal(redis.ops.some((op) => op[0] === "hSet" && op[1] === "secrets:demo:api"), false);
    assert.deepEqual(workerSecretRejectionLogsSince(logStart), [{
      level: "error",
      event: "secret_mutation_rejected",
      fields: {
        request_id: "rid-worker-secret-bump-contention",
        namespace: "demo",
        worker: "api",
        key: "KEY",
        method: "PUT",
        status: 503,
        reason: "secret_mutation_contention",
        error_message: "active version changed during secret mutation; retry later",
      },
    }]);
  });
});

test("worker secret DELETE precheck logs direct mutation aborts through the shared rejection shape", async () => {
  const state = workerSecretState;
  const logStart = state.logs.length;
  await withWorkerSecretRedis(state, (redis) => {
    redis.strings.set("worker-delete-lock:demo:api", "holder-token");
  }, async () => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/KEY", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["KEY"],
      requestId: "rid-worker-secret-delete-precheck",
    });

    const body = await readJsonResponse(response, 409);
    assert.equal(body.error, "deleting");
    assert.deepEqual(workerSecretRejectionLogsSince(logStart), [{
      level: "warn",
      event: "secret_mutation_rejected",
      fields: {
        request_id: "rid-worker-secret-delete-precheck",
        namespace: "demo",
        worker: "api",
        key: "KEY",
        method: "DELETE",
        status: 409,
        reason: "deleting",
        error_message: "deleting",
      },
    }]);
  });
});

test("worker secret DELETE missing key returns noop before bump allocation", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, () => {}, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/MISSING", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["MISSING"],
      requestId: "rid-worker-secret-delete-missing",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.deleted, false);
    assert.equal(redis.commands.some((op) => op[0] === "incr"), false);
    assert.equal(redis.ops.length, 0);
    assert.deepEqual(redis.watched, []);
  });
});

test("worker secret DELETE missing key after precheck returns noop inside bump transaction", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted });
  }, async (redis) => {
    const session = redis.session.bind(redis);
    let sessionCalls = 0;
    redis.session = /** @type {TestFakeRedis["session"]} */ (async (fn) => {
      sessionCalls += 1;
      const result = await session(fn);
      if (sessionCalls === 1) {
        redis.hashes.set("secrets:demo:api", {});
      }
      return result;
    });

    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-raced-missing",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.deleted, false);
    assert.equal(sessionCalls, 2);
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
    assert.equal(redis.ops.some((op) => op[0] === "copy"), false);
    assert.equal(redis.hashes.get("routes:demo")?.api, "v1");
  });
});

test("worker secret DELETE checks env revealed by removing a higher-precedence secret", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("small", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("worker:demo:api:v:1", {
      __meta__: JSON.stringify({ vars: { TOKEN: "x".repeat(1024 * 1024) } }),
    });
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted });
    seedWorkerSecretVersions(redis, ["v1"]);
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-budget",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("worker secret PUT budgets the active bump with its allocated version", async () => {
  const state = workerSecretState;
  const workerSecrets = { TOKEN: "plain-secret" };
  const padLength = WORKER_LOADER_ENV_MAX_BYTES -
    workflowBudgetEnvBytes({
      padLength: 0,
      version: WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
      workerSecrets,
    }) +
    1;
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v1", workerSecrets }) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v2", workerSecrets }) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(
    workflowBudgetEnvBytes({ padLength, version: WORKER_LOADER_ENV_VERSION_PLACEHOLDER, workerSecrets }) >
      WORKER_LOADER_ENV_MAX_BYTES
  );
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("worker:demo:api:v:1", {
      __meta__: JSON.stringify(workflowBudgetMetaWithPad(padLength)),
    });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-future-version-budget",
    });

    const body = await readJsonResponse(response, 200);
    assert.equal(body.namespace, "demo");
    assert.equal(body.name, "api");
    assert.equal(body.key, "TOKEN");
    assert.equal(body.version, "v2");
    assert.equal(body.previousVersion, "v1");
    assert.equal(body.set, true);
    assert.equal(redis.ops.some((op) => op[0] === "hSet" && op[1] === "secrets:demo:api"), true);
    assert.deepEqual(
      redis.commands.filter((op) => op[0] === "incr").at(-1),
      ["incr", "worker:demo:api:next_version"]
    );
    assert.equal(redis.strings.get("worker:demo:api:next_version"), "2");
  });
});

test("worker secret PUT rejects overbudget bump source before writing secret", async () => {
  const state = workerSecretState;
  const workerSecrets = { TOKEN: "plain-secret" };
  const newVersion = "v1000000000";
  const padLength = maxWorkflowBudgetPadFor("v2", { workerSecrets });
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v2", workerSecrets }) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(workflowBudgetEnvBytes({ padLength, version: newVersion, workerSecrets }) > WORKER_LOADER_ENV_MAX_BYTES);
  await withWorkerSecretRedis(state, (redis) => {
    seedWorkerSecretActive(redis, "v2");
    redis.strings.set("worker:demo:api:next_version", "999999999");
    redis.hashes.set("worker:demo:api:v:2", {
      __meta__: JSON.stringify(workflowBudgetMetaWithPad(padLength)),
    });
  }, async (redis, calls) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-bump-budget",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(redis.ops.some((op) => op[0] === "hSet"), false);
    assert.equal(calls(), 1);
  });
});

test("worker secret PUT checks retry source under its retained version", async () => {
  const state = workerSecretState;
  const workerSecrets = { TOKEN: "plain-secret" };
  const padLength = maxWorkflowBudgetPadFor("v999", { workerSecrets });
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v999", workerSecrets }) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(workflowBudgetEnvBytes({ padLength, version: "v1000", workerSecrets }) > WORKER_LOADER_ENV_MAX_BYTES);
  await withWorkerSecretRedis(state, (redis) => {
    seedWorkerSecretActive(redis, "v1000");
    redis.strings.set("worker:demo:api:next_version", "998");
    redis.hashes.set("worker:demo:api:v:1000", {
      __meta__: JSON.stringify(workflowBudgetMetaWithPad(padLength)),
    });
    seedWorkerSecretVersions(redis, ["v1000"]);
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-retained-source-budget",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(redis.ops.some((op) => op[0] === "hSet"), false);
  });
});

test("worker secret DELETE removes a corrupt target envelope", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo:api", { TOKEN: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-corrupt",
    });

    assert.equal(response.status, 200);
    assert.deepEqual(redis.ops.find((op) => op[0] === "hDel"), ["hDel", "secrets:demo:api", "TOKEN"]);
  });
});

test("worker secret DELETE fails closed on other corrupt worker envelopes", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted, BAD: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-other-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("worker secret DELETE fails closed on unknown-kid worker envelopes", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  const unknownKid = await encryptSecretValue("plain", {
    env: { ...env, SECRET_ENVELOPE_KID: "local:test:secret-envelope:v2" },
    hashKey: "secrets:demo:api",
    fieldName: "BAD",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted, BAD: unknownKid });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-unknown-kid",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "unknown_kid");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("worker secret DELETE fails closed on corrupt namespace envelopes", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo", { BAD: "WDL-ENC:not-json" });
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-ns-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
  });
});

test("worker secret DELETE checks corrupt namespace envelopes before active-version bump", async () => {
  const state = workerSecretState;
  const encrypted = await encryptSecretValue("plain", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("worker:demo:api:v:1", { __meta__: JSON.stringify({ vars: { SAFE: "ok" } }) });
    redis.hashes.set("secrets:demo", { BAD: "WDL-ENC:not-json" });
    redis.hashes.set("secrets:demo:api", { TOKEN: encrypted });
  }, async (redis, calls) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "DELETE",
      }),
      env,
      method: "DELETE",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-delete-bump-ns-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hDel"), false);
    assert.equal(calls(), 2);
  });
});

test("worker secret PUT still fails closed on other corrupt worker envelopes", async () => {
  const state = workerSecretState;
  await withWorkerSecretRedis(state, (redis) => {
    redis.hashes.set("secrets:demo:api", { BAD: "WDL-ENC:not-json" });
  }, async (redis) => {
    const response = await workerHandle({
      request: new Request("http://control.test/ns/demo/workers/api/secrets/TOKEN", {
        method: "PUT",
        body: JSON.stringify({ value: "plain-secret" }),
      }),
      env,
      method: "PUT",
      ns: "demo",
      name: "api",
      subPath: ["TOKEN"],
      requestId: "rid-worker-secret-put-other-corrupt",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "invalid_envelope");
    assert.equal(redis.ops.some((op) => op[0] === "hSet"), false);
  });
});
