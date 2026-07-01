import { test } from "node:test";
import assert from "node:assert/strict";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { decryptSecretValue, encryptSecretValue, isSecretEnvelope } from "../../shared/secret-envelope.js";
import { applyModuleReplacements, moduleDataUrl, readRepositoryFile, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { readJsonResponse } from "../helpers/response-json.js";

const SECRET_ENVELOPE_URL = repositoryFileUrl("shared/secret-envelope.js");
const SHARED_VERSION_URL = repositoryFileUrl("shared/version.js");
const env = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
  SECRET_ENVELOPE_KID: "local:test:secret-envelope:v1",
};

const validateSecretKeyStubSource = `
const SECRET_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const WDL_RESERVED_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;
export function validateSecretKey(key) {
  if (typeof key !== "string" || !SECRET_KEY_RE.test(key)) throw new Error("bad key");
  if (WDL_RESERVED_BINDING_RE.test(key)) throw new Error("reserved key");
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
  const source = applyModuleReplacements(readRepositoryFile("control/env-budget.js"), [
    [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
    [/from "shared-version";/, `from ${JSON.stringify(SHARED_VERSION_URL)};`],
  ]);
  return moduleDataUrl(source);
}

const controlSharedUrl = controlSharedStubUrl(`
export const state = {
  log() {},
  redis: {
    writes: [],
    async hKeys() { return []; },
    async hGet() { return null; },
    async hGetAll() { return {}; },
    async sMembers() { return []; },
    async zRange() { return []; },
    async hSet(key, field, value) {
      this.writes.push({ key, field, value });
      return 1;
    },
    async hDel() { return 0; },
  },
};
`);
const controlLibStubUrl = moduleDataUrl(`
${validateSecretKeyStubSource}
export const workersIndexKey = (ns) => \`workers:\${ns}\`;
`);
const src = applyModuleReplacements(readRepositoryFile("control/handlers/ns-secrets.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(controlLibStubUrl)};`],
  [/from "control-handlers-secret-put";/, `from ${JSON.stringify(secretPutUrl(controlSharedUrl, controlLibStubUrl))};`],
  [/from "shared-version";/, `from ${JSON.stringify(SHARED_VERSION_URL)};`],
  [/from "control-env-budget";/, `from ${JSON.stringify(envBudgetUrl())};`],
  [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
]);

const { handle } = await import(moduleDataUrl(src));

test("namespace secret PUT stores an envelope instead of plaintext", async () => {
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
  const { state } = await import(controlSharedUrl);
  assert.equal(state.redis.writes.length, 1);
  assert.equal(state.redis.writes[0].key, "secrets:demo");
  assert.equal(state.redis.writes[0].field, "TOKEN");
  assert.equal(isSecretEnvelope(state.redis.writes[0].value), true);
  assert.equal(state.redis.writes[0].value.includes("plain-secret"), false);
  assert.equal(
    await decryptSecretValue(state.redis.writes[0].value, {
      env,
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    "plain-secret"
  );
});

test("namespace secret mutation rejects invalid keys through shared validator", async () => {
  const { state } = await import(controlSharedUrl);
  const writesBefore = state.redis.writes.length;
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
  assert.equal(state.redis.writes.length, writesBefore);
});

test("namespace secret PUT accepts lowercase secret keys like production", async () => {
  const { state } = await import(controlSharedUrl);
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
  assert.equal(state.redis.writes.at(-1).field, "lowercase");
});

test("namespace secret PUT checks retained worker versions before storing", async () => {
  const { state } = await import(controlSharedUrl);
  const original = {
    hGet: state.redis.hGet,
    hGetAll: state.redis.hGetAll,
    sMembers: state.redis.sMembers,
    zRange: state.redis.zRange,
  };
  const writesBefore = state.redis.writes.length;
  /** @param {string} key */
  state.redis.hGetAll = async (key) => {
    if (key === "routes:demo") return {};
    return {};
  };
  /** @param {string} key */
  state.redis.sMembers = async (key) => key === "workers:demo" ? ["api"] : [];
  /** @param {string} key */
  state.redis.zRange = async (key) => key === "worker-versions:demo:api" ? ["v1"] : [];
  /** @param {string} key @param {string} field */
  state.redis.hGet = async (key, field) => {
    if (key === "worker:demo:api:v:1" && field === "__meta__") {
      return JSON.stringify({ vars: { BIG: "x".repeat(1024 * 1024) } });
    }
    return null;
  };

  try {
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
    assert.equal(state.redis.writes.length, writesBefore);
  } finally {
    Object.assign(state.redis, original);
  }
});

const workerControlSharedUrl = controlSharedStubUrl(`
class WatchError extends Error {}
export function formatError(err) {
  return { error: err?.code || "internal_error", message: err?.message || String(err) };
}
export const state = {
  log() {},
  redis: {
    execCalls: 0,
    writes: [],
    async session(fn) {
      return await fn({
        async watch() {},
        async unwatch() {},
        async get() { return null; },
        async hKeys() { return []; },
        async hGet() { return null; },
        async hGetAll() { return {}; },
        async zCard() { return 0; },
        async zRange() { return []; },
        multi() {
          return {
            hSet(key, field, value) {
              state.redis.writes.push({ key, field, value });
            },
            hDel() {},
            sAdd() {},
            sRem() {},
            async exec() {
              state.redis.execCalls += 1;
              if (state.redis.execCalls === 1) throw new WatchError("simulated watch conflict");
            },
          };
        },
      });
    },
  },
};
`);
const workerLibStubUrl = moduleDataUrl(`
${validateSecretKeyStubSource}
export const deleteLockKey = (ns, worker) => \`worker-delete-lock:\${ns}:\${worker}\`;
export const workerVersionsKey = (ns, worker) => \`worker-versions:\${ns}:\${worker}\`;
export const routesKey = (ns) => \`routes:\${ns}\`;
export const workersIndexKey = (ns) => \`workers:\${ns}\`;
`);
const lifecycleStubUrl = moduleDataUrl(`
export function stageWorkerHidden() {}
export function stageWorkerVisible(multi, ns, name) {
  multi.sAdd(\`workers:\${ns}\`, name);
}
`);
const routingStubUrl = moduleDataUrl(`
export class RoutingError extends Error {}
export async function bumpActiveAndPromote() {
  return { previousVersion: "v1", version: "v2" };
}
`);
const workerSrc = applyModuleReplacements(readRepositoryFile("control/handlers/worker-secrets.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(workerControlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(workerLibStubUrl)};`],
  [/from "control-handlers-secret-put";/, `from ${JSON.stringify(secretPutUrl(workerControlSharedUrl, workerLibStubUrl))};`],
  [/from "control-lifecycle-indexes";/, `from ${JSON.stringify(lifecycleStubUrl)};`],
  [/from "control-routing";/, `from ${JSON.stringify(routingStubUrl)};`],
  [/from "shared-version";/, `from ${JSON.stringify(SHARED_VERSION_URL)};`],
  [/from "control-env-budget";/, `from ${JSON.stringify(envBudgetUrl())};`],
  [/from "shared-secret-envelope";/, `from ${JSON.stringify(SECRET_ENVELOPE_URL)};`],
]);
const { handle: workerHandle } = await import(moduleDataUrl(workerSrc));

test("worker secret PUT encrypts before WATCH retries and reuses the envelope", async () => {
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
  const { state } = await import(workerControlSharedUrl);
  assert.equal(state.redis.execCalls, 2);
  assert.equal(state.redis.writes.length, 2);
  assert.equal(state.redis.writes[0].key, "secrets:demo:api");
  assert.equal(state.redis.writes[0].field, "TOKEN");
  assert.equal(state.redis.writes[0].value, state.redis.writes[1].value);
  assert.equal(isSecretEnvelope(state.redis.writes[0].value), true);
  assert.equal(state.redis.writes[0].value.includes("plain-secret"), false);
  assert.equal(
    await decryptSecretValue(state.redis.writes[0].value, {
      env,
      hashKey: "secrets:demo:api",
      fieldName: "TOKEN",
    }),
    "plain-secret"
  );
});

test("worker secret mutation rejects invalid keys through shared validator", async () => {
  const { state } = await import(workerControlSharedUrl);
  const writesBefore = state.redis.writes.length;
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
  assert.equal(state.redis.writes.length, writesBefore);
});

test("worker secret DELETE checks env revealed by removing a higher-precedence secret", async () => {
  const { state } = await import(workerControlSharedUrl);
  const originalSession = state.redis.session;
  const encrypted = await encryptSecretValue("small", {
    env,
    hashKey: "secrets:demo:api",
    fieldName: "TOKEN",
  });
  let execCalled = false;
  /** @param {(session: unknown) => Promise<unknown>} fn */
  state.redis.session = async (fn) => await fn({
    async watch() {},
    async unwatch() {},
    async get() { return null; },
    async hKeys() { return ["TOKEN"]; },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      if (key === "routes:demo" && field === "api") return "v1";
      if (key === "worker:demo:api:v:1" && field === "__meta__") {
        return JSON.stringify({ vars: { TOKEN: "x".repeat(1024 * 1024) } });
      }
      return null;
    },
    /** @param {string} key */
    async hGetAll(key) {
      if (key === "secrets:demo:api") return { TOKEN: encrypted };
      return {};
    },
    async zCard() { return 1; },
    async zRange() { return ["v1"]; },
    multi() {
      return {
        hSet() {},
        hDel() {},
        sAdd() {},
        sRem() {},
        async exec() { execCalled = true; },
      };
    },
  });

  try {
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
    assert.equal(execCalled, false);
  } finally {
    state.redis.session = originalSession;
  }
});
