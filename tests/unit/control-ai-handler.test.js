import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  createControlHandlerState,
  controlSharedHarnessUrl,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { decryptSecretValue } from "../../shared/secret-envelope.js";
import { readJsonResponse } from "../helpers/response-json.js";

const GLOBAL = "__controlAiHandlerState";
const env = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
  SECRET_ENVELOPE_KID: "local:test:ai:v1",
};
const state = installControlHandlerState(GLOBAL, createControlHandlerState({
  redis: createFakeRedis(),
  env,
  logs: [],
}));
const controlSharedUrl = controlSharedHarnessUrl(GLOBAL);
const nsPatternUrl = repositoryFileUrl("shared/ns-pattern.js");
const aiContractUrl = repositoryModuleDataUrl("shared/ai-contract.js", [
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
  [/from "shared-utf8";/, `from ${JSON.stringify(repositoryFileUrl("shared/utf8.js"))};`],
]);
const source = applyModuleReplacements(readRepositoryFile("control/handlers/ai.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "shared-ai-contract";/, `from ${JSON.stringify(aiContractUrl)};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
  [/from "shared-secret-envelope";/, `from ${JSON.stringify(repositoryFileUrl("shared/secret-envelope.js"))};`],
]);
const { AI_CREDENTIAL_MAX_BYTES } = await import(aiContractUrl);
const { handle } = await import(moduleDataUrl(source));

beforeEach(() => {
  state.redis = createFakeRedis();
  state.logs.length = 0;
});

/** @param {string} [kind] */
function providerBody(kind = "openai") {
  return {
    kind,
    models: {
      primary: {
        upstreamModel: kind === "deepseek" ? "deepseek-v4-flash" : "gpt-5.6-luna",
        protocol: "responses",
        transports: ["http", "sse"],
        capabilities: { functionTools: true, structuredOutput: true },
      },
    },
  };
}

/** @param {number} count */
function providerBodyWithModels(count) {
  const descriptor = providerBody().models.primary;
  return {
    kind: "openai",
    models: Object.fromEntries(Array.from(
      { length: count },
      (_, index) => [`model-${index}`, descriptor]
    )),
  };
}

/** @returns {ReturnType<typeof createFakeRedis>} */
function redis() {
  return /** @type {ReturnType<typeof createFakeRedis>} */ (state.redis);
}

/** @param {string} method @param {string[]} subPath @param {unknown} [body] */
async function call(method, subPath, body = undefined) {
  const request = new Request(`http://control.test/ns/demo/ai/${subPath.join("/")}`, {
    method,
    ...(body === undefined ? {} : {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
  return await handle({ request, env, method, ns: "demo", subPath, requestId: "rid-ai" });
}

/** @param {string[]} subPath @param {string} body */
async function callRawPut(subPath, body) {
  const request = new Request(`http://control.test/ns/demo/ai/${subPath.join("/")}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body,
  });
  return await handle({ request, env, method: "PUT", ns: "demo", subPath, requestId: "rid-ai" });
}

test("AI provider and credential lifecycle uses revision CAS without exposing plaintext", async () => {
  const created = await call("PUT", ["providers", "openai"], providerBody());
  assert.equal(created.status, 200);
  const createdBody = await readJsonResponse(created, 200);
  const revision = createdBody.provider.revision;
  assert.match(revision, /^[0-9a-f]{32}$/);
  assert.equal(createdBody.provider.credentialConfigured, false);

  const modelsBeforeCredential = await readJsonResponse(await call("GET", ["models"]), 200);
  assert.deepEqual(
    modelsBeforeCredential.models.map((/** @type {{ id: string }} */ model) => model.id),
    ["openai/primary"]
  );

  const stale = await call("PUT", ["providers", "openai", "credential"], {
    revision: "f".repeat(32),
    credential: "not-stored",
  });
  assert.equal(stale.status, 409);

  const stored = await call("PUT", ["providers", "openai", "credential"], {
    revision,
    credential: "provider-secret",
  });
  assert.equal(stored.status, 200);
  const encrypted = redis().hashes.get("ai:provider-credentials:demo")?.openai;
  assert.ok(encrypted);
  assert.notEqual(encrypted, "provider-secret");
  assert.equal(await decryptSecretValue(encrypted, {
    env,
    hashKey: "ai:provider-credentials:demo",
    fieldName: "openai",
  }), "provider-secret");

  const credentialReadTrap = Object.create(null);
  Object.defineProperty(credentialReadTrap, "openai", {
    enumerable: true,
    get() { throw new Error("credential value must not be read while listing providers"); },
  });
  redis().hashes.set("ai:provider-credentials:demo", credentialReadTrap);
  const listed = await call("GET", ["providers"]);
  const listedBody = await readJsonResponse(listed, 200);
  assert.equal(listedBody.providers[0].credentialConfigured, true);
  assert.doesNotMatch(JSON.stringify(listedBody), /provider-secret|WDL-ENC/);

  const models = await call("GET", ["models"]);
  const modelsBody = await readJsonResponse(models, 200);
  assert.deepEqual(
    modelsBody.models.map((/** @type {{ id: string }} */ model) => model.id),
    ["openai/primary"]
  );
});

test("AI provider update rotates revision and preserves a compatible credential", async () => {
  const first = await readJsonResponse(
    await call("PUT", ["providers", "deepseek"], providerBody("deepseek")),
    200
  );
  await call("PUT", ["providers", "deepseek", "credential"], {
    revision: first.provider.revision,
    credential: "deepseek-key",
  });
  const second = await readJsonResponse(
    await call("PUT", ["providers", "deepseek"], providerBody("deepseek")),
    200
  );
  assert.notEqual(second.provider.revision, first.provider.revision);
  assert.equal(second.provider.credentialConfigured, true);
  assert.ok(redis().hashes.get("ai:provider-credentials:demo")?.deepseek);
});

test("AI provider kind change atomically removes its credential", async () => {
  const first = await readJsonResponse(
    await call("PUT", ["providers", "primary"], providerBody("openai")),
    200
  );
  await call("PUT", ["providers", "primary", "credential"], {
    revision: first.provider.revision,
    credential: "openai-key",
  });
  const second = await readJsonResponse(
    await call("PUT", ["providers", "primary"], providerBody("xai")),
    200
  );
  assert.notEqual(second.provider.revision, first.provider.revision);
  assert.equal(second.provider.credentialConfigured, false);
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.primary, undefined);
});

test("AI provider creation removes a credential-only repair residue", async () => {
  redis().hashes.set("ai:provider-credentials:demo", { openai: "WDL-ENC:residual" });
  const created = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  assert.equal(created.provider.credentialConfigured, false);
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.openai, undefined);
});

test("AI credential rejects values that cannot form a Bearer header", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  for (const credential of ["token value", "token\nvalue", "token-\u5bc6\u94a5"]) {
    const response = await call("PUT", ["providers", "openai", "credential"], {
      revision: created.provider.revision,
      credential,
    });
    assert.equal(response.status, 400);
    assert.equal((await readJsonResponse(response, 400)).error, "invalid_request");
  }
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.openai, undefined);
});

test("AI credential body budget admits the maximum escaped credential", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  const credential = "!".repeat(AI_CREDENTIAL_MAX_BYTES);
  const rawBody = `{"revision":"${created.provider.revision}","credential":"${"\\u0021".repeat(AI_CREDENTIAL_MAX_BYTES)}"}`;
  assert.ok(rawBody.length > AI_CREDENTIAL_MAX_BYTES * 5);

  assert.equal((await callRawPut(["providers", "openai", "credential"], rawBody)).status, 200);
  const encrypted = redis().hashes.get("ai:provider-credentials:demo")?.openai;
  assert.ok(encrypted);
  assert.equal(await decryptSecretValue(encrypted, {
    env,
    hashKey: "ai:provider-credentials:demo",
    fieldName: "openai",
  }), credential);
});

test("AI provider delete removes metadata and credential while missing delete is idempotent", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "xai"], providerBody("xai")),
    200
  );
  await call("PUT", ["providers", "xai", "credential"], {
    revision: created.provider.revision,
    credential: "xai-key",
  });
  assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", "xai"]), 200), {
    ok: true,
    deleted: true,
  });
  assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", "xai"]), 200), {
    ok: true,
    deleted: false,
  });
  assert.equal(redis().hashes.get("ai:providers:demo")?.xai, undefined);
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.xai, undefined);
});

test("AI provider delete removes a residual credential without metadata", async () => {
  redis().hashes.set("ai:provider-credentials:demo", { orphan: "WDL-ENC:residual" });
  assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", "orphan"]), 200), {
    ok: true,
    deleted: true,
  });
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.orphan, undefined);
});

test("AI provider reader fails closed on malformed persisted state", async () => {
  redis().hashes.set("ai:providers:demo", { openai: "{not-json" });
  const response = await call("GET", ["providers"]);
  assert.equal(response.status, 500);
  assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
});

test("AI provider reader fails closed when persisted aggregate bounds are exceeded", async () => {
  await call("PUT", ["providers", "seed"], providerBody());
  const seed = redis().hashes.get("ai:providers:demo")?.seed;
  assert.ok(seed);
  redis().hashes.set("ai:providers:demo", Object.fromEntries(
    Array.from({ length: 9 }, (_, index) => [`provider-${index}`, seed])
  ));
  const response = await call("GET", ["providers"]);
  assert.equal(response.status, 500);
  assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
});

test("AI provider readers fail closed on malformed credential fields", async () => {
  await call("PUT", ["providers", "openai"], providerBody());
  redis().hashes.set("ai:provider-credentials:demo", { "bad/name": "WDL-ENC:repair-only" });

  for (const path of [["providers"], ["models"]]) {
    const response = await call("GET", path);
    assert.equal(response.status, 500);
    assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
  }
});

test("AI provider readers fail closed when credential fields exceed their bound", async () => {
  await call("PUT", ["providers", "openai"], providerBody());
  redis().hashes.set("ai:provider-credentials:demo", Object.fromEntries(
    Array.from({ length: 9 }, (_, index) => [`provider-${index}`, "WDL-ENC:repair-only"])
  ));

  const response = await call("GET", ["providers"]);
  assert.equal(response.status, 500);
  assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
});

test("AI provider writes enforce the namespace provider bound", async () => {
  for (let index = 0; index < 8; index += 1) {
    assert.equal((await call("PUT", ["providers", `provider-${index}`], providerBody())).status, 200);
  }
  const response = await call("PUT", ["providers", "provider-8"], providerBody());
  assert.equal((await readJsonResponse(response, 409)).error, "ai_provider_limit");
  assert.equal(Object.keys(redis().hashes.get("ai:providers:demo") || {}).length, 8);
});

test("AI provider writes enforce the aggregate namespace model bound", async () => {
  for (let index = 0; index < 4; index += 1) {
    assert.equal(
      (await call("PUT", ["providers", `provider-${index}`], providerBodyWithModels(32))).status,
      200
    );
  }
  const response = await call("PUT", ["providers", "provider-4"], providerBody());
  assert.equal((await readJsonResponse(response, 409)).error, "ai_model_limit");
  assert.equal(Object.keys(redis().hashes.get("ai:providers:demo") || {}).length, 4);
});
