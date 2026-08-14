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
  [/from "shared-utf8";/, `from ${JSON.stringify(repositoryFileUrl("shared/utf8.js"))};`],
]);
const {
  AI_CREDENTIAL_ENVELOPE_MAX_BYTES,
  AI_CREDENTIAL_MAX_BYTES,
  AI_PROVIDER_RECORD_MAX_BYTES,
} = await import(aiContractUrl);

/** @param {string} value */
function bomPrefixedUtf8(value) {
  const encoded = new TextEncoder().encode(value);
  const bytes = new Uint8Array(encoded.byteLength + 3);
  bytes.set([0xef, 0xbb, 0xbf]);
  bytes.set(encoded, 3);
  return bytes;
}

/** @param {Record<string, string>} hash @param {string} field */
function fakeHashFieldBytes(hash, field) {
  const descriptor = Object.getOwnPropertyDescriptor(hash, field);
  if (!descriptor) return 0;
  // Redis HSTRLEN observes stored bytes without returning the value. Preserve
  // that distinction for tests whose accessor throws if Control reads it.
  if (!("value" in descriptor)) return 1;
  return new TextEncoder().encode(String(descriptor.value)).byteLength;
}

/** @param {string} _script @param {string[]} keys @param {unknown[]} args @param {ReturnType<typeof createFakeRedis>["state"]} redisState */
function evaluateAiProviderSnapshot(_script, keys, args, redisState) {
  const providers = redisState.hashes.get(keys[0]) || {};
  const credentials = redisState.hashes.get(keys[1]) || {};
  const providerNames = Object.keys(providers);
  const credentialNames = Object.keys(credentials);
  const [maxCount, maxNameBytes, maxProviderBytes, maxCredentialBytes] = args.map(Number);
  if (
    providerNames.length > maxCount ||
    credentialNames.length > maxCount ||
    providerNames.some((name) =>
      new TextEncoder().encode(name).byteLength > maxNameBytes ||
      fakeHashFieldBytes(providers, name) > maxProviderBytes
    ) ||
    credentialNames.some((name) =>
      new TextEncoder().encode(name).byteLength > maxNameBytes ||
      fakeHashFieldBytes(credentials, name) > maxCredentialBytes
    )
  ) {
    return [0, [], []];
  }
  return [
    1,
    providerNames.flatMap((name) => [name, providers[name]]),
    credentialNames,
  ];
}

/** @returns {ReturnType<typeof createFakeRedis>} */
function createAiRedis() {
  return createFakeRedis(undefined, { eval: evaluateAiProviderSnapshot });
}

const state = installControlHandlerState(GLOBAL, createControlHandlerState({
  redis: createAiRedis(),
  env,
  logs: [],
}));
const { handle } = await import(moduleDataUrl(source));

beforeEach(() => {
  state.redis = createAiRedis();
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

test("AI credential writer rejects an envelope beyond the persisted read bound", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  const originalKid = env.SECRET_ENVELOPE_KID;
  env.SECRET_ENVELOPE_KID = `local:${"k".repeat(AI_CREDENTIAL_ENVELOPE_MAX_BYTES)}`;
  try {
    const response = await call("PUT", ["providers", "openai", "credential"], {
      revision: created.provider.revision,
      credential: "provider-secret",
    });
    assert.equal(
      (await readJsonResponse(response, 503)).error,
      "ai_credential_encryption_unavailable"
    );
    assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.openai, undefined);
  } finally {
    env.SECRET_ENVELOPE_KID = originalKid;
  }
});

test("AI credential writer fills the eighth credential field", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "target"], providerBody()),
    200
  );
  redis().hashes.set(
    "ai:provider-credentials:demo",
    Object.fromEntries(
      Array.from({ length: 7 }, (_, index) => [`residue-${index}`, "WDL-ENC:residual"])
    )
  );

  const response = await call("PUT", ["providers", "target", "credential"], {
    revision: created.provider.revision,
    credential: "target-key",
  });
  assert.equal(response.status, 200);
  assert.equal(
    Object.keys(redis().hashes.get("ai:provider-credentials:demo") || {}).length,
    8
  );
});

test("AI credential writer rejects a ninth credential field", async () => {
  let targetRevision = "";
  for (let index = 0; index < 8; index += 1) {
    const name = `provider-${index}`;
    const created = await readJsonResponse(
      await call("PUT", ["providers", name], providerBody()),
      200
    );
    if (index === 7) targetRevision = created.provider.revision;
  }
  redis().hashes.set("ai:provider-credentials:demo", {
    orphan: "WDL-ENC:residual",
    ...Object.fromEntries(
      Array.from({ length: 7 }, (_, index) => [`provider-${index}`, "WDL-ENC:configured"])
    ),
  });

  const response = await call("PUT", ["providers", "provider-7", "credential"], {
    revision: targetRevision,
    credential: "provider-7-key",
  });
  assert.equal((await readJsonResponse(response, 409)).error, "ai_credential_limit");
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.["provider-7"], undefined);
});

test("AI credential writer rotates an existing credential at the field limit", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "provider-0"], providerBody()),
    200
  );
  redis().hashes.set(
    "ai:provider-credentials:demo",
    Object.fromEntries(
      Array.from({ length: 8 }, (_, index) => [`provider-${index}`, "WDL-ENC:configured"])
    )
  );

  const response = await call("PUT", ["providers", "provider-0", "credential"], {
    revision: created.provider.revision,
    credential: "rotated-provider-key",
  });
  assert.equal(response.status, 200);
  const encrypted = redis().hashes.get("ai:provider-credentials:demo")?.["provider-0"];
  assert.ok(encrypted);
  assert.equal(await decryptSecretValue(encrypted, {
    env,
    hashKey: "ai:provider-credentials:demo",
    fieldName: "provider-0",
  }), "rotated-provider-key");
});

test("AI credential writer retries a competing claim to the final credential slot", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "target"], providerBody()),
    200
  );
  const providersKey = "ai:providers:demo";
  const credentialsKey = "ai:provider-credentials:demo";
  redis().hashes.set(
    credentialsKey,
    Object.fromEntries(
      Array.from({ length: 7 }, (_, index) => [`residue-${index}`, "WDL-ENC:residual"])
    )
  );

  const redisState = redis().state;
  let competingWriteInjected = false;
  state.redis = createFakeRedis(redisState, {
    async eval(script, keys, args, currentState) {
      const snapshot = evaluateAiProviderSnapshot(script, keys, args, currentState);
      if (
        !competingWriteInjected &&
        keys[0] === providersKey &&
        keys[1] === credentialsKey
      ) {
        competingWriteInjected = true;
        currentState.hashes.set(credentialsKey, {
          ...(currentState.hashes.get(credentialsKey) || {}),
          competitor: "WDL-ENC:configured",
        });
      }
      return snapshot;
    },
  });

  const response = await call("PUT", ["providers", "target", "credential"], {
    revision: created.provider.revision,
    credential: "target-key",
  });
  assert.equal(competingWriteInjected, true);
  assert.equal((await readJsonResponse(response, 409)).error, "ai_credential_limit");
  assert.equal(
    Object.keys(redis().hashes.get("ai:provider-credentials:demo") || {}).length,
    8
  );
  assert.equal(redis().hashes.get(credentialsKey)?.target, undefined);
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

test("AI provider recreation rejects a credential revision from the deleted incarnation", async () => {
  const first = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  assert.equal((await call("DELETE", ["providers", "openai"])).status, 200);

  const recreated = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  assert.notEqual(recreated.provider.revision, first.provider.revision);

  const stale = await call("PUT", ["providers", "openai", "credential"], {
    revision: first.provider.revision,
    credential: "stale-provider-key",
  });
  assert.equal((await readJsonResponse(stale, 409)).error, "ai_provider_revision_mismatch");
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.openai, undefined);

  assert.equal((await call("PUT", ["providers", "openai", "credential"], {
    revision: recreated.provider.revision,
    credential: "current-provider-key",
  })).status, 200);
});

test("AI provider delete removes a residual credential without metadata", async () => {
  redis().hashes.set("ai:provider-credentials:demo", { orphan: "WDL-ENC:residual" });
  assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", "orphan"]), 200), {
    ok: true,
    deleted: true,
  });
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.orphan, undefined);
});

test("AI provider delete repairs malformed and oversized provider records", async () => {
  redis().hashes.set("ai:providers:demo", {
    malformed: "{not-json",
    oversized: "x".repeat(AI_PROVIDER_RECORD_MAX_BYTES + 1),
  });

  for (const provider of ["malformed", "oversized"]) {
    assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", provider]), 200), {
      ok: true,
      deleted: true,
    });
    assert.equal(redis().hashes.get("ai:providers:demo")?.[provider], undefined);
  }
});

test("AI provider reader fails closed on malformed persisted state", async () => {
  redis().hashes.set("ai:providers:demo", { openai: "{not-json" });
  const response = await call("GET", ["providers"]);
  assert.equal(response.status, 500);
  assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
});

test("AI provider readers reject invalid UTF-8 before mutation", async () => {
  const created = await readJsonResponse(
    await call("PUT", ["providers", "openai"], providerBody()),
    200
  );
  const redisState = redis().state;
  const stored = redisState.hashes.get("ai:providers:demo")?.openai;
  assert.ok(stored);
  const invalidRecord = new TextEncoder().encode(stored);
  const modelOffset = stored.indexOf("gpt-5.6-luna");
  assert.notEqual(modelOffset, -1);
  invalidRecord[modelOffset] = 0xff;
  state.redis = createFakeRedis(redisState, {
    eval() {
      return [1, ["openai", invalidRecord], []];
    },
  });

  for (const response of [
    await call("GET", ["providers"]),
    await call("PUT", ["providers", "openai", "credential"], {
      revision: created.provider.revision,
      credential: "must-not-be-written",
    }),
  ]) {
    assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
  }
  assert.equal(redisState.hashes.get("ai:provider-credentials:demo")?.openai, undefined);
});

test("AI provider reader rejects invalid UTF-8 field names", async () => {
  await call("PUT", ["providers", "openai"], providerBody());
  const redisState = redis().state;
  const stored = redisState.hashes.get("ai:providers:demo")?.openai;
  assert.ok(stored);
  state.redis = createFakeRedis(redisState, {
    eval() {
      return [1, [new Uint8Array([0x6f, 0x70, 0x80]), stored], []];
    },
  });

  const response = await call("GET", ["models"]);
  assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
});

test("AI provider reader fails closed on UTF-8 BOMs in persisted names and records", async () => {
  await call("PUT", ["providers", "openai"], providerBody());
  const redisState = redis().state;
  const stored = redisState.hashes.get("ai:providers:demo")?.openai;
  assert.ok(stored);

  for (const providerFields of [
    ["openai", bomPrefixedUtf8(stored)],
    [bomPrefixedUtf8("openai"), stored],
  ]) {
    state.redis = createFakeRedis(redisState, {
      eval() {
        return [1, providerFields, []];
      },
    });
    const response = await call("GET", ["providers"]);
    assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
  }
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

test("AI provider snapshots reject oversized persisted fields before returning them", async () => {
  await call("PUT", ["providers", "openai"], providerBody());
  const validProvider = redis().hashes.get("ai:providers:demo")?.openai;
  assert.ok(validProvider);

  const oversizedProvider = "x".repeat(AI_PROVIDER_RECORD_MAX_BYTES + 1);
  redis().hashes.set("ai:providers:demo", { openai: oversizedProvider });
  for (const response of [
    await call("GET", ["providers"]),
    await call("PUT", ["providers", "xai"], providerBody("xai")),
  ]) {
    assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
  }
  assert.equal(redis().hashes.get("ai:providers:demo")?.openai, oversizedProvider);

  redis().hashes.set("ai:providers:demo", { openai: validProvider });
  redis().hashes.set("ai:provider-credentials:demo", {
    openai: "x".repeat(AI_CREDENTIAL_ENVELOPE_MAX_BYTES + 1),
  });
  for (const path of [["providers"], ["models"]]) {
    const response = await call("GET", path);
    assert.equal((await readJsonResponse(response, 500)).error, "ai_state_corrupt");
  }
});

test("AI provider delete can remove an oversized credential-only residue", async () => {
  redis().hashes.set("ai:provider-credentials:demo", {
    orphan: "x".repeat(AI_CREDENTIAL_ENVELOPE_MAX_BYTES + 1),
  });
  assert.deepEqual(await readJsonResponse(await call("DELETE", ["providers", "orphan"]), 200), {
    ok: true,
    deleted: true,
  });
  assert.equal(redis().hashes.get("ai:provider-credentials:demo")?.orphan, undefined);
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
