import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildSync } from "esbuild";

import {
  adminFetch,
  adminPut,
  assertStatus,
  composeKill,
  composeUp,
  deployAndPromote,
  encodeClientBinaryFrame,
  encodeClientCloseFrame,
  encodeClientTextFrame,
  frameJson,
  gatewayFetch,
  gatewayStream,
  parseJsonText,
  readIntegrationJson,
  readOneServerBinaryFrame,
  readOneServerCloseFrame,
  readOneServerTextFrame,
  recreateDoSingleRuntime,
  serviceInternalGet,
  serviceInternalPost,
  setupIntegrationSuite,
  uniqueNs,
  waitUntil,
  wsHandshake,
} from "./helpers/index.js";
import { prometheusCounter } from "./helpers/prometheus.js";
import { redisDel, redisHGet, redisHSet } from "./helpers/redis.js";

setupIntegrationSuite();

const AI_WORKER = readFileSync(
  new URL("../../test-workers/ai-binding/src/index.js", import.meta.url),
  "utf8"
);
const AI_OPENAI_SDK_WORKER = buildSync({
  entryPoints: [new URL("../../test-workers/ai-openai-sdk/src/index.js", import.meta.url).pathname],
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2025",
  conditions: ["worker", "browser"],
  external: ["cloudflare:workers"],
  write: false,
}).outputFiles[0].text;

/**
 * @typedef {{
 *   credential: string,
 *   record: {
 *     kind: string,
 *     models: Record<string, {
 *       upstreamModel: string,
 *       protocol: string,
 *       transports: string[],
 *       inputModalities: string[],
 *       outputModalities: string[],
 *       capabilities?: Record<string, boolean>,
 *     }>,
 *   },
 * }} AiProviderFixture
 */

/** @type {Readonly<{ openai: AiProviderFixture, xai: AiProviderFixture, deepseek: AiProviderFixture }>} */
const PROVIDERS = Object.freeze({
  openai: {
    credential: "fake-openai-key",
    record: {
      kind: "openai",
      models: {
        primary: {
          upstreamModel: "gpt-test",
          protocol: "responses",
          transports: ["http", "sse", "responses_websocket"],
          inputModalities: ["text", "image", "file"],
          outputModalities: ["text"],
          capabilities: {
            functionTools: true,
            structuredOutput: true,
            reasoning: true,
            previousResponseId: true,
          },
        },
        embedding: {
          upstreamModel: "text-embedding-test",
          protocol: "embeddings",
          transports: ["http"],
          inputModalities: ["text"],
          outputModalities: ["text"],
        },
      },
    },
  },
  xai: {
    credential: "fake-xai-key",
    record: {
      kind: "xai",
      models: {
        agent: {
          upstreamModel: "grok-test",
          protocol: "responses",
          transports: ["http", "sse", "responses_websocket"],
          inputModalities: ["text"],
          outputModalities: ["text"],
        },
        realtime: {
          upstreamModel: "grok-realtime-test",
          protocol: "realtime",
          transports: ["realtime_websocket"],
          inputModalities: ["text", "audio"],
          outputModalities: ["text", "audio"],
          capabilities: { binaryFrames: true },
        },
      },
    },
  },
  deepseek: {
    credential: "fake-deepseek-key",
    record: {
      kind: "deepseek",
      models: {
        chat: {
          upstreamModel: "deepseek-v4-flash",
          protocol: "chat_completions",
          transports: ["http", "sse"],
          inputModalities: ["text"],
          outputModalities: ["text"],
          capabilities: { functionTools: true, reasoning: true },
        },
        flash: {
          upstreamModel: "deepseek-v4-flash",
          protocol: "responses",
          transports: ["http", "sse"],
          inputModalities: ["text"],
          outputModalities: ["text"],
          capabilities: { functionTools: true, reasoning: true },
        },
      },
    },
  },
});

/** @param {string} ns */
async function configureProviders(ns) {
  for (const [name, provider] of Object.entries(PROVIDERS)) {
    const created = await adminPut(`/ns/${ns}/ai/providers/${name}`, provider.record);
    assertStatus(created, 200, `${name} AI provider create`);
    const revision = created.json.provider.revision;
    const credential = await adminPut(`/ns/${ns}/ai/providers/${name}/credential`, {
      revision,
      credential: provider.credential,
    });
    assertStatus(credential, 200, `${name} AI credential create`);
  }
}

/** @param {string} ns @param {{ compatibilityFlags?: string[] }} [options] */
async function deployAiWorker(ns, { compatibilityFlags = [] } = {}) {
  await deployAndPromote(ns, "ai", {
    mainModule: "worker.js",
    modules: { "worker.js": AI_WORKER },
    compatibilityDate: "2026-08-11",
    compatibilityFlags,
    bindings: {
      AI: { type: "ai" },
      AI_PROBE: { type: "do", className: "AiProbe" },
    },
  });
}

/** @param {string} prefix */
async function setupAiNamespace(prefix) {
  const ns = uniqueNs(prefix);
  await configureProviders(ns);
  await deployAiWorker(ns);
  return { ns };
}

/** @param {string} name @param {string} pool @param {string} [outcome] */
function doRuntimeAiMetric(name, pool, outcome = undefined) {
  const body = serviceInternalGet("do-runtime", 8788, "/_metrics").body;
  /** @type {Record<string, string>} */
  const labels = { service: "do-runtime", pool };
  if (outcome !== undefined) labels.outcome = outcome;
  return prometheusCounter(body, name, labels);
}

/** @param {string} name @param {string} pool @param {string} [outcome] */
function userRuntimeAiMetric(name, pool, outcome = undefined) {
  const body = serviceInternalGet("user-runtime", 8088, "/_metrics").body;
  /** @type {Record<string, string>} */
  const labels = { service: "user-runtime", pool };
  if (outcome !== undefined) labels.outcome = outcome;
  return prometheusCounter(body, name, labels);
}

function userRedisProxyAiModelsCount() {
  const body = serviceInternalGet("redis-proxy-user", 7070, "/_metrics").body;
  return prometheusCounter(body, "wdl_requests_total", {
    route: "ai_models",
    service: "redis-proxy",
    status: "200",
  });
}

function doRuntimeRequestReleaseCount() {
  return ["cancelled", "completed", "deadline"].reduce(
    (total, outcome) => total + doRuntimeAiMetric("wdl_ai_pool_events_total", "request", outcome),
    0
  );
}

/** @param {string} pool */
function userRuntimeReleaseCount(pool) {
  return [
    "cancelled",
    "client_closed",
    "completed",
    "deadline",
    "idle_timeout",
    "provider_closed",
    "provider_error",
    "provider_failed",
    "provider_incomplete",
    "stream_error",
  ].reduce((total, outcome) =>
    total + userRuntimeAiMetric("wdl_ai_pool_events_total", pool, outcome), 0);
}

test("AI provider control rejects non-well-formed upstream model identifiers", async () => {
  const ns = uniqueNs("ai-provider-unicode");
  const response = await adminPut(`/ns/${ns}/ai/providers/openai`, {
    kind: "openai",
    models: {
      primary: {
        upstreamModel: JSON.parse('"\\ud800"'),
        protocol: "responses",
        transports: ["http"],
      },
    },
  });
  assertStatus(response, 400, "non-well-formed AI upstream model");
  assert.equal(response.json.error, "invalid_ai_provider");
});

test("AI model listing sorts complete ids across prefix-related providers", async () => {
  const ns = uniqueNs("ai-model-order");
  for (const [provider, alias] of [["my", "zeta"], ["my-provider", "alpha"]]) {
    const created = await adminPut(`/ns/${ns}/ai/providers/${provider}`, {
      kind: "openai",
      models: {
        [alias]: {
          upstreamModel: "gpt-test",
          protocol: "responses",
          transports: ["http"],
          inputModalities: ["text"],
          outputModalities: ["text"],
        },
      },
    });
    assertStatus(created, 200, `${provider} AI provider create`);
    const credential = await adminPut(`/ns/${ns}/ai/providers/${provider}/credential`, {
      revision: created.json.provider.revision,
      credential: "fake-openai-key",
    });
    assertStatus(credential, 200, `${provider} AI credential create`);
  }
  await deployAiWorker(ns);
  const expected = ["my-provider/alpha", "my/zeta"];
  const controlModels = await readIntegrationJson(
    await adminFetch(`/ns/${ns}/ai/models`),
    200,
    "Control AI model order"
  );
  assert.deepEqual(
    controlModels.models.map((/** @type {{ id: string }} */ model) => model.id),
    expected
  );
  const runtimeModels = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/models"),
    200,
    "runtime AI model order"
  );
  assert.deepEqual(
    runtimeModels.models.map((/** @type {{ id: string }} */ model) => model.id),
    expected
  );
  const result = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=my-provider%2Falpha"),
    200,
    "AI run after cross-provider model ordering"
  );
  assert.equal(result.model, "gpt-test");
});

test("AI catalog metadata remains usable when a credential is added after snapshot load", async () => {
  const ns = uniqueNs("ai-credential-after-catalog");
  const created = await adminPut(`/ns/${ns}/ai/providers/openai`, PROVIDERS.openai.record);
  assertStatus(created, 200, "OpenAI provider create without credential");
  await deployAiWorker(ns);
  const modelsBefore = userRedisProxyAiModelsCount();

  const initialModels = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/models"),
    200,
    "AI catalog before credential"
  );
  assert.ok(initialModels.models.some(
    (/** @type {{ id: string }} */ model) => model.id === "openai/primary"
  ));

  const unavailable = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/raw?model=openai%2Fprimary"),
    503,
    "AI inference before credential"
  );
  assert.equal(unavailable.error, "ai_credential_not_configured");

  const credential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: created.json.provider.revision,
    credential: PROVIDERS.openai.credential,
  });
  assertStatus(credential, 200, "OpenAI credential after catalog load");

  const result = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary"),
    200,
    "AI run after credential is configured"
  );
  assert.equal(result.model, "gpt-test");
  assert.equal(userRedisProxyAiModelsCount(), modelsBefore + 1);
});

test("AI resolver snapshots reject overbound hashes and malformed credential fields", () => {
  const ns = uniqueNs("ai-model-bounds");
  const providersKey = `ai:providers:${ns}`;
  const credentialsKey = `ai:provider-credentials:${ns}`;
  const overbound = Object.fromEntries(
    Array.from({ length: 9 }, (_, index) => [`p${index}`, "malformed"])
  );
  try {
    redisHSet(providersKey, overbound);
    const providers = serviceInternalPost("redis-proxy-user", 7070, "/ai/models", { ns });
    assert.equal(providers.status, 500);
    assert.equal(parseJsonText(providers.body, "overbound provider snapshot").error, "ai_state_corrupt");
    const providerResolve = serviceInternalPost("redis-proxy-user", 7070, "/ai/resolve", {
      ns,
      model: "p0/primary",
      protocol: "responses",
      transport: "http",
    });
    assert.equal(providerResolve.status, 500);
    assert.equal(
      parseJsonText(providerResolve.body, "overbound provider resolve").error,
      "ai_state_corrupt"
    );

    redisDel(providersKey);
    redisHSet(credentialsKey, overbound);
    const credentials = serviceInternalPost("redis-proxy-user", 7070, "/ai/models", { ns });
    assert.equal(credentials.status, 500);
    assert.equal(parseJsonText(credentials.body, "overbound credential snapshot").error, "ai_state_corrupt");
    const credentialResolve = serviceInternalPost("redis-proxy-user", 7070, "/ai/resolve", {
      ns,
      model: "p0/primary",
      protocol: "responses",
      transport: "http",
    });
    assert.equal(credentialResolve.status, 500);
    assert.equal(
      parseJsonText(credentialResolve.body, "overbound credential resolve").error,
      "ai_state_corrupt"
    );

    redisDel(credentialsKey);
    redisHSet(credentialsKey, { "bad/name": "malformed" });
    const malformed = serviceInternalPost("redis-proxy-user", 7070, "/ai/models", { ns });
    assert.equal(malformed.status, 500);
    assert.equal(
      parseJsonText(malformed.body, "malformed credential field").error,
      "ai_state_corrupt"
    );
  } finally {
    redisDel(providersKey);
    redisDel(credentialsKey);
  }
});

test("AI persisted readers reject oversized provider and credential fields", async () => {
  const ns = uniqueNs("ai-field-bounds");
  const providersKey = `ai:providers:${ns}`;
  const credentialsKey = `ai:provider-credentials:${ns}`;
  const created = await adminPut(`/ns/${ns}/ai/providers/openai`, PROVIDERS.openai.record);
  assertStatus(created, 200, "bounded provider setup");
  const validProvider = redisHGet(providersKey, "openai");
  assert.ok(validProvider);
  const oversized = "x".repeat(64 * 1024 + 1);
  /** @type {Array<[string, Record<string, unknown>]>} */
  const readerRequests = [
    ["/ai/models", { ns }],
    ["/ai/resolve", {
      ns,
      model: "openai/primary",
      protocol: "responses",
      transport: "http",
    }],
  ];
  try {
    redisHSet(providersKey, { openai: oversized });
    const controlProvider = await adminFetch(`/ns/${ns}/ai/providers`);
    assert.equal(
      (await readIntegrationJson(controlProvider, 500, "oversized Control provider")).error,
      "ai_state_corrupt"
    );
    for (const [path, body] of readerRequests) {
      const response = serviceInternalPost("redis-proxy-user", 7070, path, body);
      assert.equal(response.status, 500);
      assert.equal(parseJsonText(response.body, `oversized provider ${path}`).error, "ai_state_corrupt");
    }

    redisHSet(providersKey, { openai: validProvider });
    redisHSet(credentialsKey, { openai: oversized });
    const controlCredential = await adminFetch(`/ns/${ns}/ai/providers`);
    assert.equal(
      (await readIntegrationJson(controlCredential, 500, "oversized Control credential")).error,
      "ai_state_corrupt"
    );
    for (const [path, body] of readerRequests) {
      const response = serviceInternalPost("redis-proxy-user", 7070, path, body);
      assert.equal(response.status, 500);
      assert.equal(parseJsonText(response.body, `oversized credential ${path}`).error, "ai_state_corrupt");
    }
  } finally {
    redisDel(providersKey);
    redisDel(credentialsKey);
  }
});

test("AI persisted snapshots reject oversized field names before returning values", async () => {
  const ns = uniqueNs("ai-field-name-bounds");
  const providersKey = `ai:providers:${ns}`;
  const credentialsKey = `ai:provider-credentials:${ns}`;
  const oversizedName = "p".repeat(33);
  const expectedMessage = "AI provider state exceeds its read bounds";
  try {
    for (const [key, label] of [
      [providersKey, "provider"],
      [credentialsKey, "credential"],
    ]) {
      redisHSet(key, { [oversizedName]: "small" });

      const control = await readIntegrationJson(
        await adminFetch(`/ns/${ns}/ai/providers`),
        500,
        `oversized Control ${label} field name`
      );
      assert.equal(control.error, "ai_state_corrupt");
      assert.equal(control.message, "Internal error");

      const models = serviceInternalPost("redis-proxy-user", 7070, "/ai/models", { ns });
      assert.equal(models.status, 500);
      const modelError = parseJsonText(models.body, `oversized ${label} field name`);
      assert.equal(modelError.error, "ai_state_corrupt");
      assert.equal(modelError.message, expectedMessage);

      redisDel(key);
    }
  } finally {
    redisDel(providersKey);
    redisDel(credentialsKey);
  }
});

/** @param {import("node:net").Socket} socket @param {string} label */
async function readAiCloseFrame(socket, label) {
  try {
    return await readOneServerCloseFrame(socket);
  } catch (error) {
    throw new Error(`${label}: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  }
}

test("AI facade loads one catalog snapshot per loaded Worker module", async () => {
  const { ns } = await setupAiNamespace("ai-catalog-cache");
  const before = userRedisProxyAiModelsCount();

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const modelList = await readIntegrationJson(
      await gatewayFetch(ns, "/ai/models"),
      200,
      `AI cached models ${attempt + 1}`
    );
    assert.equal(modelList.models.length, 6);
  }
  const inference = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary"),
    200,
    "AI inference with cached catalog"
  );
  assert.equal(inference.model, "gpt-test");
  assert.equal(userRedisProxyAiModelsCount(), before + 1);
});

test("AI binding exposes agent-capable HTTP and SSE without exposing provider credentials", async () => {
  const { ns } = await setupAiNamespace("ai-http");

  const surface = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/surface"),
    200,
    "AI surface"
  );
  assert.deepEqual(surface, {
    handler: { fetch: "function", run: "function", models: "function" },
    imported: { fetch: "function", run: "function", models: "function" },
    moduleScope: { fetch: "function", run: "function", models: "function" },
    moduleScopeCalls: { fetch: "resolved", run: "rejected", models: "rejected" },
    hidden: {
      doBackend: "undefined",
      ownerNetwork: "undefined",
      workflowsBackend: "undefined",
    },
    processEnvContainsCredential: false,
  });

  const imported = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/imported"),
    200,
    "imported AI facade"
  );
  assert.equal(imported.model, "gpt-test");
  assert.equal(imported.status, "completed");
  assert.deepEqual(imported.models, [
    "deepseek/chat",
    "deepseek/flash",
    "openai/embedding",
    "openai/primary",
    "xai/agent",
    "xai/realtime",
  ]);

  const modelList = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/models"),
    200,
    "AI models"
  );
  assert.deepEqual(modelList.models.map((/** @type {{ id: string }} */ model) => model.id), [
    "deepseek/chat",
    "deepseek/flash",
    "openai/embedding",
    "openai/primary",
    "xai/agent",
    "xai/realtime",
  ]);
  assert.equal(modelList.models.some((/** @type {Record<string, unknown>} */ model) =>
    "upstreamModel" in model), false);

  for (const [model, upstreamModel] of [
    ["openai/primary", "gpt-test"],
    ["xai/agent", "grok-test"],
    ["deepseek/flash", "deepseek-v4-flash"],
  ]) {
    const response = await readIntegrationJson(
      await gatewayFetch(ns, `/ai/json?model=${encodeURIComponent(model)}`),
      200,
      `${model} JSON response`
    );
    assert.equal(response.model, upstreamModel);
    assert.equal(response.status, "completed");
  }

  const rawRequestId = "ai-binding-integration-request";
  const raw = await gatewayFetch(ns, "/ai/raw", {
    headers: { "x-request-id": rawRequestId },
  });
  assert.equal(raw.headers["openai-request-id"], "fake-provider-request");
  assert.equal(raw.headers["x-ai-provider-request-id"], "fake-provider-generic-request");
  assert.equal(raw.headers["x-request-id"], rawRequestId);
  assert.equal((await readIntegrationJson(raw, 200, "raw AI response")).model, "gpt-test");

  const fileInput = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/file"),
    200,
    "Responses file input"
  );
  assert.deepEqual(fileInput, {
    model: "gpt-test",
    type: "input_file",
    fileUrl: "https://files.example/input.pdf",
  });

  const deepSeekFile = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/deepseek-file"),
    200,
    "DeepSeek file input rejection"
  );
  assert.deepEqual(deepSeekFile, {
    status: 400,
    code: "ai_input_modality_unsupported",
  });

  const malformedModel = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/raw?model=OpenAI%2Fprimary"),
    400,
    "malformed raw AI model"
  );
  assert.equal(malformedModel.error, "ai_invalid_model");

  assert.deepEqual(await readIntegrationJson(
    await gatewayFetch(ns, "/ai/invalid-json"),
    200,
    "malformed successful AI response"
  ), {
    name: "AIError",
    status: 502,
    code: "ai_request_failed",
  });

  assert.deepEqual(await readIntegrationJson(
    await gatewayFetch(ns, "/ai/provider-error"),
    200,
    "OpenAI-compatible provider error"
  ), {
    name: "AIError",
    status: 429,
    code: "rate_limit_exceeded",
    message: "rate limited by fake provider",
  });

  const chat = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/chat"),
    200,
    "DeepSeek Chat Completions response"
  );
  assert.equal(chat.object, "chat.completion");
  assert.equal(chat.model, "deepseek-v4-flash");

  const embedding = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/embedding"),
    200,
    "OpenAI embeddings response"
  );
  assert.equal(embedding.object, "list");
  assert.equal(embedding.model, "text-embedding-test");
  assert.deepEqual(embedding.data[0].embedding, [0.1, 0.2]);

  const stream = await gatewayFetch(ns, "/ai/sse");
  assertStatus(stream, 200, "Responses SSE");
  assert.match(String(stream.headers["content-type"]), /^text\/event-stream/);
  const streamText = await stream.text();
  assert.match(streamText, /event: response\.created/);
  assert.match(streamText, /event: response\.completed/);
  assert.match(streamText, /"model":"grok-test"/);

  const chatStream = await gatewayFetch(ns, "/ai/chat-sse");
  assertStatus(chatStream, 200, "Chat Completions SSE");
  assert.match(String(chatStream.headers["content-type"]), /^text\/event-stream/);
  const chatStreamText = await chatStream.text();
  assert.match(chatStreamText, /"object":"chat\.completion\.chunk"/);
  assert.match(chatStreamText, /data: \[DONE\]/);

  const aborted = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/abort"),
    200,
    "AI cancellation"
  );
  assert.deepEqual(aborted, {
    aborted: true,
    name: "AbortError",
    code: 20,
  });

  const deleted = await adminFetch(`/ns/${ns}/ai/providers/deepseek`, { method: "DELETE" });
  assert.deepEqual(
    await readIntegrationJson(deleted, 200, "DeepSeek provider delete"),
    { ok: true, deleted: true }
  );
  const missing = await adminFetch(`/ns/${ns}/ai/providers/deepseek`);
  assertStatus(missing, 404, "deleted DeepSeek provider");
  assert.doesNotMatch(await missing.text(), /fake-deepseek-key/);
});

test("AI positional facades remain usable when importable env is disabled", async () => {
  const ns = uniqueNs("ai-importable-env-disabled");
  await configureProviders(ns);
  await deployAiWorker(ns, { compatibilityFlags: ["disallow_importable_env"] });

  const hidden = {
    doBackend: "undefined",
    ownerNetwork: "undefined",
    workflowsBackend: "undefined",
  };
  const imported = { fetch: "undefined", run: "undefined", models: "undefined" };
  const handler = { fetch: "function", run: "function", models: "function" };
  const surface = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/surface"),
    200,
    "disabled importable env AI surface"
  );
  assert.deepEqual(surface, {
    handler,
    imported,
    moduleScope: imported,
    moduleScopeCalls: { fetch: "missing", run: "missing", models: "missing" },
    hidden,
    processEnvContainsCredential: false,
  });

  const result = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json"),
    200,
    "disabled importable env positional AI call"
  );
  assert.equal(result.status, "completed");

  const doSurface = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/do/surface?name=disabled"),
    200,
    "disabled importable env DO AI surface"
  );
  assert.deepEqual(doSurface, {
    handler,
    imported,
    moduleScope: imported,
    moduleScopeCalls: { fetch: "missing", run: "missing", models: "missing" },
    hidden,
  });
  const doResult = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/do/json?name=disabled"),
    200,
    "disabled importable env DO positional AI call"
  );
  assert.equal(doResult.status, "completed");
});

test("AI provider rotation reaches the next request and new socket without redeploy", async () => {
  const ns = uniqueNs("ai-rotation");
  const created = await adminPut(`/ns/${ns}/ai/providers/openai`, PROVIDERS.openai.record);
  assertStatus(created, 200, "initial OpenAI provider create");
  const initialRevision = created.json.provider.revision;
  const initialCredential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: initialRevision,
    credential: PROVIDERS.openai.credential,
  });
  assertStatus(initialCredential, 200, "initial OpenAI credential create");
  await deployAiWorker(ns);

  const initial = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary"),
    200,
    "initial AI request"
  );
  assert.equal(initial.model, "gpt-test");

  const rotatedRecord = structuredClone(PROVIDERS.openai.record);
  rotatedRecord.models.primary.upstreamModel = "gpt-test-rotated";
  const rotated = await adminPut(`/ns/${ns}/ai/providers/openai`, rotatedRecord);
  assertStatus(rotated, 200, "OpenAI provider rotation");
  const rotatedRevision = rotated.json.provider.revision;
  assert.notEqual(rotatedRevision, initialRevision);
  assert.equal(rotated.json.provider.credentialConfigured, true);

  const staleCredential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: initialRevision,
    credential: "fake-openai-key-rotated",
  });
  assertStatus(staleCredential, 409, "stale OpenAI credential write");

  const nextRequest = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary"),
    200,
    "AI request after provider metadata rotation"
  );
  assert.equal(nextRequest.model, "gpt-test-rotated");

  const rotatedCredential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: rotatedRevision,
    credential: "fake-openai-key-rotated",
  });
  assertStatus(rotatedCredential, 200, "rotated OpenAI credential write");

  const nextSocket = await wsHandshake(ns, "/ai/responses-ws");
  try {
    assertStatus(nextSocket, 101, "Responses WebSocket after provider rotation");
    nextSocket.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "response.create",
      model: "openai/primary",
      input: "after rotation",
    })));
    const completed = frameJson(await readOneServerTextFrame(nextSocket.socket));
    assert.equal(completed.response.model, "gpt-test-rotated");
    const close = readOneServerCloseFrame(nextSocket.socket);
    nextSocket.socket.write(encodeClientCloseFrame(1000, "done"));
    assert.deepEqual(await close, { code: 1000, reason: "done" });
  } finally {
    nextSocket.socket.destroy();
  }
});

test("AI streams forward provider errors and release cancelled stream permits", async () => {
  const { ns } = await setupAiNamespace("ai-stream-lifecycle");
  const baseline = userRuntimeAiMetric("wdl_ai_pool_in_use", "stream");
  const beforeAcquired = userRuntimeAiMetric("wdl_ai_pool_events_total", "stream", "acquired");
  const beforeReleased = userRuntimeReleaseCount("stream");
  const beforeProviderError = userRuntimeAiMetric(
    "wdl_ai_pool_events_total",
    "stream",
    "provider_error"
  );

  const cancelled = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/stream-cancel"),
    200,
    "cancelled AI stream"
  );
  assert.equal(cancelled.done, false);
  assert.match(cancelled.first, /event: response\.created/);
  await waitUntil("cancelled AI stream permit acquired", () =>
    userRuntimeAiMetric("wdl_ai_pool_events_total", "stream", "acquired") === beforeAcquired + 1);
  await waitUntil("cancelled AI stream permit released", () =>
    userRuntimeReleaseCount("stream") === beforeReleased + 1);
  assert.equal(userRuntimeAiMetric("wdl_ai_pool_in_use", "stream"), baseline);

  const providerError = await gatewayFetch(ns, "/ai/sse-error");
  assertStatus(providerError, 200, "AI provider SSE error");
  const providerErrorText = await providerError.text();
  assert.match(providerErrorText, /event: error/);
  assert.match(providerErrorText, /fake provider error/);
  await waitUntil("provider-error AI stream permit released", () =>
    userRuntimeAiMetric("wdl_ai_pool_in_use", "stream") === baseline);
  assert.equal(
    userRuntimeAiMetric("wdl_ai_pool_events_total", "stream", "provider_error") -
      beforeProviderError,
    1
  );
});

test("AI stream and WebSocket idle deadlines release runtime permits", async () => {
  const { ns } = await setupAiNamespace("ai-idle-deadline");
  const streamBaseline = userRuntimeAiMetric("wdl_ai_pool_in_use", "stream");
  const beforeStreamIdle = userRuntimeAiMetric(
    "wdl_ai_pool_events_total",
    "stream",
    "idle_timeout"
  );
  const response = await gatewayStream(ns, "/ai/stream-idle");
  assertStatus(response, 200, "idle AI stream");
  const iterator = response.body[Symbol.asyncIterator]();
  let pendingRead = Promise.resolve();
  try {
    const first = await iterator.next();
    assert.equal(first.done, false);
    assert.match(Buffer.from(first.value).toString("utf8"), /event: response\.created/);
    pendingRead = iterator.next().then(() => undefined, () => undefined);
    await waitUntil("idle AI stream deadline", () =>
      userRuntimeAiMetric("wdl_ai_pool_events_total", "stream", "idle_timeout") ===
        beforeStreamIdle + 1,
    { timeoutMs: 10_000, intervalMs: 100 });
    assert.equal(userRuntimeAiMetric("wdl_ai_pool_in_use", "stream"), streamBaseline);
  } finally {
    response.body.destroy();
    await pendingRead;
  }

  const websocketBaseline = userRuntimeAiMetric("wdl_ai_pool_in_use", "websocket");
  const beforeWebSocketIdle = userRuntimeAiMetric(
    "wdl_ai_pool_events_total",
    "websocket",
    "idle_timeout"
  );
  const idleSocket = await wsHandshake(ns, "/ai/responses-ws");
  try {
    assertStatus(idleSocket, 101, "idle Responses WebSocket upgrade");
    assert.deepEqual(await readAiCloseFrame(idleSocket.socket, "idle Responses close"), {
      code: 1012,
      reason: "AI websocket idle timeout",
    });
    await waitUntil("idle AI WebSocket deadline", () =>
      userRuntimeAiMetric("wdl_ai_pool_events_total", "websocket", "idle_timeout") ===
        beforeWebSocketIdle + 1);
    assert.equal(userRuntimeAiMetric("wdl_ai_pool_in_use", "websocket"), websocketBaseline);
  } finally {
    idleSocket.socket.destroy();
  }
});

test("AI providers outlive the last worker and remain usable after namespace recreation", async () => {
  const ns = uniqueNs("ai-lifecycle");
  const created = await adminPut(`/ns/${ns}/ai/providers/openai`, PROVIDERS.openai.record);
  assertStatus(created, 200, "retained OpenAI provider create");
  const credential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: created.json.provider.revision,
    credential: PROVIDERS.openai.credential,
  });
  assertStatus(credential, 200, "retained OpenAI credential create");
  await deployAiWorker(ns);

  const deleted = await adminFetch(`/ns/${ns}/worker/ai/delete`, { method: "POST" });
  const deletedBody = await readIntegrationJson(deleted, 200, "AI worker delete");
  assert.equal(deletedBody.deleted, true);

  const retained = await readIntegrationJson(
    await adminFetch(`/ns/${ns}/ai/providers/openai`),
    200,
    "retained AI provider"
  );
  assert.equal(retained.provider.credentialConfigured, true);
  const models = await readIntegrationJson(
    await adminFetch(`/ns/${ns}/ai/models`),
    200,
    "retained AI models"
  );
  assert.deepEqual(
    models.models.map((/** @type {{ id: string }} */ model) => model.id),
    ["openai/embedding", "openai/primary"]
  );

  await deployAiWorker(ns);
  const response = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary"),
    200,
    "AI after namespace recreation"
  );
  assert.equal(response.model, "gpt-test");
});

test("AI request admission bounds a stalled tenant upload before provider I/O", async () => {
  const { ns } = await setupAiNamespace("ai-slow-upload");
  const startedAt = Date.now();
  const response = await gatewayFetch(ns, "/ai/slow-upload");
  const body = await readIntegrationJson(response, 504, "stalled AI upload");
  assert.equal(body.error, "ai_request_timeout");
  assert.equal(Date.now() - startedAt < 10_000, true);
});

test("AI binding bridges agent and Realtime WebSockets with bounded lifecycle", async () => {
  const { ns } = await setupAiNamespace("ai-ws");
  const baseline = userRuntimeAiMetric("wdl_ai_pool_in_use", "websocket");

  const responses = await wsHandshake(ns, "/ai/responses-ws");
  try {
    assertStatus(responses, 101, "Responses WebSocket upgrade");
    responses.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "response.create",
      model: "openai/primary",
      input: "wdl_test_tool_loop",
    })));
    const toolCall = frameJson(await readOneServerTextFrame(responses.socket));
    assert.equal(toolCall.type, "response.completed");
    assert.equal(toolCall.response.id, "resp_tool");
    assert.equal(toolCall.response.model, "gpt-test");
    assert.deepEqual(toolCall.response.output[0], {
      type: "function_call",
      call_id: "call_fake",
      name: "lookup",
      arguments: "{}",
    });
    responses.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "response.create",
      model: "openai/primary",
      previous_response_id: "resp_tool",
      input: [{ type: "function_call_output", call_id: "call_fake", output: "value" }],
    })));
    const completed = frameJson(await readOneServerTextFrame(responses.socket));
    assert.equal(completed.response.id, "resp_final");
    assert.equal(completed.response.model, "gpt-test");
    assert.equal(completed.response.output[0].content[0].text, "tool result accepted");
    const close = readAiCloseFrame(responses.socket, "Responses reciprocal close");
    responses.socket.write(encodeClientCloseFrame(1000, "done"));
    assert.deepEqual(await close, { code: 1000, reason: "done" });
  } finally {
    responses.socket.destroy();
  }

  const realtime = await wsHandshake(ns, "/ai/realtime-ws");
  try {
    assertStatus(realtime, 101, "Realtime WebSocket upgrade");
    realtime.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "session.update",
      session: { model: "xai/realtime", modalities: ["text", "audio"] },
    })));
    const updated = frameJson(await readOneServerTextFrame(realtime.socket));
    assert.equal(updated.type, "session.updated");
    assert.equal(updated.session.model, "grok-realtime-test");

    const binary = Buffer.from([0, 1, 127, 128, 255]);
    const echoed = readOneServerBinaryFrame(realtime.socket);
    realtime.socket.write(encodeClientBinaryFrame(binary));
    assert.deepEqual(await echoed, binary);
    const close = readAiCloseFrame(realtime.socket, "Realtime reciprocal close");
    realtime.socket.write(encodeClientCloseFrame(1000, "done"));
    assert.deepEqual(await close, { code: 1000, reason: "done" });
  } finally {
    realtime.socket.destroy();
  }

  const malformed = await wsHandshake(ns, "/ai/responses-ws");
  try {
    assertStatus(malformed, 101, "malformed Responses WebSocket upgrade");
    malformed.socket.write(encodeClientTextFrame("not JSON"));
    assert.deepEqual(await readAiCloseFrame(malformed.socket, "malformed frame close"), {
      code: 1008,
      reason: "AI websocket frame rejected",
    });
  } finally {
    malformed.socket.destroy();
  }

  const abandoned = await wsHandshake(ns, "/ai/responses-ws");
  assertStatus(abandoned, 101, "abandoned Responses WebSocket upgrade");
  await waitUntil("abandoned AI WebSocket permit acquired", () =>
    userRuntimeAiMetric("wdl_ai_pool_in_use", "websocket") === baseline + 1);
  abandoned.socket.destroy();
  await waitUntil("AI WebSocket permits released", () =>
    userRuntimeAiMetric("wdl_ai_pool_in_use", "websocket") === baseline,
  { timeoutMs: 10_000, intervalMs: 100 });
});

test("tenant intrinsic patches cannot bypass the host AI WebSocket model fence", async () => {
  const { ns } = await setupAiNamespace("ai-ws-intrinsic-guard");
  const response = await wsHandshake(ns, "/ai/responses-ws-intrinsic-guard");
  try {
    assertStatus(response, 101, "intrinsic guard Responses WebSocket upgrade");
    response.socket.write(encodeClientTextFrame(
      '{"type":"response.create","model":"attacker-model","model":"gpt-test"}'
    ));
    assert.deepEqual(await readAiCloseFrame(response.socket, "intrinsic guard frame close"), {
      code: 1008,
      reason: "AI websocket frame rejected",
    });
  } finally {
    response.socket.destroy();
  }
});

test("AI provider loss terminates the public WebSocket without Gateway session reset", async () => {
  const { ns } = await setupAiNamespace("ai-ws-provider-loss");
  const response = await wsHandshake(ns, "/ai/responses-ws-provider-loss");
  try {
    assertStatus(response, 101, "provider-loss Responses WebSocket upgrade");
    response.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "response.create",
      input: "provider loss",
      wdl_test_mode: "provider_loss",
    })));
    assert.deepEqual(await readOneServerCloseFrame(response.socket), {
      code: 1013,
      reason: "AI provider connection lost",
    });
  } finally {
    response.socket.destroy();
  }
});

test("tenant-bridged AI WebSocket preserves terminal policy after runtime loss", async () => {
  const { ns } = await setupAiNamespace("ai-ws-runtime-loss");
  const response = await wsHandshake(ns, "/ai/responses-ws-bridge");
  try {
    assertStatus(response, 101, "tenant-bridged Responses WebSocket upgrade");
    assert.equal(response.headers["x-wdl-websocket-reconnect-policy"], undefined);
    response.socket.write(encodeClientTextFrame(JSON.stringify({
      type: "response.create",
      input: "tenant bridge",
    })));
    const completed = frameJson(await readOneServerTextFrame(response.socket));
    assert.equal(completed.type, "response.completed");
    composeKill("user-runtime");
    assert.deepEqual(await readOneServerCloseFrame(response.socket, { timeoutMs: 10_000 }), {
      code: 1012,
      reason: "service restart",
    });
  } finally {
    response.socket.destroy();
    composeUp(["--wait", "user-runtime"], { stdio: "pipe" });
  }
  await waitUntil("user-runtime available after AI WebSocket replacement", async () => {
    try {
      return (await gatewayFetch(ns, "/ai/json?model=openai%2Fprimary")).status === 200;
    } catch {
      return false;
    }
  }, { timeoutMs: 10_000, intervalMs: 100 });
});

test("DO AI WebSocket terminates instead of replacing its provider session after do-runtime loss", async () => {
  const { ns } = await setupAiNamespace("ai-do-ws-runtime-loss");
  const response = await wsHandshake(ns, "/ai/do/responses-ws?name=runtime-loss");
  try {
    assertStatus(response, 101, "DO runtime-loss Responses WebSocket upgrade");
    composeKill("do-runtime");
    assert.deepEqual(await readOneServerCloseFrame(response.socket, { timeoutMs: 10_000 }), {
      code: 1012,
      reason: "service restart",
    });
  } finally {
    response.socket.destroy();
    await recreateDoSingleRuntime();
  }
});

test("official OpenAI SDK uses the AI Fetcher for Responses JSON, SSE, and cancellation", async () => {
  const ns = uniqueNs("ai-openai-sdk");
  const created = await adminPut("/ns/" + ns + "/ai/providers/openai", PROVIDERS.openai.record);
  assertStatus(created, 200, "SDK OpenAI provider create");
  const credential = await adminPut(`/ns/${ns}/ai/providers/openai/credential`, {
    revision: created.json.provider.revision,
    credential: PROVIDERS.openai.credential,
  });
  assertStatus(credential, 200, "SDK OpenAI credential create");
  await deployAndPromote(ns, "sdk", {
    mainModule: "worker.js",
    modules: { "worker.js": AI_OPENAI_SDK_WORKER },
    compatibilityDate: "2026-08-11",
    bindings: { AI: { type: "ai" } },
  });

  const surface = await readIntegrationJson(
    await gatewayFetch(ns, "/sdk/surface"),
    200,
    "AI-only facade surface"
  );
  assert.deepEqual(surface, {
    handler: { fetch: "function", run: "function", models: "function" },
    imported: { fetch: "function", run: "function", models: "function" },
  });

  const json = await readIntegrationJson(
    await gatewayFetch(ns, "/sdk/json"),
    200,
    "OpenAI SDK JSON"
  );
  assert.deepEqual(json, { id: "resp_fake", model: "gpt-test", status: "completed" });

  const stream = await readIntegrationJson(
    await gatewayFetch(ns, "/sdk/stream"),
    200,
    "OpenAI SDK stream"
  );
  assert.deepEqual(stream.eventTypes, [
    "response.created",
    "response.output_text.delta",
    "response.completed",
  ]);

  const aborted = await readIntegrationJson(
    await gatewayFetch(ns, "/sdk/abort"),
    200,
    "OpenAI SDK cancellation"
  );
  assert.deepEqual(aborted, {
    aborted: true,
    name: "Error",
    constructor: "APIUserAbortError",
    message: "Request was aborted.",
  });
});

test("DO AI calls complete and caller teardown releases the host permit", async () => {
  const { ns } = await setupAiNamespace("ai-do-teardown");
  const completed = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/do/json?name=teardown"),
    200,
    "DO AI request"
  );
  assert.equal(completed.model, "gpt-test");
  assert.equal(completed.status, "completed");

  const imported = await readIntegrationJson(
    await gatewayFetch(ns, "/ai/do/imported-json?name=teardown"),
    200,
    "DO imported AI facade request"
  );
  assert.equal(imported.model, "gpt-test");
  assert.equal(imported.status, "completed");

  const beforeGauge = doRuntimeAiMetric("wdl_ai_pool_in_use", "request");
  const beforeAcquired = doRuntimeAiMetric("wdl_ai_pool_events_total", "request", "acquired");
  const beforeCancelled = doRuntimeAiMetric("wdl_ai_pool_events_total", "request", "cancelled");
  const beforeReleased = doRuntimeRequestReleaseCount();

  const started = await gatewayFetch(ns, "/ai/do/start?name=teardown");
  assertStatus(started, 202, "DO AI request start");
  await waitUntil("DO AI request permit acquired", () =>
    doRuntimeAiMetric("wdl_ai_pool_in_use", "request") === beforeGauge + 1
  );

  await gatewayFetch(ns, "/ai/do/abort?name=teardown").catch(() => null);
  await waitUntil("DO AI request permit released after actor teardown", () =>
    doRuntimeAiMetric("wdl_ai_pool_in_use", "request") === beforeGauge,
  { timeoutMs: 10_000, intervalMs: 100 });

  assert.equal(
    doRuntimeAiMetric("wdl_ai_pool_events_total", "request", "acquired") - beforeAcquired,
    1
  );
  assert.equal(doRuntimeRequestReleaseCount() - beforeReleased, 1);
  assert.equal(
    doRuntimeAiMetric("wdl_ai_pool_events_total", "request", "cancelled") - beforeCancelled,
    1
  );
});
