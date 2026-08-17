import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  readRepositoryJson,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import {
  isValidAiModelAlias,
  isValidAiProviderName,
} from "../../shared/ns-pattern.js";
import { expectedAiProviderDestination } from "../../runtime/bindings/ai-provider.js";

/**
 * @typedef {{
 *   name: string,
 *   valid: boolean,
 *   deserializes?: boolean,
 *   value: Record<string, unknown>,
 * }} AiContractCase
 * @typedef {{
 *   limits: {
 *     providerMaxCount: number,
 *     modelsPerProviderMax: number,
 *     namespaceModelMaxCount: number,
 *     providerNameMaxBytes: number,
 *     providerRecordMaxBytes: number,
 *     upstreamModelMaxBytes: number,
 *     credentialMaxBytes: number,
 *     credentialEnvelopeMaxBytes: number,
 *   },
 *   boundaries: {
 *     providerNameLengths: Array<{ length: number, valid: boolean }>,
 *     modelAliasLengths: Array<{ length: number, valid: boolean }>,
 *     upstreamModels: Array<{
 *       name: string,
 *       unit: string,
 *       repeat: number,
 *       suffix: string,
 *       bytes: number,
 *       valid: boolean,
 *     }>,
 *     credentialLengths: Array<{ length: number, valid: boolean }>,
 *     providerModelCounts: Array<{ count: number, valid: boolean }>,
 *     modelsResponseCounts: Array<{ count: number, valid: boolean }>,
 *   },
 *   aliases: Array<{ value: string, provider: boolean, model: boolean }>,
 *   upstreamModels: Array<{ name: string, json: string, valid: boolean }>,
 *   destinations: Array<{ kind: string, protocol: string, transport: string, destination: string | null }>,
 *   providerRecords: AiContractCase[],
 *   resolveRequests: AiContractCase[],
 *   modelsRequests: AiContractCase[],
 *   resolveResponses: AiContractCase[],
 *   modelsResponses: AiContractCase[],
 * }} AiContractFixture
 */

const fixture = /** @type {AiContractFixture} */ (
  readRepositoryJson("tests/fixtures/ai-contract.json")
);
const aiContract = await importRepositoryModule("shared/ai-contract.js", importSpecifierReplacements({
  "shared-ns-pattern": repositoryFileUrl("shared/ns-pattern.js"),
  "shared-utf8": repositoryFileUrl("shared/utf8.js"),
}));
const capabilities = {
  functionTools: false,
  structuredOutput: false,
  reasoning: false,
  previousResponseId: false,
  providerTools: false,
  binaryFrames: false,
};

/** @param {string} upstreamModel */
function descriptor(upstreamModel) {
  return {
    upstreamModel,
    protocol: "responses",
    transports: ["http"],
    inputModalities: ["text"],
    outputModalities: ["text"],
    capabilities,
  };
}

/** @param {number} count @param {string} [upstreamModel] */
function providerRecord(count, upstreamModel = "model") {
  return {
    revision: "0".repeat(32),
    kind: "openai",
    models: Object.fromEntries(Array.from({ length: count }, (_, index) => [
      `m${String(index).padStart(2, "0")}`,
      descriptor(upstreamModel),
    ])),
  };
}

/** @param {string} upstreamModel @param {string} [credential] */
function resolveResponse(upstreamModel, credential = "token") {
  return {
    provider: "openai",
    alias: "primary",
    kind: "openai",
    upstreamModel,
    protocol: "responses",
    transport: "http",
    destination: "https://api.openai.com/v1/responses",
    credential,
    inputModalities: ["text"],
    capabilities,
  };
}

/** @param {number} count */
function modelsResponse(count) {
  return {
    models: Array.from({ length: count }, (_, index) => ({
      id: `p${String(Math.floor(index / 32)).padStart(2, "0")}/m${String(index % 32).padStart(2, "0")}`,
      protocol: "responses",
      transports: ["http"],
      inputModalities: ["text"],
      outputModalities: ["text"],
      capabilities,
    })),
  };
}

test("AI persisted limits match the cross-language fixture", () => {
  assert.deepEqual(fixture.limits, {
    providerMaxCount: aiContract.AI_PROVIDER_MAX_COUNT,
    modelsPerProviderMax: aiContract.AI_MODELS_PER_PROVIDER_MAX,
    namespaceModelMaxCount: aiContract.AI_NAMESPACE_MODEL_MAX_COUNT,
    providerNameMaxBytes: aiContract.AI_PROVIDER_NAME_MAX_BYTES,
    providerRecordMaxBytes: aiContract.AI_PROVIDER_RECORD_MAX_BYTES,
    upstreamModelMaxBytes: aiContract.AI_UPSTREAM_MODEL_MAX_BYTES,
    credentialMaxBytes: aiContract.AI_CREDENTIAL_MAX_BYTES,
    credentialEnvelopeMaxBytes: aiContract.AI_CREDENTIAL_ENVELOPE_MAX_BYTES,
  });
});

test("AI aliases match the cross-language fixture", () => {
  for (const item of fixture.aliases) {
    assert.equal(isValidAiProviderName(item.value), item.provider, item.value);
    assert.equal(isValidAiModelAlias(item.value), item.model, item.value);
  }
});

test("AI size and count boundaries match the cross-language fixture", () => {
  for (const item of fixture.boundaries.providerNameLengths) {
    assert.equal(isValidAiProviderName("p".repeat(item.length)), item.valid);
  }
  for (const item of fixture.boundaries.modelAliasLengths) {
    assert.equal(isValidAiModelAlias("m".repeat(item.length)), item.valid);
  }
  for (const item of fixture.boundaries.upstreamModels) {
    const value = item.unit.repeat(item.repeat) + item.suffix;
    assert.equal(new TextEncoder().encode(value).byteLength, item.bytes, item.name);
    const provider = () => aiContract.normalizeAiProviderRecord(providerRecord(1, value));
    const response = () => aiContract.normalizeAiResolveResponse(resolveResponse(value));
    if (item.valid) {
      assert.equal(provider().models.m00.upstreamModel, value, item.name);
      assert.equal(response().upstreamModel, value, item.name);
    } else {
      assert.throws(provider, Error, item.name);
      assert.throws(response, Error, item.name);
    }
  }
  for (const item of fixture.boundaries.credentialLengths) {
    const credential = "x".repeat(item.length);
    const parse = () => aiContract.normalizeAiResolveResponse(resolveResponse("model", credential));
    if (item.valid) assert.equal(parse().credential, credential);
    else assert.throws(parse);
  }
  for (const item of fixture.boundaries.providerModelCounts) {
    const parse = () => aiContract.normalizeAiProviderRecord(providerRecord(item.count));
    if (item.valid) assert.equal(Object.keys(parse().models).length, item.count);
    else assert.throws(parse);
  }
  for (const item of fixture.boundaries.modelsResponseCounts) {
    const parse = () => aiContract.normalizeAiModelsResponse(modelsResponse(item.count));
    if (item.valid) assert.equal(parse().models.length, item.count);
    else assert.throws(parse);
  }
});

test("AI official destinations match the cross-language fixture", () => {
  const protocolTransports = {
    responses: ["http", "sse", "responses_websocket"],
    chat_completions: ["http", "sse"],
    embeddings: ["http"],
    realtime: ["realtime_websocket"],
  };
  const expectedCases = new Set();
  for (const kind of aiContract.AI_PROVIDER_KINDS) {
    for (const [protocol, transports] of Object.entries(protocolTransports)) {
      for (const transport of transports) expectedCases.add(`${kind}/${protocol}/${transport}`);
    }
  }
  assert.deepEqual(
    new Set(fixture.destinations.map(
      ({ kind, protocol, transport }) => `${kind}/${protocol}/${transport}`
    )),
    expectedCases,
    "destination fixture must cover every legal provider/protocol/transport combination"
  );
  for (const item of fixture.destinations) {
    assert.equal(
      expectedAiProviderDestination(item.kind, item.protocol, item.transport),
      item.destination,
      `${item.kind}/${item.protocol}/${item.transport}`
    );
  }
});

test("AI provider records match the cross-language fixture", () => {
  for (const item of fixture.providerRecords) {
    const parse = () => aiContract.normalizeAiProviderRecord(item.value);
    if (item.valid) {
      const record = parse();
      assert.equal(record.revision, item.value.revision);
      assert.equal(record.kind, item.value.kind);
      assert.deepEqual(
        Object.keys(record.models),
        Object.keys(/** @type {Record<string, unknown>} */ (item.value.models)).toSorted()
      );
      assert.equal(JSON.stringify(record), JSON.stringify(item.value), item.name);
    } else {
      assert.throws(parse, Error, item.name);
    }
  }
});

test("AI upstream model strings match the cross-language fixture", () => {
  for (const item of fixture.upstreamModels) {
    const upstreamModel = JSON.parse(item.json);
    const parse = () => aiContract.normalizeAiProviderWrite({
      kind: "openai",
      models: {
        primary: {
          upstreamModel,
          protocol: "responses",
          transports: ["http"],
        },
      },
    }, "0".repeat(32));
    if (item.valid) assert.equal(parse().models.primary.upstreamModel, upstreamModel, item.name);
    else assert.throws(parse, Error, item.name);
  }
});

test("AI resolve and model-list requests match the cross-language fixture", () => {
  for (const item of fixture.resolveRequests) {
    const parse = () => aiContract.normalizeAiResolveRequest(item.value);
    if (item.valid) assert.deepEqual(parse(), item.value);
    else assert.throws(parse);
  }
  for (const item of fixture.modelsRequests) {
    const parse = () => aiContract.normalizeAiModelsRequest(item.value);
    if (item.valid) assert.deepEqual(parse(), item.value);
    else assert.throws(parse);
  }
  for (const item of fixture.resolveResponses) {
    const parse = () => aiContract.normalizeAiResolveResponse(item.value);
    if (item.valid) assert.equal(parse().destination, item.value.destination);
    else assert.throws(parse);
  }
  for (const item of fixture.modelsResponses) {
    const parse = () => aiContract.normalizeAiModelsResponse(item.value);
    if (item.valid) {
      const expected = /** @type {{ models: Array<{ id: string }> }} */ (item.value);
      assert.deepEqual(
        parse().models.map((/** @type {{ id: string }} */ { id }) => id),
        expected.models.map(({ id }) => id)
      );
    }
    else assert.throws(parse);
  }
});

test("AI provider writes generate canonical records without accepting revision input", () => {
  const found = fixture.providerRecords.find((item) => item.name === "responses");
  assert.ok(found);
  const input = found.value;
  const { revision: _revision, ...write } = input;
  assert.equal(typeof input.revision, "string");
  assert.equal(
    aiContract.normalizeAiProviderWrite(write, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").revision,
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  );
  assert.throws(
    () => aiContract.normalizeAiProviderWrite(input, /** @type {string} */ (input.revision)),
    /revision/
  );
});
