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
 * @typedef {{ name: string, valid: boolean, value: Record<string, unknown> }} AiContractCase
 * @typedef {{
 *   limits: {
 *     providerMaxCount: number,
 *     modelsPerProviderMax: number,
 *     namespaceModelMaxCount: number,
 *     providerRecordMaxBytes: number,
 *     upstreamModelMaxBytes: number,
 *     credentialMaxBytes: number,
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
}));

test("AI persisted limits match the cross-language fixture", () => {
  assert.deepEqual(fixture.limits, {
    providerMaxCount: aiContract.AI_PROVIDER_MAX_COUNT,
    modelsPerProviderMax: aiContract.AI_MODELS_PER_PROVIDER_MAX,
    namespaceModelMaxCount: aiContract.AI_NAMESPACE_MODEL_MAX_COUNT,
    providerRecordMaxBytes: aiContract.AI_PROVIDER_RECORD_MAX_BYTES,
    upstreamModelMaxBytes: aiContract.AI_UPSTREAM_MODEL_MAX_BYTES,
    credentialMaxBytes: aiContract.AI_CREDENTIAL_MAX_BYTES,
  });
});

test("AI aliases match the cross-language fixture", () => {
  for (const item of fixture.aliases) {
    assert.equal(isValidAiProviderName(item.value), item.provider, item.value);
    assert.equal(isValidAiModelAlias(item.value), item.model, item.value);
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
      assert.deepEqual(
        parse().models.map((/** @type {{ id: string }} */ { id }) => id),
        ["openai/primary"]
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
