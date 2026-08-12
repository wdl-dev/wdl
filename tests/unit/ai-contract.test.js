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

/**
 * @typedef {{ name: string, valid: boolean, value: Record<string, unknown> }} AiContractCase
 * @typedef {{
 *   aliases: Array<{ value: string, provider: boolean, model: boolean }>,
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

test("AI aliases match the cross-language fixture", () => {
  for (const item of fixture.aliases) {
    assert.equal(isValidAiProviderName(item.value), item.provider, item.value);
    assert.equal(isValidAiModelAlias(item.value), item.model, item.value);
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
