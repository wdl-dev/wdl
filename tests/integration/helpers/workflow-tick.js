import assert from "node:assert/strict";
import { readRepositoryJson } from "../../helpers/load-shared-module.js";

const WORKFLOW_TICK_RESPONSE_FIXTURE = readRepositoryJson(
  "tests/fixtures/workflow-tick-response.json"
);
const WORKFLOW_TICK_RESPONSE_FIELDS = Object.entries(WORKFLOW_TICK_RESPONSE_FIXTURE);

/** @param {unknown} value @returns {Record<string, number | boolean>} */
function workflowTickResponse(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    assert.fail("workflow tick response must be an object");
  }
  const response = /** @type {Record<string, unknown>} */ (value);
  assert.deepEqual(
    Object.keys(response).sort(),
    WORKFLOW_TICK_RESPONSE_FIELDS.map(([field]) => field).sort()
  );
  for (const [field, example] of WORKFLOW_TICK_RESPONSE_FIELDS) {
    const actual = response[field];
    if (typeof example === "boolean") {
      assert.equal(typeof actual, "boolean", field);
    } else {
      assert.equal(
        typeof actual === "number" && Number.isSafeInteger(actual) && actual >= 0,
        true,
        field
      );
    }
  }
  return /** @type {Record<string, number | boolean>} */ (response);
}

/** @param {unknown} value @param {string} field */
export function workflowTickCount(value, field) {
  assert.equal(typeof WORKFLOW_TICK_RESPONSE_FIXTURE[field], "number", field);
  const count = workflowTickResponse(value)[field];
  assert.equal(typeof count, "number", field);
  return /** @type {number} */ (count);
}
