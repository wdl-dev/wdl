import assert from "node:assert/strict";
import { test } from "node:test";
import {
  KV_ID_RE,
  QUEUE_NAME_RE,
  WORKER_NAME_RE,
  WORKFLOW_INSTANCE_ID_RE,
  isValidRouteNs,
  isValidRuntimeLoadNs,
  isValidTenantNs,
} from "../../shared/ns-pattern.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";

const IDENTITY_CASES = readRepositoryJson("tests/fixtures/cross-language-identity.json");

/**
 * @param {string} key
 * @param {(value: string) => boolean} predicate
 */
function assertIdentityCases(key, predicate) {
  for (const entry of IDENTITY_CASES[key]) {
    assert.equal(predicate(entry.value), entry.valid, `${key}:${JSON.stringify(entry.value)}`);
  }
}

test("cross-language identity grammar fixture matches JS owners", () => {
  assertIdentityCases("tenantNs", isValidTenantNs);
  assertIdentityCases("routeNs", isValidRouteNs);
  assertIdentityCases("runtimeLoadNs", isValidRuntimeLoadNs);
  assertIdentityCases("workerNames", (value) => WORKER_NAME_RE.test(value));
  assertIdentityCases("queueNames", (value) => QUEUE_NAME_RE.test(value));
  assertIdentityCases("kvIds", (value) => KV_ID_RE.test(value));
  assertIdentityCases("workflowInstanceIds", (value) => WORKFLOW_INSTANCE_ID_RE.test(value));
});
