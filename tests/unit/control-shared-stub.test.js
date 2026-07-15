import { test } from "node:test";
import assert from "node:assert/strict";
import {
  jsonError as productionJsonError,
  jsonResponse as productionJsonResponse,
  sanitizeJsonErrorDetails as productionSanitizeJsonErrorDetails,
} from "../../shared/respond.js";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import { readJsonResponse } from "../helpers/response-json.js";

const stub = await import(controlSharedStubUrl());

test("control shared stub re-exports the production JSON response contract", () => {
  assert.equal(stub.jsonError, productionJsonError);
  assert.equal(stub.jsonResponse, productionJsonResponse);
  assert.equal(stub.sanitizeJsonErrorDetails, productionSanitizeJsonErrorDetails);
});

test("control shared stub enforces streamed request limits through shared bounded-body", async () => {
  let canceled = false;
  const requestInit = /** @type {RequestInit} */ (/** @type {unknown} */ ({
    method: "POST",
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"a":'));
        controller.enqueue(new TextEncoder().encode('"too large"}'));
      },
      cancel() {
        canceled = true;
      },
    }),
    duplex: "half",
  }));
  const request = new Request("http://control.test/", requestInit);

  const result = await stub.readJsonBody(request, { maxBytes: 8 });
  assert.ok("response" in result);
  await readJsonResponse(result.response, 413);
  assert.equal(canceled, true);
});

test("control shared stub matches production internal-auth header validation", async () => {
  const missing = await import(controlSharedStubUrl("export const state = { env: {} };"));
  assert.throws(
    () => missing.controlInternalJsonHeaders(),
    /WDL_INTERNAL_AUTH_TOKEN must be configured/,
  );

  const invalid = await import(controlSharedStubUrl(`
    export const state = { env: { WDL_INTERNAL_AUTH_TOKEN: "bad token" } };
  `));
  assert.throws(
    () => invalid.controlInternalJsonHeaders(),
    /visible ASCII without whitespace or commas/,
  );

  const valid = await import(controlSharedStubUrl(`
    export const state = { env: { WDL_INTERNAL_AUTH_TOKEN: "test-token" } };
  `));
  const headers = valid.controlInternalJsonHeaders();
  assert.equal(headers.get("content-type"), "application/json");
  assert.equal(headers.get("x-wdl-internal-auth"), "test-token");
});
