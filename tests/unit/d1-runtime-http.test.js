import { test } from "node:test";
import assert from "node:assert/strict";
import {
  repositoryFileUrl,
  repositoryModuleDataUrl,
  importRepositoryModule,
} from "../helpers/load-shared-module.js";
import { d1TransportDataUrl } from "../helpers/load-d1-protocol.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const d1QueryWireUrl = repositoryModuleDataUrl("shared/d1-query-wire.js", [
  [/from "shared-d1-params";/, `from ${JSON.stringify(repositoryFileUrl("shared/d1-params.js"))};`],
  [/from "shared-d1-data-field";/, `from ${JSON.stringify(repositoryFileUrl("shared/d1-data-field.js"))};`],
]);
const { decodeD1QueryResponse } = await import(d1QueryWireUrl);
const { d1QuerySuccessResponse, jsonError } = await importRepositoryModule("d1-runtime/http.js", [
  [
    /from "shared-d1-transport";/,
    `from ${JSON.stringify(d1TransportDataUrl())};`
  ],
  [/from "shared-d1-query-wire";/, `from ${JSON.stringify(d1QueryWireUrl)};`],
  [/from "shared-respond";/, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
]);

test("D1 runtime success responses carry native blob values", async () => {
  const response = d1QuerySuccessResponse({
    success: true,
    results: [{ data: new Uint8Array([0, 1, 2, 255]) }],
  });

  assert.equal(response.headers.get("x-wdl-d1-result"), "ok");
  assert.equal(response.headers.get("x-wdl-d1-changed-db"), "0");
  assert.equal(response.headers.get("x-wdl-d1-value-encoding"), "native-bytes-v1");
  const payload = /** @type {any} */ (
    decodeD1QueryResponse(new Uint8Array(await response.arrayBuffer()))
  );
  assert.ok(payload.results[0].data instanceof Uint8Array);
  assert.deepEqual(Array.from(payload.results[0].data), [0, 1, 2, 255]);
});

test("D1 runtime jsonError strips top-level reserved detail keys", async () => {
  const response = jsonError(409, "d1_lock_lost", "D1 lock was lost", {
    error: "detail_code",
    message: "detail message",
    reason: "legacy reason",
    databaseId: "main",
    nested: {
      error: "nested_code",
      message: "nested message",
      kept: "yes",
    },
  });

  await assertJsonResponse(response, 409, {
    databaseId: "main",
    nested: {
      error: "nested_code",
      message: "nested message",
      kept: "yes",
    },
    error: "d1_lock_lost",
    message: "D1 lock was lost",
  });
});
