import { test } from "node:test";
import assert from "node:assert/strict";
import {
  discardResponseBody,
  echoResponseWithRequestId,
  internalErrorResponse,
  jsonError,
  jsonErrorWith,
  jsonInitResponse,
  jsonResponse,
  prometheusResponse,
  rebuildResponseWithHeaders,
  sanitizeJsonErrorDetails,
} from "../../shared/respond.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

const OBSERVABILITY_CONTRACT = readRepositoryJson("tests/fixtures/observability-contract.json");

/** @param {Promise<unknown>} promise */
async function settlesWithinTestWindow(promise) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise.then(() => true),
      new Promise((resolve) => {
        timeout = setTimeout(() => resolve(false), 100);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

test("discardResponseBody cancels response bodies", async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({
    cancel() {
      cancelled = true;
    },
  }));

  await discardResponseBody(response);

  assert.equal(cancelled, true);
});

test("discardResponseBody ignores cancellation failures", async () => {
  const response = new Response(new ReadableStream({
    cancel() {
      throw new Error("cancel failed");
    },
  }));

  await discardResponseBody(response);
});

test("discardResponseBody does not wait for cancellation cleanup", async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({
    cancel() {
      cancelled = true;
      return new Promise(() => {});
    },
  }));

  const settled = await settlesWithinTestWindow(discardResponseBody(response));

  assert.equal(settled, true);
  assert.equal(cancelled, true);
});

// Node's Response doesn't accept `webSocket` in init, so the 101 branch
// is covered end-to-end by tests/integration/gateway-websocket.test.js.
test("echoResponseWithRequestId preserves status/body/headers and stamps x-request-id", async () => {
  const src = new Response("hello", {
    status: 418,
    headers: { "content-type": "text/plain", "x-upstream": "yes" },
  });
  const out = echoResponseWithRequestId(src, "rid-abc");
  assert.equal(out.status, 418);
  assert.equal(out.headers.get("content-type"), "text/plain");
  assert.equal(out.headers.get("x-upstream"), "yes");
  assert.equal(out.headers.get("x-request-id"), "rid-abc");
  assert.equal(await out.text(), "hello");
});

test("echoResponseWithRequestId overwrites a pre-existing x-request-id", () => {
  const src = new Response(null, {
    status: 200,
    headers: { "x-request-id": "upstream-id" },
  });
  const out = echoResponseWithRequestId(src, "ours-id");
  assert.equal(out.headers.get("x-request-id"), "ours-id");
});

test("echoResponseWithRequestId filters copied headers before stamping the request id", () => {
  const src = new Response(null, {
    headers: {
      "x-private": "hidden",
      "x-public": "visible",
      "x-request-id": "upstream-id",
    },
  });
  const out = echoResponseWithRequestId(src, "ours-id", (headers) => {
    headers.delete("x-private");
    headers.delete("x-request-id");
  });

  assert.equal(out.headers.get("x-private"), null);
  assert.equal(out.headers.get("x-public"), "visible");
  assert.equal(out.headers.get("x-request-id"), "ours-id");
});

test("rebuildResponseWithHeaders preserves response fields while replacing headers", async () => {
  const src = new Response("hello", {
    status: 202,
    statusText: "Accepted",
    headers: { "x-old": "hidden" },
  });
  const out = rebuildResponseWithHeaders(src, { "x-new": "visible" });

  assert.equal(out.status, 202);
  assert.equal(out.statusText, "Accepted");
  assert.equal(out.headers.get("x-old"), null);
  assert.equal(out.headers.get("x-new"), "visible");
  assert.equal(await out.text(), "hello");
});

test("jsonResponse returns JSON with the canonical content-type", async () => {
  const out = jsonResponse(201, { ok: true });
  assert.equal(out.headers.get("content-type"), "application/json");
  await assertJsonResponse(out, 201, { ok: true });
});

test("jsonInitResponse applies ResponseInit options and keeps canonical content-type", async () => {
  const out = jsonInitResponse({ ok: true }, {
    status: 202,
    statusText: "Accepted",
    headers: {
      "content-type": "text/plain",
      "x-extra": "yes",
    },
  });

  assert.equal(out.statusText, "Accepted");
  assert.equal(out.headers.get("content-type"), "application/json");
  assert.equal(out.headers.get("x-extra"), "yes");
  await assertJsonResponse(out, 202, { ok: true });
});

test("jsonError returns machine error plus human message and details", async () => {
  const out = jsonError(409, "route_conflict", "Route is already owned", {
    host: "demo.workers.example",
  });
  await assertJsonResponse(out, 409, {
    host: "demo.workers.example",
    error: "route_conflict",
    message: "Route is already owned",
  });
});

test("jsonError does not let top-level details override machine error or human message", async () => {
  const out = jsonError(400, "invalid_request", "Bad request", {
    error: "human sentence",
    message: "overridden",
    reason: "legacy_reason",
    field: "name",
    nested: {
      error: "nested error",
      message: "nested message",
      kept: true,
    },
  });
  await assertJsonResponse(out, 400, {
    field: "name",
    nested: {
      error: "nested error",
      message: "nested message",
      kept: true,
    },
    error: "invalid_request",
    message: "Bad request",
  });
});

test("jsonErrorWith preserves empty messages through a custom JSON function", async () => {
  const out = jsonErrorWith(jsonInitResponse, 400, "invalid_request", "", {
    field: "name",
  });

  assert.equal(out.headers.get("content-type"), "application/json");
  await assertJsonResponse(out, 400, {
    field: "name",
    error: "invalid_request",
    message: "",
  });
});

test("jsonError preserves magic detail keys as data fields", async () => {
  const details = JSON.parse('{"__proto__":"detail-value","nested":{"__proto__":"nested-value"}}');
  const out = jsonError(400, "invalid_request", "Bad request", details);
  const body = await readJsonResponse(out, 400);

  assert.equal(body.__proto__, "detail-value");
  assert.equal(body.nested.__proto__, "nested-value");
  assert.equal(body.error, "invalid_request");
});

test("sanitizeJsonErrorDetails preserves non-record details", () => {
  assert.deepEqual(sanitizeJsonErrorDetails([
    { error: "item_error", message: "item message" },
  ]), [
    { error: "item_error", message: "item message" },
  ]);
});

test("jsonError omits empty messages", async () => {
  const out = jsonError(500, "internal_error", "", { request_id: "rid" });
  await assertJsonResponse(out, 500, {
    request_id: "rid",
    error: "internal_error",
  });
});

test("internalErrorResponse returns generic message plus request id", async () => {
  const out = internalErrorResponse(502, "gateway_error", "Gateway error", "rid-123");
  await assertJsonResponse(out, 502, {
    request_id: "rid-123",
    error: "gateway_error",
    message: "Gateway error",
  });
});

test("prometheusResponse renders a Prometheus text response", async () => {
  const out = prometheusResponse({ renderPrometheus: () => "# HELP x\n" });
  assert.equal(out.status, 200);
  assert.equal(
    out.headers.get("content-type"),
    OBSERVABILITY_CONTRACT.prometheusContentType
  );
  assert.equal(await out.text(), "# HELP x\n");
});
