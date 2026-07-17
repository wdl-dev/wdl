import { test } from "node:test";
import assert from "node:assert/strict";
import { loadControlD1RuntimeClient, loadD1QueryWire } from "../helpers/load-d1-protocol.js";
import { withRecordingFetch } from "../helpers/mock-fetch.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const {
  decodeD1QueryRequest,
  D1_QUERY_CONTENT_TYPE,
  encodeD1QueryResponse,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
} = await loadD1QueryWire();
const {
  d1RuntimeFailure,
  d1RuntimeFailureLogFields,
  d1RuntimePublicResult,
  d1RuntimeQuery,
  d1RuntimeReleaseOwner,
} = await loadControlD1RuntimeClient();

/** @template {Record<string, unknown>} T @param {T} env */
function withInternalAuthEnv(env) {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN, ...env };
}

/**
 * @param {any} body
 * @param {ResponseInit} [init]
 */
function d1Response(body, init = {}) {
  return new Response(encodeD1QueryResponse(body), {
    ...init,
    headers: {
      "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE,
      .../** @type {Record<string, string>} */ (init.headers || {}),
    },
  });
}

test("control D1 runtime client query sends request timeout signal", async () => {
  /** @type {any[]} */
  const calls = [];
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_QUERY_TIMEOUT_MS: "1234",
    D1_BACKEND: {
      /** @param {any} _url @param {any} init */
      async fetch(_url, init) {
        calls.push(init);
        return d1Response(
          { success: true, results: [{ ok: 1 }] },
          {
            headers: {
              "x-wdl-d1-owner-task-id": "task-a",
              "x-wdl-d1-owner-endpoint": "10.0.0.9:8787",
              "x-wdl-d1-owner-generation": "7",
            },
          }
        );
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }], "rid-control");

  assert.equal(result.ok, true);
  assert.ok(calls[0].signal instanceof AbortSignal);
  assert.equal(new Headers(calls[0].headers).get("x-request-id"), "rid-control");
  assert.equal(new Headers(calls[0].headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
  assert.equal(new Headers(calls[0].headers).get("content-type"), D1_QUERY_CONTENT_TYPE);
  assert.equal(decodeD1QueryRequest(calls[0].body).databaseId, "db1");
  assert.deepEqual(result.owner, {
    taskId: "task-a",
    endpoint: "10.0.0.9:8787",
    generation: 7,
  });
});

test("control D1 runtime client rejects non-positive and unsafe owner generation hints", async () => {
  for (const rawGeneration of ["0", "9007199254740992"]) {
    const result = await d1RuntimeQuery(withInternalAuthEnv({
      D1_BACKEND: {
        async fetch() {
          return d1Response(
            { success: true, results: [] },
            {
              headers: {
                "x-wdl-d1-owner-task-id": "task-a",
                "x-wdl-d1-owner-endpoint": "10.0.0.9:8787",
                "x-wdl-d1-owner-generation": rawGeneration,
              },
            }
          );
        },
      },
    }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

    assert.equal(result.ok, true);
    assert.equal(result.owner, null, rawGeneration);
  }
});

test("control D1 runtime client rejects fractional owner generation hints", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_BACKEND: {
      async fetch() {
        return d1Response(
          { success: true, results: [] },
          {
            headers: {
              "x-wdl-d1-owner-task-id": "task-a",
              "x-wdl-d1-owner-endpoint": "10.0.0.9:8787",
              "x-wdl-d1-owner-generation": "1.5",
            },
          }
        );
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, true);
  assert.equal(result.owner, null);
});

test("control D1 runtime client query maps transport timeout to stable response", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_QUERY_TIMEOUT_MS: "1",
    D1_BACKEND: {
      /** @param {any} _url @param {any} init */
      async fetch(_url, init) {
        return await new Promise((_, reject) => {
          init.signal.addEventListener("abort", () => {
            reject(init.signal.reason || new DOMException("operation timed out", "TimeoutError"));
          }, { once: true });
        });
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, false);
  assert.equal(result.status, 504);
  assert.equal(result.body.error, "timeout");
  assert.match(result.body.message, /timed out/);
  assert.equal(result.body.category, "timeout");
  assert.equal(result.body.retryable, false);
});

test("control D1 runtime client query maps transport failure to shared unavailable response", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_BACKEND: {
      async fetch() {
        throw new Error("connect ECONNREFUSED");
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, false);
  assert.equal(result.status, 503);
  assert.equal(result.body.error, "backend-unavailable");
  assert.equal(result.body.category, "internal");
  assert.equal(result.body.retryable, true);
  assert.equal(result.body.message, "D1 backend is unavailable.");
});

test("control D1 runtime client query treats unsupported response content-type as upstream failure", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_BACKEND: {
      async fetch() {
        return Response.json({ success: true });
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, false);
  assert.equal(result.status, 502);
  assert.equal(result.body.error, "invalid_d1_runtime_response");
  assert.match(result.body.message, /unsupported d1-runtime response content-type/);
  assert.equal(result.body.category, "invalid-response");
  assert.equal(result.body.retryable, false);
  assert.equal(result.body.causeCode, "unsupported-content-type");
  assert.equal(result.owner, null);
  assert.deepEqual(d1RuntimeFailureLogFields(result), {
    upstream_status: 502,
    upstream_code: "invalid_d1_runtime_response",
    upstream_category: "invalid-response",
    upstream_retryable: false,
    upstream_cause_code: "unsupported-content-type",
  });
});

test("control D1 runtime client query treats undecodable binary response as upstream failure", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_BACKEND: {
      async fetch() {
        return new Response(new Uint8Array([0x80]), {
          status: 200,
          headers: {
            "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE,
            "x-wdl-d1-owner-task-id": "task-a",
            "x-wdl-d1-owner-endpoint": "10.0.0.9:8787",
            "x-wdl-d1-owner-generation": "7",
          },
        });
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, false);
  assert.equal(result.status, 502);
  assert.equal(result.body.error, "invalid_d1_runtime_response");
  assert.equal(result.body.category, "invalid-response");
  assert.equal(result.body.retryable, false);
  assert.equal(result.body.causeCode, "binary-decode-failed");
  assert.equal(result.owner, null);
});

test("control D1 runtime client release posts directly to owner endpoint", async () => {
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const calls = [];
  await withRecordingFetch(calls, async () => {
    const result = await d1RuntimeReleaseOwner(
      { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN },
      "tenant-a",
      "db1",
      { taskId: "task-a", endpoint: "10.0.0.9:8787", generation: 7 },
      "rid-release"
    );

    assert.equal(result.ok, true);
    assert.equal(calls[0].url, "http://10.0.0.9:8787/internal/d1/rebalance");
    assert.equal(new Headers(calls[0].init.headers).get("x-request-id"), "rid-release");
    assert.equal(new Headers(calls[0].init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
    assert.deepEqual(parseJsonObjectRequestBody(calls[0].init, "D1 release request body"), {
      databases: [{ namespace: "tenant-a", databaseId: "db1" }],
      target: null,
    });
  }, {
    response: Response.json({ results: [{ outcome: "released" }] }),
  });
});

test("control D1 runtime client release rejects invalid owner endpoints before fetch", async () => {
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const calls = [];
  await withRecordingFetch(calls, async () => {
    const result = await d1RuntimeReleaseOwner(
      { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN },
      "tenant-a",
      "db1",
      { taskId: "task-a", endpoint: "metadata.google.internal:8787", generation: 7 },
      "rid-release"
    );

    assert.equal(result.ok, false);
    assert.equal(result.status, 0);
    assert.equal(result.body.error, "owner-endpoint-invalid");
    assert.deepEqual(calls, []);
  });
});

test("control D1 runtime client public result exposes tagged BLOB as JSON-safe object", () => {
  assert.deepEqual(
    d1RuntimePublicResult({
      success: true,
      results: [{ data: { __wdl_d1_binary_v1: true, base64: "AAEC/w==" } }],
    }),
    {
      success: true,
      results: [{ data: { type: "blob", base64: "AAEC/w==", byteLength: 4 } }],
    }
  );
});

test("control D1 runtime client public result normalizes row/column payloads", () => {
  assert.deepEqual(
    d1RuntimePublicResult([
      {
        success: true,
        results: { columns: ["id", "body"], rows: [["m1", "hello"]] },
        meta: { rows_read: 1 },
      },
      {
        success: true,
        results: { columns: ["id"], rows: [] },
        meta: { rows_read: 0 },
      },
    ]),
    [
      {
        success: true,
        results: [{ id: "m1", body: "hello" }],
        meta: { rows_read: 1 },
      },
      {
        success: true,
        results: [],
        meta: { rows_read: 0 },
      },
    ]
  );
});

test("control D1 runtime client public result preserves raw row/column payloads", () => {
  assert.deepEqual(
    d1RuntimePublicResult({
      success: true,
      results: { columns: ["id", "id"], rows: [["m1", "shadow"]] },
      meta: { rows_read: 1 },
    }, "raw"),
    {
      success: true,
      results: { columns: ["id", "id"], rows: [["m1", "shadow"]] },
      meta: { rows_read: 1 },
    }
  );
});

test("control D1 runtime failure redacts 5xx diagnostics and keeps safe extra fields", () => {
  const body = d1RuntimeFailure("d1_execute_failed", "demo", "db1", {
    status: 502,
    body: {
      error: "result-unknown",
      message: "commit reply was lost",
      category: "result-unknown",
      retryable: false,
    },
  }, {
    error: "wrong",
    message: "wrong",
    reason: "wrong",
    namespace: "wrong-ns",
    upstreamStatus: 999,
    diagnostic: "kept",
  });

  assert.deepEqual(body, {
    diagnostic: "kept",
    error: "d1_execute_failed",
    namespace: "demo",
    databaseId: "db1",
    message: "Internal error",
    upstreamCode: "result-unknown",
    upstreamCategory: "result-unknown",
    upstreamRetryable: false,
    upstreamStatus: 502,
  });
  assert.deepEqual(d1RuntimeFailureLogFields({
    status: 502,
    body: {
      error: "result-unknown",
      message: "commit reply was lost",
      category: "result-unknown",
      retryable: false,
    },
  }), {
    upstream_status: 502,
    upstream_code: "result-unknown",
    upstream_category: "result-unknown",
    upstream_retryable: false,
  });
});

test("control D1 runtime failure exposes only bounded machine classifiers", () => {
  const result = {
    status: 503,
    body: {
      error: "panic at /srv/d1-runtime/actor.rs:42",
      category: `internal-${"x".repeat(128)}`,
      causeCode: "backend\ntrace",
      retryable: true,
    },
  };

  assert.deepEqual(d1RuntimeFailure("d1_execute_failed", "demo", "db1", result), {
    error: "d1_execute_failed",
    namespace: "demo",
    databaseId: "db1",
    message: "Internal error",
    upstreamCode: "d1-runtime-error",
    upstreamCategory: "internal",
    upstreamRetryable: true,
    upstreamStatus: 503,
  });
  assert.deepEqual(d1RuntimeFailureLogFields(result), {
    upstream_status: 503,
    upstream_code: "d1-runtime-error",
    upstream_category: "internal",
    upstream_retryable: true,
  });
});

test("control D1 runtime failure uses the outward status to redact upstream 4xx details", () => {
  assert.deepEqual(d1RuntimeFailure("d1_database_initialize_failed", "demo", "db1", {
    status: 400,
    body: {
      error: "sql-error",
      message: "private initialization SQL",
      category: "sql",
      retryable: false,
    },
  }, {}, { publicStatus: 503 }), {
    error: "d1_database_initialize_failed",
    namespace: "demo",
    databaseId: "db1",
    message: "Internal error",
    upstreamCode: "sql-error",
    upstreamCategory: "sql",
    upstreamRetryable: false,
    upstreamStatus: 400,
  });
});

test("control D1 runtime failure preserves client-facing 4xx diagnostics", () => {
  assert.deepEqual(d1RuntimeFailure("d1_execute_failed", "demo", "db1", {
    status: 400,
    body: {
      error: "sql-error",
      message: "near syntax",
      category: "sql",
      retryable: false,
    },
  }), {
    error: "d1_execute_failed",
    namespace: "demo",
    databaseId: "db1",
    message: "near syntax",
    upstreamCode: "sql-error",
    upstreamCategory: "sql",
    upstreamRetryable: false,
    upstreamStatus: 400,
    detail: {
      error: "sql-error",
      message: "near syntax",
      category: "sql",
      retryable: false,
    },
  });
});
