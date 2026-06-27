import { test } from "node:test";
import assert from "node:assert/strict";
import { loadD1QueryWire, d1QueryWireDataUrl, d1TransportDataUrl } from "../helpers/load-d1-protocol.js";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
  sharedModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { withRecordingFetch } from "../helpers/mock-fetch.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const {
  decodeD1QueryRequest,
  D1_QUERY_CONTENT_TYPE,
  encodeD1QueryResponse,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
} = await loadD1QueryWire();
const {
  d1RuntimeFailure,
  d1RuntimePublicResult,
  d1RuntimeQuery,
  d1RuntimeReleaseOwner,
} = await importRepositoryModule("control/d1-runtime-client.js", importSpecifierReplacements({
  "shared-d1-timeout": sharedModuleDataUrl("shared/d1-timeout.js"),
  "shared-d1-transport": d1TransportDataUrl(),
  "shared-d1-query-wire": d1QueryWireDataUrl(),
  "shared-respond": repositoryFileUrl("shared/respond.js"),
  "shared-internal-auth": sharedInternalAuthUrl(),
  "runtime-owner-endpoint": repositoryFileUrl("runtime/_wdl-owner-endpoint.js"),
}));

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

test("control D1 runtime client preserves zero owner generation hints", async () => {
  const result = await d1RuntimeQuery(withInternalAuthEnv({
    D1_BACKEND: {
      async fetch() {
        return d1Response(
          { success: true, results: [] },
          {
            headers: {
              "x-wdl-d1-owner-task-id": "task-a",
              "x-wdl-d1-owner-endpoint": "10.0.0.9:8787",
              "x-wdl-d1-owner-generation": "0",
            },
          }
        );
      },
    },
  }), "tenant-a", "db1", "all", [{ sql: "select 1", params: [] }]);

  assert.equal(result.ok, true);
  assert.deepEqual(result.owner, {
    taskId: "task-a",
    endpoint: "10.0.0.9:8787",
    generation: 0,
  });
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
  assert.equal(result.owner, null);
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

test("control D1 runtime failure keeps extra fields additive only", () => {
  const body = d1RuntimeFailure("d1_execute_failed", "demo", "db1", {
    status: 502,
    body: {
      error: "sql-error",
      message: "near syntax",
      category: "sql",
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
    message: "near syntax",
    upstreamCode: "sql-error",
    upstreamCategory: "sql",
    upstreamRetryable: false,
    upstreamStatus: 502,
    detail: {
      error: "sql-error",
      message: "near syntax",
      category: "sql",
      retryable: false,
    },
  });
});
