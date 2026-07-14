import { test } from "node:test";
import assert from "node:assert/strict";

import { loadD1Protocol } from "../helpers/load-d1-protocol.js";

const {
  classifyD1Error,
  D1_ACTOR_QUERY_CONTENT_TYPE,
  D1_MAX_QUERY_ENVELOPE_BYTES,
  D1_MAX_QUERY_PAYLOAD_BYTES,
  D1_MAX_STATEMENTS,
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  D1ProtocolError,
  dbKeyOf,
  d1ErrorPayload,
  encodeD1ActorQueryRequest,
  encodeD1QueryResponse,
  encodeD1QueryRequest,
  normalizeD1Param,
  normalizeQueryRequest,
  readD1QueryResponse,
  readD1ActorControlRequest,
  readD1ActorQueryRequest,
  readD1QueryRequest,
  slotOf,
} = await loadD1Protocol();

const textEncoder = new TextEncoder();

/**
 * @param {unknown} err
 * @param {number} status
 * @param {string} code
 */
function isProtocolError(err, status, code) {
  const e = /** @type {{ status?: number, code?: string }} */ (err);
  return err instanceof D1ProtocolError && e.status === status && e.code === code;
}

/**
 * @param {Record<string, unknown>} metadata
 * @param {Uint8Array} queryBytes
 */
function actorEnvelope(metadata, queryBytes) {
  const metadataBytes = textEncoder.encode(JSON.stringify(metadata));
  const body = new Uint8Array(4 + metadataBytes.length + queryBytes.length);
  new DataView(body.buffer, body.byteOffset, body.byteLength).setUint32(0, metadataBytes.length, false);
  body.set(metadataBytes, 4);
  body.set(queryBytes, 4 + metadataBytes.length);
  return body;
}

test("D1 protocol: db key includes namespace and database id", () => {
  assert.equal(dbKeyOf("tenant-a", "main"), "tenant-a:main");
  assert.notEqual(dbKeyOf("tenant-a", "main"), dbKeyOf("tenant-b", "main"));
});

test("D1 protocol: db keys reject non-canonical runtime namespaces and database ids", () => {
  assert.throws(() => dbKeyOf("admin", "main"), /namespace is invalid/);
  assert.throws(() => dbKeyOf("tenant-a", "db/child"), /databaseId is invalid/);
  assert.throws(() => dbKeyOf("tenant-a", "a".repeat(129)), /databaseId is invalid/);
});

test("D1 protocol: slot hash is stable and bounded", () => {
  const slot = slotOf("tenant-a", "main", 128);
  assert.equal(slot, slotOf("tenant-a", "main", 128));
  assert.ok(slot >= 0 && slot < 128);
});

test("D1 protocol: normalizes D1 bind parameter types", () => {
  assert.equal(normalizeD1Param("x"), "x");
  assert.equal(normalizeD1Param(7), 7);
  assert.equal(normalizeD1Param(-0), 0);
  assert.equal(normalizeD1Param(7n), 7);
  assert.equal(normalizeD1Param(true), 1);
  assert.equal(normalizeD1Param(false), 0);
  assert.equal(normalizeD1Param(null), null);
  assert.deepEqual(normalizeD1Param(new Uint8Array([1, 2])), [1, 2]);
  assert.deepEqual(normalizeD1Param(new Uint8Array([3, 4]).buffer), [3, 4]);
});

test("D1 protocol: rejects unsupported bind parameter types", () => {
  assert.throws(() => normalizeD1Param({ nope: true }), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(NaN), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(Infinity), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(BigInt(Number.MAX_SAFE_INTEGER) + 1n), /D1_TYPE_ERROR/);
});

test("D1 protocol: enforces Cloudflare-aligned statement and parameter limits", () => {
  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: [{ sql: `select '${"x".repeat(100_001)}'`, params: [] }],
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );

  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: [{ sql: "select 1", params: Array.from({ length: 101 }, () => 1) }],
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );

  assert.throws(
    () => normalizeD1Param("x".repeat(2_000_001)),
    /D1_LIMIT_ERROR: Maximum string size is 2000000 bytes/
  );

  assert.throws(
    () => normalizeD1Param(new Uint8Array(2_000_001)),
    /D1_LIMIT_ERROR: Maximum BLOB size is 2000000 bytes/
  );

  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: Array.from({ length: D1_MAX_STATEMENTS + 1 }, () => ({ sql: "select 1", params: [] })),
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );

  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: Array.from({ length: D1_MAX_STATEMENTS }, () => ({
        sql: `select '${"x".repeat(90_000)}'`,
        params: [],
      })),
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
  assert.equal(D1_MAX_QUERY_PAYLOAD_BYTES, D1_MAX_QUERY_ENVELOPE_BYTES);
});

test("D1 protocol: normalizes query request to slot and statements", () => {
  const req = normalizeQueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    binding: "DB",
    statements: [{ sql: "select ? as n", params: [true] }],
  });
  assert.equal(req.dbKey, "tenant-a:main");
  assert.equal(req.binding, "DB");
  assert.equal(req.statements[0].sql, "select ? as n");
  assert.deepEqual(req.statements[0].params, [1]);
  assert.ok(req.slot >= 0);
});

test("D1 protocol: decodes binary query wire requests", async () => {
  const body = encodeD1QueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    binding: "DB",
    mode: "all",
    statements: [{ sql: "select ? as blob, ? as n", params: [new Uint8Array([1, 2]), true] }],
  });
  const req = normalizeQueryRequest(await readD1QueryRequest(new Request("http://d1/internal/d1/query", {
    method: "POST",
    headers: { "content-type": D1_QUERY_CONTENT_TYPE },
    body,
  })));
  assert.equal(req.dbKey, "tenant-a:main");
  assert.equal(req.binding, "DB");
  assert.deepEqual(req.statements, [{ sql: "select ? as blob, ? as n", params: [[1, 2], 1] }]);
});

test("D1 protocol: query endpoint enforces a bounded binary body", async () => {
  await assert.rejects(
    () => readD1QueryRequest(new Request("http://d1/internal/d1/query", {
      method: "POST",
      headers: { "content-type": D1_QUERY_CONTENT_TYPE },
      body: new Uint8Array(5),
    }), { maxBytes: 4 }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
});

test("D1 protocol: binary query wire round-trips empty string parameters", async () => {
  const body = encodeD1QueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    binding: "DB",
    mode: "all",
    statements: [{ sql: "select ? as empty", params: [""] }],
  });
  const req = normalizeQueryRequest(await readD1QueryRequest(new Request("http://d1/internal/d1/query", {
    method: "POST",
    headers: { "content-type": D1_QUERY_CONTENT_TYPE },
    body,
  })));
  assert.deepEqual(req.statements, [{ sql: "select ? as empty", params: [""] }]);
});

test("D1 protocol: query endpoint rejects JSON media type", async () => {
  await assert.rejects(
    () => readD1QueryRequest(new Request("http://d1/internal/d1/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ namespace: "tenant-a", databaseId: "main", statements: [] }),
    })),
    (err) => isProtocolError(err, 415, "unsupported-media-type")
  );
});

test("D1 protocol: decodes binary query wire responses", async () => {
  const body = encodeD1QueryResponse({
    success: true,
    results: [{ ok: 1, empty: "", "": "empty-key", blob: { __wdl_d1_binary_v1: true, base64: "AQI=" } }, {}, []],
    meta: {},
  });
  const payload = await readD1QueryResponse(new Response(body, {
    headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
  }));
  assert.deepEqual(payload, {
    success: true,
    results: [{ ok: 1, empty: "", "": "empty-key", blob: { __wdl_d1_binary_v1: true, base64: "AQI=" } }, {}, []],
    meta: {},
  });
});

test("D1 protocol: query response preserves magic object keys as data fields", async () => {
  const body = encodeD1QueryResponse({
    success: true,
    results: [JSON.parse('{"__proto__":"row-value","nested":{"__proto__":"nested-value"}}')],
    meta: {},
  });
  const payload = await readD1QueryResponse(new Response(body, {
    headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
  }));
  const row = /** @type {Record<string, unknown>} */ (payload.results[0]);

  assert.equal(Object.hasOwn(row, "__proto__"), true);
  assert.equal(row.__proto__, "row-value");
  assert.equal(/** @type {Record<string, unknown>} */ (row.nested).__proto__, "nested-value");
});

test("D1 protocol: query response rejects scalar fields mixed with arrays or objects", async () => {
  for (const body of [
    Uint8Array.from([0x40, 0x01, 0x08, 0x01]),
    Uint8Array.from([0x48, 0x01, 0x08, 0x01]),
  ]) {
    await assert.rejects(
      () => readD1QueryResponse(new Response(body, {
        headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
      })),
      (err) => isProtocolError(err, 502, "invalid-response")
    );
  }
});

test("D1 protocol: query response rejects JSON media type", async () => {
  await assert.rejects(
    () => readD1QueryResponse(Response.json({ success: true })),
    (err) => isProtocolError(err, 502, "invalid-response")
  );
});

test("D1 protocol: actor query envelope carries owner and query bytes", async () => {
  const query = normalizeQueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    binding: "DB",
    mode: "all",
    statements: [{ sql: "select ? as n", params: [1] }],
  });
  const owner = {
    dbKey: query.dbKey,
    taskId: "task-a",
    generation: 7,
  };
  const body = encodeD1ActorQueryRequest({ ...query, __control: "hold-transaction", __holdMs: 25 }, owner);
  const decoded = await readD1ActorQueryRequest(new Request("http://d1-actor/query", {
    method: "POST",
    headers: { "content-type": D1_ACTOR_QUERY_CONTENT_TYPE },
    body,
  }));

  assert.deepEqual(decoded.owner, owner);
  assert.equal(decoded.namespace, "tenant-a");
  assert.equal(decoded.databaseId, "main");
  assert.equal(decoded.binding, "DB");
  assert.equal(decoded.mode, "all");
  assert.deepEqual(decoded.statements, [{ sql: "select ? as n", params: [1] }]);
  assert.equal(decoded.__control, "hold-transaction");
  assert.equal(decoded.__holdMs, 25);
});

test("D1 protocol: actor query endpoint enforces a bounded binary body", async () => {
  await assert.rejects(
    () => readD1ActorQueryRequest(new Request("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": D1_ACTOR_QUERY_CONTENT_TYPE },
      body: new Uint8Array(5),
    }), { maxBytes: 4 }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
});

test("D1 protocol: actor query endpoint normalizes decoded query shape", async () => {
  const body = actorEnvelope(
    { owner: { taskId: "task-a" } },
    encodeD1QueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      mode: "all",
      statements: Array.from({ length: D1_MAX_STATEMENTS + 1 }, () => ({ sql: "select 1", params: [] })),
    })
  );

  await assert.rejects(
    () => readD1ActorQueryRequest(new Request("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": D1_ACTOR_QUERY_CONTENT_TYPE },
      body,
    })),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
});

test("D1 protocol: actor control endpoint enforces a bounded JSON body", async () => {
  await assert.rejects(
    () => readD1ActorControlRequest(new Request("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: new Uint8Array(5),
    }), { maxBytes: 4 }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
});

test("D1 protocol: actor control endpoint rejects non-object JSON", async () => {
  await assert.rejects(
    () => readD1ActorControlRequest(new Request("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "null",
    })),
    (err) => isProtocolError(err, 400, "invalid-json")
  );
});

test("D1 protocol: actor query rejects JSON media type", async () => {
  await assert.rejects(
    () => readD1ActorQueryRequest(new Request("http://d1-actor/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ mode: "all" }),
    })),
    (err) => isProtocolError(err, 415, "unsupported-media-type")
  );
});

test("D1 protocol: invalid request shape throws protocol error", () => {
  assert.throws(
    () => normalizeQueryRequest({ namespace: "tenant-a", databaseId: "main", statements: [] }),
    (err) => isProtocolError(err, 400, "empty-statements")
  );
  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      mode: "surprise",
      statements: [{ sql: "select 1", params: [] }],
    }),
    (err) => isProtocolError(err, 400, "invalid-mode")
  );
  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      mode: 0,
      statements: [{ sql: "select 1", params: [] }],
    }),
    (err) => isProtocolError(err, 400, "invalid-mode")
  );
  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      __control: "hold-transaction",
      statements: [{ sql: "select 1", params: [] }],
    }),
    (err) => isProtocolError(err, 400, "invalid-control")
  );
});

test("D1 protocol: classifies user-facing errors by category", () => {
  for (const code of [
    "owner-record-invalid",
    "owner-endpoint-invalid",
    "owner-lease-too-short",
    "lease-budget-exhausted",
  ]) {
    assert.deepEqual(
      classifyD1Error(new D1ProtocolError(503, code, "lease budget low")),
      {
        status: 503,
        code,
        category: "ownership",
        retryable: true,
        message: `D1 database is temporarily unavailable while ownership is changing; retry the request (${code}).`,
      }
    );
  }

  assert.equal(classifyD1Error(new Error("SQLITE_CONSTRAINT: unique failed")).code, "sql-error");
  assert.equal(classifyD1Error(new Error("database or disk is full")).code, "quota-exceeded");
  assert.deepEqual(classifyD1Error(new Error("request timed out")), {
    status: 504,
    code: "timeout",
    category: "timeout",
    retryable: false,
    message: "D1 request timed out; write outcome may be unknown, do not blindly retry non-idempotent requests.",
  });
  assert.deepEqual(classifyD1Error(new D1ProtocolError(503, "result-unknown", "owner response was lost")), {
    status: 503,
    code: "result-unknown",
    category: "result-unknown",
    retryable: false,
    message: "owner response was lost",
  });
  assert.deepEqual(classifyD1Error(new Error("D1_LIMIT_ERROR: Maximum string size is 2000000 bytes")), {
    status: 413,
    code: "limit-exceeded",
    category: "limit",
    retryable: false,
    message: "D1_LIMIT_ERROR: Maximum string size is 2000000 bytes",
  });
  assert.deepEqual(d1ErrorPayload(new Error("no such table: posts")), {
    success: false,
    error: "sql-error",
    message: "SQL error: no such table: posts",
    category: "sql",
    retryable: false,
  });
});
