import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { normalizeD1Param } from "../../shared/d1-params.js";

import { loadD1Protocol, loadD1QueryWire } from "../helpers/load-d1-protocol.js";

const {
  D1_OWNERSHIP_CODES,
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  encodeD1QueryRequest,
  encodeD1QueryResponse,
} = await loadD1QueryWire();

const {
  classifyD1Error,
  D1_ACTOR_QUERY_CONTENT_TYPE,
  D1ProtocolError,
  dbKeyOf,
  d1ErrorPayload,
  encodeD1ActorQueryRequest,
  normalizeQueryRequest,
  readD1QueryResponseWithBytes,
  readD1ActorControlRequest,
  readD1ActorQueryRequest,
  readD1QueryRequest,
  sqliteBindParams,
  slotOf,
} = await loadD1Protocol();

const D1_QUERY_ENVELOPE_MAX_BYTES = 8 * 1024 * 1024;
const D1_QUERY_PAYLOAD_MAX_BYTES = 8 * 1024 * 1024;
const D1_SQL_STATEMENT_MAX_BYTES = 100_000;
const D1_STATEMENT_MAX_COUNT = 1000;
const textEncoder = new TextEncoder();

/** @param {Uint8Array} bytes */
function wireDigest(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

/** @param {Response} response */
async function readD1QueryResponse(response) {
  return (await readD1QueryResponseWithBytes(response)).payload;
}

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

/** @param {number} totalBytes */
function unknownD1Envelope(totalBytes) {
  const body = new Uint8Array(totalBytes);
  body[0] = 0x7a; // Unknown field 15, length-delimited.
  let payloadBytes = totalBytes - 5;
  let offset = 1;
  while (payloadBytes >= 0x80) {
    body[offset++] = (payloadBytes & 0x7f) | 0x80;
    payloadBytes >>>= 7;
  }
  body[offset++] = payloadBytes;
  assert.equal(offset, 5);
  return body;
}

/** @param {number} totalBytes */
function statementsWithSqlBytes(totalBytes) {
  const statements = [];
  let remaining = totalBytes;
  while (remaining > 0) {
    const sqlBytes = Math.min(remaining, D1_SQL_STATEMENT_MAX_BYTES);
    statements.push({ sql: "x".repeat(sqlBytes), params: [] });
    remaining -= sqlBytes;
  }
  return statements;
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

test("D1 protocol: slot hash matches persisted golden vectors", () => {
  assert.equal(slotOf("tenant-a", "main"), 770);
  for (const [namespace, databaseId, slotCount, expected] of [
    ["tenant-a", "main", 128, 2],
    ["tenant-d", "main", 128, 67],
    ["tenant-d", "main", 64, 3],
    ["__platform__", "system-db", 128, 30],
    ["tenant-123", "db_with-dashes", 1024, 248],
  ]) {
    assert.equal(slotOf(namespace, databaseId, slotCount), expected);
  }
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

test("D1 query wire bytes remain stable across representative and varint-boundary payloads", () => {
  const cases = [
    {
      name: "request",
      bytes: encodeD1QueryRequest({
        namespace: "tenant-a",
        databaseId: "main",
        binding: "DB",
        mode: "batch",
        statements: [
          {
            sql: "select ? as text, ? as blob, ? as n",
            params: ["caf\u00e9\ud83c\udf0c", new Uint8Array([0, 1, 127, 128, 255]), 42, null],
          },
          { sql: "select 2", params: [] },
        ],
      }),
      length: 115,
      digest: "62b756f5d83f518a12d06ed973ac1b280c823061f08e1ff208b96dbe8380af5b",
    },
    {
      name: "response",
      bytes: encodeD1QueryResponse({
        success: true,
        results: [{
          text: "caf\u00e9\ud83c\udf0c",
          blob: new Uint8Array([0, 1, 127, 128, 255]),
          nested: [
            null,
            true,
            3.5,
            JSON.parse('{"empty":"","__proto__":"data"}'),
          ],
        }],
        meta: { duration: 1.25, changed_db: false },
      }),
      length: 198,
      digest: "a180c0f94b7f42731a1e4e3c5a4e8523a66fcf9afa043d21e51b8cd56ffd060a",
    },
    {
      name: "error response",
      bytes: encodeD1QueryResponse({
        success: false,
        error: "SQLITE_CONSTRAINT: duplicate",
        code: "constraint",
        meta: { duration: 0 },
      }),
      length: 115,
      digest: "588a59fbc6a9ad13680d6883b8191e7e760a366134625e20b81a1f1c102c9f09",
    },
    {
      name: "127-byte string",
      bytes: encodeD1QueryResponse("x".repeat(127)),
      length: 129,
      digest: "15b114504d512e7a41a51e855a4ab5b072e6899f71f59fdedda0b0d078cacb07",
    },
    {
      name: "128-byte string",
      bytes: encodeD1QueryResponse("x".repeat(128)),
      length: 131,
      digest: "4e8a04b2c4edda8088a2c0c88caac5173a25e6428c690161c1658c552d1378c5",
    },
    {
      name: "16383-byte string",
      bytes: encodeD1QueryResponse("x".repeat(16_383)),
      length: 16_386,
      digest: "c2689583d7dd0cf7bc42e7c54840bd273b69877806c42b33a94cda89d2a8db36",
    },
    {
      name: "16384-byte string",
      bytes: encodeD1QueryResponse("x".repeat(16_384)),
      length: 16_388,
      digest: "b15af4fc7ee4bd941c118f20781d5280ded81fa3fc5454b876611520f529e90a",
    },
  ];

  for (const item of cases) {
    assert.equal(item.bytes.length, item.length, item.name);
    assert.equal(wireDigest(item.bytes), item.digest, item.name);
  }
});

test("D1 protocol: restores wire byte arrays for SQLite binding", () => {
  const params = sqliteBindParams(["x", 7, null, [0, 127, 255]]);
  assert.deepEqual(params.slice(0, 3), ["x", 7, null]);
  assert.deepEqual(params[3], new Uint8Array([0, 127, 255]));
});

test("D1 protocol: rejects unsupported bind parameter types", () => {
  assert.throws(() => normalizeD1Param({ nope: true }), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(NaN), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(Infinity), /D1_TYPE_ERROR/);
  assert.throws(() => normalizeD1Param(BigInt(Number.MAX_SAFE_INTEGER) + 1n), /D1_TYPE_ERROR/);
});

test("D1 protocol: enforces Cloudflare-aligned statement and parameter limits", () => {
  assert.doesNotThrow(() => normalizeQueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    statements: Array.from({ length: D1_STATEMENT_MAX_COUNT }, () => ({ sql: "select 1", params: [] })),
  }));

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
      statements: Array.from({ length: D1_STATEMENT_MAX_COUNT + 1 }, () => ({ sql: "select 1", params: [] })),
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );

  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: statementsWithSqlBytes(D1_QUERY_PAYLOAD_MAX_BYTES + 1),
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );

  assert.doesNotThrow(() => normalizeQueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    statements: statementsWithSqlBytes(D1_QUERY_PAYLOAD_MAX_BYTES),
  }));
});

test("D1 protocol: SQL statement cap counts exact UTF-8 bytes", () => {
  const exact = "\u00e9".repeat(D1_SQL_STATEMENT_MAX_BYTES / 2);
  assert.doesNotThrow(() => normalizeQueryRequest({
    namespace: "tenant-a",
    databaseId: "main",
    statements: [{ sql: exact, params: [] }],
  }));
  assert.throws(
    () => normalizeQueryRequest({
      namespace: "tenant-a",
      databaseId: "main",
      statements: [{ sql: `${exact}\u00e9`, params: [] }],
    }),
    (err) => isProtocolError(err, 413, "limit-exceeded")
  );
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

test("D1 protocol: query endpoint keeps the documented 8 MiB default body cap", async () => {
  assert.deepEqual(
    await readD1QueryRequest(new Request("http://d1/internal/d1/query", {
      method: "POST",
      headers: { "content-type": D1_QUERY_CONTENT_TYPE },
      body: unknownD1Envelope(D1_QUERY_ENVELOPE_MAX_BYTES),
    })),
    { namespace: "", databaseId: "", binding: null, mode: undefined, statements: [] }
  );

  await assert.rejects(
    () => readD1QueryRequest(new Request("http://d1/internal/d1/query", {
      method: "POST",
      headers: { "content-type": D1_QUERY_CONTENT_TYPE },
      body: unknownD1Envelope(D1_QUERY_ENVELOPE_MAX_BYTES + 1),
    })),
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

test("D1 protocol: can decode a query response while retaining its exact wire bytes", async () => {
  const body = encodeD1QueryResponse({ success: true, results: ["ok"], meta: {} });
  const result = await readD1QueryResponseWithBytes(new Response(body, {
    headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
  }));

  assert.deepEqual(result.payload, { success: true, results: ["ok"], meta: {} });
  assert.deepEqual(result.bytes, body);
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
      statements: Array.from({ length: D1_STATEMENT_MAX_COUNT + 1 }, () => ({ sql: "select 1", params: [] })),
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
  assert.deepEqual(D1_OWNERSHIP_CODES, [
    "not-owner",
    "owner-not-ready",
    "owner-unavailable",
    "owner-record-invalid",
    "owner-endpoint-missing",
    "owner-endpoint-invalid",
    "forward-hop-exhausted",
    "owner-claim-raced",
    "owner-takeover-raced",
    "owner-rebalance-raced",
    "owner-release-raced",
    "owner-renew-raced",
    "owner-lease-expired",
    "owner-lease-too-short",
    "lease-budget-exhausted",
    "task-draining",
  ]);
  for (const code of D1_OWNERSHIP_CODES) {
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
