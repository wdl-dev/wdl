import { test } from "node:test";
import assert from "node:assert/strict";
import { d1TransportDataUrl } from "../helpers/load-d1-protocol.js";

const {
  decodeD1Transport,
  decodeD1TransportForJson,
  encodeD1Transport,
} = await import(d1TransportDataUrl());

test("D1 transport encodes and decodes binary values", () => {
  const encoded = encodeD1Transport({
    rows: [[new Uint8Array([0, 1, 2, 255])]],
  });

  assert.deepEqual(encoded, {
    rows: [[{ __wdl_d1_binary_v1: true, base64: "AAEC/w==" }]],
  });

  const decoded = /** @type {{ rows: Uint8Array[][] }} */ (decodeD1Transport(encoded));
  assert.ok(decoded.rows[0][0] instanceof Uint8Array);
  assert.deepEqual(Array.from(decoded.rows[0][0]), [0, 1, 2, 255]);
});

test("D1 transport exposes binary values as JSON-safe admin API objects", () => {
  assert.deepEqual(
    decodeD1TransportForJson({
      data: { __wdl_d1_binary_v1: true, base64: "AAEC/w==" },
    }),
    {
      data: { type: "blob", base64: "AAEC/w==", byteLength: 4 },
    }
  );
});

test("D1 transport preserves native bytes for runtime readers", () => {
  const bytes = new Uint8Array([0, 1, 2, 255]);

  assert.equal(/** @type {any} */ (decodeD1Transport({ data: bytes })).data, bytes);
  assert.deepEqual(decodeD1TransportForJson({ data: bytes }), {
    data: { type: "blob", base64: "AAEC/w==", byteLength: 4 },
  });
});

test("D1 transport rejects unsupported binary tag versions", () => {
  assert.throws(
    () => decodeD1Transport({ __wdl_d1_binary_v2: true, base64: "AA==" }),
    (err) => Object(err).code === "unsupported-d1-transport-version"
  );
  assert.throws(
    () => decodeD1TransportForJson({ __wdl_d1_binary_v2: true, base64: "AA==" }),
    (err) => Object(err).code === "unsupported-d1-transport-version"
  );
});

test("D1 transport does not reject ordinary rows with tag-like column names", () => {
  assert.deepEqual(
    decodeD1Transport({ __wdl_d1_binary_v2: "column value", base64: "plain text", other: 1 }),
    { __wdl_d1_binary_v2: "column value", base64: "plain text", other: 1 }
  );
});

test("D1 transport preserves magic object keys as data fields", () => {
  const input = JSON.parse('{"__proto__":"row-value","nested":{"__proto__":"nested-value"}}');
  const encoded = /** @type {Record<string, unknown>} */ (encodeD1Transport(input));
  const decoded = /** @type {Record<string, unknown>} */ (decodeD1Transport(encoded));
  const jsonDecoded = /** @type {Record<string, unknown>} */ (decodeD1TransportForJson(input));

  assert.equal(Object.hasOwn(encoded, "__proto__"), true);
  assert.equal(encoded.__proto__, "row-value");
  assert.equal(Object.hasOwn(decoded, "__proto__"), true);
  assert.equal(decoded.__proto__, "row-value");
  assert.equal(/** @type {Record<string, unknown>} */ (decoded.nested).__proto__, "nested-value");
  assert.equal(Object.hasOwn(jsonDecoded, "__proto__"), true);
  assert.equal(jsonDecoded.__proto__, "row-value");
});
