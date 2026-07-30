import test from "node:test";
import assert from "node:assert/strict";

import { utf8ByteLength } from "../../shared/utf8.js";

const utf8Encoder = new TextEncoder();

/** @param {string} value */
function encodedLength(value) {
  return utf8Encoder.encode(value).byteLength;
}

test("utf8ByteLength matches TextEncoder for every single UTF-16 code unit", () => {
  for (let code = 0; code <= 0xffff; code += 1) {
    const value = String.fromCharCode(code);
    assert.equal(utf8ByteLength(value), encodedLength(value), `U+${code.toString(16)}`);
  }
});

test("utf8ByteLength matches TextEncoder across surrogate and threshold boundaries", () => {
  for (const value of [
    "",
    "plain ASCII",
    "caf\u00e9",
    "\u6c49\u5b57",
    "\ud83c\udf0c",
    "\ud800",
    "\udc00",
    "\ud800x",
    "x\udc00",
    "a".repeat(511),
    "a".repeat(512),
    "a".repeat(513),
    `${"x".repeat(511)}\ud83c\udf0c`,
    `${"x".repeat(512)}\ud800`,
    "\u6c49".repeat(2048),
  ]) {
    assert.equal(utf8ByteLength(value), encodedLength(value));
  }
});
