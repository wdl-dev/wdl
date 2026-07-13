import assert from "node:assert/strict";
import { test } from "node:test";

import { errorMessage } from "../../shared/errors.js";
import { bytesToHex } from "../../shared/hex.js";
import { randomHex } from "../../shared/random-id.js";

test("bytesToHex renders lowercase fixed-width bytes", () => {
  assert.equal(bytesToHex(new Uint8Array([0, 1, 15, 16, 255])), "00010f10ff");
});

test("randomHex returns two lowercase hex chars per byte", () => {
  assert.match(randomHex(8), /^[0-9a-f]{16}$/);
  assert.equal(randomHex(0), "");
});

test("errorMessage extracts string messages without structured log shape", () => {
  assert.equal(errorMessage(new TypeError("boom")), "boom");
  assert.equal(errorMessage(42), "42");
  assert.equal(errorMessage(null), "null");
});

test("errorMessage never replaces a pathological thrown value", () => {
  const throwable = Object.create(null);
  assert.equal(errorMessage(throwable), "Unknown error");

  const brokenError = new Error("original");
  Object.defineProperty(brokenError, "message", {
    get() { throw new Error("message getter failed"); },
  });
  assert.equal(errorMessage(brokenError), "Unknown error");
});
