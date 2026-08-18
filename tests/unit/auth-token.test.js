import { test } from "node:test";
import assert from "node:assert/strict";
import { extractToken } from "../../shared/auth-token.js";

const TOKEN_HEADER_MAX_BYTES = 256;

test("extractToken accepts trimmed simple value", () => {
  const headers = new Headers({ "x-admin-token": "  hello-world  " });
  assert.equal(extractToken(headers), "hello-world");
});

test("extractToken returns null on missing / empty / control-char / multi-value / too long", () => {
  // Use plain objects so we can include values (\n, \t) that Node's
  // strict undici-backed Headers constructor rejects outright.
  const cases = [
    {}, // no header
    { "x-admin-token": "" },
    { "x-admin-token": "   " },
    { "x-admin-token": "valid,extra" }, // multi-value join
    { "x-admin-token": "tab\there" }, // control char (\t)
    { "x-admin-token": "newline\nhere" }, // control char
    { "x-admin-token": "x".repeat(TOKEN_HEADER_MAX_BYTES + 1) },
  ];
  for (const value of cases) {
    assert.equal(
      extractToken(value),
      null,
      `case ${JSON.stringify(value)} should be dirty`,
    );
  }
});

test("extractToken accepts plain object via bracket-access fallback", () => {
  assert.equal(extractToken({ "x-admin-token": "ok" }), "ok");
});

test("extractToken applies its header cap to UTF-8 bytes", () => {
  const exact = "\u00e9".repeat(TOKEN_HEADER_MAX_BYTES / 2);
  assert.equal(extractToken({ "x-admin-token": exact }), exact);
  assert.equal(extractToken({ "x-admin-token": `${exact}\u00e9` }), null);
});
