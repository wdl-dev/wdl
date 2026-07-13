import assert from "node:assert/strict";
import { test } from "node:test";

import {
  INTERNAL_AUTH_HEADER,
  INTERNAL_AUTH_ENV,
  INTERNAL_AUTH_FAILURE_CODE,
  INTERNAL_AUTH_FAILURE_MESSAGE,
  INTERNAL_AUTH_PREVIOUS_ENV,
  internalAuthFailureResponse,
  internalAuthPreviousToken,
  internalAuthToken,
  stripInternalAuthHeader,
  verifyInternalAuthHeaders,
  withInternalAuth,
  withInternalAuthEntries,
} from "../../shared/internal-auth.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const TOKEN = "test-internal-auth-token";
const ENV = { [INTERNAL_AUTH_ENV]: TOKEN };
const contract = /** @type {{ header: string, currentEnv: string, previousEnv: string, failure: { status: number, error: string, message: string }, tokenCases: Array<{ value: string, accepted: boolean }>, rotationCases: Array<{ actual: string | null, current: string, previous: string | null, accepted: boolean }>, headerCases: Array<{ values: string[], current: string, previous: string | null, accepted: boolean }> }} */ (
  readRepositoryJson("tests/fixtures/internal-auth-contract.json")
);

test("internal auth literals and rotation match the shared Rust/JS contract", () => {
  assert.equal(INTERNAL_AUTH_HEADER, contract.header);
  assert.equal(INTERNAL_AUTH_ENV, contract.currentEnv);
  assert.equal(INTERNAL_AUTH_PREVIOUS_ENV, contract.previousEnv);
  assert.equal(INTERNAL_AUTH_FAILURE_CODE, contract.failure.error);
  assert.equal(INTERNAL_AUTH_FAILURE_MESSAGE, contract.failure.message);

  for (const entry of contract.tokenCases) {
    const read = () => internalAuthToken({ [INTERNAL_AUTH_ENV]: entry.value });
    if (entry.accepted) assert.equal(read(), entry.value);
    else assert.throws(read);
  }
  for (const entry of contract.rotationCases) {
    const headers = new Headers();
    if (entry.actual !== null) headers.set(INTERNAL_AUTH_HEADER, entry.actual);
    const env = {
      [INTERNAL_AUTH_ENV]: entry.current,
      ...(entry.previous === null ? {} : { [INTERNAL_AUTH_PREVIOUS_ENV]: entry.previous }),
    };
    assert.equal(verifyInternalAuthHeaders(headers, env), entry.accepted);
  }
  for (const entry of contract.headerCases) {
    const headers = new Headers();
    for (const value of entry.values) headers.append(INTERNAL_AUTH_HEADER, value);
    const env = {
      [INTERNAL_AUTH_ENV]: entry.current,
      ...(entry.previous === null ? {} : { [INTERNAL_AUTH_PREVIOUS_ENV]: entry.previous }),
    };
    assert.equal(verifyInternalAuthHeaders(headers, env), entry.accepted);
  }
});

test("internal auth token requires a non-empty configured string", () => {
  assert.equal(internalAuthToken(ENV), TOKEN);
  assert.equal(internalAuthPreviousToken(ENV), null);
  assert.equal(internalAuthPreviousToken({ [INTERNAL_AUTH_PREVIOUS_ENV]: "old" }), "old");
  assert.equal(internalAuthPreviousToken({ [INTERNAL_AUTH_PREVIOUS_ENV]: "" }), null);
  assert.throws(() => internalAuthToken({}), /WDL_INTERNAL_AUTH_TOKEN must be configured/);
  assert.throws(() => internalAuthToken({ [INTERNAL_AUTH_ENV]: "" }), /WDL_INTERNAL_AUTH_TOKEN must be configured/);
  assert.throws(
    () => internalAuthToken({ [INTERNAL_AUTH_ENV]: "tokén" }),
    /WDL_INTERNAL_AUTH_TOKEN must be configured as visible ASCII/
  );
  assert.throws(
    () => internalAuthPreviousToken({ [INTERNAL_AUTH_PREVIOUS_ENV]: "prévious" }),
    /WDL_INTERNAL_AUTH_PREVIOUS_TOKEN must be configured as visible ASCII/
  );
  assert.throws(
    () => internalAuthToken({ [INTERNAL_AUTH_ENV]: "token,other" }),
    /without whitespace or commas/
  );
});

test("internal auth writer overwrites spoofed headers", () => {
  const headers = withInternalAuth({ [INTERNAL_AUTH_HEADER]: "spoofed", "x-test": "1" }, ENV);
  assert.equal(headers.get(INTERNAL_AUTH_HEADER), TOKEN);
  assert.equal(headers.get("x-test"), "1");
});

test("internal auth entries writer avoids tenant-visible Headers and Array hooks", () => {
  /** @type {string[]} */
  const capturedAuthWrites = [];
  const originalSet = Headers.prototype.set;
  const originalPush = Array.prototype.push;
  return withMockedProperty(Headers.prototype, "set", /** @this {Headers} */ function set(
    /** @type {string} */ name,
    /** @type {string} */ value
  ) {
    if (String(name).toLowerCase() === INTERNAL_AUTH_HEADER) capturedAuthWrites.push(String(value));
    return originalSet.call(this, name, value);
  }, async () => withMockedProperty(Array.prototype, "push", /** @this {unknown[]} */ function push(...items) {
    for (const item of items) {
      if (Array.isArray(item) && item.includes(TOKEN)) capturedAuthWrites.push(String(item));
    }
    return originalPush.apply(this, items);
  }, async () => withMockedProperty(Object, "create", /** @type {typeof Object.create} */ (/** @type {unknown} */ (/** @param {...unknown} args */ function create(...args) {
    throw new Error(`Object.create should not handle ${args.length} arguments in this helper`);
  })), async () => {
    const entries = withInternalAuthEntries(new Headers({
      [INTERNAL_AUTH_HEADER]: "spoofed",
      "x-test": "1",
    }), ENV);
    assert.equal(Object.getPrototypeOf(entries), null);
    const headers = new Headers(entries);
    assert.equal(headers.get(INTERNAL_AUTH_HEADER), TOKEN);
    assert.equal(headers.get("x-test"), "1");
    assert.deepEqual(capturedAuthWrites, []);
  })));
});

test("internal auth verifier accepts current and optional previous tokens", () => {
  assert.equal(verifyInternalAuthHeaders(new Headers({ [INTERNAL_AUTH_HEADER]: TOKEN }), ENV), true);
  assert.equal(
    verifyInternalAuthHeaders(
      new Headers({ [INTERNAL_AUTH_HEADER]: "old-token" }),
      { ...ENV, [INTERNAL_AUTH_PREVIOUS_ENV]: "old-token" }
    ),
    true
  );
  assert.equal(verifyInternalAuthHeaders(new Headers({ [INTERNAL_AUTH_HEADER]: "wrong" }), ENV), false);
  assert.equal(
    verifyInternalAuthHeaders(
      new Headers({ [INTERNAL_AUTH_HEADER]: "wrong" }),
      { ...ENV, [INTERNAL_AUTH_PREVIOUS_ENV]: "old-token" }
    ),
    false
  );
  assert.equal(verifyInternalAuthHeaders(new Headers(), ENV), false);
  assert.equal(verifyInternalAuthHeaders(new Headers({ [INTERNAL_AUTH_HEADER]: TOKEN }), {}), false);
  assert.equal(
    verifyInternalAuthHeaders(
      new Headers({ [INTERNAL_AUTH_HEADER]: "old-token" }),
      { ...ENV, [INTERNAL_AUTH_PREVIOUS_ENV]: "prévious" }
    ),
    false
  );
});

test("internal auth strip removes only the internal header", () => {
  const headers = new Headers({ [INTERNAL_AUTH_HEADER]: TOKEN, "x-test": "1" });
  stripInternalAuthHeader(headers);
  assert.equal(headers.get(INTERNAL_AUTH_HEADER), null);
  assert.equal(headers.get("x-test"), "1");
});

test("internal auth failure response has stable public shape", async () => {
  const response = internalAuthFailureResponse();
  await assertJsonResponse(response, contract.failure.status, {
    error: contract.failure.error,
    message: contract.failure.message,
  });
});
