import { test } from "node:test";
import assert from "node:assert/strict";

import {
  installMockProperty,
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";

test("installMockProperty removes properties that did not originally exist", () => {
  /** @type {Record<string, unknown>} */
  const target = {};
  const restore = installMockProperty(target, "missing", "mocked");

  assert.equal(target.missing, "mocked");
  restore();

  assert.equal(Object.hasOwn(target, "missing"), false);
});

test("installMockProperty detects out-of-order restores instead of leaking mocks", () => {
  const target = { value: "original" };
  const restoreA = installMockProperty(target, "value", "mock-a");
  const restoreB = installMockProperty(target, "value", "mock-b");

  assert.throws(() => restoreA(), /out of order/);
  assert.equal(target.value, "mock-b");

  restoreB();
  assert.equal(target.value, "mock-a");

  restoreA();
  assert.equal(target.value, "original");
});

test("withMockedProperty restores after callback failures", async () => {
  const target = { value: "original" };

  await assert.rejects(
    () => withMockedProperty(target, "value", "mocked", async () => {
      assert.equal(target.value, "mocked");
      throw new Error("boom");
    }),
    /boom/
  );

  assert.equal(target.value, "original");
});

test("withMockedPropertyDescriptor removes newly defined properties", async () => {
  /** @type {Record<string, unknown>} */
  const target = {};

  await withMockedPropertyDescriptor(
    target,
    "missing",
    { value: "mocked" },
    () => {
      assert.equal(target.missing, "mocked");
    }
  );

  assert.equal(Object.hasOwn(target, "missing"), false);
});

test("withMockedPropertyDescriptor restores after callback failures", async () => {
  const target = { value: "original" };

  await assert.rejects(
    () => withMockedPropertyDescriptor(
      target,
      "value",
      { value: "mocked" },
      () => { throw new Error("boom"); }
    ),
    /boom/
  );

  assert.equal(target.value, "original");
});

test("withMockedPropertyDescriptor rejects non-configurable properties before mocking", async () => {
  /** @type {Record<string, unknown>} */
  const target = {};
  Object.defineProperty(target, "fixed", {
    configurable: false,
    value: "original",
    writable: true,
  });
  let callbackCalls = 0;

  await assert.rejects(
    () => withMockedPropertyDescriptor(
      target,
      "fixed",
      { value: "mocked" },
      () => { callbackCalls += 1; }
    ),
    /non-configurable property fixed/
  );

  assert.equal(callbackCalls, 0);
  assert.equal(target.fixed, "original");
});
