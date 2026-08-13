import { test } from "node:test";
import assert from "node:assert/strict";

import {
  installMockProperty,
  withMockedProperty,
  withMockedPropertyDescriptor,
  withMockedPropertyDescriptors,
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
      assert.equal(
        Object.getOwnPropertyDescriptor(target, "missing")?.configurable,
        true
      );
    }
  );

  assert.equal(Object.hasOwn(target, "missing"), false);
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

test("withMockedPropertyDescriptors restores all properties after callback failures", async () => {
  const target = { existing: "original" };
  const originalDescriptor = Object.getOwnPropertyDescriptor(target, "existing");
  assert.ok(originalDescriptor);

  await assert.rejects(
    () => withMockedPropertyDescriptors([
      {
        target,
        name: "existing",
        descriptor: { value: "mocked", writable: false },
      },
      {
        target,
        name: "missing",
        descriptor: { get: () => "mocked" },
      },
    ], () => {
      assert.equal(target.existing, "mocked");
      assert.equal(/** @type {any} */ (target).missing, "mocked");
      throw new Error("boom");
    }),
    /boom/
  );

  assert.deepEqual(
    Object.getOwnPropertyDescriptor(target, "existing"),
    originalDescriptor
  );
  assert.equal(Object.hasOwn(target, "missing"), false);
});

test("withMockedPropertyDescriptors restores best-effort and preserves callback errors", async () => {
  const earlier = { value: "original-earlier" };
  const middle = { value: "original-middle" };
  const later = { value: "original-later" };
  const callbackError = new Error("callback failed");

  await assert.rejects(
    () => withMockedPropertyDescriptors([
      {
        target: earlier,
        name: "value",
        descriptor: { value: "mocked-earlier" },
      },
      {
        target: middle,
        name: "value",
        descriptor: { value: "mocked-middle" },
      },
      {
        target: later,
        name: "value",
        descriptor: { value: "mocked-later" },
      },
    ], () => {
      Object.defineProperty(middle, "value", {
        configurable: false,
        value: "stuck-middle",
      });
      Object.defineProperty(later, "value", {
        configurable: false,
        value: "stuck-later",
      });
      throw callbackError;
    }),
    (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors.length, 3);
      assert.strictEqual(error.errors[0], callbackError);
      assert.ok(error.errors[1] instanceof TypeError);
      assert.ok(error.errors[2] instanceof TypeError);
      return true;
    }
  );

  assert.equal(earlier.value, "original-earlier");
  assert.equal(middle.value, "stuck-middle");
  assert.equal(later.value, "stuck-later");
});

test("withMockedPropertyDescriptors restores later mocks after replacing Array.prototype.push", async () => {
  const target = { value: "original" };
  const originalPush = Object.getOwnPropertyDescriptor(Array.prototype, "push");
  assert.ok(originalPush);

  await withMockedPropertyDescriptors([
    {
      target: Array.prototype,
      name: "push",
      descriptor: { value() {} },
    },
    {
      target,
      name: "value",
      descriptor: { value: "mocked" },
    },
  ], () => {
    assert.equal(target.value, "mocked");
  });

  assert.equal(target.value, "original");
  assert.deepEqual(
    Object.getOwnPropertyDescriptor(Array.prototype, "push"),
    originalPush
  );
});
