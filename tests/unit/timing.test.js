import { test } from "node:test";
import assert from "node:assert/strict";
import { delay, settlementWithin, waitUntil } from "../helpers/timing.js";

test("delay resolves after the requested timeout", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let settled = false;
  const pending = delay(5).then(() => { settled = true; });

  await Promise.resolve();
  assert.equal(settled, false);
  t.mock.timers.tick(4);
  await Promise.resolve();
  assert.equal(settled, false);
  t.mock.timers.tick(1);
  await Promise.resolve();
  assert.equal(settled, true);
  await pending;
});

test("settlementWithin reports fulfillment and rejection", async () => {
  const error = new Error("rejected");

  assert.deepEqual(await settlementWithin(Promise.resolve("value")), {
    status: "fulfilled",
    value: "value",
  });
  assert.deepEqual(await settlementWithin(Promise.reject(error)), {
    status: "rejected",
    reason: error,
  });
});

test("settlementWithin reports promises that remain pending", async () => {
  const outcome = await settlementWithin(new Promise(() => {}), 1);

  assert.deepEqual(outcome, { status: "pending" });
});

test("waitUntil retries until the condition succeeds", async () => {
  let attempts = 0;
  await waitUntil("unit condition", () => {
    attempts += 1;
    return attempts === 2;
  }, { timeoutMs: 100, intervalMs: 1 });
  assert.equal(attempts, 2);
});

test("waitUntil reports the last thrown error on timeout", async () => {
  await assert.rejects(
    () => waitUntil("unit timeout", () => {
      throw new Error("last failure");
    }, { timeoutMs: 5, intervalMs: 1 }),
    /timeout waiting for unit timeout: last failure/
  );
});
