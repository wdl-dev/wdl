import { test } from "node:test";
import assert from "node:assert/strict";
import { nextFireMs, slotMsFor } from "../../shared/cron-time.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";

const cronParityCases = readRepositoryJson("tests/fixtures/cron-parity.json");

test("cron parity fixture is valid", () => {
  assert.ok(Array.isArray(cronParityCases), "cron-parity fixture must be an array");
  assert.ok(cronParityCases.length > 0, "cron-parity fixture must not be empty");
  for (const entry of cronParityCases) {
    assert.ok(entry && typeof entry === "object", "each fixture entry must be an object");
    assert.equal(typeof entry.id, "string", "entry.id must be a string");
    assert.equal(typeof entry.cron, "string", `${entry.id}: entry.cron must be a string`);
    assert.equal(typeof entry.timezone, "string", `${entry.id}: entry.timezone must be a string`);
    assert.equal(typeof entry.afterMs, "number", `${entry.id}: entry.afterMs must be a number`);
    assert.equal(typeof entry.nextMs, "number", `${entry.id}: entry.nextMs must be a number`);
  }
});

test("nextFireMs: next */5 after 12:00 is 12:05", () => {
  const base = Date.UTC(2026, 3, 15, 12, 0, 0);
  assert.equal(nextFireMs("*/5 * * * *", "UTC", base), Date.UTC(2026, 3, 15, 12, 5, 0));
});

test("nextFireMs: respects timezone — 09:00 Shanghai = 01:00 UTC", () => {
  const base = Date.UTC(2026, 3, 15, 0, 0, 0);
  assert.equal(
    nextFireMs("0 9 * * *", "Asia/Shanghai", base),
    Date.UTC(2026, 3, 15, 1, 0, 0)
  );
});

test("nextFireMs: invalid cron throws", () => {
  assert.throws(() => nextFireMs("not a cron", "UTC", 0));
});

test("nextFireMs: rejects non-5-field cron syntax", () => {
  assert.throws(
    () => nextFireMs("* * * * * *", "UTC", 0),
    /exactly 5 fields/
  );
  assert.throws(
    () => nextFireMs("@daily", "UTC", 0),
    /exactly 5 fields/
  );
});

test("nextFireMs: rejects date-like fields before croner one-shot parsing", () => {
  assert.throws(
    () => nextFireMs("9999:9 * * * *", "UTC", 0),
    /must not contain ':'/
  );
});

test("nextFireMs: matches shared JS/Rust parity fixture", () => {
  for (const entry of cronParityCases) {
    assert.equal(
      nextFireMs(entry.cron, entry.timezone, entry.afterMs),
      entry.nextMs,
      entry.id
    );
  }
});

test("slotMsFor: minute-aligned", () => {
  assert.equal(slotMsFor(Date.UTC(2026, 3, 15, 12, 5, 42, 123)), Date.UTC(2026, 3, 15, 12, 5, 0));
  assert.equal(slotMsFor(Date.UTC(2026, 3, 15, 12, 5, 0)), Date.UTC(2026, 3, 15, 12, 5, 0));
});
