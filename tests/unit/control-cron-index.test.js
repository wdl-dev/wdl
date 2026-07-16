// Unit coverage for control/cron-index.js. The source imports via the
// workerd capnp module name "shared-cron-time" which Node can't resolve
// as a bare specifier, so we load the file via data: URL with that
// re-export stubbed out (the tests below don't exercise nextFireMs /
// slotMsFor — those go through scheduler's node-redis path where the
// cron engine dependency is already wired).

import { test } from "node:test";
import assert from "node:assert/strict";
import { importRepositoryModule, readRepositoryJson } from "../helpers/load-shared-module.js";

const schedulerProjectionContract = /** @type {{ cron: { cronId: string, entry: { cron: string, timezone: string } } }} */ (
  readRepositoryJson("tests/fixtures/scheduler-projection-contract.json")
);

const { cronId, diffCrons } = await importRepositoryModule("control/cron-index.js", [
  [/export \{[^}]+\} from "shared-cron-time";/, "/* re-export stubbed for node test */"],
]);

test("cronId: stable + 10 hex chars", () => {
  assert.equal(cronId("*/5 * * * *", "UTC"), cronId("*/5 * * * *", "UTC"));
  assert.match(cronId("*/5 * * * *", "UTC"), /^[0-9a-f]{10}$/);
});

test("cronId matches the cross-language scheduler projection fixture", () => {
  assert.equal(
    cronId(
      schedulerProjectionContract.cron.entry.cron,
      schedulerProjectionContract.cron.entry.timezone
    ),
    schedulerProjectionContract.cron.cronId
  );
});

test("cronId: differs on cron or timezone", () => {
  assert.notEqual(cronId("*/5 * * * *", "UTC"), cronId("*/6 * * * *", "UTC"));
  assert.notEqual(cronId("*/5 * * * *", "UTC"), cronId("*/5 * * * *", "Asia/Shanghai"));
});

test("diffCrons: all new entries reported as added (no gen assigned)", () => {
  const { added, removed, kept } = diffCrons(
    {},
    [{ cron: "*/5 * * * *", timezone: "UTC" }]
  );
  assert.equal(added.length, 1);
  assert.equal(removed.length, 0);
  assert.equal(kept.length, 0);
  assert.equal(added[0].cron, "*/5 * * * *");
  assert.equal(added[0].timezone, "UTC");
  assert.equal(added[0].gen, undefined);
});

test("diffCrons: unchanged entry preserves prior gen (no schedule reset)", () => {
  const id = cronId("*/5 * * * *", "UTC");
  const old = { [id]: { cron: "*/5 * * * *", timezone: "UTC", gen: 7 } };
  const { added, removed, kept } = diffCrons(
    old,
    [{ cron: "*/5 * * * *", timezone: "UTC" }]
  );
  assert.equal(added.length, 0);
  assert.equal(removed.length, 0);
  assert.equal(kept.length, 1);
  assert.equal(kept[0].gen, 7);
});

test("diffCrons: removed entry reported with its gen", () => {
  const id = cronId("0 0 * * *", "UTC");
  const old = { [id]: { cron: "0 0 * * *", timezone: "UTC", gen: 3 } };
  const { added, removed, kept } = diffCrons(old, []);
  assert.equal(added.length, 0);
  assert.equal(kept.length, 0);
  assert.deepEqual(removed, [{ id, gen: 3 }]);
});

test("diffCrons: same cron, different timezone = different entry", () => {
  const oldId = cronId("0 9 * * *", "UTC");
  const old = { [oldId]: { cron: "0 9 * * *", timezone: "UTC", gen: 1 } };
  const { added, removed } = diffCrons(
    old,
    [{ cron: "0 9 * * *", timezone: "Asia/Shanghai" }]
  );
  assert.equal(added.length, 1);
  assert.equal(added[0].timezone, "Asia/Shanghai");
  assert.deepEqual(removed, [{ id: oldId, gen: 1 }]);
});
