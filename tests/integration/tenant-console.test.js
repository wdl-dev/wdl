// A tenant's console formatting failure must stay inside that tenant's
// invocation: it cannot abort the shared runtime process or disrupt other
// namespaces served by it.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  deployAndPromote,
  gatewayFetch,
  readIntegrationJson,
  setupIntegrationSuite,
  uniqueNs,
} from "./helpers/index.js";

setupIntegrationSuite();

// Each value makes node:util formatting throw something that cannot itself
// be stringified, so workerd's formatter fallback throws a second time.
const CONSOLE_FORMAT_FAILURE_WORKER = `
export default {
  fetch() {
    const cases = [
      () => console.log({ get [Symbol.toStringTag]() { throw Object.create(null); } }),
      () => console.error({ [Symbol.for("nodejs.util.inspect.custom")]() { throw Object.create(null); } }),
    ];
    const results = [];
    for (const run of cases) {
      try {
        run();
        results.push("returned");
      } catch (err) {
        results.push("threw");
      }
    }
    return Response.json({ results });
  },
};
`;

const BYSTANDER_WORKER = `export default { fetch() { return new Response("bystander ok"); } };`;

test("console formatting failures do not crash the runtime or other tenants", async () => {
  const tenantNs = uniqueNs("console-format");
  const bystanderNs = uniqueNs("console-bystander");
  await deployAndPromote(tenantNs, "w", { code: CONSOLE_FORMAT_FAILURE_WORKER });
  await deployAndPromote(bystanderNs, "w", { code: BYSTANDER_WORKER });

  const before = await gatewayFetch(bystanderNs, "/w/");
  assert.equal(before.status, 200);
  assert.equal(await before.text(), "bystander ok");

  // workerd drops the unformattable log line without surfacing an exception.
  assert.deepEqual(
    await readIntegrationJson(await gatewayFetch(tenantNs, "/w/"), 200, "console format failure"),
    { results: ["returned", "returned"] }
  );

  const after = await gatewayFetch(bystanderNs, "/w/");
  assert.equal(after.status, 200);
  assert.equal(await after.text(), "bystander ok");
});
