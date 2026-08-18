import assert from "node:assert/strict";
import { test } from "node:test";

import { canonicalPositiveIntegerEnv } from "../../shared/env.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";

for (const fixturePath of [
  "tests/fixtures/owner-ttl-env.json",
  "tests/fixtures/owner-drain-timeout-env.json",
]) {
  test(`${fixturePath} follows the canonical positive integer env contract`, () => {
    const fixture = readRepositoryJson(fixturePath);
    for (const { name, raw, expected } of fixture.cases) {
      const env = raw === null ? {} : { VALUE: raw };
      assert.equal(
        canonicalPositiveIntegerEnv(env, "VALUE", fixture.fallback, fixture.max),
        expected,
        name,
      );
    }
  });
}
