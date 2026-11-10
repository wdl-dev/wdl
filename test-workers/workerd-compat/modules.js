// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0
// Adapted from workerd's module-tla-nested-require and unknown-import-assertions tests.

import { rejects, strictEqual } from "node:assert";

export const cyclicRequireRejectsWithoutCrashing = {
  async test() {
    await rejects(import("./require-entry.js"), /Circular dependency when resolving module/);
    strictEqual((await import("./healthy.js")).default, 42);
  },
};

export const nestedDynamicImportSettles = {
  async test() {
    const entry = await import("./dynamic-entry.js");
    strictEqual((await entry.chunk).result, 2);
  },
};

export const failedLinkLeavesGraphReusable = {
  async test() {
    const expected = /Unrecognized import attributes|Unsupported import attribute/;
    await rejects(import("./invalid-root.js"), expected);
    await rejects(import("./invalid-root.js"), expected);
    await rejects(import("./invalid-sibling.js"), expected);
    strictEqual((await import("./healthy.js")).default, 42);
  },
};
