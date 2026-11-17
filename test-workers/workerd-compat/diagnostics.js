// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { ok, rejects, strictEqual } from "node:assert";

export default {
  async test(_controller, env) {
    await rejects(env.SOURCE.fetch("https://diagnostics.test/"));
    const deadline = Date.now() + 5_000;
    let exception;
    while (Date.now() < deadline) {
      exception = await (await env.SINK.fetch("https://diagnostics.test/last")).json();
      if (exception) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    ok(exception, "native exception should reach the tail worker");
    strictEqual(exception.name, "LongError");
    const length = new TextEncoder().encode(exception.message).byteLength;
    ok(length > 0 && length <= 4096, `native exception message has ${length} bytes`);
    strictEqual(exception.message.includes("\ufffd"), false);
  },
};
