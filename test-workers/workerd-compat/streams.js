// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0
// Narrowed from workerd's streams-terminal-state regression.

import { deepStrictEqual, strictEqual } from "node:assert";

let session;

export const terminalReentry = {
  async test(_controller, env) {
    const origin = await env.SELF.fetch("https://streams.test/origin");
    strictEqual(await origin.text(), "armed");
    const response = await env.SELF.fetch("https://streams.test/settle");
    deepStrictEqual(await response.json(), {
      entries: 1,
      desiredSize: null,
      nestedError: "TypeError",
      results: [
        { error: "source error" },
        { done: false, bytes: [17] },
        { done: false, bytes: [17] },
      ],
    });
  },
};

export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/origin") {
      let controller;
      const source = new ReadableStream({
        type: "bytes",
        start(value) {
          controller = value;
        },
      });
      const [first, rest] = source.tee();
      const [second, third] = rest.tee();
      const readers = [first, second, third].map((stream) => stream.getReader({ mode: "byob" }));
      for (const reader of readers) reader.closed.catch(() => {});
      // Leave a partially filled read owned by this request after it returns.
      readers[0].read(new Uint8Array(8), { min: 2 }).catch(() => {});
      controller.enqueue(Uint8Array.of(17));
      session = { controller, readers };
      return new Response("armed");
    }

    const { controller, readers } = session;
    session = undefined;
    const pending = readers.map((reader) => reader.read(new Uint8Array(8)).then(
      ({ done, value }) => ({ done, bytes: Array.from(value) }),
      (error) => ({ error: error.message })
    ));
    let entries = 0;
    let desiredSize;
    let nestedError = null;
    const descriptor = Object.getOwnPropertyDescriptor(Error.prototype, "name");
    Object.defineProperty(Error.prototype, "name", {
      configurable: true,
      get() {
        return "Error";
      },
      set() {
        if (entries !== 0) return;
        entries++;
        desiredSize = controller.desiredSize;
        // Native cross-request diagnostics run this setter during settlement.
        try {
          controller.close();
        } catch (error) {
          nestedError = error.name;
        }
      },
    });
    try {
      controller.error(new Error("source error"));
    } finally {
      Object.defineProperty(Error.prototype, "name", descriptor);
    }
    const results = await Promise.all(pending);
    for (const reader of readers) reader.releaseLock();
    return Response.json({ entries, desiredSize, nestedError, results });
  },
};
