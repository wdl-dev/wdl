// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0
// ALS case adapted from workerd's als-gc-test.js.

import { strictEqual } from "node:assert";
import { AsyncLocalStorage } from "node:async_hooks";

async function assertCyclesCollected(label, allocate) {
  const count = 40;
  let finalized = 0;
  const registry = new FinalizationRegistry(() => {
    finalized++;
  });
  // Allocate in a separate call frame so no loop local retains the final cycle.
  for (let i = 0; i < count; i++) allocate(registry);
  for (let i = 0; i < 12 && finalized < count; i++) {
    gc();
    // Finalization callbacks need a turn after each collection.
    await scheduler.wait(1);
  }
  strictEqual(finalized, count, `${label}: collected ${finalized}/${count} cycles`);
}

export const storeReachingResponseIsCollected = {
  async test() {
    const als = new AsyncLocalStorage();
    await assertCyclesCollected("ALS store/Response", (registry) => {
      const store = {};
      registry.register(store, undefined);
      als.run(store, () => {
        // Response captures the native async frame, which retains this store.
        store.response = new Response(null);
      });
    });
  },
};

export const backpressuredChunkCycle = {
  async test() {
    await assertCyclesCollected("backpressured TransformStream chunk", (registry) => {
      const stream = new TransformStream();
      registry.register(stream, undefined);
      // No reader: the queued chunk points back to its stream. Awaiting the
      // write, draining, or aborting would hang or dismantle the cycle under test.
      void stream.writable.getWriter().write(stream).catch(() => {});
    });
  },
};

export const iteratorCycle = {
  async test() {
    await assertCyclesCollected("ReadableStream.values iterator/reader", (registry) => {
      const stream = new ReadableStream();
      const iterator = stream.values();
      // The iterator's native reader retains the stream; do not release its lock.
      stream.iterator = iterator;
      registry.register(iterator, undefined);
    });
  },
};
