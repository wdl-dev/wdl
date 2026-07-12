import { once } from "node:events";
import { PassThrough, Writable } from "node:stream";
import { test } from "node:test";
import assert from "node:assert/strict";
import { pipeWithPrefix } from "../../scripts/_stream-prefix.js";

test("pipeWithPrefix prefixes complete lines and flushes a partial tail", async () => {
  const source = new PassThrough();
  /** @type {string[]} */
  const chunks = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(String(chunk));
      callback();
    },
  });
  pipeWithPrefix(source, destination, "[task] ");

  source.write("first");
  source.write(" line\nsecond\n");
  source.end("tail");
  await once(source, "end");

  assert.equal(chunks.join(""), "[task] first line\n[task] second\n[task] tail\n");
});
