// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0
// Stdio cases adapted from workerd's stdio-writesync-reentry-uaf-test.js.

import { match, strictEqual, throws } from "node:assert";
import { writeSync } from "node:fs";
import { BlockList } from "node:net";

function assertStdioSnapshot(mutate) {
  const lineCount = 64;
  const buffer = new ArrayBuffer(lineCount * 5, { maxByteLength: 1024 });
  const bytes = new Uint8Array(buffer);
  bytes.set(new TextEncoder().encode("line\n".repeat(lineCount)));
  const originalLog = console.log;
  const lines = [];
  console.log = (line) => {
    lines.push(line);
    if (lines.length === 1) mutate(buffer);
  };
  try {
    writeSync(1, bytes);
  } finally {
    console.log = originalLog;
  }
  strictEqual(buffer.byteLength, 0);
  strictEqual(lines.length, lineCount);
  for (const line of lines) match(line, /(^| )line$/);
}

export const stdioShrinkDuringLog = {
  test() { assertStdioSnapshot((buffer) => buffer.resize(0)); },
};

export const stdioTransferDuringLog = {
  test() { assertStdioSnapshot((buffer) => new Uint8Array(buffer.transfer()).fill(0x5a)); },
};

export const blockListAddressForms = {
  test() {
    const range = new BlockList();
    range.addRange("10.0.0.0", "200.0.0.0");
    strictEqual(range.check("150.0.0.1"), true);
    strictEqual(range.check("200.0.0.1"), false);
    throws(() => range.addRange("200.0.0.0", "10.0.0.0"));
    const v6 = new BlockList();
    v6.addAddress("2001:DB8::1", "ipv6");
    strictEqual(v6.check("2001:db8:0:0:0:0:0:1", "ipv6"), true);
    const mapped = new BlockList();
    mapped.addAddress("192.168.10.1");
    strictEqual(mapped.check("::ffff:c0a8:a01", "ipv6"), true);
    strictEqual(mapped.check("64:ff9b::192.168.10.1", "ipv6"), false);
  },
};

export const integerConversionBoundaries = {
  test() {
    const blob = new Blob(["abc"]);
    strictEqual(blob.slice(2147483647.5).size, 0);
    strictEqual(blob.slice(-2147483648.5).size, 3);
    throws(() => blob.slice(2147483648), TypeError);
    throws(() => blob.slice(-2147483649), TypeError);
    // The <=32-bit truncation rule does not relax 64-bit stream lengths.
    throws(() => new FixedLengthStream(-0.5), TypeError);
    throws(() => new ReadableStream({}, { highWaterMark: -0.5 }), TypeError);
  },
};
