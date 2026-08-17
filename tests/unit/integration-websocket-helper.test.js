import assert from "node:assert/strict";
import { test } from "node:test";

import {
  encodeClientBinaryFrame,
  encodeClientCloseFrame,
  encodeClientTextFrame,
} from "../integration/helpers/websocket.js";

/** @param {Buffer} frame */
function decodeClientFrame(frame) {
  assert.equal((frame[1] & 0x80) !== 0, true);
  let length = frame[1] & 0x7f;
  let offset = 2;
  if (length === 126) {
    length = frame.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    length = Number(frame.readBigUInt64BE(offset));
    offset += 8;
  }
  const mask = frame.subarray(offset, offset + 4);
  offset += 4;
  const payload = Buffer.alloc(length);
  for (let index = 0; index < length; index += 1) {
    payload[index] = frame[offset + index] ^ mask[index % 4];
  }
  assert.equal(frame.length, offset + length);
  return payload;
}

test("integration WebSocket helper encodes short and extended client frames", () => {
  for (const length of [125, 126, 65_536]) {
    const payload = Buffer.alloc(length, length % 251);
    assert.deepEqual(decodeClientFrame(encodeClientBinaryFrame(payload)), payload);
  }
  assert.equal(decodeClientFrame(encodeClientTextFrame("agent tool output")).toString(), "agent tool output");
});

test("integration WebSocket helper keeps control frames within RFC 6455 bounds", () => {
  assert.throws(
    () => encodeClientCloseFrame(1000, "x".repeat(124)),
    /control frame payload exceeds 125 bytes/
  );
});
