const HEX_BYTE_TABLE = Array.from(
  { length: 256 },
  (_, value) => value.toString(16).padStart(2, "0")
);

/** @param {Uint8Array} bytes @returns {string} */
export function bytesToHex(bytes) {
  let out = "";
  for (let i = 0; i < bytes.length; i += 1) out += HEX_BYTE_TABLE[bytes[i]];
  return out;
}
