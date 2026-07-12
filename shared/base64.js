// Stack-safe byte/base64 helpers shared by workerd tiers. Keep this module
// dependency-free so it can be embedded anywhere a small shared primitive is
// needed.

const CHUNK_SIZE = 0x8000;
const BASE64_ALPHABET_RE = /^[A-Za-z0-9+/]*$/;
const BASE64_WHITESPACE_RE = /[\t\n\f\r ]/;
const BASE64_WHITESPACE_GLOBAL_RE = /[\t\n\f\r ]/g;
const utf8Encoder = new TextEncoder();

// Buffer.from(..., "base64") skips invalid bytes and accepts base64url.
// Validate with the same forgiving-base64 grammar as atob first so the
// nodejs_compat and web-platform branches fail closed identically.
/** @param {string} value */
function validatedBufferBase64(value) {
  let payload = BASE64_WHITESPACE_RE.test(value)
    ? value.replace(BASE64_WHITESPACE_GLOBAL_RE, "")
    : value;
  if (payload.length % 4 === 0) {
    if (payload.endsWith("==")) payload = payload.slice(0, -2);
    else if (payload.endsWith("=")) payload = payload.slice(0, -1);
  }
  if (payload.length % 4 === 1 || !BASE64_ALPHABET_RE.test(payload)) {
    throw new TypeError("Invalid base64 input");
  }
  return payload;
}

/** @param {Uint8Array} bytes @returns {string} */
export function bytesToBase64(bytes) {
  if (typeof Buffer !== "undefined") return Buffer.from(bytes).toString("base64");
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += CHUNK_SIZE) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + CHUNK_SIZE));
  }
  return btoa(binary);
}

/** @param {string} value @returns {Uint8Array} */
export function base64ToBytes(value) {
  if (typeof Buffer !== "undefined") {
    return new Uint8Array(Buffer.from(validatedBufferBase64(value), "base64"));
  }
  const binary = atob(value);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) out[i] = binary.charCodeAt(i);
  return out;
}

/** @param {string} value @returns {string} */
export function textToBase64(value) {
  return bytesToBase64(utf8Encoder.encode(value));
}
