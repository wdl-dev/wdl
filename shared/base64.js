// Stack-safe byte/base64 helpers shared by workerd tiers. Keep this module
// dependency-free so it can be embedded anywhere a small shared primitive is
// needed.

const CHUNK_SIZE = 0x8000;
const BASE64_NON_ALPHABET_RE = /[^A-Za-z0-9+/]/;
const BASE64_WHITESPACE_RE = /[\t\n\f\r ]/;
const BASE64_WHITESPACE_GLOBAL_RE = /[\t\n\f\r ]/g;
const utf8Encoder = new TextEncoder();
const nodeBuffer = /** @type {{ Buffer?: WdlNodeBufferConstructor }} */ (globalThis).Buffer;

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
  if (payload.length % 4 === 1 || BASE64_NON_ALPHABET_RE.test(payload)) {
    throw new TypeError("Invalid base64 input");
  }
  return payload;
}

/** @param {number} code */
function base64Sextet(code) {
  if (code >= 0x41 && code <= 0x5a) return code - 0x41;
  if (code >= 0x61 && code <= 0x7a) return code - 0x61 + 26;
  if (code >= 0x30 && code <= 0x39) return code - 0x30 + 52;
  if (code === 0x2b) return 62;
  if (code === 0x2f) return 63;
  return -1;
}

/** @param {string} value */
export function assertCanonicalBase64(value) {
  if (value.length % 4 !== 0) throw new TypeError("Invalid canonical base64 input");

  let payloadEnd = value.length;
  let padding = 0;
  if (payloadEnd > 0 && value.charCodeAt(payloadEnd - 1) === 0x3d) {
    padding = 1;
    payloadEnd -= 1;
    if (payloadEnd > 0 && value.charCodeAt(payloadEnd - 1) === 0x3d) {
      padding = 2;
      payloadEnd -= 1;
    }
  }
  const payload = payloadEnd === value.length ? value : value.slice(0, payloadEnd);
  if (BASE64_NON_ALPHABET_RE.test(payload)) {
    throw new TypeError("Invalid canonical base64 input");
  }

  const finalSextet = payloadEnd === 0 ? -1 : base64Sextet(value.charCodeAt(payloadEnd - 1));
  if (
    (padding === 1 && (finalSextet < 0 || (finalSextet & 0x03) !== 0)) ||
    (padding === 2 && (finalSextet < 0 || (finalSextet & 0x0f) !== 0))
  ) {
    throw new TypeError("Invalid canonical base64 input");
  }
}

/** @param {Uint8Array} bytes @returns {string} */
export function bytesToBase64(bytes) {
  if (nodeBuffer) return nodeBuffer.from(bytes).toString("base64");
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += CHUNK_SIZE) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + CHUNK_SIZE));
  }
  return btoa(binary);
}

/** @param {string} value @returns {Uint8Array} */
function decodeBase64(value) {
  if (nodeBuffer) return new Uint8Array(nodeBuffer.from(value, "base64"));
  const binary = atob(value);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) out[i] = binary.charCodeAt(i);
  return out;
}

/** @param {string} value @returns {Uint8Array} */
export function base64ToBytes(value) {
  return decodeBase64(nodeBuffer ? validatedBufferBase64(value) : value);
}

/** @param {string} value @returns {Uint8Array} */
export function canonicalBase64ToBytes(value) {
  assertCanonicalBase64(value);
  return decodeBase64(value);
}

/** @param {readonly (string | null)[]} values */
export function prepareCanonicalBase64Values(values) {
  const prepared = Array.from(values);
  for (const value of prepared) {
    if (value !== null) assertCanonicalBase64(value);
  }
  return {
    /**
     * @template T
     * @param {(value: Uint8Array | null, index: number) => T} mapper
     * @returns {T[]}
     */
    map(mapper) {
      return prepared.map((value, index) =>
        mapper(value === null ? null : decodeBase64(value), index));
    },
  };
}

/** @param {string} value @returns {string} */
export function textToBase64(value) {
  return bytesToBase64(utf8Encoder.encode(value));
}
