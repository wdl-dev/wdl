import { setDataField } from "shared-d1-data-field";

const BINARY_TAG = "__wdl_d1_binary_v1";
const PUBLIC_BINARY_TYPE = "blob";
const BINARY_TAG_RE = /^__wdl_d1_binary_v\d+$/;
const BASE64_CHUNK_SIZE = 0x8000;

/**
 * @param {Uint8Array} bytes
 * @returns {string}
 */
function bytesToBase64(bytes) {
  let bin = "";
  // This file is also injected as `_wdl-d1-transport.js` into loaded
  // workers, so it cannot import shared-base64 without source rewriting.
  for (let offset = 0; offset < bytes.length; offset += BASE64_CHUNK_SIZE) {
    bin += String.fromCharCode(...bytes.subarray(offset, offset + BASE64_CHUNK_SIZE));
  }
  return btoa(bin);
}

/**
 * @param {string} base64
 * @returns {Uint8Array}
 */
function base64ToBytes(base64) {
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
export function encodeD1Transport(value) {
  return walkD1Transport(value, (item) => {
    if (item instanceof Uint8Array) {
      return { [BINARY_TAG]: true, base64: bytesToBase64(item) };
    }
    if (item instanceof ArrayBuffer) {
      return { [BINARY_TAG]: true, base64: bytesToBase64(new Uint8Array(item)) };
    }
    if (ArrayBuffer.isView(item)) {
      return {
        [BINARY_TAG]: true,
        base64: bytesToBase64(new Uint8Array(item.buffer, item.byteOffset, item.byteLength)),
      };
    }
    return undefined;
  });
}

/**
 * @param {unknown} value
 * @returns {value is Record<string, unknown> & { base64: string }}
 */
function isTaggedBinary(value) {
  if (!value || typeof value !== "object") return false;
  const record = /** @type {Record<string, unknown>} */ (value);
  return record[BINARY_TAG] === true &&
    typeof record.base64 === "string" &&
    Object.keys(record).every((key) => key === BINARY_TAG || key === "base64");
}

/**
 * @param {Record<string, unknown>} value
 * @returns {void}
 */
function assertSupportedBinaryTag(value) {
  const keys = Object.keys(value);
  const tagKey = keys.find((key) => BINARY_TAG_RE.test(key));
  if (
    !tagKey ||
    tagKey === BINARY_TAG ||
    value[tagKey] !== true ||
    typeof value.base64 !== "string" ||
    !keys.every((key) => key === tagKey || key === "base64")
  ) {
    return;
  }
  const err = new Error(`D1 transport binary tag ${tagKey} is not supported by this runtime`);
  Object.assign(err, {
    code: "unsupported-d1-transport-version",
    category: "internal",
    retryable: false,
  });
  throw err;
}

/**
 * @param {unknown} value
 * @param {(value: object) => unknown | undefined} objectLeaf
 * @returns {unknown}
 */
function walkD1Transport(value, objectLeaf) {
  if (value == null || typeof value !== "object") return value;
  const leaf = objectLeaf(value);
  if (leaf !== undefined) return leaf;
  if (Array.isArray(value) && value.length === 0) return value;
  if (Array.isArray(value)) return value.map((item) => walkD1Transport(item, objectLeaf));
  if (Object.keys(value).length === 0) return value;
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const [key, item] of Object.entries(value)) setDataField(out, key, walkD1Transport(item, objectLeaf));
  return out;
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
export function decodeD1Transport(value) {
  return walkD1Transport(value, (item) => {
    const record = /** @type {Record<string, unknown>} */ (item);
    if (isTaggedBinary(record)) return base64ToBytes(record.base64);
    assertSupportedBinaryTag(record);
    return undefined;
  });
}

/**
 * @param {unknown} value
 * @returns {unknown}
 */
export function decodeD1TransportForJson(value) {
  return walkD1Transport(value, (item) => {
    const record = /** @type {Record<string, unknown>} */ (item);
    if (isTaggedBinary(record)) {
      return {
        type: PUBLIC_BINARY_TYPE,
        base64: record.base64,
        byteLength: base64ToBytes(record.base64).byteLength,
      };
    }
    assertSupportedBinaryTag(record);
    return undefined;
  });
}
