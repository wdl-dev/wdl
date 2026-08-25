import { utf8ByteLength } from "./utf8.js";

export const D1_MAX_VALUE_BYTES = 2_000_000;

/**
 * @param {number} bytes
 * @param {string} kind
 * @returns {void}
 */
function assertValueBytes(bytes, kind) {
  if (bytes > D1_MAX_VALUE_BYTES) {
    throw new Error(`D1_LIMIT_ERROR: Maximum ${kind} size is ${D1_MAX_VALUE_BYTES} bytes`);
  }
}

/**
 * @typedef {string | number | null | undefined | Uint8Array<ArrayBuffer>} NormalizedD1Param
 */

/** @param {Uint8Array<ArrayBufferLike>} bytes */
function snapshotBytes(bytes) {
  const snapshot = new Uint8Array(bytes.byteLength);
  snapshot.set(bytes);
  return snapshot;
}

/**
 * @param {unknown} value
 * @param {boolean} snapshot
 * @returns {NormalizedD1Param}
 */
function normalizeParam(value, snapshot) {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`D1_TYPE_ERROR: Non-finite number '${value}' not supported`);
    }
    return Object.is(value, -0) ? 0 : value;
  }
  if (typeof value === "bigint") {
    if (value < BigInt(Number.MIN_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`D1_TYPE_ERROR: BigInt '${value}' exceeds safe integer range`);
    }
    return Number(value);
  }
  if (typeof value === "string") {
    if (value.length * 3 > D1_MAX_VALUE_BYTES) {
      assertValueBytes(utf8ByteLength(value), "string");
    }
    return value;
  }
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value === null) return null;
  if (value === undefined) return undefined;
  if (Array.isArray(value)) {
    if (value.every((b) => Number.isInteger(b) && b >= 0 && b <= 255)) {
      assertValueBytes(value.length, "BLOB");
      return Uint8Array.from(value);
    }
  }
  if (value instanceof Uint8Array) {
    assertValueBytes(value.byteLength, "BLOB");
    return snapshot || !(value.buffer instanceof ArrayBuffer)
      ? snapshotBytes(value)
      : /** @type {Uint8Array<ArrayBuffer>} */ (value);
  }
  if (value instanceof ArrayBuffer) {
    assertValueBytes(value.byteLength, "BLOB");
    return snapshotBytes(new Uint8Array(value));
  }
  if (ArrayBuffer.isView(value)) {
    const view = /** @type {ArrayBufferView} */ (value);
    assertValueBytes(view.byteLength, "BLOB");
    return snapshotBytes(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
  }
  throw new Error(`D1_TYPE_ERROR: Type '${typeof value}' not supported for value '${value}'`);
}

/**
 * @param {unknown} value
 * @returns {NormalizedD1Param}
 */
export function normalizeD1Param(value) {
  return normalizeParam(value, true);
}

/**
 * Validate an already-owned internal wire value without recopying native bytes.
 * @param {unknown} value
 * @returns {NormalizedD1Param}
 */
export function normalizeD1WireParam(value) {
  return normalizeParam(value, false);
}
