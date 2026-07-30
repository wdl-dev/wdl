import { normalizeD1Param } from "shared-d1-params";
import { setDataField } from "shared-d1-data-field";

export const D1_QUERY_CONTENT_TYPE = "application/vnd.wdl.d1-query";
export const D1_QUERY_RESPONSE_CONTENT_TYPE = "application/vnd.wdl.d1-query-response";

const WIRE_VARINT = 0;
const WIRE_FIXED64 = 1;
const WIRE_LEN = 2;
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

/**
 * @typedef {string | number | number[] | null} D1QueryParam
 * @typedef {Uint8Array<ArrayBufferLike>} ByteArray
 * @typedef {{ sql?: unknown, params?: unknown[] }} D1QueryStatementInput
 * @typedef {{ sql: string, params: D1QueryParam[] }} D1QueryStatement
 * @typedef {{
 *   namespace?: unknown,
 *   databaseId?: unknown,
 *   binding?: unknown,
 *   mode?: unknown,
 *   statements?: D1QueryStatementInput[],
 * }} D1QueryRequestInput
 * @typedef {{
 *   namespace: string,
 *   databaseId: string,
 *   binding: string | null,
 *   mode: string | undefined,
 *   statements: D1QueryStatement[],
 * }} D1QueryRequest
 */

/** @param {string} value */
function bytesOf(value) {
  return utf8Encoder.encode(value);
}

/** @param {number | bigint} value */
function checkedVarint(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) throw new Error("invalid varint");
  return n;
}

/** @param {number} value */
function varintByteLength(value) {
  let n = value;
  let length = 1;
  while (n >= 0x80) {
    n = Math.floor(n / 0x80);
    length += 1;
  }
  return length;
}

/**
 * @param {Uint8Array} destination
 * @param {number} offset
 * @param {number} value
 */
function writeVarint(destination, offset, value) {
  let n = value;
  while (n >= 0x80) {
    destination[offset++] = (n & 0x7f) | 0x80;
    n = Math.floor(n / 0x80);
  }
  destination[offset++] = n;
  return offset;
}

/**
 * @typedef {{ kind: "length", value: number, bodyStart: number } | { kind: "double", value: number }} WireMarker
 * @typedef {number | ByteArray | WireMarker} WirePart
 */

class WirePlan {
  constructor() {
    /** @type {WirePart[]} */
    this.parts = [];
    this.byteLength = 0;
  }

  /** @param {number | bigint} value */
  appendVarint(value) {
    const normalized = checkedVarint(value);
    this.parts.push(normalized);
    this.byteLength += varintByteLength(normalized);
  }

  /** @param {ByteArray} bytes */
  appendBytes(bytes) {
    this.parts.push(bytes);
    this.byteLength += bytes.length;
  }

  /** @param {number} field @param {number | bigint} value */
  varintField(field, value) {
    this.appendVarint(field * 8 + WIRE_VARINT);
    this.appendVarint(value);
  }

  /** @param {number} field @param {number} value */
  doubleField(field, value) {
    this.appendVarint(field * 8 + WIRE_FIXED64);
    this.parts.push({ kind: "double", value });
    this.byteLength += 8;
  }

  /** @param {number} field @param {ByteArray} bytes */
  bytesField(field, bytes) {
    this.appendVarint(field * 8 + WIRE_LEN);
    this.appendVarint(bytes.length);
    this.appendBytes(bytes);
  }

  /** @param {number} field @param {unknown} value */
  stringField(field, value) {
    if (value == null) return;
    this.bytesField(field, bytesOf(String(value)));
  }

  /** @param {number} field @param {unknown} value */
  nonEmptyStringField(field, value) {
    if (value == null || value === "") return;
    this.stringField(field, value);
  }

  /** @param {number} field */
  beginMessage(field) {
    this.appendVarint(field * 8 + WIRE_LEN);
    /** @type {Extract<WireMarker, { kind: "length" }>} */
    const marker = {
      kind: "length",
      value: 0,
      bodyStart: this.byteLength,
    };
    this.parts.push(marker);
    return marker;
  }

  /** @param {Extract<WireMarker, { kind: "length" }>} marker */
  endMessage(marker) {
    marker.value = this.byteLength - marker.bodyStart;
    this.byteLength += varintByteLength(marker.value);
  }

  /** @returns {Uint8Array<ArrayBuffer>} */
  finish() {
    const output = new Uint8Array(this.byteLength);
    const view = new DataView(output.buffer, output.byteOffset, output.byteLength);
    let offset = 0;
    for (const part of this.parts) {
      if (typeof part === "number") {
        offset = writeVarint(output, offset, part);
      } else if (part instanceof Uint8Array) {
        output.set(part, offset);
        offset += part.length;
      } else if (part.kind === "length") {
        offset = writeVarint(output, offset, part.value);
      } else {
        view.setFloat64(offset, part.value, true);
        offset += 8;
      }
    }
    if (offset !== output.length) throw new Error("D1 query wire size mismatch");
    return output;
  }
}

/** @param {WirePlan} plan @param {unknown} value */
function appendParam(plan, value) {
  const normalized = normalizeD1Param(value);
  if (normalized == null) {
    plan.varintField(1, 1);
  } else if (typeof normalized === "number") {
    plan.doubleField(2, normalized);
  } else if (typeof normalized === "string") {
    plan.stringField(3, normalized);
  } else if (Array.isArray(normalized)) {
    plan.bytesField(4, Uint8Array.from(normalized));
  } else {
    throw new Error(`D1_TYPE_ERROR: Type '${typeof normalized}' not supported for query wire`);
  }
}

/** @param {WirePlan} plan @param {D1QueryStatementInput | null | undefined} statement */
function appendStatement(plan, statement) {
  plan.nonEmptyStringField(1, statement?.sql);
  for (const param of statement?.params || []) {
    const message = plan.beginMessage(2);
    appendParam(plan, param);
    plan.endMessage(message);
  }
}

/** @param {D1QueryRequestInput | null | undefined} input */
export function encodeD1QueryRequest(input) {
  const plan = new WirePlan();
  plan.nonEmptyStringField(1, input?.namespace);
  plan.nonEmptyStringField(2, input?.databaseId);
  plan.nonEmptyStringField(3, input?.binding);
  plan.nonEmptyStringField(4, input?.mode);
  for (const statement of input?.statements || []) {
    const message = plan.beginMessage(5);
    appendStatement(plan, statement);
    plan.endMessage(message);
  }
  return plan.finish();
}

class Reader {
  /** @param {ArrayBuffer | ArrayBufferView<ArrayBufferLike>} bytes */
  constructor(bytes) {
    this.bytes = bytes instanceof Uint8Array
      ? bytes
      : bytes instanceof ArrayBuffer
        ? new Uint8Array(bytes)
        : new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.offset = 0;
    this.end = this.bytes.length;
  }

  /** @returns {boolean} */
  done() {
    return this.offset >= this.end;
  }

  /** @returns {number} */
  readVarint() {
    let shift = 0;
    let out = 0;
    while (this.offset < this.end && shift <= 49) {
      const byte = this.bytes[this.offset++];
      out += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return out;
      shift += 7;
    }
    throw new Error("truncated varint");
  }

  /** @returns {{ field: number, wireType: number }} */
  readTag() {
    const value = this.readVarint();
    return { field: Math.floor(value / 8), wireType: value % 8 };
  }

  /** @param {number} length */
  readBytes(length) {
    if (length < 0 || this.offset + length > this.end) throw new Error("truncated length-delimited field");
    const out = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return out;
  }

  /** @returns {number} */
  readLengthDelimitedEnd() {
    const length = this.readVarint();
    const end = this.offset + length;
    if (end < this.offset || end > this.end) throw new Error("truncated length-delimited field");
    return end;
  }

  /** @returns {Uint8Array} */
  readLengthDelimited() {
    const end = this.readLengthDelimitedEnd();
    return this.readBytes(end - this.offset);
  }

  /**
   * @template T
   * @param {(reader: Reader) => T} decode
   * @returns {T}
   */
  readMessage(decode) {
    const parentEnd = this.end;
    const childEnd = this.readLengthDelimitedEnd();
    this.end = childEnd;
    try {
      return decode(this);
    } finally {
      this.offset = childEnd;
      this.end = parentEnd;
    }
  }

  /** @returns {string} */
  readString() {
    return utf8Decoder.decode(this.readLengthDelimited());
  }

  /** @returns {number} */
  readDouble() {
    const bytes = this.readBytes(8);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0, true);
  }

  /** @param {number} wireType */
  skip(wireType) {
    if (wireType === WIRE_VARINT) {
      this.readVarint();
      return;
    }
    if (wireType === WIRE_FIXED64) {
      this.readBytes(8);
      return;
    }
    if (wireType === WIRE_LEN) {
      this.offset = this.readLengthDelimitedEnd();
      return;
    }
    throw new Error(`unsupported wire type ${wireType}`);
  }
}

/** @param {Reader} reader */
function decodeParam(reader) {
  let seen = false;
  let value = null;
  while (!reader.done()) {
    const { field, wireType } = reader.readTag();
    seen = true;
    if (field === 1 && wireType === WIRE_VARINT) {
      reader.readVarint();
      value = null;
    } else if (field === 2 && wireType === WIRE_FIXED64) {
      value = reader.readDouble();
    } else if (field === 3 && wireType === WIRE_LEN) {
      value = reader.readString();
    } else if (field === 4 && wireType === WIRE_LEN) {
      value = Array.from(reader.readLengthDelimited());
    } else {
      reader.skip(wireType);
    }
  }
  if (!seen) throw new Error("empty D1 param");
  return value;
}

/** @param {Reader} reader */
function decodeStatement(reader) {
  /** @type {D1QueryStatement} */
  const statement = { sql: "", params: [] };
  while (!reader.done()) {
    const { field, wireType } = reader.readTag();
    if (field === 1 && wireType === WIRE_LEN) {
      statement.sql = reader.readString();
    } else if (field === 2 && wireType === WIRE_LEN) {
      statement.params.push(reader.readMessage(decodeParam));
    } else {
      reader.skip(wireType);
    }
  }
  return statement;
}

/** @param {ArrayBuffer | ArrayBufferView<ArrayBufferLike>} bytes */
export function decodeD1QueryRequest(bytes) {
  const reader = new Reader(bytes);
  /** @type {D1QueryRequest} */
  const out = { namespace: "", databaseId: "", binding: null, mode: undefined, statements: [] };
  while (!reader.done()) {
    const { field, wireType } = reader.readTag();
    if (field === 1 && wireType === WIRE_LEN) {
      out.namespace = reader.readString();
    } else if (field === 2 && wireType === WIRE_LEN) {
      out.databaseId = reader.readString();
    } else if (field === 3 && wireType === WIRE_LEN) {
      out.binding = reader.readString();
    } else if (field === 4 && wireType === WIRE_LEN) {
      out.mode = reader.readString();
    } else if (field === 5 && wireType === WIRE_LEN) {
      out.statements.push(reader.readMessage(decodeStatement));
    } else {
      reader.skip(wireType);
    }
  }
  return out;
}

/** @param {WirePlan} plan @param {unknown} value */
function appendValue(plan, value) {
  if (value == null) {
    plan.varintField(1, 1);
    return;
  }
  if (typeof value === "boolean") {
    plan.varintField(2, value ? 1 : 0);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("D1 response value contains a non-finite number");
    plan.doubleField(3, value);
    return;
  }
  if (typeof value === "string") {
    plan.stringField(4, value);
    return;
  }
  if (value instanceof Uint8Array) {
    plan.bytesField(5, value);
    return;
  }
  if (value instanceof ArrayBuffer) {
    plan.bytesField(5, new Uint8Array(value));
    return;
  }
  if (ArrayBuffer.isView(value)) {
    plan.bytesField(5, new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    return;
  }
  if (Array.isArray(value)) {
    plan.varintField(8, 1);
    for (const item of value) {
      const message = plan.beginMessage(6);
      appendValue(plan, item);
      plan.endMessage(message);
    }
    return;
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    plan.varintField(9, 1);
    for (const [key, item] of Object.entries(value)) {
      if (item === undefined) continue;
      const entryMessage = plan.beginMessage(7);
      plan.stringField(1, key);
      const valueMessage = plan.beginMessage(2);
      appendValue(plan, item);
      plan.endMessage(valueMessage);
      plan.endMessage(entryMessage);
    }
    return;
  }
  throw new Error(`D1 response value type ${Object.prototype.toString.call(value)} is not supported`);
}

/** @param {Reader} reader @returns {[string, unknown]} */
function decodeObjectEntry(reader) {
  let key = "";
  /** @type {unknown} */
  let value = null;
  while (!reader.done()) {
    const { field, wireType } = reader.readTag();
    if (field === 1 && wireType === WIRE_LEN) {
      key = reader.readString();
    } else if (field === 2 && wireType === WIRE_LEN) {
      value = reader.readMessage(decodeValue);
    } else {
      reader.skip(wireType);
    }
  }
  return [key, value];
}

/** @param {string} kind */
function assertScalarCompatible(kind) {
  if (kind !== "unset" && kind !== "scalar") {
    throw new Error("mixed D1 response value wire kinds");
  }
}

/** @param {Reader} reader */
function decodeValue(reader) {
  let kind = "unset";
  /** @type {unknown} */
  let value = null;
  while (!reader.done()) {
    const { field, wireType } = reader.readTag();
    if (field === 1 && wireType === WIRE_VARINT) {
      assertScalarCompatible(kind);
      reader.readVarint();
      kind = "scalar";
      value = null;
    } else if (field === 2 && wireType === WIRE_VARINT) {
      assertScalarCompatible(kind);
      kind = "scalar";
      value = reader.readVarint() !== 0;
    } else if (field === 3 && wireType === WIRE_FIXED64) {
      assertScalarCompatible(kind);
      kind = "scalar";
      value = reader.readDouble();
    } else if (field === 4 && wireType === WIRE_LEN) {
      assertScalarCompatible(kind);
      kind = "scalar";
      value = reader.readString();
    } else if (field === 5 && wireType === WIRE_LEN) {
      assertScalarCompatible(kind);
      kind = "scalar";
      value = Array.from(reader.readLengthDelimited());
    } else if (field === 6 && wireType === WIRE_LEN) {
      if (kind === "unset") {
        kind = "array";
        value = [];
      }
      if (kind !== "array") throw new Error("mixed D1 response value wire kinds");
      /** @type {unknown[]} */ (value).push(reader.readMessage(decodeValue));
    } else if (field === 7 && wireType === WIRE_LEN) {
      if (kind === "unset") {
        kind = "object";
        value = {};
      }
      if (kind !== "object") throw new Error("mixed D1 response value wire kinds");
      const [key, item] = reader.readMessage(decodeObjectEntry);
      setDataField(/** @type {Record<string, unknown>} */ (value), key, item);
    } else if (field === 8 && wireType === WIRE_VARINT) {
      reader.readVarint();
      if (kind === "unset") {
        kind = "array";
        value = [];
      }
      if (kind !== "array") throw new Error("mixed D1 response value wire kinds");
    } else if (field === 9 && wireType === WIRE_VARINT) {
      reader.readVarint();
      if (kind === "unset") {
        kind = "object";
        value = {};
      }
      if (kind !== "object") throw new Error("mixed D1 response value wire kinds");
    } else {
      reader.skip(wireType);
    }
  }
  if (kind === "unset") throw new Error("empty D1 response value");
  return value;
}

/** @param {unknown} payload */
export function encodeD1QueryResponse(payload) {
  const plan = new WirePlan();
  appendValue(plan, payload);
  return plan.finish();
}

/** @param {ArrayBuffer | ArrayBufferView<ArrayBufferLike>} bytes */
export function decodeD1QueryResponse(bytes) {
  return decodeValue(new Reader(bytes));
}
