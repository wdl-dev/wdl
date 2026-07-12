export const WORKFLOW_RESULT_BYTES_MAX = 1024 * 1024;
export const WORKFLOW_BACKEND_REQUEST_BYTES_MAX = 2 * 1024 * 1024;
export const WORKFLOW_PAYLOAD_TOO_LARGE_CODE = "workflow_payload_too_large";
const WORKFLOW_JSON_ENCODE_CHARS = 8192;
const WORKFLOW_JSON_FLUSH_CHARS = 8192;
const utf8Encoder = new TextEncoder();

/**
 * @param {string} code
 * @param {string} message
 */
export function workflowStepError(code, message) {
  const err = new Error(message);
  err.name = code;
  return err;
}

/**
 * @param {string} kind
 * @param {number} [maxBytes]
 */
function workflowPayloadTooLarge(kind, maxBytes = WORKFLOW_RESULT_BYTES_MAX) {
  return workflowStepError(
    WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
    `Workflow ${kind} exceeds the ${maxBytes} byte limit`
  );
}

/**
 * @param {unknown} value
 * @param {string} kind
 * @param {number} [maxBytes]
 * @param {Record<string, { kind: string, maxBytes: number }>} [fieldCaps]
 */
export function stringifyWorkflowJson(value, kind, maxBytes = WORKFLOW_RESULT_BYTES_MAX, fieldCaps = {}) {
  /** @type {string[]} */
  const parts = [];
  /** @type {Set<object>} */
  const seen = new Set();
  let bytes = 0;

  /** @param {string} part */
  const push = (part) => {
    for (let offset = 0; offset < part.length;) {
      let end = Math.min(part.length, offset + WORKFLOW_JSON_ENCODE_CHARS);
      const last = part.charCodeAt(end - 1);
      if (end < part.length && last >= 0xd800 && last <= 0xdbff) end -= 1;
      const chunk = part.slice(offset, end);
      bytes += utf8Encoder.encode(chunk).byteLength;
      if (bytes > maxBytes) throw workflowPayloadTooLarge(kind, maxBytes);
      parts.push(chunk);
      offset = end;
    }
  };

  /** @param {string} raw */
  const writeString = (raw) => {
    push("\"");
    let start = 0;
    /** @param {number} end */
    const flush = (end) => {
      if (end > start) push(raw.slice(start, end));
      start = end;
    };
    /** @param {number} index */
    const flushPlainIfNeeded = (index) => {
      if (index - start < WORKFLOW_JSON_FLUSH_CHARS) return;
      let end = index;
      const prev = raw.charCodeAt(end - 1);
      if (prev >= 0xd800 && prev <= 0xdbff) end -= 1;
      flush(end);
    };
    for (let i = 0; i < raw.length; i += 1) {
      const code = raw.charCodeAt(i);
      if (code === 0x22) {
        flush(i);
        push("\\\"");
        start = i + 1;
      } else if (code === 0x5c) {
        flush(i);
        push("\\\\");
        start = i + 1;
      } else if (code === 0x08) {
        flush(i);
        push("\\b");
        start = i + 1;
      } else if (code === 0x09) {
        flush(i);
        push("\\t");
        start = i + 1;
      } else if (code === 0x0a) {
        flush(i);
        push("\\n");
        start = i + 1;
      } else if (code === 0x0c) {
        flush(i);
        push("\\f");
        start = i + 1;
      } else if (code === 0x0d) {
        flush(i);
        push("\\r");
        start = i + 1;
      } else if (code < 0x20) {
        flush(i);
        push(`\\u${code.toString(16).padStart(4, "0")}`);
        start = i + 1;
      } else if (code >= 0xd800 && code <= 0xdbff) {
        const next = raw.charCodeAt(i + 1);
        if (next >= 0xdc00 && next <= 0xdfff) {
          i += 1;
          flushPlainIfNeeded(i + 1);
        } else {
          flush(i);
          push(`\\u${code.toString(16).padStart(4, "0")}`);
          start = i + 1;
        }
      } else if (code >= 0xdc00 && code <= 0xdfff) {
        flush(i);
        push(`\\u${code.toString(16).padStart(4, "0")}`);
        start = i + 1;
      } else {
        flushPlainIfNeeded(i + 1);
      }
    }
    flush(raw.length);
    push("\"");
  };

  /**
   * @param {unknown} entry
   * @param {string} key
   * @returns {unknown}
   */
  const normalize = (entry, key) => (
    entry && typeof entry === "object" && typeof /** @type {{ toJSON?: unknown }} */ (entry).toJSON === "function"
      ? /** @type {{ toJSON(key: string): unknown }} */ (entry).toJSON(key)
      : entry
  );
  /** @param {unknown} entry */
  const writable = (entry) => (
    entry !== undefined && typeof entry !== "function" && typeof entry !== "symbol"
  );
  /** @param {unknown} entry */
  const primitive = (entry) => (
    entry === null || (typeof entry !== "object" && typeof entry !== "function")
  );
  /**
   * @param {unknown} entry
   * @param {{ call(thisArg: unknown): unknown }} intrinsic
   * @returns {{ ok: boolean, value: unknown }}
   */
  const boxedValue = (entry, intrinsic) => {
    try {
      return { ok: true, value: intrinsic.call(entry) };
    } catch {
      return { ok: false, value: undefined };
    }
  };
  /** @param {unknown} entry */
  const isBigIntWrapper = (entry) => boxedValue(entry, BigInt.prototype.valueOf).ok;
  /** @param {unknown} entry */
  const numberWrapperValue = (entry) => {
    const boxed = /** @type {Record<PropertyKey, unknown>} */ (/** @type {object} */ (entry));
    const exotic = boxed[Symbol.toPrimitive];
    let value;
    if (exotic != null) {
      if (typeof exotic !== "function") throw new TypeError("Cannot convert object to primitive value");
      value = exotic.call(entry, "number");
      if (!primitive(value)) throw new TypeError("Cannot convert object to primitive value");
    } else {
      value = typeof boxed.valueOf === "function" ? boxed.valueOf() : entry;
      if (!primitive(value)) {
        value = typeof boxed.toString === "function" ? boxed.toString() : entry;
        if (!primitive(value)) {
          throw new TypeError("Cannot convert object to primitive value");
        }
      }
    }
    if (typeof value === "bigint") throw new TypeError("Cannot convert a BigInt value to a number");
    return Number(value);
  };
  /**
   * @param {unknown} entry
   * @param {string} [key]
   * @param {boolean} [alreadyNormalized]
   */
  const writeValue = (entry, key = "", alreadyNormalized = false) => {
    const normalized = alreadyNormalized ? entry : normalize(entry, key);
    if (!writable(normalized)) return false;
    if (normalized === null) {
      push("null");
    } else if (isBigIntWrapper(normalized)) {
      throw new TypeError("Do not know how to serialize a BigInt");
    } else if (boxedValue(normalized, String.prototype.valueOf).ok) {
      writeString(String(normalized));
    } else if (boxedValue(normalized, Number.prototype.valueOf).ok) {
      const value = numberWrapperValue(normalized);
      push(Number.isFinite(value) ? String(value) : "null");
    } else {
      const boxedBoolean = boxedValue(normalized, Boolean.prototype.valueOf);
      if (boxedBoolean.ok) {
        push(boxedBoolean.value ? "true" : "false");
      } else if (typeof normalized === "string") {
        writeString(normalized);
      } else if (typeof normalized === "number") {
        push(Number.isFinite(normalized) ? String(normalized) : "null");
      } else if (typeof normalized === "boolean") {
        push(normalized ? "true" : "false");
      } else if (typeof normalized === "bigint") {
        throw new TypeError("Do not know how to serialize a BigInt");
      } else if (Array.isArray(normalized)) {
        if (seen.has(normalized)) throw new TypeError("Converting circular structure to JSON");
        seen.add(normalized);
        push("[");
        for (let i = 0; i < normalized.length; i += 1) {
          if (i > 0) push(",");
          if (!writeValue(normalized[i], String(i))) push("null");
        }
        push("]");
        seen.delete(normalized);
      } else {
        const objectValue = /** @type {Record<string, unknown>} */ (normalized);
        if (seen.has(objectValue)) throw new TypeError("Converting circular structure to JSON");
        seen.add(objectValue);
        push("{");
        let first = true;
        for (const prop of Object.keys(objectValue)) {
          const child = normalize(objectValue[prop], prop);
          if (!writable(child)) continue;
          if (!first) push(",");
          first = false;
          writeString(prop);
          push(":");
          const beforeValue = bytes;
          writeValue(child, prop, true);
          if (fieldCaps[prop] && bytes - beforeValue > fieldCaps[prop].maxBytes) {
            throw workflowPayloadTooLarge(fieldCaps[prop].kind, fieldCaps[prop].maxBytes);
          }
        }
        push("}");
        seen.delete(objectValue);
      }
    }
    return true;
  };

  if (!writeValue(value)) return "null";
  return parts.join("");
}

/**
 * @param {unknown} value
 * @param {string} kind
 */
export function stringifyWorkflowResult(value, kind) {
  return stringifyWorkflowJson(value, kind, WORKFLOW_RESULT_BYTES_MAX);
}

/**
 * @param {unknown} value
 * @param {number} [maxBytes]
 */
export function _stringifyWorkflowJsonForTest(value, maxBytes = WORKFLOW_RESULT_BYTES_MAX) {
  return stringifyWorkflowJson(value, "test value", maxBytes);
}

/** @param {string} path @param {unknown} body */
export function _stringifyWorkflowBackendBodyForTest(path, body) {
  return workflowBackendBody(path, body);
}

/** @param {string} path @param {unknown} body */
export function workflowBackendBody(path, body) {
  /** @type {Record<string, { kind: string, maxBytes: number }>} */
  const fieldCaps = {};
  if (path === "commit-step-success") {
    fieldCaps.output = { kind: "step output", maxBytes: WORKFLOW_RESULT_BYTES_MAX };
  }
  if (path === "commit-step-error") {
    fieldCaps.error = { kind: "step error", maxBytes: WORKFLOW_RESULT_BYTES_MAX };
  }
  return stringifyWorkflowJson(
    body,
    "backend request body",
    WORKFLOW_BACKEND_REQUEST_BYTES_MAX,
    fieldCaps
  );
}

/**
 * @param {number} status
 * @param {string} bodyPrefix
 * @param {string} resultKey
 * @param {unknown} resultValue
 * @param {string} kind
 * @param {number} durationMs
 */
export function workflowJsonResponse(status, bodyPrefix, resultKey, resultValue, kind, durationMs) {
  const result = stringifyWorkflowResult(resultValue, kind);
  return new Response(
    `${bodyPrefix}"${resultKey}":${result},"duration_ms":${durationMs}}`,
    { status, headers: { "content-type": "application/json" } }
  );
}
