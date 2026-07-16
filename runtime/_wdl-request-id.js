const intrinsicArrayIsArray = Array.isArray;
const intrinsicObjectHasOwn = Object.hasOwn;
const intrinsicReflectApply = Reflect.apply;
const intrinsicStringCharCodeAt = String.prototype.charCodeAt;
const intrinsicStringIndexOf = String.prototype.indexOf;
const intrinsicStringSlice = String.prototype.slice;

/** @param {string} value */
function trimAsciiWhitespace(value) {
  let start = 0;
  let end = value.length;
  const isWhitespace = (/** @type {number} */ code) =>
    code === 0x20 || code === 0x09 || code === 0x0a || code === 0x0c || code === 0x0d;
  while (start < end && isWhitespace(intrinsicReflectApply(intrinsicStringCharCodeAt, value, [start]))) start += 1;
  while (end > start && isWhitespace(intrinsicReflectApply(intrinsicStringCharCodeAt, value, [end - 1]))) end -= 1;
  return intrinsicReflectApply(intrinsicStringSlice, value, [start, end]);
}

/** @param {unknown} raw @returns {string | null} */
export function sanitizeRequestId(raw) {
  if (intrinsicReflectApply(intrinsicArrayIsArray, undefined, [raw])) {
    raw = /** @type {unknown[]} */ (raw)[0];
  }
  if (typeof raw !== "string") return null;
  const comma = intrinsicReflectApply(intrinsicStringIndexOf, raw, [","]);
  const first = trimAsciiWhitespace(comma === -1 ? raw : intrinsicReflectApply(intrinsicStringSlice, raw, [0, comma]));
  if (!first || first.length > 128) return null;
  for (let i = 0; i < first.length; i++) {
    const code = intrinsicReflectApply(intrinsicStringCharCodeAt, first, [i]);
    if (code < 0x21 || code > 0x7e || code === 0x22 || code === 0x5c) return null;
  }
  return first;
}

/**
 * Resolve the request id options shared by loaded-isolate host facades.
 *
 * Provider-first is the runtime wrapper contract: a class-style entrypoint keeps
 * a stable env wrapper and swaps the current request id through the provider.
 *
 * @param {unknown} options
 * @returns {string | null}
 */
export function requestIdFromOptions(options) {
  if (!options || typeof options !== "object") return null;
  const record = /** @type {{ requestIdProvider?: unknown, requestId?: unknown }} */ (options);
  const fromProvider = () => {
    if (!intrinsicReflectApply(intrinsicObjectHasOwn, undefined, [record, "requestIdProvider"])) return null;
    const provider = record.requestIdProvider;
    if (typeof provider !== "function") return null;
    return sanitizeRequestId(intrinsicReflectApply(provider, undefined, []));
  };
  const fromValue = () => intrinsicReflectApply(intrinsicObjectHasOwn, undefined, [record, "requestId"])
    ? sanitizeRequestId(record.requestId)
    : null;
  return fromProvider() || fromValue();
}
