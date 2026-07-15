// @ts-nocheck

// node_modules/@wdl-dev/aws-sigv4/dist/index.js
var textEncoder = new TextEncoder();
var AUTHORIZATION_HEADER = "authorization";
var HOST_HEADER = "host";
var AMZ_CONTENT_SHA256_HEADER = "x-amz-content-sha256";
var AMZ_DATE_HEADER = "x-amz-date";
var AMZ_SECURITY_TOKEN_HEADER = "x-amz-security-token";
var CONTENT_TYPE_HEADER = "content-type";
var MANDATORY_SIGNED_HEADERS = /* @__PURE__ */ new Set([
  HOST_HEADER,
  AMZ_CONTENT_SHA256_HEADER,
  AMZ_DATE_HEADER,
  AMZ_SECURITY_TOKEN_HEADER
]);
var DEFAULT_UNSIGNABLE_HEADERS = /* @__PURE__ */ new Set([
  AUTHORIZATION_HEADER,
  "accept-encoding",
  "connection",
  "content-length",
  "expect",
  "keep-alive",
  "presigned-expires",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "user-agent",
  "x-amzn-trace-id"
]);
var AWS_ALGORITHM = "AWS4-HMAC-SHA256";
var AWS_REQUEST = "aws4_request";
var UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
var LOWER_HEX = "0123456789abcdef";
var inFlightSigningKeys = /* @__PURE__ */ new WeakMap();
async function signatureHex(options) {
  let signingKey;
  if (options.cache === void 0) {
    signingKey = await deriveSigningKey(options);
  } else {
    const secretAccessKeyHash = options.secretAccessKeyHash ?? await sha256Hex(options.secretAccessKey);
    const cacheKey = ["sigv4", secretAccessKeyHash, options.date, options.region, options.service].join(",");
    const cachedSigningKey = options.cache.get(cacheKey);
    signingKey = isSigningKeyCacheMiss(cachedSigningKey) ? await deriveCachedSigningKey(options, options.cache, cacheKey) : cachedSigningKey;
  }
  return hex(await hmac(signingKey, options.stringToSign));
}
function isSigningKeyCacheMiss(value) {
  return value === void 0 || value === null;
}
async function deriveCachedSigningKey(options, cache, cacheKey) {
  let byKey = inFlightSigningKeys.get(cache);
  if (byKey === void 0) {
    byKey = /* @__PURE__ */ new Map();
    inFlightSigningKeys.set(cache, byKey);
  }
  const existing = byKey.get(cacheKey);
  if (existing !== void 0) {
    return existing;
  }
  const derivation = (async () => {
    try {
      const signingKey = await deriveSigningKey(options);
      cache.set(cacheKey, signingKey);
      return signingKey;
    } finally {
      byKey.delete(cacheKey);
      if (byKey.size === 0) {
        inFlightSigningKeys.delete(cache);
      }
    }
  })();
  byKey.set(cacheKey, derivation);
  return derivation;
}
async function deriveSigningKey(options) {
  const kDate = await hmac(`AWS4${options.secretAccessKey}`, options.date);
  const kRegion = await hmac(kDate, options.region);
  const kService = await hmac(kRegion, options.service);
  return hmac(kService, AWS_REQUEST);
}
async function sha256Hex(value) {
  const bytes = cryptoBufferSource(value);
  return hex(await crypto.subtle.digest("SHA-256", bytes));
}
function cryptoBufferSource(value) {
  if (typeof value === "string") {
    return textEncoder.encode(value);
  }
  if (value instanceof ArrayBuffer) {
    return value;
  }
  if (ArrayBuffer.isView(value) && value.buffer instanceof ArrayBuffer) {
    return value;
  }
  return new Uint8Array(value);
}
async function hmac(key, value) {
  const cryptoKey = await crypto.subtle.importKey("raw", typeof key === "string" ? textEncoder.encode(key) : key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return crypto.subtle.sign("HMAC", cryptoKey, textEncoder.encode(value));
}
function hex(input) {
  const bytes = new Uint8Array(input);
  let out = "";
  for (const byte of bytes) {
    out += hexNibble(byte >>> 4);
    out += hexNibble(byte & 15);
  }
  return out;
}
function hexNibble(value) {
  return LOWER_HEX.charAt(value);
}
async function prepareSigningBody(body, headers, options) {
  if (options.unsignedPayload && !headers.has(AMZ_CONTENT_SHA256_HEADER)) {
    headers.set(AMZ_CONTENT_SHA256_HEADER, UNSIGNED_PAYLOAD);
  }
  const hasBody = body !== null && body !== void 0;
  const computePayloadHash = !headers.has(AMZ_CONTENT_SHA256_HEADER);
  const materialize = options.replay && shouldMaterializeBodyForReplay(body) || computePayloadHash && hasBody;
  const prepared = await prepareBody(body, headers, materialize, options.signal);
  let payloadHash = headers.get(AMZ_CONTENT_SHA256_HEADER);
  if (payloadHash === null) {
    if (prepared.bytes === void 0) {
      throw new Error("body bytes must be materialized before payload hashing");
    }
    payloadHash = await sha256Hex(prepared.bytes);
    options.signal?.throwIfAborted();
    if (hasBody || options.service === "s3") {
      headers.set(AMZ_CONTENT_SHA256_HEADER, payloadHash);
    }
  }
  return { body: prepared.body, payloadHash };
}
async function prepareBody(body, headers, materialize, signal) {
  signal?.throwIfAborted();
  assertSupportedBody(body);
  if (body === null || body === void 0) {
    return { body, bytes: new Uint8Array() };
  }
  if (body instanceof FormData) {
    rejectManualFormDataContentType(headers);
    const request = new Request("https://aws-sigv4.invalid/", { method: "POST", body });
    const contentType = request.headers.get(CONTENT_TYPE_HEADER);
    if (contentType) {
      headers.set(CONTENT_TYPE_HEADER, contentType);
    }
    const bytes2 = signal && request.body ? await readStreamBytes(request.body, signal) : new Uint8Array(await request.arrayBuffer());
    signal?.throwIfAborted();
    return { body: bytes2, bytes: bytes2 };
  }
  setGeneratedContentType(body, headers);
  if (body instanceof ReadableStream) {
    assertReadableStreamUsable(body);
    if (!materialize) {
      return { body };
    }
    const bytes2 = await readStreamBytes(body, signal);
    return { body: bytes2, bytes: bytes2 };
  }
  if (!materialize && (typeof body === "string" || body instanceof Blob)) {
    return { body };
  }
  const bytes = await bodyBytes(body, signal);
  signal?.throwIfAborted();
  return { body: stableMaterializedBody(body, bytes), bytes };
}
function shouldMaterializeBodyForReplay(body) {
  return body !== void 0 && body !== null && typeof body !== "string" && !(body instanceof Blob);
}
function rejectManualFormDataContentType(headers) {
  if (headers.has(CONTENT_TYPE_HEADER)) {
    throw new TypeError("FormData content-type must be generated by the runtime");
  }
}
function setGeneratedContentType(body, headers) {
  if (headers.has(CONTENT_TYPE_HEADER)) {
    return;
  }
  if (typeof body === "string") {
    headers.set(CONTENT_TYPE_HEADER, "text/plain;charset=UTF-8");
  } else if (body instanceof URLSearchParams) {
    headers.set(CONTENT_TYPE_HEADER, "application/x-www-form-urlencoded;charset=UTF-8");
  } else if (body instanceof Blob) {
    if (body.type) {
      headers.set(CONTENT_TYPE_HEADER, body.type);
    }
  }
}
async function bodyBytes(body, signal) {
  if (typeof body === "string") {
    return textEncoder.encode(body);
  }
  if (body instanceof Uint8Array) {
    return new Uint8Array(body);
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body).slice();
  }
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice();
  }
  if (body instanceof URLSearchParams) {
    return textEncoder.encode(body.toString());
  }
  if (body instanceof Blob) {
    if (signal) {
      return readStreamBytes(body.stream(), signal);
    }
    return new Uint8Array(await body.arrayBuffer());
  }
  throw new TypeError("body must be a string, Blob, URLSearchParams, ArrayBuffer, or ArrayBufferView");
}
function assertReadableStreamUsable(body) {
  try {
    void new Response(body);
  } catch (error) {
    if (error instanceof TypeError) {
      throw new TypeError("ReadableStream body must not be disturbed or locked", { cause: error });
    }
    throw error;
  }
}
async function readStreamBytes(stream, signal) {
  const reader = stream.getReader();
  const chunks = [];
  let byteLength = 0;
  const onAbort = () => {
    if (signal) {
      void reader.cancel(signal.reason).catch(() => void 0);
    }
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    for (; ; ) {
      signal?.throwIfAborted();
      const result = await reader.read();
      signal?.throwIfAborted();
      if (result.done) {
        break;
      }
      const value = result.value;
      if (!(value instanceof Uint8Array)) {
        throw new TypeError("ReadableStream body must yield Uint8Array chunks");
      }
      const chunk = new Uint8Array(value);
      chunks.push(chunk);
      byteLength += chunk.byteLength;
    }
  } catch (err) {
    if (signal?.aborted) {
      throw signal.reason;
    }
    void reader.cancel(err).catch(() => void 0);
    throw err;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    reader.releaseLock();
  }
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}
function isAsyncIterable(value) {
  return typeof value[Symbol.asyncIterator] === "function";
}
function assertSupportedBody(body) {
  if (body === null || body === void 0) {
    return;
  }
  if (typeof body === "string" || body instanceof Blob || body instanceof FormData || body instanceof ReadableStream || body instanceof URLSearchParams || body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
    return;
  }
  if (isAsyncIterable(body)) {
    throw new TypeError("async iterable bodies are not supported; use a ReadableStream");
  }
  throw new TypeError("body must be a string, Blob, URLSearchParams, ArrayBuffer, or ArrayBufferView");
}
function stableMaterializedBody(body, bytes) {
  if (typeof body === "string") {
    return body;
  }
  if (bytes.buffer instanceof ArrayBuffer) {
    return bytes;
  }
  return new Uint8Array(bytes);
}
function rejectNonPrintableAscii(value, message) {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit < 32 || codeUnit > 126) {
      throw new TypeError(message);
    }
  }
}
function canonicalHeaderBlock(url, headers, options) {
  const signable = signedHeaderNames(headers, options);
  const canonicalHeaders = signable.map((header) => {
    const value = header === HOST_HEADER ? url.host : canonicalHeaderValue(headers.get(header) || "", header);
    return `${header}:${value}`;
  }).join("\n");
  return {
    canonicalHeaders,
    signedHeaders: signable.join(";")
  };
}
function validateSignedHeaderValues(headers, options) {
  rejectMandatoryHeaderExclusions(headers, options);
  const overwrittenHeaderNames = new Set((options.overwrittenHeaderNames || []).map((value) => value.toLowerCase()));
  for (const header of signedHeaderNames(headers, options)) {
    if (header !== HOST_HEADER && !overwrittenHeaderNames.has(header)) {
      rejectNonPrintableAsciiHeaderValue(headers.get(header) || "", header);
    }
  }
}
function signerOverwrittenHeaderNames(hasSessionToken) {
  return hasSessionToken ? [AMZ_DATE_HEADER, AMZ_SECURITY_TOKEN_HEADER] : [AMZ_DATE_HEADER];
}
function signedHeaderNames(headers, options) {
  const userUnsignable = new Set((options.unsignableHeaders || []).map((value) => value.toLowerCase()));
  return [.../* @__PURE__ */ new Set([HOST_HEADER, ...headers.keys()])].filter((header) => header !== AUTHORIZATION_HEADER).filter((header) => {
    if (isMandatorySignedHeader(header, options.service)) {
      return true;
    }
    if (userUnsignable.has(header)) {
      return false;
    }
    return options.signAllHeaders || !DEFAULT_UNSIGNABLE_HEADERS.has(header);
  }).sort();
}
function rejectMandatoryHeaderExclusions(headers, options) {
  for (const value of options.unsignableHeaders || []) {
    const header = value.toLowerCase();
    const isPresentDynamicHeader = headers.has(header) && (header.startsWith("x-amz-") || options.service === "s3" && header === "content-md5");
    if (MANDATORY_SIGNED_HEADERS.has(header) || isPresentDynamicHeader) {
      throw new TypeError(`unsignableHeaders must not include mandatory signed header ${header}`);
    }
  }
}
function isMandatorySignedHeader(header, service) {
  return MANDATORY_SIGNED_HEADERS.has(header) || header.startsWith("x-amz-") || service === "s3" && header === "content-md5";
}
function canonicalHeaderValue(value, name) {
  rejectNonPrintableAsciiHeaderValue(value, name);
  return value.trim().replace(/\s+/gu, " ");
}
function rejectNonPrintableAsciiHeaderValue(value, name) {
  rejectNonPrintableAscii(value, `${name} header value must contain only printable ASCII characters`);
}
var ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})$/u;
var SIGNING_DATE_ERROR = "signingDate must be a valid Date, ISO-8601 string, or YYYYMMDDTHHMMSSZ string";
function optionalAmzDate(value) {
  if (value === void 0 || value === null) {
    return void 0;
  }
  return formatAmzDate(value);
}
function formatAmzDate(value) {
  if (typeof value === "string" && /^\d{8}T\d{6}Z$/u.test(value)) {
    if (!isValidCompactAmzDate(value)) {
      throw new TypeError(SIGNING_DATE_ERROR);
    }
    return value;
  }
  if (typeof value === "string" && !ISO_DATE_RE.test(value)) {
    throw new TypeError(SIGNING_DATE_ERROR);
  }
  if (typeof value === "string" && !isValidIsoDate(value)) {
    throw new TypeError(SIGNING_DATE_ERROR);
  }
  const date = typeof value === "string" ? new Date(value) : value;
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new TypeError(SIGNING_DATE_ERROR);
  }
  const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, "");
  if (!/^\d{8}T\d{6}Z$/u.test(amzDate) || !isValidCompactAmzDate(amzDate)) {
    throw new TypeError(SIGNING_DATE_ERROR);
  }
  return amzDate;
}
function isValidIsoDate(value) {
  const match = ISO_DATE_RE.exec(value);
  if (!match) {
    return false;
  }
  const [datePart, timePart] = value.split("T");
  const [yearText, monthText, dayText] = datePart.split("-");
  const [hourText, minuteText, secondText] = timePart.split(/[.:Z+-]/u);
  return isValidDateParts(Number(yearText), Number(monthText), Number(dayText), Number(hourText), Number(minuteText), Number(secondText));
}
function isValidCompactAmzDate(value) {
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(4, 6));
  const day = Number(value.slice(6, 8));
  const hour = Number(value.slice(9, 11));
  const minute = Number(value.slice(11, 13));
  const second = Number(value.slice(13, 15));
  return isValidDateParts(year, month, day, hour, minute, second);
}
function isValidDateParts(year, month, day, hour, minute, second) {
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  date.setUTCFullYear(year);
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day && date.getUTCHours() === hour && date.getUTCMinutes() === minute && date.getUTCSeconds() === second;
}
var AUTH_PARAM_SEPARATOR_RE = /[,=;]/u;
var CONTROL_CHAR_RE = /[\u0000-\u001f\u007f]/u;
var HEADER_NAME_RE = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/u;
var WHITESPACE_RE = /\s/u;
var CLIENT_SIGNING_OPTION_KEYS = /* @__PURE__ */ new Set([
  "service",
  "region",
  "signingDate",
  "unsignedPayload",
  "signAllHeaders",
  "unsignableHeaders",
  "doubleUrlEncode"
]);
var UNSIGNABLE_HEADER_SNAPSHOTS = /* @__PURE__ */ new WeakMap();
function snapshotSignAwsRequestOptions(value) {
  requireOptionsObject(value, "signAwsRequest options are required");
  const snapshot = { ...value };
  validateCredentialOptions(snapshot, "signAwsRequest options are required");
  snapshot.unsignableHeaders = snapshotUnsignableHeaders(value, snapshot.unsignableHeaders, "unsignableHeaders");
  return snapshot;
}
function normalizeClientSigningOptions(options) {
  if (options === void 0) {
    return {};
  }
  if (options === null || typeof options !== "object") {
    throw new TypeError("init.signing must be an object");
  }
  const source = { ...options };
  for (const key of Object.keys(source)) {
    if (!CLIENT_SIGNING_OPTION_KEYS.has(key)) {
      throw new TypeError(`${signingOptionDisplayName(key)} cannot override client credentials or transport options`);
    }
  }
  return {
    service: optionalCredentialComponent(nullAsUndefined(source.service), "init.signing.service"),
    region: optionalCredentialComponent(nullAsUndefined(source.region), "init.signing.region"),
    signingDate: optionalAmzDate(nullAsUndefined(source.signingDate)),
    unsignedPayload: optionalBoolean(source.unsignedPayload, "init.signing.unsignedPayload"),
    signAllHeaders: optionalBoolean(source.signAllHeaders, "init.signing.signAllHeaders"),
    unsignableHeaders: snapshotUnsignableHeaders(options, source.unsignableHeaders, "init.signing.unsignableHeaders"),
    doubleUrlEncode: optionalBoolean(source.doubleUrlEncode, "init.signing.doubleUrlEncode")
  };
}
function signingOptionDisplayName(key) {
  if (key.length === 0) {
    return "init.signing option";
  }
  for (let index = 0; index < key.length; index += 1) {
    const codeUnit = key.charCodeAt(index);
    if (codeUnit < 32 || codeUnit > 126) {
      return "init.signing option";
    }
  }
  return `init.signing.${key}`;
}
function validateCredentialOptions(options, message) {
  requireOptionsObject(options, message);
  const record = options;
  requireCredentialComponent(record.accessKeyId, "accessKeyId");
  requireSecretAccessKey(record.secretAccessKey);
  requireLowercaseCredentialComponent(record.service, "service");
  requireLowercaseCredentialComponent(record.region, "region");
  if (record.sessionToken !== void 0) {
    validateSessionToken(record.sessionToken);
  }
}
function validateSessionToken(value) {
  requireString(value, "sessionToken");
  rejectControlChars(value, "sessionToken");
  rejectSurroundingWhitespace(value, "sessionToken");
  rejectNonPrintableAscii(value, "sessionToken must contain only printable ASCII characters");
}
function requireCredentialComponent(value, name) {
  requireString(value, name);
  rejectControlChars(value, name);
  rejectWhitespace(value, name);
  rejectAuthorizationParamSeparators(value, name);
  rejectNonPrintableAscii(value, `${name} must contain only printable ASCII characters`);
  if (value.includes("/")) {
    throw new TypeError(`${name} must not contain /`);
  }
}
function requireSecretAccessKey(value) {
  requireString(value, "secretAccessKey");
  rejectControlChars(value, "secretAccessKey");
  if (!value.isWellFormed()) {
    throw new TypeError("secretAccessKey must contain well-formed UTF-16");
  }
}
function requireLowercaseCredentialComponent(value, name) {
  requireCredentialComponent(value, name);
  if (value !== value.toLowerCase()) {
    throw new TypeError(`${name} must be lowercase`);
  }
}
function requireNonNegativeInteger(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative integer within the safe integer range`);
  }
  return value;
}
function requireNonNegativeFiniteNumber(value, name) {
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative finite number`);
  }
  return value;
}
function optionalBoolean(value, name) {
  if (value === void 0) {
    return void 0;
  }
  if (typeof value !== "boolean") {
    throw new TypeError(`${name} must be a boolean`);
  }
  return value;
}
function resolveUnsignedPayload(explicit, service) {
  return explicit ?? service === "s3";
}
function resolveDoubleUrlEncode(explicit, service) {
  return explicit ?? service !== "s3";
}
function normalizeUnsignableHeaders(value, name) {
  if (value === void 0) {
    return void 0;
  }
  if (value === null || typeof value === "string" || typeof value[Symbol.iterator] !== "function") {
    throw new TypeError(`${name} must be an iterable of header names`);
  }
  return [...value].map((header) => {
    if (typeof header !== "string" || header.length === 0) {
      throw new TypeError(`${name} must contain only non-empty strings`);
    }
    if (!HEADER_NAME_RE.test(header)) {
      throw new TypeError(`${name} must contain only valid header names`);
    }
    return header;
  });
}
function snapshotUnsignableHeaders(owner, source, name) {
  if (source === void 0) {
    return void 0;
  }
  if (!isIterable(source) || !isOneShotIterable(source)) {
    return normalizeUnsignableHeaders(source, name);
  }
  const cached = UNSIGNABLE_HEADER_SNAPSHOTS.get(owner);
  if (cached?.source === source) {
    if (cached.result.ok) {
      return cached.result.value;
    }
    throw cached.result.error;
  }
  try {
    const value = normalizeUnsignableHeaders(source, name);
    UNSIGNABLE_HEADER_SNAPSHOTS.set(owner, { source, result: { ok: true, value } });
    return value;
  } catch (error) {
    UNSIGNABLE_HEADER_SNAPSHOTS.set(owner, { source, result: { ok: false, error } });
    throw error;
  }
}
function requireOptionsObject(value, message) {
  if (value === null || typeof value !== "object") {
    throw new TypeError(message);
  }
}
function requireDefinedOption(value, name) {
  if (value === null || value === void 0) {
    throw new TypeError(`${name} is a required option`);
  }
}
function requireSigningCache(value, name) {
  if (value === void 0) {
    return void 0;
  }
  if (!isSigningCache(value)) {
    throw new TypeError(`${name} must be a Map-like cache`);
  }
  return value;
}
function requireString(value, name) {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}
function rejectControlChars(value, name) {
  if (CONTROL_CHAR_RE.test(value)) {
    throw new TypeError(`${name} must not contain control characters`);
  }
}
function rejectAuthorizationParamSeparators(value, name) {
  if (AUTH_PARAM_SEPARATOR_RE.test(value)) {
    throw new TypeError(`${name} must not contain Authorization parameter separators`);
  }
}
function rejectWhitespace(value, name) {
  if (WHITESPACE_RE.test(value)) {
    throw new TypeError(`${name} must not contain whitespace`);
  }
}
function rejectSurroundingWhitespace(value, name) {
  if (value.trim() !== value) {
    throw new TypeError(`${name} must not contain leading or trailing whitespace`);
  }
}
function optionalCredentialComponent(value, name) {
  if (value === void 0) {
    return void 0;
  }
  requireLowercaseCredentialComponent(value, name);
  return value;
}
function nullAsUndefined(value) {
  return value === null ? void 0 : value;
}
function isOneShotIterable(value) {
  return Object.is(value[Symbol.iterator](), value);
}
function isIterable(value) {
  return value !== null && (typeof value === "object" || typeof value === "function") && typeof value[Symbol.iterator] === "function";
}
function isSigningCache(value) {
  if (value === null || typeof value !== "object" || value instanceof WeakMap) {
    return false;
  }
  const candidate = value;
  return typeof candidate.get === "function" && typeof candidate.set === "function";
}
var RFC3986_EXTRA_ESCAPE_RE = /[!'()*]/g;
function parseRequestUrl(input) {
  const raw = String(input);
  if (typeof input === "string" && /[\s\u0000-\u001f\u007f]/u.test(raw)) {
    throw new TypeError("url must not contain unescaped whitespace or control characters");
  }
  if (typeof input === "string" && raw.includes("\\")) {
    throw new TypeError("url must not contain backslashes");
  }
  const url = new URL(raw);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("url must use http: or https:");
  }
  if (url.username || url.password) {
    throw new TypeError("url must not include username or password");
  }
  if (typeof input !== "string") {
    if (hasMalformedPercentEncoding(url.pathname) || hasMalformedPercentEncoding(url.search)) {
      throw new TypeError("url must not contain malformed percent encoding");
    }
    return {
      url,
      href: stripUrlFragment(url.toString()),
      pathname: url.pathname || "/",
      search: url.search
    };
  }
  const match = /^[A-Za-z][A-Za-z0-9+.-]*:\/\/([^/?#]*)([^?#]*)?(\?[^#]*)?/u.exec(raw);
  if (!match) {
    throw new TypeError("url must include scheme://host");
  }
  if (!match[1]) {
    throw new TypeError("url must include scheme://host");
  }
  const pathname = match[2] || "/";
  const search = match[3] || "";
  if (!pathname.isWellFormed() || !search.isWellFormed()) {
    throw new TypeError("url must not contain invalid UTF-16");
  }
  if (hasMalformedPercentEncoding(pathname) || hasMalformedPercentEncoding(search)) {
    throw new TypeError("url must not contain malformed percent encoding");
  }
  return {
    url,
    href: `${url.protocol}//${url.host}${pathname}${search}`,
    pathname,
    search
  };
}
function canonicalPathname(pathname, service, doubleUrlEncode) {
  if (!doubleUrlEncode) {
    return canonicalSingleEncodedPathname(pathname);
  }
  if (service !== "s3") {
    if (hasDotPathSegment(pathname)) {
      throw new TypeError("non-S3 doubleUrlEncode URLs must not contain dot path segments");
    }
    return canonicalDoubleEncodedPathname(collapsePathSlashes(pathname));
  }
  return canonicalDoubleEncodedPathname(pathname);
}
function canonicalQuery(search) {
  if (search === "") {
    return "";
  }
  return search.slice(1).split("&").filter((part) => part.length > 0).map((part) => {
    const separator = part.indexOf("=");
    const key = separator === -1 ? part : part.slice(0, separator);
    const value = separator === -1 ? "" : part.slice(separator + 1);
    return [canonicalUriComponent(key), canonicalUriComponent(value)];
  }).sort(([ak, av], [bk, bv]) => compareCodepoint(ak, bk) || compareCodepoint(av, bv)).map(([key, value]) => `${key}=${value}`).join("&");
}
function hasDotPathSegment(pathname) {
  return pathname.split("/").some((segment) => {
    const value = segment.replace(/%2e/giu, ".");
    return value === "." || value === "..";
  });
}
function stripUrlFragment(value) {
  const index = value.indexOf("#");
  return index === -1 ? value : value.slice(0, index);
}
function hasMalformedPercentEncoding(value) {
  return /%(?![0-9A-Fa-f]{2})/u.test(value);
}
function compareCodepoint(left, right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}
function encodeRfc3986(value) {
  return value.replace(RFC3986_EXTRA_ESCAPE_RE, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}
function strictEncode(value) {
  try {
    return encodeRfc3986(encodeURIComponent(value));
  } catch (err) {
    if (err instanceof URIError) {
      throw new TypeError("url must not contain invalid UTF-16");
    }
    throw err;
  }
}
function canonicalSingleEncodedPathname(pathname) {
  let out = "";
  for (let index = 0; index < pathname.length; ) {
    const char = pathname[index];
    if (char === "/") {
      out += "/";
      index += 1;
      continue;
    }
    if (char === "%" && isHexPair(pathname, index + 1)) {
      out += pathname.slice(index, index + 3);
      index += 3;
      continue;
    }
    const codePoint = pathname.codePointAt(index);
    if (codePoint === void 0) {
      break;
    }
    const charValue = String.fromCodePoint(codePoint);
    out += strictEncode(charValue);
    index += charValue.length;
  }
  return out;
}
function canonicalDoubleEncodedPathname(pathname) {
  return strictEncode(wireEncodedPathname(pathname)).replace(/%2F/gu, "/");
}
function wireEncodedPathname(pathname) {
  let out = "";
  for (let index = 0; index < pathname.length; ) {
    const char = pathname[index];
    if (char === "/" || char === "%" && isHexPair(pathname, index + 1)) {
      const width = char === "/" ? 1 : 3;
      out += pathname.slice(index, index + width);
      index += width;
      continue;
    }
    const codePoint = pathname.codePointAt(index);
    if (codePoint === void 0) {
      break;
    }
    const charValue = String.fromCodePoint(codePoint);
    out += shouldWhatwgEncodePathCodePoint(codePoint) ? strictEncode(charValue) : charValue;
    index += charValue.length;
  }
  return out;
}
function shouldWhatwgEncodePathCodePoint(codePoint) {
  return codePoint > 126 || codePoint === 34 || codePoint === 60 || codePoint === 62 || codePoint === 94 || codePoint === 96 || codePoint === 123 || codePoint === 125;
}
function collapsePathSlashes(pathname) {
  return pathname.replace(/\/+/gu, "/");
}
function canonicalUriComponent(value) {
  let out = "";
  for (let index = 0; index < value.length; ) {
    const char = value[index];
    if (char === "%" && isHexPair(value, index + 1)) {
      const hex2 = value.slice(index + 1, index + 3).toUpperCase();
      const byte = parseInt(hex2, 16);
      out += isUnreservedByte(byte) ? String.fromCharCode(byte) : `%${hex2}`;
      index += 3;
      continue;
    }
    const codePoint = value.codePointAt(index);
    if (codePoint === void 0) {
      break;
    }
    const charValue = String.fromCodePoint(codePoint);
    out += strictEncode(charValue);
    index += charValue.length;
  }
  return out;
}
function isHexPair(value, index) {
  return /^[0-9A-Fa-f]{2}$/u.test(value.slice(index, index + 2));
}
function isUnreservedByte(byte) {
  return byte >= 65 && byte <= 90 || byte >= 97 && byte <= 122 || byte >= 48 && byte <= 57 || byte === 45 || byte === 46 || byte === 95 || byte === 126;
}
var HTTP_METHOD_RE = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/u;
var IDEMPOTENT_METHODS = /* @__PURE__ */ new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]);
function resolveClientSignRequest(input, init) {
  return resolveClientRequest(input, init).request;
}
function resolveClientFetchRequest(input, init) {
  const { request, inheritedRedirect, explicitRedirect } = resolveClientRequest(input, init);
  const redirectPolicy = resolveFetchRedirectPolicy(inheritedRedirect, explicitRedirect);
  request.init.redirect = "manual";
  return { ...request, redirectPolicy };
}
function resolveClientRequest(input, init) {
  const inputSnapshot = input instanceof Request ? snapshotRequestInput(input) : void 0;
  const requestUrl = parseRequestUrl(inputSnapshot?.url ?? input);
  const initSnapshot = snapshotRequestInit(init);
  const requestInit = inputSnapshot ? mergeDefinedRequestInit(inputSnapshot.init, initSnapshot) : initSnapshot;
  const signal = requestInit.signal ?? void 0;
  signal?.throwIfAborted();
  rejectNoCorsMode(requestInit.mode);
  const headers = inputSnapshot === void 0 ? new Headers(requestInit.headers) : mergeHeaders(inputSnapshot.headers, requestInit.headers);
  let body = requestInit.body;
  let method = requestInit.method;
  if (inputSnapshot !== void 0) {
    rejectUsedRequestBody(inputSnapshot.bodyUsed, body);
    if ((body === void 0 || body === null) && inputSnapshot.body) {
      body = inputSnapshot.body;
    }
    if (method === void 0) {
      method = inputSnapshot.method;
    }
  }
  const normalizedMethod = normalizeMethod(method === void 0 ? defaultMethod(body) : method);
  rejectFetchForbiddenMethod(normalizedMethod);
  rejectRequestBodyForGetHead(normalizedMethod, body);
  requestInit.method = normalizedMethod;
  requestInit.headers = headers;
  if (body === void 0) {
    delete requestInit.body;
  } else {
    requestInit.body = body;
  }
  return {
    request: {
      requestUrl,
      init: requestInit,
      method: normalizedMethod,
      headers,
      body,
      signal
    },
    inheritedRedirect: inputSnapshot?.init.redirect,
    explicitRedirect: initSnapshot.redirect
  };
}
function mergeHeaders(base, override) {
  const headers = new Headers(base);
  if (override !== void 0) {
    new Headers(override).forEach((value, name) => {
      headers.set(name, value);
    });
  }
  return headers;
}
function mergeDefinedRequestInit(base, override) {
  const out = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (value !== void 0) {
      out[key] = value;
    }
  }
  return out;
}
function snapshotRequestInit(value) {
  if (value !== void 0 && value !== null && typeof value !== "object") {
    throw new TypeError("init must be an object");
  }
  if (value === void 0 || value === null) {
    return {};
  }
  return { ...value };
}
function rejectEmptyHeader(headers, name) {
  if (headers.get(name) === "") {
    throw new TypeError(`${name} must not be empty`);
  }
}
function snapshotRequestInput(request) {
  const url = request.url;
  const method = request.method;
  const headers = new Headers(request.headers);
  const body = request.body;
  const bodyUsed = request.bodyUsed;
  const init = {
    cache: request.cache,
    credentials: request.credentials,
    integrity: request.integrity,
    keepalive: request.keepalive,
    mode: request.mode,
    redirect: request.redirect,
    referrer: request.referrer,
    referrerPolicy: request.referrerPolicy,
    signal: request.signal,
    window: null
  };
  const duplex = request.duplex;
  if (duplex === "half") {
    return {
      url,
      method,
      headers,
      body,
      bodyUsed,
      init: { ...init, duplex }
    };
  }
  return { url, method, headers, body, bodyUsed, init };
}
function rejectUsedRequestBody(bodyUsed, override) {
  if ((override === void 0 || override === null) && bodyUsed) {
    throw new TypeError("Request body has already been used");
  }
}
function defaultMethod(body) {
  return hasRequestBody(body) ? "POST" : "GET";
}
function normalizeMethod(method) {
  if (typeof method !== "string" || !HTTP_METHOD_RE.test(method)) {
    throw new TypeError("method must be a valid HTTP token");
  }
  return method.toUpperCase();
}
function rejectFetchForbiddenMethod(method) {
  if (method === "CONNECT" || method === "TRACE" || method === "TRACK") {
    throw new TypeError(`SigV4Client cannot use Fetch-forbidden method ${method}`);
  }
}
function isIdempotentMethod(method) {
  return IDEMPOTENT_METHODS.has(method);
}
function rejectRequestBodyForGetHead(method, body) {
  if ((method === "GET" || method === "HEAD") && hasRequestBody(body)) {
    throw new TypeError("GET and HEAD requests with a body require signAwsRequest");
  }
}
function hasRequestBody(body) {
  return body !== void 0 && body !== null;
}
function assertParsedRequestCanRepresentSignedUrl(pathname, service) {
  if (hasDotPathSegment(pathname)) {
    throw new TypeError(`SigV4Client cannot represent ${service} URLs with dot segments; use signAwsRequest`);
  }
}
function requestInitForSignedRequest(base, signed) {
  const out = {
    ...base,
    method: signed.method,
    headers: signed.headers
  };
  if (signed.body !== void 0) {
    out.body = signed.body;
  }
  return out;
}
function createSignedRequest(url, init, expectedHeaders) {
  let request;
  try {
    request = new Request(url, init);
  } catch (err) {
    const duplex = init.duplex;
    if (!(err instanceof TypeError) || !(init.body instanceof ReadableStream) || duplex !== void 0) {
      throw err;
    }
    request = new Request(url, {
      ...init,
      duplex: "half"
    });
  }
  assertSignedRequestHeadersPreserved(expectedHeaders, request.headers);
  return request;
}
function rejectNoCorsMode(mode) {
  if (mode === "no-cors") {
    throw new TypeError('SigV4Client cannot sign requests with mode "no-cors" because required headers may be removed');
  }
}
function validateRequestBeforeTransport(request) {
  try {
    request.signal.throwIfAborted();
    rejectNoCorsMode(request.mode);
    if (request.redirect !== "manual") {
      throw new TypeError('SigV4Client.fetch signed Request must use redirect: "manual"');
    }
    return request.signal;
  } catch (err) {
    cancelRequestBody(request, err);
    throw err;
  }
}
function isRedirectResponse(response) {
  return response.type === "opaqueredirect" || response.redirected || response.status === 301 || response.status === 302 || response.status === 303 || response.status === 307 || response.status === 308;
}
function assertSignedRequestHeadersPreserved(expected, actual) {
  const authorization = expected.get(AUTHORIZATION_HEADER);
  if (authorization === null || actual.get(AUTHORIZATION_HEADER) !== authorization) {
    throw new TypeError("runtime removed or rewrote the authorization header after signing");
  }
  const signedHeaders = /(?:^|,\s*)SignedHeaders=([^,\s]+)/u.exec(authorization)?.[1];
  if (!signedHeaders) {
    throw new Error("generated SigV4 authorization is missing SignedHeaders");
  }
  for (const name of signedHeaders.split(";")) {
    if (name === HOST_HEADER) {
      continue;
    }
    if (actual.get(name) !== expected.get(name)) {
      throw new TypeError(`runtime removed or rewrote the signed ${name} header`);
    }
  }
}
function bindFetch(fetchFn) {
  return Object.is(fetchFn, globalThis.fetch) ? fetchFn.bind(globalThis) : fetchFn;
}
function resolveFetchRedirectPolicy(inherited, explicit) {
  if (explicit !== void 0) {
    if (explicit === "follow") {
      throw new TypeError('SigV4Client.fetch does not allow redirect: "follow"; redirected requests must be re-signed');
    }
    if (explicit !== "error" && explicit !== "manual") {
      throw new TypeError('redirect must be "error" or "manual"');
    }
    return explicit;
  }
  return inherited === "manual" ? "manual" : "error";
}
function cancelRequestBody(request, reason) {
  try {
    const cancellation = request.body?.cancel(reason);
    if (cancellation !== void 0) {
      void cancellation.catch(() => void 0);
    }
  } catch {
  }
}
function isAbortError(err) {
  return err instanceof DOMException && err.name === "AbortError";
}
function sleep(ms, signal) {
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    let timeout;
    const onAbort = () => {
      if (timeout !== void 0) {
        clearTimeout(timeout);
      }
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
  });
}
function cancelResponseBody(response) {
  try {
    const cancellation = response.body?.cancel();
    if (cancellation) {
      void cancellation.catch(() => void 0);
    }
  } catch {
  }
}
async function signAwsRequest(options) {
  const snapshot = snapshotSignAwsRequestOptions(options);
  const signal = snapshot.signal ?? void 0;
  signal?.throwIfAborted();
  const signed = await signAwsRequestInternal(snapshot);
  signal?.throwIfAborted();
  return signed;
}
async function signAwsRequestInternal(options, secretAccessKeyHash, parsedRequestUrl, reusablePreparedBody) {
  validateCredentialOptions(options, "signAwsRequest options are required");
  const cache = requireSigningCache(options.cache, "cache");
  requireDefinedOption(options.url, "url");
  const requestUrl = parsedRequestUrl ?? parseRequestUrl(options.url);
  const url = requestUrl.url;
  const method = normalizeMethod(options.method === void 0 ? defaultMethod(options.body) : options.method);
  const headers = new Headers(options.headers);
  rejectEmptyHeader(headers, AMZ_CONTENT_SHA256_HEADER);
  const unsignedPayload = resolveUnsignedPayload(optionalBoolean(options.unsignedPayload, "unsignedPayload"), options.service);
  const signAllHeaders = optionalBoolean(options.signAllHeaders, "signAllHeaders");
  const unsignableHeaders = normalizeUnsignableHeaders(options.unsignableHeaders, "unsignableHeaders");
  const doubleUrlEncode = resolveDoubleUrlEncode(optionalBoolean(options.doubleUrlEncode, "doubleUrlEncode"), options.service);
  const explicitAmzDate = optionalAmzDate(options.signingDate);
  headers.set(HOST_HEADER, url.host);
  if (options.sessionToken) {
    headers.set(AMZ_SECURITY_TOKEN_HEADER, options.sessionToken);
  }
  validateSignedHeaderValues(headers, {
    service: options.service,
    signAllHeaders,
    unsignableHeaders,
    overwrittenHeaderNames: signerOverwrittenHeaderNames(options.sessionToken !== void 0)
  });
  const canonicalPath = canonicalPathname(requestUrl.pathname, options.service, doubleUrlEncode);
  const preparedBody = reusablePreparedBody ?? await prepareSigningBody(options.body, headers, {
    service: options.service,
    unsignedPayload,
    replay: false,
    signal: options.signal ?? void 0
  });
  const amzDate = explicitAmzDate ?? formatAmzDate(/* @__PURE__ */ new Date());
  const date = amzDate.slice(0, 8);
  const credentialScope = `${date}/${options.region}/${options.service}/${AWS_REQUEST}`;
  headers.set(AMZ_DATE_HEADER, amzDate);
  const canonicalPayloadHash = preparedBody.payloadHash;
  const { canonicalHeaders, signedHeaders } = canonicalHeaderBlock(url, headers, {
    service: options.service,
    signAllHeaders,
    unsignableHeaders
  });
  const canonicalRequest = [
    method,
    canonicalPath,
    canonicalQuery(requestUrl.search),
    `${canonicalHeaders}
`,
    signedHeaders,
    canonicalPayloadHash
  ].join("\n");
  const stringToSign = [AWS_ALGORITHM, amzDate, credentialScope, await sha256Hex(canonicalRequest)].join("\n");
  const resolvedSecretAccessKeyHash = typeof secretAccessKeyHash === "function" ? await secretAccessKeyHash() : secretAccessKeyHash;
  const signature = await signatureHex({
    secretAccessKey: options.secretAccessKey,
    secretAccessKeyHash: resolvedSecretAccessKeyHash,
    date,
    region: options.region,
    service: options.service,
    stringToSign,
    cache
  });
  headers.set(AUTHORIZATION_HEADER, [
    `${AWS_ALGORITHM} Credential=${options.accessKeyId}/${credentialScope}`,
    `SignedHeaders=${signedHeaders}`,
    `Signature=${signature}`
  ].join(", "));
  return {
    method,
    url: requestUrl.href,
    headers,
    body: preparedBody.body
  };
}
var MAX_RETRY_DELAY_MS = 2147483647;
var SigV4Client = class {
  #accessKeyId;
  #secretAccessKey;
  #secretAccessKeyHash;
  #sessionToken;
  #service;
  #region;
  #cache;
  #retries;
  #initialRetryDelayMs;
  #maxRetryDelayMs;
  #unsignedPayload;
  #signAllHeaders;
  #unsignableHeaders;
  #doubleUrlEncode;
  #fetchFn;
  constructor(options) {
    validateCredentialOptions(options, "SigV4Client options are required");
    this.#accessKeyId = options.accessKeyId;
    this.#secretAccessKey = options.secretAccessKey;
    this.#sessionToken = options.sessionToken;
    this.#service = options.service;
    this.#region = options.region;
    this.#cache = requireSigningCache(options.cache, "cache") ?? /* @__PURE__ */ new Map();
    this.#retries = requireNonNegativeInteger(options.retries === void 0 ? 0 : options.retries, "retries");
    this.#initialRetryDelayMs = requireNonNegativeFiniteNumber(options.initialRetryDelayMs === void 0 ? 50 : options.initialRetryDelayMs, "initialRetryDelayMs");
    this.#maxRetryDelayMs = requireNonNegativeFiniteNumber(options.maxRetryDelayMs === void 0 ? 5e3 : options.maxRetryDelayMs, "maxRetryDelayMs");
    this.#unsignedPayload = optionalBoolean(options.unsignedPayload, "unsignedPayload");
    this.#signAllHeaders = optionalBoolean(options.signAllHeaders, "signAllHeaders");
    this.#unsignableHeaders = normalizeUnsignableHeaders(options.unsignableHeaders, "unsignableHeaders");
    this.#doubleUrlEncode = optionalBoolean(options.doubleUrlEncode, "doubleUrlEncode");
    const fetchFn = options.fetch === void 0 ? globalThis.fetch : options.fetch;
    if (typeof fetchFn !== "function") {
      throw new TypeError(options.fetch === void 0 ? "fetch is not available" : "fetch must be a function");
    }
    this.#fetchFn = bindFetch(fetchFn);
  }
  async sign(input, init) {
    const request = resolveClientSignRequest(input, init);
    const signing = this.#resolveSigningOptions(request.init.signing);
    delete request.init.signing;
    assertParsedRequestCanRepresentSignedUrl(request.requestUrl.pathname, signing.service);
    return this.#signResolvedRequest(request, signing);
  }
  async #signResolvedRequest(request, signing, preparedBody) {
    request.signal?.throwIfAborted();
    const signed = await signAwsRequestInternal({
      accessKeyId: this.#accessKeyId,
      secretAccessKey: this.#secretAccessKey,
      sessionToken: this.#sessionToken,
      service: signing.service,
      region: signing.region,
      cache: this.#cache,
      unsignedPayload: signing.unsignedPayload,
      signAllHeaders: signing.signAllHeaders,
      unsignableHeaders: signing.unsignableHeaders,
      doubleUrlEncode: signing.doubleUrlEncode,
      signingDate: signing.signingDate,
      signal: request.signal,
      method: request.method,
      url: request.requestUrl.href,
      headers: request.headers,
      body: request.body
    }, () => this.#getSecretAccessKeyHash(), request.requestUrl, preparedBody);
    request.signal?.throwIfAborted();
    const signedInit = requestInitForSignedRequest(request.init, signed);
    return createSignedRequest(signed.url, signedInit, signed.headers);
  }
  async fetch(input, init) {
    const request = resolveClientFetchRequest(input, init);
    const signing = this.#resolveSigningOptions(request.init.signing);
    delete request.init.signing;
    assertParsedRequestCanRepresentSignedUrl(request.requestUrl.pathname, signing.service);
    const retryableMethod = isIdempotentMethod(request.method);
    const prepared = await prepareFetchRequest(request, signing, this.#sessionToken !== void 0, this.#retries > 0 && retryableMethod);
    const fetchFn = this.#fetchFn;
    for (let attempt = 0; attempt <= this.#retries; attempt += 1) {
      const signedRequest = await this.#signResolvedRequest(prepared.request, signing, prepared.preparedBody);
      const attemptSignal = validateRequestBeforeTransport(signedRequest);
      let response;
      try {
        response = await fetchFn(signedRequest);
      } catch (err) {
        attemptSignal.throwIfAborted();
        if (attempt === this.#retries || !retryableMethod || isAbortError(err)) {
          throw err;
        }
        await sleep(Math.random() * this.#retryDelayMs(attempt), attemptSignal);
        continue;
      }
      if (attemptSignal.aborted) {
        cancelResponseBody(response);
        attemptSignal.throwIfAborted();
      }
      if (prepared.request.redirectPolicy === "manual" && response.redirected) {
        cancelResponseBody(response);
        attemptSignal.throwIfAborted();
        throw new TypeError('SigV4Client.fetch custom transport followed a redirect despite redirect: "manual"');
      }
      if (prepared.request.redirectPolicy === "error" && isRedirectResponse(response)) {
        cancelResponseBody(response);
        attemptSignal.throwIfAborted();
        throw new TypeError("SigV4Client.fetch received a redirect response; redirect targets must be re-signed");
      }
      const retryableResponse = retryableMethod && (response.status >= 500 || response.status === 429);
      if (attempt === this.#retries || !retryableResponse) {
        return response;
      }
      cancelResponseBody(response);
      attemptSignal.throwIfAborted();
      await sleep(Math.random() * this.#retryDelayMs(attempt), attemptSignal);
    }
    throw new Error("unreachable retry loop exit");
  }
  #resolveSigningOptions(value) {
    const options = normalizeClientSigningOptions(value);
    const service = options.service ?? this.#service;
    return {
      service,
      region: options.region ?? this.#region,
      unsignedPayload: resolveUnsignedPayload(options.unsignedPayload ?? this.#unsignedPayload, service),
      signAllHeaders: options.signAllHeaders ?? this.#signAllHeaders,
      unsignableHeaders: options.unsignableHeaders ?? this.#unsignableHeaders,
      doubleUrlEncode: resolveDoubleUrlEncode(options.doubleUrlEncode ?? this.#doubleUrlEncode, service),
      signingDate: options.signingDate
    };
  }
  #getSecretAccessKeyHash() {
    if (this.#secretAccessKeyHash !== void 0) {
      return this.#secretAccessKeyHash;
    }
    const hash = sha256Hex(this.#secretAccessKey).catch((err) => {
      if (this.#secretAccessKeyHash === hash) {
        this.#secretAccessKeyHash = void 0;
      }
      throw err;
    });
    this.#secretAccessKeyHash = hash;
    return hash;
  }
  #retryDelayMs(attempt) {
    return Math.min(MAX_RETRY_DELAY_MS, this.#maxRetryDelayMs, this.#initialRetryDelayMs * 2 ** attempt);
  }
};
async function prepareFetchRequest(request, signing, hasSessionToken, replay) {
  const headers = new Headers(request.headers);
  rejectEmptyHeader(headers, AMZ_CONTENT_SHA256_HEADER);
  validateSignedHeaderValues(headers, {
    service: signing.service,
    signAllHeaders: signing.signAllHeaders,
    unsignableHeaders: signing.unsignableHeaders,
    overwrittenHeaderNames: signerOverwrittenHeaderNames(hasSessionToken)
  });
  const preparedBody = await prepareSigningBody(request.body, headers, {
    service: signing.service,
    unsignedPayload: signing.unsignedPayload,
    replay,
    signal: request.signal
  });
  const out = {
    ...request.init,
    headers
  };
  if (preparedBody.body === void 0) {
    delete out.body;
  } else {
    out.body = preparedBody.body;
  }
  return {
    request: {
      ...request,
      init: out,
      headers,
      body: preparedBody.body
    },
    preparedBody
  };
}
export {
  SigV4Client,
  signAwsRequest
};
