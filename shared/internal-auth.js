export const INTERNAL_AUTH_HEADER = "x-wdl-internal-auth";
export const INTERNAL_AUTH_ENV = "WDL_INTERNAL_AUTH_TOKEN";
export const INTERNAL_AUTH_PREVIOUS_ENV = "WDL_INTERNAL_AUTH_PREVIOUS_TOKEN";
export const INTERNAL_AUTH_FAILURE_CODE = "internal_auth_failed";
export const INTERNAL_AUTH_FAILURE_MESSAGE = "Internal authentication failed";

/**
 * @param {string} name
 * @param {string} token
 */
function assertHeaderToken(name, token) {
  if (token === "") {
    throw new Error(`${name} must be configured as visible ASCII without whitespace or commas`);
  }
  for (let i = 0; i < token.length; i++) {
    const code = token.charCodeAt(i);
    if (code < 0x21 || code > 0x7e || code === 0x2c) {
      throw new Error(`${name} must be configured as visible ASCII without whitespace or commas`);
    }
  }
  return token;
}

/**
 * @param {Record<string, unknown> | null | undefined} env
 */
export function internalAuthToken(env) {
  const token = env?.[INTERNAL_AUTH_ENV];
  if (typeof token !== "string" || token === "") {
    throw new Error(`${INTERNAL_AUTH_ENV} must be configured`);
  }
  return assertHeaderToken(INTERNAL_AUTH_ENV, token);
}

/**
 * @param {Record<string, unknown> | null | undefined} env
 */
export function internalAuthPreviousToken(env) {
  const token = env?.[INTERNAL_AUTH_PREVIOUS_ENV];
  return typeof token === "string" && token !== "" ? assertHeaderToken(INTERNAL_AUTH_PREVIOUS_ENV, token) : null;
}

/**
 * @param {string} actual
 * @param {string} expected
 */
function constantTimeEqual(actual, expected) {
  const max = Math.max(actual.length, expected.length);
  let diff = actual.length ^ expected.length;
  for (let i = 0; i < max; i++) {
    diff |= (actual.charCodeAt(i) || 0) ^ (expected.charCodeAt(i) || 0);
  }
  return diff === 0;
}

/**
 * @param {Headers} headers
 * @param {Record<string, unknown> | null | undefined} env
 */
export function verifyInternalAuthHeaders(headers, env) {
  try {
    const expected = internalAuthToken(env);
    const actual = headers.get(INTERNAL_AUTH_HEADER) || "";
    if (actual === "") return false;
    if (constantTimeEqual(actual, expected)) return true;
    const previous = internalAuthPreviousToken(env);
    return previous !== null && constantTimeEqual(actual, previous);
  } catch {
    return false;
  }
}

/**
 * @param {HeadersInit | undefined} headers
 * @param {Record<string, unknown> | null | undefined} env
 */
export function withInternalAuth(headers, env) {
  const out = new Headers(headers);
  out.set(INTERNAL_AUTH_HEADER, internalAuthToken(env));
  return out;
}

/**
 * Build authenticated headers without calling `Headers#set` with the token.
 * Use this for backend capabilities callable from generated tenant facades,
 * where tenant code may have monkeypatched `Headers.prototype.set`.
 *
 * @param {HeadersInit | undefined} headers
 * @param {Record<string, unknown> | null | undefined} env
 * @returns {Record<string, string>}
 */
export function withInternalAuthEntries(headers, env) {
  /** @type {Record<string, string>} */
  const out = /** @type {Record<string, string>} */ (/** @type {unknown} */ ({ __proto__: null }));
  if (headers) {
    if (typeof /** @type {{ [Symbol.iterator]?: unknown }} */ (headers)[Symbol.iterator] === "function") {
      for (const entry of /** @type {Iterable<[unknown, unknown]>} */ (headers)) {
        const [name, value] = entry;
        if (String(name).toLowerCase() !== INTERNAL_AUTH_HEADER) {
          out[String(name)] = String(value);
        }
      }
    } else {
      for (const [name, value] of Object.entries(headers)) {
        if (name.toLowerCase() !== INTERNAL_AUTH_HEADER) {
          out[name] = String(value);
        }
      }
    }
  }
  out[INTERNAL_AUTH_HEADER] = internalAuthToken(env);
  return out;
}

/** @param {Headers} headers */
export function stripInternalAuthHeader(headers) {
  headers.delete(INTERNAL_AUTH_HEADER);
  return headers;
}

export function internalAuthFailureResponse() {
  return Response.json({
    error: INTERNAL_AUTH_FAILURE_CODE,
    message: INTERNAL_AUTH_FAILURE_MESSAGE,
  }, { status: 401 });
}
