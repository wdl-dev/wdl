const OWNER_ENDPOINT_SERVICE_RE = {
  "d1-runtime": /^d1-runtime(?:-[a-z0-9-]+)?$/,
  "do-runtime": /^do-runtime(?:-[a-z0-9-]+)?$/,
};

const OWNER_ENDPOINT_HEADLESS_RE = {
  "d1-runtime": /^d1-runtime-[0-9]+\.d1-runtime-headless(?:\.[a-z0-9-]+\.svc(?:\.cluster\.local)?)?$/,
  "do-runtime": /^do-runtime-[0-9]+\.do-runtime-headless(?:\.[a-z0-9-]+\.svc(?:\.cluster\.local)?)?$/,
};
const INVALID_OWNER_ENDPOINT_RE = /[/?#@[\]\\]/;
const IPV4_OCTET_RE = /^(?:0|[1-9]\d{0,2})$/;
const IntrinsicNumber = Number;
const IntrinsicString = String;
const IntrinsicURL = URL;
const intrinsicReflectApply = Reflect.apply;
const intrinsicRegExpTest = RegExp.prototype.test;
const intrinsicStringSplit = String.prototype.split;
const intrinsicUrlHashGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "hash"));
const intrinsicUrlHostGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "host"));
const intrinsicUrlHostnameGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "hostname"));
const intrinsicUrlPasswordGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "password"));
const intrinsicUrlPathnameGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "pathname"));
const intrinsicUrlPortGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "port"));
const intrinsicUrlSearchGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "search"));
const intrinsicUrlUsernameGet = /** @type {(this: URL) => string} */ (prototypeGetter(URL.prototype, "username"));

/** @param {object | null} prototype @param {string} name */
function prototypeGetter(prototype, name) {
  let current = prototype;
  while (current) {
    const getter = Object.getOwnPropertyDescriptor(current, name)?.get;
    if (getter) return getter;
    current = Object.getPrototypeOf(current);
  }
  return undefined;
}

/** @param {RegExp} regexp @param {string} value */
function regexpTest(regexp, value) {
  return intrinsicReflectApply(intrinsicRegExpTest, regexp, [value]);
}

/** @param {string} value */
function splitDots(value) {
  return intrinsicReflectApply(intrinsicStringSplit, value, ["."]);
}

/** @param {unknown} value */
function numberValue(value) {
  return intrinsicReflectApply(IntrinsicNumber, undefined, [value]);
}

/** @param {unknown} value */
function stringValue(value) {
  return intrinsicReflectApply(IntrinsicString, undefined, [value]);
}

/** @param {(this: URL) => string} getter @param {URL} url */
function urlValue(getter, url) {
  return intrinsicReflectApply(getter, url, []);
}

/**
 * @param {unknown} endpoint
 * @param {number} port
 * @param {"d1-runtime" | "do-runtime"} serviceName
 * @returns {boolean}
 */
export function validOwnerEndpointForService(endpoint, port, serviceName) {
  if (typeof endpoint !== "string" || !endpoint) return false;
  const serviceRe = OWNER_ENDPOINT_SERVICE_RE[serviceName];
  const headlessRe = OWNER_ENDPOINT_HEADLESS_RE[serviceName];
  if (!serviceRe) throw new Error(`Unknown owner endpoint service ${serviceName}`);
  if (regexpTest(INVALID_OWNER_ENDPOINT_RE, endpoint)) return false;
  let url;
  try {
    url = new IntrinsicURL(`http://${endpoint}`);
  } catch {
    return false;
  }
  if (
    urlValue(intrinsicUrlHostGet, url) !== endpoint ||
    urlValue(intrinsicUrlUsernameGet, url) ||
    urlValue(intrinsicUrlPasswordGet, url) ||
    urlValue(intrinsicUrlPathnameGet, url) !== "/" ||
    urlValue(intrinsicUrlSearchGet, url) ||
    urlValue(intrinsicUrlHashGet, url)
  ) {
    return false;
  }
  if (urlValue(intrinsicUrlPortGet, url) !== stringValue(port)) return false;
  const hostname = urlValue(intrinsicUrlHostnameGet, url);
  // ECS task ENI addresses are VPC-local but not necessarily RFC1918; some
  // deployments may use non-RFC1918 VPC-local ranges. Trust comes from
  // runtime-authored owner-hint headers, while this check rejects URL injection
  // and obviously unsafe endpoint classes.
  return regexpTest(serviceRe, hostname) || regexpTest(headlessRe, hostname) || acceptableIpv4(hostname);
}

/**
 * @param {string} hostname
 * @returns {boolean}
 */
function acceptableIpv4(hostname) {
  const parts = splitDots(hostname);
  if (parts.length !== 4) return false;
  /** @type {number[]} */
  const octets = [];
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (!regexpTest(IPV4_OCTET_RE, part)) return false;
    const value = numberValue(part);
    if (value < 0 || value > 255) return false;
    octets[i] = value;
  }
  const a = octets[0];
  const b = octets[1];
  if (a === 0 || a === 127) return false;
  if (a === 169 && b === 254) return false;
  if (a >= 224) return false;
  return true;
}
