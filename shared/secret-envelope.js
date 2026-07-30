import {
  base64ToBytes as decodeBase64Bytes,
  bytesToBase64,
} from "./base64.js";

export const SECRET_ENVELOPE_PREFIX = "WDL-ENC:";
export const SECRET_ENVELOPE_VERSION = 1;
export const SECRET_ENVELOPE_ALG = "AES-256-GCM";
export const SECRET_ENVELOPE_LOCAL_KEY_ENV = "SECRET_ENVELOPE_LOCAL_KEY_B64";
export const SECRET_ENVELOPE_KID_ENV = "SECRET_ENVELOPE_KID";

const AES_GCM_TAG_BYTES = 16;
const AES_GCM_IV_BYTES = 12;
const AES_256_KEY_BYTES = 32;
const LOCAL_PROVIDER_CACHE_MAX_ENTRIES = 4;
const ENVELOPE_CANONICAL_FIELDS = ["v", "alg", "kid", "edek", "iv", "ct", "tag"];
const ENVELOPE_FIELDS = ENVELOPE_CANONICAL_FIELDS.toSorted();
const utf8Encoder = new TextEncoder();
const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });
/** @type {Map<string, Promise<{ kid: string, key: CryptoKey }>>} */
const localProviderCache = new Map();

/**
 * @typedef {{
 *   v: number,
 *   alg: string,
 *   kid: string,
 *   edek: string,
 *   iv: string,
 *   ct: string,
 *   tag: string,
 * }} SecretEnvelope
 */

export class SecretEnvelopeError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   */
  constructor(code, message) {
    super(message);
    this.name = "SecretEnvelopeError";
    this.code = code;
  }
}

/** @param {string} value @returns {Uint8Array} */
function textBytes(value) {
  return utf8Encoder.encode(value);
}

/** @param {Uint8Array} bytes @returns {string} */
function bytesToText(bytes) {
  return utf8FatalDecoder.decode(bytes);
}

export { bytesToBase64 };

/** @param {Record<string, unknown>} envelope */
function canonicalEnvelopeJson(envelope) {
  /** @type {Record<string, unknown>} */
  const ordered = {};
  for (const field of ENVELOPE_CANONICAL_FIELDS) ordered[field] = envelope[field];
  return JSON.stringify(ordered);
}

/**
 * @param {unknown} value
 * @param {string} [fieldName]
 * @param {{ allowEmpty?: boolean }} [options]
 * @returns {Uint8Array}
 */
export function base64ToBytes(value, fieldName = "base64 field", { allowEmpty = false } = {}) {
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    const requirement = allowEmpty ? "a base64 string" : "a non-empty base64 string";
    throw new SecretEnvelopeError("invalid_envelope", `${fieldName} must be ${requirement}`);
  }
  let bytes;
  try {
    bytes = decodeBase64Bytes(value);
  } catch {
    throw new SecretEnvelopeError("invalid_envelope", `${fieldName} is not valid base64`);
  }
  if (bytesToBase64(bytes) !== value) {
    throw new SecretEnvelopeError("invalid_envelope", `${fieldName} is not canonical base64`);
  }
  return bytes;
}

/** @param {number} length @returns {Uint8Array} */
export function randomBytes(length) {
  const out = new Uint8Array(length);
  crypto.getRandomValues(out);
  return out;
}

/** @param {string} hashKey @param {string} fieldName @returns {string} */
function storageAadString(hashKey, fieldName) {
  return `${hashKey}\0${fieldName}\0${SECRET_ENVELOPE_VERSION}`;
}

/** @param {string} hashKey @param {string} fieldName @returns {Uint8Array} */
function payloadAadBytes(hashKey, fieldName) {
  return textBytes(
    `WDL-SECRET\0${storageAadString(hashKey, fieldName)}\0v=${SECRET_ENVELOPE_VERSION}\0alg=${SECRET_ENVELOPE_ALG}`
  );
}

/** @param {string} kid @param {string} hashKey @param {string} fieldName @returns {Uint8Array} */
function dataKeyAadBytes(kid, hashKey, fieldName) {
  return textBytes(`WDL-SECRET-DEK\0${kid}\0${storageAadString(hashKey, fieldName)}`);
}

/**
 * @param {Record<string, string | undefined>} [env]
 * @returns {{ kid: string, localKeyB64: string, keyBytes: Uint8Array }}
 */
function requireLocalConfig(env = {}) {
  const kid = env[SECRET_ENVELOPE_KID_ENV];
  if (typeof kid !== "string" || !kid.startsWith("local:")) {
    throw new SecretEnvelopeError(
      "secret_encryption_unconfigured",
      `${SECRET_ENVELOPE_KID_ENV} must be a canonical local provider kid`
    );
  }
  const localKeyB64 = env[SECRET_ENVELOPE_LOCAL_KEY_ENV];
  let keyBytes;
  try {
    keyBytes = base64ToBytes(localKeyB64, SECRET_ENVELOPE_LOCAL_KEY_ENV);
  } catch (err) {
    if (err instanceof SecretEnvelopeError) {
      throw new SecretEnvelopeError("secret_encryption_unconfigured", err.message);
    }
    throw err;
  }
  if (keyBytes.length !== AES_256_KEY_BYTES) {
    throw new SecretEnvelopeError(
      "secret_encryption_unconfigured",
      `${SECRET_ENVELOPE_LOCAL_KEY_ENV} must decode to 32 bytes`
    );
  }
  return { kid, localKeyB64: /** @type {string} */ (localKeyB64), keyBytes };
}

/** @param {Uint8Array} keyBytes @returns {Promise<CryptoKey>} */
async function importAesKey(keyBytes) {
  return await crypto.subtle.importKey(
    "raw",
    /** @type {BufferSource} */ (keyBytes),
    "AES-GCM",
    false,
    ["encrypt", "decrypt"]
  );
}

/**
 * @param {Record<string, string | undefined>} env
 * @returns {Promise<{ kid: string, key: CryptoKey }>}
 */
async function requireLocalProvider(env) {
  const kid = env[SECRET_ENVELOPE_KID_ENV];
  const localKeyB64 = env[SECRET_ENVELOPE_LOCAL_KEY_ENV];
  const unvalidatedCacheKey = typeof kid === "string" && typeof localKeyB64 === "string"
    ? `${kid}\0${localKeyB64}`
    : null;
  const cached = unvalidatedCacheKey === null ? undefined : localProviderCache.get(unvalidatedCacheKey);
  if (cached) return await cached;

  const config = requireLocalConfig(env);
  const cacheKey = `${config.kid}\0${config.localKeyB64}`;
  const providerPromise = importAesKey(config.keyBytes).then((key) => ({ kid: config.kid, key }));
  localProviderCache.set(cacheKey, providerPromise);
  if (localProviderCache.size > LOCAL_PROVIDER_CACHE_MAX_ENTRIES) {
    const oldest = localProviderCache.keys().next().value;
    if (oldest !== undefined) localProviderCache.delete(oldest);
  }
  try {
    return await providerPromise;
  } catch (err) {
    if (localProviderCache.get(cacheKey) === providerPromise) localProviderCache.delete(cacheKey);
    throw err;
  }
}

/**
 * @param {CryptoKey} key
 * @param {Uint8Array} iv
 * @param {Uint8Array} plaintext
 * @param {Uint8Array} aad
 * @returns {Promise<{ ct: Uint8Array, tag: Uint8Array }>}
 */
async function aesGcmEncryptWithKey(key, iv, plaintext, aad) {
  const encrypted = new Uint8Array(await crypto.subtle.encrypt(
    {
      name: "AES-GCM",
      iv: /** @type {BufferSource} */ (iv),
      additionalData: /** @type {BufferSource} */ (aad),
      tagLength: AES_GCM_TAG_BYTES * 8,
    },
    key,
    /** @type {BufferSource} */ (plaintext)
  ));
  return {
    ct: encrypted.subarray(0, encrypted.length - AES_GCM_TAG_BYTES),
    tag: encrypted.subarray(encrypted.length - AES_GCM_TAG_BYTES),
  };
}

/**
 * @param {Uint8Array} keyBytes
 * @param {Uint8Array} iv
 * @param {Uint8Array} plaintext
 * @param {Uint8Array} aad
 * @returns {Promise<{ ct: Uint8Array, tag: Uint8Array }>}
 */
async function aesGcmEncrypt(keyBytes, iv, plaintext, aad) {
  return await aesGcmEncryptWithKey(await importAesKey(keyBytes), iv, plaintext, aad);
}

/**
 * @param {CryptoKey} key
 * @param {Uint8Array} iv
 * @param {Uint8Array} ct
 * @param {Uint8Array} tag
 * @param {Uint8Array} aad
 * @returns {Promise<Uint8Array>}
 */
async function aesGcmDecryptWithKey(key, iv, ct, tag, aad) {
  const combined = new Uint8Array(ct.length + tag.length);
  combined.set(ct);
  combined.set(tag, ct.length);
  try {
    return new Uint8Array(await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: /** @type {BufferSource} */ (iv),
        additionalData: /** @type {BufferSource} */ (aad),
        tagLength: AES_GCM_TAG_BYTES * 8,
      },
      key,
      /** @type {BufferSource} */ (combined)
    ));
  } catch {
    throw new SecretEnvelopeError("secret_decrypt_failed", "secret envelope authentication failed");
  }
}

/**
 * @param {Uint8Array} keyBytes
 * @param {Uint8Array} iv
 * @param {Uint8Array} ct
 * @param {Uint8Array} tag
 * @param {Uint8Array} aad
 * @returns {Promise<Uint8Array>}
 */
async function aesGcmDecrypt(keyBytes, iv, ct, tag, aad) {
  return await aesGcmDecryptWithKey(await importAesKey(keyBytes), iv, ct, tag, aad);
}

/**
 * @param {string} plaintext
 * @param {{
 *   env: Record<string, string | undefined>,
 *   hashKey: string,
 *   fieldName: string,
 *   random?: (length: number) => Uint8Array,
 * }} options
 */
export async function encryptSecretValue(plaintext, { env, hashKey, fieldName, random = randomBytes }) {
  if (typeof plaintext !== "string") {
    throw new SecretEnvelopeError("invalid_secret_value", "secret plaintext must be a string");
  }
  if (typeof hashKey !== "string" || hashKey === "" || typeof fieldName !== "string" || fieldName === "") {
    throw new SecretEnvelopeError("invalid_secret_location", "secret hash key and field name are required");
  }
  const { kid, key: localKey } = await requireLocalProvider(env);
  const dek = random(AES_256_KEY_BYTES);
  const dekIv = random(AES_GCM_IV_BYTES);
  const payloadIv = random(AES_GCM_IV_BYTES);
  const wrappedDek = await aesGcmEncryptWithKey(localKey, dekIv, dek, dataKeyAadBytes(kid, hashKey, fieldName));
  const edekBytes = new Uint8Array(dekIv.length + wrappedDek.ct.length + wrappedDek.tag.length);
  edekBytes.set(dekIv);
  edekBytes.set(wrappedDek.ct, dekIv.length);
  edekBytes.set(wrappedDek.tag, dekIv.length + wrappedDek.ct.length);

  const payload = await aesGcmEncrypt(dek, payloadIv, textBytes(plaintext), payloadAadBytes(hashKey, fieldName));
  return `${SECRET_ENVELOPE_PREFIX}${canonicalEnvelopeJson({
    v: SECRET_ENVELOPE_VERSION,
    alg: SECRET_ENVELOPE_ALG,
    kid,
    edek: bytesToBase64(edekBytes),
    iv: bytesToBase64(payloadIv),
    ct: bytesToBase64(payload.ct),
    tag: bytesToBase64(payload.tag),
  })}`;
}

/**
 * @param {unknown} value
 * @returns {SecretEnvelope}
 */
function parseEnvelope(value) {
  if (typeof value !== "string" || !value.startsWith(SECRET_ENVELOPE_PREFIX)) {
    throw new SecretEnvelopeError("secret_not_encrypted", "secret value is not envelope encrypted");
  }
  const json = value.slice(SECRET_ENVELOPE_PREFIX.length);
  let parsed;
  try {
    parsed = JSON.parse(json);
  } catch {
    throw new SecretEnvelopeError("invalid_envelope", "secret envelope JSON is invalid");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new SecretEnvelopeError("invalid_envelope", "secret envelope must be a JSON object");
  }
  const envelope = /** @type {Record<string, unknown>} */ (parsed);
  const fields = Object.keys(envelope).toSorted();
  if (fields.length !== ENVELOPE_FIELDS.length || fields.some((field, idx) => field !== ENVELOPE_FIELDS[idx])) {
    throw new SecretEnvelopeError("invalid_envelope", "secret envelope has unknown or missing fields");
  }
  if (canonicalEnvelopeJson(envelope) !== json) {
    throw new SecretEnvelopeError("invalid_envelope", "secret envelope JSON is not canonical");
  }
  if (envelope.v !== SECRET_ENVELOPE_VERSION || envelope.alg !== SECRET_ENVELOPE_ALG) {
    throw new SecretEnvelopeError("unsupported_envelope", "secret envelope version or algorithm is unsupported");
  }
  for (const field of ["kid", "edek", "iv", "tag"]) {
    if (typeof envelope[field] !== "string" || envelope[field] === "") {
      throw new SecretEnvelopeError("invalid_envelope", `secret envelope ${field} is missing`);
    }
  }
  if (typeof envelope.ct !== "string") {
    throw new SecretEnvelopeError("invalid_envelope", "secret envelope ct is missing");
  }
  return /** @type {SecretEnvelope} */ (envelope);
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isSecretEnvelope(value) {
  return typeof value === "string" && value.startsWith(SECRET_ENVELOPE_PREFIX);
}

/**
 * @param {string} envelopeValue
 * @param {{
 *   env: Record<string, string | undefined>,
 *   hashKey: string,
 *   fieldName: string,
 * }} options
 */
export async function decryptSecretValue(envelopeValue, { env, hashKey, fieldName }) {
  if (typeof hashKey !== "string" || hashKey === "" || typeof fieldName !== "string" || fieldName === "") {
    throw new SecretEnvelopeError("invalid_secret_location", "secret hash key and field name are required");
  }
  const { kid, key: localKey } = await requireLocalProvider(env);
  const envelope = parseEnvelope(envelopeValue);
  if (envelope.kid !== kid) {
    throw new SecretEnvelopeError("unknown_kid", "secret envelope kid is not configured");
  }
  const edek = base64ToBytes(envelope.edek, "edek");
  if (edek.length !== AES_GCM_IV_BYTES + AES_256_KEY_BYTES + AES_GCM_TAG_BYTES) {
    throw new SecretEnvelopeError("invalid_envelope", "local provider edek has invalid length");
  }
  const dekIv = edek.subarray(0, AES_GCM_IV_BYTES);
  const dekCt = edek.subarray(AES_GCM_IV_BYTES, AES_GCM_IV_BYTES + AES_256_KEY_BYTES);
  const dekTag = edek.subarray(AES_GCM_IV_BYTES + AES_256_KEY_BYTES);
  const dek = await aesGcmDecryptWithKey(localKey, dekIv, dekCt, dekTag, dataKeyAadBytes(kid, hashKey, fieldName));
  if (dek.length !== AES_256_KEY_BYTES) {
    throw new SecretEnvelopeError("secret_decrypt_failed", "decrypted data key has invalid length");
  }
  const plaintext = await aesGcmDecrypt(
    dek,
    base64ToBytes(envelope.iv, "iv"),
    base64ToBytes(envelope.ct, "ct", { allowEmpty: true }),
    base64ToBytes(envelope.tag, "tag"),
    payloadAadBytes(hashKey, fieldName)
  );
  try {
    return bytesToText(plaintext);
  } catch {
    throw new SecretEnvelopeError("secret_decrypt_failed", "decrypted secret plaintext is not valid utf-8");
  }
}
