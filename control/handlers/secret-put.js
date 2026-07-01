import {
  jsonError,
  readJsonBody,
  errMessage,
  stringEnv,
} from "control-shared";
import { validateSecretKey } from "control-lib";
import { encryptSecretValue, SecretEnvelopeError } from "shared-secret-envelope";

const SECRET_PUT_JSON_BODY_MAX_BYTES = 128 * 1024;

/**
 * @param {string} key
 * @returns {Response | null}
 */
export function invalidSecretMutationKeyResponse(key) {
  try {
    validateSecretKey(key);
    return null;
  } catch (err) {
    return jsonError(400, "invalid_request", errMessage(err));
  }
}

/**
 * @param {{
 *   request: Request,
 *   env: Record<string, unknown>,
 *   hashKey: string,
 *   fieldName: string,
 * }} args
 * @returns {Promise<{ response: Response } | { encrypted: string, plaintext: string }>}
 */
export async function readEncryptedSecretPutValue({ request, env, hashKey, fieldName }) {
  const parsed = await readJsonBody(request, {
    requireObject: true,
    maxBytes: SECRET_PUT_JSON_BODY_MAX_BYTES,
  });
  if (parsed.response) return { response: parsed.response };
  const body = /** @type {Record<string, unknown>} */ (parsed.body);
  if (typeof body.value !== "string") {
    return { response: jsonError(400, "invalid_request", "Body must be { value: string }") };
  }
  if (Buffer.byteLength(body.value, "utf8") > 64 * 1024) {
    return { response: jsonError(400, "invalid_request", "secret value too large (max 64 KiB utf-8)") };
  }
  try {
    return {
      plaintext: body.value,
      encrypted: await encryptSecretValue(body.value, { env: stringEnv(env), hashKey, fieldName }),
    };
  } catch (err) {
    if (err instanceof SecretEnvelopeError) {
      return { response: jsonError(503, err.code, err.message) };
    }
    throw err;
  }
}
