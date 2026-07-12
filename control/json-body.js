import { BodyTooLargeError, readBoundedText } from "shared-bounded-body";
import { jsonError } from "shared-respond";

export const DEFAULT_JSON_BODY_MAX_BYTES = 1024 * 1024;

/**
 * @param {Request} request
 * @param {{ requireObject?: boolean, allowEmpty?: boolean, maxBytes?: number }} [opts]
 */
export async function readJsonBody(
  request,
  { requireObject = false, allowEmpty = false, maxBytes = DEFAULT_JSON_BODY_MAX_BYTES } = {},
) {
  let body;
  try {
    const text = await readBoundedText(request, maxBytes);
    if (text === "") {
      if (!allowEmpty) {
        return {
          response: jsonError(400, "invalid_json", "Body must be valid JSON"),
        };
      }
      body = {};
    } else {
      body = JSON.parse(text);
    }
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      return {
        response: jsonError(
          413,
          "request_body_too_large",
          `Body must be at most ${maxBytes} bytes`
        ),
      };
    }
    return {
      response: jsonError(400, "invalid_json", "Body must be valid JSON"),
    };
  }
  if (requireObject && (!body || typeof body !== "object" || Array.isArray(body))) {
    return {
      response: jsonError(400, "invalid_json_object", "Body must be a JSON object"),
    };
  }
  return { body };
}
