import { readBoundedBytes } from "shared-bounded-body";
import { discardResponseBody } from "shared-respond";

export const DO_ALARM_RESPONSE_MAX_BYTES = 16 * 1024;
export const DO_ALARM_RESPONSE_DEADLINE_MS = 5_000;

const strictUtf8Decoder = new TextDecoder("utf-8", { fatal: true });
const intrinsicArrayIsArray = Array.isArray;
const intrinsicJsonParse = JSON.parse;
const intrinsicNumberIsSafeInteger = Number.isSafeInteger;
const intrinsicObjectHasOwn = Object.hasOwn;
const intrinsicObjectKeys = Object.keys;
const intrinsicReflectApply = Reflect.apply;
const intrinsicTextDecoderDecode = TextDecoder.prototype.decode;

/** @param {Response} response @param {AbortSignal} [signal] */
export async function readDoAlarmResponseText(
  response,
  signal = AbortSignal.timeout(DO_ALARM_RESPONSE_DEADLINE_MS)
) {
  try {
    const bytes = await readBoundedBytes(response, DO_ALARM_RESPONSE_MAX_BYTES, signal);
    return intrinsicReflectApply(intrinsicTextDecoderDecode, strictUtf8Decoder, [bytes]);
  } catch (error) {
    void discardResponseBody(response);
    throw error;
  }
}

/** @param {string} text */
function responseRecord(text) {
  const parsed = intrinsicJsonParse(text);
  if (parsed === null || typeof parsed !== "object" || intrinsicArrayIsArray(parsed)) {
    throw new TypeError("DO alarm response must be a JSON object");
  }
  return /** @type {Record<string, unknown>} */ (parsed);
}

/** @param {Record<string, unknown>} record @param {string[]} expected */
function hasExactFields(record, expected) {
  const keys = intrinsicObjectKeys(record);
  if (keys.length !== expected.length) return false;
  for (let index = 0; index < expected.length; index += 1) {
    if (!intrinsicObjectHasOwn(record, expected[index])) return false;
  }
  return true;
}

/** @param {string} text */
export function parseDoAlarmDispatchSuccess(text) {
  const record = responseRecord(text);
  if (record.ok !== true) throw new TypeError("DO alarm dispatch response must set ok=true");
  if (hasExactFields(record, ["ok"])) return { ignored: false };
  if (
    hasExactFields(record, ["ok", "ignored"]) &&
    record.ignored === true
  ) {
    return { ignored: true };
  }
  throw new TypeError("DO alarm dispatch response has an invalid success variant");
}

/** @param {string} text */
function parseDoAlarmMutationRecord(text) {
  const record = responseRecord(text);
  if (!hasExactFields(record, ["ok", "jobId", "changed", "deleted"])) {
    throw new TypeError("DO alarm mutation response has invalid fields");
  }
  if (
    record.ok !== true ||
    typeof record.changed !== "boolean" ||
    typeof record.deleted !== "number" ||
    !intrinsicNumberIsSafeInteger(record.deleted) ||
    record.deleted < 0
  ) {
    throw new TypeError("DO alarm mutation response has invalid field values");
  }
  return record;
}

/** @param {string} text */
export function parseDoAlarmMutationSuccess(text) {
  const record = parseDoAlarmMutationRecord(text);
  if (typeof record.jobId !== "string" || record.jobId.length === 0) {
    throw new TypeError("DO alarm mutation response has invalid jobId");
  }
  return record;
}

/** @param {string} text */
export function parseDoAlarmCleanupSuccess(text) {
  const record = parseDoAlarmMutationRecord(text);
  if (record.jobId !== null) {
    throw new TypeError("DO alarm cleanup response must set jobId=null");
  }
  return record;
}
