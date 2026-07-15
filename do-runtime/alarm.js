import {
  DoRuntimeError,
  isWellFormedUnicodeString,
  nonEmptyAlarmString,
} from "do-runtime-protocol";
import { errorMessage } from "shared-errors";
import { withInternalAuth } from "shared-internal-auth";

/**
 * @typedef {{
 *   WORKFLOWS_BACKEND?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> } | null,
 *   WDL_INTERNAL_AUTH_TOKEN?: unknown,
 * }} DoEnv
 * @typedef {{ ns: string, worker: string, version: string, doStorageId: string }} AlarmProps
 * @typedef {{ className: string, objectName: string, scheduledTime?: unknown, retryCount?: unknown, token?: unknown }} AlarmInput
 */

/** @param {unknown} value */
export function normalizeAlarmScheduledTime(value) {
  const scheduledTime = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(scheduledTime) || scheduledTime <= 0) {
    throw new TypeError("setAlarm() cannot be called with an alarm time <= 0");
  }
  return Math.max(Date.now(), Math.trunc(scheduledTime));
}

/** @param {unknown} value */
export function alarmRetryCount(value) {
  if (value === undefined || value === null) return 0;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new TypeError("DO alarm retryCount must be a non-negative integer");
  }
  return value;
}

/**
 * @param {unknown} value
 * @param {string} field
 */
function requiredString(value, field) {
  const text = nonEmptyAlarmString(value);
  if (text === null) {
    throw new TypeError(`DO alarm ${field} must be a non-empty string`);
  }
  if (!isWellFormedUnicodeString(text)) {
    throw new TypeError(`DO alarm ${field} must contain well-formed Unicode`);
  }
  return text;
}

/** @param {DoEnv} env */
function workflowsBackend(env) {
  const backend = env.WORKFLOWS_BACKEND;
  if (!backend || typeof backend.fetch !== "function") {
    throw new DoRuntimeError(503, "do_alarm_backend_unavailable", "DO alarm backend is not configured");
  }
  return backend;
}

/**
 * @param {DoEnv} env
 * @param {string} path
 * @param {Record<string, unknown>} body
 */
async function postWorkflowsAlarm(env, path, body) {
  let response;
  let parsed;
  try {
    response = await workflowsBackend(env).fetch(`http://workflows${path}`, {
      method: "POST",
      headers: withInternalAuth({ "content-type": "application/json" }, env),
      body: JSON.stringify(body),
    });
    parsed = await response.json().catch(() => null);
  } catch (err) {
    throw new DoRuntimeError(503, "do_alarm_backend_unavailable", "DO alarm backend request failed", {
      error_message: errorMessage(err),
    });
  }
  if (!response.ok) {
    throw new DoRuntimeError(503, "do_alarm_backend_failed", "DO alarm backend rejected the request", {
      upstream_status: response.status,
      upstream_error: parsed && typeof parsed === "object" && "error" in parsed
        ? /** @type {{ error?: unknown }} */ (parsed).error
        : null,
    });
  }
  return parsed;
}

/**
 * @param {DoEnv} env
 * @param {AlarmProps} props
 * @param {AlarmInput} input
 */
export async function setAlarmIndex(env, props, input) {
  const className = requiredString(input.className, "className");
  const objectName = requiredString(input.objectName, "objectName");
  const token = requiredString(input.token, "token");
  const scheduledTime = normalizeAlarmScheduledTime(input.scheduledTime);
  const retryCount = alarmRetryCount(input.retryCount);
  return await postWorkflowsAlarm(env, "/internal/workflows/do-alarms/set", {
    ns: props.ns,
    worker: props.worker,
    version: props.version,
    doStorageId: props.doStorageId,
    className,
    objectName,
    scheduledTime,
    retryCount,
    token,
  });
}

/**
 * @param {DoEnv} env
 * @param {AlarmProps} props
 * @param {AlarmInput} input
 */
export async function deleteAlarmIndex(env, props, input) {
  const className = requiredString(input.className, "className");
  const objectName = requiredString(input.objectName, "objectName");
  const token = requiredString(input.token, "token");
  return await postWorkflowsAlarm(env, "/internal/workflows/do-alarms/delete", {
    ns: props.ns,
    worker: props.worker,
    doStorageId: props.doStorageId,
    className,
    objectName,
    token,
  });
}
