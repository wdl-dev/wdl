import {
  DoRuntimeError,
  isWellFormedUnicodeString,
  nonEmptyAlarmString,
} from "do-runtime-protocol";
import { log } from "do-runtime-state";
import { withInternalAuth } from "shared-internal-auth";
import {
  DO_ALARM_RESPONSE_DEADLINE_MS,
  parseDoAlarmMutationSuccess,
  readDoAlarmResponseText,
} from "shared-do-alarm-response";
import { formatError } from "shared-observability";
import { discardResponseBody } from "shared-respond";

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

/** @param {number | undefined} status */
function alarmBackendMutationError(status = undefined) {
  if (status !== undefined && status >= 400 && status < 500) {
    return new DoRuntimeError(503, "do_alarm_backend_failed", "DO alarm backend rejected the request");
  }
  return new DoRuntimeError(503, "do_alarm_result_unknown", "DO alarm backend result is unknown");
}

/**
 * @param {string} path
 * @param {Record<string, unknown>} body
 * @param {string} phase
 * @param {{ error?: unknown, upstreamStatus?: number }} [diagnostic]
 */
function logAlarmBackendMutationFailure(path, body, phase, diagnostic = {}) {
  log("warn", "do_alarm_backend_mutation_failed", {
    operation: path.endsWith("/set") ? "set" : "delete",
    namespace: body.ns,
    worker: body.worker,
    ...(typeof body.version === "string" ? { version: body.version } : {}),
    class_name: body.className,
    object_name: body.objectName,
    ...(typeof body.retryCount === "number" ? { retry_count: body.retryCount } : {}),
    phase,
    ...(diagnostic.upstreamStatus === undefined
      ? {}
      : { upstream_status: diagnostic.upstreamStatus }),
    ...(diagnostic.error === undefined ? {} : formatError(diagnostic.error)),
  });
}

/**
 * @param {DoEnv} env
 * @param {string} path
 * @param {Record<string, unknown>} body
 */
async function postWorkflowsAlarm(env, path, body) {
  const signal = AbortSignal.timeout(DO_ALARM_RESPONSE_DEADLINE_MS);
  let backend;
  let headers;
  let requestBody;
  try {
    backend = workflowsBackend(env);
    headers = withInternalAuth({ "content-type": "application/json" }, env);
    requestBody = JSON.stringify(body);
  } catch (err) {
    logAlarmBackendMutationFailure(path, body, "request_setup", { error: err });
    if (err instanceof DoRuntimeError) throw err;
    throw new DoRuntimeError(503, "do_alarm_backend_unavailable", "DO alarm backend is unavailable");
  }
  let response;
  try {
    response = await backend.fetch(`http://workflows${path}`, {
      method: "POST",
      headers,
      body: requestBody,
      signal,
    });
  } catch (err) {
    logAlarmBackendMutationFailure(path, body, "transport", { error: err });
    throw alarmBackendMutationError();
  }
  if (!response.ok) {
    logAlarmBackendMutationFailure(path, body, "response_status", {
      upstreamStatus: response.status,
    });
    void discardResponseBody(response);
    throw alarmBackendMutationError(response.status);
  }
  let text;
  try {
    text = await readDoAlarmResponseText(response, signal);
  } catch (err) {
    logAlarmBackendMutationFailure(path, body, "response_body", {
      error: err,
      upstreamStatus: response.status,
    });
    throw alarmBackendMutationError();
  }
  try {
    return parseDoAlarmMutationSuccess(text);
  } catch (err) {
    logAlarmBackendMutationFailure(path, body, "response_parse", {
      error: err,
      upstreamStatus: response.status,
    });
    throw alarmBackendMutationError();
  }
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
