import { formatWorkerId } from "shared-worker-id";
import { parseVersion } from "shared-version";
import { DoRuntimeError, hostIdForObject, nonEmptyAlarmString, readJsonBody } from "do-runtime-protocol";
import { json } from "do-runtime-http";

/**
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {Record<string, unknown> & { DO_HOSTS: DurableObjectNamespace }} DoEnv
 * @typedef {(env: DoEnv, invoke: DoInvoke, requestId?: string | null) => Promise<Response>} AlarmDispatcher
 */

/**
 * @param {unknown} value
 * @param {string} field
 */
function requiredAlarmString(value, field) {
  const text = nonEmptyAlarmString(value);
  if (text === null) {
    throw new DoRuntimeError(400, "invalid_do_alarm_dispatch", `DO alarm ${field} must be a non-empty string`);
  }
  return text;
}

/** @param {unknown} value */
function requiredAlarmVersion(value) {
  const version = requiredAlarmString(value, "version");
  if (parseVersion(version) == null) {
    throw new DoRuntimeError(400, "invalid_do_alarm_dispatch", "DO alarm version is invalid");
  }
  return version;
}

/** @param {unknown} value */
function alarmRetryCount(value) {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new DoRuntimeError(400, "invalid_do_alarm_dispatch", "DO alarm retryCount must be a non-negative integer");
  }
  return value;
}

/**
 * @param {Request} request
 * @param {DoEnv} env
 * @param {AlarmDispatcher} dispatchInvoke
 * @param {string | null} [requestId]
 */
export async function handleAlarmDispatch(request, env, dispatchInvoke, requestId = null) {
  const body = await readJsonBody(request);
  const record = body && typeof body === "object" && !Array.isArray(body)
    ? /** @type {Record<string, unknown>} */ (body)
    : {};
  const ns = requiredAlarmString(record.ns, "ns");
  const worker = requiredAlarmString(record.worker, "worker");
  const version = requiredAlarmVersion(record.version);
  const doStorageId = requiredAlarmString(record.doStorageId, "doStorageId");
  const className = requiredAlarmString(record.className, "className");
  const objectName = requiredAlarmString(record.objectName, "objectName");
  const retryCount = alarmRetryCount(record.retryCount);
  const token = requiredAlarmString(record.token, "token");
  /** @type {DoInvoke} */
  const invoke = {
    kind: "alarm",
    ns,
    worker,
    version,
    doStorageId,
    workerId: formatWorkerId({ namespace: ns, worker, version }),
    hostId: hostIdForObject(doStorageId, className, objectName),
    className,
    objectName,
    props: {
      ns,
      worker,
      version,
      doStorageId,
      className,
    },
    alarm: {
      retryCount,
      isRetry: retryCount > 0,
      token,
    },
  };
  const response = await dispatchInvoke(env, invoke, requestId);
  const text = await response.text();
  if (!response.ok) {
    throw new DoRuntimeError(503, "do_alarm_dispatch_failed", "DO alarm dispatch failed", {
      upstream_status: response.status,
      upstream_body: text.slice(0, 1024),
    });
  }
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {}
  return json({
    ok: true,
    ignored: parsed && typeof parsed === "object" && /** @type {{ ignored?: unknown }} */ (parsed).ignored === true,
  });
}
