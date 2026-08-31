import { DoRuntimeError, normalizeDoInvokeRequest, readJsonBody } from "do-runtime-protocol";
import { json } from "do-runtime-http";
import {
  DO_ALARM_RESPONSE_DEADLINE_MS,
  parseDoAlarmDispatchSuccess,
  readDoAlarmResponseText,
} from "shared-do-alarm-response";
import { ownerHintFromHeaders } from "runtime-do-transport";

/**
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {Record<string, unknown> & { DO_HOSTS: DurableObjectNamespace }} DoEnv
 * @typedef {(env: DoEnv, invoke: DoInvoke, requestId?: string | null, hopCount?: number, onDispatchStart?: () => void) => Promise<Response>} AlarmDispatcher
 */

function alarmDispatchResultUnknown() {
  return new DoRuntimeError(
    503,
    "do_alarm_dispatch_result_unknown",
    "DO alarm dispatch result is unknown"
  );
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
  if (record.retryCount == null) {
    throw new DoRuntimeError(400, "invalid_request", "alarm.retryCount is required");
  }
  const invoke = normalizeDoInvokeRequest({
    kind: "alarm",
    ns: record.ns,
    worker: record.worker,
    version: record.version,
    doStorageId: record.doStorageId,
    className: record.className,
    objectName: record.objectName,
    alarm: {
      retryCount: record.retryCount,
      token: record.token,
    },
  });
  if (invoke.kind !== "alarm" || invoke.alarm.token == null) {
    throw new DoRuntimeError(400, "invalid_request", "alarm.token is required");
  }
  let dispatchStarted = false;
  let response;
  try {
    response = await dispatchInvoke(
      env,
      invoke,
      requestId,
      0,
      () => { dispatchStarted = true; }
    );
  } catch (error) {
    if (dispatchStarted) {
      throw alarmDispatchResultUnknown();
    }
    throw error;
  }
  let text;
  try {
    text = await readDoAlarmResponseText(
      response,
      AbortSignal.timeout(DO_ALARM_RESPONSE_DEADLINE_MS)
    );
  } catch {
    throw alarmDispatchResultUnknown();
  }
  if (!response.ok) {
    const responseOwner = ownerHintFromHeaders(response.headers);
    if (responseOwner?.ownerKey !== invoke.hostId) {
      throw alarmDispatchResultUnknown();
    }
    throw new DoRuntimeError(503, "do_alarm_dispatch_failed", "DO alarm dispatch failed", {
      upstream_status: response.status,
      upstream_body: text.slice(0, 1024),
    });
  }
  let parsed;
  try {
    parsed = parseDoAlarmDispatchSuccess(text);
  } catch {
    throw alarmDispatchResultUnknown();
  }
  return json({ ok: true, ignored: parsed.ignored });
}
