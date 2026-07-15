import { DoRuntimeError, normalizeDoInvokeRequest, readJsonBody } from "do-runtime-protocol";
import { json } from "do-runtime-http";

/**
 * @typedef {import("do-runtime-protocol").DoInvoke} DoInvoke
 * @typedef {Record<string, unknown> & { DO_HOSTS: DurableObjectNamespace }} DoEnv
 * @typedef {(env: DoEnv, invoke: DoInvoke, requestId?: string | null) => Promise<Response>} AlarmDispatcher
 */

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
