import { errorMessage } from "shared-errors";
import { ControlAbort } from "control-errors";

export const WORKFLOWS_INTERNAL_TIMEOUT_MS = 5_000;

/**
 * @typedef {{ fetch: typeof fetch }} WorkflowBackend
 * @typedef {(level: string, event: string, fields?: Record<string, unknown>) => void} WorkflowClientLogger
 * @typedef {"unavailable" | "request_failed"} WorkflowTransportFailure
 */

/**
 * @param {{
 *   workflows: WorkflowBackend | null | undefined,
 *   headers: () => HeadersInit,
 *   endpoint: string,
 *   body: unknown,
 *   log?: WorkflowClientLogger | null,
 *   logEvent: string,
 *   logFields?: Record<string, unknown>,
 *   timeoutMs: number | null,
 *   makeError: (failure: WorkflowTransportFailure) => Error,
 * }} args
 * @returns {Promise<{ response: Response, body: unknown }>}
 */
export async function postWorkflowsInternalRequest({
  workflows,
  headers,
  endpoint,
  body,
  log = null,
  logEvent,
  logFields = {},
  timeoutMs,
  makeError,
}) {
  if (!workflows || typeof workflows.fetch !== "function") {
    throw makeError("unavailable");
  }

  try {
    const response = await workflows.fetch(`http://workflows/internal/${endpoint}`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify(body),
      ...(timeoutMs === null ? {} : { signal: AbortSignal.timeout(timeoutMs) }),
    });
    return {
      response,
      body: await response.json().catch(() => null),
    };
  } catch (err) {
    log?.("error", logEvent, {
      ...logFields,
      error_message: errorMessage(err),
    });
    throw makeError("request_failed");
  }
}

/**
 * @param {{
 *   getWorkflows: () => WorkflowBackend | null | undefined,
 *   headers: () => HeadersInit,
 *   getLog?: () => WorkflowClientLogger | null | undefined,
 * }} dependencies
 */
export function createPostWorkflowsInternal({ getWorkflows, headers, getLog = () => null }) {
  /**
   * @param {{
   *   endpoint: string,
   *   body: unknown,
   *   logEvent: string,
   *   logFields?: Record<string, unknown>,
   *   errorDetails?: Record<string, unknown>,
   *   timeoutMs: number | null,
   *   unavailableMessage?: string,
   *   requestFailedMessage?: string,
   * }} args
   */
  return async function postWorkflowsInternal({
    endpoint,
    body,
    logEvent,
    logFields = {},
    errorDetails = {},
    timeoutMs,
    unavailableMessage = "Workflow backend is unavailable",
    requestFailedMessage = "Workflow backend request failed",
  }) {
    return await postWorkflowsInternalRequest({
      workflows: getWorkflows(),
      headers,
      endpoint,
      body,
      log: getLog(),
      logEvent,
      logFields,
      timeoutMs,
      makeError: (failure) => new ControlAbort(503, "workflow_internal_dispatch_failed", {
        ...errorDetails,
        message: failure === "unavailable"
          ? unavailableMessage
          : requestFailedMessage,
      }),
    });
  };
}
