import { errorMessage } from "shared-errors";
import { readBoundedBytes } from "shared-bounded-body";
import { discardResponseBody } from "shared-respond";
import { ControlAbort } from "control-errors";

export const WORKFLOWS_INTERNAL_TIMEOUT_MS = 5_000;
export const MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES = 8 * 1024 * 1024;
export const WORKFLOW_LIFECYCLE_MAX_PAGES = 16;
export const WORKFLOW_LIFECYCLE_TIMEOUT_MS = 10_000;
export const WORKFLOW_LIFECYCLE_RESPONSE_MAX_BYTES = 64 * 1024;

const workflowResponseDecoder = new TextDecoder("utf-8", { fatal: true });

/** @param {Response} response @param {number} maxBytes @param {AbortSignal} [signal] */
async function readWorkflowResponse(response, maxBytes, signal) {
  try {
    const bytes = await readBoundedBytes(response, maxBytes, signal);
    return { body: JSON.parse(workflowResponseDecoder.decode(bytes)), bytes };
  } catch (err) {
    await discardResponseBody(response);
    throw err;
  }
}

/** @param {Response} response @param {AbortSignal} [signal] */
export async function readWorkflowInstancesResponse(response, signal) {
  return await readWorkflowResponse(response, MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES, signal);
}

/** @param {Response} response @param {AbortSignal} [signal] */
export async function readWorkflowLifecycleResponse(response, signal) {
  return (await readWorkflowResponse(response, WORKFLOW_LIFECYCLE_RESPONSE_MAX_BYTES, signal)).body;
}

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
 *   requestId?: string | null,
 *   log?: WorkflowClientLogger | null,
 *   logEvent: string,
 *   logFields?: Record<string, unknown>,
 *   timeoutMs: number | null,
 *   makeError: (failure: WorkflowTransportFailure) => Error,
 *   readBody?: (response: Response, signal?: AbortSignal) => Promise<unknown>,
 * }} args
 * @returns {Promise<{ response: Response, body: unknown }>}
 */
export async function postWorkflowsInternalRequest({
  workflows,
  headers,
  endpoint,
  body,
  requestId = null,
  log = null,
  logEvent,
  logFields = {},
  timeoutMs,
  makeError,
  readBody = async (response) => await response.json().catch(() => null),
}) {
  if (!workflows || typeof workflows.fetch !== "function") {
    throw makeError("unavailable");
  }

  try {
    const requestHeaders = new Headers(headers());
    if (typeof requestId === "string" && requestId) {
      requestHeaders.set("x-request-id", requestId);
    }
    const deadline = timeoutMs === null ? null : Date.now() + timeoutMs;
    const signal = timeoutMs === null ? undefined : AbortSignal.timeout(timeoutMs);
    const response = await workflows.fetch(`http://workflows/internal/${endpoint}`, {
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(body),
      ...(signal === undefined ? {} : { signal }),
    });
    const responseBody = await readBody(response, signal);
    signal?.throwIfAborted();
    if (deadline !== null && Date.now() >= deadline) {
      throw new DOMException("Workflow backend request timed out", "TimeoutError");
    }
    return {
      response,
      body: responseBody,
    };
  } catch (err) {
    log?.("error", logEvent, {
      ...logFields,
      ...(typeof requestId === "string" && requestId ? { request_id: requestId } : {}),
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
   *   requestId?: string | null,
   *   logEvent: string,
   *   logFields?: Record<string, unknown>,
   *   errorDetails?: Record<string, unknown>,
   *   timeoutMs: number | null,
   *   unavailableMessage?: string,
   *   requestFailedMessage?: string,
   *   readBody?: (response: Response, signal?: AbortSignal) => Promise<unknown>,
   * }} args
   */
  return async function postWorkflowsInternal({
    endpoint,
    body,
    requestId = null,
    logEvent,
    logFields = {},
    errorDetails = {},
    timeoutMs,
    unavailableMessage = "Workflow backend is unavailable",
    requestFailedMessage = "Workflow backend request failed",
    readBody,
  }) {
    return await postWorkflowsInternalRequest({
      workflows: getWorkflows(),
      headers,
      endpoint,
      body,
      requestId,
      log: getLog(),
      logEvent,
      logFields,
      timeoutMs,
      readBody,
      makeError: (failure) => new ControlAbort(503, "workflow_internal_dispatch_failed", {
        ...errorDetails,
        message: failure === "unavailable"
          ? unavailableMessage
          : requestFailedMessage,
      }),
    });
  };
}
