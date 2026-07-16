// Worker-event dispatch helpers for runtime. This module owns loaded-worker
// fetch plus _scheduled/_queued body parsing, handler dispatch, tail
// invocation events, and outcome response shaping; runtime/index.js delegates
// worker-event routes here.

import { internalErrorResponse, jsonError, jsonResponse } from "shared-respond";
import { BodyTooLargeError, readBoundedText } from "shared-bounded-body";
import { withInternalAuthEntries } from "shared-internal-auth";
import { errorMessage } from "shared-errors";
import {
  decodeQueuedDispatchMessages,
  normalizeWorkflowNotifyBody,
  normalizeWorkflowRunBody,
  normalizeQueuedDispatchBody,
  normalizeScheduledDispatchBody,
} from "runtime-lib";
import {
  emitRuntimeTailEvent,
  fetchTailFields,
  startTailEnvelope,
} from "runtime-tail-forwarder";
import {
  _stringifyWorkflowBackendBodyForTest as stringifyWorkflowBackendBodyForTest,
  _stringifyWorkflowJsonForTest as stringifyWorkflowJsonForTest,
  workflowJsonResponse,
  workflowStepError,
} from "runtime-dispatch-workflow-json";
export { _resetWorkflowReplayCacheForTest } from "runtime-dispatch-workflow-replay-cache";
import {
  createStepController,
  isWorkflowSuspensionSignal,
  isWorkflowSuspended,
  workflowError,
} from "runtime-dispatch-workflow-step";
export const _stringifyWorkflowBackendBodyForTest = stringifyWorkflowBackendBodyForTest;
export const _stringifyWorkflowJsonForTest = stringifyWorkflowJsonForTest;

/**
 * @typedef {{ respond(response: Response): Response, markError(err: unknown): void, requestId: string }} DispatchScope
 * @typedef {{ fetch(request: Request): Promise<Response>, scheduled?(controller: unknown): Promise<unknown>, queue?(queueName: string, messages: unknown[]): Promise<unknown>, run?(event: unknown, step: unknown): Promise<unknown> }} LoadedEntrypoint
 * @typedef {{ getEntrypoint(name?: string): LoadedEntrypoint }} LoadedWorkerStub
 * @typedef {{ namespace: string, workerName: string, workerId: string, requestId: string | null }} RuntimeIdentity
 * @typedef {{ waitUntil?(promise: Promise<unknown>): void }} RuntimeCtx
 * @typedef {{ WORKFLOWS_BACKEND?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> } | null, WDL_INTERNAL_AUTH_TOKEN?: unknown, [key: string]: unknown }} RuntimeEnv
 * @typedef {{ ns: string, worker: string, frozenVersion: string, workflowName: string, workflowKey: string, className: string, instanceId: string, generation: number, runToken: string, createdAtMs: number, event: unknown }} WorkflowRunDispatch
 * @typedef {{ request: Request, stub: LoadedWorkerStub, scope: DispatchScope, env: RuntimeEnv, ctx: RuntimeCtx, identity: RuntimeIdentity }} WorkerDispatchArgs
 */

const SMALL_DISPATCH_JSON_BODY_BYTES = 256 * 1024;
// Workflow run dispatch carries instance params, capped by Rust's
// MAX_WORKFLOW_PARAMS_BYTES (1MiB), plus identity framing. Event notify uses a
// separate smaller payload cap and stays on SMALL_DISPATCH_JSON_BODY_BYTES.
const WORKFLOW_RUN_DISPATCH_JSON_BODY_BYTES = 2 * 1024 * 1024;
// Queue dispatch bodies carry base64-encoded message bodies. The scheduler may
// dispatch up to 100 messages, each with a 128KB raw body, so this private
// endpoint needs a larger cap than control-ish scheduled/workflow notify bodies.
const QUEUE_DISPATCH_JSON_BODY_BYTES = 20 * 1024 * 1024;

/** @param {RuntimeEnv} env */
function workflowBackendForStep(env) {
  const backend = env?.WORKFLOWS_BACKEND;
  if (!backend || typeof backend.fetch !== "function") return backend;
  return {
    async fetch(
      /** @type {RequestInfo | URL} */ input,
      /** @type {RequestInit} */ init = {}
    ) {
      return await backend.fetch(input, {
        ...init,
        headers: withInternalAuthEntries(init?.headers, env),
      });
    },
  };
}

/** @param {unknown} result */
function queueDispatchResult(result) {
  const record = result && typeof result === "object"
    ? /** @type {Record<string, unknown>} */ (result)
    : {};
  /** @type {Record<string, unknown>} */
  const out = {};
  if (typeof record.outcome === "string") out.outcome = record.outcome;
  if (typeof record.ackAll === "boolean") out.ackAll = record.ackAll;
  if (Array.isArray(record.explicitAcks)) out.explicitAcks = record.explicitAcks;
  if (Array.isArray(record.retryMessages)) out.retryMessages = record.retryMessages;
  if (record.retryBatch && typeof record.retryBatch === "object") out.retryBatch = record.retryBatch;
  return out;
}

/** @param {Request} request @param {number} maxBytes */
async function readJsonBody(request, maxBytes) {
  try {
    return { body: JSON.parse(await readBoundedText(request, maxBytes)) };
  } catch (err) {
    if (err instanceof BodyTooLargeError) {
      return {
        response: jsonError(
          413,
          "request_body_too_large",
          `Body exceeds ${maxBytes} byte limit`
        ),
      };
    }
    return {
      response: jsonError(400, "invalid_json", "Body must be valid JSON"),
    };
  }
}

/** @param {{ scheduledTime: number, cron: string }} scheduled */
function scheduledTailFields(scheduled) {
  return {
    scheduled_time: scheduled.scheduledTime,
    cron: scheduled.cron,
  };
}

/** @param {{ queueName: string }} queued @param {number} batchSize */
function queueTailFields(queued, batchSize) {
  return {
    queue: queued.queueName,
    batch_size: batchSize,
  };
}

/** @param {{ workflowName: string, workflowKey: string, className: string, instanceId: string, generation: number }} run */
function workflowTailFields(run) {
  return {
    workflow: run.workflowName,
    workflow_key: run.workflowKey,
    workflow_class: run.className,
    instance_id: run.instanceId,
    generation: run.generation,
  };
}

/** @param {WorkerDispatchArgs} args */
export async function handleFetchDispatch({ request, stub, scope, env, ctx, identity }) {
  const tail = startTailEnvelope({
    env, ctx, identity,
    event: "worker_fetch",
    fields: fetchTailFields(request),
  });
  try {
    const response = await stub.getEntrypoint().fetch(request);
    tail.finish({
      outcome: "ok",
      status: response.status,
    });
    return scope.respond(response);
  } catch (err) {
    scope.markError(err);
    tail.finishError(err);
    return scope.respond(internalErrorResponse(502, "runtime_error", "Runtime error", scope.requestId));
  }
}

/** @param {Request} request */
export async function readWorkflowRunDispatch(request) {
  const parsed = await readJsonBody(request, WORKFLOW_RUN_DISPATCH_JSON_BODY_BYTES);
  if (parsed.response) return parsed;
  try {
    return { body: normalizeWorkflowRunBody(parsed.body) };
  } catch (err) {
    return {
      response: jsonError(400, "invalid_workflow_run_body", errorMessage(err)),
    };
  }
}

/** @param {Request} request */
export async function readWorkflowNotifyDispatch(request) {
  const parsed = await readJsonBody(request, SMALL_DISPATCH_JSON_BODY_BYTES);
  if (parsed.response) return parsed;
  try {
    return { body: normalizeWorkflowNotifyBody(parsed.body) };
  } catch (err) {
    return {
      response: jsonError(400, "invalid_workflow_notify_body", errorMessage(err)),
    };
  }
}

/** @param {{ run: WorkflowRunDispatch, stub: LoadedWorkerStub, scope: DispatchScope, env: RuntimeEnv, ctx: RuntimeCtx, identity: RuntimeIdentity }} args */
export async function handleWorkflowRunDispatch({ run, stub, scope, env, ctx, identity }) {
  const startedAt = Date.now();
  const fields = workflowTailFields(run);
  const startTailEvent = emitRuntimeTailEvent({
    env, ctx, identity,
    event: "worker_workflow",
    phase: "start",
    fields,
  });
  let step = null;
  try {
    const entry = stub.getEntrypoint(run.className);
    step = createStepController(run, workflowBackendForStep(env), scope.requestId);
    if (typeof entry.run !== "function") {
      throw workflowStepError("workflow_invalid_step", `workflow class ${run.className} does not expose run()`);
    }
    const output = await entry.run(run.event, step.facade);
    if (step.hasInFlightSteps()) {
      step.closeForRunReturn();
      throw workflowStepError("workflow_invalid_step", "workflow run returned while workflow steps were still in flight");
    }
    if (step.hasTerminalStepFailure()) throw step.terminalStepFailure();
    if (step.isSuspended()) {
      throw workflowStepError("workflow_invalid_step", "workflow run returned after a step suspension was registered");
    }
    const durationMs = Date.now() - startedAt;
    const response = workflowJsonResponse(
      200,
      "{\"outcome\":\"completed\",",
      "output",
      output ?? null,
      "output",
      durationMs
    );
    emitRuntimeTailEvent({
      env, ctx, identity,
      event: "worker_workflow",
      phase: "finish",
      after: startTailEvent,
      fields: {
        ...fields,
        outcome: "completed",
        duration_ms: durationMs,
      },
    });
    return scope.respond(response);
  } catch (err) {
    let caught = err;
    const caughtSuspended = Boolean(
      step?.isSuspended() && isWorkflowSuspensionSignal(caught)
    );
    if (step?.hasInFlightSteps() && !caughtSuspended) {
      step.closeForRunReturn();
    }
    if (step?.hasInFlightSteps() && caughtSuspended) {
      const settled = await step.waitForInFlightSteps();
      const terminalFailure = settled.find((result) => (
        result.status === "rejected" && !isWorkflowSuspended(result.reason)
      ));
      if (terminalFailure?.status === "rejected") caught = terminalFailure.reason;
    }
    let terminalStepError = null;
    if (step?.hasTerminalStepFailure()) {
      caught = step.terminalStepFailure();
      terminalStepError = step.terminalStepError();
    }
    if (
      !terminalStepError &&
      step?.isSuspended() &&
      isWorkflowSuspensionSignal(caught)
    ) {
      const durationMs = Date.now() - startedAt;
      emitRuntimeTailEvent({
        env, ctx, identity,
        event: "worker_workflow",
        phase: "finish",
        after: startTailEvent,
        fields: {
          ...fields,
          outcome: "suspended",
          duration_ms: durationMs,
        },
      });
      return scope.respond(jsonResponse(200, {
        outcome: "suspended",
        duration_ms: durationMs,
      }));
    }
    const durationMs = Date.now() - startedAt;
    const error = terminalStepError ?? workflowError(caught);
    scope.markError(caught);
    emitRuntimeTailEvent({
      env, ctx, identity,
      event: "worker_workflow",
      phase: "finish",
      after: startTailEvent,
      fields: {
        ...fields,
        outcome: "failed",
        error: error.message,
        duration_ms: durationMs,
      },
    });
    let response;
    try {
      response = workflowJsonResponse(
        200,
        "{\"outcome\":\"failed\",",
        "error",
        error,
        "error",
        durationMs
      );
    } catch (serializeErr) {
      response = workflowJsonResponse(
        200,
        "{\"outcome\":\"failed\",",
        "error",
        workflowError(serializeErr),
        "error",
        durationMs
      );
    }
    return scope.respond(response);
  }
}

/** @param {{ notify: unknown, stub: LoadedWorkerStub, scope: DispatchScope }} args */
export async function handleWorkflowNotifyDispatch({ notify, stub, scope }) {
  try {
    const response = await stub.getEntrypoint("__WdlWorkflowNotify__").fetch(new Request("https://runtime.local/internal/workflows/notify", {
      method: "POST",
      headers: { "content-type": "application/json", "x-request-id": scope.requestId },
      body: JSON.stringify(notify),
    }));
    if (!response.ok) {
      return scope.respond(jsonError(502, "workflow_callback_failed", "Workflow callback failed"));
    }
    return scope.respond(new Response(null, { status: 204 }));
  } catch (err) {
    scope.markError(err);
    return scope.respond(internalErrorResponse(502, "workflow_callback_failed", "Workflow callback failed", scope.requestId));
  }
}

/** @param {WorkerDispatchArgs} args */
export async function handleScheduledDispatch({ request, stub, scope, env, ctx, identity }) {
  const parsed = await readJsonBody(request, SMALL_DISPATCH_JSON_BODY_BYTES);
  if (parsed.response) return scope.respond(parsed.response);

  let scheduled;
  try {
    scheduled = normalizeScheduledDispatchBody(parsed.body);
  } catch (err) {
    return scope.respond(jsonError(400, "invalid_scheduled_body", errorMessage(err)));
  }

  const tail = startTailEnvelope({
    env, ctx, identity,
    event: "worker_scheduled",
    fields: scheduledTailFields(scheduled),
  });
  try {
    // Handler failures must surface as 200 outcome:"error" so cron dispatch
    // never looks transient and triggers retry semantics Cloudflare forbids.
    // service_binding_extra_handlers may return outcome:"exception" instead
    // of throwing, so keep both branches aligned.
    const entry = stub.getEntrypoint();
    if (typeof entry.scheduled !== "function") {
      throw new Error("worker does not expose scheduled()");
    }
    const scheduledResult = await entry.scheduled(scheduled);
    const scheduledRecord = scheduledResult && typeof scheduledResult === "object"
      ? /** @type {Record<string, unknown>} */ (scheduledResult)
      : {};
    if (scheduledRecord.outcome === "exception") {
      const message = typeof scheduledRecord.error === "string"
        ? scheduledRecord.error
        : "scheduled handler threw";
      scope.markError(message);
      const durationMs = tail.finish({
        outcome: "error",
        error: message,
      });
      return scope.respond(jsonResponse(200, {
        outcome: "error",
        error: message,
        duration_ms: durationMs,
      }));
    }
    const durationMs = tail.finish({
      outcome: "ok",
    });
    return scope.respond(jsonResponse(200, {
      outcome: "ok",
      duration_ms: durationMs,
    }));
  } catch (err) {
    scope.markError(err);
    const message = errorMessage(err);
    const durationMs = tail.finish({
      outcome: "error",
      error: message,
    });
    return scope.respond(jsonResponse(200, {
      outcome: "error",
      error: message,
      duration_ms: durationMs,
    }));
  }
}

/** @param {WorkerDispatchArgs} args */
export async function handleQueuedDispatch({ request, stub, scope, env, ctx, identity }) {
  const parsed = await readJsonBody(request, QUEUE_DISPATCH_JSON_BODY_BYTES);
  if (parsed.response) return scope.respond(parsed.response);

  let queued;
  try {
    queued = normalizeQueuedDispatchBody(parsed.body);
  } catch (err) {
    return scope.respond(jsonError(400, "invalid_queue_body", errorMessage(err)));
  }

  let decoded;
  try {
    // Match workerd's native queue dispatch: Object / String / Uint8Array
    // cross the isolate boundary unchanged after Redis wire decoding.
    decoded = decodeQueuedDispatchMessages(queued.messages);
  } catch (err) {
    return scope.respond(jsonError(
      400,
      "queue_message_decode_failed",
      `queue message decode failed: ${errorMessage(err)}`
    ));
  }

  const tail = startTailEnvelope({
    env, ctx, identity,
    event: "worker_queue",
    fields: queueTailFields(queued, decoded.length),
  });
  try {
    const entry = stub.getEntrypoint();
    if (typeof entry.queue !== "function") {
      throw new Error("worker does not expose queue()");
    }
    const resp = await entry.queue(queued.queueName, decoded);
    const result = queueDispatchResult(resp);
    const handlerFailed = result.outcome === "exception";
    const message = "queue handler threw";
    if (handlerFailed) scope.markError(message);
    const durationMs = tail.finish({
      outcome: handlerFailed ? "error" : "ok",
      ...(handlerFailed ? { error: message } : {}),
    });
    return scope.respond(jsonResponse(200, {
      outcome: "ok",
      result,
      duration_ms: durationMs,
    }));
  } catch (err) {
    scope.markError(err);
    const message = errorMessage(err);
    const durationMs = tail.finish({
      outcome: "error",
      error: message,
    });
    return scope.respond(jsonResponse(200, {
      outcome: "error",
      error: message,
      duration_ms: durationMs,
    }));
  }
}
