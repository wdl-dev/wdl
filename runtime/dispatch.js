// Worker-event dispatch helpers for runtime. This module owns loaded-worker
// fetch plus _scheduled/_queued body parsing, handler dispatch, tail
// invocation events, and outcome response shaping; runtime/index.js delegates
// worker-event routes here.

import { WorkerEntrypoint } from "cloudflare:workers";
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
  workflowJsonResponse,
  workflowStepError,
} from "runtime-dispatch-workflow-json";
import {
  createStepController,
  isWorkflowInfrastructureError,
  isWorkflowSuspensionSignal,
  WORKFLOW_BACKEND_UNAVAILABLE_CODE,
  WORKFLOW_BACKEND_UNAVAILABLE_MESSAGE,
  workflowError,
  workflowInFlightSettleTimeoutError,
  workflowInfrastructureError,
  workflowInfrastructureLogError,
} from "runtime-dispatch-workflow-step";
import {
  KV_READ_INFRASTRUCTURE_ERROR_CODE,
  WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
} from "runtime-infrastructure-error";
import { WORKFLOW_INFRASTRUCTURE_REPORTER_PROP } from "runtime-load-module-rewrite";
/**
 * @typedef {{ respond(response: Response): Response, markError(err: unknown): void, requestId: string }} DispatchScope
 * @typedef {{ fetch(request: Request): Promise<Response>, scheduled?(controller: unknown): Promise<unknown>, queue?(queueName: string, messages: unknown[]): Promise<unknown>, run?(event: unknown, step: unknown): Promise<unknown> }} LoadedEntrypoint
 * @typedef {{ getEntrypoint(name?: string, options?: { props?: Record<string, unknown> }): LoadedEntrypoint }} LoadedWorkerStub
 * @typedef {{ namespace: string, workerName: string, workerId: string, requestId: string | null }} RuntimeIdentity
 * @typedef {{ waitUntil?(promise: Promise<unknown>): void, exports?: unknown }} RuntimeCtx
 * @typedef {{ WORKFLOWS_BACKEND?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> } | null, WDL_INTERNAL_AUTH_TOKEN?: unknown, [key: string]: unknown }} RuntimeEnv
 * @typedef {{ ns: string, worker: string, frozenVersion: string, workflowName: string, workflowKey: string, className: string, instanceId: string, generation: number, runToken: string, createdAtMs: number, dispatchDeadlineMs: number, event: unknown }} WorkflowRunDispatch
 * @typedef {{ request: Request, stub: LoadedWorkerStub, scope: DispatchScope, env: RuntimeEnv, ctx: RuntimeCtx, identity: RuntimeIdentity }} WorkerDispatchArgs
 */

const SMALL_DISPATCH_JSON_BODY_BYTES = 256 * 1024;
// Workflow run dispatch carries instance params, capped by Rust's
// MAX_WORKFLOW_PARAMS_BYTES (1MiB), plus identity framing. Event notify uses a
// separate smaller payload cap and stays on SMALL_DISPATCH_JSON_BODY_BYTES.
const WORKFLOW_RUN_DISPATCH_JSON_BODY_BYTES = 2 * 1024 * 1024;
const WORKFLOW_DISPATCH_RESPONSE_HEADROOM_MS = 1000;
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

/** @param {number} deadlineMs @param {string} diagnostic */
function assertWorkflowDispatchDeadline(deadlineMs, diagnostic) {
  if (Date.now() >= deadlineMs) {
    throw workflowInfrastructureError(diagnostic);
  }
}

/**
 * @template T
 * @param {() => T | Promise<T>} invoke
 * @param {number} deadlineMs
 * @returns {Promise<T>}
 */
async function runWorkflowBeforeDeadline(invoke, deadlineMs) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  const deadline = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(workflowInfrastructureError(
      "Workflow dispatch deadline expired before terminal outcome"
    )), Math.max(0, deadlineMs - Date.now()));
  });
  try {
    return await Promise.race([invoke(), deadline]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/** @type {Map<string, { reported: boolean }>} */
const workflowInfrastructureReports = new Map();

/** @param {unknown} props @param {unknown} code */
function recordWorkflowInfrastructureReport(props, code) {
  if (code !== KV_READ_INFRASTRUCTURE_ERROR_CODE) {
    throw new TypeError("Workflow infrastructure report code is invalid");
  }
  const reportId = /** @type {Record<string, unknown> | null | undefined} */ (props)?.reportId;
  const state = typeof reportId === "string"
    ? workflowInfrastructureReports.get(reportId)
    : undefined;
  if (!state) {
    throw new TypeError("Workflow infrastructure report is closed or invalid");
  }
  state.reported = true;
}

export class WorkflowInfrastructureReporter extends WorkerEntrypoint {
  /** @param {Request} request */
  fetch(request) {
    const url = new URL(request.url);
    if (
      request.method !== "GET" ||
      url.origin !== WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN ||
      url.pathname !== `/${KV_READ_INFRASTRUCTURE_ERROR_CODE}`
    ) {
      throw new TypeError("Workflow infrastructure report request is invalid");
    }
    recordWorkflowInfrastructureReport(this.ctx.props, KV_READ_INFRASTRUCTURE_ERROR_CODE);
    return new Response(null, { status: 204 });
  }
}

function beginWorkflowInfrastructureReport() {
  let id;
  do {
    id = crypto.randomUUID();
  } while (workflowInfrastructureReports.has(id));
  const state = { reported: false };
  workflowInfrastructureReports.set(id, state);
  return {
    id,
    reported: () => state.reported,
    close: () => workflowInfrastructureReports.delete(id),
  };
}

/** @param {unknown} result */
function queueDispatchResult(result) {
  if (
    result === null ||
    typeof result !== "object" ||
    Array.isArray(result)
  ) {
    throw new TypeError("queue handler returned an invalid result envelope");
  }
  const record = /** @type {Record<string, unknown>} */ (result);
  /** @type {Record<string, unknown>} */
  const out = {};
  if (!Object.hasOwn(record, "outcome") || typeof record.outcome !== "string") {
    throw new TypeError("queue handler returned an invalid outcome");
  }
  out.outcome = record.outcome;
  if (Object.hasOwn(record, "ackAll")) {
    if (typeof record.ackAll !== "boolean") {
      throw new TypeError("queue handler returned an invalid ackAll value");
    }
    out.ackAll = record.ackAll;
  }
  if (Object.hasOwn(record, "explicitAcks")) {
    if (!Array.isArray(record.explicitAcks)) {
      throw new TypeError("queue handler returned invalid explicitAcks");
    }
    out.explicitAcks = record.explicitAcks;
  }
  if (Object.hasOwn(record, "retryMessages")) {
    if (!Array.isArray(record.retryMessages)) {
      throw new TypeError("queue handler returned invalid retryMessages");
    }
    out.retryMessages = record.retryMessages;
  }
  if (Object.hasOwn(record, "retryBatch")) {
    if (
      record.retryBatch === null ||
      typeof record.retryBatch !== "object" ||
      Array.isArray(record.retryBatch)
    ) {
      throw new TypeError("queue handler returned an invalid retryBatch");
    }
    out.retryBatch = record.retryBatch;
  }
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
    fields: () => fetchTailFields(request),
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
  const siblingSettleDeadlineMs = run.dispatchDeadlineMs - WORKFLOW_DISPATCH_RESPONSE_HEADROOM_MS;
  const fields = workflowTailFields(run);
  const startTailEvent = emitRuntimeTailEvent({
    env, ctx, identity,
    event: "worker_workflow",
    phase: "start",
    fields,
  });
  const infrastructureReport = beginWorkflowInfrastructureReport();
  /** @type {ReturnType<typeof createStepController> | null} */
  let step = null;
  /** @param {unknown} error */
  const finishInfrastructureFailure = (error) => {
    const durationMs = Date.now() - startedAt;
    scope.markError(workflowInfrastructureLogError(error));
    emitRuntimeTailEvent({
      env, ctx, identity,
      event: "worker_workflow",
      phase: "finish",
      after: startTailEvent,
      fields: {
        ...fields,
        outcome: "error",
        error: WORKFLOW_BACKEND_UNAVAILABLE_MESSAGE,
        duration_ms: durationMs,
      },
    });
    return scope.respond(internalErrorResponse(
      503,
      WORKFLOW_BACKEND_UNAVAILABLE_CODE,
      WORKFLOW_BACKEND_UNAVAILABLE_MESSAGE,
      scope.requestId
    ));
  };
  const finishDeadlineFailure = () => finishInfrastructureFailure(
    workflowInfrastructureError(
      "Workflow dispatch deadline expired before terminal outcome"
    )
  );
  try {
    assertWorkflowDispatchDeadline(
      run.dispatchDeadlineMs,
      "Workflow dispatch deadline expired before tenant execution"
    );
    const runtimeExports = /** @type {{ WorkflowInfrastructureReporter?: (options: { props: { reportId: string } }) => unknown }} */ (
      ctx.exports
    );
    if (typeof runtimeExports?.WorkflowInfrastructureReporter !== "function") {
      throw workflowInfrastructureError(
        "Workflow infrastructure reporter binding is unavailable"
      );
    }
    const reporter = runtimeExports.WorkflowInfrastructureReporter({
      props: { reportId: infrastructureReport.id },
    });
    const entry = stub.getEntrypoint(run.className, {
      props: {
        [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: reporter,
      },
    });
    const stepController = createStepController(
      run,
      workflowBackendForStep(env),
      scope.requestId,
      infrastructureReport.reported
    );
    step = stepController;
    const runEntrypoint = entry.run;
    if (typeof runEntrypoint !== "function") {
      throw workflowStepError("workflow_invalid_step", `workflow class ${run.className} does not expose run()`);
    }
    assertWorkflowDispatchDeadline(
      run.dispatchDeadlineMs,
      "Workflow dispatch deadline expired before tenant execution"
    );
    // Workerd RPC callables are receiver-independent. Binding the dynamic
    // entrypoint as `this` attempts to transfer that entrypoint over JSRPC.
    const output = await runWorkflowBeforeDeadline(
      () => runEntrypoint(run.event, stepController.facade),
      run.dispatchDeadlineMs
    );
    step.closeForRunReturn();
    if (step.hasInFlightSteps()) {
      throw workflowStepError("workflow_invalid_step", "workflow run returned while workflow steps were still in flight");
    }
    if (step.hasTerminalStepFailure()) throw step.terminalStepFailure();
    if (step.isSuspended()) {
      throw workflowStepError("workflow_invalid_step", "workflow run returned after a step suspension was registered");
    }
    assertWorkflowDispatchDeadline(
      run.dispatchDeadlineMs,
      "Workflow dispatch deadline expired before terminal outcome"
    );
    const durationMs = Date.now() - startedAt;
    const response = workflowJsonResponse(
      200,
      "{\"outcome\":\"completed\",",
      "output",
      output ?? null,
      "output",
      durationMs
    );
    if (Date.now() >= run.dispatchDeadlineMs) return finishDeadlineFailure();
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
    if (step?.hasInFlightSteps()) {
      const caughtTrackedStepFailure = step.isTrackedStepFailure(caught);
      if (caughtSuspended || caughtTrackedStepFailure) {
        step.closeStepAdmission();
      } else {
        step.closeForRunReturn();
      }
      const settleBudgetMs = Math.max(0, siblingSettleDeadlineMs - Date.now());
      const settled = await step.waitForInFlightSteps(settleBudgetMs);
      if (!settled) {
        step.closeForRunReturn();
        caught = workflowInFlightSettleTimeoutError();
      }
    }
    step?.closeForRunReturn();
    if (infrastructureReport.reported()) {
      caught = workflowInfrastructureError(
        "Runtime KV infrastructure failure escaped tenant boundary"
      );
    }
    if (!isWorkflowInfrastructureError(caught) && Date.now() >= run.dispatchDeadlineMs) {
      caught = workflowInfrastructureError(
        "Workflow dispatch deadline expired before terminal outcome"
      );
    }
    let terminalStepError = null;
    if (step?.hasTerminalStepFailure()) {
      const recordedFailure = step.terminalStepFailure();
      if (
        isWorkflowInfrastructureError(recordedFailure) ||
        !isWorkflowInfrastructureError(caught)
      ) {
        caught = recordedFailure;
        terminalStepError = step.terminalStepError();
      }
    }
    const caughtIsSuspension = Boolean(
      !terminalStepError &&
      step?.isSuspended() &&
      isWorkflowSuspensionSignal(caught)
    );
    if (isWorkflowInfrastructureError(caught)) {
      return finishInfrastructureFailure(caught);
    }
    if (caughtIsSuspension) {
      const durationMs = Date.now() - startedAt;
      const response = jsonResponse(200, {
        outcome: "suspended",
        duration_ms: durationMs,
      });
      if (Date.now() >= run.dispatchDeadlineMs) return finishDeadlineFailure();
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
      return scope.respond(response);
    }
    const durationMs = Date.now() - startedAt;
    const error = terminalStepError ?? workflowError(caught);
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
    if (Date.now() >= run.dispatchDeadlineMs) return finishDeadlineFailure();
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
    return scope.respond(response);
  } finally {
    try {
      step?.closeForRunReturn();
    } finally {
      infrastructureReport.close();
    }
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
