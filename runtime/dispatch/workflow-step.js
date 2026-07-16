import {
  workflowBackendBody,
  workflowStepError,
} from "runtime-dispatch-workflow-json";
import {
  WORKFLOW_REPLAY_PAGE_SIZE,
  canonicalJson,
  getWorkflowReplayCache,
  recordWorkflowReplayCacheOutcome,
  rememberWorkflowReplayStep,
  workflowReplayIdentity,
} from "runtime-dispatch-workflow-replay-cache";

/**
 * @typedef {{ fetch(url: string, init: RequestInit): Promise<Response> }} WorkflowBackend
 * @typedef {{
 *   ns: string,
 *   worker: string,
 *   frozenVersion: string,
 *   workflowName: string,
 *   workflowKey: string,
 *   className: string,
 *   instanceId: string,
 *   generation: number,
 *   runToken: string,
 *   createdAtMs: number,
 * }} WorkflowRun
 * @typedef {{ ordinal: number, stepName: string, nameCount: number, dependencies: number[], config: unknown }} StepIdentity
 * @typedef {import("runtime-dispatch-workflow-replay-cache").WorkflowReplayStepRecord} WorkflowReplayStepRecord
 * @typedef {import("runtime-dispatch-workflow-replay-cache").WorkflowReplayCache} WorkflowReplayCache
 */

/** @param {unknown} value @param {"name" | "message"} field */
function readThrowableField(value, field) {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) {
    return undefined;
  }
  try {
    return /** @type {Record<string, unknown>} */ (value)[field];
  } catch {
    return undefined;
  }
}

/** @param {unknown} err */
export function workflowError(err) {
  const rawName = readThrowableField(err, "name");
  const rawMessage = readThrowableField(err, "message");
  let message = "Workflow run failed";
  if (typeof rawMessage === "string") {
    message = rawMessage;
  } else if (err != null) {
    try {
      message = String(err);
    } catch {
      // Some valid thrown values have no usable primitive conversion.
    }
  }
  return {
    name: typeof rawName === "string" && rawName ? rawName : "Error",
    message,
  };
}

/**
 * Rehydrate persisted step failures as ordinary user-visible errors. Internal
 * workflow error provenance is process-local and must not be restored from
 * user-controlled stored names.
 *
 * @param {{ name?: unknown, message?: unknown } | null | undefined} error
 * @param {string} fallbackMessage
 */
function persistedStepError(error, fallbackMessage) {
  const err = new Error(
    typeof error?.message === "string" && error.message ? error.message : fallbackMessage
  );
  err.name = typeof error?.name === "string" && error.name ? error.name : "Error";
  return err;
}

/** @param {unknown} error */
function persistedStepErrorRecord(error) {
  return /** @type {{ name?: unknown, message?: unknown } | null | undefined} */ (
    error && typeof error === "object" ? error : null
  );
}

const WORKFLOWS_BASE_URL = "http://workflows/internal/workflows";
export const MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN = 1000;
export const MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN = 1000;

class WorkflowSuspended extends Error {
  constructor(message = "Workflow run suspended") {
    super(message);
    this.name = "WorkflowSuspended";
  }
}

/** @param {unknown} err */
export function isWorkflowSuspended(err) {
  try {
    return err instanceof WorkflowSuspended;
  } catch {
    return false;
  }
}

/** @param {unknown} err */
export function isWorkflowSuspensionSignal(err) {
  if (isWorkflowSuspended(err)) return true;
  return readThrowableField(err, "name") === "WorkflowSuspended";
}

/** @param {unknown} value */
function parseSleepDurationMs(value) {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || value < 0) {
      throw workflowStepError("workflow_invalid_step", "workflow sleep duration must be a non-negative finite number");
    }
    return Math.ceil(value);
  }
  if (typeof value !== "string") {
    throw workflowStepError("workflow_invalid_step", "workflow sleep duration must be a number or duration string");
  }
  const match = value.trim().match(/^(\d+(?:\.\d+)?)\s*(ms|millisecond|milliseconds|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours)$/i);
  if (!match) {
    throw workflowStepError("workflow_invalid_step", "workflow sleep duration string is invalid");
  }
  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const multiplier = unit.startsWith("ms") || unit.startsWith("millisecond") ? 1
    : unit === "s" || unit.startsWith("sec") ? 1000
      : unit === "m" || unit.startsWith("min") ? 60_000
        : 3_600_000;
  return Math.ceil(amount * multiplier);
}

/** @param {unknown} value */
function parseSleepUntilMs(value) {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  if (typeof value === "string") return Date.parse(value);
  throw workflowStepError("workflow_invalid_step", "workflow sleepUntil target must be a Date, timestamp, or date string");
}

/**
 * @param {WorkflowBackend | null | undefined} backend
 * @param {string} path
 * @param {unknown} body
 * @param {string | null} [requestId]
 * @returns {Promise<Record<string, unknown>>}
 */
async function workflowBackendCall(backend, path, body, requestId = null) {
  if (!backend || typeof backend.fetch !== "function") {
    throw workflowStepError("workflow_backend_unavailable", "Workflow backend binding is not configured");
  }
  /** @type {Record<string, string>} */
  const headers = { "content-type": "application/json" };
  if (requestId) headers["x-request-id"] = requestId;
  const response = await backend.fetch(`${WORKFLOWS_BASE_URL}/${path}`, {
    method: "POST",
    headers,
    body: workflowBackendBody(path, body),
  });
  let parsed;
  try {
    parsed = await response.json();
  } catch {
    parsed = {};
  }
  if (!response.ok) {
    throw workflowStepError(
      parsed?.error || "workflow_step_failed",
      parsed?.message || `Workflow step backend returned HTTP ${response.status}`
    );
  }
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {WorkflowBackend | null | undefined} backend
 * @param {WorkflowRun} run
 * @param {WorkflowReplayCache} cache
 * @param {number} ordinal
 * @param {string | null} requestId
 */
async function fetchReplayStepPage(backend, run, cache, ordinal, requestId) {
  while (!cache.complete && ordinal >= cache.nextOrdinal) {
    const page = await workflowBackendCall(backend, "replay-steps", {
      ...workflowReplayIdentity(run),
      startOrdinal: cache.nextOrdinal,
      limit: WORKFLOW_REPLAY_PAGE_SIZE,
    }, requestId);
    const steps = Array.isArray(page?.steps) ? page.steps : [];
    for (const step of steps) {
      if (Number.isInteger(step?.ordinal)) rememberWorkflowReplayStep(cache, step.ordinal, step);
    }
    const nextOrdinal = typeof page?.nextOrdinal === "number" && Number.isInteger(page.nextOrdinal)
      ? page.nextOrdinal
      : cache.nextOrdinal + steps.length;
    cache.nextOrdinal = Math.max(cache.nextOrdinal, nextOrdinal);
    cache.complete = Boolean(page?.done) || steps.length === 0;
  }
}

/**
 * @param {WorkflowRun} run
 * @param {WorkflowBackend | null | undefined} backend
 * @param {string | null} [requestId]
 */
export function createStepController(run, backend, requestId = null) {
  let ordinal = 0;
  let startedSteps = 0;
  let suspendingStepInFlight = false;
  let suspended = false;
  let runReturned = false;
  let stepCallbackDepth = 0;
  /** @type {unknown} */
  let terminalStepFailure = null;
  /** @type {{ name: string, message: string } | null} */
  let terminalStepError = null;
  let hasTerminalStepFailure = false;
  const activeStepPromises = new Set();
  const nameCounts = new Map();
  const dependencyFrontier = new Set();
  const replayCache = getWorkflowReplayCache(run);
  /** @type {Set<{ identity: StepIdentity | null, settled: boolean }>} */
  const activeStepRecords = new Set();
  /** @type {Promise<void> | null} */
  let replayFetchPromise = null;

  /** @param {unknown} reason @param {{ name: string, message: string } | null} [error] */
  const rememberTerminalStepFailure = (reason, error = null) => {
    if (hasTerminalStepFailure) return;
    terminalStepFailure = reason;
    terminalStepError = error ?? workflowError(reason);
    hasTerminalStepFailure = true;
  };

  /** @param {number[]} left @param {number[]} right */
  const sameDependencies = (left, right) => (
    left.length === right.length && left.every((value, index) => value === right[index])
  );

  const activeBlockingStepRecords = () => {
    /** @type {Array<{ identity: StepIdentity | null, settled: boolean }>} */
    const records = [];
    for (const record of activeStepRecords) {
      if (record.settled) continue;
      if (record.identity && dependencyFrontier.has(record.identity.ordinal)) continue;
      records.push(record);
    }
    return records;
  };

  const activeUnsettledStepCount = () => {
    return activeBlockingStepRecords().length;
  };

  /** @param {StepIdentity} identity */
  const markActiveStepSettled = (identity) => {
    for (const record of activeStepRecords) {
      if (record.identity?.ordinal === identity.ordinal) record.settled = true;
    }
  };

  /** @param {{ suspending?: boolean, stepDo?: boolean, stepDependencies?: number[] }} [options] */
  const assertCanStartStep = (options = {}) => {
    const activeRecords = activeBlockingStepRecords();
    const activeStepCount = activeRecords.length;
    if (runReturned) {
      throw workflowStepError("workflow_invalid_step", "workflow step cannot start after the run returned");
    }
    if (suspended) {
      throw workflowStepError("workflow_invalid_step", "workflow cannot start another step after a suspension was registered");
    }
    if (stepCallbackDepth > 0) {
      throw workflowStepError("workflow_invalid_step", "workflow steps cannot start while another step.do callback is in flight");
    }
    if (hasTerminalStepFailure) throw terminalStepFailure;
    if (options.suspending) {
      if (activeStepCount > 0) {
        throw workflowStepError("workflow_invalid_step", "workflow suspending steps must execute when no other step is in flight");
      }
      return;
    }
    if (suspendingStepInFlight) {
      throw workflowStepError("workflow_invalid_step", "workflow step.do cannot run while a suspending step is in flight");
    }
    if (options.stepDo && activeStepCount > 0) {
      const dependencies = options.stepDependencies;
      if (
        !dependencies ||
        activeRecords.some((record) => !record.identity || !sameDependencies(record.identity.dependencies, dependencies))
      ) {
        throw workflowStepError(
          "workflow_invalid_step",
          "workflow step.do fan-out must be created before awaiting prior steps"
        );
      }
      if (activeStepCount >= MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN) {
        throw workflowStepError(
          "request_too_large",
          `workflow run has more than ${MAX_WORKFLOW_ACTIVE_STEPS_PER_RUN_TURN} workflow steps in flight`
        );
      }
    }
  };

  const reserveStartedStep = () => {
    startedSteps += 1;
    if (startedSteps > MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN) {
      throw workflowStepError(
        "request_too_large",
        `workflow run started more than ${MAX_WORKFLOW_STARTED_STEPS_PER_RUN_TURN} steps in one dispatch turn`
      );
    }
  };

  const assertRunStillOpen = () => {
    if (runReturned) {
      throw workflowStepError("workflow_invalid_step", "workflow run returned while workflow steps were still in flight");
    }
  };

  /**
   * @param {{
   *   identity: StepIdentity & WorkflowRun & { startedAtMs: number },
   *   action: "register-sleep" | "register-wait",
   *   dueAtMs: number | null,
   *   invalidStatePrefix: string,
   *   returnsOutput?: boolean,
   * }} step
   */
  async function runSuspendingStep(step) {
    suspendingStepInFlight = true;
    try {
      const cached = await cachedCompletion(step.identity);
      if (cached?.state === "complete") {
        markStepCompleted(step.identity);
        return step.returnsOutput ? cached.output ?? null : undefined;
      }
      assertRunStillOpen();
      reserveStartedStep();
      const registered = await workflowBackendCall(backend, step.action, {
        ...step.identity,
        dueAtMs: step.dueAtMs,
      }, requestId);
      assertRunStillOpen();
      if (registered?.state === "complete") {
        /** @type {Record<string, unknown>} */
        const record = { status: "completed", attempt: 1 };
        if (step.returnsOutput) record.output = registered.output ?? null;
        cacheStep(step.identity, record);
        markStepCompleted(step.identity);
        return step.returnsOutput ? registered.output ?? null : undefined;
      }
      if (registered?.state !== "waiting") {
        throw workflowStepError(
          "workflow_invalid_step",
          `${step.invalidStatePrefix} ${JSON.stringify(registered?.state)}`
        );
      }
      cacheStep(step.identity, { status: "waiting", attempt: 1, dueAtMs: step.dueAtMs });
      suspended = true;
      throw new WorkflowSuspended();
    } finally {
      suspendingStepInFlight = false;
    }
  }

  const waitForInFlightSteps = async () => {
    const settled = [];
    while (activeStepPromises.size > 0) {
      settled.push(...await Promise.allSettled([...activeStepPromises]));
    }
    return settled;
  };

  /**
   * @template T
   * @param {Promise<T>} promise
   * @param {StepIdentity | null} [identity]
   */
  const trackStepPromise = (promise, identity = null) => {
    const record = { identity, settled: false };
    /** @type {{ promise: Promise<T> | null }} */
    const trackedRef = { promise: null };
    const tracked = (async () => {
      try {
        return await promise;
      } catch (reason) {
        if (!isWorkflowSuspended(reason)) rememberTerminalStepFailure(reason);
        throw reason;
      } finally {
        activeStepRecords.delete(record);
        if (trackedRef.promise) activeStepPromises.delete(trackedRef.promise);
      }
    })();
    trackedRef.promise = tracked;
    activeStepRecords.add(record);
    activeStepPromises.add(tracked);
    tracked.catch(() => {});
    return tracked;
  };

  /** @param {StepIdentity} identity */
  const markStepCompleted = (identity) => {
    for (const dependency of identity.dependencies) dependencyFrontier.delete(dependency);
    dependencyFrontier.add(identity.ordinal);
    markActiveStepSettled(identity);
  };

  /**
   * @param {string} name
   * @param {unknown} config
   * @returns {StepIdentity & WorkflowRun & { ordinal: number, stepName: string, nameCount: number, dependencies: number[], config: unknown, startedAtMs: number }}
   */
  const nextStepIdentity = (name, config) => {
    const nameCount = (nameCounts.get(name) || 0) + 1;
    nameCounts.set(name, nameCount);
    const dependencies = [...dependencyFrontier].toSorted((a, b) => a - b);
    return {
      ns: run.ns,
      worker: run.worker,
      frozenVersion: run.frozenVersion,
      workflowName: run.workflowName,
      workflowKey: run.workflowKey,
      className: run.className,
      instanceId: run.instanceId,
      generation: run.generation,
      runToken: run.runToken,
      createdAtMs: run.createdAtMs,
      ordinal: ordinal++,
      stepName: name,
      nameCount,
      dependencies,
      config,
      startedAtMs: Date.now(),
    };
  };

  /**
   * @param {ReturnType<typeof nextStepIdentity>} identity
   * @param {Record<string, unknown>} record
   */
  const cacheStep = (identity, record) => {
    rememberWorkflowReplayStep(replayCache, identity.ordinal, {
      ordinal: identity.ordinal,
      name: identity.stepName,
      nameCount: identity.nameCount,
      dependencies: identity.dependencies,
      config: canonicalJson(identity.config),
      ...record,
    });
    if (identity.ordinal === replayCache.nextOrdinal) replayCache.nextOrdinal += 1;
  };

  /** @param {number} targetOrdinal */
  const fetchReplayThrough = async (targetOrdinal) => {
    while (!replayCache.complete && targetOrdinal >= replayCache.nextOrdinal) {
      replayFetchPromise ??= fetchReplayStepPage(backend, run, replayCache, targetOrdinal, requestId)
        .finally(() => {
          replayFetchPromise = null;
        });
      await replayFetchPromise;
    }
  };

  /** @param {ReturnType<typeof nextStepIdentity>} identity */
  const cachedStep = async (identity) => {
    if (!replayCache.steps.has(identity.ordinal)) {
      try {
        await fetchReplayThrough(identity.ordinal);
      } catch {
        recordWorkflowReplayCacheOutcome("error");
        return null;
      }
    }
    const cached = replayCache.steps.get(identity.ordinal);
    if (!cached) {
      recordWorkflowReplayCacheOutcome("miss");
      return null;
    }
    if (
      cached.name !== identity.stepName ||
      cached.nameCount !== identity.nameCount ||
      !Array.isArray(cached.dependencies) ||
      !sameDependencies(cached.dependencies, identity.dependencies) ||
      cached.config !== canonicalJson(identity.config)
    ) {
      recordWorkflowReplayCacheOutcome("miss");
      return null;
    }
    recordWorkflowReplayCacheOutcome("hit");
    return cached;
  };

  /** @param {ReturnType<typeof nextStepIdentity>} identity */
  const cachedCompletion = async (identity) => {
    const cached = await cachedStep(identity);
    if (!cached) return null;
    if (cached.status === "completed") {
      return { state: "complete", output: cached.output ?? null };
    }
    if (cached.status === "failed") {
      return { state: "failed", error: cached.error ?? { name: "Error", message: "Workflow step failed" } };
    }
    return null;
  };

  const facade = {
    /**
     * @param {string} name
     * @param {unknown | (() => unknown | Promise<unknown>)} configOrCallback
     * @param {undefined | (() => unknown | Promise<unknown>)} maybeCallback
     */
    do(name, configOrCallback, maybeCallback) {
      let callback;
      let identity;
      try {
        if (typeof name !== "string" || name === "") {
          throw workflowStepError("workflow_invalid_step", "workflow step name must be a non-empty string");
        }
        callback = typeof configOrCallback === "function" ? configOrCallback : maybeCallback;
        if (typeof callback !== "function") {
          throw workflowStepError("workflow_invalid_step", "workflow step.do requires a callback");
        }
        const config = typeof configOrCallback === "function"
          ? null
          : configOrCallback ?? null;
        identity = nextStepIdentity(name, config);
        assertCanStartStep({ stepDo: true, stepDependencies: identity.dependencies });
      } catch (err) {
        if (!isWorkflowSuspended(err)) rememberTerminalStepFailure(err);
        const rejected = Promise.reject(err);
        rejected.catch(() => {});
        return rejected;
      }
      return trackStepPromise((async () => {
        const cached = await cachedCompletion(identity);
        if (cached?.state === "complete") {
          markStepCompleted(identity);
          return cached.output ?? null;
        }
        if (cached?.state === "failed") {
          throw persistedStepError(cached.error, "Workflow step failed");
        }
        assertRunStillOpen();
        reserveStartedStep();
        const claim = await workflowBackendCall(backend, "claim-step", identity, requestId);
        assertRunStillOpen();
        if (claim?.state === "complete") {
          cacheStep(identity, { status: "completed", attempt: 1, output: claim.output ?? null });
          markStepCompleted(identity);
          return claim.output ?? null;
        }
        if (claim?.state === "waiting") {
          cacheStep(identity, { status: "waiting", attempt: 1, dueAtMs: claim.dueAtMs ?? null });
          suspended = true;
          throw new WorkflowSuspended();
        }
        if (claim?.state === "failed") {
          cacheStep(identity, { status: "failed", attempt: 1, error: claim.error ?? null });
          throw persistedStepError(persistedStepErrorRecord(claim.error), "Workflow step failed");
        }
        if (claim?.state !== "run") {
          throw workflowStepError("workflow_invalid_step", `workflow step claim returned invalid state ${JSON.stringify(claim?.state)}`);
        }
        const attempt = typeof claim.attempt === "number" && Number.isInteger(claim.attempt) && claim.attempt > 0
          ? claim.attempt
          : 1;
        let output;
        try {
          stepCallbackDepth += 1;
          try {
            output = await callback({ attempt });
          } finally {
            stepCallbackDepth -= 1;
          }
          assertRunStillOpen();
        } catch (err) {
          if (runReturned) throw err;
          const error = workflowError(err);
          const committed = await workflowBackendCall(backend, "commit-step-error", {
            ...identity,
            attempt,
            error,
            nonRetryable: error.name === "NonRetryableError" || error.name === "workflow_invalid_step",
          }, requestId);
          if (committed?.state === "waiting") {
            cacheStep(identity, { status: "waiting", attempt, dueAtMs: committed.dueAtMs ?? null });
            suspended = true;
            throw new WorkflowSuspended();
          }
          if (committed?.state === "failed") {
            cacheStep(identity, { status: "failed", attempt, error });
          }
          rememberTerminalStepFailure(err, error);
          throw err;
        }
        await workflowBackendCall(backend, "commit-step-success", {
          ...identity,
          attempt,
          output: output ?? null,
        }, requestId);
        cacheStep(identity, { status: "completed", attempt, output: output ?? null });
        markStepCompleted(identity);
        return output;
      })(), identity);
    },
    /**
     * @param {string} name
     * @param {unknown} duration
     */
    sleep(name, duration) {
      return trackStepPromise((async () => {
        if (typeof name !== "string" || name === "") {
          throw workflowStepError("workflow_invalid_step", "workflow sleep name must be a non-empty string");
        }
        assertCanStartStep({ suspending: true });
        const durationMs = parseSleepDurationMs(duration);
        const dueAtMs = Date.now() + durationMs;
        const identity = nextStepIdentity(name, { type: "sleep", durationMs });
        await runSuspendingStep({
          identity,
          action: "register-sleep",
          dueAtMs,
          invalidStatePrefix: "workflow sleep returned invalid state",
        });
      })());
    },
    /**
     * @param {string} name
     * @param {unknown} target
     */
    sleepUntil(name, target) {
      return trackStepPromise((async () => {
        if (typeof name !== "string" || name === "") {
          throw workflowStepError("workflow_invalid_step", "workflow sleepUntil name must be a non-empty string");
        }
        assertCanStartStep({ suspending: true });
        const dueAtMs = parseSleepUntilMs(target);
        if (!Number.isFinite(dueAtMs)) {
          throw workflowStepError("workflow_invalid_step", "workflow sleepUntil target is invalid");
        }
        const dueAtMsCeil = Math.ceil(dueAtMs);
        const identity = nextStepIdentity(name, { type: "sleepUntil", dueAtMs });
        await runSuspendingStep({
          identity,
          action: "register-sleep",
          dueAtMs: dueAtMsCeil,
          invalidStatePrefix: "workflow sleepUntil returned invalid state",
        });
      })());
    },
    /**
     * @param {string} name
     * @param {unknown} [options]
     */
    waitForEvent(name, options = {}) {
      return trackStepPromise((async () => {
        if (typeof name !== "string" || name === "") {
          throw workflowStepError("workflow_invalid_step", "workflow waitForEvent name must be a non-empty string");
        }
        assertCanStartStep({ suspending: true });
        if (!options || typeof options !== "object" || Array.isArray(options)) {
          throw workflowStepError("workflow_invalid_step", "workflow waitForEvent options must be an object");
        }
        const waitOptions = /** @type {{ type?: unknown, timeout?: unknown }} */ (options);
        const eventType = waitOptions.type;
        if (typeof eventType !== "string" || eventType === "") {
          throw workflowStepError("workflow_invalid_step", "workflow waitForEvent type must be a non-empty string");
        }
        const timeoutMs = waitOptions.timeout == null ? null : parseSleepDurationMs(waitOptions.timeout);
        const dueAtMs = timeoutMs == null ? null : Date.now() + timeoutMs;
        const identity = nextStepIdentity(name, { type: "waitForEvent", eventType, timeoutMs });
        return await runSuspendingStep({
          identity,
          action: "register-wait",
          dueAtMs: dueAtMs == null ? null : Math.ceil(dueAtMs),
          invalidStatePrefix: "workflow waitForEvent returned invalid state",
          returnsOutput: true,
        });
      })());
    },
  };

  return {
    facade,
    isSuspended() {
      return suspended;
    },
    terminalStepFailure() {
      return terminalStepFailure;
    },
    terminalStepError() {
      return terminalStepError;
    },
    hasTerminalStepFailure() {
      return hasTerminalStepFailure;
    },
    hasInFlightSteps() {
      return activeUnsettledStepCount() > 0;
    },
    closeForRunReturn() {
      runReturned = true;
    },
    waitForInFlightSteps,
  };
}
