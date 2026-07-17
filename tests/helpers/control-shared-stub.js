// `controlSharedStubUrl(extraSource)` appends a test-specific `state`/exports
// tail so each test wires its own redis fakes without re-declaring the base
// helpers.

import {
  moduleDataUrl,
} from "./load-shared-module.js";
import { compileControlSharedDependencies } from "./load-control-shared.js";
import { sharedRedisStubUrl } from "./mocks/fake-redis.js";
import { OBSERVABILITY_NOOP_URL } from "./mocks/observability.js";

const {
  sharedErrorsUrl: SHARED_ERRORS_URL,
  sharedInternalAuthUrl: SHARED_INTERNAL_AUTH_URL,
  sharedRandomIdUrl: SHARED_RANDOM_ID_URL,
  sharedRespondUrl: SHARED_RESPOND_URL,
  controlErrorsUrl: CONTROL_ERRORS_URL,
  controlWorkflowsClientUrl: CONTROL_WORKFLOWS_CLIENT_URL,
  controlOptimisticUrl: CONTROL_OPTIMISTIC_URL,
  controlJsonBodyUrl: CONTROL_JSON_BODY_URL,
} = compileControlSharedDependencies({ sharedRedisUrl: sharedRedisStubUrl() });

const CONTROL_SHARED_BASE = `
import { jsonError, jsonResponse, sanitizeJsonErrorDetails } from ${JSON.stringify(SHARED_RESPOND_URL)};
import { createPostWorkflowsInternal } from ${JSON.stringify(CONTROL_WORKFLOWS_CLIENT_URL)};
import { ControlAbort, controlAbortLogDetails, codedErrorLogFields, codedErrorResponse, controlAbortResponse, secretEnvelopeErrorResponse } from ${JSON.stringify(CONTROL_ERRORS_URL)};
import { runOptimistic, withOptimisticRetries } from ${JSON.stringify(CONTROL_OPTIMISTIC_URL)};
import { readJsonBody } from ${JSON.stringify(CONTROL_JSON_BODY_URL)};
import { errorMessage as errMessage } from ${JSON.stringify(SHARED_ERRORS_URL)};
import { withInternalAuth } from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};
import { randomHex } from ${JSON.stringify(SHARED_RANDOM_ID_URL)};
import { formatError } from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};
export {
  ControlAbort,
  controlAbortLogDetails,
  codedErrorLogFields,
  codedErrorResponse,
  controlAbortResponse,
  secretEnvelopeErrorResponse,
  errMessage,
  formatError,
  jsonError,
  jsonResponse,
  randomHex,
  readJsonBody,
  runOptimistic,
  sanitizeJsonErrorDetails,
  withOptimisticRetries,
};
export function prefixedId(prefix, bytes = 16) {
  return prefix + randomHex(bytes);
}
export function stringEnv(env) {
  const out = {};
  for (const [key, value] of Object.entries(env)) {
    if (typeof value === "string") out[key] = value;
    else if (value === undefined) out[key] = undefined;
  }
  return out;
}
export function requireControlLog() {
  return state.log;
}
export function requireControlRedis() {
  return state.redis;
}
export function requireControlDataRedis() {
  return state.dataRedis;
}
export function controlTailRedis() {
  return state.dataRedis || state.redis;
}
export function getControlS3() {
  return state.s3;
}
export function getControlR2() {
  return state.r2;
}
export function controlInternalJsonHeaders() {
  return withInternalAuth({ "content-type": "application/json" }, state.env);
}
export const postWorkflowsInternal = createPostWorkflowsInternal({
  getWorkflows: () => state.workflows,
  headers: controlInternalJsonHeaders,
  getLog: () => state.log,
});
export async function recordCleanupIntentOrWarn({
  cleanupIntent,
  cleanupTaskId,
  warningMessage,
  logEvent,
  logFields,
  log,
}) {
  const warnings = [];
  let queueHintStatus = cleanupTaskId ? "queued" : "none";
  if (cleanupIntent) {
    try {
      await recordS3CleanupIntent(cleanupIntent);
    } catch (err) {
      queueHintStatus = "failed";
      warnings.push({ code: "cleanup_queue_failed", message: warningMessage });
      log("warn", logEvent, {
        ...logFields,
        task_id: cleanupTaskId,
        error_message: errMessage(err),
      });
    }
  }
  return { queueHintStatus, warnings };
}
`;

export function controlSharedStubUrl(extraSource = "") {
  const cleanupStub = /\brecordS3CleanupIntent\b/.test(extraSource)
    ? ""
    : "\nexport async function recordS3CleanupIntent(_cleanupIntent) {}\n";
  return moduleDataUrl(`${CONTROL_SHARED_BASE}\n${extraSource}${cleanupStub}`);
}
