// `controlSharedStubUrl(extraSource)` appends a test-specific `state`/exports
// tail so each test wires its own redis fakes without re-declaring the base
// helpers.

import {
  freshRepositoryModuleDataUrl,
  moduleDataUrl,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { sharedRedisStubUrl } from "./mocks/fake-redis.js";
import { OBSERVABILITY_NOOP_URL } from "./mocks/observability.js";

const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const SHARED_RANDOM_ID_URL = repositoryFileUrl("shared/random-id.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_OPTIMISTIC_RETRY_URL = repositoryFileUrl("shared/optimistic-retry.js");
const CONTROL_ERRORS_URL = freshRepositoryModuleDataUrl("control/errors.js", [
  [/from "shared-respond"/g, `from ${JSON.stringify(SHARED_RESPOND_URL)}`],
]);
const CONTROL_WORKFLOWS_CLIENT_URL = freshRepositoryModuleDataUrl("control/workflows-client.js", [
  [/from "shared-errors"/g, `from ${JSON.stringify(SHARED_ERRORS_URL)}`],
  [/from "control-errors"/g, `from ${JSON.stringify(CONTROL_ERRORS_URL)}`],
]);
const CONTROL_OPTIMISTIC_REDIS_URL = sharedRedisStubUrl();
const CONTROL_OPTIMISTIC_URL = freshRepositoryModuleDataUrl("control/optimistic.js", [
  [/from "shared-redis"/g, `from ${JSON.stringify(CONTROL_OPTIMISTIC_REDIS_URL)}`],
  [/from "shared-optimistic-retry"/g, `from ${JSON.stringify(SHARED_OPTIMISTIC_RETRY_URL)}`],
]);
const CONTROL_JSON_BODY_URL = freshRepositoryModuleDataUrl("control/json-body.js", [
  [/from "shared-bounded-body"/g, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)}`],
  [/from "shared-respond"/g, `from ${JSON.stringify(SHARED_RESPOND_URL)}`],
]);

const CONTROL_SHARED_BASE = `
import { jsonError, jsonResponse, sanitizeJsonErrorDetails } from ${JSON.stringify(SHARED_RESPOND_URL)};
import { createPostWorkflowsInternal } from ${JSON.stringify(CONTROL_WORKFLOWS_CLIENT_URL)};
import { ControlAbort, codedErrorResponse, controlAbortResponse } from ${JSON.stringify(CONTROL_ERRORS_URL)};
import { runOptimistic, withOptimisticRetries } from ${JSON.stringify(CONTROL_OPTIMISTIC_URL)};
import { readJsonBody } from ${JSON.stringify(CONTROL_JSON_BODY_URL)};
import { errorMessage as errMessage } from ${JSON.stringify(SHARED_ERRORS_URL)};
import { randomHex } from ${JSON.stringify(SHARED_RANDOM_ID_URL)};
import { formatError } from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};
export {
  ControlAbort,
  codedErrorResponse,
  controlAbortResponse,
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
  const token = state.env?.WDL_INTERNAL_AUTH_TOKEN;
  return token
    ? { "content-type": "application/json", "x-wdl-internal-auth": token }
    : { "content-type": "application/json" };
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
