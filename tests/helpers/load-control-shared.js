import {
  freshRepositoryModuleDataUrl,
  importSpecifierReplacements,
  repositoryFileUrl,
} from "./load-shared-module.js";
import { OBSERVABILITY_NOOP_URL } from "./mocks/observability.js";

const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const SHARED_INTERNAL_AUTH_URL = repositoryFileUrl("shared/internal-auth.js");
const SHARED_RANDOM_ID_URL = repositoryFileUrl("shared/random-id.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_OPTIMISTIC_RETRY_URL = repositoryFileUrl("shared/optimistic-retry.js");
const SHARED_QUEUE_KEYS_URL = repositoryFileUrl("shared/queue-keys.js");
const SHARED_S3_CLEANUP_LIFECYCLE_URL = repositoryFileUrl("shared/s3-cleanup-lifecycle.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");

/**
 * Compile the production-backed pure dependencies shared by the synthetic
 * control-shared stub and the real control/shared.js module graph.
 *
 * @param {{ sharedRedisUrl: string }} options
 */
export function compileControlSharedDependencies({ sharedRedisUrl }) {
  const controlErrorsUrl = freshRepositoryModuleDataUrl(
    "control/errors.js",
    importSpecifierReplacements({
      "shared-errors": SHARED_ERRORS_URL,
      "shared-respond": SHARED_RESPOND_URL,
    }),
  );
  const controlWorkflowsClientUrl = freshRepositoryModuleDataUrl(
    "control/workflows-client.js",
    importSpecifierReplacements({
      "shared-errors": SHARED_ERRORS_URL,
      "control-errors": controlErrorsUrl,
    }),
  );
  const controlOptimisticUrl = freshRepositoryModuleDataUrl(
    "control/optimistic.js",
    importSpecifierReplacements({
      "shared-redis": sharedRedisUrl,
      "shared-optimistic-retry": SHARED_OPTIMISTIC_RETRY_URL,
    }),
  );
  const controlJsonBodyUrl = freshRepositoryModuleDataUrl(
    "control/json-body.js",
    importSpecifierReplacements({
      "shared-bounded-body": SHARED_BOUNDED_BODY_URL,
      "shared-respond": SHARED_RESPOND_URL,
    }),
  );

  return {
    sharedErrorsUrl: SHARED_ERRORS_URL,
    sharedInternalAuthUrl: SHARED_INTERNAL_AUTH_URL,
    sharedRandomIdUrl: SHARED_RANDOM_ID_URL,
    sharedRespondUrl: SHARED_RESPOND_URL,
    controlErrorsUrl,
    controlWorkflowsClientUrl,
    controlOptimisticUrl,
    controlJsonBodyUrl,
  };
}

/**
 * @param {{
 *   sharedRedisUrl: string,
 *   controlS3Url: string,
 *   controlR2Url: string,
 *   sharedAuthTokenUrl: string,
 *   sharedAuthRolesUrl: string,
 *   sharedQueueKeysUrl?: string,
 * }} options
 */
export function compileControlSharedGraph({
  sharedRedisUrl,
  controlS3Url,
  controlR2Url,
  sharedAuthTokenUrl,
  sharedAuthRolesUrl,
  sharedQueueKeysUrl = SHARED_QUEUE_KEYS_URL,
}) {
  const dependencies = compileControlSharedDependencies({ sharedRedisUrl });
  const sharedRedisLockUrl = freshRepositoryModuleDataUrl(
    "shared/redis-lock.js",
    importSpecifierReplacements({
      "shared-random-id": dependencies.sharedRandomIdUrl,
    }),
  );
  const controlSharedUrl = freshRepositoryModuleDataUrl(
    "control/shared.js",
    importSpecifierReplacements({
      "shared-redis": sharedRedisUrl,
      "control-s3": controlS3Url,
      "control-r2": controlR2Url,
      "shared-auth-token": sharedAuthTokenUrl,
      "shared-auth-roles": sharedAuthRolesUrl,
      "shared-queue-keys": sharedQueueKeysUrl,
      "shared-respond": dependencies.sharedRespondUrl,
      "shared-internal-auth": dependencies.sharedInternalAuthUrl,
      "shared-errors": dependencies.sharedErrorsUrl,
      "shared-random-id": dependencies.sharedRandomIdUrl,
      "shared-worker-contract": WORKER_CONTRACT_URL,
      "shared-s3-cleanup-lifecycle": SHARED_S3_CLEANUP_LIFECYCLE_URL,
      "shared-observability": OBSERVABILITY_NOOP_URL,
      "control-workflows-client": dependencies.controlWorkflowsClientUrl,
      "control-errors": dependencies.controlErrorsUrl,
      "control-optimistic": dependencies.controlOptimisticUrl,
      "control-json-body": dependencies.controlJsonBodyUrl,
      "shared-redis-lock": sharedRedisLockUrl,
    }),
  );

  return { ...dependencies, controlSharedUrl };
}
