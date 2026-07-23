import {
  jsonResponse,
  jsonError,
  errMessage,
  requireControlLog,
  requireControlRedis,
  stringEnv,
  codedErrorLogFields,
  codedErrorResponse,
  runOptimistic,
  ControlAbort,
  controlAbortResponse,
  secretEnvelopeErrorResponse,
} from "control-shared";
import {
  invalidSecretMutationKeyResponse,
  readEncryptedSecretPutValue,
} from "control-handlers-secret-put";
import { routesKey, workerVersionsKey } from "shared-worker-contract";
import { nsSecretsKey, workerSecretsKey } from "shared-secret-keys";
import { workersIndexKey } from "control-lib";
import {
  BundleMetaError,
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  assertWorkersVersionsUserEnvBudget,
  decryptMutatedSecretHashForBudget,
  decryptSecretHash,
} from "control-env-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";

const MAX_NS_SECRET_ATTEMPTS = 5;
const NS_SECRET_WORKER_READ_BATCH_SIZE = 16;

class NamespaceSecretAbort extends ControlAbort {}

/**
 * @param {unknown} err
 * @param {{ log: import("control-shared").ControlLogger, requestId: string, nsName: string, secretKey: string, method: string }} context
 */
function namespaceSecretMutationErrorResponse(err, { log, requestId, nsName, secretKey, method }) {
  if (err instanceof NamespaceSecretAbort) {
    log(err.status >= 500 ? "error" : "warn", "ns_secret_mutation_rejected", {
      request_id: requestId,
      namespace: nsName,
      key: secretKey,
      method,
      ...codedErrorLogFields(err),
    });
    return controlAbortResponse(err);
  }
  if (err instanceof BundleMetaError) {
    log("error", "ns_secret_mutation_rejected", {
      request_id: requestId,
      namespace: nsName,
      key: secretKey,
      method,
      ...codedErrorLogFields(err, err.code, { errorDetail: errMessage(err.cause) }),
    });
    return codedErrorResponse(err, err.code);
  }
  if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
  if (err instanceof SecretEnvelopeError) {
    return secretEnvelopeErrorResponse({
      err,
      log,
      event: "ns_secret_mutation_rejected",
      fields: {
        request_id: requestId,
        namespace: nsName,
        key: secretKey,
        method,
      },
    });
  }
  return null;
}

/**
 * @param {{
 *   redis: import("shared-redis").RedisSession,
 *   controlEnv: Record<string, string | undefined>,
 *   nsName: string,
 *   nsSecrets: Record<string, string>,
 * }} args
 */
async function validateNamespaceSecretBudget({
  redis,
  controlEnv,
  nsName,
  nsSecrets,
}) {
  const activeRoutes = await redis.hGetAll(routesKey(nsName));
  const indexedWorkers = await redis.sMembers(workersIndexKey(nsName));
  const workerNames = new Set([
    ...indexedWorkers.filter((worker) => typeof worker === "string" && worker),
    ...Object.keys(activeRoutes),
  ]);
  if (workerNames.size === 0) {
    assertWorkerLoaderUserEnvBudget({
      ns: nsName,
      nsSecrets,
      assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
    });
    return;
  }

  const workers = [...workerNames];
  for (let offset = 0; offset < workers.length; offset += NS_SECRET_WORKER_READ_BATCH_SIZE) {
    const batch = workers.slice(offset, offset + NS_SECRET_WORKER_READ_BATCH_SIZE);
    await redis.watch(...batch.flatMap((worker) => [
      workerVersionsKey(nsName, worker),
      workerSecretsKey(nsName, worker),
    ]));
  }

  for (let offset = 0; offset < workers.length; offset += NS_SECRET_WORKER_READ_BATCH_SIZE) {
    const batch = workers.slice(offset, offset + NS_SECRET_WORKER_READ_BATCH_SIZE);
    const versionKeys = batch.map((worker) => workerVersionsKey(nsName, worker));
    const secretKeys = batch.map((worker) => workerSecretsKey(nsName, worker));
    const retainedByWorker = await redis.zRangeMany(versionKeys, 0, -1);
    const encryptedByWorker = await redis.hGetAllMany(secretKeys);
    const secretsByWorker = await Promise.all(encryptedByWorker.map((encrypted, index) =>
      decryptSecretHash({
        encrypted,
        env: controlEnv,
        hashKey: secretKeys[index],
      })
    ));
    /** @type {Array<{ worker: string, versions: string[], workerSecrets: Record<string, string> }>} */
    const budgetWorkers = [];
    for (let index = 0; index < batch.length; index += 1) {
      const worker = batch[index];
      const activeVersion = activeRoutes[worker];
      budgetWorkers.push({
        worker,
        versions: [
          ...retainedByWorker[index],
          ...(typeof activeVersion === "string" && activeVersion ? [activeVersion] : []),
        ],
        workerSecrets: secretsByWorker[index],
      });
    }

    await assertWorkersVersionsUserEnvBudget({
      redis,
      ns: nsName,
      workers: budgetWorkers,
      nsSecrets,
      assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
    });
  }
}

/**
 * @param {{
 *   redis: import("shared-redis").RedisClient,
 *   env: Record<string, unknown>,
 *   nsName: string,
 *   secretKey: string,
 *   method: "PUT" | "DELETE",
 *   encrypted?: string | null,
 *   plaintext?: string | null,
 * }} args
 */
async function mutateNamespaceSecret({
  redis,
  env,
  nsName,
  secretKey,
  method,
  encrypted = null,
  plaintext = null,
}) {
  const controlEnv = stringEnv(env);
  const nsSecretHashKey = nsSecretsKey(nsName);
  return await runOptimistic(redis, {
    attempts: MAX_NS_SECRET_ATTEMPTS,
    onExhausted: () => {
      throw new NamespaceSecretAbort(503, "namespace_secret_mutation_contention", {
        message: `exhausted ${MAX_NS_SECRET_ATTEMPTS} retries; retry later`,
      });
    },
  }, async (iso) => {
    await iso.watch(nsSecretHashKey, routesKey(nsName), workersIndexKey(nsName));

    const existingEncrypted = await iso.hGetAll(nsSecretHashKey);
    if (method === "DELETE" && !Object.hasOwn(existingEncrypted, secretKey)) {
      return { mutated: false };
    }

    const nsSecrets = await decryptMutatedSecretHashForBudget({
      encrypted: existingEncrypted,
      env: controlEnv,
      hashKey: nsSecretHashKey,
      key: secretKey,
      method,
      plaintext,
    });
    if (method === "PUT" && typeof encrypted !== "string") {
      throw new Error("PUT namespace secret encrypted value missing");
    }

    await validateNamespaceSecretBudget({
      redis: iso,
      controlEnv,
      nsName,
      nsSecrets,
    });

    const multi = iso.multi();
    if (method === "PUT") {
      multi.hSet(nsSecretHashKey, secretKey, /** @type {string} */ (encrypted));
    } else {
      multi.hDel(nsSecretHashKey, secretKey);
    }
    await multi.exec();
    return { mutated: true };
  });
}

/**
 * @param {{
 *   request: Request,
 *   env: Record<string, unknown>,
 *   method: string,
 *   nsName: string,
 *   secretKey?: string,
 *   requestId: string,
 * }} args
 */
export async function handle({ request, env, method, nsName, secretKey, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const nsSecretHashKey = nsSecretsKey(nsName);

  if (method === "GET" && secretKey === undefined) {
    const keys = await redis.hKeys(nsSecretHashKey);
    return jsonResponse(200, { namespace: nsName, keys: keys.toSorted() });
  }
  if (method === "PUT" && secretKey !== undefined) {
    const invalidKey = invalidSecretMutationKeyResponse(secretKey);
    if (invalidKey) return invalidKey;
    let put;
    try {
      put = await readEncryptedSecretPutValue({
        request,
        env,
        hashKey: nsSecretHashKey,
        fieldName: secretKey,
      });
    } catch (err) {
      const response = namespaceSecretMutationErrorResponse(err, {
        log, requestId, nsName, secretKey, method,
      });
      if (response) return response;
      throw err;
    }
    if ("response" in put) return put.response;
    try {
      await mutateNamespaceSecret({
        redis,
        env,
        nsName,
        secretKey,
        method: "PUT",
        encrypted: put.encrypted,
        plaintext: put.plaintext,
      });
    } catch (err) {
      const response = namespaceSecretMutationErrorResponse(err, {
        log, requestId, nsName, secretKey, method,
      });
      if (response) return response;
      throw err;
    }
    log("info", "ns_secret_set", { request_id: requestId, namespace: nsName, key: secretKey });
    return jsonResponse(200, {
      namespace: nsName,
      key: secretKey,
      set: true,
      note: "effect on next natural cold-load (new deploy / runtime recycle)",
    });
  }
  if (method === "DELETE" && secretKey !== undefined) {
    const invalidKey = invalidSecretMutationKeyResponse(secretKey);
    if (invalidKey) return invalidKey;
    let result;
    try {
      result = await mutateNamespaceSecret({
        redis,
        env,
        nsName,
        secretKey,
        method: "DELETE",
      });
    } catch (err) {
      const response = namespaceSecretMutationErrorResponse(err, {
        log, requestId, nsName, secretKey, method,
      });
      if (response) return response;
      throw err;
    }
    log("info", "ns_secret_deleted", {
      request_id: requestId,
      namespace: nsName,
      key: secretKey,
      existed: result.mutated,
    });
    return jsonResponse(200, { namespace: nsName, key: secretKey, deleted: result.mutated });
  }
  return jsonError(405, "method_not_allowed", "Method not allowed for /secrets");
}
