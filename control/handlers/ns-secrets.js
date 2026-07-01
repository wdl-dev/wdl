import {
  jsonResponse,
  jsonError,
  requireControlLog,
  requireControlRedis,
  stringEnv,
  codedErrorResponse,
  runOptimistic,
  ControlAbort,
  controlAbortResponse,
} from "control-shared";
import {
  invalidSecretMutationKeyResponse,
  readEncryptedSecretPutValue,
} from "control-handlers-secret-put";
import { routesKey, workerVersionsKey } from "shared-version";
import { workersIndexKey } from "control-lib";
import {
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  assertWorkerVersionsUserEnvBudget,
  decryptSecretHash,
} from "control-env-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";

const MAX_NS_SECRET_ATTEMPTS = 5;

class NamespaceSecretAbort extends ControlAbort {}

/**
 * @param {{
 *   redis: import("shared-redis").RedisSession,
 *   controlEnv: Record<string, string | undefined>,
 *   nsName: string,
 *   nsSecrets: Record<string, string>,
 * }} args
 */
async function validateNamespaceSecretBudget({ redis, controlEnv, nsName, nsSecrets }) {
  const activeRoutes = await redis.hGetAll(routesKey(nsName));
  const indexedWorkers = await redis.sMembers(workersIndexKey(nsName));
  const workerNames = new Set([
    ...indexedWorkers.filter((worker) => typeof worker === "string" && worker),
    ...Object.keys(activeRoutes),
  ]);
  if (workerNames.size === 0) {
    assertWorkerLoaderUserEnvBudget({ ns: nsName, nsSecrets });
    return;
  }

  for (const worker of workerNames) {
    const workerSecretsKey = `secrets:${nsName}:${worker}`;
    await redis.watch(workerVersionsKey(nsName, worker), workerSecretsKey);
    const activeVersion = activeRoutes[worker];
    const retainedVersions = await redis.zRange(workerVersionsKey(nsName, worker), 0, -1);
    const workerEncrypted = await redis.hGetAll(workerSecretsKey);
    const workerSecrets = await decryptSecretHash({
      encrypted: workerEncrypted,
      env: controlEnv,
      hashKey: workerSecretsKey,
    });
    await assertWorkerVersionsUserEnvBudget({
      redis,
      ns: nsName,
      worker,
      versions: [
        ...retainedVersions,
        ...(typeof activeVersion === "string" && activeVersion ? [activeVersion] : []),
      ],
      nsSecrets,
      workerSecrets,
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
  const nsSecretsKey = `secrets:${nsName}`;
  return await runOptimistic(redis, {
    attempts: MAX_NS_SECRET_ATTEMPTS,
    onExhausted: () => {
      throw new NamespaceSecretAbort(503, "namespace_secret_mutation_contention", {
        message: `exhausted ${MAX_NS_SECRET_ATTEMPTS} retries; retry later`,
      });
    },
  }, async (iso) => {
    await iso.watch(nsSecretsKey, routesKey(nsName), workersIndexKey(nsName));

    const existingEncrypted = await iso.hGetAll(nsSecretsKey);
    if (method === "DELETE" && !Object.hasOwn(existingEncrypted, secretKey)) {
      return { mutated: false };
    }

    const budgetEncrypted = { ...existingEncrypted };
    delete budgetEncrypted[secretKey];
    const nsSecrets = await decryptSecretHash({
      encrypted: budgetEncrypted,
      env: controlEnv,
      hashKey: nsSecretsKey,
    });
    if (method === "PUT") {
      if (typeof encrypted !== "string") throw new Error("PUT namespace secret encrypted value missing");
      if (typeof plaintext !== "string") throw new Error("PUT namespace secret plaintext missing");
      nsSecrets[secretKey] = plaintext;
    } else {
      delete nsSecrets[secretKey];
    }

    await validateNamespaceSecretBudget({
      redis: iso,
      controlEnv,
      nsName,
      nsSecrets,
    });

    const multi = iso.multi();
    if (method === "PUT") {
      multi.hSet(nsSecretsKey, secretKey, /** @type {string} */ (encrypted));
    } else {
      multi.hDel(nsSecretsKey, secretKey);
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
  const nsSecretsKey = `secrets:${nsName}`;

  if (method === "GET" && secretKey === undefined) {
    const keys = await redis.hKeys(nsSecretsKey);
    return jsonResponse(200, { namespace: nsName, keys: keys.toSorted() });
  }
  if (method === "PUT" && secretKey !== undefined) {
    const invalidKey = invalidSecretMutationKeyResponse(secretKey);
    if (invalidKey) return invalidKey;
    const put = await readEncryptedSecretPutValue({
      request,
      env,
      hashKey: nsSecretsKey,
      fieldName: secretKey,
    });
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
      if (err instanceof NamespaceSecretAbort) return controlAbortResponse(err);
      if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
      if (err instanceof SecretEnvelopeError) return jsonError(503, err.code, err.message);
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
      if (err instanceof NamespaceSecretAbort) return controlAbortResponse(err);
      if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
      if (err instanceof SecretEnvelopeError) return jsonError(503, err.code, err.message);
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
