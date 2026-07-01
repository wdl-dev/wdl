import {
  jsonResponse,
  jsonError,
  requireControlLog,
  requireControlRedis,
  stringEnv,
  codedErrorResponse,
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

/**
 * @param {{
 *   redis: import("shared-redis").RedisClient,
 *   env: Record<string, unknown>,
 *   nsName: string,
 *   secretKey: string,
 *   plaintext: string,
 * }} args
 */
async function validateNamespaceSecretBudget({ redis, env, nsName, secretKey, plaintext }) {
  const controlEnv = stringEnv(env);
  const nsSecretsKey = `secrets:${nsName}`;
  const existingEncrypted = await redis.hGetAll(nsSecretsKey);
  const nsSecrets = await decryptSecretHash({
    encrypted: existingEncrypted,
    env: controlEnv,
    hashKey: nsSecretsKey,
  });
  nsSecrets[secretKey] = plaintext;

  const [activeRoutes, indexedWorkers] = await Promise.all([
    redis.hGetAll(routesKey(nsName)),
    redis.sMembers(workersIndexKey(nsName)),
  ]);
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
      await validateNamespaceSecretBudget({
        redis,
        env,
        nsName,
        secretKey,
        plaintext: put.plaintext,
      });
    } catch (err) {
      if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
      if (err instanceof SecretEnvelopeError) return jsonError(503, err.code, err.message);
      throw err;
    }
    const encrypted = put.encrypted;
    await redis.hSet(nsSecretsKey, secretKey, encrypted);
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
    const removed = Number(await redis.hDel(nsSecretsKey, secretKey)) > 0;
    log("info", "ns_secret_deleted", {
      request_id: requestId,
      namespace: nsName,
      key: secretKey,
      existed: removed,
    });
    return jsonResponse(200, { namespace: nsName, key: secretKey, deleted: removed });
  }
  return jsonError(405, "method_not_allowed", "Method not allowed for /secrets");
}
