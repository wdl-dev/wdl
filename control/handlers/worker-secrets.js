import {
  jsonResponse,
  jsonError,
  ControlAbort,
  controlAbortResponse,
  errMessage,
  requireControlLog,
  requireControlRedis,
  runOptimistic,
  stringEnv,
  codedErrorLogFields,
  codedErrorResponse,
  secretEnvelopeErrorResponse,
} from "control-shared";
import { workflowDefsKey } from "control-lib";
import {
  invalidSecretMutationKeyResponse,
  readEncryptedSecretPutValue,
} from "control-handlers-secret-put";
import { stageWorkerHidden, stageWorkerVisible } from "control-lifecycle-indexes";
import { bumpActiveAndPromote, RoutingError } from "control-routing";
import {
  BundleMetaError,
  WorkerEnvBudgetError,
  assertWorkerVersionsUserEnvBudget,
  decryptMutatedSecretHashForBudget,
  decryptSecretHash,
} from "control-env-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";
import { nsSecretsKey, workerSecretsKey } from "shared-secret-keys";
import { deleteLockKey, routesKey, workerVersionsKey } from "shared-worker-contract";

const MAX_SECRET_ATTEMPTS = 5;

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("control-routing").RedisClient} RoutingRedisClient
 * @typedef {{
 *   namespace: string,
 *   name: string,
 *   key: string,
 *   version: string,
 *   previousVersion: string,
 *   set?: boolean,
 *   deleted?: boolean,
 * }} SecretMutationVersionPayload
 */

class SecretAbort extends ControlAbort {}
class SecretNoop extends Error {}

function secretMutationContentionAbort() {
  return new SecretAbort(503, "secret_mutation_contention", {
    message: "active version changed during secret mutation; retry later",
  });
}

/**
 * @param {{
 *   abort: ControlAbort,
 *   requestId: string,
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: string,
 *   log: (level: string, event: string, fields: Record<string, unknown>) => void,
 * }} args
 */
function rejectSecretMutation({ abort, requestId, ns, name, key, method, log }) {
  log(abort.status >= 500 ? "error" : "warn", "secret_mutation_rejected", {
    request_id: requestId,
    namespace: ns,
    worker: name,
    key,
    method,
    ...codedErrorLogFields(abort),
  });
  return controlAbortResponse(abort);
}

/**
 * @param {{
 *   err: SecretEnvelopeError,
 *   requestId: string,
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: string,
 *   log: (level: string, event: string, fields: Record<string, unknown>) => void,
 * }} args
 */
function rejectSecretEnvelopeMutation({ err, requestId, ns, name, key, method, log }) {
  return secretEnvelopeErrorResponse({
    err,
    log,
    event: "secret_mutation_rejected",
    fields: {
      request_id: requestId,
      namespace: ns,
      worker: name,
      key,
      method,
    },
  });
}

/**
 * @param {{
 *   request: Request,
 *   env: Record<string, unknown>,
 *   method: string,
 *   ns: string,
 *   name: string,
 *   subPath: string[],
 *   requestId: string,
 * }} args
 */
export async function handle({ request, env, method, ns, name, subPath, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const secretsKey = workerSecretsKey(ns, name);

  if (method === "GET" && subPath.length === 0) {
    const keys = await redis.hKeys(secretsKey);
    return jsonResponse(200, { namespace: ns, name, keys: keys.toSorted() });
  }

  if ((method === "PUT" || method === "DELETE") && subPath.length === 1) {
    const key = subPath[0];
    const invalidKey = invalidSecretMutationKeyResponse(key);
    if (invalidKey) return invalidKey;
    const controlEnv = stringEnv(env);

    let storedValue = null;
    let putPlaintext = null;
    if (method === "PUT") {
      let put;
      try {
        put = await readEncryptedSecretPutValue({
          request,
          env,
          hashKey: secretsKey,
          fieldName: key,
        });
      } catch (err) {
        if (err instanceof SecretEnvelopeError) {
          return rejectSecretEnvelopeMutation({ err, requestId, ns, name, key, method, log });
        }
        throw err;
      }
      if ("response" in put) return put.response;
      storedValue = put.encrypted;
      putPlaintext = put.plaintext;
    }

    try {
      if (method === "DELETE" && await workerSecretDeleteWouldNoop({ redis, ns, name, key })) {
        return workerSecretNoopResponse({ requestId, ns, name, key, log });
      }

      for (let attempt = 0; attempt < MAX_SECRET_ATTEMPTS; attempt += 1) {
        try {
          const result = await bumpActiveAndPromote(
            /** @type {RoutingRedisClient} */ (redis),
            ns,
            name,
            {
              log,
              requestId,
              stageBeforeCopy: ({ iso, multi, currentVersion, newVersion }) =>
                stageWorkerSecretForBump({
                  iso,
                  multi,
                  ns,
                  name,
                  key,
                  method: /** @type {"PUT" | "DELETE"} */ (method),
                  value: storedValue,
                  plaintext: putPlaintext,
                  currentVersion,
                  newVersion,
                  controlEnv,
                }),
            }
          );
          log("info", method === "PUT" ? "secret_set" : "secret_deleted", {
            request_id: requestId,
            namespace: ns,
            worker: name,
            key,
            previous_version: result.previousVersion,
            new_version: result.version,
          });
          /** @type {SecretMutationVersionPayload} */
          const payload = {
            namespace: ns,
            name,
            key,
            version: result.version,
            previousVersion: result.previousVersion,
          };
          if (method === "PUT") payload.set = true;
          else payload.deleted = true;
          return jsonResponse(200, payload);
        } catch (err) {
          if (err instanceof SecretNoop) return workerSecretNoopResponse({ requestId, ns, name, key, log });
          if (!(err instanceof RoutingError && err.status === 404)) throw err;
        }

        const preDeploy = await mutateSecretWithoutActive({
          redis,
          ns,
          name,
          key,
          method: /** @type {"PUT" | "DELETE"} */ (method),
          value: storedValue,
          plaintext: putPlaintext,
          controlEnv,
        });
        if (preDeploy.activePresent) continue;
        if (!preDeploy.mutated) return workerSecretNoopResponse({ requestId, ns, name, key, log });
        log("info",
          method === "PUT" ? "secret_set_pre_deploy" : "secret_deleted_pre_deploy", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
        });
        /** @type {{ namespace: string, name: string, key: string, note: string, set?: boolean, deleted?: boolean }} */
        const payload = {
          namespace: ns,
          name,
          key,
          note: "stored; will apply on next load or deploy (no active version to promote)",
        };
        if (method === "PUT") payload.set = true;
        else payload.deleted = true;
        return jsonResponse(200, payload);
      }
      throw secretMutationContentionAbort();
    } catch (err) {
      if (err instanceof SecretAbort) {
        return rejectSecretMutation({ abort: err, requestId, ns, name, key, method, log });
      }
      if (err instanceof BundleMetaError) {
        log("error", "secret_mutation_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
          method,
          ...codedErrorLogFields(err, err.code, { errorDetail: errMessage(err.cause) }),
        });
        return codedErrorResponse(err, err.code);
      }
      if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
      if (err instanceof RoutingError) {
        if (err.code === "bump_contention") {
          return rejectSecretMutation({
            abort: secretMutationContentionAbort(),
            requestId,
            ns,
            name,
            key,
            method,
            log,
          });
        }
        if (err.code === "caller_deleting") {
          const abort = new SecretAbort(409, "deleting", {
            namespace: ns, worker: name,
          });
          return rejectSecretMutation({ abort, requestId, ns, name, key, method, log });
        }
        log(err.status >= 500 ? "error" : "warn", "secret_mutation_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
          method,
          ...codedErrorLogFields(err, "routing_error"),
        });
        return codedErrorResponse(err, err.code);
      }
      if (err instanceof SecretEnvelopeError) {
        return rejectSecretEnvelopeMutation({ err, requestId, ns, name, key, method, log });
      }
      throw err;
    }
  }

  return jsonError(405, "method_not_allowed", "Method not allowed for /secrets");
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, key: string }} args
 */
async function workerSecretDeleteWouldNoop({ redis, ns, name, key }) {
  const secretsKey = workerSecretsKey(ns, name);
  return await redis.session(async (iso) => {
    const callerLock = await iso.get(deleteLockKey(ns, name));
    if (callerLock) {
      throw new SecretAbort(409, "deleting", {
        namespace: ns, worker: name,
      });
    }
    return !(await iso.hExists(secretsKey, key));
  });
}

/**
 * @param {{ requestId: string, ns: string, name: string, key: string, log: (level: string, event: string, fields: Record<string, unknown>) => void }} args
 */
function workerSecretNoopResponse({ requestId, ns, name, key, log }) {
  log("info", "secret_deleted_noop", {
    request_id: requestId,
    namespace: ns,
    worker: name,
    key,
  });
  return jsonResponse(200, {
    namespace: ns, name, key, deleted: false,
  });
}

/**
 * @param {import("shared-redis").RedisMulti} multi
 * @param {{
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   value: string | null,
 *   active: boolean,
 *   removeFromWorkersIndex?: boolean,
 * }} args
 */
function stageWorkerSecretMutation(multi, { ns, name, key, method, value, active, removeFromWorkersIndex = false }) {
  const secretsKey = workerSecretsKey(ns, name);
  if (method === "PUT") {
    if (typeof value !== "string") throw new Error("PUT secret value missing");
    multi.hSet(secretsKey, key, value);
    stageWorkerVisible(multi, ns, name);
  } else {
    multi.hDel(secretsKey, key);
    if (!active && removeFromWorkersIndex) {
      stageWorkerHidden(multi, ns, name);
    }
  }
}

/**
 * @param {{
 *   nsEncrypted: Record<string, string | null | undefined>,
 *   workerEncrypted: Record<string, string | null | undefined>,
 *   controlEnv: Record<string, string | undefined>,
 *   nsSecretHashKey: string,
 *   workerSecretHashKey: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   plaintext?: string | null,
 * }} args
 */
async function decryptBudgetSecrets({
  nsEncrypted,
  workerEncrypted,
  controlEnv,
  nsSecretHashKey,
  workerSecretHashKey,
  key,
  method,
  plaintext = null,
}) {
  const [nsSecrets, workerSecrets] = await Promise.all([
    decryptSecretHash({
      encrypted: nsEncrypted,
      env: controlEnv,
      hashKey: nsSecretHashKey,
    }),
    decryptMutatedSecretHashForBudget({
      encrypted: workerEncrypted,
      env: controlEnv,
      hashKey: workerSecretHashKey,
      key,
      method,
      plaintext,
    }),
  ]);
  return { nsSecrets, workerSecrets };
}

/**
 * @param {{
 *   iso: {
 *     watch: (...keys: string[]) => Promise<unknown>,
 *     hGetMany: (pairs: Array<[string, string]>) => Promise<Array<string | null | undefined>>,
 *     hGetAllMany: (keys: string[]) => Promise<Array<Record<string, string | null | undefined>>>,
 *     zRange: (key: string, start: number, stop: number) => Promise<string[]>,
 *   },
 *   multi: import("shared-redis").RedisMulti,
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   value: string | null,
 *   plaintext?: string | null,
 *   currentVersion: string,
 *   newVersion: string,
 *   controlEnv: Record<string, string | undefined>,
 * }} args
 */
async function stageWorkerSecretForBump({
  iso,
  multi,
  ns,
  name,
  key,
  method,
  value,
  plaintext = null,
  currentVersion,
  newVersion,
  controlEnv,
}) {
  const secretsKey = workerSecretsKey(ns, name);
  const nsSecretHashKey = nsSecretsKey(ns);
  const versionsKey = workerVersionsKey(ns, name);
  await iso.watch(nsSecretHashKey, secretsKey, versionsKey);

  const retainedVersions = await iso.zRange(versionsKey, 0, -1);
  const [nsEncrypted, workerEncrypted] = await iso.hGetAllMany([nsSecretHashKey, secretsKey]);
  if (method === "DELETE" && !Object.hasOwn(workerEncrypted, key)) {
    throw new SecretNoop();
  }
  const { nsSecrets, workerSecrets } = await decryptBudgetSecrets({
    nsEncrypted,
    workerEncrypted,
    controlEnv,
    nsSecretHashKey,
    workerSecretHashKey: secretsKey,
    key,
    method,
    plaintext,
  });

  await assertWorkerVersionsUserEnvBudget({
    redis: iso,
    ns,
    worker: name,
    versions: retainedVersions,
    versionEstimates: [{
      sourceVersion: currentVersion,
      estimatedVersion: newVersion,
    }],
    nsSecrets,
    workerSecrets,
    assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
  });

  stageWorkerSecretMutation(multi, {
    ns,
    name,
    key,
    method,
    value,
    active: true,
  });
}

// No-active secret mutations write the secret hash directly. Active workers use
// stageWorkerSecretForBump() so the secret write, bundle copy, and route flip
// commit in one WATCH/MULTI transaction.
/**
 * @param {{
 *   redis: RedisClient,
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   value: string | null,
 *   plaintext?: string | null,
 *   controlEnv: Record<string, string | undefined>,
 * }} args
 */
async function mutateSecretWithoutActive({ redis, ns, name, key, method, value, plaintext = null, controlEnv }) {
  const secretsKey = workerSecretsKey(ns, name);
  const nsSecretHashKey = nsSecretsKey(ns);
  const defsKey = workflowDefsKey(ns, name);
  return await runOptimistic(redis, {
    attempts: MAX_SECRET_ATTEMPTS,
    onExhausted: () => {
      throw new SecretAbort(503, "secret_mutation_contention", {
        message: `exhausted ${MAX_SECRET_ATTEMPTS} retries; retry later`,
      });
    },
  }, async (iso) => {
    const watches = [
      deleteLockKey(ns, name),
      nsSecretHashKey,
      secretsKey,
      routesKey(ns),
      workerVersionsKey(ns, name),
    ];
    if (method === "DELETE") watches.push(defsKey);
    await iso.watch(...watches);

    const callerLock = await iso.get(deleteLockKey(ns, name));
    if (callerLock) {
      throw new SecretAbort(409, "deleting", {
        namespace: ns, worker: name,
      });
    }

    const activeVersion = await iso.hGet(routesKey(ns), name);
    if (activeVersion) return { activePresent: true, mutated: false };

    const retainedVersions = await iso.zRange(workerVersionsKey(ns, name), 0, -1);
    let removeFromWorkersIndex = false;
    if (method === "DELETE") {
      const hkeys = await iso.hKeys(secretsKey);
      if (!hkeys.includes(key)) {
        return { activePresent: false, mutated: false };
      }
      if (hkeys.length === 1 && hkeys[0] === key) {
        if (retainedVersions.length === 0) {
          removeFromWorkersIndex = (await iso.exists(defsKey)) === 0;
        }
      }
    }

    const [nsEncrypted, workerEncrypted] = await iso.hGetAllMany([nsSecretHashKey, secretsKey]);
    const { nsSecrets, workerSecrets } = await decryptBudgetSecrets({
      nsEncrypted,
      workerEncrypted,
      controlEnv,
      nsSecretHashKey,
      workerSecretHashKey: secretsKey,
      key,
      method,
      plaintext,
    });
    await assertWorkerVersionsUserEnvBudget({
      redis: iso,
      ns,
      worker: name,
      versions: retainedVersions,
      nsSecrets,
      workerSecrets,
      assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
    });

    const multi = iso.multi();
    stageWorkerSecretMutation(multi, {
      ns,
      name,
      key,
      method,
      value,
      active: false,
      removeFromWorkersIndex,
    });

    await multi.exec();
    return { activePresent: false, mutated: true };
  });
}
