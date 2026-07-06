import {
  jsonResponse,
  jsonError,
  formatError,
  ControlAbort,
  controlAbortResponse,
  requireControlLog,
  requireControlRedis,
  runOptimistic,
  stringEnv,
  codedErrorResponse,
  errMessage,
} from "control-shared";
import {
  deleteLockKey,
  routesKey,
  workerVersionsKey,
} from "control-lib";
import {
  invalidSecretMutationKeyResponse,
  readEncryptedSecretPutValue,
} from "control-handlers-secret-put";
import { stageWorkerHidden, stageWorkerVisible } from "control-lifecycle-indexes";
import { bumpActiveAndPromote, RoutingError } from "control-routing";
import {
  WorkerEnvBudgetError,
  assertWorkerVersionsUserEnvBudget,
  decryptSecretHash,
} from "control-env-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";

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
 * @typedef {{
 *   namespace: string,
 *   name: string,
 *   key: string,
 *   secretWritten: boolean,
 *   reloadForced: boolean,
 *   effect: string,
 *   warnings: { kind: string, reason: string, nextPickup: string }[],
 *   set?: boolean,
 *   deleted?: boolean,
 * }} SecretMutationDeferredPayload
 */

class SecretAbort extends ControlAbort {}

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
  const secretsKey = `secrets:${ns}:${name}`;

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
      const put = await readEncryptedSecretPutValue({
        request,
        env,
        hashKey: secretsKey,
        fieldName: key,
      });
      if ("response" in put) return put.response;
      storedValue = put.encrypted;
      putPlaintext = put.plaintext;
    }

    let mutationResult;
    try {
      mutationResult = await mutateSecret({
        redis, ns, name, key, method,
        value: storedValue,
        plaintext: putPlaintext,
        controlEnv,
      });
    } catch (err) {
      if (err instanceof SecretAbort) {
        log("warn", "secret_mutation_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
          method,
          status: err.status,
          reason: err.code,
        });
        return controlAbortResponse(err);
      }
      if (err instanceof WorkerEnvBudgetError) return codedErrorResponse(err, err.code);
      if (err instanceof SecretEnvelopeError) return jsonError(503, err.code, err.message);
      throw err;
    }

    if (!mutationResult.mutated) {
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

    try {
      const result = await bumpActiveAndPromote(
        /** @type {RoutingRedisClient} */ (redis),
        ns,
        name,
        {
          log,
          requestId,
          beforeStageCopy: ({ iso, currentVersion, newVersion }) =>
            assertWorkerSecretBumpEnvBudget({
              iso,
              ns,
              name,
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
      // 404 → pre-deploy flow: hash stays, first deploy picks it up.
      if (err instanceof RoutingError && err.status === 404) {
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
          note: "stored; will apply on first deploy (no active version to promote)",
        };
        if (method === "PUT") payload.set = true;
        else payload.deleted = true;
        return jsonResponse(200, payload);
      }
      if (err instanceof WorkerEnvBudgetError) {
        const rollback = await rollbackSecretMutation({
          redis,
          ns,
          name,
          key,
          method: /** @type {"PUT" | "DELETE"} */ (method),
          appliedValue: storedValue,
          previousExisted: mutationResult.previousExisted,
          previousValue: mutationResult.previousValue,
          controlEnv,
        });
        /** @type {Record<string, string>} */
        let rollbackFields = {};
        if ("reason" in rollback) {
          rollbackFields = {
            rollback_reason: rollback.reason,
            ...(rollback.errorMessage ? { rollback_error_message: rollback.errorMessage } : {}),
          };
        }
        log("warn", "secret_bump_budget_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
          status: err.status,
          rolled_back: rollback.rolledBack,
          ...rollbackFields,
          ...formatError(err),
        });
        if (!rollback.rolledBack) {
          return jsonError(
            503,
            "secret_mutation_rollback_failed",
            "secret mutation was written, but the version bump failed env budget checks and rollback could not be confirmed; retry the mutation or repair the secret before rollout",
            {
              namespace: ns,
              worker: name,
              key,
              method,
              budget_error: err.code,
              rollbackConfirmed: false,
              secretWritten: true,
            },
          );
        }
        return codedErrorResponse(err, err.code);
      }
      if (err instanceof RoutingError) {
        // Secret already landed in our own MULTI; bump failure degrades
        // to a deferred reload — the secret is picked up on next natural
        // cold-load, or wiped by a concurrent whole-delete.
        log("warn", "secret_bump_promote_rejected", {
          request_id: requestId,
          namespace: ns,
          worker: name,
          key,
          status: err.status,
          ...formatError(err),
        });
        /** @type {SecretMutationDeferredPayload} */
        const payload = {
          namespace: ns,
          name,
          key,
          secretWritten: true,
          reloadForced: false,
          effect: "deferred",
          warnings: [{
            kind: "promote_failed",
            reason: err.message,
            nextPickup: "natural cold-load (runtime recycle, isolate eviction, or next deploy)",
          }],
        };
        if (method === "PUT") payload.set = true;
        else payload.deleted = true;
        return jsonResponse(200, payload);
      }
      throw err;
    }
  }

  return jsonError(405, "method_not_allowed", "Method not allowed for /secrets");
}

// Secret mutations watch routes, worker-versions, and both secret hashes because
// env-budget checks must cover active and retained versions before writing a
// new secret shape.
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
async function mutateSecret({ redis, ns, name, key, method, value, plaintext = null, controlEnv }) {
  const secretsKey = `secrets:${ns}:${name}`;
  const nsSecretsKey = `secrets:${ns}`;
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
      nsSecretsKey,
      secretsKey,
      routesKey(ns),
      workerVersionsKey(ns, name),
    ];
    await iso.watch(...watches);

    const callerLock = await iso.get(deleteLockKey(ns, name));
    if (callerLock) {
      throw new SecretAbort(409, "deleting", {
        namespace: ns, worker: name,
      });
    }

    let removeFromWorkersIndex = false;
    if (method === "DELETE") {
      const hkeys = await iso.hKeys(secretsKey);
      if (!hkeys.includes(key)) {
        return { mutated: false };
      }
      if (hkeys.length === 1 && hkeys[0] === key) {
        const active = await iso.hGet(routesKey(ns), name);
        const verCount = await iso.zCard(workerVersionsKey(ns, name));
        if (!active && verCount === 0) {
          removeFromWorkersIndex = true;
        }
      }
    }

    if (method === "PUT" && typeof plaintext !== "string") throw new Error("PUT secret plaintext missing");
    const activeVersion = await iso.hGet(routesKey(ns), name);
    const retainedVersions = await iso.zRange(workerVersionsKey(ns, name), 0, -1);
    const nsEncrypted = await iso.hGetAll(nsSecretsKey);
    const workerEncrypted = await iso.hGetAll(secretsKey);
    const workerBudgetEncrypted = { ...workerEncrypted };
    delete workerBudgetEncrypted[key];
    const [nsSecrets, workerSecrets] = await Promise.all([
      decryptSecretHash({
        encrypted: nsEncrypted,
        env: controlEnv,
        hashKey: nsSecretsKey,
      }),
      decryptSecretHash({
        encrypted: workerBudgetEncrypted,
        env: controlEnv,
        hashKey: secretsKey,
      }),
    ]);
    if (method === "PUT") {
      workerSecrets[key] = /** @type {string} */ (plaintext);
    }
    await assertWorkerVersionsUserEnvBudget({
      redis: iso,
      ns,
      worker: name,
      versions: [
        ...retainedVersions,
        ...(typeof activeVersion === "string" && activeVersion ? [activeVersion] : []),
      ],
      nsSecrets,
      workerSecrets,
      assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
    });

    const multi = iso.multi();
    if (method === "PUT") {
      if (typeof value !== "string") throw new Error("PUT secret value missing");
      multi.hSet(secretsKey, key, value);
      // SADD even on secret-only / pre-deploy workers so they're
      // visible to GET /workers and reachable by whole-delete.
      stageWorkerVisible(multi, ns, name);
    } else {
      multi.hDel(secretsKey, key);
      if (removeFromWorkersIndex) {
        stageWorkerHidden(multi, ns, name);
      }
    }

    await multi.exec();
    return {
      mutated: true,
      previousExisted: Object.hasOwn(workerEncrypted, key),
      previousValue: typeof workerEncrypted[key] === "string" ? workerEncrypted[key] : null,
    };
  });
}

/**
 * @param {{
 *   redis: RedisClient,
 *   ns: string,
 *   name: string,
 *   key: string,
 *   method: "PUT" | "DELETE",
 *   appliedValue: string | null,
 *   previousExisted?: boolean,
 *   previousValue?: string | null,
 *   controlEnv: Record<string, string | undefined>,
 * }} args
 * @returns {Promise<{ rolledBack: true } | { rolledBack: false, reason: string, errorMessage?: string }>}
 */
async function rollbackSecretMutation({
  redis,
  ns,
  name,
  key,
  method,
  appliedValue,
  previousExisted = false,
  previousValue = null,
  controlEnv,
}) {
  const nsSecretsKey = `secrets:${ns}`;
  const secretsKey = `secrets:${ns}:${name}`;
  /** @param {string} reason @param {unknown} [err] */
  const failed = (reason, err = undefined) => ({
    rolledBack: false,
    reason,
    ...(err === undefined ? {} : { errorMessage: errMessage(err) }),
  });
  try {
    return await runOptimistic(redis, {
      attempts: MAX_SECRET_ATTEMPTS,
      onExhausted: () => failed("contention"),
    }, async (iso) => {
      await iso.watch(
        deleteLockKey(ns, name),
        nsSecretsKey,
        secretsKey,
        routesKey(ns),
        workerVersionsKey(ns, name)
      );
      const callerLock = await iso.get(deleteLockKey(ns, name));
      if (callerLock) return failed("delete_lock");

      const current = await iso.hGet(secretsKey, key);
      if (method === "PUT") {
        if (typeof appliedValue !== "string" || current !== appliedValue) return failed("secret_changed");
      } else if (current != null) {
        return failed("secret_restored");
      }

      const active = await iso.hGet(routesKey(ns), name);
      const retainedVersions = await iso.zRange(workerVersionsKey(ns, name), 0, -1);
      const secretKeys = await iso.hKeys(secretsKey);
      if (!active && retainedVersions.length === 0 && previousExisted) return failed("worker_deleted");

      const nsEncrypted = await iso.hGetAll(nsSecretsKey);
      const workerEncrypted = await iso.hGetAll(secretsKey);
      const workerBudgetEncrypted = { ...workerEncrypted };
      if (previousExisted && typeof previousValue === "string") {
        workerBudgetEncrypted[key] = previousValue;
      } else {
        delete workerBudgetEncrypted[key];
      }
      const nsSecrets = await decryptSecretHash({
        encrypted: nsEncrypted,
        env: controlEnv,
        hashKey: nsSecretsKey,
      });
      const workerSecrets = await decryptSecretHash({
        encrypted: workerBudgetEncrypted,
        env: controlEnv,
        hashKey: secretsKey,
      });
      try {
        await assertWorkerVersionsUserEnvBudget({
          redis: iso,
          ns,
          worker: name,
          versions: [
            ...retainedVersions,
            ...(typeof active === "string" && active ? [active] : []),
          ],
          nsSecrets,
          workerSecrets,
          assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
        });
      } catch (err) {
        return failed("budget_rejected", err);
      }

      const multi = iso.multi();
      if (previousExisted && typeof previousValue === "string") {
        multi.hSet(secretsKey, key, previousValue);
        stageWorkerVisible(multi, ns, name);
      } else {
        multi.hDel(secretsKey, key);
        if (
          method === "PUT" &&
          secretKeys.length <= 1 &&
          !active &&
          retainedVersions.length === 0
        ) {
          stageWorkerHidden(multi, ns, name);
        }
      }
      await multi.exec();
      return { rolledBack: true };
    });
  } catch (err) {
    return failed("error", err);
  }
}

/**
 * @param {{
 *   iso: {
 *     watch: (...keys: string[]) => Promise<unknown>,
 *     hGet: (key: string, field: string) => Promise<string | null | undefined>,
 *     hGetAll: (key: string) => Promise<Record<string, string | null | undefined>>,
 *   },
 *   ns: string,
 *   name: string,
 *   currentVersion: string,
 *   newVersion: string,
 *   controlEnv: Record<string, string | undefined>,
 * }} args
 */
async function assertWorkerSecretBumpEnvBudget({
  iso,
  ns,
  name,
  currentVersion,
  newVersion,
  controlEnv,
}) {
  const nsSecretsKey = `secrets:${ns}`;
  const workerSecretsKey = `secrets:${ns}:${name}`;
  await iso.watch(nsSecretsKey, workerSecretsKey);

  // Keep reads sequential: RedisSession is a single RESP stream.
  const nsEncrypted = await iso.hGetAll(nsSecretsKey);
  const workerEncrypted = await iso.hGetAll(workerSecretsKey);
  const nsSecrets = await decryptSecretHash({
    encrypted: nsEncrypted,
    env: controlEnv,
    hashKey: nsSecretsKey,
  });
  const workerSecrets = await decryptSecretHash({
    encrypted: workerEncrypted,
    env: controlEnv,
    hashKey: workerSecretsKey,
  });

  await assertWorkerVersionsUserEnvBudget({
    redis: iso,
    ns,
    worker: name,
    versions: [],
    versionEstimates: [{
      sourceVersion: currentVersion,
      estimatedVersion: newVersion,
    }],
    nsSecrets,
    workerSecrets,
    assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
  });
}
