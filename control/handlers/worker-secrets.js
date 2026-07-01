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
import { bundleKey } from "shared-version";
import {
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  decryptSecretHash,
} from "control-env-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";

const MAX_SECRET_ATTEMPTS = 5;

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("control-routing").RedisClient} RoutingRedisClient
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
        controlEnv: stringEnv(env),
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
        { log, requestId }
      );
      log("info", method === "PUT" ? "secret_set" : "secret_deleted", {
        request_id: requestId,
        namespace: ns,
        worker: name,
        key,
        previous_version: result.previousVersion,
        new_version: result.version,
      });
      /** @type {{ namespace: string, name: string, key: string, version: string, previousVersion: string, set?: boolean, deleted?: boolean }} */
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
        /** @type {{ namespace: string, name: string, key: string, secretWritten: boolean, reloadForced: boolean, effect: string, warnings: { kind: string, reason: string, nextPickup: string }[], set?: boolean, deleted?: boolean }} */
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

// DELETE extends WATCH to routes + worker-versions so the
// "last key → SREM workers:<ns>" branch can trust its preconditions.
/**
 * @param {{ redis: RedisClient, ns: string, name: string, key: string, method: string, value: string | null, plaintext?: string | null, controlEnv: Record<string, string | undefined> }} args
 */
async function mutateSecret({ redis, ns, name, key, method, value, plaintext = null, controlEnv }) {
  const secretsKey = `secrets:${ns}:${name}`;
  return await runOptimistic(redis, {
    attempts: MAX_SECRET_ATTEMPTS,
    onExhausted: () => {
      throw new SecretAbort(503, "secret_mutation_contention", {
        message: `exhausted ${MAX_SECRET_ATTEMPTS} retries; retry later`,
      });
    },
  }, async (iso) => {
    const watches = [deleteLockKey(ns, name), secretsKey];
    if (method === "DELETE") {
      watches.push(routesKey(ns), workerVersionsKey(ns, name));
    }
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

    const multi = iso.multi();
    if (method === "PUT") {
      if (typeof value !== "string") throw new Error("PUT secret value missing");
      if (typeof plaintext !== "string") throw new Error("PUT secret plaintext missing");
      const activeVersion = await iso.hGet(routesKey(ns), name);
      const nsEncrypted = await iso.hGetAll(`secrets:${ns}`);
      const workerEncrypted = await iso.hGetAll(secretsKey);
      const [nsSecrets, workerSecrets] = await Promise.all([
        decryptSecretHash({ encrypted: nsEncrypted, env: controlEnv, hashKey: `secrets:${ns}` }),
        decryptSecretHash({ encrypted: workerEncrypted, env: controlEnv, hashKey: secretsKey }),
      ]);
      workerSecrets[key] = plaintext;
      let vars = null;
      if (typeof activeVersion === "string" && activeVersion) {
        const rawMeta = await iso.hGet(bundleKey(ns, name, activeVersion), "__meta__");
        const meta = typeof rawMeta === "string" ? JSON.parse(rawMeta) : {};
        vars = meta && typeof meta === "object" ? meta.vars : null;
      }
      assertWorkerLoaderUserEnvBudget({
        ns,
        worker: name,
        vars,
        nsSecrets,
        workerSecrets,
      });
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
    return { mutated: true };
  });
}
