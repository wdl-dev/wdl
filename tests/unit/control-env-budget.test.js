import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { encryptSecretValue } from "../../shared/secret-envelope.js";

const secretEnvelopeUrl = repositoryFileUrl("shared/secret-envelope.js");
const sharedVersionUrl = repositoryFileUrl("shared/version.js");
const {
  UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES,
  WORKER_LOADER_ENV_HEADROOM_BYTES,
  WORKER_LOADER_ENV_MAX_BYTES,
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  assertWorkerVersionsUserEnvBudget,
  decryptSecretHash,
  mergedUserEnvStrings,
  userEnvSerializedBytes,
} = await importRepositoryModule("control/env-budget.js", importSpecifierReplacements({
  "shared-secret-envelope": secretEnvelopeUrl,
  "shared-version": sharedVersionUrl,
}));

const envelopeEnv = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
  SECRET_ENVELOPE_KID: "local:test:secret-envelope:v1",
};

test("worker env budget counts merged vars and secrets with worker-secret precedence", () => {
  const merged = mergedUserEnvStrings({
    vars: { TOKEN: "var", ONLY_VAR: "v" },
    nsSecrets: { TOKEN: "ns", ONLY_NS: "n" },
    workerSecrets: { TOKEN: "worker" },
  });

  assert.deepEqual(merged, {
    TOKEN: "worker",
    ONLY_VAR: "v",
    ONLY_NS: "n",
  });
  assert.equal(userEnvSerializedBytes(merged), Buffer.byteLength(JSON.stringify(merged), "utf8"));
});

test("worker env budget rejects user-controlled env above workerd workerLoader limit", () => {
  assert.equal(WORKER_LOADER_ENV_MAX_BYTES, UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES - WORKER_LOADER_ENV_HEADROOM_BYTES);
  assert.throws(
    () => assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      worker: "api",
      vars: { BIG: "x".repeat(WORKER_LOADER_ENV_MAX_BYTES) },
    }),
    (err) => {
      if (!(err instanceof WorkerEnvBudgetError)) return false;
      const budgetErr = /** @type {InstanceType<typeof WorkerEnvBudgetError>} */ (err);
      assert.equal(budgetErr.code, "worker_env_too_large");
      assert.equal(budgetErr.status, 400);
      assert.equal(budgetErr.details.namespace, "demo");
      assert.equal(budgetErr.details.worker, "api");
      assert.equal(budgetErr.details.upstream_max_env_bytes, UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES);
      assert.equal(budgetErr.details.headroom_bytes, WORKER_LOADER_ENV_HEADROOM_BYTES);
      return true;
    }
  );
});

test("decryptSecretHash returns plaintext secret values for budget checks", async () => {
  const hashKey = "secrets:demo";
  const encrypted = await encryptSecretValue("plain", {
    env: envelopeEnv,
    hashKey,
    fieldName: "TOKEN",
  });

  assert.deepEqual(
    {
      ...(await decryptSecretHash({
        encrypted: { TOKEN: encrypted, MISSING: null },
        env: envelopeEnv,
        hashKey,
      })),
    },
    { TOKEN: "plain" }
  );
});

test("worker env budget checks every retained worker version", async () => {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(field, "__meta__");
      if (key === "worker:demo:api:v:1") return JSON.stringify({ vars: { SMALL: "ok" } });
      if (key === "worker:demo:api:v:2") return JSON.stringify({ vars: { BIG: "x".repeat(WORKER_LOADER_ENV_MAX_BYTES) } });
      return null;
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: ["v1", "v2"],
      nsSecrets: { TOKEN: "secret" },
    }),
    (err) => {
      assert.equal(err instanceof WorkerEnvBudgetError, true);
      assert.equal(/** @type {WorkerEnvBudgetError} */ (err).code, "worker_env_too_large");
      return true;
    }
  );
});

test("worker env budget reports bundle metadata parse context", async () => {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(key, "worker:demo:api:v:1");
      assert.equal(field, "__meta__");
      return "{not-json";
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: ["v1"],
    }),
    /invalid bundle metadata for demo\/api@v1/
  );
});
