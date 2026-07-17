import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { encryptSecretValue } from "../../shared/secret-envelope.js";

const { libUrl: controlLibUrl } = await compileControlGraph();
const secretEnvelopeUrl = repositoryFileUrl("shared/secret-envelope.js");
const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");
const sharedRedisUrl = sharedRedisStubUrl();
const runtimeEnvBuildUrl = repositoryModuleDataUrl(
  "runtime/load/env-build.js",
  importSpecifierReplacements({
    "shared-ns-pattern": repositoryFileUrl("shared/ns-pattern.js"),
    "shared-worker-contract": repositoryFileUrl("shared/worker-contract.js"),
  })
);
const {
  UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES,
  WORKER_LOADER_ENV_HEADROOM_BYTES,
  WORKER_LOADER_ENV_MAX_BYTES,
  BundleMetaError,
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  assertWorkerVersionsUserEnvBudget,
  decryptSecretHash,
  estimatedWorkerLoaderEnv,
  estimatedWorkerLoaderEnvBytes,
  WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
} = await importRepositoryModule("control/env-budget.js", importSpecifierReplacements({
  "control-lib": controlLibUrl,
  "runtime-load-env-build": runtimeEnvBuildUrl,
  "shared-secret-envelope": secretEnvelopeUrl,
  "shared-worker-contract": workerContractUrl,
  "shared-redis": sharedRedisUrl,
}));

const envelopeEnv = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
  SECRET_ENVELOPE_KID: "local:test:secret-envelope:v1",
};

/** @param {unknown} err @param {string} reason */
function assertBundleMetaError(err, reason) {
  assert.ok(err instanceof BundleMetaError);
  const bundleError = /** @type {Error & { status: number, code: string, details: Record<string, string>, cause: unknown }} */ (err);
  assert.equal(bundleError.status, 500);
  assert.equal(bundleError.code, "corrupt_meta");
  assert.equal(bundleError.message, "Corrupt __meta__ for demo/api/v1");
  assert.deepEqual(bundleError.details, { namespace: "demo", worker: "api", version: "v1" });
  assert.ok(bundleError.cause instanceof Error);
  assert.equal(bundleError.cause.message, reason);
  return true;
}

test("worker env budget counts merged vars and secrets with worker-secret precedence", () => {
  const estimated = estimatedWorkerLoaderEnv({
    ns: "demo",
    vars: { TOKEN: "var", ONLY_VAR: "v" },
    nsSecrets: { TOKEN: "ns", ONLY_NS: "n" },
    workerSecrets: { TOKEN: "worker" },
  });

  assert.deepEqual(estimated, {
    TOKEN: "worker",
    ONLY_VAR: "v",
    ONLY_NS: "n",
  });
  assert.equal(
    assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      vars: { TOKEN: "var", ONLY_VAR: "v" },
      nsSecrets: { TOKEN: "ns", ONLY_NS: "n" },
      workerSecrets: { TOKEN: "worker" },
    }),
    estimatedWorkerLoaderEnvBytes(estimated)
  );
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

test("worker env budget reports deploy version separately from retained source versions", () => {
  assert.throws(
    () => assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      worker: "api",
      version: "v7",
      vars: { BIG: "x".repeat(WORKER_LOADER_ENV_MAX_BYTES) },
    }),
    (err) => {
      if (!(err instanceof WorkerEnvBudgetError)) return false;
      const budgetErr = /** @type {WorkerEnvBudgetError} */ (err);
      assert.match(budgetErr.message, /demo\/api@v7/);
      assert.equal(budgetErr.details.version, "v7");
      assert.equal(Object.hasOwn(budgetErr.details, "source_version"), false);
      assert.equal(Object.hasOwn(budgetErr.details, "estimated_version"), false);
      return true;
    }
  );
});

test("worker env budget counts required caller secret copies in service binding props", () => {
  const secret = "x".repeat(Math.floor(WORKER_LOADER_ENV_MAX_BYTES * 0.6));

  assert.throws(
    () => assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      worker: "caller",
      vars: { SMALL: "ok" },
      nsSecrets: { API_TOKEN: secret },
      meta: {
        bindings: {
          PLATFORM: {
            type: "service",
            ns: "__platform__",
            service: "platformApi",
            version: "v1",
            entrypoint: "Api",
            requiredCallerSecrets: ["API_TOKEN"],
          },
        },
      },
    }),
    (err) => {
      assert.equal(err instanceof WorkerEnvBudgetError, true);
      assert.equal(/** @type {WorkerEnvBudgetError} */ (err).code, "worker_env_too_large");
      return true;
    }
  );
});

test("worker env budget accounts for V8 two-byte strings on mixed non-Latin-1 env", () => {
  const mixed = `${"x".repeat(Math.floor(WORKER_LOADER_ENV_MAX_BYTES / 2) + 1)}中`;
  const estimated = estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "api",
    vars: { BIG: mixed },
  });
  const jsonBytes = Buffer.byteLength(JSON.stringify(estimated), "utf8");
  const estimatedBytes = estimatedWorkerLoaderEnvBytes(estimated);

  assert.ok(jsonBytes <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(estimatedBytes > WORKER_LOADER_ENV_MAX_BYTES);
  assert.throws(
    () => assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      worker: "api",
      vars: { BIG: mixed },
    }),
    (err) => {
      if (!(err instanceof WorkerEnvBudgetError)) return false;
      const budgetErr = /** @type {WorkerEnvBudgetError} */ (err);
      assert.equal(budgetErr.code, "worker_env_too_large");
      assert.equal(budgetErr.details.env_bytes, estimatedBytes);
      return true;
    }
  );
});

test("worker env budget stores required caller secrets in a JSRPC-serializable object", () => {
  const estimated = estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "caller",
    nsSecrets: { API_TOKEN: "secret" },
    meta: {
      bindings: {
        PLATFORM: {
          type: "service",
          ns: "__platform__",
          service: "platformApi",
          version: "v1",
          requiredCallerSecrets: ["API_TOKEN"],
        },
      },
    },
  });
  const callerSecrets = /** @type {any} */ (estimated.PLATFORM).props.callerSecrets;

  assert.equal(Object.getPrototypeOf(callerSecrets), Object.prototype);
  assert.deepEqual(callerSecrets, { API_TOKEN: "secret" });
});

test("worker env budget counts configured assets CDN base", () => {
  const meta = {
    vars: { PAD: "" },
    assets: { prefix: "assets/demo/api/token/" },
    bindings: {
      ASSETS: { type: "assets" },
    },
  };
  const assetsCdnBase = `https://${"assets-subdomain-".repeat(600)}example.test`;
  /** @param {number} padLength @param {string | null | undefined} cdnBase */
  const bytesWithPad = (padLength, cdnBase) => estimatedWorkerLoaderEnvBytes(estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "api",
    vars: { PAD: "x".repeat(padLength) },
    meta,
    assetsCdnBase: cdnBase,
  }));
  const padLength = WORKER_LOADER_ENV_MAX_BYTES - bytesWithPad(0, assetsCdnBase) + 1;

  assert.ok(bytesWithPad(padLength, null) <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(bytesWithPad(padLength, assetsCdnBase) > WORKER_LOADER_ENV_MAX_BYTES);
  assert.throws(
    () => assertWorkerLoaderUserEnvBudget({
      ns: "demo",
      worker: "api",
      vars: { PAD: "x".repeat(padLength) },
      meta,
      assetsCdnBase,
    }),
    (err) => {
      assert.equal(err instanceof WorkerEnvBudgetError, true);
      assert.equal(/** @type {WorkerEnvBudgetError} */ (err).code, "worker_env_too_large");
      return true;
    }
  );
});

test("worker env budget includes do-runtime alarm binding for Durable Object workers", () => {
  const estimated = estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "api",
    version: "v12",
    meta: {
      bindings: {
        ROOM: {
          type: "do",
          className: "Room",
          doStorageId: "do_0123456789abcdef0123456789abcdef",
        },
      },
    },
  });

  assert.deepEqual(estimated.ROOM, {
    __wdlBinding: "do",
    ns: "demo",
    worker: "api",
    version: "v12",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
    hostProxy: {
      __wdlBinding: "do-host-proxy",
      props: {
        ns: "demo",
        worker: "api",
        version: "v12",
        doStorageId: "do_0123456789abcdef0123456789abcdef",
        binding: "ROOM",
        className: "Room",
      },
    },
  });
  assert.deepEqual(estimated.__WDL_DO_ALARMS__, {
    __wdlBinding: "do-alarms",
    props: {
      ns: "demo",
      worker: "api",
      version: "v12",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
    },
  });
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

test("decryptSecretHash fails closed on corrupt envelopes", async () => {
  const hashKey = "secrets:demo";

  await assert.rejects(
    decryptSecretHash({
      encrypted: { BAD: "WDL-ENC:not-json" },
      env: envelopeEnv,
      hashKey,
    }),
    /secret envelope JSON is invalid/
  );
});

test("decryptSecretHash fails closed on unknown envelope kids", async () => {
  const hashKey = "secrets:demo";
  const encrypted = await encryptSecretValue("plain", {
    env: { ...envelopeEnv, SECRET_ENVELOPE_KID: "local:test:secret-envelope:v2" },
    hashKey,
    fieldName: "TOKEN",
  });

  await assert.rejects(
    decryptSecretHash({
      encrypted: { TOKEN: encrypted },
      env: envelopeEnv,
      hashKey,
    }),
    /secret envelope kid is not configured/
  );
});

test("worker env budget checks every retained worker version", async () => {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(field, "__meta__");
      if (key === "worker:demo:api:v:1") return JSON.stringify({ vars: { SMALL: "ok" } });
      if (key === "worker:demo:api:v:2") {
        return JSON.stringify({ vars: { BIG: "x".repeat(WORKER_LOADER_ENV_MAX_BYTES) } });
      }
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

test("worker env budget checks retained-version binding env injections", async () => {
  const secret = "x".repeat(Math.floor(WORKER_LOADER_ENV_MAX_BYTES * 0.6));
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(key, "worker:demo:caller:v:1");
      assert.equal(field, "__meta__");
      return JSON.stringify({
        bindings: {
          PLATFORM: {
            type: "service",
            ns: "__platform__",
            service: "platformApi",
            version: "v1",
            requiredCallerSecrets: ["API_TOKEN"],
          },
        },
      });
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "caller",
      versions: ["v1"],
      nsSecrets: { API_TOKEN: secret },
    }),
    (err) => {
      assert.equal(err instanceof WorkerEnvBudgetError, true);
      assert.equal(/** @type {WorkerEnvBudgetError} */ (err).code, "worker_env_too_large");
      return true;
    }
  );
});

test("worker env budget estimates a source bundle under a future version string", async () => {
  const baseMeta = {
    vars: { PAD: "" },
    workflows: [{
      binding: "FLOW",
      name: "flow",
      className: "Flow",
      workflowKey: "wf_0123456789abcdef0123456789abcdef",
    }],
  };
  /** @param {number} padLength @param {string} version */
  const bytesWithPad = (padLength, version) => estimatedWorkerLoaderEnvBytes(estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "api",
    version,
    vars: { PAD: "x".repeat(padLength) },
    meta: baseMeta,
  }));
  const padLength = WORKER_LOADER_ENV_MAX_BYTES - bytesWithPad(0, WORKER_LOADER_ENV_VERSION_PLACEHOLDER) + 1;
  assert.ok(bytesWithPad(padLength, "v1") <= WORKER_LOADER_ENV_MAX_BYTES);
  assert.ok(bytesWithPad(padLength, WORKER_LOADER_ENV_VERSION_PLACEHOLDER) > WORKER_LOADER_ENV_MAX_BYTES);

  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(key, "worker:demo:api:v:1");
      assert.equal(field, "__meta__");
      return JSON.stringify({
        ...baseMeta,
        vars: { PAD: "x".repeat(padLength) },
      });
    },
  };

  await assert.doesNotReject(() => assertWorkerVersionsUserEnvBudget({
    redis,
    ns: "demo",
    worker: "api",
    versions: ["v1"],
  }));
  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: [],
      versionEstimates: [{
        sourceVersion: "v1",
        estimatedVersion: WORKER_LOADER_ENV_VERSION_PLACEHOLDER,
      }],
    }),
    (err) => {
      assert.equal(err instanceof WorkerEnvBudgetError, true);
      const budgetErr = /** @type {WorkerEnvBudgetError} */ (err);
      assert.equal(budgetErr.code, "worker_env_too_large");
      assert.match(budgetErr.message, /demo\/api@v1/);
      assert.equal(budgetErr.details.source_version, "v1");
      assert.equal(budgetErr.details.estimated_version, WORKER_LOADER_ENV_VERSION_PLACEHOLDER);
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
    (err) => {
      assert.ok(err instanceof BundleMetaError);
      const bundleError = /** @type {Error & { cause: unknown }} */ (err);
      assert.ok(bundleError.cause instanceof SyntaxError);
      return assertBundleMetaError(err, bundleError.cause.message);
    }
  );
});

test("worker env budget maps strict retained metadata failures to corrupt_meta", async () => {
  const redis = {
    async hGet() {
      return JSON.stringify({
        workflows: [{ binding: "FLOW", name: "flow", className: "Flow" }],
      });
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: ["v1"],
    }),
    (err) => assertBundleMetaError(
      err,
      'Workflow binding "FLOW" is missing workflow metadata (redeploy demo/api)'
    )
  );
});

test("worker env budget surfaces missing retained bundles as watch retry", async () => {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(key, "worker:demo:api:v:1");
      assert.equal(field, "__meta__");
      return null;
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: ["v1"],
    }),
    (err) => err instanceof Error && err.name === "WatchError"
  );
});

test("worker env budget fails closed when retained bundle metadata is not an object", async () => {
  const redis = {
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      assert.equal(key, "worker:demo:api:v:1");
      assert.equal(field, "__meta__");
      return "[]";
    },
  };

  await assert.rejects(
    () => assertWorkerVersionsUserEnvBudget({
      redis,
      ns: "demo",
      worker: "api",
      versions: ["v1"],
    }),
    (err) => assertBundleMetaError(err, "__meta__ must be a JSON object")
  );
});
