import { afterEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  importControlHandler,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import {
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryModuleSource,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { createFakeRedisSession, sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { realRuntimeInjectionSourcesUrl } from "../helpers/runtime-injection-sources.js";

const {
  libUrl: controlLibUrl,
  lifecycleIndexesUrl,
  sharedAuthRolesUrl,
} = await compileControlGraph();

/** @type {any} */
const CONTROL_DEPLOY_TEST_STATE = {
  strings: new Map(),
  hashes: new Map(),
  sets: new Map(),
  zsets: new Map(),
  revisions: new Map(),
  stagedMeta: null,
  execFailures: 0,
  hSetCalls: [],
  s3: null,
  cleanupIntents: null,
  preparedBundle: null,
  assetsToUpload: null,
  putAssetCalls: null,
  putAssetError: null,
  parsedCrons: null,
  parsedQueueConsumers: null,
  parsedPlatformBindings: null,
  watchedKeys: null,
  watchBatches: [],
  redisCommands: [],
  envBudgetError: false,
  envBudgetCalls: [],
  secretEnvelopeError: null,
  redis: null,
  logs: [],
  metrics: { increment() {}, observe() {} },
  service: "control",
};
installControlHandlerState("__controlDeployTestState", CONTROL_DEPLOY_TEST_STATE);

function resetControlDeployTestState() {
  CONTROL_DEPLOY_TEST_STATE.strings = new Map();
  CONTROL_DEPLOY_TEST_STATE.hashes = new Map();
  CONTROL_DEPLOY_TEST_STATE.sets = new Map();
  CONTROL_DEPLOY_TEST_STATE.zsets = new Map();
  CONTROL_DEPLOY_TEST_STATE.revisions = new Map();
  CONTROL_DEPLOY_TEST_STATE.stagedMeta = null;
  CONTROL_DEPLOY_TEST_STATE.execFailures = 0;
  CONTROL_DEPLOY_TEST_STATE.hSetCalls = [];
  CONTROL_DEPLOY_TEST_STATE.redis = null;
  CONTROL_DEPLOY_TEST_STATE.s3 = null;
  CONTROL_DEPLOY_TEST_STATE.cleanupIntents = null;
  CONTROL_DEPLOY_TEST_STATE.preparedBundle = null;
  CONTROL_DEPLOY_TEST_STATE.assetsToUpload = null;
  CONTROL_DEPLOY_TEST_STATE.putAssetCalls = null;
  CONTROL_DEPLOY_TEST_STATE.putAssetError = null;
  CONTROL_DEPLOY_TEST_STATE.parsedCrons = null;
  CONTROL_DEPLOY_TEST_STATE.parsedQueueConsumers = null;
  CONTROL_DEPLOY_TEST_STATE.parsedPlatformBindings = null;
  CONTROL_DEPLOY_TEST_STATE.watchedKeys = null;
  CONTROL_DEPLOY_TEST_STATE.watchBatches = [];
  CONTROL_DEPLOY_TEST_STATE.redisCommands = [];
  CONTROL_DEPLOY_TEST_STATE.envBudgetError = false;
  CONTROL_DEPLOY_TEST_STATE.envBudgetCalls = [];
  CONTROL_DEPLOY_TEST_STATE.secretEnvelopeError = null;
  CONTROL_DEPLOY_TEST_STATE.logs = [];
  CONTROL_DEPLOY_TEST_STATE.metrics = { increment() {}, observe() {} };
  CONTROL_DEPLOY_TEST_STATE.service = "control";
}

afterEach(() => {
  resetControlDeployTestState();
});

const controlSharedExtraSource = `
export function stageBundleCommit(_multi, _key, { meta }) {
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = meta;
}
export function buildS3CleanupTaskId() { return "s3cleanup:test"; }
export async function recordS3CleanupIntent(intent) {
  if (!Array.isArray(/** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents)) {
    /** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents = [];
  }
  /** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents.push(intent);
}
`;

const controlBundleUrl = moduleDataUrl(`
export function deepFreeze(value) { return value; }
export function prepareBundle(mainModule, modules, options = {}) {
  if (/** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle) {
    return /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle;
  }
  const meta = {
    mainModule,
    modules,
    bindings: options.bindings,
    exports: options.exports,
    workflows: options.workflows,
  };
  if (options.vars !== undefined) meta.vars = options.vars;
  return {
    meta,
    normalized: Object.entries(modules || {}),
  };
}
export function normalizeAssets(value) {
  if (/** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload) {
    return /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload;
  }
  return value || null;
}
`);

const controlBindingsUrl = moduleDataUrl(`
export function parseAllowedCallers() { return []; }
export function parseExports() { return []; }
export function parsePlatformBindings() {
  return /** @type {any} */ (globalThis).__controlDeployTestState.parsedPlatformBindings || [];
}
export function validateBindings() {}
export function normalizeBindings(bindings) { return bindings == null ? null : bindings; }
export async function linkServiceBinding({ callerNs, bindingName, spec, lookupTargetVersion, lookupTargetMeta }) {
  const targetNs = spec.ns || callerNs;
  const version = await lookupTargetVersion(targetNs, spec.service);
  if (!version) throw new LinkError(409, "service_binding_target_inactive", "inactive target");
  const meta = await lookupTargetMeta(targetNs, spec.service, version);
  const entry = Array.isArray(meta?.exports)
    ? meta.exports.find((candidate) => candidate?.entrypoint === (spec.entrypoint || "default"))
    : null;
  const allowed = Array.isArray(entry?.allowedCallers) ? entry.allowedCallers : [];
  if (targetNs !== callerNs && !allowed.includes("*") && !allowed.includes(callerNs)) {
    throw new LinkError(403, "service_binding_acl_denied", "acl denied");
  }
  spec.version = version;
  return spec;
}
export function linkPlatformBinding({
  callerNs,
  bindingReq,
  existingBindings,
  platformExports,
  availableCallerSecrets,
}) {
  if (existingBindings[bindingReq.binding]) {
    throw new LinkError(400, "platform_binding_name_collision", "binding collision");
  }
  const match = platformExports.find((entry) => entry.as === bindingReq.platform);
  if (!match) {
    throw new LinkError(400, "platform_binding_not_registered", "platform binding missing");
  }
  const allowed = Array.isArray(match.allowedCallers) ? match.allowedCallers : [];
  if (match.ns !== callerNs && !allowed.includes("*") && !allowed.includes(callerNs)) {
    throw new LinkError(403, "platform_binding_acl_denied", "acl denied");
  }
  const required = Array.isArray(match.requiredCallerSecrets) ? match.requiredCallerSecrets : [];
  const missing = required.filter((secret) => !availableCallerSecrets.has(secret));
  return {
    warning: missing.length ? {
      binding: bindingReq.binding,
      platform: bindingReq.platform,
      missingCallerSecrets: missing,
    } : undefined,
    expanded: {
      type: "service",
      ns: match.ns,
      service: match.worker,
      version: match.version,
      ...(match.entrypoint && match.entrypoint !== "default" ? { entrypoint: match.entrypoint } : {}),
      ...(required.length ? { requiredCallerSecrets: required } : {}),
    },
  };
}
export class LinkError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
`);

const controlTopologyUrl = moduleDataUrl(`
export function parseRoutes() { return []; }
export function parseCronList() {
  return /** @type {any} */ (globalThis).__controlDeployTestState.parsedCrons || [];
}
export function parseQueueConsumers() {
  return /** @type {any} */ (globalThis).__controlDeployTestState.parsedQueueConsumers || [];
}
`);

const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");

const sharedRedisUrl = sharedRedisStubUrl();

const sharedNsUrl = repositoryFileUrl("shared/ns-pattern.js");

const controlS3Url = moduleDataUrl(`
export async function putAsset() {
  if (!Array.isArray(/** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls)) {
    /** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls = [];
  }
  /** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls.push(Array.from(arguments));
  if (/** @type {any} */ (globalThis).__controlDeployTestState.putAssetError) {
    throw /** @type {any} */ (globalThis).__controlDeployTestState.putAssetError;
  }
}
export function inferContentType() { return "text/plain"; }
`);

const sharedAssetsUrl = moduleDataUrl(`
export function generateAssetsToken() { return "token"; }
export function assetsPrefixFor() { return "assets/demo/"; }
`);

const d1StoreUrl = moduleDataUrl(`
export async function resolveDatabaseRefFrom(session, ns, databaseRef) {
  const byId = await session.hGetAll("d1:database:" + ns + ":" + databaseRef);
  if (byId && Object.keys(byId).length) return byId;
  const physicalId = await session.get("d1:database-name:" + ns + ":" + databaseRef);
  if (!physicalId) return null;
  const raw = await session.hGetAll("d1:database:" + ns + ":" + physicalId);
  return raw && Object.keys(raw).length ? raw : null;
}
`);

const secretEnvelopeUrl = moduleDataUrl(`
export class SecretEnvelopeError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
`);

const controlEnvBudgetUrl = moduleDataUrl(`
import { SecretEnvelopeError } from ${JSON.stringify(secretEnvelopeUrl)};
export class WorkerEnvBudgetError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.status = 400;
    this.code = "worker_env_too_large";
    this.details = details;
  }
}
export function assertWorkerLoaderUserEnvBudget() {
  /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls.push(Array.from(arguments)[0] || {});
  if (/** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError) {
    const args = Array.from(arguments)[0] || {};
    throw new WorkerEnvBudgetError("env too large", {
      namespace: args.ns,
      worker: args.worker,
      version: args.version,
    });
  }
  return 0;
}
export async function decryptSecretHash() {
  const error = /** @type {any} */ (globalThis).__controlDeployTestState.secretEnvelopeError;
  if (error) {
    throw new SecretEnvelopeError(error.code, error.message);
  }
  return {};
}
`);

const runtimeLoadCodeBudgetUrl = moduleDataUrl(readRepositoryModuleSource(
  "runtime/load/code-budget.js",
  importSpecifierReplacements({
    "shared-ns-pattern": repositoryFileUrl("shared/ns-pattern.js"),
    "runtime-load-module-rewrite": repositoryFileUrl("runtime/load/module-rewrite.js"),
    "runtime-load-wrapper-generate": repositoryFileUrl("runtime/load/wrapper-generate.js"),
  })
));
const runtimeLoadInjectionSourcesUrl = realRuntimeInjectionSourcesUrl();
const controlWorkerCodeBudgetUrl = moduleDataUrl(readRepositoryModuleSource(
  "control/worker-code-budget.js",
  importSpecifierReplacements({
    "runtime-load-code-budget": runtimeLoadCodeBudgetUrl,
    "runtime-load-injection-sources": runtimeLoadInjectionSourcesUrl,
    "runtime-load-module-rewrite": repositoryFileUrl("runtime/load/module-rewrite.js"),
    "do-runtime-load-code-budget": repositoryFileUrl("do-runtime/load-code-budget.js"),
    "shared-errors": repositoryFileUrl("shared/errors.js"),
    "do-runtime-alarm-shim-source": repositoryFileUrl("do-runtime/alarm-shim-source.js"),
  })
));
const {
  WORKER_LOADER_CODE_MAX_BYTES,
  WorkerCodeBudgetError,
  assertWorkerLoaderCodeBudget,
} = await import(controlWorkerCodeBudgetUrl);

/** @param {{ meta: Record<string, unknown>, normalized: Array<[string, string | Uint8Array]> }} bundle */
function assertTestWorkerCodeBytes({ meta, normalized }) {
  return assertWorkerLoaderCodeBudget({
    ns: "tenant-a",
    worker: "unit",
    meta,
    normalized,
  });
}

const { commitWithWatch, handle } = await importControlHandler("control/handlers/deploy.js", {
  globalName: "__controlDeployTestState",
  extraSharedSource: controlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "control-bundle": controlBundleUrl,
    "control-bindings": controlBindingsUrl,
    "control-topology": controlTopologyUrl,
    "shared-worker-contract": workerContractUrl,
    "shared-redis": sharedRedisUrl,
    "shared-ns-pattern": sharedNsUrl,
    "shared-auth-roles": sharedAuthRolesUrl,
    "control-s3": controlS3Url,
    "shared-assets-token": sharedAssetsUrl,
    "control-d1-store": d1StoreUrl,
    "control-env-budget": controlEnvBudgetUrl,
    "control-worker-code-budget": controlWorkerCodeBudgetUrl,
    "shared-secret-envelope": secretEnvelopeUrl,
    "shared-secret-keys": repositoryFileUrl("shared/secret-keys.js"),
  },
});

test("worker code budget shape failures are domain errors", () => {
  assert.throws(
    () => assertTestWorkerCodeBytes({ meta: {}, normalized: [] }),
    (err) => {
      const budgetErr = /** @type {{ code?: string, status?: number }} */ (err);
      return err instanceof WorkerCodeBudgetError &&
        budgetErr.code === "worker_code_invalid" &&
        budgetErr.status === 400;
    }
  );
});

test("worker code budget wraps runtime estimator validation failures as domain errors", () => {
  assert.throws(
    () => assertTestWorkerCodeBytes({
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "module" } },
        bindings: { ROOM: { type: "do", className: "not a class" } },
      },
      normalized: [["worker.js", "export default {}"]],
    }),
    (err) => {
      const budgetErr = /** @type {{ code?: string, status?: number }} */ (err);
      return err instanceof WorkerCodeBudgetError &&
        budgetErr.code === "worker_code_invalid" &&
        budgetErr.status === 400 &&
        err instanceof Error &&
        /valid JS class declaration/.test(err.message);
    }
  );
});

test("worker code budget allows do-runtime reserved names when no DO wrapper is injected", () => {
  assert.doesNotThrow(() => assertTestWorkerCodeBytes({
    meta: {
      mainModule: "worker.js",
      modules: {
        "worker.js": { type: "module" },
        "_wdl-do-runtime-wrapper.js": { type: "module" },
        "_wdl-do-alarm-shim.js": { type: "module" },
      },
    },
    normalized: [
      ["worker.js", "export default {}"],
      ["_wdl-do-runtime-wrapper.js", ""],
      ["_wdl-do-alarm-shim.js", ""],
    ],
  }));
});

function makeSession() {
  const state = /** @type {any} */ (globalThis).__controlDeployTestState;
  if (!Array.isArray(state.watchedKeys)) state.watchedKeys = [];
  const fakeState = {
    strings: state.strings,
    hashes: state.hashes,
    sets: state.sets,
    zsets: state.zsets,
    ops: [],
    watched: state.watchedKeys,
    watchBatches: state.watchBatches,
    commands: state.redisCommands,
    expirations: new Map(),
    revisions: state.revisions,
    execFailures: state.execFailures,
    nowMs: Date.now(),
  };
  const session = createFakeRedisSession(fakeState, {
    onExecFailure() {
      state.execFailures = fakeState.execFailures;
      state.strings.set("d1:database-name:tenant-a:main", "d1_new");
    },
  });
  const realMulti = session.multi.bind(session);
  session.multi = () => {
    const multi = realMulti();
    const hSet = multi.hSet.bind(multi);
    multi.hSet = (key, fieldsOrField, maybeValue) => {
      const fields = typeof fieldsOrField === "object"
        ? fieldsOrField
        : { [fieldsOrField]: maybeValue };
      for (const [field, value] of Object.entries(fields)) {
        state.hSetCalls.push({ key, field, value });
      }
      return hSet(key, fieldsOrField, maybeValue);
    };
    return multi;
  };
  return session;
}

const PLATFORM_AUTH_WARNING = Object.freeze({
  binding: "AUTH",
  platform: "auth",
  missingCallerSecrets: Object.freeze(["API_TOKEN"]),
});

const PLATFORM_AUTH_META = Object.freeze({
  exports: Object.freeze([Object.freeze({
    type: "service",
    as: "auth",
    entrypoint: "default",
    allowedCallers: Object.freeze(["*"]),
    requiredCallerSecrets: Object.freeze(["API_TOKEN"]),
  })]),
});

/**
 * @param {{
 *   incr?: () => Promise<number>,
 *   session?: (fn: (s: ReturnType<typeof makeSession>) => Promise<unknown>) => Promise<unknown>,
 * }} [options]
 */
function installPlatformAuthWarningFixture(options = {}) {
  /** @type {any} */ (globalThis).__controlDeployTestState.parsedPlatformBindings = [
    { binding: PLATFORM_AUTH_WARNING.binding, platform: PLATFORM_AUTH_WARNING.platform },
  ];
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async hKeys() {
      return [];
    },
    /** @param {string} key */
    async hGetAll(key) {
      /** @type {any} */ (globalThis).__controlDeployTestState.redisCommands.push(["PLATFORM_HGETALL", key]);
      return key === "routes:__platform__" ? { auth: "v1" } : {};
    },
    /** @param {string[]} keys */
    async hGetAllMany(keys) {
      /** @type {any} */ (globalThis).__controlDeployTestState.redisCommands.push(["PLATFORM_HGETALLMANY", keys]);
      return keys.map((key) => key === "routes:__platform__" ? { auth: "v1" } : {});
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      /** @type {any} */ (globalThis).__controlDeployTestState.redisCommands.push(["PLATFORM_HGET", key, field]);
      if (key === "worker:__platform__:auth:v:1" && field === "__meta__") {
        return JSON.stringify(PLATFORM_AUTH_META);
      }
      return null;
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      /** @type {any} */ (globalThis).__controlDeployTestState.redisCommands.push(["PLATFORM_HGETMANY", pairs]);
      return pairs.map(([key, field]) => {
        if (key === "worker:__platform__:auth:v:1" && field === "__meta__") {
          return JSON.stringify(PLATFORM_AUTH_META);
        }
        return null;
      });
    },
    async incr() {
      return await (options.incr ? options.incr() : 1);
    },
    ...(options.session ? { session: options.session } : {}),
  };
}

test("commitWithWatch re-resolves a D1 alias after a watched recreate race", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map([
    ["d1:database-name:tenant-a:main", "d1_old"],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["d1:database:tenant-a:d1_old", { databaseId: "d1_old", databaseName: "main" }],
    ["d1:database:tenant-a:d1_new", { databaseId: "d1_new", databaseName: "main" }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 1;

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "demo",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        bindings: {
          DB: { type: "d1", databaseId: "main" },
        },
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [{ binding: "DB", databaseId: "main" }],
    controlEnv: {},
  });

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.DB.databaseId, "d1_new");
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.DB.databaseName, "main");
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database-name:tenant-a:main"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database:tenant-a:d1_old"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database:tenant-a:d1_new"));
});

test("commitWithWatch validates deploy env budget under watched secret hashes", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls = [];

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "demo",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        vars: { TOKEN: "from-vars" },
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [],
    controlEnv: { ASSETS_CDN_BASE: "https://assets.example/cdn" },
  });

  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("secrets:tenant-a"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("secrets:tenant-a:demo"));
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.watchBatches.length, 1);
  assert.deepEqual(
    /** @type {any} */ (globalThis).__controlDeployTestState.redisCommands.filter(
      (/** @type {[string, unknown]} */ [command, key]) => {
        if (command === "hGetAll") return String(key).startsWith("secrets:tenant-a");
        return command === "hGetAllMany" &&
          Array.isArray(key) && key.some((item) => String(item).startsWith("secrets:tenant-a"));
      }
    ),
    [["hGetAllMany", ["secrets:tenant-a", "secrets:tenant-a:demo"]]]
  );
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls.length, 1);
  assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls[0].vars, { TOKEN: "from-vars" });
  assert.equal(
    /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls[0].assetsCdnBase,
    "https://assets.example/cdn"
  );
});

test("commitWithWatch validates env budget against materialized D1 metadata", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map([
    ["d1:database-name:tenant-a:main", "d1_0123456789abcdef0123456789abcdef"],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["d1:database:tenant-a:d1_0123456789abcdef0123456789abcdef", {
      databaseId: "d1_0123456789abcdef0123456789abcdef",
      databaseName: "main",
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls = [];

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "demo",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        bindings: {
          DB: { type: "d1", databaseId: "main" },
        },
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [{ binding: "DB", databaseId: "main" }],
    controlEnv: {},
  });

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls.length, 1);
  assert.equal(
    /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetCalls[0].meta.bindings.DB.databaseId,
    "d1_0123456789abcdef0123456789abcdef"
  );
});

test("deploy handler resolves cross-namespace service-binding meta from the target namespace", async () => {
  /** @type {unknown[][]} */
  const bindingReads = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["routes:other", { api: "v1", jobs: "v2" }],
    ["worker:other:api:v:1", {
      "__meta__": JSON.stringify({
        exports: [{ entrypoint: "default", allowedCallers: ["tenant-a"] }],
      }),
    }],
    ["worker:tenant-a:api:v:1", {
      "__meta__": JSON.stringify({
        exports: [{ entrypoint: "default", allowedCallers: [] }],
      }),
    }],
    ["worker:other:jobs:v:2", {
      "__meta__": JSON.stringify({
        exports: [{ entrypoint: "default", allowedCallers: ["tenant-a"] }],
      }),
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;

  const session = makeSession();
  const redis = {
    /** @param {string} key */
    async incr(key) {
      assert.equal(key, "worker:tenant-a:caller:next_version");
      return 1;
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      bindingReads.push(["HGET", key, field]);
      return await session.hGet(key, field);
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      bindingReads.push(["HGETMANY", pairs]);
      return await session.hGetMany(pairs);
    },
    /** @param {string} key */
    async hGetAll(key) {
      return await session.hGetAll(key);
    },
    async hKeys() {
      return [];
    },
    /** @param {(s: typeof session) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(session);
    },
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = redis;

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/caller/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          bindings: {
            API: { type: "service", ns: "other", service: "api" },
            JOBS: { type: "service", ns: "other", service: "jobs" },
          },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "caller",
      requestId: "rid-cross-ns-service",
    });

    assert.equal(response.status, 201);
    assert.deepEqual(bindingReads, [
      ["HGETMANY", [
        ["routes:other", "api"],
        ["routes:other", "jobs"],
      ]],
      ["HGETMANY", [
        ["worker:other:api:v:1", "__meta__"],
        ["worker:other:jobs:v:2", "__meta__"],
      ]],
    ]);
    assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.API.version, "v1");
    assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.JOBS.version, "v2");
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
  }
});

test("deploy handler classifies empty service target metadata before commit", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      return pairs.map(([key, field]) => {
        if (key === "routes:other" && field === "api") return "v1";
        if (key === "worker:other:api:v:1" && field === "__meta__") return "";
        return null;
      });
    },
    async incr() {
      throw new Error("corrupt target metadata must fail before version allocation");
    },
  };

  const response = await handle({
    request: new Request("http://control/ns/tenant-a/workers/caller/deploy", {
      method: "POST",
      body: JSON.stringify({
        mainModule: "worker.js",
        modules: { "worker.js": "export default {}" },
        bindings: {
          API: { type: "service", ns: "other", service: "api" },
        },
      }),
    }),
    env: {},
    ns: "tenant-a",
    name: "caller",
    requestId: "rid-corrupt-service-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.message, "Internal error");
  assert.ok(CONTROL_DEPLOY_TEST_STATE.logs.some((/** @type {any} */ entry) => (
    entry.event === "deploy_rejected" &&
    entry.fields.error_message === "Corrupt __meta__ for other/api/v1"
  )));
});

test("deploy handler fails closed instead of hiding empty platform export metadata", async () => {
  installPlatformAuthWarningFixture();
  /** @param {Array<[string, string]>} pairs */
  const corruptPlatformMetaHGetMany = async (pairs) => {
    return pairs.map(([key, field]) =>
      key === "worker:__platform__:auth:v:1" && field === "__meta__" ? "" : null
    );
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.redis.hGetMany = corruptPlatformMetaHGetMany;

  const response = await handle({
    request: new Request("http://control/ns/tenant-a/workers/caller/deploy", {
      method: "POST",
      body: JSON.stringify({
        mainModule: "worker.js",
        modules: { "worker.js": "export default {}" },
      }),
    }),
    env: {},
    ns: "tenant-a",
    name: "caller",
    requestId: "rid-corrupt-platform-meta",
  });

  const body = await readJsonResponse(response, 500);
  assert.equal(body.error, "corrupt_meta");
  assert.equal(body.message, "Internal error");
});

test("deploy handler schedules cleanup when the first asset upload fails", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = [["style.css", new Uint8Array([1, 2, 3])]];
  /** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.putAssetError = new Error("simulated upload failure");
  /** @type {any} */ (globalThis).__controlDeployTestState.s3 = {};
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;

  const session = makeSession();
  const redis = {
    /** @param {string} key */
    async incr(key) {
      assert.equal(key, "worker:tenant-a:assets-app:next_version");
      return 1;
    },
    async hKeys() {
      return [];
    },
    /** @param {string} key */
    async hGetAll(key) {
      return await session.hGetAll(key);
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      return await session.hGet(key, field);
    },
    /** @param {(s: typeof session) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(session);
    },
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = redis;

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/assets-app/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          assets: { "style.css": "AQID" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "assets-app",
      requestId: "rid-asset-upload-fail",
    });

    const body = await readJsonResponse(response, 502);
    assert.equal(body.error, "asset_upload_failed");
    assert.equal(body.message, "Asset upload failed");
    assert.doesNotMatch(JSON.stringify(body), /simulated upload failure/);
    assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls[0].slice(0, 2), [
      {},
      "assets/demo/style.css",
    ]);
    assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents, [{
      taskId: "s3cleanup:test",
      prefixes: ["assets/demo/"],
      source: {
        kind: "deploy-abort",
        ns: "tenant-a",
        worker: "assets-app",
        version: "v1",
        requestId: "rid-asset-upload-fail",
        reason: "asset_upload_failed",
      },
    }]);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.s3 = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.putAssetError = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler rejects assets without S3 before allocating a version", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = [["style.css", new Uint8Array([1, 2, 3])]];
  /** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.s3 = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/assets-app/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          assets: { "style.css": "AQID" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "assets-app",
      requestId: "rid-assets-no-s3",
    });

    assert.equal((await readJsonResponse(response, 503)).error, "s3_not_configured");
    assert.equal(incrCalled, false);
    assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls, []);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler counts runtime-generated wrapper code before allocating a version", async () => {
  const oversizedEntrypoint = `Entrypoint${"A".repeat(600 * 1024)}`;
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: { "worker.js": { type: "module" } },
      bindings: { DB: { type: "d1", databaseId: "db" } },
      exports: [{ entrypoint: oversizedEntrypoint }],
    },
    normalized: [["worker.js", new Uint8Array(64 * 1024 * 1024 - 512 * 1024)]],
  };
  let incrCalled = false;
  installPlatformAuthWarningFixture({
    async incr() {
      incrCalled = true;
      return 1;
    },
  });

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/code-heavy/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "code-heavy",
      requestId: "rid-code-budget",
    });

    const body = await readJsonResponse(response, 413);
    assert.equal(body.error, "worker_code_too_large");
    assert.match(body.message, /final WorkerCode/);
    assert.equal(body.namespace, "tenant-a");
    assert.equal(body.worker, "code-heavy");
    assert.deepEqual(body.warnings, [{
      binding: "AUTH",
      platform: "auth",
      missingCallerSecrets: ["API_TOKEN"],
    }]);
    assert.equal(typeof body.code_bytes, "number");
    assert.equal(body.max_code_bytes, WORKER_LOADER_CODE_MAX_BYTES);
    assert.equal(incrCalled, false);
    assert.deepEqual(CONTROL_DEPLOY_TEST_STATE.redisCommands.filter((/** @type {unknown[]} */ command) =>
      String(command[0]).startsWith("PLATFORM_")), [
      ["PLATFORM_HGETALLMANY", ["routes:__platform__"]],
      ["PLATFORM_HGETMANY", [["worker:__platform__:auth:v:1", "__meta__"]]],
    ]);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.parsedPlatformBindings = null;
  }
});

test("deploy handler preserves warnings on commit env-budget rejection", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: { "worker.js": { type: "module" } },
    },
    normalized: [["worker.js", "export default {}"]],
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError = true;

  installPlatformAuthWarningFixture({
    async incr() {
      return 7;
    },
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  });

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/env-heavy/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "env-heavy",
      requestId: "rid-env-budget",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_env_too_large");
    assert.equal(body.namespace, "tenant-a");
    assert.equal(body.worker, "env-heavy");
    assert.equal(body.version, "v7");
    assert.deepEqual(body.warnings, [{
      binding: "AUTH",
      platform: "auth",
      missingCallerSecrets: ["API_TOKEN"],
    }]);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.parsedPlatformBindings = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError = false;
  }
});

test("deploy handler filters diagnostic aliases from rejection logs", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 10;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async hKeys() {
      return [];
    },
    async hGetAll() {
      return {};
    },
    async hGet() {
      return null;
    },
    async incr() {
      return 1;
    },
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/contention/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "contention",
      requestId: "rid-deploy-contention",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "deploy_contention");
    const rejection = CONTROL_DEPLOY_TEST_STATE.logs.find((/** @type {any} */ entry) =>
      entry.event === "deploy_rejected"
    );
    assert.equal(rejection.fields.version, "v1");
    assert.equal(rejection.fields.error_message, "exhausted 5 retries; retry later");
    assert.equal(Object.hasOwn(rejection.fields, "message"), false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
  }
});

test("deploy handler keeps D1 ids camelCase on the wire and snake_case in logs", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async hKeys() {
      return [];
    },
    async hGetAll() {
      return {};
    },
    async hGet() {
      return null;
    },
    async incr() {
      return 2;
    },
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/d1-missing/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          bindings: {
            DB: { type: "d1", databaseId: "missing-db" },
          },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "d1-missing",
      requestId: "rid-deploy-d1-missing",
    });

    const body = await readJsonResponse(response, 404);
    assert.equal(body.error, "d1_database_not_found");
    assert.equal(body.databaseId, "missing-db");
    const rejection = CONTROL_DEPLOY_TEST_STATE.logs.find((/** @type {any} */ entry) =>
      entry.event === "deploy_rejected"
    );
    assert.equal(rejection.fields.database_id, "missing-db");
    assert.equal(Object.hasOwn(rejection.fields, "databaseId"), false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
  }
});

test("deploy handler hides secret provider diagnostics and logs them", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: { "worker.js": { type: "module" } },
    },
    normalized: [["worker.js", "export default {}"]],
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.secretEnvelopeError = {
    code: "secret_encryption_unconfigured",
    message: "SECRET_ENVELOPE_LOCAL_KEY_B64 must decode to 32 bytes",
  };

  installPlatformAuthWarningFixture({
    async incr() {
      return 8;
    },
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  });

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/secret-error/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "secret-error",
      requestId: "rid-secret-provider",
    });

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "secret_encryption_unconfigured");
    assert.equal(body.message, "Internal error");
    assert.equal(JSON.stringify(body).includes("SECRET_ENVELOPE_LOCAL_KEY_B64"), false);
    assert.deepEqual(body.warnings, [{
      binding: "AUTH",
      platform: "auth",
      missingCallerSecrets: ["API_TOKEN"],
    }]);
    assert.deepEqual(
      CONTROL_DEPLOY_TEST_STATE.logs.find((/** @type {any} */ entry) =>
        entry.event === "deploy_rejected"
      ),
      {
        level: "error",
        event: "deploy_rejected",
        fields: {
          request_id: "rid-secret-provider",
          namespace: "tenant-a",
          worker: "secret-error",
          version: "v8",
          status: 503,
          reason: "secret_encryption_unconfigured",
          error_message: "SECRET_ENVELOPE_LOCAL_KEY_B64 must decode to 32 bytes",
          error_detail: "SECRET_ENVELOPE_LOCAL_KEY_B64 must decode to 32 bytes",
        },
      },
    );
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.parsedPlatformBindings = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.secretEnvelopeError = null;
  }
});

test("deploy handler rejects runtime reserved module collisions before version allocation", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: {
        "worker.js": { type: "module" },
        "_wdl-wrapper.js": { type: "module" },
      },
    },
    normalized: [
      ["worker.js", "export default {}"],
      ["_wdl-wrapper.js", ""],
    ],
  };

  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/reserved/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "reserved",
      requestId: "rid-code-reserved-runtime",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_code_invalid");
    assert.match(body.message, /reserved module name _wdl-wrapper\.js/);
    assert.equal(incrCalled, false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler rejects do-runtime reserved module collisions before version allocation", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: {
        "worker.js": { type: "module" },
        "_wdl-do-runtime-wrapper.js": { type: "module" },
      },
      bindings: { ROOM: { type: "do", className: "Room" } },
    },
    normalized: [
      ["worker.js", "export class Room {}"],
      ["_wdl-do-runtime-wrapper.js", ""],
    ],
  };

  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/do-reserved/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export class Room {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "do-reserved",
      requestId: "rid-code-reserved-do",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "worker_code_invalid");
    assert.match(body.message, /reserved module name _wdl-do-runtime-wrapper\.js/);
    assert.equal(incrCalled, false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler counts do-runtime wrapper code before allocating a version", async () => {
  const className = `Room${"A".repeat(64 * 1024)}`;
  const meta = {
    mainModule: "worker.js",
    modules: { "worker.js": { type: "module" } },
    bindings: { ROOM: { type: "do", className } },
  };
  const emptyOverhead = assertTestWorkerCodeBytes({
    meta,
    normalized: [["worker.js", ""]],
  });
  const source = " ".repeat(WORKER_LOADER_CODE_MAX_BYTES - emptyOverhead + 1);
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta,
    normalized: [["worker.js", source]],
  };

  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/do-heavy/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "do-heavy",
      requestId: "rid-do-code-budget",
    });

    const body = await readJsonResponse(response, 413);
    assert.equal(body.error, "worker_code_too_large");
    assert.equal(body.namespace, "tenant-a");
    assert.equal(body.worker, "do-heavy");
    assert.equal(typeof body.code_bytes, "number");
    assert.equal(body.max_code_bytes, WORKER_LOADER_CODE_MAX_BYTES);
    assert.equal(incrCalled, false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler skips cleanup for empty assets when commit fails", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.s3 = {};
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;

  const session = makeSession();
  const realMulti = session.multi.bind(session);
  session.multi = () => {
    const multi = realMulti();
    multi.exec = async () => {
      throw new Error("commit failed after empty assets");
    };
    return multi;
  };
  const redis = {
    /** @param {string} key */
    async incr(key) {
      assert.equal(key, "worker:tenant-a:empty-assets-app:next_version");
      return 1;
    },
    async hKeys() {
      return [];
    },
    /** @param {string} key */
    async hGetAll(key) {
      return await session.hGetAll(key);
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      return await session.hGet(key, field);
    },
    /** @param {(s: typeof session) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(session);
    },
  };
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = redis;

  try {
    await assert.rejects(
      () => handle({
        request: new Request("http://control/ns/tenant-a/workers/empty-assets-app/deploy", {
          method: "POST",
          body: JSON.stringify({
            mainModule: "worker.js",
            modules: { "worker.js": "export default {}" },
            assets: {},
          }),
        }),
        env: {},
        ns: "tenant-a",
        name: "empty-assets-app",
        requestId: "rid-empty-assets-commit-fail",
      }),
      /commit failed after empty assets/
    );

    assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.putAssetCalls, []);
    assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.cleanupIntents, []);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.s3 = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.assetsToUpload = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler rejects cron and queue dispatch triggers for platform-tier namespaces", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.parsedCrons = [{ cron: "*/5 * * * *", timezone: "UTC" }];
  /** @type {any} */ (globalThis).__controlDeployTestState.parsedQueueConsumers = [
    { queue: "jobs", maxBatchSize: 1, maxBatchTimeoutMs: 2000, maxRetries: 3 },
  ];
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/__platform__/workers/platform-api/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          crons: [{ cron: "*/5 * * * *" }],
          queueConsumers: [{ queue: "jobs" }],
        }),
      }),
      env: {},
      ns: "__platform__",
      name: "platform-api",
      requestId: "rid-platform-dispatch-trigger",
    });

    const body = await readJsonResponse(response, 400);
    assert.equal(body.error, "invalid_request");
    assert.equal(incrCalled, false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.parsedCrons = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.parsedQueueConsumers = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("commitWithWatch freezes a physical DO storage id into DO bindings", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map([
    ["worker:do-storage:tenant-a:chat", "do_existing0123456789abcdef012345"],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "chat",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        bindings: {
          ROOM: { type: "do", className: "Room" },
          OTHER: { type: "kv", id: "sessions" },
        },
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [],
    controlEnv: {},
  });

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.ROOM.doStorageId, "do_existing0123456789abcdef012345");
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.OTHER.doStorageId, undefined);
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("worker:do-storage:tenant-a:chat"));
});

test("commitWithWatch assigns stable workflow keys into bundle meta and wf:defs", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["wf:defs:tenant-a:orders", {
      "existing-flow": JSON.stringify({
        workflowKey: "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        className: "OldClass",
      }),
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;
  /** @type {any} */ (globalThis).__controlDeployTestState.hSetCalls = [];

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "orders",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        workflows: [
          { name: "existing-flow", binding: "EXISTING", className: "ExistingFlow" },
          { name: "new-flow", binding: "NEW_FLOW", className: "NewFlow" },
        ],
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [],
    controlEnv: {},
  });

  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("wf:defs:tenant-a:orders"));
  assert.equal(
    /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[0].workflowKey,
    "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  );
  assert.match(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[1].workflowKey, /^wf_[0-9a-f]{32}$/);
  assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.hSetCalls.filter((/** @type {any} */ call) => call.key === "wf:defs:tenant-a:orders"), [
    {
      key: "wf:defs:tenant-a:orders",
      field: "existing-flow",
      value: JSON.stringify({
        workflowKey: "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        className: "ExistingFlow",
      }),
    },
    {
      key: "wf:defs:tenant-a:orders",
      field: "new-flow",
      value: JSON.stringify({
        workflowKey: /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[1].workflowKey,
        className: "NewFlow",
      }),
    },
  ]);
});

test("commitWithWatch reads only workflow definitions declared by the new bundle", async () => {
  const duplicateKey = "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  /** @type {unknown[][]} */
  const workflowDefReads = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["wf:defs:tenant-a:orders", {
      orders: JSON.stringify({ workflowKey: duplicateKey, className: "OrderWorkflow" }),
      historical: "not-json",
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      const session = makeSession();
      const hMGet = session.hMGet.bind(session);
      session.hMGet = async (key, fields) => {
        workflowDefReads.push([key, [...fields]]);
        return await hMGet(key, fields);
      };
      return await fn(session);
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "orders",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        workflows: [
          { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
        ],
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [],
    controlEnv: {},
  });
  assert.equal(
    /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[0].workflowKey,
    duplicateKey
  );
  assert.deepEqual(workflowDefReads, [["wf:defs:tenant-a:orders", ["orders"]]]);
});

test("commitWithWatch checks code budget after workflow keys are materialized", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["wf:defs:tenant-a:orders", {
      flow: JSON.stringify({
        workflowKey: "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        className: "Flow",
      }),
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;

  const preparedMeta = {
    mainModule: "worker.js",
    modules: { "worker.js": { type: "module" } },
    workflows: [
      { name: "flow", binding: "FLOW", className: "Flow" },
    ],
  };
  const committedMeta = {
    ...preparedMeta,
    workflows: [
      { ...preparedMeta.workflows[0], workflowKey: "wf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    ],
  };
  const emptyCommittedBytes = assertTestWorkerCodeBytes({
    meta: committedMeta,
    normalized: [["worker.js", ""]],
  });
  const source = " ".repeat(WORKER_LOADER_CODE_MAX_BYTES - emptyCommittedBytes + 1);
  assert.ok(assertTestWorkerCodeBytes({
    meta: preparedMeta,
    normalized: [["worker.js", source]],
  }) <= WORKER_LOADER_CODE_MAX_BYTES);
  assert.throws(
    () => assertTestWorkerCodeBytes({
      meta: committedMeta,
      normalized: [["worker.js", source]],
    }),
    (err) => /** @type {{ code?: string }} */ (err).code === "worker_code_too_large"
  );

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await assert.rejects(
    commitWithWatch({
      redis,
      ns: "tenant-a",
      name: "orders",
      version: "v1",
      prepared: {
        meta: preparedMeta,
        normalized: [["worker.js", source]],
      },
      outgoingRefs: [],
      d1Refs: [],
      controlEnv: {},
    }),
    /** @param {unknown} err */
    (err) => /** @type {{ code?: string }} */ (err).code === "worker_code_too_large"
  );
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta, null);
});

test("commitWithWatch reads workflow defs with own-property discipline", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([["wf:defs:tenant-a:orders", {}]]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;
  /** @type {any} */ (globalThis).__controlDeployTestState.hSetCalls = [];

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await commitWithWatch({
    redis,
    ns: "tenant-a",
    name: "orders",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        workflows: [
          { name: "constructor", binding: "WF", className: "Flow" },
        ],
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [],
    d1Refs: [],
    controlEnv: {},
  });

  assert.match(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[0].workflowKey, /^wf_[0-9a-f]{32}$/);
  assert.deepEqual(/** @type {any} */ (globalThis).__controlDeployTestState.hSetCalls.filter((/** @type {any} */ call) => call.key === "wf:defs:tenant-a:orders"), [
    {
      key: "wf:defs:tenant-a:orders",
      field: "constructor",
      value: JSON.stringify({
        workflowKey: /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.workflows[0].workflowKey,
        className: "Flow",
      }),
    },
  ]);
});

/** @param {any} redis */
function platformBindingCommitArgs(redis) {
  return {
    redis,
    ns: "tenant-a",
    name: "caller",
    version: "v1",
    prepared: {
      meta: {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "esm" } },
        bindings: {
          PLATFORM: {
            type: "service",
            ns: "__platform__",
            service: "platformApi",
            version: "v1",
            entrypoint: "Api",
          },
        },
      },
      normalized: [["worker.js", "export default {}"]],
    },
    outgoingRefs: [{
      binding: "PLATFORM",
      targetNs: "__platform__",
      targetWorker: "platformApi",
      targetVersion: "v1",
    }],
    d1Refs: [],
    controlEnv: {},
  };
}

test("commitWithWatch rejects platform binding target drift before commit", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["routes:__platform__", { platformApi: "v2" }],
    ["worker:__platform__:platformApi:v:1", {
      "__meta__": JSON.stringify({
        mainModule: "worker.js",
        exports: [{ entrypoint: "Api", as: "platform-api", allowedCallers: ["tenant-a"] }],
      }),
    }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await assert.rejects(
    () => commitWithWatch(platformBindingCommitArgs(redis)),
    (err) => {
      const deployErr = /** @type {any} */ (err);
      assert.equal(deployErr.code, "target_drift");
      assert.equal(deployErr.details.target.ns, "__platform__");
      assert.equal(deployErr.details.target.worker, "platformApi");
      assert.equal(deployErr.details.target.expected_version, "v1");
      assert.equal(deployErr.details.target.observed_active, "v2");
      return true;
    }
  );

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta, null);
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("routes:__platform__"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("worker:__platform__:platformApi:v:1"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("worker-delete-lock:__platform__:platformApi"));
});

test("commitWithWatch rejects empty platform binding target metadata before commit", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["routes:__platform__", { platformApi: "v1" }],
    ["worker:__platform__:platformApi:v:1", { "__meta__": "" }],
  ]);
  /** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.execFailures = 0;

  const redis = {
    /** @param {(s: ReturnType<typeof makeSession>) => Promise<unknown>} fn */
    async session(fn) {
      return await fn(makeSession());
    },
  };

  await assert.rejects(
    () => commitWithWatch(platformBindingCommitArgs(redis)),
    (err) => {
      const deployErr = /** @type {any} */ (err);
      assert.equal(deployErr.code, "target_drift");
      assert.equal(deployErr.details.target.ns, "__platform__");
      assert.equal(deployErr.details.target.worker, "platformApi");
      assert.equal(deployErr.details.target.version, "v1");
      assert.equal(deployErr.details.target.reason, "bundle_missing");
      return true;
    }
  );

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta, null);
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("worker:__platform__:platformApi:v:1"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("worker-delete-lock:__platform__:platformApi"));
});
