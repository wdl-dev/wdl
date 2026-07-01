import { afterEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  importControlHandler,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import {
  moduleDataUrl,
} from "../helpers/load-shared-module.js";
import { readJsonResponse } from "../helpers/response-json.js";

const { libUrl: controlLibUrl, lifecycleIndexesUrl } = await compileControlGraph();

/** @type {any} */
const CONTROL_DEPLOY_TEST_STATE = {
  strings: new Map(),
  hashes: new Map(),
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
  watchedKeys: null,
  envBudgetError: false,
  redis: null,
  logs: [],
  metrics: { increment() {}, observe() {} },
  service: "control",
};
installControlHandlerState("__controlDeployTestState", CONTROL_DEPLOY_TEST_STATE);

function resetControlDeployTestState() {
  CONTROL_DEPLOY_TEST_STATE.strings = new Map();
  CONTROL_DEPLOY_TEST_STATE.hashes = new Map();
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
  CONTROL_DEPLOY_TEST_STATE.watchedKeys = null;
  CONTROL_DEPLOY_TEST_STATE.envBudgetError = false;
  CONTROL_DEPLOY_TEST_STATE.redis = null;
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
  return /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle || {
    meta: {
      mainModule,
      modules,
      bindings: options.bindings,
      exports: options.exports,
      workflows: options.workflows,
    },
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
export function parsePlatformBindings() { return []; }
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
export function linkPlatformBinding() { return null; }
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

const sharedVersionUrl = moduleDataUrl(`
export function routesKey(ns) { return "routes:" + ns; }
export function formatVersion(n) { return "v" + n; }
export function parseVersion(tag) {
  const match = /^v([1-9][0-9]*)$/.exec(tag);
  return match ? Number(match[1]) : null;
}
export function bundleKey(ns, worker, version) {
  const n = parseVersion(version);
  if (n == null) throw new Error("invalid version tag " + JSON.stringify(version));
  return "worker:" + ns + ":" + worker + ":v:" + n;
}
`);

const sharedRedisUrl = moduleDataUrl(`
export class WatchError extends Error {}
`);

const sharedNsUrl = moduleDataUrl(`
export function isReservedNs(ns) { return typeof ns === "string" && ns.startsWith("__"); }
export function isValidRouteNs(ns) {
  return typeof ns === "string" &&
    (/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(ns) || ns === "__system__");
}
export const ROUTES_ALLOWED_RESERVED_NS = new Set(["__system__"]);
`);

const sharedAuthRolesUrl = moduleDataUrl(`
export const PLATFORM_TIER_RESERVED_NS = new Set(["__platform__"]);
`);

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

const controlEnvBudgetUrl = moduleDataUrl(`
export class WorkerEnvBudgetError extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
    this.code = "worker_env_too_large";
  }
}
export function assertWorkerLoaderUserEnvBudget() {
  if (/** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError) {
    throw new WorkerEnvBudgetError("env too large");
  }
  return 0;
}
export async function decryptSecretHash() { return {}; }
`);

const secretEnvelopeUrl = moduleDataUrl(`
export class SecretEnvelopeError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
`);

const { commitWithWatch, handle } = await importControlHandler("control/handlers/deploy.js", {
  globalName: "__controlDeployTestState",
  extraSharedSource: controlSharedExtraSource,
  replacements: {
    "control-lib": controlLibUrl,
    "control-lifecycle-indexes": lifecycleIndexesUrl,
    "control-bundle": controlBundleUrl,
    "control-bindings": controlBindingsUrl,
    "control-topology": controlTopologyUrl,
    "shared-version": sharedVersionUrl,
    "shared-redis": sharedRedisUrl,
    "shared-ns-pattern": sharedNsUrl,
    "shared-auth-roles": sharedAuthRolesUrl,
    "control-s3": controlS3Url,
    "shared-assets-token": sharedAssetsUrl,
    "control-d1-store": d1StoreUrl,
    "control-env-budget": controlEnvBudgetUrl,
    "shared-secret-envelope": secretEnvelopeUrl,
  },
});
const { WatchError } = await import(sharedRedisUrl);

function makeSession() {
  return {
    /** @param {string[]} keys */
    async watch(...keys) {
      if (!Array.isArray(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys)) /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys = [];
      /** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.push(...keys);
    },
    async unwatch() {},
    /** @param {string} key */
    async get(key) {
      return /** @type {any} */ (globalThis).__controlDeployTestState.strings.has(key) ? /** @type {any} */ (globalThis).__controlDeployTestState.strings.get(key) : null;
    },
    /** @param {string[]} keys */
    async getMany(keys) {
      return keys.map((key) => /** @type {any} */ (globalThis).__controlDeployTestState.strings.has(key) ? /** @type {any} */ (globalThis).__controlDeployTestState.strings.get(key) : null);
    },
    /** @param {string} key @param {string} field */
    async hGet(key, field) {
      const hash = /** @type {any} */ (globalThis).__controlDeployTestState.hashes.get(key);
      return hash && Object.hasOwn(hash, field) ? hash[field] : null;
    },
    /** @param {Array<[string, string]>} pairs */
    async hGetMany(pairs) {
      return pairs.map(([key, field]) => {
        const hash = /** @type {any} */ (globalThis).__controlDeployTestState.hashes.get(key);
        return hash && Object.hasOwn(hash, field) ? hash[field] : null;
      });
    },
    /** @param {string} key @param {string[]} fields */
    async hMGet(key, fields) {
      const hash = /** @type {any} */ (globalThis).__controlDeployTestState.hashes.get(key);
      return fields.map((field) => hash && Object.hasOwn(hash, field) ? hash[field] : null);
    },
    /** @param {string} key */
    async hGetAll(key) {
      return /** @type {any} */ (globalThis).__controlDeployTestState.hashes.get(key) || {};
    },
    multi() {
      return {
        sAdd() { return this; },
        zAdd() { return this; },
        /** @param {string} key @param {string} field @param {unknown} value */
        hSet(key, field, value) {
          /** @type {any} */ (globalThis).__controlDeployTestState.hSetCalls.push({ key, field, value });
          return this;
        },
        del() { return this; },
        /**
         * @param {string} key
         * @param {unknown} value
         * @param {{ nx?: boolean }} [opts]
         */
        set(key, value, opts = {}) {
          if (!opts.nx || !/** @type {any} */ (globalThis).__controlDeployTestState.strings.has(key)) {
            /** @type {any} */ (globalThis).__controlDeployTestState.strings.set(key, value);
          }
          return this;
        },
        async exec() {
          if (/** @type {any} */ (globalThis).__controlDeployTestState.execFailures > 0) {
            /** @type {any} */ (globalThis).__controlDeployTestState.execFailures -= 1;
            /** @type {any} */ (globalThis).__controlDeployTestState.strings.set("d1:database-name:tenant-a:main", "d1_new");
            throw new WatchError("watched key changed");
          }
          return [];
        },
      };
    },
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
  });

  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.DB.databaseId, "d1_new");
  assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.DB.databaseName, "main");
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database-name:tenant-a:main"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database:tenant-a:d1_old"));
  assert.ok(/** @type {any} */ (globalThis).__controlDeployTestState.watchedKeys.includes("d1:database:tenant-a:d1_new"));
});

test("deploy handler resolves cross-namespace service-binding meta from the target namespace", async () => {
  /** @type {string[]} */
  const metaReads = [];
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map([
    ["routes:other", { api: "v1" }],
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
      if (field === "__meta__") metaReads.push(key);
      return await session.hGet(key, field);
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
          },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "caller",
      requestId: "rid-cross-ns-service",
    });

    assert.equal(response.status, 201);
    assert.ok(metaReads.includes("worker:other:api:v:1"));
    assert.ok(!metaReads.includes("worker:tenant-a:api:v:1"));
    assert.equal(/** @type {any} */ (globalThis).__controlDeployTestState.stagedMeta.bindings.API.version, "v1");
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
  }
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

    assert.equal(response.status, 502);
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

test("deploy handler rejects workerLoader env budget violations before allocating a version", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError = true;

  const session = makeSession();
  let incrCalled = false;
  /** @type {any} */ (globalThis).__controlDeployTestState.redis = {
    async incr() {
      incrCalled = true;
      return 1;
    },
    /** @param {string} key */
    async hGetAll(key) {
      return await session.hGetAll(key);
    },
  };

  try {
    const response = await handle({
      request: new Request("http://control/ns/tenant-a/workers/env-heavy/deploy", {
        method: "POST",
        body: JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
          vars: { BIG: "x" },
        }),
      }),
      env: {},
      ns: "tenant-a",
      name: "env-heavy",
      requestId: "rid-env-budget",
    });

    assert.equal((await readJsonResponse(response, 400)).error, "worker_env_too_large");
    assert.equal(incrCalled, false);
  } finally {
    /** @type {any} */ (globalThis).__controlDeployTestState.redis = null;
    /** @type {any} */ (globalThis).__controlDeployTestState.envBudgetError = false;
    /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = null;
  }
});

test("deploy handler rejects workerLoader code size violations before allocating a version", async () => {
  /** @type {any} */ (globalThis).__controlDeployTestState.strings = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.hashes = new Map();
  /** @type {any} */ (globalThis).__controlDeployTestState.preparedBundle = {
    meta: {
      mainModule: "worker.js",
      modules: { "worker.js": { type: "module" } },
    },
    normalized: [["worker.js", new Uint8Array(64 * 1024 * 1024 + 1)]],
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

    assert.equal((await readJsonResponse(response, 413)).error, "worker_code_too_large");
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
  session.multi = () => ({
    sAdd() { return this; },
    zAdd() { return this; },
    hSet() { return this; },
    del() { return this; },
    set() { return this; },
    async exec() {
      throw new Error("commit failed after empty assets");
    },
  });
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
    () => commitWithWatch({
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
    }),
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
