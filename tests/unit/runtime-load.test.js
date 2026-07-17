import { after, test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  readRepositoryJson,
  readRepositoryModuleSource,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { makeRecordingFetch, withMockedFetch } from "../helpers/mock-fetch.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";
import {
  STUB_RUNTIME_INJECTION_SOURCES,
  stubRuntimeInjectionSourcesUrl,
} from "../helpers/runtime-injection-sources.js";
import { makeTempDir, removeTempDir, withTempDir } from "../helpers/temp-dir.js";

const SHARED_NS_PATTERN_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_WORKER_ID_URL = repositoryFileUrl("shared/worker-id.js");
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_SECRET_ENVELOPE_URL = repositoryFileUrl("shared/secret-envelope.js");
const WORKER_CONTRACT_URL = repositoryFileUrl("shared/worker-contract.js");
const SHARED_REDIS_URL = sharedRedisStubUrl();
const { libUrl: CONTROL_LIB_URL } = await compileControlGraph();
const RUNTIME_ENV_BUILD_URL = repositoryModuleDataUrl(
  "runtime/load/env-build.js",
  importSpecifierReplacements({
    "shared-ns-pattern": SHARED_NS_PATTERN_URL,
    "shared-worker-contract": WORKER_CONTRACT_URL,
  })
);
const versionFixture = readRepositoryJson("tests/fixtures/version-tags.json");
const FETCH_STUB = { fetch() {} };
const { estimatedWorkerLoaderEnv } = await importRepositoryModule("control/env-budget.js", importSpecifierReplacements({
  "control-lib": CONTROL_LIB_URL,
  "runtime-load-env-build": RUNTIME_ENV_BUILD_URL,
  "shared-secret-envelope": SHARED_SECRET_ENVELOPE_URL,
  "shared-worker-contract": WORKER_CONTRACT_URL,
  "shared-redis": SHARED_REDIS_URL,
}));
const RUNTIME_LOAD_INJECTION_SOURCES_URL = stubRuntimeInjectionSourcesUrl();
const src = readRepositoryModuleSource("runtime/load.js", [
  [
    /const REDIS_PROXY_LOAD_TIMEOUT_MS = 8000;/,
    "const REDIS_PROXY_LOAD_TIMEOUT_MS = 5;"
  ],
  [
    /import \{ bundleToWorkerCode \} from "runtime-lib";/,
    `const bundleToWorkerCode = (hash) => {
      const meta = JSON.parse(new TextDecoder().decode(hash.__meta__));
      return {
        compatibilityDate: "2026-04-24",
        mainModule: meta.mainModule,
        modules: Object.fromEntries(Object.keys(meta.modules).map((name) => [name, new TextDecoder().decode(hash[name])])),
        meta,
      };
    };`
  ],
  ...importSpecifierReplacements({
    "shared-observability": OBSERVABILITY_NOOP_URL,
    "shared-ns-pattern": SHARED_NS_PATTERN_URL,
    "shared-worker-id": SHARED_WORKER_ID_URL,
    "shared-internal-auth": SHARED_INTERNAL_AUTH_URL,
    "shared-respond": SHARED_RESPOND_URL,
  }),
  [
    /import \{ evictSiblings, recordLoadedWorker \} from "runtime-state";/,
    `const recordLoadedWorker = (workerId) => {
       /** @type {any[]} */ (globalThis).__runtimeLoadRecordedWorkers.push(workerId);
     };
     const evictSiblings = async (options) => {
       /** @type {any[]} */ (globalThis).__runtimeLoadEvictions.push(options);
     };`
  ],
  [
    /from "runtime-load-code-budget";/,
    'from "./load/code-budget.js";'
  ],
  [
    /from "runtime-load-injection-sources";/,
    `from ${JSON.stringify(RUNTIME_LOAD_INJECTION_SOURCES_URL)};`
  ],
  [
    /from "runtime-load-module-rewrite";/,
    'from "./load/module-rewrite.js";'
  ],
  [
    /from "runtime-load-env-build";/g,
    'from "./load/env-build.js";'
  ],
  [
    /from "runtime-load-wrapper-generate";/,
    'from "./load/wrapper-generate.js";'
  ],
]);

const LOAD_TEST_DIR = makeTempDir("wdl-runtime-load-module-");
const LOAD_TEST_RUNTIME_DIR = path.join(LOAD_TEST_DIR, "runtime");
const LOAD_TEST_SUBMODULE_DIR = path.join(LOAD_TEST_RUNTIME_DIR, "load");
const ENV_BUILD_SOURCE = readRepositoryModuleSource("runtime/load/env-build.js", importSpecifierReplacements({
  "shared-ns-pattern": SHARED_NS_PATTERN_URL,
  "shared-worker-contract": WORKER_CONTRACT_URL,
}));
mkdirSync(LOAD_TEST_SUBMODULE_DIR, { recursive: true });
writeFileSync(path.join(LOAD_TEST_RUNTIME_DIR, "load.js"), src);
for (const name of ["env-build.js", "code-budget.js", "module-rewrite.js", "wrapper-generate.js"]) {
  const moduleSource = name === "env-build.js"
    ? ENV_BUILD_SOURCE
    : readRepositoryModuleSource(`runtime/load/${name}`, importSpecifierReplacements({
      "runtime-load-module-rewrite": pathToFileURL(path.join(LOAD_TEST_SUBMODULE_DIR, "module-rewrite.js")).href,
      "runtime-load-wrapper-generate": pathToFileURL(path.join(LOAD_TEST_SUBMODULE_DIR, "wrapper-generate.js")).href,
      "shared-ns-pattern": SHARED_NS_PATTERN_URL,
    }));
  writeFileSync(
    path.join(LOAD_TEST_SUBMODULE_DIR, name),
    moduleSource
  );
}
after(() => removeTempDir(LOAD_TEST_DIR));

/** @type {any} */ (globalThis).__runtimeLoadRecordedWorkers = [];
/** @type {any} */ (globalThis).__runtimeLoadEvictions = [];

const mod = await import(pathToFileURL(path.join(LOAD_TEST_RUNTIME_DIR, "load.js")).href);
const codeBudgetMod = await import(pathToFileURL(path.join(LOAD_TEST_SUBMODULE_DIR, "code-budget.js")).href);
const {
  buildWorkerEnv,
  createLoaderCallback,
  decodeRuntimeLoadPayload,
  getLoadedWorkerStub,
  runtimeLoadContentTypeMatches,
  wrapWorkerCodeForHostBindings,
} = mod;
const {
  estimateFinalWorkerLoaderCodeBytes,
  injectRuntimeModulesForHostBindings,
} = codeBudgetMod;

const RUNTIME_LOAD_MAGIC = "WDLLOAD!";
const RUNTIME_LOAD_CONTENT_TYPE = "application/vnd.wdl.runtime-load";

/** @param {{ bundle: Record<string, any>, ns_secrets?: Record<string, unknown>, worker_secrets?: Record<string, unknown> }} args */
function encodeRuntimeLoadPayload({ bundle, ns_secrets = {}, worker_secrets = {} }) {
  const chunks = [Buffer.from(RUNTIME_LOAD_MAGIC)];
  const pushU32 = (/** @type {number} */ value) => {
    const bytes = Buffer.alloc(4);
    bytes.writeUInt32BE(value);
    chunks.push(bytes);
  };
  const header = Buffer.from(JSON.stringify({ ns_secrets, worker_secrets }));
  pushU32(header.length);
  chunks.push(header);
  pushU32(Object.keys(bundle).length);
  for (const [key, value] of Object.entries(bundle)) {
    const keyBytes = Buffer.from(key, "utf8");
    const valueBytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
    pushU32(keyBytes.length);
    pushU32(valueBytes.length);
    chunks.push(keyBytes, valueBytes);
  }
  return Buffer.concat(chunks);
}

/** @param {Buffer} buffer */
function bufferToArrayBuffer(buffer) {
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
}

/** @param {number} value */
function u32(value) {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32BE(value);
  return bytes;
}

/** @param {Record<string, unknown> | null} [runtimeEnv] */
function makeCtx(runtimeEnv = null) {
  return {
    exports: {
      KV: (/** @type {{ props: any }} */ { props }) => ({ kind: "kv", props }),
      Assets: (/** @type {{ props: any }} */ { props }) => ({ kind: "assets", props }),
      QueueProducer: (/** @type {{ props: any }} */ { props }) => ({ kind: "queue", props }),
      D1Database: (/** @type {{ props: any }} */ { props }) => ({ kind: "d1", props }),
      R2Bucket: (/** @type {{ props: any }} */ { props }) => ({ kind: "r2", props }),
      ServiceBinding: (/** @type {{ props: any }} */ { props }) => ({ kind: "service", props }),
      DurableObjectNamespace: (/** @type {{ props: any }} */ { props }) => ({ kind: "do", props }),
      InternalAuthBackend: (/** @type {{ props: any }} */ { props }) => ({
        kind: "internal-auth-backend",
        props,
        async fetch(/** @type {RequestInfo | URL | string} */ input, /** @type {RequestInit | undefined} */ init = undefined) {
          const env = runtimeEnv || {};
          const backend = /** @type {{ fetch(input: RequestInfo | URL | string, init?: RequestInit): Promise<Response> }} */ (env[props.binding]);
          return await backend.fetch(input, {
            ...init,
            headers: {
              ...Object.fromEntries(new Headers(init?.headers)),
              "x-wdl-internal-auth": String(env.WDL_INTERNAL_AUTH_TOKEN),
            },
          });
        },
      }),
    },
  };
}

test("buildWorkerEnv: merges vars + secrets with worker secrets taking precedence and materializes bindings", () => {
  const env = buildWorkerEnv(
    {
      vars: { SHARED: "vars", ONLY_VAR: "var-only" },
      assets: { token: "abc", prefix: "assets/demo/app/abc/" },
      bindings: {
        KV1: { type: "kv", id: "sessions" },
        ASSETS: { type: "assets" },
        Q: { type: "queue", id: "orders", deliveryDelaySeconds: 12 },
        AUTH: { type: "service", service: "auth", version: "v3" },
      },
    },
    { SHARED: "ns-secret", NS_ONLY: "ns-only" },
    { SHARED: "worker-secret", WORKER_ONLY: "worker-only" },
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );

  assert.equal(env.SHARED, "worker-secret");
  assert.equal(env.ONLY_VAR, "var-only");
  assert.equal(env.NS_ONLY, "ns-only");
  assert.equal(env.WORKER_ONLY, "worker-only");
  assert.deepEqual(env.KV1, { kind: "kv", props: { ns: "demo", id: "sessions" } });
  assert.deepEqual(env.ASSETS, {
    kind: "assets",
    props: { cdnBase: "https://assets.example", prefix: "assets/demo/app/abc/" },
  });
  assert.deepEqual(env.Q, {
    kind: "queue",
    props: { ns: "demo", id: "orders", deliveryDelaySeconds: 12 },
  });
  assert.deepEqual(env.AUTH, {
    kind: "service",
    props: {
      targetNs: "demo",
      targetWorker: "auth",
      targetVersion: "v3",
      targetEntrypoint: null,
      callerNs: "demo",
    },
  });
});

test("buildWorkerEnv stays aligned with control env budget binding estimates", () => {
  const meta = {
    vars: { SHARED: "vars", ONLY_VAR: "var-only" },
    assets: { prefix: "assets/demo/app/token/" },
    bindings: {
      KV1: { type: "kv", id: "sessions" },
      ASSETS: { type: "assets" },
      Q: { type: "queue", id: "orders", deliveryDelaySeconds: 12 },
      DB: { type: "d1", databaseId: "d1_0123456789abcdef0123456789abcdef" },
      BUCKET: { type: "r2", bucketName: "uploads" },
      ROOM: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" },
      AUTH: {
        type: "service",
        ns: "__platform__",
        service: "auth",
        version: "v3",
        entrypoint: "Auth",
        requiredCallerSecrets: ["API_TOKEN"],
      },
    },
    workflows: [{
      name: "orders",
      binding: "ORDERS",
      className: "OrderWorkflow",
      workflowKey: "wf_0123456789abcdef0123456789abcdef",
    }],
  };
  const nsSecrets = { SHARED: "ns-secret", API_TOKEN: "ns-token" };
  const workerSecrets = { SHARED: "worker-secret", API_TOKEN: "worker-token" };
  const doBackend = { __wdlBinding: "internal", name: "DO_BACKEND" };
  const doOwnerNetwork = { __wdlBinding: "internal", name: "DO_OWNER_NETWORK" };
  const workflowsBackend = { __wdlBinding: "internal", name: "WORKFLOWS_BACKEND" };
  const ctx = {
    exports: {
      KV: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "kv", props }),
      Assets: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "assets", props }),
      QueueProducer: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "queue", props }),
      D1Database: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "d1", props }),
      R2Bucket: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "r2", props }),
      ServiceBinding: (/** @type {{ props: any }} */ { props }) => ({ __wdlBinding: "service", props }),
    },
  };
  const runtimeEnv = buildWorkerEnv(
    meta,
    nsSecrets,
    workerSecrets,
    "demo",
    "app",
    "v5",
    "https://assets.example",
    /** @type {any} */ (ctx),
    doBackend,
    {
      doOwnerNetwork,
      workflowsBackend,
      // Record the same host-proxy payload shape that the runtime DO factory
      // passes into Worker env so the control estimator must mirror it.
      /** @param {{ name: string, spec: any, ns: string, worker: string, version: string }} input */
      doBindingFactory(input) {
        const { name, spec, ns, worker, version } = input;
        const props = {
          ns,
          worker,
          version,
          doStorageId: spec.doStorageId,
          binding: name,
          className: spec.className,
        };
        return {
          __wdlBinding: "do",
          ...props,
          hostProxy: { __wdlBinding: "do-host-proxy", props },
        };
      },
    }
  );

  const estimatedEnv = estimatedWorkerLoaderEnv({
    ns: "demo",
    worker: "app",
    version: "v5",
    vars: meta.vars,
    nsSecrets,
    workerSecrets,
    meta,
    assetsCdnBase: "https://assets.example",
  });
  const jsonShape = (/** @type {unknown} */ value) => JSON.parse(JSON.stringify(value));
  const estimatedRuntimeEnv = { ...estimatedEnv };
  delete estimatedRuntimeEnv.__WDL_DO_ALARMS__;
  assert.deepEqual(jsonShape(runtimeEnv), jsonShape(estimatedRuntimeEnv));
  assert.deepEqual(jsonShape(estimatedEnv.__WDL_DO_ALARMS__), {
    __wdlBinding: "do-alarms",
    props: {
      ns: "demo",
      worker: "app",
      version: "v5",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
    },
  });
});

test("buildWorkerEnv: service binding with named entrypoint sets targetEntrypoint prop", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        AUTH: { type: "service", service: "auth", version: "v3", entrypoint: "Auth" },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );
  assert.deepEqual(env.AUTH, {
    kind: "service",
    props: {
      targetNs: "demo",
      targetWorker: "auth",
      targetVersion: "v3",
      targetEntrypoint: "Auth",
      callerNs: "demo",
    },
  });
});

test("buildWorkerEnv: materializes D1 bindings with namespace and binding name", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        DB: { type: "d1", databaseId: "main-db", databaseName: "main" },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );

  assert.deepEqual(env.DB, {
    kind: "d1",
    props: {
      ns: "demo",
      databaseId: "main-db",
      binding: "DB",
    },
  });
});

test("buildWorkerEnv: materializes R2 bindings with namespace-scoped bucket props", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        BUCKET: { type: "r2", bucketName: "uploads" },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );

  assert.deepEqual(env.BUCKET, {
    kind: "r2",
    props: {
      ns: "demo",
      bucketName: "uploads",
      binding: "BUCKET",
    },
  });
});

test("buildWorkerEnv: materializes DO metadata with internal direct backend", () => {
  const backend = { fetch() {} };
  const env = buildWorkerEnv(
    {
      bindings: {
        ROOM: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx(),
    backend
  );

  assert.deepEqual(env.ROOM, {
    ns: "demo",
    worker: "app",
    version: "v5",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  });
  assert.equal(env.__WDL_DO_BACKEND__, backend);
  assert.equal(Object.hasOwn(env, "__WDL_INTERNAL_AUTH_TOKEN__"), false);
});

test("buildWorkerEnv: materializes Workflow metadata with internal backend", () => {
  const backend = { fetch() {} };
  const env = buildWorkerEnv(
    {
      workflows: [
        {
          name: "orders",
          binding: "ORDERS",
          className: "OrderWorkflow",
          workflowKey: "wf_0123456789abcdef0123456789abcdef",
        },
      ],
    },
    {},
    {},
    "demo",
    "shop",
    "v4",
    "https://assets.example",
    makeCtx(),
    null,
    { workflowsBackend: backend }
  );

  assert.deepEqual(env.ORDERS, {
    ns: "demo",
    worker: "shop",
    version: "v4",
    name: "orders",
    binding: "ORDERS",
    className: "OrderWorkflow",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
  });
  assert.equal(env.__WDL_WORKFLOWS_BACKEND__, backend);
  assert.equal(Object.hasOwn(env, "__WDL_INTERNAL_AUTH_TOKEN__"), false);
});

test("buildWorkerEnv: workflow binding backend must come from runtime options", () => {
  assert.throws(
    () => buildWorkerEnv(
      {
        vars: { WORKFLOWS_BACKEND: { fetch() {} } },
        workflows: [
          {
            name: "orders",
            binding: "ORDERS",
            className: "OrderWorkflow",
            workflowKey: "wf_0123456789abcdef0123456789abcdef",
          },
        ],
      },
      {},
      {},
      "demo",
      "shop",
      "v4",
      "https://assets.example",
      makeCtx()
    ),
    /requires WORKFLOWS_BACKEND service binding/
  );
});

test("buildWorkerEnv: workflow binding requires frozen workflow metadata", () => {
  assert.throws(
    () => buildWorkerEnv(
      { workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }] },
      {},
      {},
      "demo",
      "shop",
      "v4",
      "https://assets.example",
      makeCtx(),
    ),
    /missing workflow metadata/
  );
});

test("buildWorkerEnv: custom DO factory supports do-runtime JSRPC namespaces", () => {
  const ctx = makeCtx();
  const env = buildWorkerEnv(
    {
      bindings: {
        ROOM: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    ctx,
    null,
    {
      doBindingFactory(/** @type {{ name: string, spec: any, ns: string, worker: string, version: string }} */ { name, spec, ns, worker, version }) {
        return ctx.exports.DurableObjectNamespace({
          props: { ns, worker, version, doStorageId: spec.doStorageId, binding: name, className: spec.className },
        });
      },
    }
  );

  assert.deepEqual(env.ROOM, {
    kind: "do",
    props: {
      ns: "demo",
      worker: "app",
      version: "v5",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    },
  });
  assert.equal(env.__WDL_DO_BACKEND__, undefined);
});

test("buildWorkerEnv: cross-ns service binding overrides targetNs from spec.ns", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        BILLING: {
          type: "service",
          service: "shared-billing",
          ns: "acme",
          version: "v7",
          entrypoint: "Billing",
        },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );
  assert.deepEqual(env.BILLING, {
    kind: "service",
    props: {
      targetNs: "acme",
      targetWorker: "shared-billing",
      targetVersion: "v7",
      targetEntrypoint: "Billing",
      callerNs: "demo",
    },
  });
});

test("buildWorkerEnv: service binding with requiredCallerSecrets filters from ns+worker secrets", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        JSJ: {
          type: "service",
          ns: "__platform__",
          service: "jsj-bridge",
          version: "v2",
          entrypoint: "JSJ",
          requiredCallerSecrets: ["JSJ_API_KEY", "MISSING_KEY"],
        },
      },
    },
    { JSJ_API_KEY: "ns-level-key", OTHER_SECRET: "leak-me" },
    { JSJ_API_KEY: "worker-level-key" },
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );
  // Listed keys only, worker wins over ns, missing key silently absent.
  assert.equal(Object.getPrototypeOf(env.JSJ.props.callerSecrets), Object.prototype);
  assert.deepEqual(env.JSJ.props.callerSecrets, { JSJ_API_KEY: "worker-level-key" });
  assert.equal(env.JSJ.props.callerNs, "demo");
  assert.equal(env.JSJ.props.targetNs, "__platform__");
});

test("buildWorkerEnv: requiredCallerSecrets does NOT fall back to vars", () => {
  const env = buildWorkerEnv(
    {
      vars: { JSJ_API_KEY: "var-fallback-should-not-leak" },
      bindings: {
        JSJ: {
          type: "service",
          ns: "__platform__",
          service: "jsj-bridge",
          version: "v2",
          entrypoint: "JSJ",
          requiredCallerSecrets: ["JSJ_API_KEY"],
        },
      },
    },
    {},
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );
  assert.equal(Object.getPrototypeOf(env.JSJ.props.callerSecrets), Object.prototype);
  assert.deepEqual(env.JSJ.props.callerSecrets, {});
});

test("buildWorkerEnv: service binding without requiredCallerSecrets omits callerSecrets prop", () => {
  const env = buildWorkerEnv(
    {
      bindings: {
        AUTH: { type: "service", service: "auth", version: "v3" },
      },
    },
    { SOMETHING: "x" },
    {},
    "demo",
    "app",
    "v5",
    "https://assets.example",
    makeCtx()
  );
  assert.ok(!("callerSecrets" in env.AUTH.props), "callerSecrets should be absent when not declared");
  assert.equal(env.AUTH.props.callerNs, "demo");
});

test("buildWorkerEnv: assets binding requires ASSETS_CDN_BASE", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          assets: { token: "t", prefix: "assets/demo/app/t/" },
          bindings: { ASSETS: { type: "assets" } },
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "",
        makeCtx()
      ),
    /requires ASSETS_CDN_BASE/
  );
});

test("buildWorkerEnv: assets binding requires __meta__.assets.prefix", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { ASSETS: { type: "assets" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /requires __meta__\.assets\.prefix/
  );
});

test("buildWorkerEnv: queue binding requires queue id", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { Q: { type: "queue" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /queue binding but missing id/
  );
});

test("buildWorkerEnv: D1 binding requires databaseId", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { DB: { type: "d1" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /D1 binding but has invalid databaseId/
  );
});

test("buildWorkerEnv: D1 bindings revalidate namespace and database id grammar", () => {
  assert.throws(
    () => buildWorkerEnv(
      { bindings: { DB: { type: "d1", databaseId: "db/child" } } },
      {},
      {},
      "demo",
      "app",
      "v1",
      "https://assets.example",
      makeCtx()
    ),
    /invalid databaseId/
  );
});

test("buildWorkerEnv: R2 binding requires bucketName", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { BUCKET: { type: "r2" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /R2 binding but missing bucketName/
  );
});

test("buildWorkerEnv: service binding requires service and version", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { AUTH: { type: "service", service: "auth" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /service binding but missing service\/version/
  );
});

test("buildWorkerEnv: stored service binding versions match the shared fixture", () => {
  for (const { tag, parsed } of versionFixture.cases) {
    if (parsed != null || tag === "") continue;
    assert.throws(
      () => buildWorkerEnv(
        { bindings: { AUTH: { type: "service", service: "auth", version: tag } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
      /service binding but has invalid version/,
      tag
    );
  }
});

test("buildWorkerEnv: stored service binding cannot target runtime-reserved entrypoints", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          bindings: {
            SVC: {
              type: "service",
              service: "target",
              version: "v1",
              entrypoint: "__WdlAbort__",
            },
          },
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /targets reserved runtime entrypoint "__WdlAbort__"/
  );
});

test("buildWorkerEnv: stored service binding rejects invalid entrypoints", () => {
  for (const entrypoint of ["", "not-valid-name"]) {
    assert.throws(
      () =>
        buildWorkerEnv(
          {
            bindings: {
              SVC: {
                type: "service",
                service: "target",
                version: "v1",
                entrypoint,
              },
            },
          },
          {},
          {},
          "demo",
          "app",
          "v1",
          "https://assets.example",
          makeCtx()
        ),
      /service binding but has invalid entrypoint/
    );
  }
});

test("buildWorkerEnv: stored DO binding cannot target runtime-reserved class names", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          bindings: {
            ROOMS: { type: "do", className: "__WdlAbort__", doStorageId: "do_0123456789abcdef0123456789abcdef" },
          },
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx(),
        FETCH_STUB
      ),
    /targets reserved runtime entrypoint "__WdlAbort__"/
  );
});

test("buildWorkerEnv: stored DO binding cannot target reserved JS class names", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          bindings: {
            ROOMS: { type: "do", className: "class", doStorageId: "do_0123456789abcdef0123456789abcdef" },
          },
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx(),
        FETCH_STUB
      ),
    /invalid Durable Object class name "class"/
  );
});

test("buildWorkerEnv: stored workflow binding cannot target reserved JS class names", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          workflows: [{ name: "flow", binding: "FLOW", className: "class", workflowKey: "wf_test" }],
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx(),
        null,
        FETCH_STUB
      ),
    /invalid workflow class name "class"/
  );
  assert.throws(
    () =>
      buildWorkerEnv(
        {
          workflows: [{ name: "flow", binding: "FLOW", className: "__WdlAbort__", workflowKey: "wf_test" }],
        },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx(),
        null,
        FETCH_STUB
      ),
    /targets reserved runtime entrypoint "__WdlAbort__"/
  );
});

test("buildWorkerEnv: rejects corrupted runtime-internal binding names", () => {
  for (const name of ["__WDL_DO_BACKEND__", "bad-name", "toString"]) {
    assert.throws(
      () =>
        buildWorkerEnv(
          { bindings: { [name]: { type: "kv", id: "cache" } } },
          {},
          {},
          "demo",
          "app",
          "v1",
          "https://assets.example",
          makeCtx()
        ),
      /not a valid runtime binding name/
    );
  }
});

test("buildWorkerEnv: rejects corrupted env source names before materialization", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { vars: JSON.parse('{"__proto__":"x"}') },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /var "__proto__" is a reserved Object\.prototype key/
  );
  assert.throws(
    () =>
      buildWorkerEnv(
        { vars: { __WDL_DO_BACKEND__: "true" } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /var "__WDL_DO_BACKEND__" is reserved for runtime-internal bindings/
  );
  assert.throws(
    () =>
      buildWorkerEnv(
        {},
        { toString: "x" },
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /namespace secret "toString" is a reserved Object\.prototype key/
  );
});

test("buildWorkerEnv: unsupported binding types fail loudly", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { X: { type: "unknown" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /Unsupported binding "X": type "unknown"/
  );
});

test("buildWorkerEnv: Object.prototype binding types are unsupported", () => {
  assert.throws(
    () =>
      buildWorkerEnv(
        { bindings: { X: { type: "constructor" } } },
        {},
        {},
        "demo",
        "app",
        "v1",
        "https://assets.example",
        makeCtx()
      ),
    /Unsupported binding "X": type "constructor"/
  );
});

test("decodeRuntimeLoadPayload: reads length-prefixed bundle bytes and secrets", () => {
  const payload = encodeRuntimeLoadPayload({
    bundle: {
      "__meta__": JSON.stringify({
        mainModule: "worker.js",
        modules: { "worker.js": { type: "module" }, "data.bin": { type: "data" } },
      }),
      "worker.js": "export default {};",
      "data.bin": Buffer.from([0, 1, 255]),
    },
    ns_secrets: { A: "ns" },
    worker_secrets: { A: "worker", B: "local" },
  });

  const decoded = decodeRuntimeLoadPayload(bufferToArrayBuffer(payload));
  assert.equal(new TextDecoder().decode(decoded.bundle["worker.js"]), "export default {};");
  assert.deepEqual([...decoded.bundle["data.bin"]], [0, 1, 255]);
  assert.equal(decoded.bundle["data.bin"].buffer.byteLength, 3);
  assert.deepEqual(decoded.ns_secrets, { A: "ns" });
  assert.deepEqual(decoded.worker_secrets, { A: "worker", B: "local" });
});

test("decodeRuntimeLoadPayload: preserves magic bundle keys as data entries", () => {
  const bundle = Object.fromEntries([
    ["__meta__", JSON.stringify({
      mainModule: "worker.js",
      modules: { "worker.js": { type: "module" } },
    })],
    ["worker.js", "export default {};"],
    ["__proto__", "magic-entry"],
  ]);
  const payload = encodeRuntimeLoadPayload({
    bundle,
  });

  const decoded = decodeRuntimeLoadPayload(bufferToArrayBuffer(payload));
  assert.equal(Object.hasOwn(decoded.bundle, "__proto__"), true);
  assert.equal(new TextDecoder().decode(decoded.bundle.__proto__), "magic-entry");
});

test("decodeRuntimeLoadPayload: accepts an empty header as no secrets", () => {
  const key = Buffer.from("worker.js");
  const value = Buffer.from("ok");
  const payload = Buffer.concat([
    Buffer.from(RUNTIME_LOAD_MAGIC),
    u32(0),
    u32(1),
    u32(key.length),
    u32(value.length),
    key,
    value,
  ]);

  const decoded = decodeRuntimeLoadPayload(bufferToArrayBuffer(payload));
  assert.equal(new TextDecoder().decode(decoded.bundle["worker.js"]), "ok");
  assert.deepEqual(decoded.ns_secrets, {});
  assert.deepEqual(decoded.worker_secrets, {});
});

test("decodeRuntimeLoadPayload: rejects malformed binary envelopes", () => {
  assert.throws(
    () => decodeRuntimeLoadPayload(new TextEncoder().encode("not a bundle").buffer),
    /invalid magic/
  );
  const payload = encodeRuntimeLoadPayload({ bundle: { "worker.js": "ok" } });
  assert.throws(
    () => decodeRuntimeLoadPayload(bufferToArrayBuffer(payload.subarray(0, payload.length - 1))),
    /truncated/
  );
  const oversizedHeader = Buffer.concat([
    Buffer.from(RUNTIME_LOAD_MAGIC),
    u32(1024 * 1024 + 1),
  ]);
  assert.throws(
    () => decodeRuntimeLoadPayload(bufferToArrayBuffer(oversizedHeader)),
    /header exceeds 1048576 bytes/
  );
});

test("createLoaderCallback: attaches configured tail worker and always wraps mainModule with the abort/binding shim", async () => {
  /** @type {any[]} */
  const fetchCalls = [];

  const env = {
    REDIS_PROXY_URL: "http://redis-proxy.local",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    PUBLIC_NETWORK: { kind: "public-network" },
    TAIL_WORKER: { kind: "tail-fetcher" },
  };

  await withMockedFetch(
    makeRecordingFetch(fetchCalls, {
      capture: (_call, url, init) => ({ url: new URL(/** @type {string | URL} */ (url)), init }),
      response: (url, init) => {
        const u = new URL(/** @type {string | URL} */ (url));
        assert.equal(u.pathname, "/runtime/load");
        assert.equal(u.searchParams.get("ns"), "demo");
        assert.equal(u.searchParams.get("worker"), "app");
        assert.equal(u.searchParams.get("version"), "v1");
        assert.ok(init?.signal instanceof AbortSignal);
        assert.equal(new Headers(init?.headers).get("x-wdl-internal-auth"), "test-internal-auth-token");
        return new Response(encodeRuntimeLoadPayload({
          bundle: {
            "__meta__": JSON.stringify({
              mainModule: "worker.js",
              modules: { "worker.js": { type: "module" } },
            }),
            "worker.js": "export default {};",
          },
          ns_secrets: {},
          worker_secrets: {},
        }), {
          status: 200,
          headers: { "content-type": RUNTIME_LOAD_CONTENT_TYPE },
        });
      },
    }),
    async () => {
      const workerCode = await createLoaderCallback({
        requestId: "rid-1",
        env,
        ctx: makeCtx(),
        ns: "demo",
        worker: "app",
        version: "v1",
        workerId: "demo:app:v1",
      })();

      // Every loaded worker is rewritten to point at the shim that injects the
      // __WdlAbort__ entrypoint; the user's main module stays in `modules` so
      // the shim can `import * as user from "./worker.js"`.
      assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
      assert.equal(/** @type {any} */ (workerCode.modules)["worker.js"], "export default {};");
      assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class __WdlAbort__/);
      assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \{ WorkerEntrypoint, abortIsolate \} from "cloudflare:workers"/);
      assert.equal("allowExperimental" in workerCode, false);
      assert.deepEqual(workerCode.tails, [{ kind: "tail-fetcher" }]);
      assert.deepEqual(workerCode.globalOutbound, { kind: "public-network" });
      assert.ok(!("meta" in workerCode), "loader callback should not propagate meta into the workerLoader object");
      assert.equal(fetchCalls.length, 1);
      assert.equal(fetchCalls[0].init.signal.aborted, false);
    }
  );
});

test("getLoadedWorkerStub records cache misses before scheduling active-version eviction", async () => {
  /** @type {Array<{ id: string, factory: () => Promise<unknown> }>} */
  const loaderCalls = [];
  /** @type {Promise<unknown>[]} */
  const backgroundTasks = [];
  /** @type {any} */ (globalThis).__runtimeLoadRecordedWorkers.length = 0;
  /** @type {any} */ (globalThis).__runtimeLoadEvictions.length = 0;
  const stub = { getEntrypoint() {} };
  const env = {
    REDIS_PROXY_URL: "http://redis-proxy.local",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    PUBLIC_NETWORK: { kind: "public-network" },
    LOADER: {
      get(/** @type {string} */ id, /** @type {() => Promise<unknown>} */ factory) {
        loaderCalls.push({ id, factory });
        return stub;
      },
    },
  };
  const ctx = {
    ...makeCtx(),
    waitUntil(/** @type {Promise<unknown>} */ promise) {
      backgroundTasks.push(promise);
    },
  };

  await withMockedFetch(
    async () => new Response(encodeRuntimeLoadPayload({
      bundle: {
        "__meta__": JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": { type: "module" } },
        }),
        "worker.js": "export default {};",
      },
    }), {
      status: 200,
      headers: { "content-type": RUNTIME_LOAD_CONTENT_TYPE },
    }),
    async () => {
      const loaded = getLoadedWorkerStub({
        requestId: "rid-loader-wrapper",
        env,
        ctx,
        ns: "demo",
        worker: "app",
        version: "v2",
        evictOnLoad: true,
      });

      assert.equal(loaded.workerId, "demo:app:v2");
      assert.equal(loaded.stub, stub);
      assert.equal(loaderCalls.length, 1);
      assert.equal(loaderCalls[0].id, "demo:app:v2");
      assert.deepEqual(/** @type {any} */ (globalThis).__runtimeLoadRecordedWorkers, []);

      await loaderCalls[0].factory();
      await Promise.all(backgroundTasks);

      assert.deepEqual(/** @type {any} */ (globalThis).__runtimeLoadRecordedWorkers, ["demo:app:v2"]);
      assert.deepEqual(
        /** @type {any} */ (globalThis).__runtimeLoadEvictions.map(
          (/** @type {{ workerId: string }} */ entry) => entry.workerId
        ),
        ["demo:app:v2"]
      );
    }
  );
});

test("createLoaderCallback: DO bindings are host proxies and workflow auth stays out of generated facade code", async () => {
  /** @type {Array<{ service: string, headers: HeadersInit | undefined }>} */
  const privateCalls = [];
  const doBackend = {
    async fetch(/** @type {RequestInfo | URL | string} */ _input, /** @type {RequestInit} */ init = {}) {
      privateCalls.push({ service: "do", headers: init.headers });
      return new Response("ok");
    },
  };
  const doOwnerNetwork = {
    async fetch(/** @type {RequestInfo | URL | string} */ _input, /** @type {RequestInit} */ init = {}) {
      privateCalls.push({ service: "do-owner-network", headers: init.headers });
      return new Response("ok");
    },
  };
  const workflowsBackend = {
    async fetch(/** @type {RequestInfo | URL | string} */ _input, /** @type {RequestInit} */ init = {}) {
      privateCalls.push({ service: "workflows", headers: init.headers });
      return new Response("ok");
    },
  };
  const originalSet = Headers.prototype.set;
  const originalPush = Array.prototype.push;

  await withMockedFetch(
    async () => new Response(encodeRuntimeLoadPayload({
      bundle: {
        "__meta__": JSON.stringify({
          mainModule: "worker.js",
          modules: { "worker.js": { type: "module" } },
          bindings: {
            ROOMS: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" },
          },
          workflows: [{
            name: "orders",
            binding: "ORDERS",
            className: "OrderWorkflow",
            workflowKey: "wf_0123456789abcdef0123456789abcdef",
          }],
        }),
        "worker.js": "export class Room {}; export class OrderWorkflow {}; export default {};",
      },
      ns_secrets: {},
      worker_secrets: {},
    }), {
      status: 200,
      headers: { "content-type": RUNTIME_LOAD_CONTENT_TYPE },
    }),
    async () => {
      const env = {
        REDIS_PROXY_URL: "http://redis-proxy.local",
        WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
        ASSETS_CDN_BASE: "https://assets.example",
        PUBLIC_NETWORK: { kind: "public-network" },
        DO_BACKEND: doBackend,
        DO_OWNER_NETWORK: doOwnerNetwork,
        WORKFLOWS_BACKEND: workflowsBackend,
      };
      const workerCode = await createLoaderCallback({
        requestId: "rid-private-backend",
        env,
        ctx: makeCtx(env),
        ns: "demo",
        worker: "app",
        version: "v1",
        workerId: "demo:app:v1",
      })();

      const wrapperSource = /** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"];
      assert.doesNotMatch(wrapperSource, /internalAuthToken|__WDL_INTERNAL_AUTH_TOKEN__/);
      assert.deepEqual(workerCode.env.ROOMS, {
        ns: "demo",
        worker: "app",
        version: "v1",
        doStorageId: "do_0123456789abcdef0123456789abcdef",
        binding: "ROOMS",
        className: "Room",
        hostProxy: {
          kind: "do",
          props: {
            ns: "demo",
            worker: "app",
            version: "v1",
            doStorageId: "do_0123456789abcdef0123456789abcdef",
            binding: "ROOMS",
            className: "Room",
          },
        },
      });
      assert.equal(Object.hasOwn(workerCode.env, "__WDL_DO_BACKEND__"), true);
      assert.equal(Object.hasOwn(workerCode.env, "__WDL_DO_OWNER_NETWORK__"), true);
      assert.equal(Object.hasOwn(workerCode.env, "__WDL_INTERNAL_AUTH_TOKEN__"), false);
      /** @type {string[]} */
      const capturedAuthWrites = [];
      await withMockedProperty(Headers.prototype, "set", /** @this {Headers} */ function set(
        /** @type {string} */ name,
        /** @type {string} */ value
      ) {
        if (String(name).toLowerCase() === "x-wdl-internal-auth") {
          capturedAuthWrites.push(String(value));
        }
        return originalSet.call(this, name, value);
      }, async () => withMockedProperty(Array.prototype, "push", /** @this {unknown[]} */ function push(...items) {
        for (const item of items) {
          if (Array.isArray(item) && item.includes("test-internal-auth-token")) {
            capturedAuthWrites.push(String(item));
          }
        }
        return originalPush.apply(this, items);
      }, async () => {
        await /** @type {any} */ (workerCode.env).__WDL_DO_BACKEND__.fetch("http://do-runtime/internal/do/invoke", {
          headers: { "x-wdl-internal-auth": "spoofed" },
        });
        await /** @type {any} */ (workerCode.env).__WDL_DO_OWNER_NETWORK__.fetch("http://do-owner-network/internal/do/invoke", {
          headers: { "x-wdl-internal-auth": "spoofed" },
        });
        await /** @type {any} */ (workerCode.env).__WDL_WORKFLOWS_BACKEND__.fetch("http://workflows/internal/workflows/create", {
          headers: { "x-wdl-internal-auth": "spoofed" },
        });
      }));
      assert.deepEqual(capturedAuthWrites, []);
      assert.deepEqual(privateCalls.map((call) => [
        call.service,
        new Headers(call.headers).get("x-wdl-internal-auth"),
      ]), [
        ["do", "test-internal-auth-token"],
        ["do-owner-network", "test-internal-auth-token"],
        ["workflows", "test-internal-auth-token"],
      ]);
    }
  );
});

test("createLoaderCallback: allows platform-tier runtime-load targets", async () => {
  /** @type {URL[]} */
  const fetchUrls = [];

  const env = {
    REDIS_PROXY_URL: "http://redis-proxy.local",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    PUBLIC_NETWORK: { kind: "public-network" },
  };

  await withMockedFetch(
    async (/** @type {any} */ url) => {
      const u = new URL(url);
      fetchUrls.push(u);
      return new Response(encodeRuntimeLoadPayload({
        bundle: {
          "__meta__": JSON.stringify({
            mainModule: "worker.js",
            modules: { "worker.js": { type: "module" } },
          }),
          "worker.js": "export default {};",
        },
        ns_secrets: {},
        worker_secrets: {},
      }), {
        status: 200,
        headers: { "content-type": RUNTIME_LOAD_CONTENT_TYPE },
      });
    },
    async () => {
    await createLoaderCallback({
      requestId: "rid-platform-load",
      env,
      ctx: makeCtx(),
      ns: "__platform__",
      worker: "platform-api",
      version: "v1",
      workerId: "__platform__:platform-api:v1",
    })();

    assert.equal(fetchUrls.length, 1);
    assert.equal(fetchUrls[0].searchParams.get("ns"), "__platform__");
    assert.equal(fetchUrls[0].searchParams.get("worker"), "platform-api");
    assert.equal(fetchUrls[0].searchParams.get("version"), "v1");
    }
  );
});

test("createLoaderCallback: rejects identities that do not match runtime-load grammar", async () => {
  let called = false;

  await withMockedFetch(
    async () => {
      called = true;
      return new Response("unexpected");
    },
    async () => {
      await assert.rejects(
        createLoaderCallback({
          requestId: "rid-bad-load",
          env: {
            REDIS_PROXY_URL: "http://redis-proxy.local",
            WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
            ASSETS_CDN_BASE: "https://assets.example",
            PUBLIC_NETWORK: { kind: "public-network" },
          },
          ctx: makeCtx(),
          ns: "__community__",
          worker: "community-api",
          version: "v1",
          workerId: "__community__:community-api:v1",
        })(),
        /Invalid runtime load worker identity/
      );
      assert.equal(called, false);
    }
  );
});

test("runtimeLoadContentTypeMatches ignores parameters without accepting wrong media types", () => {
  assert.equal(runtimeLoadContentTypeMatches(RUNTIME_LOAD_CONTENT_TYPE), true);
  assert.equal(
    runtimeLoadContentTypeMatches(` ${RUNTIME_LOAD_CONTENT_TYPE.toUpperCase()} ; charset=binary`),
    true
  );
  assert.equal(runtimeLoadContentTypeMatches(`${RUNTIME_LOAD_CONTENT_TYPE}; version=1`), true);
  assert.equal(
    runtimeLoadContentTypeMatches(`${RUNTIME_LOAD_CONTENT_TYPE}; charset=binary; foo=bar`),
    true
  );
  assert.equal(runtimeLoadContentTypeMatches("application/json"), false);
  assert.equal(runtimeLoadContentTypeMatches(`${RUNTIME_LOAD_CONTENT_TYPE}.evil`), false);
  assert.equal(runtimeLoadContentTypeMatches(null), false);
});

test("createLoaderCallback: rejects runtime load responses with the wrong content type", async () => {
  let calls = 0;

  const env = {
    REDIS_PROXY_URL: "http://redis-proxy.local",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    PUBLIC_NETWORK: { kind: "public-network" },
  };

  await withMockedFetch(
    async () => {
      calls++;
      return new Response("{}", {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
    async () => {
      await assert.rejects(
        createLoaderCallback({
          requestId: "rid-1",
          env,
          ctx: makeCtx(),
          ns: "demo",
          worker: "app",
          version: "v1",
          workerId: "demo:app:v1",
        })(),
        /unsupported content-type application\/json/
      );
      assert.equal(calls, 3);
    }
  );
});

test("createLoaderCallback: aborts hung redis proxy runtime load requests", async () => {
  let calls = 0;

  const env = {
    REDIS_PROXY_URL: "http://redis-proxy.local",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    PUBLIC_NETWORK: { kind: "public-network" },
  };

  await withMockedFetch(
    async (/** @type {any} */ _url, /** @type {any} */ init) => {
      calls++;
      assert.ok(init?.signal instanceof AbortSignal);
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => {
          reject(new DOMException("aborted", "AbortError"));
        }, { once: true });
      });
    },
    async () => {
      await assert.rejects(
        createLoaderCallback({
          requestId: "rid-timeout",
          env,
          ctx: makeCtx(),
          ns: "demo",
          worker: "app",
          version: "v1",
          workerId: "demo:app:v1",
        })(),
        /Redis proxy runtime load timed out after 5ms/
      );
      assert.equal(calls, 3);
    }
  );
});

/** @param {{ modules: Record<string, unknown> }} workerCode */
function workerCodeModuleBytes(workerCode) {
  let total = 0;
  for (const value of Object.values(workerCode.modules)) {
    if (typeof value === "string") {
      total += Buffer.byteLength(value, "utf8");
      continue;
    }
    const record = value && typeof value === "object"
      ? /** @type {{ cjs?: unknown }} */ (value)
      : null;
    if (typeof record?.cjs === "string") {
      total += Buffer.byteLength(record.cjs, "utf8");
      continue;
    }
    assert.fail(`unexpected workerCode module value ${JSON.stringify(value)}`);
  }
  return total;
}

test("workerLoader code estimator matches runtime wrapper injection exactly", () => {
  const entrypoint = `Api${"A".repeat(1024)}`;
  const source = 'import { WorkflowEntrypoint } from "cloudflare:workflows"; export default {};';
  const meta = {
    mainModule: "src/worker.js",
    modules: { "src/worker.js": { type: "module" } },
    bindings: { DB: { type: "d1", databaseId: "main" } },
    exports: [{ entrypoint }],
    workflows: [{ binding: "FLOW", name: "flow", className: "FlowHandler" }],
  };
  /** @type {{ mainModule: string, modules: Record<string, string> }} */
  const workerCode = {
    mainModule: meta.mainModule,
    modules: { "src/worker.js": source },
  };

  injectRuntimeModulesForHostBindings(workerCode, meta, STUB_RUNTIME_INJECTION_SOURCES);

  assert.equal(
    estimateFinalWorkerLoaderCodeBytes({
      mainModule: meta.mainModule,
      normalized: [["src/worker.js", Buffer.from(source)]],
      meta,
      runtimeSources: STUB_RUNTIME_INJECTION_SOURCES,
    }),
    workerCodeModuleBytes(workerCode)
  );
  assert.match(
    /** @type {string} */ (workerCode.modules["src/worker.js"]),
    /"\.\.\/_wdl-cloudflare-workflows\.js"/
  );
  assert.match(/** @type {string} */ (workerCode.modules["_wdl-wrapper.js"]), new RegExp(entrypoint));
});

test("workerLoader code estimator does not rewrite CommonJS workflow strings", () => {
  const source = 'module.exports = { label: "cloudflare:workflows" };';
  const meta = {
    mainModule: "src/worker.cjs",
    modules: { "src/worker.cjs": { type: "cjs" } },
    workflows: [{ binding: "FLOW", name: "flow", className: "FlowHandler" }],
  };
  /** @type {{ mainModule: string, modules: Record<string, string | { cjs: string }> }} */
  const injectedWorkerCode = {
    mainModule: meta.mainModule,
    modules: { [meta.mainModule]: { cjs: source } },
  };
  injectRuntimeModulesForHostBindings(injectedWorkerCode, meta, STUB_RUNTIME_INJECTION_SOURCES);
  assert.deepEqual(injectedWorkerCode.modules[meta.mainModule], { cjs: source });
  const injectedBytes = workerCodeModuleBytes(injectedWorkerCode) - Buffer.byteLength(source, "utf8");

  assert.equal(
    estimateFinalWorkerLoaderCodeBytes({
      mainModule: meta.mainModule,
      normalized: [["src/worker.cjs", Buffer.from(source)]],
      meta,
      runtimeSources: STUB_RUNTIME_INJECTION_SOURCES,
    }),
    Buffer.byteLength(source, "utf8") + injectedBytes
  );
});

test("wrapWorkerCodeForHostBindings rejects empty reserved module collisions", () => {
  for (const reserved of ["_wdl-wrapper.js", "_wdl-host-wrapper-runtime.js"]) {
    const workerCode = {
      mainModule: "worker.js",
      modules: {
        "worker.js": "export default {};",
        [reserved]: "",
      },
    };

    assert.throws(
      () => injectRuntimeModulesForHostBindings(workerCode, {
        mainModule: "worker.js",
        modules: { "worker.js": { type: "module" } },
      }, STUB_RUNTIME_INJECTION_SOURCES),
      /reserved module names/
    );
  }
});

test("wrapWorkerCodeForHostBindings: injects local D1 client wrapper and preserves original main module", () => {
  const workerCode = {
    mainModule: "src/worker.js",
    modules: {
      "src/worker.js": "import x from './lib.js'; export default { fetch() { return x; } };",
      "src/lib.js": "export default 1;",
    },
  };

  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: {
      DB: { type: "d1", databaseId: "main" },
      KV: { type: "kv", id: "cache" },
    },
    exports: [
      { entrypoint: "default", allowedCallers: ["*"] },
      { entrypoint: "Api", allowedCallers: ["*"] },
      { entrypoint: "Admin", allowedCallers: ["ops"] },
    ],
  });

  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.equal(/** @type {any} */ (workerCode.modules)["src/worker.js"], "import x from './lib.js'; export default { fetch() { return x; } };");
  assert.equal(/** @type {any} */ (workerCode.modules)["src/lib.js"], "export default 1;");
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-d1-data-field.js"], /setDataField/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-d1-params.js"], /normalizeD1Param/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-sql-splitter.js"], /splitSqlStatements/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-d1-transport.js"], /decodeD1Transport/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-d1-transport.js"], /from "\.\/_wdl-d1-data-field\.js";/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-request-id.js"], /requestIdFromOptions/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-d1-client.js"], /class D1Database/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-host-wrapper-runtime.js"], /intrinsicArrayForEach/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-host-wrapper-runtime.js"], /AsyncLocalStorage/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \* as __WdlHostRuntime__/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \* as __WdlUserModule__ from "\.\/src\/worker\.js";/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /Explicit aliases replace same-name star exports/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const D1_BINDINGS = \["DB"\];/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const R2_BINDINGS = \[\];/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const DO_BINDINGS = \[\];/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /new D1Database\(out\[name\], requestIdOptions\(requestIdOrContext\)\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /requestIdFromEventArg/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /wrapClassInstance\(this, requestContext\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /forEachArray\(HOST_WRAPPED_HANDLER_KEYS/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class extends __WdlUserModule__\.Api/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /__WdlWrappedEntrypoint\d+__ as Admin/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class default/);
  assert.equal(/** @type {any} */ (workerCode).compatibilityFlags, undefined);
});

test("wrapWorkerCodeForHostBindings preserves compatibility flags", () => {
  for (const input of [
    ["no_nodejs_als"],
    ["no_global_navigator", "no_nodejs_als"],
    ["nodejs_compat", "no_nodejs_als"],
  ]) {
    const workerCode = {
      compatibilityFlags: input,
      mainModule: "worker.js",
      modules: { "worker.js": "export default {};" },
    };
    wrapWorkerCodeForHostBindings(workerCode, {
      bindings: { DB: { type: "d1", databaseId: "main" } },
    });
    assert.deepEqual(workerCode.compatibilityFlags, input);
  }
});

test("wrapWorkerCodeForHostBindings: default object wraps inherited and accessor handler keys", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        class Worker {
          fetch(_request, env) {
            return Response.json({ name: env.DB?.constructor?.name, raw: env.DB?.raw === true });
          }
        }
        const worker = new Worker();
        Object.defineProperty(worker, "queue", {
          enumerable: true,
          get() {
            return (_queueName, env) => Response.json({ name: env.DB?.constructor?.name, raw: env.DB?.raw === true });
          },
        });
        export default worker;
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { DB: { type: "d1", databaseId: "main" } },
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });

  await withTempDir("wdl-default-wrapper-", async (dir) => {
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }
      export function abortIsolate() {}
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source.replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);
    const env = {
      DB: { raw: true },
      __WDL_HOST_BINDINGS_WRAPPED: true,
      [Symbol.for("wdl.host-bindings-wrapped")]: true,
    };

    assert.deepEqual(await (await wrapped.default.fetch(new Request("http://worker/"), env, {})).json(), {
      name: "D1Database",
      raw: false,
    });
    assert.deepEqual(await (await wrapped.default.queue("queue-a", env, {})).json(), {
      name: "D1Database",
      raw: false,
    });
  });
});

test("wrapWorkerCodeForHostBindings: declared named entrypoints are wrapper subclasses with D1 facade env", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        import * as hostRuntime from "./_wdl-host-wrapper-runtime.js";
        export function hostRuntimeContextExports() {
          return {
            currentRequestId: typeof hostRuntime.currentRequestId,
            runWithRequestContext: typeof hostRuntime.runWithRequestContext,
          };
        }
        export class Api {
          constructor(ctx, env) { this.env = env; }
          dbConstructorName() { return this.env.DB?.constructor?.name; }
          rawDbVisible() { return this.env.DB?.raw === true; }
          echo(value) { return value; }
        }
        export class currentRequestId extends Api {}
        export class runWithRequestContext extends Api {}
        export class __wdlRunWithRequestContext extends Api {}
        export default {};
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { DB: { type: "d1", databaseId: "main" } },
    exports: [
      { entrypoint: "Api", allowedCallers: ["*"] },
      { entrypoint: "currentRequestId", allowedCallers: ["*"] },
      { entrypoint: "runWithRequestContext", allowedCallers: ["*"] },
      { entrypoint: "__wdlRunWithRequestContext", allowedCallers: ["*"] },
    ],
  });

  await withTempDir("wdl-d1-wrapper-", async (dir) => {
    // Node can't resolve "cloudflare:workers" — workerd-only specifier.
    // Substitute with a local stub so the wrapper module loads under
    // node:test, where we exercise the D1 wrapping behavior.
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }
      export function abortIsolate() {}
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source
          .replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const user = await import(`file://${path.join(dir, "worker.js")}`);
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);

    assert.notEqual(wrapped.Api, user.Api);
    assert.ok(wrapped.Api.prototype instanceof user.Api);
    const instance = new wrapped.Api({}, {
      DB: { raw: true },
      __WDL_HOST_BINDINGS_WRAPPED: true,
      [Symbol.for("wdl.host-bindings-wrapped")]: true,
    });
    assert.equal(instance.dbConstructorName(), "D1Database");
    assert.equal(instance.rawDbVisible(), false);
    for (const name of ["currentRequestId", "runWithRequestContext", "__wdlRunWithRequestContext"]) {
      assert.ok(new wrapped[name]({}, { DB: { raw: true } }) instanceof user[name]);
    }

    assert.deepEqual(wrapped.hostRuntimeContextExports(), {
      currentRequestId: "undefined",
      runWithRequestContext: "undefined",
    });
  });
});

test("wrapWorkerCodeForHostBindings: workers without host facades get only the abort shim", () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: { "worker.js": "export default {};" },
  };

  wrapWorkerCodeForHostBindings(workerCode, { bindings: { KV: { type: "kv", id: "cache" } } });

  // Eviction shim is unconditional — the entire point is that workerd's
  // workerLoader cache never evicts on its own, so every loaded worker
  // must expose the abort entrypoint.
  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.equal(/** @type {any} */ (workerCode.modules)["worker.js"], "export default {};");
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class __WdlAbort__/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class __WdlWorkflowNotify__/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /abortIsolate\(reason \?\? "wdl-evict"\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export default wrappedDefault/);
  // No host facade helper modules — those only appear when the corresponding
  // binding type is actually used.
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-d1-client.js"], undefined);
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-r2-client.js"], undefined);
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-do-client.js"], undefined);
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-host-wrapper-runtime.js"], undefined);
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-do-transport.js"], undefined);
  assert.equal(/** @type {any} */ (workerCode.modules)["_wdl-request-id.js"], undefined);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-cloudflare-workflows.js"], /class NonRetryableError extends Error/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const D1_BINDINGS/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const R2_BINDINGS/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const DO_BINDINGS/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /function wrapEnv/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /requestIdFromEventArg/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \{ D1Database \}/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \{ R2Bucket \}/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \{ DurableObjectNamespace \}/);
});

test("wrapWorkerCodeForHostBindings: abort-only shim does not inspect named entrypoint exports", () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: { "worker.js": "export const Api = {}; export default {};" },
  };

  assert.doesNotThrow(() => wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { KV: { type: "kv", id: "cache" } },
    exports: [{ entrypoint: "not-valid-js-identifier", allowedCallers: ["*"] }],
  }));

  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export \* from "\.\/worker\.js";/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export class not-valid-js-identifier/);
});

test("wrapWorkerCodeForHostBindings: abort-only shim preserves user exports", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        export const Api = { value: 1 };
        export default { fetch() { return "ok"; } };
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, { bindings: {} });

  await withTempDir("wdl-abort-wrapper-", async (dir) => {
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint {}
      export function abortIsolate() {}
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source.replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const user = await import(`file://${path.join(dir, "worker.js")}`);
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);

    assert.equal(wrapped.default, user.default);
    assert.equal(wrapped.Api, user.Api);
    assert.equal(typeof wrapped.__WdlAbort__, "function");
  });
});

test("wrapWorkerCodeForHostBindings: abort-only shim adapts function default to fetch handler", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        export default function(request, env) {
          return new Response(env.PREFIX + new URL(request.url).pathname);
        }
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, { bindings: { KV: { type: "kv", id: "cache" } } });

  await withTempDir("wdl-abort-function-wrapper-", async (dir) => {
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint {}
      export function abortIsolate() {}
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source.replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);

    assert.equal(typeof wrapped.default.fetch, "function");
    const response = await wrapped.default.fetch(
      new Request("https://demo.workers.example/ok"),
      { PREFIX: "fn:" },
      {}
    );
    assert.equal(await response.text(), "fn:/ok");
  });
});

test("wrapWorkerCodeForHostBindings: injects local R2 facade for R2 bindings", () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export default { fetch() { return new Response('ok'); } };",
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { BUCKET: { type: "r2", bucketName: "uploads" } },
  });
  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-r2-client.js"], /class R2Bucket/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-request-id.js"], /requestIdFromOptions/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-r2-client.js"], /this\._stub/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const R2_BINDINGS = \["BUCKET"\];/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /new R2Bucket\(out\[name\], requestIdOptions\(requestIdOrContext\)\)/);
});

test("wrapWorkerCodeForHostBindings: injects local DO facade for Durable Object bindings", () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": "export class Room {}; export default { fetch() { return new Response('ok'); } };",
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { ROOMS: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" } },
  });
  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-do-client.js"], /class DurableObjectNamespace/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-do-transport.js"], /function requestSpec/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-owner-endpoint.js"], /validOwnerEndpointForService/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-request-id.js"], /requestIdFromOptions/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const DO_BINDINGS = \["ROOMS"\];/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /new DurableObjectNamespace\(out\[name\], doOptions\(requestIdOrContext, doBackend, doOwnerNetwork\)\)/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /internalAuthToken|__WDL_INTERNAL_AUTH_TOKEN__/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class extends __WdlUserModule__\.Room/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export \* from/);
});

test("wrapWorkerCodeForHostBindings: injects local Workflow facade and wraps workflow class", () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": "import \"./setup.js\"; import\"cloudflare:workflows\"; import/*side-effect*/\"cloudflare:workflows\"; import { NonRetryableError } from \"cloudflare:workflows\"\nconst label = \"x\";\nexport {}; export { WorkflowEntrypoint } from \"cloudflare:workflows\";\nexport class OrderWorkflow { label() { return \"cloudflare:workflows\"; } text() { return 'import { X } from \"cloudflare:workflows\"'; } fail() { throw new NonRetryableError('x'); } }; export default { fetch() { return new Response('ok'); } };",
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    workflows: [
      {
        name: "orders",
        binding: "ORDERS",
        className: "OrderWorkflow",
        workflowKey: "wf_0123456789abcdef0123456789abcdef",
      },
    ],
  });
  assert.equal(workerCode.mainModule, "_wdl-wrapper.js");
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["worker.js"], /import"cloudflare:workflows"/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["worker.js"], /import\/\*side-effect\*\/"cloudflare:workflows"/);
  assert.match(/** @type {any} */ (workerCode.modules)["worker.js"], /from "\.\/_wdl-cloudflare-workflows\.js"/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["worker.js"], /export \{ WorkflowEntrypoint \} from "cloudflare:workflows"/);
  assert.match(/** @type {any} */ (workerCode.modules)["worker.js"], /return "cloudflare:workflows"/);
  assert.match(/** @type {any} */ (workerCode.modules)["worker.js"], /return 'import \{ X \} from "cloudflare:workflows"'/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-workflows-client.js"], /class Workflow/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-request-id.js"], /requestIdFromOptions/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-cloudflare-workflows.js"], /export \{ WorkflowEntrypoint \}/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-cloudflare-workflows.js"], /this\.name = "NonRetryableError"/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /import \{ Workflow \}/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /const WORKFLOW_BINDINGS = \{"ORDERS":/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /new Workflow\(out\[name\] \|\| metadata, workflowOptions\(requestIdOrContext, workflowsBackend\)\)/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /internalAuthToken|__WDL_INTERNAL_AUTH_TOKEN__/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /notifyWorkflowCallback/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /notifyWorkflowCallback\(request, wrapEnv\(this\.env, requestIdFromEventArg\(request\)\)\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /class extends __WdlUserModule__\.OrderWorkflow/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export \* from "\.\/worker\.js";/);
});

test("wrapWorkerCodeForHostBindings: rewrites workflow module imports relative to nested modules", () => {
  const workerCode = {
    mainModule: "src/index.js",
    modules: {
      "src/index.js": [
        "import {",
        "  WorkflowEntrypoint",
        "} from \"cloudflare:workflows\";",
        "import { \"cloudflare:workflows\" as wfName } from \"./other.js\";",
        "import { from as importedFrom, \"cloudflare:workflows\" as quotedName } from \"./other.js\";",
        "import { \"WorkflowEntrypoint\" as QuotedEntrypoint } from \"cloudflare:workflows\";",
        "/* @preserve */ import { NonRetryableError as PreservedError } from \"cloudflare:workflows\";",
        "/* @preserve */ export { WorkflowEntrypoint as PreservedEntrypoint } from \"cloudflare:workflows\";",
        "const loader = { import: (specifier) => specifier };",
        "const untouchedMethod = loader.import(\"cloudflare:workflows\");",
        "const untouchedCommentedMethod = loader./* comment */import(\"cloudflare:workflows\");",
        "const untouchedNewlineMethod = loader.",
        "  import(\"cloudflare:workflows\");",
        "const untouchedBlockCommentNewlineMethod = loader. /* comment */",
        "  import(\"cloudflare:workflows\");",
        "const untouchedLineCommentedMethod = loader. // comment",
        "  import(\"cloudflare:workflows\");",
        "const untouchedCrlfLineCommentedMethod = loader. // comment\r",
        "  import(\"cloudflare:workflows\");",
        "const stringCommentMarker = \"a.//\";",
        "const dynamicAfterStringCommentMarker = import(\"cloudflare:workflows\");",
        "const regexCommentMarker = /[.//]/;",
        "const dynamicAfterRegexCommentMarker = import(\"cloudflare:workflows\");",
        "class PrivateMethodImport { #import(value) { return value; } m() { return this.#import(\"cloudflare:workflows\"); } }",
        "const untouchedImportMetaResolve = import.meta.resolve /* comment */ (\"cloudflare:workflows\");",
        "const regexFromReturn = () => { return /import\\(\"cloudflare:workflows\"\\)/; };",
        "if (true)",
        "  /import\\(\"cloudflare:workflows\"\\)/.test(\"x\");",
        "if (true) /import\\(\"cloudflare:workflows\"\\)/.test(\"same-line\");",
        "else /import\\(\"cloudflare:workflows\"\\)/.test(\"else-line\");",
        "function regexAfterFunction() {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function\");",
        "const regexAfterArrow = () => {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-arrow\");",
        "{}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-bare-block\");",
        "try {} catch (e) {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-try-catch\");",
        "if (label) {} else {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-else\");",
        "label: {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-label\");",
        "switch (label) { default: break; }",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-switch\");",
        "{ const s = \"}\"; }",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-string-brace\");",
        "function braceInString() { const s = \"}\"; }",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function-string-brace\");",
        "if (parenSource === \")\") {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-if-string-paren\");",
        "function parenInDefault(arg = \")\") {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function-string-paren\");",
        "{ const r = /[}//]/; }",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-regex-brace\");",
        "{",
        "  if (true) {}",
        "  /}/.test(\"}\");",
        "}",
        "/import(\"cloudflare:workflows\")/g.test(\"after-outer-block-regex-brace\");",
        "class RegexAfterClass {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-class\");",
        "class RegexAfterExtendedClass extends Base {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-extended-class\");",
        "class RegexAfterMixinClass extends mixin(Base) {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-mixin-class\");",
        "class RegexAfterExpressionClass extends (class {}) {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-expression-class\");",
        "class RegexAfterExpressionClassActual extends (class {}) {}",
        "/import(\"cloudflare:workflows\")/g.test(\"after-expression-class-actual\");",
        "export class RegexAfterExportMixinClass extends mixin(Base) {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-mixin-class\");",
        "export class RegexAfterExportClass {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-class\");",
        "export default class RegexAfterExportDefaultClass {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-default-class\");",
        "export function regexAfterExportFunction() {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-function\");",
        "export default function regexAfterExportDefaultFunction() {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-default-function\");",
        "export async function regexAfterExportAsyncFunction() {}",
        "/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-async-function\");",
        "if (true) /; import { X } from \"cloudflare:workflows\"/.test(\"static-looking-regex\");",
        "const regexType = typeof /import\\(\"cloudflare:workflows\"\\)/;",
        "const cfg = {",
        "  import: \"x\",",
        "  from: \"cloudflare:workflows\",",
        "};",
        "class ImportFields {",
        "  import = \"x\"",
        "  from = \"cloudflare:workflows\"",
        "}",
        "export const from = \"cloudflare:workflows\";",
        "const untouchedRegex = /import\\(\"cloudflare:workflows\"\\)/;",
        "const untouchedTemplate = `import(\"cloudflare:workflows\")`;",
        "const divisionFromMember = obj.in / import(\"cloudflare:workflows\") / x;",
        "class PrivateImportFields { #in; m(obj) { return obj.#in / import(\"cloudflare:workflows\") / x; } }",
        "const divisionAfterCall = foo()",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterObject = ({})",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterObjectClassProp = ({ class: 1 })",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterObjectClassMethod = ({ class() {} })",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterClass = class {}",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterFunctionExpression = function() {}",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterAsyncFunctionExpression = async function() {}",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterWrappedFunctionExpression =",
        "function() {}",
        "/import(\"cloudflare:workflows\")/g;",
        "const divisionAfterWrappedAsyncFunctionExpression =",
        "async function() {}",
        "/import(\"cloudflare:workflows\")/g;",
        "const dynamicModule = import(\"cloudflare:workflows\");",
        "const dynamicModuleWithInnerComment = import(/* @vite-ignore */ \"cloudflare:workflows\");",
        "const dynamicModuleWithOuterComment = import /* comment */ (\"cloudflare:workflows\");",
        "const templateRegexExpression = `${(() => {",
        "  if (true) /import(\"cloudflare:workflows\")/.test(\"template-regex\");",
        "  return 1;",
        "})()}`;",
        "const templateBlockRegexExpression = `${(() => {",
        "  if (true) {}",
        "  /import(\"cloudflare:workflows\")/.test(\"template-block-regex\");",
        "  return 1;",
        "})()}`;",
        "export class OrderWorkflow extends WorkflowEntrypoint { async load() { return `${await import(\"cloudflare:workflows\")}`; } }",
        "export default { fetch() { return new Response('ok'); } };",
      ].join("\n"),
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    workflows: [
      {
        name: "orders",
        binding: "ORDERS",
        className: "OrderWorkflow",
        workflowKey: "wf_0123456789abcdef0123456789abcdef",
      },
    ],
  });
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /from "\.\.\/_wdl-cloudflare-workflows\.js"/);
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("import { \"cloudflare:workflows\" as wfName } from \"./other.js\";"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("import { from as importedFrom, \"cloudflare:workflows\" as quotedName } from \"./other.js\";"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("import { \"WorkflowEntrypoint\" as QuotedEntrypoint } from \"../_wdl-cloudflare-workflows.js\";"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/* @preserve */ import { NonRetryableError as PreservedError } from \"../_wdl-cloudflare-workflows.js\";"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/* @preserve */ export { WorkflowEntrypoint as PreservedEntrypoint } from \"../_wdl-cloudflare-workflows.js\";"));
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /loader\.import\("cloudflare:workflows"\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /loader\.\/\* comment \*\/import\("cloudflare:workflows"\)/);
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("loader.\n  import(\"cloudflare:workflows\")"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("loader. /* comment */\n  import(\"cloudflare:workflows\")"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("loader. // comment\n  import(\"cloudflare:workflows\")"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("loader. // comment\r\n  import(\"cloudflare:workflows\")"));
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const dynamicAfterStringCommentMarker = import\("\.\.\/_wdl-cloudflare-workflows\.js"\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const dynamicAfterRegexCommentMarker = import\("\.\.\/_wdl-cloudflare-workflows\.js"\)/);
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("this.#import(\"cloudflare:workflows\")"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("import.meta.resolve /* comment */ (\"cloudflare:workflows\")"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("return /import\\(\"cloudflare:workflows\"\\)/;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("  /import\\(\"cloudflare:workflows\"\\)/.test(\"x\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("if (true) /import\\(\"cloudflare:workflows\"\\)/.test(\"same-line\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("else /import\\(\"cloudflare:workflows\"\\)/.test(\"else-line\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-arrow\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-bare-block\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-try-catch\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-else\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-label\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-switch\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-string-brace\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function-string-brace\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-if-string-paren\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-function-string-paren\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-regex-brace\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import(\"cloudflare:workflows\")/g.test(\"after-outer-block-regex-brace\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-extended-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-mixin-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-expression-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import(\"cloudflare:workflows\")/g.test(\"after-expression-class-actual\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-mixin-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-default-class\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-function\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-default-function\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import\\(\"cloudflare:workflows\"\\)/.test(\"after-export-async-function\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("if (true) /; import { X } from \"cloudflare:workflows\"/.test(\"static-looking-regex\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const regexType = typeof /import\\(\"cloudflare:workflows\"\\)/;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("from: \"cloudflare:workflows\""));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("from = \"cloudflare:workflows\""));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("export const from = \"cloudflare:workflows\";"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const untouchedRegex = /import\\(\"cloudflare:workflows\"\\)/;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const untouchedTemplate = `import(\"cloudflare:workflows\")`;"));
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const divisionFromMember = obj\.in \/ import\("\.\.\/_wdl-cloudflare-workflows\.js"\) \/ x;/);
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /return obj\.#in \/ import\("\.\.\/_wdl-cloudflare-workflows\.js"\) \/ x;/);
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterCall = foo()\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterObject = ({})\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterObjectClassProp = ({ class: 1 })\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterObjectClassMethod = ({ class() {} })\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterClass = class {}\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterFunctionExpression = function() {}\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterAsyncFunctionExpression = async function() {}\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterWrappedFunctionExpression =\nfunction() {}\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("const divisionAfterWrappedAsyncFunctionExpression =\nasync function() {}\n/import(\"../_wdl-cloudflare-workflows.js\")/g;"));
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const dynamicModule = import\("\.\.\/_wdl-cloudflare-workflows\.js"\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const dynamicModuleWithInnerComment = import\(\/\* @vite-ignore \*\/ "\.\.\/_wdl-cloudflare-workflows\.js"\)/);
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /const dynamicModuleWithOuterComment = import \/\* comment \*\/ \("\.\.\/_wdl-cloudflare-workflows\.js"\)/);
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("if (true) /import(\"cloudflare:workflows\")/.test(\"template-regex\");"));
  assert.ok(/** @type {any} */ (workerCode.modules)["src/index.js"].includes("/import(\"cloudflare:workflows\")/.test(\"template-block-regex\");"));
  assert.match(/** @type {any} */ (workerCode.modules)["src/index.js"], /`\$\{await import\("\.\.\/_wdl-cloudflare-workflows\.js"\)\}`/);
  assert.doesNotMatch(/** @type {any} */ (workerCode.modules)["_wdl-wrapper.js"], /export \* from "\.\/src\/index\.js";/);
});

test("wrapWorkerCodeForHostBindings: DO wrappers hide internal backend from undeclared entrypoints", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        export class Room {}
        export class RawEntrypoint {}
        export default {
          fetch(_request, env) {
            return Response.json({
              backend: Object.hasOwn(env, "__WDL_DO_BACKEND__"),
              ownerNetwork: Object.hasOwn(env, "__WDL_DO_OWNER_NETWORK__"),
              futureInternal: Object.hasOwn(env, "__WDL_FUTURE_INTERNAL__"),
            });
          },
        };
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    bindings: { ROOMS: { type: "do", className: "Room", doStorageId: "do_0123456789abcdef0123456789abcdef" } },
  });

  await withTempDir("wdl-do-wrapper-", async (dir) => {
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint {}
      export function abortIsolate() {}
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source.replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);

    assert.equal("RawEntrypoint" in wrapped, false);
    assert.equal("Room" in wrapped, true);
    const response = await wrapped.default.fetch(
      new Request("https://demo.workers.example/"),
      {
        ROOMS: {
          ns: "demo",
          worker: "chat",
          version: "v1",
          doStorageId: "do_0123456789abcdef0123456789abcdef",
          binding: "ROOMS",
          className: "Room",
        },
        __WDL_DO_BACKEND__: { fetch() {} },
        __WDL_DO_OWNER_NETWORK__: { fetch() {} },
        __WDL_FUTURE_INTERNAL__: { fetch() {} },
      },
      {}
    );
    await assertJsonResponse(response, 200, {
      backend: false,
      ownerNetwork: false,
      futureInternal: false,
    });
  });
});

test("wrapWorkerCodeForHostBindings: workflow wrappers hide internal backend from undeclared entrypoints", async () => {
  const workerCode = {
    mainModule: "worker.js",
    modules: {
      "worker.js": `
        export class OrderWorkflow {}
        export class RawEntrypoint {}
        export default {
          fetch(_request, env) {
            return Response.json({
              workflowsBackend: Object.hasOwn(env, "__WDL_WORKFLOWS_BACKEND__"),
            });
          },
        };
      `,
    },
  };
  wrapWorkerCodeForHostBindings(workerCode, {
    workflows: [{
      name: "orders",
      binding: "ORDERS",
      className: "OrderWorkflow",
      workflowKey: "wf_test",
    }],
  });

  await withTempDir("wdl-workflow-wrapper-", async (dir) => {
    const cwStub = path.join(dir, "_cf_workers_stub.js");
    writeFileSync(cwStub, `
      export class WorkerEntrypoint {}
      export function abortIsolate() {}
    `);
    writeFileSync(path.join(dir, "_wdl-workflows-client.js"), `
      export class Workflow {
        constructor(metadata) { this.metadata = metadata; }
      }
    `);
    for (const [name, source] of Object.entries(workerCode.modules)) {
      const file = path.join(dir, name);
      const stubbed = name === "_wdl-wrapper.js"
        ? source.replace(`from "cloudflare:workers"`, `from "./_cf_workers_stub.js"`)
        : source;
      writeFileSync(file, stubbed);
    }
    const wrapped = await import(`file://${path.join(dir, workerCode.mainModule)}`);

    assert.equal("RawEntrypoint" in wrapped, false);
    assert.equal("OrderWorkflow" in wrapped, true);
    const response = await wrapped.default.fetch(
      new Request("https://demo.workers.example/"),
      {
        ORDERS: {
          name: "orders",
          binding: "ORDERS",
          className: "OrderWorkflow",
          workflowKey: "wf_test",
        },
        __WDL_WORKFLOWS_BACKEND__: { fetch() {} },
      },
      {}
    );
    await assertJsonResponse(response, 200, {
      workflowsBackend: false,
    });
  });
});

test("wrapWorkerCodeForHostBindings: rejects reserved wrapper module as mainModule", () => {
  assert.throws(
    () => wrapWorkerCodeForHostBindings(
      {
        mainModule: "_wdl-wrapper.js",
        modules: { "src/worker.js": "export default {};", "_wdl-owner-endpoint.js": "export default {};" },
      },
      { bindings: { DB: { type: "d1", databaseId: "main" } } }
    ),
    /reserved module names/
  );
});

test("wrapWorkerCodeForHostBindings: stored exports cannot target reserved runtime entrypoints", () => {
  assert.throws(
    () =>
      wrapWorkerCodeForHostBindings(
        {
          mainModule: "worker.js",
          modules: { "worker.js": "export default {}" },
        },
        {
          bindings: { DB: { type: "d1", id: "db1" } },
          exports: [{ entrypoint: "__WdlAbort__", allowedCallers: ["*"] }],
        }
      ),
    /Exported entrypoint targets reserved runtime entrypoint "__WdlAbort__"/
  );
});
