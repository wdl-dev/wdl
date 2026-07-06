import assert from "node:assert/strict";
import { beforeEach, afterEach, test } from "node:test";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { parseJsonText } from "../helpers/json-payload.js";
import { installMockFetch, withMockedFetch } from "../helpers/mock-fetch.js";
import { withMockedGlobal } from "../helpers/mock-global.js";
import { installConsoleMethodCapture } from "../helpers/output-capture.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

/** @type {any} */ (globalThis).__doLoadFetches = [];
/** @type {any} */ (globalThis).__doLoadResponses = [];
/** @type {any} */ (globalThis).__doLoadWarnings = [];

const runtimeLibUrl = moduleDataUrl(`
export function bundleToWorkerCode(bundle) { return bundle; }
`);
const runtimeLoadUrl = moduleDataUrl(`
export function buildWorkerEnv() { return {}; }
export function internalAuthBackend(ctx, env, binding) {
  return typeof ctx.exports.InternalAuthBackend === "function"
    ? ctx.exports.InternalAuthBackend({ props: { binding } })
    : env[binding];
}
export function decodeRuntimeLoadPayload(buffer) {
  return JSON.parse(new TextDecoder().decode(buffer));
}
export function runtimeLoadContentTypeMatches(value) {
  return /^application\\/json\\b/.test(value || "");
}
export function wrapWorkerCodeForHostBindings(workerCode) {
  if (typeof /** @type {any} */ (globalThis).__doRuntimeHostWrap === "function") {
    /** @type {any} */ (globalThis).__doRuntimeHostWrap(workerCode);
  }
}
`);
const protocolUrl = moduleDataUrl(`
export class DoRuntimeError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.name = "DoRuntimeError";
    this.status = status;
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}
`);
const observabilityUrl = moduleDataUrl(`
export function formatError(err) {
  return { error_name: err.name, error_message: err.message, error_code: err.code };
}
export function logStructured(_service, _level, _event, fields = {}) {
  /** @type {any} */ (globalThis).__doLoadWarnings.push(fields);
}
`);
const alarmShimSourceUrl = repositoryFileUrl("do-runtime/alarm-shim-source.js");
const doRuntimeLoadCodeBudgetUrl = repositoryFileUrl("do-runtime/load-code-budget.js");
const sharedInternalAuthUrlForTest = sharedInternalAuthUrl();
const sharedRespondUrlForTest = repositoryFileUrl("shared/respond.js");

const src = applyModuleReplacements(readRepositoryFile("do-runtime/load.js"), [
  [/from "runtime-lib";/, `from ${JSON.stringify(runtimeLibUrl)};`],
  [/from "runtime-load";/, `from ${JSON.stringify(runtimeLoadUrl)};`],
  [/from "do-runtime-protocol";/, `from ${JSON.stringify(protocolUrl)};`],
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "shared-internal-auth";/, `from ${JSON.stringify(sharedInternalAuthUrlForTest)};`],
  [/from "shared-respond";/, `from ${JSON.stringify(sharedRespondUrlForTest)};`],
  [/from "do-runtime-alarm-shim-source";/, `from ${JSON.stringify(alarmShimSourceUrl)};`],
  [/from "do-runtime-load-code-budget";/, `from ${JSON.stringify(doRuntimeLoadCodeBudgetUrl)};`],
]);

const mod = await import(moduleDataUrl(src));
let restoreFetch = () => {};
let restoreConsoleWarn = () => {};

beforeEach(() => {
  restoreConsoleWarn();
  restoreConsoleWarn = () => {};
  /** @type {any} */ (globalThis).__doLoadFetches = [];
  /** @type {any} */ (globalThis).__doLoadResponses = [];
  /** @type {any} */ (globalThis).__doLoadWarnings = [];
  /** @type {any} */ (globalThis).__doRuntimeHostWrap = null;
  restoreFetch = installMockFetch(async (/** @type {any} */ url) => {
    /** @type {any} */ (globalThis).__doLoadFetches.push(String(url));
    const next = /** @type {any} */ (globalThis).__doLoadResponses.shift();
    if (next instanceof Error) throw next;
    return next;
  });
  restoreConsoleWarn = installConsoleMethodCapture(
    "warn",
    /** @type {any[]} */ (/** @type {any} */ (globalThis).__doLoadWarnings),
    (line) => parseJsonText(String(line), "DO runtime load warning log")
  );
});

afterEach(() => {
  restoreFetch();
  restoreFetch = () => {};
  restoreConsoleWarn();
  restoreConsoleWarn = () => {};
});

function invoke() {
  return {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    workerId: "tenant:chat:v1",
  };
}

/**
 * @param {number} status
 * @param {any} body
 */
function jsonLoadResponse(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/**
 * @param {unknown} err
 * @param {{ status: number, code: string, details?: unknown }} expect
 */
function doErrorMatches(err, { status, code, details = undefined }) {
  const actual = /** @type {{ status?: number, code?: string, details?: unknown }} */ (err);
  return (
    actual?.status === status &&
    actual?.code === code &&
    (details === undefined || JSON.stringify(actual.details) === JSON.stringify(details))
  );
}

test("DO runtime load: retries transient proxy failures before decoding payload", async () => {
  /** @type {any} */ (globalThis).__doLoadResponses.push(
    jsonLoadResponse(503, { error: "temporary" }),
    new Error("socket reset"),
    jsonLoadResponse(200, { bundle: { __meta__: {} } })
  );

  const loaded = await mod.loadViaProxy({
    REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
  }, invoke(), "req-1");

  assert.deepEqual(loaded, { bundle: { __meta__: {} } });
  assert.equal(/** @type {any} */ (globalThis).__doLoadFetches.length, 3);
  assert.deepEqual(/** @type {any} */ (globalThis).__doLoadWarnings.map((/** @type {any} */ entry) => entry.attempt), [1, 2]);
});

test("DO runtime load: does not retry non-retryable proxy responses", async () => {
  /** @type {any} */ (globalThis).__doLoadResponses.push(jsonLoadResponse(404, { error: "missing" }));

  await assert.rejects(
    mod.loadViaProxy({ REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" }, invoke(), "req-2"),
    (err) => doErrorMatches(err, {
      status: 404,
      code: "bundle_not_found",
      details: { upstreamStatus: 404 },
    })
  );

  assert.equal(/** @type {any} */ (globalThis).__doLoadFetches.length, 1);
});

test("DO runtime load: invalid content-type fails without retrying", async () => {
  /** @type {any} */ (globalThis).__doLoadResponses.push(new Response("not json", {
    status: 200,
    headers: { "content-type": "text/plain" },
  }));

  await assert.rejects(
    mod.loadViaProxy({ REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" }, invoke(), "req-3"),
    (err) => doErrorMatches(err, { status: 503, code: "bundle_load_invalid_content_type" })
  );

  assert.equal(/** @type {any} */ (globalThis).__doLoadFetches.length, 1);
});

test("DO runtime load: abort timeout is retried inside the load budget", async () => {
  /** @type {number[]} */
  const timeouts = [];
  await withMockedGlobal(
    "setTimeout",
    /** @type {typeof setTimeout} */ (/** @type {unknown} */ ((/** @type {() => void} */ callback, /** @type {number} */ ms) => {
      timeouts.push(ms);
      queueMicrotask(callback);
      return { ms };
    })),
    async () => withMockedGlobal(
      "clearTimeout",
      /** @type {typeof clearTimeout} */ (() => {}),
      async () => {
        await withMockedFetch(
          async (/** @type {any} */ url, /** @type {any} */ init = {}) => {
            /** @type {any} */ (globalThis).__doLoadFetches.push(String(url));
            return await new Promise((resolve, reject) => {
              init.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
            });
          },
          async () => {
            await assert.rejects(
              mod.loadViaProxy({ REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" }, invoke(), "req-timeout"),
              (err) => doErrorMatches(err, { status: 503, code: "bundle_load_timeout" })
            );
          }
        );
      }
    )
  );

  assert.equal(/** @type {any} */ (globalThis).__doLoadFetches.length, 3);
  assert.deepEqual(timeouts, [5000, 100, 5000, 200, 5000]);
  assert.deepEqual(/** @type {any} */ (globalThis).__doLoadWarnings.map((/** @type {any} */ entry) => entry.attempt), [1, 2]);
});

test("DO runtime load: applies the host-binding wrapper before the alarm wrapper", async () => {
  /** @type {any} */ (globalThis).__doRuntimeHostWrap = (/** @type {any} */ workerCode) => {
    workerCode.modules["_wdl-wrapper.js"] = "export class Room {}; export default {};";
    workerCode.mainModule = "_wdl-wrapper.js";
  };
  /** @type {any} */ (globalThis).__doLoadResponses.push(jsonLoadResponse(200, {
    bundle: {
      meta: { bindings: { ROOM: { type: "do", className: "Room" } } },
      compatibilityFlags: ["nodejs_compat", "delete_all_deletes_alarm"],
      mainModule: "worker.js",
      modules: {
        "worker.js": "export class Room {}; export default {};",
      },
    },
  }));

  const loaded = await mod.loadDoWorkerCode({
    REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    ASSETS_CDN_BASE: "https://assets.example",
    DO_BACKEND: { fetch() {} },
  }, {
    exports: {
      InternalAuthBackend: (/** @type {{ props: any }} */ { props }) => ({ kind: "internal-auth-backend", props }),
      DoAlarmBinding: (/** @type {{ props: any }} */ { props }) => ({ props }),
    },
  }, invoke(), "req-wrapper");

  assert.equal(loaded.mainModule, "_wdl-do-runtime-wrapper.js");
  assert.deepEqual(loaded.compatibilityFlags, ["nodejs_compat", "delete_all_preserves_alarm"]);
  assert.equal(loaded.modules["_wdl-wrapper.js"], "export class Room {}; export default {};");
  assert.match(loaded.modules["_wdl-do-alarm-shim.js"], /function withoutInternalEnv/);
  assert.match(loaded.modules["_wdl-do-alarm-shim.js"], /function deleteAllKvStorage/);
  assert.match(loaded.modules["_wdl-do-alarm-shim.js"], /function deleteAllSqlStorage/);
  assert.match(loaded.modules["_wdl-do-alarm-shim.js"], /export function wrapDurableObjectClass/);
  assert.match(loaded.modules["_wdl-do-alarm-shim.js"], /delete out\[ALARMS_BINDING\];/);
  assert.doesNotMatch(loaded.modules["_wdl-do-alarm-shim.js"], /delete out\.__WDL_HOST_BINDINGS_WRAPPED__/);
  const wrapper = loaded.modules["_wdl-do-runtime-wrapper.js"];
  assert.match(wrapper, /import \* as user from "\.\/_wdl-wrapper\.js";/);
  assert.match(wrapper, /import \{ wrapDurableObjectClass \} from "\.\/_wdl-do-alarm-shim\.js";/);
  assert.match(wrapper, /export class Room extends wrapDurableObjectClass\(user\.Room, "Room"\)/);
});

test("DO runtime load: rejects reserved alarm shim module collisions", async () => {
  /** @type {any} */ (globalThis).__doLoadResponses.push(jsonLoadResponse(200, {
    bundle: {
      meta: { bindings: { ROOM: { type: "do", className: "Room" } } },
      mainModule: "worker.js",
      modules: {
        "worker.js": "export class Room {}; export default {};",
        "_wdl-do-alarm-shim.js": "export default {};",
      },
    },
  }));

  await assert.rejects(
    mod.loadDoWorkerCode({
      REDIS_PROXY_URL: "http://redis-proxy", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
      ASSETS_CDN_BASE: "https://assets.example",
      DO_BACKEND: { fetch() {} },
    }, {
      exports: {
        InternalAuthBackend: (/** @type {{ props: any }} */ { props }) => ({ kind: "internal-auth-backend", props }),
        DoAlarmBinding: (/** @type {{ props: any }} */ { props }) => ({ props }),
      },
    }, invoke(), "req-reserved"),
    (err) => doErrorMatches(err, { status: 400, code: "reserved_module_name" })
  );
});
