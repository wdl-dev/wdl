import { test } from "node:test";
import assert from "node:assert/strict";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const observabilityUrl = repositoryFileUrl("shared/observability.js");
const errorsUrl = repositoryFileUrl("shared/errors.js");
const workerIdUrl = repositoryFileUrl("shared/worker-id.js");

// Eviction tests don't exercise the request-scope path; stub it inline so we
// don't need to chain-rewrite shared/request-scope.js's own bare imports.
const requestScopeStubUrl = moduleDataUrl(`
export function createHttpRequestScope() {
  throw new Error("eviction tests must not call createHttpRequestScope");
}
`);

const src = applyModuleReplacements(readRepositoryFile("runtime/runtime.js"), [
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "shared-errors";/, `from ${JSON.stringify(errorsUrl)};`],
  [/from "shared-request-scope";/, `from ${JSON.stringify(requestScopeStubUrl)};`],
  [/from "shared-worker-id";/, `from ${JSON.stringify(workerIdUrl)};`],
  [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
]);

const mod = await import(moduleDataUrl(src));
const {
  recordLoadedWorker,
  loadedWorkerCount,
  loadedWorkerIds,
  siblingsFor,
  abortLoadedWorker,
  evictSiblings,
  expectedRuntimeServiceForNamespace,
  runtimeServiceAllowsNamespace,
  _resetLoadedWorkersForTest,
} = mod;

/** @param {{ behavior: string }} opts */
function makeAbortStub({ behavior }) {
  // behavior:
  //   "internal-error" → throw on abort, like workerd's real abortIsolate path
  //   "no-op"          → return cleanly (shim missing or older worker)
  //   "other-error"    → throw a non-internal-error
  //   "factory-thrown" → simulate get() returning a stub whose factory threw
  return {
    LOADER: {
      get(/** @type {string} */ _id, /** @type {() => unknown} */ factory) {
        if (behavior === "factory-thrown") {
          // simulate factory invocation - real workerd would only call on
          // cache miss, here we mirror that path.
          return {
            getEntrypoint() {
              return {
                async abort() {
                  factory();
                  throw new Error("unreachable");
                },
              };
            },
          };
        }
        return {
          getEntrypoint() {
            return {
              async abort() {
                if (behavior === "internal-error") throw new Error("internal error; reference = abc");
                if (behavior === "internal-error-other") throw new Error("internal error; abort signal aborted");
                if (behavior === "internal-error-suffix") throw new Error("preamble: internal error; reference = abc");
                if (behavior === "other-error") throw new Error("kaboom");
                if (behavior === "false-error") return Promise.reject(false);
                // no-op: returns cleanly
              },
            };
          },
        };
      },
    },
  };
}

function captureLogs() {
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const records = [];
  const log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    records.push({ level, event, fields });
  return { log, records };
}

test("recordLoadedWorker tracks ids; siblingsFor narrows to same ns/name", () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  recordLoadedWorker("alpha:web:v2");
  recordLoadedWorker("alpha:other:v1");
  recordLoadedWorker("beta:web:v1");

  assert.equal(loadedWorkerCount(), 4);
  assert.deepEqual(siblingsFor("alpha:web:v3").toSorted(), ["alpha:web:v1", "alpha:web:v2"]);
  // self is excluded
  assert.deepEqual(siblingsFor("alpha:web:v1"), ["alpha:web:v2"]);
  // no siblings → empty
  assert.deepEqual(siblingsFor("alpha:other:v1"), []);
  // malformed id → empty
  assert.deepEqual(siblingsFor("not-a-worker-id"), []);
});

test("runtime pool rule maps only __system__ dispatch to system-runtime", () => {
  assert.equal(expectedRuntimeServiceForNamespace("__system__"), "system-runtime");
  assert.equal(expectedRuntimeServiceForNamespace("demo"), "user-runtime");
  assert.equal(expectedRuntimeServiceForNamespace("__platform__"), "user-runtime");

  assert.equal(runtimeServiceAllowsNamespace("system-runtime", "__system__"), true);
  assert.equal(runtimeServiceAllowsNamespace("system-runtime", "demo"), false);
  assert.equal(runtimeServiceAllowsNamespace("user-runtime", "demo"), true);
  assert.equal(runtimeServiceAllowsNamespace("user-runtime", "__system__"), false);
  assert.equal(runtimeServiceAllowsNamespace("runtime", "demo"), false);
});

test("siblingsFor ignores entries with malformed ids", () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  recordLoadedWorker(""); // malformed
  recordLoadedWorker("alpha:web"); // malformed (only 2 segments)
  assert.deepEqual(siblingsFor("alpha:web:v2"), ["alpha:web:v1"]);
});

test("abortLoadedWorker treats `internal error; reference =` as success and removes the entry", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  const env = makeAbortStub({ behavior: "internal-error" });
  const result = await abortLoadedWorker({ env, workerId: "alpha:web:v1" });
  assert.deepEqual(result, { aborted: true });
  assert.equal(loadedWorkerCount(), 0);
});

test("abortLoadedWorker does NOT treat other `internal error;` phrasings as success", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  const env = makeAbortStub({ behavior: "internal-error-other" });
  const result = await abortLoadedWorker({ env, workerId: "alpha:web:v1" });
  // Only the documented `internal error; reference =` shape signals abort
  // success. Anything else stays in the unexpected branch — the entry
  // remains in the registry for caller-side decisioning.
  assert.equal(result.aborted, false);
  assert.equal(result.reason, "unexpected");
  assert.equal(loadedWorkerCount(), 1);
});

test("abortLoadedWorker requires the success prefix at index 0 (startsWith, not includes)", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  // A nested workerd error wrapper that includes the abort-success phrase
  // somewhere mid-string (e.g. inside a JSRPC chain annotation) must NOT
  // be treated as success. workerd's own upstream test asserts
  // `startsWith('internal error; reference = ')`; we mirror that anchor.
  const env = makeAbortStub({ behavior: "internal-error-suffix" });
  const result = await abortLoadedWorker({ env, workerId: "alpha:web:v1" });
  assert.equal(result.aborted, false);
  assert.equal(result.reason, "unexpected");
  assert.equal(loadedWorkerCount(), 1);
});

test("abortLoadedWorker reports the no-op branch and keeps the entry recorded", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  const env = makeAbortStub({ behavior: "no-op" });
  const result = await abortLoadedWorker({ env, workerId: "alpha:web:v1" });
  assert.equal(result.aborted, false);
  assert.equal(result.reason, "no_internal_error");
  // The entry should remain because the abort visibly did not take effect.
  assert.equal(loadedWorkerCount(), 1);
});

test("abortLoadedWorker surfaces unexpected errors as { aborted: false, reason: 'unexpected' }", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  const env = makeAbortStub({ behavior: "other-error" });
  const result = await abortLoadedWorker({ env, workerId: "alpha:web:v1" });
  assert.equal(result.aborted, false);
  assert.equal(result.reason, "unexpected");
  assert.match(String(result.error?.message || ""), /kaboom/);
  // Entry stays — caller decides what to do with the unexpected error.
  assert.equal(loadedWorkerCount(), 1);
});

test("evictSiblings preserves falsey unexpected errors in diagnostics", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  recordLoadedWorker("alpha:web:v2");
  const { log, records } = captureLogs();

  await evictSiblings({
    env: makeAbortStub({ behavior: "false-error" }),
    workerId: "alpha:web:v2",
    log,
  });

  assert.deepEqual(records.at(-1), {
    level: "warn",
    event: "evict_skipped",
    fields: {
      worker_id: "alpha:web:v1",
      triggered_by: "alpha:web:v2",
      reason: "unexpected",
      error_message: "false",
    },
  });
});

test("evictSiblings aborts every same-ns/same-name sibling and keeps current loaded", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  recordLoadedWorker("alpha:web:v2");
  recordLoadedWorker("alpha:web:v3");
  recordLoadedWorker("alpha:other:v1"); // unrelated
  recordLoadedWorker("beta:web:v1"); // unrelated ns
  const env = makeAbortStub({ behavior: "internal-error" });
  const { log, records } = captureLogs();

  const result = await evictSiblings({ env, workerId: "alpha:web:v3", log });

  assert.equal(result.considered, 2);
  assert.equal(result.aborted, 2);
  // All non-target loaded workers in same ns/name removed; unrelated stay.
  const remaining = loadedWorkerIds().toSorted();
  assert.deepEqual(remaining, ["alpha:other:v1", "alpha:web:v3", "beta:web:v1"]);
  // Each abort should produce one info-level log line.
  const infos = records.filter((r) => r.event === "evict_aborted");
  assert.equal(infos.length, 2);
  assert.equal(new Set(infos.map((r) => r.fields.worker_id)).size, 2);
  for (const r of infos) {
    assert.equal(r.fields.triggered_by, "alpha:web:v3");
    assert.equal(r.level, "info");
  }
});

test("evictSiblings with no siblings is a no-op and produces no abort logs", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v3");
  const env = makeAbortStub({ behavior: "internal-error" });
  const { log, records } = captureLogs();
  const result = await evictSiblings({ env, workerId: "alpha:web:v3", log });
  assert.deepEqual(result, { considered: 0, aborted: 0 });
  assert.equal(records.length, 0);
});

test("evictSiblings logs evict_skipped when abort returns no_internal_error", async () => {
  _resetLoadedWorkersForTest();
  recordLoadedWorker("alpha:web:v1");
  recordLoadedWorker("alpha:web:v2");
  const env = makeAbortStub({ behavior: "no-op" });
  const { log, records } = captureLogs();
  const result = await evictSiblings({ env, workerId: "alpha:web:v2", log });
  assert.equal(result.considered, 1);
  assert.equal(result.aborted, 0);
  assert.equal(records.length, 1);
  assert.equal(records[0].event, "evict_skipped");
  assert.equal(records[0].fields.reason, "no_internal_error");
  // Entry stays because abort did not take effect.
  assert.equal(loadedWorkerIds().toSorted().join(","), "alpha:web:v1,alpha:web:v2");
});
