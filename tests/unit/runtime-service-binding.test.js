import assert from "node:assert/strict";
import { test } from "node:test";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { readJsonResponse } from "../helpers/response-json.js";

// Keep synchronized with INTERNAL_AUTH_HEADER from "shared-internal-auth".
const TEST_INTERNAL_AUTH_HEADER = "x-wdl-internal-auth";

const { ServiceBinding } = await importRepositoryModule("runtime/bindings/service.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [
    /import \{ INTERNAL_AUTH_HEADER \} from "shared-internal-auth";/,
    `const INTERNAL_AUTH_HEADER = ${JSON.stringify(TEST_INTERNAL_AUTH_HEADER)};`
  ],
  [
    /import \{ sanitizeRequestId \} from "shared-observability";/,
    `import { sanitizeRequestId } from ${JSON.stringify(repositoryFileUrl("shared/observability.js"))};`
  ],
  [
    /import \{ getLoadedWorkerStub \} from "runtime-load";/,
    `const getLoadedWorkerStub = (options) => {
       /** @type {any} */ (globalThis).__serviceBindingLoaderCallbackOptions.push(options);
       return {
         workerId: options.workerId,
         stub: options.env.LOADER.get(options.workerId, async () => ({})),
       };
     };`
  ],
  [
    /import \{ fetchTailFields, startTailEnvelope \} from "runtime-tail-forwarder";/,
    `const fetchTailFields = () => ({});
     const startTailEnvelope = () => ({ finish() {}, finishError() {} });`
  ],
]);

function resetLoaderCallbackOptions() {
  /** @type {any} */ (globalThis).__serviceBindingLoaderCallbackOptions = [];
}

function loaderCallbackOptions() {
  return /** @type {any[]} */ (/** @type {any} */ (globalThis).__serviceBindingLoaderCallbackOptions);
}

function makeEntrypointMethods(/** @type {string} */ name, /** @type {any} */ opts, /** @type {string} */ targetId) {
  return {
    fetch(/** @type {Request} */ request) {
      return Response.json({
        name,
        opts,
        targetId,
        requestId: request.headers.get("x-request-id"),
        workerId: request.headers.get("x-worker-id"),
        workerPrefix: request.headers.get("x-worker-prefix"),
        upstreamBinding: request.headers.get("x-wdl-upstream-binding"),
        internalAuth: request.headers.get(TEST_INTERNAL_AUTH_HEADER),
      });
    },
    ping(/** @type {unknown[]} */ ...args) {
      return { args, name, opts, targetId };
    },
  };
}

function makeEntrypointGetter(/** @type {string} */ targetId) {
  return {
    getEntrypoint(/** @type {string} */ name, /** @type {any} */ opts) {
      return makeEntrypointMethods(name, opts, targetId);
    },
  };
}

function makeMockLoader() {
  return {
    get(/** @type {string} */ targetId) {
      return makeEntrypointGetter(targetId);
    },
  };
}

function makeServiceBinding() {
  return new ServiceBinding({
    props: {
      callerNs: "caller-ns",
      targetNs: "target-ns",
      targetWorker: "api",
      targetVersion: "v1",
      targetEntrypoint: "Api",
    },
  }, {
    LOADER: makeMockLoader(),
  });
}

test("ServiceBinding RPC cold-load does not mint a service-binding request id", async () => {
  resetLoaderCallbackOptions();
  const binding = makeServiceBinding();

  const result = await binding.ping("hello");

  assert.deepEqual(result.args, ["hello"]);
  assert.equal(result.name, "Api");
  assert.deepEqual(result.opts, { props: { callerNs: "caller-ns" } });
  assert.equal(result.targetId, "target-ns:api:v1");
  const options = loaderCallbackOptions();
  assert.equal(options.length, 1);
  assert.equal(options[0].requestId, null);
  assert.equal(options[0].evictOnLoad, undefined);
});

test("ServiceBinding fetch strips caller-supplied platform forwarding headers", async () => {
  resetLoaderCallbackOptions();
  const binding = makeServiceBinding();

  const response = await binding.fetch(new Request("https://worker.workers.example/rpc", {
    headers: {
      "x-request-id": "rid-service",
      "x-worker-id": "caller:forged:v1",
      "x-worker-prefix": "/forged",
      "x-wdl-upstream-binding": "RUNTIME_SYSTEM",
      [TEST_INTERNAL_AUTH_HEADER]: "forged-token",
    },
  }));
  const result = await readJsonResponse(response, 200);

  assert.deepEqual(result.opts, { props: { callerNs: "caller-ns" } });
  assert.equal(result.requestId, "rid-service");
  assert.equal(result.workerId, "target-ns:api:v1");
  assert.equal(result.workerPrefix, null);
  assert.equal(result.upstreamBinding, null);
  assert.equal(result.internalAuth, null);
});

test("ServiceBinding fetch cold-load propagates request id when present", async () => {
  resetLoaderCallbackOptions();
  const binding = makeServiceBinding();
  const requestId = "req-123";

  const response = await binding.fetch(new Request("https://worker.workers.example/rpc", {
    headers: { "x-request-id": requestId },
  }));
  const result = await readJsonResponse(response, 200);

  assert.deepEqual(result.opts, { props: { callerNs: "caller-ns" } });
  assert.equal(result.requestId, requestId);
  const options = loaderCallbackOptions();
  assert.equal(options.length, 1);
  assert.equal(options[0].requestId, requestId);
});

test("ServiceBinding fetch canonicalizes request ids without minting replacements", async () => {
  resetLoaderCallbackOptions();
  const binding = makeServiceBinding();

  const canonical = await readJsonResponse(await binding.fetch(new Request("https://worker.workers.example/rpc", {
    headers: { "x-request-id": " rid-first , rid-second" },
  })), 200);
  assert.equal(canonical.requestId, "rid-first");

  const invalid = await readJsonResponse(await binding.fetch(new Request("https://worker.workers.example/rpc", {
    headers: { "x-request-id": "bad\\id" },
  })), 200);
  assert.equal(invalid.requestId, null);
  assert.equal(loaderCallbackOptions().at(-1).requestId, null);
});
