import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  repositoryFileUrl,
  runtimeLibModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { installMockFetch, makeRecordingFetch } from "../helpers/mock-fetch.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const RUNTIME_LIB_URL = runtimeLibModuleDataUrl();
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");

/** @param {Array<[RegExp | string, string]>} [replacements] */
async function loadKvBinding(replacements = []) {
  const baseReplacements = [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/from "runtime-lib";/, `from ${JSON.stringify(RUNTIME_LIB_URL)};`],
    [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
    [/from "shared-base64";/, `from ${JSON.stringify(SHARED_BASE64_URL)};`],
    [
      /from "runtime-bindings-proxy";/,
      `from ${JSON.stringify(PROXY_BINDING_URL)};`,
    ],
    [/from "shared-bounded-body";/, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)};`],
    [/from "shared-respond";/, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
  ];
  return importRepositoryModule("runtime/bindings/kv.js", /** @type {Array<[RegExp | string, string]>} */ ([...baseReplacements, ...replacements]));
}

/** @param {(setFetch: (stub: any) => void) => Promise<unknown>} fn */
function withFetchStub(fn) {
  return async () => {
    let restoreFetch = () => {};
    try {
      await fn((/** @type {any} */ stub) => {
        restoreFetch();
        restoreFetch = installMockFetch(stub);
      });
    } finally {
      restoreFetch();
    }
  };
}

/** @param {any} KV */
function makeKv(KV) {
  return new KV(
    { props: { ns: "tenant-a", id: "cache" } },
    { REDIS_PROXY_URL: "http://redis-proxy", SERVICE_NAME: "unit", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" }
  );
}

test("KV list clamps user limit before forwarding to Redis proxy", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  /** @type {URL[]} */
  const observedUrls = [];
  setFetch(async (/** @type {any} */ url) => {
    observedUrls.push(new URL(url));
    return new Response(JSON.stringify({ keys: [], list_complete: true }), {
      headers: { "content-type": "application/json" },
    });
  });

  await makeKv(KV).list({ prefix: "p", limit: 1_000_000 });

  assert.equal(observedUrls.length, 1);
  const observedUrl = observedUrls[0];
  assert.equal(observedUrl.searchParams.get("limit"), "1000");
  assert.equal(observedUrl.searchParams.get("prefix"), "p");
}));

test("KV list forwards metadata option", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  /** @type {URL[]} */
  const observedUrls = [];
  setFetch(async (/** @type {any} */ url) => {
    observedUrls.push(new URL(url));
    return new Response(JSON.stringify({ keys: [], list_complete: true }), {
      headers: { "content-type": "application/json" },
    });
  });

  await makeKv(KV).list({ prefix: "p", metadata: true });

  assert.equal(observedUrls[0].searchParams.get("metadata"), "true");
}));

test("KV batch get calls the batch proxy endpoint and returns a Map", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  /** @type {any[]} */
  const calls = [];
  const response = new Response(JSON.stringify({
    entries: [
      { key: "a", value_b64: btoa("alpha"), metadata: null },
      { key: "missing", value_b64: null, metadata: null },
    ],
  }), { headers: { "content-type": "application/json" } });
  setFetch(makeRecordingFetch(calls, {
    capture: (_call, url, init) => ({ url: new URL(/** @type {string | URL} */ (url)), init }),
    response,
  }));

  const out = await makeKv(KV).get(["a", "missing"]);

  assert.equal(calls[0].url.pathname, "/kv/get-batch");
  assert.equal(calls[0].init.method, "POST");
  assert.deepEqual(parseJsonObjectRequestBody(calls[0].init, "KV batch get request body"), { keys: ["a", "missing"] });
  assert.deepEqual([...out.entries()], [["a", "alpha"], ["missing", null]]);
}));

test("KV batch getWithMetadata requests metadata and returns a Map", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  /** @type {any[]} */
  const calls = [];
  const response = new Response(JSON.stringify({
    entries: [
      { key: "a", value_b64: btoa("{\"n\":1}"), metadata: { tag: "a" } },
    ],
  }), { headers: { "content-type": "application/json" } });
  setFetch(makeRecordingFetch(calls, {
    capture: (_call, url, init) => ({ url: new URL(/** @type {string | URL} */ (url)), init }),
    response,
  }));

  const out = await makeKv(KV).getWithMetadata(["a"], "json");

  assert.deepEqual(parseJsonObjectRequestBody(calls[0].init, "KV batch get request body"), { keys: ["a"], metadata: true });
  assert.deepEqual([...out.entries()], [["a", { value: { n: 1 }, metadata: { tag: "a" } }]]);
}));

test("KV batch get rejects unsupported stream-like types before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("proxy should not be called");
  });

  await assert.rejects(() => makeKv(KV).get(["a"], "arrayBuffer"), /unsupported batch type "arrayBuffer"/);
  await assert.rejects(() => makeKv(KV).getWithMetadata(["a"], { type: "stream" }), /unsupported batch type "stream"/);
}));

test("KV list rejects invalid limits before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for invalid list limit");
  });

  await assert.rejects(
    () => makeKv(KV).list({ limit: 0 }),
    /KV list: limit must be an integer/
  );
  await assert.rejects(
    () => makeKv(KV).list({ limit: 1.5 }),
    /KV list: limit must be an integer/
  );
}));

test("KV put rejects oversized typed-array values before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, KV_VALUE_MAX_BYTES } = await loadKvBinding([
    [/export const KV_VALUE_MAX_BYTES = 25 \* 1024 \* 1024;/, "export const KV_VALUE_MAX_BYTES = 4;"],
  ]);
  setFetch(async () => {
    throw new Error("fetch should not be called for oversized values");
  });

  assert.equal(KV_VALUE_MAX_BYTES, 4);
  await assert.rejects(
    () => makeKv(KV).put("too-big", new Uint8Array(5)),
    /KV put: value exceeds 4 byte limit/
  );
}));

test("KV put rejects explicit zero expirationTtl before expiration fallback", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for invalid expirationTtl");
  });

  await assert.rejects(
    () => makeKv(KV).put("zero-ttl", "value", { expirationTtl: 0, expiration: 123 }),
    /KV put: expirationTtl must be a positive integer/
  );
}));

test("KV put rejects non-serializable metadata before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for invalid metadata");
  });

  await assert.rejects(
    () => makeKv(KV).put("bad-metadata", "value", { metadata: () => {} }),
    /KV put: metadata must be JSON-serializable/
  );
}));

test("KV put rejects explicit zero expiration before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for invalid expiration");
  });

  await assert.rejects(
    () => makeKv(KV).put("zero-expiration", "value", { expiration: 0 }),
    /KV put: expiration must be a positive integer/
  );
}));

test("KV put rejects non-number expiration options before proxy work", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for invalid expiration options");
  });

  await assert.rejects(
    () => makeKv(KV).put("string-ttl", "value", { expirationTtl: "60" }),
    /KV put: expirationTtl must be a positive integer/
  );
  await assert.rejects(
    () => makeKv(KV).put("boolean-expiration", "value", { expiration: true }),
    /KV put: expiration must be a positive integer/
  );
}));

test("KV put rejects expiration values above the safe integer boundary", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => {
    throw new Error("fetch should not be called for unsafe expiration options");
  });

  await assert.rejects(
    () => makeKv(KV).put("unsafe-ttl", "value", {
      expirationTtl: Number.MAX_SAFE_INTEGER + 1,
    }),
    /KV put: expirationTtl must be a positive integer/
  );
  await assert.rejects(
    () => makeKv(KV).put("unsafe-expiration", "value", {
      expiration: Number.MAX_SAFE_INTEGER + 1,
    }),
    /KV put: expiration must be a positive integer/
  );
}));

test("KV put cancels oversized streams while reading", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding([
    [/export const KV_VALUE_MAX_BYTES = 25 \* 1024 \* 1024;/, "export const KV_VALUE_MAX_BYTES = 4;"],
  ]);
  setFetch(async () => {
    throw new Error("fetch should not be called for oversized streams");
  });
  let cancelled = false;
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]));
      controller.enqueue(new Uint8Array([4, 5]));
    },
    cancel() {
      cancelled = true;
    },
  });

  await assert.rejects(
    () => makeKv(KV).put("stream", stream),
    /KV put: value exceeds 4 byte limit/
  );
  assert.equal(cancelled, true);
}));
