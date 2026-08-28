import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  freshRepositoryModuleDataUrl,
  repositoryFileUrl,
  runtimeLibModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { installMockFetch, makeRecordingFetch } from "../helpers/mock-fetch.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";
import { delay } from "../helpers/timing.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const RUNTIME_LIB_URL = runtimeLibModuleDataUrl();
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");

/**
 * @param {Array<[RegExp | string, string]>} [replacements]
 * @param {Array<[RegExp | string, string]>} [capacityReplacements]
 */
async function loadKvBinding(replacements = [], capacityReplacements = []) {
  const infrastructureErrorUrl = freshRepositoryModuleDataUrl("runtime/infrastructure-error.js");
  const capacityUrl = freshRepositoryModuleDataUrl("runtime/bindings/kv-capacity.js", [
    [/from "runtime-infrastructure-error";/, `from ${JSON.stringify(infrastructureErrorUrl)};`],
    [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
    [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
    ...capacityReplacements,
  ]);
  const baseReplacements = [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/from "runtime-lib";/, `from ${JSON.stringify(RUNTIME_LIB_URL)};`],
    [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
    [/from "runtime-infrastructure-error";/, `from ${JSON.stringify(infrastructureErrorUrl)};`],
    [/from "runtime-bindings-kv-capacity";/, `from ${JSON.stringify(capacityUrl)};`],
    [/from "shared-base64";/, `from ${JSON.stringify(SHARED_BASE64_URL)};`],
    [
      /from "runtime-bindings-proxy";/,
      `from ${JSON.stringify(PROXY_BINDING_URL)};`,
    ],
    [/from "shared-bounded-body";/, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)};`],
    [/from "shared-respond";/, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
  ];
  const [kv, kvCapacity, runtimeInfrastructure] = await Promise.all([
    importRepositoryModule("runtime/bindings/kv.js", /** @type {Array<[RegExp | string, string]>} */ ([...baseReplacements, ...replacements])),
    import(capacityUrl),
    import(infrastructureErrorUrl),
  ]);
  return { ...kv, kvCapacity, runtimeInfrastructure };
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
    {
      props: { ns: "tenant-a", id: "cache" },
      /** @param {Promise<unknown>} promise */
      waitUntil(promise) { void promise.catch(() => {}); },
    },
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

test("KV read capacity rejects concurrent large materialization and releases leases", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding();
  const largeLength = 20 * 1024 * 1024;
  const largeBytes = new Uint8Array(largeLength);
  largeBytes[0] = 120;
  const releaseFirst = Promise.withResolvers();
  let secondCancelled = false;
  let calls = 0;
  setFetch(async () => {
    calls += 1;
    if (calls === 1) {
      const body = new ReadableStream({
        start(controller) {
          void releaseFirst.promise.then(() => {
            const midpoint = largeLength / 2;
            controller.enqueue(largeBytes.subarray(0, midpoint));
            controller.enqueue(largeBytes.subarray(midpoint));
            controller.close();
          });
        },
      });
      return new Response(body, { headers: { "content-length": String(largeLength) } });
    }
    if (calls === 2) {
      const body = new ReadableStream({
        cancel() { secondCancelled = true; },
      });
      return new Response(body, { headers: { "content-length": String(largeLength) } });
    }
    return new Response("x", { headers: { "content-length": "1" } });
  });

  const kv = makeKv(KV);
  const first = kv.get("first", "arrayBuffer");
  for (let attempt = 0; attempt < 20 && kvCapacity.kvReadCapacityStateForTest().inUseBytes === 0; attempt += 1) {
    await delay(0);
  }
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, largeLength);

  await assert.rejects(
    () => kv.get("second"),
    (error) => error instanceof Error &&
      error.message === kvCapacity.KV_READ_CAPACITY_ERROR_MESSAGE &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
  assert.equal(secondCancelled, true);
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, largeLength);

  releaseFirst.resolve(undefined);
  const firstValue = await first;
  assert.ok(firstValue instanceof ArrayBuffer);
  assert.equal(firstValue.byteLength, largeLength);
  assert.equal(new Uint8Array(firstValue)[0], 120);
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
  assert.equal(await kv.get("third"), "x");
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
}));

test("KV rejects oversized host envelopes with attribution and lease cleanup", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, KV_READ_RESPONSE_MAX_BYTES, kvCapacity, runtimeInfrastructure } =
    await loadKvBinding();
  const invocation = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  let cancelled = false;
  setFetch(async () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  }), {
    headers: { "content-length": String(KV_READ_RESPONSE_MAX_BYTES + 1) },
  }));

  try {
    await assert.rejects(
      () => makeKv(KV).get("oversized", undefined, invocation.id),
      (error) => error instanceof Error &&
        error.message === "KV read response is too large" &&
        runtimeInfrastructure.isRuntimeInfrastructureError(error)
    );
    assert.equal(
      invocation.diagnostic(),
      "Runtime KV response exceeded its wire byte limit"
    );
    assert.equal(cancelled, true);
    assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
  } finally {
    invocation.close();
  }
}));

test("KV read capacity releases reservation when body consumption fails", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding();
  setFetch(async () => new Response(new ReadableStream({
    start(controller) { controller.error(new Error("body failed")); },
  }), { headers: { "content-length": String(20 * 1024 * 1024) } }));

  await assert.rejects(
    () => makeKv(KV).get("broken"),
    (error) => error instanceof Error &&
      error.message === "KV read response failed" &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
}));

test("raw KV host failures do not affect active Workflow invocations", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const active = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(new ReadableStream({
    start(controller) { controller.error(new Error("body failed")); },
  }), { headers: { "content-length": "1" } }));

  try {
    await assert.rejects(() => makeKv(KV).get("broken"), /KV read response failed/);
    assert.equal(active.diagnostic(), undefined);
  } finally {
    active.close();
  }
}));

test("stale cached KV attribution cannot affect a later Workflow invocation", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const stale = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  const staleId = stale.id;
  stale.close();
  const current = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(new ReadableStream({
    start(controller) { controller.error(new Error("body failed")); },
  }), { headers: { "content-length": "1" } }));

  try {
    await assert.rejects(
      () => makeKv(KV).get("broken", undefined, staleId),
      /KV read response failed/
    );
    assert.equal(current.diagnostic(), undefined);
  } finally {
    current.close();
  }
}));

test("KV malformed batch envelopes are attributed to the exact Workflow invocation", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const cases = [
    { keys: ["a"], response: () => new Response(JSON.stringify({ entries: {} })) },
    { keys: ["a"], response: () => new Response(JSON.stringify({ entries: [] })) },
    {
      keys: ["a"],
      response: () => new Response(JSON.stringify({
        entries: [{ key: "other", value_b64: "", metadata: null }],
      })),
    },
    {
      keys: ["a"],
      response: () => new Response(JSON.stringify({
        entries: [{ key: "a", value_b64: "Zg", metadata: null }],
      })),
    },
    {
      keys: ["a"],
      response: () => new Response(JSON.stringify({
        entries: [{ key: "a", value_b64: null, metadata: { orphan: true } }],
      })),
    },
    { keys: ["a"], response: () => new Response(new Uint8Array([0xff])) },
    {
      keys: ["a", "b"],
      type: "json",
      response: () => new Response(JSON.stringify({
        entries: [
          { key: "a", value_b64: btoa("not-json"), metadata: null },
          { key: "b", value_b64: "Zg", metadata: null },
        ],
      })),
    },
  ];

  for (const testCase of cases) {
    const invocation = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
    setFetch(async () => testCase.response());
    try {
      await assert.rejects(
        () => makeKv(KV).get(testCase.keys, testCase.type, invocation.id),
        (error) => error instanceof Error &&
          error.message === "KV read response is invalid" &&
          runtimeInfrastructure.isRuntimeInfrastructureError(error)
      );
      assert.equal(
        invocation.diagnostic(),
        "Runtime KV batch response envelope is invalid"
      );
    } finally {
      invocation.close();
    }
  }
}));

test("KV metadata envelope preserves empty values and attributes malformed base64", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const valid = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(JSON.stringify({ value_b64: "", metadata: null })));
  try {
    const result = await makeKv(KV).getWithMetadata("empty", "arrayBuffer", valid.id);
    assert.ok(result.value instanceof ArrayBuffer);
    assert.equal(result.value.byteLength, 0);
    assert.equal(result.metadata, null);
    assert.equal(valid.diagnostic(), undefined);
  } finally {
    valid.close();
  }

  const invalid = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(JSON.stringify({ value_b64: "Zg", metadata: null })));
  try {
    await assert.rejects(
      () => makeKv(KV).getWithMetadata("broken", undefined, invalid.id),
      /KV read response is invalid/
    );
    assert.equal(
      invalid.diagnostic(),
      "Runtime KV metadata response envelope is invalid"
    );
  } finally {
    invalid.close();
  }

  const orphanMetadata = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(JSON.stringify({
    value_b64: null,
    metadata: { orphan: true },
  })));
  try {
    await assert.rejects(
      () => makeKv(KV).getWithMetadata("missing", undefined, orphanMetadata.id),
      /KV read response is invalid/
    );
    assert.equal(
      orphanMetadata.diagnostic(),
      "Runtime KV metadata response envelope is invalid"
    );
  } finally {
    orphanMetadata.close();
  }
}));

test("KV malformed list envelopes are attributed to the exact Workflow invocation", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const invocation = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(JSON.stringify({ keys: {}, list_complete: true })));
  try {
    await assert.rejects(
      () => makeKv(KV).list({}, invocation.id),
      /KV read response is invalid/
    );
    assert.equal(
      invocation.diagnostic(),
      "Runtime KV list response envelope is invalid"
    );
  } finally {
    invocation.close();
  }
}));

test("KV tenant JSON decoding errors do not mark the Workflow invocation", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const invocation = runtimeInfrastructure.beginRuntimeInfrastructureInvocation();
  setFetch(async () => new Response(JSON.stringify({
    entries: [{ key: "a", value_b64: btoa("not-json"), metadata: null }],
  })));
  try {
    await assert.rejects(
      () => makeKv(KV).get(["a"], "json", invocation.id),
      SyntaxError
    );
    assert.equal(invocation.diagnostic(), undefined);
  } finally {
    invocation.close();
  }
}));

test("KV read capacity rejects a missing declared response body", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding();
  setFetch(async () => new Response(null, { headers: { "content-length": "1" } }));

  await assert.rejects(
    () => makeKv(KV).get("broken"),
    (error) => error instanceof Error &&
      error.message === "KV read response failed" &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
}));

test("KV read capacity deadline releases a stalled body lease", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding([], [
    [/export const KV_READ_LEASE_MAX_MS = 5_000;/, "export const KV_READ_LEASE_MAX_MS = 20;"],
  ]);
  setFetch(async () => new Response(new ReadableStream({}), {
    headers: { "content-length": String(20 * 1024 * 1024) },
  }));

  await assert.rejects(
    () => makeKv(KV).get("stalled"),
    (error) => error instanceof Error &&
      error.message === kvCapacity.KV_READ_TIMEOUT_ERROR_MESSAGE &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
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

test("KV proxy accepts 404 only for scalar get", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => new Response(null, { status: 404 }));
  const kv = makeKv(KV);

  assert.equal(await kv.get("missing"), null);
  await assert.rejects(() => kv.get(["missing"]), /KV proxy \/kv\/get-batch failed with 404/);
  await assert.rejects(
    () => kv.getWithMetadata("missing"),
    /KV proxy \/kv\/get-with-metadata failed with 404/
  );
  await assert.rejects(() => kv.list(), /KV proxy \/kv\/list failed with 404/);
  await assert.rejects(() => kv.put("key", "value"), /KV proxy \/kv\/put failed with 404/);
  await assert.rejects(() => kv.delete("key"), /KV proxy \/kv\/delete failed with 404/);
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
