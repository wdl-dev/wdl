import { test } from "node:test";
import assert from "node:assert/strict";
import {
  importRepositoryModule,
  freshRepositoryModuleDataUrl,
  moduleDataUrl,
  readRepositoryJson,
  repositoryFileUrl,
  runtimeLibModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { installMockFetch, makeRecordingFetch } from "../helpers/mock-fetch.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";
import { delay } from "../helpers/timing.js";
import { withMockedProperty } from "../helpers/mock-global.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const RUNTIME_LIB_URL = runtimeLibModuleDataUrl();
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const kvHostResponse = /** @type {any} */ (
  readRepositoryJson("tests/fixtures/kv-host-response.json")
);

/**
 * @param {Array<[RegExp | string, string]>} [replacements]
 * @param {Array<[RegExp | string, string]>} [capacityReplacements]
 * @param {string} [capacityMetricsUrl]
 */
async function loadKvBinding(
  replacements = [],
  capacityReplacements = [],
  capacityMetricsUrl = RUNTIME_METRICS_NOOP_URL
) {
  const infrastructureErrorUrl = freshRepositoryModuleDataUrl("runtime/infrastructure-error.js");
  const capacityUrl = freshRepositoryModuleDataUrl("runtime/bindings/kv-capacity.js", [
    [/from "runtime-infrastructure-error";/, `from ${JSON.stringify(infrastructureErrorUrl)};`],
    [/from "runtime-metrics";/, `from ${JSON.stringify(capacityMetricsUrl)};`],
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

/** @param {any} KV @param {Record<string, unknown>} [env] */
function makeKv(KV, env = {
  REDIS_PROXY_URL: "http://redis-proxy",
  SERVICE_NAME: "unit",
  WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
}) {
  return new KV(
    {
      props: {
        ns: "tenant-a",
        id: "cache",
      },
      /** @param {Promise<unknown>} promise */
      waitUntil(promise) { void promise.catch(() => {}); },
    },
    env
  );
}

/** @param {Record<string, unknown>} runtimeInfrastructure */
function brandedInfrastructureError(runtimeInfrastructure) {
  return (/** @type {unknown} */ error) => (
    error instanceof Error &&
    Object.hasOwn(error, "code") &&
    Object.prototype.propertyIsEnumerable.call(error, "code") &&
    /** @type {any} */ (error).code ===
      runtimeInfrastructure.KV_READ_INFRASTRUCTURE_ERROR_CODE
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

test("KV host response readers match the cross-language fixture", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, KV_READ_RESPONSE_MAX_BYTES } = await loadKvBinding();
  assert.equal(KV_READ_RESPONSE_MAX_BYTES, kvHostResponse.maxResponseBytes);
  const responses = [
    kvHostResponse.batch.response,
    kvHostResponse.metadata.found,
    kvHostResponse.metadata.missing,
    kvHostResponse.list.complete,
    kvHostResponse.list.incomplete,
  ];
  setFetch(async () => Response.json(responses.shift()));
  const kv = makeKv(KV);

  const batch = await kv.get(kvHostResponse.batch.requestedKeys);
  assert.deepEqual([...batch.entries()], [
    ["binary", new TextDecoder().decode(Uint8Array.from([0, 0xff, 0x10]))],
    ["empty", ""],
    ["missing", null],
  ]);
  assert.deepEqual(await kv.getWithMetadata("binary", "arrayBuffer"), {
    value: Uint8Array.from([0xff, 0]).buffer,
    metadata: { kind: "binary" },
  });
  assert.deepEqual(await kv.getWithMetadata("missing"), { value: null, metadata: null });
  assert.deepEqual(await kv.list({ metadata: true }), kvHostResponse.list.complete);
  assert.deepEqual(await kv.list(), kvHostResponse.list.incomplete);
}));

test("KV capacity metrics preserve outcome and process-lifetime gauge contracts", async () => {
  const recordingMetricsUrl = moduleDataUrl(`
export const calls = [];
export const metrics = {
  increment(name, labels, value = 1) {
    calls.push({ kind: "increment", name, labels: { ...labels }, value });
  },
  setGauge(name, labels, value) {
    calls.push({ kind: "gauge", name, labels: { ...labels }, value });
  },
};
`);
  const { kvCapacity } = await loadKvBinding([], [
    [/export const KV_READ_LEASE_MAX_MS = 5_000;/, "export const KV_READ_LEASE_MAX_MS = 20;"],
  ], recordingMetricsUrl);
  const recording = /** @type {{ calls: Array<{
   *   kind: "increment" | "gauge",
   *   name: string,
   *   labels: Record<string, string>,
   *   value: number,
   * }> }} */ (await import(recordingMetricsUrl));
  kvCapacity.resetKvReadCapacityForTest();
  /** @type {Promise<unknown>[]} */
  const tasks = [];
  const binding = {
    env: { SERVICE_NAME: "unit" },
    ctx: {
      /** @param {Promise<unknown>} promise */
      waitUntil(promise) { tasks.push(promise); },
    },
  };
  /** @param {number} length */
  const response = (length) => new Response(null, {
    headers: { "content-length": String(length) },
  });

  const completed = kvCapacity.acquireKvReadLease(binding, response(8), () => {});
  assert.ok(completed);
  assert.equal(completed.release(), true);

  const full = kvCapacity.acquireKvReadLease(
    binding,
    response(kvCapacity.KV_READ_IN_FLIGHT_MAX_BYTES),
    () => {}
  );
  assert.ok(full);
  assert.equal(kvCapacity.acquireKvReadLease(binding, response(1), () => {}), null);
  assert.equal(full.release(), true);

  let deadlineCalls = 0;
  assert.ok(kvCapacity.acquireKvReadLease(binding, response(4), () => {
    deadlineCalls += 1;
  }));
  await delay(30);
  assert.equal(deadlineCalls, 1);

  assert.throws(
    () => kvCapacity.acquireKvReadLease({
      env: binding.env,
      ctx: { waitUntil() { throw new Error("waitUntil failed"); } },
    }, response(2), () => {}),
    /waitUntil failed/
  );

  kvCapacity.prepareKvReadCapacityMetrics(binding.env);
  kvCapacity.prepareKvReadCapacityMetrics(binding.env);
  await Promise.all(tasks);

  const increments = recording.calls.filter((call) => call.kind === "increment");
  assert.deepEqual(increments.map((call) => [call.name, call.labels, call.value]), [
    ["kv_read_capacity_events", { service: "unit", outcome: "acquired" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "completed" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "acquired" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "saturated" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "completed" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "acquired" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "deadline" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "acquired" }, 1],
    ["kv_read_capacity_events", { service: "unit", outcome: "setup_error" }, 1],
  ]);
  const gauges = recording.calls.filter((call) => call.kind === "gauge");
  assert.deepEqual(gauges.map((call) => [call.name, call.labels, call.value]), [
    ["kv_read_in_flight_bytes", { service: "unit" }, 0],
    ["kv_read_in_flight_high_water_bytes", { service: "unit" }, kvCapacity.KV_READ_IN_FLIGHT_MAX_BYTES],
    ["kv_read_in_flight_bytes", { service: "unit" }, 0],
    ["kv_read_in_flight_high_water_bytes", { service: "unit" }, kvCapacity.KV_READ_IN_FLIGHT_MAX_BYTES],
  ]);
});

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

test("KV canonical declared-length scalar reads use the native body consumer", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding();
  const originalArrayBuffer = Response.prototype.arrayBuffer;
  let nativeReads = 0;
  await withMockedProperty(
    Response.prototype,
    "arrayBuffer",
    /** @this {Response} */
    function arrayBuffer() {
      nativeReads += 1;
      return Reflect.apply(originalArrayBuffer, this, []);
    },
    async () => {
      const responses = [
        { declared: 4, chunks: [[1, 2], [3, 4]] },
        { declared: 2, chunks: [[1, 2], [3]] },
        { declared: 4, chunks: [[1, 2, 3]] },
      ];
      setFetch(async () => {
        const response = responses.shift();
        assert.ok(response);
        return new Response(new ReadableStream({
          type: "bytes",
          start(controller) {
            for (const chunk of response.chunks) controller.enqueue(new Uint8Array(chunk));
            controller.close();
          },
        }), { headers: { "content-length": String(response.declared) } });
      });

      const value = await makeKv(KV).get("exact", "arrayBuffer");
      assert.deepEqual([...new Uint8Array(value)], [1, 2, 3, 4]);
      for (const key of ["long", "short"]) {
        await assert.rejects(
          () => makeKv(KV).get(key),
          (error) => error instanceof Error &&
            error.message === "KV read response failed" &&
            runtimeInfrastructure.isRuntimeInfrastructureError(error)
        );
      }
    }
  );

  assert.equal(nativeReads, 3);
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
}));

test("KV native scalar path excludes unknown and legacy oversized lengths", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding([
    [/export const KV_VALUE_MAX_BYTES = 25 \* 1024 \* 1024;/, "export const KV_VALUE_MAX_BYTES = 2;"],
  ]);
  const responses = [
    new Response("a"),
    new Response(new Uint8Array([1, 2, 3]), { headers: { "content-length": "3" } }),
  ];
  setFetch(async () => {
    const response = responses.shift();
    assert.ok(response);
    return response;
  });

  await withMockedProperty(Response.prototype, "arrayBuffer", () => {
    throw new Error("noncanonical scalar response must use the bounded reader");
  }, async () => {
    assert.equal(await makeKv(KV).get("unknown-length"), "a");
    const legacy = await makeKv(KV).get("legacy-oversized", "arrayBuffer");
    assert.deepEqual([...new Uint8Array(legacy)], [1, 2, 3]);
  });
}));

test("KV rejects oversized host envelopes with a branded error and lease cleanup", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, KV_READ_RESPONSE_MAX_BYTES, kvCapacity, runtimeInfrastructure } =
    await loadKvBinding();
  let cancelled = false;
  setFetch(async () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  }), {
    headers: { "content-length": String(KV_READ_RESPONSE_MAX_BYTES + 1) },
  }));

  await assert.rejects(
    () => makeKv(KV).get("oversized"),
    (error) => error instanceof Error &&
      error.message === "KV read response is too large" &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
  assert.equal(cancelled, true);
  assert.equal(kvCapacity.kvReadCapacityStateForTest().inUseBytes, 0);
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

test("KV malformed batch envelopes return branded infrastructure errors", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
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
        entries: [{ key: "a", ...kvHostResponse.invalid.nonCanonicalBase64 }],
      })),
    },
    {
      keys: ["a"],
      response: () => new Response(JSON.stringify({
        entries: [{ key: "a", ...kvHostResponse.invalid.orphanMetadata }],
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
    setFetch(async () => testCase.response());
    await assert.rejects(
      () => makeKv(KV).get(testCase.keys, testCase.type),
      (error) => error instanceof Error &&
        error.message === "KV read response is invalid" &&
        runtimeInfrastructure.isRuntimeInfrastructureError(error)
    );
  }
}));

test("KV metadata envelope preserves empty values and brands malformed responses", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  setFetch(async () => new Response(JSON.stringify({ value_b64: "", metadata: null })));
  const result = await makeKv(KV).getWithMetadata("empty", "arrayBuffer");
  assert.ok(result.value instanceof ArrayBuffer);
  assert.equal(result.value.byteLength, 0);
  assert.equal(result.metadata, null);

  setFetch(async () => new Response(JSON.stringify(
    kvHostResponse.invalid.nonCanonicalBase64
  )));
  await assert.rejects(
    () => makeKv(KV).getWithMetadata("broken"),
    (error) => error instanceof Error &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );

  setFetch(async () => new Response(JSON.stringify(kvHostResponse.invalid.orphanMetadata)));
  await assert.rejects(
    () => makeKv(KV).getWithMetadata("missing"),
    (error) => error instanceof Error &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
}));

test("KV malformed list envelopes return branded infrastructure errors", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  setFetch(async () => new Response(JSON.stringify(
    kvHostResponse.invalid.incompleteListWithoutCursor
  )));
  await assert.rejects(
    () => makeKv(KV).list(),
    (error) => error instanceof Error &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );
}));

test("KV tenant JSON decoding errors remain ordinary user-data errors", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV } = await loadKvBinding();
  setFetch(async () => new Response(JSON.stringify({
    entries: [{ key: "a", value_b64: btoa("not-json"), metadata: null }],
  })));
  await assert.rejects(() => makeKv(KV).get(["a"], "json"), SyntaxError);
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

test("KV native scalar deadline aborts the fetch-owned response", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, kvCapacity, runtimeInfrastructure } = await loadKvBinding([], [
    [/export const KV_READ_LEASE_MAX_MS = 5_000;/, "export const KV_READ_LEASE_MAX_MS = 20;"],
  ]);
  /** @type {AbortSignal | undefined} */
  let fetchSignal;
  let upstreamAborted = false;
  setFetch(async (
    /** @type {string | URL | Request} */ _url,
    /** @type {RequestInit} */ init
  ) => {
    fetchSignal = init.signal ?? undefined;
    const body = new ReadableStream({
      start(controller) {
        fetchSignal?.addEventListener("abort", () => {
          upstreamAborted = true;
          controller.error(fetchSignal?.reason);
        }, { once: true });
        controller.enqueue(new Uint8Array([1]));
      },
    });
    return new Response(body, { headers: { "content-length": "2" } });
  });

  await assert.rejects(
    () => makeKv(KV).get("stalled"),
    (error) => error instanceof Error &&
      error.message === kvCapacity.KV_READ_TIMEOUT_ERROR_MESSAGE &&
      runtimeInfrastructure.isRuntimeInfrastructureError(error)
  );

  assert.equal(fetchSignal?.aborted, true);
  assert.equal(upstreamAborted, true);
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
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  setFetch(async () => new Response(null, { status: 404 }));
  const kv = makeKv(KV);
  const isBranded = brandedInfrastructureError(runtimeInfrastructure);

  assert.equal(await kv.get("missing"), null);
  await assert.rejects(() => kv.get(["missing"]), isBranded);
  await assert.rejects(() => kv.getWithMetadata("missing"), isBranded);
  await assert.rejects(() => kv.list(), isBranded);
  await assert.rejects(() => kv.put("key", "value"), /KV proxy \/kv\/put failed with 404/);
  await assert.rejects(() => kv.delete("key"), /KV proxy \/kv\/delete failed with 404/);
}));

test("KV read host configuration, auth, route, transport, and 5xx failures carry the infrastructure code", withFetchStub(async (/** @type {(stub: any) => void} */ setFetch) => {
  const { KV, runtimeInfrastructure } = await loadKvBinding();
  const kv = makeKv(KV);
  const isBranded = brandedInfrastructureError(runtimeInfrastructure);

  await assert.rejects(
    () => makeKv(KV, { SERVICE_NAME: "unit" }).get("key"),
    isBranded
  );
  await assert.rejects(
    () => makeKv(KV, { REDIS_PROXY_URL: "http://[", SERVICE_NAME: "unit" }).list(),
    isBranded
  );

  setFetch(async () => { throw new Error("connect failed"); });
  await assert.rejects(() => kv.get("key"), isBranded);

  for (const status of [401, 404, 405, 503]) {
    setFetch(async () => new Response(null, { status }));
    await assert.rejects(() => kv.list(), isBranded);
  }

  for (const status of [400, 413]) {
    setFetch(async () => new Response(null, { status }));
    await assert.rejects(
      () => kv.getWithMetadata("key"),
      (error) => error instanceof Error && !Object.hasOwn(error, "code")
    );
  }
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
