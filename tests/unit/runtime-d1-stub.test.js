import { beforeEach, afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { loadD1QueryWire } from "../helpers/load-d1-protocol.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { makeRecordingFetch, withMockedFetch } from "../helpers/mock-fetch.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  sharedModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { runtimeProxyBindingStubUrl, sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const OWNER_ENDPOINT_URL = repositoryFileUrl("runtime/_wdl-owner-endpoint.js");
const STALE_OWNER_HINT_ERRORS = [
  "task-draining",
  "owner-lease-expired",
  "owner-lease-too-short",
  "lease-budget-exhausted",
];
// Long enough that normal test fetches do not race the timeout path.
const TEST_D1_QUERY_TIMEOUT_LONG_MS = 1234;
// Short enough to force the transport-timeout branch without slowing the suite.
const TEST_D1_TRANSPORT_TIMEOUT_SHORT_MS = 10;
const INTERNAL_OWNER_TASK_ARN = "arn:aws:ecs:example-region-1:123456789012:task/example-cluster/task-abcdef";
const INTERNAL_OWNER_DETAIL_RE = /arn:|123456789012|example-region-1|example-cluster|task-abcdef|d1-runtime(?:-a:8787)?|internal\/d1\/query/;
const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_INTERNAL_AUTH_URL = sharedInternalAuthUrl();
const SHARED_D1_TIMEOUT_URL = sharedModuleDataUrl("shared/d1-timeout.js");
const SHARED_ERRORS_URL = repositoryFileUrl("shared/errors.js");
const {
  decodeD1QueryRequest,
  encodeD1QueryRequest,
  decodeD1QueryResponse,
  encodeD1QueryResponse,
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
} = await loadD1QueryWire();
globalThis.__encodeD1QueryRequest = encodeD1QueryRequest;
globalThis.__decodeD1QueryResponse = decodeD1QueryResponse;

/**
 * @typedef {object} MetricIncrementEntry
 * @property {string} name
 * @property {{ outcome?: string }} labels
 */

/**
 * @typedef {{
 *   metricIncrements: MetricIncrementEntry[]
 * }} D1RuntimeStubTestState
 */
/** @type {D1RuntimeStubTestState} */
const D1_RUNTIME_STUB_TEST_STATE = {
  metricIncrements: [],
};

/** @type {typeof globalThis & { __d1RuntimeStubTestState?: D1RuntimeStubTestState }} */
const d1RuntimeStubGlobal = globalThis;
d1RuntimeStubGlobal.__d1RuntimeStubTestState = D1_RUNTIME_STUB_TEST_STATE;

const ownerHintCacheSource = readRepositoryFile("runtime/_wdl-owner-hint-cache.js");
/** @type {Array<[RegExp, string]>} */
const stubSourceReplacements = [
  [
    /^\s*import\b[^\n]*\bfrom\s+"cloudflare:workers";\s*$/gm,
    `import { WorkerEntrypoint } from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`,
  ],
  [
    /import \{[^}]*\} from "shared-d1-timeout";/g,
    `import {
     createD1QueryDeadline,
     d1BackendUnavailablePayload,
     d1ResultUnknownPayload,
     d1QueryTimeoutPayload,
     isD1QueryTimeoutError,
   } from ${JSON.stringify(SHARED_D1_TIMEOUT_URL)};`,
  ],
  [
    /import \{[^}]*\} from "shared-d1-query-wire";/g,
    `const D1_QUERY_CONTENT_TYPE = ${JSON.stringify(D1_QUERY_CONTENT_TYPE)};
   const D1_QUERY_RESPONSE_CONTENT_TYPE = ${JSON.stringify(D1_QUERY_RESPONSE_CONTENT_TYPE)};
   const decodeD1QueryResponse = globalThis.__decodeD1QueryResponse;
   const encodeD1QueryRequest = globalThis.__encodeD1QueryRequest;`,
  ],
  [
    /import \{ metrics \} from "runtime-metrics";/g,
    `const metrics = {
     increment(name, labels) {
       /** @type {any} */ (globalThis).__d1RuntimeStubTestState.metricIncrements.push({ name, labels });
     },
   };`,
  ],
  [/import \{ createOwnerHintCache \} from "runtime-owner-hint-cache";/g, ownerHintCacheSource],
  [
    /import \{ validOwnerEndpointForService \} from "runtime-owner-endpoint";/g,
    `import { validOwnerEndpointForService } from ${JSON.stringify(OWNER_ENDPOINT_URL)};`,
  ],
  [/from "runtime-bindings-proxy";/g, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
  [/from "shared-internal-auth";/g, `from ${JSON.stringify(SHARED_INTERNAL_AUTH_URL)};`],
  [/from "shared-errors";/g, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
];
const stubSrc = applyModuleReplacements(
  readRepositoryFile("runtime/bindings/d1.js"),
  stubSourceReplacements
);

const stubMod = await import(moduleDataUrl(stubSrc));
const {
  clearD1OwnerHintsForTest,
  setD1OwnerHintMaxEntriesForTest,
  D1Database: RuntimeD1Database,
} = stubMod;

beforeEach(() => {
  D1_RUNTIME_STUB_TEST_STATE.metricIncrements = [];
  clearD1OwnerHintsForTest();
});

afterEach(() => {
  setD1OwnerHintMaxEntriesForTest(null);
});

/** @param {string} name */
function metricOutcomes(name) {
  return D1_RUNTIME_STUB_TEST_STATE.metricIncrements
    .filter((entry) => entry.name === name)
    .map((entry) => entry.labels.outcome);
}

/**
 * @param {any} body
 * @param {ResponseInit} [init]
 */
function d1Response(body, init = {}) {
  return new Response(encodeD1QueryResponse(body), {
    ...init,
    headers: {
      "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE,
      .../** @type {Record<string, string>} */ (init.headers || {}),
    },
  });
}

/**
 * Creates a RuntimeD1Database test binding with stable default props and env.
 *
 * @param {(url: any, init: any) => Promise<Response>} fetchImpl
 * @param {{ props?: Record<string, unknown>, env?: Record<string, unknown> }} [options]
 */
function createRuntimeDb(fetchImpl, options = {}) {
  return new RuntimeD1Database(
    {
      props: {
        ns: "tenant-a",
        databaseId: "main-db",
        databaseName: "main",
        binding: "DB",
        ...(options.props || {}),
      },
    },
    {
      D1_BACKEND: { fetch: fetchImpl },
      D1_QUERY_TIMEOUT_MS: TEST_D1_QUERY_TIMEOUT_LONG_MS,
      SERVICE_NAME: "user-runtime",
      WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
      ...(options.env || {}),
    }
  );
}

/**
 * Creates a D1 runtime stub fixture that records decoded backend calls.
 *
 * @param {((body: any, init: any, url: any) => any) | undefined} [handler]
 * @returns {{ db: any, makeDb: () => any, calls: any[] }}
 */
function makeRuntimeDb(handler) {
  /** @type {any[]} */
  const calls = [];
  const makeDb = () => createRuntimeDb(
    /** @param {any} url @param {any} init */
    async (url, init) => {
      const body = decodeD1QueryRequest(init.body);
      calls.push({
        ...body,
        url,
        headers: Object.fromEntries(new Headers(init.headers).entries()),
        signal: init.signal,
      });
      const handled = handler ? await handler(body, init, url) : {
        success: true,
        results: [{ ok: 1 }],
        meta: { duration: 0 },
      };
      return handled instanceof Response ? handled : d1Response(handled);
    }
  );
  return { db: makeDb(), makeDb, calls };
}

test("D1 host RPC surface exposes only the query method", () => {
  assert.deepEqual(Object.getOwnPropertyNames(RuntimeD1Database.prototype).toSorted(), [
    "constructor",
    "query",
  ]);
});

test("D1 runtime stub: query sends platform identity and mode to backend", async () => {
  const { db, calls } = makeRuntimeDb();

  const result = await db.query("all", [{ sql: "select ? as ok", params: [1] }]);

  assert.deepEqual(result.results, [{ ok: 1 }]);
  assert.equal(calls[0].namespace, "tenant-a");
  assert.equal(calls[0].databaseId, "main-db");
  assert.equal("databaseName" in calls[0], false);
  assert.equal(calls[0].binding, "DB");
  assert.equal(calls[0].mode, "all");
  assert.ok(calls[0].signal instanceof AbortSignal);
  assert.deepEqual(calls[0].statements, [{ sql: "select ? as ok", params: [1] }]);
});

test("D1 runtime stub: forwards request id header to backend", async () => {
  const { db, calls } = makeRuntimeDb();

  await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");

  assert.equal(calls[0].headers["x-request-id"], "rid-d1");
});

test("D1 runtime stub: backend transport failures do not expose internal details", async () => {
  const { db } = makeRuntimeDb(() => {
    throw new Error(`connect ECONNREFUSED http://d1-runtime/internal/d1/query ${INTERNAL_OWNER_TASK_ARN}`);
  });

  await assert.rejects(
    () => db.query("all", [{ sql: "select 1", params: [] }], "rid-d1"),
    (err) => err instanceof Error &&
      err.name === "D1_ERROR" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "backend-unavailable" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "internal" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === true &&
      /D1 backend is unavailable/.test(err.message) &&
      !INTERNAL_OWNER_DETAIL_RE.test(err.message)
  );
});

test("D1 runtime stub: malformed direct router response is result-unknown", async () => {
  const { db, calls } = makeRuntimeDb(() => new Response("not a D1 payload", {
    status: 502,
    headers: { "content-type": "text/plain" },
  }));

  await assert.rejects(
    () => db.query("all", [{ sql: "insert into t values (1)", params: [] }], "rid-d1"),
    (err) => err instanceof Error &&
      err.name === "D1_ERROR" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "result-unknown" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "result-unknown" &&
      /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === false &&
      /outcome may be unknown/.test(err.message)
  );

  assert.equal(calls.length, 1);
  assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss"]);
});

test("D1 runtime stub: learned owner hints survive new binding instances", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, makeDb, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  const nextDb = makeDb();
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    return d1Response({
      success: true,
      results: [{ via: "owner" }],
    }, { headers: ownerHeaders });
  }, async () => {
    const first = await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    const second = await nextDb.query("all", [{ sql: "select 2", params: [] }], "rid-d1");

    assert.deepEqual(first.results, [{ via: "router" }]);
    assert.deepEqual(second.results, [{ via: "owner" }]);
    assert.equal(calls.length, 1);
    assert.equal(directCalls.length, 1);
    assert.equal(directCalls[0].url, "http://d1-runtime-a:8787/internal/d1/query");
    assert.equal(directCalls[0].headers["x-request-id"], "rid-d1");
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "hit", "learned"]);
  });
});

test("D1 runtime stub: owner hints preserve zero and reject fractional generations", async () => {
  const { db, makeDb, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, {
    headers: {
      "x-wdl-d1-owner-task-id": "task-a",
      "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
      "x-wdl-d1-owner-generation": "0",
    },
  }));
  await db.query("all", [{ sql: "select 1", params: [] }]);

  /** @type {string[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url) => {
    directCalls.push(String(url));
    return d1Response({ success: true, results: [{ via: "owner" }] });
  }, async () => {
    await makeDb().query("all", [{ sql: "select 2", params: [] }]);
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(directCalls, ["http://d1-runtime-a:8787/internal/d1/query"]);

  clearD1OwnerHintsForTest();
  const bad = makeRuntimeDb(() => d1Response({
    success: true,
    results: [],
  }, {
    headers: {
      "x-wdl-d1-owner-task-id": "task-b",
      "x-wdl-d1-owner-endpoint": "d1-runtime-b:8787",
      "x-wdl-d1-owner-generation": "1.5",
    },
  }));
  await bad.db.query("all", [{ sql: "select 3", params: [] }]);
  let unexpectedFractionalFetchCalled = false;
  await withMockedFetch(async () => {
    unexpectedFractionalFetchCalled = true;
    return d1Response({ success: true, results: [] });
  }, async () => {
    await bad.makeDb().query("all", [{ sql: "select 4", params: [] }]);
  });
  assert.equal(
    unexpectedFractionalFetchCalled,
    false,
    "expected no direct fetch for fractional generation, but fetch was called"
  );
  assert.equal(bad.calls.length, 2);

  clearD1OwnerHintsForTest();
  const missing = makeRuntimeDb(() => d1Response({
    success: true,
    results: [],
  }, {
    headers: {
      "x-wdl-d1-owner-task-id": "task-c",
      "x-wdl-d1-owner-endpoint": "d1-runtime-c:8787",
    },
  }));
  await missing.db.query("all", [{ sql: "select 5", params: [] }]);
  let unexpectedMissingGenerationFetchCalled = false;
  await withMockedFetch(async () => {
    unexpectedMissingGenerationFetchCalled = true;
    return d1Response({ success: true, results: [] });
  }, async () => {
    await missing.makeDb().query("all", [{ sql: "select 6", params: [] }]);
  });
  assert.equal(
    unexpectedMissingGenerationFetchCalled,
    false,
    "expected no direct fetch without owner generation, but fetch was called"
  );
  assert.equal(missing.calls.length, 2);
});

test("D1 runtime stub: accepts Kubernetes headless owner endpoints", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "d1-runtime-0",
    "x-wdl-d1-owner-endpoint": "d1-runtime-0.d1-runtime-headless:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, makeDb, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  const nextDb = makeDb();
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    return d1Response({
      success: true,
      results: [{ via: "owner" }],
    }, { headers: ownerHeaders });
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    const second = await nextDb.query("all", [{ sql: "select 2", params: [] }], "rid-d1");

    assert.deepEqual(second.results, [{ via: "owner" }]);
    assert.equal(calls.length, 1);
    assert.equal(directCalls.length, 1);
    assert.equal(directCalls[0].url, "http://d1-runtime-0.d1-runtime-headless:8787/internal/d1/query");
    assert.equal(directCalls[0].headers["x-request-id"], "rid-d1");
  });
});

test("D1 runtime stub: owner hint cache evicts oldest entries at the configured cap", async () => {
  setD1OwnerHintMaxEntriesForTest(2);
  const ownerHeaders = (/** @type {string} */ task) => ({
    "x-wdl-d1-owner-task-id": task,
    "x-wdl-d1-owner-endpoint": `d1-runtime-${task}:8787`,
    "x-wdl-d1-owner-generation": "7",
  });
  const makeDb = (/** @type {string} */ databaseId) => {
    /** @type {any[]} */
    const calls = [];
    const db = createRuntimeDb(
      makeRecordingFetch(calls, {
        capture: (_call, url, init) => ({ url, body: decodeD1QueryRequest(init.body) }),
        response: () => d1Response({
          success: true,
          results: [{ via: "router", databaseId }],
        }, { headers: ownerHeaders(`task-${databaseId}`) }),
      }),
      {
        props: { databaseId },
      }
    );
    return { db, calls };
  };
  const first = makeDb("one");
  const second = makeDb("two");
  const third = makeDb("three");
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(makeRecordingFetch(directCalls, {
    capture: (_call, url, init) => ({ url: String(url), body: decodeD1QueryRequest(init.body) }),
    response: () => d1Response({
      success: true,
      results: [{ via: "owner" }],
    }, { headers: ownerHeaders("task-refreshed") }),
  }), async () => {
    await first.db.query("all", [{ sql: "select 1", params: [] }]);
    await second.db.query("all", [{ sql: "select 2", params: [] }]);
    await third.db.query("all", [{ sql: "select 3", params: [] }]);

    const againSecond = makeDb("two");
    await againSecond.db.query("all", [{ sql: "select 2", params: [] }]);
    const againFirst = makeDb("one");
    await againFirst.db.query("all", [{ sql: "select 1", params: [] }]);

    assert.equal(againFirst.calls.length, 1, "oldest owner hint should be evicted");
    assert.equal(againSecond.calls.length, 0, "newer owner hint should still route direct");
    assert.equal(directCalls.length, 1);
    assert.equal(directCalls[0].url, "http://d1-runtime-task-two:8787/internal/d1/query");
  });
});

test("D1 runtime stub: ignores invalid owner hint endpoints", async () => {
  for (const endpoint of [
    "workers.example:8787",
    "d1-runtime-0.d1-runtime-headless.evil.com:8787",
  ]) {
    D1_RUNTIME_STUB_TEST_STATE.metricIncrements = [];
    const ownerHeaders = {
      "x-wdl-d1-owner-task-id": "task-a",
      "x-wdl-d1-owner-endpoint": endpoint,
      "x-wdl-d1-owner-generation": "7",
    };
    const { db, makeDb, calls } = makeRuntimeDb(() => d1Response({
      success: true,
      results: [{ via: "router" }],
    }, { headers: ownerHeaders }));
    const nextDb = makeDb();
    await withMockedFetch(async () => {
      throw new Error("invalid D1 owner endpoint should not be used");
    }, async () => {
      await db.query("all", [{ sql: "select 1", params: [] }]);
      await nextDb.query("all", [{ sql: "select 2", params: [] }]);

      assert.equal(calls.length, 2);
      assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "miss"]);
    });
  }
});

test("D1 runtime stub: stale owner hint ownership error clears hint and falls back to router", async () => {
  for (const staleError of STALE_OWNER_HINT_ERRORS) {
    clearD1OwnerHintsForTest();
    D1_RUNTIME_STUB_TEST_STATE.metricIncrements = [];
    const ownerHeaders = {
      "x-wdl-d1-owner-task-id": "task-a",
      "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
      "x-wdl-d1-owner-generation": "7",
    };
    const { db, calls } = makeRuntimeDb(() => d1Response({
      success: true,
      results: [{ via: "router" }],
    }, { headers: ownerHeaders }));
    /** @type {any[]} */
    const directCalls = [];
    await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
      directCalls.push({
        url: String(url),
        body: decodeD1QueryRequest(init.body),
        headers: Object.fromEntries(new Headers(init.headers).entries()),
      });
      return d1Response({
        success: false,
        error: staleError,
        message: "old owner is unavailable",
        category: "ownership",
        retryable: true,
      }, { status: 503 });
    }, async () => {
      await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
      const recovered = await db.query("all", [{ sql: "select 2", params: [] }], "rid-d1");

      assert.deepEqual(recovered.results, [{ via: "router" }], staleError);
      assert.equal(directCalls.length, 1, staleError);
      assert.equal(calls.length, 2, staleError);
      assert.equal(calls[1].headers["x-request-id"], "rid-d1", staleError);
      assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "cleared", "learned"], staleError);
    });
  }
});

test("D1 runtime stub: malformed stale-owner router fallback is result-unknown", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, calls } = makeRuntimeDb(() => {
    if (calls.length === 1) {
      return d1Response({
        success: true,
        results: [{ via: "router" }],
      }, { headers: ownerHeaders });
    }
    return new Response("not a D1 payload", {
      status: 502,
      headers: { "content-type": "text/plain" },
    });
  });
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    return d1Response({
      success: false,
      error: "task-draining",
      message: "old owner is draining",
      category: "ownership",
      retryable: true,
    }, { status: 503 });
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    await assert.rejects(
      () => db.query("all", [{ sql: "select 2", params: [] }], "rid-d1"),
      (err) => err instanceof Error &&
        err.name === "D1_ERROR" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === false &&
        /outcome may be unknown/.test(err.message)
    );

    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 2);
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "cleared"]);
  });
});

test("D1 runtime stub: direct owner transport failures are result-unknown and not retried through router", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    throw new Error(`connection reset after request to http://d1-runtime-a:8787/internal/d1/query ${INTERNAL_OWNER_TASK_ARN}`);
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    await assert.rejects(
      () => db.query("all", [{ sql: "insert into t values (1)", params: [] }], "rid-d1"),
      (err) => err instanceof Error &&
        err.name === "D1_ERROR" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === false &&
        /outcome may be unknown/.test(err.message) &&
        !INTERNAL_OWNER_DETAIL_RE.test(err.message)
    );

    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 1);
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "cleared"]);
  });
});

test("D1 runtime stub: direct owner body read failures are result-unknown and clear hints", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    const response = new Response(encodeD1QueryResponse({
      success: true,
      results: [{ via: "owner" }],
    }), {
      headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
    });
    Object.defineProperty(response, "arrayBuffer", {
      value: async () => { throw new Error("connection reset while reading response body"); },
    });
    return response;
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    await assert.rejects(
      () => db.query("all", [{ sql: "insert into t values (1)", params: [] }], "rid-d1"),
      (err) => err instanceof Error &&
        err.name === "D1_ERROR" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === false &&
        /outcome may be unknown/.test(err.message)
    );

    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 1);
    const recovered = await db.query("all", [{ sql: "select 2", params: [] }], "rid-d1");
    assert.deepEqual(recovered.results, [{ via: "router" }]);
    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 2);
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "cleared", "miss", "learned"]);
  });
});

test("D1 runtime stub: direct owner unavailable responses are result-unknown and clear hints", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    return new Response("upstream request timeout", { status: 504 });
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    await assert.rejects(
      () => db.query("all", [{ sql: "insert into t values (1)", params: [] }], "rid-d1"),
      (err) => err instanceof Error &&
        err.name === "D1_ERROR" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).code === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).category === "result-unknown" &&
        /** @type {{ code?: unknown, category?: unknown, retryable?: unknown }} */ (err).retryable === false &&
        /outcome may be unknown/.test(err.message)
    );

    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 1);
    const recovered = await db.query("all", [{ sql: "select 2", params: [] }], "rid-d1");
    assert.deepEqual(recovered.results, [{ via: "router" }]);
    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 2);
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "cleared", "miss", "learned"]);
  });
});

test("D1 runtime stub: non-ownership owner-hint errors are not retried through router", async () => {
  const ownerHeaders = {
    "x-wdl-d1-owner-task-id": "task-a",
    "x-wdl-d1-owner-endpoint": "d1-runtime-a:8787",
    "x-wdl-d1-owner-generation": "7",
  };
  const { db, calls } = makeRuntimeDb(() => d1Response({
    success: true,
    results: [{ via: "router" }],
  }, { headers: ownerHeaders }));
  /** @type {any[]} */
  const directCalls = [];
  await withMockedFetch(async (/** @type {any} */ url, /** @type {any} */ init) => {
    directCalls.push({
      url: String(url),
      body: decodeD1QueryRequest(init.body),
      headers: Object.fromEntries(new Headers(init.headers).entries()),
    });
    return d1Response({
      success: false,
      error: "internal",
      message: "owner returned an internal error",
      category: "internal",
      retryable: true,
    }, { status: 500 });
  }, async () => {
    await db.query("all", [{ sql: "select 1", params: [] }], "rid-d1");
    await assert.rejects(
      () => db.query("all", [{ sql: "select 2", params: [] }], "rid-d1"),
      (err) => err instanceof Error &&
        err.name === "D1_ERROR" &&
        /** @type {{ code?: unknown }} */ (err).code === "internal" &&
        /owner returned an internal error/.test(err.message)
    );

    assert.equal(directCalls.length, 1);
    assert.equal(calls.length, 1);
    assert.deepEqual(metricOutcomes("d1_owner_hint_outcomes"), ["miss", "learned", "hit"]);
  });
});

test("D1 runtime stub: runtime client errors include stable D1 code", async () => {
  const { db } = makeRuntimeDb(() => ({
    success: false,
    error: "sql-error",
    message: "SQL error: no such table: posts",
    category: "sql",
    retryable: false,
  }));

  await assert.rejects(
    () => db.query("all", [{ sql: "select * from posts", params: [] }]),
    /D1_ERROR \[sql-error\]: SQL error: no such table: posts/
  );
});

test("D1 runtime stub: transport timeout is wrapped as stable D1 error", async () => {
  /** @type {any[]} */
  const calls = [];
  const db = createRuntimeDb(
    async (/** @type {any} */ _url, /** @type {any} */ init) => {
      calls.push({ signal: init.signal });
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => {
          reject(init.signal.reason || new DOMException("operation timed out", "TimeoutError"));
        }, { once: true });
      });
    },
    {
      env: { D1_QUERY_TIMEOUT_MS: TEST_D1_TRANSPORT_TIMEOUT_SHORT_MS },
    }
  );

  await assert.rejects(
    () => db.query("all", [{ sql: "select 1", params: [] }]),
    (err) => err instanceof Error &&
      err.name === "D1_ERROR" &&
      /** @type {any} */ (err).code === "timeout" &&
      /** @type {any} */ (err).category === "timeout" &&
      /** @type {any} */ (err).retryable === false &&
      /D1_ERROR \[timeout\]/.test(err.message)
  );
  assert.ok(calls[0].signal instanceof AbortSignal);
});
