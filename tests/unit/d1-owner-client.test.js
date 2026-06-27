import assert from "node:assert/strict";
import { beforeEach, afterEach, test } from "node:test";
import {
  D1_QUERY_CONTENT_TYPE,
  D1_QUERY_RESPONSE_CONTENT_TYPE,
  d1OwnerClientHarnessState,
  decodeD1QueryRequest,
  loadD1OwnerClient,
  resetD1OwnerClientHarness,
} from "../helpers/load-d1-owner-client.js";
import { installMockFetch, makeRecordingFetch } from "../helpers/mock-fetch.js";

const mod = loadD1OwnerClient();
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const D1_OWNER_CLIENT_TEST_STATE = d1OwnerClientHarnessState();
const INTERNAL_OWNER_TASK_ARN = "arn:aws:ecs:example-region-1:123456789012:task/example-cluster/task-abcdef";
const INTERNAL_OWNER_DETAIL_RE = /arn:|123456789012|example-region-1|example-cluster|task-abcdef|d1-runtime-b:8787|internal\/d1\/query/;

let restoreFetch = () => {};

beforeEach(() => {
  resetD1OwnerClientHarness();
  restoreFetch = installMockFetch(makeRecordingFetch(D1_OWNER_CLIENT_TEST_STATE.fetches, {
    response: new Response(null, { status: 302 }),
  }));
});

afterEach(() => {
  restoreFetch();
  restoreFetch = () => {};
});

function query() {
  return {
    namespace: "tenant",
    databaseId: "main",
    binding: "DB",
    mode: "all",
    statements: [{ sql: "select 1", params: [] }],
    dbKey: "tenant:main",
    slot: 3,
  };
}

function owner() {
  return {
    taskId: "d1-runtime-b",
    generation: 4,
    endpoint: "d1-runtime-b:8787",
  };
}

function ownerWithTaskArn() {
  return {
    ...owner(),
    taskId: INTERNAL_OWNER_TASK_ARN,
  };
}

function env() {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
}

test("D1 owner client classifies non-error forwarded HTTP status as ok", async () => {
  const response = await mod.forwardToOwner(query(), env(), owner(), "req-1", 1);

  assert.equal(response.status, 302);
  assert.equal(D1_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  const call = D1_OWNER_CLIENT_TEST_STATE.fetches[0];
  assert.equal(call.url, "http://d1-runtime-b:8787/internal/d1/query");
  assert.equal(new Headers(call.init.headers).get("x-request-id"), "req-1");
  assert.equal(new Headers(call.init.headers).get("x-wdl-d1-hop-count"), "2");
  assert.equal(new Headers(call.init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
  assert.equal(new Headers(call.init.headers).get("content-type"), D1_QUERY_CONTENT_TYPE);
  assert.deepEqual(decodeD1QueryRequest(call.init.body), {
    namespace: "tenant",
    databaseId: "main",
    binding: "DB",
    mode: "all",
    statements: [{ sql: "select 1", params: [] }],
  });
  assert.deepEqual(D1_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "d1_forwards",
    labels: { service: "d1-runtime", outcome: "ok" },
  });
  assert.equal(D1_OWNER_CLIENT_TEST_STATE.logs.at(-1).level, "info");
});

test("D1 owner client maps post-forward transport failures to result-unknown", async () => {
  restoreFetch();
  restoreFetch = installMockFetch(makeRecordingFetch(D1_OWNER_CLIENT_TEST_STATE.fetches, {
    response: () => {
      throw new Error(`connection reset after request to http://d1-runtime-b:8787/internal/d1/query ${INTERNAL_OWNER_TASK_ARN}`);
    },
  }));

  await assert.rejects(
    () => mod.forwardToOwner(query(), env(), ownerWithTaskArn(), "req-2", 1),
    (err) => {
      assert.ok(err instanceof Error);
      assert.equal(/** @type {{ status?: unknown }} */ (err).status, 503);
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "result-unknown");
      assert.match(err.message, /outcome may be unknown/);
      assert.doesNotMatch(err.message, INTERNAL_OWNER_DETAIL_RE);
      return true;
    }
  );
  assert.equal(D1_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  assert.deepEqual(D1_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "d1_forwards",
    labels: { service: "d1-runtime", outcome: "unavailable" },
  });
  assert.deepEqual(D1_OWNER_CLIENT_TEST_STATE.logs.at(-1), {
    level: "warn",
    event: "d1_forward_unavailable",
    fields: {
      request_id: "req-2",
      namespace: "tenant",
      database_id: "main",
      slot: 3,
      owner_task_id: INTERNAL_OWNER_TASK_ARN,
      owner_endpoint: "d1-runtime-b:8787",
      error_message: `connection reset after request to http://d1-runtime-b:8787/internal/d1/query ${INTERNAL_OWNER_TASK_ARN}`,
    },
  });
});

test("D1 owner client maps non-wire owner unavailable responses to result-unknown", async () => {
  restoreFetch();
  restoreFetch = installMockFetch(makeRecordingFetch(D1_OWNER_CLIENT_TEST_STATE.fetches, {
    response: new Response("upstream request timeout", {
      status: 504,
      headers: { "content-type": "text/plain" },
    }),
  }));

  await assert.rejects(
    () => mod.forwardToOwner(query(), env(), ownerWithTaskArn(), "req-3", 1),
    (err) => {
      assert.ok(err instanceof Error);
      assert.equal(/** @type {{ status?: unknown }} */ (err).status, 503);
      assert.equal(/** @type {{ code?: unknown }} */ (err).code, "result-unknown");
      assert.match(err.message, /HTTP 504/);
      assert.doesNotMatch(err.message, INTERNAL_OWNER_DETAIL_RE);
      return true;
    }
  );
  assert.equal(D1_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  assert.deepEqual(D1_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "d1_forwards",
    labels: { service: "d1-runtime", outcome: "error" },
  });
});

test("D1 owner client preserves wire owner unavailable responses", async () => {
  const ownerErrorBody = new Uint8Array([1, 2, 3]);
  restoreFetch();
  restoreFetch = installMockFetch(makeRecordingFetch(D1_OWNER_CLIENT_TEST_STATE.fetches, {
    response: new Response(ownerErrorBody, {
      status: 503,
      headers: { "content-type": D1_QUERY_RESPONSE_CONTENT_TYPE },
    }),
  }));

  const response = await mod.forwardToOwner(query(), env(), owner(), "req-4", 1);

  assert.equal(response.status, 503);
  assert.equal(response.headers.get("content-type"), D1_QUERY_RESPONSE_CONTENT_TYPE);
  assert.deepEqual(new Uint8Array(await response.arrayBuffer()), ownerErrorBody);
  assert.equal(D1_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  assert.deepEqual(D1_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "d1_forwards",
    labels: { service: "d1-runtime", outcome: "error" },
  });
});
