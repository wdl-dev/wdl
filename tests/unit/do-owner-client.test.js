import assert from "node:assert/strict";
import { beforeEach, afterEach, test } from "node:test";

import { decodeDoEnvelopeMetadata as decodeDoEnvelope } from "../helpers/do-envelope.js";
import {
  DO_INVOKE_CONTENT_TYPE,
  doOwnerClientHarnessState,
  loadDoOwnerClient,
  resetDoOwnerClientHarness,
} from "../helpers/load-do-owner-client.js";
import { installMockFetch, makeRecordingFetch, withMockedFetch } from "../helpers/mock-fetch.js";

const mod = loadDoOwnerClient();
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";
const DO_OWNER_CLIENT_TEST_STATE = doOwnerClientHarnessState();

const DO_STORAGE_ID = "do_0123456789abcdef0123456789abcdef";
const OWNER_KEY = `${DO_STORAGE_ID}:Room:shard0`;
let restoreFetch = () => {};

beforeEach(() => {
  resetDoOwnerClientHarness();
  restoreFetch = installMockFetch(makeRecordingFetch(DO_OWNER_CLIENT_TEST_STATE.fetches, {
    response: Response.json({ ok: true }),
  }));
});

afterEach(() => {
  restoreFetch();
  restoreFetch = () => {};
});

function invoke() {
  return {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    workerId: "tenant:chat:v1",
    doStorageId: DO_STORAGE_ID,
    hostId: OWNER_KEY,
    className: "Room",
    objectName: "room-a",
    request: { method: "GET", url: "https://demo.workers.example/", headers: [] },
  };
}

function owner() {
  return {
    ownerKey: OWNER_KEY,
    taskId: "task-b",
    generation: 4,
    endpoint: "do-runtime-b:8788",
  };
}

function env() {
  return { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN };
}

/**
 * @param {unknown} err
 * @param {{ status: number, code: string, message?: string }} expect
 */
function doErrorMatches(err, { status, code, message = undefined }) {
  const actual = /** @type {{ status?: number, code?: string, message?: string }} */ (err);
  return (
    actual?.status === status &&
    actual?.code === code &&
    (message === undefined || actual.message === message)
  );
}

test("DO owner client parseHopCount rejects NaN and negative hop headers", () => {
  assert.equal(mod.parseHopCount("abc"), 0);
  assert.equal(mod.parseHopCount("-1"), 0);
  assert.equal(mod.parseHopCount("5"), 5);
  assert.equal(mod.parseHopCount("1.9"), 1);
  assert.equal(mod.parseHopCount(null), 0);
});

test("DO owner client forwards invoke requests with owner fence and hop headers", async () => {
  const response = await mod.forwardToOwner(invoke(), env(), owner(), "req-1", 1, "/internal/do/storage/delete");

  assert.equal(response.status, 200);
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  const call = DO_OWNER_CLIENT_TEST_STATE.fetches[0];
  assert.equal(call.url, "http://do-runtime-b:8788/internal/do/storage/delete");
  assert.equal(new Headers(call.init.headers).get("x-request-id"), "req-1");
  assert.equal(new Headers(call.init.headers).get("x-wdl-do-hop-count"), "2");
  assert.equal(new Headers(call.init.headers).get("x-wdl-internal-auth"), TEST_INTERNAL_AUTH_TOKEN);
  assert.equal(new Headers(call.init.headers).get("content-type"), DO_INVOKE_CONTENT_TYPE);
  const body = decodeDoEnvelope(call.init.body);
  assert.deepEqual(body.owner, {
    ownerKey: OWNER_KEY,
    taskId: "task-b",
    generation: 4,
  });
  assert.deepEqual(DO_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "do_forwards",
    labels: { service: "do-runtime", outcome: "ok" },
  });
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.logs.at(-1).event, "do_forward_complete");
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.logs.at(-1).fields.path, "/internal/do/storage/delete");
});

test("DO owner client rejects invalid endpoints before authenticated forwarding", async () => {
  await assert.rejects(
    mod.forwardToOwner(invoke(), env(), { ...owner(), endpoint: "8.8.8.8:8788" }, "req-invalid", 0),
    (err) => doErrorMatches(err, { status: 503, code: "owner_unavailable" })
  );
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.fetches.length, 0);
});

test("DO owner client maps exhausted forward hops to a stable 503 code", async () => {
  await assert.rejects(
    mod.forwardToOwner(invoke(), env(), owner(), "req-2", 2),
    (err) => doErrorMatches(err, { status: 503, code: "forward_hop_exhausted" })
  );
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.fetches.length, 0);
});

test("DO owner client maps unreachable owners to owner_unavailable", async () => {
  await withMockedFetch(
    async () => {
      throw new Error("connect ECONNREFUSED");
    },
    async () => {
      await assert.rejects(
        mod.forwardToOwner(invoke(), env(), owner(), "req-3", 0),
        (err) => doErrorMatches(err, {
          status: 503,
          code: "owner_unavailable",
          message: "DO owner is unavailable",
        })
      );
    }
  );
  assert.deepEqual(DO_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "do_forwards",
    labels: { service: "do-runtime", outcome: "unavailable" },
  });
});

test("DO owner client maps unreachable WebSocket owners without leaking transport errors", async () => {
  const request = new Request("https://do-runtime/internal/do/connect", {
    headers: { upgrade: "websocket" },
  });

  await withMockedFetch(
    async () => {
      throw new Error("connect ECONNREFUSED 10.0.0.1:8788");
    },
    async () => {
      await assert.rejects(
        mod.forwardConnectToOwner(request, invoke(), env(), owner(), "req-4", 0),
        (err) => doErrorMatches(err, {
          status: 503,
          code: "owner_unavailable",
          message: "DO owner is unavailable",
        })
      );
    }
  );
});

test("DO owner client forwards WebSocket connect requests with owner headers", async () => {
  const request = new Request("https://do-runtime/internal/do/connect", {
    headers: { upgrade: "websocket", "x-request-id": "outer" },
  });
  const response = await withMockedFetch(
    makeRecordingFetch(DO_OWNER_CLIENT_TEST_STATE.fetches, {
      response: () => /** @type {Response} */ ({ status: 101 }),
    }),
    async () => mod.forwardConnectToOwner(request, invoke(), env(), owner(), "req-4", 0)
  );

  assert.equal(response.status, 101);
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.fetches.length, 1);
  const call = DO_OWNER_CLIENT_TEST_STATE.fetches[0];
  assert.equal(call.url, "http://do-runtime-b:8788/internal/do/connect");
  assert.equal(call.init.headers.get("x-request-id"), "req-4");
  assert.equal(call.init.headers.get("x-wdl-do-hop-count"), "1");
  assert.equal(call.init.headers.get("x-wdl-do-owner-key"), OWNER_KEY);
  assert.equal(call.init.headers.get("x-wdl-do-owner-task-id"), "task-b");
  assert.equal(call.init.headers.get("x-wdl-do-owner-generation"), "4");
  assert.deepEqual(DO_OWNER_CLIENT_TEST_STATE.metrics.at(-1), {
    name: "do_forwards",
    labels: { service: "do-runtime", outcome: "ok" },
  });
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.logs.at(-1).level, "info");
});

test("DO owner client maps exhausted WebSocket forward hops to a stable 503 code", async () => {
  const request = new Request("https://do-runtime/internal/do/connect", {
    headers: { upgrade: "websocket" },
  });

  await assert.rejects(
    mod.forwardConnectToOwner(request, invoke(), env(), owner(), "req-5", 2),
    (err) => doErrorMatches(err, { status: 503, code: "forward_hop_exhausted" })
  );
  assert.equal(DO_OWNER_CLIENT_TEST_STATE.fetches.length, 0);
});
