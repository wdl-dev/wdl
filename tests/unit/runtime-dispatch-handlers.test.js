import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { loadRuntimeDispatch } from "../helpers/load-runtime-dispatch.js";
import {
  installTailFetchSpy,
  jsonRequest,
  makeCtx,
  makeScope,
  makeStub,
  tailAppendPayloads,
} from "../helpers/runtime-dispatch-fixtures.js";
import { assertJsonResponse, readJsonResponse } from "../helpers/response-json.js";

const { runtimeDispatch } = await loadRuntimeDispatch();
const {
  _resetWorkflowReplayCacheForTest,
  handleFetchDispatch,
  handleQueuedDispatch,
  handleScheduledDispatch,
} = runtimeDispatch;

beforeEach(() => {
  _resetWorkflowReplayCacheForTest();
});

test("handleScheduledDispatch forwards normalized scheduler payload", async () => {
  /** @type {any[]} */
  const calls = [];
  const scope = makeScope();
  const res = await handleScheduledDispatch({
    request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
    scope,
    stub: makeStub({
      async scheduled(/** @type {any} */ payload) {
        calls.push(payload);
        return { outcome: "ok" };
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.equal(body.outcome, "ok");
  assert.equal(typeof body.duration_ms, "number");
  assert.deepEqual(calls, [{ scheduledTime: 123, cron: "* * * * *" }]);
  assert.deepEqual(scope.errors, []);
});

test("handleScheduledDispatch emits start and finish tail events with worker identity", async () => {
  const fetchSpy = installTailFetchSpy(["demo:cron-worker"]);
  const ctx = makeCtx();
  const scope = makeScope();

  try {
    const res = await handleScheduledDispatch({
      request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "cron-worker",
        workerId: "demo:cron-worker:v1",
        requestId: "rid-scheduled",
      },
      stub: makeStub({
        async scheduled() {
          return { outcome: "ok" };
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 200);
    const events = tailAppendPayloads(fetchSpy.calls);
    assert.equal(events.length, 2);
    assert.deepEqual(events.map((e) => [e.ns, e.worker]), [
      ["demo", "cron-worker"],
      ["demo", "cron-worker"],
    ]);
    assert.deepEqual(events.map((e) => ({
      event: e.payload.event,
      phase: e.payload.phase,
      worker_id: e.payload.worker_id,
      request_id: e.payload.request_id,
      scheduled_time: e.payload.scheduled_time,
      cron: e.payload.cron,
      outcome: e.payload.outcome,
    })), [
      {
        event: "worker_scheduled",
        phase: "start",
        worker_id: "demo:cron-worker:v1",
        request_id: "rid-scheduled",
        scheduled_time: 123,
        cron: "* * * * *",
        outcome: undefined,
      },
      {
        event: "worker_scheduled",
        phase: "finish",
        worker_id: "demo:cron-worker:v1",
        request_id: "rid-scheduled",
        scheduled_time: 123,
        cron: "* * * * *",
        outcome: "ok",
      },
    ]);
    assert.equal(typeof events[0].payload.ts, "number");
    assert.equal(typeof events[1].payload.duration_ms, "number");
  } finally {
    fetchSpy.restore();
  }
});

test("handleScheduledDispatch checks active subscriptions before appending tail events", async () => {
  const fetchSpy = installTailFetchSpy([]);
  const ctx = makeCtx();
  const scope = makeScope();

  try {
    const res = await handleScheduledDispatch({
      request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "cold-cron-worker",
        workerId: "demo:cold-cron-worker:v1",
        requestId: "rid-cold-scheduled",
      },
      stub: makeStub({
        async scheduled() {
          return { outcome: "ok" };
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 200);
    assert.equal(
      fetchSpy.calls.filter((c) => c.url === "http://proxy/logs/tail/append").length,
      0,
      "inactive worker must not pay append POSTs"
    );
  } finally {
    fetchSpy.restore();
  }
});

test("handleFetchDispatch emits start and finish tail events with status", async () => {
  const fetchSpy = installTailFetchSpy(["demo:fetch-worker"]);
  const ctx = makeCtx();
  const scope = makeScope();

  try {
    const res = await handleFetchDispatch({
      request: new Request("http://runtime.test/app", { method: "POST" }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "fetch-worker",
        workerId: "demo:fetch-worker:v1",
        requestId: "rid-fetch",
      },
      stub: makeStub({
        async fetch() {
          return new Response("created", { status: 201 });
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 201);
    const events = tailAppendPayloads(fetchSpy.calls).map((e) => e.payload);
    assert.deepEqual(events.map((e) => ({
      event: e.event,
      phase: e.phase,
      method: e.method,
      path: e.path,
      worker_id: e.worker_id,
      request_id: e.request_id,
      status: e.status,
      outcome: e.outcome,
    })), [
      {
        event: "worker_fetch",
        phase: "start",
        method: "POST",
        path: "/app",
        worker_id: "demo:fetch-worker:v1",
        request_id: "rid-fetch",
        status: undefined,
        outcome: undefined,
      },
      {
        event: "worker_fetch",
        phase: "finish",
        method: "POST",
        path: "/app",
        worker_id: "demo:fetch-worker:v1",
        request_id: "rid-fetch",
        status: 201,
        outcome: "ok",
      },
    ]);
    assert.equal(typeof events[0].ts, "number");
    assert.equal(typeof events[1].duration_ms, "number");
  } finally {
    fetchSpy.restore();
  }
});

test("handleFetchDispatch truncates tail path fields before append", async () => {
  const fetchSpy = installTailFetchSpy(["demo:fetch-worker"]);
  const ctx = makeCtx();
  const scope = makeScope();
  const longPath = `/${"x".repeat(1200)}`;

  try {
    const res = await handleFetchDispatch({
      request: new Request(`http://runtime.test${longPath}`, { method: "GET" }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "fetch-worker",
        workerId: "demo:fetch-worker:v1",
        requestId: "rid-fetch",
      },
      stub: makeStub({
        async fetch() {
          return new Response("ok");
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 200);
    const events = tailAppendPayloads(fetchSpy.calls).map((e) => e.payload);
    const firstEvent = events[0];
    const secondEvent = events[1];
    assert.ok(firstEvent, "expected first tail warning event");
    assert.ok(secondEvent, "expected second tail warning event");
    const firstPath = firstEvent.path;
    const secondPath = secondEvent.path;
    if (typeof firstPath !== "string") assert.fail("expected first tail warning path");
    if (typeof secondPath !== "string") assert.fail("expected second tail warning path");
    assert.equal(firstPath.length, 1024);
    assert.equal(firstEvent.path_truncated, true);
    assert.equal(secondPath.length, 1024);
    assert.equal(secondEvent.path_truncated, true);
  } finally {
    fetchSpy.restore();
  }
});

test("handleFetchDispatch preserves falsey thrown values in the error finish tail event", async () => {
  const fetchSpy = installTailFetchSpy(["demo:fetch-error-worker"]);
  const ctx = makeCtx();
  const scope = makeScope();
  const err = 0;

  try {
    const res = await handleFetchDispatch({
      request: new Request("http://runtime.test/app", { method: "GET" }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "fetch-error-worker",
        workerId: "demo:fetch-error-worker:v1",
        requestId: "rid-fetch-error",
      },
      stub: makeStub({
        async fetch() {
          throw err;
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 502);
    assert.deepEqual(scope.errors, [err]);
    const events = tailAppendPayloads(fetchSpy.calls).map((e) => e.payload);
    assert.equal(events.length, 2);
    assert.equal(events[1].event, "worker_fetch");
    assert.equal(events[1].phase, "finish");
    assert.equal(events[1].path, "/app");
    assert.equal(events[1].outcome, "error");
    assert.equal(events[1].error, "0");
  } finally {
    fetchSpy.restore();
  }
});

test("handleScheduledDispatch records handler errors without turning them into HTTP 5xx", async () => {
  const scope = makeScope();
  const err = new Error("scheduled failed");
  const res = await handleScheduledDispatch({
    request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
    scope,
    stub: makeStub({
      async scheduled() {
        throw err;
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual(scope.errors, [err]);
  assert.equal(body.outcome, "error");
  assert.equal(body.error, "scheduled failed");
});

test("handleScheduledDispatch preserves falsey thrown values", async () => {
  for (const thrown of [0, false]) {
    const scope = makeScope();
    const res = await handleScheduledDispatch({
      request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
      scope,
      stub: makeStub({
        async scheduled() {
          throw thrown;
        },
      }),
    });

    const body = await readJsonResponse(res, 200, String(thrown));
    assert.deepEqual(scope.errors, [thrown]);
    assert.equal(body.outcome, "error");
    assert.equal(body.error, String(thrown));
  }
});

test("handleScheduledDispatch maps service_binding_extra_handlers exception outcome", async () => {
  const scope = makeScope();
  const res = await handleScheduledDispatch({
    request: jsonRequest({ scheduledTime: 123, cron: "* * * * *" }),
    scope,
    stub: makeStub({
      async scheduled() {
        return { outcome: "exception", error: "thrown by handler" };
      },
    }),
  });

  const body = await readJsonResponse(res, 200);
  assert.deepEqual(scope.errors, ["thrown by handler"]);
  assert.equal(body.outcome, "error");
  assert.equal(body.error, "thrown by handler");
});

test("handleQueuedDispatch decodes queue messages before JSRPC dispatch", async () => {
  /** @type {any[]} */
  const calls = [];
  const scope = makeScope();
  const res = await handleQueuedDispatch({
    request: jsonRequest({
      queue: "jobs",
      messages: [{
        id: "m1",
        first_seen_ms: "123",
        attempts: "2",
        body_b64: btoa(JSON.stringify({ ok: true })),
        content_type: "json",
      }],
    }),
    scope,
    stub: makeStub({
      async queue(/** @type {string} */ queueName, /** @type {any[]} */ messages) {
        calls.push({ queueName, messages });
        return {
          ackAll: false,
          explicitAcks: ["m1"],
          retryMessages: [],
          retryBatch: { retry: false },
          extra: "not part of QueueResponse",
        };
      },
    }),
  });

  const responseBody = await readJsonResponse(res, 200);
  assert.equal(responseBody.outcome, "ok");
  assert.deepEqual(responseBody.result, {
    ackAll: false,
    explicitAcks: ["m1"],
    retryMessages: [],
    retryBatch: { retry: false },
  });
  assert.equal(calls[0].queueName, "jobs");
  assert.deepEqual(calls[0].messages[0].body, { ok: true });
  assert.equal(calls[0].messages[0].timestamp.getTime(), 123);
  assert.equal(calls[0].messages[0].attempts, 3);
});

test("handleQueuedDispatch records service_binding_extra_handlers exception outcome", async () => {
  const scope = makeScope();
  const response = await handleQueuedDispatch({
    request: jsonRequest({
      queue: "jobs",
      messages: [{
        id: "m1",
        first_seen_ms: "123",
        attempts: "0",
        body_b64: btoa("payload"),
        content_type: "text",
      }],
    }),
    scope,
    stub: makeStub({
      async queue() {
        return {
          outcome: "exception",
          ackAll: false,
          explicitAcks: [],
          retryMessages: [],
          retryBatch: { retry: false },
        };
      },
    }),
  });

  const body = await readJsonResponse(response, 200);
  assert.equal(body.outcome, "ok");
  assert.equal(body.result.outcome, "exception");
  assert.deepEqual(scope.errors, ["queue handler threw"]);
});

test("handleQueuedDispatch preserves falsey thrown values", async () => {
  for (const thrown of [0, false]) {
    const scope = makeScope();
    const res = await handleQueuedDispatch({
      request: jsonRequest({
        queue: "jobs",
        messages: [{
          id: "m1",
          first_seen_ms: "123",
          attempts: "0",
          body_b64: btoa("payload"),
          content_type: "text",
        }],
      }),
      scope,
      stub: makeStub({
        async queue() {
          throw thrown;
        },
      }),
    });

    const body = await readJsonResponse(res, 200, String(thrown));
    assert.deepEqual(scope.errors, [thrown]);
    assert.equal(body.outcome, "error");
    assert.equal(body.error, String(thrown));
  }
});

test("handleQueuedDispatch maps invalid base64 to a permanent decode failure", async () => {
  let dispatchCalls = 0;
  const scope = makeScope();
  const res = await handleQueuedDispatch({
    request: jsonRequest({
      queue: "jobs",
      messages: [{
        id: "m1",
        first_seen_ms: "123",
        attempts: "0",
        body_b64: "%%%",
        content_type: "bytes",
      }],
    }),
    scope,
    stub: makeStub({
      async queue() {
        dispatchCalls += 1;
        return { ackAll: true };
      },
    }),
  });

  const body = await readJsonResponse(res, 400);
  assert.equal(body.error, "queue_message_decode_failed");
  assert.match(body.message, /Invalid base64 input/);
  assert.equal(dispatchCalls, 0);
});

test("handleQueuedDispatch accepts legal queue batches above the small dispatch cap", async () => {
  /** @type {any[]} */
  const calls = [];
  const scope = makeScope();
  const body = "x".repeat(128_000);
  const bodyB64 = Buffer.from(body).toString("base64");

  const res = await handleQueuedDispatch({
    request: jsonRequest({
      queue: "jobs",
      messages: [
        {
          id: "m1",
          first_seen_ms: "123",
          attempts: "0",
          body_b64: bodyB64,
          content_type: "text",
        },
        {
          id: "m2",
          first_seen_ms: "124",
          attempts: "0",
          body_b64: bodyB64,
          content_type: "text",
        },
      ],
    }),
    scope,
    stub: makeStub({
      async queue(/** @type {string} */ queueName, /** @type {any[]} */ messages) {
        calls.push({ queueName, messages });
        return { acked: messages.length };
      },
    }),
  });

  const responseBody = await readJsonResponse(res, 200);
  assert.equal(responseBody.outcome, "ok");
  assert.equal(calls[0].messages.length, 2);
  assert.equal(calls[0].messages[0].body.length, 128_000);
});

test("handleQueuedDispatch emits start and finish tail events with queue batch metadata", async () => {
  const fetchSpy = installTailFetchSpy(["demo:queue-worker"]);
  const ctx = makeCtx();
  const scope = makeScope();

  try {
    const res = await handleQueuedDispatch({
      request: jsonRequest({
        queue: "jobs",
        messages: [{
          id: "m1",
          first_seen_ms: "123",
          attempts: "0",
          body_b64: btoa("hello"),
          content_type: "text",
        }],
      }),
      scope,
      env: { REDIS_PROXY_URL: "http://proxy/", WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token" },
      ctx,
      identity: {
        namespace: "demo",
        workerName: "queue-worker",
        workerId: "demo:queue-worker:v2",
        requestId: "rid-queue",
      },
      stub: makeStub({
        async queue() {
          return { ackAll: true };
        },
      }),
    });
    await Promise.all(ctx.tasks);

    assert.equal(res.status, 200);
    const events = tailAppendPayloads(fetchSpy.calls).map((e) => e.payload);
    assert.deepEqual(events.map((e) => ({
      event: e.event,
      phase: e.phase,
      worker_id: e.worker_id,
      request_id: e.request_id,
      queue: e.queue,
      batch_size: e.batch_size,
      outcome: e.outcome,
    })), [
      {
        event: "worker_queue",
        phase: "start",
        worker_id: "demo:queue-worker:v2",
        request_id: "rid-queue",
        queue: "jobs",
        batch_size: 1,
        outcome: undefined,
      },
      {
        event: "worker_queue",
        phase: "finish",
        worker_id: "demo:queue-worker:v2",
        request_id: "rid-queue",
        queue: "jobs",
        batch_size: 1,
        outcome: "ok",
      },
    ]);
    assert.equal(typeof events[0].ts, "number");
    assert.equal(typeof events[1].duration_ms, "number");
  } finally {
    fetchSpy.restore();
  }
});

test("handleQueuedDispatch rejects invalid JSON before handler dispatch", async () => {
  const scope = makeScope();
  const res = await handleQueuedDispatch({
    request: new Request("http://runtime.test/_queued", {
      method: "POST",
      body: "{",
    }),
    scope,
    stub: makeStub({
      async queue() {
        throw new Error("must not dispatch");
      },
    }),
  });

  await assertJsonResponse(res, 400, {
    error: "invalid_json",
    message: "Body must be valid JSON",
  });
  assert.deepEqual(scope.errors, []);
});
