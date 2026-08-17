import { test } from "node:test";
import assert from "node:assert/strict";
import { Ai } from "../../runtime/ai-client.js";
import { withMockedProperty } from "../helpers/mock-global.js";

function responseModel(transports = ["http", "sse", "responses_websocket"]) {
  return {
    id: "openai/primary",
    protocol: "responses",
    transports,
    inputModalities: ["text"],
    outputModalities: ["text"],
    capabilities: {},
  };
}

/**
 * @param {unknown} error
 * @param {{ status: number, code: string, message?: string }} expected
 */
function matchesAiError(error, expected) {
  if (!(error instanceof Error)) return false;
  const details = /** @type {Error & { status?: unknown, code?: unknown }} */ (error);
  return details.name === "AIError" &&
    details.status === expected.status &&
    details.code === expected.code &&
    (expected.message === undefined || details.message === expected.message);
}

test("AI tenant facade exposes raw fetch and bounded model discovery", async () => {
  /** @type {Request[]} */
  const requests = [];
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      requests.push(request);
      return Response.json({ models: [responseModel()] });
    },
  }, { requestId: "rid-ai-client" });
  const firstModels = await ai.models();
  assert.deepEqual(firstModels, [responseModel()]);
  firstModels[0].id = "tenant-mutated";
  assert.deepEqual(await ai.models(), [responseModel()]);
  assert.equal(requests[0].url, "https://ai.wdl/v1/models");
  assert.equal(requests[0].method, "GET");

  const raw = await ai.fetch("https://ai.wdl/v1/models", {
    headers: { "x-request-id": "tenant-forged" },
  });
  assert.equal(raw.status, 200);
  assert.equal(requests.length, 2);
  assert.deepEqual(requests.map((request) => request.headers.get("x-request-id")), [
    "rid-ai-client",
    "rid-ai-client",
  ]);
});

test("AI tenant run selects JSON and SSE transport from the model contract", async () => {
  /** @type {Request[]} */
  const calls = [];
  const streamBytes = new TextEncoder().encode("event: response.completed\ndata: {}\n\n");
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      calls.push(request);
      if (request.url.endsWith("/v1/models")) return Response.json({ models: [responseModel()] });
      const body = await request.json();
      if (body.stream === true) {
        return new Response(streamBytes, { headers: { "content-type": "text/event-stream" } });
      }
      return Response.json({ id: "resp_test", model: body.model, input: body.input });
    },
  });

  assert.deepEqual(await ai.run("openai/primary", { input: "hello" }), {
    id: "resp_test",
    model: "openai/primary",
    input: "hello",
  });
  const stream = await ai.run("openai/primary", { input: "hello", stream: true });
  assert.equal(new TextDecoder().decode(await new Response(stream).arrayBuffer()), new TextDecoder().decode(streamBytes));
  assert.equal(calls.filter((request) => request.url.endsWith("/v1/models")).length, 1);
  const finalCall = calls.at(-1);
  assert.ok(finalCall);
  assert.equal(finalCall.headers.get("accept"), "text/event-stream");
});

test("AI tenant facades share one successful catalog snapshot per loaded worker scope", async () => {
  let modelReads = 0;
  let inferenceCalls = 0;
  const fetcher = {
    /** @param {Request} request */
    async fetch(request) {
      assert.ok(request instanceof Request);
      if (request.url.endsWith("/v1/models")) {
        modelReads += 1;
        if (modelReads === 1) {
          return Response.json({ error: "temporary" }, { status: 503 });
        }
        return Response.json({ models: [responseModel()] });
      }
      inferenceCalls += 1;
      return Response.json({ id: `resp_${inferenceCalls}` });
    },
  };
  const workerScope = {};
  const firstInvocation = new Ai(fetcher, {}, workerScope);
  const secondInvocation = new Ai(fetcher, {}, workerScope);

  await assert.rejects(
    () => firstInvocation.models(),
    (error) => matchesAiError(error, { status: 503, code: "temporary" })
  );
  assert.deepEqual(await firstInvocation.models(), [responseModel()]);
  assert.deepEqual(
    await secondInvocation.run("openai/primary", { input: "hello" }),
    { id: "resp_1" }
  );
  assert.equal(modelReads, 2);
  assert.equal(inferenceCalls, 1);
});

test("AI tenant catalog keeps the first successful concurrent snapshot", async () => {
  const firstRead = Promise.withResolvers();
  const secondRead = Promise.withResolvers();
  let modelReads = 0;
  const fetcher = {
    /** @param {Request} request */
    async fetch(request) {
      assert.ok(request instanceof Request);
      assert.ok(request.url.endsWith("/v1/models"));
      modelReads += 1;
      return await (modelReads === 1 ? firstRead.promise : secondRead.promise);
    },
  };
  const workerScope = {};
  const firstInvocation = new Ai(fetcher, {}, workerScope);
  const secondInvocation = new Ai(fetcher, {}, workerScope);
  const firstModels = firstInvocation.models();
  const secondModels = secondInvocation.models();
  assert.equal(modelReads, 2);

  const winningModel = { ...responseModel(), id: "openai/winner" };
  secondRead.resolve(Response.json({ models: [winningModel] }));
  assert.deepEqual(await secondModels, [winningModel]);
  firstRead.resolve(Response.json({
    models: [{ ...responseModel(), id: "openai/late" }],
  }));
  assert.deepEqual(await firstModels, [winningModel]);
  assert.deepEqual(await firstInvocation.models(), [winningModel]);
  assert.equal(modelReads, 2);
});

test("AI tenant cancellation wins over a concurrent catalog snapshot", async () => {
  const winningRead = Promise.withResolvers();
  const cancelledRead = Promise.withResolvers();
  let modelReads = 0;
  let inferenceCalls = 0;
  const fetcher = {
    /** @param {Request} request */
    async fetch(request) {
      assert.ok(request instanceof Request);
      if (request.url.endsWith("/v1/models")) {
        modelReads += 1;
        return await (modelReads === 1 ? winningRead.promise : cancelledRead.promise);
      }
      inferenceCalls += 1;
      return Response.json({ id: "must-not-run" });
    },
  };
  const workerScope = {};
  const winner = new Ai(fetcher, {}, workerScope).models();
  const controller = new AbortController();
  const cancelledAi = new Ai(fetcher, {}, workerScope);
  const cancelled = cancelledAi.run(
    "openai/primary",
    { input: "cancelled" },
    { signal: controller.signal }
  );
  const cancelledAssertion = assert.rejects(
    cancelled,
    (error) => error instanceof DOMException && error.name === "AbortError"
  );
  assert.equal(modelReads, 2);

  controller.abort();
  winningRead.resolve(Response.json({ models: [responseModel()] }));
  assert.deepEqual(await winner, [responseModel()]);
  cancelledRead.reject(controller.signal.reason);
  await cancelledAssertion;
  assert.equal(inferenceCalls, 0);

  const alreadyCancelled = new AbortController();
  alreadyCancelled.abort();
  await withMockedProperty(
    AbortSignal.prototype,
    "throwIfAborted",
    () => {},
    async () => {
      await assert.rejects(
        () => cancelledAi.run(
          "openai/primary",
          { input: "still cancelled" },
          { signal: alreadyCancelled.signal }
        ),
        (error) => error instanceof DOMException && error.name === "AbortError"
      );
    }
  );
  assert.equal(inferenceCalls, 0);
});

test("AI tenant cancellation wins after a cached catalog lookup", async () => {
  let modelReads = 0;
  let inferenceCalls = 0;
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      if (request.url.endsWith("/v1/models")) {
        modelReads += 1;
        return Response.json({ models: [responseModel()] });
      }
      inferenceCalls += 1;
      return Response.json({ id: "must-not-run" });
    },
  });
  await ai.models();

  const controller = new AbortController();
  const cancelled = ai.run(
    "openai/missing",
    { input: "cancelled" },
    { signal: controller.signal }
  );
  controller.abort();

  await assert.rejects(cancelled, (error) => error === controller.signal.reason);
  assert.equal(modelReads, 1);
  assert.equal(inferenceCalls, 0);
});

test("AI tenant collection intrinsic changes cannot observe the private catalog", async () => {
  const originalIsArray = Array.isArray;
  let observedCatalog = false;
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      return Response.json({ models: [responseModel()] });
    },
  });

  const models = await withMockedProperty(Array, "isArray", (value) => {
    const array = originalIsArray(value);
    if (
      array &&
      value.length === 1 &&
      value[0]?.id === "openai/primary"
    ) {
      observedCatalog = true;
      value[0].id = "tenant-mutated";
    }
    return array;
  }, async () => await ai.models());

  assert.equal(observedCatalog, false);
  assert.deepEqual(models, [responseModel()]);
  assert.deepEqual(await ai.models(), [responseModel()]);
});

test("AI tenant run classifies a malformed successful response as a gateway error", async () => {
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      if (request.url.endsWith("/v1/models")) {
        return Response.json({ models: [responseModel()] });
      }
      return new Response("not json", { status: 200, headers: { "content-type": "text/plain" } });
    },
  });

  await assert.rejects(
    () => ai.run("openai/primary", { input: "hello" }),
    (error) => matchesAiError(error, { status: 502, code: "ai_request_failed" })
  );
});

test("AI tenant run preserves OpenAI-compatible provider error details", async () => {
  /** @type {Array<{
   *   providerError: { message: string, type: string, code?: string },
   *   expectedCode: string,
   * }>} */
  const scenarios = [
    {
      providerError: {
        message: "rate limited by provider",
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
      },
      expectedCode: "rate_limit_exceeded",
    },
    {
      providerError: { message: "provider authentication failed", type: "authentication_error" },
      expectedCode: "authentication_error",
    },
  ];
  for (const { providerError, expectedCode } of scenarios) {
    const ai = new Ai({
      async fetch(request) {
        assert.ok(request instanceof Request);
        if (request.url.endsWith("/v1/models")) {
          return Response.json({ models: [responseModel()] });
        }
        return Response.json({ error: providerError }, { status: 429 });
      },
    });

    await assert.rejects(
      () => ai.run("openai/primary", { input: "hello" }),
      (error) => matchesAiError(error, {
        status: 429,
        code: expectedCode,
        message: providerError.message,
      })
    );
  }
});

test("AI tenant run rejects unsupported options and unavailable transports before inference", async () => {
  let calls = 0;
  const ai = new Ai({
    async fetch() {
      calls += 1;
      return Response.json({ models: [responseModel(["http"])] });
    },
  });
  await assert.rejects(
    () => ai.run("openai/primary", { input: "hello" }, { returnRawResponse: true }),
    /use env\.AI\.fetch\(\)/
  );
  assert.equal(calls, 0);
  await assert.rejects(
    () => ai.run("openai/primary", null),
    /inputs must be an object/
  );
  await assert.rejects(
    () => ai.run("openai/primary", { input: "hello", stream: "yes" }),
    /inputs\.stream must be boolean/
  );
  await assert.rejects(
    () => ai.run("openai/primary", { input: "not allowed" }, { websocket: true }),
    /inputs to be null/
  );
  assert.equal(calls, 0);
  await assert.rejects(
    () => ai.run("openai/primary", { input: "hello", stream: true }),
    /does not support sse/
  );
});

test("AI tenant run rejects malformed model references before host I/O", async () => {
  let calls = 0;
  const ai = new Ai({
    async fetch() {
      calls += 1;
      throw new Error("unexpected host I/O");
    },
  });

  for (const model of ["", "bad", "OpenAI/primary", "openai/-bad", "openai/2", "openai/a/b"]) {
    await assert.rejects(
      () => ai.run(model, { input: "hello" }),
      (error) => matchesAiError(error, {
        status: 400,
        code: "ai_invalid_model",
        message: "AI model must be <provider>/<alias>",
      })
    );
  }
  assert.equal(calls, 0);
});

test("AI tenant model grammar ignores replaced RegExp intrinsics", async () => {
  let calls = 0;
  const ai = new Ai({
    async fetch() {
      calls += 1;
      return Response.json({ models: [responseModel()] });
    },
  });

  const forgedMatch = /** @type {RegExpExecArray} */ (
    Object.assign(["bad"], { index: 0, input: "bad" })
  );
  await withMockedProperty(RegExp.prototype, "exec", () => forgedMatch, async () => {
    await assert.rejects(
      () => ai.run("bad", { input: "hello" }),
      (error) => matchesAiError(error, {
        status: 400,
        code: "ai_invalid_model",
      })
    );
  });
  assert.equal(calls, 0);
});

test("AI tenant websocket run returns the native websocket response", async () => {
  const socket = {};
  const ai = new Ai({
    async fetch(request) {
      assert.ok(request instanceof Request);
      if (request.url.endsWith("/v1/models")) return Response.json({ models: [responseModel()] });
      assert.equal(request.method, "GET");
      assert.equal(new URL(request.url).searchParams.get("model"), "openai/primary");
      return /** @type {Response} */ (/** @type {unknown} */ ({ status: 101, webSocket: socket }));
    },
  });
  const response = await ai.run("openai/primary", null, { websocket: true });
  assert.equal(response.webSocket, socket);
});
