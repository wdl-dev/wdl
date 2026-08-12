import { test } from "node:test";
import assert from "node:assert/strict";
import { Ai } from "../../runtime/ai-client.js";

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
  assert.equal(calls.filter((request) => request.url.endsWith("/v1/models")).length, 2);
  const finalCall = calls.at(-1);
  assert.ok(finalCall);
  assert.equal(finalCall.headers.get("accept"), "text/event-stream");
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
    () => ai.run("openai/primary", { input: "hello", stream: true }),
    /does not support sse/
  );
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
  await assert.rejects(
    () => ai.run("openai/primary", { input: "not allowed" }, { websocket: true }),
    /inputs to be null/
  );
});
