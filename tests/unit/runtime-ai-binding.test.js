import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  AI_HOST_TEST_STATE,
  AI_REQUEST_MAX_BYTES,
  AI_RESPONSE_MAX_BYTES,
  AI_STREAM_FRAME_MAX_BYTES,
  AI_STREAM_MAX_BYTES,
  AI_WS_FRAME_MAX_BYTES,
  AiBinding,
  aiPoolStateForTest,
  makeAiBinding,
  modelList,
  openAiResolution,
  resetAiHostTestState,
} from "../helpers/load-runtime-ai-binding.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { delay } from "../helpers/timing.js";

beforeEach(resetAiHostTestState);

/** @param {Record<string, unknown>} body @param {string} [path] */
function request(body, path = "/v1/responses") {
  return new Request(`https://ai.wdl${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "tenant-secret" },
    body: JSON.stringify(body),
  });
}

/** @param {ReturnType<typeof openAiResolution>} [resolution] @returns {typeof fetch} */
function resolver(resolution = openAiResolution()) {
  return async (input, init = {}) => {
    const url = String(input);
    const headers = new Headers(init.headers);
    assert.equal(headers.get("x-wdl-internal-auth"), "test-internal-auth-token");
    assert.equal(headers.get("x-request-id"), "rid-ai-test");
    if (url.endsWith("/ai/models")) return Response.json(modelList(resolution));
    if (url.endsWith("/ai/resolve")) return Response.json(resolution);
    throw new Error(`unexpected resolver URL ${url}`);
  };
}

test("AI host RPC surface exposes only fetch", () => {
  assert.deepEqual(Object.getOwnPropertyNames(AiBinding.prototype).toSorted(), ["constructor", "fetch"]);
});

test("AI host resolves trusted identity and attaches only the platform credential", async () => {
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const providerCalls = [];
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerCalls.push({ url: String(url), init });
        return Response.json({
          id: "resp_test",
          model: parseJsonObjectRequestBody(init, "provider request").model,
        }, {
          headers: {
            "openai-request-id": "provider-rid",
            "x-request-id": "provider-generic-rid",
            authorization: "must-not-forward",
          },
        });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      input: [
        { type: "input_text", text: "describe this image" },
        { type: "input_image", image_url: "https://images.example/input.png" },
      ],
      tools: [{ type: "function", name: "lookup", parameters: { type: "object" } }],
      reasoning: { effort: "low" },
      text: {
        format: {
          type: "json_schema",
          name: "answer",
          schema: { type: "object", properties: { answer: { type: "string" } } },
          strict: true,
        },
      },
    }));
    assert.deepEqual(
      await readJsonResponse(response, 200, "AI host response"),
      { id: "resp_test", model: "gpt-test" }
    );
    assert.equal(response.headers.get("openai-request-id"), "provider-rid");
    assert.equal(response.headers.get("x-ai-provider-request-id"), "provider-generic-rid");
    assert.equal(response.headers.get("x-request-id"), "rid-ai-test");
    assert.equal(response.headers.get("authorization"), null);
  });
  const providerCall = providerCalls[0];
  assert.ok(providerCall);
  assert.equal(providerCall.url, "https://api.openai.com/v1/responses");
  const providerHeaders = new Headers(providerCall.init.headers);
  assert.equal(providerHeaders.get("authorization"), "Bearer fake-openai-key");
  assert.equal(providerHeaders.get("x-wdl-internal-auth"), null);
  const sentBody = providerCall.init.body;
  assert.equal(typeof sentBody, "string");
  assert.deepEqual(JSON.parse(/** @type {string} */ (sentBody)), {
    model: "gpt-test",
    input: [
      { type: "input_text", text: "describe this image" },
      { type: "input_image", image_url: "https://images.example/input.png" },
    ],
    tools: [{ type: "function", name: "lookup", parameters: { type: "object" } }],
    reasoning: { effort: "low" },
    text: {
      format: {
        type: "json_schema",
        name: "answer",
        schema: { type: "object", properties: { answer: { type: "string" } } },
        strict: true,
      },
    },
  });
  assert.deepEqual(aiPoolStateForTest().request, { inUse: 0, highWater: 1 });
});

test("AI host inspects only user content when enforcing input modalities", async () => {
  /** @type {Record<string, unknown>[]} */
  const providerBodies = [];
  const textOnly = openAiResolution({ inputModalities: ["text"] });
  const imageOnly = openAiResolution({ inputModalities: ["image"] });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerBodies.push(parseJsonObjectRequestBody(init, "provider request"));
        return Response.json({ id: "resp_test" });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(textOnly), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      input: "hello",
      tools: [{
        type: "function",
        name: "inspect",
        parameters: {
          type: "object",
          properties: { image_url: { type: "string" } },
        },
      }],
    }));
    assert.equal(response.status, 200);
  });
  await withMockedProperty(globalThis, "fetch", resolver(imageOnly), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      input: [{ type: "input_image", image_url: "https://images.example/input.png" }],
    }));
    assert.equal(response.status, 200);
  });
  assert.equal(providerBodies.length, 2);
});

test("AI host model list strips resolver-only identity and credential fields", async () => {
  const { binding } = makeAiBinding();
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(new Request("https://ai.wdl/v1/models"));
    const payload = await readJsonResponse(response, 200, "AI model list");
    assert.deepEqual(Object.keys(payload.models[0]).toSorted(), [
      "capabilities", "id", "inputModalities", "outputModalities", "protocol", "transports",
    ]);
    assert.doesNotMatch(JSON.stringify(payload), /credential|revision|upstreamModel|gpt-test/);
  });
});

test("AI host rejects unsupported DeepSeek and background inputs before provider I/O", async () => {
  let providerCalls = 0;
  const resolution = openAiResolution({
    provider: "deepseek",
    alias: "flash",
    kind: "deepseek",
    upstreamModel: "deepseek-v4-flash",
    destination: "https://api.deepseek.com/responses",
    credential: "fake-deepseek-key",
    inputModalities: ["text"],
    capabilities: { ...openAiResolution().capabilities, previousResponseId: false },
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: { async fetch() { providerCalls += 1; return Response.json({}); } },
  });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    /** @type {Array<[Record<string, unknown>, string]>} */
    const cases = [
      [{ input: "hello", previous_response_id: "resp_old" }, "ai_continuation_unsupported"],
      [{ input: "hello", store: true }, "ai_store_unsupported"],
      [{ input: [{ type: "input_image", image_url: "https://images.example/input.png" }] }, "ai_input_modality_unsupported"],
      [{ input: "hello", background: true }, "ai_background_unsupported"],
    ];
    for (const [body, error] of cases) {
      const response = await binding.fetch(request({ model: "deepseek/flash", ...body }));
      assert.equal((await readJsonResponse(response, 400, error)).error, error);
    }
  });
  assert.equal(providerCalls, 0);
});

test("AI host reports an oversized tenant request as 413 before resolution", async () => {
  const { binding } = makeAiBinding();
  const response = await binding.fetch(new Request("https://ai.wdl/v1/responses", {
    method: "POST",
    headers: {
      "content-length": String(AI_REQUEST_MAX_BYTES + 1),
      "content-type": "application/json",
    },
    body: "{}",
  }));
  assert.equal(
    (await readJsonResponse(response, 413, "oversized AI request")).error,
    "ai_request_too_large"
  );
});

test("AI host replaces tenant headers with the official provider allowlist", async () => {
  /** @type {Headers[]} */
  const providerCalls = [];
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerCalls.push(new Headers(init.headers));
        return Response.json({ id: "resp_test" });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(new Request("https://ai.wdl/v1/responses", {
      method: "POST",
      headers: {
        authorization: "Bearer tenant-secret",
        cookie: "tenant-cookie=secret",
        "content-type": "application/json",
        "openai-organization": "tenant-org",
        "sec-websocket-protocol": "tenant-protocol",
        "x-api-key": "tenant-api-key",
        "x-wdl-internal-auth": "tenant-internal-token",
      },
      body: JSON.stringify({ model: "openai/primary", input: "hello" }),
    }));
    assert.equal(response.status, 200);
  });
  const providerHeaders = providerCalls[0];
  assert.ok(providerHeaders);
  assert.deepEqual([...providerHeaders.keys()].toSorted(), [
    "accept",
    "authorization",
    "content-type",
  ]);
  assert.equal(providerHeaders.get("authorization"), "Bearer fake-openai-key");
});

test("AI host preserves semantic SSE bytes and releases the stream permit at terminal event", async () => {
  const frames = [
    ": keepalive\r\n\r\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\"}}\n\n",
  ];
  const expected = frames.join("");
  /** @type {AbortSignal | null} */
  let providerSignal = null;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(_url, init = {}) {
        providerSignal = init.signal ?? null;
        const bytes = new TextEncoder().encode(expected);
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(bytes.slice(0, 17));
            controller.enqueue(bytes.slice(17));
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    assert.equal(await response.text(), expected);
  });
  assert.deepEqual(aiPoolStateForTest().stream, { inUse: 0, highWater: 1 });
  assert.equal(providerSignal?.aborted, true);
});

test("AI host forwards a near-limit SSE frame assembled from small provider chunks", async () => {
  const prefix = "event: response.completed\r\ndata: {\"type\":\"response.completed\",\"padding\":\"";
  const suffix = "\"}\r\n\r\n";
  const frame = `${prefix}${"x".repeat(AI_STREAM_FRAME_MAX_BYTES - prefix.length - suffix.length)}${suffix}`;
  const bytes = new TextEncoder().encode(frame);
  assert.equal(bytes.byteLength, AI_STREAM_FRAME_MAX_BYTES);

  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        let offset = 0;
        return new Response(new ReadableStream({
          pull(controller) {
            if (offset >= bytes.byteLength) return;
            const end = Math.min(offset + 1021, bytes.byteLength);
            controller.enqueue(bytes.subarray(offset, end));
            offset = end;
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    assert.equal(await response.text(), frame);
  });
  assert.deepEqual(aiPoolStateForTest().stream, { inUse: 0, highWater: 1 });
});

test("AI host forwards a top-level SSE error and records a provider failure", async () => {
  const frame = "event: error\ndata: {\"type\":\"error\",\"message\":\"provider failed\"}\n\n";
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(frame, { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    assert.equal(await response.text(), frame);
  });
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
  assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "stream" &&
    entry.labels.outcome === "provider_error"));
});

test("AI host treats a Chat Completions error event as terminal", async () => {
  const frame = "event: error\ndata: {\"error\":{\"message\":\"provider failed\"}}\n\n";
  const resolution = openAiResolution({
    protocol: "chat_completions",
    destination: "https://api.openai.com/v1/chat/completions",
    transport: "sse",
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(frame, { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      messages: [{ role: "user", content: "hello" }],
      stream: true,
    }, "/v1/chat/completions"));
    assert.equal(await response.text(), frame);
  });
  assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "stream" &&
    entry.labels.outcome === "provider_error"));
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host distinguishes failed and incomplete Responses terminal events", async () => {
  const cases = [
    ["response.failed", "provider_failed"],
    ["response.incomplete", "provider_incomplete"],
  ];
  for (const [type] of cases) {
    const frame = `event: ${type}\ndata: ${JSON.stringify({ type })}\n\n`;
    const { binding } = makeAiBinding({
      AI_NETWORK: {
        async fetch() {
          return new Response(frame, { headers: { "content-type": "text/event-stream" } });
        },
      },
    });
    await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
      const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
      assert.equal(await response.text(), frame);
    });
  }
  for (const [, outcome] of cases) {
    assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
      entry.name === "ai_pool_events" &&
      entry.labels.pool === "stream" &&
      entry.labels.outcome === outcome));
  }
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host fails an incomplete Responses stream and releases its permit", async () => {
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response("event: response.created\ndata: {\"type\":\"response.created\"}\n\n", {
          headers: { "content-type": "text/event-stream" },
        });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    await assert.rejects(() => response.text(), /terminal event/);
  });
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host rejects a complete SSE frame above the per-frame limit", async () => {
  const frame = `data: ${"x".repeat(AI_STREAM_FRAME_MAX_BYTES)}\n\n`;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(frame, { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    await assert.rejects(() => response.text(), /stream frame exceeds/);
  });
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host bounds total SSE bytes independently of valid frame size", async () => {
  // A max-sized SSE comment is valid and non-terminal, isolating the aggregate cap.
  const frame = new Uint8Array(AI_STREAM_FRAME_MAX_BYTES);
  frame.fill(0x20);
  frame[0] = 0x3a;
  frame[frame.byteLength - 2] = 0x0a;
  frame[frame.byteLength - 1] = 0x0a;
  let frames = 0;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({
          pull(controller) {
            controller.enqueue(frame);
            frames += 1;
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({ transport: "sse" })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello", stream: true }));
    await assert.rejects(() => response.arrayBuffer(), new RegExp(`stream exceeds ${AI_STREAM_MAX_BYTES} bytes`));
  });
  assert.ok(frames > AI_STREAM_MAX_BYTES / AI_STREAM_FRAME_MAX_BYTES);
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host cancels an oversized non-streaming provider response", async () => {
  let cancelled = false;
  const providerBody = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(AI_RESPONSE_MAX_BYTES + 1));
    },
    cancel() { cancelled = true; },
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(providerBody, { headers: { "content-type": "application/json" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
    assert.equal(
      (await readJsonResponse(response, 502, "oversized AI provider response")).error,
      "ai_binding_error"
    );
  });
  assert.equal(cancelled, true);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI stream duration terminates the tenant stream when upstream ignores abort", async () => {
  const { binding, waitUntilTasks } = makeAiBinding({
    AI_STREAM_MAX_DURATION_MS: "10",
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({ pull() { return new Promise(() => {}); } }), {
          headers: { "content-type": "text/event-stream" },
        });
      },
    },
  });
  const resolution = openAiResolution({ transport: "sse" });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      input: "wait forever",
      stream: true,
    }));
    await assert.rejects(() => response.text(), /duration exceeded/);
  });
  await Promise.all(waitUntilTasks);
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI request pool saturates without a queue and recovers after completion", async () => {
  /** @type {(response: Response) => void} */
  let releaseResolver = () => {};
  let resolverCalls = 0;
  const pendingResolver = new Promise((resolve) => { releaseResolver = resolve; });
  const { binding } = makeAiBinding({
    AI_REQUEST_MAX_IN_FLIGHT: "1",
    AI_NETWORK: { async fetch() { return Response.json({ ok: true }); } },
  });
  await withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    if (resolverCalls === 1) return await pendingResolver;
    return Response.json(openAiResolution());
  }, async () => {
    const first = binding.fetch(request({ model: "openai/primary", input: "first" }));
    await delay(0);
    const saturated = await binding.fetch(request({ model: "openai/primary", input: "second" }));
    assert.equal(saturated.status, 429);
    releaseResolver(Response.json(openAiResolution()));
    assert.equal((await first).status, 200);
  });
  assert.equal(aiPoolStateForTest().request.inUse, 0);
  assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
    entry.name === "ai_pool_events" && entry.labels.outcome === "saturated"));
});

test("AI capacity rolls back when the host cannot register its watchdog", async () => {
  let resolverCalls = 0;
  const { binding } = makeAiBinding({}, {}, {
    waitUntil() { throw new Error("watchdog registration failed"); },
  });
  await withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    return Response.json(openAiResolution());
  }, async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
    assert.equal(
      (await readJsonResponse(response, 502, "AI watchdog setup failure")).error,
      "ai_binding_error"
    );
  });
  assert.equal(resolverCalls, 0);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
  assert.equal(AI_HOST_TEST_STATE.metrics.filter((entry) =>
    entry.name === "ai_pool_events" && entry.labels.outcome === "setup_error").length, 1);
});

test("AI stream pool saturates independently and recovers after cancellation", async () => {
  let resolverCalls = 0;
  let providerCalls = 0;
  /** @type {AbortSignal | null} */
  let providerSignal = null;
  const { binding } = makeAiBinding({
    AI_STREAM_MAX_IN_FLIGHT: "1",
    AI_NETWORK: {
      async fetch(_url, init = {}) {
        providerCalls += 1;
        providerSignal = init.signal ?? null;
        return new Response(new ReadableStream({ pull() { return new Promise(() => {}); } }), {
          headers: { "content-type": "text/event-stream" },
        });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", async (input, init) => {
    resolverCalls += 1;
    return await resolver(openAiResolution({ transport: "sse" }))(input, init);
  }, async () => {
    const first = await binding.fetch(request({ model: "openai/primary", input: "first", stream: true }));
    assert.equal(first.status, 200);
    assert.equal(aiPoolStateForTest().stream.inUse, 1);

    const saturated = await binding.fetch(request({ model: "openai/primary", input: "second", stream: true }));
    assert.equal(
      (await readJsonResponse(saturated, 429, "saturated AI stream pool")).error,
      "ai_capacity_exhausted"
    );
    assert.equal(resolverCalls, 1);
    assert.equal(providerCalls, 1);
    await first.body?.cancel("test complete");
  });
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
  assert.equal(providerSignal?.aborted, true);
  assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "stream" &&
    entry.labels.outcome === "saturated"));
});

test("AI request watchdog aborts a stuck resolver and releases capacity", async () => {
  const { binding, waitUntilTasks } = makeAiBinding({ AI_REQUEST_BUDGET_MS: "10" });
  await withMockedProperty(globalThis, "fetch", async (_input, init) => await new Promise((_, reject) => {
    const signal = init?.signal;
    assert.ok(signal);
    signal.addEventListener("abort", () => reject(signal.reason || new Error("aborted")), { once: true });
  }), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "stuck" }));
    assert.equal(response.status, 504);
  });
  await Promise.all(waitUntilTasks);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI request watchdog bounds a stalled request body before resolver I/O", async () => {
  let resolverCalls = 0;
  const { binding, waitUntilTasks } = makeAiBinding({ AI_REQUEST_BUDGET_MS: "10" });
  const body = new ReadableStream({ pull() { return new Promise(() => {}); } });
  const requestInit = /** @type {RequestInit & { duplex: "half" }} */ ({
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
    duplex: "half",
  });
  const pending = withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    return Response.json(openAiResolution());
  }, async () => await binding.fetch(new Request("https://ai.wdl/v1/responses", requestInit)));

  const response = await pending;
  assert.equal(
    (await readJsonResponse(response, 504, "stalled AI request body")).error,
    "ai_request_timeout"
  );
  assert.equal(resolverCalls, 0);
  await Promise.all(waitUntilTasks);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI streaming requests transfer one lease after their bounded body is read", async () => {
  /** @type {ReadableStreamDefaultController<Uint8Array> | null} */
  let bodyController = null;
  const body = new ReadableStream({ start(controller) { bodyController = controller; } });
  /** @type {ReadableStreamDefaultController<Uint8Array> | null} */
  let providerController = null;
  const providerBody = new ReadableStream({ start(controller) { providerController = controller; } });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(providerBody, {
          headers: { "content-type": "text/event-stream" },
        });
      },
    },
  });
  const resolution = openAiResolution({ transport: "sse" });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const requestInit = /** @type {RequestInit & { duplex: "half" }} */ ({
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      duplex: "half",
    });
    const pending = binding.fetch(new Request("https://ai.wdl/v1/responses", requestInit));
    await delay(0);
    assert.equal(aiPoolStateForTest().request.inUse, 1);
    assert.equal(aiPoolStateForTest().stream.inUse, 0);

    assert.ok(bodyController);
    bodyController.enqueue(new TextEncoder().encode(JSON.stringify({
      model: "openai/primary",
      input: "stream",
      stream: true,
    })));
    bodyController.close();
    const response = await pending;
    assert.equal(aiPoolStateForTest().request.inUse, 0);
    assert.equal(aiPoolStateForTest().stream.inUse, 1);
    assert.ok(providerController);
    providerController.enqueue(new TextEncoder().encode(
      "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
    ));
    providerController.close();
    assert.match(await response.text(), /response\.completed/);
  });
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI caller cancellation retains capacity until stuck provider I/O reaches its deadline", async () => {
  /** @type {() => void} */
  let providerStarted = () => {};
  const started = new Promise((resolve) => { providerStarted = () => resolve(undefined); });
  const { binding, waitUntilTasks } = makeAiBinding({
    AI_REQUEST_BUDGET_MS: "20",
    AI_NETWORK: {
      async fetch() {
        providerStarted();
        return await new Promise(() => {});
      },
    },
  });
  const controller = new AbortController();
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    void binding.fetch(new Request("https://ai.wdl/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "openai/primary", input: "cancel" }),
      signal: controller.signal,
    }));
    await started;
    controller.abort();
    assert.equal(aiPoolStateForTest().request.inUse, 1);
    await Promise.all(waitUntilTasks);
  });
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI request deadline rejects a provider response that arrives after abort", async () => {
  const { binding, waitUntilTasks } = makeAiBinding({
    AI_REQUEST_BUDGET_MS: "10",
    AI_NETWORK: {
      async fetch() {
        await delay(25);
        return Response.json({ id: "too-late" });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "late" }));
    assert.equal(
      (await readJsonResponse(response, 504, "late AI response")).error,
      "ai_request_timeout"
    );
  });
  await Promise.all(waitUntilTasks);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI stream AbortSignal remains active after response headers", async () => {
  const controller = new AbortController();
  const { binding, waitUntilTasks } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({ pull() { return new Promise(() => {}); } }), {
          headers: { "content-type": "text/event-stream" },
        });
      },
    },
  });
  const resolution = openAiResolution({ transport: "sse" });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const response = await binding.fetch(new Request("https://ai.wdl/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "openai/primary", input: "cancel stream", stream: true }),
      signal: controller.signal,
    }));
    const body = response.text();
    controller.abort();
    await assert.rejects(() => body, /aborted/i);
  });
  await Promise.all(waitUntilTasks);
  assert.equal(aiPoolStateForTest().stream.inUse, 0);
});

test("AI host rejects resolver destination drift before provider I/O", async () => {
  let providerCalls = 0;
  const { binding } = makeAiBinding({
    AI_NETWORK: { async fetch() { providerCalls += 1; return Response.json({}); } },
  });
  await withMockedProperty(globalThis, "fetch", resolver(openAiResolution({
    destination: "https://attacker.invalid/v1/responses",
  })), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
    assert.equal(
      (await readJsonResponse(response, 503, "AI resolver destination drift")).error,
      "ai_resolver_invalid"
    );
  });
  assert.equal(providerCalls, 0);
});

test("AI host binds resolver responses to the requested model and transport", async () => {
  const cases = [
    openAiResolution({ alias: "swapped" }),
    openAiResolution({
      provider: "xai",
      kind: "xai",
      destination: "https://api.x.ai/v1/responses",
    }),
    openAiResolution({
      protocol: "chat_completions",
      destination: "https://api.openai.com/v1/chat/completions",
    }),
    openAiResolution({ transport: "sse" }),
  ];
  for (const resolution of cases) {
    let providerCalls = 0;
    const { binding } = makeAiBinding({
      AI_NETWORK: { async fetch() { providerCalls += 1; return Response.json({}); } },
    });
    await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
      const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
      assert.equal(
        (await readJsonResponse(response, 503, "AI resolver tuple drift")).error,
        "ai_resolver_invalid"
      );
    });
    assert.equal(providerCalls, 0);
  }
});

test("AI host sanitizes internal resolver failures", async () => {
  let calls = 0;
  const { binding } = makeAiBinding();
  await withMockedProperty(globalThis, "fetch", async () => {
    calls += 1;
    return Response.json({
      error: "redis_error",
      message: "WRONGTYPE ai:provider-credentials:private-ns",
    }, { status: 500 });
  }, async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
    const payload = await readJsonResponse(response, 503, "sanitized AI resolver failure");
    assert.equal(payload.error, "redis_error");
    assert.equal(payload.message, "AI resolver is unavailable");
    assert.doesNotMatch(JSON.stringify(payload), /WRONGTYPE|private-ns/);
  });
  assert.equal(calls, 2);
});

test("AI host rejects duplicate WebSocket model parameters before resolution", async () => {
  const { binding } = makeAiBinding();
  const response = await binding.fetch(new Request(
    "https://ai.wdl/v1/responses?model=openai%2Fprimary&model=openai%2Fother",
    { headers: { upgrade: "websocket" } }
  ));
  assert.equal(
    (await readJsonResponse(response, 400, "duplicate AI websocket model")).error,
    "ai_invalid_request"
  );
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

class FakeResponse {
  /**
   * @param {BodyInit | null} [body]
   * @param {ResponseInit & { webSocket?: FakeWebSocket }} [init]
   */
  constructor(body = null, init = {}) {
    this.body = body;
    this.status = init.status ?? 200;
    this.headers = new Headers(init.headers);
    this.webSocket = init.webSocket;
    this.ok = this.status >= 200 && this.status < 300;
  }

  /** @param {unknown} data @param {ResponseInit} [init] */
  static json(data, init = {}) {
    const headers = new Headers(init.headers);
    if (!headers.has("content-type")) headers.set("content-type", "application/json");
    return new FakeResponse(JSON.stringify(data), { ...init, headers });
  }
}

class FakeWebSocket {
  constructor() {
    this.binaryType = "blob";
    this.accepted = false;
    /** @type {unknown[]} */
    this.sent = [];
    /** @type {{ code?: number, reason?: string } | null} */
    this.closed = null;
    /** @type {Map<string, Array<(event: unknown) => void>>} */
    this.listeners = new Map();
  }

  accept() { this.accepted = true; }

  /** @param {string} type @param {(event: unknown) => void} callback */
  addEventListener(type, callback) {
    const callbacks = this.listeners.get(type) || [];
    callbacks.push(callback);
    this.listeners.set(type, callbacks);
  }

  /** @param {string} type @param {unknown} [event] */
  dispatch(type, event = {}) {
    for (const callback of this.listeners.get(type) || []) callback(event);
  }

  /** @param {unknown} data */
  send(data) {
    if (!this.accepted) throw new TypeError("socket is not accepted");
    this.sent.push(data);
  }

  /** @param {number} [code] @param {string} [reason] */
  close(code = undefined, reason = undefined) {
    if (!this.accepted) throw new TypeError("socket is not accepted");
    this.closed = { code, reason };
  }
}

/**
 * @param {{
 *   resolution?: ReturnType<typeof openAiResolution>,
 *   env?: Record<string, unknown>,
 *   providerResponse?: Response,
 *   requestHeaders?: HeadersInit,
 *   expectedStatus?: number,
 * }} [options]
 */
async function openFakeAiWebSocket(options = {}) {
  const resolution = options.resolution ?? openAiResolution({
    transport: "responses_websocket",
    destination: "wss://api.openai.com/v1/responses",
  });
  const upstream = new FakeWebSocket();
  /** @type {FakeWebSocket | null} */
  let downstream = null;
  const nativeResponse = Response;
  /** @type {Array<{ url: string, headers: Headers }>} */
  const providerCalls = [];
  let resolverCalls = 0;
  /** @type {Headers | null} */
  let responseHeaders = null;
  const { binding, waitUntilTasks } = makeAiBinding({
    ...options.env,
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerCalls.push({ url: String(url), headers: new Headers(init.headers) });
        if (options.providerResponse) return options.providerResponse;
        return { status: 101, headers: new Headers(), body: null, webSocket: upstream };
      },
    },
  });
  const fakeResponse = /** @type {typeof Response} */ (/** @type {unknown} */ (FakeResponse));
  const websocketGlobal = /** @type {typeof globalThis & { WebSocketPair: unknown }} */ (
    /** @type {unknown} */ (globalThis)
  );
  await withMockedProperty(globalThis, "Response", fakeResponse, async () => {
    await withMockedProperty(websocketGlobal, "WebSocketPair", class {
      constructor() {
        const client = new FakeWebSocket();
        downstream = new FakeWebSocket();
        return [client, downstream];
      }
    }, async () => {
      await withMockedProperty(globalThis, "fetch", async () => {
        resolverCalls += 1;
        return nativeResponse.json(resolution);
      }, async () => {
        const protocol = resolution.protocol === "realtime" ? "realtime" : "responses";
        const headers = new Headers(options.requestHeaders);
        headers.set("upgrade", "websocket");
        const response = await binding.fetch(new Request(
          `https://ai.wdl/v1/${protocol}?model=${resolution.provider}%2F${resolution.alias}`,
          { headers }
        ));
        assert.equal(response.status, options.expectedStatus ?? 101);
        if (response.status === 101) {
          assert.ok(response.webSocket);
          responseHeaders = new Headers(response.headers);
        }
      });
    });
  });

  if ((options.expectedStatus ?? 101) === 101) assert.ok(downstream);
  return {
    upstream,
    downstream: /** @type {FakeWebSocket | null} */ (/** @type {unknown} */ (downstream)),
    providerCalls,
    resolverCalls,
    responseHeaders,
    waitUntilTasks,
  };
}

test("AI host WebSocket pins the model and preserves a no-status close", async () => {
  const { upstream, downstream, providerCalls, resolverCalls, responseHeaders } = await openFakeAiWebSocket({
    requestHeaders: {
      authorization: "Bearer tenant-secret",
      cookie: "tenant-cookie=secret",
      "openai-organization": "tenant-org",
      "sec-websocket-protocol": "tenant-protocol",
      "x-api-key": "tenant-api-key",
      "x-wdl-internal-auth": "tenant-internal-token",
    },
  });
  assert.ok(downstream);
  assert.equal(resolverCalls, 1);
  assert.equal(providerCalls.length, 1);
  assert.equal(providerCalls[0].url, "https://api.openai.com/v1/responses");
  assert.deepEqual([...providerCalls[0].headers.keys()].toSorted(), ["authorization", "upgrade"]);
  assert.equal(providerCalls[0].headers.get("authorization"), "Bearer fake-openai-key");
  assert.equal(
    /** @type {Headers | null} */ (responseHeaders)?.get("x-wdl-websocket-reconnect-policy"),
    "disabled"
  );

  downstream.dispatch("message", {
    data: JSON.stringify({ type: "response.create", model: "openai/primary", input: "hello" }),
  });
  assert.equal(JSON.parse(/** @type {string} */ (upstream.sent[0])).model, "gpt-test");
  upstream.dispatch("message", { data: JSON.stringify({ type: "response.completed", response: { id: "r" } }) });
  assert.equal(JSON.parse(/** @type {string} */ (downstream.sent[0])).type, "response.completed");
  upstream.dispatch("close", { code: 1005, reason: "" });
  downstream.dispatch("close", { code: 1000, reason: "duplicate peer event" });
  assert.deepEqual(upstream.closed, { code: undefined, reason: undefined });
  assert.deepEqual(downstream.closed, { code: undefined, reason: undefined });
  assert.equal(providerCalls.length, 1);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  assert.equal(AI_HOST_TEST_STATE.metrics.filter((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "websocket" &&
    ["provider_closed", "client_closed"].includes(entry.labels.outcome)).length, 1);
});

test("AI host cancels an oversized WebSocket rejection response", async () => {
  let cancelled = false;
  const providerBody = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(AI_RESPONSE_MAX_BYTES + 1));
    },
    cancel() { cancelled = true; },
  });
  await openFakeAiWebSocket({
    providerResponse: new Response(providerBody, {
      status: 400,
      headers: { "content-type": "application/json" },
    }),
    expectedStatus: 502,
  });
  assert.equal(cancelled, true);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host makes provider-loss closes terminal to Gateway", async () => {
  for (const code of [1001, 1006, 1011]) {
    const { upstream, downstream } = await openFakeAiWebSocket();
    assert.ok(downstream);
    upstream.dispatch("close", { code, reason: "provider lost" });
    assert.deepEqual(downstream.closed, {
      code: 1013,
      reason: "AI provider connection lost",
    });
    assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  }

  const errored = await openFakeAiWebSocket();
  assert.ok(errored.downstream);
  errored.upstream.dispatch("error");
  assert.deepEqual(errored.downstream.closed, {
    code: 1013,
    reason: "AI provider connection lost",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host WebSocket rejects malformed JSON and cross-model frames", async () => {
  for (const data of [
    "not JSON",
    JSON.stringify({ type: "response.create", model: "openai/other", input: "hello" }),
  ]) {
    const { upstream, downstream } = await openFakeAiWebSocket();
    assert.ok(downstream);
    downstream.dispatch("message", { data });
    assert.equal(upstream.sent.length, 0);
    assert.deepEqual(upstream.closed, { code: 1008, reason: "AI websocket frame rejected" });
    assert.deepEqual(downstream.closed, { code: 1008, reason: "AI websocket frame rejected" });
    assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  }
});

test("AI host WebSocket enforces binary capability and rewrites Realtime models", async () => {
  const denied = await openFakeAiWebSocket();
  assert.ok(denied.downstream);
  denied.downstream.dispatch("message", { data: new Uint8Array([1, 2, 3]).buffer });
  assert.equal(denied.upstream.sent.length, 0);
  assert.equal(denied.upstream.closed?.code, 1003);
  assert.equal(denied.downstream.closed?.code, 1003);

  const realtime = await openFakeAiWebSocket({
    resolution: openAiResolution({
      provider: "xai",
      alias: "realtime",
      kind: "xai",
      upstreamModel: "grok-realtime-test",
      protocol: "realtime",
      transport: "realtime_websocket",
      destination: "wss://api.x.ai/v1/realtime",
      inputModalities: ["audio", "text"],
      capabilities: { ...openAiResolution().capabilities, binaryFrames: true },
    }),
  });
  assert.ok(realtime.downstream);
  realtime.downstream.dispatch("message", {
    data: JSON.stringify({
      type: "session.update",
      session: { model: "xai/realtime", modalities: ["text", "audio"] },
    }),
  });
  assert.equal(
    JSON.parse(/** @type {string} */ (realtime.upstream.sent[0])).session.model,
    "grok-realtime-test"
  );
  const binary = new Uint8Array([0, 1, 127, 128, 255]).buffer;
  realtime.downstream.dispatch("message", { data: binary });
  assert.deepEqual(realtime.upstream.sent[1], binary);
  assert.equal(
    realtime.providerCalls[0].url,
    "https://api.x.ai/v1/realtime?model=grok-realtime-test"
  );
  realtime.downstream.dispatch("close", { code: 1000, reason: "done" });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI WebSocket pool saturates before resolution and recovers after close", async () => {
  const first = await openFakeAiWebSocket({ env: { AI_WS_MAX_SESSIONS: "1" } });
  assert.ok(first.downstream);
  assert.equal(aiPoolStateForTest().websocket.inUse, 1);

  const saturated = await openFakeAiWebSocket({
    env: { AI_WS_MAX_SESSIONS: "1" },
    expectedStatus: 429,
  });
  assert.equal(saturated.resolverCalls, 0);
  assert.equal(saturated.providerCalls.length, 0);
  assert.equal(aiPoolStateForTest().websocket.inUse, 1);

  first.downstream.dispatch("close", { code: 1000, reason: "done" });
  await Promise.all(first.waitUntilTasks);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI WebSocket idle and overall deadlines close both peers and release capacity", async () => {
  /** @type {Array<[Record<string, unknown>, string, string]>} */
  const cases = [
    [{ AI_WS_IDLE_TIMEOUT_MS: "10", AI_WS_MAX_DURATION_MS: "1000" }, "AI websocket idle timeout", "idle_timeout"],
    [{ AI_WS_IDLE_TIMEOUT_MS: "1000", AI_WS_MAX_DURATION_MS: "10" }, "AI websocket deadline", "deadline"],
  ];
  for (const [env, reason, outcome] of cases) {
    const session = await openFakeAiWebSocket({ env });
    assert.ok(session.downstream);
    await delay(25);
    assert.deepEqual(session.upstream.closed, { code: 1012, reason });
    assert.deepEqual(session.downstream.closed, { code: 1012, reason });
    await Promise.all(session.waitUntilTasks);
    assert.equal(aiPoolStateForTest().websocket.inUse, 0);
    assert.ok(AI_HOST_TEST_STATE.metrics.some((entry) =>
      entry.name === "ai_pool_events" &&
      entry.labels.pool === "websocket" &&
      entry.labels.outcome === outcome));
  }
});

test("AI host WebSocket applies frame bounds after upstream model injection", async () => {
  const base = JSON.stringify({ type: "response.create", model: "openai/primary", input: "" });
  const input = "x".repeat(AI_WS_FRAME_MAX_BYTES - new TextEncoder().encode(base).byteLength - 64);
  const incoming = JSON.stringify({ type: "response.create", model: "openai/primary", input });
  assert.ok(new TextEncoder().encode(incoming).byteLength < AI_WS_FRAME_MAX_BYTES);
  const { upstream, downstream } = await openFakeAiWebSocket({
    resolution: openAiResolution({
      upstreamModel: "m".repeat(256),
      transport: "responses_websocket",
      destination: "wss://api.openai.com/v1/responses",
    }),
  });
  assert.ok(downstream);

  try {
    downstream.dispatch("message", { data: incoming });
    assert.equal(upstream.sent.length, 0);
    assert.equal(downstream.closed?.code, 1009);
    assert.equal(upstream.closed?.code, 1009);
    assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  } finally {
    upstream.dispatch("close", { code: 1000, reason: "test cleanup" });
  }
});
