import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import {
  AI_HOST_TEST_STATE,
  AI_REQUEST_MAX_BYTES,
  AI_REQUEST_MAX_JSON_DEPTH,
  AI_RESPONSE_MAX_BYTES,
  AI_STREAM_FRAME_MAX_BYTES,
  AI_STREAM_MAX_BYTES,
  AI_WS_FRAME_MAX_BYTES,
  AI_WS_MAX_JSON_DEPTH,
  AI_WS_MAX_BYTES,
  AiBinding,
  aiProviderWebSocketRequest,
  aiPoolStateForTest,
  makeAiBinding,
  modelList,
  openAiResolution,
  resetAiHostTestState,
} from "../helpers/load-runtime-ai-binding.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { delay, waitUntil } from "../helpers/timing.js";

beforeEach(resetAiHostTestState);

test("AI fixed payload limits match the public byte contract", () => {
  assert.deepEqual({
    requestBytes: AI_REQUEST_MAX_BYTES,
    requestJsonDepth: AI_REQUEST_MAX_JSON_DEPTH,
    responseBytes: AI_RESPONSE_MAX_BYTES,
    streamBytes: AI_STREAM_MAX_BYTES,
    streamFrameBytes: AI_STREAM_FRAME_MAX_BYTES,
    websocketFrameBytes: AI_WS_FRAME_MAX_BYTES,
    websocketJsonDepth: AI_WS_MAX_JSON_DEPTH,
    websocketDirectionBytes: AI_WS_MAX_BYTES,
  }, {
    requestBytes: 1_048_576,
    requestJsonDepth: 128,
    responseBytes: 4_194_304,
    streamBytes: 33_554_432,
    streamFrameBytes: 1_048_576,
    websocketFrameBytes: 1_048_576,
    websocketJsonDepth: 128,
    websocketDirectionBytes: 67_108_864,
  });
});

/** @param {Record<string, unknown>} body @param {string} [path] */
function request(body, path = "/v1/responses") {
  return new Request(`https://ai.wdl${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "tenant-secret" },
    body: JSON.stringify(body),
  });
}

/** @param {AbortSignal | null} signal @param {string} [message] @returns {AbortSignal} */
function assertAborted(signal, message = undefined) {
  assert.ok(signal, message);
  assert.equal(signal.aborted, true, message);
  return signal;
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
  const fileOnly = openAiResolution({ inputModalities: ["file"] });
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
  await withMockedProperty(globalThis, "fetch", resolver(fileOnly), async () => {
    const response = await binding.fetch(request({
      model: "openai/primary",
      input: [{ type: "input_file", file_url: "https://files.example/input.pdf" }],
    }));
    assert.equal(response.status, 200);
  });
  assert.equal(providerBodies.length, 3);
});

test("AI host counts non-empty Responses instructions as text input", async () => {
  let providerCalls = 0;
  const imageOnly = openAiResolution({ inputModalities: ["image"] });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "resp_test" });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(imageOnly), async () => {
    const rejected = await binding.fetch(request({
      model: "openai/primary",
      instructions: "Describe the image",
      input: [{ type: "input_image", image_url: "https://images.example/input.png" }],
    }));
    assert.equal(
      (await readJsonResponse(rejected, 400, "instructions modality")).error,
      "ai_input_modality_unsupported"
    );

    const allowed = await binding.fetch(request({
      model: "openai/primary",
      instructions: "",
      input: [{ type: "input_image", image_url: "https://images.example/input.png" }],
    }));
    assert.equal(allowed.status, 200);
  });
  assert.equal(providerCalls, 1);
});

test("AI host classifies Responses history and prompt variables before provider I/O", async () => {
  let providerCalls = 0;
  const imageOnly = openAiResolution({ inputModalities: ["image"] });
  const textOnly = openAiResolution({ inputModalities: ["text"] });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "must-not-run" });
      },
    },
  });

  const cases = [
    {
      resolution: imageOnly,
      body: {
        model: "openai/primary",
        input: [{
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text: "previous answer", annotations: [] }],
        }],
      },
    },
    {
      resolution: imageOnly,
      body: {
        model: "openai/primary",
        input: [{
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "refusal", refusal: "previous refusal" }],
        }],
      },
    },
    {
      resolution: textOnly,
      body: {
        model: "openai/primary",
        prompt: {
          id: "pmpt_test",
          variables: {
            subject: {
              type: "input_image",
              image_url: "https://images.example/prompt.png",
            },
          },
        },
      },
    },
    {
      resolution: textOnly,
      body: {
        model: "openai/primary",
        prompt: {
          id: "pmpt_test",
          variables: {
            source: {
              type: "input_file",
              file_url: "https://files.example/prompt.pdf",
            },
          },
        },
      },
    },
  ];
  for (const { resolution, body } of cases) {
    await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
      const response = await binding.fetch(request(body));
      assert.equal(
        (await readJsonResponse(response, 400, "Responses input modality")).error,
        "ai_input_modality_unsupported"
      );
    });
  }
  assert.equal(providerCalls, 0);
});

test("AI host classifies Responses replay modalities before provider I/O", async () => {
  let providerCalls = 0;
  const imageOnly = openAiResolution({ inputModalities: ["image"] });
  const textOnly = openAiResolution({ inputModalities: ["text"] });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "must-not-run" });
      },
    },
  });
  const cases = [
    {
      resolution: imageOnly,
      input: {
        id: "local_shell_output_1",
        output: "command output",
        type: "local_shell_call_output",
        status: "completed",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "shell_output_1",
        call_id: "shell_call_1",
        max_output_length: null,
        output: [{
          stdout: "command output",
          stderr: "",
          outcome: { type: "exit", exit_code: 0 },
        }],
        status: "completed",
        type: "shell_call_output",
      },
    },
    {
      resolution: imageOnly,
      input: {
        call_id: "patch_call_1",
        output: "patch applied",
        status: "completed",
        type: "apply_patch_call_output",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "mcp_call_1",
        arguments: "{}",
        name: "lookup",
        server_label: "tools",
        output: "tool result",
        type: "mcp_call",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "mcp_call_2",
        arguments: "{}",
        name: "lookup",
        server_label: "tools",
        error: "tool failed",
        type: "mcp_call",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "mcp_tools_1",
        server_label: "tools",
        tools: [],
        error: "list failed",
        type: "mcp_list_tools",
      },
    },
    {
      resolution: imageOnly,
      input: {
        approval_request_id: "approval_1",
        approve: false,
        reason: "not approved",
        type: "mcp_approval_response",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "program_output_1",
        call_id: "program_call_1",
        result: "program result",
        status: "completed",
        type: "program_output",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "code_call_1",
        code: "print('hello')",
        container_id: "container_1",
        outputs: [{ type: "logs", logs: "hello" }],
        status: "completed",
        type: "code_interpreter_call",
      },
    },
    {
      resolution: textOnly,
      input: {
        id: "code_call_2",
        code: "plot()",
        container_id: "container_1",
        outputs: [{ type: "image", url: "https://images.example/plot.png" }],
        status: "completed",
        type: "code_interpreter_call",
      },
    },
    {
      resolution: textOnly,
      input: {
        id: "image_call_1",
        result: "base64-image-data",
        status: "completed",
        type: "image_generation_call",
      },
    },
    {
      resolution: imageOnly,
      input: {
        id: "file_search_1",
        queries: ["deployment guide"],
        results: [{
          file_id: "file_1",
          filename: "guide.txt",
          score: 0.9,
          text: "retrieved text",
        }],
        status: "completed",
        type: "file_search_call",
      },
    },
  ];

  for (const { resolution, input } of cases) {
    await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
      const response = await binding.fetch(request({ model: "openai/primary", input: [input] }));
      assert.equal(
        (await readJsonResponse(response, 400, `${input.type} modality`)).error,
        "ai_input_modality_unsupported"
      );
    });
  }
  assert.equal(providerCalls, 0);
});

test("AI host treats Responses reasoning replay as opaque provider state", async () => {
  /** @type {Record<string, unknown>[]} */
  const providerBodies = [];
  const imageOnly = openAiResolution({ inputModalities: ["image"] });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerBodies.push(parseJsonObjectRequestBody(init, "reasoning replay request"));
        return Response.json({ id: "resp_test" });
      },
    },
  });
  const body = {
    model: "openai/primary",
    input: [
      {
        id: "reasoning_1",
        summary: [{ type: "summary_text", text: "reasoning summary" }],
        content: [{ type: "reasoning_text", text: "reasoning content" }],
        encrypted_content: "opaque-reasoning-state",
        type: "reasoning",
      },
      { type: "input_image", image_url: "https://images.example/current.png" },
    ],
    reasoning: { context: "current_turn" },
  };

  await withMockedProperty(globalThis, "fetch", resolver(imageOnly), async () => {
    const response = await binding.fetch(request(body));
    assert.equal(response.status, 200);
  });
  assert.deepEqual(providerBodies, [{ ...body, model: "gpt-test" }]);
});

test("AI host classifies Chat assistant refusal and audio history before provider I/O", async () => {
  let providerCalls = 0;
  /** @param {string[]} inputModalities */
  const chatResolution = (inputModalities) => openAiResolution({
    protocol: "chat_completions",
    destination: "https://api.openai.com/v1/chat/completions",
    inputModalities,
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "must-not-run" });
      },
    },
  });
  const cases = [
    {
      resolution: chatResolution(["image"]),
      message: { role: "assistant", content: null, refusal: "previous refusal" },
    },
    {
      resolution: chatResolution(["text"]),
      message: { role: "assistant", content: null, audio: { id: "audio_1" } },
    },
  ];

  for (const { resolution, message } of cases) {
    await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
      const response = await binding.fetch(request(
        { model: "openai/primary", messages: [message] },
        "/v1/chat/completions"
      ));
      assert.equal(
        (await readJsonResponse(response, 400, "Chat assistant history modality")).error,
        "ai_input_modality_unsupported"
      );
    });
  }
  assert.equal(providerCalls, 0);
});

test("AI host treats Embeddings token arrays as text input", async () => {
  /** @type {Record<string, unknown>[]} */
  const providerBodies = [];
  /** @param {string[]} inputModalities */
  const embeddingResolution = (inputModalities) => openAiResolution({
    protocol: "embeddings",
    transport: "http",
    destination: "https://api.openai.com/v1/embeddings",
    inputModalities,
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerBodies.push(parseJsonObjectRequestBody(init, "embedding provider request"));
        return Response.json({ object: "list", data: [] });
      },
    },
  });

  for (const input of [[101, 202], [[101, 202]]]) {
    await withMockedProperty(
      globalThis,
      "fetch",
      resolver(embeddingResolution(["image"])),
      async () => {
        const response = await binding.fetch(request(
          { model: "openai/primary", input },
          "/v1/embeddings"
        ));
        assert.equal(
          (await readJsonResponse(response, 400, "embedding input modality")).error,
          "ai_input_modality_unsupported"
        );
      }
    );
  }
  assert.equal(providerBodies.length, 0);

  await withMockedProperty(
    globalThis,
    "fetch",
    resolver(embeddingResolution(["text"])),
    async () => {
      const response = await binding.fetch(request(
        { model: "openai/primary", input: [101, 202] },
        "/v1/embeddings"
      ));
      assert.equal(response.status, 200);
    }
  );
  assert.deepEqual(providerBodies, [{ model: "gpt-test", input: [101, 202] }]);
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

test("AI host allows empty DeepSeek state and rejects unsupported inputs before provider I/O", async () => {
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
    AI_NETWORK: { async fetch() { providerCalls += 1; return Response.json({ id: "resp_test" }); } },
  });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const allowed = await binding.fetch(request({
      model: "deepseek/flash",
      input: "hello",
      previous_response_id: null,
      conversation: null,
      store: false,
    }));
    assert.equal(allowed.status, 200);

    /** @type {Array<[Record<string, unknown>, string]>} */
    const cases = [
      [{ input: "hello", previous_response_id: "resp_old" }, "ai_continuation_unsupported"],
      [{ input: "hello", conversation: { id: "conv_1" } }, "ai_continuation_unsupported"],
      [{ input: "hello", store: true }, "ai_store_unsupported"],
      [{ input: [{ type: "input_image", image_url: "https://images.example/input.png" }] }, "ai_input_modality_unsupported"],
      [{ input: [{ type: "input_file", file_url: "https://files.example/input.pdf" }] }, "ai_input_modality_unsupported"],
      [{ messages: [{ role: "user", content: [{ type: "file", file: { file_id: "file_1" } }] }] }, "ai_input_modality_unsupported"],
      [{
        input: [{
          type: "function_call_output",
          call_id: "call_1",
          output: [{ type: "input_image", image_url: "https://images.example/tool.png" }],
        }],
      }, "ai_input_modality_unsupported"],
      [{
        input: [{
          type: "custom_tool_call_output",
          call_id: "call_2",
          output: [{ type: "input_file", file_url: "https://files.example/tool.pdf" }],
        }],
      }, "ai_input_modality_unsupported"],
      [{
        input: [{
          type: "computer_call_output",
          call_id: "call_3",
          output: {
            type: "computer_screenshot",
            image_url: "https://images.example/screenshot.png",
          },
        }],
      }, "ai_input_modality_unsupported"],
      [{ input: "hello", background: true }, "ai_background_unsupported"],
    ];
    for (const [body, error] of cases) {
      const response = await binding.fetch(request({ model: "deepseek/flash", ...body }));
      assert.equal((await readJsonResponse(response, 400, error)).error, error);
    }
  });
  assert.equal(providerCalls, 1);
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

test("AI host rejects invalid UTF-8 before resolver or provider I/O", async () => {
  let resolverCalls = 0;
  let providerCalls = 0;
  const prefix = new TextEncoder().encode('{"model":"openai/primary","input":"');
  const suffix = new TextEncoder().encode('"}');
  const body = new Uint8Array(prefix.byteLength + 1 + suffix.byteLength);
  body.set(prefix);
  body[prefix.byteLength] = 0x80;
  body.set(suffix, prefix.byteLength + 1);
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "must-not-run" });
      },
    },
  });

  const response = await withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    return Response.json(openAiResolution());
  }, async () => await binding.fetch(new Request("https://ai.wdl/v1/responses", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  })));

  assert.equal(
    (await readJsonResponse(response, 400, "invalid UTF-8 AI request")).error,
    "ai_request_body_unreadable"
  );
  assert.equal(resolverCalls, 0);
  assert.equal(providerCalls, 0);
});

test("AI host bounds the final provider JSON after model replacement", async () => {
  let resolverCalls = 0;
  let providerCalls = 0;
  const publicModel = "a/b";
  const prefix = `{"model":"${publicModel}","input":"`;
  const suffix = '"}';
  const body = `${prefix}${"x".repeat(AI_REQUEST_MAX_BYTES - prefix.length - suffix.length)}${suffix}`;
  assert.equal(new TextEncoder().encode(body).byteLength, AI_REQUEST_MAX_BYTES);
  const resolution = openAiResolution({
    provider: "a",
    alias: "b",
    upstreamModel: "m".repeat(256),
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "must-not-run" });
      },
    },
  });

  const response = await withMockedProperty(globalThis, "fetch", async (input, init = {}) => {
    resolverCalls += 1;
    return await resolver(resolution)(input, init);
  }, async () => await binding.fetch(new Request("https://ai.wdl/v1/responses", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  })));

  assert.equal(
    (await readJsonResponse(response, 413, "expanded AI provider request")).error,
    "ai_request_too_large"
  );
  assert.equal(resolverCalls, 1);
  assert.equal(providerCalls, 0);
});

test("AI host rejects malformed HTTP and WebSocket model references before resolution", async () => {
  let resolverCalls = 0;
  const { binding } = makeAiBinding();
  await withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    throw new Error("resolver must not run");
  }, async () => {
    const http = await binding.fetch(request({ model: "openai", input: "hello" }));
    assert.equal(
      (await readJsonResponse(http, 400, "malformed AI HTTP model")).error,
      "ai_invalid_model"
    );

    const websocket = await binding.fetch(new Request(
      "https://ai.wdl/v1/responses?model=OpenAI%2Fprimary",
      { headers: { upgrade: "websocket" } }
    ));
    assert.equal(
      (await readJsonResponse(websocket, 400, "malformed AI WebSocket model")).error,
      "ai_invalid_model"
    );
  });
  assert.equal(resolverCalls, 0);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
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
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
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
  assertAborted(providerSignal);
});

test("AI host accepts bare-CR SSE frames and split CRLF boundaries", async () => {
  const chunks = [
    "event: response.created\r",
    "\ndata: {\"type\":\"response.created\"}\r",
    "\n\r",
    "\nevent: response.completed\r",
    "data: {\"type\":\"response.completed\"}\r",
    "\r",
  ];
  const expected = chunks.join("");
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({
          start(controller) {
            for (const chunk of chunks) controller.enqueue(new TextEncoder().encode(chunk));
            controller.close();
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(
    globalThis,
    "fetch",
    resolver(openAiResolution({ transport: "sse" })),
    async () => {
      const response = await binding.fetch(request({
        model: "openai/primary",
        input: "hello",
        stream: true,
      }));
      assert.equal(await response.text(), expected);
    }
  );
  assert.deepEqual(aiPoolStateForTest().stream, { inUse: 0, highWater: 1 });
});

test("AI host flushes a bare-CR terminal frame from a quiet provider", async () => {
  const frame = "event: response.completed\rdata: {\"type\":\"response.completed\"}\r\r";
  const { binding } = makeAiBinding({
    AI_STREAM_IDLE_TIMEOUT_MS: "10",
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(frame));
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(
    globalThis,
    "fetch",
    resolver(openAiResolution({ transport: "sse" })),
    async () => {
      const response = await binding.fetch(request({
        model: "openai/primary",
        input: "hello",
        stream: true,
      }));
      assert.equal(await response.text(), frame);
    }
  );
  assert.deepEqual(aiPoolStateForTest().stream, { inUse: 0, highWater: 1 });
});

test("AI host does not treat consumer backpressure as provider idle", async () => {
  const frames = [
    'event: response.created\ndata: {"type":"response.created"}\n\n',
    'event: response.in_progress\ndata: {"type":"response.in_progress"}\n\n',
    'event: response.completed\ndata: {"type":"response.completed"}\n\n',
  ];
  const expected = frames.join("");
  const { binding } = makeAiBinding({
    AI_STREAM_IDLE_TIMEOUT_MS: "20",
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(frames[0]));
            controller.enqueue(new TextEncoder().encode(frames.slice(1).join("")));
          },
        }), { headers: { "content-type": "text/event-stream" } });
      },
    },
  });
  await withMockedProperty(
    globalThis,
    "fetch",
    resolver(openAiResolution({ transport: "sse" })),
    async () => {
      const response = await binding.fetch(request({
        model: "openai/primary",
        input: "hello",
        stream: true,
      }));
      await delay(60);
      assert.equal(await response.text(), expected);
    }
  );
  assert.deepEqual(aiPoolStateForTest().stream, { inUse: 0, highWater: 1 });
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

test("AI host treats Chat Completions DONE as terminal", async () => {
  const frame = "data: [DONE]\n\n";
  let cancelled = false;
  const resolution = openAiResolution({
    protocol: "chat_completions",
    destination: "https://api.openai.com/v1/chat/completions",
    transport: "sse",
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        return new Response(new ReadableStream({
          start(controller) { controller.enqueue(new TextEncoder().encode(frame)); },
          cancel() { cancelled = true; },
        }), { headers: { "content-type": "text/event-stream" } });
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
  assert.equal(cancelled, true);
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
  /** @type {AbortSignal | null} */
  let providerSignal = null;
  const providerBody = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(AI_RESPONSE_MAX_BYTES + 1));
    },
    cancel() { cancelled = true; },
  });
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerSignal = init.signal ?? null;
        return new Response(providerBody, { headers: { "content-type": "application/json" } });
      },
    },
  });
  await withMockedProperty(globalThis, "fetch", resolver(), async () => {
    const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
    assert.equal(
      (await readJsonResponse(response, 502, "oversized AI provider response")).error,
      "ai_provider_response_too_large"
    );
  });
  assert.equal(cancelled, true);
  assertAborted(providerSignal);
  assert.equal(aiPoolStateForTest().request.inUse, 0);
});

test("AI host aborts rejected provider responses before releasing capacity", async () => {
  for (const scenario of [
    {
      name: "redirect",
      body: { model: "openai/primary", input: "hello" },
      resolution: openAiResolution(),
      response: () => new Response("redirect", { status: 302, headers: { location: "https://other.invalid" } }),
      code: "ai_provider_redirect",
    },
    {
      name: "invalid event stream",
      body: { model: "openai/primary", input: "hello", stream: true },
      resolution: openAiResolution({ transport: "sse" }),
      response: () => new Response("not an event stream", { headers: { "content-type": "text/plain" } }),
      code: "ai_provider_invalid_response",
    },
  ]) {
    /** @type {AbortSignal | null} */
    let providerSignal = null;
    const { binding } = makeAiBinding({
      AI_NETWORK: {
        async fetch(
          /** @type {RequestInfo | URL} */ _url,
          /** @type {RequestInit} */ init = {}
        ) {
          providerSignal = init.signal ?? null;
          return scenario.response();
        },
      },
    });
    await withMockedProperty(globalThis, "fetch", resolver(scenario.resolution), async () => {
      const response = await binding.fetch(request(scenario.body));
      assert.equal(
        (await readJsonResponse(response, 502, `AI provider ${scenario.name}`)).error,
        scenario.code
      );
    });
    assertAborted(providerSignal, scenario.name);
    assert.equal(aiPoolStateForTest().request.inUse, 0, scenario.name);
    assert.equal(aiPoolStateForTest().stream.inUse, 0, scenario.name);
  }
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
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
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
  assertAborted(providerSignal);
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

test("AI request body read failures use a fixed public and log message", async () => {
  const marker = "tenant-secret-prompt-marker";
  let resolverCalls = 0;
  const body = new ReadableStream({ pull() { throw new Error(marker); } });
  const requestInit = /** @type {RequestInit & { duplex: "half" }} */ ({
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
    duplex: "half",
  });
  const { binding } = makeAiBinding();
  const response = await withMockedProperty(globalThis, "fetch", async () => {
    resolverCalls += 1;
    return Response.json(openAiResolution());
  }, async () => await binding.fetch(new Request("https://ai.wdl/v1/responses", requestInit)));

  assert.equal(
    (await readJsonResponse(response, 400, "failed AI request body")).error,
    "ai_request_body_unreadable"
  );
  assert.equal(resolverCalls, 0);
  assert.doesNotMatch(JSON.stringify(AI_HOST_TEST_STATE.logs), new RegExp(marker));
  assert.match(JSON.stringify(AI_HOST_TEST_STATE.logs), /AI request body could not be read/);
});

test("AI request JSON depth accepts 128 and rejects 129 before parsing", async () => {
  let resolverCalls = 0;
  let providerCalls = 0;
  const requestParseCalls = new Map();
  /** @param {number} depth */
  const bodyAtDepth = (depth) =>
    `{"model":"openai/primary","input":${"{\"next\":".repeat(depth - 1)}null${"}".repeat(depth - 1)}}`;
  const acceptedBody = bodyAtDepth(AI_REQUEST_MAX_JSON_DEPTH);
  const rejectedBody = bodyAtDepth(AI_REQUEST_MAX_JSON_DEPTH + 1);
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "resp_depth" });
      },
    },
  });
  const originalParse = JSON.parse;
  const [accepted, rejected] = await withMockedProperty(
    JSON,
    "parse",
    (text, reviver) => {
      if (text === acceptedBody || text === rejectedBody) {
        requestParseCalls.set(text, (requestParseCalls.get(text) || 0) + 1);
      }
      return originalParse(text, reviver);
    },
    async () => await withMockedProperty(
      globalThis,
      "fetch",
      async () => {
        resolverCalls += 1;
        return Response.json(openAiResolution());
      },
      async () => await Promise.all([acceptedBody, rejectedBody].map(async (body) =>
        await binding.fetch(new Request("https://ai.wdl/v1/responses", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body,
        }))
      ))
    )
  );

  assert.equal((await readJsonResponse(accepted, 200, "AI request at depth limit")).id, "resp_depth");
  assert.equal(
    (await readJsonResponse(rejected, 400, "AI request above depth limit")).error,
    "ai_request_too_deep"
  );
  assert.equal(requestParseCalls.get(acceptedBody), 1);
  assert.equal(requestParseCalls.get(rejectedBody) || 0, 0);
  assert.equal(resolverCalls, 1);
  assert.equal(providerCalls, 1);
});

test("AI request JSON depth validation accepts wide shallow input", async () => {
  let providerCalls = 0;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch() {
        providerCalls += 1;
        return Response.json({ id: "resp_wide" });
      },
    },
  });
  const input = {
    values: Array.from({ length: 50_000 }, () => ({})),
    stringDelimiters: "[{\\\"".repeat(AI_REQUEST_MAX_JSON_DEPTH + 1),
  };
  const response = await withMockedProperty(
    globalThis,
    "fetch",
    resolver(),
    async () => await binding.fetch(request({ model: "openai/primary", input }))
  );

  assert.equal((await readJsonResponse(response, 200, "wide AI request")).id, "resp_wide");
  assert.equal(providerCalls, 1);
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

test("AI host does not resolve failure messages through object prototypes", async () => {
  for (const code of ["toString", "__proto__"]) {
    const { binding } = makeAiBinding();
    await withMockedProperty(globalThis, "fetch", async () => Response.json({
      error: code,
      message: "private-state",
    }, { status: 500 }), async () => {
      const response = await binding.fetch(request({ model: "openai/primary", input: "hello" }));
      assert.deepEqual(
        await readJsonResponse(response, 503, `sanitized ${code} resolver failure`),
        { request_id: "rid-ai-test", error: code, message: "AI resolver is unavailable" }
      );
    });
  }
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
  /** @type {AbortSignal | null} */
  let providerSignal = null;
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
        providerSignal = init.signal ?? null;
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
    providerSignal,
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
    data: '{"type":"response.create","model":"openai/primary","integer":9007199254740993,"overflow":1e400,"negativeZero":-0}',
  });
  assert.equal(
    upstream.sent[0],
    '{"type":"response.create","model":"gpt-test","integer":9007199254740993,"overflow":1e400,"negativeZero":-0}'
  );
  downstream.dispatch("message", {
    data: '{"type":"response.create","integer":9007199254740993,"negativeZero":-0}',
  });
  assert.equal(
    upstream.sent[1],
    '{"type":"response.create","integer":9007199254740993,"negativeZero":-0,"model":"gpt-test"}'
  );
  upstream.dispatch("message", { data: JSON.stringify({ type: "response.completed", response: { id: "r" } }) });
  assert.equal(JSON.parse(/** @type {string} */ (downstream.sent[0])).type, "response.completed");
  upstream.dispatch("close", { code: 1005, reason: "" });
  downstream.dispatch("close", { code: 1000, reason: "duplicate peer event" });
  assert.deepEqual(upstream.closed, { code: undefined, reason: undefined });
  assert.deepEqual(downstream.closed, { code: undefined, reason: undefined });
  assert.equal(providerCalls.length, 1);
  assert.deepEqual(AI_HOST_TEST_STATE.bindingOperations, ["responses_websocket"]);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  assert.equal(AI_HOST_TEST_STATE.metrics.filter((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "websocket" &&
    ["provider_closed", "client_closed"].includes(entry.labels.outcome)).length, 1);
});

test("AI provider WebSocket duration follows the official adapter bound", () => {
  const base = {
    transport: "responses_websocket",
    destination: "wss://api.openai.com/v1/responses",
  };
  assert.equal(
    aiProviderWebSocketRequest(openAiResolution(base)).maxDurationMs,
    59 * 60_000
  );
  assert.equal(
    aiProviderWebSocketRequest(openAiResolution({
      ...base,
      provider: "xai",
      kind: "xai",
      destination: "wss://api.x.ai/v1/responses",
    })).maxDurationMs,
    24 * 60_000
  );
});

test("AI host bounds provider WebSocket frames", async () => {
  const { upstream, downstream } = await openFakeAiWebSocket();
  assert.ok(downstream);
  upstream.dispatch("message", { data: new Uint8Array(AI_WS_FRAME_MAX_BYTES + 1).buffer });
  assert.equal(upstream.closed?.code, 1009);
  assert.equal(downstream.closed?.code, 1009);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host cancels an oversized WebSocket rejection response", async () => {
  let cancelled = false;
  const providerBody = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(AI_RESPONSE_MAX_BYTES + 1));
    },
    cancel() { cancelled = true; },
  });
  const { providerSignal } = await openFakeAiWebSocket({
    providerResponse: new Response(providerBody, {
      status: 400,
      headers: { "content-type": "application/json" },
    }),
    expectedStatus: 502,
  });
  assert.equal(cancelled, true);
  assertAborted(providerSignal);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host rejects WebSocket provider redirects before releasing capacity", async () => {
  /** @type {AbortSignal | null} */
  let providerSignal = null;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerSignal = init.signal ?? null;
        return new Response("redirect", {
          status: 302,
          headers: { location: "https://other.invalid" },
        });
      },
    },
  });
  const resolution = openAiResolution({
    transport: "responses_websocket",
    destination: "wss://api.openai.com/v1/responses",
  });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const response = await binding.fetch(new Request(
      "https://ai.wdl/v1/responses?model=openai%2Fprimary",
      { headers: { upgrade: "websocket" } }
    ));
    assert.equal(
      (await readJsonResponse(response, 502, "AI WebSocket provider redirect")).error,
      "ai_provider_redirect"
    );
  });
  assertAborted(providerSignal);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host rejects a successful provider response without a WebSocket upgrade", async () => {
  /** @type {AbortSignal | null} */
  let providerSignal = null;
  const { binding } = makeAiBinding({
    AI_NETWORK: {
      async fetch(
        /** @type {RequestInfo | URL} */ _url,
        /** @type {RequestInit} */ init = {}
      ) {
        providerSignal = init.signal ?? null;
        return Response.json({ status: "accepted_without_upgrade" });
      },
    },
  });
  const resolution = openAiResolution({
    transport: "responses_websocket",
    destination: "wss://api.openai.com/v1/responses",
  });
  await withMockedProperty(globalThis, "fetch", resolver(resolution), async () => {
    const response = await binding.fetch(new Request(
      "https://ai.wdl/v1/responses?model=openai%2Fprimary",
      { headers: { upgrade: "websocket" } }
    ));
    assert.equal(
      (await readJsonResponse(response, 502, "AI WebSocket missing provider upgrade")).error,
      "ai_provider_invalid_response"
    );
  });
  assertAborted(providerSignal);
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host preserves a bounded provider WebSocket rejection status", async () => {
  await openFakeAiWebSocket({
    providerResponse: Response.json(
      { error: { type: "rate_limit_error", message: "retry later" } },
      { status: 429 }
    ),
    expectedStatus: 429,
  });
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
  await waitUntil(
    "AI provider error fallback",
    () => errored.downstream?.closed !== null,
    { timeoutMs: 1000, intervalMs: 5 }
  );
  assert.deepEqual(errored.downstream.closed, {
    code: 1013,
    reason: "AI provider connection lost",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host preserves a terminal provider close after an error event", async () => {
  const { upstream, downstream } = await openFakeAiWebSocket();
  assert.ok(downstream);

  upstream.dispatch("error");
  downstream.dispatch("message", { data: '{"type":"response.cancel"}' });
  assert.equal(upstream.sent.length, 0);
  await delay(1);
  upstream.dispatch("close", { code: 1009, reason: "provider frame too large" });
  await delay(0);

  assert.deepEqual(downstream.closed, {
    code: 1009,
    reason: "provider frame too large",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host applies the client WebSocket error fallback exactly once", async () => {
  const { upstream, downstream } = await openFakeAiWebSocket();
  assert.ok(downstream);

  downstream.dispatch("error");
  await waitUntil(
    "AI client error fallback",
    () => upstream.closed !== null,
    { timeoutMs: 1000, intervalMs: 5 }
  );

  assert.deepEqual(upstream.closed, {
    code: 1011,
    reason: "AI websocket client error",
  });
  assert.deepEqual(downstream.closed, {
    code: 1011,
    reason: "AI websocket client error",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  assert.deepEqual(AI_HOST_TEST_STATE.metrics.filter((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "websocket" &&
    entry.labels.outcome === "client_error").map((entry) => entry.labels.outcome), [
    "client_error",
  ]);
});

test("AI host preserves a terminal client close after an error event", async () => {
  const { upstream, downstream } = await openFakeAiWebSocket();
  assert.ok(downstream);

  downstream.dispatch("error");
  upstream.dispatch("message", { data: '{"type":"response.created"}' });
  assert.equal(downstream.sent.length, 0);
  await delay(1);
  downstream.dispatch("close", { code: 1009, reason: "client frame too large" });
  await delay(0);

  assert.deepEqual(upstream.closed, {
    code: 1009,
    reason: "client frame too large",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
  assert.deepEqual(AI_HOST_TEST_STATE.metrics.filter((entry) =>
    entry.name === "ai_pool_events" &&
    entry.labels.pool === "websocket" &&
    ["client_closed", "client_error"].includes(entry.labels.outcome)
  ).map((entry) => entry.labels.outcome), ["client_closed"]);
});

test("AI host WebSocket rejects malformed JSON and cross-model frames", async () => {
  for (const data of [
    "not JSON",
    JSON.stringify({ type: "response.create", model: "openai/other", input: "hello" }),
    '{"type":"response.create","type":"response.cancel"}',
    '{"type":"response.create","model":"openai/other","model":"openai/primary"}',
    '{"type":"response.create","m\\u006fdel":"openai/other","model":"openai/primary"}',
    '{"type":"session.update","session":{"model":"openai/primary","model":"gpt-test"}}',
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

test("AI host WebSocket bounds JSON depth before materializing the full payload", async () => {
  const acceptedArrays = AI_WS_MAX_JSON_DEPTH - 1;
  const accepted = `{"type":"response.cancel","payload":${"[".repeat(acceptedArrays)}0${"]".repeat(acceptedArrays)}}`;
  const acceptedSocket = await openFakeAiWebSocket();
  assert.ok(acceptedSocket.downstream);
  acceptedSocket.downstream.dispatch("message", { data: accepted });
  assert.equal(acceptedSocket.upstream.sent[0], accepted);
  acceptedSocket.downstream.dispatch("close", { code: 1000, reason: "done" });

  const rejectedArrays = AI_WS_MAX_JSON_DEPTH;
  const rejected = `{"type":"response.cancel","payload":${"[".repeat(rejectedArrays)}0${"]".repeat(rejectedArrays)}}`;
  const rejectedSocket = await openFakeAiWebSocket();
  const rejectedDownstream = rejectedSocket.downstream;
  assert.ok(rejectedDownstream);
  const intrinsicJsonParse = JSON.parse;
  let materializedRejectedPayload = false;
  await withMockedProperty(JSON, "parse", (text, reviver) => {
    if (text === rejected) materializedRejectedPayload = true;
    return intrinsicJsonParse(text, reviver);
  }, () => {
    rejectedDownstream.dispatch("message", { data: rejected });
  });
  assert.equal(materializedRejectedPayload, false);
  assert.equal(rejectedSocket.upstream.sent.length, 0);
  assert.deepEqual(rejectedSocket.upstream.closed, {
    code: 1008,
    reason: "AI websocket frame rejected",
  });
  assert.deepEqual(rejectedDownstream.closed, {
    code: 1008,
    reason: "AI websocket frame rejected",
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
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
    data: '{"type":"session.update","session":{"model":"xai/realtime","temperature":-0},"vendor":1e400}',
  });
  assert.equal(
    realtime.upstream.sent[0],
    '{"type":"session.update","session":{"model":"grok-realtime-test","temperature":-0},"vendor":1e400}'
  );
  const unchanged =
    '{ "type": "input_audio_buffer.append", "sequence": 9007199254740993, "gain": -0, "audio": "AA==" }';
  realtime.downstream.dispatch("message", { data: unchanged });
  assert.equal(realtime.upstream.sent[1], unchanged);
  const binary = new Uint8Array([0, 1, 127, 128, 255]).buffer;
  realtime.downstream.dispatch("message", { data: binary });
  assert.deepEqual(realtime.upstream.sent[2], binary);
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
  const providerSignal = assertAborted(first.providerSignal);
  assert.equal(providerSignal.reason?.name, "AbortError");
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

test("AI WebSocket idle deadline tracks provider frames rather than client activity", async () => {
  const nativeSetTimeout = globalThis.setTimeout;
  /** @type {number[]} */
  const scheduled = [];
  const recordingSetTimeout = new Proxy(nativeSetTimeout, {
    apply(target, thisArg, args) {
      scheduled.push(Number(args[1]));
      return Reflect.apply(target, thisArg, args);
    },
  });
  await withMockedProperty(globalThis, "setTimeout", recordingSetTimeout, async () => {
    const session = await openFakeAiWebSocket({
      env: { AI_WS_IDLE_TIMEOUT_MS: "1000", AI_WS_MAX_DURATION_MS: "2000" },
    });
    assert.ok(session.downstream);
    const idleSchedules = () => scheduled.filter((delayMs) => delayMs === 1000).length;
    const initialSchedules = idleSchedules();

    session.downstream.dispatch("message", {
      data: JSON.stringify({ type: "response.create", input: "client activity" }),
    });
    assert.equal(idleSchedules(), initialSchedules);

    session.upstream.dispatch("message", {
      data: JSON.stringify({ type: "response.created", response: { id: "r" } }),
    });
    assert.equal(idleSchedules(), initialSchedules + 1);
    session.downstream.dispatch("close", { code: 1000, reason: "done" });
    await Promise.all(session.waitUntilTasks);
  });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
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

test("AI host WebSocket charges original JSON whitespace to the session byte limit", async () => {
  const core = JSON.stringify({ type: "response.cancel", response_id: "resp_test" });
  const frame = `${core}${" ".repeat(AI_WS_FRAME_MAX_BYTES - core.length)}`;
  assert.equal(new TextEncoder().encode(frame).byteLength, AI_WS_FRAME_MAX_BYTES);
  const { upstream, downstream } = await openFakeAiWebSocket();
  assert.ok(downstream);

  const acceptedFrames = AI_WS_MAX_BYTES / AI_WS_FRAME_MAX_BYTES;
  for (let index = 0; index <= acceptedFrames; index += 1) {
    downstream.dispatch("message", { data: frame });
  }
  assert.equal(upstream.sent.length, acceptedFrames);
  assert.deepEqual(upstream.closed, { code: 1009, reason: "AI websocket byte limit" });
  assert.deepEqual(downstream.closed, { code: 1009, reason: "AI websocket byte limit" });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});

test("AI host WebSocket charges expanded model-pinned frames to the session byte limit", async () => {
  const upstreamModel = "m".repeat(200);
  const targetBytes = AI_WS_MAX_BYTES / 128;
  const empty = JSON.stringify({
    type: "response.create",
    model: "openai/primary",
    input: "",
  });
  const frame = JSON.stringify({
    type: "response.create",
    model: "openai/primary",
    input: "x".repeat(targetBytes - empty.length),
  });
  assert.equal(new TextEncoder().encode(frame).byteLength, targetBytes);
  const { upstream, downstream } = await openFakeAiWebSocket({
    resolution: openAiResolution({
      upstreamModel,
      transport: "responses_websocket",
      destination: "wss://api.openai.com/v1/responses",
    }),
  });
  assert.ok(downstream);
  let sent = 0;
  upstream.send = () => { sent += 1; };

  for (let index = 0; index < 128; index += 1) {
    downstream.dispatch("message", { data: frame });
  }
  assert.equal(sent, 127);
  assert.deepEqual(upstream.closed, { code: 1009, reason: "AI websocket byte limit" });
  assert.deepEqual(downstream.closed, { code: 1009, reason: "AI websocket byte limit" });
  assert.equal(aiPoolStateForTest().websocket.inUse, 0);
});
