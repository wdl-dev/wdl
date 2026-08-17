const CREDENTIALS = Object.freeze({
  "api.openai.com": Object.freeze({
    "fake-openai-key": "initial",
    "fake-openai-key-rotated": "rotated",
  }),
  "api.x.ai": Object.freeze({ "fake-xai-key": "initial" }),
  "api.deepseek.com": Object.freeze({ "fake-deepseek-key": "initial" }),
});
const encoder = new TextEncoder();

const ALLOWED_PATHS = Object.freeze({
  "api.openai.com": new Set(["/v1/responses", "/v1/chat/completions", "/v1/embeddings", "/v1/realtime"]),
  "api.x.ai": new Set(["/v1/responses", "/v1/chat/completions", "/v1/embeddings", "/v1/realtime"]),
  "api.deepseek.com": new Set(["/responses", "/chat/completions"]),
});

function json(data, status = 200) {
  return Response.json(data, {
    status,
    headers: {
      "openai-request-id": "fake-provider-request",
      "x-request-id": "fake-provider-generic-request",
    },
  });
}

function authenticate(request, url) {
  const authorization = request.headers.get("authorization") || "";
  const credential = authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
  const credentials = CREDENTIALS[url.hostname];
  return credentials && Object.hasOwn(credentials, credential) ? credentials[credential] : null;
}

function responseObject(body) {
  return {
    id: "resp_fake",
    object: "response",
    status: "completed",
    model: body.model,
    output: [{
      id: "msg_fake",
      type: "message",
      role: "assistant",
      status: "completed",
      content: [{ type: "output_text", text: "fake response" }],
    }],
    usage: { input_tokens: 3, output_tokens: 2, total_tokens: 5 },
  };
}

/** @param {string} event @param {unknown} data */
function eventFrame(event, data) {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

/** @param {unknown} data */
function dataFrame(data) {
  return `data: ${typeof data === "string" ? data : JSON.stringify(data)}\n\n`;
}

function eventStream(body, protocol) {
  const staysOpen = body.wdl_test_mode === "open_stream";
  let frames;
  if (body.wdl_test_mode === "sse_error") {
    frames = [
      eventFrame("error", { type: "error", error: { message: "fake provider error" } }),
    ];
  } else if (staysOpen) {
    frames = [
      eventFrame("response.created", {
        type: "response.created",
        response: { id: "resp_fake", model: body.model },
      }),
    ];
  } else if (protocol === "chat_completions") {
    frames = [
      dataFrame({
        id: "chatcmpl_fake",
        object: "chat.completion.chunk",
        model: body.model,
        choices: [{ delta: { content: "fake" } }],
      }),
      dataFrame("[DONE]"),
    ];
  } else {
    frames = [
      eventFrame("response.created", {
        type: "response.created",
        response: { id: "resp_fake", model: body.model },
      }),
      eventFrame("response.output_text.delta", {
        type: "response.output_text.delta",
        delta: "fake",
      }),
      eventFrame("response.completed", {
        type: "response.completed",
        response: responseObject(body),
      }),
    ];
  }
  return new Response(new ReadableStream({
    start(controller) {
      const bytes = encoder.encode(frames.join(""));
      const split = Math.max(1, Math.floor(bytes.byteLength / 3));
      controller.enqueue(bytes.slice(0, split));
      controller.enqueue(bytes.slice(split, split + 7));
      controller.enqueue(bytes.slice(split + 7));
      if (!staysOpen) controller.close();
    },
    async pull() {
      if (staysOpen) await new Promise((resolve) => setTimeout(resolve, 60_000));
    },
  }), {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "openai-request-id": "fake-provider-stream",
    },
  });
}

function websocket(request, url, credentialGeneration) {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.binaryType = "arraybuffer";
  server.accept();
  server.addEventListener("message", (event) => {
    if (typeof event.data !== "string") {
      server.send(event.data);
      return;
    }
    let message;
    try { message = JSON.parse(event.data); } catch {
      server.close(1007, "invalid JSON");
      return;
    }
    if (url.pathname === "/v1/responses" && message.type === "response.create") {
      if (message.wdl_test_mode === "provider_loss") {
        server.close(1001, "provider restart");
        return;
      }
      if (message.model === "gpt-test-rotated" && credentialGeneration !== "rotated") {
        server.close(1008, "stale fake credential");
        return;
      }
      if (message.input === "wdl_test_tool_loop") {
        server.send(JSON.stringify({
          type: "response.completed",
          response: {
            ...responseObject(message),
            id: "resp_tool",
            output: [{
              type: "function_call",
              call_id: "call_fake",
              name: "lookup",
              arguments: "{}",
            }],
          },
        }));
        return;
      }
      if (message.previous_response_id === "resp_tool") {
        server.send(JSON.stringify({
          type: "response.completed",
          response: {
            ...responseObject(message),
            id: "resp_final",
            output: [{
              type: "message",
              role: "assistant",
              status: "completed",
              content: [{ type: "output_text", text: "tool result accepted" }],
            }],
          },
        }));
        return;
      }
      server.send(JSON.stringify({
        type: "response.completed",
        response: responseObject(message),
      }));
      return;
    }
    if (url.pathname === "/v1/realtime" && message.type === "session.update") {
      server.send(JSON.stringify({ type: "session.updated", session: message.session }));
      return;
    }
    server.send(JSON.stringify({ type: "fake.echo", event: message }));
  });
  return new Response(null, {
    status: 101,
    headers: { "openai-request-id": "fake-provider-websocket" },
    webSocket: client,
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const credentialGeneration = authenticate(request, url);
    if (!credentialGeneration) {
      return json({ error: { message: "invalid fake credential", type: "authentication_error" } }, 401);
    }
    if (!ALLOWED_PATHS[url.hostname]?.has(url.pathname)) {
      return json({ error: { message: "unsupported fake provider path", type: "invalid_request_error" } }, 404);
    }
    if ((request.headers.get("upgrade") || "").toLowerCase() === "websocket") {
      return websocket(request, url, credentialGeneration);
    }
    if (request.method !== "POST") {
      return json({ error: { message: "method not allowed", type: "invalid_request_error" } }, 405);
    }
    const body = await request.json();
    if (body.wdl_test_delay_ms) {
      await new Promise((resolve) => setTimeout(resolve, Number(body.wdl_test_delay_ms)));
    }
    if (body.stream === true) {
      const protocol = url.pathname.endsWith("/chat/completions")
        ? "chat_completions"
        : "responses";
      return eventStream(body, protocol);
    }
    if (body.wdl_test_mode === "invalid_json") {
      return new Response("not json", { status: 200, headers: { "content-type": "text/plain" } });
    }
    if (body.wdl_test_mode === "provider_error") {
      return json({
        error: {
          message: "rate limited by fake provider",
          type: "rate_limit_error",
          code: "rate_limit_exceeded",
        },
      }, 429);
    }
    if (body.wdl_test_mode === "file_input") {
      const file = body.input?.[0]?.content?.[0];
      if (file?.type !== "input_file" || typeof file.file_url !== "string") {
        return json({ error: { message: "missing fake file input", type: "invalid_request_error" } }, 400);
      }
      return json({ ...responseObject(body), wdl_file_input: file });
    }
    if (url.pathname.endsWith("/chat/completions")) {
      return json({
        id: "chatcmpl_fake",
        object: "chat.completion",
        model: body.model,
        choices: [{ index: 0, message: { role: "assistant", content: "fake response" } }],
      });
    }
    if (url.pathname.endsWith("/embeddings")) {
      return json({ object: "list", model: body.model, data: [{ index: 0, embedding: [0.1, 0.2] }] });
    }
    return json(responseObject(body));
  },
};
