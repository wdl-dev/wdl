import { DurableObject, env as importedEnv } from "cloudflare:workers";

const AI_ORIGIN = "https://ai.wdl";
const intrinsicJsonParse = JSON.parse;
const moduleScopeEnv = {
  ai: importedEnv.AI,
  doBackend: importedEnv.__WDL_DO_BACKEND__,
  ownerNetwork: importedEnv.__WDL_DO_OWNER_NETWORK__,
  workflowsBackend: importedEnv.__WDL_WORKFLOWS_BACKEND__,
};

async function moduleScopeAiCalls() {
  if (!moduleScopeEnv.ai) {
    return { fetch: "missing", run: "missing", models: "missing" };
  }
  let fetch = "rejected";
  try {
    const response = await moduleScopeEnv.ai.fetch(`${AI_ORIGIN}/v1/models`);
    fetch = response.status === 200 ? "resolved" : `status:${response.status}`;
  } catch {}
  let run = "resolved";
  try {
    await moduleScopeEnv.ai.run("openai/primary", { input: "module scope" });
  } catch {
    run = "rejected";
  }
  let models = "resolved";
  try {
    await moduleScopeEnv.ai.models();
  } catch {
    models = "rejected";
  }
  return { fetch, run, models };
}

async function aiSurface(env) {
  return {
    handler: {
      fetch: typeof env.AI?.fetch,
      run: typeof env.AI?.run,
      models: typeof env.AI?.models,
    },
    imported: {
      fetch: typeof importedEnv.AI?.fetch,
      run: typeof importedEnv.AI?.run,
      models: typeof importedEnv.AI?.models,
    },
    moduleScope: {
      fetch: typeof moduleScopeEnv.ai?.fetch,
      run: typeof moduleScopeEnv.ai?.run,
      models: typeof moduleScopeEnv.ai?.models,
    },
    moduleScopeCalls: await moduleScopeAiCalls(),
    hidden: {
      doBackend: typeof moduleScopeEnv.doBackend,
      ownerNetwork: typeof moduleScopeEnv.ownerNetwork,
      workflowsBackend: typeof moduleScopeEnv.workflowsBackend,
    },
  };
}

function aiRequest(path, body, signal) {
  return new Request(`${AI_ORIGIN}${path}`, {
    method: "POST",
    headers: {
      accept: body.stream === true ? "text/event-stream" : "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });
}

function stalledAiRequest() {
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('{"model":"openai/primary","input":"'));
    },
  });
  return new Request(`${AI_ORIGIN}/v1/responses`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
}

function processEnvContainsCredential() {
  if (typeof process !== "object" || !process?.env) return false;
  return Object.values(process.env).some((value) =>
    typeof value === "string" && value.includes("fake-")
  );
}

function closeBridgePeer(peer, event) {
  try {
    if (event.code === 1005 && event.reason === "") {
      peer.close();
      return;
    }
    const code = [1004, 1005, 1006, 1015].includes(event.code) ? 1011 : event.code;
    peer.close(code, event.reason);
  } catch {
    // The opposite close handler may already have completed the handshake.
  }
}

async function bridgedAiWebSocket(env) {
  const upstreamResponse = await env.AI.run("openai/primary", null, { websocket: true });
  const upstream = upstreamResponse.webSocket;
  if (!upstream) throw new Error("AI upgrade did not return a WebSocket");

  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  upstream.binaryType = "arraybuffer";
  server.binaryType = "arraybuffer";
  upstream.accept();
  server.accept();
  upstream.addEventListener("message", (event) => server.send(event.data));
  server.addEventListener("message", (event) => upstream.send(event.data));
  upstream.addEventListener("close", (event) => closeBridgePeer(server, event));
  server.addEventListener("close", (event) => closeBridgePeer(upstream, event));

  return new Response(null, {
    status: 101,
    headers: upstreamResponse.headers,
    webSocket: client,
  });
}

export class AiProbe extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/surface") {
      return Response.json(await aiSurface(this.env));
    }
    if (url.pathname === "/start") {
      const pending = this.env.AI.fetch(aiRequest("/v1/responses", {
        model: "openai/primary",
        input: "hold until the actor is aborted",
        wdl_test_delay_ms: 10_000,
      })).then((response) => response.arrayBuffer());
      this.ctx.waitUntil(pending.catch(() => {}));
      return new Response(null, { status: 202 });
    }
    if (url.pathname === "/abort") {
      this.ctx.abort("AI caller teardown probe");
      return new Response("unreachable");
    }
    if (url.pathname === "/json") {
      return Response.json(await this.env.AI.run("openai/primary", { input: "from durable object" }));
    }
    if (url.pathname === "/imported-json") {
      return Response.json(await importedEnv.AI.run("openai/primary", {
        input: "from durable object imported env",
      }));
    }
    if (url.pathname === "/responses-ws") {
      return await this.env.AI.run("openai/primary", null, { websocket: true });
    }
    return new Response("not found", { status: 404 });
  }
}

async function handler(request, env) {
  const url = new URL(request.url);
  if (url.pathname === "/surface") {
    return Response.json({
      ...await aiSurface(env),
      processEnvContainsCredential: processEnvContainsCredential(),
    });
  }
  if (url.pathname === "/models") {
    return Response.json({ models: await env.AI.models() });
  }
  if (url.pathname === "/imported") {
    const models = await importedEnv.AI.models();
    const result = await importedEnv.AI.run("openai/primary", {
      input: "from imported env",
    });
    return Response.json({
      model: result.model,
      status: result.status,
      models: models.map((model) => model.id),
    });
  }
  if (url.pathname === "/json") {
    const model = url.searchParams.get("model") || "openai/primary";
    return Response.json(await env.AI.run(model, {
      input: "hello",
      tools: [{
        type: "function",
        name: "lookup",
        description: "Look up a value",
        parameters: { type: "object", properties: {}, additionalProperties: false },
      }],
      reasoning: { effort: "low" },
    }));
  }
  if (url.pathname === "/raw") {
    return await env.AI.fetch(aiRequest("/v1/responses", {
      model: url.searchParams.get("model") || "openai/primary",
      input: "raw response",
    }));
  }
  if (url.pathname === "/file") {
    const response = await env.AI.run("openai/primary", {
      input: [{
        role: "user",
        content: [{
          type: "input_file",
          file_url: "https://files.example/input.pdf",
        }],
      }],
      wdl_test_mode: "file_input",
    });
    return Response.json({
      model: response.model,
      type: response.wdl_file_input?.type,
      fileUrl: response.wdl_file_input?.file_url,
    });
  }
  if (url.pathname === "/deepseek-file") {
    try {
      await env.AI.run("deepseek/flash", {
        input: [{ type: "input_file", file_url: "https://files.example/input.pdf" }],
      });
      return Response.json({ status: 200, code: null });
    } catch (error) {
      return Response.json({ status: error?.status, code: error?.code });
    }
  }
  if (url.pathname === "/deepseek-vision") {
    const response = await env.AI.run("deepseek/vision", {
      input: [{
        role: "user",
        content: [
          { type: "input_text", text: "describe the image" },
          {
            type: "input_image",
            image_url: "https://images.example/vision.png",
            detail: "low",
          },
        ],
      }],
      wdl_test_mode: "vision_input",
    });
    return Response.json({
      model: response.model,
      type: response.wdl_vision_input?.type,
      imageUrl: response.wdl_vision_input?.image_url,
      detail: response.wdl_vision_input?.detail,
    });
  }
  if (url.pathname === "/deepseek-audio") {
    try {
      await env.AI.run("deepseek/chat", {
        messages: [{ role: "user", content: "hello from chat completions" }],
        modalities: ["audio"],
      });
      return Response.json({ status: 200, code: null });
    } catch (error) {
      return Response.json({ status: error?.status, code: error?.code });
    }
  }
  if (url.pathname === "/invalid-json") {
    try {
      await env.AI.run("openai/primary", { input: "invalid JSON", wdl_test_mode: "invalid_json" });
      return Response.json({ failed: false });
    } catch (error) {
      return Response.json({ name: error?.name, status: error?.status, code: error?.code });
    }
  }
  if (url.pathname === "/provider-error") {
    try {
      await env.AI.run("openai/primary", {
        input: "provider error",
        wdl_test_mode: "provider_error",
      });
      return Response.json({ failed: false });
    } catch (error) {
      return Response.json({
        name: error?.name,
        status: error?.status,
        code: error?.code,
        message: error?.message,
      });
    }
  }
  if (url.pathname === "/slow-upload") {
    return await env.AI.fetch(stalledAiRequest());
  }
  if (url.pathname === "/chat") {
    return Response.json(await env.AI.run("deepseek/chat", {
      messages: [{ role: "user", content: "hello from chat completions" }],
    }));
  }
  if (url.pathname === "/embedding") {
    return Response.json(await env.AI.run("openai/embedding", {
      input: ["hello from embeddings"],
    }));
  }
  if (url.pathname === "/sse") {
    const stream = await env.AI.run("xai/agent", { input: "stream", stream: true });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  if (url.pathname === "/chat-sse") {
    const stream = await env.AI.run("deepseek/chat", {
      messages: [{ role: "user", content: "stream from chat completions" }],
      stream: true,
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  if (url.pathname === "/sse-error") {
    const stream = await env.AI.run("xai/agent", {
      input: "provider error",
      stream: true,
      wdl_test_mode: "sse_error",
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  if (url.pathname === "/stream-cancel") {
    const stream = await env.AI.run("xai/agent", {
      input: "cancel stream",
      stream: true,
      wdl_test_mode: "open_stream",
    });
    const reader = stream.getReader();
    const first = await reader.read();
    await reader.cancel("integration cancellation");
    return Response.json({
      done: first.done,
      first: new TextDecoder().decode(first.value),
    });
  }
  if (url.pathname === "/stream-idle") {
    const stream = await env.AI.run("xai/agent", {
      input: "idle stream",
      stream: true,
      wdl_test_mode: "open_stream",
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  if (url.pathname === "/abort") {
    const controller = new AbortController();
    const pending = env.AI.run("openai/primary", {
      input: "cancel me",
      wdl_test_delay_ms: 10_000,
    }, { signal: controller.signal });
    setTimeout(() => controller.abort(), 50);
    try {
      await pending;
      return Response.json({ aborted: false });
    } catch (error) {
      return Response.json({
        aborted: true,
        name: error?.name,
        code: error?.code,
        status: error?.status,
      });
    }
  }
  if (url.pathname === "/responses-ws") {
    return await env.AI.run("openai/primary", null, { websocket: true });
  }
  if (url.pathname === "/responses-ws-intrinsic-guard") {
    let modelKeyReads = 0;
    RegExp.prototype.exec = () => null;
    JSON.parse = (text) => {
      if (text === '"model"') {
        modelKeyReads += 1;
        return modelKeyReads === 1 ? "ignored-model" : "model";
      }
      if (text.startsWith("{")) return { type: "response.create", model: "gpt-test" };
      return intrinsicJsonParse(text);
    };
    return await env.AI.run("openai/primary", null, { websocket: true });
  }
  if (url.pathname === "/responses-ws-bridge") {
    return await bridgedAiWebSocket(env);
  }
  if (url.pathname === "/responses-ws-provider-loss") {
    return await env.AI.run("openai/primary", null, { websocket: true });
  }
  if (url.pathname === "/realtime-ws") {
    return await env.AI.run("xai/realtime", null, { websocket: true });
  }
  if (url.pathname.startsWith("/do/")) {
    const name = url.searchParams.get("name") || "main";
    const id = env.AI_PROBE.idFromName(name);
    const target = new URL(request.url);
    target.pathname = url.pathname.slice(3);
    return await env.AI_PROBE.get(id).fetch(new Request(target, request));
  }
  return new Response("not found", { status: 404 });
}

export default { fetch: handler };
