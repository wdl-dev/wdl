import { DurableObject, env as importedEnv } from "cloudflare:workers";

const AI_ORIGIN = "https://ai.wdl";

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

export class AiProbe extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
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
    return new Response("not found", { status: 404 });
  }
}

async function handler(request, env) {
  const url = new URL(request.url);
  if (url.pathname === "/surface") {
    return Response.json({
      handler: {
        fetch: typeof env.AI.fetch,
        run: typeof env.AI.run,
        models: typeof env.AI.models,
      },
      imported: {
        fetch: typeof importedEnv.AI.fetch,
        run: typeof importedEnv.AI.run,
        models: typeof importedEnv.AI.models,
      },
      processEnvContainsCredential: processEnvContainsCredential(),
    });
  }
  if (url.pathname === "/models") {
    return Response.json({ models: await env.AI.models() });
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
      model: "openai/primary",
      input: "raw response",
    }));
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
