import OpenAI from "openai";
import { env as importedEnv } from "cloudflare:workers";

function bindingSurface(binding) {
  return {
    fetch: typeof binding?.fetch,
    run: typeof binding?.run,
    models: typeof binding?.models,
  };
}

function client(env) {
  return new OpenAI({
    apiKey: "tenant-placeholder",
    baseURL: "https://ai.wdl/v1",
    fetch: env.AI.fetch.bind(env.AI),
    dangerouslyAllowBrowser: true,
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/surface") {
      return Response.json({
        handler: bindingSurface(env.AI),
        imported: bindingSurface(importedEnv.AI),
      });
    }
    const openai = client(env);
    if (url.pathname === "/json") {
      const response = await openai.responses.create({
        model: "openai/primary",
        input: "official SDK JSON",
        tools: [{
          type: "function",
          name: "lookup",
          description: "Look up a value",
          parameters: { type: "object", properties: {}, additionalProperties: false },
        }],
      });
      return Response.json({ id: response.id, model: response.model, status: response.status });
    }
    if (url.pathname === "/stream") {
      const stream = await openai.responses.create({
        model: "openai/primary",
        input: "official SDK stream",
        stream: true,
      });
      const eventTypes = [];
      for await (const event of stream) eventTypes.push(event.type);
      return Response.json({ eventTypes });
    }
    if (url.pathname === "/abort") {
      const controller = new AbortController();
      const pending = openai.responses.create({
        model: "openai/primary",
        input: "official SDK abort",
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
          constructor: error?.constructor?.name,
          message: error?.message,
        });
      }
    }
    return new Response("not found", { status: 404 });
  },
};
