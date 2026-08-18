// Manual HTTP, WebSocket, streaming, and cancellation smoke worker.
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    if (path === "/") {
      return Response.json({
        ok: true,
        worker: "ws-echo",
        workerId: request.headers.get("x-worker-id"),
        requestId: request.headers.get("x-request-id"),
        path: url.pathname,
      });
    }

    if (path === "/ws") {
      if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
        return new Response("expected websocket upgrade", { status: 426 });
      }
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      server.accept();
      server.addEventListener("message", (event) => {
        const data = typeof event.data === "string" ? event.data : "<binary>";
        if (data === "bye") {
          server.close(1000, "bye");
          return;
        }
        server.send(`echo:${data}`);
      });
      server.addEventListener("close", () => {
        console.log("ws-echo: server close");
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    if (path === "/stream") {
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        async start(controller) {
          for (let index = 0; index < 10; index += 1) {
            controller.enqueue(encoder.encode(`data: chunk ${index} @ ${Date.now()}\n\n`));
            await new Promise((resolve) => setTimeout(resolve, 150));
          }
          controller.close();
        },
      });
      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
        },
      });
    }

    if (path === "/wait") {
      const encoder = new TextEncoder();
      const { promise: outcome, resolve: resolveOutcome } = Promise.withResolvers();
      // Register up-front because scheduling from cancel races IoContext teardown.
      ctx.waitUntil((async () => {
        const state = await outcome;
        console.log(`ws-echo: /wait outcome=${state}`);
      })());
      const stream = new ReadableStream({
        async start(controller) {
          controller.enqueue(encoder.encode("ready\n"));
          for (let index = 0; index < 600; index += 1) {
            await new Promise((resolve) => setTimeout(resolve, 100));
            try {
              controller.enqueue(encoder.encode(`tick:${index}\n`));
            } catch {
              resolveOutcome("enqueue-threw");
              return;
            }
          }
          try {
            controller.close();
          } catch {}
          resolveOutcome("ended-normally");
        },
        cancel() {
          resolveOutcome("cancel");
        },
      });
      return new Response(stream, { headers: { "content-type": "text/plain" } });
    }

    return new Response("not found", { status: 404 });
  },
};
