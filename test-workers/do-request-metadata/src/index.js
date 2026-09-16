import { DurableObject } from "cloudflare:workers";
import { Buffer } from "node:buffer";

const encoder = new TextEncoder();

export class MetadataEcho extends DurableObject {
  async fetch(request) {
    const value = request.headers.get("cf-mcp-message") ?? "";
    const bytes = encoder.encode(value);
    const metadata = {
      headerBytes: bytes.byteLength,
      headerHash: Buffer.from(await crypto.subtle.digest("SHA-256", bytes)).toString("hex"),
      urlBytes: encoder.encode(request.url).byteLength,
      internalAuthVisible: request.headers.has("x-wdl-internal-auth"),
    };
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return Response.json(metadata);
    }
    const { 0: client, 1: server } = new WebSocketPair();
    server.accept();
    server.addEventListener("message", () => server.send(JSON.stringify(metadata)));
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(request, env) {
    const { message, urlBytes, websocket = false } = await request.json();
    const prefix = "https://do.internal/metadata?pad=";
    const url = prefix + "a".repeat(urlBytes - prefix.length);
    // McpAgent's Streamable HTTP bridge carries its message array in an upgrade header.
    const headers = {
      "cf-mcp-message": Buffer.from(JSON.stringify([message])).toString("base64"),
      ...(websocket ? { Upgrade: "websocket" } : {}),
    };
    const stub = env.ECHO.get(env.ECHO.idFromName("metadata"));
    const response = await stub.fetch(url, { headers });
    if (!websocket || response.status !== 101) return response;

    const socket = response.webSocket;
    const received = Promise.withResolvers();
    socket.accept();
    socket.addEventListener("message", (event) => received.resolve(JSON.parse(event.data)));
    socket.addEventListener("error", () => received.reject(new Error("metadata WebSocket failed")));
    socket.addEventListener("close", () => received.reject(new Error("metadata WebSocket closed before its reply")));
    const timer = setTimeout(() => received.reject(new Error("metadata WebSocket timed out")), 10000);
    try {
      socket.send("metadata");
      return Response.json({ ...await received.promise, transport: "websocket" });
    } finally {
      clearTimeout(timer);
      socket.close(1000, "done");
    }
  },
};
