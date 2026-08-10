import { DurableObject } from "cloudflare:workers";

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.evictionBuildLabel = env.EVICTION_BUILD_LABEL || "fixture";
    this.evictionHttpHits = 0;
    this.evictionWebSocketMessages = 0;
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.endsWith("/auto-response")) {
      const pair = this.ctx.getWebSocketAutoResponse();
      return Response.json(pair ? {
        request: pair.request,
        response: pair.response,
      } : null);
    }
    if (url.pathname.endsWith("/status")) {
      const sql = this.ctx.storage.sql;
      sql.exec("CREATE TABLE IF NOT EXISTS ws_close_events (name TEXT PRIMARY KEY, code INTEGER, reason TEXT, clean INTEGER, attachment_id TEXT)");
      const row = [...sql.exec("SELECT code, reason, clean, attachment_id FROM ws_close_events WHERE name = ?", "last")][0];
      return Response.json(row ? {
        code: row.code,
        reason: row.reason,
        clean: row.clean === 1,
        attachmentId: row.attachment_id,
      } : null);
    }
    if (url.pathname.endsWith("/eviction-counter")) {
      this.evictionHttpHits += 1;
      const sql = this.ctx.storage.sql;
      sql.exec("CREATE TABLE IF NOT EXISTS eviction_counter (name TEXT PRIMARY KEY, hits INTEGER NOT NULL)");
      sql.exec(
        "INSERT INTO eviction_counter (name, hits) VALUES (?, 1) " +
          "ON CONFLICT(name) DO UPDATE SET hits = hits + 1",
        "http"
      );
      const row = [...sql.exec("SELECT hits FROM eviction_counter WHERE name = ?", "http")][0];
      return Response.json({
        buildLabel: this.evictionBuildLabel,
        memoryHits: this.evictionHttpHits,
        storageHits: row.hits,
      });
    }
    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      return new Response("need websocket", { status: 426 });
    }
    const tags = url.searchParams.getAll("tag");
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server, tags.length ? tags : ["room"]);
    server.serializeAttachment({
      id: String(this.ctx.id),
      joinedAt: 123,
      seen: 0,
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    if (message instanceof ArrayBuffer) {
      ws.send(message);
      return;
    }
    const attachment = ws.deserializeAttachment();
    if (String(message) === "eviction-probe") {
      this.evictionWebSocketMessages += 1;
      const next = {
        ...attachment,
        seen: (attachment?.seen || 0) + 1,
      };
      ws.serializeAttachment(next);
      ws.send(JSON.stringify({
        buildLabel: this.evictionBuildLabel,
        id: next.id,
        joinedAt: next.joinedAt,
        seen: next.seen,
        tags: this.ctx.getTags(ws),
        allSockets: this.ctx.getWebSockets().length,
        memoryMessages: this.evictionWebSocketMessages,
      }));
      return;
    }
    if (String(message) === "bump") {
      const next = {
        ...attachment,
        seen: (attachment?.seen || 0) + 1,
      };
      ws.serializeAttachment(next);
      ws.send(JSON.stringify({
        id: next.id,
        seen: next.seen,
        tags: this.ctx.getTags(ws),
      }));
      return;
    }
    ws.send(JSON.stringify({
      id: attachment.id,
      joinedAt: attachment.joinedAt,
      seen: attachment.seen,
      tags: this.ctx.getTags(ws),
      roomSockets: this.ctx.getWebSockets("room").length,
      vipSockets: this.ctx.getWebSockets("vip").length,
      allSockets: this.ctx.getWebSockets().length,
      text: String(message),
    }));
  }

  async webSocketClose(ws, code, reason, wasClean) {
    const attachment = ws.deserializeAttachment();
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS ws_close_events (name TEXT PRIMARY KEY, code INTEGER, reason TEXT, clean INTEGER, attachment_id TEXT)");
    sql.exec(
      "INSERT INTO ws_close_events (name, code, reason, clean, attachment_id) VALUES (?, ?, ?, ?, ?) " +
        "ON CONFLICT(name) DO UPDATE SET code = excluded.code, reason = excluded.reason, clean = excluded.clean, attachment_id = excluded.attachment_id",
      "last",
      code,
      reason,
      wasClean ? 1 : 0,
      attachment?.id || null
    );
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.ROOM.idFromName(url.searchParams.get("name") || "main");
    if (url.pathname.endsWith("/auto-response")) {
      return await env.ROOM.get(id).fetch("https://do.local/auto-response");
    }
    if (url.pathname.endsWith("/status")) {
      return await env.ROOM.get(id).fetch("https://do.local/status");
    }
    return await env.ROOM.get(id).fetch(request);
  },
};
