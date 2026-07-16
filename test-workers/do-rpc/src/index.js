import { DurableObject } from "cloudflare:workers";

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.memory = 0;
  }

  addMessage(text, meta) {
    this.memory += 1;
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL)");
    sql.exec("INSERT INTO messages(text) VALUES (?)", text);
    const stored = [...sql.exec("SELECT COUNT(*) AS count FROM messages")][0]?.count ?? 0;
    return {
      objectId: String(this.ctx.id),
      memory: this.memory,
      stored,
      text,
      meta,
    };
  }

  fail() {
    throw new TypeError("room-rpc-failed");
  }

  throwString() {
    throw "room-rpc-string-failed";
  }

  throwObject() {
    throw { custom: 1 };
  }

  returnUndefined() {
    return undefined;
  }

  async forwardMessage(targetName, text) {
    const target = this.env.ROOM.get(this.env.ROOM.idFromName(targetName));
    return {
      forwardedBy: String(this.ctx.id),
      result: await target.addMessage(text, { role: "peer" }),
    };
  }

  async forwardRequestId(targetName) {
    this.env.ROOM.requestId = () => "tenant-rid";
    const target = this.env.ROOM.get(this.env.ROOM.idFromName(targetName));
    const response = await target.fetch(new Request("https://do.internal/request-id"));
    return response.text();
  }

  async nestedForwardRequestId(targetName) {
    const response = await this.fetch(new Request(
      `https://do.internal/forward-request-id?to=${encodeURIComponent(targetName)}`
    ));
    return response.text();
  }

  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/forward-request-id") {
      const target = this.env.ROOM.get(this.env.ROOM.idFromName(
        url.searchParams.get("to") || "peer"
      ));
      return target.fetch(new Request("https://do.internal/request-id"));
    }
    return new Response(request.headers.get("x-request-id") || "");
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.ROOM.idFromName(url.searchParams.get("name") || "main");
    const stub = env.ROOM.get(id);
    if (url.pathname === "/fail") {
      try {
        await stub.fail();
        return Response.json({ ok: false }, { status: 500 });
      } catch (err) {
        return Response.json({ name: err.name, message: err.message, code: err.code || null }, { status: 500 });
      }
    }
    if (url.pathname === "/throw-string") {
      try {
        await stub.throwString();
        return Response.json({ ok: false }, { status: 500 });
      } catch (err) {
        return Response.json({ name: err.name, message: err.message, code: err.code || null }, { status: 500 });
      }
    }
    if (url.pathname === "/throw-object") {
      try {
        await stub.throwObject();
        return Response.json({ ok: false }, { status: 500 });
      } catch (err) {
        return Response.json({ name: err.name, message: err.message, code: err.code || null }, { status: 500 });
      }
    }
    if (url.pathname === "/undefined") {
      const result = await stub.returnUndefined();
      return Response.json({ hasResult: result !== undefined, result: result ?? null });
    }
    if (url.pathname === "/forward") {
      return Response.json(await stub.forwardMessage(
        url.searchParams.get("to") || "peer",
        "forwarded"
      ));
    }
    if (url.pathname === "/request-id") {
      return Response.json({
        requestId: await stub.forwardRequestId(url.searchParams.get("to") || "peer"),
      });
    }
    if (url.pathname === "/nested-request-id") {
      return Response.json({
        requestId: await stub.nestedForwardRequestId(url.searchParams.get("to") || "peer"),
      });
    }
    return Response.json(await stub.addMessage("hello", { role: "user" }));
  },
};
