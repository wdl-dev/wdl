export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const op = url.searchParams.get("op");
    const key = url.searchParams.get("key") || "";
    const val = url.searchParams.get("val");
    const meta = url.searchParams.get("meta");
    const kv = env.KV;
    try {
      if (op === "putOversizedStream") {
        let cancellations = 0;
        let remaining = Number(url.searchParams.get("bytes"));
        if (!Number.isSafeInteger(remaining) || remaining < 1) throw new Error("invalid probe byte count");
        let controller;
        const chunk = new Uint8Array(1024 * 1024);
        const source = new ReadableStream({
          start(value) { controller = value; },
          pull(value) {
            if (remaining > 0) {
              const size = Math.min(remaining, chunk.byteLength);
              value.enqueue(chunk.subarray(0, size));
              remaining -= size;
            }
          },
          cancel() { cancellations++; },
        });
        const timer = setTimeout(() => controller.error(new Error("KV source probe timed out")), 10_000);
        let error = null;
        try {
          await kv.put(key, source);
        } catch (caught) {
          error = caught.message;
        } finally {
          clearTimeout(timer);
        }
        const deadline = Date.now() + 5_000;
        while (cancellations === 0 && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        return Response.json({ error, cancellations, stored: await kv.get(key) });
      }
      if (op === "put") {
        const opts = {};
        if (meta) opts.metadata = JSON.parse(meta);
        const ttl = url.searchParams.get("ttl");
        if (ttl) opts.expirationTtl = parseInt(ttl, 10);
        const exp = url.searchParams.get("expiration");
        if (exp) opts.expiration = parseInt(exp, 10);
        await kv.put(key, val, Object.keys(opts).length ? opts : undefined);
        return new Response("ok");
      }
      if (op === "get") {
        const type = url.searchParams.get("type");
        const v = await kv.get(key, type || undefined);
        if (v === null) return new Response("__null__");
        if (type === "arrayBuffer") {
          return new Response(v, { headers: { "content-type": "application/octet-stream" }});
        }
        if (type === "json") return Response.json(v);
        return new Response(v);
      }
      if (op === "getBatch") {
        const type = url.searchParams.get("type");
        const keys = JSON.parse(url.searchParams.get("keys") || "[]");
        const values = await kv.get(keys, type || undefined);
        return Response.json([...values.entries()]);
      }
      if (op === "getMeta") {
        const type = url.searchParams.get("type");
        const r = await kv.getWithMetadata(key, type || undefined);
        return Response.json(r);
      }
      if (op === "getMetaBatch") {
        const type = url.searchParams.get("type");
        const keys = JSON.parse(url.searchParams.get("keys") || "[]");
        const values = await kv.getWithMetadata(keys, type || undefined);
        return Response.json([...values.entries()]);
      }
      if (op === "del") {
        await kv.delete(key);
        return new Response("ok");
      }
      if (op === "list") {
        const prefix = url.searchParams.get("prefix") || "";
        const limit = url.searchParams.get("limit");
        const cursor = url.searchParams.get("cursor");
        const opts = { prefix };
        if (limit) opts.limit = parseInt(limit, 10);
        if (cursor) opts.cursor = cursor;
        if (url.searchParams.get("metadata") === "true") opts.metadata = true;
        const r = await kv.list(opts);
        return Response.json(r);
      }
      return new Response("bad op", { status: 400 });
    } catch (err) {
      return new Response("err: " + err.message, { status: 500 });
    }
  }
};
