export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const size = Number(url.searchParams.get("size"));
    const type = url.searchParams.get("type") || "text";
    let body;
    let contentType = type;
    if (type === "view") {
      body = new DataView(new ArrayBuffer(size + 32), 16, size);
      contentType = "bytes";
    } else if (type === "bytes") {
      body = new Uint8Array(size);
    } else if (type === "unicode") {
      body = "\u4e2d".repeat(Math.floor(size / 3)) + "a".repeat(size % 3);
      contentType = "text";
    } else {
      body = "x".repeat(type === "json" ? size - 2 : size);
    }
    try {
      if (url.pathname === "/batch") {
        await env.QUEUE.sendBatch([
          { body: new Uint8Array(128_000), contentType: "bytes" },
          { body: new Uint8Array(128_000), contentType: "bytes" },
          { body, contentType },
        ]);
      } else {
        await env.QUEUE.send(body, { contentType });
      }
      return Response.json({ accepted: true });
    } catch (error) {
      return Response.json({ error: "queue_send_failed", message: error.message }, { status: 400 });
    }
  },
};
