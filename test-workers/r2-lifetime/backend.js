const calls = new Map();
const uploads = new Map();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const key = url.pathname.split("/").at(-1);
    if (key === "stats") return Response.json(Object.fromEntries(calls));
    if (key === "uploaded") return Response.json(uploads.get(url.searchParams.get("key")));
    calls.set(key, (calls.get(key) || 0) + 1);
    if (request.method === "PUT" && key !== "put-stall" && key !== "blob-stall") {
      const bytes = new Uint8Array(await request.arrayBuffer());
      uploads.set(key, {
        attempts: calls.get(key), size: bytes.byteLength,
        text: new TextDecoder().decode(bytes.subarray(0, 100)),
        contentType: request.headers.get("content-type"),
        payloadHash: request.headers.get("x-amz-content-sha256"),
      });
      if (key === "blob-retry" && calls.get(key) === 1) return new Response(null, { status: 429 });
    }
    if (key === "header-stall" || key === "put-stall" || key === "blob-stall") {
      if (request.method === "PUT") await request.arrayBuffer();
      await delay(5 * 60_000);
    }
    if (key === "body-stall") {
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode("prefix")); },
        async pull() { await delay(5 * 60_000); },
      }));
    }
    if (key === "slow-consumer") return new Response(new Uint8Array(2 * 1024 * 1024).fill(7));
    return new Response("ok", { headers: { etag: '"fixture"' } });
  },
};
