const calls = new Map();
const uploads = new Map();
const cancellations = new Map();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);
    const key = url.pathname.split("/").at(-1);
    if (key === "stats") return Response.json(Object.fromEntries(calls));
    if (key === "uploaded") return Response.json(uploads.get(url.searchParams.get("key")));
    if (key === "cancellations") return Response.json(Object.fromEntries(cancellations));
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
    if (key === "cancelled-single" || key === "cancelled-tee") {
      const finished = Promise.withResolvers();
      // Keep the source request alive independently of its idle stream, but
      // bound a failed probe without counting that timeout as cancellation.
      const timer = setTimeout(() => finished.resolve(), 10_000);
      ctx.waitUntil(finished.promise.finally(() => clearTimeout(timer)));
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(Uint8Array.of(7)); },
        cancel() {
          cancellations.set(key, (cancellations.get(key) || 0) + 1);
          finished.resolve();
        },
      }));
    }
    if (key === "slow-consumer") return new Response(new Uint8Array(2 * 1024 * 1024).fill(7));
    return new Response("ok", { headers: { etag: '"fixture"' } });
  },
};
