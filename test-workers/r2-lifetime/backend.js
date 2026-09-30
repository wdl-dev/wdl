const calls = new Map();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default {
  async fetch(request) {
    const key = new URL(request.url).pathname.split("/").at(-1);
    if (key === "stats") return Response.json(Object.fromEntries(calls));
    calls.set(key, (calls.get(key) || 0) + 1);
    if (key === "header-stall" || key === "put-stall") {
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
