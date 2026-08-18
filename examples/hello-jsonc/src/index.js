export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    return Response.json({
      greeting: env.GREETING,
      path: url.pathname,
      configFormat: "wrangler.jsonc",
    });
  },
};
