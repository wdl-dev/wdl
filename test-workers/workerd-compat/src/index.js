const moduleClock = {
  dateNow: Date.now(),
  dateValue: new Date().valueOf(),
  performanceNow: performance.now(),
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.searchParams.get("abort") === "1") {
      ctx.abort(new Error("workerd compatibility abort probe"));
      return new Response("unreachable");
    }

    const span = ctx.tracing.startSpan("wdl-workerd-compat-probe");
    const setAttributeChained = span.setAttribute("probe", "one") === span;
    const setAttributesChained = span.setAttributes({ second: 2, omitted: undefined }) === span;
    span.end();

    return Response.json({
      moduleClock,
      requestClock: {
        dateNow: Date.now(),
        dateValue: new Date().valueOf(),
        performanceNow: performance.now(),
      },
      abortType: typeof ctx.abort,
      tracing: {
        startSpanType: typeof ctx.tracing.startSpan,
        setAttributeChained,
        setAttributesChained,
      },
      nodeGlobals: {
        Buffer: typeof Buffer,
        process: typeof process,
        global: typeof global,
        setImmediate: typeof setImmediate,
      },
    });
  },
};
