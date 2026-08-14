const moduleClock = {
  dateNow: Date.now(),
  dateValue: new Date().valueOf(),
  performanceNow: performance.now(),
};

async function byobProbe() {
  const stream = new ReadableStream({
    type: "bytes",
    start(controller) {
      controller.enqueue(Uint8Array.of(1, 2, 3, 4));
      controller.close();
    },
  });
  const reader = stream.getReader({ mode: "byob" });
  const first = await reader.read(new Uint8Array(8));
  const final = await reader.read(new Uint8Array(8));
  return {
    firstDone: first.done,
    firstBytes: Array.from(first.value),
    finalDone: final.done,
    finalBytes: Array.from(final.value),
  };
}

function eventTargetSelfSignalProbe() {
  const controller = new AbortController();
  const { signal } = controller;
  const noop = () => {};
  let victimCalls = 0;
  const victim = () => { victimCalls += 1; };

  // Distinct event types must not corrupt a self-signal listener or its removal.
  signal.addEventListener("one", noop);
  signal.addEventListener("two", noop);
  signal.addEventListener("three", noop);
  signal.addEventListener("victim", victim, { signal });
  signal.dispatchEvent(new Event("victim"));
  controller.abort();
  signal.dispatchEvent(new Event("victim"));
  return { aborted: signal.aborted, victimCalls };
}

async function htmlRewriterProbe() {
  let uppercaseAttributeMatches = 0;
  const response = new HTMLRewriter()
    .on("[HREF]", {
      element() {
        uppercaseAttributeMatches += 1;
      },
    })
    .transform(new Response('<a href="/">link</a>'));
  await response.text();
  return { uppercaseAttributeMatches };
}

function pendingInternalByobResponse(request, mode) {
  if (!request.body) throw new TypeError("request body is required");
  const reader = request.body.getReader({ mode: "byob" });
  const buffer = new ArrayBuffer(16, { maxByteLength: 16 });
  const view = new Uint8Array(buffer, 4, 8);
  const pending = reader.read(view);
  let transferredByteLength = null;
  if (mode === "resize") {
    buffer.resize(6);
  } else if (mode === "transfer") {
    const transferred = structuredClone(buffer, { transfer: [buffer] });
    transferredByteLength = transferred.byteLength;
  } else {
    throw new TypeError("unsupported pending BYOB probe mode");
  }
  const body = new ReadableStream({
    async start(controller) {
      try {
        const result = await pending;
        controller.enqueue(new TextEncoder().encode(JSON.stringify({
          done: result.done,
          bytes: Array.from(result.value),
          resultByteOffset: result.value.byteOffset,
          resultBufferByteLength: result.value.buffer.byteLength,
          originalBufferByteLength: buffer.byteLength,
          transferredByteLength,
        })));
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
  });
  return new Response(body, {
    headers: {
      "content-type": "application/json",
      "x-byob-read-pending": "1",
    },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pendingByobMode = url.searchParams.get("pendingByob");
    if (pendingByobMode) {
      return pendingInternalByobResponse(request, pendingByobMode);
    }
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
      eventTargetSelfSignal: eventTargetSelfSignalProbe(),
      htmlRewriter: await htmlRewriterProbe(),
      byob: await byobProbe(),
      importMetaPathHelpers: {
        dirname: typeof Reflect.get(import.meta, "dirname"),
        filename: typeof Reflect.get(import.meta, "filename"),
      },
      nodeGlobals: {
        Buffer: typeof Buffer,
        process: typeof process,
        global: typeof global,
        setImmediate: typeof setImmediate,
      },
      urlParsing: {
        nonUts46XnLabel: new URL("https://XN--pokxncvks/").hostname,
      },
    });
  },
};
