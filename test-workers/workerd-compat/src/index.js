const moduleClock = {
  dateNow: Date.now(),
  dateValue: new Date().valueOf(),
  performanceNow: performance.now(),
};

async function tracingProbe(ctx) {
  const previousSpan = ctx.tracing.getActiveSpan();
  const span = ctx.tracing.startSpan("wdl-workerd-compat-probe");
  const setAttributeChained = span.setAttribute("probe", "one") === span;
  const setAttributesChained = span.setAttributes({ second: 2, omitted: undefined }) === span;
  const startSpanPreservesActive = ctx.tracing.getActiveSpan() === previousSpan;
  span.end();

  const pending = ctx.tracing.startActiveSpan("wdl-workerd-compat-active", async (activeSpan) => {
    try {
      const beforeAwait = ctx.tracing.getActiveSpan() === activeSpan;
      await Promise.resolve();
      const afterAwait = ctx.tracing.getActiveSpan() === activeSpan;

      // Fixed, bounded inputs probe acceptance, not exported exception payloads.
      const handledError = new Error("workerd compatibility handled error");
      handledError.stack = "Error: workerd compatibility handled error";
      let errorReturnedVoid = false;
      try {
        throw handledError;
      } catch (error) {
        errorReturnedVoid = activeSpan.recordException(error) === undefined;
      }
      const stringReturnedVoid = activeSpan.recordException("workerd compatibility string") === undefined;
      const codeZeroReturnedVoid = activeSpan.recordException({ code: 0 }) === undefined;
      return {
        beforeAwait,
        afterAwait,
        recordException: { errorReturnedVoid, stringReturnedVoid, codeZeroReturnedVoid },
      };
    } finally {
      activeSpan.end();
    }
  });
  const callerPreservedWhilePending = ctx.tracing.getActiveSpan() === previousSpan;
  const activeSpanResult = await pending;
  return {
    invocationSpanPresent: previousSpan !== undefined,
    startSpanType: typeof ctx.tracing.startSpan,
    setAttributeChained,
    setAttributesChained,
    startSpanPreservesActive,
    activeSpan: {
      ...activeSpanResult,
      callerPreservedWhilePending,
      callerRestoredAfterAwait: ctx.tracing.getActiveSpan() === previousSpan,
    },
  };
}

function listenerExceptionProbe(target, type, dispatch) {
  const listenerError = new Error(`workerd compatibility ${type} listener`);
  const listeners = [];
  const reports = [];
  target.addEventListener(type, () => {
    listeners.push("first");
    throw listenerError;
  });
  target.addEventListener(type, () => {
    listeners.push("second");
  });
  const onError = (event) => {
    reports.push(event.error === listenerError);
    if (event.error === listenerError) event.preventDefault();
  };
  let threw = false;
  let caughtSameError = false;
  let dispatchResult = null;
  // Reporting is synchronous; never retain this global listener across an await.
  globalThis.addEventListener("error", onError);
  try {
    dispatchResult = dispatch() ?? null;
  } catch (error) {
    threw = true;
    caughtSameError = error === listenerError;
  } finally {
    globalThis.removeEventListener("error", onError);
  }
  return { listeners, reports, threw, caughtSameError, dispatchResult };
}

function listenerExceptionsProbe() {
  const target = new EventTarget();
  const eventTarget = listenerExceptionProbe(
    target, "probe", () => target.dispatchEvent(new Event("probe"))
  );
  const controller = new AbortController();
  const reason = new Error("workerd compatibility abort reason");
  const abortSignal = listenerExceptionProbe(
    controller.signal, "abort", () => controller.abort(reason)
  );
  return {
    eventTarget,
    abortSignal: {
      ...abortSignal,
      aborted: controller.signal.aborted,
      reasonPreserved: controller.signal.reason === reason,
    },
  };
}

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

    const tracing = await tracingProbe(ctx);

    return Response.json({
      moduleClock,
      requestClock: {
        dateNow: Date.now(),
        dateValue: new Date().valueOf(),
        performanceNow: performance.now(),
      },
      abortType: typeof ctx.abort,
      tracing,
      listenerExceptions: listenerExceptionsProbe(),
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
