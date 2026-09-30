export const S3_REQUEST_TIMEOUT_MS = 60_000;
export const S3_STREAM_READ_TIMEOUT_MS = 30_000;
export const S3_STREAM_IDLE_TIMEOUT_MS = 5 * 60_000;
export const S3_ERROR_BODY_MAX_BYTES = 4 * 1024;

/**
 * One deadline covers signing, transport, retry waits, and result construction.
 * A successful streaming GET transfers body ownership to streamS3Response().
 * @template T
 * @param {(aborter: AbortController) => Promise<T>} callback
 * @param {number} [timeoutMs]
 * @returns {Promise<T>}
 */
export async function withS3Request(callback, timeoutMs = S3_REQUEST_TIMEOUT_MS) {
  const aborter = new AbortController();
  const deadline = Date.now() + timeoutMs;
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timer;
  const timeoutError = () => new DOMException("S3 operation deadline exceeded", "TimeoutError");
  const expired = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = timeoutError();
      aborter.abort(error);
      reject(error);
    }, timeoutMs);
  });
  try {
    const result = await Promise.race([callback(aborter), expired]);
    if (Date.now() >= deadline && !aborter.signal.aborted) aborter.abort(timeoutError());
    aborter.signal.throwIfAborted();
    return /** @type {T} */ (result);
  } catch (error) {
    aborter.abort(error);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Reclaim inactive GET bodies even when the caller never reads or cancels them.
 * Non-empty chunks extend the idle deadline; active streams have no total limit.
 * The read timer runs only while awaiting upstream, not during backpressure.
 * @param {Response} response
 * @param {AbortController} aborter
 * @param {{ waitUntil(promise: Promise<unknown>): void } | null | undefined} ctx
 * @returns {ReadableStream<Uint8Array>}
 */
export function streamS3Response(response, aborter, ctx) {
  if (!response.body) return new ReadableStream({ start(controller) { controller.close(); } });
  const reader = response.body.getReader();
  const { promise, resolve } = Promise.withResolvers();
  let idleDeadline = Date.now() + S3_STREAM_IDLE_TIMEOUT_MS;
  let closed = false;
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let idleTimer;
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let readTimer;
  /** @type {ReadableStreamDefaultController<Uint8Array> | undefined} */
  let output;
  const idleError = () => new DOMException("S3 response body idle timeout", "TimeoutError");
  const readError = () => new DOMException("S3 response body read timed out", "TimeoutError");
  const cleanup = () => {
    closed = true;
    clearTimeout(idleTimer);
    idleTimer = undefined;
    clearTimeout(readTimer);
    aborter.signal.removeEventListener("abort", aborted);
    try { reader.releaseLock(); } catch {}
    resolve(undefined);
  };
  /** @param {unknown} reason */
  const stop = (reason) => {
    if (closed) return;
    closed = true;
    aborter.signal.removeEventListener("abort", aborted);
    aborter.abort(reason);
    try { void reader.cancel(reason).catch(() => {}); } catch {}
    cleanup();
  };
  /** @param {unknown} error */
  const fail = (error) => {
    if (closed) return;
    output?.error(error);
    stop(error);
  };
  const aborted = () => fail(aborter.signal.reason);
  // Progress updates a timestamp, without replacing the watchdog on every chunk.
  const checkIdle = () => {
    if (closed) return;
    const remaining = idleDeadline - Date.now();
    if (remaining <= 0) fail(idleError());
    else idleTimer = setTimeout(checkIdle, remaining);
  };
  try {
    if (!ctx) throw new Error("S3 streaming response requires a request context");
    ctx.waitUntil(promise);
    aborter.signal.throwIfAborted();
  } catch (error) {
    stop(error);
    throw error;
  }
  idleTimer = setTimeout(checkIdle, S3_STREAM_IDLE_TIMEOUT_MS);
  aborter.signal.addEventListener("abort", aborted, { once: true });
  return new ReadableStream({
    start(controller) { output = controller; },
    async pull(controller) {
      const started = Date.now();
      if (started >= idleDeadline) {
        fail(idleError());
        return;
      }
      const readDeadline = started + S3_STREAM_READ_TIMEOUT_MS;
      readTimer = setTimeout(() => fail(readError()), S3_STREAM_READ_TIMEOUT_MS);
      try {
        const { done, value } = await reader.read();
        if (closed) return;
        const now = Date.now();
        if (now >= idleDeadline) throw idleError();
        if (now >= readDeadline) throw readError();
        if (done) {
          cleanup();
          controller.close();
        } else {
          if (value.byteLength > 0) idleDeadline = now + S3_STREAM_IDLE_TIMEOUT_MS;
          controller.enqueue(value);
        }
      } catch (error) {
        fail(error);
      } finally {
        clearTimeout(readTimer);
      }
    },
    cancel(reason) { stop(reason); },
  }, { highWaterMark: 0 });
}
