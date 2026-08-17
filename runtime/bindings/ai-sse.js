import { errorMessage } from "shared-errors";

const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Incremental SSE frame buffer. Provider bytes are scanned once and retained in a
 * geometrically grown buffer until a complete frame can be copied to the tenant.
 *
 * @param {number} maxFrameBytes
 */
function createFrameBuffer(maxFrameBytes) {
  let bytes = new Uint8Array();
  let start = 0;
  let end = 0;
  let scan = 0;
  let lineStart = 0;

  /** @param {number} additional */
  const ensureCapacity = (additional) => {
    if (additional <= bytes.byteLength - end) return;
    const active = end - start;
    if (active + additional <= bytes.byteLength) {
      bytes.copyWithin(0, start, end);
      end = active;
      scan -= start;
      lineStart -= start;
      start = 0;
      return;
    }
    let capacity = Math.max(4096, bytes.byteLength);
    const required = active + additional;
    while (capacity < required) capacity *= 2;
    const next = new Uint8Array(capacity);
    next.set(bytes.subarray(start, end));
    scan -= start;
    lineStart -= start;
    start = 0;
    end = active;
    bytes = next;
  };

  /** @param {Uint8Array} chunk */
  const append = (chunk) => {
    ensureCapacity(chunk.byteLength);
    bytes.set(chunk, end);
    end += chunk.byteLength;
  };

  /** @param {boolean} flushTrailingCr */
  const findFrameEnd = (flushTrailingCr) => {
    let index = scan;
    while (index < end) {
      const byte = bytes[index];
      if (byte !== 0x0a && byte !== 0x0d) {
        index += 1;
        continue;
      }
      if (byte === 0x0d && index + 1 >= end && !flushTrailingCr) {
        scan = index;
        return -1;
      }
      const lineEnd = byte === 0x0d && bytes[index + 1] === 0x0a ? index + 2 : index + 1;
      if (index === lineStart) {
        scan = lineEnd;
        return lineEnd;
      }
      lineStart = lineEnd;
      index = lineEnd;
    }
    scan = index;
    return -1;
  };

  /** @param {boolean} [flushTrailingCr] */
  const take = (flushTrailingCr = false) => {
    const frameEnd = findFrameEnd(flushTrailingCr);
    if (frameEnd < 0) {
      if (end - start > maxFrameBytes) {
        throw new Error(`AI stream frame exceeds ${maxFrameBytes} bytes`);
      }
      return null;
    }
    const length = frameEnd - start;
    if (length > maxFrameBytes) {
      throw new Error(`AI stream frame exceeds ${maxFrameBytes} bytes`);
    }
    const frame = bytes.slice(start, frameEnd);
    start = frameEnd;
    scan = frameEnd;
    lineStart = frameEnd;
    if (start === end) {
      start = 0;
      end = 0;
      scan = 0;
      lineStart = 0;
    }
    return frame;
  };

  return { append, take };
}

/**
 * @param {Uint8Array} frame
 * @param {string} protocol
 * @returns {"completed" | "provider_error" | "provider_failed" | "provider_incomplete" | null}
 */
function terminalOutcome(frame, protocol) {
  const text = utf8Decoder.decode(frame);
  let event = "";
  const data = [];
  for (const rawLine of text.split(/\r\n|\r|\n/)) {
    if (rawLine.startsWith("event:")) event = rawLine.slice(6).trimStart();
    if (rawLine.startsWith("data:")) data.push(rawLine.slice(5).trimStart());
  }
  if (data.length === 0) return null;
  const joined = data.join("\n");
  if (event === "error") {
    JSON.parse(joined);
    return "provider_error";
  }
  if (protocol === "chat_completions") {
    if (joined.trim() === "[DONE]") return "completed";
    JSON.parse(joined);
    return null;
  }
  const payload = JSON.parse(joined);
  const type = event || (
    payload !== null && typeof payload === "object" && !Array.isArray(payload) &&
      typeof payload.type === "string"
      ? payload.type
      : ""
  );
  if (type === "error") return "provider_error";
  if (type === "response.failed") return "provider_failed";
  if (type === "response.incomplete") return "provider_incomplete";
  return type === "response.completed" ? "completed" : null;
}

/**
 * @param {{
 *   response: Response,
 *   protocol: string,
 *   lease: { release(outcome: string): boolean, schedule(ms: number): void },
 *   aborter: AbortController,
 *   idleMs: number,
 *   maxDurationMs: number,
 *   maxBytes: number,
 *   maxFrameBytes: number,
 *   onCleanup(): void,
 * }} options
 */
export function createAiStreamingResponse({
  response,
  protocol,
  lease,
  aborter,
  idleMs,
  maxDurationMs,
  maxBytes,
  maxFrameBytes,
  onCleanup,
}) {
  if (!response.body) throw new Error("AI stream lifecycle is not configured");
  const reader = response.body.getReader();
  const frames = createFrameBuffer(maxFrameBytes);
  let total = 0;
  let closed = false;
  let providerDone = false;
  /** @type {ReadableStreamDefaultController<Uint8Array> | null} */
  let output = null;
  /** @type {ReturnType<typeof setTimeout> | null} */
  let idleTimer = null;

  /** @param {string} outcome */
  const cleanup = (outcome) => {
    if (closed) return;
    closed = true;
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = null;
    lease.release(outcome);
    try { reader.releaseLock(); } catch {}
    try { onCleanup(); } catch {}
  };
  /** @param {unknown} reason */
  const stopUpstream = (reason) => {
    try { aborter.abort(reason); } catch {}
    try { void reader.cancel(reason).catch(() => {}); } catch {}
  };
  /** @param {Error} error @param {string} outcome */
  const fail = (error, outcome) => {
    if (closed) return;
    stopUpstream(error);
    try { output?.error(error); } catch {}
    cleanup(outcome);
  };
  const clearIdle = () => {
    if (idleTimer !== null) clearTimeout(idleTimer);
    idleTimer = null;
  };
  const armIdle = () => {
    clearIdle();
    idleTimer = setTimeout(() => {
      try {
        const trailing = frames.take(true);
        if (output !== null && trailing !== null && enqueueFrame(output, trailing)) return;
        fail(new Error("AI stream idle timeout"), "idle_timeout");
      } catch (err) {
        fail(err instanceof Error ? err : new Error(errorMessage(err)), "stream_error");
      }
    }, idleMs);
  };
  lease.schedule(maxDurationMs);

  /**
   * @param {ReadableStreamDefaultController<Uint8Array>} controller
   * @param {Uint8Array} frame
   */
  function enqueueFrame(controller, frame) {
    const outcome = terminalOutcome(frame, protocol);
    controller.enqueue(frame);
    if (outcome === null) return false;
    stopUpstream(new Error("AI stream terminal event"));
    cleanup(outcome);
    controller.close();
    return true;
  }

  const body = new ReadableStream({
    start(controller) { output = controller; },
    async pull(controller) {
      try {
        for (;;) {
          const frame = frames.take(providerDone);
          if (frame !== null) {
            enqueueFrame(controller, frame);
            return;
          }
          if (providerDone) throw new Error("AI stream ended before its terminal event");
          armIdle();
          const { value, done } = await reader.read().finally(clearIdle);
          if (closed) return;
          if (done) {
            providerDone = true;
            continue;
          }
          const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
          total += chunk.byteLength;
          if (total > maxBytes) throw new Error(`AI stream exceeds ${maxBytes} bytes`);
          frames.append(chunk);
        }
      } catch (err) {
        fail(err instanceof Error ? err : new Error(errorMessage(err)), "stream_error");
      }
    },
    cancel(reason) {
      stopUpstream(reason);
      cleanup("cancelled");
    },
  });
  return {
    body,
    /** @param {unknown} reason */
    cancel(reason) {
      const error = reason instanceof Error
        ? reason
        : new DOMException("This operation was aborted", "AbortError");
      fail(error, "cancelled");
    },
    deadline() {
      fail(new Error("AI stream duration exceeded"), "deadline");
    },
  };
}
