const utf8Decoder = new TextDecoder();

export class BodyTooLargeError extends Error {
  /** @param {number} maxBytes */
  constructor(maxBytes) {
    super(`Request body exceeds ${maxBytes} bytes`);
    this.name = "BodyTooLargeError";
    this.maxBytes = maxBytes;
  }
}

/**
 * @param {ReadableStream<Uint8Array>} stream
 * @param {number} maxBytes
 * @param {() => Error} [overflowError]
 * @param {AbortSignal} [signal]
 * @returns {Promise<Uint8Array>}
 */
export async function readBoundedStreamBytes(
  stream,
  maxBytes,
  overflowError = () => new BodyTooLargeError(maxBytes),
  signal
) {
  const reader = stream.getReader();
  const abort = () => {
    try {
      void reader.cancel(signal?.reason).catch(() => {});
    } catch {
      // Cancellation is best-effort; throwIfAborted() owns the caller-visible error.
    }
  };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  /** @type {Uint8Array[]} */
  const chunks = [];
  let total = 0;
  try {
    signal?.throwIfAborted();
    while (true) {
      const { done, value } = await reader.read();
      signal?.throwIfAborted();
      if (done) break;
      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      total += chunk.byteLength;
      if (total > maxBytes) {
        const error = overflowError();
        try {
          void reader.cancel(error).catch(() => {});
        } catch {
          // Cancellation is best-effort; the size error must not wait on it.
        }
        throw error;
      }
      chunks.push(chunk);
    }
  } finally {
    signal?.removeEventListener("abort", abort);
    try { reader.releaseLock(); } catch {}
  }

  if (chunks.length === 1) {
    const [chunk] = chunks;
    if (chunk.byteOffset === 0 && chunk.byteLength === chunk.buffer.byteLength) return chunk;
    return new Uint8Array(chunk);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 * @param {AbortSignal} [signal]
 * @returns {Promise<Uint8Array>}
 */
export async function readBoundedBytes(request, maxBytes, signal) {
  signal?.throwIfAborted();
  const contentLength = request.headers.get("content-length");
  if (contentLength != null && contentLength !== "") {
    const declared = Number(contentLength);
    if (Number.isFinite(declared) && declared > maxBytes) {
      throw new BodyTooLargeError(maxBytes);
    }
  }
  if (!request.body) return new Uint8Array();
  return readBoundedStreamBytes(request.body, maxBytes, undefined, signal);
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 * @param {AbortSignal} [signal]
 */
export async function readBoundedText(request, maxBytes, signal) {
  return utf8Decoder.decode(await readBoundedBytes(request, maxBytes, signal));
}
