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
 * @returns {Promise<Uint8Array>}
 */
export async function readBoundedStreamBytes(
  stream,
  maxBytes,
  overflowError = () => new BodyTooLargeError(maxBytes)
) {
  const reader = stream.getReader();
  /** @type {Uint8Array[]} */
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
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
 * @returns {Promise<Uint8Array>}
 */
export async function readBoundedBytes(request, maxBytes) {
  const contentLength = request.headers.get("content-length");
  if (contentLength != null && contentLength !== "") {
    const declared = Number(contentLength);
    if (Number.isFinite(declared) && declared > maxBytes) {
      throw new BodyTooLargeError(maxBytes);
    }
  }
  if (!request.body) return new Uint8Array();
  return readBoundedStreamBytes(request.body, maxBytes);
}

/**
 * @param {Request} request
 * @param {number} maxBytes
 */
export async function readBoundedText(request, maxBytes) {
  return utf8Decoder.decode(await readBoundedBytes(request, maxBytes));
}
