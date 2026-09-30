import { discardResponseBody } from "./respond.js";

export const S3_TRANSIENT_RETRIES = 10;
const S3_RETRY_BASE_MS = 50;
const S3_RETRY_MAX_MS = 5_000;

/** @param {number} ms @param {AbortSignal | null | undefined} signal */
function delay(ms, signal) {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve(undefined);
    }, ms);
    signal?.addEventListener("abort", abort, { once: true });
  });
}

/** @param {number} attempt */
function s3RetryDelayMs(attempt) {
  return Math.min(S3_RETRY_BASE_MS * 2 ** attempt, S3_RETRY_MAX_MS);
}

/** @param {Response} response */
export function isTransientS3Response(response) {
  return response.status === 429 || response.status >= 500;
}

/**
 * DeleteObjects is a POST, but deleting the same key set is safe to retry.
 * Keep this scoped to S3 POST calls with known idempotent semantics.
 * @param {{ fetch(url: string, init?: RequestInit): Promise<Response> }} client
 * @param {string} url
 * @param {RequestInit} init
 */
export async function fetchRetryableS3Post(client, url, init) {
  const signal = init.signal;
  for (let attempt = 0; attempt <= S3_TRANSIENT_RETRIES; attempt += 1) {
    signal?.throwIfAborted();
    let response;
    try {
      response = await client.fetch(url, init);
    } catch (err) {
      signal?.throwIfAborted();
      if (attempt === S3_TRANSIENT_RETRIES) throw err;
      await delay(Math.random() * s3RetryDelayMs(attempt), signal);
      continue;
    }
    if (signal?.aborted) {
      await discardResponseBody(response);
      signal.throwIfAborted();
    }
    if (attempt === S3_TRANSIENT_RETRIES || !isTransientS3Response(response)) {
      return response;
    }
    await discardResponseBody(response);
    await delay(Math.random() * s3RetryDelayMs(attempt), signal);
  }
  throw new Error("unreachable S3 retry loop exit");
}
