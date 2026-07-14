/**
 * @template T
 * @param {(attempt: number) => Promise<T>} operation
 * @param {{
 *   attempts: number,
 *   isRetryableError?: (err: unknown) => boolean,
 *   onRetryableError?: (err: unknown, attempt: number) => void,
 *   shouldRetryResult?: (result: T, attempt: number) => boolean,
 *   onExhausted: () => T | Promise<T>,
 * }} options
 * @returns {Promise<T>}
 */
export async function withOptimisticRetries(operation, {
  attempts,
  isRetryableError = () => false,
  onRetryableError,
  shouldRetryResult,
  onExhausted,
}) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const result = await operation(attempt);
      if (shouldRetryResult?.(result, attempt)) continue;
      return result;
    } catch (err) {
      if (isRetryableError(err)) {
        onRetryableError?.(err, attempt);
        continue;
      }
      throw err;
    }
  }
  return await onExhausted();
}
