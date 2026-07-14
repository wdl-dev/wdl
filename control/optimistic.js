import { WatchError } from "shared-redis";
import { withOptimisticRetries } from "shared-optimistic-retry";

export { withOptimisticRetries };

/**
 * @template T, S
 * @param {{ session: <U>(fn: (session: S) => Promise<U>) => Promise<U> }} redis
 * @param {{ attempts?: number, onExhausted: () => unknown | Promise<unknown>, onWatchError?: (err: unknown, attempt: number) => void, shouldRetryResult?: (result: T, attempt: number) => boolean }} options
 * @param {(session: S, attempt: number) => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function runOptimistic(
  redis,
  { attempts = 5, onExhausted, onWatchError, shouldRetryResult },
  fn
) {
  return await withOptimisticRetries(
    async (attempt) => await redis.session((session) => fn(session, attempt)),
    {
      attempts,
      isRetryableError: (err) => err instanceof WatchError,
      onRetryableError: onWatchError,
      shouldRetryResult,
      onExhausted: async () => /** @type {T} */ (await onExhausted()),
    }
  );
}
