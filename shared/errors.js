/** @param {unknown} err */
export function errorMessage(err) {
  try {
    return String(err instanceof Error ? err.message : err);
  } catch {
    return "Unknown error";
  }
}
