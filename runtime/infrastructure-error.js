const diagnostics = new WeakMap();
/** @type {Map<string, { diagnostic: string | undefined }>} */
const activeInvocations = new Map();

export function beginRuntimeInfrastructureInvocation() {
  let id;
  do {
    id = crypto.randomUUID();
  } while (activeInvocations.has(id));
  const state = { diagnostic: undefined };
  activeInvocations.set(id, state);
  let closed = false;
  return {
    id,
    diagnostic() {
      return state.diagnostic;
    },
    close() {
      if (closed) return;
      closed = true;
      activeInvocations.delete(id);
    },
  };
}

/** @param {{ diagnostic: string | undefined }} invocation @param {string} diagnostic */
function recordDiagnostic(invocation, diagnostic) {
  if (invocation.diagnostic === undefined) invocation.diagnostic = diagnostic;
}

/**
 * Mark a host-generated error as retryable runtime infrastructure failure.
 * The WeakMap identity cannot be forged through tenant-controlled name/code fields.
 *
 * @param {string} message
 * @param {string} [diagnostic]
 * @param {string | null} [invocationId]
 */
export function runtimeInfrastructureError(
  message,
  diagnostic = message,
  invocationId = null
) {
  const error = new Error(message);
  diagnostics.set(error, diagnostic);
  const invocation = invocationId === null ? undefined : activeInvocations.get(invocationId);
  if (invocation) recordDiagnostic(invocation, diagnostic);
  return error;
}

/** @param {unknown} error */
export function isRuntimeInfrastructureError(error) {
  return error !== null &&
    (typeof error === "object" || typeof error === "function") &&
    diagnostics.has(/** @type {object} */ (error));
}

/** @param {unknown} error */
export function runtimeInfrastructureDiagnostic(error) {
  return error !== null && (typeof error === "object" || typeof error === "function")
    ? diagnostics.get(/** @type {object} */ (error))
    : undefined;
}
