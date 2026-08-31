export const KV_READ_INFRASTRUCTURE_ERROR_CODE = "wdl_kv_read_infrastructure";
export const KV_FACADE_RPC_METHOD = "__wdlKvInvoke";
export const WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN =
  "https://workflow-infrastructure.invalid";

const infrastructureErrors = new WeakSet();

/**
 * Mark a host-generated KV read failure for direct-call facade branding.
 * The stable own code survives enhanced JSRPC error serialization.
 *
 * @param {string} message
 */
export function runtimeInfrastructureError(message) {
  const error = new Error(message);
  Object.defineProperty(error, "code", {
    value: KV_READ_INFRASTRUCTURE_ERROR_CODE,
    enumerable: true,
  });
  infrastructureErrors.add(error);
  return error;
}

/** @param {unknown} error */
export function isRuntimeInfrastructureError(error) {
  return error !== null &&
    (typeof error === "object" || typeof error === "function") &&
    infrastructureErrors.has(/** @type {object} */ (error));
}
