import { WorkerEntrypoint } from "cloudflare:workers";
import {
  WORKFLOW_BACKEND_REQUEST_BYTES_MAX,
  workflowBackendBody,
} from "runtime-dispatch-workflow-json";
import { readBoundedText } from "shared-bounded-body";
import { withInternalAuthEntries } from "shared-internal-auth";
import { sanitizeRequestId } from "shared-observability";

export const WORKFLOW_BINDING_REQUEST_BYTES_MAX = WORKFLOW_BACKEND_REQUEST_BYTES_MAX;

const WORKFLOW_BASE_PATH = "/internal/workflows/";
const WORKFLOW_OPERATIONS = new Set([
  "create",
  "create-batch",
  "get",
  "status",
  "pause",
  "resume",
  "terminate",
  "restart",
  "send-event",
]);

/**
 * @typedef {{ ns: string, worker: string, version: string, name: string, workflowKey: string, className: string }} WorkflowBindingProps
 * @typedef {{ WORKFLOWS_BACKEND?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }, WDL_INTERNAL_AUTH_TOKEN?: unknown }} WorkflowBindingEnv
 * @typedef {{ ctx: { props?: Partial<WorkflowBindingProps> }, env: WorkflowBindingEnv }} WorkflowBindingView
 */

/** @param {WorkflowBinding} binding */
function bindingView(binding) {
  return /** @type {WorkflowBindingView} */ (/** @type {unknown} */ (binding));
}

/** @param {WorkflowBinding} binding */
function bindingProps(binding) {
  const props = bindingView(binding).ctx.props || {};
  for (const field of ["ns", "worker", "version", "name", "workflowKey", "className"]) {
    if (typeof props[/** @type {keyof WorkflowBindingProps} */ (field)] !== "string" ||
        !props[/** @type {keyof WorkflowBindingProps} */ (field)]) {
      throw new Error("Workflow binding adapter is not configured");
    }
  }
  return /** @type {WorkflowBindingProps} */ (props);
}

/** @param {Request} request */
function workflowOperation(request) {
  if (request.method !== "POST") {
    throw new TypeError("Workflow binding transport requires POST");
  }
  const pathname = new URL(request.url).pathname;
  if (!pathname.startsWith(WORKFLOW_BASE_PATH)) {
    throw new TypeError("Workflow binding transport path is not allowed");
  }
  const operation = pathname.slice(WORKFLOW_BASE_PATH.length);
  if (!WORKFLOW_OPERATIONS.has(operation)) {
    throw new TypeError("Workflow binding operation is not allowed");
  }
  return operation;
}

/** @param {Request} request */
async function workflowFields(request) {
  const signal = request.signal;
  let value;
  try {
    value = JSON.parse(await readBoundedText(request, WORKFLOW_BINDING_REQUEST_BYTES_MAX, signal));
  } catch (error) {
    signal.throwIfAborted();
    throw new TypeError("Workflow binding request body must be bounded JSON", { cause: error });
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Workflow binding request body must be an object");
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/**
 * Binding-scoped transport for the generated Workflow facade. The caller can
 * select only a public Workflow operation; immutable worker/workflow identity
 * and mesh authentication remain in this host realm.
 */
export class WorkflowBinding extends WorkerEntrypoint {
  /** @param {Request} request */
  async fetch(request) {
    const operation = workflowOperation(request);
    const props = bindingProps(this);
    const fields = await workflowFields(request);
    const requestId = sanitizeRequestId(request.headers.get("x-request-id"));
    const body = {
      ...fields,
      ns: props.ns,
      worker: props.worker,
      frozenVersion: props.version,
      workflowName: props.name,
      workflowKey: props.workflowKey,
      className: props.className,
      requestId,
    };
    const bodyJson = workflowBackendBody(operation, body);
    const env = bindingView(this).env;
    const backend = env.WORKFLOWS_BACKEND;
    if (!backend || typeof backend.fetch !== "function") {
      throw new Error("WORKFLOWS_BACKEND service binding is not configured");
    }
    return await backend.fetch(`http://workflows${WORKFLOW_BASE_PATH}${operation}`, {
      method: "POST",
      headers: withInternalAuthEntries({
        "content-type": "application/json",
        ...(requestId ? { "x-request-id": requestId } : {}),
      }, env),
      body: bodyJson,
    });
  }
}
