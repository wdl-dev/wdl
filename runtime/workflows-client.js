import { requestIdFromOptions } from "./_wdl-request-id.js";

const WORKFLOWS_BASE_URL = "http://workflows/internal/workflows";
const MAX_CREATE_BATCH = 100;
const WORKFLOW_INSTANCE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
const intrinsicJsonStringify = JSON.stringify;
const intrinsicObjectAssign = Object.assign;
const intrinsicObjectCreate = Object.create;
const intrinsicReflectApply = Reflect.apply;

/** @param {unknown} value */
function stringifyJson(value) {
  return intrinsicReflectApply(intrinsicJsonStringify, undefined, [value]);
}

/**
 * @param {Record<string, unknown>} fields
 * @param {WorkflowMetadata} metadata
 * @param {string | null} requestId
 */
function workflowRequestBody(fields, metadata, requestId) {
  const body = /** @type {Record<string, unknown>} */ (intrinsicObjectCreate(null));
  intrinsicObjectAssign(body, fields);
  body.ns = metadata.ns;
  body.worker = metadata.worker;
  body.frozenVersion = metadata.version;
  body.workflowName = metadata.name;
  body.workflowKey = metadata.workflowKey;
  body.className = metadata.className;
  body.requestId = requestId;
  return body;
}

/**
 * @typedef {{ fetch(url: string, init: RequestInit): Promise<Response> }} WorkflowBackend
 * @typedef {{ requestId?: string, requestIdProvider?: () => string | null, backend?: WorkflowBackend }} WorkflowOptions
 * @typedef {{ ns?: string, worker?: string, version?: string, name?: string, workflowKey?: string, className?: string }} WorkflowMetadata
 * @typedef {(endpoint: string, fields: Record<string, unknown>) => Promise<Record<string, unknown>>} WorkflowCall
 */

/** @param {unknown} value @param {string} label @returns {Record<string, unknown>} */
function ensureObject(value, label) {
  if (value == null) return /** @type {Record<string, unknown>} */ ({});
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/** @param {unknown} id */
function ensureId(id) {
  if (id === undefined || id === null || id === "") return crypto.randomUUID();
  return validateInstanceId(id);
}

/** @param {unknown} id */
function validateInstanceId(id) {
  if (typeof id !== "string") throw new TypeError("Workflow instance id must be a string");
  if (!WORKFLOW_INSTANCE_ID_RE.test(id)) {
    throw new TypeError(`Workflow instance id must match ${WORKFLOW_INSTANCE_ID_RE}`);
  }
  return id;
}

/** @param {unknown} id @param {number} index */
function validateCreateBatchResponseId(id, index) {
  try {
    return validateInstanceId(id);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new TypeError(`Workflow createBatch response entry ${index}: ${message}`, { cause: err });
  }
}

/** @param {Response} response @returns {Promise<Record<string, unknown>>} */
async function readWorkflowResponse(response) {
  let body;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  if (!response.ok) {
    const record = /** @type {{ error?: unknown, message?: unknown } | null} */ (
      body && typeof body === "object" ? body : null
    );
    const code = typeof record?.error === "string" && record.error ? record.error : "workflow_backend_error";
    const message = typeof record?.message === "string" && record.message
      ? record.message
      : `Workflows backend failed with ${response.status}`;
    const err = new Error(message);
    Object.defineProperty(err, "code", { value: code });
    throw err;
  }
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("Workflows backend returned an invalid response");
  }
  return /** @type {Record<string, unknown>} */ (body);
}

export class WorkflowInstance {
  /** @type {WorkflowCall} */
  #call;

  /** @param {string} id @param {WorkflowCall} call */
  constructor(id, call) {
    this.#call = call;
    this.id = id;
  }

  /** @param {unknown} [options] */
  async status(options = undefined) {
    return await this.#call("status", {
      instanceId: this.id,
      options: ensureObject(options, "Workflow status options"),
    });
  }

  async pause() {
    await this.#call("pause", { instanceId: this.id });
    return this;
  }

  async resume() {
    await this.#call("resume", { instanceId: this.id });
    return this;
  }

  async terminate() {
    await this.#call("terminate", { instanceId: this.id });
    return this;
  }

  async restart() {
    await this.#call("restart", { instanceId: this.id });
    return this;
  }

  /** @param {unknown} event */
  async sendEvent(event) {
    await this.#call("send-event", {
      instanceId: this.id,
      event: ensureObject(event, "Workflow event"),
    });
    return this;
  }
}

export class Workflow {
  /** @type {WorkflowBackend | undefined} */
  #backend;
  /** @type {WorkflowMetadata} */
  #metadata;
  /** @type {WorkflowOptions} */
  #options;

  /** @param {WorkflowMetadata | null | undefined} metadata @param {WorkflowOptions} [options] */
  constructor(metadata, options = {}) {
    this.#metadata = metadata || {};
    this.#backend = options?.backend;
    this.#options = options || {};
  }

  /** @param {unknown} [options] */
  async create(options = undefined) {
    const opts = ensureObject(options, "Workflow create options");
    const id = ensureId(opts.id);
    const body = await this.#call("create", {
      instanceId: id,
      params: opts.params ?? null,
      retention: opts.retention ?? null,
      callback: opts.callback ?? null,
    });
    const responseId = validateInstanceId(body.id);
    return this.#instance(responseId);
  }

  /** @param {unknown} options */
  async createBatch(options) {
    if (!Array.isArray(options) || options.length === 0) {
      throw new TypeError("Workflow createBatch options must be a non-empty array");
    }
    if (options.length > MAX_CREATE_BATCH) {
      throw new TypeError(`Workflow createBatch exceeds ${MAX_CREATE_BATCH} item limit`);
    }
    const entries = options.map((entry) => {
      const opts = ensureObject(entry, "Workflow createBatch entry");
      return {
        instanceId: ensureId(opts.id),
        params: opts.params ?? null,
        retention: opts.retention ?? null,
        callback: opts.callback ?? null,
      };
    });
    const body = await this.#call("create-batch", { entries });
    if (!Array.isArray(body.instances)) {
      throw new Error("Workflow createBatch response must include instances");
    }
    return body.instances.map((entry, index) => {
      const instance = ensureObject(entry, "Workflow createBatch response entry");
      const id = validateCreateBatchResponseId(instance.id, index);
      return this.#instance(id);
    });
  }

  /** @param {string} id */
  async get(id) {
    validateInstanceId(id);
    const body = await this.#call("get", { instanceId: id });
    const responseId = validateInstanceId(body.id);
    return this.#instance(responseId);
  }

  /** @param {string} id */
  #instance(id) {
    return new WorkflowInstance(id, (endpoint, fields) => this.#call(endpoint, fields));
  }

  /** @param {string} endpoint @param {Record<string, unknown>} [fields] */
  async #call(endpoint, fields = {}) {
    if (!this.#backend || typeof this.#backend.fetch !== "function") {
      throw new Error("Workflow backend is not configured");
    }
    const metadata = this.#metadata;
    const requestId = requestIdFromOptions(this.#options);
    const body = workflowRequestBody(fields, metadata, requestId);
    const response = await this.#backend.fetch(`${WORKFLOWS_BASE_URL}/${endpoint}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(requestId ? { "x-request-id": requestId } : {}),
      },
      body: stringifyJson(body),
    });
    return await readWorkflowResponse(response);
  }
}
