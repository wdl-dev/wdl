import { parseBundleMeta, workflowDefsKey } from "control-lib";
import {
  ControlAbort,
  codedErrorLogFields,
  controlAbortResponse,
  errMessage,
  jsonError,
  jsonResponse,
  postWorkflowsInternal,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { bundleKey, routesKey } from "shared-worker-contract";
import {
  BINDING_NAME_RE,
  WORKER_NAME_RE,
  WORKFLOW_NAME_RE,
  WORKFLOW_KEY_RE,
  isValidWorkerName,
  isValidWorkflowName,
  isValidJsClassDeclarationName,
} from "shared-ns-pattern";

const LIFECYCLE_ACTIONS = new Set(["pause", "resume", "restart", "terminate"]);
const MAX_WORKFLOW_SNAPSHOT_ATTEMPTS = 2;

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {{ method: string, url: URL, ns: string, subPath: string[], requestId: string }} WorkflowsHandlerArgs
 * @typedef {{ redis: RedisClient }} RedisDeps
 * @typedef {{ name: string, binding: string, className: string, workflowKey: string }} ActiveWorkflowMeta
 * @typedef {{ name: string, binding: string | null, className: string, workflowKey: string, retired?: boolean }} WorkflowEntry
 * @typedef {WorkflowEntry & { namespace: string, worker: string, activeVersion: string }} ListedWorkflowEntry
 * @typedef {{ workflowKey: string, className: string }} RetiredWorkflowDef
 * @typedef {Record<string, RetiredWorkflowDef>} RetiredWorkflowDefs
 * @typedef {{
 *   ns: string,
 *   worker: string,
 *   frozenVersion: string,
 *   workflowName: string,
 *   workflowKey: string,
 *   className: string,
 *   instanceId?: string,
 *   options?: Record<string, unknown>,
 *   requestId?: string,
 * }} WorkflowRequest
 * @typedef {{ workflow: WorkflowEntry, request: WorkflowRequest }} ResolvedWorkflow
 * @typedef {{ error: string, message?: unknown, [key: string]: unknown }} UpstreamErrorBody
 */

/** @param {WorkflowsHandlerArgs} args */
export async function handle({ method, url, ns, subPath, requestId }) {
  try {
    return await handleInner({ method, url, ns, subPath, requestId });
  } catch (err) {
    if (err instanceof ControlAbort) {
      if (err.status >= 500) {
        const detailWorker = typeof err.details?.worker === "string"
          ? err.details.worker
          : subPath[0];
        requireControlLog()("error", "workflow_request_rejected", {
          request_id: requestId,
          namespace: ns,
          ...(detailWorker ? { worker: detailWorker } : {}),
          ...(subPath[1] ? { workflow: subPath[1] } : {}),
          ...codedErrorLogFields(err),
        });
      }
      return controlAbortResponse(err);
    }
    throw err;
  }
}

/** @param {WorkflowsHandlerArgs} args */
async function handleInner({ method, url, ns, subPath, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const deps = { redis };
  if (method === "GET" && subPath.length === 0) {
    const body = await listWorkflowDefinitions(deps, ns);
    log("info", "workflows_listed", {
      request_id: requestId,
      namespace: ns,
      count: body.workflows.length,
    });
    return jsonResponse(200, body);
  }

  if (subPath.length >= 3 && subPath[2] === "instances") {
    const [worker, workflowName] = subPath;
    const workflow = await resolveWorkflow(deps, ns, worker, workflowName);

    if (method === "GET" && subPath.length === 3) {
      const body = await callWorkflowsRust("instances", {
        ...workflow.request,
        options: listOptions(url),
        requestId,
      });
      log("info", "workflow_instances_listed", {
        request_id: requestId,
        namespace: ns,
        worker,
        workflow: workflowName,
        count: Array.isArray(body.instances) ? body.instances.length : 0,
      });
      return jsonResponse(200, body);
    }

    if (subPath.length >= 4) {
      const instanceId = decodePathSegment(subPath[3], "workflow instance id");
      if (method === "GET" && subPath.length === 4) {
        const body = await callWorkflowsRust("status", {
          ...workflow.request,
          instanceId,
          options: statusOptions(url),
          requestId,
        });
        log("info", "workflow_instance_status_read", {
          request_id: requestId,
          namespace: ns,
          worker,
          workflow: workflowName,
          instance_id: instanceId,
          status: body.status || null,
        });
        return jsonResponse(200, body);
      }

      if (method === "POST" && subPath.length === 5 && LIFECYCLE_ACTIONS.has(subPath[4])) {
        const action = subPath[4];
        if (workflow.workflow?.retired && action === "restart") {
          throw new ControlAbort(409, "workflow_not_exported", {
            message: `Workflow ${ns}/${worker}/${workflowName} is not exported by the active worker version`,
          });
        }
        const body = await callWorkflowsRust(action, {
          ...workflow.request,
          instanceId,
          requestId,
        });
        log("info", "workflow_instance_lifecycle", {
          request_id: requestId,
          namespace: ns,
          worker,
          workflow: workflowName,
          instance_id: instanceId,
          action,
          status: body.status || null,
        });
        return jsonResponse(200, body);
      }
    }
  }

  return jsonError(404, "not_found", "Not found");
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 */
async function listWorkflowDefinitions({ redis }, ns) {
  for (let attempt = 0; attempt < MAX_WORKFLOW_SNAPSHOT_ATTEMPTS; attempt += 1) {
    const routes = await redis.hGetAll(routesKey(ns));
    /** @type {Array<[string, string]>} */
    const routeEntries = [];
    for (const [worker, activeVersion] of Object.entries(routes)) {
      if (typeof activeVersion === "string" && activeVersion) routeEntries.push([worker, activeVersion]);
    }
    if (routeEntries.length === 0) return { namespace: ns, workflows: [] };
    const [metaRaws, defsRaws] = await readWorkflowListRaws({ redis }, ns, routeEntries);
    const currentRoutes = await redis.hGetAll(routesKey(ns));
    /** @type {Array<Record<string, unknown> | undefined>} */
    const metas = new Array(routeEntries.length);
    for (let index = 0; index < routeEntries.length; index += 1) {
      const [worker, activeVersion] = routeEntries[index];
      if (currentRoutes[worker] === activeVersion) {
        metas[index] = workflowBundleMeta(ns, worker, activeVersion, metaRaws[index]);
      }
    }
    if (!sameRouteSnapshot(routes, currentRoutes)) continue;
    return buildWorkflowDefinitionList(
      ns,
      routeEntries,
      /** @type {Record<string, unknown>[]} */ (metas),
      defsRaws
    );
  }
  throw new ControlAbort(503, "workflow_metadata_contention", {
    message: "Workflow metadata changed while it was being read",
    namespace: ns,
  });
}

/**
 * @param {string} ns
 * @param {Array<[string, string]>} routeEntries
 * @param {Array<Record<string, unknown>>} metas
 * @param {Array<Record<string, string | null | undefined>>} defsRaws
 */
function buildWorkflowDefinitionList(ns, routeEntries, metas, defsRaws) {
  /** @type {ListedWorkflowEntry[]} */
  const workflows = [];
  for (let i = 0; i < routeEntries.length; i += 1) {
    const [worker, activeVersion] = routeEntries[i];
    const meta = metas[i];
    const activeByName = new Map();
    for (const workflow of workflowsFromMeta(meta)) {
      activeByName.set(workflow.name, workflow);
      workflows.push({
        namespace: ns,
        worker,
        activeVersion,
        name: workflow.name,
        binding: workflow.binding,
        className: workflow.className,
        workflowKey: workflow.workflowKey,
      });
    }
    const defs = parseWorkflowDefs(defsRaws[i], { ns, worker });
    for (const [name, def] of Object.entries(defs)) {
      if (activeByName.has(name)) continue;
      workflows.push({
        namespace: ns,
        worker,
        activeVersion,
        name,
        binding: null,
        className: def.className,
        workflowKey: def.workflowKey,
        retired: true,
      });
    }
  }
  const sortedWorkflows = workflows.toSorted((a, b) =>
    a.worker.localeCompare(b.worker) ||
    a.name.localeCompare(b.name)
  );
  return { namespace: ns, workflows: sortedWorkflows };
}

/** @param {Record<string, unknown>} left @param {Record<string, unknown>} right */
function sameRouteSnapshot(left, right) {
  const keys = Object.keys(left);
  return keys.length === Object.keys(right).length && keys.every((key) => right[key] === left[key]);
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 * @param {string} worker
 * @param {string} workflowName
 * @returns {Promise<ResolvedWorkflow>}
 */
async function resolveWorkflow({ redis }, ns, worker, workflowName) {
  if (!isValidWorkerName(worker)) {
    throw new ControlAbort(400, "invalid_worker_name", {
      message: `Invalid worker name ${JSON.stringify(worker)}. Must match ${WORKER_NAME_RE}.`,
    });
  }
  if (!isValidWorkflowName(workflowName)) {
    throw new ControlAbort(400, "invalid_workflow_name", {
      message: `Invalid workflow name ${JSON.stringify(workflowName)}. Must match ${WORKFLOW_NAME_RE}.`,
    });
  }
  for (let attempt = 0; attempt < MAX_WORKFLOW_SNAPSHOT_ATTEMPTS; attempt += 1) {
    const active = await readActiveWorkflowMeta({ redis }, ns, worker);
    if (!active) continue;
    const { activeVersion, meta } = active;
    const activeWorkflow = workflowsFromMeta(meta).find((entry) => entry.name === workflowName);
    /** @type {WorkflowEntry | undefined} */
    let workflow = activeWorkflow;
    if (!workflow) {
      const def = await readWorkflowDef({ redis }, ns, worker, workflowName);
      if (def) {
        workflow = {
          name: workflowName,
          binding: null,
          className: def.className,
          workflowKey: def.workflowKey,
          retired: true,
        };
      }
    }
    if (await redis.hGet(routesKey(ns), worker) !== activeVersion) continue;
    if (!workflow) {
      throw new ControlAbort(404, "workflow_not_found", {
        message: `Workflow ${ns}/${worker}/${workflowName} is not exported`,
      });
    }
    return {
      workflow,
      request: {
        ns,
        worker,
        frozenVersion: activeVersion,
        workflowName: workflow.name,
        workflowKey: workflow.workflowKey,
        className: workflow.className,
      },
    };
  }
  throw new ControlAbort(503, "workflow_metadata_contention", {
    message: `Workflow metadata changed while ${ns}/${worker} was being read`,
    namespace: ns,
    worker,
  });
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 * @param {string} worker
 * @returns {Promise<{ activeVersion: string, meta: Record<string, unknown> } | null>}
 */
async function readActiveWorkflowMeta({ redis }, ns, worker) {
  const activeVersion = await redis.hGet(routesKey(ns), worker);
  if (!activeVersion) {
    throw new ControlAbort(404, "worker_not_found", {
      message: `Worker ${ns}/${worker} is not active`,
    });
  }
  const meta = await readBundleMeta({ redis }, ns, worker, activeVersion);
  return meta ? { activeVersion, meta } : null;
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 * @param {string} worker
 * @param {string} workflowName
 * @returns {Promise<RetiredWorkflowDef | null>}
 */
async function readWorkflowDef({ redis }, ns, worker, workflowName) {
  const raw = await redis.hGet(workflowDefsKey(ns, worker), workflowName);
  return parseWorkflowDef(workflowName, raw, { ns, worker });
}

/**
 * @param {string} name
 * @param {string | null | undefined} value
 * @param {{ ns: string, worker: string }} context
 * @returns {RetiredWorkflowDef | null}
 */
function parseWorkflowDef(name, value, context) {
  if (value == null) return null;
  let parsed;
  try {
    parsed = typeof value === "string" ? JSON.parse(value) : null;
  } catch {
    parsed = null;
  }
  if (
    !isValidWorkflowName(name) ||
    !parsed ||
    typeof parsed !== "object" ||
    Array.isArray(parsed) ||
    typeof parsed.workflowKey !== "string" ||
    !WORKFLOW_KEY_RE.test(parsed.workflowKey) ||
    !isValidJsClassDeclarationName(parsed.className)
  ) {
    throw new ControlAbort(500, "corrupt_meta", {
      message: `Corrupt workflow definition for ${context.ns}/${context.worker}`,
      namespace: context.ns,
      worker: context.worker,
      stage: "workflow_defs_parse",
    });
  }
  return {
    workflowKey: parsed.workflowKey,
    className: parsed.className,
  };
}

/**
 * @param {Record<string, string | null | undefined>} raw
 * @param {{ ns: string, worker: string }} context
 * @returns {RetiredWorkflowDefs}
 */
function parseWorkflowDefs(raw, context) {
  /** @type {RetiredWorkflowDefs} */
  const defs = Object.create(null);
  for (const [name, value] of Object.entries(raw || {})) {
    const parsed = parseWorkflowDef(name, value, context);
    if (!parsed) continue;
    defs[name] = parsed;
  }
  return defs;
}

/**
 * @param {string} ns
 * @param {string} worker
 * @param {string} version
 * @param {unknown} raw
 * @returns {Record<string, unknown>}
 */
function workflowBundleMeta(ns, worker, version, raw) {
  const meta = parseBundleMeta(raw, {
    ns,
    worker,
    version,
    makeError: ({ message, reason }) => new ControlAbort(500, "corrupt_meta", {
      message,
      namespace: ns,
      worker,
      version,
      stage: "bundle_meta_parse",
      detail: reason,
    }),
  });
  const workflows = meta.workflows;
  if (workflows !== undefined) {
    if (!Array.isArray(workflows)) {
      throw corruptWorkflowEntries(ns, worker, version);
    }
    const names = new Set();
    const bindings = new Set();
    const workflowKeys = new Set();
    for (const entry of workflows) {
      if (
        !isActiveWorkflowMeta(entry) ||
        names.has(entry.name) ||
        bindings.has(entry.binding) ||
        workflowKeys.has(entry.workflowKey)
      ) {
        throw corruptWorkflowEntries(ns, worker, version);
      }
      names.add(entry.name);
      bindings.add(entry.binding);
      workflowKeys.add(entry.workflowKey);
    }
  }
  return meta;
}

/** @param {string} ns @param {string} worker @param {string} version */
function corruptWorkflowEntries(ns, worker, version) {
  return new ControlAbort(500, "corrupt_meta", {
    message: `Corrupt workflow metadata for ${ns}/${worker}/${version}`,
    namespace: ns,
    worker,
    version,
    stage: "workflow_entries_parse",
  });
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 * @param {Array<[string, string]>} routeEntries
 * @returns {Promise<[Array<string | null | undefined>, Array<Record<string, string | null | undefined>>]>}
 */
async function readWorkflowListRaws({ redis }, ns, routeEntries) {
  try {
    return await Promise.all([
      redis.hGetMany(
        routeEntries.map(([worker, activeVersion]) => [bundleKey(ns, worker, activeVersion), "__meta__"])
      ),
      redis.hGetAllMany(routeEntries.map(([worker]) => workflowDefsKey(ns, worker))),
    ]);
  } catch (err) {
    requireControlLog()("error", "workflow_metadata_unavailable", {
      namespace: ns,
      worker_count: routeEntries.length,
      error_message: errMessage(err),
    });
    throw new ControlAbort(500, "workflow_metadata_unavailable", {
      message: "Workflow metadata is unavailable",
      namespace: ns,
      worker_count: routeEntries.length,
    });
  }
}

/**
 * @param {RedisDeps} deps
 * @param {string} ns
 * @param {string} worker
 * @param {string} version
 * @returns {Promise<Record<string, unknown> | null>}
 */
async function readBundleMeta({ redis }, ns, worker, version) {
  let raw;
  try {
    raw = await redis.hGet(bundleKey(ns, worker, version), "__meta__");
    if (raw == null && await redis.hGet(routesKey(ns), worker) !== version) return null;
  } catch (err) {
    requireControlLog()("error", "workflow_metadata_unavailable", {
      namespace: ns,
      worker,
      version,
      error_message: errMessage(err),
    });
    throw new ControlAbort(500, "workflow_metadata_unavailable", {
      message: "Workflow metadata is unavailable",
      namespace: ns,
      worker,
      version,
    });
  }
  return workflowBundleMeta(ns, worker, version, raw);
}

/**
 * @param {unknown} meta
 * @returns {ActiveWorkflowMeta[]}
 */
function workflowsFromMeta(meta) {
  const record = /** @type {Record<string, unknown> | null} */ (
    meta && typeof meta === "object" ? meta : null
  );
  if (!record || !Array.isArray(record.workflows)) return [];
  return /** @type {ActiveWorkflowMeta[]} */ (record.workflows);
}

/**
 * @param {unknown} entry
 * @returns {entry is ActiveWorkflowMeta}
 */
function isActiveWorkflowMeta(entry) {
  const record = /** @type {Record<string, unknown> | null} */ (
    entry && typeof entry === "object" ? entry : null
  );
  return Boolean(
    record &&
    isValidWorkflowName(record.name) &&
    typeof record.binding === "string" && BINDING_NAME_RE.test(record.binding) &&
    isValidJsClassDeclarationName(record.className) &&
    typeof record.workflowKey === "string" && WORKFLOW_KEY_RE.test(record.workflowKey),
  );
}

/**
 * @param {string} endpoint
 * @param {WorkflowRequest} body
 * @returns {Promise<Record<string, unknown>>}
 */
async function callWorkflowsRust(endpoint, body) {
  const { response, body: parsed } = await postWorkflowsInternal({
    endpoint: `workflows/${endpoint}`,
    body,
    requestId: body.requestId || null,
    logEvent: "workflow_backend_request_failed",
    logFields: {
      endpoint,
    },
    timeoutMs: null,
  });
  if (!response.ok) {
    if (isUpstreamErrorBody(parsed)) {
      if (response.status >= 500) {
        requireControlLog()("error", "workflow_backend_error", {
          request_id: body.requestId || null,
          endpoint,
          upstream_status: response.status,
          error: parsed.error,
          error_message: typeof parsed.message === "string" ? parsed.message : null,
        });
      }
      return throwUpstreamError(response.status, parsed);
    }
    throw new ControlAbort(
      response.status >= 400 ? response.status : 502,
      "workflow_internal_dispatch_failed",
      {
        message: "Workflow backend request failed",
        upstream_status: response.status,
      },
    );
  }
  if (!parsed || typeof parsed !== "object") {
    throw new ControlAbort(502, "workflow_internal_dispatch_failed", {
      message: "Workflow backend returned an invalid response",
    });
  }
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {number} status
 * @param {UpstreamErrorBody} body
 * @returns {never}
 */
function throwUpstreamError(status, body) {
  if (status >= 500) {
    throw new ControlAbort(
      status,
      body.error,
      {
        message: "Workflow backend request failed",
        upstream_status: status,
      },
    );
  }
  throw new ControlAbort(
    status,
    body.error,
    {
      ...filterDetails(body),
      message: typeof body.message === "string" ? body.message : body.error,
    },
  );
}

/**
 * @param {unknown} body
 * @returns {body is UpstreamErrorBody}
 */
function isUpstreamErrorBody(body) {
  const record = /** @type {Record<string, unknown> | null} */ (
    body && typeof body === "object" ? body : null
  );
  return Boolean(record && typeof record.error === "string");
}

/**
 * @param {UpstreamErrorBody} body
 * @returns {Record<string, unknown>}
 */
function filterDetails(body) {
  /** @type {Record<string, unknown>} */
  const details = {};
  for (const [key, value] of Object.entries(body)) {
    if (key === "error" || key === "message") continue;
    details[key] = value;
  }
  return details;
}

/** @param {URL} url */
function listOptions(url) {
  /** @type {Record<string, unknown>} */
  const options = {};
  const limit = url.searchParams.get("limit");
  const cursor = url.searchParams.get("cursor");
  if (limit != null) options.limit = parseIntegerOption(limit, "limit");
  if (cursor != null) options.cursor = cursor;
  return options;
}

/** @param {URL} url */
function statusOptions(url) {
  /** @type {Record<string, unknown>} */
  const options = {};
  if (url.searchParams.has("include_steps") || url.searchParams.has("step_limit")) {
    throw new ControlAbort(400, "invalid_request", {
      message: "workflow status query options use camelCase",
    });
  }
  const includeSteps = url.searchParams.get("includeSteps");
  if (includeSteps != null) options.includeSteps = parseBooleanOption(includeSteps, "includeSteps");
  const stepLimit = url.searchParams.get("stepLimit");
  if (stepLimit != null) options.stepLimit = parseIntegerOption(stepLimit, "stepLimit");
  return options;
}

/**
 * @param {string} raw
 * @param {string} label
 */
function parseIntegerOption(raw, label) {
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new ControlAbort(400, "invalid_request", { message: `${label} must be an integer` });
  }
  return Number(raw);
}

/**
 * @param {string} raw
 * @param {string} label
 */
function parseBooleanOption(raw, label) {
  // Bare query flags such as ?includeSteps are accepted intentionally;
  // numeric options stay strict because an empty number has no useful meaning.
  if (raw === "" || raw === "1" || raw === "true") return true;
  if (raw === "0" || raw === "false") return false;
  throw new ControlAbort(400, "invalid_request", { message: `${label} must be true or false` });
}

/**
 * @param {string} value
 * @param {string} label
 */
function decodePathSegment(value, label) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new ControlAbort(400, "invalid_request", {
      message: `invalid percent-encoding in ${label}`,
    });
  }
}
