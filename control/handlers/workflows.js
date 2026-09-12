import { parseBundleMeta, workflowDefsKey } from "control-lib";
import {
  ControlAbort,
  WORKFLOWS_INTERNAL_TIMEOUT_MS,
  codedErrorLogFields,
  controlAbortResponse,
  errorMessage,
  jsonError,
  jsonResponse,
  postWorkflowsInternal,
  readWorkflowInstancesResponse,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { bundleKey, routesKey } from "shared-worker-contract";
import { bytesToBase64, canonicalBase64ToBytes } from "base64.js";
import { utf8ByteLength } from "shared-utf8";
import {
  WORKFLOW_DEFINITION_CURSOR_MAX_BYTES,
  WORKFLOW_DEFINITION_PAGE_MAX_BYTES,
  WORKFLOW_DEFINITION_PAGE_READ_MAX_BYTES,
  WORKFLOW_DEFINITION_PAGE_MAX_WORKERS,
  readWorkflowDefinitionSnapshot,
  readWorkflowRoutePage,
  workflowDeclarationsFit,
} from "control-workflow-definitions";
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
const cursorEncoder = new TextEncoder();
const cursorDecoder = new TextDecoder("utf-8", { fatal: true });

/**
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("shared-redis").RedisSession} RedisSession
 * @typedef {{ method: string, url: URL, ns: string, subPath: string[], requestId: string }} WorkflowsHandlerArgs
 * @typedef {{ redis: RedisClient | RedisSession }} RedisDeps
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
 * @typedef {{ scan: string, worker?: string, name?: string, fingerprint?: string, definitions?: string }} DefinitionCursor
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
    const body = await listWorkflowDefinitions(deps, ns, listOptions(url));
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
      const { body, responseBytes } = await callWorkflowsRust("instances", {
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
      return new Response(/** @type {BodyInit | null} */ (responseBytes), {
        headers: { "content-type": "application/json" },
      });
    }

    if (subPath.length >= 4) {
      const instanceId = decodePathSegment(subPath[3], "workflow instance id");
      if (method === "GET" && subPath.length === 4) {
        const { body } = await callWorkflowsRust("status", {
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
        const { body } = await callWorkflowsRust(action, {
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
 * @param {{ redis: RedisClient }} deps
 * @param {string} ns
 * @param {Record<string, unknown>} options
 */
async function listWorkflowDefinitions({ redis }, ns, options) {
  const limit = options.limit ?? 100;
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new ControlAbort(400, "invalid_request", { message: "Workflow definition limit must be in [1, 1000]" });
  }
  const cursor = decodeDefinitionCursor(options.cursor);
  try {
    return await redis.session(async (session) =>
      listWorkflowDefinitionsFromSession({ redis: session }, ns, limit, cursor));
  } catch (err) {
    if (err instanceof ControlAbort) throw err;
    requireControlLog()("error", "workflow_metadata_unavailable", { namespace: ns, error_message: errorMessage(err) });
    throw new ControlAbort(500, "workflow_metadata_unavailable", { namespace: ns, message: "Workflow metadata is unavailable" });
  }
}

/** @param {Uint8Array} bytes */
function base64Url(bytes) {
  return bytesToBase64(bytes).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

/** @param {unknown} value */
async function definitionFingerprint(value) {
  return base64Url(new Uint8Array(await crypto.subtle.digest("SHA-256", cursorEncoder.encode(JSON.stringify(value)))));
}

/** @param {DefinitionCursor} cursor */
function encodeDefinitionCursor(cursor) {
  return base64Url(cursorEncoder.encode(JSON.stringify(cursor)));
}

/** @param {unknown} raw @returns {DefinitionCursor} */
function decodeDefinitionCursor(raw) {
  if (raw === undefined || raw === "") return { scan: "0" };
  try {
    if (typeof raw !== "string" || raw.length > WORKFLOW_DEFINITION_CURSOR_MAX_BYTES || !/^[A-Za-z0-9_-]+$/.test(raw)) throw new Error("cursor");
    const base64 = raw.replaceAll("-", "+").replaceAll("_", "/");
    const parsed = JSON.parse(cursorDecoder.decode(canonicalBase64ToBytes(base64 + "=".repeat((4 - base64.length % 4) % 4))));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || typeof parsed.scan !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(parsed.scan) || BigInt(parsed.scan) > 18446744073709551615n) throw new Error("cursor");
    if (parsed.worker !== undefined && (!isValidWorkerName(parsed.worker) || typeof parsed.fingerprint !== "string")) throw new Error("cursor");
    if (parsed.name !== undefined && (parsed.worker === undefined || !isValidWorkflowName(parsed.name) || typeof parsed.definitions !== "string")) throw new Error("cursor");
    return parsed;
  } catch {
    throw new ControlAbort(400, "invalid_request", { message: "Workflow definition cursor is invalid" });
  }
}

/**
 * @param {{ redis: RedisSession }} deps
 * @param {string} ns
 * @param {number} limit
 * @param {DefinitionCursor} cursor
 */
async function listWorkflowDefinitionsFromSession({ redis }, ns, limit, cursor) {
  for (let attempt = 0; attempt < MAX_WORKFLOW_SNAPSHOT_ATTEMPTS; attempt += 1) {
    const routePage = await readWorkflowRoutePage(redis, routesKey(ns), cursor.scan);
    const entries = Object.entries(routePage.routes).filter(([, version]) => version).toSorted(([a], [b]) => a.localeCompare(b));
    const fingerprint = await definitionFingerprint([routePage.next, entries]);
    if (cursor.fingerprint && cursor.fingerprint !== fingerprint) break;
    /** @type {ListedWorkflowEntry[]} */
    const workflows = [];
    /** @type {DefinitionCursor} */
    let progress = { ...cursor, fingerprint };
    /** @type {DefinitionCursor | null} */
    let next = routePage.next === "0" ? null : { scan: routePage.next };
    let bytes = utf8ByteLength(JSON.stringify({ namespace: ns, workflows: [], cursor: "" })) + WORKFLOW_DEFINITION_CURSOR_MAX_BYTES;
    let workers = 0;
    let inputBytes = 0;
    let changed = false;
    for (const [worker, version] of entries) {
      const order = cursor.worker === undefined ? 1 : worker.localeCompare(cursor.worker);
      if (order < 0 || (order === 0 && cursor.name === undefined)) continue;
      if (workers === WORKFLOW_DEFINITION_PAGE_MAX_WORKERS || workflows.length === limit) { next = progress; break; }
      workers += 1;
      const snapshot = await readWorkflowListSnapshot(redis, ns, worker, version, entries.length);
      if (snapshot.status === 2) { changed = true; break; }
      if (snapshot.status === -1) {
        throw new ControlAbort(413, "workflow_listing_metadata_too_large", { namespace: ns, worker, message: "Worker metadata exceeds the Workflow listing read budget" });
      }
      if (snapshot.status !== 1) {
        throw new ControlAbort(500, "corrupt_meta", { namespace: ns, worker, message: "Workflow listing metadata exceeds its read bounds" });
      }
      if (inputBytes + snapshot.bytes > WORKFLOW_DEFINITION_PAGE_READ_MAX_BYTES) {
        if (inputBytes === 0) {
          throw new ControlAbort(413, "workflow_listing_metadata_too_large", { namespace: ns, worker, message: "Worker metadata exceeds the Workflow listing read budget" });
        }
        next = progress;
        break;
      }
      inputBytes += snapshot.bytes;
      const meta = workflowBundleMeta(ns, worker, version, snapshot.metaRaw);
      const definitions = buildWorkerWorkflowDefinitions(ns, worker, version, meta, snapshot.defs);
      const defsFingerprint = await definitionFingerprint(definitions);
      if (order === 0 && cursor.name !== undefined && cursor.definitions !== defsFingerprint) { changed = true; break; }
      let pageFull = false;
      for (const definition of definitions) {
        if (order === 0 && cursor.name !== undefined && definition.name.localeCompare(cursor.name) <= 0) continue;
        const size = utf8ByteLength(JSON.stringify(definition)) + (workflows.length > 0 ? 1 : 0);
        if (workflows.length === limit || bytes + size > WORKFLOW_DEFINITION_PAGE_MAX_BYTES) {
          if (workflows.length === 0) throw new ControlAbort(500, "corrupt_meta", { namespace: ns, worker, message: "Workflow definition exceeds the page byte limit" });
          next = progress;
          pageFull = true;
          break;
        }
        workflows.push(definition);
        bytes += size;
        progress = { scan: cursor.scan, fingerprint, worker, name: definition.name, definitions: defsFingerprint };
      }
      if (pageFull) break;
      progress = { scan: cursor.scan, fingerprint, worker };
    }
    if (changed) continue;
    const after = await readWorkflowRoutePage(redis, routesKey(ns), cursor.scan);
    const afterEntries = Object.entries(after.routes).filter(([, version]) => version).toSorted(([a], [b]) => a.localeCompare(b));
    if (await definitionFingerprint([after.next, afterEntries]) !== fingerprint) continue;
    return { namespace: ns, workflows, cursor: next === null ? null : encodeDefinitionCursor(next) };
  }
  throw new ControlAbort(503, "workflow_metadata_contention", {
    message: "Workflow metadata changed; restart definition listing without a cursor",
    namespace: ns,
  });
}

/**
 * @param {string} ns
 * @param {string} worker
 * @param {string} activeVersion
 * @param {Record<string, unknown>} meta
 * @param {Record<string, string | null | undefined>} defsRaw
 */
function buildWorkerWorkflowDefinitions(ns, worker, activeVersion, meta, defsRaw) {
  /** @type {ListedWorkflowEntry[]} */
  const workflows = [];
  const activeNames = new Set();
  for (const workflow of workflowsFromMeta(meta)) {
    activeNames.add(workflow.name);
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
  const defs = parseWorkflowDefs(defsRaw, { ns, worker });
  for (const [name, def] of Object.entries(defs)) {
    if (activeNames.has(name)) continue;
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
  return workflows.toSorted((a, b) => a.name.localeCompare(b.name));
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
    if (!workflowDeclarationsFit(workflows) || !Array.isArray(workflows)) {
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

/** @param {RedisSession} redis @param {string} ns @param {string} worker @param {string} version @param {number} workerCount */
async function readWorkflowListSnapshot(redis, ns, worker, version, workerCount) {
  try {
    return await readWorkflowDefinitionSnapshot(redis, workflowDefsKey(ns, worker), bundleKey(ns, worker, version), {
      key: routesKey(ns), worker, version,
    });
  } catch (err) {
    requireControlLog()("error", "workflow_metadata_unavailable", {
      namespace: ns, worker_count: workerCount, error_message: errorMessage(err),
    });
    throw new ControlAbort(500, "workflow_metadata_unavailable", {
      message: "Workflow metadata is unavailable", namespace: ns, worker_count: workerCount,
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
      error_message: errorMessage(err),
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
 * @returns {Promise<{ body: Record<string, unknown>, responseBytes: Uint8Array | null }>}
 */
async function callWorkflowsRust(endpoint, body) {
  /** @type {Uint8Array | null} */
  let responseBytes = null;
  const { response, body: parsed } = await postWorkflowsInternal({
    endpoint: `workflows/${endpoint}`,
    body,
    requestId: body.requestId || null,
    logEvent: "workflow_backend_request_failed",
    logFields: {
      endpoint,
    },
    timeoutMs: endpoint === "instances" ? WORKFLOWS_INTERNAL_TIMEOUT_MS : null,
    readBody: endpoint === "instances" ? async (response, signal) => {
      const result = await readWorkflowInstancesResponse(response, signal);
      responseBytes = result.bytes;
      return result.body;
    } : undefined,
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
  return { body: /** @type {Record<string, unknown>} */ (parsed), responseBytes };
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
