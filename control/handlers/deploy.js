import {
  jsonResponse, jsonError, readJsonBody, formatError,
  requireControlLog, requireControlRedis,
  errMessage, prefixedId, stringEnv,
  getControlS3,
  runOptimistic,
  stageBundleCommit, buildS3CleanupTaskId, recordS3CleanupIntent,
  ControlAbort, controlAbortResponse, codedErrorLogFields, codedErrorResponse,
  secretEnvelopeErrorResponse,
} from "control-shared";
import { PLATFORM_TIER_RESERVED_NS } from "shared-auth-roles";
import {
  d1DatabaseKey,
  d1DatabaseNameKey,
  extractD1Refs,
  extractOutgoingRefs,
  parseBundleMeta,
  workflowDefsKey,
} from "control-lib";
import {
  stageD1ReferrerAdds,
  stageOutgoingReferrerAdds,
  stageWorkerVersionIndexUpsert,
} from "control-lifecycle-indexes";
import { deepFreeze, prepareBundle, normalizeAssets } from "control-bundle";
import {
  parseExports,
  parsePlatformBindings,
  validateBindings,
  normalizeBindings,
  linkServiceBinding,
  linkPlatformBinding,
  LinkError,
} from "control-bindings";
import {
  parseRoutes,
  parseCronList,
  parseQueueConsumers,
} from "control-topology";
import {
  bundleKey,
  deleteLockKey,
  doStorageIdKey,
  formatVersion,
  nextVersionKey,
  parseVersion,
  routesKey,
} from "shared-worker-contract";
import { nsSecretsKey, workerSecretsKey } from "shared-secret-keys";
import {
  isReservedNs,
  isValidRouteNs,
  platformDomainFromEnv,
  ROUTES_ALLOWED_RESERVED_NS,
  WORKFLOW_KEY_RE,
} from "shared-ns-pattern";
import { putAsset, inferContentType } from "control-s3";
import { generateAssetsToken, assetsPrefixFor } from "shared-assets-token";
import { resolveDatabaseRefFrom } from "control-d1-store";
import {
  WorkerEnvBudgetError,
  assertWorkerLoaderUserEnvBudget,
  decryptSecretHash,
} from "control-env-budget";
import {
  WorkerCodeBudgetError,
  assertWorkerLoaderCodeBudget,
} from "control-worker-code-budget";
import { SecretEnvelopeError } from "shared-secret-envelope";

const MAX_COMMIT_ATTEMPTS = 5;
const DEPLOY_JSON_BODY_MAX_BYTES = 32 * 1024 * 1024;
const DEPLOY_ASSET_UPLOAD_CONCURRENCY = 8;
const DEPLOY_PREFLIGHT_READ_BATCH_SIZE = 32;

// Deploy keeps thin local wrappers even though they mirror ControlAbort-like
// fields: commit aborts need cleanup/log handling, while request errors are
// pre-commit shape rejections.
class DeployAbort extends ControlAbort {}

/**
 * @param {RedisClient} redis
 * @param {Array<[string, string]>} pairs
 */
async function readDeployHGetMany(redis, pairs) {
  /** @type {(string | null)[]} */
  const values = [];
  for (let offset = 0; offset < pairs.length; offset += DEPLOY_PREFLIGHT_READ_BATCH_SIZE) {
    const batch = await redis.hGetMany(pairs.slice(offset, offset + DEPLOY_PREFLIGHT_READ_BATCH_SIZE));
    values.push(...batch.map((value) => value ?? null));
  }
  return values;
}

/** @param {Record<string, unknown>} details */
function deployAbortLogContext(details) {
  const { databaseId, ...context } = details;
  if (databaseId !== undefined) context.database_id = databaseId;
  return context;
}

/**
 * @typedef {import("control-shared").ControlLogger} ControlLogger
 * @typedef {import("shared-redis").RedisClient} RedisClient
 * @typedef {import("shared-redis").RedisSession} RedisSession
 * @typedef {import("shared-redis").RedisMulti} RedisMulti
 * @typedef {import("control-topology").RoutePattern} RoutePattern
 * @typedef {import("control-topology").CronSpec} CronSpec
 * @typedef {import("control-topology").QueueConsumer} QueueConsumer
 * @typedef {import("control-bindings").BindingSpec} BindingSpec
 * @typedef {import("control-bindings").ExportEntry} ExportEntry
 * @typedef {import("control-bindings").PlatformBindingRequest} PlatformBindingRequest
 * @typedef {import("control-bindings").PlatformExport} PlatformExport
 * @typedef {import("control-bindings").ExpandedPlatformBinding} ExpandedPlatformBinding
 * @typedef {import("control-lifecycle-indexes").OutgoingRef} OutgoingRef
 * @typedef {import("control-lifecycle-indexes").D1Ref} D1Ref
 * @typedef {D1Ref & { databaseId: string }} DeployD1Ref
 * @typedef {DeployD1Ref & { resolvedDatabaseId: string, resolvedDatabaseName?: string | null }} ResolvedD1Ref
 * @typedef {Record<string, unknown>} JsonObject
 * @typedef {Record<string, BindingSpec>} BindingMap
 * @typedef {[string, string | Uint8Array]} NormalizedModule
 * @typedef {{ request: Request, env: Record<string, unknown>, ns: string, name: string, requestId: string }} DeployHandlerArgs
 * @typedef {{ mainModule: string, modules: Record<string, unknown> }} DeployModules
 * @typedef {{
 *   mainModule: string,
 *   modules: Record<string, unknown>,
 *   assetsToUpload: Array<[string, Uint8Array]> | null,
 *   routes: RoutePattern[],
 *   exportsList: ExportEntry[],
 *   platformBindingsList: PlatformBindingRequest[],
 *   crons: CronSpec[],
 *   queueConsumers: QueueConsumer[],
 *   mergedBindings: BindingMap,
 *   compatibilityDate: unknown,
 *   compatibilityFlags: unknown,
 *   vars: unknown,
 *   workflows: unknown,
 * }} DeployRequest
 * @typedef {JsonObject & { bindings?: BindingMap, workflows?: JsonObject[] }} PreparedMeta
 * @typedef {JsonObject & { name: string, className: string, workflowKey?: string }} WorkflowMeta
 * @typedef {JsonObject & { bindings: BindingMap, workflows?: WorkflowMeta[] }} CommittedMeta
 * @typedef {{ normalized: NormalizedModule[], meta: PreparedMeta }} PreparedBundle
 * @typedef {{ prefix: string, token: string }} AssetsMeta
 * @typedef {{ prepared: PreparedBundle, assetsMeta?: AssetsMeta, outgoingRefs: OutgoingRef[], d1Refs: DeployD1Ref[] }} CommittedBundle
 * @typedef {RedisMulti} DeployCommitMulti
 * @typedef {{ kind?: string, prefix?: string, reason?: string, [key: string]: unknown }} DeployWarning
 */

class DeployRequestError extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {string} message
   */
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/** @param {DeployRequestError} err @param {Record<string, unknown>} [extraDetails] */
function deployRequestErrorResponse(err, extraDetails = {}) {
  return codedErrorResponse(err, err.code, extraDetails);
}

/** @param {string} message @returns {DeployRequestError} */
function invalidDeployRequest(message) {
  return new DeployRequestError(400, "invalid_request", message);
}

/** @param {unknown} value @returns {value is string[]} */
function isStringArray(value) {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

/** @param {unknown} value @returns {value is ExportEntry & { as: string }} */
function isPlatformExportMeta(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = /** @type {Record<string, unknown>} */ (value);
  return (
    typeof record.entrypoint === "string" &&
    typeof record.as === "string" &&
    isStringArray(record.allowedCallers) &&
    (record.requiredCallerSecrets === undefined || isStringArray(record.requiredCallerSecrets))
  );
}

/** @param {string} ns @param {string} worker @param {string} version @param {unknown} raw */
function linkableBundleMeta(ns, worker, version, raw) {
  return parseBundleMeta(raw, {
    ns,
    worker,
    version,
    makeError: ({ message }) => new LinkError(500, "corrupt_meta", message),
  });
}

/** @param {unknown} err @returns {DeployRequestError} */
function deployRequestErrorFromUnknown(err) {
  const record = err && typeof err === "object" ? /** @type {Record<string, unknown>} */ (err) : {};
  return new DeployRequestError(
    typeof record.status === "number" ? record.status : 400,
    typeof record.code === "string" ? record.code : "invalid_request",
    typeof record.message === "string" ? record.message : String(err)
  );
}

/** @param {BindingMap} [bindings] */
function hasDurableObjectBinding(bindings = {}) {
  return Object.values(bindings).some((spec) => spec?.type === "do");
}

function newDoStorageId() {
  return prefixedId("do_");
}

function newWorkflowKey() {
  return prefixedId("wf_");
}

/**
 * @param {CommittedMeta} meta
 * @param {string | null} doStorageId
 */
function attachDoStorageId(meta, doStorageId) {
  if (!doStorageId || !meta.bindings) return meta;
  for (const spec of Object.values(meta.bindings)) {
    if (spec?.type === "do") spec.doStorageId = doStorageId;
  }
  return meta;
}

/**
 * @param {JsonObject} body
 * @returns {DeployModules}
 */
function parseDeployModules(body) {
  if (typeof body.code === "string") {
    return { mainModule: "worker.js", modules: { "worker.js": body.code } };
  }
  if (typeof body.mainModule === "string" && body.modules && typeof body.modules === "object") {
    return { mainModule: body.mainModule, modules: /** @type {Record<string, unknown>} */ (body.modules) };
  }
  throw invalidDeployRequest("Body must have either 'code' (string) or 'mainModule' + 'modules'");
}

/**
 * @param {{ body: JsonObject, ns: string, platformDomain: string }} args
 * @returns {DeployRequest}
 */
function prepareDeployRequest({ body, ns, platformDomain }) {
  const { mainModule, modules } = parseDeployModules(body);

  let assetsToUpload;
  try {
    assetsToUpload = normalizeAssets(body.assets);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }

  let routes;
  try {
    routes = parseRoutes(body.routes, platformDomain);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }

  // Reserved ns are JSRPC-only unless explicitly whitelisted for routes.
  // promoteWithRoutes re-checks in case a bundle was committed before this gate.
  if (
    isReservedNs(ns) &&
    !ROUTES_ALLOWED_RESERVED_NS.has(ns) &&
    routes.length > 0
  ) {
    throw invalidDeployRequest(
      `Namespace "${ns}" is reserved and may not declare routes (JSRPC-only)`
    );
  }

  let exportsList;
  try {
    exportsList = parseExports(body.exports, { ns });
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }

  let platformBindingsList;
  try {
    platformBindingsList = parsePlatformBindings(body.platformBindings);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }
  if (body.allowedCallers !== undefined) {
    throw invalidDeployRequest(
      "Service binding access must be declared through exports[].allowedCallers"
    );
  }

  let crons;
  try {
    crons = parseCronList(body.crons);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }

  let queueConsumers;
  try {
    queueConsumers = parseQueueConsumers(body.queueConsumers);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }

  // Scheduler and queue dispatch enter runtime through x-worker-id, which uses
  // route namespace grammar. Platform-tier workers are cold-load-only through
  // [[platform_bindings]], so reject triggers at deploy time instead of
  // accepting work that would later fail at runtime/internal.
  if (!isValidRouteNs(ns) && (crons.length > 0 || queueConsumers.length > 0)) {
    throw invalidDeployRequest(
      `Namespace "${ns}" is not dispatch-routeable and may not declare crons or queueConsumers`
    );
  }

  // Callers declare assets, not the binding; control picks the name.
  try {
    validateBindings(body.bindings);
  } catch (err) {
    throw invalidDeployRequest(errMessage(err));
  }
  const declaredBindings = /** @type {BindingMap} */ (normalizeBindings(body.bindings) || {});
  const mergedBindings = { ...declaredBindings };
  if (assetsToUpload && !mergedBindings.ASSETS) {
    mergedBindings.ASSETS = { type: "assets" };
  }

  return {
    mainModule,
    modules,
    assetsToUpload: /** @type {Array<[string, Uint8Array]> | null} */ (assetsToUpload),
    routes,
    exportsList,
    platformBindingsList,
    crons,
    queueConsumers,
    mergedBindings,
    compatibilityDate: body.compatibilityDate,
    compatibilityFlags: body.compatibilityFlags,
    vars: body.vars ?? undefined,
    workflows: body.workflows ?? undefined,
  };
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, bindings: BindingMap }} args
 */
async function validateServiceBindingsPreflight({ redis, ns, name, bindings }) {
  /** @type {Map<string, { targetNs: string, worker: string }>} */
  const targets = new Map();
  for (const spec of Object.values(bindings)) {
    if (!spec || spec.type !== "service" || typeof spec.service !== "string" || !spec.service) continue;
    const targetNs = spec.ns == null ? ns : spec.ns;
    if (typeof targetNs !== "string") continue;
    if (targetNs === ns && spec.service === name) continue;
    if (PLATFORM_TIER_RESERVED_NS.has(targetNs)) continue;
    targets.set(`${targetNs}\0${spec.service}`, { targetNs, worker: spec.service });
  }

  const targetList = [...targets.values()];
  const versions = await readDeployHGetMany(
    redis,
    targetList.map(({ targetNs, worker }) => [routesKey(targetNs), worker])
  );
  /** @type {Map<string, string | null>} */
  const versionCache = new Map();
  /** @type {Array<{ targetNs: string, worker: string, version: string }>} */
  const versionedTargets = [];
  for (let index = 0; index < targetList.length; index += 1) {
    const target = targetList[index];
    const version = versions[index] ?? null;
    versionCache.set(`${target.targetNs}\0${target.worker}`, version);
    if (version) versionedTargets.push({ ...target, version });
  }

  const rawMetas = await readDeployHGetMany(
    redis,
    versionedTargets.map(({ targetNs, worker, version }) => [
      bundleKey(targetNs, worker, version),
      "__meta__",
    ])
  );
  /** @type {Map<string, string | null>} */
  const metaCache = new Map();
  for (let index = 0; index < versionedTargets.length; index += 1) {
    const { targetNs, worker, version } = versionedTargets[index];
    metaCache.set(`${targetNs}\0${worker}\0${version}`, rawMetas[index] ?? null);
  }

  /**
   * @param {string} targetNs
   * @param {string} worker
   */
  const lookupTargetVersion = async (targetNs, worker) =>
    versionCache.get(`${targetNs}\0${worker}`) ?? null;
  /**
   * @param {string} targetNs
   * @param {string} worker
   * @param {string} version
   */
  const lookupTargetMeta = async (targetNs, worker, version) => {
    const key = `${targetNs}\0${worker}\0${version}`;
    const rawMeta = metaCache.get(key) ?? null;
    return rawMeta == null
      ? null
      : linkableBundleMeta(targetNs, worker, version, rawMeta);
  };
  for (const [bname, spec] of Object.entries(bindings)) {
    if (!spec || spec.type !== "service") continue;
    await linkServiceBinding({
      callerNs: ns,
      callerName: name,
      bindingName: bname,
      spec,
      lookupTargetVersion,
      lookupTargetMeta,
    });
  }
}

/** @param {RedisClient} redis */
async function collectPlatformExports(redis) {
  const platformNamespaces = [...PLATFORM_TIER_RESERVED_NS];
  const routeHashes = await redis.hGetAllMany(
    platformNamespaces.map((platformNs) => routesKey(platformNs))
  );
  const routeEntries = platformNamespaces.flatMap((platformNs, index) => {
    const platformRoutesHash = routeHashes[index];
    return Object.entries(platformRoutesHash)
      .filter((entry) => typeof entry[1] === "string")
      .map(([worker, version]) => ({
        ns: platformNs,
        worker,
        version: /** @type {string} */ (version),
      }));
  });

  const rawMetas = await readDeployHGetMany(
    redis,
    routeEntries.map(({ ns, worker, version }) => [bundleKey(ns, worker, version), "__meta__"])
  );
  const exportsByWorker = routeEntries.map(({ ns, worker, version }, index) => {
    const rawMeta = rawMetas[index];
    if (rawMeta == null) return [];
    const meta = linkableBundleMeta(ns, worker, version, rawMeta);
    if (!Array.isArray(meta.exports)) return [];
    const exportsList = /** @type {unknown[]} */ (meta.exports);
    return exportsList
      .filter(isPlatformExportMeta)
      .map((entry) => ({
        ns,
        worker,
        version,
        entrypoint: entry.entrypoint,
        as: entry.as,
        allowedCallers: entry.allowedCallers || [],
        requiredCallerSecrets: entry.requiredCallerSecrets || [],
      }));
  });

  return exportsByWorker.flat();
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, bindings: BindingMap, platformBindingsList: PlatformBindingRequest[] }} args
 */
async function resolvePlatformBindings({ redis, ns, name, bindings, platformBindingsList }) {
  /** @type {DeployWarning[]} */
  const warnings = [];
  if (!platformBindingsList.length) return { bindings, warnings };

  const [
    platformExports,
    nsSecretKeys,
    workerSecretKeys,
  ] = await Promise.all([
    collectPlatformExports(redis),
    redis.hKeys(nsSecretsKey(ns)),
    redis.hKeys(workerSecretsKey(ns, name)),
  ]);
  const availableCallerSecrets = new Set([...nsSecretKeys, ...workerSecretKeys]);
  const expandedBindings = { ...bindings };

  for (const pb of platformBindingsList) {
    const linked = linkPlatformBinding({
      callerNs: ns,
      bindingReq: pb,
      existingBindings: expandedBindings,
      platformExports,
      availableCallerSecrets,
    });
    if (linked.warning) warnings.push(linked.warning);
    expandedBindings[pb.binding] = linked.expanded;
  }

  return { bindings: expandedBindings, warnings };
}

/**
 * @param {{ deployRequest: DeployRequest, ns: string, name: string, bindings: BindingMap }} args
 * @returns {CommittedBundle}
 */
function prepareCommittedBundle({ deployRequest, ns, name, bindings }) {
  // Prefix must live in the immutable meta — generate before prepareBundle
  // so deepFreeze locks it along with everything else.
  let assetsMeta;
  if (deployRequest.assetsToUpload) {
    const token = generateAssetsToken();
    assetsMeta = { token, prefix: assetsPrefixFor(ns, name, token) };
  }

  let prepared;
  try {
    prepared = /** @type {PreparedBundle} */ (/** @type {unknown} */ (prepareBundle(deployRequest.mainModule, deployRequest.modules, {
      compatibilityDate: deployRequest.compatibilityDate,
      compatibilityFlags: deployRequest.compatibilityFlags,
      bindings: Object.keys(bindings).length ? bindings : undefined,
      vars: deployRequest.vars,
      exports: deployRequest.exportsList,
      workflows: deployRequest.workflows,
      assets: assetsMeta,
    })));
    if (deployRequest.routes.length) prepared.meta.routes = deployRequest.routes;
    if (deployRequest.crons.length) prepared.meta.crons = deployRequest.crons;
    if (deployRequest.queueConsumers.length) {
      prepared.meta.queueConsumers = deployRequest.queueConsumers;
    }
  } catch (err) {
    throw deployRequestErrorFromUnknown(err);
  }

  // Last write to meta — freeze before it leaves this block so
  // commitBundle / any downstream observer sees an immutable snapshot.
  deepFreeze(prepared.meta);

  return {
    prepared,
    assetsMeta,
    outgoingRefs: extractOutgoingRefs(prepared.meta.bindings, ns),
    d1Refs: /** @type {DeployD1Ref[]} */ (extractD1Refs(prepared.meta.bindings)),
  };
}

/**
 * @param {{
 *   s3: NonNullable<import("control-shared").ControlState["s3"]>,
 *   assetsToUpload: Array<[string, Uint8Array]>,
 *   assetsMeta: AssetsMeta,
 *   ns: string,
 *   name: string,
 *   version: string,
 *   requestId: string,
 *   warnings: DeployWarning[],
 *   log: ControlLogger,
 * }} args
 */
async function uploadDeployAssets({
  s3, assetsToUpload, assetsMeta, ns, name, version, requestId, warnings, log,
}) {
  const cleanupPrefix = assetsMeta.prefix;
  /** @type {{ assetPath: string, key: string, err: unknown } | null} */
  let firstFailure = null;
  let nextIndex = 0;
  async function uploadNextAsset() {
    while (firstFailure === null) {
      const current = nextIndex;
      nextIndex += 1;
      if (current >= assetsToUpload.length) return;
      const [assetPath, bytes] = assetsToUpload[current];
      const key = `${assetsMeta.prefix}${assetPath}`;
      try {
        await putAsset(s3, key, bytes, inferContentType(assetPath));
      } catch (err) {
        if (firstFailure === null) firstFailure = { assetPath, key, err };
        return;
      }
    }
  }
  await Promise.all(
    Array.from(
      { length: Math.min(DEPLOY_ASSET_UPLOAD_CONCURRENCY, assetsToUpload.length) },
      () => uploadNextAsset()
    )
  );
  if (firstFailure) {
    const { key, err } = firstFailure;
    log("error", "asset_upload_failed", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version,
      key,
      ...formatError(err),
    });
    await scheduleDeployAbortCleanup({
      ns, name, version, requestId, reason: "asset_upload_failed",
      prefix: cleanupPrefix, warnings, log,
    });
    return {
      uploadedPrefix: cleanupPrefix,
      response: jsonError(
        502,
        "asset_upload_failed",
        "Asset upload failed",
        { ...(warnings.length ? { warnings } : {}) }
      ),
    };
  }
  return assetsToUpload.length ? { uploadedPrefix: cleanupPrefix } : {};
}

/** @param {Record<string, unknown>} [extraDetails] */
function deployAssetsS3NotConfiguredResponse(extraDetails = {}) {
  return jsonError(
    503,
    "s3_not_configured",
    "Deploy carried 'assets' but control's S3 client is not configured (S3_ENDPOINT/S3_BUCKET unset)",
    extraDetails
  );
}

/**
 * @param {{ request: Request, ns: string, platformDomain: string }} args
 * @returns {Promise<{ response: Response, deployRequest?: never } | { response?: never, deployRequest: DeployRequest }>}
 */
async function parseDeployRequestForHandler({ request, ns, platformDomain }) {
  const parsed = await readJsonBody(request, {
    requireObject: true,
    maxBytes: DEPLOY_JSON_BODY_MAX_BYTES,
  });
  if (parsed.response) return { response: parsed.response };
  const body = parsed.body;

  try {
    return { deployRequest: prepareDeployRequest({ body, ns, platformDomain }) };
  } catch (err) {
    if (err instanceof DeployRequestError) return { response: deployRequestErrorResponse(err) };
    throw err;
  }
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, deployRequest: DeployRequest }} args
 */
async function runDeployPreflight({ redis, ns, name, deployRequest }) {
  await validateServiceBindingsPreflight({
    redis,
    ns,
    name,
    bindings: deployRequest.mergedBindings,
  });
  return await resolvePlatformBindings({
    redis,
    ns,
    name,
    bindings: deployRequest.mergedBindings,
    platformBindingsList: deployRequest.platformBindingsList,
  });
}

/**
 * @param {{ redis: RedisClient | RedisSession, controlEnv: Record<string, string | undefined>, ns: string, name: string, meta: PreparedMeta | CommittedMeta, version?: string }} args
 */
async function validateCommittedEnvBudget({ redis, controlEnv, ns, name, meta, version = undefined }) {
  const nsSecretHashKey = nsSecretsKey(ns);
  const workerSecretHashKey = workerSecretsKey(ns, name);
  const [nsEncrypted, workerEncrypted] = await redis.hGetAllMany([
    nsSecretHashKey,
    workerSecretHashKey,
  ]);
  const [nsSecrets, workerSecrets] = await Promise.all([
    decryptSecretHash({ encrypted: nsEncrypted, env: controlEnv, hashKey: nsSecretHashKey }),
    decryptSecretHash({ encrypted: workerEncrypted, env: controlEnv, hashKey: workerSecretHashKey }),
  ]);
  assertWorkerLoaderUserEnvBudget({
    ns,
    worker: name,
    version,
    vars: meta.vars && typeof meta.vars === "object" && !Array.isArray(meta.vars)
      ? /** @type {Record<string, unknown>} */ (meta.vars)
      : null,
    nsSecrets,
    workerSecrets,
    meta,
    assetsCdnBase: controlEnv.ASSETS_CDN_BASE,
  });
}

/**
 * @param {{ deployRequest: DeployRequest, ns: string, name: string, mergedBindings: BindingMap, warningDetails?: Record<string, unknown> }} args
 * @returns {{ response: Response, committed?: never } | { response?: never, committed: CommittedBundle }}
 */
function prepareDeployCommitCandidate({ deployRequest, ns, name, mergedBindings, warningDetails = {} }) {
  try {
    return {
      committed: prepareCommittedBundle({
        deployRequest,
        ns,
        name,
        bindings: mergedBindings,
      }),
    };
  } catch (err) {
    if (err instanceof DeployRequestError) return { response: deployRequestErrorResponse(err, warningDetails) };
    throw err;
  }
}

/**
 * @param {{
 *   deployRequest: DeployRequest,
 *   s3: ReturnType<typeof getControlS3>,
 *   assetsMeta?: AssetsMeta,
 *   ns: string,
 *   name: string,
 *   version: string,
 *   requestId: string,
 *   warnings: DeployWarning[],
 *   log: ControlLogger,
 * }} args
 * @returns {Promise<{ response?: Response, uploadedPrefix?: string | null }>}
 */
async function uploadDeployAssetsBeforeCommit({
  deployRequest, s3, assetsMeta, ns, name, version, requestId, warnings, log,
}) {
  // Assets upload MUST precede the Redis commit: a committed bundle is
  // promote-eligible, and a promoted worker handing out URLs to absent
  // objects would 404 at the CDN. Partial-upload / commit-abort orphans
  // are cleaned via the deploy-abort task below.
  if (!deployRequest.assetsToUpload) return { uploadedPrefix: null };
  return await uploadDeployAssets({
    s3: /** @type {NonNullable<typeof s3>} */ (s3),
    assetsToUpload: deployRequest.assetsToUpload,
    assetsMeta: /** @type {AssetsMeta} */ (assetsMeta),
    ns,
    name,
    version,
    requestId,
    warnings,
    log,
  });
}

/**
 * @param {{
 *   redis: RedisClient,
 *   ns: string,
 *   name: string,
 *   version: string,
 *   prepared: PreparedBundle,
 *   outgoingRefs: OutgoingRef[],
 *   d1Refs: DeployD1Ref[],
 *   uploadedPrefix: string | null | undefined,
 *   requestId: string,
 *   warnings: DeployWarning[],
 *   log: ControlLogger,
 *   controlEnv: Record<string, string | undefined>,
 * }} args
 * @returns {Promise<{ response: Response, commitDurationMs?: never } | { response?: never, commitDurationMs: number }>}
 */
async function commitPreparedDeploy({
  redis, ns, name, version, prepared, outgoingRefs, d1Refs, uploadedPrefix, requestId, warnings, log, controlEnv,
}) {
  const commitStartedAt = Date.now();
  try {
    await commitWithWatch({
      redis, ns, name, version, prepared, outgoingRefs, d1Refs, controlEnv,
    });
  } catch (err) {
    if (uploadedPrefix) {
      await scheduleDeployAbortCleanup({
        ns, name, version, requestId,
        reason: err instanceof DeployAbort ? err.code : "commit_failed",
        prefix: uploadedPrefix, warnings, log,
      });
    }
    const warningDetails = warnings.length ? { warnings } : {};
    if (err instanceof DeployAbort) {
      log(err.status >= 500 ? "error" : "warn", "deploy_rejected", {
        request_id: requestId,
        namespace: ns,
        worker: name,
        version,
        ...codedErrorLogFields(err, err.code, { context: deployAbortLogContext(err.details) }),
      });
      return { response: controlAbortResponse(err, warningDetails) };
    }
    if (err instanceof WorkerEnvBudgetError) return { response: codedErrorResponse(err, err.code, warningDetails) };
    if (err instanceof WorkerCodeBudgetError) return { response: codedErrorResponse(err, err.code, warningDetails) };
    if (err instanceof SecretEnvelopeError) {
      return {
        response: secretEnvelopeErrorResponse({
          err,
          log,
          event: "deploy_rejected",
          fields: {
            request_id: requestId,
            namespace: ns,
            worker: name,
            version,
          },
          responseDetails: warningDetails,
        }),
      };
    }
    throw err;
  }
  return { commitDurationMs: Date.now() - commitStartedAt };
}

/** @param {DeployHandlerArgs} args */
export async function handle({ request, env, ns, name, requestId }) {
  const redis = requireControlRedis();
  const s3 = getControlS3();
  const log = requireControlLog();
  const platformDomain = platformDomainFromEnv(env);

  const parsed = await parseDeployRequestForHandler({ request, ns, platformDomain });
  if (parsed.response) return parsed.response;

  let platformResult;
  try {
    platformResult = await runDeployPreflight({
      redis,
      ns,
      name,
      deployRequest: parsed.deployRequest,
    });
  } catch (err) {
    if (err instanceof LinkError) {
      log(err.status >= 500 ? "error" : "warn", "deploy_rejected", {
        request_id: requestId,
        namespace: ns,
        worker: name,
        ...codedErrorLogFields(err, "link_error"),
      });
      return codedErrorResponse(err, "link_error");
    }
    throw err;
  }
  const { bindings: mergedBindings, warnings } = platformResult;
  const warningDetails = warnings.length ? { warnings } : {};

  const candidate = prepareDeployCommitCandidate({
    deployRequest: parsed.deployRequest,
    ns,
    name,
    mergedBindings,
    warningDetails,
  });
  if (candidate.response) return candidate.response;
  const {
    prepared,
    assetsMeta,
    outgoingRefs,
    d1Refs,
  } = candidate.committed;
  const controlEnv = stringEnv(env);

  try {
    assertWorkerLoaderCodeBudget({
      ns,
      worker: name,
      meta: prepared.meta,
      normalized: prepared.normalized,
    });
  } catch (err) {
    if (err instanceof WorkerCodeBudgetError) return codedErrorResponse(err, err.code, warningDetails);
    throw err;
  }

  if (parsed.deployRequest.assetsToUpload && !s3) {
    return deployAssetsS3NotConfiguredResponse(warningDetails);
  }

  const num = await redis.incr(nextVersionKey(ns, name));
  const version = formatVersion(num);

  const uploadResult = await uploadDeployAssetsBeforeCommit({
    deployRequest: parsed.deployRequest,
    s3,
    assetsMeta,
    ns,
    name,
    version,
    requestId,
    warnings,
    log,
  });
  if (uploadResult.response) return uploadResult.response;

  const commitResult = await commitPreparedDeploy({
    redis,
    ns,
    name,
    version,
    prepared,
    outgoingRefs,
    d1Refs,
    uploadedPrefix: uploadResult.uploadedPrefix,
    requestId,
    warnings,
    log,
    controlEnv,
  });
  if (commitResult.response) return commitResult.response;

  log("info", "worker_deployed", {
    request_id: requestId,
    namespace: ns,
    worker: name,
    version,
    module_count: prepared.normalized.length,
    commit_duration_ms: commitResult.commitDurationMs,
  });
  return jsonResponse(201, {
    namespace: ns,
    name,
    version,
    active: false,
    ...(warnings.length ? { warnings } : {}),
  });
}

// Cleanup-intent failure is appended to `warnings`; the caller still
// surfaces the original deploy error.
/**
 * @param {{ ns: string, name: string, version: string, requestId: string, reason: string, prefix: string, warnings: DeployWarning[], log: ControlLogger }} args
 */
async function scheduleDeployAbortCleanup({
  ns, name, version, requestId, reason, prefix, warnings, log,
}) {
  try {
    const taskId = buildS3CleanupTaskId();
    await recordS3CleanupIntent({
      taskId,
      prefixes: [prefix],
      source: { kind: "deploy-abort", ns, worker: name, version, requestId, reason },
    });
    log("info", "deploy_abort_cleanup_scheduled", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version,
      reason,
      task_id: taskId,
    });
  } catch (err) {
    log("warn", "assets_cleanup_task_failed", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version,
      prefix,
      ...formatError(err),
    });
    warnings.push({
      kind: "assets_cleanup_task_failed",
      prefix,
      reason: "cleanup_task_write_failed",
    });
  }
}

/**
 * @param {{ redis: RedisClient, ns: string, name: string, version: string, prepared: PreparedBundle, outgoingRefs: OutgoingRef[], d1Refs: DeployD1Ref[], controlEnv: Record<string, string | undefined> }} args
 */
export async function commitWithWatch({
  redis, ns, name, version, prepared, outgoingRefs, d1Refs, controlEnv,
}) {
  const vNum = parseVersion(version);
  if (vNum == null) throw new Error(`commitWithWatch: bad version ${version}`);

  return await runOptimistic(redis, {
    attempts: MAX_COMMIT_ATTEMPTS,
    onExhausted: () => {
      throw new DeployAbort(503, "deploy_contention", {
        message: `exhausted ${MAX_COMMIT_ATTEMPTS} retries; retry later`,
      });
    },
  }, async (iso) => {
    await watchCommitKeys(iso, { ns, name, prepared, outgoingRefs, d1Refs });

    const resolvedD1Refs = await resolveD1RefsForCommit(iso, { ns, d1Refs });
    await validateCallerNotDeleting(iso, { ns, name });
    await validateOutgoingRefsForCommit(iso, { outgoingRefs });

    const {
      committedMeta,
      workflowDefUpdates,
      doStorageId,
    } = await materializeCommittedMetadata(iso, {
      ns,
      name,
      prepared,
      resolvedD1Refs,
    });
    // Keep this commit-time code-budget check. Workflow keys are materialized
    // above and then stringified into the generated host wrapper source.
    assertWorkerLoaderCodeBudget({
      ns,
      worker: name,
      version,
      meta: committedMeta,
      normalized: prepared.normalized,
    });
    await validateCommittedEnvBudget({
      redis: iso,
      controlEnv,
      ns,
      name,
      meta: committedMeta,
      version,
    });

    const multi = iso.multi();
    stageDeployCommit(multi, {
      ns,
      name,
      version,
      vNum,
      prepared,
      committedMeta,
      outgoingRefs,
      resolvedD1Refs,
      workflowDefUpdates,
      doStorageId,
    });

    await multi.exec();
  });
}

/**
 * @param {RedisSession} iso
 * @param {{ ns: string, name: string, prepared: PreparedBundle, outgoingRefs: OutgoingRef[], d1Refs: DeployD1Ref[] }} args
 */
async function watchCommitKeys(iso, { ns, name, prepared, outgoingRefs, d1Refs }) {
  const watchKeys = [
    deleteLockKey(ns, name),
    nsSecretsKey(ns),
    workerSecretsKey(ns, name),
  ];
  if (hasDurableObjectBinding(prepared.meta.bindings)) {
    watchKeys.push(doStorageIdKey(ns, name));
  }
  if (Array.isArray(prepared.meta.workflows) && prepared.meta.workflows.length) {
    watchKeys.push(workflowDefsKey(ns, name));
  }
  for (const ref of d1Refs) {
    watchKeys.push(d1DatabaseKey(ns, ref.databaseId));
    watchKeys.push(d1DatabaseNameKey(ns, ref.databaseId));
  }
  for (const ref of outgoingRefs) {
    watchKeys.push(routesKey(ref.targetNs));
    watchKeys.push(bundleKey(ref.targetNs, ref.targetWorker, ref.targetVersion));
    watchKeys.push(deleteLockKey(ref.targetNs, ref.targetWorker));
  }
  await iso.watch(...watchKeys);
}

/**
 * @param {RedisSession} iso
 * @param {{ ns: string, d1Refs: DeployD1Ref[] }} args
 * @returns {Promise<ResolvedD1Ref[]>}
 */
async function resolveD1RefsForCommit(iso, { ns, d1Refs }) {
  /** @type {ResolvedD1Ref[]} */
  const resolvedD1Refs = [];
  for (const ref of d1Refs) {
    const database = await resolveDatabaseRefFrom(iso, ns, ref.databaseId);
    if (!database) {
      throw new DeployAbort(404, "d1_database_not_found", {
        binding: ref.binding,
        databaseId: ref.databaseId,
      });
    }
    // The first WATCH set includes the user-provided ref, so alias flips
    // abort the transaction. Once resolved, also WATCH the physical DB key
    // that will be frozen into bundle metadata.
    await iso.watch(d1DatabaseKey(ns, database.databaseId));
    const currentDatabase = await resolveDatabaseRefFrom(iso, ns, database.databaseId);
    if (!currentDatabase || currentDatabase.databaseId !== database.databaseId) {
      throw new DeployAbort(404, "d1_database_not_found", {
        binding: ref.binding,
        databaseId: ref.databaseId,
      });
    }
    resolvedD1Refs.push({
      ...ref,
      resolvedDatabaseId: currentDatabase.databaseId,
      resolvedDatabaseName: currentDatabase.databaseName,
    });
  }
  return resolvedD1Refs;
}

/**
 * @param {RedisSession} iso
 * @param {{ ns: string, name: string }} args
 */
async function validateCallerNotDeleting(iso, { ns, name }) {
  const callerLock = await iso.get(deleteLockKey(ns, name));
  if (callerLock) {
    throw new DeployAbort(409, "caller_deleting", {
      namespace: ns,
      worker: name,
    });
  }
}

/**
 * @param {RedisSession} iso
 * @param {{ outgoingRefs: OutgoingRef[] }} args
 */
async function validateOutgoingRefsForCommit(iso, { outgoingRefs }) {
  // Target must still be pinnable: active version unchanged, bundle
  // present, no delete in flight. Otherwise we're about to commit a
  // caller version referencing a soon-to-vanish target.
  if (outgoingRefs.length === 0) return;
  const targetLocks = await iso.getMany(
    outgoingRefs.map((ref) => deleteLockKey(ref.targetNs, ref.targetWorker))
  );
  const currentActives = await iso.hGetMany(
    outgoingRefs.map((ref) => [routesKey(ref.targetNs), ref.targetWorker])
  );
  const targetMetas = await iso.hGetMany(
    outgoingRefs.map((ref) => [
      bundleKey(ref.targetNs, ref.targetWorker, ref.targetVersion),
      "__meta__",
    ])
  );
  for (const [index, ref] of outgoingRefs.entries()) {
    const targetLock = targetLocks[index];
    if (targetLock) {
      throw new DeployAbort(409, "target_deleting", {
        target: {
          ns: ref.targetNs,
          worker: ref.targetWorker,
          version: ref.targetVersion,
          binding: ref.binding,
        },
      });
    }
    const currentActive = currentActives[index];
    if (currentActive !== ref.targetVersion) {
      throw new DeployAbort(409, "target_drift", {
        target: {
          ns: ref.targetNs,
          worker: ref.targetWorker,
          binding: ref.binding,
          expected_version: ref.targetVersion,
          observed_active: currentActive || null,
        },
      });
    }
    const targetMeta = targetMetas[index];
    if (typeof targetMeta !== "string" || targetMeta.length === 0) {
      throw new DeployAbort(409, "target_drift", {
        target: {
          ns: ref.targetNs,
          worker: ref.targetWorker,
          version: ref.targetVersion,
          binding: ref.binding,
          reason: "bundle_missing",
        },
      });
    }
  }
}

/**
 * @param {RedisSession} iso
 * @param {{ ns: string, name: string, prepared: PreparedBundle, resolvedD1Refs: ResolvedD1Ref[] }} args
 */
async function materializeCommittedMetadata(iso, { ns, name, prepared, resolvedD1Refs }) {
  const committedMeta = /** @type {CommittedMeta} */ (structuredClone({
    ...prepared.meta,
    bindings: prepared.meta.bindings || {},
  }));
  /** @type {Array<[string, string, string]>} */
  const workflowDefUpdates = [];
  if (Array.isArray(committedMeta.workflows) && committedMeta.workflows.length) {
    const defsKey = workflowDefsKey(ns, name);
    const existingDefRaws = await iso.hMGet(
      defsKey,
      committedMeta.workflows.map((workflow) => workflow.name)
    );
    for (let index = 0; index < committedMeta.workflows.length; index += 1) {
      const workflow = committedMeta.workflows[index];
      const existingDef = parsePersistedWorkflowDef(workflow.name, existingDefRaws[index]);
      let workflowKey;
      if (existingDef) {
        workflowKey = existingDef.workflowKey;
      } else {
        workflowKey = newWorkflowKey();
      }
      workflow.workflowKey = workflowKey;
      workflowDefUpdates.push([
        defsKey,
        workflow.name,
        JSON.stringify({ workflowKey, className: workflow.className }),
      ]);
    }
  }

  let doStorageId = null;
  if (hasDurableObjectBinding(committedMeta.bindings)) {
    doStorageId = await iso.get(doStorageIdKey(ns, name));
    if (!doStorageId) doStorageId = newDoStorageId();
    attachDoStorageId(committedMeta, doStorageId);
  }
  for (const ref of resolvedD1Refs) {
    committedMeta.bindings[ref.binding] = {
      ...committedMeta.bindings[ref.binding],
      databaseId: ref.resolvedDatabaseId,
      databaseName: ref.resolvedDatabaseName,
    };
  }
  deepFreeze(committedMeta);
  return { committedMeta, workflowDefUpdates, doStorageId };
}

/**
 * @param {string} workflowName
 * @param {string | null | undefined} rawDef
 * @returns {{ workflowKey: string } | null}
 */
function parsePersistedWorkflowDef(workflowName, rawDef) {
  if (rawDef == null) return null;
  /** @type {JsonObject | null} */
  let parsed;
  try {
    parsed = typeof rawDef === "string"
      ? /** @type {JsonObject} */ (JSON.parse(rawDef))
      : null;
  } catch {
    parsed = null;
  }
  if (
    !parsed ||
    Array.isArray(parsed) ||
    typeof parsed.workflowKey !== "string" ||
    !WORKFLOW_KEY_RE.test(parsed.workflowKey)
  ) {
    throw new DeployAbort(500, "workflow_definition_corrupt", {
      workflow: workflowName,
    });
  }
  return { workflowKey: parsed.workflowKey };
}

/**
 * @param {DeployCommitMulti} multi
 * @param {{
 *   ns: string,
 *   name: string,
 *   version: string,
 *   vNum: number,
 *   prepared: PreparedBundle,
 *   committedMeta: CommittedMeta,
 *   outgoingRefs: OutgoingRef[],
 *   resolvedD1Refs: ResolvedD1Ref[],
 *   workflowDefUpdates: Array<[string, string, string]>,
 *   doStorageId: string | null,
 * }} args
 */
function stageDeployCommit(multi, {
  ns,
  name,
  version,
  vNum,
  prepared,
  committedMeta,
  outgoingRefs,
  resolvedD1Refs,
  workflowDefUpdates,
  doStorageId,
}) {
  stageBundleCommit(multi, bundleKey(ns, name, version), {
    meta: committedMeta,
    normalized: prepared.normalized,
  });
  if (doStorageId) multi.set(doStorageIdKey(ns, name), doStorageId, { nx: true });
  for (const [key, field, value] of workflowDefUpdates) multi.hSet(key, field, value);
  stageWorkerVersionIndexUpsert(multi, ns, name, version, vNum);
  stageOutgoingReferrerAdds(multi, { ns, worker: name, version, refs: outgoingRefs });
  stageD1ReferrerAdds(multi, {
    ns, worker: name, version, refs: /** @type {D1Ref[]} */ (resolvedD1Refs),
    databaseIdFor: (ref) => /** @type {ResolvedD1Ref} */ (ref).resolvedDatabaseId,
  });
}
