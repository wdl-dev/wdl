import {
  BINDING_NAME_RE,
  D1_DATABASE_ID_RE,
  RESERVED_OBJECT_KEYS,
  WDL_RESERVED_BINDING_RE,
  WDL_RESERVED_ENTRYPOINT_RE,
  isValidJsIdentifier,
  isValidJsClassDeclarationName,
} from "shared-ns-pattern";
import { parseVersion } from "shared-worker-contract";

const DO_BACKEND_BINDING = "__WDL_DO_BACKEND__";
const DO_OWNER_NETWORK_BINDING = "__WDL_DO_OWNER_NETWORK__";
const WORKFLOWS_BACKEND_BINDING = "__WDL_WORKFLOWS_BACKEND__";

/**
 * @typedef {Record<string, unknown> & { type?: string, id?: unknown, databaseId?: unknown, bucketName?: unknown, className?: unknown, doStorageId?: unknown, service?: unknown, ns?: unknown, version?: unknown, entrypoint?: unknown, deliveryDelaySeconds?: unknown, requiredCallerSecrets?: unknown }} RuntimeBindingSpec
 * @typedef {{ binding?: unknown, name?: unknown, className?: unknown, workflowKey?: unknown }} RuntimeWorkflowSpec
 * @typedef {{ vars?: Record<string, unknown> | null, workflows?: RuntimeWorkflowSpec[] | null, bindings?: Record<string, RuntimeBindingSpec> | null, assets?: { prefix?: unknown } | null }} RuntimeBundleMeta
 * @typedef {(options: { props: Record<string, unknown> }) => unknown} RuntimeEntrypointFactory
 * @typedef {{ exports: Record<string, RuntimeEntrypointFactory> & { KV: RuntimeEntrypointFactory, Assets: RuntimeEntrypointFactory, QueueProducer: RuntimeEntrypointFactory, D1Database: RuntimeEntrypointFactory, R2Bucket: RuntimeEntrypointFactory, ServiceBinding: RuntimeEntrypointFactory } }} RuntimeContext
 * @typedef {{ name: string, spec: RuntimeBindingSpec, ns: string, worker: string, version: string }} DoBindingFactoryArgs
 * @typedef {{ doOwnerNetwork?: unknown, doBindingFactory?: (args: DoBindingFactoryArgs) => unknown, workflowsBackend?: unknown, bindingEntries?: Array<[string, RuntimeBindingSpec]>, workflows?: RuntimeWorkflowSpec[] }} BuildWorkerEnvOptions
 * @typedef {{ value: unknown, needsDoBackend?: boolean }} RuntimeBindingMaterialized
 * @typedef {{
 *   name: string,
 *   spec: RuntimeBindingSpec,
 *   meta: RuntimeBundleMeta,
 *   ns: string,
 *   worker: string,
 *   version: string,
 *   cdnBase: string | undefined | null,
 *   ctx: RuntimeContext,
 *   nsSecrets: Record<string, string>,
 *   workerSecrets: Record<string, string>,
 *   options: BuildWorkerEnvOptions,
 * }} RuntimeBindingMaterializerArgs
 * @typedef {(args: RuntimeBindingMaterializerArgs) => RuntimeBindingMaterialized} RuntimeBindingMaterializer
 */

/** @param {Record<string, unknown> | null | undefined} source @param {string} label @param {string} ns @param {string} worker */
function validateEnvSourceNames(source, label, ns, worker) {
  for (const name of Object.keys(source || {})) {
    if (WDL_RESERVED_BINDING_RE.test(name)) {
      throw new Error(`${label} "${name}" is reserved for runtime-internal bindings (redeploy ${ns}/${worker})`);
    }
    if (RESERVED_OBJECT_KEYS.has(name)) {
      throw new Error(`${label} "${name}" is a reserved Object.prototype key (redeploy ${ns}/${worker})`);
    }
  }
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeKvBinding({ name, spec, ns, ctx }) {
  if (typeof spec.id !== "string" || !spec.id) {
    throw new Error(`Binding "${name}" is a KV binding but missing id`);
  }
  return { value: ctx.exports.KV({ props: { ns, id: spec.id } }) };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeAssetsBinding({ name, meta, ns, worker, cdnBase, ctx }) {
  if (!cdnBase) {
    throw new Error(
      `Binding "${name}" requires ASSETS_CDN_BASE but it is not configured on runtime`
    );
  }
  const prefix = meta.assets?.prefix;
  if (typeof prefix !== "string" || !prefix) {
    throw new Error(
      `Binding "${name}" requires __meta__.assets.prefix (bundle shape mismatch — redeploy ${ns}/${worker})`
    );
  }
  return { value: ctx.exports.Assets({ props: { cdnBase, prefix } }) };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeQueueBinding({ name, spec, ns, ctx }) {
  if (typeof spec.id !== "string" || !spec.id) {
    throw new Error(`Binding "${name}" is a queue binding but missing id (queue name)`);
  }
  return {
    value: ctx.exports.QueueProducer({
      props: {
        ns,
        id: spec.id,
        deliveryDelaySeconds: spec.deliveryDelaySeconds ?? 0,
      },
    }),
  };
}

// The estimated control-side copy of the do-runtime alarm binding env value
// must serialize like the real one; both build their props here.
/** @param {{ ns: string, worker: string, version: string, doStorageId: string }} identity */
export function doAlarmBindingProps({ ns, worker, version, doStorageId }) {
  return { ns, worker, version, doStorageId };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeD1Binding({ name, spec, ns, ctx }) {
  const databaseId = spec.databaseId;
  if (typeof databaseId !== "string" || !D1_DATABASE_ID_RE.test(databaseId)) {
    throw new Error(`Binding "${name}" is a D1 binding but has invalid databaseId`);
  }
  return {
    value: ctx.exports.D1Database({
      props: {
        ns,
        databaseId,
        binding: name,
      },
    }),
  };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeR2Binding({ name, spec, ns, ctx }) {
  const bucketName = spec.bucketName;
  if (typeof bucketName !== "string" || !bucketName) {
    throw new Error(`Binding "${name}" is an R2 binding but missing bucketName`);
  }
  return {
    value: ctx.exports.R2Bucket({
      props: {
        ns,
        bucketName,
        binding: name,
      },
    }),
  };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeDoBinding({ name, spec, ns, worker, version, options }) {
  if (typeof spec.className !== "string" || !spec.className) {
    throw new Error(`Binding "${name}" is a Durable Object binding but missing className`);
  }
  if (typeof spec.doStorageId !== "string" || !spec.doStorageId) {
    throw new Error(`Binding "${name}" is a Durable Object binding but missing doStorageId (redeploy ${ns}/${worker})`);
  }
  if (!isValidJsClassDeclarationName(spec.className)) {
    throw new Error(`Binding "${name}" has invalid Durable Object class name ${JSON.stringify(spec.className)}`);
  }
  if (WDL_RESERVED_ENTRYPOINT_RE.test(spec.className)) {
    throw new Error(
      `Binding "${name}" targets reserved runtime entrypoint "${spec.className}" (redeploy ${ns}/${worker})`
    );
  }
  if (typeof options.doBindingFactory === "function") {
    return {
      value: options.doBindingFactory({ name, spec, ns, worker, version }),
      needsDoBackend: true,
    };
  }
  return {
    value: {
      ns,
      worker,
      version,
      doStorageId: spec.doStorageId,
      binding: name,
      className: spec.className,
    },
    needsDoBackend: true,
  };
}

/** @param {RuntimeBindingMaterializerArgs} args */
function materializeServiceBinding({ name, spec, ns, worker, nsSecrets, workerSecrets, ctx }) {
  if (typeof spec.service !== "string" || !spec.service || typeof spec.version !== "string" || !spec.version) {
    throw new Error(
      `Binding "${name}" is a service binding but missing service/version (control should have pinned these at deploy time)`
    );
  }
  if (parseVersion(spec.version) == null) {
    throw new Error(`Binding "${name}" is a service binding but has invalid version (redeploy ${ns}/${worker})`);
  }
  if (spec.ns != null && (typeof spec.ns !== "string" || !spec.ns)) {
    throw new Error(`Binding "${name}" is a service binding but has invalid ns (redeploy ${ns}/${worker})`);
  }
  if (spec.entrypoint != null && (typeof spec.entrypoint !== "string" || !isValidJsIdentifier(spec.entrypoint))) {
    throw new Error(`Binding "${name}" is a service binding but has invalid entrypoint (redeploy ${ns}/${worker})`);
  }
  if (typeof spec.entrypoint === "string" && WDL_RESERVED_ENTRYPOINT_RE.test(spec.entrypoint)) {
    throw new Error(
      `Binding "${name}" targets reserved runtime entrypoint "${spec.entrypoint}" (redeploy ${ns}/${worker})`
    );
  }
  // Secrets-only (not vars): a credential moved from secrets → vars
  // stops reaching the target instead of silently continuing.
  // Missing keys are dropped; control warned at deploy.
  const requiredCallerSecrets = Array.isArray(spec.requiredCallerSecrets)
    ? spec.requiredCallerSecrets
    : [];
  /** @type {Record<string, string> | undefined} */
  const callerSecrets = requiredCallerSecrets.length
    // workerd's JSRPC props serializer rejects null-prototype objects.
    ? {}
    : undefined;
  if (callerSecrets) {
    for (const k of requiredCallerSecrets) {
      if (typeof k !== "string") continue;
      if (Object.hasOwn(workerSecrets, k)) {
        callerSecrets[k] = workerSecrets[k];
      } else if (Object.hasOwn(nsSecrets, k)) {
        callerSecrets[k] = nsSecrets[k];
      }
    }
  }
  return {
    value: ctx.exports.ServiceBinding({
      props: {
        targetNs: spec.ns ?? ns,
        targetWorker: spec.service,
        targetVersion: spec.version,
        targetEntrypoint: spec.entrypoint ?? null,
        callerNs: ns,
        ...(callerSecrets ? { callerSecrets } : {}),
      },
    }),
  };
}

/** @type {Record<string, RuntimeBindingMaterializer>} */
const RUNTIME_BINDING_MATERIALIZERS = Object.assign(Object.create(null), {
  kv: materializeKvBinding,
  assets: materializeAssetsBinding,
  queue: materializeQueueBinding,
  d1: materializeD1Binding,
  r2: materializeR2Binding,
  do: materializeDoBinding,
  service: materializeServiceBinding,
});

// Env merge precedence: vars < nsSecrets < workerSecrets.
/**
 * @param {RuntimeBundleMeta} meta
 * @param {Record<string, string>} nsSecrets
 * @param {Record<string, string>} workerSecrets
 * @param {string} ns
 * @param {string} worker
 * @param {string} version
 * @param {string | undefined | null} cdnBase
 * @param {RuntimeContext} ctx
 * @param {unknown} [doBackend]
 * @param {BuildWorkerEnvOptions} [options]
 */
export function buildWorkerEnv(
  meta,
  nsSecrets,
  workerSecrets,
  ns,
  worker,
  version,
  cdnBase,
  ctx,
  doBackend = null,
  options = {}
) {
  validateEnvSourceNames(meta.vars, "var", ns, worker);
  validateEnvSourceNames(nsSecrets, "namespace secret", ns, worker);
  validateEnvSourceNames(workerSecrets, "worker secret", ns, worker);
  /** @type {Record<string, unknown>} */
  const env = { ...(meta.vars || {}), ...nsSecrets, ...workerSecrets };
  const doOwnerNetwork = options?.doOwnerNetwork ?? null;
  let hasDoBinding = false;
  let hasWorkflowBinding = false;
  for (const workflow of options.workflows || (Array.isArray(meta.workflows) ? meta.workflows : [])) {
    const { binding, name, className, workflowKey } = workflow || {};
    if (
      typeof binding !== "string" ||
      !BINDING_NAME_RE.test(binding) ||
      WDL_RESERVED_BINDING_RE.test(binding) ||
      RESERVED_OBJECT_KEYS.has(binding)
    ) {
      throw new Error(`Workflow binding "${binding}" is not a valid runtime binding name (redeploy ${ns}/${worker})`);
    }
    if (typeof name !== "string" || !name || typeof className !== "string" || !className || typeof workflowKey !== "string" || !workflowKey) {
      throw new Error(`Workflow binding "${binding}" is missing workflow metadata (redeploy ${ns}/${worker})`);
    }
    if (!isValidJsClassDeclarationName(className)) {
      throw new Error(`Workflow binding "${binding}" targets invalid workflow class name ${JSON.stringify(className)} (redeploy ${ns}/${worker})`);
    }
    if (WDL_RESERVED_ENTRYPOINT_RE.test(className)) {
      throw new Error(`Workflow binding "${binding}" targets reserved runtime entrypoint "${className}" (redeploy ${ns}/${worker})`);
    }
    hasWorkflowBinding = true;
    env[binding] = {
      ns,
      worker,
      version,
      name,
      binding,
      className,
      workflowKey,
    };
  }
  for (const [name, spec] of options.bindingEntries || Object.entries(meta.bindings || {})) {
    if (!BINDING_NAME_RE.test(name) || WDL_RESERVED_BINDING_RE.test(name) || RESERVED_OBJECT_KEYS.has(name)) {
      throw new Error(`Binding "${name}" is not a valid runtime binding name (redeploy ${ns}/${worker})`);
    }
    const materialize = typeof spec.type === "string" && Object.hasOwn(RUNTIME_BINDING_MATERIALIZERS, spec.type)
      ? RUNTIME_BINDING_MATERIALIZERS[spec.type]
      : undefined;
    if (!materialize) {
      throw new Error(`Unsupported binding "${name}": type "${spec.type}"`);
    }
    const materialized = materialize({
      name,
      spec,
      meta,
      ns,
      worker,
      version,
      cdnBase,
      ctx,
      nsSecrets,
      workerSecrets,
      options,
    });
    env[name] = materialized.value;
    hasDoBinding ||= materialized.needsDoBackend === true;
  }
  if (hasDoBinding) {
    if (doBackend != null) env[DO_BACKEND_BINDING] = doBackend;
    if (doOwnerNetwork != null) env[DO_OWNER_NETWORK_BINDING] = doOwnerNetwork;
  }
  if (hasWorkflowBinding) {
    if (options.workflowsBackend == null) {
      throw new Error("Workflow binding requires WORKFLOWS_BACKEND service binding on runtime");
    }
    env[WORKFLOWS_BACKEND_BINDING] = options.workflowsBackend;
  }
  return env;
}
