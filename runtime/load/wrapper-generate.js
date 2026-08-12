const WDL_ABORT_SHIM_SOURCE = `// Reserved name (WDL_RESERVED_ENTRYPOINT_RE) — control rejects user
// [[exports]] / [[services]] matches; implicit user exports collide
// silently via the export-* shadow rule.
export class __WdlAbort__ extends WorkerEntrypoint {
  abort(reason) {
    abortIsolate(reason ?? "wdl-evict");
  }
}`;

const DEFAULT_EXPORT_SOURCE_SNIPPET = "const source = Function.prototype.toString.call(raw);";
const DEFAULT_EXPORT_CLASS_TEST_SOURCE = "/^\\s*class\\b/.test(source)";

/**
 * @typedef {{ ns: string, worker: string, version: string }} RuntimeWorkerIdentity
 */

/**
 * @param {Record<string, unknown>} workflowBindings
 * @param {RuntimeWorkerIdentity | null | undefined} workerIdentity
 */
function identifiedWorkflowBindings(workflowBindings, workerIdentity) {
  const entries = Object.entries(workflowBindings);
  if (entries.length === 0) return workflowBindings;
  const { ns, worker, version } = workerIdentity || {};
  if (typeof ns !== "string" || !ns || typeof worker !== "string" || !worker ||
      typeof version !== "string" || !version) {
    throw new Error("Workflow binding wrapper requires worker identity");
  }
  return Object.fromEntries(entries.map(([binding, metadata]) => {
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
      throw new Error(`Workflow binding "${binding}" requires metadata`);
    }
    return [binding, { ...metadata, ns, worker, version }];
  }));
}

export const HOST_BINDING_RUNTIME_MODULE_NAME = "_wdl-host-wrapper-runtime.js";
export const HOST_BINDING_RUNTIME_SOURCE = `
import { sanitizeRequestId } from "./_wdl-request-id.js";

const IntrinsicObject = Object;
const IntrinsicPromise = Promise;
const IntrinsicProxy = Proxy;
const IntrinsicReflect = Reflect;
const IntrinsicSymbol = Symbol;
const intrinsicArrayForEach = Array.prototype.forEach;
const intrinsicFunctionToString = Function.prototype.toString;
const intrinsicObjectDefineProperty = Object.defineProperty;
const intrinsicObjectEntries = Object.entries;
const intrinsicObjectKeys = Object.keys;
const intrinsicPromiseResolve = Promise.resolve;
const intrinsicPromiseThen = Promise.prototype.then;
const intrinsicReflectApply = Reflect.apply;
const intrinsicReflectGet = Reflect.get;
const intrinsicRegExpTest = RegExp.prototype.test;

export function applyFunction(fn, receiver, args) {
  return intrinsicReflectApply(fn, receiver, args);
}

export function createPrivateSymbol(description) {
  return IntrinsicSymbol(description);
}

export function createProxy(target, handler) {
  return new IntrinsicProxy(target, handler);
}

export function defineProperty(target, property, descriptor) {
  return intrinsicReflectApply(intrinsicObjectDefineProperty, IntrinsicObject, [target, property, descriptor]);
}

export function forEachArray(values, callback) {
  intrinsicReflectApply(intrinsicArrayForEach, values, [callback]);
}

export function forEachObjectEntry(record, callback) {
  const entries = intrinsicReflectApply(intrinsicObjectEntries, IntrinsicObject, [record]);
  forEachArray(entries, (entry) => callback(entry[0], entry[1]));
}

export function functionSource(fn) {
  return intrinsicReflectApply(intrinsicFunctionToString, fn, []);
}

export function objectKeys(record) {
  return intrinsicReflectApply(intrinsicObjectKeys, IntrinsicObject, [record]);
}

export function reflectGet(target, property, receiver) {
  return intrinsicReflectApply(intrinsicReflectGet, IntrinsicReflect, [target, property, receiver]);
}

export function regexpTest(regexp, value) {
  return intrinsicReflectApply(intrinsicRegExpTest, regexp, [value]);
}

export function requestIdFromEventArg(arg) {
  if (!arg || typeof arg !== "object") return null;
  try {
    const headers = arg.headers;
    return headers && typeof headers.get === "function"
      ? sanitizeRequestId(headers.get("x-request-id"))
      : null;
  } catch {
    return null;
  }
}

export function settleWithFinally(value, callback) {
  const promise = intrinsicReflectApply(intrinsicPromiseResolve, IntrinsicPromise, [value]);
  return intrinsicReflectApply(intrinsicPromiseThen, promise, [
    (result) => {
      callback();
      return result;
    },
    (error) => {
      callback();
      throw error;
    },
  ]);
}

`;

/** @param {string} userMainSpecifier */
export function generateAbortShimWrapperModule(userMainSpecifier) {
  const userMain = JSON.stringify(`./${userMainSpecifier}`);
  return `
import * as user from ${userMain};
import { WorkerEntrypoint, abortIsolate } from "cloudflare:workers";
export * from ${userMain};

${WDL_ABORT_SHIM_SOURCE}

export class __WdlWorkflowNotify__ extends WorkerEntrypoint {
  fetch() {
    return new Response("workflow callbacks require the host binding wrapper", { status: 501 });
  }
}

const raw = user.default;
let wrappedDefault = raw;

if (typeof raw === "function") {
  ${DEFAULT_EXPORT_SOURCE_SNIPPET}
  if (!${DEFAULT_EXPORT_CLASS_TEST_SOURCE}) {
    wrappedDefault = {
      fetch(request, env, ctx) {
        return raw.call(undefined, request, env, ctx);
      },
    };
  }
}

export default wrappedDefault;
`;
}

/**
 * @param {string} userMainSpecifier
 * @param {{
 *   d1Bindings?: string[],
 *   r2Bindings?: string[],
 *   doBindings?: string[],
 *   workflowBindings?: Record<string, unknown>,
 *   entrypointNames?: string[],
 *   workerIdentity?: RuntimeWorkerIdentity | null,
 *   aiBindings?: string[],
 *   importableEnvDisabled?: boolean,
 * }} [options]
 */
export function generateHostBindingWrapperModule(userMainSpecifier, options = {}) {
  const {
    d1Bindings = [],
    r2Bindings = [],
    doBindings = [],
    workflowBindings = {},
    entrypointNames = [],
    workerIdentity = null,
    aiBindings = [],
    importableEnvDisabled = false,
  } = options;
  const userMain = JSON.stringify(`./${userMainSpecifier}`);
  const d1BindingJson = JSON.stringify(d1Bindings);
  const r2BindingJson = JSON.stringify(r2Bindings);
  const doBindingJson = JSON.stringify(doBindings);
  const aiBindingJson = JSON.stringify(aiBindings);
  const workflowBindingJson = JSON.stringify(
    identifiedWorkflowBindings(workflowBindings, workerIdentity)
  );
  // Host facade helper modules are only added to workerCode when bindings
  // exist; importing them unconditionally would 404 the resolver.
  const d1Import = d1Bindings.length ? `import { D1Database } from "./_wdl-d1-client.js";` : "";
  const r2Import = r2Bindings.length ? `import { R2Bucket } from "./_wdl-r2-client.js";` : "";
  const doImport = doBindings.length ? `import { DurableObjectNamespace } from "./_wdl-do-client.js";` : "";
  const aiImport = aiBindings.length ? `import { Ai } from "./_wdl-ai-client.js";` : "";
  const workflowImport = Object.keys(workflowBindings).length ? `import { Workflow } from "./_wdl-workflows-client.js";` : "";
  const hidesRawEnvExports = doBindings.length || Object.keys(workflowBindings).length;
  const starExport = hidesRawEnvExports
    ? "// Host facades are required; only wrapped entrypoints are re-exported."
    : `export * from ${userMain};`;
  const namedEntrypoints = entrypointNames.map((/** @type {string} */ name, index) => `
const __WdlWrappedEntrypoint${index}__ = ({
  [${JSON.stringify(name)}]: class extends __WdlUserModule__.${name} {
    constructor(ctx, env) {
      const requestContext = createRequestContext();
      const wrappedEnv = wrapEnv(env, requestContext);
      withTenantEnv(wrappedEnv, () => super(ctx, wrappedEnv));
      return wrapClassInstance(this, requestContext, wrappedEnv);
    }
  },
})[${JSON.stringify(name)}];
export { __WdlWrappedEntrypoint${index}__ as ${name} };
`).join("");
  return `
import { WorkerEntrypoint, abortIsolate, withEnv } from "cloudflare:workers";
import * as __WdlHostRuntime__ from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";
${d1Import}
${r2Import}
${doImport}
${aiImport}
${workflowImport}
import * as __WdlUserModule__ from ${userMain};
// Explicit aliases replace same-name star exports without declaring tenant
// entrypoint names in the generated module's local binding scope.
${starExport}

${WDL_ABORT_SHIM_SOURCE}

export class __WdlWorkflowNotify__ extends WorkerEntrypoint {
  fetch(request) {
    return notifyWorkflowCallback(request, wrapEnv(this.env, requestIdFromEventArg(request)));
  }
}

const D1_BINDINGS = ${d1BindingJson};
const R2_BINDINGS = ${r2BindingJson};
const DO_BINDINGS = ${doBindingJson};
const AI_BINDINGS = ${aiBindingJson};
const WORKFLOW_BINDINGS = ${workflowBindingJson};
const IMPORTABLE_ENV_DISABLED = ${JSON.stringify(importableEnvDisabled)};
const HOST_BINDINGS_WRAPPED = __WdlHostRuntime__.createPrivateSymbol("wdl.host-bindings-wrapped");
const INTERNAL_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;

function requestIdFromEventArg(arg) {
  return __WdlHostRuntime__.requestIdFromEventArg(arg);
}

function createRequestContext(requestId = null) {
  return { requestId };
}

function requestIdOptions(requestIdOrContext) {
  return requestIdOrContext && typeof requestIdOrContext === "object"
    ? { requestIdProvider: () => requestIdOrContext.requestId }
    : { requestId: requestIdOrContext };
}

function withTenantEnv(env, callback) {
  return IMPORTABLE_ENV_DISABLED
    ? __WdlHostRuntime__.applyFunction(callback, undefined, [])
    : withEnv(env, callback);
}

function withRequestContext(context, arg, fn) {
  const previous = context.requestId;
  const requestId = requestIdFromEventArg(arg);
  if (requestId) context.requestId = requestId;
  try {
    const result = fn();
    if (result && typeof result.then === "function") {
      return __WdlHostRuntime__.settleWithFinally(result, () => {
        context.requestId = previous;
      });
    }
    context.requestId = previous;
    return result;
  } catch (err) {
    context.requestId = previous;
    throw err;
  }
}

function wrapClassInstance(instance, requestContext, wrappedEnv) {
  return __WdlHostRuntime__.createProxy(instance, {
    get(target, prop) {
      return withTenantEnv(wrappedEnv, () => {
        const value = __WdlHostRuntime__.reflectGet(target, prop, target);
        if (typeof value !== "function") return value;
        return function(...args) {
          return withTenantEnv(wrappedEnv, () =>
            withRequestContext(requestContext, args[0], () =>
              __WdlHostRuntime__.applyFunction(value, target, args)));
        };
      });
    },
  });
}

let lastRawEnv = null;
let lastEnvTemplateState = null;

function envTemplateState(env) {
  if (env === lastRawEnv && lastEnvTemplateState) return lastEnvTemplateState;
  const template = { ...env };
  const state = { template };
  __WdlHostRuntime__.forEachArray(__WdlHostRuntime__.objectKeys(template), (name) => {
    if (__WdlHostRuntime__.regexpTest(INTERNAL_BINDING_RE, name)) delete template[name];
  });
  lastRawEnv = env;
  lastEnvTemplateState = state;
  return state;
}

function wrapEnv(env, requestIdOrContext = null) {
  // Idempotence is a contract, not an optimization: WorkerEntrypoint methods
  // and default handlers may re-enter with an env already wrapped by this
  // module. A symbol marker cannot be forged by tenant vars/secrets.
  if (!env || env[HOST_BINDINGS_WRAPPED] === true) return env;
  const { template } = envTemplateState(env);
  const out = { ...template };
  __WdlHostRuntime__.forEachArray(D1_BINDINGS, (name) => {
    if (out[name] !== undefined) out[name] = new D1Database(out[name], requestIdOptions(requestIdOrContext));
  });
  __WdlHostRuntime__.forEachArray(R2_BINDINGS, (name) => {
    if (out[name] !== undefined) out[name] = new R2Bucket(out[name], requestIdOptions(requestIdOrContext));
  });
  __WdlHostRuntime__.forEachArray(DO_BINDINGS, (name) => {
    if (out[name] !== undefined) {
      out[name] = new DurableObjectNamespace(out[name], requestIdOptions(requestIdOrContext));
    }
  });
  __WdlHostRuntime__.forEachArray(AI_BINDINGS, (name) => {
    if (out[name] !== undefined) out[name] = new Ai(out[name], requestIdOptions(requestIdOrContext));
  });
  __WdlHostRuntime__.forEachObjectEntry(WORKFLOW_BINDINGS, (name, metadata) => {
    out[name] = new Workflow(metadata, {
      ...requestIdOptions(requestIdOrContext),
      backend: out[name],
    });
  });
  __WdlHostRuntime__.defineProperty(out, HOST_BINDINGS_WRAPPED, { value: true });
  return out;
}

async function notifyWorkflowCallback(request, env) {
  const body = await request.json();
  const callback = body && body.callback;
  const progress = body && body.progress;
  if (!callback || callback.kind !== "do") {
    return Response.json({ error: "invalid_workflow_callback", message: "Workflow callback must target a Durable Object" }, { status: 400 });
  }
  const bindingName = callback.binding;
  const idName = callback.idFromName;
  if (typeof bindingName !== "string" || !bindingName || typeof idName !== "string" || !idName) {
    return Response.json({ error: "invalid_workflow_callback", message: "Workflow callback binding and idFromName are required" }, { status: 400 });
  }
  const namespace = env[bindingName];
  if (!namespace || typeof namespace.idFromName !== "function" || typeof namespace.get !== "function") {
    return Response.json({ error: "invalid_workflow_callback", message: "Workflow callback binding is not a Durable Object namespace" }, { status: 400 });
  }
  const path = typeof callback.path === "string" && callback.path.startsWith("/") ? callback.path : "/internal/workflow-progress";
  const stub = namespace.get(namespace.idFromName(idName));
  const payload = {
    workflow: {
      ns: body.ns,
      worker: body.worker,
      frozenVersion: body.frozenVersion,
      workflowName: body.workflowName,
      workflowKey: body.workflowKey,
      className: body.className,
      instanceId: body.instanceId,
      generation: body.generation,
    },
    progress: progress ?? {},
  };
  const response = await stub.fetch(new Request("https://workflow-callback.local" + path, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  }));
  if (!response.ok) {
    return Response.json({ error: "workflow_callback_failed", message: "Workflow callback target returned an error" }, { status: 502 });
  }
  return new Response(null, { status: 204 });
}

function wrapHandler(owner, fn) {
  return function(arg1, env, ctx) {
    const wrappedEnv = wrapEnv(env, requestIdFromEventArg(arg1));
    return withTenantEnv(wrappedEnv, () =>
      __WdlHostRuntime__.applyFunction(fn, owner, [arg1, wrappedEnv, ctx]));
  };
}

const HOST_WRAPPED_HANDLER_KEYS = ["fetch", "scheduled", "queue", "tail"];

const raw = __WdlUserModule__.default;
let wrappedDefault = raw;

if (raw && typeof raw === "object") {
  wrappedDefault = { ...raw };
  const wrapDefaultFunctionKey = (key) => {
    const fn = raw[key];
    if (typeof fn === "function") {
      __WdlHostRuntime__.defineProperty(wrappedDefault, key, {
        value: wrapHandler(raw, fn),
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  };
  __WdlHostRuntime__.forEachArray(HOST_WRAPPED_HANDLER_KEYS, (key) => {
    wrapDefaultFunctionKey(key);
  });
} else if (typeof raw === "function") {
  const source = __WdlHostRuntime__.functionSource(raw);
  if (__WdlHostRuntime__.regexpTest(/^\\s*class\\b/, source)) {
    wrappedDefault = class extends raw {
      constructor(ctx, env) {
        const requestContext = createRequestContext();
        const wrappedEnv = wrapEnv(env, requestContext);
        withTenantEnv(wrappedEnv, () => super(ctx, wrappedEnv));
        return wrapClassInstance(this, requestContext, wrappedEnv);
      }
    };
  } else {
    wrappedDefault = { fetch: wrapHandler(undefined, raw) };
  }
}

${namedEntrypoints}
export default wrappedDefault;
`;
}
