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
 * @param {string[]} d1Bindings
 * @param {string[]} r2Bindings
 * @param {string[]} doBindings
 * @param {Record<string, unknown>} workflowBindings
 * @param {string[]} entrypointNames
 */
export function generateHostBindingWrapperModule(userMainSpecifier, d1Bindings, r2Bindings, doBindings, workflowBindings, entrypointNames) {
  const userMain = JSON.stringify(`./${userMainSpecifier}`);
  const d1BindingJson = JSON.stringify(d1Bindings);
  const r2BindingJson = JSON.stringify(r2Bindings);
  const doBindingJson = JSON.stringify(doBindings);
  const workflowBindingJson = JSON.stringify(workflowBindings);
  // Host facade helper modules are only added to workerCode when bindings
  // exist; importing them unconditionally would 404 the resolver.
  const d1Import = d1Bindings.length ? `import { D1Database } from "./_wdl-d1-client.js";` : "";
  const r2Import = r2Bindings.length ? `import { R2Bucket } from "./_wdl-r2-client.js";` : "";
  const doImport = doBindings.length ? `import { DurableObjectNamespace } from "./_wdl-do-client.js";` : "";
  const workflowImport = Object.keys(workflowBindings).length ? `import { Workflow } from "./_wdl-workflows-client.js";` : "";
  const hidesRawEnvExports = doBindings.length || Object.keys(workflowBindings).length;
  const starExport = hidesRawEnvExports
    ? "// Internal Fetcher bindings are present; only wrapped entrypoints are re-exported."
    : `export * from ${userMain};`;
  const namedEntrypoints = entrypointNames.map((/** @type {string} */ name) => `
export class ${name} extends user.${name} {
  constructor(ctx, env) {
    const requestContext = createRequestContext();
    super(ctx, wrapEnv(env, requestContext));
    return wrapClassInstance(this, requestContext);
  }
}
`).join("");
  return `
import * as user from ${userMain};
import { WorkerEntrypoint, abortIsolate } from "cloudflare:workers";
${d1Import}
${r2Import}
${doImport}
${workflowImport}
// Local wrapper subclasses intentionally shadow same-name user exports, so
// declared entrypoints get facade-aware env even when star exports are present.
${starExport}

${WDL_ABORT_SHIM_SOURCE}

export class __WdlWorkflowNotify__ extends WorkerEntrypoint {
  async fetch(request) {
    return await notifyWorkflowCallback(request, wrapEnv(this.env, requestIdFromEventArg(request)));
  }
}

const D1_BINDINGS = ${d1BindingJson};
const R2_BINDINGS = ${r2BindingJson};
const DO_BINDINGS = ${doBindingJson};
const WORKFLOW_BINDINGS = ${workflowBindingJson};
const DO_BACKEND_BINDING = "__WDL_DO_BACKEND__";
const DO_OWNER_NETWORK_BINDING = "__WDL_DO_OWNER_NETWORK__";
const WORKFLOWS_BACKEND_BINDING = "__WDL_WORKFLOWS_BACKEND__";
const HOST_BINDINGS_WRAPPED = Symbol("wdl.host-bindings-wrapped");
const INTERNAL_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;

function requestIdFromEventArg(arg) {
  if (!arg || !arg.headers || typeof arg.headers.get !== "function") return null;
  return arg.headers.get("x-request-id");
}

function createRequestContext(requestId = null) {
  // workerd constructs a fresh WorkerEntrypoint instance per call; covered by
  // service-bindings-rpc.test.js. Do not share this object across instances.
  return { requestId };
}

function requestIdOptions(requestIdOrContext) {
  if (requestIdOrContext && typeof requestIdOrContext === "object") {
    return { requestIdProvider: () => requestIdOrContext.requestId };
  }
  return { requestId: requestIdOrContext };
}

function doOptions(requestIdOrContext, backend, ownerNetwork) {
  return { ...requestIdOptions(requestIdOrContext), backend, ownerNetwork };
}

function workflowOptions(requestIdOrContext, backend) {
  return { ...requestIdOptions(requestIdOrContext), backend };
}

function withRequestContext(context, arg, fn) {
  const previous = context.requestId;
  const requestId = requestIdFromEventArg(arg);
  if (requestId) context.requestId = requestId;
  try {
    const result = fn();
    if (result && typeof result.then === "function") {
      return Promise.resolve(result).finally(() => {
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

function wrapClassInstance(instance, requestContext) {
  return new Proxy(instance, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      return function(...args) {
        return withRequestContext(requestContext, args[0], () => value.apply(target, args));
      };
    },
  });
}

function wrapEnv(env, requestIdOrContext = null) {
  // Idempotence is a contract, not an optimization: WorkerEntrypoint methods
  // and default handlers may re-enter with an env already wrapped by this
  // module. A symbol marker cannot be forged by tenant vars/secrets.
  if (!env || env[HOST_BINDINGS_WRAPPED] === true) return env;
  const out = { ...env };
  const doBackend = out[DO_BACKEND_BINDING];
  const doOwnerNetwork = out[DO_OWNER_NETWORK_BINDING];
  const workflowsBackend = out[WORKFLOWS_BACKEND_BINDING];
  delete out[DO_BACKEND_BINDING];
  delete out[DO_OWNER_NETWORK_BINDING];
  delete out[WORKFLOWS_BACKEND_BINDING];
  for (const name of Object.keys(out)) {
    if (INTERNAL_BINDING_RE.test(name)) delete out[name];
  }
  for (const name of D1_BINDINGS) {
    if (out[name] !== undefined) out[name] = new D1Database(out[name], requestIdOptions(requestIdOrContext));
  }
  for (const name of R2_BINDINGS) {
    if (out[name] !== undefined) out[name] = new R2Bucket(out[name], requestIdOptions(requestIdOrContext));
  }
  for (const name of DO_BINDINGS) {
    if (out[name] !== undefined) {
      out[name] = new DurableObjectNamespace(out[name], doOptions(requestIdOrContext, doBackend, doOwnerNetwork));
    }
  }
  for (const [name, metadata] of Object.entries(WORKFLOW_BINDINGS)) {
    if (out[name] !== undefined) {
      out[name] = new Workflow(out[name] || metadata, workflowOptions(requestIdOrContext, workflowsBackend));
    }
  }
  Object.defineProperty(out, HOST_BINDINGS_WRAPPED, { value: true });
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
    return fn.call(owner, arg1, wrapEnv(env, requestIdFromEventArg(arg1)), ctx);
  };
}

const HOST_WRAPPED_HANDLER_KEYS = ["fetch", "scheduled", "queue", "tail"];

const raw = user.default;
let wrappedDefault = raw;

if (raw && typeof raw === "object") {
  wrappedDefault = { ...raw };
  const wrapDefaultFunctionKey = (key) => {
    const fn = raw[key];
    if (typeof fn === "function") {
      wrappedDefault[key] = wrapHandler(raw, fn);
    }
  };
  for (const key of HOST_WRAPPED_HANDLER_KEYS) {
    wrapDefaultFunctionKey(key);
  }
} else if (typeof raw === "function") {
  ${DEFAULT_EXPORT_SOURCE_SNIPPET}
  if (${DEFAULT_EXPORT_CLASS_TEST_SOURCE}) {
    wrappedDefault = class extends raw {
      constructor(ctx, env) {
        const requestContext = createRequestContext();
        super(ctx, wrapEnv(env, requestContext));
        return wrapClassInstance(this, requestContext);
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
