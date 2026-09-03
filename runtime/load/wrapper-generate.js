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

export const HOST_BINDING_RUNTIME_MODULE_NAME = "_wdl-host-wrapper-runtime.js";
export const WORKFLOW_KV_CAPTURE_MODULE_NAME = "_wdl-workflow-kv-capture.js";
export const HOST_BINDING_RUNTIME_SOURCE = `
import { sanitizeRequestId } from "./_wdl-request-id.js";

const IntrinsicObject = Object;
const IntrinsicArray = Array;
const IntrinsicError = Error;
const IntrinsicPromise = Promise;
const IntrinsicProxy = Proxy;
const IntrinsicReflect = Reflect;
const IntrinsicSymbol = Symbol;
const IntrinsicWeakMap = WeakMap;
const IntrinsicWeakSet = WeakSet;
const intrinsicArrayForEach = Array.prototype.forEach;
const intrinsicArrayIsArray = Array.isArray;
const intrinsicArrayPush = Array.prototype.push;
const intrinsicFunctionToString = Function.prototype.toString;
const intrinsicObjectDefineProperty = Object.defineProperty;
const intrinsicObjectCreate = Object.create;
const intrinsicObjectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const intrinsicObjectHasOwn = Object.hasOwn;
const intrinsicObjectIsPrototypeOf = Object.prototype.isPrototypeOf;
const intrinsicObjectKeys = Object.keys;
const intrinsicObjectSetPrototypeOf = Object.setPrototypeOf;
const intrinsicPromiseResolve = Promise.resolve;
const intrinsicPromiseThen = Promise.prototype.then;
const intrinsicReflectApply = Reflect.apply;
const intrinsicReflectDeleteProperty = Reflect.deleteProperty;
const intrinsicReflectGet = Reflect.get;
const intrinsicReflectGetPrototypeOf = Reflect.getPrototypeOf;
const intrinsicRegExpTest = RegExp.prototype.test;
const intrinsicWeakSetAdd = WeakSet.prototype.add;
const intrinsicWeakSetHas = WeakSet.prototype.has;
const intrinsicWeakMapGet = WeakMap.prototype.get;
const intrinsicWeakMapHas = WeakMap.prototype.has;
const intrinsicWeakMapSet = WeakMap.prototype.set;

export function applyFunction(fn, receiver, args) {
  return intrinsicReflectApply(fn, receiver, args);
}

export function createPrivateSymbol(description) {
  return IntrinsicSymbol(description);
}

export function createPrivateIdentitySet() {
  const values = new IntrinsicWeakSet();
  return {
    add(value) {
      intrinsicReflectApply(intrinsicWeakSetAdd, values, [value]);
    },
    has(value) {
      return intrinsicReflectApply(intrinsicWeakSetHas, values, [value]);
    },
  };
}

export function createPrivateIdentityMap() {
  const values = new IntrinsicWeakMap();
  return {
    get(key) {
      return intrinsicReflectApply(intrinsicWeakMapGet, values, [key]);
    },
    has(key) {
      return intrinsicReflectApply(intrinsicWeakMapHas, values, [key]);
    },
    set(key, value) {
      intrinsicReflectApply(intrinsicWeakMapSet, values, [key, value]);
    },
  };
}

export function createNullPrototypeObject() {
  return intrinsicReflectApply(intrinsicObjectCreate, IntrinsicObject, [null]);
}

function defineIntrinsicDataProperty(target, name, value) {
  const descriptor = intrinsicReflectApply(
    intrinsicObjectCreate,
    IntrinsicObject,
    [null]
  );
  descriptor.value = value;
  intrinsicReflectApply(intrinsicObjectDefineProperty, IntrinsicObject, [
    target,
    name,
    descriptor,
  ]);
}

let privateErrorPrototype = null;

function getPrivateErrorPrototype() {
  if (privateErrorPrototype === null) {
    // Workerd reads these through ordinary Get(), but copies generic own Error
    // fields through a setter-sensitive temporary object during serialization.
    privateErrorPrototype = intrinsicReflectApply(
      intrinsicObjectCreate,
      IntrinsicObject,
      [IntrinsicError.prototype]
    );
    defineIntrinsicDataProperty(privateErrorPrototype, "stack", undefined);
    defineIntrinsicDataProperty(privateErrorPrototype, "overloaded", false);
    defineIntrinsicDataProperty(privateErrorPrototype, "retryable", false);
  }
  return privateErrorPrototype;
}

export function createError(message) {
  const error = new IntrinsicError(message);
  intrinsicReflectApply(intrinsicObjectSetPrototypeOf, IntrinsicObject, [
    error,
    getPrivateErrorPrototype(),
  ]);
  defineIntrinsicDataProperty(error, "name", "Error");
  intrinsicReflectApply(intrinsicReflectDeleteProperty, IntrinsicReflect, [error, "stack"]);
  return error;
}

export function captureRpcMethod(target, property, shadowPrototypes) {
  const hiddenPrototypes = [];
  const hiddenDescriptors = [];
  try {
    for (let index = 0; index < shadowPrototypes.length; index += 1) {
      const prototype = shadowPrototypes[index];
      const descriptor = intrinsicReflectApply(
        intrinsicObjectGetOwnPropertyDescriptor,
        IntrinsicObject,
        [prototype, property]
      );
      if (descriptor === undefined) continue;
      if (descriptor.configurable !== true) {
        throw new TypeError("RPC method is shadowed by a non-configurable prototype property");
      }
      const deleted = intrinsicReflectApply(
        intrinsicReflectDeleteProperty,
        IntrinsicReflect,
        [prototype, property]
      );
      if (!deleted) throw new TypeError("RPC method shadow could not be removed");
      intrinsicReflectApply(intrinsicArrayPush, hiddenPrototypes, [prototype]);
      intrinsicReflectApply(intrinsicArrayPush, hiddenDescriptors, [descriptor]);
    }
    const fn = intrinsicReflectApply(intrinsicReflectGet, IntrinsicReflect, [
      target,
      property,
      target,
    ]);
    if (typeof fn !== "function") throw new TypeError("RPC method is unavailable");
    return fn;
  } finally {
    for (let index = hiddenPrototypes.length - 1; index >= 0; index -= 1) {
      intrinsicReflectApply(intrinsicObjectDefineProperty, IntrinsicObject, [
        hiddenPrototypes[index],
        property,
        hiddenDescriptors[index],
      ]);
    }
  }
}

export function getPrototypeOf(value) {
  return intrinsicReflectApply(intrinsicReflectGetPrototypeOf, IntrinsicReflect, [value]);
}

export function isPrototypeOf(prototype, value) {
  return intrinsicReflectApply(intrinsicObjectIsPrototypeOf, prototype, [value]);
}

export function createProxy(target, handler) {
  return new IntrinsicProxy(target, handler);
}

export function defineProperty(target, property, descriptor) {
  return intrinsicReflectApply(intrinsicObjectDefineProperty, IntrinsicObject, [target, property, descriptor]);
}

export function deleteProperty(target, property) {
  return intrinsicReflectApply(intrinsicReflectDeleteProperty, IntrinsicReflect, [target, property]);
}

export function forEachArray(values, callback) {
  intrinsicReflectApply(intrinsicArrayForEach, values, [callback]);
}

export function isArray(value) {
  return intrinsicReflectApply(intrinsicArrayIsArray, IntrinsicArray, [value]);
}

export function pushArray(values, value) {
  intrinsicReflectApply(intrinsicArrayPush, values, [value]);
}

export function functionSource(fn) {
  return intrinsicReflectApply(intrinsicFunctionToString, fn, []);
}

export function objectKeys(record) {
  return intrinsicReflectApply(intrinsicObjectKeys, IntrinsicObject, [record]);
}

export function objectHasOwn(record, property) {
  return intrinsicReflectApply(intrinsicObjectHasOwn, IntrinsicObject, [record, property]);
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

/**
 * Capture raw KV callables before the tenant module can mutate importable env.
 * `disallow_importable_env` leaves module env empty, so that configuration uses
 * the invocation-owned binding hidden behind the generated facade instead.
 *
 * @param {string[]} kvBindings
 * @param {{ importableEnvDisabled?: boolean, rpcMethod?: string, reportOrigin?: string }} [options]
 */
export function generateWorkflowKvCaptureModule(kvBindings, options = {}) {
  const importableEnvDisabled = options.importableEnvDisabled === true;
  if (typeof options.rpcMethod !== "string" || options.rpcMethod.length === 0) {
    throw new TypeError("Workflow KV RPC method is required");
  }
  if (typeof options.reportOrigin !== "string" || options.reportOrigin.length === 0) {
    throw new TypeError("Workflow infrastructure report origin is required");
  }
  return `
import {
  env as __WdlRawEnv__,
  RpcPromise as __WdlRpcPromise__,
  ServiceStub as __WdlServiceStub__,
} from "cloudflare:workers";
import * as __WdlHostRuntime__ from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";

const IntrinsicPromise = Promise;
const IMPORTABLE_ENV_DISABLED = ${JSON.stringify(importableEnvDisabled)};
const KV_BINDINGS = ${JSON.stringify(kvBindings)};
const KV_RPC_METHOD = ${JSON.stringify(options.rpcMethod)};
const captured = [];
const fallbackCapturesByBinding = [];
const SERVICE_STUB_PROTOTYPES = [];
let serviceStubPrototype = __WdlServiceStub__.prototype;
while (serviceStubPrototype !== null) {
  SERVICE_STUB_PROTOTYPES.push(serviceStubPrototype);
  serviceStubPrototype = __WdlHostRuntime__.getPrototypeOf(serviceStubPrototype);
}
const SERVICE_STUB_FETCH = __WdlHostRuntime__.reflectGet(
  __WdlServiceStub__.prototype,
  "fetch",
  __WdlServiceStub__.prototype
);
const RPC_PROMISE_PROTOTYPE = __WdlRpcPromise__.prototype;
const RPC_PROMISE_THEN = __WdlHostRuntime__.reflectGet(
  RPC_PROMISE_PROTOTYPE,
  "then",
  RPC_PROMISE_PROTOTYPE
);
const PROMISE_PROTOTYPE = IntrinsicPromise.prototype;
const PROMISE_THEN = __WdlHostRuntime__.reflectGet(
  PROMISE_PROTOTYPE,
  "then",
  PROMISE_PROTOTYPE
);

function assertServiceStubPrototypeChain() {
  for (let index = 0; index < SERVICE_STUB_PROTOTYPES.length; index += 1) {
    const expected = SERVICE_STUB_PROTOTYPES[index + 1] ?? null;
    if (__WdlHostRuntime__.getPrototypeOf(SERVICE_STUB_PROTOTYPES[index]) !== expected) {
      throw new TypeError("ServiceStub prototype chain changed before KV binding capture");
    }
  }
}

function settleHostResult(result, onRejected = null) {
  let then = null;
  if (__WdlHostRuntime__.isPrototypeOf(RPC_PROMISE_PROTOTYPE, result)) {
    then = RPC_PROMISE_THEN;
  } else if (__WdlHostRuntime__.isPrototypeOf(PROMISE_PROTOTYPE, result)) {
    __WdlHostRuntime__.defineProperty(result, "constructor", {
      value: undefined,
    });
    then = PROMISE_THEN;
  }
  if (then === null) return result;
  // Keep downstream await away from tenant-mutated native Promise settlement.
  const settlement = __WdlHostRuntime__.createNullPrototypeObject();
  __WdlHostRuntime__.defineProperty(settlement, "then", {
    value(resolve, reject) {
      return __WdlHostRuntime__.applyFunction(then, result, [
        resolve,
        (error) => {
          if (onRejected !== null) {
            try {
              __WdlHostRuntime__.applyFunction(onRejected, undefined, [error]);
            } catch (callbackError) {
              return __WdlHostRuntime__.applyFunction(reject, undefined, [callbackError]);
            }
          }
          return __WdlHostRuntime__.applyFunction(reject, undefined, [error]);
        },
      ]);
    },
  });
  return settlement;
}

function captureBinding(binding, name) {
  if (!binding || (typeof binding !== "object" && typeof binding !== "function")) {
    throw new TypeError("Workflow KV binding " + name + " is unavailable");
  }
  assertServiceStubPrototypeChain();
  const invoke = __WdlHostRuntime__.captureRpcMethod(
    binding,
    KV_RPC_METHOD,
    SERVICE_STUB_PROTOTYPES
  );
  return (operation, args, onRejected = null) => {
    const callArgs = [operation];
    for (let index = 0; index < args.length; index += 1) {
      __WdlHostRuntime__.pushArray(callArgs, args[index]);
    }
    return settleHostResult(
      __WdlHostRuntime__.applyFunction(invoke, binding, callArgs),
      onRejected
    );
  };
}

if (!IMPORTABLE_ENV_DISABLED) {
  for (let bindingIndex = 0; bindingIndex < KV_BINDINGS.length; bindingIndex += 1) {
    const name = KV_BINDINGS[bindingIndex];
    const binding = __WdlHostRuntime__.reflectGet(__WdlRawEnv__, name, __WdlRawEnv__);
    captured[bindingIndex] = captureBinding(binding, name);
  }
} else {
  for (let bindingIndex = 0; bindingIndex < KV_BINDINGS.length; bindingIndex += 1) {
    // Reuse only for the same raw binding identity; wrapper construction is
    // tenant-callable.
    fallbackCapturesByBinding[bindingIndex] = __WdlHostRuntime__.createPrivateIdentityMap();
  }
}

export function bindWorkflowKvBinding(bindingIndex, fallbackBinding) {
  let callBinding = captured[bindingIndex];
  if (IMPORTABLE_ENV_DISABLED) {
    const fallbackCache = fallbackCapturesByBinding[bindingIndex];
    if (!fallbackCache) throw new TypeError("Workflow KV binding capture is unavailable");
    callBinding = fallbackCache.get(fallbackBinding);
    if (!callBinding) {
      callBinding = captureBinding(fallbackBinding, KV_BINDINGS[bindingIndex]);
      fallbackCache.set(fallbackBinding, callBinding);
    }
  }
  if (!callBinding) throw new TypeError("Workflow KV binding capture is unavailable");
  return callBinding;
}

export function bindWorkflowInfrastructureReporter(reporter) {
  if (typeof SERVICE_STUB_FETCH !== "function") {
    throw new TypeError("Workflow infrastructure reporter fetch method is unavailable");
  }
  return (code) => settleHostResult(
    __WdlHostRuntime__.applyFunction(SERVICE_STUB_FETCH, reporter, [
      ${JSON.stringify(options.reportOrigin)} + "/" + code,
    ])
  );
}
`;
}

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
 *   kvBindings?: string[],
 *   d1Bindings?: string[],
 *   r2Bindings?: string[],
 *   doBindings?: string[],
 *   workflowBindings?: Record<string, unknown>,
 *   workflowClassNames?: string[],
 *   workflowInfrastructureReporterProp?: string,
 *   kvReadInfrastructureErrorCode?: string,
 *   entrypointNames?: string[],
 *   aiBindings?: string[],
 *   importableEnvDisabled?: boolean,
 * }} [options]
 */
export function generateHostBindingWrapperModule(userMainSpecifier, options = {}) {
  const {
    kvBindings = [],
    d1Bindings = [],
    r2Bindings = [],
    doBindings = [],
    workflowBindings = {},
    workflowClassNames = [],
    workflowInfrastructureReporterProp = null,
    kvReadInfrastructureErrorCode = null,
    entrypointNames = [],
    aiBindings = [],
    importableEnvDisabled = false,
  } = options;
  const userMain = JSON.stringify(`./${userMainSpecifier}`);
  const kvBindingJson = JSON.stringify(kvBindings);
  const d1BindingJson = JSON.stringify(d1Bindings);
  const r2BindingJson = JSON.stringify(r2Bindings);
  const doBindingJson = JSON.stringify(doBindings);
  const aiBindingJson = JSON.stringify(aiBindings);
  const workflowBindingJson = JSON.stringify(Object.keys(workflowBindings));
  const workflowInfrastructureReporterPropJson = JSON.stringify(
    workflowInfrastructureReporterProp
  );
  const kvReadInfrastructureErrorCodeJson = JSON.stringify(
    kvReadInfrastructureErrorCode
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
  const workflowClassNameSet = new Set(workflowClassNames);
  const hasWorkflowClasses = workflowClassNames.length > 0;
  if (
    hasWorkflowClasses &&
    (typeof workflowInfrastructureReporterProp !== "string" ||
      workflowInfrastructureReporterProp.length === 0)
  ) {
    throw new TypeError("Workflow infrastructure reporter prop is required");
  }
  const needsWorkflowKvFacade = kvBindings.length > 0 && workflowClassNames.length > 0;
  if (
    needsWorkflowKvFacade &&
    (typeof kvReadInfrastructureErrorCode !== "string" ||
      kvReadInfrastructureErrorCode.length === 0)
  ) {
    throw new TypeError("KV read infrastructure error code is required");
  }
  const kvCaptureImport = needsWorkflowKvFacade
    ? `import { bindWorkflowInfrastructureReporter as __WdlBindWorkflowInfrastructureReporter__, bindWorkflowKvBinding as __WdlBindWorkflowKvBinding__ } from "./${WORKFLOW_KV_CAPTURE_MODULE_NAME}";`
    : "";
  const kvFacadeSource = needsWorkflowKvFacade ? `
const WORKFLOW_KV_INFRASTRUCTURE_SOURCES = __WdlHostRuntime__.createPrivateIdentityMap();

function prepareWorkflowKvKey(key) {
  if (typeof key === "string") return key;
  if (!__WdlHostRuntime__.isArray(key)) {
    throw new TypeError("KV read key must be a string or an array of strings");
  }
  const keys = [];
  const length = __WdlHostRuntime__.reflectGet(key, "length", key);
  for (let index = 0; index < length; index += 1) {
    if (!__WdlHostRuntime__.objectHasOwn(key, index)) {
      throw new TypeError("KV batch read keys must not contain empty slots");
    }
    const value = __WdlHostRuntime__.reflectGet(key, index, key);
    if (typeof value !== "string") {
      throw new TypeError("KV batch read keys must be strings");
    }
    __WdlHostRuntime__.pushArray(keys, value);
  }
  return keys;
}

function prepareWorkflowKvGetOptions(options) {
  if (options === undefined || typeof options === "string") return options;
  if (!options || typeof options !== "object" || __WdlHostRuntime__.isArray(options)) {
    throw new TypeError("KV read options must be a type string or an options object");
  }
  const type = __WdlHostRuntime__.reflectGet(options, "type", options);
  if (type !== undefined && typeof type !== "string") {
    throw new TypeError("KV read options type must be a string");
  }
  const cacheTtl = __WdlHostRuntime__.reflectGet(options, "cacheTtl", options);
  if (cacheTtl !== undefined && typeof cacheTtl !== "number") {
    throw new TypeError("KV read options cacheTtl must be a number");
  }
  return { type, cacheTtl };
}

function workflowKvListOption(options, name, type, nullable = false) {
  const value = __WdlHostRuntime__.reflectGet(options, name, options);
  if (value !== undefined && !(nullable && value === null) && typeof value !== type) {
    throw new TypeError(\`KV list option \${name} must be a \${type}\`);
  }
  return value;
}

function prepareWorkflowKvListOptions(options) {
  if (options === undefined) return undefined;
  if (!options || typeof options !== "object" || __WdlHostRuntime__.isArray(options)) {
    throw new TypeError("KV list options must be an object");
  }
  return {
    prefix: workflowKvListOption(options, "prefix", "string", true),
    cursor: workflowKvListOption(options, "cursor", "string", true),
    limit: workflowKvListOption(options, "limit", "number"),
    metadata: workflowKvListOption(options, "metadata", "boolean"),
  };
}

function brandWorkflowKvInfrastructureError(callBinding, error) {
  if (
    error &&
    (typeof error === "object" || typeof error === "function") &&
    __WdlHostRuntime__.objectHasOwn(error, "code") &&
    __WdlHostRuntime__.reflectGet(error, "code", error) === KV_READ_INFRASTRUCTURE_ERROR_CODE
  ) {
    // Preserve the first observed source binding for this Error identity.
    if (!WORKFLOW_KV_INFRASTRUCTURE_SOURCES.has(error)) {
      WORKFLOW_KV_INFRASTRUCTURE_SOURCES.set(error, callBinding);
    }
  }
}

function callWorkflowKvReadBinding(callBinding, brandError, method, args) {
  return __WdlHostRuntime__.applyFunction(callBinding, undefined, [
    method,
    args,
    brandError,
  ]);
}

function wrapKvBinding(binding, bindingIndex, requestContext) {
  const callBinding = __WdlBindWorkflowKvBinding__(bindingIndex, binding);
  if (
    requestContext &&
    typeof requestContext === "object" &&
    requestContext.infrastructureReporter !== null
  ) {
    requestContext.workflowKvBindings ??= __WdlHostRuntime__.createPrivateIdentitySet();
    requestContext.workflowKvBindings.add(callBinding);
  }
  const brandError = (error) => brandWorkflowKvInfrastructureError(callBinding, error);
  return {
    async get(key, options) {
      const preparedKey = prepareWorkflowKvKey(key);
      const preparedOptions = prepareWorkflowKvGetOptions(options);
      return callWorkflowKvReadBinding(
        callBinding,
        brandError,
        "get",
        [preparedKey, preparedOptions]
      );
    },
    async getWithMetadata(key, options) {
      const preparedKey = prepareWorkflowKvKey(key);
      const preparedOptions = prepareWorkflowKvGetOptions(options);
      return callWorkflowKvReadBinding(
        callBinding,
        brandError,
        "getWithMetadata",
        [preparedKey, preparedOptions]
      );
    },
    async put(key, value, options) {
      return __WdlHostRuntime__.applyFunction(
        callBinding,
        undefined,
        ["put", [key, value, options]]
      );
    },
    async delete(key) {
      return __WdlHostRuntime__.applyFunction(callBinding, undefined, ["delete", [key]]);
    },
    async list(options) {
      const preparedOptions = prepareWorkflowKvListOptions(options);
      return callWorkflowKvReadBinding(
        callBinding,
        brandError,
        "list",
        [preparedOptions]
      );
    },
  };
}
` : "";
  // An ordinary handler can initialize a module-scoped binding cache that a
  // later Workflow invocation reuses, so every invocation needs the facade.
  const kvEnvWrappingSource = needsWorkflowKvFacade ? `
  __WdlHostRuntime__.forEachArray(KV_BINDINGS, (name, bindingIndex) => {
    if (out[name] !== undefined) {
      out[name] = wrapKvBinding(out[name], bindingIndex, requestIdOrContext);
    }
  });
` : "";
  const workflowInfrastructureSource = hasWorkflowClasses ? `
function takeWorkflowInfrastructureReporter(ctx) {
  if (!ctx || typeof ctx !== "object") return null;
  const props = __WdlHostRuntime__.reflectGet(ctx, "props", ctx);
  if (!props || typeof props !== "object") {
    return null;
  }
  const hasReporter = __WdlHostRuntime__.objectHasOwn(
    props,
    WORKFLOW_INFRASTRUCTURE_REPORTER_PROP
  );
  const hasReportId = __WdlHostRuntime__.objectHasOwn(
    props,
    WORKFLOW_INFRASTRUCTURE_REPORT_ID_PROP
  );
  if (!hasReporter && !hasReportId) return null;
  if (!hasReporter || !hasReportId) {
    throw new TypeError("Workflow infrastructure reporter is invalid");
  }
  const reporter = __WdlHostRuntime__.reflectGet(
    props,
    WORKFLOW_INFRASTRUCTURE_REPORTER_PROP,
    props
  );
  const reportId = __WdlHostRuntime__.reflectGet(
    props,
    WORKFLOW_INFRASTRUCTURE_REPORT_ID_PROP,
    props
  );
  if (
    !__WdlHostRuntime__.deleteProperty(props, WORKFLOW_INFRASTRUCTURE_REPORTER_PROP) ||
    !__WdlHostRuntime__.deleteProperty(props, WORKFLOW_INFRASTRUCTURE_REPORT_ID_PROP)
  ) {
    throw new TypeError("Workflow infrastructure reporter could not be consumed");
  }
  if (
    !reporter ||
    (typeof reporter !== "object" && typeof reporter !== "function") ||
    typeof reportId !== "string" ||
    reportId.length === 0
  ) {
    throw new TypeError("Workflow infrastructure reporter is invalid");
  }
  ${needsWorkflowKvFacade
    ? `const bound = __WdlBindWorkflowInfrastructureReporter__(reporter);
  __WdlHostRuntime__.defineProperty(bound, WORKFLOW_INFRASTRUCTURE_REPORT_ID, {
    value: reportId,
  });
  return bound;`
    : "return true;"}
}

function contextOnlyEnv(requestContext) {
  requestContext.contextOnlyEnv ??= __WdlHostRuntime__.createProxy({}, {
    get() {
      return undefined;
    },
    set() {
      return true;
    },
    defineProperty() {
      return true;
    },
    deleteProperty() {
      return true;
    },
    has() {
      return false;
    },
    ownKeys() {
      return [];
    },
    getOwnPropertyDescriptor() {
      return undefined;
    },
  });
  return requestContext.contextOnlyEnv;
}

${needsWorkflowKvFacade ? `async function invokeWorkflowBoundary(callback, requestContext) {
  try {
    return await __WdlHostRuntime__.applyFunction(callback, undefined, []);
  } catch (error) {
    const source = WORKFLOW_KV_INFRASTRUCTURE_SOURCES.get(error);
    if (source && requestContext.workflowKvBindings?.has(source)) {
      const report = requestContext.infrastructureReporter;
      if (!report) {
        throw new TypeError("Workflow infrastructure reporter is unavailable");
      }
      try {
        await __WdlHostRuntime__.applyFunction(
          report,
          undefined,
          [KV_READ_INFRASTRUCTURE_ERROR_CODE]
        );
      } catch {
        // Preserve fail-closed classification when the report side-channel
        // rejects before its host-local latch is set.
        const reportError = __WdlHostRuntime__.createError(
          WORKFLOW_INFRASTRUCTURE_REPORT_FAILURE_PREFIX +
          __WdlHostRuntime__.reflectGet(
            report,
            WORKFLOW_INFRASTRUCTURE_REPORT_ID,
            report
          )
        );
        throw reportError;
      }
    }
    throw error;
  }
}` : ""}

function wrapWorkflowStepCallback(callback, requestContext, wrappedEnv) {
  return function(...args) {
    return withTenantEnv(wrappedEnv, () =>
      ${needsWorkflowKvFacade
        ? "invokeWorkflowBoundary(() => __WdlHostRuntime__.applyFunction(callback, undefined, args), requestContext)"
        : "__WdlHostRuntime__.applyFunction(callback, undefined, args)"}, requestContext);
  };
}

function wrapWorkflowStep(step, requestContext, wrappedEnv) {
  const method = (name) => {
    if (!__WdlHostRuntime__.objectHasOwn(step, name)) {
      throw new TypeError("Workflow step facade omitted method " + name);
    }
    const value = __WdlHostRuntime__.reflectGet(step, name, step);
    if (typeof value !== "function") {
      throw new TypeError("Workflow step facade method " + name + " is invalid");
    }
    return value;
  };
  const facade = __WdlHostRuntime__.createNullPrototypeObject();
  __WdlHostRuntime__.defineProperty(facade, "do", {
    enumerable: true,
    value(name, configOrCallback, maybeCallback) {
      const rawDo = method("do");
      if (typeof configOrCallback === "function") {
        return __WdlHostRuntime__.applyFunction(rawDo, step, [
          name,
          wrapWorkflowStepCallback(configOrCallback, requestContext, wrappedEnv),
        ]);
      }
      return __WdlHostRuntime__.applyFunction(rawDo, step, [
        name,
        configOrCallback,
        typeof maybeCallback === "function"
          ? wrapWorkflowStepCallback(maybeCallback, requestContext, wrappedEnv)
          : maybeCallback,
      ]);
    },
  });
  __WdlHostRuntime__.forEachArray(["sleep", "sleepUntil", "waitForEvent"], (name) => {
    __WdlHostRuntime__.defineProperty(facade, name, {
      enumerable: true,
      value(...args) {
        const rawMethod = method(name);
        return __WdlHostRuntime__.applyFunction(rawMethod, step, args);
      },
    });
  });
  return facade;
}
` : "";
  const namedEntrypoints = entrypointNames.map((/** @type {string} */ name, index) => `
const __WdlWrappedEntrypoint${index}__ = ({
  [${JSON.stringify(name)}]: class extends __WdlUserModule__.${name} {
    constructor(ctx, env) {
      const requestContext = createRequestContext(
        null,
        ${workflowClassNameSet.has(name) ? "takeWorkflowInfrastructureReporter(ctx)" : "null"}
      );
      const workflowInfrastructure = requestContext.infrastructureReporter !== null;
      const wrappedEnv = wrapEnv(env, requestContext);
      withTenantEnv(wrappedEnv, () => super(ctx, wrappedEnv), requestContext);
      return wrapClassInstance(
        this,
        requestContext,
        wrappedEnv,
        workflowInfrastructure
      );
    }
  },
})[${JSON.stringify(name)}];
export { __WdlWrappedEntrypoint${index}__ as ${name} };
`).join("");
  return `
import { WorkerEntrypoint, abortIsolate, withEnv } from "cloudflare:workers";
import * as __WdlHostRuntime__ from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";
${kvCaptureImport}
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

${needsWorkflowKvFacade ? `const KV_BINDINGS = ${kvBindingJson};` : ""}
${needsWorkflowKvFacade ? `const KV_READ_INFRASTRUCTURE_ERROR_CODE = ${kvReadInfrastructureErrorCodeJson};` : ""}
const D1_BINDINGS = ${d1BindingJson};
const R2_BINDINGS = ${r2BindingJson};
const DO_BINDINGS = ${doBindingJson};
const AI_BINDINGS = ${aiBindingJson};
const WORKFLOW_BINDINGS = ${workflowBindingJson};
const IMPORTABLE_ENV_DISABLED = ${JSON.stringify(importableEnvDisabled)};
const AI_CATALOG_SCOPE = {};
const HOST_BINDINGS_WRAPPED = __WdlHostRuntime__.createPrivateSymbol("wdl.host-bindings-wrapped");
${hasWorkflowClasses ? `const WORKFLOW_INFRASTRUCTURE_REPORTER_PROP = ${workflowInfrastructureReporterPropJson};` : ""}
${hasWorkflowClasses ? "const WORKFLOW_INFRASTRUCTURE_REPORT_ID_PROP = WORKFLOW_INFRASTRUCTURE_REPORTER_PROP + \"Id\";" : ""}
${needsWorkflowKvFacade ? "const WORKFLOW_INFRASTRUCTURE_REPORT_FAILURE_PREFIX = WORKFLOW_INFRASTRUCTURE_REPORTER_PROP + \":\";" : ""}
${needsWorkflowKvFacade ? "const WORKFLOW_INFRASTRUCTURE_REPORT_ID = __WdlHostRuntime__.createPrivateSymbol(\"wdl.workflow-infrastructure-report-id\");" : ""}
const INTERNAL_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;

function requestIdFromEventArg(arg) {
  return __WdlHostRuntime__.requestIdFromEventArg(arg);
}

function createRequestContext(requestId = null, infrastructureReporter = null) {
  return {
    requestId,
    infrastructureReporter,
    workflowKvBindings: null,
    contextOnlyEnv: null,
  };
}

function requestIdOptions(requestIdOrContext) {
  return requestIdOrContext && typeof requestIdOrContext === "object"
    ? { requestIdProvider: () => requestIdOrContext.requestId }
    : { requestId: requestIdOrContext };
}

${workflowInfrastructureSource}

function withTenantEnv(env, callback, requestContext = null) {
  if (!IMPORTABLE_ENV_DISABLED) return withEnv(env, callback);
  return requestContext && requestContext.infrastructureReporter !== null
    ? withEnv(contextOnlyEnv(requestContext), callback)
    : __WdlHostRuntime__.applyFunction(callback, undefined, []);
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

${kvFacadeSource}

function wrapClassInstance(
  instance,
  requestContext,
  wrappedEnv,
  workflowInfrastructure = false
) {
  return __WdlHostRuntime__.createProxy(instance, {
    get(target, prop) {
      return withTenantEnv(wrappedEnv, () => {
        const value = __WdlHostRuntime__.reflectGet(target, prop, target);
        if (typeof value !== "function") return value;
        return function(...args) {
          return withTenantEnv(wrappedEnv, () =>
            withRequestContext(requestContext, args[0], () => {
              const callArgs = workflowInfrastructure && prop === "run"
                ? [args[0], wrapWorkflowStep(args[1], requestContext, wrappedEnv)]
                : args;
              const invoke = () => __WdlHostRuntime__.applyFunction(value, target, callArgs);
              return ${needsWorkflowKvFacade
                ? `workflowInfrastructure && prop === "run"
                ? invokeWorkflowBoundary(invoke, requestContext)
                : invoke()`
                : "invoke()"};
            }), requestContext);
        };
      }, requestContext);
    },
  });
}

let lastRawEnv = null;
let lastEnvTemplate = null;

function envTemplate(env) {
  if (env === lastRawEnv && lastEnvTemplate) return lastEnvTemplate;
  const template = { ...env };
  __WdlHostRuntime__.forEachArray(__WdlHostRuntime__.objectKeys(template), (name) => {
    if (__WdlHostRuntime__.regexpTest(INTERNAL_BINDING_RE, name)) delete template[name];
  });
  lastRawEnv = env;
  lastEnvTemplate = template;
  return template;
}

function wrapEnv(env, requestIdOrContext = null) {
  // Idempotence is a contract, not an optimization: WorkerEntrypoint methods
  // and default handlers may re-enter with an env already wrapped by this
  // module. A symbol marker cannot be forged by tenant vars/secrets.
  if (!env || env[HOST_BINDINGS_WRAPPED] === true) return env;
  const out = { ...envTemplate(env) };
${kvEnvWrappingSource}
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
    if (out[name] !== undefined) {
      out[name] = new Ai(out[name], requestIdOptions(requestIdOrContext), AI_CATALOG_SCOPE);
    }
  });
  __WdlHostRuntime__.forEachArray(WORKFLOW_BINDINGS, (name) => {
    out[name] = new Workflow(out[name], requestIdOptions(requestIdOrContext));
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
