import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  WORKFLOW_KV_CAPTURE_MODULE_NAME,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
  generateWorkflowKvCaptureModule,
} from "../../runtime/load/wrapper-generate.js";
import { WORKFLOW_INFRASTRUCTURE_REPORTER_PROP } from "../../runtime/load/module-rewrite.js";
import {
  KV_FACADE_RPC_METHOD,
  KV_READ_INFRASTRUCTURE_ERROR_CODE,
  WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
  runtimeInfrastructureError,
} from "../../runtime/infrastructure-error.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";

const HOST_BINDING_RUNTIME_TEST_SOURCE = applyModuleReplacements(HOST_BINDING_RUNTIME_SOURCE, [
  [
    'from "./_wdl-request-id.js"',
    `from ${JSON.stringify(repositoryFileUrl("runtime/_wdl-request-id.js"))}`,
  ],
]);
const KV_BINDINGS = ["CACHE"];
let workflowWrapperModuleEnvSequence = 0;

function generatedWrappers() {
  return {
    abortOnly: generateAbortShimWrapperModule("worker.js"),
    hostBindings: generateHostBindingWrapperModule("worker.js"),
  };
}

test("host wrapper runtime exports only helpers consumed by generated wrappers", () => {
  const exported = [...HOST_BINDING_RUNTIME_SOURCE.matchAll(/^export function (\w+)\(/gm)]
    .map((match) => match[1])
    .toSorted();
  const generatedSources = [
    generateHostBindingWrapperModule("worker.js", {
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      kvBindings: KV_BINDINGS,
      workflowInfrastructureReporterProp: WORKFLOW_INFRASTRUCTURE_REPORTER_PROP,
      kvReadInfrastructureErrorCode: KV_READ_INFRASTRUCTURE_ERROR_CODE,
    }),
    generateWorkflowKvCaptureModule(KV_BINDINGS, {
      rpcMethod: KV_FACADE_RPC_METHOD,
      reportOrigin: WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
    }),
  ];
  const consumed = [...new Set(
    generatedSources.flatMap((source) =>
      [...source.matchAll(/__WdlHostRuntime__\.(\w+)/g)].map((match) => match[1])
    )
  )].toSorted();
  assert.deepEqual(exported, consumed);
});

test("host wrapper RPC capture restores earlier prototype shadows on failure", async () => {
  const hostRuntime = await import(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE));
  const outer = Object.create(null);
  const inner = Object.create(outer);
  const target = Object.create(inner);
  Object.defineProperty(inner, "list", {
    configurable: true,
    value: "inner-shadow",
  });
  Object.defineProperty(outer, "list", {
    configurable: false,
    value: "outer-shadow",
  });

  assert.throws(
    () => hostRuntime.captureRpcMethod(target, "list", [inner, outer]),
    /non-configurable prototype property/
  );
  assert.equal(Object.getOwnPropertyDescriptor(inner, "list")?.value, "inner-shadow");
});

/**
 * @param {string} userSource
 * @param {{ importableEnvDisabled?: boolean, moduleEnv?: Record<string, unknown> }} [options]
 */
async function loadWorkflowWrapper(userSource, options = {}) {
  const moduleEnvKey = `__wdlWorkflowWrapperEnv${workflowWrapperModuleEnvSequence += 1}`;
  const globals = /** @type {Record<string, unknown>} */ (
    /** @type {unknown} */ (globalThis)
  );
  globals[moduleEnvKey] = options.moduleEnv || {};
  const cloudflareUrl = moduleDataUrl(`
    import { AsyncLocalStorage } from "node:async_hooks";
    const envStorage = new AsyncLocalStorage();
    const moduleEnv = globalThis[${JSON.stringify(moduleEnvKey)}];
    export const env = new Proxy({}, {
      get(_target, property) {
        return Reflect.get(envStorage.getStore() || moduleEnv, property);
      },
      set(_target, property, value) {
        return Reflect.set(envStorage.getStore() || moduleEnv, property, value);
      },
      defineProperty(_target, property, descriptor) {
        return Reflect.defineProperty(envStorage.getStore() || moduleEnv, property, descriptor);
      },
      deleteProperty(_target, property) {
        return Reflect.deleteProperty(envStorage.getStore() || moduleEnv, property);
      },
      has(_target, property) {
        return Reflect.has(envStorage.getStore() || moduleEnv, property);
      },
      ownKeys() { return Reflect.ownKeys(envStorage.getStore() || moduleEnv); },
      getOwnPropertyDescriptor(_target, property) {
        return Reflect.getOwnPropertyDescriptor(
          envStorage.getStore() || moduleEnv,
          property
        );
      },
    });
    export class ServiceStub {
      async fetch(input) { return this.__fetch(input); }
    }
    export class RpcPromise extends Promise {}
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(value, fn) { return envStorage.run(value, fn); }
  `);
  const hostRuntimeUrl = moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE);
  const captureSource = applyModuleReplacements(
    generateWorkflowKvCaptureModule(KV_BINDINGS, {
      importableEnvDisabled: options.importableEnvDisabled === true,
      rpcMethod: KV_FACADE_RPC_METHOD,
      reportOrigin: WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN,
    }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [
        `from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`,
        `from ${JSON.stringify(hostRuntimeUrl)}`,
      ],
    ]
  );
  const captureUrl = moduleDataUrl(captureSource);
  const userUrl = moduleDataUrl(applyModuleReplacements(userSource, [
    ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
  ]));
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule("worker.js", {
      kvBindings: KV_BINDINGS,
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      workflowInfrastructureReporterProp: WORKFLOW_INFRASTRUCTURE_REPORTER_PROP,
      kvReadInfrastructureErrorCode: KV_READ_INFRASTRUCTURE_ERROR_CODE,
      importableEnvDisabled: options.importableEnvDisabled === true,
    }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(hostRuntimeUrl)}`],
      [`from "./${WORKFLOW_KV_CAPTURE_MODULE_NAME}"`, `from ${JSON.stringify(captureUrl)}`],
      [/from "\.\/worker\.js"/g, `from ${JSON.stringify(userUrl)}`],
    ]
  );
  try {
    return {
      wrapped: await import(moduleDataUrl(source)),
      user: await import(userUrl),
      cloudflare: await import(cloudflareUrl),
    };
  } finally {
    delete globals[moduleEnvKey];
  }
}

function workflowReporter() {
  /** @type {unknown[]} */
  const codes = [];
  const capability = Object.create({
    report() { codes.push("prototype-report-intercepted"); },
  });
  capability.fetch = mockFetcherFetch;
  /** @param {RequestInfo | URL} input */
  capability.__fetch = (input) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    assert.equal(url.origin, WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN);
    codes.push(url.pathname.slice(1));
    return new Response(null, { status: 204 });
  };
  return {
    codes,
    capability,
  };
}

function crossedKvInfrastructureError() {
  const hostError = runtimeInfrastructureError(
    "KV read failed"
  );
  const crossed = new Error(hostError.message);
  Object.defineProperty(crossed, "code", {
    value: /** @type {any} */ (hostError).code,
    enumerable: true,
  });
  return crossed;
}

/** @this {{ __fetch(input: RequestInfo | URL): unknown }} @param {RequestInfo | URL} input */
function mockFetcherFetch(input) {
  return this.__fetch(input);
}

/** @param {Record<string, (...args: any[]) => unknown>} [overrides] */
function completeKvBinding(overrides = {}) {
  const binding = {
    fetch: mockFetcherFetch,
    __fetch() { return new Response(null, { status: 204 }); },
    get() { return null; },
    getWithMetadata() { return { value: null, metadata: null }; },
    put() {},
    delete() {},
    list() { return { keys: [], list_complete: true }; },
    ...overrides,
  };
  const callables = {
    get: binding.get,
    getWithMetadata: binding.getWithMetadata,
    put: binding.put,
    delete: binding.delete,
    list: binding.list,
  };
  Object.defineProperty(binding, KV_FACADE_RPC_METHOD, {
    /** @param {keyof typeof callables} operation @param {...unknown} args */
    async value(operation, ...args) {
      return Reflect.apply(callables[operation], binding, args);
    },
  });
  return binding;
}

test("workflow wrappers brand cached KV failures only when the same Error escapes run", async () => {
  let failure = false;
  /** @type {unknown[][]} */
  const getCalls = [];
  /** @type {unknown[][]} */
  const listCalls = [];
  const rawKv = completeKvBinding({
    get(...args) {
      getCalls.push(args);
      if (failure) throw crossedKvInfrastructureError();
      return "value";
    },
    list(...args) {
      listCalls.push(args);
      if (failure) throw crossedKvInfrastructureError();
      return { keys: [], list_complete: true };
    },
  });
  const userSource = `
    import { env as importedEnv } from "cloudflare:workers";
    let cachedKv;
    let savedError;
    export const observedProps = [];
    const originalRawGet = importedEnv.CACHE.get;
    Object.defineProperty(importedEnv.CACHE, "get", {
      configurable: true,
      value(key, ...args) {
        if (key === "shadow-forged") {
          const error = new Error("tenant method shadow");
          error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
          throw error;
        }
        return Reflect.apply(originalRawGet, importedEnv.CACHE, [key, ...args]);
      },
    });
    export class Flow {
      constructor(ctx, env) {
        observedProps.push({ ...ctx.props });
        this.env = env;
      }
      async run(event) {
        if (event.mode === "relay") throw savedError;
        if (event.mode === "shadow-forged") {
          return await cachedKv.get("shadow-forged");
        }
        if (event.mode === "get-options") {
          return await cachedKv.get("options", { type: "text", cacheTtl: 60 });
        }
        if (event.mode === "projection-timing") {
          const observe = async (call) => {
            let synchronous = false;
            let operation;
            try {
              operation = call();
            } catch {
              synchronous = true;
            }
            let message = null;
            try {
              await operation;
            } catch (error) {
              message = String(error.message);
            }
            return { synchronous, message };
          };
          const sparseKey = ["a", , "b"];
          const listOptions = new Proxy({}, {
            get() { throw new Error("tenant list getter"); },
          });
          return await Promise.all([
            observe(() => cachedKv.get(sparseKey)),
            observe(() => cachedKv.getWithMetadata(sparseKey)),
            observe(() => cachedKv.list(listOptions)),
          ]);
        }
        if (event.mode === "nullable-list") {
          return await cachedKv.list({ prefix: null, cursor: null });
        }
        if (event.mode === "options-getter") {
          const error = new Error("tenant options getter");
          error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
          const options = {};
          Object.defineProperty(options, "type", {
            enumerable: true,
            get() { throw error; },
          });
          return await cachedKv.get("key", options);
        }
        if (event.mode === "key-proxy") {
          const error = new Error("tenant key proxy");
          error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
          const key = new Proxy(["key"], {
            get(target, property, receiver) {
              if (property === "0") throw error;
              return Reflect.get(target, property, receiver);
            },
          });
          return await cachedKv.get(key);
        }
        if (event.mode === "list-getter") {
          const error = new Error("tenant list getter");
          error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
          return await cachedKv.list(new Proxy({}, {
            get() { throw error; },
          }));
        }
        if (event.mode === "forged") {
          const error = new Error("forged");
          error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
          throw error;
        }
        cachedKv ??= this.env.CACHE;
        try {
          return await cachedKv.get("key");
        } catch (error) {
          if (event.mode === "caught") return "fallback";
          if (event.mode === "capture") {
            savedError = error;
            return "captured";
          }
          if (event.mode === "new-error") throw new Error("replacement", { cause: error });
          throw error;
        }
      }
    }
    export default {
      fetch(_request, env) {
        cachedKv ??= env.CACHE;
        return "cached";
      },
    };
  `;
  const { wrapped, user } = await loadWorkflowWrapper(userSource, {
    moduleEnv: { CACHE: rawKv },
  });
  assert.equal(await wrapped.default.fetch(null, { CACHE: rawKv }, {}), "cached");

  const shadowReporter = workflowReporter();
  const shadow = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: shadowReporter.capability },
  }, { CACHE: rawKv });
  assert.equal(await shadow.run({ mode: "shadow-forged" }, {}), "value");
  assert.deepEqual(shadowReporter.codes, []);

  const optionsReporter = workflowReporter();
  const options = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: optionsReporter.capability },
  }, { CACHE: rawKv });
  assert.equal(await options.run({ mode: "get-options" }, {}), "value");
  assert.deepEqual(getCalls, [
    ["shadow-forged", undefined],
    ["options", { type: "text", cacheTtl: 60 }],
  ]);
  assert.deepEqual(optionsReporter.codes, []);

  const timingReporter = workflowReporter();
  const timing = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: timingReporter.capability },
  }, { CACHE: rawKv });
  assert.deepEqual(await timing.run({ mode: "projection-timing" }, {}), [
    {
      synchronous: false,
      message: "KV batch read keys must not contain empty slots",
    },
    {
      synchronous: false,
      message: "KV batch read keys must not contain empty slots",
    },
    { synchronous: false, message: "tenant list getter" },
  ]);
  assert.deepEqual(timingReporter.codes, []);

  const nullableReporter = workflowReporter();
  const nullable = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: nullableReporter.capability },
  }, { CACHE: rawKv });
  assert.deepEqual(await nullable.run({ mode: "nullable-list" }, {}), {
    keys: [],
    list_complete: true,
  });
  assert.deepEqual(listCalls, [[{
    prefix: null,
    cursor: null,
    limit: undefined,
    metadata: undefined,
  }]]);
  assert.deepEqual(nullableReporter.codes, []);

  failure = true;

  const caughtReporter = workflowReporter();
  const caughtProps = {
    [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: caughtReporter.capability,
  };
  const caught = new wrapped.Flow({ props: caughtProps }, { CACHE: rawKv });
  assert.equal(await caught.run({ mode: "caught" }, {}), "fallback");
  assert.deepEqual(caughtReporter.codes, []);
  assert.deepEqual(user.observedProps[0], {});
  assert.equal(Object.hasOwn(caughtProps, WORKFLOW_INFRASTRUCTURE_REPORTER_PROP), false);

  const uncaughtReporter = workflowReporter();
  const uncaught = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: uncaughtReporter.capability },
  }, { CACHE: rawKv });
  await assert.rejects(() => uncaught.run({ mode: "uncaught" }, {}), /KV read failed/);
  assert.deepEqual(uncaughtReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);

  for (const mode of [
    "forged",
    "new-error",
    "options-getter",
    "key-proxy",
    "list-getter",
  ]) {
    const reporter = workflowReporter();
    const flow = new wrapped.Flow({
      props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: reporter.capability },
    }, { CACHE: rawKv });
    await assert.rejects(() => flow.run({ mode }, {}));
    assert.deepEqual(reporter.codes, []);
  }

  const captureReporter = workflowReporter();
  const capture = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: captureReporter.capability },
  }, { CACHE: rawKv });
  assert.equal(await capture.run({ mode: "capture" }, {}), "captured");
  assert.deepEqual(captureReporter.codes, []);

  const relayReporter = workflowReporter();
  const relay = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: relayReporter.capability },
  }, { CACHE: rawKv });
  await assert.rejects(() => relay.run({ mode: "relay" }, {}), /KV read failed/);
  assert.deepEqual(relayReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);
});

test("workflow wrappers report only KV Errors escaping the durable callback boundary", async () => {
  const rawKv = completeKvBinding({
    get() { throw crossedKvInfrastructureError(); },
  });
  const { wrapped, cloudflare } = await loadWorkflowWrapper(`
    export let detached = null;
    export class Flow {
      constructor(_ctx, env) { this.env = env; }
      async run(event, step) {
        if (event.mode === "detached") {
          detached = this.env.CACHE.get("key").catch(() => undefined);
          return "detached";
        }
        try {
          let stepDo = step.do;
          if (event.mode === "descriptor") {
            if (Object.getPrototypeOf(step) !== null || "dup" in step) {
              throw new Error("Workflow step facade exposed its raw target");
            }
            stepDo = Object.getOwnPropertyDescriptor(step, "do")?.value;
            if (typeof stepDo !== "function") {
              throw new Error("Workflow step facade omitted wrapped do");
            }
          }
          return await stepDo("read", async () => {
            if (event.mode === "forged") {
              const error = new Error("forged");
              error.code = ${JSON.stringify(KV_READ_INFRASTRUCTURE_ERROR_CODE)};
              throw error;
            }
            try {
              return await this.env.CACHE.get("key");
            } catch (error) {
              if (event.mode === "caught") return "fallback";
              if (event.mode === "new-error") {
                throw new Error("replacement", { cause: error });
              }
              throw error;
            }
          });
        } catch (error) {
          if (event.outerCatch) return "outer fallback";
          throw error;
        }
      }
    }
    export default {};
  `, { moduleEnv: { CACHE: rawKv } });
  const step = {
    /** @param {string} _name @param {(...args: unknown[]) => unknown} callback */
    do(_name, callback) {
      return cloudflare.withEnv(
        new Proxy({}, { get: () => ({ forged: true }) }),
        () => callback({ attempt: 1 })
      );
    },
  };

  for (const mode of ["caught", "new-error", "forged", "detached"]) {
    const reporter = workflowReporter();
    const flow = new wrapped.Flow({
      props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: reporter.capability },
    }, { CACHE: rawKv });
    if (mode === "caught") {
      assert.equal(await flow.run({ mode }, step), "fallback");
    } else if (mode === "detached") {
      assert.equal(await flow.run({ mode }, step), "detached");
    } else {
      await assert.rejects(() => flow.run({ mode }, step));
    }
    assert.deepEqual(reporter.codes, []);
  }

  const escapedReporter = workflowReporter();
  const escaped = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: escapedReporter.capability },
  }, { CACHE: rawKv });
  assert.equal(
    await escaped.run({ mode: "same-error", outerCatch: true }, step),
    "outer fallback"
  );
  assert.deepEqual(escapedReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);

  const descriptorReporter = workflowReporter();
  const descriptor = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: descriptorReporter.capability },
  }, { CACHE: rawKv });
  assert.equal(
    await descriptor.run({ mode: "descriptor", outerCatch: true }, step),
    "outer fallback"
  );
  assert.deepEqual(descriptorReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);
});

test("workflow classes preserve ordinary named-entrypoint run calls without a private prop", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const rawKv = completeKvBinding({
    /** @param {unknown[]} args */
    get(...args) {
      calls.push(args);
      return "value";
    },
  });
  const { wrapped } = await loadWorkflowWrapper(`
    export class Flow {
      constructor(ctx, env) {
        this.ctx = ctx;
        this.env = env;
      }
      run(...args) {
        return {
          args,
          value: this.env.CACHE.get("ordinary"),
          props: { ...this.ctx.props },
        };
      }
    }
    export default {};
  `, { moduleEnv: { CACHE: rawKv } });
  const flow = new wrapped.Flow({ props: { ordinary: true } }, { CACHE: rawKv });

  const result = flow.run("ordinary", "second", "third");
  assert.deepEqual(result.args, ["ordinary", "second", "third"]);
  assert.equal(await result.value, "value");
  assert.deepEqual(result.props, { ordinary: true });
  assert.deepEqual(calls, [["ordinary", undefined]]);
});

test("workflow KV facade preserves disallow_importable_env capability identity", async () => {
  /** @type {unknown[][]} */
  const calls = [];
  const sharedHostError = crossedKvInfrastructureError();
  let failWithSharedHostError = false;
  const rawKv = completeKvBinding({
    /** @param {unknown[]} args */
    get(...args) {
      calls.push(args);
      if (failWithSharedHostError) throw sharedHostError;
      return "value";
    },
  });
  const { wrapped, cloudflare } = await loadWorkflowWrapper(`
    import { env as importedEnv } from "cloudflare:workers";
    let cachedFakeKv;
    let savedHostError;
    export class Flow {
      constructor(_ctx, env) {
        if (env.CAPTURE_FAKE) cachedFakeKv = env.CACHE;
        if (env.FAIL_CONSTRUCTION) throw new Error("tenant constructor failed");
        this.env = env;
      }
      async run(event) {
        if (event.relayHostError) throw savedHostError;
        if (event.useFakeFacade && event.relayHostErrorThroughFake) {
          return await cachedFakeKv.get("saved");
        }
        return arguments[1].do("read", async () => {
          if (event.captureHostError) {
            try {
              await this.env.CACHE.get("capture");
            } catch (error) {
              savedHostError = error;
              return "captured";
            }
          }
          if (event.useFakeFacade) return await cachedFakeKv.get("forged");
          importedEnv.x = 1;
          const objectDefineResult = Object.defineProperty(
            importedEnv,
            "y",
            { value: 2, enumerable: true }
          );
          const reflectDefineResult = Reflect.defineProperty(
            importedEnv,
            "z",
            { value: 3, enumerable: true }
          );
          const listed = await this.env.CACHE.list();
          return {
            value: await this.env.CACHE.get("key"),
            listComplete: listed.list_complete,
            objectDefineReturnedEnv: objectDefineResult === importedEnv,
            reflectDefineResult,
            importedKeys: Object.keys(importedEnv),
            importedKv: importedEnv.CACHE,
            hasX: "x" in importedEnv,
            hasY: "y" in importedEnv,
            hasZ: "z" in importedEnv,
            xDescriptor: Object.getOwnPropertyDescriptor(importedEnv, "x"),
            yDescriptor: Object.getOwnPropertyDescriptor(importedEnv, "y"),
            zDescriptor: Object.getOwnPropertyDescriptor(importedEnv, "z"),
          };
        });
      }
    }
    export default {};
  `, { importableEnvDisabled: true });
  const step = {
    /** @param {string} _name @param {(...args: unknown[]) => unknown} callback */
    do(_name, callback) {
      return cloudflare.withEnv(
        new Proxy({}, { get: () => ({ forged: true }) }),
        () => callback({ attempt: 1 })
      );
    },
  };
  const expected = {
    value: "value",
    listComplete: true,
    objectDefineReturnedEnv: true,
    reflectDefineResult: true,
    importedKeys: [],
    importedKv: undefined,
    hasX: false,
    hasY: false,
    hasZ: false,
    xDescriptor: undefined,
    yDescriptor: undefined,
    zDescriptor: undefined,
  };
  let fakeCalls = 0;
  const forgedError = crossedKvInfrastructureError();
  const fakeKv = completeKvBinding({
    get(key) {
      fakeCalls += 1;
      throw key === "saved" ? sharedHostError : forgedError;
    },
    list() {
      fakeCalls += 1;
      return { keys: [], list_complete: false };
    },
  });
  assert.throws(
    () => new wrapped.Flow(
      { props: {} },
      { CACHE: fakeKv, CAPTURE_FAKE: true, FAIL_CONSTRUCTION: true }
    ),
    /tenant constructor failed/
  );
  const first = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: workflowReporter().capability,
    },
  }, { CACHE: rawKv });
  assert.deepEqual(await first.run({}, step), expected);

  const fakeRelayReporter = workflowReporter();
  const fakeRelay = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: fakeRelayReporter.capability,
    },
  }, { CACHE: rawKv });
  await assert.rejects(
    () => fakeRelay.run({ useFakeFacade: true }, step),
    /KV read failed/
  );
  assert.deepEqual(fakeRelayReporter.codes, []);

  failWithSharedHostError = true;
  const captureReporter = workflowReporter();
  const capture = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: captureReporter.capability,
    },
  }, { CACHE: rawKv });
  assert.equal(await capture.run({ captureHostError: true }, step), "captured");
  assert.deepEqual(captureReporter.codes, []);
  failWithSharedHostError = false;

  const relayReporter = workflowReporter();
  const relay = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: relayReporter.capability,
    },
  }, { CACHE: rawKv });
  await assert.rejects(() => relay.run({ relayHostError: true }, step), /KV read failed/);
  assert.deepEqual(relayReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);

  const fakeOverwriteReporter = workflowReporter();
  const fakeOverwrite = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: fakeOverwriteReporter.capability,
    },
  }, { CACHE: rawKv });
  await assert.rejects(
    () => fakeOverwrite.run({ useFakeFacade: true, relayHostErrorThroughFake: true }, step),
    /KV read failed/
  );
  assert.deepEqual(fakeOverwriteReporter.codes, [KV_READ_INFRASTRUCTURE_ERROR_CODE]);

  rawKv.list = () => {
    throw new Error("later KV method shadow must not replace the bound callable");
  };
  const second = new wrapped.Flow({
    props: {
      [WORKFLOW_INFRASTRUCTURE_REPORTER_PROP]: workflowReporter().capability,
    },
  }, { CACHE: rawKv });
  assert.deepEqual(await second.run({}, step), expected);
  assert.deepEqual(calls, [
    ["key", undefined],
    ["capture", undefined],
    ["key", undefined],
  ]);
  assert.equal(fakeCalls, 2);
});

/**
 * @param {string} source
 * @param {string} startMarker
 * @param {string} endMarker
 */
function sourceFragment(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing generated source marker: ${startMarker}`);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(end, -1, `missing generated source marker: ${endMarker}`);
  return source.slice(start, end);
}

test("generated wrapper flavors share the exact abort shim", () => {
  const { abortOnly, hostBindings } = generatedWrappers();
  const start = "// Reserved name (WDL_RESERVED_ENTRYPOINT_RE)";
  const end = "\n\nexport class __WdlWorkflowNotify__";

  assert.equal(
    sourceFragment(abortOnly, start, end),
    sourceFragment(hostBindings, start, end)
  );
});

test("generated wrapper flavors preserve default-export class detection", () => {
  const { abortOnly, hostBindings } = generatedWrappers();
  const sourceLine = "const source = Function.prototype.toString.call(raw);";
  const classTest = "/^\\s*class\\b/.test(source)";

  assert.equal(abortOnly.split(sourceLine).length - 1, 1);
  assert.match(abortOnly, new RegExp(`if \\(!${RegExp.escape(classTest)}\\)`));
  assert.match(hostBindings, /const source = __WdlHostRuntime__\.functionSource\(raw\);/);
  assert.match(hostBindings, /if \(__WdlHostRuntime__\.regexpTest\(\/\^\\s\*class\\b\/, source\)\)/);
});

test("host wrapper support modules evaluate before the tenant module", () => {
  const source = generateHostBindingWrapperModule("worker.js", {
    doBindings: ["ROOM"],
    kvBindings: KV_BINDINGS,
    workflowClassNames: ["Flow"],
    workflowInfrastructureReporterProp: WORKFLOW_INFRASTRUCTURE_REPORTER_PROP,
    kvReadInfrastructureErrorCode: KV_READ_INFRASTRUCTURE_ERROR_CODE,
  });
  assert.ok(
    source.indexOf(`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";`) <
      source.indexOf('import * as __WdlUserModule__ from "./worker.js";')
  );
  assert.ok(
    source.indexOf(`from "./${WORKFLOW_KV_CAPTURE_MODULE_NAME}";`) <
      source.indexOf('import * as __WdlUserModule__ from "./worker.js";')
  );
  assert.match(source, /import \* as __WdlHostRuntime__/);
  assert.match(HOST_BINDING_RUNTIME_SOURCE, /import \{ sanitizeRequestId \} from "\.\/_wdl-request-id\.js"/);
  assert.doesNotMatch(HOST_BINDING_RUNTIME_SOURCE, /AsyncLocalStorage|node:async_hooks/);
});

test("generated host wrappers alias legal entrypoint names without declaration collisions", async () => {
  const entrypointNames = [
    "user",
    "WorkerEntrypoint",
    "abortIsolate",
    "withEnv",
    "withRequestContext",
    "wrapEnv",
    "wrapClassInstance",
    "D1Database",
    "R2Bucket",
    "DurableObjectNamespace",
    "Workflow",
  ];
  const userUrl = moduleDataUrl(`
    ${entrypointNames.map((name) => `export class ${name} {}`).join("\n")}
    export default {};
  `);
  const cloudflareUrl = moduleDataUrl(`
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(_env, fn) { return fn(); }
  `);
  const d1Url = moduleDataUrl("export class D1Database {}");
  const r2Url = moduleDataUrl("export class R2Bucket {}");
  const doUrl = moduleDataUrl("export class DurableObjectNamespace {}");
  const workflowUrl = moduleDataUrl("export class Workflow {}");
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule(
      "worker.js",
      {
        d1Bindings: ["DB"],
        r2Bindings: ["BUCKET"],
        doBindings: ["ROOM"],
        workflowBindings: { FLOW: { className: "Workflow" } },
        entrypointNames,
      }
    ),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
      ['from "./_wdl-d1-client.js"', `from ${JSON.stringify(d1Url)}`],
      ['from "./_wdl-r2-client.js"', `from ${JSON.stringify(r2Url)}`],
      ['from "./_wdl-do-client.js"', `from ${JSON.stringify(doUrl)}`],
      ['from "./_wdl-workflows-client.js"', `from ${JSON.stringify(workflowUrl)}`],
      ['from "./worker.js"', `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  const userModule = await import(userUrl);

  for (const name of entrypointNames) {
    assert.equal(wrapped[name].name, name);
    assert.ok(new wrapped[name]({}, {}) instanceof userModule[name]);
  }
});

test("default host wrappers reuse stripping work without sharing request-scoped facades", async () => {
  const userUrl = moduleDataUrl(`
    export default {
      fetch(request, env) {
        return { env, requestId: env.DB.requestId() };
      },
    };
  `);
  const cloudflareUrl = moduleDataUrl(`
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(_env, fn) { return fn(); }
  `);
  const d1Url = moduleDataUrl(`
    export class D1Database {
      constructor(binding, options) {
        this.binding = binding;
        this.options = options;
      }
      requestId() {
        return typeof this.options.requestIdProvider === "function"
          ? this.options.requestIdProvider()
          : this.options.requestId;
      }
    }
  `);
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule("worker.js", { d1Bindings: ["DB"] }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
      ['from "./_wdl-d1-client.js"', `from ${JSON.stringify(d1Url)}`],
      [/from "\.\/worker\.js"/g, `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  let internalReads = 0;
  const rawEnv = { DB: { name: "db" }, PLAIN: "original" };
  Object.defineProperty(rawEnv, "__WDL_UNUSED__", {
    enumerable: true,
    get() {
      internalReads += 1;
      return { hidden: true };
    },
  });

  const first = wrapped.default.fetch(new Request("https://example.test/", {
    headers: { "x-request-id": "request-a" },
  }), rawEnv, {});
  first.env.PLAIN = "mutated";
  const second = wrapped.default.fetch(new Request("https://example.test/", {
    headers: { "x-request-id": "request-b" },
  }), rawEnv, {});

  assert.equal(internalReads, 1);
  assert.notEqual(first.env, second.env);
  assert.notEqual(first.env.DB, second.env.DB);
  assert.equal(first.requestId, "request-a");
  assert.equal(second.requestId, "request-b");
  assert.equal(first.env.__WDL_UNUSED__, undefined);
  assert.equal(second.env.__WDL_UNUSED__, undefined);
  assert.equal(second.env.PLAIN, "original");
});
