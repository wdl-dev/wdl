import { test } from "node:test";
import assert from "node:assert/strict";
import {
  HOST_BINDING_RUNTIME_MODULE_NAME,
  HOST_BINDING_RUNTIME_SOURCE,
  generateAbortShimWrapperModule,
  generateHostBindingWrapperModule,
} from "../../runtime/load/wrapper-generate.js";
import { WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP } from "../../runtime/load/module-rewrite.js";
import {
  beginRuntimeInfrastructureInvocation,
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
  const consumed = [...new Set(
    [...generateHostBindingWrapperModule("worker.js", {
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      workflowInfrastructureInvocationProp: WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP,
    }).matchAll(/__WdlHostRuntime__\.(\w+)/g)]
      .map((match) => match[1])
  )].toSorted();
  assert.deepEqual(exported, consumed);
});

test("workflow wrappers keep cached KV attribution current and hide invocation props", async () => {
  const userUrl = moduleDataUrl(`
    let cachedKv;
    export const capturedBridgeIds = [];
    export const observedProps = [];
    export class Flow {
      constructor(ctx, env) {
        observedProps.push({ ...ctx.props });
        Object.getPrototypeOf(this).__WdlRunWorkflow__ = (_event, _step, id) => {
          capturedBridgeIds.push(id);
        };
        this.env = env;
      }
      async run(event, step) {
        this.__WdlRunWorkflow__?.(event, step, "forged");
        const argumentCount = arguments.length;
        return step.do("read", async () => {
          cachedKv ??= this.env.CACHE;
          try {
            const value = await cachedKv.get("key", { type: "text" }, "tenant-extra");
            return { event, value, argumentCount };
          } catch {
            return { event, value: "fallback", argumentCount };
          }
        });
      }
    }
    export default {
      fetch(_request, env) {
        cachedKv ??= env.CACHE;
        return cachedKv.get("ordinary");
      },
    };
  `);
  const cloudflareUrl = moduleDataUrl(`
    import { AsyncLocalStorage } from "node:async_hooks";
    const envStorage = new AsyncLocalStorage();
    export const env = new Proxy({}, {
      get(_target, property) { return envStorage.getStore()?.[property]; },
    });
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(value, fn) { return envStorage.run(value, fn); }
  `);
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule("worker.js", {
      kvBindings: ["CACHE"],
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      workflowInfrastructureInvocationProp: WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP,
    }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
      [/from "\.\/worker\.js"/g, `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  const user = await import(userUrl);
  const cloudflare = await import(cloudflareUrl);
  /** @type {unknown[][]} */
  const calls = [];
  let failNext = false;
  const rawKv = {
    /** @param {unknown[]} args */
    get(...args) {
      calls.push(args);
      if (failNext) {
        failNext = false;
        const error = runtimeInfrastructureError(
          "KV read failed",
          "isolated callback KV failure",
          /** @type {string} */ (args[2])
        );
        throw new Error(error.message);
      }
      return "value";
    },
  };
  const step = {
    /** @param {string} _name @param {(...args: unknown[]) => unknown} callback */
    do(_name, callback) {
      return cloudflare.withEnv({}, () => callback({ attempt: 1 }));
    },
  };
  assert.equal(
    await wrapped.default.fetch(new Request("https://worker.test"), { CACHE: rawKv }, {}),
    "value"
  );
  const firstProps = { [WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP]: "invocation-1" };
  const first = new wrapped.Flow({ props: firstProps }, { CACHE: rawKv });
  const firstResult = await first.run({ payload: 1 }, step);
  const secondProps = { [WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP]: "invocation-2" };
  const second = new wrapped.Flow({ props: secondProps }, { CACHE: rawKv });
  const secondResult = await second.run({ payload: 2 }, step);

  assert.equal(firstResult.argumentCount, 2);
  assert.equal(secondResult.argumentCount, 2);
  assert.deepEqual([firstResult.event, secondResult.event], [
    { payload: 1 },
    { payload: 2 },
  ]);
  assert.deepEqual(calls, [
    ["ordinary", undefined, null],
    ["key", { type: "text" }, "invocation-1"],
    ["key", { type: "text" }, "invocation-2"],
  ]);
  assert.deepEqual(user.observedProps, [{}, {}]);
  assert.deepEqual(user.capturedBridgeIds, ["forged", "forged"]);
  assert.equal(Object.hasOwn(firstProps, WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP), false);
  assert.equal(Object.hasOwn(secondProps, WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP), false);

  const unaffected = beginRuntimeInfrastructureInvocation();
  const failed = beginRuntimeInfrastructureInvocation();
  try {
    failNext = true;
    const failing = new wrapped.Flow({
      props: { [WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP]: failed.id },
    }, { CACHE: rawKv });
    const fallback = await failing.run({ payload: "failure" }, step);
    assert.equal(fallback.value, "fallback");
    assert.equal(failed.diagnostic(), "isolated callback KV failure");
    assert.equal(unaffected.diagnostic(), undefined);
  } finally {
    failed.close();
    unaffected.close();
  }
});

test("workflow classes preserve ordinary named-entrypoint run calls without a private prop", async () => {
  const userUrl = moduleDataUrl(`
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
  `);
  const cloudflareUrl = moduleDataUrl(`
    export const env = {};
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(_value, fn) { return fn(); }
  `);
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule("worker.js", {
      kvBindings: ["CACHE"],
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      workflowInfrastructureInvocationProp: WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP,
    }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
      [/from "\.\/worker\.js"/g, `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  /** @type {unknown[][]} */
  const calls = [];
  const rawKv = {
    /** @param {unknown[]} args */
    get(...args) {
      calls.push(args);
      return "value";
    },
  };
  const flow = new wrapped.Flow({ props: { ordinary: true } }, { CACHE: rawKv });

  const result = flow.run("ordinary", "second", "third");
  assert.deepEqual(result.args, ["ordinary", "second", "third"]);
  assert.equal(result.value, "value");
  assert.deepEqual(result.props, { ordinary: true });
  assert.deepEqual(calls, [["ordinary", undefined, null]]);
});

test("workflow KV attribution survives disallow_importable_env without exposing bindings", async () => {
  const cloudflareUrl = moduleDataUrl(`
    import { AsyncLocalStorage } from "node:async_hooks";
    const envStorage = new AsyncLocalStorage();
    export const env = new Proxy({}, {
      get(_target, property) { return envStorage.getStore()?.[property]; },
      set(_target, property, value) {
        const current = envStorage.getStore();
        return current ? Reflect.set(current, property, value) : true;
      },
      defineProperty(_target, property, descriptor) {
        const current = envStorage.getStore();
        return current ? Reflect.defineProperty(current, property, descriptor) : true;
      },
      deleteProperty(_target, property) {
        const current = envStorage.getStore();
        return current ? Reflect.deleteProperty(current, property) : true;
      },
      has(_target, property) {
        const current = envStorage.getStore();
        return current ? Reflect.has(current, property) : false;
      },
      ownKeys() { return Reflect.ownKeys(envStorage.getStore() || {}); },
      getOwnPropertyDescriptor(_target, property) {
        const current = envStorage.getStore();
        return current ? Reflect.getOwnPropertyDescriptor(current, property) : undefined;
      },
    });
    export class WorkerEntrypoint {}
    export function abortIsolate() {}
    export function withEnv(value, fn) { return envStorage.run(value, fn); }
  `);
  const userUrl = moduleDataUrl(`
    import { env as importedEnv } from ${JSON.stringify(cloudflareUrl)};
    export class Flow {
      constructor(_ctx, env) { this.env = env; }
      async run() {
        return arguments[1].do("read", async () => {
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
          return {
            value: await this.env.CACHE.get("key"),
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
  `);
  const source = applyModuleReplacements(
    generateHostBindingWrapperModule("worker.js", {
      kvBindings: ["CACHE"],
      entrypointNames: ["Flow"],
      workflowClassNames: ["Flow"],
      workflowInfrastructureInvocationProp: WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP,
      importableEnvDisabled: true,
    }),
    [
      ['from "cloudflare:workers"', `from ${JSON.stringify(cloudflareUrl)}`],
      [`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}"`, `from ${JSON.stringify(moduleDataUrl(HOST_BINDING_RUNTIME_TEST_SOURCE))}`],
      [/from "\.\/worker\.js"/g, `from ${JSON.stringify(userUrl)}`],
    ]
  );
  const wrapped = await import(moduleDataUrl(source));
  const cloudflare = await import(cloudflareUrl);
  /** @type {unknown[][]} */
  const calls = [];
  const flow = new wrapped.Flow({
    props: { [WORKFLOW_INFRASTRUCTURE_INVOCATION_PROP]: "private-disabled" },
  }, {
    CACHE: {
      /** @param {unknown[]} args */
      get(...args) {
        calls.push(args);
        return "value";
      },
    },
  });

  const step = {
    /** @param {string} _name @param {(...args: unknown[]) => unknown} callback */
    do(_name, callback) {
      return cloudflare.withEnv({}, () => callback({ attempt: 1 }));
    },
  };
  assert.deepEqual(await flow.run({}, step), {
    value: "value",
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
  });
  assert.deepEqual(calls, [["key", undefined, "private-disabled"]]);
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

test("host wrapper runtime evaluates before the tenant module", () => {
  const source = generateHostBindingWrapperModule("worker.js", { doBindings: ["ROOM"] });
  assert.ok(
    source.indexOf(`from "./${HOST_BINDING_RUNTIME_MODULE_NAME}";`) <
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
