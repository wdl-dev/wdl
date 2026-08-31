import {
  env as importedEnv,
  RpcPromise,
  ServiceStub,
  WorkflowEntrypoint,
} from "cloudflare:workers";

const KV_FACADE_RPC_METHOD = "__WDL_KV_FACADE_RPC_METHOD__";
const KV_READ_INFRASTRUCTURE_ERROR_CODE =
  "__WDL_KV_READ_INFRASTRUCTURE_ERROR_CODE__";
const WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN =
  "__WDL_WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN__";

let poisonRpcPromiseThen = false;
let poisonServiceStubFetch = false;
let cachedFakeKv = null;
let savedInfrastructureError = null;
const observation = {
  captured: null,
  defineSucceeded: false,
  error: null,
  prototypeListCalls: 0,
  prototypeReportCalls: 0,
  promisePrototypeThenCalls: 0,
  reporterPromiseSpeciesCalls: 0,
  reporterPromiseThenCalls: 0,
  rpcPromiseThenCalls: 0,
  serviceStubReportIntercepts: 0,
  serviceStubListCalls: 0,
  serviceStubInvokeCalls: 0,
};
Reflect.defineProperty(Object.prototype, "list", {
  configurable: true,
  value() {
    observation.prototypeListCalls += 1;
    const error = new Error("tenant Object.prototype.list");
    error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
    throw error;
  },
});
Reflect.defineProperty(Object.prototype, "report", {
  configurable: true,
  value() {
    observation.prototypeReportCalls += 1;
  },
});
Reflect.defineProperty(ServiceStub.prototype, "list", {
  configurable: true,
  value() {
    observation.serviceStubListCalls += 1;
    const error = new Error("tenant ServiceStub.prototype.list");
    error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
    throw error;
  },
});
Reflect.defineProperty(ServiceStub.prototype, KV_FACADE_RPC_METHOD, {
  configurable: true,
  value() {
    observation.serviceStubInvokeCalls += 1;
    const error = new Error("tenant ServiceStub KV trampoline");
    error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
    throw error;
  },
});
const nativeServiceStubFetch = ServiceStub.prototype.fetch;
Reflect.defineProperty(ServiceStub.prototype, "fetch", {
  configurable: true,
  value(...args) {
    const target = typeof args[0] === "string" ? args[0] : args[0]?.url;
    if (
      poisonServiceStubFetch &&
      typeof target === "string" &&
      target.startsWith(`${WORKFLOW_INFRASTRUCTURE_REPORT_ORIGIN}/`)
    ) {
      observation.serviceStubReportIntercepts += 1;
      return Promise.resolve(new Response(null, { status: 204 }));
    }
    return Reflect.apply(nativeServiceStubFetch, this, args);
  },
});
const nativeRpcPromiseThen = RpcPromise.prototype.then;
Reflect.defineProperty(RpcPromise.prototype, "then", {
  configurable: true,
  value(...args) {
    if (poisonRpcPromiseThen) {
      observation.rpcPromiseThenCalls += 1;
      const error = new Error("tenant RpcPromise.prototype.then");
      error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
      throw error;
    }
    return Reflect.apply(nativeRpcPromiseThen, this, args);
  },
});

function poisonNextNativePromiseSettlement() {
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "constructor"
  );
  const thenDescriptor = Object.getOwnPropertyDescriptor(Promise.prototype, "then");
  const restore = () => {
    Reflect.defineProperty(Promise.prototype, "constructor", constructorDescriptor);
    Reflect.defineProperty(Promise.prototype, "then", thenDescriptor);
  };
  class TenantPromise extends Promise {}
  Reflect.defineProperty(Promise.prototype, "constructor", {
    configurable: true,
    value: TenantPromise,
  });
  Reflect.defineProperty(Promise.prototype, "then", {
    configurable: true,
    value(resolve, reject) {
      observation.promisePrototypeThenCalls += 1;
      restore();
      const error = new Error("tenant Promise.prototype.then");
      error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
      return Reflect.apply(thenDescriptor.value, this, [
        () => reject(error),
        () => reject(error),
      ]);
    },
  });
}

let restoreReporterPromisePrototype = null;
function observeReporterPromiseSettlement() {
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "constructor"
  );
  const thenDescriptor = Object.getOwnPropertyDescriptor(Promise.prototype, "then");
  const speciesDescriptor = Object.getOwnPropertyDescriptor(Promise, Symbol.species);
  restoreReporterPromisePrototype = () => {
    Reflect.defineProperty(Promise.prototype, "constructor", constructorDescriptor);
    Reflect.defineProperty(Promise.prototype, "then", thenDescriptor);
    Reflect.defineProperty(Promise, Symbol.species, speciesDescriptor);
    restoreReporterPromisePrototype = null;
  };
  class TenantPromise extends Promise {}
  Reflect.defineProperty(Promise.prototype, "constructor", {
    configurable: true,
    value: TenantPromise,
  });
  Reflect.defineProperty(Promise.prototype, "then", {
    configurable: true,
    value(resolve, reject) {
      return Reflect.apply(thenDescriptor.value, this, [
        (value) => {
          if (value instanceof Response) {
            observation.reporterPromiseThenCalls += 1;
            restoreReporterPromisePrototype?.();
          }
          return resolve(value);
        },
        reject,
      ]);
    },
  });
  Reflect.defineProperty(Promise, Symbol.species, {
    configurable: true,
    get() {
      if (this !== Promise) return this;
      observation.reporterPromiseSpeciesCalls += 1;
      restoreReporterPromisePrototype?.();
      throw new Error("tenant Promise Symbol.species");
    },
  });
}

const rawKv = importedEnv.CACHE;
try {
  const originalGet = rawKv.get;
  Object.defineProperty(rawKv, "get", {
    configurable: true,
    value(...args) {
      if (args[0] === "shadow-forged") {
        const error = new Error("tenant method shadow");
        error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
        throw error;
      }
      observation.captured = args[2] ?? null;
      return Reflect.apply(originalGet, rawKv, args);
    },
  });
  observation.defineSucceeded = Object.hasOwn(rawKv, "get");
} catch (error) {
  observation.error = String(error);
}

export class Flow extends WorkflowEntrypoint {
  constructor(ctx, env) {
    if (env.CAPTURE_FAKE) cachedFakeKv = env.CACHE;
    super(ctx, env);
    this.constructorProps = Object.keys(ctx.props ?? {});
  }

  async run(event, step) {
    if (event.payload.fakeFacade) {
      return step.do("fake-facade", async () => await cachedFakeKv.get("forged"));
    }
    if (event.payload.captureInfrastructureError) {
      return step.do("capture-infrastructure-error", async () => {
        try {
          await Promise.all([
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
          ]);
        } catch (error) {
          savedInfrastructureError = error;
          return {
            captured: true,
            code: error.code ?? null,
          };
        }
        throw new Error("expected Workflow KV infrastructure failure");
      });
    }
    if (
      event.payload.relayInfrastructureError ||
      event.payload.fakeRelayInfrastructureError
    ) {
      const fakeRelay = event.payload.fakeRelayInfrastructureError === true;
      const marker = fakeRelay ? "fake-relay-ran" : "relay-ran";
      if (await this.env.CACHE.get(marker) === "1") {
        return { retried: true, runs: 2 };
      }
      await this.env.CACHE.put(marker, "1");
      if (savedInfrastructureError === null) {
        throw new Error("Workflow KV infrastructure Error was not captured");
      }
      if (fakeRelay) return await cachedFakeKv.get("saved");
      throw savedInfrastructureError;
    }
    if (event.payload.directRunInfrastructure) {
      if (await this.env.CACHE.get("direct-run-ran") === "1") {
        return { retried: true, runs: 2 };
      }
      await this.env.CACHE.put("direct-run-ran", "1");
      await Promise.all([
        this.env.CACHE.get(["large"]),
        this.env.CACHE.get(["large"]),
        this.env.CACHE.get(["large"]),
        this.env.CACHE.get(["large"]),
      ]);
      return { retried: false, runs: 1 };
    }
    if (event.payload.outerCatch) {
      try {
        return await step.do("outer-catch", async () => {
          if (await this.env.CACHE.get("outer-catch-ran") === "1") {
            return { retried: true, runs: 2 };
          }
          await this.env.CACHE.put("outer-catch-ran", "1");
          await Promise.all([
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
            this.env.CACHE.get(["large"]),
          ]);
          return { retried: false, runs: 1 };
        });
      } catch {
        return { outerFallback: true };
      }
    }
    if (event.payload.prototypeList) {
      return step.do("prototype-list", async () => {
        const result = await this.env.CACHE.list();
        return {
          listComplete: result.list_complete,
          prototypeListCalls: observation.prototypeListCalls,
          serviceStubListCalls: observation.serviceStubListCalls,
          serviceStubInvokeCalls: observation.serviceStubInvokeCalls,
        };
      });
    }
    if (event.payload.rpcPromiseThen) {
      return step.do("rpc-promise-then", async () => {
        poisonRpcPromiseThen = true;
        try {
          return {
            value: await this.env.CACHE.get("rpc-promise-missing"),
            rpcPromiseThenCalls: observation.rpcPromiseThenCalls,
          };
        } catch (error) {
          return {
            error: String(error.message),
            rpcPromiseThenCalls: observation.rpcPromiseThenCalls,
          };
        } finally {
          poisonRpcPromiseThen = false;
        }
      });
    }
    if (event.payload.promisePrototypeThen) {
      return step.do("promise-prototype-then", async () => {
        if (await this.env.CACHE.get("promise-prototype-ran") === "1") {
          return { retried: true };
        }
        await this.env.CACHE.put("promise-prototype-ran", "1");
        poisonNextNativePromiseSettlement();
        return await this.env.CACHE.get("promise-prototype-missing");
      });
    }
    if (event.payload.stepBypass) {
      if (Object.getPrototypeOf(step) !== null || typeof step.dup === "function") {
        throw new Error("Workflow step facade exposed its raw target");
      }
      const stepDo = Object.getOwnPropertyDescriptor(step, "do")?.value;
      if (typeof stepDo !== "function") {
        throw new Error("Workflow step facade omitted do descriptor");
      }
      return stepDo("descriptor-capacity", async () => {
        if (await this.env.CACHE.get("descriptor-capacity-ran") === "1") {
          return { retried: true, runs: 2 };
        }
        await this.env.CACHE.put("descriptor-capacity-ran", "1");
        await Promise.all([
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
        ]);
        return { retried: false, runs: 1 };
      });
    }
    if (event.payload.methodShadow) {
      return step.do("method-shadow", async () => {
        await this.env.CACHE.put("shadow-forged", "trusted");
        return await this.env.CACHE.get("shadow-forged");
      });
    }
    if (event.payload.forgedReadArgs) {
      return step.do("forged-read-args", async () => {
        const error = new Error(
          `tenant ${event.payload.forgedReadArgs} serialization`
        );
        error.code = KV_READ_INFRASTRUCTURE_ERROR_CODE;
        if (event.payload.forgedReadArgs === "options") {
          const options = {};
          Object.defineProperty(options, "type", {
            enumerable: true,
            get() { throw error; },
          });
          return await this.env.CACHE.get("probe", options);
        }
        const key = new Proxy(["probe"], {
          get(target, property, receiver) {
            if (property === "0") throw error;
            return Reflect.get(target, property, receiver);
          },
        });
        return await this.env.CACHE.get(key);
      });
    }
    if (
      event.payload.capacityRetry ||
      event.payload.capacityUncaught ||
      event.payload.promiseReporterThen
    ) {
      const uncaught = event.payload.capacityUncaught === true ||
        event.payload.promiseReporterThen === true;
      return step.do("capacity", async () => {
        const marker = event.payload.promiseReporterThen
          ? "capacity-reporter-promise-ran"
          : uncaught ? "capacity-uncaught-ran" : "capacity-caught-ran";
        if (uncaught && await this.env.CACHE.get(marker) === "1") {
          poisonServiceStubFetch = false;
          const reporterPromiseSpeciesCalls = observation.reporterPromiseSpeciesCalls;
          const reporterPromiseThenCalls = observation.reporterPromiseThenCalls;
          restoreReporterPromisePrototype?.();
          return {
            retried: true,
            runs: 2,
            serviceStubReportIntercepts: observation.serviceStubReportIntercepts,
            ...(event.payload.promiseReporterThen
              ? { reporterPromiseSpeciesCalls, reporterPromiseThenCalls }
              : {}),
          };
        }
        await this.env.CACHE.put(marker, "1");
        const operations = [
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
          this.env.CACHE.get(["large"]),
        ];
        if (uncaught) {
          poisonServiceStubFetch = event.payload.promiseReporterThen !== true;
          try {
            await Promise.all(operations);
          } catch (error) {
            if (event.payload.promiseReporterThen) observeReporterPromiseSettlement();
            throw error;
          }
        }
        const reads = await Promise.allSettled(operations);
        const rejected = reads.filter((result) => result.status === "rejected");
        return {
          fallbackCommitted: rejected.length > 0,
          rejectedReads: rejected.length,
          rejectionCodes: rejected.map((result) => result.reason?.code ?? null),
          runs: 1,
        };
      });
    }
    return step.do("probe", async () => {
      await this.env.CACHE.put("probe", "value");
      return {
        ...observation,
        constructorProps: this.constructorProps,
        value: await this.env.CACHE.get("probe"),
      };
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.endsWith("/seed")) {
      await env.CACHE.put("large", new Uint8Array(20 * 1024 * 1024).fill(97));
      return Response.json({ ok: true });
    }
    if (url.pathname.endsWith("/prime-fake")) {
      const fakeBinding = {
        async [KV_FACADE_RPC_METHOD](_operation, key) {
          if (key === "saved" && savedInfrastructureError !== null) {
            throw savedInfrastructureError;
          }
          const error = new Error("tenant forged infrastructure error");
          Object.defineProperty(error, "code", {
            value: KV_READ_INFRASTRUCTURE_ERROR_CODE,
            enumerable: true,
          });
          throw error;
        },
      };
      try {
        const { Flow: WrappedFlow } = await import("./_wdl-wrapper.js");
        new WrappedFlow({ props: {} }, { CACHE: fakeBinding, CAPTURE_FAKE: true });
      } catch {}
      return Response.json({ seeded: cachedFakeKv !== null });
    }
    if (url.pathname.endsWith("/create")) {
      const capacityRetry = url.searchParams.get("capacityRetry") === "1";
      const capacityUncaught = url.searchParams.get("capacityUncaught") === "1";
      const directRunInfrastructure =
        url.searchParams.get("directRunInfrastructure") === "1";
      const captureInfrastructureError =
        url.searchParams.get("captureInfrastructureError") === "1";
      const fakeFacade = url.searchParams.get("fakeFacade") === "1";
      const fakeRelayInfrastructureError =
        url.searchParams.get("fakeRelayInfrastructureError") === "1";
      const forgedReadArgs = url.searchParams.get("forgedReadArgs") || "";
      const methodShadow = url.searchParams.get("methodShadow") === "1";
      const outerCatch = url.searchParams.get("outerCatch") === "1";
      const promiseReporterThen = url.searchParams.get("promiseReporterThen") === "1";
      const prototypeList = url.searchParams.get("prototypeList") === "1";
      const promisePrototypeThen = url.searchParams.get("promisePrototypeThen") === "1";
      const rpcPromiseThen = url.searchParams.get("rpcPromiseThen") === "1";
      const relayInfrastructureError =
        url.searchParams.get("relayInfrastructureError") === "1";
      const stepBypass = url.searchParams.get("stepBypass") === "1";
      const id = stepBypass ? "step-bypass"
        : fakeRelayInfrastructureError ? "fake-relay-infrastructure"
        : relayInfrastructureError ? "relay-infrastructure"
        : captureInfrastructureError ? "capture-infrastructure"
        : fakeFacade ? "fake-facade"
        : outerCatch ? "outer-catch"
        : directRunInfrastructure ? "direct-run-infrastructure"
        : promiseReporterThen ? "promise-reporter-then"
        : promisePrototypeThen ? "promise-prototype-then"
        : rpcPromiseThen ? "rpc-promise-then"
        : prototypeList ? "prototype-list"
        : methodShadow ? "method-shadow"
        : forgedReadArgs ? `forged-${forgedReadArgs}`
        : capacityUncaught ? "capacity-uncaught"
        : capacityRetry ? "capacity"
          : "probe";
      const instance = await env.FLOW.create({
        id,
        params: {
          capacityRetry,
          capacityUncaught,
          captureInfrastructureError,
          directRunInfrastructure,
          fakeFacade,
          fakeRelayInfrastructureError,
          forgedReadArgs,
          methodShadow,
          outerCatch,
          promiseReporterThen,
          prototypeList,
          promisePrototypeThen,
          relayInfrastructureError,
          rpcPromiseThen,
          stepBypass,
        },
      });
      return Response.json({ id: instance.id });
    }
    const id = url.searchParams.get("id") || "probe";
    const instance = await env.FLOW.get(id);
    return Response.json(await instance.status());
  },
};
