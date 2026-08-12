import {
  importRepositoryModule,
  moduleDataUrl,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "./load-shared-module.js";
import { CLOUDFLARE_WORKERS_URL } from "./mocks/cloudflare-workers.js";
import { runtimeProxyBindingStubUrl } from "./runtime-proxy-stub.js";

/**
 * @typedef {{
 *   kind: string,
 *   name: string,
 *   labels: Record<string, string>,
 *   value: number,
 * }} AiHostMetric
 */

/** @type {{ metrics: AiHostMetric[], logs: unknown[], bindingOperations: string[] }} */
export const AI_HOST_TEST_STATE = {
  metrics: [],
  logs: [],
  bindingOperations: [],
};
/** @type {typeof globalThis & { __aiHostTestState?: typeof AI_HOST_TEST_STATE }} */
const aiHostGlobal = globalThis;
aiHostGlobal.__aiHostTestState = AI_HOST_TEST_STATE;

const aiContractUrl = repositoryModuleDataUrl("shared/ai-contract.js", [
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(repositoryFileUrl("shared/ns-pattern.js"))};`],
]);
const observabilityUrl = moduleDataUrl(`
export function ensureRequestId(headers) { return headers.get("x-request-id") || "rid-ai-test"; }
export function logStructured(service, level, event, fields) {
  globalThis.__aiHostTestState.logs.push({ service, level, event, fields });
}
`);
const metricsUrl = moduleDataUrl(`
export const metrics = {
  increment(name, labels, value = 1) {
    globalThis.__aiHostTestState.metrics.push({ kind: "increment", name, labels, value });
  },
  observe(name, labels, value) {
    globalThis.__aiHostTestState.metrics.push({ kind: "observe", name, labels, value });
  },
  setGauge(name, labels, value) {
    globalThis.__aiHostTestState.metrics.push({ kind: "gauge", name, labels, value });
  },
};
export async function recordBindingOperation(_service, _binding, operation, fn) {
  globalThis.__aiHostTestState.bindingOperations.push(operation);
  return await fn();
}
`);
const aiSseUrl = repositoryModuleDataUrl("runtime/bindings/ai-sse.js", [
  [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
]);
const aiCapacityUrl = repositoryModuleDataUrl("runtime/bindings/ai-capacity.js", [
  [/from "shared-ai-runtime-config";/, `from ${JSON.stringify(repositoryFileUrl("shared/ai-runtime-config.js"))};`],
  [/from "runtime-metrics";/, `from ${JSON.stringify(metricsUrl)};`],
  [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(runtimeProxyBindingStubUrl())};`],
]);
const aiProviderUrl = repositoryModuleDataUrl("runtime/bindings/ai-provider.js");
const aiWebSocketUrl = repositoryModuleDataUrl("runtime/bindings/ai-websocket.js");
const aiProvider = await import(aiProviderUrl);

const mod = await importRepositoryModule("runtime/bindings/ai.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [/from "shared-ai-contract";/, `from ${JSON.stringify(aiContractUrl)};`],
  [/from "shared-ai-runtime-config";/, `from ${JSON.stringify(repositoryFileUrl("shared/ai-runtime-config.js"))};`],
  [/from "shared-bounded-body";/, `from ${JSON.stringify(repositoryFileUrl("shared/bounded-body.js"))};`],
  [/from "shared-errors";/, `from ${JSON.stringify(repositoryFileUrl("shared/errors.js"))};`],
  [/from "shared-internal-auth";/, `from ${JSON.stringify(repositoryFileUrl("shared/internal-auth.js"))};`],
  [/from "shared-observability";/, `from ${JSON.stringify(observabilityUrl)};`],
  [/from "shared-respond";/, `from ${JSON.stringify(repositoryFileUrl("shared/respond.js"))};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(repositoryFileUrl("shared/worker-contract.js"))};`],
  [/from "runtime-metrics";/, `from ${JSON.stringify(metricsUrl)};`],
  [/from "runtime-bindings-ai-capacity";/, `from ${JSON.stringify(aiCapacityUrl)};`],
  [/from "runtime-bindings-ai-provider";/, `from ${JSON.stringify(aiProviderUrl)};`],
  [/from "runtime-bindings-ai-sse";/, `from ${JSON.stringify(aiSseUrl)};`],
  [/from "runtime-bindings-ai-websocket";/, `from ${JSON.stringify(aiWebSocketUrl)};`],
  [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(runtimeProxyBindingStubUrl())};`],
]);

export const {
  AiBinding,
  AI_REQUEST_MAX_BYTES,
  AI_RESPONSE_MAX_BYTES,
  AI_STREAM_FRAME_MAX_BYTES,
  AI_STREAM_MAX_BYTES,
  AI_WS_FRAME_MAX_BYTES,
  AI_WS_MAX_BYTES,
  aiPoolStateForTest,
  resetAiPoolStateForTest,
} = mod;
export const { aiProviderWebSocketRequest } = aiProvider;

export function resetAiHostTestState() {
  AI_HOST_TEST_STATE.metrics.length = 0;
  AI_HOST_TEST_STATE.logs.length = 0;
  AI_HOST_TEST_STATE.bindingOperations.length = 0;
  resetAiPoolStateForTest();
}

/**
 * @param {Record<string, unknown>} [envOverrides]
 * @param {{ ns?: string, worker?: string, version?: string }} [props]
 * @param {{ waitUntil?: (promise: Promise<unknown>) => void }} [hostOptions]
 */
export function makeAiBinding(envOverrides = {}, props = {}, hostOptions = {}) {
  /** @type {Promise<unknown>[]} */
  const waitUntilTasks = [];
  const binding = new AiBinding({
    props: {
      ns: props.ns ?? "demo",
      worker: props.worker ?? "agent",
      version: props.version ?? "v1",
    },
    waitUntil(/** @type {Promise<unknown>} */ promise) {
      if (hostOptions.waitUntil) hostOptions.waitUntil(promise);
      else waitUntilTasks.push(promise);
    },
  }, {
    SERVICE_NAME: "user-runtime",
    REDIS_PROXY_URL: "http://redis-proxy:8080",
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    AI_NETWORK: { async fetch() { throw new Error("unexpected AI provider request"); } },
    ...envOverrides,
  });
  return { binding, waitUntilTasks };
}

/**
 * @typedef {{
 *   provider: string,
 *   alias: string,
 *   kind: string,
 *   upstreamModel: string,
 *   protocol: string,
 *   transport: string,
 *   destination: string,
 *   credential: string,
 *   inputModalities: string[],
 *   capabilities: Record<string, boolean>,
 * }} AiTestResolution
 */

/** @param {Partial<AiTestResolution>} [overrides] @returns {AiTestResolution} */
export function openAiResolution(overrides = {}) {
  return {
    provider: "openai",
    alias: "primary",
    kind: "openai",
    upstreamModel: "gpt-test",
    protocol: "responses",
    transport: "http",
    destination: "https://api.openai.com/v1/responses",
    credential: "fake-openai-key",
    inputModalities: ["image", "text"],
    capabilities: {
      functionTools: true,
      structuredOutput: true,
      reasoning: true,
      previousResponseId: true,
      providerTools: false,
      binaryFrames: false,
    },
    ...overrides,
  };
}

/** @param {AiTestResolution} [resolution] */
export function modelList(resolution = openAiResolution()) {
  return {
    models: [{
      id: `${resolution.provider}/${resolution.alias}`,
      protocol: resolution.protocol,
      transports: [resolution.transport],
      inputModalities: resolution.inputModalities,
      outputModalities: ["text"],
      capabilities: resolution.capabilities,
    }],
  };
}
