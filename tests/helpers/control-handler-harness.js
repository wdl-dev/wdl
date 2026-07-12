import { controlSharedStubUrl } from "./control-shared-stub.js";
import {
  importRepositoryModule,
  importSpecifierReplacements,
} from "./load-shared-module.js";
import { createFakeRedis } from "./mocks/fake-redis.js";

const DEFAULT_GLOBAL_NAME = "__controlHandlerHarnessState";

/**
 * @typedef {{
 *   redis?: unknown,
 *   dataRedis?: unknown,
 *   env?: Record<string, string | undefined>,
 *   logs?: Array<{ level: string, event: string, fields: Record<string, unknown> }>,
 *   metrics?: {
 *     increment?: (...args: unknown[]) => void,
 *     observe?: (...args: unknown[]) => void,
 *   },
 *   s3?: unknown,
 *   r2?: unknown,
 *   workflows?: unknown,
 *   service?: string,
 * }} ControlHandlerStateOptions
 */

/** @param {ControlHandlerStateOptions} [options] */
export function createControlHandlerState(options = {}) {
  return {
    redis: options.redis ?? createFakeRedis(),
    dataRedis: options.dataRedis ?? null,
    env: options.env ?? {},
    logs: options.logs ?? [],
    metrics: options.metrics ?? { increment() {}, observe() {} },
    s3: options.s3 ?? null,
    r2: options.r2 ?? null,
    workflows: options.workflows ?? null,
    service: options.service ?? "control",
  };
}

/**
 * @template {object} T
 * @param {string} globalName
 * @param {T} state
 * @returns {T}
 */
export function installControlHandlerState(globalName, state) {
  Object.defineProperty(globalThis, globalName, {
    value: state,
    configurable: true,
    writable: true,
  });
  return state;
}

/**
 * @param {string} [globalName]
 * @param {string} [extraSource]
 */
export function controlSharedHarnessUrl(globalName = DEFAULT_GLOBAL_NAME, extraSource = "") {
  const key = JSON.stringify(globalName);
  return controlSharedStubUrl(`
const harnessState = () => globalThis[${key}];
export const state = {
  get redis() { return harnessState().redis; },
  get dataRedis() { return harnessState().dataRedis; },
  get env() { return harnessState().env; },
  get s3() { return harnessState().s3; },
  get r2() { return harnessState().r2; },
  get workflows() { return harnessState().workflows; },
  get service() { return harnessState().service || "control"; },
  log(level, event, fields = {}) {
    harnessState().logs.push({ level, event, fields });
  },
};
export const metrics = {
  increment(...args) { harnessState().metrics.increment?.(...args); },
  observe(...args) { harnessState().metrics.observe?.(...args); },
};
${extraSource}
`);
}

/**
 * @param {string} relativePath
 * @param {{
 *   globalName?: string,
 *   extraSharedSource?: string,
 *   replacements?: Record<string, string>,
 * }} [options]
 */
export async function importControlHandler(relativePath, options = {}) {
  const globalName = options.globalName ?? DEFAULT_GLOBAL_NAME;
  const controlSharedUrl = controlSharedHarnessUrl(globalName, options.extraSharedSource ?? "");
  return await importRepositoryModule(relativePath, importSpecifierReplacements({
    "control-shared": controlSharedUrl,
    ...(options.replacements ?? {}),
  }));
}
