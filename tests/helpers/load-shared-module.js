// The style-contract tripwire exempts this file because moduleDataUrl /
// repositoryFileUrl define themselves directly here (cannot self-call).

import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseJsonText } from "./json-payload.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");
const SHARED_ENV_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/env.js")).href;
const SHARED_ERRORS_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/errors.js")).href;
const SHARED_BASE64_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/base64.js")).href;
const SHARED_NS_PATTERN_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/ns-pattern.js")).href;
const WORKER_CONTRACT_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/worker-contract.js")).href;
const SHARED_HEX_URL = pathToFileURL(path.resolve(REPO_ROOT, "shared/hex.js")).href;
const SHARED_OPTIMISTIC_RETRY_URL = pathToFileURL(
  path.resolve(REPO_ROOT, "shared/optimistic-retry.js")
).href;
const SHARED_WORKERD_COMPAT_FLAGS_URL = pathToFileURL(
  path.resolve(REPO_ROOT, "shared/workerd-compat-flags.js")
).href;

/** @typedef {Array<[RegExp | string, string]>} ModuleReplacements */

/** @param {string} src */
export function moduleDataUrl(src) {
  return `data:text/javascript,${encodeURIComponent(src)}`;
}

let freshSerial = 0;
// Data: URLs are cached by Node's ESM loader keyed on the URL string. Tag the
// source with a per-call serial so harnesses that need fresh module instances
// per load get a unique URL each call.
/** @param {string} src */
export function freshModuleDataUrl(src) {
  return moduleDataUrl(`// __loadSerial__: ${++freshSerial}\n${src}`);
}

/** @param {string} relativePath */
export function readRepositoryFile(relativePath) {
  return readFileSync(path.resolve(REPO_ROOT, relativePath), "utf8");
}

/** @param {string} relativePath */
export function readRepositoryJson(relativePath) {
  return parseJsonText(readRepositoryFile(relativePath), relativePath);
}

/** @param {string} relativePath */
export function repositoryFileUrl(relativePath) {
  return pathToFileURL(path.resolve(REPO_ROOT, relativePath)).href;
}

/**
 * @param {string} source
 * @param {ModuleReplacements} [replacements]
 */
export function applyModuleReplacements(source, replacements = []) {
  let out = source;
  for (const [pattern, replacement] of replacements) {
    out = out.replace(pattern, replacement);
  }
  return out;
}

/**
 * @param {Record<string, string>} specifierUrls
 * @returns {ModuleReplacements}
 */
export function importSpecifierReplacements(specifierUrls) {
  return Object.entries(specifierUrls).map(([specifier, url]) => [
    new RegExp(String.raw`from\s+"${RegExp.escape(specifier)}";?`, "g"),
    `from ${JSON.stringify(url)};`,
  ]);
}

/**
 * @param {string} relativePath
 * @param {ModuleReplacements} [replacements]
 */
export function readRepositoryModuleSource(relativePath, replacements = []) {
  return applyModuleReplacements(readRepositoryFile(relativePath), replacements);
}

/**
 * @param {string} relativePath
 * @param {ModuleReplacements} [replacements]
 */
export function repositoryModuleDataUrl(relativePath, replacements = []) {
  return moduleDataUrl(readRepositoryModuleSource(relativePath, replacements));
}

export function runtimeLibModuleDataUrl() {
  return repositoryModuleDataUrl("runtime/lib.js", importSpecifierReplacements({
    "shared-base64": SHARED_BASE64_URL,
    "shared-ns-pattern": SHARED_NS_PATTERN_URL,
    "shared-worker-contract": WORKER_CONTRACT_URL,
    "shared-workerd-compat-flags": SHARED_WORKERD_COMPAT_FLAGS_URL,
  }));
}

/**
 * Like repositoryModuleDataUrl but cache-busted per call. Returns the URL (not
 * the imported module) so loaders can chain it into a sibling's replacements.
 * @param {string} relativePath
 * @param {ModuleReplacements} [replacements]
 */
export function freshRepositoryModuleDataUrl(relativePath, replacements = []) {
  return freshModuleDataUrl(readRepositoryModuleSource(relativePath, replacements));
}

/**
 * @param {string} relativePath
 * @param {ModuleReplacements} [replacements]
 */
export async function importRepositoryModule(relativePath, replacements = []) {
  return await import(repositoryModuleDataUrl(relativePath, replacements));
}

/**
 * Like importRepositoryModule but returns a fresh module each call. Use when
 * the harness reloads between tests to clear module-level state.
 * @param {string} relativePath
 * @param {ModuleReplacements} [replacements]
 */
export async function importRepositoryModuleFresh(relativePath, replacements = []) {
  return await import(freshRepositoryModuleDataUrl(relativePath, replacements));
}

/** @param {string} relativePath */
export function sharedModuleDataUrl(relativePath) {
  const src = applyModuleReplacements(readRepositoryFile(relativePath), [
    [/from "shared-env";?/g, `from ${JSON.stringify(SHARED_ENV_URL)};`],
    [/from "shared-optimistic-retry";?/g, `from ${JSON.stringify(SHARED_OPTIMISTIC_RETRY_URL)};`],
    [/from "\.\/errors\.js";?/g, `from ${JSON.stringify(SHARED_ERRORS_URL)};`],
    [/from "\.\/hex\.js";?/g, `from ${JSON.stringify(SHARED_HEX_URL)};`],
  ]);
  return moduleDataUrl(src);
}
