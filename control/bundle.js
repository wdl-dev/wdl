// Normalize deploy-wire module payloads into the immutable per-version
// { meta, modules } pair that control commits to Redis. deepFreeze is
// the commit-boundary guarantee that promote / linker / extractOutgoingRefs
// observe a truly immutable bundle.

import {
  BINDING_NAME_RE,
  RESERVED_OBJECT_KEYS,
  WDL_RESERVED_BINDING_RE,
  WDL_RESERVED_ENTRYPOINT_RE,
  WORKFLOW_NAME_RE,
  isValidJsClassDeclarationName,
  validateModulePath,
} from "shared-ns-pattern";
import {
  LEGACY_ERROR_SERIALIZATION_FLAG,
  MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE,
  firstWorkerdExperimentalCompatFlag,
} from "shared-workerd-compat-flags";
import { normalizeBindings, validateBindings } from "control-bindings";
import { parseWorkerdDependencyVersion } from "control-lib";
import PACKAGE_JSON_SOURCE from "wdl-package-json-source";

export class BundleConfigError extends Error {
  /** @param {number} status @param {string} code @param {string} message */
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

const COMPATIBILITY_DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** @param {string} source */
export function maxWorkerCompatibilityDateFromPackageJson(source) {
  const version = parseWorkerdDependencyVersion(source);
  if (!version) return null;
  const max = new Date(Date.UTC(version.year, version.month - 1, version.day + 7));
  return utcDateString(max);
}

const maxWorkerCompatibilityDate = maxWorkerCompatibilityDateFromPackageJson(PACKAGE_JSON_SOURCE);
if (!maxWorkerCompatibilityDate) {
  throw new Error("Unable to derive maximum compatibilityDate from bundled workerd dependency");
}
export const MAX_WORKER_COMPATIBILITY_DATE = maxWorkerCompatibilityDate;

/**
 * @param {Date} date
 * @returns {string}
 */
function utcDateString(date) {
  return [
    String(date.getUTCFullYear()).padStart(4, "0"),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("-");
}

/**
 * @param {unknown} value
 * @param {Date} [today]
 * @returns {string | undefined}
 */
export function validateCompatibilityDate(value, today = new Date()) {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string" || value === "") {
    throw new Error(`compatibilityDate must be a YYYY-MM-DD string, got ${JSON.stringify(value)}`);
  }
  const match = COMPATIBILITY_DATE_RE.exec(value);
  if (!match) {
    throw new Error(`compatibilityDate must use YYYY-MM-DD format, got ${JSON.stringify(value)}`);
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    throw new Error(`compatibilityDate must be a real calendar date, got ${JSON.stringify(value)}`);
  }
  if (value < MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE) {
    throw new Error(
      `compatibilityDate ${value} is older than WDL supports (${MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE})`
    );
  }
  const todayUtc = utcDateString(today);
  if (value > todayUtc) {
    throw new Error(`compatibilityDate ${value} must not be later than today UTC (${todayUtc})`);
  }
  if (value > MAX_WORKER_COMPATIBILITY_DATE) {
    throw new Error(
      `compatibilityDate ${value} is newer than bundled workerd supports (${MAX_WORKER_COMPATIBILITY_DATE})`
    );
  }
  return value;
}

// Canonical base64 only — re-encoding must match the input, which
// rejects the non-canonical variants both atob and Buffer silently accept.
/** @param {string} value @param {string} fieldName */
function decodeBase64Strict(value, fieldName) {
  if (value === "") return Buffer.alloc(0);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(value) || value.length % 4 !== 0) {
    throw new Error(`Invalid base64 in ${fieldName}`);
  }
  const bytes = Buffer.from(value, "base64");
  if (bytes.toString("base64") !== value) {
    throw new Error(`Invalid base64 in ${fieldName}`);
  }
  return bytes;
}

/** @param {unknown} value */
export function normalizeModule(value) {
  if (typeof value === "string") {
    return { type: "module", bytes: Buffer.from(value, "utf8") };
  }
  if (value && typeof value === "object") {
    const record = /** @type {Record<string, unknown>} */ (value);
    if (typeof record.data_b64 === "string")
      return { type: "data", bytes: decodeBase64Strict(record.data_b64, "data_b64") };
    if (typeof record.wasm_b64 === "string")
      return { type: "wasm", bytes: decodeBase64Strict(record.wasm_b64, "wasm_b64") };
    if (typeof record.text === "string")
      return { type: "text", bytes: Buffer.from(record.text, "utf8") };
    if (record.json !== undefined)
      return { type: "json", bytes: Buffer.from(JSON.stringify(record.json), "utf8") };
    if (typeof record.cjs === "string")
      return { type: "cjs", bytes: Buffer.from(record.cjs, "utf8") };
    if (typeof record.py === "string") {
      throw new BundleConfigError(
        400,
        "python_workers_unsupported",
        "Python Workers modules are not supported by WDL"
      );
    }
  }
  throw new Error(
    "Unrecognized module value (expected string or {data_b64|wasm_b64|text|json|cjs})"
  );
}

// 400 at deploy ingress, so runtime can trust the shape post-commit.
// Silent coercion at load (array-or-empty) would mean a bad payload
// like { compatibilityFlags: "nodejs_compat" } or { flag: true } drops
// the user's declaration and leaves only the platform floor — looks
// healthy, isn't.
/** @param {unknown} flags */
function validateCompatibilityFlags(flags) {
  if (flags === undefined || flags === null) return;
  if (!Array.isArray(flags)) {
    throw new Error(
      `compatibilityFlags must be an array of strings, got ${JSON.stringify(flags)}`
    );
  }
  for (const f of flags) {
    if (typeof f !== "string" || f === "") {
      throw new Error(
        `compatibilityFlags entries must be non-empty strings, got ${JSON.stringify(f)}`
      );
    }
  }
  const experimentalFlag = firstWorkerdExperimentalCompatFlag(flags);
  if (experimentalFlag) {
    throw new BundleConfigError(
      400,
      "experimental_compat_flag_unsupported",
      `compatibilityFlags contains experimental workerd flag ${JSON.stringify(experimentalFlag)}, which WDL does not support for tenant workers`
    );
  }
  if (flags.includes(LEGACY_ERROR_SERIALIZATION_FLAG)) {
    throw new BundleConfigError(
      400,
      "compatibility_flag_unsupported",
      `${JSON.stringify(LEGACY_ERROR_SERIALIZATION_FLAG)} is not supported because WDL requires enhanced error serialization`
    );
  }
}

/** @param {unknown} workflows */
export function normalizeWorkflows(workflows) {
  if (workflows === undefined || workflows === null) return [];
  if (!Array.isArray(workflows)) {
    throw new Error("workflows must be an array of entries");
  }
  const out = [];
  const seenNames = new Set();
  const seenBindings = new Set();
  for (const entry of workflows) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`workflows entry must be an object, got ${JSON.stringify(entry)}`);
    }
    if (entry.scriptName !== undefined || entry.script_name !== undefined) {
      throw new BundleConfigError(
        400,
        "workflow_script_name_unsupported",
        "workflows script_name is not supported in WDL Workflows V2"
      );
    }
    if (typeof entry.name !== "string" || !WORKFLOW_NAME_RE.test(entry.name)) {
      throw new Error(
        `workflows entry.name must match ${WORKFLOW_NAME_RE}, got ${JSON.stringify(entry.name)}`
      );
    }
    if (RESERVED_OBJECT_KEYS.has(entry.name)) {
      throw new Error(
        `workflows[${entry.name}].name is a reserved Object.prototype key`
      );
    }
    if (seenNames.has(entry.name)) {
      throw new Error(`workflows: duplicate name ${JSON.stringify(entry.name)}`);
    }
    seenNames.add(entry.name);
    if (typeof entry.binding !== "string" || !BINDING_NAME_RE.test(entry.binding)) {
      throw new Error(
        `workflows[${entry.name}].binding must match ${BINDING_NAME_RE}, got ${JSON.stringify(entry.binding)}`
      );
    }
    if (WDL_RESERVED_BINDING_RE.test(entry.binding)) {
      throw new Error(
        `workflows[${entry.name}].binding is reserved for runtime-internal bindings`
      );
    }
    if (RESERVED_OBJECT_KEYS.has(entry.binding)) {
      throw new Error(
        `workflows[${entry.name}].binding is a reserved Object.prototype key`
      );
    }
    if (seenBindings.has(entry.binding)) {
      throw new Error(`workflows: duplicate binding ${JSON.stringify(entry.binding)}`);
    }
    seenBindings.add(entry.binding);
    const className = entry.className ?? entry.class_name;
    if (!isValidJsClassDeclarationName(className)) {
      throw new Error(
        `workflows[${entry.name}].className must be a valid JS class declaration name, got ${JSON.stringify(className)}`
      );
    }
    if (WDL_RESERVED_ENTRYPOINT_RE.test(className)) {
      throw new Error(
        `workflows[${entry.name}].className is reserved for runtime-injected entrypoints`
      );
    }
    out.push({ name: entry.name, binding: entry.binding, className });
  }
  return out;
}

/** @param {unknown} vars */
export function normalizeVars(vars) {
  if (vars === undefined || vars === null) return undefined;
  if (typeof vars !== "object" || Array.isArray(vars)) {
    throw new Error("[vars] must be an object");
  }
  const normalized = Object.create(null);
  for (const [name, value] of Object.entries(vars)) {
    if (WDL_RESERVED_BINDING_RE.test(name)) {
      throw new Error(`[vars] ${name}: name is reserved for runtime-internal bindings`);
    }
    if (RESERVED_OBJECT_KEYS.has(name)) {
      throw new Error(`[vars] ${name}: name is a reserved Object.prototype key`);
    }
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
      throw new Error(`[vars] ${name}: only string/number/boolean values are supported`);
    }
    normalized[name] = String(value);
  }
  return normalized;
}

// Recursive freeze at commit boundaries so the "bundle is immutable"
// invariant is machine-checked, not social-contract-only.
/**
 * @template T
 * @param {T} obj
 * @returns {T}
 */
export function deepFreeze(obj) {
  if (obj === null || typeof obj !== "object" || Object.isFrozen(obj)) return obj;
  const record = /** @type {Record<string, unknown>} */ (obj);
  for (const key of Object.keys(record)) deepFreeze(record[key]);
  return Object.freeze(obj);
}

// Normalize a deploy payload into { meta, normalized[[path, bytes]] }.
// Separate from commit so a validation fail never burns a version.
/**
 * @param {string} mainModule
 * @param {Record<string, unknown>} rawModules
 * @param {{ bindings?: unknown, compatibilityDate?: unknown, compatibilityFlags?: unknown, vars?: unknown, exports?: unknown[], workflows?: unknown, assets?: unknown }} [extras]
 */
export function prepareBundle(mainModule, rawModules, extras = {}) {
  validateModulePath(mainModule);
  // hasOwn: `rawModules["constructor"]` would truthy-match via the
  // prototype chain and mask a missing mainModule.
  if (!Object.hasOwn(rawModules, mainModule)) {
    throw new Error(`mainModule "${mainModule}" not present in modules`);
  }
  validateBindings(extras.bindings);
  const compatibilityDate = validateCompatibilityDate(extras.compatibilityDate);
  validateCompatibilityFlags(extras.compatibilityFlags);
  const vars = normalizeVars(extras.vars);
  // Null-proto: any path slipping past validateModulePath can't take
  // the __proto__ setter path on assignment.
  /** @type {{ mainModule: string, modules: Record<string, { type: string }>, compatibilityDate?: string, compatibilityFlags?: unknown, bindings?: unknown, vars?: Record<string, string>, exports?: unknown[], workflows?: unknown[], assets?: unknown, routes?: unknown[], crons?: unknown[], queueConsumers?: unknown[] }} */
  const meta = { mainModule, modules: Object.create(null) };
  if (compatibilityDate) meta.compatibilityDate = compatibilityDate;
  if (extras.compatibilityFlags) meta.compatibilityFlags = extras.compatibilityFlags;
  const bindings = normalizeBindings(extras.bindings);
  if (bindings) meta.bindings = bindings;
  if (vars) meta.vars = vars;
  if (extras.exports && extras.exports.length) meta.exports = extras.exports;
  const workflows = normalizeWorkflows(extras.workflows);
  if (bindings) {
    for (const workflow of workflows) {
      if (Object.hasOwn(bindings, workflow.binding)) {
        throw new Error(`workflows[${workflow.name}].binding collides with another binding`);
      }
    }
  }
  if (workflows.length) meta.workflows = workflows;
  if (extras.assets) meta.assets = extras.assets;
  const normalized = Object.entries(rawModules).map(([p, value]) => {
    validateModulePath(p);
    const { type, bytes } = normalizeModule(value);
    meta.modules[p] = { type };
    return [p, bytes];
  });
  return { meta, normalized };
}

// Path goes verbatim into the S3 key, so reject ".."/"."/absolute
// paths and empty segments here rather than at PUT time.
/** @param {unknown} rawAssets */
export function normalizeAssets(rawAssets) {
  if (rawAssets == null) return null;
  if (typeof rawAssets !== "object" || Array.isArray(rawAssets)) {
    throw new Error("assets must be an object mapping path → base64 string");
  }
  const out = [];
  for (const [p, value] of Object.entries(rawAssets)) {
    if (typeof value !== "string") {
      throw new Error(`assets[${JSON.stringify(p)}] must be a base64 string`);
    }
    if (!p || p.startsWith("/") || p.split("/").some((seg) => seg === "" || seg === "." || seg === "..")) {
      throw new Error(`assets: invalid path ${JSON.stringify(p)}`);
    }
    const bytes = decodeBase64Strict(value, `assets[${JSON.stringify(p)}]`);
    out.push([p, bytes]);
  }
  return out;
}
