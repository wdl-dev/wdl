// Binding shape validation + cross-ns ACL + service/platform linkers.
// Deploy-time authority; runtime re-checks grammar on load.

import {
  NS_PATTERN,
  WORKER_NAME_RE,
  QUEUE_NAME_RE,
  BINDING_NAME_RE,
  KV_ID_RE,
  D1_DATABASE_ID_RE,
  R2_BUCKET_NAME_RE,
  WDL_RESERVED_BINDING_RE,
  RESERVED_OBJECT_KEYS,
  RESERVED_TENANT_NS,
  WDL_RESERVED_ENTRYPOINT_RE,
  MAX_DO_CLASS_NAME_BYTES,
  isValidKvId,
  isValidQueueName,
  isValidWorkerName,
  isValidJsIdentifier,
  isValidJsClassDeclarationName,
} from "shared-ns-pattern";
import { PLATFORM_TIER_RESERVED_NS } from "shared-auth-roles";
import { errorMessage } from "shared-errors";
import {
  NS_RE,
  isAdminAcceptableNs,
  MAX_QUEUE_DELAY_SECONDS,
} from "control-lib";

// Narrower than SECRET_KEY_RE (no lowercase) so platform slot names read
// as registered identifiers rather than local vars.
export const PLATFORM_KEY_RE = /^[A-Z_][A-Z0-9_]*$/;

/**
 * @typedef {Record<string, unknown> & { type?: string, id?: unknown, service?: unknown, ns?: unknown, entrypoint?: unknown, deliveryDelaySeconds?: unknown, databaseId?: unknown, databaseName?: unknown, bucketName?: unknown, className?: unknown, version?: string, requiredCallerSecrets?: unknown, doStorageId?: string }} BindingSpec
 * @typedef {{ entrypoint: string, allowedCallers: string[], as?: string, requiredCallerSecrets?: string[] }} ExportEntry
 * @typedef {{ binding: string, platform: string }} PlatformBindingRequest
 * @typedef {{ ns: string, worker: string, version: string, entrypoint?: string, as: string, allowedCallers: string[], requiredCallerSecrets: string[] }} PlatformExport
 * @typedef {{ type: string, ns: string, service: string, version: string, entrypoint?: string, requiredCallerSecrets?: string[] }} ExpandedPlatformBinding
 * @typedef {Record<string, unknown> & { exports?: unknown }} TargetMeta
 * @typedef {{ callerNs: string, callerName: string, bindingName: string, spec: BindingSpec, lookupTargetVersion: (ns: string, worker: string) => Promise<string | null>, lookupTargetMeta: (ns: string, worker: string, version: string) => Promise<TargetMeta | null> }} LinkServiceArgs
 */

/** @param {string} scope @param {string} entrypoint */
function reservedEntrypointMessage(scope, entrypoint) {
  return `${scope}: entrypoint ${JSON.stringify(entrypoint)} is reserved for runtime-injected entrypoints`;
}

/** @param {string} scope @param {string} entrypoint */
function assertNotRuntimeReservedEntrypoint(scope, entrypoint) {
  if (WDL_RESERVED_ENTRYPOINT_RE.test(entrypoint)) {
    throw new Error(reservedEntrypointMessage(scope, entrypoint));
  }
}

/** @param {unknown} value @returns {value is { entrypoint: string, allowedCallers: unknown, requiredCallerSecrets?: unknown }} */
function isExportEntry(value) {
  return Boolean(
    value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    typeof /** @type {Record<string, unknown>} */ (value).entrypoint === "string" &&
    Array.isArray(/** @type {Record<string, unknown>} */ (value).allowedCallers)
  );
}

// Deploy-time shape check so invalid bindings 400 instead of 502'ing
// on the first post-promote request. Runtime re-validates on load.
/** @type {Record<string, (b: BindingSpec, name: string) => void>} */
const BINDING_VALIDATORS = Object.assign(Object.create(null), /** @type {Record<string, (b: BindingSpec, name: string) => void>} */ ({
  kv(b, name) {
    if (!isValidKvId(b.id)) {
      throw new Error(
        `bindings.${name}: kv id must match ${KV_ID_RE}, got ${JSON.stringify(b.id)}`
      );
    }
  },
  assets() {},
  service(b, name) {
    if (!isValidWorkerName(b.service)) {
      throw new Error(
        `bindings.${name}: service must match ${WORKER_NAME_RE}, got ${JSON.stringify(b.service)}`
      );
    }
    // Accept reserved ns: Path A writes platform-tier ns on expansion;
    // raw [[services]] into those namespaces is rejected by the linker.
    if (b.ns != null && (typeof b.ns !== "string" || !isAdminAcceptableNs(b.ns))) {
      throw new Error(`bindings.${name}: ns must match ${NS_PATTERN}, got ${JSON.stringify(b.ns)}`);
    }
    const entrypoint = b.entrypoint;
    if (entrypoint != null) {
      if (typeof entrypoint !== "string" || !isValidJsIdentifier(entrypoint)) {
        throw new Error(
          `bindings.${name}: entrypoint must be a JS identifier, got ${JSON.stringify(entrypoint)}`
        );
      }
      // Letting a caller bind to a runtime-injected entrypoint would
      // give them out-of-band tear-down of the target isolate.
      assertNotRuntimeReservedEntrypoint(`bindings.${name}`, entrypoint);
    }
  },
  queue(b, name) {
    if (!isValidQueueName(b.id)) {
      throw new Error(
        `bindings.${name}: queue id must match ${QUEUE_NAME_RE}, got ${JSON.stringify(b.id)}`
      );
    }
    if (b.deliveryDelaySeconds != null) {
      const delay = b.deliveryDelaySeconds;
      if (
        typeof delay !== "number" ||
        !Number.isInteger(delay) ||
        delay < 0 ||
        delay > MAX_QUEUE_DELAY_SECONDS
      ) {
        throw new Error(
          `bindings.${name}: deliveryDelaySeconds must be integer in [0, ${MAX_QUEUE_DELAY_SECONDS}]`
        );
      }
    }
  },
  d1(b, name) {
    const id = b.databaseId;
    if (typeof id !== "string" || !D1_DATABASE_ID_RE.test(id)) {
      throw new Error(
        `bindings.${name}: d1 databaseId must match ${D1_DATABASE_ID_RE}, got ${JSON.stringify(id)}`
      );
    }
    if (b.databaseName != null) {
      throw new Error(
        `bindings.${name}: d1 databaseName is a system-managed metadata field; ` +
          "wrangler config may use database_name, but deploy API bindings must set databaseId to the database ref"
      );
    }
  },
  r2(b, name) {
    if (typeof b.bucketName !== "string" || !R2_BUCKET_NAME_RE.test(b.bucketName)) {
      throw new Error(
        `bindings.${name}: r2 bucketName must match ${R2_BUCKET_NAME_RE}, got ${JSON.stringify(b.bucketName)}`
      );
    }
  },
  do(b, name) {
    const className = b.className;
    if (
      typeof className !== "string" ||
      !isValidJsClassDeclarationName(className) ||
      className.length > MAX_DO_CLASS_NAME_BYTES
    ) {
      throw new Error(
        `bindings.${name}: do className must be a valid JS class declaration name of at most ` +
          `${MAX_DO_CLASS_NAME_BYTES} bytes, got ${JSON.stringify(className)}`
      );
    }
    assertNotRuntimeReservedEntrypoint(`bindings.${name}`, className);
  },
}));

/** @param {unknown} bindings */
export function validateBindings(bindings) {
  if (bindings == null) return;
  if (typeof bindings !== "object" || Array.isArray(bindings)) {
    throw new Error("bindings must be an object");
  }
  for (const [name, b] of Object.entries(bindings)) {
    // Reject before shape: `__proto__` / `toString` etc. would poison
    // env via the prototype path.
    if (typeof name !== "string" || !BINDING_NAME_RE.test(name)) {
      throw new Error(
        `bindings: name ${JSON.stringify(name)} must match ${BINDING_NAME_RE}`
      );
    }
    if (WDL_RESERVED_BINDING_RE.test(name)) {
      throw new Error(
        `bindings: name ${JSON.stringify(name)} is reserved for runtime-internal bindings`
      );
    }
    if (RESERVED_OBJECT_KEYS.has(name)) {
      throw new Error(
        `bindings: name ${JSON.stringify(name)} is a reserved Object.prototype key`
      );
    }
    if (!b || typeof b !== "object" || Array.isArray(b)) {
      throw new Error(`bindings.${name} must be an object`);
    }
    const spec = /** @type {BindingSpec} */ (b);
    const validate = typeof spec.type === "string" && Object.hasOwn(BINDING_VALIDATORS, spec.type)
      ? BINDING_VALIDATORS[spec.type]
      : undefined;
    if (!validate) {
      throw new Error(
        `bindings.${name}: unsupported type ${JSON.stringify(b.type)} ` +
          `(supported: ${Object.keys(BINDING_VALIDATORS).join(", ")})`
      );
    }
    validate(spec, name);
  }
}

/** @param {unknown} bindings */
export function normalizeBindings(bindings) {
  if (bindings == null) return null;
  const out = Object.create(null);
  for (const [name, binding] of Object.entries(bindings)) {
    const normalized = { ...binding };
    // Runtime's getEntrypoint() without args hits the default export;
    // "default" as a name would miss it, so deploy-wire metadata omits it
    // after validation instead of mutating the caller's binding object.
    if (normalized.type === "service" && normalized.entrypoint === "default") {
      delete normalized.entrypoint;
    }
    out[name] = normalized;
  }
  return out;
}

// `as` / `requiredCallerSecrets` are PLATFORM_TIER_RESERVED_NS-only —
// rejecting them on tenant ns prevents silently claiming a platform slot
// name via wrangler config. Set check so future platform-tier ns
// (`__community__`, …) inherit the rule automatically.
/** @param {unknown} raw @param {{ ns?: string }} [opts] */
export function parseExports(raw, opts = {}) {
  const ns = opts.ns;
  if (raw == null) return [];
  if (!Array.isArray(raw)) {
    throw new Error("exports must be an array of entries");
  }
  const isPlatformTier = typeof ns === "string" && PLATFORM_TIER_RESERVED_NS.has(ns);
  const out = [];
  const seenEntry = new Set();
  const seenAs = new Set();
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`exports entry must be an object, got ${JSON.stringify(entry)}`);
    }
    if (entry.entrypoint !== "default" && !isValidJsClassDeclarationName(entry.entrypoint)) {
      throw new Error(
        `exports entry.entrypoint must be a valid JS class declaration name or "default", got ${JSON.stringify(entry.entrypoint)}`
      );
    }
    // Reserved by the runtime shim wrapper. Without this gate, a
    // declared entrypoint with this name silently shadows the runtime's
    // — or, paired with any host binding, emits a duplicate
    // `export class __WdlAbort__` and fails cold-load.
    assertNotRuntimeReservedEntrypoint(`exports[${entry.entrypoint}]`, entry.entrypoint);
    if (seenEntry.has(entry.entrypoint)) {
      throw new Error(`exports: duplicate entrypoint ${JSON.stringify(entry.entrypoint)}`);
    }
    seenEntry.add(entry.entrypoint);

    // [[exports]] = opting into per-entrypoint ACL; empty list means
    // "no cross-ns caller" (same-ns still bypasses).
    let allowedCallers;
    try {
      allowedCallers = parseAllowedCallers(entry.allowedCallers);
    } catch (err) {
      const message = errorMessage(err);
      throw new Error(`exports[${entry.entrypoint}]: ${message}`, { cause: err });
    }
    if (allowedCallers == null) {
      throw new Error(`exports[${entry.entrypoint}].allowedCallers is required`);
    }

    /** @type {ExportEntry} */
    const normalized = { entrypoint: entry.entrypoint, allowedCallers };

    if (entry.as !== undefined) {
      if (!isPlatformTier) {
        throw new Error(
          `exports[${entry.entrypoint}].as is only allowed on platform-tier reserved namespaces`
        );
      }
      if (typeof entry.as !== "string" || !PLATFORM_KEY_RE.test(entry.as)) {
        throw new Error(
          `exports[${entry.entrypoint}].as must match ${PLATFORM_KEY_RE}, got ${JSON.stringify(entry.as)}`
        );
      }
      if (seenAs.has(entry.as)) {
        throw new Error(`exports: duplicate as ${JSON.stringify(entry.as)}`);
      }
      seenAs.add(entry.as);
      normalized.as = entry.as;
    }

    if (entry.requiredCallerSecrets !== undefined) {
      if (!isPlatformTier) {
        throw new Error(
          `exports[${entry.entrypoint}].requiredCallerSecrets is only allowed on platform-tier reserved namespaces`
        );
      }
      if (!Array.isArray(entry.requiredCallerSecrets)) {
        throw new Error(
          `exports[${entry.entrypoint}].requiredCallerSecrets must be an array of strings`
        );
      }
      const keys = [];
      const seenKey = new Set();
      for (const k of entry.requiredCallerSecrets) {
        if (typeof k !== "string" || !PLATFORM_KEY_RE.test(k)) {
          throw new Error(
            `exports[${entry.entrypoint}].requiredCallerSecrets entries must match ${PLATFORM_KEY_RE}, got ${JSON.stringify(k)}`
          );
        }
        if (seenKey.has(k)) continue;
        seenKey.add(k);
        keys.push(k);
      }
      normalized.requiredCallerSecrets = keys;
    }

    if (isPlatformTier && normalized.as === undefined) {
      // Without `as` the entry is unreachable: linker keys on `as`,
      // raw [[services]] into PLATFORM_TIER_RESERVED_NS is rejected elsewhere.
      throw new Error(
        `exports[${entry.entrypoint}]: platform-tier reserved namespaces require "as" on every entry`
      );
    }

    out.push(normalized);
  }
  return out;
}

// `platform` defaults to `binding` — common case is caller uses the
// canonical name directly. Explicit `platform` is for aliasing.
/** @param {unknown} raw */
export function parsePlatformBindings(raw) {
  if (raw == null) return [];
  if (!Array.isArray(raw)) {
    throw new Error("platformBindings must be an array of entries");
  }
  const out = [];
  const seen = new Set();
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`platformBindings entry must be an object, got ${JSON.stringify(entry)}`);
    }
    if (typeof entry.binding !== "string" || !PLATFORM_KEY_RE.test(entry.binding)) {
      throw new Error(
        `platformBindings entry.binding must match ${PLATFORM_KEY_RE}, got ${JSON.stringify(entry.binding)}`
      );
    }
    const platform = entry.platform == null ? entry.binding : entry.platform;
    if (typeof platform !== "string" || !PLATFORM_KEY_RE.test(platform)) {
      throw new Error(
        `platformBindings[${entry.binding}].platform must match ${PLATFORM_KEY_RE}, got ${JSON.stringify(entry.platform)}`
      );
    }
    if (seen.has(entry.binding)) {
      throw new Error(`platformBindings: duplicate binding ${JSON.stringify(entry.binding)}`);
    }
    seen.add(entry.binding);
    out.push({ binding: entry.binding, platform });
  }
  return out;
}

// Returns null for undeclared, an array otherwise (empty list is valid and
// preserved — the meta shape distinguishes absent from explicit `[]`).
/** @param {unknown} raw */
export function parseAllowedCallers(raw) {
  if (raw == null) return null;
  if (!Array.isArray(raw)) {
    throw new Error("allowedCallers must be an array of strings");
  }
  const out = [];
  const seen = new Set();
  for (const entry of raw) {
    if (typeof entry !== "string" || (entry !== "*" && !NS_RE.test(entry))) {
      throw new Error(
        `allowedCallers entries must be "*" or match ${NS_PATTERN}, got ${JSON.stringify(entry)}`
      );
    }
    if (RESERVED_TENANT_NS.has(entry)) {
      throw new Error(
        `allowedCallers entry ${JSON.stringify(entry)} is a reserved tenant name and cannot appear in an ACL`
      );
    }
    if (!seen.has(entry)) {
      seen.add(entry);
      out.push(entry);
    }
  }
  return out;
}

// Throws on deny; control wraps the message in a 403. Same-ns is a no-op
// because a caller already owns its own ns.
/** @param {string} callerNs @param {string} targetNs @param {string} targetService @param {unknown} allowedCallers */
export function evaluateServiceBindingAcl(callerNs, targetNs, targetService, allowedCallers) {
  if (callerNs === targetNs) return;
  const list = Array.isArray(allowedCallers) ? allowedCallers : [];
  if (list.includes("*") || list.includes(callerNs)) return;
  throw new Error(
    `Namespace "${targetNs}" does not allow ns "${callerNs}" to bind "${targetService}". ` +
    `Target export must list the caller namespace in allowed_callers.`
  );
}

export class LinkError extends Error {
  /** @param {number} status @param {string} code @param {string} message */
  constructor(status, code, message) {
    super(message);
    this.name = "LinkError";
    this.status = status;
    this.code = code;
  }
}

// Pins a service-binding spec to the target's currently-active version
// so __meta__ freezes the dep graph (atomic rollback, no "target deleted
// between deploy and call" 502s). Pure apart from two injected Redis
// lookups; mutates `spec` in place, throws LinkError on any rejection.
/** @param {LinkServiceArgs} args */
export async function linkServiceBinding({
  callerNs,
  callerName,
  bindingName,
  spec,
  lookupTargetVersion,
  lookupTargetMeta,
}) {
  // requiredCallerSecrets is target-declared via [[exports]]; caller-set
  // would let them force-forward their own secrets into any target's
  // ctx.props without consent.
  if (spec.requiredCallerSecrets !== undefined) {
    throw new LinkError(400, "service_binding_reserved_field",
      `bindings.${bindingName}: requiredCallerSecrets is set by the ` +
      `platform linker, not by the caller. Use [[platform_bindings]] ` +
      `to bind a platform-tier reserved namespace worker.`);
  }
  if (typeof spec.service !== "string" || !spec.service) {
    throw new LinkError(400, "service_binding_invalid_service",
      `bindings.${bindingName}: service requires non-empty "service"`);
  }
  if (spec.ns != null && (typeof spec.ns !== "string" || !isAdminAcceptableNs(spec.ns))) {
    throw new LinkError(400, "service_binding_invalid_namespace",
      `bindings.${bindingName}: ns must match ${NS_RE}, got ${JSON.stringify(spec.ns)}`);
  }
  const service = spec.service;
  const targetNs = typeof spec.ns === "string" ? spec.ns : callerNs;
  // Platform-tier ns are resource-shaped — reachable only via
  // [[platform_bindings]]. Set check inherits future members automatically.
  if (PLATFORM_TIER_RESERVED_NS.has(targetNs)) {
    throw new LinkError(400, "service_binding_platform_namespace",
      `bindings.${bindingName}: ns "${targetNs}" is a platform-tier reserved namespace; must be addressed via [[platform_bindings]], not [[services]]`);
  }
  if (service === callerName && targetNs === callerNs) {
    throw new LinkError(400, "service_binding_self_target",
      `bindings.${bindingName}: service cannot target self ("${callerNs}/${callerName}")`);
  }
  const targetVersion = await lookupTargetVersion(targetNs, service);
  if (!targetVersion) {
    throw new LinkError(409, "service_binding_target_inactive",
      `bindings.${bindingName}: service "${targetNs}/${service}" has no active version. Deploy + promote it first.`);
  }
  // Read meta even for same-ns: exports strict-mode visibility applies to
  // all callers. ACL stays cross-ns-only via evaluateServiceBindingAcl.
  let targetMeta;
  try {
    targetMeta = await lookupTargetMeta(targetNs, service, targetVersion);
    if (!targetMeta) targetMeta = {};
  } catch (err) {
    // The injected reader may classify persisted target metadata separately
    // from transport failures; preserve that domain error at the link boundary.
    if (err instanceof LinkError) throw err;
    const message = errorMessage(err);
    throw new LinkError(502, "service_binding_target_meta_unavailable",
      `bindings.${bindingName}: failed to read target meta: ${message}`);
  }
  const normalizedEntrypoint = typeof spec.entrypoint === "string" && spec.entrypoint
    ? spec.entrypoint
    : "default";
  if (normalizedEntrypoint !== "default" && !isValidJsIdentifier(normalizedEntrypoint)) {
    throw new LinkError(400, "service_binding_invalid_entrypoint",
      `bindings.${bindingName}: entrypoint must be a JS identifier, got ${JSON.stringify(normalizedEntrypoint)}`);
  }
  // Defense in depth: validateBindings already rejects this at deploy
  // ingress, but the linker is the last gate before commit and would
  // otherwise let direct Redis writes (ops recovery) bypass the check.
  if (WDL_RESERVED_ENTRYPOINT_RE.test(normalizedEntrypoint)) {
    throw new LinkError(400, "service_binding_entrypoint_reserved",
      reservedEntrypointMessage(`bindings.${bindingName}`, normalizedEntrypoint));
  }
  if (Array.isArray(targetMeta.exports) && targetMeta.exports.length) {
    const entry = targetMeta.exports.find(
      (e) => isExportEntry(e) && e.entrypoint === normalizedEntrypoint
    );
    if (!entry) {
      throw new LinkError(400, "service_binding_entrypoint_not_exported",
        `bindings.${bindingName}: entrypoint "${normalizedEntrypoint}" not exported by "${targetNs}/${service}"; declare it in [[exports]] on the target worker`);
    }
    try {
      evaluateServiceBindingAcl(callerNs, targetNs, service, entry.allowedCallers);
    } catch (err) {
      const message = errorMessage(err);
      throw new LinkError(403, "service_binding_acl_denied", `bindings.${bindingName}: ${message}`);
    }
  } else {
    if (normalizedEntrypoint !== "default") {
      throw new LinkError(400, "service_binding_entrypoint_not_exported",
        `bindings.${bindingName}: entrypoint "${normalizedEntrypoint}" not exported by "${targetNs}/${service}"; declare it in [[exports]] on the target worker`);
    }
    if (callerNs !== targetNs) {
      throw new LinkError(403, "service_binding_acl_denied",
        `bindings.${bindingName}: cross-namespace service binding requires "${targetNs}/${service}" to declare [[exports]] entrypoint "default" with allowed_callers`);
    }
  }
  spec.version = targetVersion;
  return spec;
}

// Caller pre-fetches `platformExports` (the flattened export list across
// every active platform-tier worker) and `availableCallerSecrets` (ns +
// worker secret key Set). Returns { expanded, warning? }.
/**
 * @param {{ callerNs: string, bindingReq: PlatformBindingRequest, existingBindings: Record<string, unknown>, platformExports: PlatformExport[], availableCallerSecrets: Set<string> }} args
 */
export function linkPlatformBinding({
  callerNs,
  bindingReq,
  existingBindings,
  platformExports,
  availableCallerSecrets,
}) {
  if (existingBindings[bindingReq.binding]) {
    throw new LinkError(400, "platform_binding_name_collision",
      `platformBindings.${bindingReq.binding}: binding name collides with another binding`);
  }
  const match = platformExports.find((e) => e.as === bindingReq.platform);
  if (!match) {
    throw new LinkError(400, "platform_binding_not_registered",
      `platformBindings.${bindingReq.binding}: platform "${bindingReq.platform}" not registered ` +
      `(no active worker in PLATFORM_TIER_RESERVED_NS exports this \`as\`)`);
  }
  // ACL against the export's actual ns (not a literal) so a `__community__`
  // export's allowedCallers reads from `__community__`'s perspective.
  try {
    evaluateServiceBindingAcl(callerNs, match.ns, match.worker, match.allowedCallers);
  } catch (err) {
    const message = errorMessage(err);
    throw new LinkError(403, "platform_binding_acl_denied",
      `platformBindings.${bindingReq.binding}: ${message}`);
  }
  let warning;
  if (match.requiredCallerSecrets.length) {
    const missing = match.requiredCallerSecrets.filter(
      (k) => !availableCallerSecrets.has(k)
    );
    if (missing.length) {
      warning = {
        binding: bindingReq.binding,
        platform: bindingReq.platform,
        missingCallerSecrets: missing,
      };
    }
  }
  /** @type {ExpandedPlatformBinding} */
  const expanded = {
    type: "service",
    // Export's actual ns — runtime's getEntrypoint addresses workerLoader
    // by this; a hardcoded literal would route to the wrong ns at runtime.
    ns: match.ns,
    service: match.worker,
    version: match.version,
  };
  if (match.entrypoint && match.entrypoint !== "default") {
    expanded.entrypoint = match.entrypoint;
  }
  if (match.requiredCallerSecrets.length) {
    expanded.requiredCallerSecrets = match.requiredCallerSecrets;
  }
  return { expanded, warning };
}
