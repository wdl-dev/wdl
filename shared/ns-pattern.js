// Single source of truth for the tenant namespace grammar.
//
// Both admin (validates deploy requests) and gateway (matches the subdomain)
// import this. Keeping it in one place means a change to the allowed
// character set can never drift between the tier that accepts deploys and
// the tier that routes traffic.
export const NS_PATTERN = "[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?";
export const DEFAULT_PLATFORM_DOMAIN = "workers.local";
export const MAX_PLATFORM_DOMAIN_BYTES = 126;
const DNS_LABEL_RE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/;
const DNS_FINAL_LABEL_RE = /^[A-Za-z]+$/;

/** @param {string} value */
function isAscii(value) {
  for (let index = 0; index < value.length; index += 1) {
    if (value.charCodeAt(index) > 0x7f) return false;
  }
  return true;
}

/** @param {unknown} value */
export function configuredHostname(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const trimmed = value.trim();
  const rawHost = trimmed.endsWith(".") ? trimmed.slice(0, -1) : trimmed;
  if (!isAscii(rawHost)) return null;
  if (rawHost.length === 0 || rawHost.length > MAX_PLATFORM_DOMAIN_BYTES) return null;
  const labels = rawHost.split(".");
  if (
    labels.some((label) => !DNS_LABEL_RE.test(label)) ||
    !DNS_FINAL_LABEL_RE.test(labels.at(-1) || "")
  ) return null;
  return rawHost.toLowerCase();
}

/** @param {Record<string, unknown>} env */
export function platformDomainFromEnv(env) {
  const raw = env.PLATFORM_DOMAIN;
  if (raw == null || raw === "" || typeof raw !== "string") return DEFAULT_PLATFORM_DOMAIN;
  const domain = configuredHostname(raw);
  if (!domain) throw new TypeError("PLATFORM_DOMAIN must be an ALB-compatible ASCII DNS hostname");
  return domain;
}

export const RESERVED_NS = new Set(["__system__", "__platform__", "__community__"]);

const RESERVED_NS_PATTERN = Array.from(RESERVED_NS).map(RegExp.escape).join("|");

// Superset of NS_PATTERN that also matches explicit reserved ns —
// gateway has to classify those into the subdomain branch so the
// reserved-ns 404 fires there; otherwise they'd silently fall through
// to the pattern branch with a misleading "no route matches" error.
export const SUBDOMAIN_NS_PATTERN = `(?:${NS_PATTERN}|${RESERVED_NS_PATTERN})`;

/**
 * @param {unknown} ns
 * @returns {boolean}
 */
export function isReservedNs(ns) {
  return typeof ns === "string" && RESERVED_NS.has(ns);
}

const NS_RE = new RegExp(`^${NS_PATTERN}$`);

// Pure boolean form of "is this a legal tenant ns?" — shared by
// auth-lib's assertTenantNs (throw form), validateRecordShape (verify-side
// wrap), and validatePrincipalShape (control-side check). Single source
// of truth so any rule change lands in three places at once.
/**
 * @param {unknown} ns
 * @returns {boolean}
 */
export function isValidTenantNs(ns) {
  return typeof ns === "string" &&
         ns.length > 0 &&
         !isReservedNs(ns) &&
         !RESERVED_TENANT_NS.has(ns) &&
         NS_RE.test(ns);
}

// __system__ is the one whitelist entry because control-plane workers
// (dashboard / webhook-receiver) legitimately need public HTTP. Every
// other reserved ns is JSRPC-only by policy; adding one must be
// deliberate.
export const ROUTES_ALLOWED_RESERVED_NS = new Set(["__system__"]);

// Platform-tier reserved namespaces are not public routes, but runtime-load
// must accept them because [[platform_bindings]] expand to service bindings
// whose target worker lives in the exporting platform namespace.
export const PLATFORM_TIER_RESERVED_NS = new Set(["__platform__"]);

export const RUNTIME_LOAD_ALLOWED_RESERVED_NS = new Set([
  ...ROUTES_ALLOWED_RESERVED_NS,
  ...PLATFORM_TIER_RESERVED_NS,
]);

// Tenant ns names that NS_RE accepts but the platform reserves for
// brand, product, or infrastructure use. Admin-host routing must be
// protected by canonical exact-host matching; this list is a naming
// policy and can be updated in code as the reserved-name set changes.
// Enforced at every control ingress + auth lifecycle + gateway subdomain.
export const RESERVED_TENANT_NS = new Set(["admin"]);

/**
 * @param {unknown} ns
 * @returns {boolean}
 */
export function isValidRouteNs(ns) {
  return isValidTenantNs(ns) ||
         (typeof ns === "string" && ROUTES_ALLOWED_RESERVED_NS.has(ns));
}

/**
 * @param {unknown} ns
 * @returns {boolean}
 */
export function isValidRuntimeLoadNs(ns) {
  return isValidTenantNs(ns) ||
         (typeof ns === "string" && RUNTIME_LOAD_ALLOWED_RESERVED_NS.has(ns));
}

// WDL worker names flow into URL path segments, Redis keys, x-worker-id
// headers, and pub/sub channels. Keep delimiter characters out, but allow the
// common Wrangler-style uppercase and underscore spellings.
export const WORKER_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,254}$/;

/** @param {unknown} name */
export function isValidWorkerName(name) {
  return typeof name === "string" && WORKER_NAME_RE.test(name);
}

// Workflow names are class-adjacent user identifiers rather than Redis queue
// ids, so they follow the worker-name family while keeping a smaller bound for
// UI/API surfaces.
export const WORKFLOW_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** @param {unknown} name */
export function isValidWorkflowName(name) {
  return typeof name === "string" && WORKFLOW_NAME_RE.test(name);
}

// Workflow instance ids are embedded into DB2 keys and tab-delimited scheduler
// tokens. Keep them URL/key friendly and delimiter-free.
export const WORKFLOW_INSTANCE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
export const WORKFLOW_KEY_RE = /^wf_[0-9a-f]{32}$/;

// `:` in an id would corrupt `queue:<ns>:<id>:s` parsing; camelCase
// would split one logical queue across two log-field entries.
export const QUEUE_NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

/** @param {unknown} name */
export function isValidQueueName(name) {
  return typeof name === "string" && QUEUE_NAME_RE.test(name);
}

// JS identifier — CF wrangler's "valid JavaScript variable name".
// `__proto__` / `constructor` guard is separate in RESERVED_OBJECT_KEYS
// so the regex stays CF-shaped. Cap 64 for log / metric label hygiene.
export const BINDING_NAME_RE = /^[A-Za-z_$][A-Za-z0-9_$]{0,63}$/;

// JS class / entrypoint identifier. This intentionally has no length cap:
// workerd entrypoint and Durable Object class names follow JavaScript grammar,
// while binding names add a platform cap for env/log hygiene.
export const JS_IDENTIFIER_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

// DO host ids are `<35-byte storage id>:<class>:shard15` and may not exceed
// 512 UTF-8 bytes. Class declarations use ASCII grammar, so 468 characters is
// the deploy-time bound that keeps every shard addressable.
export const MAX_DO_CLASS_NAME_BYTES = 468;

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isValidJsIdentifier(value) {
  return typeof value === "string" && JS_IDENTIFIER_RE.test(value);
}

export const JS_CLASS_DECLARATION_RESERVED_WORDS = new Set([
  "arguments",
  "await",
  "break",
  "case",
  "catch",
  "class",
  "const",
  "continue",
  "debugger",
  "default",
  "delete",
  "do",
  "else",
  "enum",
  "eval",
  "export",
  "extends",
  "false",
  "finally",
  "for",
  "function",
  "if",
  "implements",
  "import",
  "in",
  "instanceof",
  "interface",
  "let",
  "new",
  "null",
  "package",
  "private",
  "protected",
  "public",
  "return",
  "static",
  "super",
  "switch",
  "this",
  "throw",
  "true",
  "try",
  "typeof",
  "var",
  "void",
  "while",
  "with",
  "yield",
]);

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isValidJsClassDeclarationName(value) {
  return typeof value === "string" &&
         isValidJsIdentifier(value) &&
         !JS_CLASS_DECLARATION_RESERVED_WORDS.has(value);
}

// Runtime-private env bindings use the all-caps WDL shape. User bindings
// must not be allowed to shadow these names before materialization.
export const WDL_RESERVED_BINDING_RE = /^__WDL_[A-Za-z0-9_]*__$/;

// Entrypoint names reserved for runtime-injected shims (currently
// __WdlAbort__). Pattern is conservative — any `__Wdl…__` shape — so
// future shims land without re-litigating the gate.
export const WDL_RESERVED_ENTRYPOINT_RE = /^__Wdl[A-Za-z0-9_]*__$/;

// `:` in the id would alias `kv:<ns>:foo:v:bar` between (id="foo:v",
// key="bar") and (id="foo", key="v:bar").
export const KV_ID_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

/** @param {unknown} id */
export function isValidKvId(id) {
  return typeof id === "string" && KV_ID_RE.test(id);
}

// D1 API ids are stable control-plane references and may retain the
// mixed-case/underscore forms accepted by the existing D1 model.
export const D1_DATABASE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

// Worker-scoped R2 virtual bucket name. Same Redis/S3-key hygiene class as
// KV / Queue ids: lowercase, no slash, no colon, bounded length.
export const R2_BUCKET_NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

// POSIX-style paths for workerd's workerLoader module map. Regex is
// shape-only — see validateModulePath for segment rules.
export const MODULE_PATH_RE = /^[A-Za-z0-9_][A-Za-z0-9._/-]*$/;

// Segments that either take the `__proto__` setter path or shadow
// inherited Object methods. Checked per-segment so `src/__proto__` is
// also rejected, not just bare `__proto__`.
export const RESERVED_OBJECT_KEYS = new Set([
  "__proto__",
  "constructor",
  "prototype",
  "hasOwnProperty",
  "isPrototypeOf",
  "propertyIsEnumerable",
  "toString",
  "toLocaleString",
  "valueOf",
]);

// Throws on bad shape — regex + path traversal + reserved Object.prototype segments.
/**
 * @param {unknown} p
 * @returns {void}
 */
export function validateModulePath(p) {
  if (typeof p !== "string" || !MODULE_PATH_RE.test(p)) {
    throw new Error(`module path ${JSON.stringify(p)} must match ${MODULE_PATH_RE}`);
  }
  for (const seg of p.split("/")) {
    if (seg === "" || seg === "." || seg === "..") {
      throw new Error(`module path ${JSON.stringify(p)} has invalid segment ${JSON.stringify(seg)}`);
    }
    if (RESERVED_OBJECT_KEYS.has(seg)) {
      throw new Error(
        `module path ${JSON.stringify(p)} segment ${JSON.stringify(seg)} is reserved ` +
        `(would collide with Object.prototype on the module map)`
      );
    }
  }
}
