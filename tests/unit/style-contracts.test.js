import { test } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import {
  extractAssignedConstant,
  extractBraceBlock,
  extractExportedStringConst,
  extractRegex,
  extractStringConst,
  extractStringSetConst,
  fixtureSourceFile,
  markdownFiles,
  objectJsonPayloads,
  scannedTestFiles,
  serviceAnchorRegex,
  withoutStringAndTemplateLiterals,
  yamlDocuments,
} from "../helpers/style-contract-scanner.js";
import { jsFiles, readRepoFile, rustFiles, sourceFiles, withoutLineComments } from "../helpers/source-scan.js";

const CONTROL_FILES = jsFiles("control");
const GATEWAY_FILES = jsFiles("gateway");
const RUNTIME_FILES = jsFiles("runtime");
const D1_RUNTIME_FILES = jsFiles("d1-runtime");
const DO_RUNTIME_FILES = jsFiles("do-runtime");
const AUTH_FILES = jsFiles("auth");
const SHARED_FILES = jsFiles("shared");
const PRODUCTION_JS_FILES = [
  ...AUTH_FILES,
  ...CONTROL_FILES,
  ...GATEWAY_FILES,
  ...RUNTIME_FILES,
  ...D1_RUNTIME_FILES,
  ...DO_RUNTIME_FILES,
  ...SHARED_FILES.filter((file) => !file.startsWith("shared/vendor/")),
  ...jsFiles("system-workers"),
];
const PLATFORM_HTTP_ERROR_FILES = [
  // auth/ is a socket-less JSRPC worker; it returns method payloads instead of
  // platform HTTP responses.
  ...CONTROL_FILES,
  ...GATEWAY_FILES,
  ...RUNTIME_FILES,
  ...D1_RUNTIME_FILES,
  ...DO_RUNTIME_FILES,
];
const OFFICIAL_DOC_FILES = [
  "CLAUDE.md",
  "README.md",
  "README.zh.md",
];
// Intentionally empty: add paths here only when a documented active-doc
// one-language exception is approved.
const INTENTIONAL_ONE_LANGUAGE_ACTIVE_DOCS = new Set();

function activeBilingualDocFiles() {
  const notesDirName = ["arch", "ive"].join("");
  return [
    "README.md",
    "README.zh.md",
    ...markdownFiles("docs", { ignoreDirs: new Set([notesDirName]) }),
    "deploy/kubernetes/README.md",
    "deploy/kubernetes/README.zh.md",
  ];
}

// Heuristic multiline scanners for object-literal JSON payload helpers.
// They match a first object argument and an optional second object argument
// (init/options) without attempting full nested-brace parsing.
const OBJECT_LITERAL_ARG_PATTERN = String.raw`\{[\s\S]*?\}`;
const OPTIONAL_SECOND_OBJECT_ARG_PATTERN = String.raw`(?:,\s*\{[\s\S]*?\})?`;
const OBJECT_JSON_PAYLOAD_PATTERNS = [
  new RegExp(String.raw`jsonResponse\(\s*${OBJECT_LITERAL_ARG_PATTERN}\s*${OPTIONAL_SECOND_OBJECT_ARG_PATTERN}\s*\)`, "g"),
  new RegExp(String.raw`json\(\s*${OBJECT_LITERAL_ARG_PATTERN}\s*${OPTIONAL_SECOND_OBJECT_ARG_PATTERN}\s*\)`, "g"),
  new RegExp(String.raw`Response\.json\(\s*${OBJECT_LITERAL_ARG_PATTERN}\s*${OPTIONAL_SECOND_OBJECT_ARG_PATTERN}\s*\)`, "g"),
  new RegExp(
    String.raw`new Response\(\s*JSON\.stringify\(\s*${OBJECT_LITERAL_ARG_PATTERN}\s*\)\s*${OPTIONAL_SECOND_OBJECT_ARG_PATTERN}\s*\)`,
    "g",
  ),
];

const FIXTURE_OBJECT_JSON_PAYLOAD_PATTERNS = OBJECT_JSON_PAYLOAD_PATTERNS.filter(
  (pattern) => !pattern.source.startsWith("jsonResponse"),
);

// Canonical allowlist for metric label keys enforced by style-contract tests.
// Add keys only for approved, low-cardinality telemetry labels used by
// production metrics; keep this list stable to avoid schema drift.
const METRIC_ALLOWED_LABEL_KEYS = new Set([
  "binding",
  "code",
  "command",
  "kind",
  "mode",
  "operation",
  "outcome",
  "reason",
  "route",
  "scope",
  "service",
  "stage",
  "state",
  "status",
]);

// Heuristic JSDoc block matcher for repository-controlled style-contract
// scanning. It intentionally stops at the first closing "*/" and does not try
// to recover malformed comment-escaping edge cases.
const JSDOC_BLOCK_RE = /\/\*\*[\s\S]*?\*\//g;
const EXPLICIT_ANY_TYPE_RE = /\b(?:any|Function)\b/;
const TYPED_TAG_RE = /@(typedef|param|returns?|type)\b/g;

/** @param {string} source */
function moduleListBlocks(source) {
  /** @type {string[]} */
  const blocks = [];
  let searchFrom = 0;
  while (true) {
    const marker = source.indexOf("modules", searchFrom);
    if (marker < 0) return blocks;
    const equals = source.indexOf("=", marker);
    const open = source.indexOf("[", equals);
    if (equals < 0 || open < 0) {
      searchFrom = marker + "modules".length;
      continue;
    }
    if (!/^\s*=/.test(source.slice(marker + "modules".length, open))) {
      searchFrom = marker + "modules".length;
      continue;
    }
    let depth = 0;
    let closed = false;
    for (let i = open; i < source.length; i += 1) {
      const ch = source[i];
      if (ch === "[") depth += 1;
      else if (ch === "]") {
        depth -= 1;
        if (depth === 0) {
          blocks.push(source.slice(open + 1, i));
          searchFrom = i + 1;
          closed = true;
          break;
        }
      }
    }
    if (!closed) searchFrom = marker + "modules".length;
  }
}

/**
 * @param {string} configFile
 * @param {string} embedPath
 */
function embeddedSourcePath(configFile, embedPath) {
  return path.posix.normalize(path.posix.join(path.posix.dirname(configFile), embedPath));
}

/** @param {string} source */
function bareModuleImports(source) {
  const imports = new Set();
  for (const match of source.matchAll(/\bfrom\s+"([^"]+)"/g)) {
    imports.add(match[1]);
  }
  for (const match of source.matchAll(/\bimport\s+"([^"]+)"/g)) {
    imports.add(match[1]);
  }
  return [...imports].filter((specifier) =>
    // Exclude relative/absolute paths and runtime-provided built-in module
    // namespaces that do not require explicit embedding in source bundles.
    !specifier.startsWith(".") &&
    !specifier.startsWith("/") &&
    !specifier.startsWith("node:") &&
    !specifier.startsWith("cloudflare:")
  );
}

test("control handlers parse JSON request bodies through readJsonBody", () => {
  const files = CONTROL_FILES.filter((file) => file !== "control/shared.js");
  const offenders = [];
  for (const file of files) {
    const source = readRepoFile(file);
    if (/JSON\.parse\(await request\.text\(\)\)/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("source scanners ignore generated dependency directories", () => {
  const scanned = [
    ...jsFiles("system-workers"),
    ...jsFiles("test-workers"),
    ...jsFiles("examples"),
  ];
  const offenders = scanned.filter((file) =>
    file.includes("/node_modules/") ||
    file.includes("/.deploy-dist/") ||
    file.includes("/.wrangler/")
  );
  assert.deepEqual(offenders, []);
});

test("shared Redis public barrel does not expose RESP sibling-internal helpers", () => {
  const source = withoutLineComments(readRepoFile("shared/redis.js"));
  for (const name of [
    "buildHSetArgs",
    "concatBuffers",
    "decodeHashObject",
    "decodeStringArray",
    "utf8Decoder",
    "warnRedisCallback",
  ]) {
    assert.doesNotMatch(source, new RegExp(`\\b${RegExp.escape(name)}\\b`), name);
  }
});

test("shared primitive owners stay canonical", () => {
  const inlineHexAllowed = new Set(["shared/hex.js"]);
  const inlineErrorMessageAllowed = new Set([
    "shared/errors.js",
    // These files embed worker-source text where shared modules are not directly importable.
    "do-runtime/alarm-shim-source.js",
    "runtime/d1-client.js",
    "runtime/workflows-client.js",
  ]);
  const inlineHexPattern = /\.toString\(16\)\.padStart\(2/;
  // Matches: `<id> instanceof Error ? <id>.message : String(<id>)`.
  // The backreference requires the same identifier in all three positions.
  const instanceofErrorMessageTernaryPattern =
    /\b([A-Za-z_$][\w$]*)\s+instanceof\s+Error\s*\?\s*\1\.message\s*:\s*String\(\s*\1\s*\)/;
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    const source = withoutStringAndTemplateLiterals(withoutLineComments(readRepoFile(file)));
    if (!inlineHexAllowed.has(file) && inlineHexPattern.test(source)) {
      offenders.push(`${file}: use shared/hex.js#bytesToHex`);
    }
    if (!inlineErrorMessageAllowed.has(file) && instanceofErrorMessageTernaryPattern.test(source)) {
      offenders.push(`${file}: use shared/errors.js#errorMessage`);
    }
  }
  assert.deepEqual(offenders, [], `shared primitive owners must stay canonical:\n${offenders.join("\n")}`);
});

test("name grammar predicates live with the shared regex owner", () => {
  const owner = withoutLineComments(readRepoFile("shared/ns-pattern.js"));
  const controlLib = withoutLineComments(readRepoFile("control/lib.js"));
  for (const name of [
    "isValidWorkerName",
    "isValidWorkflowName",
    "isValidQueueName",
    "isValidKvId",
  ]) {
    assert.match(owner, new RegExp(`export function ${name}\\(`), name);
    assert.doesNotMatch(controlLib, new RegExp(`function ${name}\\(`), name);
  }
});

test("secret Redis key construction uses the shared JS owner", () => {
  const allowed = new Set(["shared/secret-keys.js"]);
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    if (allowed.has(file)) continue;
    const source = withoutLineComments(readRepoFile(file));
    if (
      /[`"']secrets:/.test(source) ||
      /\[\s*["']secrets["'][^\]]*\]\.join\(\s*["']:["']\s*\)/.test(source)
    ) {
      offenders.push(file);
    }
  }
  assert.deepEqual(
    offenders,
    [],
    `production JS must use shared/secret-keys.js for inline secret Redis key construction:\n${offenders.join("\n")}`
  );
});

test("runtime workflow instance id grammar matches shared control grammar", () => {
  assert.equal(
    extractRegex("runtime/workflows-client.js", "WORKFLOW_INSTANCE_ID_RE"),
    extractRegex("shared/ns-pattern.js", "WORKFLOW_INSTANCE_ID_RE")
  );
});

test("D1 object field setters stay shared across wire and transport codecs", () => {
  assert.match(readRepoFile("shared/d1-data-field.js"), /export function setDataField\(/);
  for (const file of ["shared/d1-query-wire.js", "shared/d1-transport.js"]) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /import \{ setDataField \} from "shared-d1-data-field";/, file);
    assert.doesNotMatch(source, /function setDataField\(/, file);
  }
});

test("control handlers do not bypass jsonError for literal error responses", () => {
  const files = [
    ...jsFiles("control/handlers"),
    "control/d1-lifecycle.js",
    "control/d1-migrations.js",
    "control/d1-store.js",
    "control/index.js",
  ];
  const offenders = [];
  for (const file of files) {
    const source = readRepoFile(file);
    // Deliberately strict: control error responses should flow through
    // jsonError(). If a future success payload legitimately needs an `error`
    // field, narrow this check instead of deleting the guard.
    if (/jsonResponse\([\s\S]{0,120}\{\s*error\s*:/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("worker control-plane registry keys go through shared/worker-contract.js helpers", () => {
  // Only shared/worker-contract.js may hold the literal. The `${...}`-anchored regex
  // matches key construction, so channel names ("routes:invalidate"/"flush")
  // and comments are skipped.
  const files = [
    ...CONTROL_FILES,
    ...AUTH_FILES,
    ...GATEWAY_FILES,
    ...DO_RUNTIME_FILES,
    ...RUNTIME_FILES,
  ].filter((file) => file !== "shared/worker-contract.js");
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/`routes:\$\{/.test(source)) offenders.push(`${file}: inline routes:<ns> — use routesKey(ns)`);
    if (/`patterns:\$\{/.test(source)) offenders.push(`${file}: inline patterns:<host> — use patternsKey(host)`);
    if (/`worker-versions:\$\{/.test(source)) {
      offenders.push(`${file}: inline worker-versions: — use workerVersionsKey(ns, worker)`);
    }
    if (/`worker:do-storage:\$\{/.test(source)) {
      offenders.push(`${file}: inline worker:do-storage: — use doStorageIdKey(ns, worker)`);
    }
    if (/`hosts:\$\{/.test(source)) offenders.push(`${file}: inline hosts:<ns> — use hostsKey(ns)`);
    if (/`ns-hosts:\$\{/.test(source)) offenders.push(`${file}: inline ns-hosts:<ns> — use nsHostsKey(ns)`);
    if (/`worker:\$\{[^`]*:next_version`/.test(source)) {
      offenders.push(`${file}: inline next_version counter — use nextVersionKey(ns, worker)`);
    }
    if (/`cron:seq:\$\{/.test(source)) {
      offenders.push(`${file}: inline cron sequence counter — use cronSequenceKey(ns, worker)`);
    }
    if (/"namespaces"/.test(source)) offenders.push(`${file}: inline namespaces set — use NAMESPACES_KEY`);
    if (/"declared-hosts"/.test(source)) {
      offenders.push(`${file}: inline declared-hosts set — use DECLARED_HOSTS_KEY`);
    }
    if (/"host-declarations:"/.test(source)) {
      offenders.push(`${file}: inline host-declarations prefix — use hostDeclarationsKey(host)`);
    }
  }
  // Rust services are production readers too; rust/common/worker_contract.rs holds the
  // only literals (mirror of shared/worker-contract.js), every other crate must call
  // routes_key()/worker_versions_key()/do_storage_id_key().
  const rustOffenderFiles = rustFiles("rust").filter(
    (file) => file !== "rust/common/src/worker_contract.rs"
  );
  for (const file of rustOffenderFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (/format!\("routes:/.test(source)) offenders.push(`${file}: inline routes: — use routes_key(ns)`);
    if (/format!\("worker-versions:/.test(source)) {
      offenders.push(`${file}: inline worker-versions: — use worker_versions_key(ns, worker)`);
    }
    if (/format!\("worker:do-storage:/.test(source)) {
      offenders.push(`${file}: inline worker:do-storage: — use do_storage_id_key(ns, worker)`);
    }
  }
  assert.deepEqual(offenders, [], `worker contract key literals must use shared helpers:\n${offenders.join("\n")}`);
});

test("platform domain configuration uses the shared normalized owner", () => {
  const owner = withoutLineComments(readRepoFile("shared/ns-pattern.js"));
  assert.match(owner, /DEFAULT_PLATFORM_DOMAIN = "workers\.local"/);
  for (const file of [
    "control/handlers/deploy.js",
    "control/handlers/promote.js",
    "control/handlers/hosts.js",
    "gateway/index.js",
  ]) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /\bplatformDomainFromEnv\b/, file);
    assert.doesNotMatch(source, /"workers\.local"/, file);
  }
});

test("secret Redis key literals stay aligned across JS and redis-proxy", () => {
  const js = readRepoFile("shared/secret-keys.js");
  const rust = readRepoFile("rust/redis-proxy/src/runtime.rs");

  assert.match(js, /return `secrets:\$\{ns\}`/);
  assert.match(js, /return `secrets:\$\{ns\}:\$\{worker\}`/);
  assert.match(rust, /format!\("secrets:\{\}", q\.ns\)/);
  assert.match(rust, /format!\("secrets:\{\}:\{\}", q\.ns, q\.worker\)/);
});

test("worker delete lock key stays aligned across control and workflows", () => {
  const workerContract = withoutLineComments(readRepoFile("shared/worker-contract.js"));
  const commonWorkerContract = withoutLineComments(
    readRepoFile("rust/common/src/worker_contract.rs"),
  );
  const controlShared = withoutLineComments(readRepoFile("control/shared.js"));
  const workflowsActiveExport = withoutLineComments(readRepoFile("rust/workflows/src/api/active_export.rs"));

  assert.match(workerContract, /`worker-delete-lock:\$\{ns\}:\$\{worker\}`/);
  assert.match(commonWorkerContract, /format!\("worker-delete-lock:\{ns\}:\{worker\}"\)/);
  assert.match(controlShared, /\bdeleteLockKey\(ns, worker\)/);
  assert.match(workflowsActiveExport, /\bworker_delete_lock_key\(ns, worker\)/);

  const rustOwner = "rust/common/src/worker_contract.rs";
  const offenders = rustFiles("rust").filter((file) =>
    file !== rustOwner && withoutLineComments(readRepoFile(file)).includes("worker-delete-lock:")
  );
  assert.deepEqual(offenders, []);
});

test("product success payloads use camelCase fields", () => {
  const promote = withoutLineComments(readRepoFile("control/handlers/promote.js"));
  const promoteSuccess = /return jsonResponse\(200,\s*\{[\s\S]*?\n\s*\}\);/.exec(promote)?.[0] || "";
  assert.notEqual(promoteSuccess, "");
  assert.equal(/\baffected_hosts\s*:/.test(promoteSuccess), false);
  assert.equal(/\bplatform_domain\s*:/.test(promoteSuccess), false);

  const workerSecrets = withoutLineComments(readRepoFile("control/handlers/worker-secrets.js"));
  assert.equal(/\bconst payload = \{[\s\S]{0,180}\bprevious_version\s*:/.test(workerSecrets), false);
  assert.equal(/\b(?:secret_written|reload_forced|next_pickup)\s*:/.test(workerSecrets), false);

  const bindings = withoutLineComments(readRepoFile("control/bindings.js"));
  assert.equal(/\bmissing_caller_secrets\s*:/.test(bindings), false);

  const shared = withoutLineComments(readRepoFile("control/shared.js"));
  assert.equal(/\breturn \{ kind: "queue_hint_failed", task_id:/.test(shared), false);

  const reload = withoutLineComments(readRepoFile("control/handlers/reload.js"));
  assert.match(reload, /\bdurationMs\s*:/);
  assert.equal(/jsonResponse\([\s\S]{0,220}\bduration_ms\s*:/.test(reload), false);

  // fixtureSourceFile() means "scannable source file" here: it filters out
  // generated dependency/deploy output inside fixture/example trees.
  const payloadFiles = [
    ...CONTROL_FILES,
    ...RUNTIME_FILES,
    ...D1_RUNTIME_FILES,
    ...DO_RUNTIME_FILES,
    ...jsFiles("test-workers").filter(fixtureSourceFile),
    ...jsFiles("examples").filter(fixtureSourceFile),
  ];
  const offenders = [];
  for (const file of payloadFiles) {
    const source = withoutLineComments(readRepoFile(file));
    const payloads = objectJsonPayloads(source, OBJECT_JSON_PAYLOAD_PATTERNS);
    for (const payload of payloads) {
      if (/\b(error|served_by)\s*:/.test(payload)) continue;
      const snakeKeys = [...payload.matchAll(/\b([a-z][A-Za-z0-9]*_[A-Za-z0-9_]+)\s*:/g)]
        .map((match) => match[1])
        // Queue message fixtures intentionally expose content_type in their stored schema.
        .filter((key) => !["content_type"].includes(key));
      if (snakeKeys.length) offenders.push(`${file}: ${snakeKeys.join(", ")} in ${payload}`);
    }
  }
  assert.deepEqual(offenders, [], `product/test/example JSON success payloads should use camelCase keys:\n${offenders.join("\n")}`);
});

test("fixture workers return JSON errors with machine error and message", () => {
  const files = [
    ...jsFiles("examples"),
    ...jsFiles("test-workers"),
  ].filter(fixtureSourceFile);
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    const payloads = objectJsonPayloads(source, FIXTURE_OBJECT_JSON_PAYLOAD_PATTERNS);
    for (const payload of payloads) {
      if (/\berror\s*:/.test(payload) && !/\bmessage\s*:/.test(payload)) {
        offenders.push(`${file}: ${payload}`);
      }
    }
    if (/console\.error\(\s*(?!JSON\.stringify\(\s*\{)/.test(source)) {
      offenders.push(`${file}: console.error should emit structured single-line JSON`);
    }
  }
  assert.deepEqual(offenders, [], `fixture JSON errors must include message:\n${offenders.join("\n")}`);
});

test("control handlers format coded domain errors through the shared helper", () => {
  const files = jsFiles("control/handlers");
  const offenders = [];
  for (const file of files) {
    const source = readRepoFile(file);
    if (/jsonError\(\s*err\.status\s*,/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("runtime JSON error helpers share the reserved-detail sanitizer", () => {
  const files = ["d1-runtime/http.js", "do-runtime/http.js"];
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (!/jsonErrorWith/.test(source)) offenders.push(`${file}:missing jsonErrorWith`);
    if (/\{\s*error\s*:\s*_error\s*,\s*message\s*:\s*_message\s*,\s*reason\s*:\s*_reason\s*,/.test(source)) {
      offenders.push(`${file}:local destructuring`);
    }
  }
  assert.deepEqual(offenders, []);
});

test("DO protocol error sanitizer uses the shared response sanitizer directly", () => {
  const source = withoutLineComments(readRepoFile("do-runtime/protocol/errors.js"));
  assert.match(source, /import \{ sanitizeJsonErrorDetails \} from "shared-respond";/);
  assert.doesNotMatch(source, /function sanitizeErrorDetails\(/);
});

test("control modules keep state member access in shared and the dispatcher", () => {
  const files = jsFiles("control").filter((file) => ![
    "control/shared.js",
    "control/index.js",
  ].includes(file));
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/\bstate\.|\b(?:const|let|var)\s*\{[^}]*\}\s*=\s*state\b/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("production JS implementation does not reintroduce explicit any typedefs", () => {
  const offenders = [];
  const findMatchingBrace = (/** @type {string} */ text, /** @type {number} */ openIndex) => {
    // This scans JSDoc type expressions, not full JavaScript source; repository
    // JSDoc types should not contain unmatched braces inside string/comment text.
    if (openIndex < 0 || openIndex >= text.length || text[openIndex] !== "{") return -1;
    let depth = 1;
    for (let idx = openIndex + 1; idx < text.length; idx += 1) {
      const ch = text[idx];
      if (ch === "{") depth += 1;
      if (ch === "}") {
        depth -= 1;
        if (depth === 0) return idx;
      }
    }
    return -1;
  };
  const jsdocTypeExpressions = (/** @type {string} */ block) => {
    const types = [];
    for (const tag of block.matchAll(TYPED_TAG_RE)) {
      const open = block.indexOf("{", tag.index);
      if (open === -1) continue;
      const close = findMatchingBrace(block, open);
      if (close === -1) continue;
      types.push(block.slice(open + 1, close));
    }
    return types;
  };
  for (const file of PRODUCTION_JS_FILES) {
    const source = readRepoFile(file);
    for (const match of source.matchAll(JSDOC_BLOCK_RE)) {
      if (jsdocTypeExpressions(match[0]).some((type) => EXPLICIT_ANY_TYPE_RE.test(type))) {
        offenders.push(file);
        break;
      }
    }
  }
  assert.deepEqual(offenders, []);
});

test("numeric defaults do not use legacy || fallback spelling", () => {
  const files = [
    ...PRODUCTION_JS_FILES,
    ...jsFiles("test-workers"),
    ...jsFiles("examples"),
  ].filter(fixtureSourceFile);
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    // Detect legacy numeric defaults that use `||` either after coercion/parsing
    // (Number(x) || 0) or inside parse/coercion arguments (parseInt(x || "0")).
    const legacyNumericDefaultPatterns = [
      // `||` fallback applied after coercion/parsing call, e.g. `Number(x) || 0`.
      /\b(?:Number|(?:Number\.)?parseInt)\([^;\n]*\)\s*\|\|\s*[^;\n]+/g,
      // `||` fallback used inside coercion/parsing arguments, e.g. `parseInt(x || "0")`.
      /\b(?:Number|(?:Number\.)?parseInt)\([^;\n]*\|\|[^;\n]*\)/g,
    ];
    for (const pattern of legacyNumericDefaultPatterns) {
      for (const match of source.matchAll(pattern)) {
        offenders.push(`${file}: ${match[0]}`);
      }
    }
  }
  assert.deepEqual(offenders, [], `numeric defaults should use ?? or explicit validation:\n${offenders.join("\n")}`);
});

test("system workers emit single-line structured JSON logs", () => {
  const source = withoutLineComments(readRepoFile("system-workers/s3-cleanup/src/index.js"));
  assert.match(source, /function logStructured\(/);
  assert.equal(/console\.log\(\s*"s3_cleanup_/.test(source), false);
  assert.equal(/console\.(?:log|warn|error)\([^)]*,\s*JSON\.stringify/.test(source), false);
  assert.equal(/logStructured\([\s\S]{0,220}\berror\s*:/.test(source), false);
  assert.match(source, /\berror_message\s*:/);
});

test("platform-generated HTTP errors include machine error and human message", () => {
  const files = PLATFORM_HTTP_ERROR_FILES;
  const offenders = [];
  for (const file of files) {
    const source = readRepoFile(file);
    if (/\{\s*error\s*:\s*"(?:Not found|not found|method not allowed)"\s*\}/.test(source)) {
      offenders.push(file);
    }
    if (/JSON\.stringify\(\{\s*error\s*:\s*message\s*\}\)/.test(source)) {
      offenders.push(file);
    }
    if (/jsonResponse\([\s\S]{0,120}\{\s*error\s*:\s*err\.message\s*\}/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("top-level JS HTTP catches do not expose err.message on the wire", () => {
  const files = ["control/index.js", "gateway/index.js", "runtime/index.js", "runtime/internal.js"];
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/jsonError\([^)]*\berr\.message\b/.test(source)) offenders.push(file);
    if (!/internalErrorResponse\(/.test(source)) offenders.push(`${file}:missing internalErrorResponse`);
  }
  assert.deepEqual(offenders, []);
});

test("JS HTTP tiers use shared generic response helpers", () => {
  const files = [
    "gateway/index.js",
    "runtime/index.js",
    "d1-runtime/index.js",
    "d1-runtime/ops.js",
    "do-runtime/index.js",
  ];
  const offenders = [];
  const helperNames = "(?:jsonResponse|jsonError|errorResponse|metricsResponse|responseWithRequestId)";
  const localHelperPattern = new RegExp(
    `\\b(?:function\\s+${helperNames}\\s*\\(|(?:const|let|var)\\s+${helperNames}\\s*=)`
  );
  for (const file of files) {
    const source = readRepoFile(file);
    // Keep this narrow: tier-specific handlers are fine, generic builders belong in shared/respond.js.
    if (localHelperPattern.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("production JS response body cleanup uses the shared helper", () => {
  const respondSource = withoutLineComments(readRepoFile("shared/respond.js"));
  assert.match(respondSource, /\bexport\s+async\s+function\s+discardResponseBody\(/);
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    if (file === "shared/respond.js") continue;
    const source = withoutStringAndTemplateLiterals(withoutLineComments(readRepoFile(file)));
    if (/\.body\s*(?:\?\.|\.)\s*cancel\s*\(/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("auth entrypoint keeps runtime IO mechanics in auth/runtime.js", () => {
  const source = withoutLineComments(readRepoFile("auth/index.js"));
  const offenders = [];
  // Keep the JSRPC entrypoint focused on method flow and policy decisions; Redis,
  // logging, and bootstrap reflection belong in auth/runtime.js.
  if (/from\s+"shared-(?:redis|observability)"/.test(source)) {
    offenders.push("runtime imports");
  }
  if (/\b(?:function\s+(?:ensureInit|ensureBootstrap|onRedisCommand)|(?:const|let|var)\s+(?:metrics|log)\b|MetricsRegistry|createLogger)\b/.test(source)) {
    offenders.push("runtime helper");
  }
  assert.deepEqual(offenders, []);
});

test("gateway entrypoint keeps cache and subscriber mechanics in gateway/runtime.js", () => {
  const source = withoutLineComments(readRepoFile("gateway/index.js"));
  const offenders = [];
  // Request dispatch can read cached routing state through gateway-runtime, but
  // Redis clients, subscribers, and cache maps stay out of the entrypoint.
  if (/from\s+"shared-redis"/.test(source)) offenders.push("redis import");
  if (/\b(?:RedisSubscriber|RedisClient|routeCache|patternCache|knownNs)\b/.test(source)) {
    offenders.push("runtime state");
  }
  assert.deepEqual(offenders, []);
});

test("JS HTTP entrypoints use the shared request scope", () => {
  const files = ["control/index.js", "gateway/index.js", "runtime/index.js", "runtime/internal.js", "d1-runtime/index.js", "do-runtime/index.js"];
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/\b(?:ensureRequestId|recordRequestComplete|echoResponseWithRequestId)\b/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("JS HTTP entrypoints propagate sanitized request scope ids", () => {
  const offenders = [];
  const file = "do-runtime/index.js";
  const source = withoutLineComments(readRepoFile(file));
  if (/request\.headers\.get\("x-request-id"\)/.test(source)) offenders.push(file);
  assert.deepEqual(offenders, []);
});

test("HTTP request completion observability stays centralized", () => {
  const files = [
    ...CONTROL_FILES,
    ...GATEWAY_FILES,
    ...RUNTIME_FILES,
    ...D1_RUNTIME_FILES,
    ...DO_RUNTIME_FILES,
    ...AUTH_FILES,
    ...SHARED_FILES,
  ].filter((file) => file !== "shared/observability.js");
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    // Strict by design: request metrics/log event names stay in one helper so
    // label additions and probe suppression cannot drift by tier. auth/ is
    // included because socket-less workers must not grow request metrics.
    if (/"requests"|"request_duration_ms"|"request_errors"|"request_complete"/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("gateway routing lookup metrics use one outcome-labeled series", () => {
  const source = withoutLineComments(readRepoFile("gateway/runtime.js"));
  const routingCalls = (source.match(/metrics\.increment\([^;]+;/gs) || [])
    .filter((call) => /["']routing_lookups["']/.test(call));
  assert.equal(routingCalls.length, 1);
  assert.match(routingCalls[0], /\bstage\b/);
  assert.match(routingCalls[0], /\boutcome\b/);

  const retiredFamilies = /route_cache_(?:hits|misses)|pattern_cache_(?:hits|misses)|known_namespace_misses/;
  const offenders = ["gateway/index.js", "gateway/runtime.js", "README.md", "README.zh.md"]
    .filter((file) => retiredFamilies.test(withoutLineComments(readRepoFile(file))));
  assert.deepEqual(offenders, []);
});

test("runtime entrypoint keeps service singleton mechanics in runtime/runtime.js", () => {
  const source = withoutLineComments(readRepoFile("runtime/index.js"));
  const offenders = [];
  if (/\b(?:createLogger|setLogLevel|createLogLevelBinder)\b/.test(source)) {
    offenders.push("observability setup");
  }
  if (/\b(?:let|const|var)\s+(?:log|serviceName|logLevelSet)\b/.test(source)) {
    offenders.push("runtime singleton");
  }
  assert.deepEqual(offenders, []);
});

test("runtime public socket does not handle internal dispatch paths", () => {
  const source = withoutLineComments(readRepoFile("runtime/index.js"));
  for (const literal of [
    '"/_healthz"',
    '"/_metrics"',
    '"/_scheduled"',
    '"/_queued"',
    '"/internal/workflows/run"',
    '"/internal/workflows/notify"',
  ]) {
    assert.equal(source.includes(literal), false, `${literal} belongs on runtime/internal.js`);
  }
});

test("runtime public socket strips internal auth before tenant dispatch", () => {
  const source = withoutLineComments(readRepoFile("runtime/index.js"));
  assert.match(source, /stripInternalAuthHeader\(forwardRequest\.headers\)/);
});

test("runtime dispatch entrypoints reject non-route platform-tier worker ids", () => {
  for (const file of ["runtime/index.js", "runtime/internal.js"]) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /parseDispatchWorkerId/, file);
    assert.doesNotMatch(source, /parseRuntimeLoadWorkerId|parseRuntimeWorkerId/, file);
  }
});

test("runtime internal active-version events evict siblings but workflows do not", () => {
  const source = withoutLineComments(readRepoFile("runtime/internal.js"));
  const scheduledStart = source.indexOf('pathname === "/_scheduled"');
  const queuedStart = source.indexOf('pathname === "/_queued"');
  const workflowRunStart = source.indexOf('pathname === "/internal/workflows/run"');
  const workerIdGuard = source.indexOf('const workerId = request.headers.get("x-worker-id")');
  const notFoundReturn = source.indexOf('return scope.respond(jsonError(404');
  for (const [name, index] of [
    ["/_scheduled branch", scheduledStart],
    ["/_queued branch", queuedStart],
    ["/internal/workflows/run branch", workflowRunStart],
    ["x-worker-id guard", workerIdGuard],
    ["404 fallthrough", notFoundReturn],
  ]) {
    assert.notEqual(index, -1, `${name} literal moved or reformatted`);
  }
  assert.ok(scheduledStart < queuedStart, "/_scheduled branch must precede /_queued branch");
  assert.ok(queuedStart < notFoundReturn, "/_queued branch must precede 404 fallthrough");
  assert.ok(workflowRunStart < workerIdGuard, "/internal/workflows/run branch must precede x-worker-id guard");
  const scheduledBranch = source.slice(scheduledStart, queuedStart);
  const queuedBranch = source.slice(queuedStart, notFoundReturn);
  // This deliberately spans /internal/workflows/run and /internal/workflows/notify;
  // neither frozen-version workflow path should evict active-version siblings.
  const workflowBranch = source.slice(workflowRunStart, workerIdGuard);
  assert.ok(scheduledBranch.length > 0, "scheduled branch source slice must be non-empty");
  assert.ok(queuedBranch.length > 0, "queued branch source slice must be non-empty");
  assert.ok(workflowBranch.length > 0, "workflow branch source slice must be non-empty");
  assert.match(scheduledBranch, /evictOnLoad:\s*true/);
  assert.match(queuedBranch, /evictOnLoad:\s*true/);
  assert.doesNotMatch(workflowBranch, /evictOnLoad:\s*true/);
});

test("runtime internal entrypoint keeps worker-event dispatch in runtime/dispatch.js", () => {
  const source = withoutLineComments(readRepoFile("runtime/internal.js"));
  const offenders = [];
  if (/\b(?:normalizeScheduledDispatchBody|normalizeQueuedDispatchBody|decodeQueuedDispatchMessages)\b/.test(source)) {
    offenders.push("dispatch normalization");
  }
  if (/\bgetEntrypoint\(\)\.(?:scheduled|queue)\b/.test(source)) {
    offenders.push("handler dispatch");
  }
  if (!/return await handleScheduledDispatch\(/.test(source)) {
    offenders.push("scheduled dispatch completion");
  }
  if (!/return await handleQueuedDispatch\(/.test(source)) {
    offenders.push("queued dispatch completion");
  }
  assert.deepEqual(offenders, []);
});

test("log-level binding belongs to shared observability", () => {
  const files = [
    ...CONTROL_FILES,
    ...GATEWAY_FILES,
    ...RUNTIME_FILES,
    ...D1_RUNTIME_FILES,
    ...DO_RUNTIME_FILES,
    ...AUTH_FILES,
    ...SHARED_FILES,
  ].filter((file) => file !== "shared/observability.js");
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/\bsetLogLevel\s*\(/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("Redis command observability goes through recordRedisCommand", () => {
  const files = [
    ...CONTROL_FILES,
    ...GATEWAY_FILES,
    ...RUNTIME_FILES,
    ...DO_RUNTIME_FILES,
    ...AUTH_FILES,
    ...SHARED_FILES,
  ].filter((file) => file !== "shared/observability.js");
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/"redis_commands"|"redis_command_duration_ms"|"redis_command_failed"|"redis_watch_invalidation"/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("Redis command metric allow-list covers shared Redis wrappers", () => {
  const redis = [
    "shared/redis-command-client.js",
    "shared/redis-session.js",
  ].map((file) => withoutLineComments(readRepoFile(file))).join("\n");
  const observability = withoutLineComments(readRepoFile("shared/observability.js"));
  const labelsBody = /const REDIS_COMMAND_LABELS = new Set\(\[([\s\S]*?)\]\);/.exec(observability)?.[1] || "";
  assert.ok(labelsBody, "REDIS_COMMAND_LABELS Set literal not found in shared/observability.js");
  const labels = new Set([...labelsBody.matchAll(/"([A-Z_]+)"/g)].map((match) => match[1]));
  const commands = new Set();
  for (const match of redis.matchAll(/\._exec\(\s*"([A-Z_]+)"/g)) {
    commands.add(match[1]);
  }
  for (const match of redis.matchAll(/\._execPipeline\(\s*"([A-Z_]+)"/g)) {
    commands.add(match[1]);
  }
  for (const match of redis.matchAll(/_withSocket\(\s*"([A-Z_]+)"/g)) {
    commands.add(match[1]);
  }
  for (const match of redis.matchAll(/const args = \[\s*"([A-Z_]+)"/g)) {
    commands.add(match[1]);
  }
  for (const match of redis.matchAll(/_commands\.push\(\[\s*"([A-Z_]+)"/g)) {
    commands.add(match[1]);
  }
  const missing = [...commands].filter((command) => !labels.has(command)).toSorted();
  assert.deepEqual(
    missing,
    [],
    `add to REDIS_COMMAND_LABELS in shared/observability.js: ${missing.join(", ")}`
  );
});

test("declared-host Redis key literals stay aligned across control, gateway, and docs", () => {
  const workerContract = withoutLineComments(readRepoFile("shared/worker-contract.js"));
  const shared = withoutLineComments(readRepoFile("control/shared.js"));
  const routing = withoutLineComments(readRepoFile("control/routing.js"));
  const gateway = withoutLineComments(readRepoFile("gateway/runtime.js"));
  const layoutEn = readRepoFile("docs/redis-key-layout.md");
  const layoutZh = readRepoFile("docs/redis-key-layout.zh.md");
  const declaredHostsKey = extractStringConst(workerContract, "DECLARED_HOSTS_KEY");
  const hostDeclarationsPrefix = extractStringConst(workerContract, "HOST_DECLARATIONS_PREFIX");

  assert.match(routing, new RegExp(`\\bDECLARED_HOSTS_KEY\\b`));
  assert.match(routing, /\bhostDeclarationsKey\b/);
  assert.match(shared, /\bhostDeclarationsKey\b/);
  assert.match(gateway, /\bDECLARED_HOSTS_KEY\b/);
  for (const source of [layoutEn, layoutZh]) {
    assert.match(source, new RegExp(`\\b${RegExp.escape(declaredHostsKey)}\\b`));
    assert.match(source, new RegExp(`\\b${RegExp.escape(hostDeclarationsPrefix)}<host>`));
  }
});

test("gateway strips every client-supplied platform header it injects", () => {
  const gateway = withoutLineComments(readRepoFile("gateway/index.js"));
  const owner = withoutLineComments(readRepoFile("gateway/lib.js"));
  const headerList = /const INTERNAL_FORWARD_HEADERS = \[([\s\S]*?)\];/.exec(owner);
  assert.ok(headerList, "gateway INTERNAL_FORWARD_HEADERS list must be present");
  const stripped = new Set([...headerList[1].matchAll(/"([^"]+)"/g)].map((match) => match[1]));
  const injectedNonPrefixedInternal = new Set(
    [...gateway.matchAll(/forwardRequest\.headers\.set\("(x-worker[^"]+)"/g)]
      .map((match) => match[1])
  );
  const missing = [...injectedNonPrefixedInternal].filter((header) => !stripped.has(header)).toSorted();
  assert.deepEqual(
    missing,
    [],
    `gateway INTERNAL_FORWARD_HEADERS must include injected non-prefixed internal headers: ${missing.join(", ")}`
  );
  assert.match(owner, /const INTERNAL_HEADER_PREFIX = "x-wdl-"/);
  assert.match(owner, /name\.toLowerCase\(\)\.startsWith\(INTERNAL_HEADER_PREFIX\)/);
  assert.match(owner, /headers\.delete\(name\)/);
  assert.match(gateway, /deleteGatewayInternalHeaders\(forwardRequest\.headers\)/);

  const websocket = withoutLineComments(readRepoFile("gateway/websocket.js"));
  assert.match(websocket, /deleteGatewayInternalHeaders\(out\)/);
});

test("D1 owner protocol errors stay aligned with runtime stale-hint handling", () => {
  const protocol = withoutLineComments(readRepoFile("d1-runtime/protocol.js"));
  const runtimeBinding = withoutLineComments(readRepoFile("runtime/bindings/d1.js"));

  assert.deepEqual(
    extractStringSetConst(runtimeBinding, "OWNER_HINT_STALE_CODES"),
    extractStringSetConst(protocol, "OWNERSHIP_CODES")
  );
});

test("JS tiers do not hand-roll structured console JSON logs", () => {
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    if (!fixtureSourceFile(file)) continue;
    const source = withoutLineComments(readRepoFile(file));
    if (/console\.(?:log|info|warn|error)\(\s*JSON\.stringify/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("JS structured warnings use stdout, not console.warn", () => {
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES.filter(fixtureSourceFile)) {
    const source = withoutLineComments(readRepoFile(file));
    if (/console\.warn\(/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("Redis collection call sites use canonical camelCase spellings", () => {
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    if (file === "shared/redis-command-client.js") continue;
    const source = withoutLineComments(readRepoFile(file));
    if (/\.(?:smembers|hgetall)\s*\(/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("Redis callback warnings use the shared logger and stay non-sensitive", () => {
  const source = withoutLineComments(readRepoFile("shared/redis-resp.js"));
  assert.match(source, /const redisCallbackLog = createLogger\("shared-redis"\)/);
  assert.match(source, /function warnRedisCallback\(/);
  assert.doesNotMatch(source, /\[redis[^\]]*\]/);
  assert.doesNotMatch(source, /console\.(?:log|info|warn|error)\(/);
  assert.match(source, /error_message:/);
});

test("shared relative imports used by embedded modules are embedded in the same module list", () => {
  const configFiles = [
    "gateway/config.capnp",
    "gateway/config-local.capnp",
    "runtime/config-user.capnp",
    "runtime/config-user-local.capnp",
    "runtime/config-system.capnp",
    "runtime/config-system-local.capnp",
    "d1-runtime/config.capnp",
    "do-runtime/config.capnp",
    "do-runtime/config-local.capnp",
  ];
  const relativeDepsBySharedFile = new Map();
  for (const file of SHARED_FILES.filter((path) => !path.startsWith("shared/vendor/"))) {
    const source = withoutLineComments(readRepoFile(file));
    const deps = [...source.matchAll(/from "\.\/([^"]+\.js)"/g)].map((match) => match[1]);
    if (deps.length) relativeDepsBySharedFile.set(file, deps);
  }

  const offenders = [];
  for (const configFile of configFiles) {
    const blocks = moduleListBlocks(withoutLineComments(readRepoFile(configFile)));
    for (const block of blocks) {
      for (const [file, deps] of relativeDepsBySharedFile) {
        if (!block.includes(`embed "../${file}"`)) continue;
        for (const dep of deps) {
          const depPath = `../shared/${dep}`;
          const depEntry = new RegExp(
            `name\\s*=\\s*"${RegExp.escape(dep)}"[\\s\\S]*?embed\\s+"${RegExp.escape(depPath)}"`
          );
          if (!depEntry.test(block)) offenders.push(`${configFile}:${file}->${dep}`);
        }
      }
    }
  }
  assert.deepEqual(offenders, []);
});

test("bare imports used by embedded modules are embedded in the same worker module list", () => {
  const configFiles = [
    "gateway/config.capnp",
    "gateway/config-local.capnp",
    "runtime/config-user.capnp",
    "runtime/config-user-local.capnp",
    "runtime/config-system.capnp",
    "runtime/config-system-local.capnp",
    "d1-runtime/config.capnp",
    "do-runtime/config.capnp",
    "do-runtime/config-local.capnp",
  ];
  const offenders = [];
  for (const configFile of configFiles) {
    const blocks = moduleListBlocks(withoutLineComments(readRepoFile(configFile)));
    for (const block of blocks) {
      const moduleEntries = [...block.matchAll(
        /\(name\s*=\s*"([^"]+)"\s*,\s*(?:esModule|text)\s*=\s*embed\s+"([^"]+)"/g
      )];
      const sourceEntries = [...block.matchAll(
        /\(name\s*=\s*"([^"]+)"\s*,\s*esModule\s*=\s*embed\s+"([^"]+)"/g
      )];
      const moduleNames = new Set(moduleEntries.map((match) => match[1]));
      for (const entry of sourceEntries) {
        const sourcePath = embeddedSourcePath(configFile, entry[2]);
        const imports = bareModuleImports(withoutLineComments(readRepoFile(sourcePath)));
        for (const specifier of imports) {
          if (!moduleNames.has(specifier)) {
            offenders.push(`${configFile}:${entry[1]} imports ${specifier}`);
          }
        }
      }
    }
  }
  assert.deepEqual(offenders, []);
});

test("Rust service logs use the shared JSON envelope emitter", () => {
  const wrappers = [
    "rust/redis-proxy/src/observability.rs",
    "rust/supervisor/src/log.rs",
  ];
  const offenders = [];
  for (const file of wrappers) {
    const source = withoutLineComments(readRepoFile(file));
    if (!/\bemit_log_line\b/.test(source)) offenders.push(`${file}:missing emit_log_line`);
    if (/\b(?:println!|eprintln!)\s*\(/.test(source)) offenders.push(`${file}:local stream routing`);
    if (/\b(?:ts|service|level|event)"\.into\(\)|"(?:ts|service|level|event)"\.to_string\(\)/.test(source)) {
      offenders.push(`${file}:hand-built envelope fields`);
    }
  }
  assert.deepEqual(offenders, []);
});

test("metric cardinality warnings use structured log events in JS and Rust", () => {
  const js = withoutLineComments(readRepoFile("shared/observability.js"));
  const rust = withoutLineComments(readRepoFile("rust/common/src/metrics.rs"));

  assert.match(js, /"metric_cardinality_warning"/);
  assert.match(js, /emitStructuredLogLine\("observability", "warn", "metric_cardinality_warning"/);
  assert.match(rust, /"metric_cardinality_warning"/);
  assert.match(rust, /emit_log_line\(\s*"observability",\s*LogLevel::Warn,\s*LogLevel::Debug,/);
  assert.doesNotMatch(js, /console\.warn\(/);
  assert.doesNotMatch(rust, /\beprintln!\s*\(/);
});

test("redis-proxy HTTP observability matches platform request strategy", () => {
  const lib = withoutLineComments(readRepoFile("rust/redis-proxy/src/lib.rs"));
  const observability = withoutLineComments(readRepoFile("rust/redis-proxy/src/observability.rs"));

  assert.match(observability, /emit_log_line\(SERVICE, level, current_level\(\), event, fields\)/);
  assert.match(lib, /"request_errors"/);
  assert.match(lib, /"request_complete"/);
  assert.match(lib, /request_id_from_headers\(request\.headers\(\)\)/);
  assert.match(lib, /matches!\(route, "healthz" \| "metrics"\)/);
  assert.match(lib, /struct ResponseError/);
});

test("active docs and sources do not point at note paths", () => {
  const notesDirName = ["arch", "ive"].join("");
  const notesPath = `docs/${notesDirName}`;
  const files = [
    ...OFFICIAL_DOC_FILES,
    ...markdownFiles("docs", { ignoreDirs: new Set([notesDirName]) }),
    ...PRODUCTION_JS_FILES,
    ...sourceFiles("scripts", { extensions: [".js", ".mjs"] }),
    ...scannedTestFiles(new Set(["tests/unit/style-contracts.test.js"])),
  ];
  const forbiddenFragments = [
    `${notesPath}/`,
    `${notesDirName}/`,
  ];
  const offenders = [];
  for (const file of files) {
    const source = readRepoFile(file);
    for (const fragment of forbiddenFragments) {
      if (source.includes(fragment)) offenders.push(`${file}:${fragment}`);
    }
  }
  assert.deepEqual(offenders.toSorted(), []);
});

test("active bilingual docs stay paired", () => {
  const files = activeBilingualDocFiles();
  const fileSet = new Set(files);
  const offenders = [];
  for (const file of files) {
    if (INTENTIONAL_ONE_LANGUAGE_ACTIVE_DOCS.has(file)) continue;
    if (file.endsWith(".zh.md")) {
      const english = file.replace(/\.zh\.md$/, ".md");
      if (!fileSet.has(english)) offenders.push(`${file}:missing ${english}`);
    } else {
      const chinese = file.replace(/\.md$/, ".zh.md");
      if (!fileSet.has(chinese)) offenders.push(`${file}:missing ${chinese}`);
    }
  }
  assert.deepEqual(offenders.toSorted(), []);
});

test("protocol and contributor contracts stay discoverable from active docs", () => {
  const links = [
    ["CLAUDE.md", "docs/protocol-contracts.md"],
    ["CLAUDE.md", "docs/contributing.md"],
    ["README.md", "docs/protocol-contracts.md"],
    ["README.md", "docs/contributing.md"],
    ["README.zh.md", "docs/protocol-contracts.zh.md"],
    ["README.zh.md", "docs/contributing.zh.md"],
    ["docs/README.md", "protocol-contracts.md"],
    ["docs/README.md", "contributing.md"],
    ["docs/README.zh.md", "protocol-contracts.zh.md"],
    ["docs/README.zh.md", "contributing.zh.md"],
    ["docs/project-standards.md", "docs/protocol-contracts.md"],
    ["docs/project-standards.zh.md", "docs/protocol-contracts.zh.md"],
    ["docs/testing.md", "docs/protocol-contracts.md"],
    ["docs/testing.zh.md", "docs/protocol-contracts.zh.md"],
  ];
  const offenders = [];
  for (const [file, needle] of links) {
    if (!readRepoFile(file).includes(needle)) offenders.push(`${file}: missing ${needle}`);
  }
  assert.deepEqual(offenders, []);
});

test("auth worker stays log-observed instead of owning an unreachable metrics registry", () => {
  const source = withoutLineComments(readRepoFile("auth/runtime.js"));
  assert.equal(/\bMetricsRegistry\b|\bmetrics\.(?:increment|observe|setGauge)\b/.test(source), false);
});

test("control worker stays log-observed instead of owning an unreachable metrics registry", () => {
  const files = jsFiles("control");
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/\bMetricsRegistry\b|\bmetrics\.(?:increment|observe|setGauge)\b/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("Rust sidecar startup logs do not expose Redis URLs", () => {
  const files = ["rust/redis-proxy/src/lib.rs", "rust/scheduler/src/lib.rs"];
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    if (/"redis_url"\s*:/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("queue main stream XADD is intentionally unbounded while auxiliary streams are capped", () => {
  const source = readRepoFile("rust/redis-proxy/src/queue.rs");
  const immediateSig = source.match(/fn\s+pipe_immediate_queue_entry\b/);
  assert.ok(immediateSig, "immediate queue command owner must remain findable");
  const immediateCommand = extractBraceBlock(source, immediateSig.index ?? -1);
  assert.ok(immediateCommand, "immediate queue command body must remain findable");
  assert.match(immediateCommand, /pipe\.cmd\("XADD"\)/);
  assert.match(immediateCommand, /\.arg\(entry\.first_seen_ms\);/);
  assert.doesNotMatch(immediateCommand, /MAXLEN/,
    "queue:<ns>:<queue>:s is the durable main stream; do not trim it with MAXLEN");
});

test("supervisor config pins tier divergences (D1 SIGTERM, DO SIGKILL)", () => {
  const cfg = readRepoFile("rust/supervisor/src/config.rs");

  // The two static configs are the source of truth for tier behavior.
  // Slice each one out so cross-tier assertions can't accidentally pick up
  // the wrong tier's value.
  const d1Match = cfg.match(
    /static D1_CONFIG: SupervisorConfig = SupervisorConfig \{[\s\S]*?\n\};/,
  );
  const doMatch = cfg.match(
    /static DO_CONFIG: SupervisorConfig = SupervisorConfig \{[\s\S]*?\n\};/,
  );
  assert.ok(d1Match, "D1_CONFIG static block must exist in config.rs");
  assert.ok(doMatch, "DO_CONFIG static block must exist in config.rs");
  const d1Config = d1Match[0];
  const doConfig = doMatch[0];

  // d1: SIGTERM (workerd graceful), no repeated-signal escalation, errors[]
  // counts as drain failure.
  assert.match(d1Config, /kill_on_drain_success: KillSignal::Term/);
  assert.match(d1Config, /repeated_signal_escalates: false/);
  assert.match(d1Config, /drain_failure_on_errors_field: true/);
  assert.match(d1Config, /drain_request_id_prefix: None/);

  // do: SIGKILL on drain success (skips workerd's 15s graceful 504 window),
  // repeated signal escalates, errors[] is ignored, request id has prefix.
  assert.match(doConfig, /kill_on_drain_success: KillSignal::Kill/);
  assert.match(doConfig, /repeated_signal_escalates: true/);
  assert.match(doConfig, /drain_failure_on_errors_field: false/);
  assert.match(doConfig, /drain_request_id_prefix: Some\("do-drain"\)/);

  // d1 has no local-variant capnp; the same compiled .bin runs in compose
  // dev and production. do reads WDL_WORKERD_CONFIG_VARIANT to switch.
  assert.match(cfg, /D1_COMPILED_CONFIG.*d1-runtime\.bin/);
  assert.equal(/d1-runtime-local\.bin/.test(cfg), false);
  assert.match(cfg, /DO_COMPILED_CONFIG_PRODUCTION.*do-runtime\.bin/);
  assert.match(cfg, /DO_COMPILED_CONFIG_LOCAL.*do-runtime-local\.bin/);
  assert.match(cfg, /WDL_WORKERD_CONFIG_VARIANT/);
});

test("do-runtime operational logs avoid known camelCase field drift", () => {
  const files = [
    "do-runtime/actor.js",
    "do-runtime/index.js",
    "do-runtime/load.js",
    "do-runtime/owner-client.js",
    "do-runtime/owner-registry.js",
  ];
  const badLogField = /(?:log|console\.(?:error|log|warn))\([^;]*\b(ownerKey|workerId|taskId|inFlight|waitedMs|drainWaitMs|upstreamStatus)\s*:/s;
  const offenders = files.filter((file) => badLogField.test(withoutLineComments(readRepoFile(file))));
  assert.deepEqual(offenders, []);
});

test("do-runtime business metrics keep labels low-cardinality", () => {
  const metricCalls = [];
  for (const file of jsFiles("do-runtime")) {
    const source = withoutLineComments(readRepoFile(file));
    for (const call of source.match(/metrics\.(?:increment|observe|setGauge)\([^;]+;/gs) || []) {
      metricCalls.push(`${file}: ${call}`);
    }
  }
  const offenders = [];
  for (const call of metricCalls) {
    if (/\b(?:ns|namespace|worker|workerName|version|className|class_name|objectName|object_name|hostId|host_id|ownerKey|owner_key)\s*:/.test(call)) {
      offenders.push(call);
    }
  }
  assert.deepEqual(offenders, []);
});

test("production metric labels use the bounded label vocabulary", () => {
  const offenders = [];
  for (const file of PRODUCTION_JS_FILES) {
    const source = withoutLineComments(readRepoFile(file));
    for (const call of source.match(/metrics\.(?:increment|observe|setGauge)\([^;]+;/gs) || []) {
      if (
        /\berrMessage\(/.test(call) ||
        /\b(?:reason|code|error|message)\s*:\s*(?:err|error)\b/.test(call) ||
        /\b(?:reason|code|error|message)\s*:[^,}]*\.message\b/.test(call)
      ) {
        offenders.push(`${file}: raw error/message in metric labels: ${call}`);
        continue;
      }
      for (const match of call.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*:/g)) {
        const key = match[1];
        if (!METRIC_ALLOWED_LABEL_KEYS.has(key)) {
          offenders.push(`${file}: unexpected metric label "${key}" in ${call}`);
        }
      }
      if (call.includes("{")) {
        for (const match of call.matchAll(/[{,]\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?=[,}])/g)) {
          const key = match[1];
          if (!METRIC_ALLOWED_LABEL_KEYS.has(key)) {
            offenders.push(`${file}: unexpected metric label "${key}" in ${call}`);
          }
        }
      }
    }
  }
  assert.deepEqual(offenders, [], `metric labels must stay bounded:\n${offenders.join("\n")}`);
});

test("Rust metric labels use the bounded label vocabulary", () => {
  const methodNames = [".increment(", ".observe(", ".add_gauge("];
  const offenders = [];
  for (const file of rustFiles("rust")) {
    const source = withoutLineComments(readRepoFile(file));
    let searchFrom = 0;
    while (searchFrom < source.length) {
      let method = "";
      let start = -1;
      for (const candidate of methodNames) {
        const index = source.indexOf(candidate, searchFrom);
        if (index >= 0 && (start < 0 || index < start)) {
          method = candidate;
          start = index;
        }
      }
      if (start < 0) break;
      const labelsStart = source.indexOf("&[", start);
      const callEnd = source.indexOf(");", start);
      searchFrom = start + method.length;
      if (callEnd < 0 || labelsStart < 0 || labelsStart > callEnd) continue;
      const labelsEnd = source.indexOf("]", labelsStart);
      if (labelsEnd < 0 || labelsEnd > callEnd) continue;
      const labelSlice = source.slice(labelsStart, labelsEnd);
      for (const match of labelSlice.matchAll(/\("([A-Za-z_][A-Za-z0-9_]*)"\s*,/g)) {
        const key = match[1];
        if (!METRIC_ALLOWED_LABEL_KEYS.has(key)) {
          offenders.push(`${file}: unexpected Rust metric label "${key}" near ${method}`);
        }
      }
    }
  }
  assert.deepEqual(offenders, [], `Rust metric labels must stay bounded:\n${offenders.join("\n")}`);
});

test("owner forward metrics classify non-error HTTP statuses consistently", () => {
  const shared = withoutLineComments(readRepoFile("shared/owner-forwarder.js"));
  assert.equal(/\bresponse\.ok\s*\?\s*"ok"\s*:\s*"error"/.test(shared), false);
  assert.match(shared, /export function forwardOutcome\(/);
  for (const file of ["d1-runtime/owner-client.js", "do-runtime/owner-client.js"]) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /from "shared-owner-forwarder"/, file);
    assert.doesNotMatch(source, /function forwardOutcome\(/, file);
  }
});

test("local compose services inherit shared image anchors", () => {
  const source = readRepoFile("docker-compose.yml");
  const offenders = [];
  for (const service of ["redis-proxy-user", "redis-proxy-system", "redis-proxy-do"]) {
    if (!serviceAnchorRegex(service, "redis-proxy-service").test(source)) {
      offenders.push(service);
    }
  }
  if (!serviceAnchorRegex("scheduler", "rust-sidecar-image").test(source)) {
    offenders.push("scheduler");
  }
  for (const service of ["user-runtime", "system-runtime", "gateway"]) {
    if (!serviceAnchorRegex(service, "workerd-image").test(source)) {
      offenders.push(service);
    }
  }
  assert.deepEqual(offenders, []);
});

test("published compose services reset local builds on each service", () => {
  const source = readRepoFile("docker-compose.images.yml");
  const serviceGroups = /** @type {Array<[string, string[]]>} */ ([
    ["rust-published", [
      "redis-proxy-user",
      "redis-proxy-system",
      "redis-proxy-do",
      "scheduler",
      "workflows",
    ]],
    ["workerd-published", [
      "user-runtime",
      "system-runtime",
      "gateway",
      "d1-runtime",
      "d1-runtime-a",
      "d1-runtime-b",
      "d1-runtime-c",
      "do-runtime",
      "do-runtime-a",
      "do-runtime-b",
      "do-runtime-c",
    ]],
  ]);
  const servicesIndex = source.indexOf("services:");
  assert.notEqual(servicesIndex, -1);
  const anchorSource = source.slice(0, servicesIndex);
  assert.doesNotMatch(anchorSource, /build:\s*!reset\b/);
  const expectedServices = serviceGroups.flatMap(([, services]) => services).toSorted();
  const actualServices = [...source.matchAll(/^ {2}([a-z0-9-]+):$/gm)]
    .map((match) => match[1])
    .toSorted();
  assert.deepEqual(actualServices, expectedServices);
  for (const [anchor, services] of serviceGroups) {
    for (const service of services) {
      const block = new RegExp(
        String.raw`(?:^|\n) {2}${RegExp.escape(service)}:\n((?: {4}[^\n]*(?:\n|$))*)`,
      ).exec(source)?.[1];
      assert.ok(block, `${service} must exist in the published-image overlay`);
      assert.match(block, new RegExp(String.raw`^ {4}<<: \*${anchor}$`, "m"), service);
      assert.match(block, /^ {4}build: !reset null$/m, service);
    }
  }
});

test("local compose runtime profiles keep base services enabled by default", () => {
  const [document] = yamlDocuments("docker-compose.yml");
  const compose = /** @type {{ services?: Record<string, { profiles?: unknown }> }} */ (
    document.toJS()
  );
  assert.ok(compose.services);
  for (const family of ["d1", "do"]) {
    const baseName = `${family}-runtime`;
    const baseService = /** @type {{ profiles?: unknown } | undefined} */ (
      compose.services[baseName]
    );
    assert.ok(baseService, `${baseName} must exist`);
    assert.equal(baseService.profiles, undefined);
    for (const suffix of ["a", "b", "c"]) {
      const variantName = `${family}-runtime-${suffix}`;
      const variantService = /** @type {{ profiles?: unknown } | undefined} */ (
        compose.services[variantName]
      );
      assert.ok(variantService, `${variantName} must exist`);
      assert.deepEqual(
        variantService.profiles,
        [`${family}-multi`],
        variantName
      );
    }
  }
});

test("Kubernetes base WDL containers pull published images", () => {
  const files = [
    "deploy/kubernetes/base/gateway.yaml",
    "deploy/kubernetes/base/user-runtime.yaml",
    "deploy/kubernetes/base/system-runtime.yaml",
    "deploy/kubernetes/base/d1-runtime.yaml",
    "deploy/kubernetes/base/do-runtime.yaml",
    "deploy/kubernetes/base/scheduler.yaml",
    "deploy/kubernetes/base/workflows.yaml",
  ];
  const wdlContainerNames = new Set([
    "gateway",
    "redis-proxy",
    "user-runtime",
    "system-runtime",
    "d1-runtime",
    "do-runtime",
    "scheduler",
    "workflows",
  ]);
  for (const file of files) {
    let foundWdlContainer = false;
    const resources = /** @type {Array<Record<string, any>>} */ (
      yamlDocuments(file).map((document) => document.toJS())
    );
    for (const resource of resources) {
      if (resource.kind !== "Deployment" && resource.kind !== "StatefulSet") continue;
      for (const container of resource.spec?.template?.spec?.containers ?? []) {
        if (!wdlContainerNames.has(container.name)) continue;
        foundWdlContainer = true;
        assert.match(container.image, /^docker\.io\/getwdl\/wdl-(?:rust|workerd):latest$/u, file);
        assert.equal(container.imagePullPolicy, "Always", `${file} ${container.name}`);
      }
    }
    assert.ok(foundWdlContainer, `${file} must contain a WDL container`);
  }
});

test("Kubernetes local gateway uses Gateway API with isolated asset routing", () => {
  const [kustomizationDocument] = yamlDocuments(
    "deploy/kubernetes/overlays/local-gateway/kustomization.yaml"
  );
  const kustomization = /** @type {{ resources?: unknown }} */ (
    kustomizationDocument.toJS()
  );
  assert.partialDeepStrictEqual(kustomization, {
    resources: ["../local", "assets-proxy.yaml", "gateway.yaml", "gateway-network-policy.yaml"],
  });

  const resources = /** @type {Array<Record<string, any>>} */ (
    yamlDocuments("deploy/kubernetes/overlays/local-gateway/gateway.yaml")
      .map((document) => document.toJS())
  );
  const gateway = resources.find((resource) => resource.kind === "Gateway");
  assert.partialDeepStrictEqual(gateway, {
    apiVersion: "gateway.networking.k8s.io/v1",
    metadata: { name: "wdl" },
    spec: {
      gatewayClassName: "nginx",
      listeners: [{
        name: "http",
        protocol: "HTTP",
        port: 80,
        allowedRoutes: { namespaces: { from: "Same" } },
      }],
    },
  });

  const routeEdges = resources
    .filter((resource) => resource.kind === "HTTPRoute")
    .flatMap((route) => {
      const routeName = route.metadata?.name;
      const hostnames = /** @type {string[]} */ (route.spec?.hostnames ?? ["*"]);
      const rules = /** @type {Array<Record<string, any>>} */ (route.spec?.rules ?? []);
      return hostnames.flatMap((hostname) =>
        rules.flatMap((rule, ruleIndex) => {
          const backends = /** @type {Array<Record<string, any>>} */ (
            rule.backendRefs ?? []
          );
          const matches = /** @type {Array<Record<string, any>>} */ (rule.matches ?? [{}]);
          assert.ok(backends.length > 0, `${routeName} rule ${ruleIndex} must have a backend`);
          return matches.flatMap((match) =>
            backends.map((backend) => [
              routeName,
              hostname,
              `${match.path?.type ?? "PathPrefix"}:${match.path?.value ?? "/"}`,
              `${backend.group ?? "core"}:${backend.kind ?? "Service"}:` +
                `${backend.namespace ?? "wdl-local"}:${backend.name}:${backend.port}`,
            ].join("|"))
          );
        })
      );
    })
    .toSorted();
  assert.deepEqual(routeEdges, [
    "wdl-assets|s3mock.local|PathPrefix:/wdl-assets/assets|core:Service:wdl-local:assets-proxy:8080",
    "wdl-gateway|*.workers.local|PathPrefix:/|core:Service:wdl-local:gateway:8080",
    "wdl-gateway|admin.test|PathPrefix:/|core:Service:wdl-local:gateway:8080",
  ].toSorted());

  const policy = resources.find((resource) => resource.kind === "ProxySettingsPolicy");
  assert.partialDeepStrictEqual(policy, {
    spec: {
      targetRefs: [{
        group: "gateway.networking.k8s.io",
        kind: "HTTPRoute",
        name: "wdl-gateway",
      }],
      timeout: { read: "3600s", send: "3600s" },
    },
  });

  const networkPolicies = /** @type {Array<Record<string, any>>} */ (
    yamlDocuments("deploy/kubernetes/overlays/local-gateway/gateway-network-policy.yaml")
      .map((document) => document.toJS())
  );
  /** @param {Record<string, any>} selector @param {string} context */
  const selectorKey = (selector, context) => {
    assert.deepEqual(Object.keys(selector).toSorted(), ["matchLabels"], `${context} selector`);
    return Object.entries(selector.matchLabels)
      .map(([key, value]) => `${key}=${value}`)
      .toSorted()
      .join(",");
  };
  const ingressEdges = networkPolicies.flatMap((policy) => {
    const receiver = selectorKey(policy.spec?.podSelector ?? {}, policy.metadata?.name);
    const ingressRules = /** @type {Array<Record<string, any>>} */ (
      policy.spec?.ingress ?? []
    );
    return ingressRules.flatMap((ingress, ingressIndex) => {
      const context = `${policy.metadata?.name} ingress ${ingressIndex}`;
      const peers = /** @type {Array<Record<string, any>>} */ (ingress.from ?? []);
      const callers = ingress.from === undefined
        ? ["*"]
        : peers.map((peer) => {
          assert.deepEqual(Object.keys(peer).toSorted(), ["podSelector"], `${context} peer`);
          return selectorKey(peer.podSelector, `${context} peer`);
        });
      const networkPorts = /** @type {Array<Record<string, any>>} */ (ingress.ports ?? []);
      const ports = ingress.ports === undefined
        ? ["*"]
        : networkPorts.map((entry) => {
          const range = entry.endPort === undefined ? entry.port : `${entry.port}-${entry.endPort}`;
          return `${entry.protocol ?? "TCP"}:${range}`;
        });
      return callers.flatMap((caller) =>
        ports.map((port) => `${caller}->${receiver}@${port}`)
      );
    });
  }).toSorted();
  assert.deepEqual(ingressEdges, [
    "*->gateway.networking.k8s.io/gateway-name=wdl@TCP:80",
    "app.kubernetes.io/component=assets-proxy,app.kubernetes.io/name=wdl->" +
      "app.kubernetes.io/component=s3mock,app.kubernetes.io/name=wdl@TCP:9090",
    "gateway.networking.k8s.io/gateway-name=wdl->" +
      "app.kubernetes.io/component=assets-proxy,app.kubernetes.io/name=wdl@TCP:8080",
  ].toSorted());
});

test("Kubernetes stateful runtimes publish Pod-specific owner endpoints", () => {
  const families = /** @type {Array<[string, number]>} */ ([["d1", 8787], ["do", 8788]]);
  for (const [family, port] of families) {
    const serviceName = `${family}-runtime`;
    const headlessName = `${serviceName}-headless`;
    const resources = /** @type {Array<Record<string, any>>} */ (
      yamlDocuments(`deploy/kubernetes/base/${serviceName}.yaml`).map((document) => document.toJS())
    );
    const router = resources.find((resource) => resource.kind === "Service" && resource.metadata?.name === serviceName);
    const headless = resources.find((resource) => resource.kind === "Service" && resource.metadata?.name === headlessName);
    const statefulSet = resources.find((resource) => resource.kind === "StatefulSet" && resource.metadata?.name === serviceName);
    assert.ok(router, `${serviceName} router Service must exist`);
    assert.notEqual(router.spec?.clusterIP, "None", `${serviceName} router must stay load-balanced`);
    assert.equal(headless?.spec?.clusterIP, "None", `${headlessName} must stay headless`);
    assert.equal(statefulSet?.spec?.serviceName, headlessName, `${serviceName} StatefulSet serviceName`);

    const container = statefulSet?.spec?.template?.spec?.containers
      ?.find((/** @type {Record<string, any>} */ entry) => entry.name === serviceName);
    assert.ok(container, `${serviceName} container must exist`);
    const env = Object.fromEntries(container.env.map(
      (/** @type {Record<string, any>} */ entry) => [entry.name, entry]
    ));
    assert.equal(env.POD_NAME?.valueFrom?.fieldRef?.fieldPath, "metadata.name");
    assert.equal(env[`${family.toUpperCase()}_TASK_ID`]?.valueFrom?.fieldRef?.fieldPath, "metadata.name");
    assert.equal(
      env[`${family.toUpperCase()}_TASK_ENDPOINT`]?.value,
      `$(POD_NAME).${headlessName}:${port}`
    );
  }
});

test("Kubernetes runtime families pin the redis-proxy functional contract", () => {
  const files = [
    "deploy/kubernetes/base/user-runtime.yaml",
    "deploy/kubernetes/base/system-runtime.yaml",
    "deploy/kubernetes/base/do-runtime.yaml",
  ];
  const sidecars = files.map((file) => {
    const resources = /** @type {Array<{
     *   kind?: string,
     *   spec?: { template?: { spec?: { containers?: Array<Record<string, unknown>> } } },
     * }>} */ (yamlDocuments(file).map((document) => document.toJS()));
    const workload = resources.find((resource) =>
      resource.kind === "Deployment" || resource.kind === "StatefulSet"
    );
    assert.ok(workload, `${file} must contain a Deployment or StatefulSet`);
    const sidecar = workload.spec?.template?.spec?.containers
      ?.find((container) => container.name === "redis-proxy");
    assert.ok(sidecar, `${file} must contain the redis-proxy sidecar`);
    return sidecar;
  });

  for (let index = 0; index < sidecars.length; index += 1) {
    const sidecar = sidecars[index];
    assert.equal(sidecar.image, "docker.io/getwdl/wdl-rust:latest", `${files[index]} image`);
    assert.deepEqual(sidecar.command, ["/redis-proxy"], `${files[index]} command`);
    assert.deepEqual(
      /** @type {Array<Record<string, unknown>> | undefined} */ (sidecar.ports)
        ?.find((port) => port.name === "redis-proxy"),
      { name: "redis-proxy", containerPort: 7070 },
      `${files[index]} port`
    );
    assert.deepEqual(sidecar.envFrom, [
      { configMapRef: { name: "wdl-config" } },
      { secretRef: { name: "wdl-secrets" } },
    ], `${files[index]} envFrom`);
    for (const probe of ["readinessProbe", "livenessProbe"]) {
      assert.deepEqual(sidecar[probe], {
        exec: { command: ["/redis-proxy", "healthcheck"] },
      }, `${files[index]} ${probe}`);
    }
    const resources = /** @type {{ requests?: Record<string, unknown>, limits?: Record<string, unknown> } | undefined} */ (
      sidecar.resources
    );
    assert.ok(resources?.requests?.cpu, `${files[index]} resource request cpu`);
    assert.ok(resources?.requests?.memory, `${files[index]} resource request memory`);
    assert.ok(resources?.limits?.memory, `${files[index]} resource limit memory`);
  }
});

test("Kubernetes NetworkPolicies pin the per-component ingress matrix", () => {
  const documents = yamlDocuments("deploy/kubernetes/base/network-policy.yaml");
  /** @type {Record<string, string[]>} */
  const actual = {};
  for (const document of documents) {
    const policy = /** @type {{
     *   spec?: {
     *     podSelector?: { matchLabels?: Record<string, unknown> },
     *     ingress?: Array<{
     *       from?: Array<{ podSelector?: { matchLabels?: Record<string, unknown> } }>,
     *       ports?: Array<{ port?: unknown }>,
     *     }>,
     *   },
     * }} */ (document.toJS());
    const receiver = policy?.spec?.podSelector?.matchLabels?.["app.kubernetes.io/component"];
    if (typeof receiver !== "string") continue;
    assert.equal(actual[receiver], undefined, `duplicate NetworkPolicy receiver ${receiver}`);

    const edges = [];
    for (const ingress of policy.spec?.ingress || []) {
      const rawCallers = ingress.from === undefined
        ? ["*"]
        : ingress.from.map((peer) => peer?.podSelector?.matchLabels?.["app.kubernetes.io/component"]);
      const ports = (ingress.ports || []).map((entry) => Number(entry.port));
      assert.ok(rawCallers.every((caller) => typeof caller === "string"), `${receiver} callers must use component selectors`);
      assert.ok(ports.every(Number.isInteger), `${receiver} ingress ports must be integers`);
      const callers = /** @type {string[]} */ (rawCallers);
      for (const caller of callers) {
        for (const port of ports) edges.push(`${caller}:${port}`);
      }
    }
    actual[receiver] = edges.toSorted();
  }

  assert.deepEqual(actual, {
    gateway: ["*:8080"],
    "user-runtime": ["gateway:8081", "scheduler:8088", "workflows:8088"],
    "system-runtime": ["gateway:8081", "gateway:8082", "scheduler:8088", "workflows:8088"],
    "d1-runtime": ["d1-runtime:8787", "do-runtime:8787", "system-runtime:8787", "user-runtime:8787"],
    "do-runtime": ["do-runtime:8788", "system-runtime:8788", "user-runtime:8788", "workflows:8788"],
    workflows: ["do-runtime:9120", "scheduler:9120", "system-runtime:9120", "user-runtime:9120"],
    valkey: [
      "d1-runtime:6379",
      "do-runtime:6379",
      "gateway:6379",
      "scheduler:6379",
      "system-runtime:6379",
      "user-runtime:6379",
      "workflows:6379",
    ],
    s3mock: ["do-runtime:9090", "system-runtime:9090", "user-runtime:9090"],
  });
});

test("local Compose routes private HTTP hops through Envoy", () => {
  const compose = withoutLineComments(readRepoFile("docker-compose.yml"));
  const [composeDocument] = yamlDocuments("docker-compose.yml");
  const composeConfig = /** @type {{ services?: Record<string, any> }} */ (
    composeDocument.toJS()
  );
  const envoy = withoutLineComments(readRepoFile("envoy/envoy.yaml"));
  const gatewayLocal = withoutLineComments(readRepoFile("gateway/config-local.capnp"));
  const runtimeUserLocal = withoutLineComments(readRepoFile("runtime/config-user-local.capnp"));
  const runtimeSystemLocal = withoutLineComments(readRepoFile("runtime/config-system-local.capnp"));
  const doRuntimeLocal = withoutLineComments(readRepoFile("do-runtime/config-local.capnp"));

  assert.match(compose, /\n {2}envoy:\n\s+image: envoyproxy\/envoy:/);
  assert.match(compose, /RUNTIME_HOST: envoy/);
  assert.match(compose, /RUNTIME_PORT: "18088"/);
  assert.match(compose, /SYSTEM_RUNTIME_HOST: envoy/);
  assert.match(compose, /SYSTEM_RUNTIME_PORT: "18089"/);
  assert.match(compose, /REDIS_URL: redis:\/\/redis:6379/);
  assert.match(compose, /REDIS_ADDR: redis:6379/);
  assert.match(compose, /WDL_WORKERD_CONFIG_VARIANT: local/);
  const envoyHealthcheck = composeConfig.services?.envoy?.healthcheck;
  assert.ok(Array.isArray(envoyHealthcheck?.test), "Envoy healthcheck command must exist");
  assert.ok(
    envoyHealthcheck.test.some((/** @type {unknown} */ part) =>
      typeof part === "string" && part.includes("GET /ready")
    ),
    "Envoy healthcheck must probe /ready",
  );
  for (const serviceName of ["gateway", "scheduler", "system-runtime", "user-runtime"]) {
    assert.equal(
      composeConfig.services?.[serviceName]?.depends_on?.envoy?.condition,
      "service_healthy",
      `${serviceName} must wait for healthy Envoy`,
    );
  }

  assert.match(gatewayLocal, /address = "envoy:18081"/);
  assert.match(gatewayLocal, /address = "envoy:18082"/);
  assert.match(gatewayLocal, /address = "envoy:18083"/);
  assert.match(runtimeUserLocal, /address = "\*:8088"/);
  assert.match(runtimeSystemLocal, /address = "\*:8088"/);
  assert.match(runtimeUserLocal, /address = "envoy:18787"/);
  assert.match(runtimeSystemLocal, /address = "envoy:18787"/);
  assert.match(doRuntimeLocal, /address = "envoy:18787"/);

  assert.match(envoy, /\badmin:\n\s+address:/);
  for (const port of ["18081", "18082", "18083", "18088", "18089", "18787", "18788"]) {
    assert.match(envoy, new RegExp(`port_value: ${RegExp.escape(port)}\\b`));
  }
  for (const upstream of [
    "socket_address: { address: user-runtime, port_value: 8081 }",
    "socket_address: { address: system-runtime, port_value: 8081 }",
    "socket_address: { address: user-runtime, port_value: 8088 }",
    "socket_address: { address: system-runtime, port_value: 8088 }",
    "socket_address: { address: system-runtime, port_value: 8082 }",
    "socket_address: { address: d1-runtime, port_value: 8787 }",
    "socket_address: { address: do-runtime-router, port_value: 8788 }",
  ]) {
    assert.match(envoy, new RegExp(RegExp.escape(upstream)));
  }
  // Only the first-hop router is Envoy-backed. Learned owner endpoints stay
  // task-specific so the lease and generation fence reaches the named owner.
  for (const service of ["d1-runtime-a", "d1-runtime-b", "d1-runtime-c"]) {
    assert.match(compose, new RegExp(`D1_TASK_ENDPOINT: ${RegExp.escape(service)}:8787`));
    assert.equal(envoy.includes(`address: ${service}, port_value: 8787`), false);
  }
  for (const service of ["do-runtime-a", "do-runtime-b", "do-runtime-c"]) {
    assert.match(compose, new RegExp(`DO_TASK_ENDPOINT: ${RegExp.escape(service)}:8788`));
    assert.equal(envoy.includes(`address: ${service}, port_value: 8788`), false);
  }
  for (const listener of [
    "user_runtime",
    "system_runtime_loader",
    "system_runtime_control",
    "d1_router",
    "do_router",
  ]) {
    assert.match(
      envoy,
      new RegExp(`name: ${RegExp.escape(listener)}[\\s\\S]*?preserve_external_request_id: true`)
    );
  }
});

test("workerd experimental process access stays limited to workerLoader tiers", () => {
  const compose = withoutLineComments(readRepoFile("docker-compose.yml"));
  const kubeGateway = withoutLineComments(readRepoFile("deploy/kubernetes/base/gateway.yaml"));
  const kubeSystemRuntime = withoutLineComments(readRepoFile("deploy/kubernetes/base/system-runtime.yaml"));
  const kubeUserRuntime = withoutLineComments(readRepoFile("deploy/kubernetes/base/user-runtime.yaml"));
  const terraformGateway = withoutLineComments(readRepoFile("terraform/modules/compute/gateway_service.tf"));
  const terraformRuntime = withoutLineComments(readRepoFile("terraform/modules/compute/runtime_service.tf"));
  const terraformSystemRuntime = withoutLineComments(readRepoFile("terraform/modules/compute/system_runtime_service.tf"));
  const supervisorConfig = readRepoFile("rust/supervisor/src/config.rs");
  const supervisorLib = readRepoFile("rust/supervisor/src/lib.rs");

  for (const tier of ["user-runtime-local", "system-runtime-local"]) {
    assert.match(
      compose,
      new RegExp(`workerd-configs/${RegExp.escape(tier)}\\.bin", "--experimental"`),
    );
  }
  assert.doesNotMatch(compose, /gateway-local\.bin", "--experimental"/);
  assert.match(kubeUserRuntime, /user-runtime\.bin[\s\S]*?- --experimental/);
  assert.match(kubeSystemRuntime, /system-runtime\.bin[\s\S]*?- --experimental/);
  assert.doesNotMatch(kubeGateway, /--experimental/);
  assert.match(terraformRuntime, /user-runtime\.bin", "--experimental"/);
  assert.match(terraformSystemRuntime, /system-runtime\.bin", "--experimental"/);
  assert.doesNotMatch(terraformGateway, /--experimental/);
  assert.match(supervisorLib, /workerd_args\(D1_COMPILED_CONFIG, false\)/);
  assert.match(supervisorLib, /workerd_args\(pick_do_compiled_config\(\), true\)/);
  assert.match(supervisorConfig, /args\.push\("--experimental"\.into\(\)\)/);

  for (const file of [
    "gateway/config.capnp",
    "gateway/config-local.capnp",
    "runtime/config-user.capnp",
    "runtime/config-user-local.capnp",
    "runtime/config-system.capnp",
    "runtime/config-system-local.capnp",
    "d1-runtime/config.capnp",
    "do-runtime/config.capnp",
    "do-runtime/config-local.capnp",
  ]) {
    const source = readRepoFile(file);
    assert.doesNotMatch(source, /compatibilityFlags\s*=\s*\[[^\]]*"experimental"/, file);
    assert.doesNotMatch(source, /allow_irrevocable_stub_storage/, file);
  }
});

test("S3 query encoding stays aligned between shared and injected runtime helpers", () => {
  const extract = (/** @type {string} */ file) => {
    const source = readRepoFile(file);
    const match = source.match(
      /function encodeS3QueryComponent[\s\S]*?export function encodeS3Query\(params\) \{[\s\S]*?\n\}/
    );
    assert.ok(match, `${file} must expose encodeS3Query next to encodeS3QueryComponent`);
    return match[0].replace(JSDOC_BLOCK_RE, "").replace(/\s+/g, " ").trim();
  };

  assert.equal(extract("shared/s3-query.js"), extract("runtime/r2-utils.js"));
});

test("tenant worker egress stays public-only at the workerd boundary", () => {
  const user = withoutLineComments(readRepoFile("runtime/config-user.capnp"));
  const userLocal = withoutLineComments(readRepoFile("runtime/config-user-local.capnp"));
  const system = withoutLineComments(readRepoFile("runtime/config-system.capnp"));
  const doRuntime = withoutLineComments(readRepoFile("do-runtime/config.capnp"));
  const doRuntimeLocal = withoutLineComments(readRepoFile("do-runtime/config-local.capnp"));
  const runtimeLoad = withoutLineComments(readRepoFile("runtime/load.js"));
  const doLoad = withoutLineComments(readRepoFile("do-runtime/load.js"));

  const networkBlock = (/** @type {string} */ source, /** @type {string} */ serviceName) => {
    // Match the repository's canonical multiline Cap'n Proto network tuple.
    const match = source.match(new RegExp(
      `\\(name = "${serviceName}", network = \\((?:\\n\\s+.*)*\\n\\s*\\)\\)`
    ));
    assert.ok(match, `${serviceName} network block must exist`);
    return match[0];
  };

  for (const [file, source] of [
    ["runtime/config-user.capnp", user],
    ["runtime/config-user-local.capnp", userLocal],
    ["do-runtime/config.capnp", doRuntime],
    ["do-runtime/config-local.capnp", doRuntimeLocal],
  ]) {
    const publicNetwork = networkBlock(source, "public-network");
    assert.match(publicNetwork, /allow = \["public"\]/, `${file} public-network must be public-only`);
    assert.doesNotMatch(publicNetwork, /"private"/, `${file} public-network must not allow private egress`);
  }

  assert.match(
    networkBlock(system, "network"),
    /allow = \["private", "public"\]/,
    "system-runtime loaded workers intentionally keep private mesh reach"
  );
  assert.match(runtimeLoad, /globalOutbound:\s*env\.PUBLIC_NETWORK/);
  assert.match(doLoad, /globalOutbound:\s*env\.PUBLIC_NETWORK/);
});

test("Valkey 9 is the local and Terraform baseline when HFE commands are used", () => {
  const compose = withoutLineComments(readRepoFile("docker-compose.yml"));
  const valkeyTf = withoutLineComments(readRepoFile("terraform/modules/data/valkey.tf"));
  const tail = withoutLineComments(readRepoFile("control/handlers/logs-tail.js"));

  assert.match(
    tail,
    /redis\.call\("HSETEX"/,
    "tail activation renews and admits active fields atomically with Valkey HSETEX"
  );
  assert.match(compose, /image: valkey\/valkey:9\.1-alpine\b/);
  assert.match(valkeyTf, /engine_version = "9\.1"/);
  assert.match(valkeyTf, /parameter_group_name = "default\.valkey9"/);
});

test("log-tail activation hash key stays aligned across control and proxy", () => {
  const controlTail = withoutLineComments(readRepoFile("control/handlers/logs-tail.js"));
  const proxyLogs = withoutLineComments(readRepoFile("rust/redis-proxy/src/logs.rs"));
  assert.match(controlTail, /const TAIL_ACTIVATION_CHANNEL = "logs:tail:active";/);
  assert.match(proxyLogs, /const TAIL_ACTIVATION_KEY: &str = "logs:tail:active";/);
});

test("Valkey logical DB routing stays explicit across Rust data paths", () => {
  const cron = withoutLineComments(readRepoFile("rust/scheduler/src/cron.rs"));
  assert.doesNotMatch(cron, /data_redis/, "cron state is control-plane state and must stay on DB 0");

  const queueDataFiles = [
    "rust/scheduler/src/queue/consume.rs",
    "rust/scheduler/src/queue/delayed.rs",
    "rust/scheduler/src/queue/delivery/dispatch.rs",
    "rust/scheduler/src/queue/delivery/retry.rs",
    "rust/scheduler/src/queue/orphan.rs",
  ];
  for (const file of queueDataFiles) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /data_redis/, `${file} must use the data Redis client for queue streams`);
    assert.doesNotMatch(source, /\bstate\.redis\b/, `${file} must not write queue data to control Redis`);
  }

  const runtimeLoad = withoutLineComments(readRepoFile("rust/redis-proxy/src/runtime.rs"));
  assert.match(runtimeLoad, /with_control_redis/, "runtime-load reads bundles and secrets from DB 0");
});

test("workflow instance state is owned by workflows DB2", () => {
  const workflowPrefixes = [
    "wf:schema_version",
    "wf:instance:",
    "wf:ready:",
    "wf:due:",
    "wf:by-worker:",
    "wf:by-workflow:",
    "wf:by-version:",
    "wf:pending-version:",
    "wf:retention",
  ];
  const allowed = new Set([
    "tests/integration/helpers/workflows-scenarios.js",
    "tests/integration/workflows-runtime-core.test.js",
    "tests/integration/workflows-runtime-scheduler.test.js",
    "tests/integration/workflows-runtime-pausing.test.js",
    "tests/unit/style-contracts.test.js",
    "rust/workflows/src/keys.rs",
    "rust/workflows/src/schema.rs",
    "rust/workflows/src/tests.rs",
  ]);
  const allowedFilePrefixes = new Set([
    // resetStack() FLUSHALLs integration Redis and re-seeds the DB2 schema
    // marker so workflows restart tests keep the greenfield schema contract.
    "tests/integration/helpers/stack.js:wf:schema_version",
  ]);
  // Docs are markdown material and intentionally excluded; this guard
  // protects executable tiers and tests that might accidentally read/write DB2
  // workflow keys.
  const files = [
    ...jsFiles("control"),
    ...jsFiles("runtime"),
    ...jsFiles("gateway"),
    ...jsFiles("auth"),
    ...jsFiles("d1-runtime"),
    ...jsFiles("do-runtime"),
    ...jsFiles("tests/unit"),
    ...jsFiles("tests/integration"),
    ...rustFiles("rust"),
  ];
  const offenders = [];
  for (const file of files) {
    if (allowed.has(file)) continue;
    const source = withoutLineComments(readRepoFile(file));
    for (const prefix of workflowPrefixes) {
      const match = `${file}:${prefix}`;
      if (source.includes(prefix) && !allowedFilePrefixes.has(match)) offenders.push(match);
    }
  }
  assert.deepEqual(offenders, []);
});

test("host wrapper hides raw exports whenever internal Fetchers are injected", () => {
  const source = withoutLineComments(readRepoFile("runtime/load/wrapper-generate.js"));
  const wrapper = source.slice(
    source.indexOf("function generateHostBindingWrapperModule"),
    source.length
  );
  assert.match(wrapper, /const hidesRawEnvExports = doBindings\.length \|\| Object\.keys\(workflowBindings\)\.length;/);
  assert.match(wrapper, /const starExport = hidesRawEnvExports\s*\?\s*"[^"]*only wrapped entrypoints are re-exported\."/);

  const wrapEnv = source.slice(
    source.indexOf("function wrapEnv"),
    source.indexOf("async function notifyWorkflowCallback")
  );
  for (const binding of [
    "DO_BACKEND_BINDING",
    "DO_OWNER_NETWORK_BINDING",
    "WORKFLOWS_BACKEND_BINDING",
  ]) {
    assert.match(wrapEnv, new RegExp(`delete out\\[${RegExp.escape(binding)}\\]`));
  }
});

test("queue discovery index literals stay aligned across scheduler, proxy, and control", () => {
  const shared = readRepoFile("shared/queue-keys.js");
  const common = readRepoFile("rust/common/src/queue_keys.rs");
  const scheduler = readRepoFile("rust/scheduler/src/queue/keys.rs");
  const proxy = readRepoFile("rust/redis-proxy/src/queue.rs");
  for (const [literal, rustName] of [
    ["queue:index:consumers", "QUEUE_CONSUMER_INDEX_KEY"],
    ["queue:index:streams", "QUEUE_STREAM_INDEX_KEY"],
    ["queue:index:delayed", "QUEUE_DELAYED_INDEX_KEY"],
  ]) {
    assert.ok(shared.includes(literal), `shared queue keys must contain ${literal}`);
    assert.ok(common.includes(literal), `Rust common queue keys must contain ${literal}`);
    assert.ok(scheduler.includes(rustName), `scheduler must import ${rustName}`);
    if (rustName !== "QUEUE_CONSUMER_INDEX_KEY") {
      assert.ok(proxy.includes(rustName), `redis proxy queue producer must import ${rustName}`);
    }
  }
  assert.match(scheduler, /wdl_rust_common::queue_keys/);
  assert.match(proxy, /wdl_rust_common::queue_keys/);
  assert.ok(shared.includes('QUEUE_DELAYED_WAKE_STREAM = "queue-delayed-wake"'));
  assert.ok(common.includes('QUEUE_DELAYED_WAKE_STREAM: &str = "queue-delayed-wake"'));
});

test("cron discovery index literals stay aligned across scheduler and control", () => {
  const control = readRepoFile("control/lifecycle-indexes.js");
  const routing = readRepoFile("control/routing.js");
  const scheduler = readRepoFile("rust/scheduler/src/cron/sweep.rs");
  const integration = readRepoFile("tests/integration/cron-triggers.test.js");
  const workerIndex = "cron:index:workers";
  assert.ok(control.includes(workerIndex), `control cron indexes must contain ${workerIndex}`);
  assert.ok(scheduler.includes(workerIndex), `scheduler cron indexes must contain ${workerIndex}`);
  assert.ok(integration.includes(workerIndex), `cron integration tests must contain ${workerIndex}`);
  assert.ok(routing.includes("stageCronProjection"), "control routing should stage the scheduler projection through its owner");
  assert.equal(/`crons:\$\{/.test(routing), false, "control routing should not hand-roll cron hash keys");
  assert.equal(/`cron-slot:\$\{/.test(routing), false, "control routing should not hand-roll cron slot keys");

  const backfilledMarker = "cron:index:workers:backfilled";
  assert.ok(scheduler.includes(backfilledMarker), `scheduler cron indexes must contain ${backfilledMarker}`);
  assert.ok(integration.includes(backfilledMarker), `cron integration tests must contain ${backfilledMarker}`);
});

test("KV bucket hash constants stay aligned across Rust and integration helpers", () => {
  const common = readRepoFile("rust/common/src/hash.rs");
  const kv = readRepoFile("rust/redis-proxy/src/kv.rs");
  const integration = readRepoFile("tests/integration/kv-binding.test.js");
  const fnv = readRepoFile("shared/fnv1a32.js");
  assert.ok(common.includes("0x811c9dc5"), "Rust common hash helper must contain FNV offset basis");
  assert.ok(common.includes("0x01000193"), "Rust common hash helper must contain FNV prime");
  assert.ok(fnv.includes("2166136261"), "JS shared FNV helper must contain FNV offset basis");
  assert.ok(fnv.includes("16777619"), "JS shared FNV helper must contain FNV prime");
  assert.match(kv, /const KV_HASH_BUCKETS: u32 = 32;/);
  assert.match(kv, /wdl_rust_common::hash::fnv1a32/);
  assert.match(integration, /fnv1a32Utf8\(key\) % 32/);
});

test("workflow ready shard constants stay aligned across Rust and integration helpers", () => {
  const keys = readRepoFile("rust/workflows/src/keys.rs");
  const limits = readRepoFile("rust/workflows/src/api/limits.rs");
  const routing = readRepoFile("rust/workflows/src/api/routing.rs");
  const integration = readRepoFile("tests/integration/helpers/workflows-scenarios.js");
  assert.match(keys, /pub\(crate\) const WORKFLOW_READY_SHARDS: usize = 32;/);
  assert.match(limits, /pub\(crate\) const READY_SHARDS: usize = crate::WORKFLOW_READY_SHARDS;/);
  assert.match(routing, /% READY_SHARDS/);
  assert.match(integration, /fnv1a32Utf8\(`\$\{ns}:\$\{workflowKey}:\$\{instanceId}`\) % 32/);
});

test("DO alarm internal ready shard constants stay aligned across Rust and integration helpers", () => {
  const keys = readRepoFile("rust/workflows/src/keys.rs");
  const integration = readRepoFile("tests/integration/helpers/durable-objects.js");
  assert.match(keys, /pub\(crate\) const DO_ALARM_READY_SHARDS: usize = 32;/);
  assert.match(keys, /% DO_ALARM_READY_SHARDS/);
  assert.match(integration, /const DO_ALARM_READY_SHARDS = 32;/);
  assert.match(integration, /fnv1a32CodeUnits\(jobId\) % DO_ALARM_READY_SHARDS/);
});

test("KV hash field prefixes stay aligned across proxy and integration seeds", () => {
  const kv = readRepoFile("rust/redis-proxy/src/kv.rs");
  const integration = readRepoFile("tests/integration/kv-binding.test.js");
  assert.match(kv, /const VALUE_FIELD_PREFIX: &str = "v:";/);
  assert.match(kv, /const META_FIELD_PREFIX: &str = "m:";/);
  assert.match(integration, /fields\.push\(`v:\$\{key\}`, value\)/);
});

test("S3 cleanup lifecycle literals stay in shared lifecycle helper", () => {
  const files = [
    "control/shared.js",
    "system-workers/s3-cleanup/src/index.js",
    "tests/integration/s3-cleanup.test.js",
    "tests/integration/delete-api.test.js",
  ];
  const offenders = [];
  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    // Strict by design: control writes cleanup intents, the system worker
    // persists task state, and integration tests seed/assert the same
    // lifecycle. Queue/table names must not drift across those surfaces.
    if (/"worker-delete-s3-cleanup"|"s3_cleanup_task"/.test(source)) {
      offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

test("D1 and DO workerd env tunables are exposed through capnp bindings", () => {
  const d1 = withoutLineComments(readRepoFile("d1-runtime/config.capnp"));
  const doRuntime = withoutLineComments(readRepoFile("do-runtime/config.capnp"));
  const exposed = (/** @type {string} */ source, /** @type {string} */ name) => new RegExp(
    `\\(name = "${name}", fromEnvironment = "${name}"\\)`
  ).test(source);

  for (const name of [
    "D1_OWNER_TTL_SECONDS",
    "D1_PROBE_TIMEOUT_MS",
    "D1_QUERY_TIMEOUT_MS",
    "D1_MAX_RESULT_ROWS",
    "D1_MAX_RESULT_BYTES",
    "D1_ACTOR_IDLE_WAIT_TIMEOUT_MS",
    "D1_DRAIN_TIMEOUT_MS",
    "D1_RENEW_CONCURRENCY",
    "D1_DRAIN_CONCURRENCY",
    "D1_OBSERVED_OWNER_TTL_MS",
    "D1_OBSERVED_OWNER_MAX_ENTRIES",
    "D1_READ_CACHE_TTL_MS",
    "D1_READ_CACHE_MAX_ENTRIES",
    "D1_TEST_HOOKS",
    "D1_OWNER_LEASE_GUARD_MS",
  ]) {
    assert.equal(exposed(d1, name), true, `${name} must reach d1-runtime workerd env`);
  }

  for (const name of [
    "DO_TASK_IDENTITY_TIMEOUT_MS",
    "DO_OWNER_TTL_SECONDS",
    "DO_OWNER_LEASE_GUARD_MS",
    "DO_RENEW_CONCURRENCY",
    "DO_DRAIN_IN_FLIGHT_TIMEOUT_MS",
  ]) {
    assert.equal(exposed(doRuntime, name), true, `${name} must reach do-runtime workerd env`);
  }
});

test("D1 and DO deployments keep explicit runtime memory ceilings", () => {
  const rootVars = readRepoFile("terraform/variables.tf");
  const moduleVars = readRepoFile("terraform/modules/compute/variables.tf");
  const main = readRepoFile("terraform/main.tf");
  const locals = readRepoFile("terraform/modules/compute/locals.tf");
  const d1Service = readRepoFile("terraform/modules/compute/d1_runtime_service.tf");
  const doService = readRepoFile("terraform/modules/compute/do_runtime_service.tf");

  for (const name of ["d1_runtime_container_memory", "do_runtime_container_memory"]) {
    assert.match(rootVars, new RegExp(`variable "${name}"`));
    assert.match(moduleVars, new RegExp(`variable "${name}"`));
    assert.match(main, new RegExp(`${name}\\s+=\\s+var\\.${name}`));
  }
  assert.match(locals, /stateful_runtime_memory_headroom\s+=\s+128/);
  assert.match(
    locals,
    /d1_runtime_container_memory\s+=\s+coalesce\(\s*var\.d1_runtime_container_memory,\s*var\.runtime_memory - local\.stateful_runtime_memory_headroom,\s*\)/,
  );

  assert.match(d1Service, /memory\s+=\s+local\.d1_runtime_container_memory/);
  assert.match(
    d1Service,
    /local\.d1_runtime_container_memory > 0 &&\s*local\.d1_runtime_container_memory <= var\.runtime_memory - local\.stateful_runtime_memory_headroom/,
  );
  assert.match(locals, /redis_proxy_memory_reservation\s+=\s+64/);
  assert.match(
    locals,
    /do_runtime_container_memory\s+=\s+coalesce\(\s*var\.do_runtime_container_memory,\s*var\.runtime_memory - local\.redis_proxy_memory_reservation - local\.stateful_runtime_memory_headroom,\s*\)/,
  );
  assert.match(doService, /memoryReservation\s+=\s+local\.redis_proxy_memory_reservation/);
  assert.match(doService, /memory\s+=\s+local\.do_runtime_container_memory/);
  assert.match(
    doService,
    /local\.do_runtime_container_memory > 0 &&\s*local\.do_runtime_container_memory <= var\.runtime_memory - local\.redis_proxy_memory_reservation - local\.stateful_runtime_memory_headroom/,
  );

  for (const serviceName of ["d1-runtime", "do-runtime"]) {
    const resources = /** @type {Array<Record<string, any>>} */ (
      yamlDocuments(`deploy/kubernetes/base/${serviceName}.yaml`)
        .map((document) => document.toJS())
    );
    const statefulSet = resources.find((resource) =>
      resource.kind === "StatefulSet" && resource.metadata?.name === serviceName
    );
    const container = statefulSet?.spec?.template?.spec?.containers
      ?.find((/** @type {Record<string, any>} */ entry) => entry.name === serviceName);
    assert.ok(container, `${serviceName} StatefulSet container must exist`);
    assert.equal(container.resources?.limits?.memory, "1Gi", `${serviceName} memory limit`);
  }
});

test("Terraform ECS services keep their Fargate placement and rollout classes", () => {
  const cluster = readRepoFile("terraform/modules/compute/cluster.tf");
  const locals = readRepoFile("terraform/modules/compute/locals.tf");
  const assertCapacityProviderDependency = (
    /** @type {string} */ source,
    /** @type {string} */ file
  ) => {
    assert.match(
      source,
      /depends_on\s+=\s+\[[\s\S]*aws_ecs_cluster_capacity_providers\.this/,
      `${file} service must depend on Fargate capacity-provider association`
    );
  };

  assert.match(cluster, /capacity_providers\s+=\s+\["FARGATE", "FARGATE_SPOT"\]/);
  assert.match(
    locals,
    /zero_downtime_deployment\s+=\s+\{\s*maximum_percent\s+=\s+200\s*minimum_healthy_percent\s+=\s+100\s*\}/,
  );
  assert.match(
    locals,
    /stop_before_start_deployment\s+=\s+\{\s*maximum_percent\s+=\s+100\s*minimum_healthy_percent\s+=\s+0\s*\}/,
  );
  assert.match(
    locals,
    /sequential_replacement_deployment\s+=\s+\{\s*maximum_percent\s+=\s+100\s*minimum_healthy_percent\s+=\s+50\s*\}/,
  );

  for (const file of [
    "terraform/modules/compute/gateway_service.tf",
    "terraform/modules/compute/runtime_service.tf",
    "terraform/modules/compute/system_runtime_service.tf",
  ]) {
    const source = readRepoFile(file);
    assert.match(source, /requires_compatibilities\s+=\s+\["FARGATE"\]/);
    assert.match(source, /capacity_provider_strategies\s+=\s+local\.fargate_stateless_capacity_provider_strategies/);
    assertCapacityProviderDependency(source, file);
    assert.match(source, /deployment\s+=\s+local\.zero_downtime_deployment/);
    assert.doesNotMatch(source, /availability_zone_rebalancing\s+=\s+"DISABLED"/);
  }

  for (const file of [
    "terraform/modules/compute/d1_runtime_service.tf",
    "terraform/modules/compute/do_runtime_service.tf",
  ]) {
    const source = readRepoFile(file);
    assert.match(source, /requires_compatibilities\s+=\s+\["FARGATE"\]/);
    assert.match(source, /capacity_provider_strategies\s+=\s+local\.fargate_ondemand_capacity_provider_strategies/);
    assertCapacityProviderDependency(source, file);
    assert.match(source, /deployment\s+=\s+local\.sequential_replacement_deployment/);
    assert.match(source, /availability_zone_rebalancing\s+=\s+"DISABLED"/);
  }

  for (const file of [
    "terraform/modules/compute/scheduler_service.tf",
  ]) {
    const source = readRepoFile(file);
    assert.match(source, /requires_compatibilities\s+=\s+\["FARGATE"\]/);
    assert.match(source, /capacity_provider_strategies\s+=\s+local\.fargate_ondemand_capacity_provider_strategies/);
    assertCapacityProviderDependency(source, file);
    assert.match(source, /deployment\s+=\s+local\.stop_before_start_deployment/);
    assert.match(source, /availability_zone_rebalancing\s+=\s+"DISABLED"/);
  }

  {
    const source = readRepoFile("terraform/modules/compute/workflows_service.tf");
    assert.match(source, /requires_compatibilities\s+=\s+\["FARGATE"\]/);
    assert.match(source, /capacity_provider_strategies\s+=\s+local\.fargate_ondemand_capacity_provider_strategies/);
    assertCapacityProviderDependency(source, "terraform/modules/compute/workflows_service.tf");
    assert.match(source, /deployment\s+=\s+local\.zero_downtime_deployment/);
    assert.doesNotMatch(source, /availability_zone_rebalancing\s+=\s+"DISABLED"/);
  }
});

test("DO invoke request-size caps stay aligned across client and server", () => {
  const runtime = readRepoFile("runtime/_wdl-do-transport.js");
  const protocol = readRepoFile("do-runtime/protocol.js");

  assert.equal(extractAssignedConstant(runtime, "MAX_DO_REQUEST_BODY_BYTES"), extractAssignedConstant(protocol, "MAX_REQUEST_BODY_BYTES"));
  assert.equal(extractAssignedConstant(runtime, "MAX_DO_INVOKE_ENVELOPE_BYTES"), extractAssignedConstant(protocol, "MAX_INVOKE_ENVELOPE_BYTES"));
  assert.equal(extractAssignedConstant(runtime, "MAX_DO_REQUEST_HEADER_COUNT"), extractAssignedConstant(protocol, "MAX_REQUEST_HEADER_COUNT"));
  assert.equal(extractAssignedConstant(runtime, "MAX_DO_REQUEST_HEADER_BYTES"), extractAssignedConstant(protocol, "MAX_REQUEST_HEADER_BYTES"));
});

test("internal binary protocol content-types stay centralized in transport constants", () => {
  const files = [
    ...PRODUCTION_JS_FILES,
    ...jsFiles("tests/unit"),
    ...jsFiles("tests/integration"),
    ...rustFiles("rust"),
  ];
  const literals = [
    "application/vnd.wdl.d1-query",
    "application/vnd.wdl.d1-actor-query",
    "application/vnd.wdl.d1-query-response",
    "application/vnd.wdl.do-invoke",
    "application/vnd.wdl.runtime-load",
  ];
  const allowed = new Set([
    "shared/d1-query-wire.js",
    "runtime/load.js",
    "d1-runtime/protocol.js",
    "runtime/_wdl-do-transport.js",
    "do-runtime/protocol.js",
    "rust/redis-proxy/src/runtime.rs",
    "tests/unit/runtime-load.test.js",
    "tests/unit/style-contracts.test.js",
  ]);
  const offenders = [];
  for (const file of files) {
    if (allowed.has(file)) continue;
    const source = withoutLineComments(readRepoFile(file));
    for (const literal of literals) {
      if (source.includes(literal)) offenders.push(`${file}:${literal}`);
    }
  }
  assert.deepEqual(offenders, []);

  assert.equal(
    extractExportedStringConst("runtime/_wdl-do-transport.js", "DO_INVOKE_CONTENT_TYPE"),
    extractExportedStringConst("do-runtime/protocol.js", "DO_INVOKE_CONTENT_TYPE")
  );

  const runtimeLoad = readRepoFile("runtime/load.js");
  const redisRuntime = readRepoFile("rust/redis-proxy/src/runtime.rs");
  const runtimeLoadMagic = runtimeLoad.match(/const RUNTIME_LOAD_MAGIC\s*=\s*"([^"]+)"/)?.[1];
  const redisRuntimeLoadMagic = redisRuntime.match(/RUNTIME_LOAD_MAGIC:\s*&\[u8\]\s*=\s*b"([^"]+)"/)?.[1];
  const runtimeLoadContentType = runtimeLoad.match(/const RUNTIME_LOAD_CONTENT_TYPE\s*=\s*"([^"]+)"/)?.[1];
  const redisRuntimeLoadContentType = redisRuntime.match(/"application\/vnd\.wdl\.runtime-load"/)?.[0]?.slice(1, -1);
  assert.equal(runtimeLoadMagic, "WDLLOAD!");
  assert.equal(redisRuntimeLoadMagic, runtimeLoadMagic);
  assert.equal(runtimeLoadContentType, "application/vnd.wdl.runtime-load");
  assert.equal(redisRuntimeLoadContentType, runtimeLoadContentType);
});

test("owner endpoint validation lives in a shared contract owner", () => {
  const endpoint = readRepoFile("shared/owner-endpoint.js");
  const adapter = readRepoFile("runtime/_wdl-owner-endpoint.js");
  const doTransport = readRepoFile("runtime/_wdl-do-transport.js");
  const d1Binding = readRepoFile("runtime/bindings/d1.js");
  const controlD1RuntimeClient = readRepoFile("control/d1-runtime-client.js");
  const userConfig = readRepoFile("runtime/config-user.capnp");
  const systemConfig = readRepoFile("runtime/config-system.capnp");
  const d1Config = readRepoFile("d1-runtime/config.capnp");
  const doConfig = readRepoFile("do-runtime/config.capnp");
  const taskIdentity = readRepoFile("shared/task-identity.js");
  const tsconfig = readRepoFile("tsconfig.json");

  assert.match(endpoint, /export function validOwnerEndpointForService/);
  assert.match(endpoint, /"d1-runtime": \/\^d1-runtime/);
  assert.match(endpoint, /"do-runtime": \/\^do-runtime/);
  assert.match(endpoint, /function acceptablePrivateIpv4/);
  assert.match(adapter, /from "\.\.\/shared\/owner-endpoint\.js"/);
  assert.match(doTransport, /from "\.\/_wdl-owner-endpoint\.js"/);
  assert.match(d1Binding, /from "shared-owner-endpoint"/);
  assert.match(controlD1RuntimeClient, /from "shared-owner-endpoint"/);
  assert.match(controlD1RuntimeClient, /validOwnerEndpointForService\(owner\.endpoint, 8787, "d1-runtime"\)/);
  assert.match(tsconfig, /"shared-\*": \["shared\/\*\.js"\]/);
  assert.doesNotMatch(taskIdentity, /_TASK_PORT/);
  assert.doesNotMatch(d1Config, /D1_TASK_PORT/);
  assert.doesNotMatch(doConfig, /DO_TASK_PORT/);
  for (const config of [userConfig, systemConfig, doConfig]) {
    assert.match(config, /name = "shared-owner-endpoint"/);
    assert.match(config, /name = "_wdl-owner-endpoint\.js"/);
    assert.match(config, /name = "runtime-owner-endpoint-source"/);
  }
  for (const config of [userConfig, systemConfig]) {
    const doOwnerNetwork = /const doOwnerNetworkWorker[\s\S]*?\n\);/.exec(config)?.[0] || "";
    assert.match(doOwnerNetwork, /name = "shared-owner-endpoint"/);
  }
});

test("DO transport relative dependencies are registered in host and loaded-worker module graphs", () => {
  const codeBudget = readRepoFile("runtime/load/code-budget.js");
  assert.match(codeBudget, /\["_wdl-request-id\.js", sources\.requestIdSource\]/);
  assert.match(codeBudget, /\["_wdl-do-transport\.js", sources\.doTransportSource\]/);

  for (const file of [
    "runtime/config-user.capnp",
    "runtime/config-system.capnp",
    "do-runtime/config.capnp",
  ]) {
    const source = readRepoFile(file);
    assert.match(source, /name = "runtime-do-transport", esModule = embed/);
    assert.match(source, /name = "_wdl-request-id\.js", esModule = embed/);
    assert.match(source, /name = "runtime-request-id-source", text = embed/);
  }
});

test("queue delay cap literals stay aligned across standalone tiers", () => {
  const expected = "86_400";
  for (const file of ["control/lib.js", "runtime/lib.js", "rust/scheduler/src/queue/mod.rs"]) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, new RegExp(`\\bMAX_QUEUE_DELAY_SECONDS\\b[^=]*=\\s*${RegExp.escape(expected)}\\b`), file);
  }
});

test("queue consumer projection caps stay aligned between Control and Scheduler", () => {
  const control = withoutLineComments(readRepoFile("control/topology.js"));
  const scheduler = withoutLineComments(readRepoFile("rust/scheduler/src/queue/mod.rs"));
  for (const [controlName, schedulerName, expected] of [
    ["MAX_BATCH_SIZE", "MAX_BATCH_SIZE_CAP", "100"],
    ["MAX_BATCH_TIMEOUT_MS", "MAX_BATCH_TIMEOUT_MS", "60_000"],
    ["MAX_RETRIES", "MAX_RETRIES", "100"],
  ]) {
    assert.match(control, new RegExp(`\\b${controlName}\\b[^=]*=\\s*${RegExp.escape(expected)}\\b`));
    assert.match(scheduler, new RegExp(`\\b${schedulerName}\\b[^=]*=\\s*${RegExp.escape(expected)}\\b`));
  }
});

test("Docker images build from pinned public base images", () => {
  const workerdDockerfile = withoutLineComments(readRepoFile("Dockerfile.workerd"));
  const rustDockerfile = withoutLineComments(readRepoFile("Dockerfile.rust"));
  const integrationEnvironment = withoutLineComments(readRepoFile("scripts/integration-environment.js"));

  assert.match(workerdDockerfile, /FROM rust:1-alpine AS supervisor-build/);
  assert.match(workerdDockerfile, /FROM node:24-slim AS build/);
  assert.match(workerdDockerfile, /FROM gcr\.io\/distroless\/base-debian13@sha256:[0-9a-f]{64} AS base/);
  assert.match(rustDockerfile, /FROM rust:1-alpine AS rust-base/);
  // The runtime stage extracts the workerd binary from the npm package so
  // it can ship without a node_modules tree or Node runtime.
  assert.match(workerdDockerfile, /cp -L \/app\/node_modules\/\.bin\/workerd \/workerd/);
  assert.match(workerdDockerfile, /COPY --from=supervisor-build \/d1-supervisor/);
  assert.match(workerdDockerfile, /COPY --from=supervisor-build \/do-supervisor/);
  assert.match(workerdDockerfile, /COPY --from=supervisor-build \/http-hc/);
  assert.doesNotMatch(workerdDockerfile, /\bapt-get\b/);
  assert.doesNotMatch(workerdDockerfile, /\bwget\b/);
  assert.doesNotMatch(workerdDockerfile, /^FROM\s+\S+:latest\b/m);
  assert.match(integrationEnvironment, /DOCKER_COMPOSE_BUILD_ARGS = \["compose", "build", "gateway", "workflows"\]/);
});

test("workerd deploy configs use the supervisor-owned health check binary", () => {
  const files = [
    "docker-compose.yml",
    "terraform/modules/compute/d1_runtime_service.tf",
    "terraform/modules/compute/do_runtime_service.tf",
    "terraform/modules/compute/runtime_service.tf",
    "terraform/modules/compute/system_runtime_service.tf",
  ];

  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    assert.doesNotMatch(source, /\bwget\b/, file);
  }

  for (const file of files) {
    const source = withoutLineComments(readRepoFile(file));
    assert.match(source, /\/usr\/local\/bin\/http-hc/, file);
  }
});

test("Build Cloud references use the getwdl builder endpoint", () => {
  const files = [
    ".github/workflows/ci.yml",
    ".github/workflows/release.yml",
    "docs/testing.md",
    "docs/testing.zh.md",
  ];

  for (const file of files) {
    const source = readRepoFile(file);
    assert.match(source, /getwdl\/builder/, file);
    const endpoints = [...source.matchAll(/endpoint:\s*"([^"]+)"/g)].map((match) => match[1]);
    for (const endpoint of endpoints) {
      assert.equal(endpoint, "getwdl/builder", `${file} Build Cloud endpoint`);
    }
  }
});

test("assets CDN does not vary the public cache key by Origin", () => {
  const cdn = withoutLineComments(readRepoFile("terraform/modules/data/cdn.tf"));

  assert.match(cdn, /resource "aws_cloudfront_origin_access_control" "assets"/);
  assert.match(cdn, /origin_access_control_id\s*=\s*aws_cloudfront_origin_access_control\.assets\[0\]\.id/);
  assert.match(cdn, /resource "aws_cloudfront_response_headers_policy" "assets_cors"/);
  assert.match(cdn, /response_headers_policy_id\s*=\s*aws_cloudfront_response_headers_policy\.assets_cors\[0\]\.id/);
  assert.match(cdn, /resource "aws_cloudfront_cache_policy" "assets"/);
  assert.match(cdn, /cache_policy_id\s*=\s*aws_cloudfront_cache_policy\.assets\[0\]\.id/);
  assert.match(cdn, /cookies_config\s*{\s*cookie_behavior\s*=\s*"none"\s*}/);
  assert.match(cdn, /headers_config\s*{\s*header_behavior\s*=\s*"none"\s*}/);
  assert.match(cdn, /query_strings_config\s*{\s*query_string_behavior\s*=\s*"none"\s*}/);
});
