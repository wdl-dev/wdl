import { test } from "node:test";
import assert from "node:assert/strict";
import { loadControlLib } from "../helpers/load-control-lib.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import {
  RESERVED_NS,
  configuredHostname,
  platformDomainFromEnv,
  validateModulePath,
} from "../../shared/ns-pattern.js";
import {
  MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE,
  WORKERD_EXPERIMENTAL_COMPAT_FLAGS,
  WORKERD_EXPERIMENTAL_COMPAT_FLAGS_SOURCE_VERSION,
} from "../../shared/workerd-compat-flags.js";

const packageJson = /** @type {any} */ (readRepositoryJson("package.json"));
const packageWorkerdDep = packageJson?.dependencies?.workerd;
if (packageWorkerdDep == null) {
  throw new Error('Missing "dependencies.workerd" in package.json');
}
const PACKAGE_WORKERD_DEP = String(packageWorkerdDep);
const packageLockJson = /** @type {any} */ (readRepositoryJson("package-lock.json"));
const workerdVersion = packageLockJson?.packages?.["node_modules/workerd"]?.version;
if (workerdVersion == null) {
  throw new Error('Missing "packages[\\"node_modules/workerd\\"].version" in package-lock.json');
}
const WORKERD_VERSION = String(workerdVersion);
const { controlLib, controlBindings, controlTopology, controlBundle, sharedAuthRoles } =
  await loadControlLib();
const { PLATFORM_TIER_RESERVED_NS } = sharedAuthRoles;
const {
  NS_RE,
  parseControlRoute,
  configuredPublicUrl,
  parseWorkerdDependencyVersion,
  platformVersionFromPackageJson,
  BundleMetaError,
  parseBundleMeta,
  bundleAssetPrefix,
  projectAccessPrincipal,
  parseR2DispatchPath,
  isAdminAcceptableNs,
  validateSecretKey,
  encodeReferrerMember,
  formatD1ReferrerBlockers,
  formatReferrerBlocker,
  extractD1Refs,
  extractOutgoingRefs,
  isValidResumeId,
  compareStreamIds,
  d1DatabaseNameKey,
  d1DatabaseReferrersKey,
  d1DatabaseTombstoneKey,
  d1DatabaseTombstonesKey,
  doObjectRegistryKey,
  referrersKey,
  workersIndexKey,
} = controlLib;
const {
  parseAllowedCallers,
  parseExports,
  parsePlatformBindings,
  evaluateServiceBindingAcl,
  linkServiceBinding,
  linkPlatformBinding,
  LinkError,
} = controlBindings;
const {
  normalizeHost,
  isPlatformDomainHost,
  parsePattern,
  parseRoutes,
  parseHostList,
  parseCronList,
  parseQueueConsumers,
  MAX_CRONS_PER_WORKER,
  MAX_BATCH_SIZE,
  MAX_BATCH_TIMEOUT_MS,
  MAX_RETRIES,
} = controlTopology;
const {
  MAX_WORKER_COMPATIBILITY_DATE,
  maxWorkerCompatibilityDateFromPackageJson,
  normalizeModule,
  prepareBundle,
  normalizeWorkflows,
  normalizeAssets,
  validateCompatibilityDate,
} = controlBundle;

/** @param {string} ns */
function addPlatformTierFixture(ns) {
  RESERVED_NS.add(ns);
  PLATFORM_TIER_RESERVED_NS.add(ns);
}

/** @param {string} ns */
function deletePlatformTierFixture(ns) {
  PLATFORM_TIER_RESERVED_NS.delete(ns);
  RESERVED_NS.delete(ns);
}

test("parseBundleMeta returns JSON objects", () => {
  assert.deepEqual(
    parseBundleMeta('{"routes":[],"vars":{"MODE":"test"}}', {
      ns: "demo",
      worker: "api",
      version: "v3",
    }),
    { routes: [], vars: { MODE: "test" } }
  );
});

test("parseBundleMeta rejects missing, malformed, and non-object metadata with context", () => {
  for (const raw of [undefined, null, "", "{bad", "null", "[]", "1", '"text"']) {
    assert.throws(
      () => parseBundleMeta(raw, { ns: "demo", worker: "api", version: "v3" }),
      (err) => {
        assert.ok(err instanceof BundleMetaError, `expected BundleMetaError for ${String(raw)}`);
        const bundleError = /** @type {Error & { status: number, code: string, details: Record<string, string>, cause: unknown }} */ (err);
        assert.equal(bundleError.status, 500);
        assert.equal(bundleError.code, "corrupt_meta");
        assert.equal(bundleError.message, "Corrupt __meta__ for demo/api/v3");
        assert.deepEqual(bundleError.details, { namespace: "demo", worker: "api", version: "v3" });
        assert.ok(bundleError.cause instanceof Error);
        return true;
      }
    );
  }
});

test("bundleAssetPrefix reads only string prefixes from object-shaped assets", () => {
  assert.equal(bundleAssetPrefix({ assets: { prefix: "assets/demo/" } }), "assets/demo/");
  assert.equal(bundleAssetPrefix({ assets: { prefix: "" } }), "");
  for (const assets of [null, [], "assets/demo/", 1, { prefix: 1 }]) {
    assert.equal(bundleAssetPrefix({ assets }), null);
  }
});

test("parseBundleMeta delegates error construction without changing parse context", () => {
  const expected = new Error("routing-owned error");
  /** @type {{ namespace: string, worker: string, version: string, message: string, reason: string, cause: unknown } | null} */
  let failure = null;
  assert.throws(
    () => parseBundleMeta("[]", {
      ns: "demo",
      worker: "api",
      version: "v3",
      makeError(/** @type {{ namespace: string, worker: string, version: string, message: string, reason: string, cause: unknown }} */ value) {
        failure = value;
        return expected;
      },
    }),
    (err) => err === expected
  );
  const captured = /** @type {{ namespace: string, worker: string, version: string, message: string, reason: string, cause: unknown } | null} */ (
    /** @type {unknown} */ (failure)
  );
  assert.ok(captured);
  assert.deepEqual(
    {
      namespace: captured.namespace,
      worker: captured.worker,
      version: captured.version,
      message: captured.message,
      reason: captured.reason,
    },
    {
      namespace: "demo",
      worker: "api",
      version: "v3",
      message: "Corrupt __meta__ for demo/api/v3",
      reason: "__meta__ must be a JSON object",
    }
  );
  assert.ok(captured.cause instanceof TypeError);
});

test("parseBundleMeta does not retain malformed persisted input in diagnostics", () => {
  const marker = "SECRET_TOKEN_ABC";
  /** @type {{ reason: string, cause: unknown } | null} */
  let failure = null;
  const expected = new Error("routing-owned error");

  assert.throws(
    () => parseBundleMeta(marker, {
      ns: "demo",
      worker: "api",
      version: "v3",
      makeError(/** @type {{ namespace: string, worker: string, version: string, message: string, reason: string, cause: unknown }} */ value) {
        failure = value;
        return expected;
      },
    }),
    (err) => err === expected
  );

  assert.ok(failure);
  const captured = /** @type {{ reason: string, cause: unknown }} */ (
    /** @type {unknown} */ (failure)
  );
  assert.equal(captured.reason, "__meta__ is not valid JSON");
  assert.ok(captured.cause instanceof SyntaxError);
  assert.equal(JSON.stringify({ reason: captured.reason }).includes(marker), false);
});

test("encodeReferrerMember: key order is canonical (alphabetical) regardless of caller shape", () => {
  const a = encodeReferrerMember({
    binding: "AUTH", callerNs: "demo", callerWorker: "api", callerVersion: "v3",
  });
  const b = encodeReferrerMember({
    callerWorker: "api", callerVersion: "v3", callerNs: "demo", binding: "AUTH",
  });
  assert.equal(a, b);
  assert.equal(a, '{"binding":"AUTH","callerNs":"demo","callerVersion":"v3","callerWorker":"api"}');
});

test("encodeReferrerMember: rejects empty fields", () => {
  for (const bad of [
    { binding: "", callerNs: "demo", callerWorker: "api", callerVersion: "v3" },
    { binding: "X", callerNs: "", callerWorker: "api", callerVersion: "v3" },
    { binding: "X", callerNs: "demo", callerWorker: "", callerVersion: "v3" },
    { binding: "X", callerNs: "demo", callerWorker: "api", callerVersion: "" },
  ]) {
    assert.throws(() => encodeReferrerMember(bad));
  }
});

test("formatD1ReferrerBlockers: reports malformed members separately", () => {
  const raw = [
    encodeReferrerMember({
      binding: "DB",
      callerNs: "demo",
      callerWorker: "app",
      callerVersion: "v2",
    }),
    "not-json",
  ];

  assert.deepEqual(formatD1ReferrerBlockers(raw), {
    blockers: [{ worker: "app", version: "v2", binding: "DB" }],
    malformedReferrerCount: 1,
  });
});

test("extractOutgoingRefs: yields every version-pinned service binding including same-ns", () => {
  const refs = extractOutgoingRefs({
    KV1:     { type: "kv", id: "sessions" },
    ASSETS:  { type: "assets" },
    Q:       { type: "queue", id: "orders" },
    AUTH:    { type: "service", service: "auth", version: "v3" },
    BILLING: { type: "service", service: "shared-billing", ns: "acme", version: "v7" },
    PENDING: { type: "service", service: "soon" },          // unversioned — skip
  }, "demo");
  assert.deepEqual(refs, [
    { targetNs: "demo", targetWorker: "auth",           targetVersion: "v3", binding: "AUTH" },
    { targetNs: "acme", targetWorker: "shared-billing", targetVersion: "v7", binding: "BILLING" },
  ]);
});

test("extractOutgoingRefs: null / non-object input yields empty", () => {
  assert.deepEqual(extractOutgoingRefs(null, "demo"), []);
  assert.deepEqual(extractOutgoingRefs(undefined, "demo"), []);
  assert.deepEqual(extractOutgoingRefs({}, "demo"), []);
});

test("extractD1Refs: yields d1 database refs only", () => {
  assert.deepEqual(extractD1Refs({
    KV1: { type: "kv", id: "sessions" },
    DB: { type: "d1", databaseId: "main-db" },
    WRONG: { type: "d1", id: "ignored-db" },
    BROKEN: { type: "d1" },
  }), [
    { binding: "DB", databaseId: "main-db" },
  ]);
  assert.deepEqual(extractD1Refs(null), []);
});

test("control Redis key helpers match canonical schema", () => {
  assert.equal(referrersKey("demo", "api", "v3"),  "worker-version-referrers:demo:api:v3");
  assert.equal(workersIndexKey("demo"),            "workers:demo");
  assert.equal(d1DatabaseNameKey("demo", "main"),  "d1:database-name:demo:main");
  assert.equal(d1DatabaseReferrersKey("demo", "d1_main"), "d1:database-referrers:demo:d1_main");
  assert.equal(d1DatabaseTombstoneKey("demo", "d1_main"), "d1:database-tombstone:demo:d1_main");
  assert.equal(d1DatabaseTombstonesKey("demo"), "d1:database-tombstones:demo");
  assert.equal(doObjectRegistryKey("do_abc"), "do:objects:do_abc");
});

test("NS_RE accepts DNS-label-compatible tenant namespaces", () => {
  for (const ok of ["demo", "a", "ns-1", "0", "a-b-c", "0123456789", "a".repeat(63)]) {
    assert.ok(NS_RE.test(ok), `expected "${ok}" to match`);
  }
});

test("NS_RE rejects uppercase, dot, empty, edge-hyphen, and long names", () => {
  for (const bad of ["", "-", "-ns", "ns-", "a".repeat(64), "Demo", "ns.one", "ns_1", "ns one", "a/b", "你好"]) {
    assert.ok(!NS_RE.test(bad), `expected "${bad}" to NOT match`);
  }
});

test("normalizeModule: string → module", () => {
  const r = normalizeModule("export default {}");
  assert.equal(r.type, "module");
  assert.equal(r.bytes.toString(), "export default {}");
});

test("normalizeModule: data_b64 decoded to bytes", () => {
  const r = normalizeModule({ data_b64: Buffer.from([1, 2, 3]).toString("base64") });
  assert.equal(r.type, "data");
  assert.deepEqual(Array.from(r.bytes), [1, 2, 3]);
});

test("normalizeModule: wasm_b64", () => {
  const r = normalizeModule({ wasm_b64: Buffer.from([0, 97, 115, 109]).toString("base64") });
  assert.equal(r.type, "wasm");
  assert.deepEqual(Array.from(r.bytes), [0, 97, 115, 109]);
});

test("normalizeModule: empty base64 is allowed", () => {
  const data = normalizeModule({ data_b64: "" });
  const wasm = normalizeModule({ wasm_b64: "" });
  assert.equal(data.bytes.length, 0);
  assert.equal(wasm.bytes.length, 0);
});

test("normalizeModule: invalid base64 is rejected", () => {
  assert.throws(() => normalizeModule({ data_b64: "@@@" }), /Invalid base64 in data_b64/);
  assert.throws(() => normalizeModule({ wasm_b64: "!!!!AQ==" }), /Invalid base64 in wasm_b64/);
});

test("normalizeModule: text", () => {
  const r = normalizeModule({ text: "hi" });
  assert.equal(r.type, "text");
  assert.equal(r.bytes.toString(), "hi");
});

test("normalizeModule: json serializes", () => {
  const r = normalizeModule({ json: { a: 1 } });
  assert.equal(r.type, "json");
  assert.equal(r.bytes.toString(), '{"a":1}');
});

test("normalizeModule: json null is allowed", () => {
  const r = normalizeModule({ json: null });
  assert.equal(r.type, "json");
  assert.equal(r.bytes.toString(), "null");
});

test("normalizeModule: cjs", () => {
  assert.equal(normalizeModule({ cjs: "module.exports = {}" }).type, "cjs");
});

test("normalizeModule: py is rejected before workerd cold-load", () => {
  assert.throws(
    () => normalizeModule({ py: "print(1)" }),
    (err) => {
      if (!(err instanceof Error)) return false;
      const coded = /** @type {Error & { code?: unknown, status?: unknown }} */ (err);
      return coded.code === "python_workers_unsupported" &&
        coded.status === 400 &&
        /Python Workers modules are not supported by WDL/.test(coded.message);
    }
  );
});

test("normalizeModule: unknown shape throws", () => {
  assert.throws(() => normalizeModule({}), /Unrecognized module value/);
  assert.throws(() => normalizeModule(null), /Unrecognized module value/);
  assert.throws(() => normalizeModule(42), /Unrecognized module value/);
});

test("prepareBundle: normalizes a minimal module bundle", () => {
  const { meta, normalized } = prepareBundle(
    "worker.js",
    { "worker.js": "export default {}", "data.json": { json: { k: 1 } } },
    { compatibilityDate: "2026-04-24", bindings: { KV: { type: "kv", id: "x" } } }
  );
  assert.equal(meta.mainModule, "worker.js");
  assert.equal(meta.compatibilityDate, "2026-04-24");
  assert.deepEqual({ ...meta.bindings }, { KV: { type: "kv", id: "x" } });
  assert.deepEqual(meta.modules["worker.js"], { type: "module" });
  assert.deepEqual(meta.modules["data.json"], { type: "json" });
  assert.equal(normalized.length, 2);
  const map = Object.fromEntries(normalized);
  assert.equal(map["worker.js"].toString(), "export default {}");
  assert.equal(map["data.json"].toString(), '{"k":1}');
});

test("prepareBundle: vars preserved", () => {
  const { meta } = prepareBundle(
    "worker.js",
    { "worker.js": "x" },
    { vars: { GREETING: "hi", COUNT: 3, ENABLED: true } }
  );
  assert.equal(Object.getPrototypeOf(meta.vars), null);
  assert.deepEqual({ ...meta.vars }, { GREETING: "hi", COUNT: "3", ENABLED: "true" });
});

test("prepareBundle: vars reject unsupported values", () => {
  assert.throws(
    () => prepareBundle("worker.js", { "worker.js": "x" }, { vars: { BAD: { nested: true } } }),
    /\[vars\] BAD: only string\/number\/boolean values are supported/
  );
});

test("prepareBundle: vars reject runtime-internal WDL names", () => {
  assert.throws(
    () => prepareBundle("worker.js", { "worker.js": "x" }, { vars: { __WDL_DO_BACKEND__: "x" } }),
    /\[vars\] __WDL_DO_BACKEND__: name is reserved for runtime-internal bindings/
  );
});

test("prepareBundle: vars reject Object.prototype-shaped names", () => {
  assert.throws(
    () => prepareBundle("worker.js", { "worker.js": "x" }, { vars: JSON.parse('{"__proto__":"x"}') }),
    /\[vars\] __proto__: name is a reserved Object\.prototype key/
  );
});

test("prepareBundle: vars reject non-object containers", () => {
  assert.throws(
    () => prepareBundle("worker.js", { "worker.js": "x" }, { vars: [] }),
    /\[vars\] must be an object/
  );
});

test("prepareBundle: missing mainModule throws without side effects", () => {
  assert.throws(
    () => prepareBundle("missing.js", { "worker.js": "x" }),
    /not present in modules/
  );
});

test("prepareBundle: bindings shape-checked — unknown type rejected", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { FOO: { type: "madeup" } } }),
    /unsupported type "madeup"/
  );
});

test("prepareBundle: Object.prototype binding types are unsupported", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { FOO: { type: "constructor" } } }),
    /unsupported type "constructor"/
  );
});

test("prepareBundle: kv binding requires CF-style id and does not alias colon-separated ids", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { KV1: { type: "kv" } } }),
    /kv id must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { KV1: { type: "kv", id: "" } } }),
    /kv id must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { KV1: { type: "kv", id: "foo:v" } } }),
    /kv id must match/
  );
});

test("prepareBundle: valid kv + assets bindings pass", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { SESSIONS: { type: "kv", id: "sess" }, ASSETS: { type: "assets" } } }
  );
  assert.equal(meta.bindings.SESSIONS.id, "sess");
  assert.equal(meta.bindings.ASSETS.type, "assets");
});

test("prepareBundle: r2 binding requires safe bucketName", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, {
      bindings: { BUCKET: { type: "r2", bucketName: "Bad_Name" } },
    }),
    /r2 bucketName must match/
  );
  const { meta } = prepareBundle("w.js", { "w.js": "x" }, {
    bindings: { BUCKET: { type: "r2", bucketName: "uploads" } },
  });
  assert.equal(meta.bindings.BUCKET.bucketName, "uploads");
});

test("prepareBundle: service binding requires worker-name-shaped 'service'", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { AUTH: { type: "service" } } }),
    /service must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { AUTH: { type: "service", service: "" } } }),
    /service must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { AUTH: { type: "service", service: "a:b" } } }),
    /service must match/
  );
});

test("prepareBundle: valid service binding passes shape check (version injected by admin later)", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { AUTH: { type: "service", service: "auth-worker" } } }
  );
  assert.equal(meta.bindings.AUTH.type, "service");
  assert.equal(meta.bindings.AUTH.service, "auth-worker");
});

test("prepareBundle: service binding folds entrypoint=\"default\" to omitted (wire-level default)", () => {
  const binding = { type: "service", service: "auth", entrypoint: "default" };
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { AUTH: binding } }
  );
  assert.ok(!("entrypoint" in meta.bindings.AUTH));
  assert.equal(binding.entrypoint, "default");
});

test("prepareBundle: service binding accepts ns + entrypoint shape", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    {
      bindings: {
        BILLING: {
          type: "service",
          service: "shared-billing",
          ns: "acme",
          entrypoint: "Billing",
        },
      },
    }
  );
  assert.equal(meta.bindings.BILLING.ns, "acme");
  assert.equal(meta.bindings.BILLING.entrypoint, "Billing");
});

test("prepareBundle: service binding rejects invalid ns format", () => {
  assert.throws(
    () =>
      prepareBundle(
        "w.js",
        { "w.js": "x" },
        { bindings: { X: { type: "service", service: "t", ns: "Bad_NS" } } }
      ),
    /ns must match/
  );
});

test("prepareBundle: service binding rejects runtime-reserved entrypoint names (__Wdl…__)", () => {
  // Runtime injects __WdlAbort__ into every loaded worker. A binding
  // targeting that entrypoint would let the caller drive the runtime's
  // abort shim. Reject at validateBindings ingress; linkServiceBinding
  // re-checks as defense in depth.
  for (const reserved of ["__WdlAbort__", "__WdlSomething__", "__Wdl__"]) {
    assert.throws(
      () =>
        prepareBundle(
          "w.js",
          { "w.js": "x" },
          { bindings: { X: { type: "service", service: "t", entrypoint: reserved } } }
        ),
      /reserved for runtime-injected/,
      `expected reserved-entrypoint rejection for ${JSON.stringify(reserved)}`,
    );
  }
  // Names that share the `__Wdl` prefix but lack the trailing `__` are
  // user-controllable — anchor the regex on both ends.
  assert.doesNotThrow(() =>
    prepareBundle(
      "w.js",
      { "w.js": "x" },
      { bindings: { X: { type: "service", service: "t", entrypoint: "__WdlNotReserved" } } }
    ),
  );
});

test("prepareBundle: service binding rejects non-identifier entrypoint", () => {
  assert.throws(
    () =>
      prepareBundle(
        "w.js",
        { "w.js": "x" },
        { bindings: { X: { type: "service", service: "t", entrypoint: "1bad" } } }
      ),
    /entrypoint must be a JS identifier/
  );
  assert.throws(
    () =>
      prepareBundle(
        "w.js",
        { "w.js": "x" },
        { bindings: { X: { type: "service", service: "t", entrypoint: "Has-Dash" } } }
      ),
    /entrypoint must be a JS identifier/
  );
});

test("prepareBundle: durable object binding requires className-shaped class", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { ROOMS: { type: "do" } } }),
    /do className must be a valid JS class declaration name/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { ROOMS: { type: "do", className: "not-valid" } } }),
    /do className must be a valid JS class declaration name/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { ROOMS: { type: "do", className: "class" } } }),
    /do className must be a valid JS class declaration name/
  );
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { ROOMS: { type: "do", className: "Room" } } }
  );
  assert.equal(meta.bindings.ROOMS.type, "do");
  assert.equal(meta.bindings.ROOMS.className, "Room");

  const maxClassName = `R${"o".repeat(467)}`;
  assert.doesNotThrow(() => prepareBundle("w.js", { "w.js": "x" }, {
    bindings: { ROOMS: { type: "do", className: maxClassName } },
  }));
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, {
      bindings: { ROOMS: { type: "do", className: `${maxClassName}m` } },
    }),
    /at most 468 bytes/
  );
});

test("prepareBundle: queue binding requires queue-name-shaped 'id'", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { Q: { type: "queue" } } }),
    /queue id must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { Q: { type: "queue", id: "" } } }),
    /queue id must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { Q: { type: "queue", id: "Bad:Name" } } }),
    /queue id must match/
  );
});

test("prepareBundle: valid queue binding passes shape check", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { Q: { type: "queue", id: "orders", deliveryDelaySeconds: 60 } } }
  );
  assert.equal(meta.bindings.Q.type, "queue");
  assert.equal(meta.bindings.Q.id, "orders");
  assert.equal(meta.bindings.Q.deliveryDelaySeconds, 60);
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, {
      bindings: { Q: { type: "queue", id: "orders", deliveryDelaySeconds: 86_401 } },
    }),
    /deliveryDelaySeconds/
  );
});

test("prepareBundle: d1 binding accepts databaseId", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { bindings: { DB: { type: "d1", databaseId: "main-db" } } }
  );
  assert.equal(meta.bindings.DB.type, "d1");
  assert.equal(meta.bindings.DB.databaseId, "main-db");
});

test("prepareBundle: d1 binding requires safe databaseId", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { DB: { type: "d1" } } }),
    /d1 databaseId must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { DB: { type: "d1", id: "main-db" } } }),
    /d1 databaseId must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { DB: { type: "d1", databaseId: "bad:id" } } }),
    /d1 databaseId must match/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, {
      bindings: { DB: { type: "d1", databaseId: "main-db", databaseName: "main" } },
    }),
    /deploy API bindings must set databaseId/
  );
});

test("prepareBundle: non-object binding rejected", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: { X: "oops" } }),
    /bindings\.X must be an object/
  );
});

test("prepareBundle: top-level bindings must be an object", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { bindings: [] }),
    /bindings must be an object/
  );
});

test("validateModulePath accepts typical paths", () => {
  for (const p of ["worker.js", "src/worker.js", "utils/a-b_c.ts", "icon.png", "_private.js", "a.b.c"]) {
    validateModulePath(p);
  }
});

test("validateModulePath rejects path traversal + reserved segments + invalid chars", () => {
  const bad = [
    "",
    "/absolute.js",
    "../escape.js",
    "a/../b.js",
    "./leading-dot.js",
    "trailing/",
    "double//slash.js",
    "__proto__",
    "src/__proto__",
    "constructor",
    "toString",
    "name with space.js",
    "ctrl\nchar.js",
  ];
  for (const p of bad) {
    assert.throws(() => validateModulePath(p), `expected reject for ${JSON.stringify(p)}`);
  }
});

test("prepareBundle rejects module path '__proto__' (would corrupt meta.modules prototype)", () => {
  assert.throws(
    () => prepareBundle("__proto__", JSON.parse('{"__proto__":"x"}')),
    /must match|segment.*is reserved/
  );
});

test("prepareBundle: mainModule 'constructor' with no own key is rejected (not masked by Object.prototype.constructor)", () => {
  assert.throws(
    () => prepareBundle("constructor", { "w.js": "x" }),
    /segment.*is reserved/
  );
});

test("prepareBundle: meta.modules has null prototype (defense-in-depth against __proto__ writes)", () => {
  const { meta } = prepareBundle("w.js", { "w.js": "x" });
  assert.equal(Object.getPrototypeOf(meta.modules), null);
});

test("prepareBundle: accepts CF-compatible binding names (SCREAMING, camelCase, _private)", () => {
  for (const ok of ["MY_Q", "productionQueue", "_private", "KV", "A"]) {
    prepareBundle(
      "w.js",
      { "w.js": "x" },
      { bindings: { [ok]: { type: "kv", id: "cache" } } }
    );
  }
});

test("prepareBundle: binding names that aren't JS identifiers fail with grammar message", () => {
  for (const bad of ["9NUM", "MY-Q", "", "has space"]) {
    assert.throws(
      () =>
        prepareBundle(
          "w.js",
          { "w.js": "x" },
          { bindings: { [bad]: { type: "kv", id: "cache" } } }
        ),
      /bindings: name .* must match/,
      `expected regex rejection for ${JSON.stringify(bad)}`
    );
  }
});

test("prepareBundle: binding name '__proto__' / 'constructor' etc rejected as reserved", () => {
  for (const reserved of ["__proto__", "constructor", "toString", "hasOwnProperty"]) {
    assert.throws(
      () =>
        prepareBundle(
          "w.js",
          { "w.js": "x" },
          { bindings: { [reserved]: { type: "kv", id: "cache" } } }
        ),
      /reserved Object\.prototype key/,
      `expected reserved-key rejection for ${JSON.stringify(reserved)}`
    );
  }
});

test("prepareBundle: binding names with __WDL_ prefix are runtime-internal", () => {
  for (const reserved of ["__WDL_DO_BACKEND__", "__WDL_DO_ALARMS__", "__WDL_FUTURE__"]) {
    assert.throws(
      () =>
        prepareBundle(
          "w.js",
          { "w.js": "x" },
          { bindings: { [reserved]: { type: "kv", id: "cache" } } }
        ),
      /runtime-internal bindings/,
      `expected runtime-internal binding rejection for ${JSON.stringify(reserved)}`
    );
  }
});

test("prepareBundle: compatibilityFlags preserved", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    { compatibilityFlags: ["nodejs_compat"] }
  );
  assert.deepEqual(meta.compatibilityFlags, ["nodejs_compat"]);
});

test("prepareBundle: experimental workerd compatibility flags are rejected", () => {
  assert.throws(
    () => prepareBundle(
      "w.js",
      { "w.js": "x" },
      { compatibilityFlags: ["nodejs_compat", "unsafe_module"] }
    ),
    (err) => {
      if (!(err instanceof Error)) return false;
      const coded = /** @type {Error & { code?: unknown, status?: unknown }} */ (err);
      return coded.code === "experimental_compat_flag_unsupported" &&
        coded.status === 400 &&
        /"unsafe_module"/.test(coded.message);
    }
  );
  assert.doesNotThrow(() =>
    prepareBundle(
      "w.js",
      { "w.js": "x" },
      { compatibilityFlags: ["nodejs_compat", "no_nodejs_compat"] }
    )
  );
});

test("prepareBundle: rejects legacy error serialization", () => {
  for (const compatibilityDate of ["2026-04-20", "2026-04-21", undefined]) {
    assert.throws(
      () => prepareBundle(
        "w.js",
        { "w.js": "x" },
        { compatibilityDate, compatibilityFlags: ["legacy_error_serialization"] }
      ),
      (err) => {
        if (!(err instanceof Error)) return false;
        const coded = /** @type {Error & { code?: unknown, status?: unknown }} */ (err);
        return coded.code === "compatibility_flag_unsupported" &&
          coded.status === 400 &&
          coded.message.includes("legacy_error_serialization");
      },
      `legacy_error_serialization should be rejected for ${compatibilityDate ?? "the default date"}`
    );
  }
});

test("prepareBundle: leaves redundant positive flags to workerd", () => {
  const { meta } = prepareBundle(
    "w.js",
    { "w.js": "x" },
    {
      compatibilityDate: "2026-04-21",
      compatibilityFlags: ["enhanced_error_serialization"],
    }
  );
  assert.deepEqual(meta.compatibilityFlags, ["enhanced_error_serialization"]);
});

test("prepareBundle: compatibilityDate validates shape before commit", () => {
  assert.equal(
    prepareBundle("w.js", { "w.js": "x" }, { compatibilityDate: "2026-04-24" }).meta.compatibilityDate,
    "2026-04-24"
  );
  for (const bad of ["20260424", "2026-2-24", "2026-02-30", "", 20260424]) {
    assert.throws(
      () => prepareBundle("w.js", { "w.js": "x" }, { compatibilityDate: bad }),
      /compatibilityDate/
    );
  }
});

test("validateCompatibilityDate rejects future and unsupported workerd dates", () => {
  const unsupported = new Date(`${MAX_WORKER_COMPATIBILITY_DATE}T00:00:00Z`);
  unsupported.setUTCDate(unsupported.getUTCDate() + 1);
  const unsupportedDate = [
    String(unsupported.getUTCFullYear()).padStart(4, "0"),
    String(unsupported.getUTCMonth() + 1).padStart(2, "0"),
    String(unsupported.getUTCDate()).padStart(2, "0"),
  ].join("-");
  const afterUnsupported = new Date(unsupported);
  afterUnsupported.setUTCDate(afterUnsupported.getUTCDate() + 1);

  assert.equal(
    validateCompatibilityDate("2026-06-20", new Date("2026-06-30T00:00:00Z")),
    "2026-06-20"
  );
  assert.equal(
    validateCompatibilityDate(MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE, new Date("2026-06-30T00:00:00Z")),
    MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE
  );
  assert.throws(
    () => validateCompatibilityDate("2026-03-31", new Date("2026-06-30T00:00:00Z")),
    /older than WDL supports/
  );
  assert.throws(
    () => validateCompatibilityDate("2026-06-15", new Date("2026-06-14T00:00:00Z")),
    /must not be later than today UTC/
  );
  assert.throws(
    () => validateCompatibilityDate(unsupportedDate, afterUnsupported),
    /newer than bundled workerd supports/
  );
});

test("MAX_WORKER_COMPATIBILITY_DATE matches pinned workerd release plus seven days", () => {
  const parsed = parseWorkerdDependencyVersion(JSON.stringify({
    dependencies: { workerd: PACKAGE_WORKERD_DEP },
  }));
  assert.ok(parsed, `unexpected workerd dependency format ${PACKAGE_WORKERD_DEP}`);
  assert.equal(parsed.version, WORKERD_VERSION);
  const max = new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day + 7));
  const expected = [
    String(max.getUTCFullYear()).padStart(4, "0"),
    String(max.getUTCMonth() + 1).padStart(2, "0"),
    String(max.getUTCDate()).padStart(2, "0"),
  ].join("-");
  assert.equal(MAX_WORKER_COMPATIBILITY_DATE, expected);
  assert.equal(
    maxWorkerCompatibilityDateFromPackageJson(JSON.stringify({ dependencies: { workerd: PACKAGE_WORKERD_DEP } })),
    expected
  );
  assert.equal(maxWorkerCompatibilityDateFromPackageJson(JSON.stringify({ dependencies: {} })), null);
});

test("workerd experimental compat flag mirror matches pinned workerd source version", () => {
  const regenerate = [
    "Regenerate shared/workerd-compat-flags.js from an upstream workerd checkout:",
    "node scripts/extract-workerd-experimental-compat-flags.mjs",
    "/path/to/workerd/src/workerd/io/compatibility-date.capnp",
  ].join(" ");
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS_SOURCE_VERSION, WORKERD_VERSION, regenerate);
  assert.ok(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("experimental"));
  assert.ok(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("unsafe_module"));
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.length, 33);
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("unique_ctx_per_invocation"), false);
  assert.equal(
    WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("nonclass_entrypoint_reuses_ctx_across_invocations"),
    false
  );
  assert.equal(
    WORKERD_EXPERIMENTAL_COMPAT_FLAGS.some((flag) => flag.startsWith("no_")),
    false,
    "only enable flags from $experimental compatibility entries should be mirrored"
  );
});

test("prepareBundle: compatibilityFlags rejected when not an array (would be silently dropped at runtime floor merge)", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { compatibilityFlags: "nodejs_compat" }),
    /compatibilityFlags must be an array/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { compatibilityFlags: { flag: true } }),
    /compatibilityFlags must be an array/
  );
});

test("prepareBundle: compatibilityFlags rejected when an entry is not a non-empty string", () => {
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { compatibilityFlags: ["nodejs_compat", 42] }),
    /must be non-empty strings/
  );
  assert.throws(
    () => prepareBundle("w.js", { "w.js": "x" }, { compatibilityFlags: [""] }),
    /must be non-empty strings/
  );
});

test("prepareBundle: exports lands in meta only when non-empty", () => {
  const modules = { "worker.js": "export default {}" };
  const withExports = prepareBundle("worker.js", modules, {
    exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
  });
  assert.deepEqual(withExports.meta.exports, [
    { entrypoint: "default", allowedCallers: ["*"] },
  ]);
  const absent = prepareBundle("worker.js", modules, { exports: [] });
  assert.equal(absent.meta.exports, undefined);
});

test("validateSecretKey: env-var grammar only", () => {
  for (const ok of ["API_KEY", "_FOO", "a", "A1_B2"]) {
    assert.doesNotThrow(() => validateSecretKey(ok), `"${ok}" should be valid`);
  }
  for (const bad of ["", "1LEADING", "kebab-case", "dot.sep", "with space", "a/b"]) {
    assert.throws(() => validateSecretKey(bad), `"${bad}" should be rejected`);
  }
  assert.throws(() => validateSecretKey("__WDL_DO_BACKEND__"), /runtime-internal bindings/);
  for (const reserved of ["__proto__", "constructor", "prototype", "toString", "valueOf"]) {
    assert.throws(
      () => validateSecretKey(reserved),
      /reserved Object\.prototype key/,
      `"${reserved}" should be rejected before persistence`
    );
  }
  assert.throws(() => validateSecretKey("A".repeat(129)), /too long/);
});

/** @param {any} route */
function authRouteShape(route) {
  /** @type {{ action?: string, ns?: string }} */
  const out = {};
  if (route.action !== undefined) out.action = route.action;
  if (route.ns !== undefined) out.ns = route.ns;
  return out;
}

/**
 * @param {string} pathname
 * @param {string} method
 * @param {{ action?: string, ns?: string }} expected
 */
function assertAuthRoute(pathname, method, expected) {
  assert.deepEqual(authRouteShape(parseControlRoute(pathname, method)), expected);
}

/**
 * @param {string} pathname
 * @param {string} method
 */
function assertNoAuthAction(pathname, method) {
  assert.equal(parseControlRoute(pathname, method).action, undefined);
}

test("parseControlRoute: known endpoints map to action", () => {
  // /reload — only POST
  assertAuthRoute("/reload", "POST", { action: "system.reload" });
  // /whoami — only GET
  assertAuthRoute("/whoami", "GET", { action: "diagnostic.whoami" });
  // /auth/tokens
  assertAuthRoute("/auth/tokens", "POST", { action: "auth.token.issue" });
  assertAuthRoute("/auth/tokens", "GET",  { action: "auth.token.list" });
  assertAuthRoute("/auth/tokens/abc123", "DELETE", { action: "auth.token.revoke" });
  assertAuthRoute("/auth/delegated-tokens", "POST", { action: "auth.delegated_token.issue" });

  // /ns/<ns>/...
  assertAuthRoute("/ns/foo/workers", "GET", { action: "worker.list", ns: "foo" });
  assertAuthRoute("/ns/foo/hosts", "GET",   { action: "host.read", ns: "foo" });
  assertAuthRoute("/ns/foo/hosts", "POST",  { action: "host.write", ns: "foo" });
  assertAuthRoute("/ns/foo/secrets", "GET", { action: "secret.read", ns: "foo" });
  assertAuthRoute("/ns/foo/secrets/KEY", "PUT",    { action: "secret.write", ns: "foo" });
  assertAuthRoute("/ns/foo/secrets/KEY", "DELETE", { action: "secret.delete", ns: "foo" });

  // logs tail
  assertAuthRoute("/ns/foo/logs/tail", "GET", { action: "worker.logs.tail", ns: "foo" });

  // workflows
  assertAuthRoute("/ns/foo/workflows", "GET", { action: "workflow.list", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances", "GET", { action: "workflow.read", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances/id-1", "GET", { action: "workflow.read", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances/id-1/pause", "POST", { action: "workflow.write", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances/id-1/resume", "POST", { action: "workflow.write", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances/id-1/restart", "POST", { action: "workflow.write", ns: "foo" });
  assertAuthRoute("/ns/foo/workflows/api/orders/instances/id-1/terminate", "POST", { action: "workflow.write", ns: "foo" });

  // d1
  assertAuthRoute("/ns/foo/d1/databases", "GET",  { action: "d1.list", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases", "POST", { action: "d1.create", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases/main", "DELETE", { action: "d1.delete", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases/main/query", "POST", { action: "d1.execute", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases/main/migrations", "GET", { action: "d1.migrate.read", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases/main/migrations/status", "POST", { action: "d1.migrate.read", ns: "foo" });
  assertAuthRoute("/ns/foo/d1/databases/main/migrations/apply", "POST", { action: "d1.migrate.write", ns: "foo" });

  // r2
  assertAuthRoute("/ns/foo/r2/buckets", "GET", { action: "r2.bucket.list", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects", "GET", { action: "r2.object.list", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects/a.txt", "GET", { action: "r2.object.get", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects//", "GET", { action: "r2.object.get", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects/dir/a.txt", "GET", { action: "r2.object.get", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects/dir/a.txt", "HEAD", { action: "r2.object.head", ns: "foo" });
  assertAuthRoute("/ns/foo/r2/buckets/uploads/objects/dir/a.txt", "DELETE", { action: "r2.object.delete", ns: "foo" });

  // worker lifecycle
  assertAuthRoute("/ns/foo/worker/bar/deploy", "POST", { action: "worker.deploy", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/promote", "POST", { action: "worker.promote", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/delete", "POST", { action: "worker.delete", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/versions", "GET", { action: "worker.versions.read", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/versions/v3", "DELETE", { action: "worker.versions.delete", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/secrets", "GET", { action: "secret.read", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/secrets/X", "PUT", { action: "secret.write", ns: "foo" });
  assertAuthRoute("/ns/foo/worker/bar/secrets/X", "DELETE", { action: "secret.delete", ns: "foo" });
});

test("parseControlRoute: classifier output is reserved-ns-agnostic", () => {
  // classifier no longer collapses reserved-ns / RESERVED_TENANT_NS to a
  // single bucket; it returns the action and ns verbatim. evaluateAccess
  // (red lines 1/2 + ROLES) decides who can use that action against that ns.
  for (const ns of ["__system__", "__platform__", "__future__", "admin"]) {
    assertAuthRoute(`/ns/${ns}/worker/bar/deploy`, "POST", { action: "worker.deploy", ns });
    assertAuthRoute(`/ns/${ns}/d1/databases`, "GET", { action: "d1.list", ns });
  }
});

test("parseControlRoute: dispatch shape carries handler params", () => {
  assert.deepEqual(parseControlRoute("/auth/tokens/abc123", "DELETE"), {
    kind: "authTokens",
    scopeRoute: "auth_tokens",
    tokenId: "abc123",
    action: "auth.token.revoke",
  });
  assert.deepEqual(parseControlRoute("/auth/delegated-tokens", "POST"), {
    kind: "authDelegatedTokens",
    scopeRoute: "auth_delegated_tokens",
    action: "auth.delegated_token.issue",
  });
  assert.deepEqual(parseControlRoute("/whoami", "GET"), {
    kind: "whoami",
    scopeRoute: "whoami",
    action: "diagnostic.whoami",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/secrets/KEY", "PUT"), {
    kind: "nsSecrets",
    scopeRoute: "ns_secrets",
    ns: "foo",
    secretKey: "KEY",
    action: "secret.write",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/d1/databases/main/query", "POST"), {
    kind: "d1",
    scopeRoute: "d1",
    ns: "foo",
    subPath: ["databases", "main", "query"],
    action: "d1.execute",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/r2/buckets/uploads/objects/a//b", "GET"), {
    kind: "r2",
    scopeRoute: "r2",
    ns: "foo",
    subPath: ["buckets", "uploads", "objects", "a", "", "b"],
    action: "r2.object.get",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/r2/buckets/uploads/objects//", "GET"), {
    kind: "r2",
    scopeRoute: "r2",
    ns: "foo",
    subPath: ["buckets", "uploads", "objects", "", ""],
    action: "r2.object.get",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/workflows/api/orders/instances/id-1/restart", "POST"), {
    kind: "workflows",
    scopeRoute: "workflows",
    ns: "foo",
    subPath: ["api", "orders", "instances", "id-1", "restart"],
    action: "workflow.write",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/worker/bar/versions/v3", "DELETE"), {
    kind: "worker",
    scopeRoute: "versions",
    ns: "foo",
    worker: "bar",
    workerAction: "versions",
    subPath: ["v3"],
    action: "worker.versions.delete",
  });
});

test("parseControlRoute: known dispatch prefixes may omit auth action", () => {
  assert.deepEqual(parseControlRoute("/auth/tokens/abc/extra", "DELETE"), {
    kind: "authTokens",
    scopeRoute: "auth_tokens",
    tokenId: undefined,
  });
  assert.deepEqual(parseControlRoute("/auth/delegated-tokens", "GET"), {
    kind: "authDelegatedTokens",
    scopeRoute: "auth_delegated_tokens",
  });
  assert.deepEqual(parseControlRoute("/ns/foo/d1", "GET"), {
    kind: "d1",
    scopeRoute: "d1",
    ns: "foo",
    subPath: [],
  });
  assert.deepEqual(parseControlRoute("/ns/foo/r2/not-buckets", "GET"), {
    kind: "r2",
    scopeRoute: "r2",
    ns: "foo",
    subPath: ["not-buckets"],
  });
  assert.deepEqual(parseControlRoute("/ns/foo/worker/bar/versions/v1/extra", "DELETE"), {
    kind: "worker",
    scopeRoute: "versions",
    ns: "foo",
    worker: "bar",
    workerAction: "versions",
    subPath: ["v1", "extra"],
  });
});

test("parseControlRoute: unknown shape has no action (red line 3 in evaluateAccess)", () => {
  assert.deepEqual(parseControlRoute("/", "GET"), {});
  assert.deepEqual(parseControlRoute("/random/path", "GET"), {});
  assert.deepEqual(parseControlRoute("/ns/foo/worker/bar", "POST"), {});
  assertNoAuthAction("/reload", "GET");  // wrong method
  assertNoAuthAction("/reload", "DELETE");
  assertNoAuthAction("/whoami", "POST");
});

test("parseControlRoute: trailing junk on known prefix has no action", () => {
  // CRITICAL invariant: `DELETE /auth/tokens/<id>/extra` must NOT match
  // auth.token.revoke (segs.length=4, not 3) — otherwise the dispatcher
  // would 404 a request that already passed auth, giving low-priv tokens
  // a 200/404 oracle on path existence.
  assertNoAuthAction("/auth/tokens/abc/extra", "DELETE");
  assert.deepEqual(parseControlRoute("/auth/delegated-tokens/extra", "POST"), {});
  assertNoAuthAction("/ns/foo/worker/bar/versions/v1/extra", "DELETE");
  assertNoAuthAction("/ns/foo/d1/databases/main/query/extra", "POST");
  assertNoAuthAction("/ns/foo/r2/buckets/uploads/objects", "DELETE");
  // GET /ns/<ns>/secrets/<key> is not a real endpoint
  assertNoAuthAction("/ns/foo/secrets/KEY", "GET");
  // logs tail is GET-only, exact-shape only
  assertNoAuthAction("/ns/foo/logs/tail", "POST");
  assert.deepEqual(parseControlRoute("/ns/foo/logs/tail/extra", "GET"), {});
  assert.deepEqual(parseControlRoute("/ns/foo/logs", "GET"), {});
});

test("parseR2DispatchPath preserves S3 key path segments", () => {
  assert.deepEqual(parseR2DispatchPath("/ns/foo/r2/buckets/uploads/objects/a//b"), {
    ns: "foo",
    subPath: ["buckets", "uploads", "objects", "a", "", "b"],
  });
  assert.deepEqual(parseR2DispatchPath("/ns/foo/r2/buckets/uploads/objects/a/"), {
    ns: "foo",
    subPath: ["buckets", "uploads", "objects", "a", ""],
  });
  assert.equal(parseR2DispatchPath("/ns/foo/d1/databases"), null);
});

test("normalizeAssets: returns null when absent", () => {
  assert.strictEqual(normalizeAssets(null), null);
  assert.strictEqual(normalizeAssets(undefined), null);
});

test("normalizeAssets: decodes each entry", () => {
  const pairs = normalizeAssets({
    "logo.png": Buffer.from([1, 2, 3]).toString("base64"),
    "css/app.css": Buffer.from("body{}", "utf8").toString("base64"),
  });
  assert.equal(pairs.length, 2);
  const byPath = Object.fromEntries(pairs);
  assert.ok(Buffer.from([1, 2, 3]).equals(byPath["logo.png"]));
  assert.ok(Buffer.from("body{}", "utf8").equals(byPath["css/app.css"]));
});

test("normalizeAssets: rejects arrays and non-objects", () => {
  assert.throws(() => normalizeAssets([]), /must be an object/);
  assert.throws(() => normalizeAssets("x"), /must be an object/);
});

test("normalizeAssets: rejects non-string value", () => {
  assert.throws(() => normalizeAssets({ "x": 1 }), /must be a base64 string/);
});

test("normalizeAssets: rejects absolute and traversal paths", () => {
  assert.throws(() => normalizeAssets({ "/abs": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "../evil": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "a/../b": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "a/./b": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "./b": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "a//b": "" }), /invalid path/);
  assert.throws(() => normalizeAssets({ "": "" }), /invalid path/);
});

test("normalizeAssets: rejects invalid base64", () => {
  assert.throws(() => normalizeAssets({ "x": "not-base64!!" }), /Invalid base64/);
});

// ---- normalizeHost ----

test("normalizeHost: lowercases and strips :port", () => {
  assert.equal(normalizeHost("Workers.Example"), "workers.example");
  assert.equal(normalizeHost("Workers.example:8080"), "workers.example");
  assert.equal(normalizeHost("  api.workers.example  "), "api.workers.example");
});

test("normalizeHost: rejects empty / non-string / invalid shapes", () => {
  assert.throws(() => normalizeHost(""), /must not be empty/);
  assert.throws(() => normalizeHost("   "), /must not be empty/);
  assert.throws(() => normalizeHost(123), /must be a string/);
  assert.throws(() => normalizeHost("has:colon"), /invalid host/);
  assert.throws(() => normalizeHost("has/slash"), /invalid host/);
  assert.throws(() => normalizeHost("has space"), /invalid host/);
  assert.throws(() => normalizeHost("has\ttab"), /invalid host/);
});

test("normalizeHost: idempotent", () => {
  const once = normalizeHost("API.Workers.example:443");
  assert.equal(normalizeHost(once), once);
});

test("normalizeHost: strips trailing FQDN dot(s)", () => {
  assert.equal(normalizeHost("workers.example."), "workers.example");
  assert.equal(normalizeHost("workers.example..."), "workers.example");
  assert.equal(normalizeHost("Workers.Example.:8080"), "workers.example");
});

// ---- isPlatformDomainHost ----

test("isPlatformDomainHost: equal or subdomain of platform domain", () => {
  assert.equal(isPlatformDomainHost("workers.local", "workers.local"), true);
  assert.equal(isPlatformDomainHost("demo.workers.local", "workers.local"), true);
  assert.equal(isPlatformDomainHost("a.b.workers.local", "workers.local"), true);
  assert.equal(isPlatformDomainHost("workers.example", "workers.local"), false);
  assert.equal(isPlatformDomainHost("notworkers.local", "workers.local"), false);
});

test("isPlatformDomainHost: trailing-dot host hits platform-domain check after normalization", () => {
  // The check itself takes a normalized host; this proves the contract
  // holds when callers normalize first.
  assert.equal(
    isPlatformDomainHost(normalizeHost("demo.workers.local."), "workers.local"),
    true
  );
});

// ---- parsePattern ----

test("parsePattern: trailing /* → kind=prefix, value strips star", () => {
  assert.deepEqual(parsePattern("workers.example/*", "workers.local"), {
    host: "workers.example", slot: "/*", kind: "prefix", value: "/",
  });
  assert.deepEqual(parsePattern("workers.example/api/*", "workers.local"), {
    host: "workers.example", slot: "/api/*", kind: "prefix", value: "/api/",
  });
});

test("parsePattern: trailing / without star → kind=exact for the apex", () => {
  assert.deepEqual(parsePattern("workers.example/", "workers.local"), {
    host: "workers.example", slot: "/", kind: "exact", value: "/",
  });
});

test("parsePattern: no trailing star → kind=exact (CF semantics)", () => {
  assert.deepEqual(parsePattern("workers.example/mcp", "workers.local"), {
    host: "workers.example", slot: "/mcp", kind: "exact", value: "/mcp",
  });
  assert.deepEqual(
    parsePattern("workers.example/.well-known/oauth-protected-resource", "workers.local"),
    {
      host: "workers.example",
      slot: "/.well-known/oauth-protected-resource",
      kind: "exact",
      value: "/.well-known/oauth-protected-resource",
    }
  );
});

test("parsePattern: host is normalized", () => {
  assert.deepEqual(parsePattern("Workers.Example:443/api/*", "workers.local"), {
    host: "workers.example", slot: "/api/*", kind: "prefix", value: "/api/",
  });
});

test("parsePattern: bare host rejected", () => {
  assert.throws(
    () => parsePattern("workers.example", "workers.local"),
    /path segment required/
  );
});

test("parsePattern: wildcard host rejected", () => {
  assert.throws(
    () => parsePattern("*.workers.example/*", "workers.local"),
    /wildcard hosts/
  );
});

test("parsePattern: mid-path wildcard rejected", () => {
  assert.throws(
    () => parsePattern("workers.example/a/*/b", "workers.local"),
    /trailing character/
  );
  assert.throws(
    () => parsePattern("workers.example/*foo", "workers.local"),
    /trailing character/
  );
});

test("parsePattern: path whitespace rejected", () => {
  assert.throws(
    () => parsePattern("workers.example/a\tb", "workers.local"),
    /path must not contain whitespace/
  );
  assert.throws(
    () => parsePattern("workers.example/a b", "workers.local"),
    /path must not contain whitespace/
  );
});

test("parsePattern: trailing * without slash → CF 'startsWith' glob", () => {
  // CF accepts patterns like /public* — match anything starting
  // with that string, no slash boundary.
  assert.deepEqual(parsePattern("demo.workers.example/public*", "workers.local"), {
    host: "demo.workers.example",
    slot: "/public*",
    kind: "prefix",
    value: "/public",
  });
});

test("parsePattern: platform domain rejected", () => {
  assert.throws(
    () => parsePattern("demo.workers.local/*", "workers.local"),
    /inside the platform domain/
  );
  assert.throws(
    () => parsePattern("workers.local/*", "workers.local"),
    /inside the platform domain/
  );
});

test("parsePattern: rejects platform-domain via trailing-dot bypass", () => {
  assert.throws(
    () => parsePattern("demo.workers.local./*", "workers.local"),
    /inside the platform domain/
  );
});

test("parsePattern: rejects query / fragment (would deploy-succeed but never match)", () => {
  assert.throws(
    () => parsePattern("workers.example/api?x=1", "workers.local"),
    /query.*fragment.*not supported/
  );
  assert.throws(
    () => parsePattern("workers.example/mcp#frag", "workers.local"),
    /query.*fragment.*not supported/
  );
});

test("parsePattern: rejects http:// / https:// scheme prefix", () => {
  assert.throws(
    () => parsePattern("https://api.workers.example/*", "workers.local"),
    /scheme prefix not supported/
  );
  assert.throws(
    () => parsePattern("http://workers.example/mcp", "workers.local"),
    /scheme prefix not supported/
  );
  assert.throws(
    () => parsePattern("HTTPS://Workers.Example/api/*", "workers.local"),
    /scheme prefix not supported/
  );
});

// ---- parseRoutes ----

test("parseRoutes: parses array", () => {
  const out = parseRoutes(
    ["workers.example/*", "api.workers.example/v1/*"],
    "workers.local"
  );
  assert.equal(out.length, 2);
  assert.deepEqual(out[0], {
    host: "workers.example", slot: "/*", kind: "prefix", value: "/",
  });
  assert.deepEqual(out[1], {
    host: "api.workers.example", slot: "/v1/*", kind: "prefix", value: "/v1/",
  });
});

test("parseRoutes: null/undefined → empty list", () => {
  assert.deepEqual(parseRoutes(null, "workers.local"), []);
  assert.deepEqual(parseRoutes(undefined, "workers.local"), []);
});

test("parseRoutes: rejects non-array", () => {
  assert.throws(() => parseRoutes("workers.example/*", "workers.local"), /must be an array/);
});

test("parseRoutes: rejects duplicates (post-normalization)", () => {
  assert.throws(
    () => parseRoutes(["WORKERS.example/api/*", "workers.example/api/*"], "workers.local"),
    /duplicate pattern/
  );
});

// ---- parseHostList ----

test("parseHostList: normalizes and dedupes", () => {
  assert.deepEqual(
    parseHostList(["Workers.example", "workers.example:8080", "api.workers.example"], "workers.local"),
    ["workers.example", "api.workers.example"]
  );
});

test("parseHostList: rejects platform-domain entries", () => {
  assert.throws(
    () => parseHostList(["demo.workers.local"], "workers.local"),
    /inside the platform domain/
  );
});

test("parseHostList: rejects non-array", () => {
  assert.throws(() => parseHostList("workers.example", "workers.local"), /must be an array/);
});

test("parseCronList: null/undefined → []", () => {
  assert.deepEqual(parseCronList(null), []);
  assert.deepEqual(parseCronList(undefined), []);
});

test("parseCronList: defaults timezone to UTC", () => {
  const out = parseCronList([{ cron: "*/5 * * * *" }]);
  assert.deepEqual(out, [{ cron: "*/5 * * * *", timezone: "UTC" }]);
});

test("parseCronList: passes through explicit timezone", () => {
  const out = parseCronList([{ cron: "0 9 * * *", timezone: "Asia/Shanghai" }]);
  assert.deepEqual(out, [{ cron: "0 9 * * *", timezone: "Asia/Shanghai" }]);
});

test("parseCronList: dedupes on (cron, timezone) pair", () => {
  const out = parseCronList([
    { cron: "*/5 * * * *" },
    { cron: "*/5 * * * *", timezone: "UTC" },
    { cron: "*/5 * * * *", timezone: "Asia/Shanghai" },
  ]);
  assert.equal(out.length, 2);
});

test("parseCronList: invalid cron rejected", () => {
  assert.throws(() => parseCronList([{ cron: "not a cron" }]), /invalid expression/);
});

test("parseCronList: rejects seconds and nickname syntax", () => {
  assert.throws(
    () => parseCronList([{ cron: "* * * * * *" }]),
    /exactly 5 fields/
  );
  assert.throws(
    () => parseCronList([{ cron: "@daily" }]),
    /exactly 5 fields/
  );
});

test("parseCronList: rejects date-like fields before croner one-shot parsing", () => {
  assert.throws(
    () => parseCronList([{ cron: "9999:9 * * * *" }]),
    /must not contain ':'/
  );
});

test("parseCronList: unknown timezone rejected", () => {
  assert.throws(
    () => parseCronList([{ cron: "*/5 * * * *", timezone: "Mars/Olympus" }]),
    /unknown timezone/
  );
});

test("parseCronList: empty cron rejected", () => {
  assert.throws(() => parseCronList([{ cron: "   " }]), /non-empty string "cron"/);
});

test("parseCronList: entry must be an object and timezone must be a non-empty string", () => {
  assert.throws(() => parseCronList(["*/5 * * * *"]), /entry must be an object/);
  assert.throws(
    () => parseCronList([{ cron: "*/5 * * * *", timezone: "" }]),
    /timezone" must be a non-empty string/
  );
});

test("parseCronList: non-array rejected", () => {
  assert.throws(() => parseCronList({}), /crons must be an array/);
});

test(`parseCronList: >${MAX_CRONS_PER_WORKER} entries rejected`, () => {
  const tooMany = Array.from({ length: MAX_CRONS_PER_WORKER + 1 }, (_, i) => ({
    cron: `${i} * * * *`,
  }));
  assert.throws(() => parseCronList(tooMany), new RegExp(`max ${MAX_CRONS_PER_WORKER}`));
});

test("parseCronList: limit is enforced AFTER dedup (duplicates don't count)", () => {
  const dup = Array.from({ length: MAX_CRONS_PER_WORKER + 1 }, () => ({
    cron: "*/5 * * * *",
  }));
  const out = parseCronList(dup);
  assert.equal(out.length, 1);
});

test("parseCronList: trims cron whitespace", () => {
  const out = parseCronList([{ cron: "  */5 * * * *  " }]);
  assert.equal(out[0].cron, "*/5 * * * *");
});

// --- parseQueueConsumers ---

test("parseQueueConsumers: null/undefined → []", () => {
  assert.deepEqual(parseQueueConsumers(null), []);
  assert.deepEqual(parseQueueConsumers(undefined), []);
});

test("parseQueueConsumers: defaults batch/retries when omitted", () => {
  const out = parseQueueConsumers([{ queue: "orders" }]);
  assert.deepEqual(out, [{
    queue: "orders",
    maxBatchSize: 10,
    maxBatchTimeoutMs: 5000,
    maxRetries: 3,
  }]);
});

test("parseQueueConsumers: all fields preserved", () => {
  const out = parseQueueConsumers([{
    queue: "events",
    maxBatchSize: 50,
    maxBatchTimeoutMs: 30000,
    maxRetries: 5,
    retryDelaySeconds: 45,
    deadLetterQueue: "events-dlq",
  }]);
  assert.equal(out[0].queue, "events");
  assert.equal(out[0].maxBatchSize, 50);
  assert.equal(out[0].maxBatchTimeoutMs, 30000);
  assert.equal(out[0].maxRetries, 5);
  assert.equal(out[0].retryDelaySeconds, 45);
  assert.equal(out[0].deadLetterQueue, "events-dlq");
});

test("parseQueueConsumers: rejects out-of-range values", () => {
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxBatchSize: 0 }]),
    /maxBatchSize/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxBatchSize: MAX_BATCH_SIZE + 1 }]),
    /maxBatchSize/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxBatchTimeoutMs: -1 }]),
    /maxBatchTimeoutMs/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxBatchTimeoutMs: MAX_BATCH_TIMEOUT_MS + 1 }]),
    /maxBatchTimeoutMs/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxRetries: -1 }]),
    /maxRetries/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", maxRetries: MAX_RETRIES + 1 }]),
    /maxRetries/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", retryDelaySeconds: 86_401 }]),
    /retryDelaySeconds/
  );
});

test("parseQueueConsumers: rejects DLQ == source", () => {
  assert.throws(
    () => parseQueueConsumers([{ queue: "q", deadLetterQueue: "q" }]),
    /differ from source/
  );
});

test("parseQueueConsumers: rejects duplicate queue", () => {
  assert.throws(
    () => parseQueueConsumers([{ queue: "q" }, { queue: "q" }]),
    /duplicate consumer/
  );
});

test("parseQueueConsumers: rejects missing / malformed queue name", () => {
  assert.throws(() => parseQueueConsumers([{}]), /queue must match/);
  assert.throws(() => parseQueueConsumers([{ queue: "" }]), /queue must match/);
  assert.throws(() => parseQueueConsumers([{ queue: "Bad:Name" }]), /queue must match/);
});

test("parseQueueConsumers: entry must be an object and deadLetterQueue must be queue-shaped", () => {
  assert.throws(() => parseQueueConsumers(["orders"]), /entry must be an object/);
  assert.throws(
    () => parseQueueConsumers([{ queue: "orders", deadLetterQueue: "" }]),
    /deadLetterQueue must match/
  );
  assert.throws(
    () => parseQueueConsumers([{ queue: "orders", deadLetterQueue: "Bad:Q" }]),
    /deadLetterQueue must match/
  );
});

test("parseQueueConsumers: rejects non-array", () => {
  assert.throws(() => parseQueueConsumers({}), /must be an array/);
});

test("parseAllowedCallers: null / undefined → null (meta stays absent)", () => {
  assert.strictEqual(parseAllowedCallers(null), null);
  assert.strictEqual(parseAllowedCallers(undefined), null);
});

test("parseAllowedCallers: valid list preserved, dedup kept in declaration order", () => {
  assert.deepEqual(parseAllowedCallers(["acme", "beta", "acme"]), ["acme", "beta"]);
});

test("parseAllowedCallers: wildcard accepted alongside ns entries", () => {
  assert.deepEqual(parseAllowedCallers(["*"]), ["*"]);
  assert.deepEqual(parseAllowedCallers(["acme", "*"]), ["acme", "*"]);
});

test("parseAllowedCallers: rejects reserved tenant names", () => {
  assert.throws(() => parseAllowedCallers(["admin"]), /reserved tenant name/);
  assert.throws(() => parseAllowedCallers(["foo", "admin"]), /reserved tenant name/);
  assert.deepEqual(parseAllowedCallers(["*"]), ["*"]);
  assert.deepEqual(parseAllowedCallers(["foo", "bar"]), ["foo", "bar"]);
});

test("parseAllowedCallers: rejects non-array", () => {
  assert.throws(() => parseAllowedCallers("*"), /must be an array/);
  assert.throws(() => parseAllowedCallers({}), /must be an array/);
});

test("parseAllowedCallers: rejects non-string / illegal ns tokens", () => {
  assert.throws(() => parseAllowedCallers([""]), /must be "\*" or match/);
  assert.throws(() => parseAllowedCallers(["Bad_NS"]), /must be "\*" or match/);
  assert.throws(() => parseAllowedCallers([123]), /must be "\*" or match/);
});

test("evaluateServiceBindingAcl: same-ns bypasses ACL entirely (even when list absent)", () => {
  assert.doesNotThrow(() => evaluateServiceBindingAcl("acme", "acme", "svc", undefined));
  assert.doesNotThrow(() => evaluateServiceBindingAcl("acme", "acme", "svc", []));
});

test("evaluateServiceBindingAcl: cross-ns requires the caller's ns (or wildcard) in the list", () => {
  assert.doesNotThrow(() =>
    evaluateServiceBindingAcl("acme", "platform", "billing", ["acme", "beta"])
  );
  assert.doesNotThrow(() =>
    evaluateServiceBindingAcl("stranger", "platform", "billing", ["*"])
  );
});

test("evaluateServiceBindingAcl: cross-ns with absent export ACL → deny", () => {
  assert.throws(
    () => evaluateServiceBindingAcl("acme", "platform", "billing", undefined),
    /does not allow ns "acme"/
  );
});

test("evaluateServiceBindingAcl: cross-ns with empty allowedCallers → deny (explicit closed)", () => {
  assert.throws(
    () => evaluateServiceBindingAcl("acme", "platform", "billing", []),
    /does not allow ns "acme"/
  );
});

test("evaluateServiceBindingAcl: cross-ns with list lacking caller → deny", () => {
  assert.throws(
    () => evaluateServiceBindingAcl("acme", "platform", "billing", ["beta", "gamma"]),
    /does not allow ns "acme"/
  );
});

test("parseExports: null/undefined returns empty array", () => {
  assert.deepEqual(parseExports(undefined, { ns: "acme" }), []);
  assert.deepEqual(parseExports(null, { ns: "acme" }), []);
});

test("parseExports: non-platform entry with entrypoint + allowedCallers", () => {
  const out = parseExports(
    [{ entrypoint: "Public", allowedCallers: ["*"] }],
    { ns: "acme" }
  );
  assert.deepEqual(out, [{ entrypoint: "Public", allowedCallers: ["*"] }]);
});

test("parseExports: rejects runtime-reserved entrypoint names (__WdlAbort__ and other __Wdl*__)", () => {
  // Runtime injects __WdlAbort__ via the wrapper shim; user-declared
  // entrypoints with the same name silently shadow ours in the no-D1/R2
  // path, and emit a duplicate `export class __WdlAbort__` once host
  // bindings are present (cold-load 502). Reject at deploy ingress.
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "__WdlAbort__", allowedCallers: ["*"] }],
        { ns: "acme" }
      ),
    /reserved for runtime-injected/
  );
  // Conservative pattern — any `__Wdl<…>__` should be reserved so future
  // shims can land without re-litigating the surface.
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "__WdlSomething__", allowedCallers: ["*"] }],
        { ns: "acme" }
      ),
    /reserved for runtime-injected/
  );
  // Names that merely start with `__Wdl` but aren't the `__Wdl…__`
  // pattern remain user-controllable. (Defensive: ensure the regex
  // anchors on both ends.)
  assert.doesNotThrow(() =>
    parseExports(
      [{ entrypoint: "__WdlNotReserved", allowedCallers: ["*"] }],
      { ns: "acme" }
    ),
  );
});

test("parseExports: rejects names that cannot be generated as wrapper classes", () => {
  assert.throws(
    () => parseExports([{ entrypoint: "class", allowedCallers: ["*"] }], { ns: "acme" }),
    /valid JS class declaration name or "default"/
  );
});

test("parseExports: rejects duplicate entrypoint", () => {
  assert.throws(
    () =>
      parseExports(
        [
          { entrypoint: "Public", allowedCallers: ["*"] },
          { entrypoint: "Public", allowedCallers: ["*"] },
        ],
        { ns: "acme" }
      ),
    /duplicate entrypoint/
  );
});

test("parseExports: default entrypoint is accepted as-is", () => {
  const out = parseExports(
    [{ entrypoint: "default", allowedCallers: ["*"] }],
    { ns: "acme" }
  );
  assert.equal(out[0].entrypoint, "default");
});

test("parseExports: missing allowedCallers → error", () => {
  assert.throws(
    () => parseExports([{ entrypoint: "Public" }], { ns: "acme" }),
    /allowedCallers is required/
  );
});

test("parseExports: non-platform-tier ns cannot declare `as`", () => {
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "Public", allowedCallers: ["*"], as: "DEMO" }],
        { ns: "acme" }
      ),
    /as is only allowed on platform-tier reserved namespaces/
  );
});

test("parseExports: non-platform-tier ns cannot declare requiredCallerSecrets", () => {
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "Public", allowedCallers: ["*"], requiredCallerSecrets: ["KEY"] }],
        { ns: "acme" }
      ),
    /requiredCallerSecrets is only allowed on platform-tier reserved namespaces/
  );
});

test("parseExports: platform ns requires `as` on every entry", () => {
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "NoAs", allowedCallers: ["*"] }],
        { ns: "__platform__" }
      ),
    /require "as" on every entry/
  );
});

test("parseExports: platform ns rejects duplicate `as`", () => {
  assert.throws(
    () =>
      parseExports(
        [
          { entrypoint: "A", as: "DEMO", allowedCallers: ["*"] },
          { entrypoint: "B", as: "DEMO", allowedCallers: ["*"] },
        ],
        { ns: "__platform__" }
      ),
    /duplicate as/
  );
});

test("parseExports: platform ns rejects bad PLATFORM_KEY_RE `as`", () => {
  assert.throws(
    () =>
      parseExports(
        [{ entrypoint: "A", as: "bad-name", allowedCallers: ["*"] }],
        { ns: "__platform__" }
      ),
    /as must match/
  );
});

test("parseExports: platform ns normalizes requiredCallerSecrets (dedup + grammar)", () => {
  const out = parseExports(
    [
      {
        entrypoint: "Echo",
        as: "DEMO",
        allowedCallers: ["*"],
        requiredCallerSecrets: ["KEY_A", "KEY_B", "KEY_A"],
      },
    ],
    { ns: "__platform__" }
  );
  assert.deepEqual(out[0].requiredCallerSecrets, ["KEY_A", "KEY_B"]);
});

test("parseExports: requiredCallerSecrets must match PLATFORM_KEY_RE", () => {
  assert.throws(
    () =>
      parseExports(
        [
          {
            entrypoint: "Echo",
            as: "DEMO",
            allowedCallers: ["*"],
            requiredCallerSecrets: ["lowercase"],
          },
        ],
        { ns: "__platform__" }
      ),
    /requiredCallerSecrets entries must match/
  );
});

test("parseExports: __pf2__ fixture (second platform-tier member) accepts `as`", () => {
  addPlatformTierFixture("__pf2__");
  try {
    const out = parseExports(
      [{ entrypoint: "Public", as: "DEMO_PF2", allowedCallers: ["*"] }],
      { ns: "__pf2__" }
    );
    assert.equal(out[0].as, "DEMO_PF2");
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("parseExports: __pf2__ fixture accepts requiredCallerSecrets", () => {
  addPlatformTierFixture("__pf2__");
  try {
    const out = parseExports(
      [{ entrypoint: "Public", as: "X", allowedCallers: ["*"], requiredCallerSecrets: ["KEY_A"] }],
      { ns: "__pf2__" }
    );
    assert.deepEqual(out[0].requiredCallerSecrets, ["KEY_A"]);
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("parseExports: __pf2__ fixture requires `as` on every entry", () => {
  addPlatformTierFixture("__pf2__");
  try {
    assert.throws(
      () => parseExports([{ entrypoint: "NoAs", allowedCallers: ["*"] }], { ns: "__pf2__" }),
      /require "as" on every entry/
    );
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("parseExports: tenant ns still rejects `as` even when __pf2__ is in the set", () => {
  // Adding a member to PLATFORM_TIER_RESERVED_NS must not weaken the
  // tenant-side constraint; `as` and `requiredCallerSecrets` are still
  // platform-tier-only.
  addPlatformTierFixture("__pf2__");
  try {
    assert.throws(
      () => parseExports(
        [{ entrypoint: "Public", as: "DEMO", allowedCallers: ["*"] }],
        { ns: "tenant-foo" }
      ),
      /platform-tier reserved namespaces/
    );
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("parsePlatformBindings: null/undefined returns empty array", () => {
  assert.deepEqual(parsePlatformBindings(undefined), []);
  assert.deepEqual(parsePlatformBindings(null), []);
});

test("parsePlatformBindings: defaults `platform` to `binding`", () => {
  const out = parsePlatformBindings([{ binding: "DEMO" }]);
  assert.deepEqual(out, [{ binding: "DEMO", platform: "DEMO" }]);
});

test("parsePlatformBindings: honors explicit `platform` for aliasing", () => {
  const out = parsePlatformBindings([{ binding: "PAYMENT", platform: "STRIPE" }]);
  assert.deepEqual(out, [{ binding: "PAYMENT", platform: "STRIPE" }]);
});

test("parsePlatformBindings: rejects bad grammar", () => {
  // Both binding and platform use PLATFORM_KEY_RE (SCREAMING_SNAKE) — the
  // user calls env.<binding> so binding is the stable platform-visible
  // identifier; treating it as part of the platform ABI matches CLI
  // validation and frees env.<binding> from looking like a regular var.
  assert.throws(
    () => parsePlatformBindings([{ binding: "NO-HYPHEN" }]),
    /binding must match/
  );
  assert.throws(
    () => parsePlatformBindings([{ binding: "lowercase" }]),
    /binding must match/
  );
  assert.throws(
    () => parsePlatformBindings([{ binding: "OK", platform: "not-upper" }]),
    /platform must match/
  );
});

test("parsePlatformBindings: rejects duplicate binding", () => {
  assert.throws(
    () =>
      parsePlatformBindings([
        { binding: "DEMO", platform: "DEMO" },
        { binding: "DEMO", platform: "OTHER" },
      ]),
    /duplicate binding/
  );
});

test("isAdminAcceptableNs: tenant grammar accepted", () => {
  for (const ok of ["demo", "ns-1", "a", "0123456789", "a-b-c", "a".repeat(63)]) {
    assert.ok(isAdminAcceptableNs(ok), `expected "${ok}" accepted`);
  }
});

test("isAdminAcceptableNs: explicit reserved ns accepted", () => {
  for (const ok of ["__platform__", "__system__", "__community__"]) {
    assert.ok(isAdminAcceptableNs(ok), `expected "${ok}" accepted`);
  }
});

test("isAdminAcceptableNs: rejects reserved tenant names", () => {
  // NS_RE accepts "admin"; the reservation lives one layer up.
  assert.equal(NS_RE.test("admin"), true);
  assert.equal(isAdminAcceptableNs("admin"), false);
  assert.equal(isAdminAcceptableNs("admin-prod"), true);
  assert.equal(isAdminAcceptableNs("administrator"), true);
  assert.equal(isAdminAcceptableNs("__system__"), true);
  assert.equal(isAdminAcceptableNs("foo"), true);
});

test("isAdminAcceptableNs: invalid shapes rejected", () => {
  for (const bad of [
    "",
    "-",
    "-ns",
    "ns-",
    "a".repeat(64),
    "UPPER",
    "with.dot",
    "ns_underscore",
    "ns space",
    "a/b",
    "__future",
    "__anything",
    "__platform__:platform-demo",
  ]) {
    assert.ok(!isAdminAcceptableNs(bad), `expected "${bad}" rejected`);
  }
  assert.ok(!isAdminAcceptableNs(null));
  assert.ok(!isAdminAcceptableNs(undefined));
  assert.ok(!isAdminAcceptableNs(42));
});

test("isAdminAcceptableNs rejects reserved namespace delimiter aliases", () => {
  const injected = "__platform__:platform-demo";
  assert.equal(
    `secrets:${injected}`,
    "secrets:__platform__:platform-demo",
    "this shape aliases the worker-secret key for __platform__/platform-demo"
  );
  assert.equal(isAdminAcceptableNs(injected), false);
});

test("projectAccessPrincipal returns only the public principal shape", () => {
  assert.deepEqual(projectAccessPrincipal({ kind: "ops", token: "secret", hash: "hash" }), {
    kind: "ops",
  });
  assert.deepEqual(projectAccessPrincipal({
    kind: "ns",
    ns: "demo",
    token: "secret",
    hash: "hash",
  }), {
    kind: "ns",
    ns: "demo",
  });
  assert.equal(projectAccessPrincipal({ kind: "ns", token: "secret" }), null);
  assert.equal(projectAccessPrincipal(null), null);
});

test("configuredHostname accepts plain hosts and rejects URL injection shapes", () => {
  assert.equal(configuredHostname(" Workers.Local. "), "workers.local");
  assert.equal(configuredHostname("workers.local."), "workers.local");
  assert.equal(
    configuredHostname(`${"a".repeat(63)}.${"b".repeat(58)}.com`),
    `${"a".repeat(63)}.${"b".repeat(58)}.com`
  );
  for (const bad of [
    "",
    "  ",
    "https://workers.local",
    "workers.local/path",
    "workers.local:8080",
    "workers local",
    "workers.example#oops",
    "workers.example?query",
    "workers@example",
    "workers..example",
    ".workers.example",
    "workers.example..",
    "-workers.example",
    "workers-.example",
    "wörkers.example",
    "K.example",
    "workers.123",
    `${"a".repeat(64)}.example`,
    `${"a".repeat(63)}.${"b".repeat(59)}.com`,
    `${"a".repeat(250)}.com`,
    null,
  ]) {
    assert.equal(configuredHostname(bad), null, `expected ${JSON.stringify(bad)} rejected`);
  }
});

test("platformDomainFromEnv normalizes configured values and owns the default", () => {
  assert.equal(platformDomainFromEnv({}), "workers.local");
  assert.equal(platformDomainFromEnv({ PLATFORM_DOMAIN: " Workers.Example. " }), "workers.example");
  assert.throws(
    () => platformDomainFromEnv({ PLATFORM_DOMAIN: "workers.example:8443" }),
    /ALB-compatible ASCII DNS hostname/
  );
});

test("configuredPublicUrl returns a safe absolute base URL hint", () => {
  assert.equal(configuredPublicUrl(" https://assets.example/base/?token=secret#frag "), "https://assets.example/base");
  assert.equal(configuredPublicUrl("http://assets.example/"), "http://assets.example");
  for (const bad of [
    "",
    "assets.example",
    "javascript:alert(1)",
    "ftp://assets.example",
    "https://user:pass@assets.example",
    null,
  ]) {
    assert.equal(configuredPublicUrl(bad), null, `expected ${JSON.stringify(bad)} rejected`);
  }
});

test("platformVersionFromPackageJson derives WDL version from bundled workerd dependency", () => {
  assert.equal(
    platformVersionFromPackageJson(JSON.stringify({ dependencies: { workerd: "^1.20260531.1" } })),
    "wdl.20260531.1"
  );
  assert.equal(
    platformVersionFromPackageJson(JSON.stringify({ dependencies: { workerd: "~1.20260601.2" } })),
    "wdl.20260601.2"
  );
  assert.equal(platformVersionFromPackageJson("{"), "wdl.unknown");
  assert.equal(platformVersionFromPackageJson(JSON.stringify({ dependencies: {} })), "wdl.unknown");
  assert.equal(
    platformVersionFromPackageJson(JSON.stringify({ dependencies: { workerd: "^2.20260531.1" } })),
    "wdl.unknown"
  );
  assert.equal(
    platformVersionFromPackageJson(JSON.stringify({ dependencies: { workerd: "1.20260531" } })),
    "wdl.unknown"
  );
});

test("parseWorkerdDependencyVersion owns the bundled dependency grammar", () => {
  assert.deepEqual(
    parseWorkerdDependencyVersion(JSON.stringify({ dependencies: { workerd: "^1.20260531.12" } })),
    { version: "1.20260531.12", year: 2026, month: 5, day: 31, patch: 12 }
  );
  for (const source of [
    "{",
    JSON.stringify({ dependencies: {} }),
    JSON.stringify({ dependencies: { workerd: "2.20260531.1" } }),
    JSON.stringify({ dependencies: { workerd: "1.20260531" } }),
  ]) {
    assert.equal(parseWorkerdDependencyVersion(source), null);
  }
});

test("normalizeWorkflows: validates deploy-wire workflow declarations", () => {
  assert.deepEqual(normalizeWorkflows(undefined), []);
  assert.deepEqual(normalizeWorkflows([
    { name: "order-workflow", binding: "ORDER_WORKFLOW", className: "OrderWorkflow" },
    { name: "My_Workflow2", binding: "WF2", class_name: "MyWorkflow" },
  ]), [
    { name: "order-workflow", binding: "ORDER_WORKFLOW", className: "OrderWorkflow" },
    { name: "My_Workflow2", binding: "WF2", className: "MyWorkflow" },
  ]);
  assert.throws(
    () => normalizeWorkflows([{ name: "bad:name", binding: "WF", className: "Flow" }]),
    /entry\.name must match/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "constructor", binding: "WF", className: "Flow" }]),
    /reserved Object\.prototype key/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "flow", binding: "__WDL_WORKFLOWS_BACKEND__", className: "Flow" }]),
    /reserved for runtime-internal bindings/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "flow", binding: "WF", className: "bad-class" }]),
    /className must be a valid JS class declaration name/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "flow", binding: "WF", className: "class" }]),
    /className must be a valid JS class declaration name/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "flow", binding: "WF", className: "__WdlAbort__" }]),
    /className is reserved for runtime-injected entrypoints/
  );
  assert.throws(
    () => normalizeWorkflows([{ name: "flow", binding: "WF", className: "Flow", scriptName: "other" }]),
    /script_name is not supported/
  );
});

test("prepareBundle: workflows land in meta only when non-empty", () => {
  const modules = { "worker.js": "export default {}" };
  const withWorkflows = prepareBundle("worker.js", modules, {
    workflows: [{ name: "order-workflow", binding: "ORDER_WORKFLOW", className: "OrderWorkflow" }],
  });
  assert.deepEqual(withWorkflows.meta.workflows, [
    { name: "order-workflow", binding: "ORDER_WORKFLOW", className: "OrderWorkflow" },
  ]);
  const absent = prepareBundle("worker.js", modules, { workflows: [] });
  assert.equal(absent.meta.workflows, undefined);
  assert.throws(
    () => prepareBundle("worker.js", modules, {
      bindings: { FLOW: { type: "kv", id: "sessions" } },
      workflows: [{ name: "flow", binding: "FLOW", className: "Flow" }],
    }),
    /binding collides with another binding/
  );
});

// --- linkServiceBinding ---

/**
 * @param {{ versions?: Record<string, any>, metas?: Record<string, any>, metaThrows?: Error | null }} [opts]
 */
function makeLookups({ versions = {}, metas = {}, metaThrows = null } = {}) {
  const lookupTargetVersion = async (/** @type {string} */ ns, /** @type {string} */ worker) =>
    versions[`${ns}/${worker}`] ?? null;
  const lookupTargetMeta = async (/** @type {string} */ ns, /** @type {string} */ worker, /** @type {string} */ version) => {
    if (metaThrows) throw metaThrows;
    return metas[`${ns}/${worker}/${version}`] ?? null;
  };
  return { lookupTargetVersion, lookupTargetMeta };
}

/**
 * @param {() => Promise<unknown>} fn
 * @param {number} status
 * @param {string} [messageIncludes]
 * @param {string} [code]
 */
async function expectLinkError(fn, status, messageIncludes, code) {
  let threw = /** @type {any} */ (null);
  try { await fn(); }
  catch (err) { threw = err; }
  assert.ok(threw, "expected LinkError, got no throw");
  assert.ok(threw instanceof LinkError,
    `expected LinkError, got ${threw?.name}: ${threw?.message}`);
  assert.equal(threw.status, status);
  if (code) assert.equal(threw.code, code);
  if (messageIncludes) {
    assert.ok(threw.message.includes(messageIncludes),
      `expected message to include ${JSON.stringify(messageIncludes)}, got ${JSON.stringify(threw.message)}`);
  }
}

/**
 * @param {() => unknown} fn
 * @param {number} status
 * @param {string} [messageIncludes]
 * @param {string} [code]
 */
function expectLinkErrorSync(fn, status, messageIncludes, code) {
  let threw = /** @type {any} */ (null);
  try { fn(); }
  catch (err) { threw = err; }
  assert.ok(threw, "expected LinkError, got no throw");
  assert.ok(threw instanceof LinkError,
    `expected LinkError, got ${threw?.name}: ${threw?.message}`);
  assert.equal(threw.status, status);
  if (code) assert.equal(threw.code, code);
  if (messageIncludes) {
    assert.ok(threw.message.includes(messageIncludes),
      `expected message to include ${JSON.stringify(messageIncludes)}, got ${JSON.stringify(threw.message)}`);
  }
}

test("linkServiceBinding: caller-set requiredCallerSecrets → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", requiredCallerSecrets: ["X"] },
      ...makeLookups(),
    }),
    400, "requiredCallerSecrets is set by the");
});

test("linkServiceBinding: missing service string → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "" },
      ...makeLookups(),
    }),
    400, "requires non-empty");
});

test("linkServiceBinding: invalid ns shape → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "BAD NS" },
      ...makeLookups(),
    }),
    400, "ns must match");
});

test("linkServiceBinding: platform-tier ns target rejected → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "w", ns: "__platform__" },
      ...makeLookups(),
    }),
    400, "must be addressed via [[platform_bindings]]");
});

test("linkServiceBinding: self-target (same ns + same name) rejected → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "c" },
      ...makeLookups(),
    }),
    400, "cannot target self", "service_binding_self_target");
});

test("linkServiceBinding: cross-ns same-name is legitimate (not self-target)", async () => {
  const spec = { type: "service", service: "c", ns: "other" };
  await linkServiceBinding({
    callerNs: "caller", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({
      versions: { "other/c": "v3" },
      metas: { "other/c/v3": {
        exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
      } },
    }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v3");
});

test("linkServiceBinding: target has no active version → 409", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "gone", ns: "other" },
      ...makeLookups(),
    }),
    409, "has no active version", "service_binding_target_inactive");
});

test("linkServiceBinding: target meta lookup throws → 502", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metaThrows: new Error("redis blew up"),
      }),
    }),
    502, "failed to read target meta: redis blew up", "service_binding_target_meta_unavailable");
});

test("linkServiceBinding: preserves target metadata domain errors", async () => {
  const corruptMeta = new LinkError(500, "corrupt_meta", "Corrupt __meta__ for other/t/v1");
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metaThrows: corruptMeta,
      }),
    }),
    500, "Corrupt __meta__ for other/t/v1", "corrupt_meta");
});

test("linkServiceBinding: rejects runtime-reserved entrypoint at link time (defense in depth)", async () => {
  // Even if a manually-crafted binding makes it past validateBindings
  // (e.g. direct Redis writes during ops recovery), the linker is the
  // last gate before commit. A binding pointing at a __Wdl…__ name must
  // 400 here too — otherwise the caller could route through the
  // runtime's injected abort shim and tear down the target's isolate.
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other", entrypoint: "__WdlAbort__" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {} },
      }),
    }),
    400,
    "is reserved for runtime-injected entrypoints",
    "service_binding_entrypoint_reserved",
  );
});

test("linkServiceBinding: rejects non-identifier entrypoint at link time (defense in depth)", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other", entrypoint: "1Bad" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {} },
      }),
    }),
    400,
    "entrypoint must be a JS identifier",
    "service_binding_invalid_entrypoint",
  );
});

test("linkServiceBinding: strict-mode exports — unlisted entrypoint → 400", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other", entrypoint: "Unlisted" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {
          exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
        } },
      }),
    }),
    400, 'entrypoint "Unlisted" not exported', "service_binding_entrypoint_not_exported");
});

test("linkServiceBinding: strict-mode per-entrypoint allowed_callers authorizes matching entrypoint", async () => {
  const spec = { type: "service", service: "t", ns: "other", entrypoint: "Api" };
  await linkServiceBinding({
    callerNs: "caller", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({
      versions: { "other/t": "v7" },
      metas: { "other/t/v7": {
        exports: [{ entrypoint: "Api", allowedCallers: ["caller"] }],
      } },
    }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v7");
});

test("linkServiceBinding: target without exports does not expose named entrypoints", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other", entrypoint: "Room" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {
          bindings: {
            ROOMS: { type: "do", className: "Room" },
          },
        } },
      }),
    }),
    400,
    'entrypoint "Room" not exported',
    "service_binding_entrypoint_not_exported",
  );
});

test("linkServiceBinding: same-ns default entrypoint binds target without exports", async () => {
  const spec = { type: "service", service: "neighbor" };
  await linkServiceBinding({
    callerNs: "caller", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({
      versions: { "caller/neighbor": "v2" },
      metas: { "caller/neighbor/v2": {} },
    }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v2");
});

test("linkServiceBinding: cross-ns default entrypoint requires explicit export", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {} },
      }),
    }),
    403,
    'declare [[exports]] entrypoint "default"',
    "service_binding_acl_denied",
  );
});

test("linkServiceBinding: cross-ns wildcard export allowedCallers → passes", async () => {
  const spec = { type: "service", service: "t", ns: "other" };
  await linkServiceBinding({
    callerNs: "caller", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({
      versions: { "other/t": "v1" },
      metas: { "other/t/v1": {
        exports: [{ entrypoint: "default", allowedCallers: ["*"] }],
      } },
    }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v1");
});

test("linkServiceBinding: cross-ns export ACL without caller → 403", async () => {
  await expectLinkError(
    () => linkServiceBinding({
      callerNs: "caller", callerName: "c", bindingName: "T",
      spec: { type: "service", service: "t", ns: "other" },
      ...makeLookups({
        versions: { "other/t": "v1" },
        metas: { "other/t/v1": {
          exports: [{ entrypoint: "default", allowedCallers: ["someone-else"] }],
        } },
      }),
    }),
    403, 'does not allow ns "caller"', "service_binding_acl_denied");
});

test("linkServiceBinding: entrypoint omitted defaults to 'default' for exports match", async () => {
  const spec = { type: "service", service: "t", ns: "other" };
  await linkServiceBinding({
    callerNs: "caller", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({
      versions: { "other/t": "v1" },
      metas: { "other/t/v1": {
        exports: [{ entrypoint: "default", allowedCallers: ["caller"] }],
      } },
    }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v1");
});

test("linkServiceBinding: null targetMeta coerced to empty same-ns default target", async () => {
  const spec = { type: "service", service: "t", ns: "other" };
  await linkServiceBinding({
    callerNs: "other", callerName: "c", bindingName: "T",
    spec,
    ...makeLookups({ versions: { "other/t": "v1" }, metas: {} }),
  });
  assert.equal(/** @type {any} */ (spec).version, "v1");
});

test("linkServiceBinding: __pf2__ target also rejected (set-based, not literal)", async () => {
  addPlatformTierFixture("__pf2__");
  try {
    /** @type {any} */
    let threw;
    try {
      await linkServiceBinding({
        callerNs: "caller", callerName: "c", bindingName: "T",
        spec: { type: "service", service: "w", ns: "__pf2__" },
        ...makeLookups(),
      });
    } catch (err) { threw = err; }
    assert.ok(threw, "expected LinkError");
    assert.equal(threw.status, 400);
    assert.ok(threw.message.includes("platform-tier reserved namespace"),
      `expected message to mention platform-tier reserved namespace, got ${threw.message}`);
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

// --- linkPlatformBinding ---

const PLATFORM_EXPORT_ECHO = {
  ns: "__platform__",
  worker: "platform-demo",
  version: "v5",
  entrypoint: "Echo",
  as: "demo",
  allowedCallers: ["*"],
  requiredCallerSecrets: [],
};

const PLATFORM_EXPORT_OPS = {
  ns: "__platform__",
  worker: "platform-demo",
  version: "v5",
  entrypoint: "Ops",
  as: "demo-ops",
  allowedCallers: ["tenant-a"],
  requiredCallerSecrets: ["API_KEY"],
};

test("linkPlatformBinding: name collision with existing binding → 400", () => {
  expectLinkErrorSync(
    () => linkPlatformBinding({
      callerNs: "tenant-a",
      bindingReq: { binding: "DEMO", platform: "demo" },
      existingBindings: { DEMO: { type: "service" } },
      platformExports: [PLATFORM_EXPORT_ECHO],
      availableCallerSecrets: new Set(),
    }),
    400, "binding name collides", "platform_binding_name_collision");
});

test("linkPlatformBinding: unknown `as` → 400", () => {
  expectLinkErrorSync(
    () => linkPlatformBinding({
      callerNs: "tenant-a",
      bindingReq: { binding: "X", platform: "not-registered" },
      existingBindings: {},
      platformExports: [PLATFORM_EXPORT_ECHO],
      availableCallerSecrets: new Set(),
    }),
    400, 'platform "not-registered" not registered', "platform_binding_not_registered");
});

test("linkPlatformBinding: ACL reject on per-entrypoint allowed_callers → 403", () => {
  expectLinkErrorSync(
    () => linkPlatformBinding({
      callerNs: "outsider",
      bindingReq: { binding: "OPS", platform: "demo-ops" },
      existingBindings: {},
      platformExports: [PLATFORM_EXPORT_OPS],
      availableCallerSecrets: new Set(["API_KEY"]),
    }),
    403, 'does not allow ns "outsider"', "platform_binding_acl_denied");
});

test("linkPlatformBinding: wildcard allowed_callers — any caller passes", () => {
  const { expanded, warning } = linkPlatformBinding({
    callerNs: "anyone",
    bindingReq: { binding: "DEMO", platform: "demo" },
    existingBindings: {},
    platformExports: [PLATFORM_EXPORT_ECHO],
    availableCallerSecrets: new Set(),
  });
  assert.equal(warning, undefined);
  assert.deepEqual(expanded, {
    type: "service",
    ns: "__platform__",
    service: "platform-demo",
    version: "v5",
    entrypoint: "Echo",
    // requiredCallerSecrets length 0 → omitted
  });
});

test("linkPlatformBinding: missing caller secrets → warning returned, expanded still built", () => {
  const { expanded, warning } = linkPlatformBinding({
    callerNs: "tenant-a",
    bindingReq: { binding: "OPS", platform: "demo-ops" },
    existingBindings: {},
    platformExports: [PLATFORM_EXPORT_OPS],
    availableCallerSecrets: new Set(),  // API_KEY missing
  });
  assert.deepEqual(warning, {
    binding: "OPS",
    platform: "demo-ops",
    missingCallerSecrets: ["API_KEY"],
  });
  assert.equal(expanded.ns, "__platform__");
  assert.equal(expanded.service, "platform-demo");
  assert.deepEqual(expanded.requiredCallerSecrets, ["API_KEY"]);
});

test("linkPlatformBinding: all required caller secrets present → no warning", () => {
  const { warning } = linkPlatformBinding({
    callerNs: "tenant-a",
    bindingReq: { binding: "OPS", platform: "demo-ops" },
    existingBindings: {},
    platformExports: [PLATFORM_EXPORT_OPS],
    availableCallerSecrets: new Set(["API_KEY", "UNRELATED"]),
  });
  assert.equal(warning, undefined);
});

test("linkPlatformBinding: entrypoint 'default' is NOT set on expanded shape", () => {
  const exp = {
    ns: "__platform__",
    worker: "platform-demo",
    version: "v5",
    entrypoint: "default",
    as: "demo-default",
    allowedCallers: ["*"],
    requiredCallerSecrets: [],
  };
  const { expanded } = linkPlatformBinding({
    callerNs: "anyone",
    bindingReq: { binding: "X", platform: "demo-default" },
    existingBindings: {},
    platformExports: [exp],
    availableCallerSecrets: new Set(),
  });
  assert.equal(expanded.entrypoint, undefined);
});

test("linkPlatformBinding: requiredCallerSecrets absent on expanded when target declares none", () => {
  const { expanded } = linkPlatformBinding({
    callerNs: "anyone",
    bindingReq: { binding: "DEMO", platform: "demo" },
    existingBindings: {},
    platformExports: [PLATFORM_EXPORT_ECHO],
    availableCallerSecrets: new Set(),
  });
  assert.equal(expanded.requiredCallerSecrets, undefined);
});

// --- formatReferrerBlocker: platform principal double-pin ---

test("formatReferrerBlocker: ops sees full referrer", () => {
  const raw = [
    encodeReferrerMember({
      callerNs: "tenant-a", callerWorker: "app", callerVersion: "v1", binding: "STRIPE",
    }),
    encodeReferrerMember({
      callerNs: "tenant-b", callerWorker: "app", callerVersion: "v2", binding: "STRIPE",
    }),
  ];
  const out = formatReferrerBlocker(raw, {
    targetNs: "__platform__",
    principal: { kind: "ops" },
  });
  assert.equal(out.referrers.length, 2);
  assert.equal(out.crossNamespaceReferrerCount, undefined);
});

test("formatReferrerBlocker: platform principal sees full referrer ONLY for own ns", () => {
  const raw = [
    encodeReferrerMember({
      callerNs: "tenant-a", callerWorker: "app", callerVersion: "v1", binding: "STRIPE",
    }),
    encodeReferrerMember({
      callerNs: "tenant-b", callerWorker: "app", callerVersion: "v2", binding: "STRIPE",
    }),
  ];
  const out = formatReferrerBlocker(raw, {
    targetNs: "__platform__",
    principal: { kind: "platform", ns: "__platform__" },
  });
  assert.equal(out.referrers.length, 2);
  assert.equal(out.crossNamespaceReferrerCount, undefined);
});

test("formatReferrerBlocker: platform principal redacts when target ns is a different platform-tier ns", () => {
  // `__community__` platform team must not see referrers of __platform__:
  // double-pin requires targetNs === principal.ns, NOT just kind === platform.
  addPlatformTierFixture("__pf2__");
  try {
    const raw = [
      encodeReferrerMember({
        callerNs: "tenant-a", callerWorker: "app", callerVersion: "v1", binding: "STRIPE",
      }),
    ];
    const out = formatReferrerBlocker(raw, {
      targetNs: "__platform__",
      principal: { kind: "platform", ns: "__pf2__" },
    });
    assert.equal(out.referrers.length, 0);
    assert.equal(out.crossNamespaceReferrerCount, 1);
  } finally {
    deletePlatformTierFixture("__pf2__");
  }
});

test("formatReferrerBlocker: malformed platform principal does not bypass the double pin", () => {
  const raw = [
    encodeReferrerMember({
      callerNs: "tenant-a", callerWorker: "app", callerVersion: "v1", binding: "STRIPE",
    }),
    encodeReferrerMember({
      callerNs: "tenant-b", callerWorker: "app", callerVersion: "v1", binding: "STRIPE",
    }),
  ];
  const out = formatReferrerBlocker(raw, {
    targetNs: "tenant-a",
    principal: { kind: "platform", ns: "tenant-a" },
  });
  assert.equal(out.referrers.length, 1);
  assert.equal(out.crossNamespaceReferrerCount, 1);

  const platformTarget = formatReferrerBlocker(raw, {
    targetNs: "__platform__",
    principal: { kind: "platform", ns: "tenant-a" },
  });
  assert.equal(platformTarget.referrers.length, 0);
  assert.equal(platformTarget.crossNamespaceReferrerCount, 2);
});

test("formatReferrerBlocker: ns principal redacts cross-ns to a count", () => {
  const raw = [
    encodeReferrerMember({
      callerNs: "tenant-a", callerWorker: "app", callerVersion: "v1", binding: "X",
    }),
    encodeReferrerMember({
      callerNs: "tenant-b", callerWorker: "other", callerVersion: "v1", binding: "Y",
    }),
  ];
  const out = formatReferrerBlocker(raw, {
    targetNs: "tenant-a",
    principal: { kind: "ns", ns: "tenant-a" },
  });
  assert.equal(out.referrers.length, 1);
  assert.equal(out.referrers[0].callerNs, "tenant-a");
  assert.equal(out.crossNamespaceReferrerCount, 1);
});

// --- log tail resume id grammar -----------------------------------------

test("isValidResumeId accepts <ms>-<seq> shape", () => {
  // canonical Redis stream ids
  assert.equal(isValidResumeId("0-0"), true);
  assert.equal(isValidResumeId("1-0"), true);
  assert.equal(isValidResumeId("1700000000000-0"), true);
  assert.equal(isValidResumeId("1700000000000-9999"), true);
  // largest unsigned 64-bit component — BigInt-safe path
  assert.equal(isValidResumeId("1700000000000-99999999999999999999"), true);
});

test("isValidResumeId rejects sentinels and malformed shapes", () => {
  // Spec sentinels would silently mean "from now" — must be rejected so
  // a `--since` typo doesn't defeat resume.
  for (const bad of ["$", "+", "-", ">", "*"]) {
    assert.equal(isValidResumeId(bad), false, `must reject sentinel ${JSON.stringify(bad)}`);
  }
  // shape errors
  for (const bad of [
    "", "1", "1-", "-1", "abc", "1.0-0", "01-0", "1-01",
    "1- 0", " 1-0", "1-0 ", "1--0", "1-0-0",
    "1700000000000-999999999999999999999",
    "999999999999999999999-0",
  ]) {
    assert.equal(isValidResumeId(bad), false, `must reject ${JSON.stringify(bad)}`);
  }
  // type errors
  for (const bad of [null, undefined, 123, {}, []]) {
    assert.equal(isValidResumeId(bad), false);
  }
});

test("compareStreamIds orders ms first then seq with BigInt safety", () => {
  // ms component
  assert.equal(compareStreamIds("1-0", "2-0") < 0, true);
  assert.equal(compareStreamIds("2-0", "1-0") > 0, true);
  // seq component (same ms)
  assert.equal(compareStreamIds("1-0", "1-1") < 0, true);
  assert.equal(compareStreamIds("1-5", "1-2") > 0, true);
  // equal
  assert.equal(compareStreamIds("1-0", "1-0"), 0);
  // BigInt-safe (>2^53)
  assert.equal(compareStreamIds("9007199254740993-0", "9007199254740994-0") < 0, true);
  assert.equal(compareStreamIds("1-9007199254740993", "1-9007199254740994") < 0, true);
});

// --- PLATFORM_TIER_RESERVED_NS shape sanity (lock at file end) ---

const PLATFORM_NS_SHAPE_RE = /^__[a-z0-9_]+__$/;
test("PLATFORM_TIER_RESERVED_NS members are __<name>__ shaped + not __system__ + not RESERVED_TENANT_NS", () => {
  for (const ns of PLATFORM_TIER_RESERVED_NS) {
    assert.ok(RESERVED_NS.has(ns),
      `platform-tier member "${ns}" must also be listed in RESERVED_NS`);
    assert.ok(PLATFORM_NS_SHAPE_RE.test(ns),
      `member "${ns}" must match /^__[a-z0-9_]+__$/`);
    assert.notEqual(ns, "__system__",
      "__system__ is the control plane and must not be in PLATFORM_TIER_RESERVED_NS");
  }
});

test("PLATFORM_TIER_RESERVED_NS at suite end is exactly the baseline {__platform__}", () => {
  assert.deepEqual([...PLATFORM_TIER_RESERVED_NS].toSorted(), ["__platform__"],
    "no test forgot a try/finally cleanup of an injected member");
});
