import { test } from "node:test";
import assert from "node:assert/strict";
import {
  deleteGatewayInternalHeaders,
  escapeRegex,
  classifyHost,
  isCanonicalPatternHost,
  normalizeRequestHost,
  sortPatterns,
  matchPatternWithStats,
  matchPatternEntry,
} from "../../gateway/lib.js";
import { NS_PATTERN, isValidRouteNs } from "../../shared/ns-pattern.js";

test("deleteGatewayInternalHeaders removes fixed and prefixed private headers", () => {
  const headers = new Headers({
    "x-worker-id": "tenant:worker:v1",
    "x-worker-prefix": "/worker",
    "x-wdl-upstream-binding": "RUNTIME_USER",
    "x-wdl-future-private": "private",
    "x-request-id": "keep-request-id",
    "sec-websocket-protocol": "keep-protocol",
  });

  deleteGatewayInternalHeaders(headers);

  assert.deepEqual(Object.fromEntries(headers), {
    "sec-websocket-protocol": "keep-protocol",
    "x-request-id": "keep-request-id",
  });
});

/**
 * @param {any} sorted
 * @param {string} pathname
 * @param {string} [search]
 * @returns {any}
 */
function matchPattern(sorted, pathname, search = "") {
  return matchPatternWithStats(sorted, pathname, search).entry;
}

test("escapeRegex produces regex-safe literals", () => {
  for (const value of ["a.b", "a*b+c?", "[a](b){c}|d", "^start$", "back\\slash", "foo-bar", "a b"]) {
    const re = new RegExp(`^${escapeRegex(value)}$`);
    assert.equal(re.test(value), true, value);
    assert.equal(re.test(`${value}x`), false, value);
  }
});

test("classifyHost: subdomain match", () => {
  const r = classifyHost("demo.workers.local", "workers.local", NS_PATTERN);
  assert.deepEqual(r, { branch: "subdomain", namespace: "demo" });
});

test("classifyHost: non-subdomain falls to pattern branch", () => {
  const r = classifyHost("workers.example", "workers.local", NS_PATTERN);
  assert.deepEqual(r, { branch: "pattern", host: "workers.example" });
});

test("classifyHost: platform-domain apex is not a subdomain match", () => {
  const r = classifyHost("workers.local", "workers.local", NS_PATTERN);
  assert.equal(r.branch, "pattern");
});

test("classifyHost: nested subdomain of platform falls to pattern", () => {
  // a.b.workers.local doesn't match ^<ns>\.workers\.local$ since NS_PATTERN
  // is char-class (no dots). Gateway treats it as pattern-branch; admin is
  // responsible for refusing to write patterns under the platform domain.
  const r = classifyHost("a.b.workers.local", "workers.local", NS_PATTERN);
  assert.equal(r.branch, "pattern");
});

test("normalizeRequestHost: strips trailing dot(s)", () => {
  assert.equal(normalizeRequestHost("workers.example."), "workers.example");
  assert.equal(normalizeRequestHost("workers.example..."), "workers.example");
  assert.equal(normalizeRequestHost("workers.example"), "workers.example");
});

test("classifyHost: trailing-dot platform-domain still routes to subdomain", () => {
  const r = classifyHost("demo.workers.local.", "workers.local", NS_PATTERN);
  assert.deepEqual(r, { branch: "subdomain", namespace: "demo" });
});

test("classifyHost: trailing-dot pattern host normalized so cache hits a single key", () => {
  // WHATWG URL lowercases hostname before classifyHost runs in production;
  // here we pass a pre-lowercased value to isolate the trailing-dot strip.
  const r = classifyHost("workers.example.", "workers.local", NS_PATTERN);
  assert.equal(r.branch, "pattern");
  assert.equal(r.host, "workers.example");
});

test("isCanonicalPatternHost accepts only control-canonical host keys", () => {
  assert.equal(isCanonicalPatternHost("workers.example"), true);
  assert.equal(isCanonicalPatternHost("api.workers.example"), true);
  assert.equal(isCanonicalPatternHost("workers.example."), false);
  assert.equal(isCanonicalPatternHost("Workers.example"), false);
  assert.equal(isCanonicalPatternHost("workers.example:443"), false);
  assert.equal(isCanonicalPatternHost("workers.example/path"), false);
  assert.equal(isCanonicalPatternHost("example .com"), false);
  assert.equal(isCanonicalPatternHost("example\t.com"), false);
  assert.equal(isCanonicalPatternHost(""), false);
});

// ---- matchPatternEntry ----

test("matchPatternEntry: exact requires identity", () => {
  const e = { kind: "exact", value: "/mcp" };
  assert.equal(matchPatternEntry(e, "/mcp"), true);
  assert.equal(matchPatternEntry(e, "/mcp/foo"), false);
  assert.equal(matchPatternEntry(e, "/mcphello"), false);
});

test("matchPatternEntry: exact rejects query string (CF full-URL semantics)", () => {
  const e = { kind: "exact", value: "/mcp" };
  assert.equal(matchPatternEntry(e, "/mcp", ""), true);
  assert.equal(matchPatternEntry(e, "/mcp", "?x=1"), false);
});

test("matchPatternEntry: prefix matches startsWith (value ends with /)", () => {
  const e = { kind: "prefix", value: "/api/" };
  assert.equal(matchPatternEntry(e, "/api/"), true);
  assert.equal(matchPatternEntry(e, "/api/foo"), true);
  assert.equal(matchPatternEntry(e, "/api"), false);
  assert.equal(matchPatternEntry(e, "/apix"), false);
});

test("matchPatternEntry: prefix ignores query string", () => {
  const e = { kind: "prefix", value: "/api/" };
  assert.equal(matchPatternEntry(e, "/api/foo", "?x=1"), true);
});

test("matchPatternEntry: prefix '/' matches everything", () => {
  const e = { kind: "prefix", value: "/" };
  assert.equal(matchPatternEntry(e, "/"), true);
  assert.equal(matchPatternEntry(e, "/anything/here"), true);
});

test("matchPatternEntry: trailing-* glob without slash matches as startsWith", () => {
  // From parsePattern("/public*") — value has no trailing slash, so
  // /public, /public-v2, and /public/foo all match.
  const e = { kind: "prefix", value: "/public" };
  assert.equal(matchPatternEntry(e, "/public"), true);
  assert.equal(matchPatternEntry(e, "/public/foo"), true);
  assert.equal(matchPatternEntry(e, "/public-v2"), true);
  assert.equal(matchPatternEntry(e, "/private"), false);
});

// ---- sortPatterns + matchPattern ----

/**
 * @param {string} slot
 * @param {string} kind
 * @param {string} value
 */
function entry(slot, kind, value, ns = "a", worker = "w", version = "v1") {
  return [slot, { ns, worker, version, kind, value }];
}

/** @param {any} entries */
function sort(entries) {
  return sortPatterns(entries, isValidRouteNs);
}

test("sortPatterns: sorts by value length desc, exact > prefix on tie", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/*", "prefix", "/", "a", "root"),
    entry("/api/*", "prefix", "/api/", "a", "api"),
    entry("/api/v2/*", "prefix", "/api/v2/", "a", "api-v2"),
  ]));
  assert.deepEqual(sorted.map((e) => e.worker), ["api-v2", "api", "root"]);
});

test("sortPatterns: exact takes precedence over prefix at same length", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/*", "prefix", "/", "a", "site"),
    entry("/", "exact", "/", "a", "apex"),
  ]));
  assert.equal(sorted[0].worker, "apex");
});

test("sortPatterns: drops malformed values and surfaces them via errors", () => {
  const { sorted, errors } = sort({
    "/a": "not-a-projection",
    "/b": { ns: "x" },  // missing fields
    "/c/*": { ns: "x", worker: "y", version: "v1", kind: "prefix", value: "/c/" },
    "/d": { ns: "x", worker: "y", kind: "exact", value: "/d" },  // missing version
    "/e": { ns: "__platform__", worker: "y", version: "v1", kind: "exact", value: "/e" },
    "/f": { ns: "admin", worker: "y", version: "v1", kind: "exact", value: "/f" },
  });
  assert.equal(sorted.length, 1);
  assert.equal(sorted[0].worker, "y");
  assert.equal(sorted[0].version, "v1");
  assert.deepEqual(errors.toSorted((a, b) => a.slot.localeCompare(b.slot)), [
    { slot: "/a", reason: "bad_shape" },
    { slot: "/b", reason: "bad_shape" },
    { slot: "/d", reason: "bad_shape" },
    { slot: "/e", reason: "bad_namespace" },
    { slot: "/f", reason: "bad_namespace" },
  ]);
});

test("sortPatterns: accepts the system route namespace but rejects other reserved namespaces", () => {
  const { sorted, errors } = sort(Object.fromEntries([
    entry("/system", "exact", "/system", "__system__", "sys"),
  ]));
  assert.equal(errors.length, 0);
  assert.equal(sorted[0].ns, "__system__");
});

test("matchPattern: CF idiom — exact /mcp + prefix /mcp/* dispatched correctly", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/mcp", "exact", "/mcp", "a", "mcp-exact"),
    entry("/mcp/*", "prefix", "/mcp/", "a", "mcp-sub"),
  ]));
  assert.equal(matchPattern(sorted, "/mcp").worker, "mcp-exact");
  assert.equal(matchPattern(sorted, "/mcp/foo").worker, "mcp-sub");
  assert.equal(matchPattern(sorted, "/mcphello"), null);
});

test("matchPattern: exact with query falls through to longer-prefix match", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/mcp", "exact", "/mcp", "a", "mcp-exact"),
    entry("/*", "prefix", "/", "a", "root"),
  ]));
  assert.equal(matchPattern(sorted, "/mcp", "").worker, "mcp-exact");
  assert.equal(matchPattern(sorted, "/mcp", "?x=1").worker, "root");
});

test("matchPattern: longest first — /api/v2/ beats /api/ beats /*", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/*", "prefix", "/", "a", "root"),
    entry("/api/*", "prefix", "/api/", "a", "api"),
    entry("/api/v2/*", "prefix", "/api/v2/", "a", "api-v2"),
  ]));
  assert.equal(matchPattern(sorted, "/api/v2/users").worker, "api-v2");
  assert.equal(matchPattern(sorted, "/api/foo").worker, "api");
  assert.equal(matchPattern(sorted, "/other").worker, "root");
});

test("matchPattern: exact .well-known doesn't over-match", () => {
  const { sorted } = sort(Object.fromEntries([
    entry(
      "/.well-known/oauth-protected-resource",
      "exact",
      "/.well-known/oauth-protected-resource",
      "a",
      "wk"
    ),
  ]));
  assert.equal(
    matchPattern(sorted, "/.well-known/oauth-protected-resource").worker,
    "wk"
  );
  assert.equal(
    matchPattern(sorted, "/.well-known/oauth-protected-resource/extra"),
    null
  );
});

test("matchPattern: empty list → null", () => {
  assert.equal(matchPattern([], "/anything"), null);
});

test("matchPatternWithStats reports comparisons until hit or miss", () => {
  const { sorted } = sort(Object.fromEntries([
    entry("/*", "prefix", "/", "a", "root"),
    entry("/api/*", "prefix", "/api/", "a", "api"),
    entry("/api/v2/*", "prefix", "/api/v2/", "a", "api-v2"),
  ]));
  const first = matchPatternWithStats(sorted, "/api/v2/users");
  assert.equal(/** @type {any} */ (first.entry).worker, "api-v2");
  assert.equal(first.comparisons, 1);

  const second = matchPatternWithStats(sorted, "/api/users");
  assert.equal(/** @type {any} */ (second.entry).worker, "api");
  assert.equal(second.comparisons, 2);

  const miss = matchPatternWithStats(sorted.slice(0, 2), "/other");
  assert.equal(miss.entry, null);
  assert.equal(miss.comparisons, 2);
});
