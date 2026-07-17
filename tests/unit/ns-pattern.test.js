import { test } from "node:test";
import assert from "node:assert/strict";
import {
  NS_PATTERN,
  SUBDOMAIN_NS_PATTERN,
  RESERVED_NS,
  isReservedNs,
  isValidRouteNs,
  isValidRuntimeLoadNs,
  PLATFORM_TIER_RESERVED_NS,
  ROUTES_ALLOWED_RESERVED_NS,
  WORKER_NAME_RE,
  WORKFLOW_NAME_RE,
  QUEUE_NAME_RE,
  BINDING_NAME_RE,
  JS_IDENTIFIER_RE,
  WDL_RESERVED_BINDING_RE,
  KV_ID_RE,
  isValidWorkerName,
  isValidWorkflowName,
  isValidQueueName,
  isValidKvId,
  isValidJsIdentifier,
} from "../../shared/ns-pattern.js";

test("NS_PATTERN anchored matches full namespace", () => {
  const re = new RegExp(`^${NS_PATTERN}$`);
  assert.ok(re.test("demo"));
  assert.ok(re.test("ns-1"));
  assert.ok(re.test("a"));
  assert.ok(re.test("a".repeat(63)));
  assert.ok(!re.test(""));
  assert.ok(!re.test("-"));
  assert.ok(!re.test("-ns"));
  assert.ok(!re.test("ns-"));
  assert.ok(!re.test("a".repeat(64)));
  assert.ok(!re.test("Demo"));
  assert.ok(!re.test("ns.1"));
});

test("NS_PATTERN embedded in subdomain regex", () => {
  const re = new RegExp(`^(${NS_PATTERN})\\.workers\\.local$`);
  assert.deepEqual("demo.workers.local".match(re)?.[1], "demo");
  assert.deepEqual(`${"a".repeat(63)}.workers.local`.match(re)?.[1], "a".repeat(63));
  assert.equal("workers.local".match(re), null);
  assert.equal("-demo.workers.local".match(re), null);
  assert.equal("demo-.workers.local".match(re), null);
  assert.equal(`${"a".repeat(64)}.workers.local`.match(re), null);
  assert.equal("Demo.workers.local".match(re), null);
});

test("isReservedNs accepts only the explicit reserved namespace set", () => {
  assert.deepEqual(RESERVED_NS, new Set(["__system__", "__platform__", "__community__"]));
  for (const ns of RESERVED_NS) {
    assert.ok(isReservedNs(ns), `expected ${ns} to be reserved`);
  }
  for (const ns of [
    "__anything",
    "__future",
    "__platform__:platform-demo",
    "__platform__/platform-demo",
    "platform",
    "_single",
    "acme",
    "",
    undefined,
  ]) {
    assert.ok(!isReservedNs(ns), `expected ${ns} not to be reserved`);
  }
});

test("SUBDOMAIN_NS_PATTERN matches tenants and explicit reserved ns only", () => {
  const re = new RegExp(`^(${SUBDOMAIN_NS_PATTERN})\\.workers\\.local$`);
  assert.deepEqual("demo.workers.local".match(re)?.[1], "demo");
  assert.deepEqual("__platform__.workers.local".match(re)?.[1], "__platform__");
  assert.deepEqual("__system__.workers.local".match(re)?.[1], "__system__");
  assert.deepEqual("__community__.workers.local".match(re)?.[1], "__community__");
  assert.equal("__anything.workers.local".match(re), null);
  assert.equal("__platform__:platform-demo.workers.local".match(re), null);
});

test("ROUTES_ALLOWED_RESERVED_NS whitelists __system__ only", () => {
  assert.ok(ROUTES_ALLOWED_RESERVED_NS.has("__system__"));
  assert.ok(!ROUTES_ALLOWED_RESERVED_NS.has("__platform__"));
  assert.ok(!ROUTES_ALLOWED_RESERVED_NS.has("__community__"));
});

test("PLATFORM_TIER_RESERVED_NS whitelists platform binding runtime targets", () => {
  assert.deepEqual(PLATFORM_TIER_RESERVED_NS, new Set(["__platform__"]));
});

test("isValidRouteNs accepts tenant routes and whitelisted reserved routes only", () => {
  assert.equal(isValidRouteNs("demo"), true);
  assert.equal(isValidRouteNs("a".repeat(63)), true);
  assert.equal(isValidRouteNs("__system__"), true);
  assert.equal(isValidRouteNs("-demo"), false);
  assert.equal(isValidRouteNs("demo-"), false);
  assert.equal(isValidRouteNs("a".repeat(64)), false);
  assert.equal(isValidRouteNs("__platform__"), false);
  assert.equal(isValidRouteNs("admin"), false);
  assert.equal(isValidRouteNs("Bad_NS"), false);
});

test("isValidRuntimeLoadNs accepts platform-tier reserved namespaces", () => {
  assert.equal(isValidRuntimeLoadNs("demo"), true);
  assert.equal(isValidRuntimeLoadNs("__system__"), true);
  assert.equal(isValidRuntimeLoadNs("__platform__"), true);
  assert.equal(isValidRuntimeLoadNs("__community__"), false);
  assert.equal(isValidRuntimeLoadNs("admin"), false);
  assert.equal(isValidRuntimeLoadNs("-demo"), false);
});

test("WORKER_NAME_RE accepts CF-compatible worker names up to 255 chars", () => {
  assert.ok(WORKER_NAME_RE.test("a"));
  assert.ok(WORKER_NAME_RE.test("hello"));
  assert.ok(WORKER_NAME_RE.test("kv-demo"));
  assert.ok(WORKER_NAME_RE.test("MyWorker"));
  assert.ok(WORKER_NAME_RE.test("my_worker"));
  assert.ok(WORKER_NAME_RE.test("My_Worker-2"));
  assert.ok(WORKER_NAME_RE.test("9nines"));
  // 63-char DNS-label limit is workers.dev-specific; WDL worker routes place
  // the worker name in the path, so they don't hit a DNS-label boundary.
  assert.ok(WORKER_NAME_RE.test("a".repeat(64)));
  assert.ok(WORKER_NAME_RE.test("a".repeat(255)));
  assert.ok(!WORKER_NAME_RE.test(""));
  assert.ok(!WORKER_NAME_RE.test("a".repeat(256)));
  // Protocol-critical: ':' breaks parseWorkerId(), '/' / '\n' break
  // Redis keys + log fields.
  assert.ok(!WORKER_NAME_RE.test("a:b"));
  assert.ok(!WORKER_NAME_RE.test("a/b"));
  assert.ok(!WORKER_NAME_RE.test("a b"));
  assert.ok(!WORKER_NAME_RE.test("a\nb"));
  assert.ok(!WORKER_NAME_RE.test("-leading-hyphen"));
  assert.ok(!WORKER_NAME_RE.test("_leading-underscore"));
});

test("QUEUE_NAME_RE rejects ':' (would corrupt queue:<ns>:<id>:s key parsing)", () => {
  assert.ok(QUEUE_NAME_RE.test("orders"));
  assert.ok(QUEUE_NAME_RE.test("orders-dlq"));
  assert.ok(!QUEUE_NAME_RE.test(""));
  assert.ok(!QUEUE_NAME_RE.test("retryCountQ"));
  assert.ok(!QUEUE_NAME_RE.test("a:b"));
  assert.ok(!QUEUE_NAME_RE.test("q.1"));
});

test("KV_ID_RE blocks ':' — otherwise id='foo:v'+key='bar' aliases id='foo'+key='v:bar'", () => {
  assert.ok(KV_ID_RE.test("cache"));
  assert.ok(KV_ID_RE.test("session-store"));
  assert.ok(!KV_ID_RE.test(""));
  assert.ok(!KV_ID_RE.test("foo:v"));
  assert.ok(!KV_ID_RE.test("foo/bar"));
  assert.ok(!KV_ID_RE.test("Foo"));
});

test("name and id predicates apply their canonical regex grammars", () => {
  /** @type {Array<[(value: unknown) => boolean, string, string]>} */
  const cases = [
    [isValidWorkerName, "My_Worker-2", "-worker"],
    [isValidWorkflowName, "Daily_Report", "workflow/name"],
    [isValidQueueName, "orders-dlq", "Orders"],
    [isValidKvId, "session-store", "session:store"],
  ];
  for (const [predicate, valid, invalid] of cases) {
    assert.equal(predicate(valid), true);
    assert.equal(predicate(invalid), false);
    assert.equal(predicate(null), false);
  }
  assert.equal(WORKFLOW_NAME_RE.test("Daily_Report"), true);
});

test("BINDING_NAME_RE accepts JS-identifier binding names (CF-compatible)", () => {
  // CF wrangler requires "valid JavaScript variable name" — SCREAMING,
  // camelCase, and _private are all valid there and here.
  assert.ok(BINDING_NAME_RE.test("MY_Q"));
  assert.ok(BINDING_NAME_RE.test("ASSETS"));
  assert.ok(BINDING_NAME_RE.test("productionQueue"));
  assert.ok(BINDING_NAME_RE.test("_private"));
  assert.ok(BINDING_NAME_RE.test("A"));
  assert.ok(!BINDING_NAME_RE.test(""));
  assert.ok(!BINDING_NAME_RE.test("9NUMERIC"));
  assert.ok(!BINDING_NAME_RE.test("MY-Q"));
  assert.ok(!BINDING_NAME_RE.test("a".repeat(65)));
  // __proto__ / constructor / toString pass the regex as JS identifiers;
  // they're rejected by bundle binding normalization via RESERVED_OBJECT_KEYS
  // (see admin-lib tests).
  assert.ok(BINDING_NAME_RE.test("__proto__"));
  assert.ok(BINDING_NAME_RE.test("constructor"));
});

test("JS_IDENTIFIER_RE accepts class and entrypoint identifiers without binding length cap", () => {
  assert.ok(JS_IDENTIFIER_RE.test("Room"));
  assert.ok(JS_IDENTIFIER_RE.test("_Private"));
  assert.ok(JS_IDENTIFIER_RE.test("$Default"));
  assert.ok(JS_IDENTIFIER_RE.test("a".repeat(200)));
  assert.ok(isValidJsIdentifier("Room"));
  assert.equal(isValidJsIdentifier("not-valid"), false);
  assert.equal(isValidJsIdentifier("9Room"), false);
  assert.equal(isValidJsIdentifier(""), false);
  assert.equal(isValidJsIdentifier(null), false);
  assert.equal(isValidJsIdentifier(undefined), false);
});

test("WDL_RESERVED_BINDING_RE matches runtime-private env binding names", () => {
  assert.ok(WDL_RESERVED_BINDING_RE.test("__WDL_DO_BACKEND__"));
  assert.ok(WDL_RESERVED_BINDING_RE.test("__WDL_DO_ALARMS__"));
  assert.ok(WDL_RESERVED_BINDING_RE.test("__WDL_FUTURE_1__"));
  assert.ok(!WDL_RESERVED_BINDING_RE.test("__WdlAbort__"));
  assert.ok(!WDL_RESERVED_BINDING_RE.test("WDL_DO_BACKEND"));
  assert.ok(!WDL_RESERVED_BINDING_RE.test("__WDL_DO_BACKEND"));
});
