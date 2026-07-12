import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DECLARED_HOSTS_KEY,
  HOST_DECLARATIONS_SCAN_PATTERN,
  HOSTS_SCAN_PATTERN,
  NAMESPACES_KEY,
  bundleKey,
  formatVersion,
  hostDeclarationsKey,
  hostsKey,
  namespaceFromHostsKey,
  nextVersionKey,
  nsHostsKey,
  parseVersion,
} from "../../shared/version.js";

test("formatVersion: integer → v<int>", () => {
  assert.equal(formatVersion(1), "v1");
  assert.equal(formatVersion(42), "v42");
});

test("formatVersion: rejects non-positive / non-integer", () => {
  assert.throws(() => formatVersion(0), /invalid/);
  assert.throws(() => formatVersion(-1), /invalid/);
  assert.throws(() => formatVersion(1.5), /invalid/);
  assert.throws(() => formatVersion("1"), /invalid/);
});

test("parseVersion: well-formed", () => {
  assert.equal(parseVersion("v1"), 1);
  assert.equal(parseVersion("v42"), 42);
});

test("parseVersion: returns null for malformed", () => {
  assert.equal(parseVersion(""), null);
  assert.equal(parseVersion("v"), null);
  assert.equal(parseVersion("v0"), null);      // leading-zero / zero rejected
  assert.equal(parseVersion("v01"), null);     // no leading zeros
  assert.equal(parseVersion("1"), null);
  assert.equal(parseVersion("V1"), null);
  assert.equal(parseVersion("v1a"), null);
  assert.equal(parseVersion(null), null);
  assert.equal(parseVersion(undefined), null);
  assert.equal(parseVersion(1), null);
});

test("bundleKey: composes worker:<ns>:<name>:v:<int>", () => {
  assert.equal(bundleKey("demo", "hello", "v1"), "worker:demo:hello:v:1");
  assert.equal(bundleKey("demo", "hello", "v42"), "worker:demo:hello:v:42");
});

test("bundleKey: rejects malformed version tags", () => {
  assert.throws(() => bundleKey("demo", "hello", "latest"), /invalid version/);
  assert.throws(() => bundleKey("demo", "hello", "v0"), /invalid version/);
  assert.throws(() => bundleKey("demo", "hello", ""), /invalid version/);
  assert.throws(() => bundleKey("demo", "hello", null), /invalid version/);
});

test("route-plane registry key helpers compose and parse canonical keys", () => {
  assert.equal(NAMESPACES_KEY, "namespaces");
  assert.equal(DECLARED_HOSTS_KEY, "declared-hosts");
  assert.equal(HOSTS_SCAN_PATTERN, "hosts:*");
  assert.equal(HOST_DECLARATIONS_SCAN_PATTERN, "host-declarations:*");
  assert.equal(hostsKey("demo"), "hosts:demo");
  assert.equal(namespaceFromHostsKey("hosts:demo"), "demo");
  assert.equal(namespaceFromHostsKey("routes:demo"), "");
  assert.equal(nsHostsKey("demo"), "ns-hosts:demo");
  assert.equal(hostDeclarationsKey("app.example"), "host-declarations:app.example");
});

test("nextVersionKey composes the worker version counter key", () => {
  assert.equal(nextVersionKey("demo", "hello"), "worker:demo:hello:next_version");
});
