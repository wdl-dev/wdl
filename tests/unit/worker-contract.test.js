import { test } from "node:test";
import assert from "node:assert/strict";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import {
  DECLARED_HOSTS_KEY,
  DO_OWNER_SCOPE_PREFIX,
  HOST_DECLARATIONS_SCAN_PATTERN,
  HOSTS_SCAN_PATTERN,
  NAMESPACES_KEY,
  PATTERNS_CHANNEL,
  ROUTES_CHANNEL,
  ROUTES_FLUSH_CHANNEL,
  VERSION_DELETE_LOCK_KIND,
  WHOLE_DELETE_LOCK_KIND,
  bundleKey,
  deleteLockKey,
  doOwnerScopeScanPatternForStorage,
  doStorageIdKey,
  formatDeleteLockToken,
  formatVersion,
  hostDeclarationsKey,
  hostsKey,
  namespaceFromHostsKey,
  nextVersionKey,
  nsHostsKey,
  parseDeleteLockKind,
  parseVersion,
  workerVersionsKey,
} from "../../shared/worker-contract.js";

const versionFixture = readRepositoryJson("tests/fixtures/version-tags.json");

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

test("parseVersion matches the shared JS/Rust version fixture", () => {
  for (const { tag, parsed } of versionFixture.cases) {
    assert.equal(parseVersion(tag), parsed, tag);
  }
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
  assert.equal(ROUTES_CHANNEL, "routes:invalidate");
  assert.equal(ROUTES_FLUSH_CHANNEL, "routes:flush");
  assert.equal(PATTERNS_CHANNEL, "patterns:invalidate");
});

test("nextVersionKey composes the worker version counter key", () => {
  assert.equal(nextVersionKey("demo", "hello"), "worker:demo:hello:next_version");
});

test("worker lifecycle key helpers compose canonical keys", () => {
  assert.equal(workerVersionsKey("demo", "hello"), "worker-versions:demo:hello");
  assert.equal(doStorageIdKey("demo", "hello"), "worker:do-storage:demo:hello");
  assert.equal(DO_OWNER_SCOPE_PREFIX, "do:owner:scope:");
  assert.equal(
    doOwnerScopeScanPatternForStorage("do_abc"),
    "do:owner:scope:do_abc%3A*"
  );
  assert.equal(deleteLockKey("demo", "hello"), "worker-delete-lock:demo:hello");
});

test("worker delete lock tokens carry the operation kind", () => {
  assert.equal(
    formatDeleteLockToken(WHOLE_DELETE_LOCK_KIND, "abc123"),
    "whole:abc123"
  );
  assert.equal(
    formatDeleteLockToken(VERSION_DELETE_LOCK_KIND, "abc123"),
    "version:abc123"
  );
  assert.equal(parseDeleteLockKind("whole:abc123"), WHOLE_DELETE_LOCK_KIND);
  assert.equal(parseDeleteLockKind("version:abc123"), VERSION_DELETE_LOCK_KIND);
  assert.equal(parseDeleteLockKind("unknown:abc123"), null);
  assert.equal(parseDeleteLockKind("whole:"), null);
  assert.equal(parseDeleteLockKind("legacy-token"), null);
});
