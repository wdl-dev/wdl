import { test } from "node:test";
import assert from "node:assert/strict";
import {
  estimateBundleEnvBytes,
  findingsForBundleMetadata,
  parseRedisHash,
  redisCli,
  scanWorkerd0701Metadata,
  secretUpperBoundStrings,
} from "../../scripts/scan-workerd-0701-metadata.mjs";
import { WORKERD_EXPERIMENTAL_COMPAT_FLAGS } from "../../shared/workerd-compat-flags.js";

test("workerd experimental compat flag mirror excludes GA context reuse flags", () => {
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("experimental"), true);
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("unsafe_module"), true);
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.length, 33);
  assert.equal(WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("unique_ctx_per_invocation"), false);
  assert.equal(
    WORKERD_EXPERIMENTAL_COMPAT_FLAGS.includes("nonclass_entrypoint_reuses_ctx_across_invocations"),
    false
  );
});

test("workerd 0701 scanner reports missing metadata, py modules, and experimental flags", () => {
  assert.deepEqual(
    findingsForBundleMetadata({ key: "worker:demo:api:v:1", rawMeta: "" }),
    [{
      kind: "missing_meta",
      key: "worker:demo:api:v:1",
      namespace: "demo",
      worker: "api",
      version: "v1",
    }]
  );

  const findings = findingsForBundleMetadata({
    key: "worker:demo:api:v:1",
    rawMeta: JSON.stringify({
      compatibilityFlags: ["unsafe_module"],
      modules: { "worker.py": { type: "py" } },
    }),
  });
  assert.equal(findings.some((finding) => finding.kind === "experimental_compat_flag"), true);
  assert.equal(findings.some((finding) => finding.kind === "python_worker_module"), true);
});

test("workerd 0701 scanner reports retained env that would exceed workerLoader budget", () => {
  const encryptedEnvelope = `WDL-ENC:${"a".repeat(600_000)}`;
  const { findings } = scanWorkerd0701Metadata({
    redis(args) {
      if (args[0] === "--scan") return "worker:demo:api:v:1\n";
      if (args[0] === "HGET") return `${JSON.stringify({ vars: { SMALL: "ok" } })}\n`;
      if (args[0] === "HGETALL" && args[1] === "secrets:demo") return `BIG\n${encryptedEnvelope}\n`;
      if (args[0] === "HGETALL" && args[1] === "secrets:demo:api") return "";
      throw new Error(`unexpected redis args ${JSON.stringify(args)}`);
    },
  });

  const finding = findings.find((entry) => entry.kind === "worker_env_too_large");
  assert.ok(finding);
  assert.equal(finding.namespace, "demo");
  assert.equal(finding.worker, "api");
  assert.equal(finding.version, "v1");
  assert.equal(finding.secret_value_estimate, "encrypted_envelope_length_as_two_byte_string");
});

test("workerd 0701 scanner parses raw redis hashes and rejects odd replies", () => {
  assert.deepEqual(Object.fromEntries(Object.entries(parseRedisHash("A\n1\nB\n2\n"))), { A: "1", B: "2" });
  assert.deepEqual(Object.fromEntries(Object.entries(parseRedisHash(""))), {});
  assert.throws(() => parseRedisHash("A\n1\nB\n"), /odd number/);
});

test("workerd 0701 scanner uses two-byte secret upper bounds capped at upstream max", () => {
  const out = secretUpperBoundStrings({
    ASCII: "WDL-ENC:abc",
    HUGE: "x".repeat(2 * 1024 * 1024),
  });
  assert.equal(out.ASCII.length, "WDL-ENC:abc".length);
  assert.equal(out.ASCII.charCodeAt(0), 0x100);
  assert.equal(out.HUGE.length, 1024 * 1024);
});

test("workerd 0701 scanner estimates assets env with the supplied CDN base", () => {
  const meta = {
    assets: { prefix: "assets/demo/api/v1/" },
    bindings: { ASSETS: { type: "assets" } },
  };
  const identity = { namespace: "demo", worker: "api", version: "v1" };
  const shortBytes = estimateBundleEnvBytes({ identity, meta, assetsCdnBase: "https://a.invalid" });
  const longBytes = estimateBundleEnvBytes({
    identity,
    meta,
    assetsCdnBase: "https://very-long-assets-hostname.example.invalid",
  });
  assert.ok(longBytes > shortBytes);
});

test("workerd 0701 scanner reports missing metadata before reading secret hashes", () => {
  let secretReads = 0;
  const { findings } = scanWorkerd0701Metadata({
    redis(args) {
      if (args[0] === "--scan") return "worker:demo:api:v:1\n";
      if (args[0] === "HGET") return "";
      if (args[0] === "HGETALL") {
        secretReads += 1;
        throw new Error("secret hash should not be read for a bundle missing __meta__");
      }
      throw new Error(`unexpected redis args ${JSON.stringify(args)}`);
    },
  });

  assert.equal(secretReads, 0);
  assert.deepEqual(findings.map((finding) => finding.kind), ["missing_meta"]);
});

test("workerd 0701 scanner reports corrupt metadata before reading secret hashes", () => {
  let secretReads = 0;
  const { findings } = scanWorkerd0701Metadata({
    redis(args) {
      if (args[0] === "--scan") return "worker:demo:api:v:1\n";
      if (args[0] === "HGET") return "not-json\n";
      if (args[0] === "HGETALL") {
        secretReads += 1;
        throw new Error("secret hash should not be read for corrupt __meta__");
      }
      throw new Error(`unexpected redis args ${JSON.stringify(args)}`);
    },
  });

  assert.equal(secretReads, 0);
  assert.deepEqual(findings.map((finding) => finding.kind), ["corrupt_meta"]);
});

test("workerd 0701 scanner fails clearly when redis-cli is missing", () => {
  const error = Object.assign(new Error("spawn redis-cli ENOENT"), { code: "ENOENT" });
  assert.throws(
    () => redisCli(["PING"], {
      redisUrl: "redis://unit",
      spawn() {
        return /** @type {any} */ ({
          error,
          status: null,
          stdout: "",
          stderr: "",
        });
      },
    }),
    /redis-cli not found/
  );
});
