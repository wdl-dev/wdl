#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { firstWorkerdExperimentalCompatFlag } from "../shared/workerd-compat-flags.js";

const redisUrl = redisUrlFromEnv();
const pattern = "worker:*:*:v:*";

function redisUrlFromEnv() {
  const raw = process.env.REDIS_URL || process.env.REDIS_ADDR || "redis://127.0.0.1:6379/0";
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) return raw;
  return `redis://${raw}`;
}

/** @param {string[]} args */
function redisCli(args) {
  const result = spawnSync("redis-cli", ["-u", redisUrl, "--raw", ...args], {
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(result.stderr.trim() || `redis-cli exited with status ${result.status}`);
  }
  return result.stdout;
}

/** @param {string} key */
function parseBundleKey(key) {
  const match = /^worker:([^:]+):([^:]+):v:([1-9][0-9]*)$/.exec(key);
  if (!match) return { namespace: null, worker: null, version: null };
  return { namespace: match[1], worker: match[2], version: `v${match[3]}` };
}

/** @param {unknown} meta */
function pythonModules(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return [];
  const modules = /** @type {{ modules?: unknown }} */ (meta).modules;
  if (!modules || typeof modules !== "object" || Array.isArray(modules)) return [];
  return Object.entries(/** @type {Record<string, unknown>} */ (modules))
    .filter(([, value]) =>
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      /** @type {{ type?: unknown }} */ (value).type === "py"
    )
    .map(([name]) => name);
}

/** @param {unknown} meta */
function experimentalFlag(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return null;
  return firstWorkerdExperimentalCompatFlag(/** @type {{ compatibilityFlags?: unknown }} */ (meta).compatibilityFlags);
}

const keys = redisCli(["--scan", "--pattern", pattern])
  .split("\n")
  .map((line) => line.trim())
  .filter(Boolean);

/** @type {Array<Record<string, unknown>>} */
const findings = [];
for (const key of keys) {
  const identity = parseBundleKey(key);
  const rawMeta = redisCli(["HGET", key, "__meta__"]).replace(/\n$/, "");
  if (!rawMeta) continue;
  /** @type {unknown} */
  let meta;
  try {
    meta = JSON.parse(rawMeta);
  } catch (err) {
    findings.push({
      kind: "corrupt_meta",
      key,
      ...identity,
      error: err instanceof Error ? err.message : String(err),
    });
    continue;
  }

  const flag = experimentalFlag(meta);
  if (flag) {
    findings.push({
      kind: "experimental_compat_flag",
      key,
      ...identity,
      flag,
    });
  }
  for (const module of pythonModules(meta)) {
    findings.push({
      kind: "python_worker_module",
      key,
      ...identity,
      module,
    });
  }
}

for (const finding of findings) {
  console.log(JSON.stringify(finding));
}
if (findings.length === 0) {
  console.error(`Scanned ${keys.length} worker bundle metadata keys; no workerd 0701 blockers found.`);
} else {
  console.error(`Scanned ${keys.length} worker bundle metadata keys; found ${findings.length} workerd 0701 blocker(s).`);
  process.exitCode = 1;
}
