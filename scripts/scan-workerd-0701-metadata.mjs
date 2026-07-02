#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { firstWorkerdExperimentalCompatFlag } from "../shared/workerd-compat-flags.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..");
const REDIS_PATTERN = "worker:*:*:v:*";
const TWO_BYTE_SECRET_CHAR = "\u0100";

const {
  estimatedWorkerLoaderEnv,
  estimatedWorkerLoaderEnvBytes,
  WORKER_LOADER_ENV_MAX_BYTES,
  UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES,
  WORKER_LOADER_ENV_HEADROOM_BYTES,
} = await importControlEnvBudget();

/** @param {string} relativePath */
function repoFileUrl(relativePath) {
  return pathToFileURL(path.resolve(REPO_ROOT, relativePath)).href;
}

/**
 * @param {string} relativePath
 * @param {Array<[RegExp | string, string]>} [replacements]
 */
function repoModuleDataUrl(relativePath, replacements = []) {
  let source = readFileSync(path.resolve(REPO_ROOT, relativePath), "utf8");
  for (const [pattern, replacement] of replacements) {
    source = source.replace(pattern, replacement);
  }
  return `data:text/javascript,${encodeURIComponent(source)}`;
}

async function importControlEnvBudget() {
  return await import(repoModuleDataUrl("control/env-budget.js", [
    [/from "shared-secret-envelope";/, `from ${JSON.stringify(repoFileUrl("shared/secret-envelope.js"))};`],
    [/from "shared-version";/, `from ${JSON.stringify(repoFileUrl("shared/version.js"))};`],
  ]));
}

export function redisUrlFromEnv(env = process.env) {
  const raw = env.REDIS_URL || env.REDIS_ADDR || "redis://127.0.0.1:6379/0";
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) return raw;
  return `redis://${raw}`;
}

/**
 * @param {string[]} args
 * @param {{ redisUrl?: string, spawn?: typeof spawnSync }} [options]
 */
export function redisCli(args, { redisUrl = redisUrlFromEnv(), spawn = spawnSync } = {}) {
  const result = spawn("redis-cli", ["-u", redisUrl, "--raw", ...args], {
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error) {
    if (/** @type {NodeJS.ErrnoException} */ (result.error).code === "ENOENT") {
      throw new Error(
        "redis-cli not found. Install redis-cli/redis-tools before running scripts/scan-workerd-0701-metadata.mjs; local compose users can inspect Redis with `docker compose exec -T redis redis-cli`."
      );
    }
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(result.stderr.trim() || `redis-cli exited with status ${result.status}`);
  }
  return result.stdout;
}

/** @param {string} raw */
export function parseRedisHash(raw) {
  const lines = raw.split("\n");
  if (lines.at(-1) === "") lines.pop();
  if (lines.length % 2 !== 0) {
    throw new Error("redis-cli HGETALL returned an odd number of raw lines");
  }
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (let i = 0; i < lines.length; i += 2) {
    out[lines[i]] = lines[i + 1];
  }
  return out;
}

/** @param {string} key */
export function parseBundleKey(key) {
  const match = /^worker:([^:]+):([^:]+):v:([1-9][0-9]*)$/.exec(key);
  if (!match) return { namespace: null, worker: null, version: null };
  return { namespace: match[1], worker: match[2], version: `v${match[3]}` };
}

/** @param {unknown} meta */
export function pythonModules(meta) {
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
export function experimentalFlag(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return null;
  return firstWorkerdExperimentalCompatFlag(/** @type {{ compatibilityFlags?: unknown }} */ (meta).compatibilityFlags);
}

/**
 * @param {string} key
 * @param {string} rawMeta
 */
function parseMetadataRecord(key, rawMeta) {
  const identity = parseBundleKey(key);
  if (!rawMeta) {
    return {
      identity,
      meta: null,
      findings: [{ kind: "missing_meta", key, ...identity }],
    };
  }

  /** @type {unknown} */
  let meta;
  try {
    meta = JSON.parse(rawMeta);
  } catch (err) {
    return {
      identity,
      meta: null,
      findings: [{
        kind: "corrupt_meta",
        key,
        ...identity,
        error: err instanceof Error ? err.message : String(err),
      }],
    };
  }
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
    return {
      identity,
      meta: null,
      findings: [{ kind: "corrupt_meta", key, ...identity, error: "__meta__ must be a JSON object" }],
    };
  }
  return {
    identity,
    meta: /** @type {Record<string, unknown>} */ (meta),
    findings: [],
  };
}

/** @param {Record<string, string>} encrypted */
export function secretUpperBoundStrings(encrypted) {
  /** @type {Record<string, string>} */
  const out = Object.create(null);
  for (const [key, value] of Object.entries(encrypted || {})) {
    if (typeof value !== "string") continue;
    out[key] = TWO_BYTE_SECRET_CHAR.repeat(Math.min(value.length, UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES));
  }
  return out;
}

/**
 * @param {{
 *   identity: { namespace: string | null, worker: string | null, version: string | null },
 *   meta: Record<string, unknown>,
 *   nsSecretsEncrypted?: Record<string, string>,
 *   workerSecretsEncrypted?: Record<string, string>,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export function estimateBundleEnvBytes({
  identity,
  meta,
  nsSecretsEncrypted = {},
  workerSecretsEncrypted = {},
  assetsCdnBase = null,
}) {
  if (!identity.namespace || !identity.worker || !identity.version) return null;
  const env = estimatedWorkerLoaderEnv({
    ns: identity.namespace,
    worker: identity.worker,
    version: identity.version,
    vars: meta.vars && typeof meta.vars === "object" && !Array.isArray(meta.vars)
      ? /** @type {Record<string, unknown>} */ (meta.vars)
      : null,
    nsSecrets: secretUpperBoundStrings(nsSecretsEncrypted),
    workerSecrets: secretUpperBoundStrings(workerSecretsEncrypted),
    meta,
    assetsCdnBase,
  });
  return estimatedWorkerLoaderEnvBytes(env);
}

/**
 * @param {{
 *   key: string,
 *   rawMeta: string,
 *   nsSecretsEncrypted?: Record<string, string>,
 *   workerSecretsEncrypted?: Record<string, string>,
 *   assetsCdnBase?: string | null,
 * }} args
 */
export function findingsForBundleMetadata({
  key,
  rawMeta,
  nsSecretsEncrypted = {},
  workerSecretsEncrypted = {},
  assetsCdnBase = null,
}) {
  const { identity, meta, findings: parseFindings } = parseMetadataRecord(key, rawMeta);
  if (!meta) return parseFindings;
  return findingsForParsedBundleMetadata({
    key,
    identity,
    meta,
    nsSecretsEncrypted,
    workerSecretsEncrypted,
    assetsCdnBase,
  });
}

/**
 * @param {{
 *   key: string,
 *   identity: { namespace: string | null, worker: string | null, version: string | null },
 *   meta: Record<string, unknown>,
 *   nsSecretsEncrypted?: Record<string, string>,
 *   workerSecretsEncrypted?: Record<string, string>,
 *   assetsCdnBase?: string | null,
 * }} args
 */
function findingsForParsedBundleMetadata({
  key,
  identity,
  meta,
  nsSecretsEncrypted = {},
  workerSecretsEncrypted = {},
  assetsCdnBase = null,
}) {
  /** @type {Array<Record<string, unknown>>} */
  const findings = [];
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

  const envBytes = estimateBundleEnvBytes({
    identity,
    meta,
    nsSecretsEncrypted,
    workerSecretsEncrypted,
    assetsCdnBase,
  });
  if (envBytes !== null && envBytes > WORKER_LOADER_ENV_MAX_BYTES) {
    findings.push({
      kind: "worker_env_too_large",
      key,
      ...identity,
      env_bytes: envBytes,
      max_env_bytes: WORKER_LOADER_ENV_MAX_BYTES,
      upstream_max_env_bytes: UPSTREAM_WORKER_LOADER_ENV_MAX_BYTES,
      headroom_bytes: WORKER_LOADER_ENV_HEADROOM_BYTES,
      secret_value_estimate: "encrypted_envelope_length_as_two_byte_string",
    });
  }
  return findings;
}

/**
 * @param {{ redis: (args: string[]) => string, assetsCdnBase?: string | null }} args
 */
export function scanWorkerd0701Metadata({ redis, assetsCdnBase = null }) {
  const keys = redis(["--scan", "--pattern", REDIS_PATTERN])
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  /** @type {Map<string, Record<string, string>>} */
  const secretHashCache = new Map();
  /** @param {string} key */
  const readSecretHash = (key) => {
    let cached = secretHashCache.get(key);
    if (!cached) {
      cached = parseRedisHash(redis(["HGETALL", key]));
      secretHashCache.set(key, cached);
    }
    return cached;
  };

  /** @type {Array<Record<string, unknown>>} */
  const findings = [];
  for (const key of keys) {
    const rawMeta = redis(["HGET", key, "__meta__"]).replace(/\n$/, "");
    const { identity, meta, findings: parseFindings } = parseMetadataRecord(key, rawMeta);
    if (!meta) {
      findings.push(...parseFindings);
      continue;
    }
    const nsSecretsEncrypted = identity.namespace
      ? readSecretHash(`secrets:${identity.namespace}`)
      : {};
    const workerSecretsEncrypted = identity.namespace && identity.worker
      ? readSecretHash(`secrets:${identity.namespace}:${identity.worker}`)
      : {};
    findings.push(...findingsForParsedBundleMetadata({
      key,
      identity,
      meta,
      nsSecretsEncrypted,
      workerSecretsEncrypted,
      assetsCdnBase,
    }));
  }
  return { keysScanned: keys.length, findings };
}

export async function runCli() {
  const redisUrl = redisUrlFromEnv();
  const { keysScanned, findings } = scanWorkerd0701Metadata({
    redis: (args) => redisCli(args, { redisUrl }),
    assetsCdnBase: process.env.ASSETS_CDN_BASE || null,
  });
  for (const finding of findings) {
    console.log(JSON.stringify(finding));
  }
  if (findings.length === 0) {
    console.error(`Scanned ${keysScanned} worker bundle metadata keys; no workerd 0701 blockers found.`);
  } else {
    console.error(`Scanned ${keysScanned} worker bundle metadata keys; found ${findings.length} workerd 0701 blocker(s).`);
    process.exitCode = 1;
  }
}

function isMainModule() {
  return process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
}

if (isMainModule()) {
  runCli().catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exitCode = 1;
  });
}
