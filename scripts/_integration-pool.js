import { spawn } from "node:child_process";
import { renameSync, writeFileSync } from "node:fs";
import {
  COMPILE_WORKERD_LOCAL_ARGS,
  DOCKER_COMPOSE_BUILD_ARGS,
  LOCAL_ADMIN_TOKEN,
  LOCAL_CONNECT_HOST,
  ROOT,
  localAssetsCdnBase,
  localControlUrl,
  shouldPrepareIntegrationArtifacts,
} from "./integration-environment.js";
import { readIntegrationDurationReport } from "./integration-test-plan.js";
import { pipeWithPrefix, writeWithPrefix } from "./_stream-prefix.js";

/**
 * @typedef {{ file: string, slot: number, durationMs: number, status: "passed" | "failed" }} FileDuration
 * @typedef {{ shardCount: number, runDurationMs: number, fileDurations: FileDuration[] }} IntegrationSummary
 * @typedef {{
 *   files: string[],
 *   shardCount: number,
 *   projectPrefix: string,
 *   basePort: number,
 *   s3PortBase: number,
 *   testArgs: string[],
 *   durationFile?: string,
 *   prepare?: boolean,
 * }} PoolOptions
 */

/** @param {unknown} err */
function errorMessage(err) {
  return err instanceof Error ? err.message : String(err);
}

// Fail loud on opaque shard env values instead of crashing later with
// RangeError from `new Array({length: NaN})` deep in pool startup.
/** @param {string} envName @param {number} defaultValue */
export function parseShardCount(envName, defaultValue) {
  const raw = process.env[envName];
  if (raw == null || raw === "") return defaultValue;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1) {
    process.stderr.write(
      `error: ${envName}=${JSON.stringify(raw)} must be a positive integer\n`
    );
    process.exit(2);
  }
  return n;
}

/** @param {PoolOptions} opts @returns {Promise<number>} aggregate exit code (0 = all green) */
export async function runPooled(opts) {
  const {
    files,
    shardCount,
    projectPrefix,
    basePort,
    s3PortBase,
    testArgs,
    durationFile,
    prepare = true,
  } = opts;
  if (files.length === 0) return 0;

  if (prepare) {
    const code = await prepareIntegrationImages();
    if (code !== 0) return code;
  }

  const runStartedAtMs = Date.now();
  const slotCount = Math.min(shardCount, files.length);
  const queue = [...files];
  const slotEnvs = [];
  const slotFailed = new Array(slotCount).fill(false);
  /** @type {FileDuration[]} */
  const fileDurations = [];
  let firstFailure = 0;

  const slotPromises = [];
  for (let i = 0; i < slotCount; i++) {
    const env = makeSlotEnv(i, projectPrefix, basePort, s3PortBase);
    slotEnvs.push(env);
    slotPromises.push(runSlot(i, env));
  }
  await Promise.all(slotPromises);
  const runDurationMs = Date.now() - runStartedAtMs;

  writeIntegrationSummary({
    shardCount: slotCount,
    runDurationMs,
    fileDurations,
  });
  if (durationFile) {
    persistDurationFile(durationFile, {
      runDurationMs,
      fileDurations,
    });
  }

  const keep = process.env.WDL_KEEP_INTEGRATION_STACK === "1";
  const teardownOnFail = process.env.WDL_TEARDOWN_INTEGRATION_STACK_ON_FAILURE === "1";

  // A failed teardown leaves its stack up; bubble its exit code into
  // firstFailure so CI catches stranded volumes instead of going green.
  const kept = [];
  const teardowns = [];
  for (let i = 0; i < slotCount; i++) {
    const project = slotEnvs[i].COMPOSE_PROJECT_NAME;
    if (keep) {
      kept.push({ project, reason: "WDL_KEEP_INTEGRATION_STACK=1" });
      continue;
    }
    if (slotFailed[i] && !teardownOnFail) {
      kept.push({ project, reason: "test failure" });
      continue;
    }
    teardowns.push(
      teardownSlot(slotEnvs[i], slotPrefix(i)).then((code) => {
        if (code !== 0) {
          kept.push({ project, reason: "teardown failure" });
          if (firstFailure === 0) firstFailure = code;
        }
      })
    );
  }
  await Promise.all(teardowns);

  if (kept.length > 0) {
    const formatted = kept
      .map((k) => `${k.project} (${k.reason})`)
      .join(", ");
    const hint = keep
      ? ""
      : " (set WDL_TEARDOWN_INTEGRATION_STACK_ON_FAILURE=1 to clean up failures)";
    process.stderr.write(`integration stacks left running: ${formatted}${hint}\n`);
  }

  return firstFailure;

  /** @param {number} idx @param {NodeJS.ProcessEnv} env */
  async function runSlot(idx, env) {
    const prefix = slotPrefix(idx);
    let processedAny = false;
    while (queue.length > 0) {
      const file = queue.shift();
      if (!file) break;
      const fileEnv = { ...env };
      if (processedAny) fileEnv.WDL_INTEGRATION_SLOT_PREPPED = "1";
      processedAny = true;
      writeWithPrefix(process.stdout, prefix, `running ${file}\n`);
      const startedAtMs = Date.now();
      const code = await runFile(fileEnv, [...testArgs, file], prefix);
      fileDurations.push({
        file,
        slot: idx,
        durationMs: Date.now() - startedAtMs,
        status: code === 0 ? "passed" : "failed",
      });
      if (code !== 0) {
        slotFailed[idx] = true;
        if (firstFailure === 0) firstFailure = code;
      }
    }
  }
}

function prepareIntegrationImages() {
  if (!shouldPrepareIntegrationArtifacts()) return Promise.resolve(0);
  return runCommand(
    process.execPath,
    COMPILE_WORKERD_LOCAL_ARGS,
    "[prep] "
  ).then((code) => {
    if (code !== 0) return code;
    return runCommand("docker", DOCKER_COMPOSE_BUILD_ARGS, "[prep] ");
  });
}

/** @param {string} command @param {string[]} args @param {string} prefix */
function runCommand(command, args, prefix) {
  return new Promise((resolve) => {
    let settled = false;
    /** @param {number} code */
    const done = (code) => {
      if (settled) return;
      settled = true;
      resolve(code);
    };
    const child = spawn(command, args, {
      cwd: ROOT,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    pipeWithPrefix(child.stdout, process.stdout, prefix);
    pipeWithPrefix(child.stderr, process.stderr, prefix);
    child.on("error", (err) => {
      writeWithPrefix(process.stderr, prefix, `${errorMessage(err)}\n`);
      done(1);
    });
    child.on("close", (code) => done(code ?? 1));
  });
}

/** @param {number} idx */
function slotPrefix(idx) {
  return `[s${idx}] `;
}

/** @param {number} idx @param {string} projectPrefix @param {number} basePort @param {number} s3PortBase @param {NodeJS.ProcessEnv} [baseEnv] */
export function makeSlotEnv(idx, projectPrefix, basePort, s3PortBase, baseEnv = process.env) {
  const gatewayPort = basePort + idx;
  const s3mockPort = s3PortBase + idx;
  const { WDL_NS: _wdlNs, CLOUDFLARE_ENV: _cloudflareEnv, ...cleanBaseEnv } = baseEnv;
  const env = {
    ...cleanBaseEnv,
    COMPOSE_PROJECT_NAME: `${projectPrefix}-${idx}`,
    WDL_GATEWAY_HOST_PORT: String(gatewayPort),
    WDL_S3MOCK_HOST_PORT: String(s3mockPort),
    WDL_WORKERD_CONFIG_VARIANT: "local",
    WDL_INTEGRATION_NO_BUILD: "1",
    ADMIN_TOKEN: LOCAL_ADMIN_TOKEN,
    CONTROL_URL: localControlUrl(gatewayPort),
    ASSETS_CDN_BASE: localAssetsCdnBase(s3mockPort),
    CONTROL_CONNECT_HOST: LOCAL_CONNECT_HOST,
  };
  return env;
}

/** @param {NodeJS.ProcessEnv} env @param {string[]} args @param {string} prefix */
function runFile(env, args, prefix) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, args, {
      cwd: ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    pipeWithPrefix(child.stdout, process.stdout, prefix);
    pipeWithPrefix(child.stderr, process.stderr, prefix);
    child.on("close", (code) => resolve(code ?? 1));
  });
}

/** @param {NodeJS.ProcessEnv} env @param {string} prefix */
function teardownSlot(env, prefix) {
  return new Promise((resolve) => {
    const child = spawn("docker", ["compose", "down", "-v"], {
      cwd: ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    pipeWithPrefix(child.stdout, process.stdout, prefix);
    pipeWithPrefix(child.stderr, process.stderr, prefix);
    child.on("close", (code) => resolve(code ?? 1));
  });
}

/** @param {ReturnType<typeof readIntegrationDurationReport> | undefined} existing @param {{ runDurationMs: number, fileDurations: FileDuration[] }} observed @param {Date} [now] */
export function buildDurationReport(existing, { runDurationMs, fileDurations }, now = new Date()) {
  /** @type {Record<string, unknown>} */
  const files = { ...(existing?.files && typeof existing.files === "object" ? existing.files : {}) };
  const updatedAt = now.toISOString();
  for (const row of fileDurations) {
    files[row.file] = {
      durationMs: row.durationMs,
      status: row.status,
      updatedAt,
    };
  }
  return {
    updatedAt,
    runDurationMs,
    files,
  };
}

/** @param {IntegrationSummary} report */
export function formatIntegrationSummary({ shardCount, runDurationMs, fileDurations }) {
  const shardTotals = new Array(shardCount)
    .fill(0)
    .map((_, slot) => ({ slot, files: 0, durationMs: 0 }));
  for (const row of fileDurations) {
    const shard = shardTotals[row.slot];
    shard.files += 1;
    shard.durationMs += row.durationMs;
  }
  const lines = [
    `integration summary: files=${fileDurations.length} test_wall_ms=${runDurationMs}`,
    "integration shard durations:",
    ...shardTotals.map((shard) =>
      `  s${shard.slot} files=${shard.files} duration_ms=${shard.durationMs}`
    ),
    "integration file durations:",
    ...[...fileDurations]
      .toSorted((a, b) => b.durationMs - a.durationMs || a.file.localeCompare(b.file))
      .map((row) =>
        `  ${row.durationMs}ms s${row.slot} ${row.status} ${row.file}`
      ),
  ];
  return `${lines.join("\n")}\n`;
}

/** @param {IntegrationSummary} report */
function writeIntegrationSummary(report) {
  process.stdout.write(formatIntegrationSummary(report));
}

/** @param {string} file @param {{ runDurationMs: number, fileDurations: FileDuration[] }} observed */
export function persistDurationFile(file, observed) {
  try {
    updateDurationFile(file, observed);
    return true;
  } catch (err) {
    process.stderr.write(
      `warning: unable to update integration duration file ${file}: ${errorMessage(err)}\n`
    );
    return false;
  }
}

/** @param {string} file @param {{ runDurationMs: number, fileDurations: FileDuration[] }} observed */
function updateDurationFile(file, observed) {
  const existing = readIntegrationDurationReport(file);
  const next = buildDurationReport(existing, observed);
  const tmp = `${file}.tmp-${process.pid}`;
  writeFileSync(tmp, `${JSON.stringify(next, null, 2)}\n`);
  renameSync(tmp, file);
}
