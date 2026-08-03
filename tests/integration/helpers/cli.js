import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";

import { ROOT, WDL_CLI_BIN } from "./env.js";

const OFFLINE_WRANGLER_ENV = {
  WRANGLER_SEND_METRICS: "false",
  WRANGLER_SEND_ERROR_REPORTS: "false",
  WRANGLER_HIDE_BANNER: "true",
  HTTP_PROXY: "",
  HTTPS_PROXY: "",
  http_proxy: "",
  https_proxy: "",
  NO_PROXY: "localhost,127.0.0.1,::1,admin.test",
  no_proxy: "localhost,127.0.0.1,::1,admin.test",
};

/** @param {...Record<string, string | undefined>} overlays */
export function integrationChildEnv(...overlays) {
  const env = { ...process.env };
  for (const overlay of overlays) Object.assign(env, overlay);
  delete env.NODE_TEST_CONTEXT;
  delete env.NODE_TEST_WORKER_ID;
  return env;
}

/**
 * @param {string} program
 * @param {string[]} args
 * @param {{ cwd?: string, input?: string | Buffer, stdio?: any, env?: NodeJS.ProcessEnv }} opts
 */
function spawnIntegrationCommand(program, args, opts) {
  /** @type {import("node:child_process").SpawnSyncOptionsWithStringEncoding} */
  const spawnOptions = {
    cwd: opts.cwd || ROOT,
    stdio: opts.stdio || "pipe",
    encoding: "utf8",
    input: opts.input,
    env: integrationChildEnv(opts.env || {}),
  };
  if (program === "docker") {
    return spawnSync("docker", args, spawnOptions);
  }
  if (program === process.execPath) {
    return spawnSync(process.execPath, args, spawnOptions);
  }
  throw new Error(`unsupported integration command: ${program}`);
}

/**
 * @param {string[]} argv
 * @param {{ cwd?: string, input?: string | Buffer, stdio?: any, env?: NodeJS.ProcessEnv }} [opts]
 */
export function sh(argv, opts = {}) {
  const [program, ...args] = argv;
  if (!program) throw new Error("empty integration command");
  const res = spawnIntegrationCommand(program, args, opts);
  if (res.error) throw res.error;
  if (res.status !== 0) {
    const command = argv.join(" ");
    throw new Error(
      `integration command failed (${res.status}): ${(res.stderr || res.stdout || command).trim()}`
    );
  }
  return res.stdout || "";
}

/**
 * @param {string[]} args
 * @param {{ cwd?: string, input?: string, env?: Record<string, string> }} [opts]
 */
export function runWdlCli(args, opts = {}) {
  return spawnSync(WDL_CLI_BIN, args, {
    cwd: opts.cwd || ROOT,
    encoding: "utf8",
    input: opts.input,
    env: integrationChildEnv(OFFLINE_WRANGLER_ENV, opts.env || {}),
  });
}

/** @param {{ status: number | null, stderr?: string, stdout?: string }} res */
export function assertOk(res) {
  assert.equal(res.status, 0, res.stderr || res.stdout || undefined);
}
