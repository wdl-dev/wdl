import { spawn } from "node:child_process";

import { composeNoBuildArgs } from "../../../scripts/integration-environment.js";
import { ROOT } from "./env.js";
import { sh } from "./cli.js";

/**
 * @param {string[]} args
 * @param {{ stdio?: any, env?: NodeJS.ProcessEnv }} [opts]
 */
export function composeUp(args, opts = {}) {
  return sh(["docker", "compose", "up", "-d", ...composeNoBuildArgs(), ...args], opts);
}

/**
 * @param {string} profile
 * @param {string[]} args
 * @param {{ stdio?: any, env?: NodeJS.ProcessEnv }} [opts]
 */
export function composeProfileUp(profile, args, opts = {}) {
  return sh(
    ["docker", "compose", "up", "-d", ...composeNoBuildArgs(), ...args],
    { ...opts, env: { ...opts.env, COMPOSE_PROFILES: profile } }
  );
}

/** @param {Record<string, string | number | boolean>} env */
function composeEnvArgs(env) {
  return Object.entries(env).flatMap(([key, value]) => ["-e", `${key}=${String(value)}`]);
}

const TEST_PROBE_SERVICE = "test-probe";

/**
 * @param {string} script
 * @param {{ env?: Record<string, string | number | boolean> }} [opts]
 */
export function runProbeNode(script, { env = {} } = {}) {
  const envArgs = composeEnvArgs(env);
  return sh(["docker", "compose", "exec", "-T", ...envArgs, TEST_PROBE_SERVICE, "node"], {
    input: script,
  });
}

/**
 * @param {string} script
 * @param {{ env?: Record<string, string | number | boolean>, input?: string | Buffer, evalScript?: boolean }} [options]
 */
export function runProbeNodeAsync(script, { env = {}, input = "", evalScript = false } = {}) {
  const envArgs = Object.entries(env).flatMap(([key, value]) => ["-e", `${key}=${String(value)}`]);
  const args = ["compose", "exec", "-T", ...envArgs, TEST_PROBE_SERVICE, "node"];
  if (evalScript) args.push("-e", script);
  const child = spawn("docker", args, {
    cwd: ROOT,
    env: process.env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk.toString("utf8"); });
  child.stderr.on("data", (chunk) => { stderr += chunk.toString("utf8"); });
  child.stdin.end(evalScript ? input : script);
  return new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`docker compose exec failed (${code}): ${(stderr || stdout).trim()}`));
        return;
      }
      resolve(stdout);
    });
  });
}

/** @param {string} service @param {string[]} args */
export function composeExec(service, args) {
  return sh(["docker", "compose", "exec", "-T", service, ...args]);
}

/** @param {string} service */
export function composeStop(service) {
  return sh(["docker", "compose", "stop", service]);
}

/** @param {string} service */
export function composeStart(service) {
  return sh(["docker", "compose", "start", service]);
}

/** @param {string} service */
export function composeRestart(service) {
  sh(["docker", "compose", "restart", service]);
  return composeUp(["--wait", service]);
}

/** @param {string} service @param {number} replicas */
export function composeScale(service, replicas) {
  return composeUp(["--wait", "--scale", `${service}=${replicas}`, service]);
}

/** @param {string} service */
export function composeRecreate(service) {
  sh(["docker", "compose", "rm", "-sf", service]);
  return composeUp(["--wait", service]);
}

/** @param {string} service */
export function composeKill(service) {
  return sh(["docker", "compose", "kill", "-s", "SIGKILL", service]);
}

/**
 * @template T
 * @param {string} service
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withServiceStopped(service, fn) {
  composeStop(service);
  try {
    return await fn();
  } finally {
    composeScale(service, 1);
  }
}
