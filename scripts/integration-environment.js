import { accessSync, constants, statSync } from "node:fs";
import path from "node:path";

export const ROOT = path.resolve(import.meta.dirname, "..");
export const COMPILE_WORKERD_LOCAL_ARGS = ["scripts/compile-workerd-configs.js", "--local"];
export const DOCKER_COMPOSE_BUILD_ARGS = ["compose", "build", "gateway", "workflows"];
export const DEFAULT_WDL_CLI_COMMAND = "wdl";
export const LOCAL_ADMIN_TOKEN = "local-dev-token";
export const ADMIN_HOST_HEADER = "admin.test";
export const LOCAL_CONNECT_HOST = "localhost";

/** @param {number} gatewayPort */
export function localControlUrl(gatewayPort) {
  return `http://${ADMIN_HOST_HEADER}:${gatewayPort}`;
}

/** @param {number} s3mockPort */
export function localAssetsCdnBase(s3mockPort) {
  return `http://${LOCAL_CONNECT_HOST}:${s3mockPort}/wdl-assets`;
}

/** @param {string} file */
function isExecutableFile(file) {
  try {
    if (!statSync(file).isFile()) return false;
    accessSync(file, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * @param {string} command
 * @param {NodeJS.ProcessEnv} env
 */
function resolveCommandOnPath(command, env) {
  const pathEnv = env.PATH || "";
  const extensions = process.platform === "win32"
    ? (env.PATHEXT || ".EXE;.CMD;.BAT;.COM").split(";")
    : [""];
  for (const dir of pathEnv.split(path.delimiter)) {
    if (!dir) continue;
    for (const ext of extensions) {
      const candidate = path.join(dir, command + ext.toLowerCase());
      if (isExecutableFile(candidate)) return candidate;
      const upperCandidate = path.join(dir, command + ext.toUpperCase());
      if (upperCandidate !== candidate && isExecutableFile(upperCandidate)) return upperCandidate;
    }
  }
  return null;
}

/** @param {NodeJS.ProcessEnv} [env] */
export function shouldPrepareIntegrationArtifacts(env = process.env) {
  return env.WDL_INTEGRATION_SKIP_PREPARE !== "1";
}

/** @param {NodeJS.ProcessEnv} [env] */
export function composeNoBuildFlag(env = process.env) {
  return env.WDL_INTEGRATION_NO_BUILD === "1" ? " --no-build" : "";
}

/** @param {NodeJS.ProcessEnv} [env] */
export function resolveWdlCliBin(env = process.env) {
  if (!env.WDL_CLI_BIN) {
    return resolveCommandOnPath(DEFAULT_WDL_CLI_COMMAND, env) || DEFAULT_WDL_CLI_COMMAND;
  }
  return path.isAbsolute(env.WDL_CLI_BIN)
    ? env.WDL_CLI_BIN
    : path.resolve(ROOT, env.WDL_CLI_BIN);
}

/** @param {NodeJS.ProcessEnv} [env] */
export function assertWdlCliAvailable(env = process.env) {
  const bin = resolveWdlCliBin(env);
  if (isExecutableFile(bin)) return bin;
  const source = env.WDL_CLI_BIN ? "WDL_CLI_BIN" : "PATH";
  process.stderr.write(
    `error: executable wdl CLI not found at ${bin} (${source}). ` +
    "Install @wdl-dev/cli globally, or set WDL_CLI_BIN to an executable downstream CLI checkout bin path.\n"
  );
  process.exit(2);
}
