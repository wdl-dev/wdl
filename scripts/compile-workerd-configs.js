#!/usr/bin/env node
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const WORKERD = resolve(ROOT, "node_modules/.bin/workerd");
const OUT_DIR = resolve(ROOT, "dist/workerd-configs");
const includeLocalConfigs =
  process.argv.includes("--local") || process.env.WDL_COMPILE_LOCAL_CONFIGS === "1";

const CONFIGS = [
  { name: "gateway", config: "gateway/config.capnp" },
  { name: "user-runtime", config: "runtime/config-user.capnp" },
  { name: "system-runtime", config: "runtime/config-system.capnp" },
  { name: "d1-runtime", config: "d1-runtime/config.capnp" },
  { name: "do-runtime", config: "do-runtime/config.capnp" },
  { name: "do-runtime-evictable", config: "do-runtime/config-evictable.capnp" },
];

if (includeLocalConfigs) {
  CONFIGS.push(
    { name: "gateway-local", config: "gateway/config-local.capnp" },
    { name: "user-runtime-local", config: "runtime/config-user-local.capnp" },
    { name: "system-runtime-local", config: "runtime/config-system-local.capnp" },
    { name: "do-runtime-local", config: "do-runtime/config-local.capnp" },
    { name: "do-runtime-local-evictable", config: "do-runtime/config-local-evictable.capnp" }
  );
}

if (!existsSync(WORKERD)) {
  process.stderr.write(
    `workerd binary not found at ${WORKERD}\n` +
      "Run `npm ci` before `npm run compile:workerd:local`.\n"
  );
  process.exit(1);
}

mkdirSync(OUT_DIR, { recursive: true });

for (const item of CONFIGS) {
  const out = resolve(OUT_DIR, `${item.name}.bin`);
  const result = spawnSync(
    WORKERD,
    ["compile", "--config-only", item.config, "config"],
    {
      cwd: ROOT,
      encoding: "buffer",
      maxBuffer: 128 * 1024 * 1024,
    }
  );
  if (result.error) {
    process.stderr.write(`workerd compile failed to start: ${result.error.message}\n`);
    process.exit(1);
  }
  if (result.status !== 0) {
    if (result.stderr?.length) {
      process.stderr.write(result.stderr);
    } else {
      process.stderr.write(`workerd compile failed for ${item.config}\n`);
    }
    process.exit(result.status || 1);
  }
  if (!result.stdout || result.stdout.length === 0) {
    process.stderr.write(`workerd compile produced empty output for ${item.config}\n`);
    process.exit(1);
  }
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, result.stdout);
  process.stdout.write(`${item.config} -> ${out}\n`);
}
