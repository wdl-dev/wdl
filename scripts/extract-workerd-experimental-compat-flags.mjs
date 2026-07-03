#!/usr/bin/env node

import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_COMPATIBILITY_DATE_CAPNP = "src/workerd/io/compatibility-date.capnp";

/** @param {string} source */
export function extractExperimentalCompatFlags(source) {
  /** @type {string[]} */
  const blocks = [];
  /** @type {string[]} */
  let currentBlock = [];
  for (const line of source.split(/\n/)) {
    if (/^\s*[A-Za-z][A-Za-z0-9_]*\s+@\d+\s*:\s*Bool/.test(line)) {
      if (currentBlock.length) blocks.push(currentBlock.join("\n"));
      currentBlock = [line];
    } else if (currentBlock.length) {
      currentBlock.push(line);
    }
  }
  if (currentBlock.length) blocks.push(currentBlock.join("\n"));

  const flags = new Set();
  for (const block of blocks) {
    if (!block.includes("$experimental")) continue;
    for (const match of block.matchAll(/\$compatEnableFlag\("([^"]+)"\)/g)) {
      flags.add(match[1]);
    }
  }
  return [...flags].sort();
}

/** @param {string[]} [argv] */
export function runCli(argv = process.argv.slice(2)) {
  const input = argv[0] || DEFAULT_COMPATIBILITY_DATE_CAPNP;
  const source = readFileSync(input, "utf8");
  for (const flag of extractExperimentalCompatFlags(source)) {
    console.log(flag);
  }
}

function isMainModule() {
  return process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
}

if (isMainModule()) {
  runCli();
}
