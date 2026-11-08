// Repository source-discovery helpers. Executable module specifiers use the
// OXC AST; the remaining scanners stay intentionally narrow.

import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseSync, Visitor } from "oxc-parser";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

const DEFAULT_JS_IGNORE_DIRS = new Set([".deploy-dist", ".wrangler", "node_modules"]);

/** @param {string} rel */
export function repoPath(rel) {
  return path.join(ROOT, rel);
}

/** @param {string} rel */
export function readRepoFile(rel) {
  return readFileSync(repoPath(rel), "utf8");
}

/**
 * Return executable static and string-literal dynamic module specifiers.
 * JSDoc imports and import-looking text inside source strings stay excluded.
 * @param {string} source
 * @param {string} [fileName]
 */
export function executableModuleSpecifiers(source, fileName = "module.js") {
  const parsed = parseSync(fileName, source, { lang: "js", sourceType: "module" });
  if (parsed.errors.length > 0) {
    throw new Error(`${fileName}: ${parsed.errors[0].message}`);
  }
  /** @type {string[]} */
  const specifiers = [];
  new Visitor({
    ImportDeclaration(node) { specifiers.push(node.source.value); },
    ExportNamedDeclaration(node) {
      if (node.source) specifiers.push(node.source.value);
    },
    ExportAllDeclaration(node) { specifiers.push(node.source.value); },
    ImportExpression({ source: specifier }) {
      if (specifier.type === "Literal" && typeof specifier.value === "string") {
        specifiers.push(specifier.value);
      } else if (
        specifier.type === "TemplateLiteral" && specifier.expressions.length === 0 &&
        typeof specifier.quasis[0].value.cooked === "string"
      ) {
        specifiers.push(specifier.quasis[0].value.cooked);
      } else {
        throw new Error(`${fileName}: dynamic import specifier must be a string literal`);
      }
    },
  }).visit(parsed.program);
  return specifiers;
}

/** @param {string} dir */
export function jsFiles(dir) {
  return jsFilesIn(repoPath(dir)).map((file) => path.relative(ROOT, file));
}

/**
 * @param {string} dir
 * @param {{ extensions: string[], ignoreDirs?: Set<string> }} options
 * @returns {string[]}
 */
export function sourceFiles(dir, options) {
  return sourceFilesIn(repoPath(dir), options).map((file) => path.relative(ROOT, file));
}

/**
 * @param {string} dir
 * @param {{ ignoreDirs?: Set<string> }} [options]
 * @returns {string[]}
 */
function jsFilesIn(dir, options = {}) {
  return sourceFilesIn(dir, { ...options, extensions: [".js"] });
}

/**
 * @param {string} dir
 * @param {{ extensions: string[], ignoreDirs?: Set<string> }} options
 * @returns {string[]}
 */
function sourceFilesIn(dir, options) {
  const ignoreDirs = new Set([...DEFAULT_JS_IGNORE_DIRS, ...(options.ignoreDirs || [])]);
  const extensions = new Set(options.extensions);
  /** @type {string[]} */
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory() && ignoreDirs.has(entry.name)) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...sourceFilesIn(full, options));
    } else if (entry.isFile() && extensions.has(path.extname(entry.name))) {
      out.push(full);
    }
  }
  return out;
}

/**
 * Repo-relative `.rs` files under `dir`, skipping Cargo `target/` build output.
 * @param {string} dir
 * @returns {string[]}
 */
export function rustFiles(dir) {
  /** @type {(abs: string) => string[]} */
  const walk = (abs) => {
    /** @type {string[]} */
    const out = [];
    for (const entry of readdirSync(abs, { withFileTypes: true })) {
      if (entry.isDirectory() && entry.name === "target") continue;
      const full = path.join(abs, entry.name);
      if (entry.isDirectory()) out.push(...walk(full));
      else if (entry.isFile() && entry.name.endsWith(".rs")) out.push(full);
    }
    return out;
  };
  return walk(repoPath(dir)).map((file) => path.relative(ROOT, file));
}

/** @param {string} source */
export function withoutLineComments(source) {
  return source.split("\n")
    .filter((line) => !line.trimStart().startsWith("//"))
    .join("\n");
}

/** @param {string} source */
export function withoutCapnpLineComments(source) {
  return withoutLineComments(source).split("\n")
    .filter((line) => !line.trimStart().startsWith("#"))
    .join("\n");
}
