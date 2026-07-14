import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import path from "node:path";
import { isAlias, isSeq, parseAllDocuments, parseDocument, visit } from "yaml";

import { readRepoFile, repoPath } from "./source-scan.js";

/**
 * @param {string} dir
 * @param {{ ignoreDirs?: Set<string> }} [options]
 * @returns {string[]}
 */
export function markdownFiles(dir, options = {}) {
  const ignoreDirs = options.ignoreDirs || new Set();
  /** @type {string[]} */
  const out = [];
  for (const entry of readdirSync(repoPath(dir), { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!ignoreDirs.has(entry.name)) {
        out.push(...markdownFiles(path.join(dir, entry.name), options));
      }
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      out.push(path.join(dir, entry.name));
    }
  }
  return out;
}

/** @param {string} file */
export function fixtureSourceFile(file) {
  return !file.includes("/.deploy-dist/") &&
    !file.includes("/.wrangler/") &&
    !file.includes("/node_modules/");
}

const TEST_SOURCE_EXTENSIONS = new Set([".js", ".cjs", ".mjs"]);

/**
 * @param {string} dir
 * @returns {string[]}
 */
export function testSourceFiles(dir) {
  /** @type {string[]} */
  const out = [];
  for (const entry of readdirSync(repoPath(dir), { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...testSourceFiles(full));
    } else if (entry.isFile() && TEST_SOURCE_EXTENSIONS.has(path.extname(entry.name))) {
      out.push(full);
    }
  }
  return out;
}

/**
 * @param {string} file
 * @param {{ merge?: boolean }} [options]
 */
export function yamlDocument(file, options = {}) {
  const document = parseDocument(readRepoFile(file), { merge: options.merge === true });
  assert.equal(document.errors.length, 0, `${file} must parse as YAML`);
  return document;
}

/** @param {string} file */
export function yamlDocuments(file) {
  const documents = parseAllDocuments(readRepoFile(file));
  for (const document of documents) {
    assert.equal(document.errors.length, 0, `${file} must parse as YAML`);
  }
  return documents;
}

/**
 * @param {import("yaml").Document} document
 * @param {(string | number)[]} pathParts
 * @returns {string[]}
 */
export function yamlMergeAliases(document, pathParts) {
  const merge = document.getIn([...pathParts, "<<"], true);
  if (isAlias(merge)) return [merge.source];
  assert.ok(isSeq(merge), `YAML path ${pathParts.join(".")} must contain a merge alias`);
  assert.ok(merge.items.every(isAlias), `YAML path ${pathParts.join(".")} merge entries must be aliases`);
  return merge.items.map((item) => /** @type {import("yaml").Alias} */ (item).source);
}

/** @param {import("yaml").Document} document @param {string} source */
export function yamlAliasCount(document, source) {
  let count = 0;
  visit(document, {
    Alias(_key, alias) {
      if (alias.source === source) count += 1;
    },
  });
  return count;
}

/** @param {Set<string>} [exempt] */
export function scannedTestFiles(exempt = new Set()) {
  const scanned = [
    ...testSourceFiles("tests/helpers"),
    ...testSourceFiles("tests/integration/helpers"),
    ...testSourceFiles("tests/unit").filter((file) => file.endsWith(".test.js")),
    ...testSourceFiles("tests/integration")
      .filter((file) => file.endsWith(".test.js") || file.endsWith(".manual.mjs")),
  ];
  const scannedSet = new Set(scanned);
  const missingExemptions = [...exempt].filter((file) => !scannedSet.has(file));
  assert.deepEqual(
    missingExemptions,
    [],
    `style-contract exemptions must point at scanned test files:\n${missingExemptions.join("\n")}`
  );
  return scanned.filter((file) => !exempt.has(file));
}

/** @param {string} source */
export function withoutStringAndTemplateLiterals(source) {
  let out = "";
  let quote = "";
  for (let idx = 0; idx < source.length; idx += 1) {
    const ch = source[idx];
    if (!quote) {
      if (ch === "\"" || ch === "'" || ch === "`") {
        quote = ch;
        out += " ";
      } else {
        out += ch;
      }
      continue;
    }
    if (ch === "\\") {
      out += " ";
      idx += 1;
      out += source[idx] === "\n" ? "\n" : " ";
      continue;
    }
    if (ch === quote) {
      quote = "";
      out += " ";
    } else {
      out += ch === "\n" ? "\n" : " ";
    }
  }
  return out;
}

// Escaped character sequence inside a regexp literal body, e.g. `\/` or `\n`.
const REGEX_ESCAPED_CHAR = String.raw`\\.`;
// One valid character inside `[ ... ]`: either escaped or not `]`, `\`, or newline.
const REGEX_CLASS_CHAR = String.raw`(?:\\.|[^\]\\\n])`;
// Character class token, including delimiters.
const REGEX_CLASS_BODY = String.raw`\[${REGEX_CLASS_CHAR}*\]`;
// Regexp literal body token: escaped char, character class, or unescaped body char.
const REGEX_BODY_TOKEN = String.raw`(?:${REGEX_ESCAPED_CHAR}|${REGEX_CLASS_BODY}|[^/\\\n[])+`;
// Standard JavaScript regexp flags accepted by this source-contract matcher.
const REGEX_FLAGS = String.raw`[dgimsuvy]*`;
const REGEX_LITERAL_PATTERN = String.raw`/${REGEX_BODY_TOKEN}/${REGEX_FLAGS}`;

/** @param {string} source @param {RegExp[]} patterns */
export function objectJsonPayloads(source, patterns) {
  return patterns.flatMap((pattern) => source.match(pattern) || []);
}

/**
 * @param {string} text
 * @param {number} startIndex
 * @returns {string | null}
 */
export function extractBraceBlock(text, startIndex) {
  const openIndex = text.indexOf("{", startIndex);
  if (openIndex === -1) return null;
  let depth = 0;
  for (let i = openIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return text.slice(startIndex, i + 1);
    }
  }
  return null;
}

/**
 * @param {string} file
 * @param {string} name
 */
export function extractRegex(file, name) {
  const source = readRepoFile(file);
  const match = source.match(
    new RegExp(String.raw`(?:export\s+)?const ${RegExp.escape(name)}\s*=\s*(${REGEX_LITERAL_PATTERN})`),
  );
  assert.ok(match, `${file} must define ${name}`);
  return match[1];
}

/**
 * @param {string} source
 * @param {number} openIndex
 * @param {string} open
 * @param {string} close
 */
function findClosingDelimiter(source, openIndex, open, close) {
  let depth = 0;
  let quote = "";
  for (let index = openIndex; index < source.length; index += 1) {
    const char = source[index];
    if (quote) {
      if (char === "\\") index += 1;
      else if (char === quote) quote = "";
      continue;
    }
    if (char === '"') {
      quote = char;
    } else if (char === open) {
      depth += 1;
    } else if (char === close) {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return -1;
}

/** @param {string} source */
export function stripCapnpComments(source) {
  let out = "";
  let quoted = false;
  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    if (quoted) {
      out += char;
      if (char === "\\") {
        index += 1;
        out += source[index] ?? "";
      } else if (char === '"') {
        quoted = false;
      }
      continue;
    }
    if (char === '"') {
      quoted = true;
      out += char;
      continue;
    }
    if (char !== "#") {
      out += char;
      continue;
    }
    while (index < source.length && source[index] !== "\n") {
      out += " ";
      index += 1;
    }
    if (index < source.length) out += "\n";
  }
  return out;
}

/**
 * Extracts a Cap'n Proto const struct while ignoring comments and formatting.
 * @param {string} source
 * @param {string} name
 */
export function extractCapnpConstBlock(source, name) {
  const clean = stripCapnpComments(source);
  const declaration = new RegExp(`const\\s+${RegExp.escape(name)}\\s+:[^=]+=`).exec(clean);
  assert.ok(declaration, `Cap'n Proto const ${name} must be present`);
  const start = clean.indexOf("(", declaration.index + declaration[0].length);
  assert.notEqual(start, -1, `Cap'n Proto const ${name} must contain a struct value`);
  const end = findClosingDelimiter(clean, start, "(", ")");
  assert.notEqual(end, -1, `Cap'n Proto const ${name} must have balanced parentheses`);
  return clean.slice(start, end + 1);
}

/**
 * Returns normalized top-level tuple entries from a list field in a Cap'n Proto struct.
 * @param {string} block
 * @param {string} field
 */
export function extractCapnpListEntries(block, field) {
  const assignment = new RegExp(`\\b${RegExp.escape(field)}\\s*=\\s*\\[`).exec(block);
  assert.ok(assignment, `Cap'n Proto field ${field} must be present`);
  const listStart = block.indexOf("[", assignment.index);
  const listEnd = findClosingDelimiter(block, listStart, "[", "]");
  assert.notEqual(listEnd, -1, `Cap'n Proto field ${field} must have balanced brackets`);

  const entries = [];
  let index = listStart + 1;
  while (index < listEnd) {
    const start = block.indexOf("(", index);
    if (start === -1 || start >= listEnd) break;
    const end = findClosingDelimiter(block, start, "(", ")");
    assert.notEqual(end, -1, `Cap'n Proto field ${field} entry must have balanced parentheses`);
    entries.push(block.slice(start, end + 1).replace(/\s+/g, " ").trim());
    index = end + 1;
  }
  return entries;
}

/**
 * @param {string} source
 * @param {string} name
 */
export function extractStringConst(source, name) {
  const match = new RegExp(`const ${RegExp.escape(name)}\\s*=\\s*"([^"]+)"`).exec(source);
  assert.ok(match, `${name} string constant must be present`);
  return match[1];
}

/**
 * @param {string} source
 * @param {string} name
 */
export function extractStringSetConst(source, name) {
  const match = new RegExp(`const ${RegExp.escape(name)}\\s*=\\s*new Set\\(\\[([\\s\\S]*?)\\]\\);`).exec(source);
  assert.ok(match, `${name} string set must be present`);
  return [...match[1].matchAll(/"([^"]+)"/g)].map((entry) => entry[1]).toSorted();
}

/**
 * @param {string} source
 * @param {string} name
 */
export function extractAssignedConstant(source, name) {
  const match = source.match(new RegExp(`${RegExp.escape(name)}\\s*=\\s*([^;\\n]+)`));
  assert.ok(match, `${name} must be defined`);
  return match[1].trim();
}

/**
 * @param {string} file
 * @param {string} name
 */
export function extractExportedStringConst(file, name) {
  const source = readRepoFile(file);
  const match = source.match(new RegExp(`export const ${RegExp.escape(name)}\\s*=\\s*"([^"]+)"`));
  assert.ok(match, `${file} must export ${name}`);
  return match[1];
}
