import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";

import { readRepoFile, repoPath, withoutLineComments } from "../helpers/source-scan.js";
import {
  scannedTestFiles,
  withoutStringAndTemplateLiterals,
} from "../helpers/style-contract-scanner.js";

test("test files use moduleDataUrl/repositoryFileUrl instead of raw fs+URL boilerplate", () => {
  // Skips:
  //   test-helper-style-contracts.test.js — holds tripwire literals as error-message text.
  //   load-shared-module.js — defines moduleDataUrl itself; cannot self-call.
  const EXEMPT = new Set([
    "tests/unit/test-helper-style-contracts.test.js",
    "tests/helpers/load-shared-module.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = readRepoFile(file);
    if (/`data:text\/javascript,\$\{encodeURIComponent\(/.test(source)) {
      offenders.push(`${file}: raw data: URL boilerplate — use moduleDataUrl(src)`);
    }
    // Catch any pathToFileURL(path.resolve(...)) regardless of base (__dirname
    // / ROOT / etc.) — repositoryFileUrl(rel) covers all of them.
    if (/pathToFileURL\(path\.resolve\(/.test(source)) {
      offenders.push(`${file}: manual pathToFileURL + path.resolve — use repositoryFileUrl("...")`);
    }
  }
  assert.deepEqual(offenders, [], `test helper conventions violated:\n${offenders.join("\n")}`);
});

test("test module source rewrites go through load-shared-module helpers", () => {
  const testFiles = scannedTestFiles();
  /** @type {string[]} */
  const offenders = [];
  const sourceReader = String.raw`(?:readFileSync|readRepositoryFile|readRepositoryModuleSource)`;
  const sourceProducer = String.raw`(?:${sourceReader}|applyModuleReplacements)`;
  const sourceProducerCall = new RegExp(String.raw`\b${sourceProducer}\s*\(`, "g");
  const sourceProducerAssignment = new RegExp(
    String.raw`\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*${sourceProducer}\s*\(`,
    "g"
  );
  const findMatchingParen = (/** @type {string} */ text, /** @type {number} */ openIndex) => {
    if (openIndex < 0 || openIndex >= text.length || text[openIndex] !== "(") return -1;
    let depth = 1;
    for (let idx = openIndex + 1; idx < text.length; idx += 1) {
      const ch = text[idx];
      if (ch === "(") depth += 1;
      if (ch === ")") {
        depth -= 1;
        if (depth === 0) return idx;
      }
    }
    return -1;
  };
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    for (const match of source.matchAll(sourceProducerCall)) {
      const open = source.indexOf("(", match.index);
      const close = findMatchingParen(source, open);
      if (close !== -1 && /^\s*(?:\.|\?\.)\s*replace(?:All)?\s*\(/.test(source.slice(close + 1))) {
        offenders.push(`${file}: direct source producer .replace chain`);
        break;
      }
    }
    for (const match of source.matchAll(sourceProducerAssignment)) {
      const variable = match[1];
      const open = source.indexOf("(", match.index);
      const close = findMatchingParen(source, open);
      const statementEnd = close === -1 ? -1 : source.indexOf(";", close);
      if (statementEnd === -1) continue;
      const afterAssignment = source.slice(statementEnd + 1);
      if (new RegExp(String.raw`\b${RegExp.escape(variable)}\s*(?:\.|\?\.)\s*replace(?:All)?\s*\(`).test(afterAssignment)) {
        offenders.push(`${file}: source producer variable ${variable} is rewritten with .replace`);
      }
    }
  }
  assert.deepEqual(offenders, [], `test module source rewrites must use load-shared-module:\n${offenders.join("\n")}`);
});

test("test global mocks go through mock-global helpers", () => {
  const testFiles = scannedTestFiles();
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (/globalThis\.(?!__)[A-Za-z_$][\w$]*\s*=(?!=)/.test(source)) {
      offenders.push(`${file}: use withMockedGlobal(...) or a typed wrapper such as withMockedFetch(...)`);
    }
    if (/\bconsole\.(?:log|warn|error|info)\s*=(?!=)/.test(source)) {
      offenders.push(`${file}: use withMockedProperty(console, ...)`);
    }
    if (/(?:^|[^\w$.])(?:fetch|setTimeout|clearTimeout|setInterval|clearInterval)\s*=(?!=)/.test(source)) {
      offenders.push(`${file}: use withMockedGlobal(...) for global function mocks`);
    }
    // Keep this allow-list in sync with the global-property mock targets named
    // in docs/testing.md. A fully generic `object.property =` matcher would
    // reject ordinary fixture setup assignments.
    if (/\b(?:process\.stderr|AbortSignal|Object|JSON|Date|Math|Headers\.prototype|Array\.prototype|Function\.prototype)\.[A-Za-z_$][\w$]*\s*=(?!=)/.test(source)) {
      offenders.push(`${file}: use withMockedProperty(...) for built-in global property mocks`);
    }
    if (/\bObject\.defineProperty\(\s*(?:Object|Function|Array|Headers|Request|Response|Promise|RegExp)\.prototype\s*,/.test(source)) {
      offenders.push(`${file}: use withMockedPropertyDescriptor(...) for built-in prototype descriptor mocks`);
    }
  }
  assert.deepEqual(offenders, [], `test global mocks must use tests/helpers/mock-global.js:\n${offenders.join("\n")}`);
});

test("auth index tests access mock state through the harness", () => {
  const EXEMPT = new Set([
    "tests/helpers/load-auth-index.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (/\b__authMockState\b/.test(source)) {
      offenders.push(`${file}: use authMockState(...) or lastAuthLog(...) from tests/helpers/load-auth-index.js`);
    }
  }
  assert.deepEqual(offenders, [], `auth index mock state accessors violated:\n${offenders.join("\n")}`);
});

test("test output capture goes through output-capture helpers", () => {
  const EXEMPT = new Set([
    "tests/helpers/output-capture.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  const directOutputMock = /installMockProperty\(\s*(?:console|process\.(?:stderr|stdout))\s*,/;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (directOutputMock.test(source)) {
      offenders.push(`${file}: use tests/helpers/output-capture.js for console or stream output capture`);
    }
    if (/\bcaptureConsole\b/.test(source)) {
      offenders.push(`${file}: use withCapturedConsole(...) from tests/helpers/output-capture.js`);
    }
  }
  assert.deepEqual(offenders, [], `test output capture must use tests/helpers/output-capture.js:\n${offenders.join("\n")}`);
});

test("test request-body and DO envelope decoders use shared helpers", () => {
  const EXEMPT = new Set([
    "tests/helpers/do-envelope.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  const directRequestBodyJsonParse =
    /JSON\.parse\(\s*(?:String\()?[^;\n)]*(?:init|call|calls|request)\??\.[^;\n)]*body/;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (directRequestBodyJsonParse.test(source)) {
      offenders.push(`${file}: use tests/helpers/request-body.js for mock request-body JSON parsing`);
    }
    if (/\bparseRequestInitJsonBody\b/.test(source)) {
      offenders.push(`${file}: use tests/helpers/request-body.js instead of a file-local request body parser`);
    }
    if (/\bfunction\s+decodeDoEnvelope\b|\bconst\s+decodeDoEnvelope\s*=/.test(source)) {
      offenders.push(`${file}: use tests/helpers/do-envelope.js instead of a file-local DO envelope decoder`);
    }
    if (/new\s+DataView\([^;\n]+?\)\.getUint32\(0,\s*false\)[\s\S]{0,300}?new\s+TextDecoder\(\)\.decode\(/.test(source)) {
      offenders.push(`${file}: use tests/helpers/do-envelope.js instead of inline DO envelope decoding`);
    }
  }
  assert.deepEqual(offenders, [], `test request body/envelope helpers violated:\n${offenders.join("\n")}`);
});

test("integration response JSON parsing uses response accessors", () => {
  const testFiles = scannedTestFiles()
    .filter((file) => file.startsWith("tests/integration/"));
  /** @type {string[]} */
  const offenders = [];
  const directResponseBodyParsers = [
    /JSON\.parse\(\s*[A-Za-z_$][\w$]*\.body\s*(?:\|\|\s*""\s*)?\)/,
    /JSON\.parse\(\s*[A-Za-z_$][\w$]*\([\s\S]{0,500}?\)\.body\s*(?:\|\|\s*""\s*)?\)/,
    /JSON\.parse\(\s*await\s*[A-Za-z_$][\w$]*\.text\(\)\s*\)/,
    /JSON\.parse\(\s*await\s*[A-Za-z_$][\w$]*\([\s\S]{0,500}?\)\.text\(\)\s*\)/,
    /json\s*:\s*(?:async\s*)?\([^)]*\)\s*=>\s*JSON\.parse\(/,
  ];
  const responseBodyAssignment =
    /\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*[A-Za-z_$][\w$]*\.body\s*;/g;
  const responseTextAssignment =
    /\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*await\s+[A-Za-z_$][\w$]*\.text\(\)\s*;/g;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (directResponseBodyParsers.some((pattern) => pattern.test(source))) {
      offenders.push(`${file}: use responseJson(...) or responseJsonOrNull(...) for integration response bodies`);
      continue;
    }
    for (const match of source.matchAll(responseBodyAssignment)) {
      const variable = match[1];
      const afterAssignment = source.slice((match.index ?? 0) + match[0].length);
      if (new RegExp(String.raw`JSON\.parse\(\s*${RegExp.escape(variable)}\s*\)`).test(afterAssignment)) {
        offenders.push(`${file}: use responseJson(...) instead of parsing a response body alias`);
        break;
      }
    }
    for (const match of source.matchAll(responseTextAssignment)) {
      const variable = match[1];
      const afterAssignment = source.slice((match.index ?? 0) + match[0].length);
      if (new RegExp(String.raw`JSON\.parse\(\s*${RegExp.escape(variable)}\s*\)`).test(afterAssignment)) {
        offenders.push(`${file}: use responseJson(...) instead of parsing a response text alias`);
        break;
      }
    }
  }
  assert.deepEqual(offenders, [], `integration response JSON accessors violated:\n${offenders.join("\n")}`);
});

test("integration fetch response JSON uses integration JSON helpers", () => {
  const testFiles = scannedTestFiles()
    .filter((file) => file.startsWith("tests/integration/"))
    .filter((file) => file.endsWith(".test.js") || file.endsWith(".manual.mjs") || file.startsWith("tests/integration/helpers/"));
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = withoutStringAndTemplateLiterals(withoutLineComments(readRepoFile(file)));
    const hasDirectAwaitJson = hasAwaitedJsonCall(source);
    if (hasDirectAwaitJson) {
      offenders.push(`${file}: use readIntegrationJson(...) or assertIntegrationJson(...) instead of awaiting response.json()`);
    }
  }
  assert.deepEqual(offenders, [], `integration direct response JSON helpers violated:\n${offenders.join("\n")}`);
});

/** @param {string} source */
function hasAwaitedJsonCall(source) {
  let searchFrom = 0;
  while (true) {
    const jsonIndex = source.indexOf(".json", searchFrom);
    if (jsonIndex === -1) return false;
    searchFrom = jsonIndex + ".json".length;

    let afterJson = searchFrom;
    while (afterJson < source.length && /\s/.test(source[afterJson])) afterJson += 1;
    if (source[afterJson] !== "(") continue;

    const statementStart = Math.max(
      source.lastIndexOf(";", jsonIndex),
      source.lastIndexOf("{", jsonIndex),
      source.lastIndexOf("}", jsonIndex)
    ) + 1;
    if (/\bawait\b/.test(source.slice(statementStart, jsonIndex))) return true;
  }
}

test("integration Redis commands use typed Redis helpers", () => {
  const DIRECT_REDIS_HELPER_FILES = new Set([
    "tests/integration/helpers/redis.js",
  ]);
  const testFiles = scannedTestFiles()
    .filter((file) => file.startsWith("tests/integration/"))
    .filter((file) => file.endsWith(".test.js") || file.startsWith("tests/integration/helpers/"));
  const directRedisCommand = /\bredis-cli\b|composeExec\(\s*["']redis["']/;
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    const hasDirectRedis = directRedisCommand.test(source);
    if (hasDirectRedis && !DIRECT_REDIS_HELPER_FILES.has(file)) {
      offenders.push(`${file}: use tests/integration/helpers/redis.js instead of direct redis-cli`);
    }
  }
  assert.deepEqual(offenders, [], `integration direct Redis helper contract violated:\n${offenders.join("\n")}`);
});

test("integration status assertions use status helpers for structured diagnostics", () => {
  const testFiles = scannedTestFiles()
    .filter((file) => file.startsWith("tests/integration/"));
  /** @type {string[]} */
  const offenders = [];
  const legacyStatusDiagnostic =
    /assert\.(?:equal|notEqual|ok)\([^;\n]*(?:\.status|\.ok)[^;\n]*JSON\.stringify\(/;
  for (const file of testFiles) {
    const lines = withoutLineComments(readRepoFile(file)).split("\n");
    if (lines.some((line) => legacyStatusDiagnostic.test(line))) {
      offenders.push(`${file}: use assertStatus(...), assertStatusIn(...), or assertNotStatus(...)`);
    }
  }
  assert.deepEqual(offenders, [], `integration status assertion diagnostics violated:\n${offenders.join("\n")}`);
});

test("unit JSON response assertions use response-json helper", () => {
  const testFiles = scannedTestFiles()
    .filter((file) => file.startsWith("tests/unit/"));
  /** @type {string[]} */
  const offenders = [];
  const legacyStatusDiagnostic =
    /assert\.(?:equal|notEqual|ok)\([^;\n]*(?:\.status|\.ok)[^;\n]*JSON\.stringify\(/;
  const statusAssertion =
    /assert\.equal\(\s*([A-Za-z_$][\w$]*)\.status\s*,\s*2\d\d\b/;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (legacyStatusDiagnostic.test(source)) {
      offenders.push(`${file}: use response-json/assertion helpers instead of JSON.stringify status diagnostics`);
      continue;
    }
    const lines = source.split("\n");
    for (let index = 0; index < lines.length; index += 1) {
      const match = statusAssertion.exec(lines[index]);
      if (!match) continue;
      const responseVariable = match[1];
      const followingLines = lines.slice(index + 1, index + 5).join("\n");
      const jsonRead = new RegExp(
        String.raw`(?:await\s+${RegExp.escape(responseVariable)}\.json\(|\(\s*await\s+${RegExp.escape(responseVariable)}\.json\(\s*\))`
      );
      if (jsonRead.test(followingLines)) {
        offenders.push(`${file}: use readJsonResponse(...) or assertJsonResponse(...) for 2xx JSON responses`);
        break;
      }
    }
  }
  assert.deepEqual(offenders, [], `unit JSON response helper contract violated:\n${offenders.join("\n")}`);
});

test("tests use delay helper for simple sleep promises", () => {
  const testFiles = scannedTestFiles()
    .filter((file) => !file.includes("/manual/"));
  /** @type {string[]} */
  const offenders = [];
  const simpleSleepPromise =
    /await\s+new\s+Promise\(\s*\(?\s*(?:resolve|r)\s*\)?\s*=>\s*setTimeout\(\s*(?:resolve|r)\s*,/;
  for (const file of testFiles) {
    const source = withoutStringAndTemplateLiterals(withoutLineComments(readRepoFile(file)));
    if (simpleSleepPromise.test(source)) {
      offenders.push(`${file}: use delay(...) from tests/helpers/timing.js`);
    }
  }
  assert.deepEqual(offenders, [], `test sleep helper contract violated:\n${offenders.join("\n")}`);
});

test("test temporary directories use temp-dir helper", () => {
  const EXEMPT = new Set([
    "tests/helpers/temp-dir.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  const tempDirBoilerplate = [
    /\bmkdtempSync\(/,
    /\brmSync\([^;\n]*\{[^;\n]*recursive\s*:\s*true/,
  ];
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (tempDirBoilerplate.some((pattern) => pattern.test(source))) {
      offenders.push(`${file}: use tests/helpers/temp-dir.js for temporary directories`);
    }
  }
  assert.deepEqual(offenders, [], `test temporary directory helpers violated:\n${offenders.join("\n")}`);
});

test("DO owner hint test fixtures use shared helper", () => {
  const EXEMPT = new Set([
    "tests/unit/style-contracts.test.js",
  ]);
  const testFiles = scannedTestFiles(EXEMPT);
  /** @type {string[]} */
  const offenders = [];
  const localOwnerHintFixture = /\bfunction\s+(?:ownerHintHeaders|ownerHintResponse|tenantBodyOwnerHintResponse)\b/;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (localOwnerHintFixture.test(source)) {
      offenders.push(`${file}: use tests/helpers/do-owner-hint.js for DO owner hint fixtures`);
    }
  }
  assert.deepEqual(offenders, [], `DO owner hint fixture contract violated:\n${offenders.join("\n")}`);
});

test("repository JSON fixtures use readRepositoryJson", () => {
  const testFiles = scannedTestFiles();
  /** @type {string[]} */
  const offenders = [];
  const fixtureJsonParse = /JSON\.parse\(\s*readFileSync\(\s*new URL\([^)]*fixtures\/[^)]*\.json/;
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    if (fixtureJsonParse.test(source)) {
      offenders.push(`${file}: use readRepositoryJson(...) for repository JSON fixtures`);
    }
  }
  assert.deepEqual(offenders, [], `repository JSON fixture helpers violated:\n${offenders.join("\n")}`);
});

test("structured JSON payloads use protocol-specific helpers", () => {
  const testFiles = scannedTestFiles();
  /** @type {string[]} */
  const offenders = [];
  const structuredJsonParsers = [
    {
      pattern: /JSON\.parse\(\s*await\s*readOneServerTextFrame\(/,
      message: "use readJsonServerFrame(...) for WebSocket JSON frames",
    },
    {
      pattern: /JSON\.parse\(\s*[A-Za-z_$][\w$]*\.frameText\s*\)/,
      message: "use frameJson(...) for WebSocket frameText payloads",
    },
    {
      pattern: /JSON\.parse\(\s*[A-Za-z_$][\w$]*\.data\s*\)/,
      message: "use a domain helper for SSE/log event JSON payloads",
    },
    {
      pattern: /JSON\.parse\(\s*Buffer\.from\([^)]*body_b64[\s\S]{0,160}["']base64["'][\s\S]{0,160}\.toString\(/,
      message: "use parseBase64Json(...) for base64 JSON payloads",
    },
    {
      pattern: /JSON\.parse\(\s*(?:runProbeNode|runProbeNodeAsync)\(/,
      message: "use parseStdoutJson(...) for command/node-eval JSON stdout",
    },
    {
      pattern: /JSON\.parse\(\s*(?:stdout|stderr)\[\d+\]\s*\)/,
      message: "use parseStdoutJson(...) for structured log stdout/stderr payloads",
    },
    {
      pattern: /JSON\.parse\(\s*(?:line|String\(\s*warnings\[\d+\]\[\d+\]\s*\))\s*\)/,
      message: "use parseJsonText(...) for structured console log payloads",
    },
    {
      pattern: /JSON\.parse\([\s\S]{0,160}\.store\.get\(/,
      message: "use parseStoredJson(...) for fake Redis/store JSON values",
    },
    {
      pattern: /JSON\.parse\(\s*redisHGetAll\(/,
      message: "use redisHashJsonField(...) or redisHGetJson(...) for Redis hash JSON fields",
    },
    {
      pattern: /JSON\.parse\(\s*[A-Za-z_$][\w$]*\.__meta__\s*\)/,
      message: "use redisHashJsonField(...) or readMeta(...) for Redis __meta__ fields",
    },
  ];
  for (const file of testFiles) {
    const source = withoutLineComments(readRepoFile(file));
    for (const { pattern, message } of structuredJsonParsers) {
      if (pattern.test(source)) {
        offenders.push(`${file}: ${message}`);
      }
    }
  }
  assert.deepEqual(offenders, [], `structured JSON helper contract violated:\n${offenders.join("\n")}`);
});

test("test files use canonical node:test import order (lifecycle then test)", () => {
  const TEST_HOOK_ORDER = ["before", "beforeEach", "after", "afterEach", "test"];
  const orderIndex = new Map(TEST_HOOK_ORDER.map((name, idx) => [name, idx]));
  const testFiles = [
    ...readdirSync(repoPath("tests/unit"))
      .filter((name) => name.endsWith(".test.js"))
      .map((name) => `tests/unit/${name}`),
    ...readdirSync(repoPath("tests/integration"))
      .filter((name) => name.endsWith(".test.js"))
      .map((name) => `tests/integration/${name}`),
  ];
  /** @type {string[]} */
  const offenders = [];
  for (const file of testFiles) {
    const source = readRepoFile(file);
    const matches = source.matchAll(/import\s+(?:\{\s*([^}]+?)\s*\}|test)\s+from\s+"node:test";?/g);
    for (const match of matches) {
      if (!match[1]) {
        offenders.push(`${file}: uses default-import form; use \`import { test } from "node:test"\``);
        continue;
      }
      const specs = match[1].split(",").map((s) => s.trim()).filter(Boolean);
      const known = specs.filter((s) => orderIndex.has(s));
      const sorted = [...known].sort((a, b) => /** @type {number} */ (orderIndex.get(a)) - /** @type {number} */ (orderIndex.get(b)));
      if (known.join(",") !== sorted.join(",")) {
        offenders.push(`${file}: \`{ ${match[1].trim()} }\` violates order (${TEST_HOOK_ORDER.join(", ")})`);
      }
    }
  }
  assert.deepEqual(offenders, [], `node:test import order violations:\n${offenders.join("\n")}`);
});
