export const WORKFLOWS_MODULE_NAME = "_wdl-cloudflare-workflows.js";

export const WORKFLOWS_MODULE_SOURCE = `
export { WorkflowEntrypoint } from "cloudflare:workers";

export class NonRetryableError extends Error {
  constructor(message = "Workflow step is not retryable", _name = undefined) {
    super(message);
    this.name = "NonRetryableError";
  }
}
`;

export const HOST_BINDING_RESERVED_MODULE_NAMES = Object.freeze([
  WORKFLOWS_MODULE_NAME,
  "_wdl-d1-data-field.js",
  "_wdl-d1-client.js",
  "_wdl-d1-params.js",
  "_wdl-sql-splitter.js",
  "_wdl-d1-transport.js",
  "_wdl-r2-client.js",
  "_wdl-r2-utils.js",
  "_wdl-do-client.js",
  "_wdl-do-transport.js",
  "_wdl-owner-endpoint.js",
  "_wdl-owner-hint-cache.js",
  "_wdl-request-id.js",
  "_wdl-workflows-client.js",
  "_wdl-host-wrapper-runtime.js",
  "_wdl-wrapper.js",
]);
export const HOST_BINDING_RESERVED_MODULES = new Set(HOST_BINDING_RESERVED_MODULE_NAMES);

/**
 * @typedef {{ start: number, end: number, value: string }} Replacement
 */

/** @param {{ modules: Record<string, unknown> }} workerCode */
export function rewriteCloudflareWorkflowsImports(workerCode) {
  for (const [name, source] of Object.entries(workerCode.modules)) {
    if (typeof source !== "string") continue;
    const depth = Math.max(0, name.split("/").length - 1);
    const localSpecifier = `${depth === 0 ? "./" : "../".repeat(depth)}${WORKFLOWS_MODULE_NAME}`;
    workerCode.modules[name] = rewriteCloudflareWorkflowsSpecifiers(source, localSpecifier);
  }
}

/** @param {string} source @param {number} quoteIndex */
function readQuotedSpecifier(source, quoteIndex) {
  const quote = source[quoteIndex];
  if (quote !== "\"" && quote !== "'") return null;
  let value = "";
  for (let i = quoteIndex + 1; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === "\\") {
      if (i + 1 >= source.length) return null;
      value += source.slice(i, i + 2);
      i += 1;
      continue;
    }
    if (ch === quote) return { start: quoteIndex + 1, end: i, value };
    value += ch;
  }
  return null;
}

// This module is loaded inside workerd, so keep the rewriter dependency-free:
// only touch real import/export specifier positions and skip user strings,
// comments, regex literals, and raw template text.
/** @param {string} source @param {number} index */
function skipQuoted(source, index) {
  const quote = source[index];
  for (let i = index + 1; i < source.length; i += 1) {
    if (source[i] === "\\") {
      i += 1;
    } else if (source[i] === quote) {
      return i + 1;
    }
  }
  return source.length;
}

/** @param {string} source @param {number} index */
function skipLineComment(source, index) {
  const next = source.indexOf("\n", index + 2);
  return next === -1 ? source.length : next + 1;
}

/** @param {string} source @param {number} index */
function skipBlockComment(source, index) {
  const next = source.indexOf("*/", index + 2);
  return next === -1 ? source.length : next + 2;
}

/** @param {string} ch */
function isIdentifierChar(ch) {
  return Boolean(ch) && /[A-Za-z0-9_$]/.test(ch);
}

/** @param {string} source @param {number} index */
function skipWhitespace(source, index) {
  let i = index;
  while (i < source.length && /\s/.test(source[i])) i += 1;
  return i;
}

/** @param {string} source @param {number} index */
function skipWhitespaceAndComments(source, index) {
  let i = index;
  for (;;) {
    i = skipWhitespace(source, i);
    if (source[i] === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (source[i] === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    return i;
  }
}

/** @param {string} source @param {number} lineStart @param {number} lineEnd */
function lineCommentStart(source, lineStart, lineEnd) {
  let cursor = lineStart;
  while (cursor < lineEnd) {
    if (source[cursor] === "\"" || source[cursor] === "'") {
      cursor = Math.min(skipQuoted(source, cursor), lineEnd);
      continue;
    }
    if (source[cursor] === "`") {
      cursor = Math.min(skipTemplateLiteral(source, cursor, lineEnd), lineEnd);
      continue;
    }
    if (source[cursor] === "/" && source[cursor + 1] === "/") return cursor;
    if (source[cursor] === "/" && source[cursor + 1] === "*") {
      cursor = Math.min(skipBlockComment(source, cursor), lineEnd);
      continue;
    }
    if (source[cursor] === "/" && looksLikeRegexLiteral(source, cursor, lineStart)) {
      cursor = Math.min(skipRegexLiteral(source, cursor, lineEnd), lineEnd);
      continue;
    }
    cursor += 1;
  }
  return -1;
}

/** @param {string} source @param {number} index */
function previousSignificantCharForImportCall(source, index) {
  for (let i = index - 1; i >= 0;) {
    if (source[i] === "\n") {
      const lineStart = (() => {
        let j = source[i - 1] === "\r" ? i - 2 : i - 1;
        while (j >= 0 && source[j] !== "\n" && source[j] !== "\r") j -= 1;
        return j + 1;
      })();
      const lineEnd = source[i - 1] === "\r" ? i - 1 : i;
      const commentStart = lineCommentStart(source, lineStart, lineEnd);
      if (commentStart !== -1) {
        i = commentStart - 1;
        continue;
      }
      i = lineEnd - 1;
      continue;
    }
    if (/\s/.test(source[i])) {
      i -= 1;
      continue;
    }
    if (source[i] === "/" && source[i - 1] === "*") {
      const open = source.lastIndexOf("/*", i - 1);
      if (open === -1) return source[i];
      i = open - 1;
      continue;
    }
    return source[i];
  }
  return "";
}

/** @param {string} source @param {number} index */
function previousSignificantIndexForModuleBoundary(source, index) {
  for (let i = index - 1; i >= 0;) {
    if (/\s/.test(source[i])) {
      i -= 1;
      continue;
    }
    if (source[i] === "/" && source[i - 1] === "*") {
      const open = source.lastIndexOf("/*", i - 1);
      if (open === -1) return i;
      i = open - 1;
      continue;
    }
    const lineStart = (() => {
      let j = i;
      while (j >= 0 && source[j] !== "\n" && source[j] !== "\r") j -= 1;
      return j + 1;
    })();
    const commentStart = lineCommentStart(source, lineStart, i + 1);
    if (commentStart !== -1) {
      i = commentStart - 1;
      continue;
    }
    return i;
  }
  return -1;
}

/** @param {string} source @param {number} index */
function isStaticModuleDeclarationBoundary(source, index) {
  const previousIndex = previousSignificantIndexForModuleBoundary(source, index);
  if (previousIndex === -1) return true;
  const previous = source[previousIndex];
  return previous === ";" || previous === "}" || /[\r\n]/.test(source.slice(previousIndex + 1, index));
}

/** @param {string} source @param {number} index @param {number} [lowerBound] */
function previousSignificantIndex(source, index, lowerBound = 0) {
  for (let i = index - 1; i >= lowerBound; i -= 1) {
    if (!/\s/.test(source[i])) return i;
  }
  return -1;
}

/** @param {string} source @param {number} index @param {number} [lowerBound] */
function previousIdentifierBefore(source, index, lowerBound = 0) {
  let i = index - 1;
  while (i >= lowerBound && /\s/.test(source[i])) i -= 1;
  const end = i + 1;
  while (i >= lowerBound && isIdentifierChar(source[i])) i -= 1;
  const start = i + 1;
  return { start, token: source.slice(start, end) };
}

/** @param {string} source @param {number} index @param {number} [start] */
function looksLikeRegexLiteralForPairScan(source, index, start = 0) {
  const prevIndex = previousSignificantIndex(source, index, start);
  const prev = prevIndex === -1 ? "" : source[prevIndex];
  if (!prev || "([{=,:;!&|?+-*~^<>".includes(prev)) return true;
  if (prev === "}" && hasLineTerminatorSincePreviousSignificantChar(source, index)) return true;
  if (prev === ")") return false;
  const previousIdentifier = previousIdentifierBefore(source, index, start);
  const beforeIdentifier = previousSignificantIndex(source, previousIdentifier.start, start);
  if (beforeIdentifier >= 0 && (source[beforeIdentifier] === "." || source[beforeIdentifier] === "#")) return false;
  return new Set(["return", "throw", "case", "yield", "await", "else", "typeof", "void", "delete", "in", "instanceof"]).has(previousIdentifier.token);
}

/** @param {string} source @param {number} closeIndex @param {number} [start] */
function matchingOpenParenIndex(source, closeIndex, start = 0) {
  const stack = [];
  for (let i = start; i <= closeIndex;) {
    const ch = source[i];
    if (ch === "\"" || ch === "'") {
      i = skipQuoted(source, i);
      continue;
    }
    if (ch === "`") {
      i = skipTemplateLiteral(source, i, closeIndex + 1);
      continue;
    }
    if (ch === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    if (ch === "/" && looksLikeRegexLiteralForPairScan(source, i, start)) {
      i = skipRegexLiteral(source, i, closeIndex + 1);
      continue;
    }
    if (ch === "(") {
      stack.push(i);
    } else if (ch === ")") {
      const open = stack.pop();
      if (i === closeIndex) return open ?? -1;
    }
    i += 1;
  }
  return -1;
}

/** @param {string} source @param {number} index */
function looksLikeRegexLiteralForBraceScan(source, index) {
  const prevIndex = previousSignificantIndex(source, index);
  const prev = prevIndex === -1 ? "" : source[prevIndex];
  if (!prev || "([{=,:;!&|?+-*~^<>".includes(prev)) return true;
  if (prev === "}" && hasLineTerminatorSincePreviousSignificantChar(source, index)) return true;
  if (prev === ")") return new Set(["if", "while", "for", "with", "catch", "switch"]).has(controlKeywordBeforeClosingParen(source, prevIndex));
  const previousIdentifier = previousIdentifierBefore(source, index);
  const beforeIdentifier = previousSignificantIndex(source, previousIdentifier.start);
  if (beforeIdentifier >= 0 && (source[beforeIdentifier] === "." || source[beforeIdentifier] === "#")) return false;
  return new Set(["return", "throw", "case", "yield", "await", "else", "typeof", "void", "delete", "in", "instanceof"]).has(previousIdentifier.token);
}

/** @param {string} source @param {number} closeIndex @param {number} [start] */
function matchingOpenBraceIndex(source, closeIndex, start = 0) {
  const stack = [];
  for (let i = start; i <= closeIndex;) {
    const ch = source[i];
    if (ch === "\"" || ch === "'") {
      i = skipQuoted(source, i);
      continue;
    }
    if (ch === "`") {
      i = skipTemplateLiteral(source, i, closeIndex + 1);
      continue;
    }
    if (ch === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    if (ch === "/" && looksLikeRegexLiteralForBraceScan(source, i)) {
      i = skipRegexLiteral(source, i, closeIndex + 1);
      continue;
    }
    if (ch === "{") {
      stack.push(i);
    } else if (ch === "}") {
      const open = stack.pop();
      if (i === closeIndex) return open ?? -1;
    }
    i += 1;
  }
  return -1;
}

/** @param {string} source @param {number} closeIndex @param {number} [start] */
function controlKeywordBeforeClosingParen(source, closeIndex, start = 0) {
  const openIndex = matchingOpenParenIndex(source, closeIndex, start);
  return openIndex === -1 ? "" : previousIdentifierBefore(source, openIndex, start).token;
}

/** @param {string} source @param {number} index @param {number} [start] */
function hasStatementBoundaryBefore(source, index, start = 0) {
  const previous = previousSignificantIndex(source, index, start);
  if (previous === -1) return true;
  return [";", "{", "}"].includes(source[previous]);
}

/** @param {string} source @param {number} keywordStart @param {number} [lowerBound] */
function declarationStartIndex(source, keywordStart, lowerBound = 0) {
  let cursor = keywordStart;
  for (;;) {
    const previous = previousIdentifierBefore(source, cursor, lowerBound);
    if (!new Set(["async", "default", "export"]).has(previous.token)) return cursor;
    cursor = previous.start;
  }
}

/** @param {string} source @param {number} openBraceIndex @param {number} [start] */
function looksLikeFunctionBlockOpen(source, openBraceIndex, start = 0) {
  const beforeBrace = previousSignificantIndex(source, openBraceIndex, start);
  if (beforeBrace === -1) return false;
  if (source[beforeBrace] === ">") return source[beforeBrace - 1] === "=";
  if (source[beforeBrace] !== ")") return false;
  const openParen = matchingOpenParenIndex(source, beforeBrace, start);
  if (openParen === -1) return false;
  const beforeParams = previousIdentifierBefore(source, openParen, start);
  const functionToken = beforeParams.token === "function"
    ? beforeParams
    : previousIdentifierBefore(source, beforeParams.start, start);
  if (functionToken.token !== "function") return false;
  return hasStatementBoundaryBefore(source, declarationStartIndex(source, functionToken.start, start), start);
}

/** @param {string} source @param {number} openBraceIndex @param {number} [start] */
function looksLikeClassBlockOpen(source, openBraceIndex, start = 0) {
  let cursor = (() => {
    for (let i = openBraceIndex - 1; i >= start; i -= 1) {
      if (source[i] === ";") return i + 1;
      if (source[i] === "\n" || source[i] === "\r") return i + 1;
    }
    return start;
  })();
  cursor = skipWhitespaceAndComments(source, cursor);
  if (source.startsWith("export", cursor) && !isIdentifierChar(source[cursor - 1]) && !isIdentifierChar(source[cursor + 6])) {
    cursor = skipWhitespaceAndComments(source, cursor + 6);
    if (source.startsWith("default", cursor) && !isIdentifierChar(source[cursor - 1]) && !isIdentifierChar(source[cursor + 7])) {
      cursor = skipWhitespaceAndComments(source, cursor + 7);
    }
  }
  return source.startsWith("class", cursor)
    && !isIdentifierChar(source[cursor - 1])
    && !isIdentifierChar(source[cursor + 5]);
}

/** @param {string} source @param {number} closeBraceIndex @param {number} [start] */
function looksLikeStatementBlockClose(source, closeBraceIndex, start = 0) {
  const openBrace = matchingOpenBraceIndex(source, closeBraceIndex, start);
  if (openBrace === -1) return false;
  const beforeBrace = previousSignificantIndex(source, openBrace, start);
  if (beforeBrace === -1) return true;
  if ([";", "{", "}"].includes(source[beforeBrace])) return true;
  if (source[beforeBrace] === ")") {
    if (new Set(["if", "while", "for", "with", "catch", "switch"]).has(controlKeywordBeforeClosingParen(source, beforeBrace, start))) return true;
    return looksLikeFunctionBlockOpen(source, openBrace, start) || looksLikeClassBlockOpen(source, openBrace, start);
  }
  const beforeBlock = previousIdentifierBefore(source, openBrace, start);
  if (new Set(["try", "finally", "else"]).has(beforeBlock.token)) return true;
  if (source[beforeBrace] === ":") return true;
  if (looksLikeClassBlockOpen(source, openBrace, start)) return true;
  return false;
}

/** @param {string} source @param {number} index */
function hasLineTerminatorSincePreviousSignificantChar(source, index) {
  const previousIndex = previousSignificantIndex(source, index);
  return previousIndex >= 0 && /[\r\n]/.test(source.slice(previousIndex + 1, index));
}

/** @param {string} source @param {number} index @param {number} [end] */
function skipRegexLiteral(source, index, end = source.length) {
  let inClass = false;
  for (let i = index + 1; i < end; i += 1) {
    const ch = source[i];
    if (ch === "\\") {
      i += 1;
      continue;
    }
    if (ch === "[") {
      inClass = true;
      continue;
    }
    if (ch === "]") {
      inClass = false;
      continue;
    }
    if (ch === "/" && !inClass) {
      i += 1;
      while (i < end && /[A-Za-z]/.test(source[i] || "")) i += 1;
      return i;
    }
  }
  return end;
}

/** @param {string} source @param {number} index @param {number} [start] */
function looksLikeRegexLiteral(source, index, start = 0) {
  const prevIndex = previousSignificantIndex(source, index, start);
  const prev = prevIndex === -1 ? "" : source[prevIndex];
  if (!prev || "([{=,:;!&|?+-*~^<>".includes(prev)) return true;
  if (prev === "}" && hasLineTerminatorSincePreviousSignificantChar(source, index)) {
    return looksLikeStatementBlockClose(source, prevIndex, start);
  }
  if (prev === ")") {
    return new Set(["if", "while", "for", "with"]).has(controlKeywordBeforeClosingParen(source, prevIndex, start));
  }
  const previousIdentifier = previousIdentifierBefore(source, index, start);
  const beforeIdentifier = previousSignificantIndex(source, previousIdentifier.start, start);
  if (beforeIdentifier >= 0 && (source[beforeIdentifier] === "." || source[beforeIdentifier] === "#")) {
    return false;
  }
  return new Set(["return", "throw", "case", "yield", "await", "else", "typeof", "void", "delete", "in", "instanceof"]).has(previousIdentifier.token);
}

/** @param {string} source @param {number} index @param {number} [end] */
function skipTemplateRaw(source, index, end = source.length) {
  let i = index + 1;
  while (i < end) {
    const ch = source[i];
    if (ch === "\\") {
      i += 2;
      continue;
    }
    if (ch === "`") return { kind: "end", index: i + 1 };
    if (ch === "$" && source[i + 1] === "{") return { kind: "expr", index: i + 2 };
    i += 1;
  }
  return { kind: "end", index: end };
}

/** @param {string} source @param {number} index @param {number} end */
function skipTemplateLiteral(source, index, end) {
  let next = skipTemplateRaw(source, index, end);
  while (next.kind === "expr") {
    const close = findTemplateExpressionClose(source, next.index, end);
    if (close >= end) return end;
    next = skipTemplateRaw(source, close, end);
  }
  return next.index;
}

/** @param {string} source @param {number} index @param {number} end */
function findTemplateExpressionClose(source, index, end) {
  let depth = 1;
  for (let i = index; i < end;) {
    const ch = source[i];
    if (ch === "\"" || ch === "'") {
      i = skipQuoted(source, i);
      continue;
    }
    if (ch === "`") {
      i = skipTemplateLiteral(source, i, end);
      continue;
    }
    if (ch === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    if (ch === "/" && looksLikeRegexLiteral(source, i, index)) {
      i = skipRegexLiteral(source, i, end);
      continue;
    }
    if (ch === "{") {
      depth += 1;
    } else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
    i += 1;
  }
  return end;
}

/** @param {string} source @param {number} keywordStart @param {string} keyword */
function staticSpecifierQuoteIndex(source, keywordStart, keyword) {
  let i = keywordStart + keyword.length;
  let sawFrom = false;
  let braceDepth = 0;
  while (i < source.length) {
    const ch = source[i];
    if (ch === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }
    if (keyword === "export" && !sawFrom && braceDepth === 0 && !isIdentifierChar(source[i - 1]) && /^(?:async|class|const|default|function|let|var)\b/.test(source.slice(i))) {
      return -1;
    }
    if (ch === "{") {
      braceDepth += 1;
      i += 1;
      continue;
    }
    if (ch === "}") {
      if (braceDepth > 0) braceDepth -= 1;
      i += 1;
      continue;
    }
    if (braceDepth === 0 && source.startsWith("from", i) && !isIdentifierChar(source[i - 1]) && !isIdentifierChar(source[i + 4])) {
      sawFrom = true;
      i += 4;
      continue;
    }
    if (ch === "\"" || ch === "'") {
      if (!sawFrom) {
        i = skipQuoted(source, i);
        continue;
      }
      return i;
    }
    if ((ch === ":" || ch === "=") && !sawFrom) return -1;
    if (ch === ";") break;
    i += 1;
  }
  return -1;
}

/** @param {string} source @param {string} localSpecifier @param {Replacement[]} replacements @param {number} [start] @param {number} [end] */
function collectCloudflareWorkflowsSpecifiers(source, localSpecifier, replacements, start = 0, end = source.length) {
  for (let i = start; i < end;) {
    const ch = source[i];
    if (ch === "\"" || ch === "'") {
      i = skipQuoted(source, i);
      continue;
    }
    if (ch === "`") {
      let next = skipTemplateRaw(source, i, end);
      while (next.kind === "expr") {
        const exprStart = next.index;
        const exprClose = findTemplateExpressionClose(source, exprStart, end);
        collectCloudflareWorkflowsSpecifiers(source, localSpecifier, replacements, exprStart, exprClose);
        next = skipTemplateRaw(source, exprClose, end);
      }
      i = next.index;
      continue;
    }
    if (ch === "/" && source[i + 1] === "/") {
      i = skipLineComment(source, i);
      continue;
    }
    if (ch === "/" && source[i + 1] === "*") {
      i = skipBlockComment(source, i);
      continue;
    }
    if (ch === "/" && looksLikeRegexLiteral(source, i, start)) {
      i = skipRegexLiteral(source, i);
      continue;
    }
    if (
      source.startsWith("import", i) &&
      !isIdentifierChar(source[i - 1]) &&
      !isIdentifierChar(source[i + 6])
    ) {
      const afterImport = skipWhitespaceAndComments(source, i + 6);
      if (source[afterImport] === ".") {
        i += 6;
        continue;
      }
      const isSideEffectImport = source[afterImport] === "\"" || source[afterImport] === "'";
      const isDynamicImport = source[afterImport] === "(";
      if (!isDynamicImport && !isStaticModuleDeclarationBoundary(source, i)) {
        i += 6;
        continue;
      }
      if (isDynamicImport && [".", "#"].includes(previousSignificantCharForImportCall(source, i))) {
        i += 6;
        continue;
      }
      const quoteIndex = isDynamicImport
        ? skipWhitespaceAndComments(source, afterImport + 1)
        : isSideEffectImport
          ? afterImport
          : staticSpecifierQuoteIndex(source, i, "import");
      const specifier = readQuotedSpecifier(source, quoteIndex);
      if (specifier?.value === "cloudflare:workflows") {
        replacements.push({ start: specifier.start, end: specifier.end, value: localSpecifier });
      }
      i = specifier ? specifier.end + 1 : i + 6;
      continue;
    }
    if (
      source.startsWith("export", i) &&
      !isIdentifierChar(source[i - 1]) &&
      !isIdentifierChar(source[i + 6]) &&
      isStaticModuleDeclarationBoundary(source, i)
    ) {
      const quoteIndex = staticSpecifierQuoteIndex(source, i, "export");
      const specifier = readQuotedSpecifier(source, quoteIndex);
      if (specifier?.value === "cloudflare:workflows") {
        replacements.push({ start: specifier.start, end: specifier.end, value: localSpecifier });
      }
      i = specifier ? specifier.end + 1 : i + 6;
      continue;
    }
    i += 1;
  }
}

/** @param {string} source @param {string} localSpecifier */
function rewriteCloudflareWorkflowsSpecifiers(source, localSpecifier) {
  if (!source.includes("cloudflare:workflows")) return source;

  /** @type {Replacement[]} */
  const replacements = [];
  collectCloudflareWorkflowsSpecifiers(source, localSpecifier, replacements);
  if (replacements.length === 0) return source;
  const sortedReplacements = replacements.toSorted((a, b) => a.start - b.start);
  let out = "";
  let last = 0;
  for (const replacement of sortedReplacements) {
    if (replacement.start < last) continue;
    out += source.slice(last, replacement.start) + replacement.value;
    last = replacement.end;
  }
  return out + source.slice(last);
}
