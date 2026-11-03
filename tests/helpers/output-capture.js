import { installMockProperty } from "./mock-global.js";

/**
 * @typedef {{ stdout: string[], stderr: string[] }} CapturedConsole
 */

/**
 * Captures console.log and console.error for synchronous assertions.
 *
 * @returns {CapturedConsole & { restore: () => void }}
 */
function installCapturedConsole() {
  /** @type {string[]} */
  const stdout = [];
  /** @type {string[]} */
  const stderr = [];
  const restoreLog = installConsoleMethodCapture("log", stdout);
  const restoreError = installConsoleMethodCapture("error", stderr);
  return {
    stdout,
    stderr,
    restore() {
      restoreError();
      restoreLog();
    },
  };
}

/**
 * Captures console.log and console.error for one synchronous callback scope.
 *
 * @template T
 * @param {(captured: CapturedConsole) => T} callback
 * @returns {T}
 */
export function withCapturedConsole(callback) {
  const captured = installCapturedConsole();
  try {
    return callback(captured);
  } finally {
    captured.restore();
  }
}

/**
 * Captures a single console method into the supplied array.
 *
 * @template T
 * @param {"log" | "error" | "warn" | "info"} method
 * @param {T[]} lines
 * @param {(...args: unknown[]) => T} [format]
 * @returns {() => void}
 */
export function installConsoleMethodCapture(method, lines, format = (...args) => /** @type {T} */ (args.join(" "))) {
  return installMockProperty(console, method, (/** @type {unknown[]} */ ...args) => {
    lines.push(format(...args));
  });
}

/**
 * Captures writes to a Node stream while preserving callback completion.
 *
 * @param {{ write: (...args: any[]) => boolean }} stream
 * @param {string[]} writes
 * @returns {() => void}
 */
export function installStreamWriteCapture(stream, writes) {
  return installMockProperty(stream, "write", ((chunk, ...args) => {
    writes.push(String(chunk));
    const callback = args.find((arg) => typeof arg === "function");
    if (callback) callback();
    return true;
  }));
}
