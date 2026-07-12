/** @param {NodeJS.WritableStream} dst @param {string} prefix @param {string} text */
export function writeWithPrefix(dst, prefix, text) {
  dst.write(`${prefix}${text}`);
}

/** @param {NodeJS.ReadableStream} src @param {NodeJS.WritableStream} dst @param {string} prefix */
export function pipeWithPrefix(src, dst, prefix) {
  let buffer = "";
  src.setEncoding("utf8");
  src.on("data", (chunk) => {
    buffer += String(chunk);
    let newline;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      writeWithPrefix(dst, prefix, `${buffer.slice(0, newline)}\n`);
      buffer = buffer.slice(newline + 1);
    }
  });
  src.on("end", () => {
    if (buffer.length) writeWithPrefix(dst, prefix, `${buffer}\n`);
  });
}
