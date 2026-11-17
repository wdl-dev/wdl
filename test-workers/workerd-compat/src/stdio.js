let retained;
let calls = 0;

export default {
  async fetch() {
    // Keep imports inside the request so the first stdio access is genuinely late.
    const stdout = process.stdout;
    const stderr = process.stderr;
    const reused = retained === undefined ||
      (retained.stdout === stdout && retained.stderr === stderr);
    retained = { stdout, stderr };
    await new Promise((resolve, reject) => stdout.write("stdio-probe\n", (error) => error ? reject(error) : resolve()));
    await new Promise((resolve, reject) => stderr.write("stdio-probe\n", (error) => error ? reject(error) : resolve()));
    const imported = await import("node:process");
    const { createRequire } = await import("node:module");
    const { default: importedConsole, Console } = await import("node:console");
    const { Writable } = await import("node:stream");
    const require = createRequire("/worker.js");
    const chunks = [];
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(chunk.toString());
        callback();
      },
    });
    new Console({ stdout: destination }).log("custom-output");
    return Response.json({
      calls: ++calls,
      reused,
      moduleIdentity: imported.default === process && require("process") === process,
      streamIdentity: imported.stdout === stdout && imported.stderr === stderr,
      consoleIdentity: importedConsole === console,
      customOutput: chunks.join(""),
    });
  },
};
