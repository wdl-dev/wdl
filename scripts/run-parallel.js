import { spawn } from "node:child_process";
import { pipeWithPrefix } from "./_stream-prefix.js";

const tasks = process.argv.slice(2);
if (tasks.length === 0) {
  process.stderr.write("usage: run-parallel.js <npm-script>...\n");
  process.exit(2);
}

const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

const procs = tasks.map((task) => {
  const child = spawn(npmCmd, ["run", "-s", task], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const prefix = `[${task}] `;
  pipeWithPrefix(child.stdout, process.stdout, prefix);
  pipeWithPrefix(child.stderr, process.stderr, prefix);
  return new Promise((resolve) => child.on("close", (code) => resolve(code ?? 1)));
});

const codes = await Promise.all(procs);
const failure = codes.find((c) => c !== 0);
process.exit(failure ?? 0);
