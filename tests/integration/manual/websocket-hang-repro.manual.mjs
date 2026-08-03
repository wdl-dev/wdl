// Local/manual direct gateway websocket lifecycle regression. Stops the
// backend longer than the reconnect budget, then closes the client. The
// gateway should release the session without a "had hung" failure.
//
// Renamed off `.test.js` so the default integration runner does not
// pick it up — this test stops `user-runtime`, which would break every
// later test sharing the same compose project. Run explicitly:
//   node --test tests/integration/manual/websocket-hang-repro.manual.mjs

import { test, before } from "node:test";
import assert from "node:assert/strict";

import {
  composeStart,
  composeStop,
  composeUpNoBuildArgs,
  deployAndPromote,
  ensureStackUp,
  encodeClientTextFrame,
  readOneServerTextFrame,
  sh,
  uniqueNs,
  wsHandshake,
} from "../helpers/index.js";

before(async () => {
  await ensureStackUp();
});

test(
  "direct gateway proxy releases a websocket after reconnect budget exhaustion",
  { timeout: 6 * 60_000 },
  async () => {
    const logSince = new Date().toISOString();
    const ns = uniqueNs("ws-hang-repro");
    const code = `
      export default {
        async fetch(request) {
          if (request.headers.get("Upgrade") !== "websocket") {
            return new Response("need upgrade", { status: 426 });
          }
          const pair = new WebSocketPair();
          const client = pair[0];
          const server = pair[1];
          server.accept();
          server.addEventListener("message", (evt) => {
            server.send("echo:" + evt.data);
          });
          return new Response(null, { status: 101, webSocket: client });
        },
      };
    `;
    await deployAndPromote(ns, "echo", { code });

    const { status, headers, socket } = await wsHandshake(ns, "/echo");
    assert.equal(status, 101);
    const requestId = headers["x-request-id"];
    assert.ok(requestId, "x-request-id should land on the 101 response");

    let backendStopped = false;

    try {
      socket.write(encodeClientTextFrame("warmup"));
      assert.equal(await readOneServerTextFrame(socket), "echo:warmup");

      composeStop("user-runtime");
      backendStopped = true;

      // Wait through the reconnect budget (~9s) plus a small grace.
      await new Promise((resolve) => setTimeout(resolve, 15_000));

      // Drop the remaining PendingEvent (couple()'s eyeball pump) so
      // the IoContext can reach the abort condition.
      socket.destroy();

      await new Promise((resolve) => setTimeout(resolve, 60_000));
    } finally {
      try { socket.destroy(); } catch {}
      if (backendStopped) {
        try {
          composeStart("user-runtime");
          sh([
            "docker",
            "compose",
            "up",
            "-d",
            ...composeUpNoBuildArgs(),
            "--wait",
            "user-runtime",
          ]);
        } catch (err) {
          console.error("failed to restart user-runtime:", err);
        }
      }
    }

    // workerd's hang error is from server.c++ stderr, not our structured
    // logger, so it may not carry our request id — search both.
    const gatewayLogs = sh(
      ["docker", "compose", "logs", "--no-color", `--since=${logSince}`, "gateway"],
      { stdio: "pipe" }
    );
    const idLogs = gatewayLogs
      .split("\n")
      .filter((line) => line.includes(requestId))
      .join("\n");
    const hangPattern = /had hung|abortFromHang|worker.*hung/i;
    const hangLogs = gatewayLogs
      .split("\n")
      .filter((line) => hangPattern.test(line))
      .join("\n");
    const hung = hangPattern.test(idLogs) || hangLogs.trim().length > 0;
    const reconnectFailed = /websocket_reconnect_failed/.test(idLogs);

    console.log("repro result", { requestId, reconnectFailed, hung });
    console.log("---structured log for this request---");
    console.log(idLogs);
    if (hangLogs.trim().length > 0) {
      console.log("---hang-related lines (any request)---");
      console.log(hangLogs);
    }

    assert.ok(reconnectFailed, "gateway should log websocket_reconnect_failed");
    assert.equal(hung, false, `gateway hang detector fired:\n${hangLogs || idLogs}`);
    console.log("no hang detector fire on this path");
  }
);
