let upgrades = 0;
let reconnectWaiting = false;
let releaseReconnect = false;
let received = 0;
let closed = 0;

export default {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      if (new URL(request.url).pathname === "/release") releaseReconnect = true;
      return Response.json({ upgrades, reconnectWaiting, received, closed });
    }
    upgrades += 1;
    if (upgrades > 1) {
      reconnectWaiting = true;
      const deadline = Date.now() + 30_000;
      while (!releaseReconnect && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      reconnectWaiting = false;
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.binaryType = "arraybuffer";
    server.accept();
    server.addEventListener("message", (event) => {
      if (event.data === "detach") server.close(1011, "reconnect");
      else received += 1;
    });
    server.addEventListener("close", () => { closed += 1; });
    return new Response(null, { status: 101, webSocket: client });
  },
};
