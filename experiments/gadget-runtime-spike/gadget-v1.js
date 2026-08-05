// "AI-generated" gadget code, v1. Loaded as a nested Dynamic Worker by the
// tenant worker, mounted as a sub-facet. Models what wdl-chat's LLM writes
// today into the MicroVM's /workspace.
import { DurableObject } from "cloudflare:workers";

export class Gadget extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS counter(k TEXT PRIMARY KEY, n INTEGER)"
    );
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/bump") {
      this.ctx.storage.sql.exec(
        "INSERT INTO counter(k, n) VALUES('c', 1) " +
          "ON CONFLICT(k) DO UPDATE SET n = n + 1"
      );
      const [row] = [
        ...this.ctx.storage.sql.exec("SELECT n FROM counter WHERE k='c'"),
      ];
      return Response.json({ version: 1, count: row.n, props: this.ctx.props });
    }

    if (url.pathname === "/spin") {
      // Burn CPU forever. `limits.cpuMs` on the load must kill this.
      const started = Date.now();
      let x = 0;
      for (;;) {
        x += Math.sqrt(x + 1);
        if (x === -1) break;
      }
      return Response.json({ neverReached: true, x, ms: Date.now() - started });
    }

    if (url.pathname === "/egress") {
      // globalOutbound: null must make this fail. If it succeeds, the gadget
      // sandbox leaks the tenant's network.
      try {
        const res = await fetch("https://example.com/");
        return Response.json({ escaped: true, status: res.status });
      } catch (err) {
        return Response.json({ escaped: false, error: String(err) });
      }
    }

    return new Response("gadget v1", { status: 404 });
  }
}

export default {
  fetch() {
    return new Response("gadget default entrypoint v1");
  },
};
