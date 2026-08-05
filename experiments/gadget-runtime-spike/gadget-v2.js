// Same gadget after the agent edited it. Used to prove a code-version swap
// re-runs new code against the *same* facet SQLite storage -- the wdl-chat
// "AI iterates on the app" loop without a redeploy.
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
      // v2 counts by 10 instead of 1, so the response proves which code ran.
      this.ctx.storage.sql.exec(
        "INSERT INTO counter(k, n) VALUES('c', 10) " +
          "ON CONFLICT(k) DO UPDATE SET n = n + 10"
      );
      const [row] = [
        ...this.ctx.storage.sql.exec("SELECT n FROM counter WHERE k='c'"),
      ];
      return Response.json({ version: 2, count: row.n, props: this.ctx.props });
    }

    return new Response("gadget v2", { status: 404 });
  }
}

export default {
  fetch() {
    return new Response("gadget default entrypoint v2");
  },
};
