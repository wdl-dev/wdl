// Models wdl-chat's chat-worker. In WDL this is a *dynamically loaded* tenant
// worker, and ChatSessionDO already runs as a level-1 facet inside
// do-runtime's WdlDoHostActor. Everything here therefore happens one level
// deeper than anything WDL does today.
import { DurableObject } from "cloudflare:workers";

// A class defined inside the tenant bundle itself, used to ask whether facets
// nest at all -- independent of the cross-worker class transfer problem.
export class TenantSubFacet extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS n(k TEXT PRIMARY KEY, v INTEGER)");
  }
  async fetch() {
    this.ctx.storage.sql.exec(
      "INSERT INTO n(k,v) VALUES('c',1) ON CONFLICT(k) DO UPDATE SET v=v+1"
    );
    const [row] = [...this.ctx.storage.sql.exec("SELECT v FROM n WHERE k='c'")];
    return Response.json({ subFacet: true, count: row.v, props: this.ctx.props });
  }
}

export class ChatSessionDO extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    // Session identity comes from ctx.props, which IS per-facet (set by the
    // host through getDurableObjectClass), unlike the loader env.
    const sessionId = this.ctx.props?.sessionId ?? "unknown";
    const gadgetId = `${sessionId}/${url.searchParams.get("gadget") ?? "g1"}`;
    const codeVersion = Number(url.searchParams.get("gv") ?? "1");

    try {
      switch (url.pathname) {
        case "/tenant/ping":
          return Response.json({
            ok: true,
            props: this.ctx.props,
            rawLoaderPresent: this.env.RAW_LOADER !== undefined,
            bridgeType: typeof this.env.GADGET_BRIDGE?.call,
            hasFacets: typeof this.ctx.facets?.get === "function",
            hasCtxExports: this.ctx.exports !== undefined,
            ctxExportKeys: this.ctx.exports
              ? Object.keys(this.ctx.exports)
              : null,
          });

        // T-D: can a level-1 facet mount a level-2 facet from a class defined
        // in its *own* bundle? Isolates facet nesting from class transfer.
        case "/tenant/subfacet": {
          const cls = this.ctx.exports?.TenantSubFacet;
          if (!cls) {
            return Response.json({
              nestedFacet: false,
              reason: "dynamically-loaded worker has no ctx.exports",
            });
          }
          const facet = this.ctx.facets.get("sub:a", () => ({
            class: cls,
            id: "sub-a",
          }));
          return await facet.fetch(new Request("http://sub/"));
        }

        // Control: a runaway loop in ordinary tenant code, no gadget involved.
        // This is what any WDL user-runtime worker can already do today.
        case "/tenant/spin-self": {
          let x = 0;
          for (;;) {
            x += Math.sqrt(x + 1);
            if (x === -1) break;
          }
          return Response.json({ neverReached: true, x });
        }

        case "/tenant/cpu": {
          const out = await this.env.GADGET_BRIDGE.call(
            gadgetId,
            codeVersion,
            "/spin",
            Number(url.searchParams.get("cpu") ?? "200")
          );
          return Response.json(out);
        }

        // T-C: the shape workerd's error message prescribes -- the host owns
        // both the load and the facet, the tenant only forwards over RPC.
        case "/tenant/gadget": {
          const out = await this.env.GADGET_BRIDGE.call(
            gadgetId,
            codeVersion,
            "/bump"
          );
          return Response.json(out);
        }

        case "/tenant/egress": {
          const out = await this.env.GADGET_BRIDGE.call(
            gadgetId,
            codeVersion,
            "/egress"
          );
          return Response.json(out);
        }

        case "/tenant/swap": {
          const out = await this.env.GADGET_BRIDGE.swap(
            gadgetId,
            codeVersion,
            "/bump"
          );
          return Response.json(out);
        }

        case "/tenant/isolation": {
          const a = await this.env.GADGET_BRIDGE.call(
            `${sessionId}/iso-a`, codeVersion, "/bump");
          const b = await this.env.GADGET_BRIDGE.call(
            `${sessionId}/iso-b`, codeVersion, "/bump");
          return Response.json({ a, b });
        }

        // T-E: can a tenant-chosen gadget name escape the host's key prefix?
        case "/tenant/steal": {
          const probe = await this.env.GADGET_BRIDGE.probeKey(
            url.searchParams.get("key") ?? "../../host-private"
          );
          return Response.json(probe);
        }

        default:
          return new Response("no tenant route", { status: 404 });
      }
    } catch (err) {
      return Response.json({ error: String(err?.stack ?? err) }, { status: 500 });
    }
  }
}

export default {
  fetch() {
    return new Response("tenant worker default entrypoint");
  },
};
