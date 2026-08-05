// Models WDL's do-runtime host actor (do-runtime/actor.js): it owns the real
// workerLoader binding, loads the tenant bundle, pulls the tenant DO class off
// the stub, and mounts it as a level-1 facet.
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

import TENANT_SRC from "tenant-src";
import GADGET_V1 from "gadget-v1-src";
import GADGET_V2 from "gadget-v2-src";

const TENANT_COMPAT_DATE = "2026-04-24";
const GADGET_COMPAT_DATE = "2026-04-24";

const GADGET_SOURCES = { 1: GADGET_V1, 2: GADGET_V2 };

/**
 * The bridge workerd's DataCloneError prescribes: "have the parent Worker
 * expose an entrypoint which constructs the dynamic worker and forwards to
 * it." Tenant code holds this stub and never touches a loader or a gadget
 * class. Identity comes from host-authored props, not from arguments.
 */
export class GadgetBridge extends WorkerEntrypoint {
  // Platform identity only. `LOADER.get(key, cb)` memoizes by key, so the
  // callback that builds this stub's props runs once per *worker version* --
  // anything per-session baked in here would freeze to whichever session
  // loaded first. ns/worker are per-load and therefore safe; the session is a
  // tenant-level concept and rides in the tenant-supplied gadget name.
  #scope() {
    const { ns, worker } = this.ctx.props;
    return `${ns}/${worker}`;
  }

  #host() {
    // Gadget facets live in their own host actor, not the session's. Re-entering
    // the session actor while it is blocked on the tenant facet would deadlock.
    const id = this.env.GADGET_HOSTS.idFromName(`gadget-shard:${this.#scope()}`);
    return this.env.GADGET_HOSTS.get(id);
  }

  #url(path, gadgetId, codeVersion, extra = "") {
    const params = new URLSearchParams({
      scope: this.#scope(),
      gadget: String(gadgetId),
      gv: String(codeVersion),
      path,
    });
    return `http://gadget-host/dispatch${extra}?${params}`;
  }

  async call(gadgetId, codeVersion, path, cpuMs) {
    const base = this.#url(path, gadgetId, codeVersion);
    const res = await this.#host().fetch(
      cpuMs ? `${base}&cpu=${cpuMs}` : base
    );
    return { status: res.status, body: await res.json() };
  }

  async swap(gadgetId, codeVersion, path) {
    const res = await this.#host().fetch(
      this.#url(path, gadgetId, codeVersion, "-swap")
    );
    return { status: res.status, body: await res.json() };
  }

  async probeKey(gadgetId) {
    return {
      requested: gadgetId,
      actualLoaderKey: `gadget/${this.#scope()}/${gadgetId}/v1`,
      actualFacetName: `gadget:${this.#scope()}:${gadgetId}`,
    };
  }
}

/** Owns gadget dynamic workers and their facets. Models a WDL gadget-runtime. */
export class GadgetHostActor extends DurableObject {
  #gadgetClass(scope, gadgetId, codeVersion, cpuMs = 5_000) {
    const source = GADGET_SOURCES[codeVersion];
    if (!source) throw new Error(`unknown gadget code version ${codeVersion}`);
    const stub = this.env.LOADER.get(
      `gadget/${scope}/${gadgetId}/v${codeVersion}/cpu${cpuMs}`,
      () => ({
        compatibilityDate: GADGET_COMPAT_DATE,
        mainModule: "server.js",
        modules: { "server.js": source },
        // Total network isolation: stricter than user-runtime's public-only.
        globalOutbound: null,
        limits: { cpuMs, subRequests: 10 },
      })
    );
    return stub.getDurableObjectClass("Gadget", {
      props: { scope, gadgetId, codeVersion },
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const scope = url.searchParams.get("scope");
    const gadgetId = url.searchParams.get("gadget");
    const codeVersion = Number(url.searchParams.get("gv"));
    const path = url.searchParams.get("path");
    const facetName = `gadget:${scope}:${gadgetId}`;

    if (url.pathname === "/dispatch-swap") {
      this.ctx.facets.abort(facetName, new Error("gadget code version changed"));
    }

    const cpuMs = Number(url.searchParams.get("cpu") ?? "5000");
    const facet = this.ctx.facets.get(facetName, () => ({
      class: this.#gadgetClass(scope, gadgetId, codeVersion, cpuMs),
      id: gadgetId,
    }));
    try {
      return await facet.fetch(new Request(`http://gadget${path}`));
    } catch (err) {
      return Response.json({ gadgetFailed: true, error: String(err) });
    }
  }
}

export class WdlDoHostActor extends DurableObject {
  #tenantStub(sessionId, { includeRawLoader = false } = {}) {
    const key = includeRawLoader
      ? "tenant:chat-worker:v1-rawloader"
      : "tenant:chat-worker:v1";
    return this.env.LOADER.get(key, () => {
      const env = {
        GADGET_BRIDGE: this.ctx.exports.GadgetBridge({
          props: { ns: "tmp-demo", worker: "chat-worker" },
        }),
      };
      if (includeRawLoader) env.RAW_LOADER = this.env.LOADER;
      return {
        compatibilityDate: TENANT_COMPAT_DATE,
        mainModule: "worker.js",
        modules: { "worker.js": TENANT_SRC },
        env,
        globalOutbound: this.env.PUBLIC_NETWORK,
      };
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const sessionId = url.searchParams.get("session") ?? "sess-1";
    const facetName = `ChatSessionDO:${sessionId}`;

    // T-A: can a native WorkerLoader binding cross the workerLoader env
    // boundary at all? If it could, tenant code could address host keys.
    if (url.pathname === "/host/raw-loader-env") {
      try {
        const stub = this.#tenantStub(sessionId, { includeRawLoader: true });
        const res = await stub.getEntrypoint().fetch("http://x/");
        return Response.json({ cloned: true, body: await res.text() });
      } catch (err) {
        return Response.json({ cloned: false, error: String(err) });
      }
    }

    // T-B: can a DO class taken off a dynamically-loaded worker be handed to
    // another dynamically-loaded worker over JSRPC?
    if (url.pathname === "/host/transfer-class") {
      try {
        const cls = this.env.LOADER.get("probe-gadget", () => ({
          compatibilityDate: GADGET_COMPAT_DATE,
          mainModule: "server.js",
          modules: { "server.js": GADGET_SOURCES[1] },
          globalOutbound: null,
        })).getDurableObjectClass("Gadget");
        const sink = this.ctx.exports.ClassSink({ props: {} });
        return Response.json({ transferred: await sink.accept(cls) });
      } catch (err) {
        return Response.json({ transferred: false, error: String(err) });
      }
    }

    if (url.pathname === "/host/drop-session") {
      const hard = url.searchParams.get("hard") === "1";
      if (hard) this.ctx.facets.delete(facetName);
      else this.ctx.facets.abort(facetName, new Error("session closed"));
      return Response.json({ facet: facetName, mode: hard ? "delete" : "abort" });
    }

    const cls = this.#tenantStub(sessionId).getDurableObjectClass("ChatSessionDO", {
      props: { sessionId },
    });
    const facet = this.ctx.facets.get(facetName, () => ({ class: cls, id: sessionId }));
    return await facet.fetch(
      new Request(`http://tenant${url.pathname}${url.search}`)
    );
  }
}

/** Only exists to receive a class over JSRPC in T-B. */
export class ClassSink extends WorkerEntrypoint {
  async accept(cls) {
    return typeof cls;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") return new Response("wdl gadget spike\n");
    const sessionId = url.searchParams.get("session") ?? "sess-1";
    const id = env.DO_HOSTS.idFromName(`shard:${sessionId}`);
    return await env.DO_HOSTS.get(id).fetch(request);
  },
};
