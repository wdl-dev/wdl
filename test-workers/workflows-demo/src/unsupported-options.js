import { WorkflowEntrypoint } from "cloudflare:workers";

let callbacks = 0;
let rollbacks = 0;
let optionReads = 0;
let lifecycleReads = 0;
let caught = 0;

export class OptionsWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    if (event.payload.mode === "wait") await step.sleep("hold", "1h");
    const mode = event.payload.mode;
    if (mode.startsWith("rollback")) {
      const callback = async () => { callbacks += 1; return "must-not-run"; };
      const readOption = () => {
        optionReads += 1;
        throw new Error("rollback options must not be inspected");
      };
      let options = { rollback: async () => { rollbacks += 1; } };
      if (mode.includes("getter")) options = { get rollback() { return readOption(); } };
      if (mode.includes("proxy")) options = new Proxy(options, { get: readOption, ownKeys: readOption });
      if (mode.includes("symbol")) options = { ...options, unsupported: Symbol("rollback") };
      try {
        return mode.endsWith("config")
          ? await step.do("unsupported", { timeout: "1s" }, callback, options)
          : await step.do("unsupported", callback, options);
      } catch {
        caught += 1;
        try { await step.do("after-rejection", callback); } catch {}
        return { fallback: true };
      }
    }
    return { nonce: crypto.randomUUID() };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/calls") return Response.json({ callbacks, rollbacks, optionReads, lifecycleReads, caught });
    const id = url.searchParams.get("id");
    if (url.pathname === "/create") {
      const instance = await env.FLOW.create({ id, params: { mode: url.searchParams.get("mode") } });
      return Response.json({ id: instance.id });
    }
    const instance = await env.FLOW.get(id);
    try {
      if (url.pathname === "/terminate-rollback" || url.pathname === "/restart-from") {
        const terminate = url.pathname === "/terminate-rollback";
        const field = terminate ? "rollback" : "from";
        let options = { [field]: terminate ? true : { name: "hold" } };
        if (url.searchParams.get("shape") === "inherited") options = Object.create(options);
        if (url.searchParams.get("shape") === "getter") options = new class {
          get [field]() { lifecycleReads += 1; throw new Error("must not read lifecycle options"); }
        }();
        if (terminate) await instance.terminate(options);
        else await instance.restart(options);
      }
      return Response.json(await instance.status({ includeSteps: true }));
    } catch (error) {
      return Response.json({ name: error.name, message: error.message }, { status: 400 });
    }
  },
};
