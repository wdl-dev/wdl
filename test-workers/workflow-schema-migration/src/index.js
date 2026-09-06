import { WorkflowEntrypoint } from "cloudflare:workers";

export class MigrationWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const { id, mode } = event.payload;
    const effect = await step.do("effect", async () => {
      const count = Number(await this.env.CACHE.get(id) ?? 0) + 1;
      await this.env.CACHE.put(id, String(count));
      return { count };
    });
    if (mode === "wait") {
      const approved = await step.waitForEvent("approval", { type: "approved" });
      return { effect, approved };
    }
    if (mode === "sleep") await step.sleep("pause", 3_600_000);
    if (mode === "retry") {
      await step.do("retry", { retries: { limit: 2, delayMs: 3_600_000, backoff: "constant" } }, async ({ attempt }) => {
        if (attempt === 1) throw new Error("retry after migration");
      });
    }
    return effect;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "migration";
    const action = url.pathname.split("/").at(-1);
    if (action === "create") {
      await env.FLOW.create({ id, params: { id, mode: url.searchParams.get("mode") } });
      return Response.json({ id });
    }
    if (action === "effect") return Response.json({ count: Number(await env.CACHE.get(id) ?? 0) });
    const instance = await env.FLOW.get(id);
    if (action === "event") await instance.sendEvent({ type: "approved", payload: { approved: true } });
    if (action === "pause") await instance.pause();
    if (action === "resume") await instance.resume();
    return Response.json(await instance.status({ includeSteps: true }));
  },
};
