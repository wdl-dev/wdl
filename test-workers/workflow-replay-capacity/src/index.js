import { WorkflowEntrypoint } from "cloudflare:workers";

const callbacks = new Map();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class ReplayCapacityWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const id = event.payload.id;
    for (let index = 0; index < 6; index += 1) {
      await step.do(`payload-${index}`, async () => {
        callbacks.set(id, (callbacks.get(id) ?? 0) + 1);
        return "x".repeat(768 * 1024);
      });
    }
    while (await this.env.STATE.get("release") !== "1") await delay(50);
    return { id, callbacks: callbacks.get(id) ?? 0 };
  }
}

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname.endsWith("/release")) {
      await env.STATE.put("release", "1");
      return Response.json({ released: true });
    }
    return Response.json({ callbacks: Object.fromEntries(callbacks) });
  },
};
