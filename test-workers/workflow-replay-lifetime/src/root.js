import { WorkerEntrypoint } from "cloudflare:workers";

export default class extends WorkerEntrypoint {
  async run(step, started) {
    step.do("pending", async () => "unexpected callback").catch(() => {});
    await started();
    throw new Error("root escaped");
  }
  async fetch() { return Response.json({ ready: true }); }
}
