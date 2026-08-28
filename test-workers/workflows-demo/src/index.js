import { WorkflowEntrypoint } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";

let cachedWorkflowKv;

export class OrderWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    if (event.payload.sleepMs) {
      await step.sleep("settle", event.payload.sleepMs);
      return await step.do("after-sleep", async () => ({
        slept: true,
        instanceId: event.payload.id ?? null,
        fromEnv: this.env.LABEL,
      }));
    }
    if (event.payload.retry) {
      return await step.do("flaky", {
        retries: {
          limit: 2,
          delayMs: event.payload.retryDelayMs ?? 100,
          backoff: "constant",
        },
      }, async ({ attempt }) => {
        if (attempt === 1) throw new Error("try again");
        return { attempt, fromEnv: this.env.LABEL };
      });
    }
    if (event.payload.nonRetryable) {
      return await step.do("non-retryable", { retries: { limit: 5, delayMs: 100, backoff: "constant" } }, async () => {
        throw new NonRetryableError("fatal validation");
      });
    }
    if (event.payload.waitAfterSleep) {
      await step.sleep("before-wait", event.payload.waitAfterSleep);
      const waitOptions = { type: "approval" };
      if (!event.payload.noWaitTimeout) {
        waitOptions.timeout = event.payload.waitTimeoutMs ? event.payload.waitTimeoutMs + "ms" : "5s";
      }
      const payload = await step.waitForEvent("approval", waitOptions);
      return await step.do("after-event", async () => payload);
    }
    if (event.payload.wait) {
      const waitOptions = { type: "approval" };
      if (!event.payload.noWaitTimeout) {
        waitOptions.timeout = event.payload.waitTimeoutMs ? event.payload.waitTimeoutMs + "ms" : "5s";
      }
      const payload = await step.waitForEvent("approval", waitOptions);
      return await step.do("after-event", async () => payload);
    }
    if (event.payload.waitVersion) {
      const waitOptions = { type: "approval" };
      if (!event.payload.noWaitTimeout) {
        waitOptions.timeout = event.payload.waitTimeoutMs ? event.payload.waitTimeoutMs + "ms" : "5s";
      }
      const payload = await step.waitForEvent("approval", waitOptions);
      return await step.do("after-event-version", async () => ({
        message: payload.message,
        sentFromEnv: payload.fromEnv,
        runFromEnv: this.env.LABEL,
      }));
    }
    if (event.payload.manySteps) {
      const values = [];
      for (let i = 0; i < 3; i++) {
        values.push(await step.do(`many-${i}`, async () => ({ i, fromEnv: this.env.LABEL })));
      }
      return values;
    }
    if (event.payload.parallelSteps) {
      const root = await step.do("dag-root", async () => ({
        name: "root",
        fromEnv: this.env.LABEL,
      }));
      const [a, b, c] = await Promise.all([
        step.do("parallel-a", async () => ({ name: "a", root: root.name, fromEnv: this.env.LABEL })),
        step.do("parallel-b", async () => ({ name: "b", root: root.name, fromEnv: this.env.LABEL })),
        step.do("parallel-c", async () => ({ name: "c", root: root.name, fromEnv: this.env.LABEL })),
      ]);
      const [ab, bc] = await Promise.all([
        step.do("join-ab", async () => ({
          names: [a.name, b.name],
          fromEnv: this.env.LABEL,
        })),
        step.do("join-bc", async () => ({
          names: [b.name, c.name],
          fromEnv: this.env.LABEL,
        })),
      ]);
      const joined = await step.do("final-join", async () => ({
        names: [ab.names.join("+"), bc.names.join("+")],
        fromEnv: this.env.LABEL,
      }));
      return { root, parallel: [a, b, c], joins: [ab, bc], joined };
    }
    if (event.payload.dynamicStepName) {
      return await step.do(event.payload.dynamicStepName, async () => ({
        name: event.payload.dynamicStepName,
        fromEnv: this.env.LABEL,
      }));
    }
    if (event.payload.largeStepResult) {
      return await step.do("large-result", async () => ({
        blob: "x".repeat(1024 * 1024),
      }));
    }
    if (event.payload.kvKey) {
      cachedWorkflowKv ??= this.env.CACHE;
      return await step.do("kv-round-trip", async () => {
        await cachedWorkflowKv.put(event.payload.kvKey, event.payload.id);
        return await cachedWorkflowKv.get(event.payload.kvKey);
      });
    }
    return await step.do("record", async () => {
      if (event.payload.runDelayMs) {
        await new Promise((resolve) => setTimeout(resolve, event.payload.runDelayMs));
      }
      if (event.payload.fail) throw new Error("workflow boom");
      return {
        instanceId: event.payload.id ?? null,
        fromEnv: this.env.LABEL,
        nonce: crypto.randomUUID(),
      };
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.endsWith("/cache-kv")) {
      cachedWorkflowKv ??= env.CACHE;
      await cachedWorkflowKv.put("ordinary-cache", "ready");
      return Response.json({ value: await cachedWorkflowKv.get("ordinary-cache") });
    }
    if (url.pathname.endsWith("/create")) {
      const id = url.searchParams.get("id") || "order-1";
      const retentionMs = Number(url.searchParams.get("retentionMs") ?? 0);
      const instance = await env.ORDERS.create({
        id,
        retention: retentionMs > 0 ? {
          successRetention: retentionMs,
          errorRetention: retentionMs,
        } : undefined,
        params: {
          source: "integration",
          id,
          fail: url.searchParams.get("fail") === "1",
          sleepMs: Number(url.searchParams.get("sleepMs") ?? 0),
          retry: url.searchParams.get("retry") === "1",
          retryDelayMs: url.searchParams.has("retryDelayMs")
            ? Number(url.searchParams.get("retryDelayMs"))
            : undefined,
          nonRetryable: url.searchParams.get("nonRetryable") === "1",
          wait: url.searchParams.get("wait") === "1",
          waitVersion: url.searchParams.get("waitVersion") === "1",
          waitAfterSleep: Number(url.searchParams.get("waitAfterSleep") ?? 0),
          waitTimeoutMs: Number(url.searchParams.get("waitTimeoutMs") ?? 0),
          noWaitTimeout: url.searchParams.get("noWaitTimeout") === "1",
          manySteps: url.searchParams.get("manySteps") === "1",
          parallelSteps: url.searchParams.get("parallelSteps") === "1",
          dynamicStepName: url.searchParams.get("dynamicStepName") || "",
          largeStepResult: url.searchParams.get("largeStepResult") === "1",
          kvKey: url.searchParams.get("kvKey") || "",
          runDelayMs: Number(url.searchParams.get("runDelayMs") ?? 0),
        },
      });
      return Response.json({
        id: instance.id,
        status: await instance.status(),
      });
    }
    if (url.pathname.endsWith("/get")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      return Response.json(await instance.status());
    }
    if (url.pathname.endsWith("/steps")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      const rawLimit = url.searchParams.get("limit");
      return Response.json(await instance.status({
        includeSteps: true,
        stepLimit: rawLimit ? Number(rawLimit) : undefined,
      }));
    }
    if (url.pathname.endsWith("/event")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      await instance.sendEvent({
        type: url.searchParams.get("type") || "approval",
        payload: {
          message: url.searchParams.get("message") || "approved",
          fromEnv: env.LABEL,
        },
      });
      return Response.json(await instance.status({ includeSteps: true }));
    }
    if (url.pathname.endsWith("/terminate")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      await instance.terminate();
      return Response.json(await instance.status({ includeSteps: true }));
    }
    if (url.pathname.endsWith("/pause")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      await instance.pause();
      return Response.json(await instance.status({ includeSteps: true }));
    }
    if (url.pathname.endsWith("/resume")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      await instance.resume();
      return Response.json(await instance.status({ includeSteps: true }));
    }
    if (url.pathname.endsWith("/restart")) {
      const id = url.searchParams.get("id") || "order-1";
      const instance = await env.ORDERS.get(id);
      await instance.restart();
      return Response.json(await instance.status({ includeSteps: true }));
    }
    if (url.pathname.endsWith("/batch")) {
      const instances = await env.ORDERS.createBatch([
        { id: "batch-a", params: { n: 1 } },
        { id: "batch-a", params: { n: 2 } },
        { id: "batch-b", params: { n: 3 } },
      ]);
      return Response.json({
        ids: instances.map((instance) => instance.id),
      });
    }
    return new Response("ok");
  },
};
