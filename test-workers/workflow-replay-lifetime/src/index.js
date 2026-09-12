import { createStepController } from "./workflow-step.js";
import { prepareWorkflowReplayCacheMetrics } from "./workflow-replay-cache.js";
import { gauges } from "./metrics.js";

let invocations = 0;
let cancellations = 0;

function state() {
  prepareWorkflowReplayCacheMetrics();
  return { invocations, cancellations, bytes: gauges.get("workflow_replay_read_in_flight_bytes") };
}

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname.endsWith("/state")) return Response.json(state());
    invocations += 1;
    const started = Promise.withResolvers();
    const controller = createStepController({
      ns: "probe", worker: "probe", frozenVersion: "v1", workflowName: "probe",
      workflowKey: "wf_probe", className: "Probe", instanceId: String(invocations),
      generation: 1, runToken: "probe", createdAtMs: 1, dispatchDeadlineMs: Date.now() + 2_000,
    }, {
      async fetch() {
        return new Response(new ReadableStream({
          pull() { started.resolve(undefined); },
          cancel() { cancellations += 1; },
        }, { highWaterMark: 0 }), { headers: { "content-length": String(1024 * 1024) } });
      },
    });
    let before;
    let rootError;
    try {
      await env.ROOT.run(controller.facade, () => started.promise);
    } catch (error) {
      rootError = error.message;
      before = state();
    } finally {
      controller.closeForRunReturn();
    }
    return Response.json({ rootError, before, after: state() }, { status: 503 });
  },
};
