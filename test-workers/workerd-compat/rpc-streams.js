import { deepStrictEqual, strictEqual } from "node:assert";

export const httpBodyPipeAcrossRpc = {
  async test(_controller, env) {
    for (const preventClose of [false, true]) {
      // TCP makes this a native system stream; an in-memory Response body does
      // not exercise the same pump finalization path.
      const response = await env.SOURCE.fetch("http://source.test/_healthz");
      strictEqual(response.status, 200);
      const sink = await env.SINK.open();
      await response.body.pipeTo(sink, { preventClose });
      strictEqual((await env.SINK.state()).closes, preventClose ? 0 : 1);
      if (preventClose) await sink.close();
      const state = await env.SINK.state();
      strictEqual(state.closes, 1);
      deepStrictEqual(JSON.parse(state.text), { ok: true, service: "user-runtime" });
    }
  },
};
