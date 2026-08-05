import assert from "node:assert/strict";
import { test } from "node:test";
import {
  createControlHandlerState,
  importControlHandler,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import {
  moduleDataUrl,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const GLOBAL_NAME = "__controlPromoteHandlerState";
const routingUrl = moduleDataUrl(`
export class RoutingError extends Error {}
export async function promoteWithRoutes(redis, ns, name, version, options) {
  const state = globalThis.${GLOBAL_NAME};
  state.promoteCalls.push({
    sameRedis: redis === state.redis,
    ns,
    name,
    version,
    requestId: options.requestId,
    hasLog: typeof options.log === "function",
  });
  return state.promoteResult;
}
`);

const { handle } = await importControlHandler("control/handlers/promote.js", {
  globalName: GLOBAL_NAME,
  replacements: {
    "control-routing": routingUrl,
    "shared-worker-contract": repositoryFileUrl("shared/worker-contract.js"),
    "shared-ns-pattern": repositoryFileUrl("shared/ns-pattern.js"),
  },
});

test("promote response exposes the committed session policy state", async () => {
  const state = installControlHandlerState(GLOBAL_NAME, {
    ...createControlHandlerState(),
    promoteCalls: [],
    promoteResult: {
      version: "v3",
      affectedHosts: ["api.example"],
      workersDev: true,
      routeUrls: ["https://api.example/v1/*"],
      sessionPolicy: "restart",
      restartSequence: 8,
    },
  });

  const response = await handle({
    request: new Request("https://admin.example/ns/demo/worker/worker/promote", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ version: "v3" }),
    }),
    env: { PLATFORM_DOMAIN: " Edge.WDL.EXAMPLE. " },
    ns: "demo",
    name: "worker",
    requestId: "rid-promote",
  });

  await assertJsonResponse(response, 200, {
    namespace: "demo",
    name: "worker",
    version: "v3",
    active: true,
    affectedHosts: ["api.example"],
    platformDomain: "edge.wdl.example",
    workersDev: true,
    sessionPolicy: "restart",
    restartSequence: 8,
    urls: {
      platform: "https://demo.edge.wdl.example/worker/",
      routes: ["https://api.example/v1/*"],
    },
  });
  assert.deepEqual(state.promoteCalls, [{
    sameRedis: true,
    ns: "demo",
    name: "worker",
    version: "v3",
    requestId: "rid-promote",
    hasLog: true,
  }]);
});
