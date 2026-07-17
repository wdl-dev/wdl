import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createControlHandlerState,
  controlSharedHarnessUrl,
  installControlHandlerState,
} from "../helpers/control-handler-harness.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { compileControlGraph } from "../helpers/load-control-lib.js";
import { createFakeRedis } from "../helpers/mocks/fake-redis.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const WORKERS_HANDLER_STATE_GLOBAL = "__workersHandlerState";
const workersHandlerState = installControlHandlerState(
  WORKERS_HANDLER_STATE_GLOBAL,
  createControlHandlerState({
    redis: createFakeRedis(),
    logs: [],
  })
);
const controlSharedUrl = controlSharedHarnessUrl(WORKERS_HANDLER_STATE_GLOBAL);
const { libUrl: controlLibUrl, workerContractUrl } = await compileControlGraph();

const sharedSecretKeysUrl = repositoryFileUrl("shared/secret-keys.js");

const src = applyModuleReplacements(readRepositoryFile("control/handlers/workers.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
  [/from "control-lib";/, `from ${JSON.stringify(controlLibUrl)};`],
  [/from "shared-secret-keys";/, `from ${JSON.stringify(sharedSecretKeysUrl)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(workerContractUrl)};`],
]);

const { handle } = await import(moduleDataUrl(src));

function resetWorkersHandlerState() {
  const redis = createFakeRedis();
  redis.sets.set("workers:demo", new Set(["gamma", "beta", "alpha"]));
  redis.hashes.set("routes:demo", { alpha: "v2" });
  redis.zsets.set("worker-versions:demo:alpha", new Map([
    ["v1", 1],
    ["v2", 2],
  ]));
  redis.zsets.set("worker-versions:demo:beta", new Map([["v1", 1]]));
  redis.hashes.set("secrets:demo:beta", { TOKEN: "WDL-ENC:test" });
  redis.hashes.set("wf:defs:demo:gamma", { flow: "{}" });
  let sessions = 0;
  const session = redis.session.bind(redis);
  redis.session = async (fn) => {
    sessions += 1;
    return await session(fn);
  };
  workersHandlerState.redis = redis;
  workersHandlerState.logs.length = 0;
  return { redis, logs: workersHandlerState.logs, sessions: () => sessions };
}

test("workers handler lists namespace state through one Redis session", async () => {
  const state = resetWorkersHandlerState();

  const response = await handle({ method: "GET", nsName: "demo", requestId: "rid-workers" });

  assert.equal(state.sessions(), 1);
  await assertJsonResponse(response, 200, {
    namespace: "demo",
    workers: [
      {
        name: "alpha",
        activeVersion: "v2",
        versions: ["v1", "v2"],
        versionCount: 2,
        hasSecrets: false,
        hasWorkflowDefs: false,
      },
      {
        name: "beta",
        activeVersion: null,
        versions: ["v1"],
        versionCount: 1,
        hasSecrets: true,
        hasWorkflowDefs: false,
      },
      {
        name: "gamma",
        activeVersion: null,
        versions: [],
        versionCount: 0,
        hasSecrets: false,
        hasWorkflowDefs: true,
      },
    ],
  });
  assert.deepEqual(state.redis.commands, [
    ["sMembers", "workers:demo"],
    ["hGetAll", "routes:demo"],
    ["zRangeMany", [
      "worker-versions:demo:alpha",
      "worker-versions:demo:beta",
      "worker-versions:demo:gamma",
    ], 0, -1],
    ["existsMany", [
      "secrets:demo:alpha",
      "secrets:demo:beta",
      "secrets:demo:gamma",
      "wf:defs:demo:alpha",
      "wf:defs:demo:beta",
      "wf:defs:demo:gamma",
    ]],
  ]);
  assert.deepEqual(state.logs, [{
    level: "info",
    event: "workers_listed",
    fields: { request_id: "rid-workers", namespace: "demo", count: 3 },
  }]);
});
