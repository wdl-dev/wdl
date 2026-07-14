import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";

import { d1ProtocolDataUrl, loadD1Protocol } from "../helpers/load-d1-protocol.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  sharedModuleDataUrl,
} from "../helpers/load-shared-module.js";

const { D1ProtocolError } = await loadD1Protocol();

const PROTOCOL_URL = d1ProtocolDataUrl();
const SHARED_TASK_IDENTITY_URL = sharedModuleDataUrl("shared/task-identity.js");
const SHARED_OWNER_ENDPOINT_URL = repositoryFileUrl("shared/owner-endpoint.js");
const src = applyModuleReplacements(readRepositoryFile("d1-runtime/task-identity.js"), [
  [
    /import \{ createTaskIdentityResolver \} from "shared-task-identity";/,
    `import { createTaskIdentityResolver } from ${JSON.stringify(SHARED_TASK_IDENTITY_URL)};`,
  ],
  [
    /import \{ validOwnerEndpointForService \} from "shared-owner-endpoint";/,
    `import { validOwnerEndpointForService } from ${JSON.stringify(SHARED_OWNER_ENDPOINT_URL)};`,
  ],
  [
    /import \{ D1ProtocolError \} from "d1-runtime-protocol";/,
    `import { D1ProtocolError } from ${JSON.stringify(PROTOCOL_URL)};`,
  ],
]);

const mod = await import(moduleDataUrl(src));
const {
  peekTaskIdentity,
  resetTaskIdentityForTests,
  resolveTaskIdentity,
  taskIdentityFromEcsMetadata,
  taskIdentityFromEnv,
} = mod;

/**
 * @param {unknown} err
 * @param {number} status
 * @param {string} code
 */
function isProtocolError(err, status, code) {
  const e = /** @type {{ status?: number, code?: string }} */ (err);
  return err instanceof D1ProtocolError && e.status === status && e.code === code;
}

beforeEach(() => {
  resetTaskIdentityForTests();
});

test("D1 task identity: env task id and endpoint win", async () => {
  const env = {
    D1_TASK_ID: "d1-runtime-a",
    D1_TASK_ENDPOINT: "d1-runtime-a:8787",
    ECS_CONTAINER_METADATA_URI_V4: "http://metadata.invalid/v4/container",
  };

  assert.deepEqual(taskIdentityFromEnv(env), {
    taskId: "d1-runtime-a",
    endpoint: "d1-runtime-a:8787",
    source: "env",
  });
  assert.deepEqual(await resolveTaskIdentity(env, async () => {
    throw new Error("metadata should not be read when env identity is complete");
  }), {
    taskId: "d1-runtime-a",
    endpoint: "d1-runtime-a:8787",
    source: "env",
  });
});

test("D1 task identity: public owner endpoints are rejected", () => {
  assert.throws(
    () => taskIdentityFromEnv({ D1_TASK_ID: "task-a", D1_TASK_ENDPOINT: "8.8.8.8:8787" }),
    (err) => isProtocolError(err, 503, "task-identity-unavailable")
  );
});

test("D1 task identity: partial env identity is rejected", () => {
  assert.throws(
    () => taskIdentityFromEnv({ D1_TASK_ID: "task-only" }),
    (err) => isProtocolError(err, 503, "task-identity-unavailable")
  );
  assert.equal(peekTaskIdentity({ D1_TASK_ID: "task-only" }), null);
});

test("D1 task identity: ECS metadata yields task arn and private IPv4 endpoint", () => {
  const identity = taskIdentityFromEcsMetadata({
    TaskARN: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    Containers: [
      {
        Name: "d1-runtime",
        Networks: [{ IPv4Addresses: ["10.0.42.17"] }],
      },
    ],
  });

  assert.deepEqual(identity, {
    taskId: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    endpoint: "10.0.42.17:8787",
    source: "ecs-metadata",
  });
});

test("D1 task identity: prefers configured runtime container IP over sidecars", () => {
  const identity = taskIdentityFromEcsMetadata({
    TaskARN: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    Containers: [
      {
        Name: "log-router",
        Networks: [{ IPv4Addresses: ["10.0.9.9"] }],
      },
      {
        Name: "d1-runtime",
        Networks: [{ IPv4Addresses: ["10.0.42.17"] }],
      },
    ],
  }, { D1_TASK_CONTAINER_NAME: "d1-runtime" });

  assert.deepEqual(identity, {
    taskId: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    endpoint: "10.0.42.17:8787",
    source: "ecs-metadata",
  });
});

test("D1 task identity: resolve lazily fetches ECS task metadata and caches it", async () => {
  let calls = 0;
  const env = { ECS_CONTAINER_METADATA_URI_V4: "http://169.254.170.2/v4/container-id" };
  const fetchImpl = async (/** @type {string} */ url) => {
    calls += 1;
    assert.equal(url, "http://169.254.170.2/v4/container-id/task");
    return Response.json({
      TaskARN: "arn:aws:ecs:region:acct:task/cluster/task-id",
      Containers: [{ Networks: [{ IPv4Addresses: ["10.0.0.5"] }] }],
    });
  };

  assert.equal(peekTaskIdentity(env), null);
  assert.deepEqual(await resolveTaskIdentity(env, fetchImpl), {
    taskId: "arn:aws:ecs:region:acct:task/cluster/task-id",
    endpoint: "10.0.0.5:8787",
    source: "ecs-metadata",
  });
  assert.deepEqual(await resolveTaskIdentity(env, async () => {
    throw new Error("cached identity should be reused");
  }), {
    taskId: "arn:aws:ecs:region:acct:task/cluster/task-id",
    endpoint: "10.0.0.5:8787",
    source: "ecs-metadata",
  });
  assert.equal(calls, 1);
});

test("D1 task identity: missing env and metadata endpoint rejects before owner claim", async () => {
  await assert.rejects(
    () => resolveTaskIdentity({}, async () => Response.json({})),
    (err) => isProtocolError(err, 503, "task-identity-unavailable")
  );
});
