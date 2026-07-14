import { beforeEach, test } from "node:test";
import assert from "node:assert/strict";

import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
  sharedModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { doProtocolDataUrl } from "../helpers/load-do-protocol.js";

const PROTOCOL_URL = doProtocolDataUrl();
const SHARED_TASK_IDENTITY_URL = sharedModuleDataUrl("shared/task-identity.js");
const SHARED_OWNER_ENDPOINT_URL = repositoryFileUrl("shared/owner-endpoint.js");
const src = applyModuleReplacements(readRepositoryFile("do-runtime/task-identity.js"), [
  [
    /import \{ createTaskIdentityResolver \} from "shared-task-identity";/,
    `import { createTaskIdentityResolver } from ${JSON.stringify(SHARED_TASK_IDENTITY_URL)};`,
  ],
  [
    /import \{ validOwnerEndpointForService \} from "shared-owner-endpoint";/,
    `import { validOwnerEndpointForService } from ${JSON.stringify(SHARED_OWNER_ENDPOINT_URL)};`,
  ],
  [
    /import \{ DoRuntimeError \} from "do-runtime-protocol";/,
    `import { DoRuntimeError } from ${JSON.stringify(PROTOCOL_URL)};`,
  ],
]);

const { DoRuntimeError } = await import(PROTOCOL_URL);
const {
  resetTaskIdentityForTests,
  resolveTaskIdentity,
  taskIdentityFromEcsMetadata,
  taskIdentityFromEnv,
} = await import(moduleDataUrl(src));

/**
 * @param {unknown} err
 * @param {number} status
 * @param {string} code
 */
function isRuntimeError(err, status, code) {
  const e = /** @type {{ status?: number, code?: string }} */ (err);
  return err instanceof DoRuntimeError && e.status === status && e.code === code;
}

beforeEach(() => {
  resetTaskIdentityForTests();
});

test("DO task identity: env task id and endpoint win", async () => {
  const env = {
    DO_TASK_ID: "do-runtime-a",
    DO_TASK_ENDPOINT: "do-runtime-a:8788",
    ECS_CONTAINER_METADATA_URI_V4: "http://metadata.invalid/v4/container",
  };

  assert.deepEqual(taskIdentityFromEnv(env), {
    taskId: "do-runtime-a",
    endpoint: "do-runtime-a:8788",
    source: "env",
  });
  assert.deepEqual(await resolveTaskIdentity(env, async () => {
    throw new Error("metadata should not be read when env identity is complete");
  }), {
    taskId: "do-runtime-a",
    endpoint: "do-runtime-a:8788",
    source: "env",
  });
});

test("DO task identity: public owner endpoints are rejected", () => {
  assert.throws(
    () => taskIdentityFromEnv({ DO_TASK_ID: "task-a", DO_TASK_ENDPOINT: "8.8.8.8:8788" }),
    (err) => isRuntimeError(err, 503, "task_identity_unavailable")
  );
});

test("DO task identity: partial env identity is rejected", () => {
  assert.throws(
    () => taskIdentityFromEnv({ DO_TASK_ID: "task-only" }),
    (err) => isRuntimeError(err, 503, "task_identity_unavailable")
  );
});

test("DO task identity: ECS metadata yields task arn and private IPv4 endpoint", () => {
  const identity = taskIdentityFromEcsMetadata({
    TaskARN: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    Containers: [
      {
        Name: "do-runtime",
        Networks: [{ IPv4Addresses: ["10.0.42.17"] }],
      },
    ],
  });

  assert.deepEqual(identity, {
    taskId: "arn:aws:ecs:us-east-1:123456789012:task/cluster/abc",
    endpoint: "10.0.42.17:8788",
    source: "ecs-metadata",
  });
});

test("DO task identity: resolve lazily fetches ECS task metadata and caches it", async () => {
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

  assert.deepEqual(await resolveTaskIdentity(env, fetchImpl), {
    taskId: "arn:aws:ecs:region:acct:task/cluster/task-id",
    endpoint: "10.0.0.5:8788",
    source: "ecs-metadata",
  });
  assert.deepEqual(await resolveTaskIdentity(env, async () => {
    throw new Error("cached identity should be reused");
  }), {
    taskId: "arn:aws:ecs:region:acct:task/cluster/task-id",
    endpoint: "10.0.0.5:8788",
    source: "ecs-metadata",
  });
  assert.equal(calls, 1);
});

test("DO task identity: missing env and metadata endpoint rejects before owner claim", async () => {
  await assert.rejects(
    () => resolveTaskIdentity({}, async () => Response.json({})),
    (err) => isRuntimeError(err, 503, "task_identity_unavailable")
  );
});
