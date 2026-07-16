import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import {
  doHostActorHarnessState,
  loadDoHostActor,
  resetDoHostActorHarness,
} from "../helpers/load-do-host-actor.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import { delay } from "../helpers/timing.js";

const { WdlDoHostActor, dispatchRpc } = await loadDoHostActor();
const harness = doHostActorHarnessState();

beforeEach(() => {
  resetDoHostActorHarness();
});

function actor(env = {}) {
  const ctx = {
    facets: {
      abort(/** @type {string} */ name, /** @type {unknown} */ reason) {
        harness.aborts.push({ name, reason });
        harness.abortReject?.(reason);
      },
    },
  };
  return new WdlDoHostActor(ctx, { ENV: "test", ...env });
}

function invoke(overrides = {}) {
  return {
    className: "Room",
    objectName: "alice",
    owner: {
      ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
      taskId: "task-a",
      generation: 7,
    },
    ...overrides,
  };
}

test("DO host actor: RPC dispatch passes request id through the internal wrapper boundary", async () => {
  /** @type {Array<{ url: string, header: string | null, requestId: string | null, body: unknown }>} */
  const calls = [];
  const facet = {
    async fetch(/** @type {Request} */ request) {
      const body = await request.json();
      calls.push({
        url: request.url,
        header: request.headers.get("x-wdl-do-internal-rpc"),
        requestId: request.headers.get("x-request-id"),
        body,
      });
      return Response.json({ ok: true, result: body });
    },
  };

  const response = await dispatchRpc(
    facet,
    { method: "inspect", args: ["value"] },
    "rid-rpc"
  );

  await assertJsonResponse(response, 200, {
    ok: true,
    result: {
      method: "inspect",
      args: ["value"],
    },
  });
  assert.deepEqual(calls, [{
    url: "https://do.internal/__wdl_rpc",
    header: "1",
    requestId: "rid-rpc",
    body: { method: "inspect", args: ["value"] },
  }]);
});

test("DO host actor: lease budget aborts a facet when the owner fence stops renewing", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 5,
  };
  const stale = Object.assign(new Error("owner generation is stale"), {
    status: 503,
    code: "stale_owner_generation",
  });
  harness.assertResponses = [owner, stale];

  await assert.rejects(
    host.dispatchWithFence(invoke(), () => new Promise((resolve, reject) => {
      harness.abortReject = reject;
    })),
    /lease budget exhausted/
  );

  assert.equal(harness.inFlight, 0);
  assert.equal(harness.draining, false);
  assert.deepEqual(harness.forgottenOwners, [owner.ownerKey]);
  assert.deepEqual(harness.aborts.map((entry) => entry.name), ["Room:alice"]);
  assert.equal(harness.logs.at(-1).event, "do_owner_lease_budget_exhausted");
  assert.equal(harness.logs.at(-1).fields.reason, "fence_failed");
});

test("DO host actor: lease budget uses Redis-time remaining budget, not local wall time", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
    leaseRemainingMs: 5,
  };
  const stale = Object.assign(new Error("owner generation is stale"), {
    status: 503,
    code: "stale_owner_generation",
  });
  harness.assertResponses = [owner, stale];

  await assert.rejects(
    host.dispatchWithFence(invoke(), () => new Promise((resolve, reject) => {
      harness.abortReject = reject;
      setTimeout(() => resolve(new Response("late")), 50);
    })),
    /lease budget exhausted/
  );

  assert.equal(harness.assertCalls, 2);
  assert.equal(harness.logs.at(-1).fields.reason, "fence_failed");
});

test("DO host actor: expired initial lease aborts before tenant dispatch", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() - 1,
  };
  harness.assertResponses = [owner];
  let ran = false;

  await assert.rejects(
    host.dispatchWithFence(invoke(), () => {
      ran = true;
      return new Response("should not run");
    }),
    /owner lease has expired/
  );

  assert.equal(ran, false);
  assert.equal(harness.inFlight, 0);
  assert.equal(harness.draining, false);
  assert.deepEqual(harness.forgottenOwners, [owner.ownerKey]);
  assert.deepEqual(harness.aborts.map((entry) => entry.name), ["Room:alice"]);
  assert.equal(harness.logs.at(-1).fields.reason, "expired");
});

test("DO host actor: stale initial owner fence does not write the object registry", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const stale = Object.assign(new Error("owner generation is stale"), {
    status: 503,
    code: "stale_owner_generation",
  });
  harness.assertResponses = [stale];
  harness.registryWait = Promise.resolve();
  let registryStarted = false;
  harness.registryWaitStarted = () => {
    registryStarted = true;
  };

  await assert.rejects(
    host.dispatchWithFence(invoke({
      doStorageId: "do_0123456789abcdef0123456789abcdef",
    }), () => new Response("should not run")),
    /owner generation is stale/
  );

  assert.equal(harness.assertCalls, 1);
  assert.equal(registryStarted, false);
  assert.deepEqual(harness.remembered, []);
});

test("DO host actor: registry delay is re-fenced before tenant dispatch", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const registryStarted = Promise.withResolvers();
  const releaseRegistry = Promise.withResolvers();
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
  };
  const stale = Object.assign(new Error("owner generation is stale"), {
    status: 503,
    code: "stale_owner_generation",
  });
  harness.assertResponses = [owner, stale];
  harness.registryWait = releaseRegistry.promise;
  harness.registryWaitStarted = () => registryStarted.resolve(undefined);
  let ran = false;

  const dispatch = host.dispatchWithFence(invoke({
    doStorageId: "do_0123456789abcdef0123456789abcdef",
  }), () => {
    ran = true;
    return new Response("should not run");
  });
  const first = await Promise.race([
    registryStarted.promise.then(() => "registry"),
    dispatch.then(() => "dispatch-resolved", () => "dispatch-rejected"),
  ]);

  assert.equal(first, "registry");
  assert.equal(harness.assertCalls, 1);
  releaseRegistry.resolve(undefined);
  await assert.rejects(dispatch, /owner generation is stale/);

  assert.equal(ran, false);
  assert.equal(harness.assertCalls, 2);
  assert.equal(harness.remembered.length, 1);
});

test("DO host actor: lease budget reschedules when renew extended the owner fence", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 5,
  };
  const renewed = {
    ...owner,
    leaseExpiresAt: Date.now() + 60_000,
  };
  harness.assertResponses = [owner, renewed];

  const response = await host.dispatchWithFence(invoke(), () => (
    new Promise((resolve) => setTimeout(() => resolve(new Response("ok")), 20))
  ));

  assert.equal(await response.text(), "ok");
  assert.equal(harness.assertResponses.length, 0);
  assert.equal(harness.inFlight, 0);
  assert.equal(harness.draining, false);
  assert.deepEqual(harness.aborts, []);
});

test("DO host actor: completed dispatch does not reschedule after an in-flight owner check", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 5,
  };
  /** @type {PromiseWithResolvers<{ ownerKey: string, taskId: string, generation: number, leaseExpiresAt: number }>} */
  const renewalControl = Promise.withResolvers();
  const renewal = renewalControl.promise;
  const resolveRenewal = renewalControl.resolve;
  harness.assertResponses = [owner, renewal];

  const response = await host.dispatchWithFence(invoke(), () => (
    new Promise((resolve) => setTimeout(() => resolve(new Response("ok")), 10))
  ));

  assert.equal(await response.text(), "ok");
  assert.equal(harness.assertCalls, 2);

  resolveRenewal({
    ...owner,
    leaseExpiresAt: Date.now() + 5,
  });
  await delay(20);

  assert.equal(harness.assertCalls, 2);
  assert.equal(harness.inFlight, 0);
  assert.equal(harness.draining, false);
  assert.deepEqual(harness.aborts, []);
  assert.deepEqual(harness.logs, []);
});

test("DO host actor: lease guard rejects near-expiry dispatch before tenant code runs", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 1000 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
    leaseRemainingMs: 250,
  };
  harness.assertResponses = [owner];
  let ran = false;

  await assert.rejects(
    host.dispatchWithFence(invoke(), () => {
      ran = true;
      return Promise.resolve(new Response("should not run"));
    }),
    /insufficient remaining budget/
  );

  assert.equal(ran, false);
  assert.equal(harness.inFlight, 0);
  assert.deepEqual(harness.forgottenOwners, [owner.ownerKey]);
  assert.equal(harness.logs.at(-1).fields.reason, "lease_guard");
});

test("DO host actor: registry remember failure is best-effort and does not fail dispatch", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  const owner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
  };
  harness.assertResponses = [owner, owner];
  harness.registryError = new Error("redis unavailable");

  const response = await host.dispatchWithFence(invoke({
    doStorageId: "storage-1",
    workerId: "demo:room:v1",
  }), () => Promise.resolve(new Response("ok")));

  assert.equal(await response.text(), "ok");
  assert.equal(harness.assertCalls, 2);
  assert.equal(harness.inFlight, 0);
  assert.deepEqual(harness.remembered, []);
  assert.equal(harness.logs.at(-1).level, "warn");
  assert.equal(harness.logs.at(-1).event, "do_object_registry_remember_failed");
  assert.equal(harness.logs.at(-1).fields.member, "Room:alice");
  assert.equal(harness.logs.at(-1).fields.worker_id, "demo:room:v1");
});

test("DO host actor strips tenant-supplied ownership error control markers", async () => {
  const host = actor({ DO_OWNER_LEASE_GUARD_MS: 0 });
  harness.assertResponses = [{
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    generation: 7,
    leaseExpiresAt: Date.now() + 60_000,
  }];

  const response = await host.dispatchWithFence(invoke(), () => Promise.resolve(
    new Response("tenant response", {
      status: 503,
      headers: { "x-wdl-do-ownership-error": "owner_fence_missing" },
    })
  ));

  assert.equal(await response.text(), "tenant response");
  assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
});
