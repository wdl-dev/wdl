import { test } from "node:test";
import assert from "node:assert/strict";
import {
  composeStop,
  deployAndPromote,
  envoyStat,
  gatewayFetch,
  serviceInternalGet,
  serviceInternalPost,
  sh,
  uniqueNs,
  waitUntil,
  withDoMultiRuntimes,
  withServiceStopped,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import { redisSet } from "./helpers/redis.js";
import {
  doInternalInvoke,
  doInternalInvokeAsync,
  doHostId,
  doOwnerRedisKey,
  DO_RPC_WORKER,
  DO_SLOW_WORKER,
  DO_WORKER,
  redisGetDoStorageId,
  redisGetDoOwner,
  redisGetDoOwnerGeneration,
  redisSetDoOwner,
} from "./helpers/durable-objects.js";

setupIntegrationSuite();

test("Durable Object takeover preserves committed SQLite state after owner loss", async () => {
  const ns = uniqueNs("do-owner-loss");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");
  const ownerKey = doHostId(ns, "counter", "Counter", "alice");

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service, /** @type {string} */ objectName) => doInternalInvoke(service, {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName,
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });

    const first = invoke("do-runtime-a", "alice");
    assert.equal(first.status, 200, first.body);
    assert.deepEqual(responseJson(first), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });

    let owner = redisGetDoOwner(ownerKey);
    assert.equal(owner.taskId, "do-runtime-a");
    const shortLeaseOwner = {
      ...owner,
      leaseExpiresAt: Date.now() + 1000,
    };
    redisSetDoOwner(ownerKey, shortLeaseOwner);
    const renew = serviceInternalPost("do-runtime-a", 8788, "/internal/do/renew", {});
    assert.equal(renew.status, 200, renew.body);
    const renewPayload = responseJson(renew);
    assert.equal(renewPayload.renewed, 1);
    assert.equal(renewPayload.lost, 0);
    owner = redisGetDoOwner(ownerKey);
    assert.equal(owner.taskId, "do-runtime-a");
    assert.equal(owner.generation, shortLeaseOwner.generation);
    assert.ok(owner.leaseExpiresAt > shortLeaseOwner.leaseExpiresAt);

    const probe = serviceInternalGet(
      "do-runtime-a",
      8788,
      `/internal/do/probe?ownerKey=${encodeURIComponent(ownerKey)}&generation=${owner.generation}`
    );
    assert.equal(probe.status, 200, probe.body);
    const probePayload = responseJson(probe);
    assert.equal(probePayload.ownerKey, ownerKey);
    assert.equal(probePayload.owner.taskId, "do-runtime-a");
    assert.equal(probePayload.owner.generation, owner.generation);

    composeStop("do-runtime-a");
    redisSetDoOwner(ownerKey, {
      ...owner,
      leaseExpiresAt: Date.now() - 1000,
    });

    const afterTakeover = invoke("do-runtime-b", "alice");
    assert.equal(afterTakeover.status, 200, afterTakeover.body);
    assert.deepEqual(responseJson(afterTakeover), {
      objectId: "alice",
      memory: 1,
      storage: 2,
      body: "from-worker",
    });
    const newOwner = redisGetDoOwner(ownerKey);
    assert.equal(newOwner.taskId, "do-runtime-b");
    assert.equal(newOwner.generation, owner.generation + 1);

    const forwarded = invoke("do-runtime-c", "alice");
    assert.equal(forwarded.status, 200, forwarded.body);
    assert.deepEqual(responseJson(forwarded), {
      objectId: "alice",
      memory: 2,
      storage: 3,
      body: "from-worker",
    });
  });
});

test("near-expiry DO owner renews before shared SQLite write", async () => {
  const ns = uniqueNs("do-lease-guard");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");
  const ownerKey = doHostId(ns, "counter", "Counter", "alice");

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service) => doInternalInvoke(service, {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "alice",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });

    const first = invoke("do-runtime-a");
    assert.equal(first.status, 200, first.body);
    assert.deepEqual(responseJson(first), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });

    const owner = redisGetDoOwner(ownerKey);
    assert.equal(owner.taskId, "do-runtime-a");
    const nearExpiryOwner = {
      ...owner,
      leaseExpiresAt: Date.now() + 3000,
    };
    redisSetDoOwner(ownerKey, nearExpiryOwner);

    const afterRenew = invoke("do-runtime-a");
    assert.equal(afterRenew.status, 200, afterRenew.body);
    assert.deepEqual(responseJson(afterRenew), {
      objectId: "alice",
      memory: 2,
      storage: 2,
      body: "from-worker",
    });

    const renewedOwner = redisGetDoOwner(ownerKey);
    assert.equal(renewedOwner.taskId, "do-runtime-a");
    assert.equal(renewedOwner.generation, owner.generation);
    assert.ok(
      Number(renewedOwner.leaseExpiresAt) > Number(nearExpiryOwner.leaseExpiresAt),
      `expected actor-side renew before write, got ${JSON.stringify({ nearExpiryOwner, renewedOwner })}`
    );

    const forwarded = invoke("do-runtime-b");
    assert.equal(forwarded.status, 200, forwarded.body);
    assert.deepEqual(responseJson(forwarded), {
      objectId: "alice",
      memory: 3,
      storage: 3,
      body: "from-worker",
    });
  }, { ownerLeaseGuardMs: 5000 });
});

test("Durable Object committed SQLite state survives hard owner SIGKILL and takeover", async () => {
  const ns = uniqueNs("do-hard-loss");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");
  const ownerKey = doHostId(ns, "counter", "Counter", "alice");

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service) => doInternalInvoke(service, {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "alice",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });

    const beforeKill = invoke("do-runtime-a");
    assert.equal(beforeKill.status, 200, beforeKill.body);
    assert.deepEqual(responseJson(beforeKill), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });

    const owner = redisGetDoOwner(ownerKey);
    assert.equal(owner.taskId, "do-runtime-a");
    sh("COMPOSE_PROFILES=do-multi docker compose kill -s KILL do-runtime-a", { stdio: "pipe" });
    redisSetDoOwner(ownerKey, {
      ...owner,
      leaseExpiresAt: Date.now() - 1000,
    });

    /** @type {string | null} */
    let survivor = null;
    await waitUntil("survivor takeover after hard-killing do-runtime-a", () => {
      for (const service of ["do-runtime-b", "do-runtime-c"]) {
        const result = invoke(service);
        if (result.status !== 200) continue;
        const body = responseJson(result);
        if (
          body.objectId === "alice" &&
          body.memory === 1 &&
          body.storage === 2 &&
          body.body === "from-worker" &&
          redisGetDoOwner(ownerKey)?.taskId === service
        ) {
          survivor = service;
          return true;
        }
      }
      return false;
    }, { timeoutMs: 8000, intervalMs: 500 });

    const afterRecovery = invoke(/** @type {string} */ (/** @type {unknown} */ (survivor)));
    assert.equal(afterRecovery.status, 200, afterRecovery.body);
    assert.deepEqual(responseJson(afterRecovery), {
      objectId: "alice",
      memory: 2,
      storage: 3,
      body: "from-worker",
    });
  });
});

test("do-runtime replicas forward a sharded object owner scope instead of splitting object memory", async () => {
  const ns = uniqueNs("do-multi");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service, /** @type {string} */ objectName) => doInternalInvoke(service, {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName,
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });

    const first = invoke("do-runtime-a", "alice");
    assert.equal(first.status, 200, first.body);
    assert.deepEqual(responseJson(first), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });

    const second = invoke("do-runtime-b", "alice");
    assert.equal(second.status, 200, second.body);
    assert.deepEqual(responseJson(second), {
      objectId: "alice",
      memory: 2,
      storage: 2,
      body: "from-worker",
    });

    const other = invoke("do-runtime-c", "bob");
    assert.equal(other.status, 200, other.body);
    assert.deepEqual(responseJson(other), {
      objectId: "bob",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });

    const third = invoke("do-runtime-c", "alice");
    assert.equal(third.status, 200, third.body);
    assert.deepEqual(responseJson(third), {
      objectId: "alice",
      memory: 3,
      storage: 3,
      body: "from-worker",
    });

    const throughGateway = await gatewayFetch(ns, "/counter?name=alice");
    const throughGatewayText = await throughGateway.text();
    assert.equal(throughGateway.status, 200, throughGatewayText);
    assert.deepEqual(responseJson({ body: throughGatewayText }), {
      objectId: "alice",
      memory: 4,
      storage: 4,
      body: "from-worker",
    });

    const ownerKey = doHostId(ns, "counter", "Counter", "alice");
    redisSetDoOwner(ownerKey, {
      ownerKey,
      hostId: ownerKey,
      ns,
      worker: "counter",
      doStorageId,
      className: "Counter",
      taskId: "do-runtime-b",
      endpoint: "do-runtime-b:8788",
      generation: 20,
      leaseExpiresAt: Date.now() + 60_000,
    });
    const hopExhausted = doInternalInvoke("do-runtime-a", {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "alice",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
      },
    }, { body: "from-worker", headers: { "x-wdl-do-hop-count": "2" } });
    assert.equal(hopExhausted.status, 503, hopExhausted.body);
    assert.equal(responseJson(hopExhausted).error, "forward_hop_exhausted");

    const takeoverOwnerKey = doHostId(ns, "counter", "Counter", "takeover");
    redisSetDoOwner(takeoverOwnerKey, {
      ownerKey: takeoverOwnerKey,
      hostId: takeoverOwnerKey,
      ns,
      worker: "counter",
      doStorageId,
      className: "Counter",
      taskId: "stale-task",
      endpoint: "stale-task:8788",
      generation: 42,
      leaseExpiresAt: Date.now() - 1000,
    });
    redisSet(`${doOwnerRedisKey(takeoverOwnerKey)}:generation`, "40");
    const takeover = invoke("do-runtime-a", "takeover");
    assert.equal(takeover.status, 200, takeover.body);
    assert.equal(redisGetDoOwnerGeneration(takeoverOwnerKey), 43);

    const drain = serviceInternalPost("do-runtime-a", 8788, "/internal/do/drain", {});
    assert.equal(drain.status, 200, drain.body);
    assert.equal(responseJson(drain).released, 1);
    const afterDrain = invoke("do-runtime-b", "alice");
    assert.equal(afterDrain.status, 200, afterDrain.body);
    assert.deepEqual(responseJson(afterDrain), {
      objectId: "alice",
      memory: 1,
      storage: 5,
      body: "from-worker",
    });
  });
});

test("runtime DO facade learns owner hints and skips the router on later calls", async () => {
  const ns = uniqueNs("do-owner-hint");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });

  await withDoMultiRuntimes(async () => {
    await withServiceStopped("scheduler", async () => {
      const beforeDoRouter = envoyStat("cluster.do_router.upstream_rq_total");

      const first = await gatewayFetch(ns, "/counter?name=hinted");
      const firstText = await first.text();
      assert.equal(first.status, 200, firstText);
      assert.deepEqual(responseJson({ body: firstText }), {
        objectId: "hinted",
        memory: 1,
        storage: 1,
        body: "from-worker",
      });
      const afterFirstRouter = envoyStat("cluster.do_router.upstream_rq_total");
      assert.ok(
        afterFirstRouter > beforeDoRouter,
        "first DO facade call should enter through the do-runtime router"
      );
      assert.equal(first.headers["x-wdl-do-owner-endpoint"], undefined);

      const second = await gatewayFetch(ns, "/counter?name=hinted");
      const secondText = await second.text();
      assert.equal(second.status, 200, secondText);
      assert.deepEqual(responseJson({ body: secondText }), {
        objectId: "hinted",
        memory: 2,
        storage: 2,
        body: "from-worker",
      });
      assert.equal(
        envoyStat("cluster.do_router.upstream_rq_total"),
        afterFirstRouter,
        "learned DO owner hint should make the next facade call reach the owner directly"
      );
      assert.equal(second.headers["x-wdl-do-owner-endpoint"], undefined);
    });
  });
});

test("runtime DO RPC facade learns owner hints and skips the router on later calls", async () => {
  const ns = uniqueNs("do-rpc-owner-hint");
  await deployAndPromote(ns, "rooms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_RPC_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  await withDoMultiRuntimes(async () => {
    await withServiceStopped("scheduler", async () => {
      const beforeDoRouter = envoyStat("cluster.do_router.upstream_rq_total");

      const first = await gatewayFetch(ns, "/rooms?name=hinted-rpc");
      const firstText = await first.text();
      assert.equal(first.status, 200, firstText);
      assert.deepEqual(responseJson({ body: firstText }), {
        objectId: "hinted-rpc",
        memory: 1,
        stored: 1,
        text: "hello",
        meta: { role: "user" },
      });
      const afterFirstRouter = envoyStat("cluster.do_router.upstream_rq_total");
      assert.ok(
        afterFirstRouter > beforeDoRouter,
        "first DO RPC facade call should enter through the do-runtime router"
      );
      assert.equal(first.headers["x-wdl-do-owner-endpoint"], undefined);

      const second = await gatewayFetch(ns, "/rooms?name=hinted-rpc");
      const secondText = await second.text();
      assert.equal(second.status, 200, secondText);
      assert.deepEqual(responseJson({ body: secondText }), {
        objectId: "hinted-rpc",
        memory: 2,
        stored: 2,
        text: "hello",
        meta: { role: "user" },
      });
      assert.equal(
        envoyStat("cluster.do_router.upstream_rq_total"),
        afterFirstRouter,
        "learned DO owner hint should make the next RPC facade call reach the owner directly"
      );
      assert.equal(second.headers["x-wdl-do-owner-endpoint"], undefined);
    });
  });
});

test("DO drain waits for in-flight handlers before releasing owner", async () => {
  const ns = uniqueNs("do-drain-flight");
  const version = await deployAndPromote(ns, "slow", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_SLOW_WORKER },
    bindings: {
      SLOW: { type: "do", className: "SlowCounter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "slow");
  const ownerKey = doHostId(ns, "slow", "SlowCounter", "main");

  await withDoMultiRuntimes(async () => {
    const invoke = {
      ns,
      worker: "slow",
      version,
      doStorageId,
      className: "SlowCounter",
      objectName: "main",
      request: {
        method: "POST",
        url: "https://do.internal/sleep?ms=3000",
        headers: { "content-type": "text/plain" },
      },
    };
    const slow = doInternalInvokeAsync("do-runtime-a", invoke, { body: "from-worker" });
    await waitUntil("do-runtime-a in-flight metric", () => {
      const metrics = serviceInternalGet("do-runtime-a", 8788, "/_metrics").body;
      return /wdl_do_in_flight_requests\{service="do-runtime"\} 1/.test(metrics);
    }, { timeoutMs: 5000, intervalMs: 100 });

    const started = Date.now();
    const drain = serviceInternalPost("do-runtime-a", 8788, "/internal/do/drain", {});
    const elapsed = Date.now() - started;
    const slowResult = await slow;

    assert.equal(slowResult.status, 200, slowResult.body);
    assert.deepEqual(responseJson(slowResult), { memory: 1, storage: 1 });
    assert.equal(drain.status, 200, drain.body);
    const drainPayload = responseJson(drain);
    assert.equal(drainPayload.released, 1);
    assert.equal(drainPayload.inFlight, 0);
    assert.ok(drainPayload.drainWaitMs >= 250, `expected drain to wait, got ${drainPayload.drainWaitMs}ms`);
    assert.ok(elapsed >= 250, `expected drain request to block, got ${elapsed}ms`);
    assert.equal(redisGetDoOwner(ownerKey), null);

    const afterDrain = doInternalInvoke("do-runtime-b", {
      ...invoke,
      request: {
        ...invoke.request,
        url: "https://do.internal/after-drain",
      },
    }, { body: "from-worker" });
    assert.equal(afterDrain.status, 200, afterDrain.body);
    assert.deepEqual(responseJson(afterDrain), { memory: 1, storage: 2 });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-b");
  });
});

test("DO live takeover aborts stale in-flight handler before post-await SQLite write", async () => {
  const ns = uniqueNs("do-live-takeover");
  const version = await deployAndPromote(ns, "slow", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_SLOW_WORKER },
    bindings: {
      SLOW: { type: "do", className: "SlowCounter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "slow");
  const ownerKey = doHostId(ns, "slow", "SlowCounter", "main");

  await withDoMultiRuntimes(async () => {
    const invoke = (/** @type {string} */ service, /** @type {string} */ path) => doInternalInvoke(service, {
      ns,
      worker: "slow",
      version,
      doStorageId,
      className: "SlowCounter",
      objectName: "main",
      request: {
        method: "POST",
        url: `https://do.internal${path}`,
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });
    const invokeAsync = (/** @type {string} */ service, /** @type {string} */ path) => doInternalInvokeAsync(service, {
      ns,
      worker: "slow",
      version,
      doStorageId,
      className: "SlowCounter",
      objectName: "main",
      request: {
        method: "POST",
        url: `https://do.internal${path}`,
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });

    const initial = invoke("do-runtime-a", "/lease-state");
    assert.equal(initial.status, 200, initial.body);
    assert.deepEqual(responseJson(initial), { started: 0, after: 0 });
    await waitUntil("do-runtime-a owns slow object before live takeover test", () => (
      redisGetDoOwner(ownerKey)?.taskId === "do-runtime-a"
    ), { timeoutMs: 5000, intervalMs: 100 });
    const originalOwner = redisGetDoOwner(ownerKey);
    assert.equal(originalOwner.taskId, "do-runtime-a");

    const staleInFlight = invokeAsync("do-runtime-a", "/post-await-write?ms=7000");
    await waitUntil("do-runtime-a stale handler in flight", () => {
      const metrics = serviceInternalGet("do-runtime-a", 8788, "/_metrics").body;
      return /wdl_do_in_flight_requests\{service="do-runtime"\} 1/.test(metrics);
    }, { timeoutMs: 5000, intervalMs: 100 });

    await waitUntil("do-runtime-a owner lease expires before takeover", () => (
      redisGetDoOwner(ownerKey) === null
    ), { timeoutMs: 8000, intervalMs: 100 });
    const takeover = invoke("do-runtime-b", "/lease-state");
    assert.equal(takeover.status, 200, takeover.body);
    assert.deepEqual(responseJson(takeover), { started: 1, after: 0 });
    const takeoverOwner = redisGetDoOwner(ownerKey);
    assert.equal(takeoverOwner.taskId, "do-runtime-b");
    assert.equal(takeoverOwner.generation, originalOwner.generation + 1);

    const staleResult = await staleInFlight.catch((err) => ({
      status: "error",
      body: err instanceof Error ? err.message : String(err),
    }));
    if (staleResult.status === 200) {
      assert.notDeepEqual(
        responseJson(staleResult),
        { started: 1, after: 1 },
        "stale owner must not complete the post-await SQLite write after takeover"
      );
    }

    const finalState = invoke("do-runtime-b", "/lease-state");
    assert.equal(finalState.status, 200, finalState.body);
    assert.deepEqual(responseJson(finalState), { started: 1, after: 0 });
  }, { ownerTtlSeconds: 3, renewStartDelayMs: 60000, renewIntervalMs: 60000 });
});

test("DO idle owners are renewed by task heartbeat without fresh invokes", async () => {
  const ns = uniqueNs("do-idle-renew");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");
  const ownerKey = doHostId(ns, "counter", "Counter", "idle");

  await withDoMultiRuntimes(async () => {
    const first = doInternalInvoke("do-runtime-a", {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "idle",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });
    assert.equal(first.status, 200, first.body);
    const initialOwner = redisGetDoOwner(ownerKey);
    assert.equal(initialOwner.taskId, "do-runtime-a");

    await waitUntil("DO owner heartbeat renew", () => {
      const owner = redisGetDoOwner(ownerKey);
      return owner?.taskId === "do-runtime-a" &&
        Number(owner.leaseExpiresAt) > Number(initialOwner.leaseExpiresAt);
    }, { timeoutMs: 10000, intervalMs: 250 });

    const afterIdle = doInternalInvoke("do-runtime-b", {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "idle",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });
    assert.equal(afterIdle.status, 200, afterIdle.body);
    assert.deepEqual(responseJson(afterIdle), {
      objectId: "idle",
      memory: 2,
      storage: 2,
      body: "from-worker",
    });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-a");
  });
});

test("DO graceful shutdown drains owned objects before container stop", async () => {
  const ns = uniqueNs("do-grace-drain");
  const version = await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });
  const doStorageId = redisGetDoStorageId(ns, "counter");
  const ownerKey = doHostId(ns, "counter", "Counter", "grace");

  await withDoMultiRuntimes(async () => {
    const beforeStop = doInternalInvoke("do-runtime-a", {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "grace",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });
    assert.equal(beforeStop.status, 200, beforeStop.body);
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-a");

    composeStop("do-runtime-a");

    const afterStop = doInternalInvoke("do-runtime-b", {
      ns,
      worker: "counter",
      version,
      doStorageId,
      className: "Counter",
      objectName: "grace",
      request: {
        method: "POST",
        url: "https://do.internal/increment",
        headers: { "content-type": "text/plain" },
      },
    }, { body: "from-worker" });
    assert.equal(afterStop.status, 200, afterStop.body);
    assert.deepEqual(responseJson(afterStop), {
      objectId: "grace",
      memory: 1,
      storage: 2,
      body: "from-worker",
    });
    assert.equal(redisGetDoOwner(ownerKey).taskId, "do-runtime-b");
  });
});
