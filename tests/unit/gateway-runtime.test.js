import { test } from "node:test";
import assert from "node:assert/strict";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { sharedRedisStubUrl } from "../helpers/mocks/fake-redis.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { delay, waitUntil } from "../helpers/timing.js";

const redisUrl = sharedRedisStubUrl(`
export function defaultBackoff(attempt) {
  return Math.min(5000, 100 * (2 ** attempt));
}
globalThis.__gatewayRuntimeRedisReplyError = RedisReplyError;
export class RedisClient {
  constructor(_address, options) {
    globalThis.__gatewayRuntimeRedisClientOptions = options;
  }
  async eval(...args) {
    const impl = globalThis.__gatewayRuntimeRedisEval;
    if (typeof impl !== "function") throw new Error("unexpected lifecycle reconciliation");
    return await impl(...args);
  }
}
export class RedisSubscriber {
  constructor(_addr, channels, handlers) {
    globalThis.__gatewayRuntimeSubscriberChannels = channels;
    globalThis.__gatewayRuntimeSubscriberHandlers = handlers;
  }
  start() { return Promise.resolve(); }
}
`);
const nsPatternOwnerUrl = repositoryFileUrl("shared/ns-pattern.js");
const nsPatternUrl = moduleDataUrl(`
export {
  isValidRouteNs,
  isValidRuntimeLoadNs,
  isValidWorkerName,
  platformDomainFromEnv,
} from ${JSON.stringify(nsPatternOwnerUrl)};
`);
const routeProjectionUrl = moduleDataUrl(`
export function decodePatternProjection(raw) {
  globalThis.__gatewayRuntimeDecodeCalls?.push(raw);
  return raw;
}
`);
const gatewayLibOwnerUrl = repositoryFileUrl("gateway/lib.js");
const gatewayLibUrl = moduleDataUrl(`
export {
  GatewayRoutingUnavailableError,
  normalizeRequestHost,
} from ${JSON.stringify(gatewayLibOwnerUrl)};
export function isPatternInvalidationKey() { return true; }
export function sortPatterns(entries) { return { sorted: entries, errors: [] }; }
`);
const webSocketLifecycleSrc = applyModuleReplacements(
  readRepositoryFile("gateway/websocket-lifecycle.js"),
  [
    [/from "shared-redis";/, `from ${JSON.stringify(redisUrl)};`],
    [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
    [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
    [/from "shared-worker-contract";/, `from ${JSON.stringify(repositoryFileUrl("shared/worker-contract.js"))};`],
    [/from "gateway-lib";/, `from ${JSON.stringify(gatewayLibUrl)};`],
  ]
);
const webSocketLifecycleUrl = moduleDataUrl(webSocketLifecycleSrc);

const src = applyModuleReplacements(readRepositoryFile("gateway/runtime.js"), [
  [/from "shared-redis";/, `from ${JSON.stringify(redisUrl)};`],
  [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
  [/from "shared-route-projection";/, `from ${JSON.stringify(routeProjectionUrl)};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(nsPatternUrl)};`],
  [/from "shared-worker-contract";/, `from ${JSON.stringify(repositoryFileUrl("shared/worker-contract.js"))};`],
  [/from "gateway-lib";/, `from ${JSON.stringify(gatewayLibUrl)};`],
  [/from "gateway-websocket-lifecycle";/, `from ${JSON.stringify(webSocketLifecycleUrl)};`],
]);

let runtimeLoadSerial = 0;

async function loadGatewayRuntime() {
  runtimeLoadSerial += 1;
  return import(moduleDataUrl(`// gateway runtime test ${runtimeLoadSerial}\n${src}`));
}

const gatewayTestGlobal = /** @type {any} */ (globalThis);
const utf8Encoder = new TextEncoder();

/** @param {string} version @param {"preserve" | "restart"} [mode] @param {number} [restartSequence] */
function activeLifecycle(version, mode = "preserve", restartSequence = 0) {
  return { kind: "active", version, mode, restartSequence };
}

/**
 * @typedef {{
 *   onConnect(): void,
 *   onDisconnect(): void,
 *   onMessage(channel: string, payload: Uint8Array): void,
 * }} GatewaySubscriberHandlers
 */

/**
 * @template T
 * @param {(address: string) => Promise<unknown>} ensureGatewaySubscriber
 * @param {(handlers: GatewaySubscriberHandlers) => Promise<T>} fn
 */
async function withGatewaySubscriber(ensureGatewaySubscriber, fn) {
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    return await fn(handlers);
  } finally {
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberChannels;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
}

const { runtimeForwardOutcome } = await loadGatewayRuntime();

test("gatewayRoutingOptionsFromEnv memoizes normalized options by env identity", async () => {
  const { gatewayRoutingOptionsFromEnv } = await loadGatewayRuntime();
  const firstEnv = {
    PLATFORM_DOMAIN: " FIRST.WORKERS.EXAMPLE. ",
    ADMIN_HOST: "FIRST-ADMIN.EXAMPLE.",
  };
  const secondEnv = {
    PLATFORM_DOMAIN: "SECOND.WORKERS.EXAMPLE",
    ADMIN_HOST: "SECOND-ADMIN.EXAMPLE.",
  };

  const first = gatewayRoutingOptionsFromEnv(firstEnv);
  assert.deepEqual(first, {
    platformDomain: "first.workers.example",
    normalizedAdminHost: "first-admin.example",
  });
  assert.equal(gatewayRoutingOptionsFromEnv(firstEnv), first);
  assert.deepEqual(gatewayRoutingOptionsFromEnv(secondEnv), {
    platformDomain: "second.workers.example",
    normalizedAdminHost: "second-admin.example",
  });
});

test("runtimeForwardOutcome treats websocket upgrades as successful forwards", () => {
  assert.equal(runtimeForwardOutcome({ status: 101 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 200 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 302 }), "ok");
  assert.equal(runtimeForwardOutcome({ status: 400 }), "error");
  assert.equal(runtimeForwardOutcome({ status: 503 }), "error");
  assert.equal(runtimeForwardOutcome(null), "error");
});

test("only Gateway lifecycle Redis commands receive the socket deadline", async () => {
  const { createGatewayLifecycleRedis, createGatewayRedis } = await loadGatewayRuntime();
  createGatewayRedis("redis:6379");
  assert.equal(gatewayTestGlobal.__gatewayRuntimeRedisClientOptions.commandTimeoutMs, undefined);
  createGatewayLifecycleRedis("redis:6379");
  assert.equal(gatewayTestGlobal.__gatewayRuntimeRedisClientOptions.commandTimeoutMs, 2_000);
  delete gatewayTestGlobal.__gatewayRuntimeRedisClientOptions;
});

test("readWebSocketLifecycleSnapshot reads route and rollout at one linearization point", async () => {
  const { readWebSocketLifecycleSnapshot } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} script @param {string[]} keys @param {unknown[]} args */
    async eval(script, keys, args) {
      calls.push([script, keys, args]);
      return [
        "v2",
        JSON.stringify({ version: "v2", mode: "restart", restartSequence: 4 }),
      ];
    },
  };

  assert.deepEqual(
    await readWebSocketLifecycleSnapshot(redis, "demo", "chat"),
    activeLifecycle("v2", "restart", 4)
  );
  assert.deepEqual(calls[0].slice(1), [
    ["routes:demo", "worker:do-rollout:demo:chat"],
    ["chat"],
  ]);
});

test("readWebSocketLifecycleSnapshot accepts default preserve state and rejects torn projections", async () => {
  const { readWebSocketLifecycleSnapshot } = await loadGatewayRuntime();
  assert.deepEqual(
    await readWebSocketLifecycleSnapshot({ async eval() { return ["v1", null]; } }, "demo", "chat"),
    activeLifecycle("v1")
  );
  assert.deepEqual(
    await readWebSocketLifecycleSnapshot({ async eval() { return [null, null]; } }, "demo", "chat"),
    { kind: "inactive" }
  );
  await assert.rejects(
    readWebSocketLifecycleSnapshot({ async eval() { return ["latest", null]; } }, "demo", "chat"),
    /routing state changed/
  );
  await assert.rejects(
    readWebSocketLifecycleSnapshot({
      async eval() {
        return [
          "v2",
          JSON.stringify({ version: "v1", mode: "restart", restartSequence: 1 }),
        ];
      },
    }, "demo", "chat"),
    /routing state changed/
  );
  await assert.rejects(
    readWebSocketLifecycleSnapshot({
      async eval() {
        return [null, JSON.stringify({ version: "v1", mode: "restart", restartSequence: 1 })];
      },
    }, "demo", "chat"),
    /routing state changed/
  );
});

test("readWebSocketLifecycleSnapshot distinguishes Redis reply errors from transport failures", async () => {
  const {
    GatewayRoutingUnavailableError,
    readWebSocketLifecycleSnapshot,
  } = await loadGatewayRuntime();
  const RedisReplyError = gatewayTestGlobal.__gatewayRuntimeRedisReplyError;
  for (const code of ["WRONGTYPE", "ERR"]) {
    await assert.rejects(
      readWebSocketLifecycleSnapshot({
        async eval() {
          throw new RedisReplyError(`${code} failed`);
        },
      }, "demo", "chat"),
      (err) => err instanceof GatewayRoutingUnavailableError
    );
  }

  for (const code of ["BUSY", "CLUSTERDOWN", "LOADING", "MASTERDOWN", "READONLY", "TRYAGAIN"]) {
    const replyError = new RedisReplyError(`${code} retry later`);
    await assert.rejects(
      readWebSocketLifecycleSnapshot({ async eval() { throw replyError; } }, "demo", "chat"),
      (err) => err === replyError
    );
  }

  const transportError = new Error("redis failover");
  await assert.rejects(
    readWebSocketLifecycleSnapshot({
      async eval() {
        throw transportError;
      },
    }, "demo", "chat"),
    (err) => err === transportError
  );
});

test("route invalidation stays cache-only before an exact rollout reconciliation", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    /** @type {Array<[string, string]>} */
    const reads = [];
    /** @param {unknown} _script @param {string[]} keys @param {string[]} args */
    gatewayTestGlobal.__gatewayRuntimeRedisEval = async (_script, keys, args) => {
      reads.push([keys[0], args[0]]);
      if (keys[0] !== "routes:demo" || args[0] !== "chat") {
        throw new Error("unrelated lifecycle group was read");
      }
      return [
        "v2",
        JSON.stringify({ version: "v2", mode: "restart", restartSequence: 3 }),
      ];
    };
    /** @type {string[]} */
    const restarted = [];
    const unregisterOld = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v1", "restart", 2),
      { restart: () => restarted.push("old"), fail: () => restarted.push("old-failed") }
    );
    const unregisterCurrent = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v2", "restart", 3),
      { restart: () => restarted.push("current"), fail: () => restarted.push("current-failed") }
    );
    const unregisterUnrelated = registerGatewayWebSocketLifecycle(
      "demo",
      "idle",
      activeLifecycle("v1"),
      { restart: () => restarted.push("unrelated"), fail: () => restarted.push("unrelated-failed") }
    );

    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));
    await delay(0);
    assert.deepEqual(reads, []);

    handlers.onMessage("do-rollout:restart", utf8Encoder.encode(JSON.stringify({
      ns: "demo",
      worker: "chat",
      version: "v2",
      restartSequence: 3,
    })));
    assert.deepEqual(restarted, [], "subscriber callback must not run request-owned I/O inline");
    await waitUntil("old websocket lifecycle signal", () => restarted.length === 1);
    assert.deepEqual(restarted, ["old"]);
    assert.deepEqual(reads, [["routes:demo", "chat"]]);

    unregisterOld();
    unregisterCurrent();
    unregisterUnrelated();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
  });
});

test("subscriber reconciliation retries only transport-failed groups", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  const reads = new Map();
  /** @param {unknown} _script @param {string[]} _keys @param {string[]} args */
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async (_script, _keys, args) => {
    const worker = args[0];
    const count = (reads.get(worker) || 0) + 1;
    reads.set(worker, count);
    if (worker === "chat" && count === 1) throw new Error("redis failover");
    if (worker === "stable") return ["v1", null];
    return [
      "v2",
      JSON.stringify({ version: "v2", mode: "restart", restartSequence: 4 }),
    ];
  };
  let restarted = 0;
  let failed = 0;
  const unregisterFailed = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1", "restart", 3),
    { restart: () => { restarted += 1; }, fail: () => { failed += 1; } }
  );
  let stableSignals = 0;
  const unregisterStable = registerGatewayWebSocketLifecycle(
    "demo",
    "stable",
    activeLifecycle("v1"),
    { restart: () => { stableSignals += 1; }, fail: () => { stableSignals += 1; } }
  );
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await waitUntil(
      "first lifecycle reconciliation",
      () => reads.get("chat") === 1 && reads.get("stable") === 1
    );
    await delay(0);
    assert.equal(restarted, 0);
    assert.equal(failed, 0);
    await waitUntil(
      "retried lifecycle reconciliation",
      () => restarted === 1,
      { timeoutMs: 1_000, intervalMs: 10 }
    );
    assert.equal(reads.get("chat"), 2);
    assert.equal(reads.get("stable"), 1);
    assert.equal(stableSignals, 0);
    assert.equal(failed, 0);
    handlers.onDisconnect();
  } finally {
    unregisterFailed();
    unregisterStable();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("subscriber reconciliation fails closed on authoritative rollout corruption", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => [
    "v2",
    JSON.stringify({ version: "v1", mode: "restart", restartSequence: 4 }),
  ];
  let restarted = 0;
  let failed = 0;
  const unregister = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1", "restart", 3),
    { restart: () => { restarted += 1; }, fail: () => { failed += 1; } }
  );
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await waitUntil("authoritative lifecycle failure", () => failed === 1);
    assert.equal(restarted, 0);
    handlers.onDisconnect();
  } finally {
    unregister();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("subscriber reconciliation closes deleted workers as service restarts", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => [null, null];
  const restarted = Promise.withResolvers();
  const unregister = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1"),
    { restart: () => restarted.resolve(undefined), fail: () => restarted.reject(new Error("unexpected failure")) }
  );
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await restarted.promise;
  } finally {
    unregister();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("worker delete events defer to current authority without fencing later sessions", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  /** @type {Array<[string, string]>} */
  const reads = [];
  const firstRead = Promise.withResolvers();
  /** @param {unknown} _script @param {string[]} keys @param {string[]} args */
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async (_script, keys, args) => {
    reads.push([keys[0], args[0]]);
    if (keys[0] !== "routes:demo" || args[0] !== "chat") {
      throw new Error("unrelated lifecycle group was read");
    }
    if (reads.length === 1) {
      await firstRead.promise;
      throw new Error("redis failover");
    }
    return ["v3", null];
  };
  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    assert.ok(gatewayTestGlobal.__gatewayRuntimeSubscriberChannels.includes("worker:delete"));
    /** @type {string[]} */
    const signals = [];
    const unregisterOld = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v1"),
      { restart: () => signals.push("old"), fail: () => signals.push("old-failed") }
    );
    let unregisterRecreated = () => {};
    const unregisterUnrelated = registerGatewayWebSocketLifecycle(
      "demo",
      "idle",
      activeLifecycle("v1"),
      { restart: () => signals.push("unrelated"), fail: () => signals.push("unrelated-failed") }
    );
    try {
      handlers.onMessage("worker:delete", utf8Encoder.encode(JSON.stringify({
        ns: "demo",
        worker: "chat",
      })));
      await waitUntil("worker delete authority read", () => reads.length === 1);
      unregisterRecreated = registerGatewayWebSocketLifecycle(
        "demo",
        "chat",
        activeLifecycle("v2"),
        { restart: () => signals.push("recreated"), fail: () => signals.push("recreated-failed") }
      );
      firstRead.resolve(undefined);
      await waitUntil(
        "worker delete authority retry",
        () => reads.length === 2,
        { timeoutMs: 1_000, intervalMs: 10 }
      );
      await delay(0);
      assert.deepEqual(signals, []);
      assert.deepEqual(reads, [
        ["routes:demo", "chat"],
        ["routes:demo", "chat"],
      ]);
    } finally {
      firstRead.resolve(undefined);
      unregisterOld();
      unregisterRecreated();
      unregisterUnrelated();
      delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    }
  });
});

test("route flush remains a routing-cache invalidation", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  let reads = 0;
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => {
    reads += 1;
    return [null, null];
  };
  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    let signals = 0;
    const unregisterDemo = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v1"),
      { restart: () => { signals += 1; }, fail: () => { signals += 1; } }
    );
    const unregisterOther = registerGatewayWebSocketLifecycle(
      "other",
      "idle",
      activeLifecycle("v1"),
      { restart: () => { signals += 1; }, fail: () => { signals += 1; } }
    );
    try {
      handlers.onMessage("routes:flush", new Uint8Array());
      await delay(0);
      assert.equal(reads, 0);
      assert.equal(signals, 0);
    } finally {
      unregisterDemo();
      unregisterOther();
      delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    }
  });
});

test("a later preserve projection supersedes an unobserved restart sequence", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  let reads = 0;
  let projection = {
    version: "v3",
    mode: "preserve",
    restartSequence: 4,
  };
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => {
    reads += 1;
    return [
      projection.version,
      JSON.stringify(projection),
    ];
  };
  let restarted = 0;
  let failed = 0;
  let unregister = () => {};
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    unregister = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v1", "preserve", 3),
      { restart: () => { restarted += 1; }, fail: () => { failed += 1; } }
    );
    handlers.onMessage("do-rollout:restart", utf8Encoder.encode(JSON.stringify({
      ns: "demo",
      worker: "chat",
      version: "v2",
      restartSequence: 4,
    })));
    await waitUntil("preserve lifecycle reconciliation", () => reads === 1);
    assert.equal(restarted, 0, "the delayed restart event must defer to the latest projection");
    assert.equal(failed, 0);

    projection = { version: "v4", mode: "restart", restartSequence: 5 };
    handlers.onMessage("do-rollout:restart", utf8Encoder.encode(JSON.stringify({
      ns: "demo",
      worker: "chat",
      version: "v4",
      restartSequence: 5,
    })));
    await waitUntil("new restart sequence", () => restarted === 1);
    assert.equal(failed, 0);
    handlers.onDisconnect();
  } finally {
    unregister();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("subscriber reconnect reconciles registered websocket generations", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => [
    "v2",
    JSON.stringify({ version: "v2", mode: "restart", restartSequence: 4 }),
  ];
  const restarted = Promise.withResolvers();
  const unregister = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1", "restart", 3),
    { restart: () => restarted.resolve(undefined), fail: () => restarted.reject(new Error("unexpected failure")) }
  );
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await restarted.promise;
  } finally {
    unregister();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("subscriber reconnect reruns lifecycle reconciliation requested during an active pass", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => {
    reads += 1;
    if (reads === 1) return await firstRead.promise;
    return [
      "v2",
      JSON.stringify({ version: "v2", mode: "restart", restartSequence: 4 }),
    ];
  };
  const restarted = Promise.withResolvers();
  const unregister = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1", "restart", 3),
    { restart: () => restarted.resolve(undefined), fail: () => restarted.reject(new Error("unexpected failure")) }
  );
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await waitUntil("first lifecycle reconciliation", () => reads === 1);
    handlers.onDisconnect();
    handlers.onConnect();
    firstRead.resolve([
      "v1",
      JSON.stringify({ version: "v1", mode: "preserve", restartSequence: 3 }),
    ]);
    await restarted.promise;
    assert.equal(reads, 2);
  } finally {
    unregister();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("subscriber reconciliation does not apply an old snapshot to newly registered sessions", async () => {
  const {
    ensureGatewaySubscriber,
    registerGatewayWebSocketLifecycle,
  } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  gatewayTestGlobal.__gatewayRuntimeRedisEval = async () => {
    reads += 1;
    if (reads === 1) return await firstRead.promise;
    return [
      "v4",
      JSON.stringify({ version: "v4", mode: "restart", restartSequence: 6 }),
    ];
  };
  const oldRestarted = Promise.withResolvers();
  let newRestarted = 0;
  let newFailed = 0;
  const unregisterOld = registerGatewayWebSocketLifecycle(
    "demo",
    "chat",
    activeLifecycle("v1", "restart", 3),
    { restart: () => oldRestarted.resolve(undefined), fail: () => oldRestarted.reject(new Error("unexpected failure")) }
  );
  let unregisterNew = () => {};
  try {
    await ensureGatewaySubscriber("redis:6379");
    const handlers = /** @type {GatewaySubscriberHandlers} */ (
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers
    );
    handlers.onConnect();
    await waitUntil("started lifecycle reconciliation", () => reads === 1);
    unregisterNew = registerGatewayWebSocketLifecycle(
      "demo",
      "chat",
      activeLifecycle("v3", "restart", 5),
      { restart: () => { newRestarted += 1; }, fail: () => { newFailed += 1; } }
    );
    firstRead.resolve([
      "v2",
      JSON.stringify({ version: "v2", mode: "restart", restartSequence: 4 }),
    ]);
    await oldRestarted.promise;
    await delay(0);
    assert.equal(newFailed, 0);
    assert.equal(newRestarted, 0);

    handlers.onMessage("do-rollout:restart", utf8Encoder.encode(JSON.stringify({
      ns: "demo",
      worker: "chat",
      version: "v4",
      restartSequence: 6,
    })));
    await waitUntil("new websocket lifecycle signal", () => newRestarted === 1);
    assert.equal(reads, 2);
    assert.equal(newFailed, 0);
    handlers.onDisconnect();
  } finally {
    unregisterOld();
    unregisterNew();
    delete gatewayTestGlobal.__gatewayRuntimeRedisEval;
    delete gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers;
  }
});

test("resolveNamespaceRoutes fills the gate and route cache in one cold read", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} namespacesKey @param {string} hashKey @param {string} setKey */
    async sMembersHGetAllAndSMembers(namespacesKey, hashKey, setKey) {
      calls.push(["sMembersHGetAllAndSMembers", namespacesKey, hashKey, setKey]);
      return {
        namespaces: ["demo"],
        hash: { app: "v3" },
        members: [],
      };
    },
    /** @param {string} hashKey @param {string} setKey */
    async hGetAllAndSMembers(hashKey, setKey) {
      calls.push(["hGetAllAndSMembers", hashKey, setKey]);
      throw new Error("unexpected route reload");
    },
  };

  const cold = await resolveNamespaceRoutes(redis, "demo");
  const hot = await resolveNamespaceRoutes(redis, "demo");

  assert.equal(cold.known, true);
  assert.equal(cold.cacheHit, false);
  assert.deepEqual([...cold.routes], [["app", "v3"]]);
  assert.equal(hot.known, true);
  assert.equal(hot.cacheHit, true);
  assert.equal(hot.routes, cold.routes);
  assert.deepEqual(calls, [
    ["sMembersHGetAllAndSMembers", "namespaces", "routes:demo", "platform-domain-disabled:demo"],
  ]);
});

test("resolveNamespaceRoutes excludes platform-domain opt-outs from the same cold snapshot", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} namespacesKey @param {string} hashKey @param {string} setKey */
    async sMembersHGetAllAndSMembers(namespacesKey, hashKey, setKey) {
      calls.push(["sMembersHGetAllAndSMembers", namespacesKey, hashKey, setKey]);
      return {
        namespaces: ["demo"],
        hash: { public: "v2", routed: "v4" },
        members: ["routed"],
      };
    },
    /** @param {string} hashKey @param {string} setKey */
    async hGetAllAndSMembers(hashKey, setKey) {
      calls.push(["hGetAllAndSMembers", hashKey, setKey]);
      throw new Error("unexpected warm-miss reload");
    },
  };

  const cold = await resolveNamespaceRoutes(redis, "demo");

  assert.equal(cold.known, true);
  assert.deepEqual([...cold.routes], [["public", "v2"]]);
  assert.deepEqual(calls, [
    ["sMembersHGetAllAndSMembers", "namespaces", "routes:demo", "platform-domain-disabled:demo"],
  ]);
});

test("resolveNamespaceRoutes subtracts opt-outs on a warm-cache miss in one round trip", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} namespacesKey @param {string} hashKey @param {string} setKey */
    async sMembersHGetAllAndSMembers(namespacesKey, hashKey, setKey) {
      calls.push(["sMembersHGetAllAndSMembers", namespacesKey, hashKey, setKey]);
      return { namespaces: ["demo"], hash: { public: "v2", routed: "v4" }, members: [] };
    },
    /** @param {string} hashKey @param {string} setKey */
    async hGetAllAndSMembers(hashKey, setKey) {
      calls.push(["hGetAllAndSMembers", hashKey, setKey]);
      return { hash: { public: "v2", routed: "v4" }, members: ["routed"] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    // Cold read fills knownNs + routeCache; a per-ns route invalidation drops
    // only that route cache entry (knownNs stays), so the next read is a
    // warm-cache miss that must still fold the disabled set into one snapshot.
    await resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));
    const warm = await resolveNamespaceRoutes(redis, "demo");

    assert.equal(warm.known, true);
    assert.equal(warm.cacheHit, false);
    assert.deepEqual([...warm.routes], [["public", "v2"]]);
    assert.deepEqual(calls, [
      ["sMembersHGetAllAndSMembers", "namespaces", "routes:demo", "platform-domain-disabled:demo"],
      ["hGetAllAndSMembers", "routes:demo", "platform-domain-disabled:demo"],
    ]);
  });
});

test("unrelated route invalidation does not restart a warm namespace read", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  let warmReads = 0;
  const redis = {
    async sMembersHGetAllAndSMembers() {
      return { namespaces: ["demo", "other"], hash: { app: "v1" }, members: [] };
    },
    async hGetAllAndSMembers() {
      warmReads += 1;
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
        "routes:invalidate",
        utf8Encoder.encode("other")
      );
      return { hash: { app: "v2" }, members: [] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    await resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));

    const result = await resolveNamespaceRoutes(redis, "demo");

    assert.deepEqual([...result.routes], [["app", "v2"]]);
    assert.equal(warmReads, 1);
  });
});

test("same-namespace invalidation restarts concurrent warm route reads", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  const staleReads = [Promise.withResolvers(), Promise.withResolvers()];
  let warmReads = 0;
  const redis = {
    async sMembersHGetAllAndSMembers() {
      return { namespaces: ["demo"], hash: { app: "v1" }, members: [] };
    },
    async hGetAllAndSMembers() {
      warmReads += 1;
      if (warmReads <= staleReads.length) return await staleReads[warmReads - 1].promise;
      return { hash: { app: "v2" }, members: [] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    await resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));

    const pending = [
      resolveNamespaceRoutes(redis, "demo"),
      resolveNamespaceRoutes(redis, "demo"),
    ];
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));
    for (const read of staleReads) {
      read.resolve({ hash: { app: "stale" }, members: [] });
    }
    const results = await Promise.all(pending);

    for (const result of results) {
      assert.deepEqual([...result.routes], [["app", "v2"]]);
    }
    assert.equal(warmReads, 4);
  });
});

test("full route reset restarts a warm namespace read", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  let coldReads = 0;
  let warmReads = 0;
  const redis = {
    async sMembersHGetAllAndSMembers() {
      coldReads += 1;
      return {
        namespaces: ["demo"],
        hash: { app: coldReads === 1 ? "v1" : "v2" },
        members: [],
      };
    },
    async hGetAllAndSMembers() {
      warmReads += 1;
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
        "routes:flush",
        new Uint8Array()
      );
      return { hash: { app: "stale" }, members: [] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    await resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));

    const result = await resolveNamespaceRoutes(redis, "demo");

    assert.deepEqual([...result.routes], [["app", "v2"]]);
    assert.equal(coldReads, 2);
    assert.equal(warmReads, 1);
  });
});

test("resolveNamespaceRoutes keeps concurrent cold replies associated with their namespace", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  const alpha = Promise.withResolvers();
  const beta = Promise.withResolvers();
  const redis = {
    /** @param {string} _namespacesKey @param {string} hashKey @param {string} _setKey */
    async sMembersHGetAllAndSMembers(_namespacesKey, hashKey, _setKey) {
      if (hashKey === "routes:alpha") return await alpha.promise;
      if (hashKey === "routes:beta") return await beta.promise;
      throw new Error(`unexpected route key ${hashKey}`);
    },
  };

  const alphaPending = resolveNamespaceRoutes(redis, "alpha");
  const betaPending = resolveNamespaceRoutes(redis, "beta");
  beta.resolve({ namespaces: ["alpha", "beta"], hash: { api: "v2" }, members: [] });
  const betaResult = await betaPending;
  alpha.resolve({ namespaces: ["alpha", "beta"], hash: { app: "v1" }, members: [] });
  const alphaResult = await alphaPending;

  assert.deepEqual([...alphaResult.routes], [["app", "v1"]]);
  assert.deepEqual([...betaResult.routes], [["api", "v2"]]);
  assert.equal((await resolveNamespaceRoutes(redis, "alpha")).routes, alphaResult.routes);
  assert.equal((await resolveNamespaceRoutes(redis, "beta")).routes, betaResult.routes);
});

test("resolveNamespaceRoutes ignores a fetched hash for an unknown namespace", async () => {
  const { resolveNamespaceRoutes } = await loadGatewayRuntime();
  const redis = {
    async sMembersHGetAllAndSMembers() {
      return {
        namespaces: ["other"],
        hash: { app: "v3" },
        members: [],
      };
    },
  };

  assert.deepEqual(await resolveNamespaceRoutes(redis, "missing"), {
    known: false,
    routes: null,
    cacheHit: false,
  });
});

test("resolveHostPatterns fills the host gate and pattern cache in one cold read", async () => {
  const { resolveHostPatterns } = await loadGatewayRuntime();
  /** @type {unknown[][]} */
  const calls = [];
  const redis = {
    /** @param {string} setKey @param {string} hashKey */
    async sMembersAndHGetAll(setKey, hashKey) {
      calls.push(["sMembersAndHGetAll", setKey, hashKey]);
      return {
        members: ["api.example"],
        hash: { "/app/*": "projection" },
      };
    },
    /** @param {string} key */
    async hGetAll(key) {
      calls.push(["hGetAll", key]);
      throw new Error("unexpected pattern reload");
    },
  };

  const cold = await resolveHostPatterns(redis, "api.example", "rid-cold");
  const hot = await resolveHostPatterns(redis, "api.example", "rid-hot");

  assert.equal(cold.known, true);
  assert.equal(cold.cacheHit, false);
  assert.deepEqual(cold.patterns, { "/app/*": "projection" });
  assert.equal(hot.known, true);
  assert.equal(hot.cacheHit, true);
  assert.equal(hot.patterns, cold.patterns);
  assert.deepEqual(calls, [[
    "sMembersAndHGetAll",
    "declared-hosts",
    "patterns:api.example",
  ]]);
});

test("unrelated pattern invalidation does not restart a warm host read", async () => {
  const { ensureGatewaySubscriber, resolveHostPatterns } = await loadGatewayRuntime();
  let coldReads = 0;
  let warmReads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      coldReads += 1;
      return {
        members: ["a.example", "b.example", "other.example"],
        hash: { "/a/*": "projection-a" },
      };
    },
    async hGetAll() {
      warmReads += 1;
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
        "patterns:invalidate",
        utf8Encoder.encode("other.example")
      );
      return { "/b/*": "projection-b" };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async () => {
    await resolveHostPatterns(redis, "a.example", "rid-a");

    const result = await resolveHostPatterns(redis, "b.example", "rid-b");

    assert.deepEqual(result.patterns, { "/b/*": "projection-b" });
    assert.equal(coldReads, 1);
    assert.equal(warmReads, 1);
  });
});

test("same-host invalidation restarts a warm pattern read", async () => {
  const { ensureGatewaySubscriber, resolveHostPatterns } = await loadGatewayRuntime();
  let coldReads = 0;
  let warmReads = 0;
  const redis = {
    /** @param {string} _setKey @param {string} hashKey */
    async sMembersAndHGetAll(_setKey, hashKey) {
      coldReads += 1;
      return hashKey === "patterns:a.example"
        ? {
            members: ["a.example", "b.example"],
            hash: { "/a/*": "projection-a" },
          }
        : {
            members: ["a.example", "b.example"],
            hash: { "/b/*": "projection-v2" },
          };
    },
    async hGetAll() {
      warmReads += 1;
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
        "patterns:invalidate",
        utf8Encoder.encode("b.example")
      );
      return { "/b/*": "stale-projection" };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async () => {
    await resolveHostPatterns(redis, "a.example", "rid-a");

    const result = await resolveHostPatterns(redis, "b.example", "rid-b");

    assert.deepEqual(result.patterns, { "/b/*": "projection-v2" });
    assert.equal(coldReads, 2);
    assert.equal(warmReads, 1);
  });
});

test("full pattern reset restarts a warm host read", async () => {
  const { ensureGatewaySubscriber, resolveHostPatterns } = await loadGatewayRuntime();
  let coldReads = 0;
  let warmReads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      coldReads += 1;
      return {
        members: ["a.example", "b.example"],
        hash: coldReads === 1
          ? { "/a/*": "projection-a" }
          : { "/b/*": "projection-v2" },
      };
    },
    async hGetAll() {
      warmReads += 1;
      gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
        "patterns:invalidate",
        utf8Encoder.encode("*")
      );
      return { "/b/*": "stale-projection" };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async () => {
    await resolveHostPatterns(redis, "a.example", "rid-a");

    const result = await resolveHostPatterns(redis, "b.example", "rid-b");

    assert.deepEqual(result.patterns, { "/b/*": "projection-v2" });
    assert.equal(coldReads, 2);
    assert.equal(warmReads, 1);
  });
});

test("resolveHostPatterns does not decode projections for an undeclared host", async () => {
  const { resolveHostPatterns } = await loadGatewayRuntime();
  const testGlobal = /** @type {any} */ (globalThis);
  testGlobal.__gatewayRuntimeDecodeCalls = [];
  const redis = {
    async sMembersAndHGetAll() {
      return {
        members: ["other.example"],
        hash: { "/app/*": "must-not-decode" },
      };
    },
  };

  try {
    assert.deepEqual(await resolveHostPatterns(redis, "missing.example", "rid-miss"), {
      known: false,
      patterns: null,
      cacheHit: false,
    });
    assert.deepEqual(testGlobal.__gatewayRuntimeDecodeCalls, []);
  } finally {
    delete testGlobal.__gatewayRuntimeDecodeCalls;
  }
});

test("route invalidation prevents an older cold snapshot from restoring stale state", async () => {
  const { ensureGatewaySubscriber, resolveNamespaceRoutes } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  const redis = {
    async sMembersHGetAllAndSMembers() {
      reads += 1;
      if (reads === 1) return await firstRead.promise;
      return { namespaces: ["demo"], hash: { app: "v2" }, members: [] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    const pending = resolveNamespaceRoutes(redis, "demo");
    handlers.onMessage("routes:invalidate", utf8Encoder.encode("demo"));
    firstRead.resolve({ namespaces: ["demo"], hash: { app: "v1" }, members: [] });

    const result = await pending;
    assert.equal(result.known, true);
    assert.deepEqual([...result.routes], [["app", "v2"]]);
    assert.equal(reads, 2);
  });
});

test("pattern invalidation prevents an older cold snapshot from restoring stale state", async () => {
  const { ensureGatewaySubscriber, resolveHostPatterns } = await loadGatewayRuntime();
  const firstRead = Promise.withResolvers();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads === 1) return await firstRead.promise;
      return { members: ["api.example"], hash: { "/v2/*": "projection-v2" } };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async (handlers) => {
    const pending = resolveHostPatterns(redis, "api.example", "rid");
    handlers.onMessage("patterns:invalidate", utf8Encoder.encode("api.example"));
    firstRead.resolve({ members: ["api.example"], hash: { "/v1/*": "projection-v1" } });

    const result = await pending;
    assert.equal(result.known, true);
    assert.deepEqual(result.patterns, { "/v2/*": "projection-v2" });
    assert.equal(reads, 2);
  });
});

test("route resolution fails closed after bounded invalidation churn", async () => {
  const {
    GatewayRoutingUnavailableError,
    ensureGatewaySubscriber,
    resolveNamespaceRoutes,
  } = await loadGatewayRuntime();
  let reads = 0;
  const redis = {
    async sMembersHGetAllAndSMembers() {
      reads += 1;
      if (reads <= 5) {
        gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
          "routes:invalidate",
          utf8Encoder.encode("other")
        );
      }
      return { namespaces: ["demo"], hash: { app: "v1" }, members: [] };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async () => {
    await assert.rejects(
      resolveNamespaceRoutes(redis, "demo"),
      (err) => {
        assert.ok(err instanceof GatewayRoutingUnavailableError);
        const unavailable = /** @type {{ status: unknown, code: unknown, publicMessage: unknown }} */ (err);
        assert.equal(unavailable.status, 503);
        assert.equal(unavailable.code, "gateway_routing_unavailable");
        assert.equal(unavailable.publicMessage, "Gateway routing temporarily unavailable");
        return true;
      }
    );
    assert.equal(reads, 5);
  });
});

test("pattern resolution fails closed after bounded invalidation churn", async () => {
  const {
    GatewayRoutingUnavailableError,
    ensureGatewaySubscriber,
    resolveHostPatterns,
  } = await loadGatewayRuntime();
  let reads = 0;
  const redis = {
    async sMembersAndHGetAll() {
      reads += 1;
      if (reads <= 5) {
        gatewayTestGlobal.__gatewayRuntimeSubscriberHandlers.onMessage(
          "patterns:invalidate",
          utf8Encoder.encode("other.example")
        );
      }
      return { members: ["api.example"], hash: { "/": "projection" } };
    },
  };

  await withGatewaySubscriber(ensureGatewaySubscriber, async () => {
    await assert.rejects(
      resolveHostPatterns(redis, "api.example", "rid"),
      (err) => err instanceof GatewayRoutingUnavailableError
    );
    assert.equal(reads, 5);
  });
});
