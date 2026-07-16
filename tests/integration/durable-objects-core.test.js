import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  deployAndPromote,
  gatewayFetch,
  uniqueNs,
  setupIntegrationSuite,
  responseJson,
} from "./helpers/index.js";
import {
  DO_BINARY_BODY_WORKER,
  DO_BINDINGS_WORKER,
  DO_RPC_WORKER,
  DO_WORKER,
} from "./helpers/durable-objects.js";
import { redisDel, redisSetEx } from "./helpers/redis.js";

setupIntegrationSuite();

const DO_INTRINSIC_GUARD_WORKER = readFileSync(
  new URL("../fixtures/do-intrinsic-guard-worker.mjs.txt", import.meta.url),
  "utf8"
);

test("worker Durable Object binding routes through do-runtime and preserves object state", async () => {
  const ns = uniqueNs("do");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });

  const first = await gatewayFetch(ns, "/counter?name=alice");
  const firstText = await first.text();
  assert.equal(first.status, 200, firstText);
  assert.deepEqual(responseJson({ body: firstText }), {
    objectId: "alice",
    memory: 1,
    storage: 1,
    body: "from-worker",
  });

  const second = await gatewayFetch(ns, "/counter?name=alice");
  const secondText = await second.text();
  assert.equal(second.status, 200, secondText);
  assert.deepEqual(responseJson({ body: secondText }), {
    objectId: "alice",
    memory: 2,
    storage: 2,
    body: "from-worker",
  });

  const other = await gatewayFetch(ns, "/counter?name=bob");
  const otherText = await other.text();
  assert.equal(other.status, 200, otherText);
  assert.deepEqual(responseJson({ body: otherText }), {
    objectId: "bob",
    memory: 1,
    storage: 1,
    body: "from-worker",
  });
});

test("version-delete lock does not interrupt active Durable Object traffic", async () => {
  const ns = uniqueNs("do-version-delete");
  await deployAndPromote(ns, "counter", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_WORKER },
    bindings: {
      COUNTER: { type: "do", className: "Counter" },
    },
  });

  const lockKey = `worker-delete-lock:${ns}:counter`;
  redisSetEx(lockKey, "version:integration-token", 30);
  try {
    const response = await gatewayFetch(ns, "/counter?name=alice");
    const text = await response.text();
    assert.equal(response.status, 200, text);
    assert.deepEqual(responseJson({ body: text }), {
      objectId: "alice",
      memory: 1,
      storage: 1,
      body: "from-worker",
    });
  } finally {
    redisDel(lockKey);
  }
});

test("tenant top-level intrinsic patches cannot bypass host env wrapping", async () => {
  const ns = uniqueNs("do-intrinsic-guard");
  await deployAndPromote(ns, "guard", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_INTRINSIC_GUARD_WORKER },
    vars: { BYPASS: true },
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const response = await gatewayFetch(ns, "/guard");
  const text = await response.text();
  assert.equal(response.status, 200, text);
  assert.deepEqual(responseJson({ body: text }), {
    room: "room-ok",
    roomFacade: "DurableObjectNamespace",
    ownerMetadataVisible: false,
    doBackendVisible: false,
    ownerNetworkVisible: false,
    workflowsBackendVisible: false,
  });
});

test("Durable Object classes receive ordinary Worker bindings", async () => {
  const ns = uniqueNs("do-bindings");
  await deployAndPromote(ns, "rooms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_BINDINGS_WORKER },
    bindings: {
      ROOM: { type: "do", className: "Room" },
      KV: { type: "kv", id: "rooms-kv" },
    },
  });

  const first = await gatewayFetch(ns, "/rooms?name=alice");
  const firstText = await first.text();
  assert.equal(first.status, 200, firstText);
  assert.deepEqual(responseJson({ body: firstText }), { current: 0, next: 1 });

  const second = await gatewayFetch(ns, "/rooms?name=alice");
  const secondText = await second.text();
  assert.equal(second.status, 200, secondText);
  assert.deepEqual(responseJson({ body: secondText }), { current: 1, next: 2 });
});

test("Durable Object fetch preserves binary request bodies", async () => {
  const ns = uniqueNs("do-body");
  await deployAndPromote(ns, "body", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_BINARY_BODY_WORKER },
    bindings: {
      ECHO: { type: "do", className: "EchoBody" },
    },
  });

  const response = await gatewayFetch(ns, "/body");
  const text = await response.text();
  assert.equal(response.status, 200, text);
  assert.deepEqual(responseJson({ body: text }), { bytes: [0, 255, 97] });
});

test("Durable Object RPC dispatches class methods with structured args", async () => {
  const ns = uniqueNs("do-rpc");
  await deployAndPromote(ns, "rooms", {
    mainModule: "worker.js",
    modules: { "worker.js": DO_RPC_WORKER },
    compatibilityFlags: ["no_nodejs_als"],
    bindings: {
      ROOM: { type: "do", className: "Room" },
    },
  });

  const first = await gatewayFetch(ns, "/rooms?name=alice");
  const firstText = await first.text();
  assert.equal(first.status, 200, firstText);
  assert.deepEqual(responseJson({ body: firstText }), {
    objectId: "alice",
    memory: 1,
    stored: 1,
    text: "hello",
    meta: { role: "user" },
  });

  const second = await gatewayFetch(ns, "/rooms?name=alice");
  const secondText = await second.text();
  assert.equal(second.status, 200, secondText);
  assert.deepEqual(responseJson({ body: secondText }), {
    objectId: "alice",
    memory: 2,
    stored: 2,
    text: "hello",
    meta: { role: "user" },
  });

  const failure = await gatewayFetch(ns, "/rooms/fail?name=alice");
  const failureText = await failure.text();
  assert.equal(failure.status, 500, failureText);
  assert.deepEqual(responseJson({ body: failureText }), {
    name: "TypeError",
    message: "room-rpc-failed",
    code: "do_rpc_error",
  });

  const stringFailure = await gatewayFetch(ns, "/rooms/throw-string?name=alice");
  const stringFailureText = await stringFailure.text();
  assert.equal(stringFailure.status, 500, stringFailureText);
  assert.deepEqual(responseJson({ body: stringFailureText }), {
    name: "Error",
    message: "room-rpc-string-failed",
    code: "do_rpc_error",
  });

  const objectFailure = await gatewayFetch(ns, "/rooms/throw-object?name=alice");
  const objectFailureText = await objectFailure.text();
  assert.equal(objectFailure.status, 500, objectFailureText);
  assert.deepEqual(responseJson({ body: objectFailureText }), {
    name: "Error",
    message: "[object Object]",
    code: "do_rpc_error",
  });

  const undefinedResult = await gatewayFetch(ns, "/rooms/undefined?name=alice");
  const undefinedResultText = await undefinedResult.text();
  assert.equal(undefinedResult.status, 200, undefinedResultText);
  assert.deepEqual(responseJson({ body: undefinedResultText }), {
    hasResult: false,
    result: null,
  });

  const forwarded = await gatewayFetch(ns, "/rooms/forward?name=alice&to=bob");
  const forwardedText = await forwarded.text();
  assert.equal(forwarded.status, 200, forwardedText);
  assert.deepEqual(responseJson({ body: forwardedText }), {
    forwardedBy: "alice",
    result: {
      objectId: "bob",
      memory: 1,
      stored: 1,
      text: "forwarded",
      meta: { role: "peer" },
    },
  });

  const requestId = "rid-do-rpc-context";
  const context = await gatewayFetch(ns, "/rooms/request-id?name=alice&to=bob", {
    headers: { "x-request-id": requestId },
  });
  const contextText = await context.text();
  assert.equal(context.status, 200, contextText);
  assert.deepEqual(responseJson({ body: contextText }), { requestId });

  const nestedContext = await gatewayFetch(
    ns,
    "/rooms/nested-request-id?name=alice&to=bob",
    { headers: { "x-request-id": requestId } }
  );
  const nestedContextText = await nestedContext.text();
  assert.equal(nestedContext.status, 200, nestedContextText);
  assert.deepEqual(responseJson({ body: nestedContextText }), { requestId });

});
