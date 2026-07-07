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

setupIntegrationSuite();

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
});
