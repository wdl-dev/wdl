import assert from "node:assert/strict";
import { test } from "node:test";

import { DurableObjectNamespace } from "../../runtime/do-client.js";
import { withMockedProperty } from "../helpers/mock-global.js";

test("DurableObjectNamespace fetch uses the binding-scoped host proxy", async () => {
  /** @type {Request[]} */
  const calls = [];
  const namespace = new DurableObjectNamespace({
    async fetch(/** @type {Request} */ request) {
      calls.push(request);
      return new Response("ok", { status: 201 });
    },
  }, { requestIdProvider: () => "rid-1" });
  Reflect.set(namespace, "requestId", () => "tenant-rid");

  const response = await namespace.get(namespace.idFromName(" room/雪 ")).fetch(
    "https://demo.workers.example/chat",
    { method: "POST", body: "hello" }
  );

  assert.equal(response.status, 201);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].headers.get("x-wdl-do-binding-object-name"), "%20room%2F%E9%9B%AA%20");
  assert.equal(calls[0].headers.get("x-wdl-do-binding-request-id"), "rid-1");
  assert.equal(calls[0].method, "POST");
  assert.equal(await calls[0].text(), "hello");
});

test("DurableObjectNamespace binding fetch rejects non-Response host results", async () => {
  const namespace = new DurableObjectNamespace({
    async fetch() {
      return { ok: true };
    },
  });

  await assert.rejects(
    namespace.get(namespace.idFromName("room-a")).fetch("https://demo.workers.example/chat"),
    /Durable Object binding fetch returned a non-Response value/
  );
});

test("DurableObjectNamespace passes ordinary and WebSocket requests to the same host proxy", async () => {
  /** @type {{ request: Request, websocket: boolean }[]} */
  const calls = [];
  const namespace = new DurableObjectNamespace({
    async fetch(/** @type {Request} */ request) {
      const websocket = request.headers.get("upgrade")?.toLowerCase() === "websocket";
      calls.push({ request, websocket });
      return new Response(websocket ? "websocket-ok" : "proxy-ok");
    },
  });

  const id = namespace.idFromName("room-a");
  const ordinary = await namespace.get(id).fetch("https://demo.workers.example/send");
  const websocket = await namespace.get(id).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(await ordinary.text(), "proxy-ok");
  assert.equal(await websocket.text(), "websocket-ok");
  assert.deepEqual(calls.map(({ websocket: upgrade }) => upgrade), [false, true]);
  assert.deepEqual(
    calls.map(({ request }) => request.headers.get("x-wdl-do-binding-object-name")),
    ["room-a", "room-a"]
  );
});

test("DurableObjectNamespace does not inspect host proxy metadata shape", async () => {
  await withMockedProperty(Object, "hasOwn", () => {
    throw new Error("tenant Object.hasOwn was called");
  }, async () => {
    const binding = {
      ns: "host-owned",
      className: "Room",
      async fetch() {
        return new Response("ok");
      },
    };
    const namespace = new DurableObjectNamespace(binding);
    const response = await namespace.get(namespace.idFromName("room")).fetch("https://example.test/");
    assert.equal(await response.text(), "ok");
  });
});

test("DurableObjectNamespace RPC preserves the host proxy receiver and private request id", async () => {
  const binding = {
    async rpcObject(
      /** @type {string} */ objectName,
      /** @type {string} */ method,
      /** @type {unknown[]} */ args,
      /** @type {string} */ requestId
    ) {
      assert.equal(this, binding);
      return { objectName, method, args, requestId };
    },
  };
  const namespace = new DurableObjectNamespace(binding, {
    requestIdProvider: () => "rid-rpc",
  });
  Reflect.set(namespace, "requestId", () => "tenant-rid");

  const result = await Reflect.get(namespace.get(namespace.idFromName("room-a")), "save")("hello");

  assert.deepEqual(result, {
    objectName: "room-a",
    method: "save",
    args: ["hello"],
    requestId: "rid-rpc",
  });
});

test("DurableObjectNamespace RPC accepts undefined host results", async () => {
  const namespace = new DurableObjectNamespace({
    async rpcObject() {
      return undefined;
    },
  });

  const result = await Reflect.get(namespace.get(namespace.idFromName("room-a")), "save")("hello");

  assert.equal(result, undefined);
});

test("DurableObjectNamespace rejects host proxies missing the requested operation", async () => {
  const fetchOnly = new DurableObjectNamespace({ async fetch() { return new Response("ok"); } });
  const rpcOnly = new DurableObjectNamespace({ async rpcObject() { return null; } });

  await assert.rejects(
    Reflect.get(fetchOnly.get(fetchOnly.idFromName("room")), "save")(),
    /Durable Object binding RPC is not configured/
  );
  await assert.rejects(
    rpcOnly.get(rpcOnly.idFromName("room")).fetch("https://example.test/"),
    /Durable Object binding fetch is not configured/
  );
});

test("DurableObjectNamespace does not revive the removed metadata transport path", async () => {
  let backendCalls = 0;
  const namespace = new DurableObjectNamespace(
    /** @type {any} */ ({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    }),
    /** @type {any} */ ({
      backend: { fetch() { backendCalls += 1; } },
      ownerNetwork: { fetch() { backendCalls += 1; } },
    })
  );
  const stub = namespace.get(namespace.idFromName("room"));

  await assert.rejects(
    stub.fetch("https://example.test/"),
    /Durable Object binding fetch is not configured/
  );
  await assert.rejects(
    Reflect.get(stub, "save")(),
    /Durable Object binding RPC is not configured/
  );
  assert.equal(backendCalls, 0);
});

test("DurableObjectNamespace validates ids and rejects foreign ids", async () => {
  const namespace = new DurableObjectNamespace({ fetch() {} });
  assert.throws(() => namespace.idFromName(""), /requires a non-empty string/);
  for (const value of ["\ud800", "\udc00"]) {
    assert.throws(() => namespace.idFromName(value), /requires well-formed Unicode/);
    assert.throws(() => namespace.idFromString(value), /requires well-formed Unicode/);
  }
  await withMockedProperty(String.prototype, "isWellFormed", () => true, () => {
    assert.throws(() => namespace.idFromName("\ud800"), /requires well-formed Unicode/);
  });
  assert.throws(
    () => namespace.get({ name: "room-a" }),
    /requires an id returned by this namespace/
  );
});
