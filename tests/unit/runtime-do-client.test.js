import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableObjectNamespace,
  clearDoOwnerHintsForTest,
  setDoOwnerHintMaxEntriesForTest,
} from "../../runtime/do-client.js";
import {
  MAX_DO_REQUEST_HEADER_BYTES,
  fetchInvokeInit,
  ownerHintFromHeaders,
  requestSpec,
  rpcInvokeBody,
} from "../../runtime/_wdl-do-transport.js";
import { decodeDoEnvelope } from "../helpers/do-envelope.js";
import {
  doOwnerHintHeaders,
  doOwnerHintResponse,
  doOwnershipErrorHeaders,
} from "../helpers/do-owner-hint.js";
import { makeRecordingFetch } from "../helpers/mock-fetch.js";
import {
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
import { readJsonResponse } from "../helpers/response-json.js";

test.afterEach(() => {
  clearDoOwnerHintsForTest();
});

/** @param {HeadersInit | undefined} headers @param {string} name */
function headerValue(headers, name) {
  return new Headers(headers).get(name);
}

test("DurableObjectNamespace facade forwards fetch with object name and request id", async () => {
  /** @type {any[]} */
  const calls = [];
  const ns = new DurableObjectNamespace({
    async fetchObject(/** @type {string} */ objectName, /** @type {Request} */ request, /** @type {string} */ requestId) {
      calls.push({ objectName, request, requestId });
      return new Response("ok", { status: 201 });
    },
  }, { requestIdProvider: () => "rid-1" });

  const id = ns.idFromName("room-a");
  const response = await ns.get(id).fetch("https://demo.workers.example/chat", {
    method: "POST",
    body: "hello",
  });

  assert.equal(response.status, 201);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].objectName, "room-a");
  assert.equal(calls[0].requestId, "rid-1");
  assert.equal(calls[0].request.method, "POST");
  assert.equal(await calls[0].request.text(), "hello");
});

test("DurableObjectNamespace binding fetch rejects non-Response host results", async () => {
  const ns = new DurableObjectNamespace({
    async fetchObject() {
      return { ok: true };
    },
  });

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/chat"),
    /Durable Object binding fetchObject returned a non-Response value/
  );
});

test("DurableObjectNamespace metadata host proxy handles normal fetch while websocket uses backend fetcher", async () => {
  /** @type {any[]} */
  const proxyCalls = [];
  /** @type {any[]} */
  const backendCalls = [];
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
    hostProxy: {
      async fetchObject(/** @type {string} */ objectName, /** @type {Request} */ request) {
        proxyCalls.push({ objectName, request });
        return new Response("proxy-ok");
      },
    },
  }, {
    backend: {
      fetch: makeRecordingFetch(backendCalls, { response: new Response("backend-ok") }),
    },
  });

  const normal = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send");
  const websocket = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(await normal.text(), "proxy-ok");
  assert.equal(await websocket.text(), "backend-ok");
  assert.equal(proxyCalls.length, 1);
  assert.equal(backendCalls.length, 1);
  assert.equal(backendCalls[0].url, "http://do-runtime/internal/do/connect");
});

test("DurableObjectNamespace direct backend keeps binding/backend in private fields", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, { response: new Response("ok", { status: 202 }) }),
  };
  const binding = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  };
  const ns = new DurableObjectNamespace(binding, { backend, requestIdProvider: () => "rid-1" });
  binding.ns = "attacker";
  Reflect.set(ns, "binding", { ns: "attacker", worker: "evil", version: "v9", className: "Evil" });
  Reflect.set(ns, "options", { backend: { fetch() { throw new Error("public options should not be used"); } } });

  const response = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  });

  assert.equal(response.status, 202);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(headerValue(calls[0].init.headers, "x-request-id"), "rid-1");
  assert.equal(headerValue(calls[0].init.headers, "x-wdl-internal-auth"), null);
  const { metadata: body, bodyBytes } = decodeDoEnvelope(calls[0].init.body);
  assert.equal(body.ns, "tenant");
  assert.equal(body.worker, "chat");
  assert.equal(body.version, "v1");
  assert.equal(body.doStorageId, "do_0123456789abcdef0123456789abcdef");
  assert.equal(body.className, "Room");
  assert.equal(body.objectName, "room-a");
  assert.equal(Buffer.from(bodyBytes).toString("utf8"), "hello");
});

test("DurableObjectNamespace direct backend does not expose internal auth through tenant Headers hooks", async () => {
  /** @type {string[]} */
  const capturedAuthWrites = [];
  const originalSet = Headers.prototype.set;
  await withMockedProperty(Headers.prototype, "set", /** @this {Headers} */ function set(
    /** @type {string} */ name,
    /** @type {string} */ value
  ) {
    if (String(name).toLowerCase() === "x-wdl-internal-auth") {
      capturedAuthWrites.push(String(value));
    }
    return originalSet.call(this, name, value);
  }, async () => {
    const backend = {
      async fetch() {
        return new Response("ok");
      },
    };
    const ns = new DurableObjectNamespace({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    }, { backend });

    await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send");
    assert.deepEqual(capturedAuthWrites, []);
  });
});

test("DurableObjectNamespace fetch rejects oversized header lists before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  const headers = new Headers();
  for (let i = 0; i < 129; i += 1) headers.set(`x-test-${i}`, "value");

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", { headers }),
    /Durable Object fetch headers exceed 128 entries/
  );
});

test("DurableObjectNamespace fetch rejects oversized aggregate headers before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  const headers = new Headers();
  for (let i = 0; i < 128; i += 1) headers.set(`x-test-${i}`, "x".repeat(600));

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", { headers }),
    /Durable Object fetch headers exceed 65536 bytes/
  );
});

test("DurableObjectNamespace fetch rejects oversized invoke envelopes before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch(`https://demo.workers.example/${"x".repeat(2 * 1024 * 1024)}`),
    /Durable Object invoke envelope exceeds 2097152 bytes/
  );
});

test("DurableObjectNamespace stub forwards arbitrary RPC methods", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, { response: Response.json({ ok: true, result: { saved: true } }) }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, requestIdProvider: () => "rid-rpc" });

  const stub = ns.get(ns.idFromName("room-a"));
  const result = await Reflect.get(stub, "addMessage")("hi", { role: "user" });

  assert.deepEqual(result, { saved: true });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(headerValue(calls[0].init.headers, "x-request-id"), "rid-rpc");
  assert.equal(headerValue(calls[0].init.headers, "x-wdl-internal-auth"), null);
  const { metadata: body } = decodeDoEnvelope(calls[0].init.body);
  assert.equal(body.kind, "rpc");
  assert.equal(body.objectName, "room-a");
  assert.deepEqual(body.rpc, {
    method: "addMessage",
    args: ["hi", { role: "user" }],
  });
});

test("DurableObjectNamespace binding RPC preserves proxy receiver", async () => {
  const binding = {
    async fetchObject() {
      return new Response("unused");
    },
    async rpcObject(/** @type {string} */ objectName, /** @type {string} */ method, /** @type {unknown[]} */ args, /** @type {string} */ requestId) {
      assert.equal(this, binding);
      return { objectName, method, args, requestId };
    },
  };
  const ns = new DurableObjectNamespace(binding, { requestIdProvider: () => "rid-rpc-receiver" });

  const result = await Reflect.get(ns.get(ns.idFromName("room-a")), "save")("hello");

  assert.deepEqual(result, {
    objectName: "room-a",
    method: "save",
    args: ["hello"],
    requestId: "rid-rpc-receiver",
  });
});

test("DurableObjectNamespace binding RPC accepts undefined host results", async () => {
  const ns = new DurableObjectNamespace({
    async fetchObject() {
      return new Response("unused");
    },
    async rpcObject() {
      return undefined;
    },
  });

  const result = await Reflect.get(ns.get(ns.idFromName("room-a")), "save")("hello");

  assert.equal(result, undefined);
});

test("DurableObjectNamespace RPC rejects non-JSON data before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  const stub = ns.get(ns.idFromName("room-a"));
  const circular = {};
  circular.self = circular;

  await assert.rejects(Reflect.get(stub, "save")(new Map([["key", "value"]])), /plain JSON object/);
  await assert.rejects(Reflect.get(stub, "save")(new Headers()), /plain JSON object/);
  await assert.rejects(Reflect.get(stub, "save")({ fn() {} }), /rpc\.args\[0\]\.fn must be JSON data/);
  await assert.rejects(Reflect.get(stub, "save")([undefined]), /rpc\.args\[0\]\[0\] must be JSON data/);
  await assert.rejects(Reflect.get(stub, "save")(Number.NaN), /finite number/);
  await assert.rejects(Reflect.get(stub, "save")(circular), /must not be circular/);
});

test("DurableObjectNamespace RPC rejects invalid and reserved methods before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  const stub = ns.get(ns.idFromName("room-a"));

  await assert.rejects(Reflect.get(stub, "not-valid-method")(), /rpc\.method is not valid/);
  await assert.rejects(Reflect.get(stub, "alarm")(), /rpc\.method is reserved/);
});

test("DO RPC validation uses captured JSON and method intrinsics", async () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };

  await withMockedProperty(Object, "entries", () => [], () => {
    assert.throws(
      () => rpcInvokeBody(props, "room-a", "save", [{ fn() {} }]),
      (error) => error instanceof TypeError && error.message === "rpc.args[0].fn must be JSON data"
    );
  });
  await withMockedProperty(Number, "isFinite", () => true, () => {
    assert.throws(
      () => rpcInvokeBody(props, "room-a", "save", [Number.NaN]),
      (error) => error instanceof TypeError && error.message === "rpc.args[0] must be a finite number"
    );
  });
  await withMockedProperty(RegExp.prototype, "test", () => true, () => {
    assert.throws(
      () => rpcInvokeBody(props, "room-a", "not-valid-method", []),
      (error) => error instanceof TypeError && error.message === "rpc.method is not valid"
    );
  });
});

test("DurableObjectNamespace RPC rejects oversized args before transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await assert.rejects(
    Reflect.get(ns.get(ns.idFromName("room-a")), "save")("x".repeat(1024 * 1024 + 1)),
    /rpc\.args exceeds 1048576 bytes/
  );
});

test("DurableObjectNamespace RPC throws structured DO errors", async () => {
  const backend = {
    async fetch() {
      return Response.json({
        error: "do_rpc_error",
        name: "TypeError",
        message: "bad method",
        stack: "TypeError: bad method\n    at remote-do",
      }, { status: 500 });
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await assert.rejects(
    Reflect.get(ns.get(ns.idFromName("room-a")), "fail")(),
    (err) => err instanceof Error &&
      err.name === "TypeError" &&
      Reflect.get(err, "code") === "do_rpc_error" &&
      err.message === "bad method" &&
      err.stack === "TypeError: bad method\n    at remote-do"
  );
});

test("DurableObjectNamespace RPC retries stale owner generation once", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => {
        if (calls.length === 1) {
          return Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("stale_owner_generation"),
          });
        }
        return Response.json({ ok: true, result: "rpc-ok" });
      },
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const result = await Reflect.get(ns.get(ns.idFromName("room-a")), "addMessage")("hello");

  assert.equal(result, "rpc-ok");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(headerValue(calls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
  assert.equal(calls[1].init.headers.get("x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace RPC retries owner claim races once", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "owner_claim_raced", message: "retry" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("owner_claim_raced"),
          })
        : Response.json({ ok: true, result: "claimed" }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const result = await Reflect.get(ns.get(ns.idFromName("room-a")), "addMessage")("hello");

  assert.equal(result, "claimed");
  assert.equal(calls.length, 2);
  assert.equal(calls[1].init.headers.get("x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace direct backend preserves binary request bodies", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, { response: new Response("ok") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
    method: "POST",
    body: new Uint8Array([0, 255, 97]),
  });

  const { bodyBytes } = decodeDoEnvelope(calls[0].init.body);
  assert.deepEqual([...Buffer.from(bodyBytes)], [0, 255, 97]);
});

test("DurableObjectNamespace direct backend retries stale owner generation once", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => {
        if (calls.length === 1) {
          return Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("stale_owner_generation"),
          });
        }
        return new Response("ok");
      },
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const response = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  });

  assert.equal(await response.text(), "ok");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(headerValue(calls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
  assert.equal(calls[1].init.headers.get("x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace does not replay tenant responses that mimic ownership errors", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: Response.json({
        error: "owner_fence_missing",
        message: "tenant response after side effect",
      }, { status: 503 }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const originalGet = Headers.prototype.get;
  await withMockedProperty(Headers.prototype, "get", /** @this {Headers} */ function get(name) {
    if (String(name).toLowerCase() === "x-wdl-do-ownership-error") {
      return "owner_fence_missing";
    }
    return originalGet.call(this, name);
  }, async () => {
    const response = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
      method: "POST",
      body: "hello",
    });

    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "owner_fence_missing",
      message: "tenant response after side effect",
    });
    assert.equal(calls.length, 1);
  });
});

test("DurableObjectNamespace owner-race classification uses the captured status getter", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "owner_claim_raced", message: "retry" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("owner_claim_raced"),
          })
        : new Response("retried"),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const response = await withMockedPropertyDescriptor(
    Response.prototype,
    "status",
    { configurable: true, get: () => 200 },
    () => ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send")
  );

  assert.equal(await response.text(), "retried");
  assert.equal(calls.length, 2);
});

test("DurableObjectNamespace retry policy uses the captured Set membership check", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "owner_unavailable", message: "outcome unknown" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("owner_unavailable"),
          })
        : new Response("replayed"),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const response = await withMockedProperty(
    Set.prototype,
    "has",
    function mockedHas() { return true; },
    () => ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
      method: "POST",
      body: "side effect",
    })
  );

  assert.equal(response.status, 503);
  assert.equal(calls.length, 1);
});

test("DurableObjectNamespace retry policy uses the captured Request method getter", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: Response.json({ error: "owner_unavailable", message: "outcome unknown" }, {
        status: 503,
        headers: doOwnershipErrorHeaders("owner_unavailable"),
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  let reads = 0;

  const response = await withMockedPropertyDescriptor(
    Request.prototype,
    "method",
    {
      configurable: true,
      get() {
        reads += 1;
        return reads === 1 ? "POST" : "GET";
      },
    },
    () => ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
      method: "POST",
      body: "side effect",
    })
  );

  assert.equal(response.status, 503);
  assert.equal(calls.length, 1);
  assert.equal(reads, 0);
  const { metadata } = decodeDoEnvelope(calls[0].init.body);
  assert.equal(/** @type {any} */ (metadata.request).method, "POST");
});

test("DurableObjectNamespace retry policy uses captured string case intrinsics", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: Response.json({ error: "owner_unavailable", message: "outcome unknown" }, {
        status: 503,
        headers: doOwnershipErrorHeaders("owner_unavailable"),
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });
  let upperReads = 0;

  const response = await withMockedProperty(
    String.prototype,
    "toUpperCase",
    function mockedToUpperCase() {
      upperReads += 1;
      return upperReads === 1 ? "POST" : "GET";
    },
    () => withMockedProperty(
      String.prototype,
      "toLowerCase",
      function mockedToLowerCase() {
        return "websocket";
      },
      () => ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
        method: "POST",
        body: "side effect",
      })
    )
  );

  assert.equal(response.status, 503);
  assert.equal(calls.length, 1);
  assert.equal(upperReads, 0);
  const { metadata } = decodeDoEnvelope(calls[0].init.body);
  assert.equal(/** @type {any} */ (metadata.request).method, "POST");
});

test("DurableObjectNamespace strips owner metadata with patched Headers methods", async () => {
  const backend = {
    fetch: makeRecordingFetch([], {
      response: new Response("ok", {
        headers: doOwnershipErrorHeaders("owner_fence_missing", doOwnerHintHeaders()),
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const iteratorSymbol = Symbol.iterator;
  const arrayIterator = Array.prototype[Symbol.iterator];
  const arrayIncludes = Array.prototype.includes;
  const response = await withMockedProperty(
    Headers.prototype,
    "delete",
    function mockedDelete() {},
    () => withMockedProperty(
      /** @type {any} */ (Array.prototype),
      iteratorSymbol,
      /** @this {unknown[]} */
      function targetedIterator() {
        if (Reflect.apply(arrayIncludes, this, ["x-wdl-do-owner-task-id"])) {
          return {
            next: () => ({ done: true, value: undefined }),
            [iteratorSymbol]() { return this; },
          };
        }
        return Reflect.apply(arrayIterator, this, []);
      },
      () => ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send")
    )
  );

  assert.equal(await response.text(), "ok");
  assert.equal(response.headers.get("x-wdl-do-owner-task-id"), null);
  assert.equal(response.headers.get("x-wdl-do-owner-endpoint"), null);
  assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
});

test("DO requestSpec header budget uses captured array iteration", async () => {
  const iteratorSymbol = Symbol.iterator;
  const arrayIterator = Array.prototype[Symbol.iterator];
  const request = new Request("https://demo.workers.example/send", {
    headers: { "x-oversized": "a".repeat(MAX_DO_REQUEST_HEADER_BYTES + 1) },
  });

  await withMockedProperty(
    /** @type {any} */ (Array.prototype),
    iteratorSymbol,
    /** @this {unknown[]} */
    function targetedIterator() {
      const first = this[0];
      if (
        (Array.isArray(first) && first[0] === "x-oversized") ||
        first === "x-oversized"
      ) {
        return {
          next: () => ({ done: true, value: undefined }),
          [iteratorSymbol]() { return this; },
        };
      }
      return Reflect.apply(arrayIterator, this, []);
    },
    async () => {
      await assert.rejects(
        () => requestSpec(request, null),
        /fetch headers exceed 65536 bytes/
      );
    }
  );
});

test("DO requestSpec header budget uses captured TextEncoder.encode", async () => {
  const textEncode = TextEncoder.prototype.encode;
  let hostileEncodeCalls = 0;
  const request = new Request("https://demo.workers.example/send", {
    headers: { "x-oversized": "a".repeat(MAX_DO_REQUEST_HEADER_BYTES + 1) },
  });

  await withMockedProperty(
    TextEncoder.prototype,
    "encode",
    /** @this {TextEncoder} */
    function targetedEncode(value = "") {
      if (value.length > MAX_DO_REQUEST_HEADER_BYTES) {
        hostileEncodeCalls += 1;
        return new Uint8Array();
      }
      return Reflect.apply(textEncode, this, [value]);
    },
    async () => {
      await assert.rejects(
        () => requestSpec(request, null),
        /fetch headers exceed 65536 bytes/
      );
    }
  );
  assert.equal(hostileEncodeCalls, 0);
});

function chunkedBodyRequest() {
  return new Request("https://demo.workers.example/send", /** @type {RequestInit} */ (/** @type {unknown} */ ({
    method: "POST",
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(Uint8Array.of(1, 2));
        controller.enqueue(Uint8Array.of(3, 4));
        controller.close();
      },
    }),
    duplex: "half",
  })));
}

test("DO requestSpec body collection uses captured Array.push", async () => {
  const arrayPush = Array.prototype.push;
  let hostilePushCalls = 0;

  const { bodyBytes } = await withMockedProperty(
    Array.prototype,
    "push",
    /** @this {unknown[]} */
    function targetedPush(value) {
      if (value instanceof Uint8Array) {
        hostilePushCalls += 1;
        return this.length;
      }
      return Reflect.apply(arrayPush, this, [value]);
    },
    () => requestSpec(chunkedBodyRequest(), null)
  );

  assert.equal(hostilePushCalls, 0);
  assert.deepEqual(bodyBytes, Uint8Array.of(1, 2, 3, 4));
});

test("DO requestSpec body copying uses captured array iteration", async () => {
  const iteratorSymbol = Symbol.iterator;
  const arrayIterator = Array.prototype[Symbol.iterator];
  let hostileIteratorCalls = 0;

  const { bodyBytes } = await withMockedProperty(
    /** @type {any} */ (Array.prototype),
    iteratorSymbol,
    /** @this {unknown[]} */
    function targetedIterator() {
      if (this[0] instanceof Uint8Array) {
        hostileIteratorCalls += 1;
        return {
          next: () => ({ done: true, value: undefined }),
          [iteratorSymbol]() { return this; },
        };
      }
      return Reflect.apply(arrayIterator, this, []);
    },
    () => requestSpec(chunkedBodyRequest(), null)
  );

  assert.equal(hostileIteratorCalls, 0);
  assert.deepEqual(bodyBytes, Uint8Array.of(1, 2, 3, 4));
});

test("DO requestSpec body copying uses captured Uint8Array.set", async () => {
  const uint8ArraySet = Uint8Array.prototype.set;
  let hostileSetCalls = 0;

  const { bodyBytes } = await withMockedProperty(
    Uint8Array.prototype,
    "set",
    /** @this {Uint8Array} */
    function targetedSet(source, offset) {
      if (this.length === 4 && source instanceof Uint8Array) {
        hostileSetCalls += 1;
        return;
      }
      return Reflect.apply(uint8ArraySet, this, [source, offset]);
    },
    () => requestSpec(chunkedBodyRequest(), null)
  );

  assert.equal(hostileSetCalls, 0);
  assert.deepEqual(bodyBytes, Uint8Array.of(1, 2, 3, 4));
});

test("DO requestSpec body reading uses the captured Request body getter", async () => {
  let hostileBodyGetterCalls = 0;
  const request = chunkedBodyRequest();

  const { bodyBytes } = await withMockedPropertyDescriptor(
    Request.prototype,
    "body",
    {
      configurable: true,
      enumerable: true,
      get() {
        hostileBodyGetterCalls += 1;
        return null;
      },
    },
    () => requestSpec(request, null)
  );

  assert.equal(hostileBodyGetterCalls, 0);
  assert.deepEqual(bodyBytes, Uint8Array.of(1, 2, 3, 4));
});

test("DO requestSpec body reading uses captured stream reader methods", async () => {
  const getReader = ReadableStream.prototype.getReader;
  const read = ReadableStreamDefaultReader.prototype.read;
  const releaseLock = ReadableStreamDefaultReader.prototype.releaseLock;
  let hostileMethodCalls = 0;

  const { bodyBytes } = await withMockedProperty(
    ReadableStream.prototype,
    "getReader",
    /** @type {any} */ (/** @this {ReadableStream<Uint8Array>} @param {ReadableStreamGetReaderOptions | undefined} options */
    function targetedGetReader(options) {
      hostileMethodCalls += 1;
      return Reflect.apply(getReader, this, [options]);
    }),
    () => withMockedProperty(
      ReadableStreamDefaultReader.prototype,
      "read",
      /** @this {ReadableStreamDefaultReader<Uint8Array>} */
      function targetedRead() {
        hostileMethodCalls += 1;
        return Reflect.apply(read, this, []);
      },
      () => withMockedProperty(
        ReadableStreamDefaultReader.prototype,
        "releaseLock",
        /** @this {ReadableStreamDefaultReader<Uint8Array>} */
        function targetedReleaseLock() {
          hostileMethodCalls += 1;
          return Reflect.apply(releaseLock, this, []);
        },
        () => requestSpec(chunkedBodyRequest(), null)
      )
    )
  );

  assert.equal(hostileMethodCalls, 0);
  assert.deepEqual(bodyBytes, Uint8Array.of(1, 2, 3, 4));
});

test("DO invoke envelope uses captured serialization intrinsics", async () => {
  const jsonStringify = JSON.stringify;
  const dataViewSetUint32 = DataView.prototype.setUint32;
  const uint8ArraySet = Uint8Array.prototype.set;
  let hostileJsonCalls = 0;
  let hostileLengthCalls = 0;
  let hostileSetCalls = 0;

  const init = await withMockedProperty(
    JSON,
    "stringify",
    /** @this {typeof JSON} */
    function targetedStringify(value) {
      if (value && typeof value === "object" && "doStorageId" in value) {
        hostileJsonCalls += 1;
        return "{}";
      }
      return Reflect.apply(jsonStringify, this, [value]);
    },
    () => withMockedProperty(
      DataView.prototype,
      "setUint32",
      /** @this {DataView} */
      function targetedSetUint32(offset, value, littleEndian) {
        if (offset === 0) {
          hostileLengthCalls += 1;
          return;
        }
        return Reflect.apply(dataViewSetUint32, this, [offset, value, littleEndian]);
      },
      () => withMockedProperty(
        Uint8Array.prototype,
        "set",
        /** @this {Uint8Array} */
        function targetedSet(source, offset) {
          if (offset === 4) {
            hostileSetCalls += 1;
            return;
          }
          return Reflect.apply(uint8ArraySet, this, [source, offset]);
        },
        () => fetchInvokeInit({
          ns: "tenant",
          worker: "chat",
          version: "v1",
          doStorageId: "do_0123456789abcdef0123456789abcdef",
          className: "Room",
        }, "room-a", new Request("https://demo.workers.example/send"), null)
      )
    )
  );

  assert.equal(hostileJsonCalls, 0);
  assert.equal(hostileLengthCalls, 0);
  assert.equal(hostileSetCalls, 0);
  const { metadata } = decodeDoEnvelope(/** @type {Uint8Array} */ (init.body));
  assert.equal(/** @type {any} */ (metadata).ns, "tenant");
  assert.equal(/** @type {any} */ (metadata).objectName, "room-a");
});

test("DO invoke envelope ignores inherited object toJSON hooks", async () => {
  const init = await withMockedProperty(
    /** @type {any} */ (Object.prototype),
    "toJSON",
    function hostileToJSON() {
      return {
        ns: "attacker",
        worker: "attacker",
        version: "v9",
        doStorageId: "do_ffffffffffffffffffffffffffffffff",
        className: "Room",
        objectName: "other",
        request: { method: "GET", url: "https://evil.example/", headers: [] },
      };
    },
    () => fetchInvokeInit({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    }, "room-a", new Request("https://demo.workers.example/send"), null)
  );

  const { metadata } = decodeDoEnvelope(/** @type {Uint8Array} */ (init.body));
  assert.equal(/** @type {any} */ (metadata).ns, "tenant");
  assert.equal(/** @type {any} */ (metadata).objectName, "room-a");
});

test("DO invoke envelope ignores inherited array toJSON hooks", async () => {
  const init = await withMockedProperty(
    /** @type {any} */ (Array.prototype),
    "toJSON",
    function hostileToJSON() {
      return [];
    },
    () => fetchInvokeInit({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    }, "room-a", new Request("https://demo.workers.example/send", {
      headers: { "x-proof": "preserved" },
    }), null)
  );

  const { metadata } = decodeDoEnvelope(/** @type {Uint8Array} */ (init.body));
  assert.deepEqual(/** @type {any} */ (metadata).request.headers, [["x-proof", "preserved"]]);
});

test("DO invoke envelope uses captured typed-array getters", async () => {
  const typedArrayPrototype = /** @type {any} */ (Object.getPrototypeOf(Uint8Array.prototype));
  let hostileGetterCalls = 0;
  const request = new Request("https://demo.workers.example/send");

  const init = await withMockedPropertyDescriptor(
    typedArrayPrototype,
    "length",
    {
      configurable: true,
      get() {
        hostileGetterCalls += 1;
        return 0;
      },
    },
    () => withMockedPropertyDescriptor(
      typedArrayPrototype,
      "byteLength",
      {
        configurable: true,
        get() {
          hostileGetterCalls += 1;
          return 0;
        },
      },
      () => withMockedPropertyDescriptor(
        typedArrayPrototype,
        "buffer",
        {
          configurable: true,
          get() {
            hostileGetterCalls += 1;
            return new ArrayBuffer(0);
          },
        },
        () => fetchInvokeInit({
          ns: "tenant",
          worker: "chat",
          version: "v1",
          doStorageId: "do_0123456789abcdef0123456789abcdef",
          className: "Room",
        }, "room-a", request, null)
      )
    )
  );

  assert.ok(hostileGetterCalls > 0);
  const { metadata } = decodeDoEnvelope(/** @type {Uint8Array} */ (init.body));
  assert.equal(/** @type {any} */ (metadata).ns, "tenant");
});

test("DurableObjectNamespace direct backend retries owner claim races once", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "owner_claim_raced", message: "retry" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("owner_claim_raced"),
          })
        : new Response("ok"),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  const response = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send");

  assert.equal(await response.text(), "ok");
  assert.equal(calls.length, 2);
});

test("DurableObjectNamespace direct backend ignores hints attached to owner-race responses", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
            status: 503,
            headers: doOwnershipErrorHeaders(
              "stale_owner_generation",
              doOwnerHintResponse().headers
            ),
          })
        : new Response("ok"),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("owner-should-not-be-called") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-race-hint")).fetch("https://demo.workers.example/send");

  assert.equal(await response.text(), "ok");
  assert.equal(routerCalls.length, 2);
  assert.equal(ownerCalls.length, 0);
  assert.equal(headerValue(routerCalls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
  assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace direct backend retries owner lease budget errors once", async () => {
  for (const error of ["owner_lease_expired", "owner_lease_too_short"]) {
    /** @type {any[]} */
    const calls = [];
    const backend = {
      fetch: makeRecordingFetch(calls, {
        response: () => calls.length === 1
          ? Response.json({ error, message: "owner lease unavailable" }, {
              status: 503,
              headers: doOwnershipErrorHeaders(error),
            })
          : new Response("ok"),
      }),
    };
    const ns = new DurableObjectNamespace({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    }, { backend });

    const response = await ns.get(ns.idFromName(`room-${error}`)).fetch("https://demo.workers.example/send");

    assert.equal(await response.text(), "ok", error);
    assert.equal(calls.length, 2, error);
    assert.equal(calls[1].init.headers.get("x-wdl-do-accept-owner-hint"), null, error);
  }
});

test("DurableObjectNamespace direct backend follows owner hints on the same request", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: new Response("owner-ok", {
        headers: { "x-wdl-do-owner-endpoint": "do-runtime-a:8788" },
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-hint-fetch")).fetch("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  });

  assert.equal(await response.text(), "owner-ok");
  assert.equal(response.headers.get("x-wdl-do-owner-endpoint"), null);
  assert.equal(routerCalls.length, 1);
  assert.equal(headerValue(routerCalls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/invoke");
  assert.equal(ownerCalls[0].init.body, routerCalls[0].init.body);
});

test("DurableObjectNamespace direct backend accepts Kubernetes headless owner endpoints", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: doOwnerHintResponse({
        taskId: "do-runtime-0",
        endpoint: "do-runtime-0.do-runtime-headless:8788",
      }),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("owner-ok") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-headless")).fetch("https://demo.workers.example/send");

  assert.equal(await response.text(), "owner-ok");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-0.do-runtime-headless:8788/internal/do/invoke");
});

test("DurableObjectNamespace direct backend ignores tenant body owner hints", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: Response.json({
        error: "do_owner_hint",
        message: "tenant-controlled body must not be trusted",
        owner: {
          ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
          taskId: "redis-proxy-user",
          endpoint: "redis-proxy-user:7070/runtime/load?ignore=",
          generation: 3,
        },
      }, { status: 409 }),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("internal") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-malicious-body")).fetch("https://demo.workers.example/send");

  const body = await readJsonResponse(response, 409);
  assert.equal(body.message, "tenant-controlled body must not be trusted");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 0);
});

test("DurableObjectNamespace direct backend does not follow tenant 409 responses with owner metadata", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: Response.json({ error: "conflict", message: "tenant conflict" }, {
        status: 409,
        headers: {
          "x-wdl-do-owner-key": "do_0123456789abcdef0123456789abcdef:Room:shard0",
          "x-wdl-do-owner-task-id": "do-runtime-a",
          "x-wdl-do-owner-endpoint": "do-runtime-a:8788",
          "x-wdl-do-owner-generation": "3",
        },
      }),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("duplicate") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-tenant-conflict")).fetch("https://demo.workers.example/send");

  const body = await readJsonResponse(response, 409);
  assert.equal(body.message, "tenant conflict");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 0);
});

test("DurableObjectNamespace direct backend ignores owner hint headers with invalid endpoints", async () => {
  for (const endpoint of [
    "do-runtime-a:8788/../../runtime/load",
    "do-runtime-0.do-runtime-headless.evil.com:8788",
  ]) {
    /** @type {any[]} */
    const routerCalls = [];
    /** @type {any[]} */
    const ownerCalls = [];
    const backend = {
      fetch: makeRecordingFetch(routerCalls, {
        response: new Response(null, {
          status: 409,
          headers: {
            "x-wdl-do-owner-key": "do_0123456789abcdef0123456789abcdef:Room:shard0",
            "x-wdl-do-owner-task-id": "do-runtime-a",
            "x-wdl-do-owner-endpoint": endpoint,
            "x-wdl-do-owner-generation": "3",
          },
        }),
      }),
    };
    const ownerNetwork = {
      fetch: makeRecordingFetch(ownerCalls, { response: new Response("internal") }),
    };
    const ns = new DurableObjectNamespace({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    }, { backend, ownerNetwork });

    const response = await ns.get(ns.idFromName(`room-invalid-endpoint-${routerCalls.length}`)).fetch("https://demo.workers.example/send");

    assert.equal(response.status, 409);
    assert.equal(routerCalls.length, 1);
    assert.equal(ownerCalls.length, 0);
  }
});

test("DurableObjectNamespace direct backend accepts VPC-local IPv4 owner hint endpoints", async () => {
  /** @type {any[]} */
  const ownerCalls = [];
  const endpoints = ["10.0.42.17:8788", "100.64.30.52:8788"];
  const backend = {
    async fetch() {
      const endpoint = endpoints[ownerCalls.length];
      return doOwnerHintResponse({
        taskId: "arn:aws:ecs:us-east-1:123456789012:task/wdl-test/abc",
        endpoint,
      });
    },
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("owner-ok") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const first = await ns.get(ns.idFromName("room-private-ip")).fetch("https://demo.workers.example/send");
  const second = await ns.get(ns.idFromName("room-non-rfc1918-ip")).fetch("https://demo.workers.example/send");

  assert.equal(await first.text(), "owner-ok");
  assert.equal(await second.text(), "owner-ok");
  assert.equal(ownerCalls.length, 2);
  assert.equal(ownerCalls[0].url, "http://10.0.42.17:8788/internal/do/invoke");
  assert.equal(ownerCalls[1].url, "http://100.64.30.52:8788/internal/do/invoke");
});

test("DO owner hint parsing keeps the validated endpoint with patched String", async () => {
  const headers = new Headers(doOwnerHintHeaders({ endpoint: "do-runtime-a:8788" }));
  const nativeString = String;
  let hostileStringCalls = 0;

  const hint = await withMockedProperty(
    globalThis,
    "String",
    /** @type {StringConstructor} */ (function hostileString(value) {
      if (value === "do-runtime-a:8788") {
        hostileStringCalls += 1;
        return "10.0.0.5:8788";
      }
      return nativeString(value);
    }),
    () => ownerHintFromHeaders(headers)
  );

  assert.equal(hostileStringCalls, 0);
  assert.equal(hint?.endpoint, "do-runtime-a:8788");
});

test("DurableObjectNamespace direct backend rejects unsafe IPv4 owner hint endpoints", async () => {
  for (const endpoint of ["0.0.0.0:8788", "127.0.0.1:8788", "169.254.169.254:8788", "224.0.0.1:8788"]) {
    /** @type {any[]} */
    const ownerCalls = [];
    const backend = {
      async fetch() {
        return doOwnerHintResponse({ endpoint });
      },
    };
    const ownerNetwork = {
      fetch: makeRecordingFetch(ownerCalls, { response: new Response("internal") }),
    };
    const ns = new DurableObjectNamespace({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    }, { backend, ownerNetwork });

    const response = await ns.get(ns.idFromName(`room-${endpoint}`)).fetch("https://demo.workers.example/send");

    assert.equal(response.status, 409);
    assert.equal(ownerCalls.length, 0);
  }
});

test("DurableObjectNamespace RPC follows owner hints on the same request", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: Response.json({ ok: true, result: "owner-rpc" }, {
        headers: { "x-wdl-do-owner-endpoint": "do-runtime-a:8788" },
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const result = await Reflect.get(ns.get(ns.idFromName("room-hint-rpc")), "addMessage")("hello");

  assert.equal(result, "owner-rpc");
  assert.equal(routerCalls.length, 1);
  assert.equal(headerValue(routerCalls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/invoke");
  assert.equal(ownerCalls[0].init.body, routerCalls[0].init.body);
});

test("DurableObjectNamespace direct backend reuses learned owner hints", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: () => new Response(`owner-${ownerCalls.length}`) }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const id = ns.idFromName("room-hint-cache");
  assert.equal(await ns.get(id).fetch("https://demo.workers.example/one").then((r) => r.text()), "owner-1");
  assert.equal(await ns.get(id).fetch("https://demo.workers.example/two").then((r) => r.text()), "owner-2");

  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 2);
  assert.equal(ownerCalls[1].url, "http://do-runtime-a:8788/internal/do/invoke");
});

test("DurableObjectNamespace RPC reuses learned owner hints", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: () => Response.json({ ok: true, result: `owner-rpc-${ownerCalls.length}` }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });
  const stub = ns.get(ns.idFromName("room-hint-rpc-cache"));

  assert.equal(await Reflect.get(stub, "addMessage")("one"), "owner-rpc-1");
  assert.equal(await Reflect.get(stub, "addMessage")("two"), "owner-rpc-2");

  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 2);
  assert.equal(ownerCalls[1].url, "http://do-runtime-a:8788/internal/do/invoke");
});

test("DurableObjectNamespace owner hint cache evicts oldest entries at the configured cap", async () => {
  setDoOwnerHintMaxEntriesForTest(1);
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: (_url, init) => {
        const { metadata: body } = decodeDoEnvelope(/** @type {ArrayBuffer | Uint8Array} */ (init.body));
        return doOwnerHintResponse({
          ownerKey: `do_0123456789abcdef0123456789abcdef:Room:shard-${body.objectName}`,
          taskId: `do-runtime-${body.objectName}`,
          endpoint: `do-runtime-${body.objectName}:8788`,
        });
      },
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("owner-ok") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/one");
  await ns.get(ns.idFromName("room-b")).fetch("https://demo.workers.example/two");
  await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/three");

  assert.equal(routerCalls.length, 3);
  assert.equal(ownerCalls.length, 3);
  assert.equal(ownerCalls[0].url, "http://do-runtime-room-a:8788/internal/do/invoke");
  assert.equal(ownerCalls[1].url, "http://do-runtime-room-b:8788/internal/do/invoke");
  assert.equal(ownerCalls[2].url, "http://do-runtime-room-a:8788/internal/do/invoke");
});

test("DurableObjectNamespace direct backend rejects oversized request bodies before invoke transport", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
      method: "POST",
      body: new Uint8Array(1024 * 1024 + 1),
    }),
    /Durable Object fetch body exceeds/
  );
});

test("DurableObjectNamespace direct backend rejects oversized declared request bodies before reading", async () => {
  const backend = {
    async fetch() {
      throw new Error("backend should not be called");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend });

  await assert.rejects(
    ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/send", {
      method: "POST",
      headers: { "content-length": String(1024 * 1024 + 1) },
      body: "x",
    }),
    /Durable Object fetch body exceeds/
  );
});

test("DO requestSpec rejects streaming bodies as soon as they cross the cap", async () => {
  let pulls = 0;
  const body = new ReadableStream({
    pull(controller) {
      pulls += 1;
      if (pulls <= 2) {
        controller.enqueue(new Uint8Array(600 * 1024));
      } else {
        return new Promise(() => {});
      }
    },
  });
  const request = new Request("https://demo.workers.example/send", /** @type {RequestInit} */ ({
    method: "POST",
    body,
    duplex: "half",
  }));
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;

  try {
    await assert.rejects(
      Promise.race([
        requestSpec(request, "rid-stream"),
        new Promise((_, reject) => {
          timeout = setTimeout(() => reject(new Error("requestSpec kept reading the oversized stream")), 1000);
        }),
      ]),
      /Durable Object fetch body exceeds/
    );
  } finally {
    if (timeout) clearTimeout(timeout);
  }
});

test("DO requestSpec rejects oversized streams without waiting for cancel", async () => {
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(1024 * 1024 + 1));
    },
    cancel() {
      return new Promise(() => {});
    },
  });
  const request = new Request("https://demo.workers.example/send", /** @type {RequestInit} */ ({
    method: "POST",
    body,
    duplex: "half",
  }));
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  let hostileCatchCalls = 0;

  try {
    await withMockedProperty(
      Promise.prototype,
      "catch",
      function hostileCatch() {
        hostileCatchCalls += 1;
        throw new Error("tenant Promise.catch must not run");
      },
      () => assert.rejects(
        Promise.race([
          requestSpec(request, "rid-cancel"),
          new Promise((_, reject) => {
            timeout = setTimeout(() => reject(new Error("requestSpec waited for stream cancel")), 1000);
          }),
        ]),
        /Durable Object fetch body exceeds/
      )
    );
  } finally {
    if (timeout) clearTimeout(timeout);
  }
  assert.equal(hostileCatchCalls, 0);
});

test("DurableObjectNamespace facade uses direct upgrade path for websockets", async () => {
  /** @type {any[]} */
  const calls = [];
  const backend = {
    fetch: makeRecordingFetch(calls, { response: new Response("upgrade") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, requestId: "rid-2" });

  const response = await ns.get(ns.idFromName("room-a")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(await response.text(), "upgrade");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/connect");
  assert.equal(calls[0].init.method, "GET");
  assert.equal(calls[0].init.headers.get("x-wdl-do-ns"), "tenant");
  assert.equal(calls[0].init.headers.get("x-wdl-do-storage-id"), "do_0123456789abcdef0123456789abcdef");
  assert.equal(calls[0].init.headers.get("x-wdl-do-class-name"), "Room");
  assert.equal(calls[0].init.headers.get("x-wdl-do-object-name"), "room-a");
  assert.equal(calls[0].init.headers.get("x-wdl-do-request-url"), "https://demo.workers.example/ws");
  assert.equal(calls[0].init.headers.get("x-request-id"), "rid-2");
});

test("DurableObjectNamespace websocket path follows owner hints before upgrade", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("upgrade") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork, requestId: "rid-3" });

  const response = await ns.get(ns.idFromName("room-hint-ws")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(await response.text(), "upgrade");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/connect");
  assert.equal(ownerCalls[0].init.headers.get("x-wdl-do-accept-owner-hint"), "1");
  assert.equal(ownerCalls[0].init.headers.get("x-request-id"), "rid-3");
});

test("DurableObjectNamespace websocket path follows bodyless owner hints from headers", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: new Response(null, {
        status: 409,
        headers: doOwnerHintHeaders(),
      }),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("upgrade") }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork, requestId: "rid-headers" });

  const response = await ns.get(ns.idFromName("room-hint-ws-headers")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(await response.text(), "upgrade");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/connect");
  assert.equal(ownerCalls[0].init.headers.get("x-request-id"), "rid-headers");
});

test("DurableObjectNamespace websocket path preserves owner tenant errors", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: new Response("maintenance", {
        status: 503,
        headers: doOwnerHintHeaders(),
      }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-hint-ws-tenant-error")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  assert.equal(response.status, 503);
  assert.equal(await response.text(), "maintenance");
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
  assert.equal(response.headers.get("x-wdl-do-owner-endpoint"), null);
});

test("DurableObjectNamespace websocket path does not fall back to router after owner hint failure", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    async fetch() {
      throw new Error("owner unavailable");
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-hint-ws-fail")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "owner_unavailable");
  assert.equal(routerCalls.length, 1);
});

test("DurableObjectNamespace POST fetch does not replay through router after direct owner failure", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? doOwnerHintResponse()
        : new Response("router-ok"),
    }),
  };
  const ownerNetwork = {
    async fetch() {
      return new Response("upstream request timeout", { status: 504 });
    },
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-hint-fallback")).fetch("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  });

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "owner_unavailable");
  assert.equal(routerCalls.length, 1);
  assert.equal(headerValue(routerCalls[0].init.headers, "x-wdl-do-accept-owner-hint"), "1");
});

test("DurableObjectNamespace POST drops stale cached owner hints without replay after endpoint timeout", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? doOwnerHintResponse()
        : new Response("router-ok"),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: () => ownerCalls.length === 1
        ? new Response("owner-ok", {
            headers: doOwnerHintHeaders(),
          })
        : new Response("upstream request timeout", { status: 504 }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });
  const id = ns.idFromName("room-stale-cached-owner");

  assert.equal(await ns.get(id).fetch("https://demo.workers.example/one", { method: "POST", body: "first" }).then((r) => r.text()), "owner-ok");
  const body = await readJsonResponse(await ns.get(id).fetch("https://demo.workers.example/two", { method: "POST", body: "second" }), 503);
  assert.equal(body.error, "owner_unavailable");

  assert.equal(ownerCalls.length, 2);
  assert.equal(routerCalls.length, 1);
});

test("DurableObjectNamespace replays safe GET after stale cached owner endpoint timeout", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const endpoint = "do-runtime-safe-get:8788";
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? doOwnerHintResponse({ endpoint, generation: 11 })
        : new Response("router-ok"),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: () => ownerCalls.length === 1
        ? new Response("owner-ok", { headers: doOwnerHintHeaders({ endpoint, generation: 11 }) })
        : new Response("upstream request timeout", { status: 504 }),
    }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });
  const id = ns.idFromName("room-stale-safe-get");

  assert.equal(await ns.get(id).fetch("https://demo.workers.example/one").then((r) => r.text()), "owner-ok");
  assert.equal(await ns.get(id).fetch("https://demo.workers.example/two").then((r) => r.text()), "router-ok");

  assert.equal(ownerCalls.length, 2);
  assert.equal(routerCalls.length, 2);
  assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace drops stale cached owner hints after owner lease budget errors", async () => {
  for (const error of ["owner_lease_expired", "owner_lease_too_short"]) {
    /** @type {any[]} */
    const routerCalls = [];
    /** @type {any[]} */
    const ownerCalls = [];
    const backend = {
      fetch: makeRecordingFetch(routerCalls, {
        response: () => routerCalls.length === 1
          ? doOwnerHintResponse()
          : new Response("router-ok"),
      }),
    };
    const ownerNetwork = {
      fetch: makeRecordingFetch(ownerCalls, {
        response: () => ownerCalls.length === 1
          ? new Response("owner-ok", { headers: doOwnerHintHeaders() })
          : Response.json({ error, message: "owner lease unavailable" }, {
              status: 503,
              headers: doOwnershipErrorHeaders(error),
            }),
      }),
    };
    const ns = new DurableObjectNamespace({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      binding: "ROOM",
      className: "Room",
    }, { backend, ownerNetwork });
    const id = ns.idFromName(`room-stale-${error}`);

    assert.equal(await ns.get(id).fetch("https://demo.workers.example/one").then((r) => r.text()), "owner-ok", error);
    assert.equal(await ns.get(id).fetch("https://demo.workers.example/two").then((r) => r.text()), "router-ok", error);

    assert.equal(ownerCalls.length, 2, error);
    assert.equal(routerCalls.length, 2, error);
    assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), "1", error);
  }
});

test("DurableObjectNamespace facade rejects foreign ids", () => {
  const ns = new DurableObjectNamespace({ fetchObject() {} });
  assert.throws(() => ns.idFromName(""), /requires a non-empty string/);
  assert.throws(() => ns.get({ name: "room-a" }), /requires an id returned by this namespace/);
});
