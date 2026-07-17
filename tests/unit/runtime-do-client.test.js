import assert from "node:assert/strict";
import { test } from "node:test";

import {
  DurableObjectNamespace,
  clearDoOwnerHintsForTest,
  setDoOwnerHintMaxEntriesForTest,
} from "../../runtime/do-client.js";
import {
  MAX_DO_REQUEST_HEADER_BYTES,
  dispatchDoInvokeWithHintCache,
  fetchInvokeInit,
  isWebSocketUpgrade,
  ownerHintFromHeaders,
  replayOwnerUnavailableForFetch,
  requestSpec,
  rpcInvokeBody,
} from "../../runtime/_wdl-do-transport.js";
import { decodeDoEnvelope } from "../helpers/do-envelope.js";
import { loadDoProtocol } from "../helpers/load-do-protocol.js";
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

const { normalizeDoInvokeRequest } = await loadDoProtocol();

test.afterEach(() => {
  clearDoOwnerHintsForTest();
});

/** @param {HeadersInit | undefined} headers @param {string} name */
function headerValue(headers, name) {
  return new Headers(headers).get(name);
}

/**
 * @template T
 * @param {Promise<T>} promise
 * @param {string} message
 */
async function withTestTimeout(promise, message) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), 1000);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/** @param {UnderlyingSourceCancelCallback} onCancel @param {ResponseInit} [init] */
function cancellableResponse(onCancel, init = {}) {
  return new Response(new ReadableStream({
    pull(controller) {
      controller.error(new Error("discarded response body was read"));
    },
    cancel: onCancel,
  }, { highWaterMark: 0 }), init);
}

/**
 * @param {UnderlyingSourceCancelCallback} onCancel
 * @param {Parameters<typeof doOwnerHintHeaders>[0]} [options]
 */
function cancellableDoOwnerHintResponse(onCancel, options = {}) {
  return cancellableResponse(onCancel, {
    status: 409,
    headers: doOwnerHintHeaders(options),
  });
}

test("DurableObjectNamespace fetch uses its private request-id provider", async () => {
  /** @type {any[]} */
  const calls = [];
  const ns = new DurableObjectNamespace({
    async fetchObject(/** @type {string} */ objectName, /** @type {Request} */ request, /** @type {string} */ requestId) {
      calls.push({ objectName, request, requestId });
      return new Response("ok", { status: 201 });
    },
  }, { requestIdProvider: () => "rid-1" });
  Reflect.set(ns, "requestId", () => "tenant-rid");

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

test("DurableObjectNamespace classifies metadata with a captured Object.hasOwn", async () => {
  await withMockedProperty(Object, "hasOwn", () => {
    throw new Error("tenant Object.hasOwn was called");
  }, async () => {
    const namespace = new DurableObjectNamespace({
      ns: "tenant",
      worker: "worker",
      version: "v1",
      doStorageId: "storage",
      className: "Room",
      hostProxy: {
        async fetchObject() {
          return new Response("ok");
        },
      },
    });
    const response = await namespace.get(namespace.idFromName("room")).fetch("https://example.test/");
    assert.equal(await response.text(), "ok");
  });
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
    headers: { "x-request-id": "tenant-forged" },
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
  const forwardedRequest = /** @type {{ headers: HeadersInit }} */ (body.request);
  assert.equal(new Headers(forwardedRequest.headers).get("x-request-id"), "rid-1");
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

test("DurableObjectNamespace RPC uses its private request-id provider", async () => {
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
  Reflect.set(ns, "requestId", () => "tenant-rid");

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
  const sparse = new Array(1);
  const sparsePrototype = Object.create(Array.prototype);
  let inheritedSlotReads = 0;
  Object.defineProperty(sparsePrototype, "0", {
    get() {
      inheritedSlotReads += 1;
      return "inherited";
    },
  });
  Object.setPrototypeOf(sparse, sparsePrototype);

  await assert.rejects(Reflect.get(stub, "save")(new Map([["key", "value"]])), /plain JSON object/);
  await assert.rejects(Reflect.get(stub, "save")(new Headers()), /plain JSON object/);
  await assert.rejects(Reflect.get(stub, "save")({ fn() {} }), /rpc\.args\[0\]\.fn must be JSON data/);
  await assert.rejects(Reflect.get(stub, "save")([undefined]), /rpc\.args\[0\]\[0\] must be JSON data/);
  await assert.rejects(Reflect.get(stub, "save")(sparse), /rpc\.args\[0\] must not be sparse/);
  assert.equal(inheritedSlotReads, 0);
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

test("DO RPC validation uses captured JSON intrinsics", async () => {
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
  await withMockedProperty(Object, "hasOwn", () => true, () => {
    assert.throws(
      () => rpcInvokeBody(props, "room-a", "save", [new Array(1)]),
      (error) => error instanceof TypeError && error.message === "rpc.args[0] must not be sparse"
    );
  });
});

test("do-runtime owns the DO RPC method byte limit", () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };
  for (const { method, valid } of [
    { method: "save", valid: true },
    { method: "m".repeat(256), valid: true },
    { method: "m".repeat(257), valid: false },
  ]) {
    const envelope = rpcInvokeBody(props, "room-a", method, []);
    if (!valid) {
      assert.throws(
        () => normalizeDoInvokeRequest(decodeDoEnvelope(envelope).metadata),
        /rpc\.method is too large/
      );
      continue;
    }
    const invoke = normalizeDoInvokeRequest(decodeDoEnvelope(envelope).metadata);
    assert.equal("rpc" in invoke ? invoke.rpc.method : null, method);
  }
});

test("DO RPC snapshots tenant arguments once before sizing and encoding", () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };
  let reads = 0;
  const argument = {};
  Object.defineProperty(argument, "payload", {
    enumerable: true,
    get() {
      reads += 1;
      return reads === 1 ? "stable" : "x".repeat(1_200_000);
    },
  });

  const envelope = rpcInvokeBody(props, "room-a", "save", [argument]);
  const { metadata } = decodeDoEnvelope(envelope);
  assert.equal(reads, 1);
  assert.equal(/** @type {any} */ (metadata).rpc.args[0].payload, "stable");
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
  let cancellations = 0;
  const backend = {
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? cancellableResponse(() => {
            cancellations += 1;
          }, {
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
  assert.equal(cancellations, 1);
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

test("DO request method and upgrade decisions use captured string normalization", async () => {
  const post = new Request("https://example.com/objects", {
    method: "POST",
    body: "payload",
  });
  const websocket = new Request("https://example.com/socket", {
    headers: { Upgrade: "WebSocket" },
  });
  const ordinary = new Request("https://example.com/fetch");
  const originalToLowerCase = String.prototype.toLowerCase;
  const originalToUpperCase = String.prototype.toUpperCase;
  /** @type {{ method: string, body: Uint8Array | null, replay: boolean, websocket: boolean, ordinary: boolean } | undefined} */
  let observed;

  await withMockedProperty(
    String.prototype,
    "toUpperCase",
    /** @this {string} */ function hostileToUpperCase() {
      const normalized = Reflect.apply(originalToUpperCase, this, []);
      return normalized === "POST" ? "GET" : normalized;
    },
    () => withMockedProperty(
      String.prototype,
      "toLowerCase",
      /** @this {string} */ function hostileToLowerCase() {
        const normalized = Reflect.apply(originalToLowerCase, this, []);
        return normalized === "" ? "websocket" : normalized;
      },
      async () => {
        const { spec, bodyBytes } = await requestSpec(post, null);
        observed = {
          method: spec.method,
          body: bodyBytes,
          replay: replayOwnerUnavailableForFetch(post),
          websocket: isWebSocketUpgrade(websocket),
          ordinary: isWebSocketUpgrade(ordinary),
        };
      },
    ),
  );

  assert.equal(observed?.method, "POST");
  assert.equal(new TextDecoder().decode(observed?.body ?? undefined), "payload");
  assert.equal(observed?.replay, false);
  assert.equal(observed?.websocket, true);
  assert.equal(observed?.ordinary, false);
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

test("DO owner-race router retry failures do not trigger another replay", async (t) => {
  const ownerMetadata = new Headers(doOwnerHintHeaders());
  ownerMetadata.delete("x-wdl-do-owner-hint");
  for (const { name, code, headers } of [
    { name: "fresh owner response without metadata", code: "owner_claim_raced", headers: undefined },
    { name: "fresh owner renew race with metadata", code: "owner_renew_raced", headers: ownerMetadata },
  ]) {
    await t.test(name, async () => {
      let routerCalls = 0;
      let ownerCalls = 0;

      await assert.rejects(
        dispatchDoInvokeWithHintCache({
          routerFetch: async () => {
            routerCalls += 1;
            if (routerCalls === 1) return doOwnerHintResponse();
            if (routerCalls === 2) throw new Error("owner-race router retry failed");
            return new Response("unexpected replay");
          },
          routerUrl: "http://do-runtime/internal/do/invoke",
          ownerFetch: async () => {
            ownerCalls += 1;
            return Response.json({ error: code, message: "retry" }, {
              status: 503,
              headers: doOwnershipErrorHeaders(code, headers),
            });
          },
          ownerPath: "/internal/do/invoke",
          init: { method: "POST" },
          cache: new Map(),
          hintKey: "room-a",
          replayOwnerUnavailable: false,
        }),
        /owner-race router retry failed/
      );
      assert.equal(ownerCalls, 1);
      assert.equal(routerCalls, 2);
    });
  }

  await t.test("cached endpoint fallback", async () => {
    const hint = ownerHintFromHeaders(doOwnerHintResponse().headers);
    assert.ok(hint);
    const cache = new Map();
    cache.set("room-a", hint);
    let routerCalls = 0;
    let ownerCalls = 0;

    await assert.rejects(
      dispatchDoInvokeWithHintCache({
        routerFetch: async () => {
          routerCalls += 1;
          if (routerCalls === 1) {
            return Response.json({ error: "owner_claim_raced", message: "retry" }, {
              status: 503,
              headers: doOwnershipErrorHeaders("owner_claim_raced"),
            });
          }
          if (routerCalls === 2) throw new Error("owner-race router retry failed");
          return new Response("unexpected replay");
        },
        routerUrl: "http://do-runtime/internal/do/invoke",
        ownerFetch: async () => {
          ownerCalls += 1;
          return new Response("owner timeout", { status: 504 });
        },
        ownerPath: "/internal/do/invoke",
        init: { method: "GET" },
        cache,
        hintKey: "room-a",
        replayOwnerUnavailable: true,
      }),
      /owner-race router retry failed/
    );
    assert.equal(ownerCalls, 1);
    assert.equal(routerCalls, 2);
  });
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

test("DurableObjectNamespace replays a safe GET through the router after a second owner hint", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  /** @type {string[]} */
  const cancellations = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? cancellableDoOwnerHintResponse(() => {
            cancellations.push("router");
          })
        : new Response("router-ok"),
    }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, {
      response: () => cancellableDoOwnerHintResponse(
        () => {
          cancellations.push("direct");
        },
        {
          taskId: "do-runtime-b",
          endpoint: "do-runtime-b:8788",
          generation: 4,
        },
      ),
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
  const streamCancel = ReadableStream.prototype.cancel;
  const promiseThen = Promise.prototype.then;
  let hostileCancelCalls = 0;
  let hostileThenCalls = 0;
  /** @type {ReadableStream | null} */
  let leakedStream = null;

  const response = await withMockedProperty(
    ReadableStream.prototype,
    "cancel",
    /** @this {ReadableStream} @param {unknown} reason */
    function hostileCancel(reason) {
      hostileCancelCalls += 1;
      leakedStream = this;
      return Reflect.apply(streamCancel, this, [reason]);
    },
    () => withMockedProperty(
      Promise.prototype,
      "then",
      /** @this {Promise<unknown>} @param {any} onFulfilled @param {any} onRejected */
      function hostileThen(onFulfilled, onRejected) {
        hostileThenCalls += 1;
        return Reflect.apply(promiseThen, this, [onFulfilled, onRejected]);
      },
      () => ns.get(ns.idFromName("room-consecutive-hint"))
        .fetch("https://demo.workers.example/send")
    )
  );
  assert.equal(await response.text(), "router-ok");
  assert.equal(response.headers.get("x-wdl-do-owner-key"), null);
  assert.equal(routerCalls.length, 2);
  assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null);
  assert.equal(ownerCalls.length, 1);
  assert.deepEqual(cancellations, ["router", "direct"]);
  assert.equal(hostileCancelCalls, 0);
  assert.equal(hostileThenCalls, 0);
  assert.equal(leakedStream, null);
});

test("DurableObjectNamespace replays a POST after a trusted second owner hint", async () => {
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
    fetch: makeRecordingFetch(ownerCalls, { response: doOwnerHintResponse({
      taskId: "do-runtime-b",
      endpoint: "do-runtime-b:8788",
      generation: 4,
    }) }),
  };
  const ns = new DurableObjectNamespace({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  }, { backend, ownerNetwork });

  const response = await ns.get(ns.idFromName("room-consecutive-post"))
    .fetch("https://demo.workers.example/send", { method: "POST", body: "hello" });

  assert.equal(await response.text(), "router-ok");
  assert.equal(response.headers.get("x-wdl-do-owner-key"), null);
  assert.equal(routerCalls.length, 2);
  assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null);
  assert.equal(ownerCalls.length, 1);
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

test("DO owner hint generation parsing requires a positive safe integer with captured intrinsics", async () => {
  const headers = new Headers(doOwnerHintHeaders({ endpoint: "do-runtime-a:8788" }));
  await withMockedProperty(Number, "isSafeInteger", () => false, () => {
    assert.equal(ownerHintFromHeaders(headers)?.generation, 3);
  });
  for (const generation of ["0", "not-an-integer", "9007199254740992"]) {
    headers.set("x-wdl-do-owner-generation", generation);
    await withMockedProperty(Number, "isSafeInteger", () => true, () => {
      assert.equal(ownerHintFromHeaders(headers), null, generation);
    });
  }
});

test("DurableObjectNamespace direct backend rejects unsafe IPv4 owner hint endpoints", async () => {
  for (const endpoint of ["0.0.0.0:8788", "8.8.8.8:8788", "127.0.0.1:8788", "169.254.169.254:8788", "224.0.0.1:8788"]) {
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

test("DurableObjectNamespace cancels a cached-owner response before router rediscovery", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  let cancellations = 0;
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
        : cancellableDoOwnerHintResponse(() => {
            cancellations += 1;
          }, {
            taskId: "do-runtime-b",
            endpoint: "do-runtime-b:8788",
            generation: 4,
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
  const id = ns.idFromName("room-cached-hint");

  assert.equal(await ns.get(id).fetch("https://demo.workers.example/one").then((r) => r.text()), "owner-ok");
  assert.equal(await ns.get(id).fetch("https://demo.workers.example/two").then((r) => r.text()), "router-ok");

  assert.equal(cancellations, 1);
  assert.equal(routerCalls.length, 2);
  assert.equal(ownerCalls.length, 2);
});

function hostileOwnerHintCacheFixture() {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  const backend = {
    fetch: makeRecordingFetch(routerCalls, { response: doOwnerHintResponse() }),
  };
  const ownerNetwork = {
    fetch: makeRecordingFetch(ownerCalls, { response: new Response("owner-ok") }),
  };
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    binding: "ROOM",
    className: "Room",
  };
  return {
    ns: new DurableObjectNamespace(props, { backend, ownerNetwork }),
    ownerCalls,
    props,
    routerCalls,
  };
}

test("DurableObjectNamespace owner hint cache ignores tenant-patched Map#get", async () => {
  const { ns, ownerCalls, props, routerCalls } = hostileOwnerHintCacheFixture();
  const objectName = "room-hostile-map-get";
  const cacheKey = `${props.doStorageId}:${props.className}:${objectName}`;
  const originalMapGet = Map.prototype.get;
  let hostileGetCalls = 0;

  const response = await withMockedProperty(
    Map.prototype,
    "get",
    /** @this {Map<unknown, unknown>} */
    function hostileMapGet(key) {
      if (key === cacheKey) {
        hostileGetCalls += 1;
        return {
          ownerKey: "forged-owner",
          taskId: "forged-task",
          endpoint: "10.0.0.5:8788",
          generation: 1,
        };
      }
      return Reflect.apply(originalMapGet, this, [key]);
    },
    () => ns.get(ns.idFromName(objectName)).fetch("https://demo.workers.example/send")
  );

  assert.equal(await response.text(), "owner-ok");
  assert.equal(hostileGetCalls, 0);
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
  assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/invoke");
});

test("DurableObjectNamespace owner hint cache hides hints from tenant-patched Map methods", async () => {
  const { ns, ownerCalls, props, routerCalls } = hostileOwnerHintCacheFixture();
  const objectName = "room-hostile-map-set";
  const cacheKey = `${props.doStorageId}:${props.className}:${objectName}`;
  const originalMapDelete = Map.prototype.delete;
  const originalMapSet = Map.prototype.set;
  let hostileDeleteCalls = 0;
  let leakedHint = null;

  const response = await withMockedProperty(
    Map.prototype,
    "delete",
    /** @this {Map<unknown, unknown>} */
    function hostileMapDelete(key) {
      if (key === cacheKey) hostileDeleteCalls += 1;
      return Reflect.apply(originalMapDelete, this, [key]);
    },
    () => withMockedProperty(
      Map.prototype,
      "set",
      /** @this {Map<unknown, unknown>} */
      function hostileMapSet(key, value) {
        if (key === cacheKey) leakedHint = value;
        return Reflect.apply(originalMapSet, this, [key, value]);
      },
      () => ns.get(ns.idFromName(objectName)).fetch("https://demo.workers.example/send")
    )
  );

  assert.equal(await response.text(), "owner-ok");
  assert.equal(hostileDeleteCalls, 0);
  assert.equal(leakedHint, null);
  assert.equal(routerCalls.length, 1);
  assert.equal(ownerCalls.length, 1);
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

test("DO request body bounds use captured Number intrinsics", async () => {
  const request = new Request("https://demo.workers.example/send", {
    method: "POST",
    headers: { "content-length": String(1024 * 1024 + 1) },
    body: "x",
  });
  await withMockedProperty(globalThis, "Number", /** @type {NumberConstructor} */ (() => 0), async () => {
    await assert.rejects(fetchInvokeInit({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    }, "room-a", request, null), /Durable Object fetch body exceeds/);
  });
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
  await assert.rejects(
    withTestTimeout(
      requestSpec(request, "rid-stream"),
      "requestSpec kept reading the oversized stream"
    ),
    /Durable Object fetch body exceeds/
  );
});

test("DO requestSpec rejects oversized streams without waiting for cancel", async () => {
  let cancellations = 0;
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(1024 * 1024 + 1));
    },
    cancel() {
      cancellations += 1;
      return new Promise(() => {});
    },
  });
  const request = new Request("https://demo.workers.example/send", /** @type {RequestInit} */ ({
    method: "POST",
    body,
    duplex: "half",
  }));
  const readerCancel = ReadableStreamDefaultReader.prototype.cancel;
  const promiseCatch = Promise.prototype.catch;
  let hostileCancelCalls = 0;
  let hostileCatchCalls = 0;

  await withMockedProperty(
    ReadableStreamDefaultReader.prototype,
    "cancel",
    /** @this {ReadableStreamDefaultReader} @param {unknown} reason */
    function hostileCancel(reason) {
      hostileCancelCalls += 1;
      return Reflect.apply(readerCancel, this, [reason]);
    },
    () => withMockedProperty(
      Promise.prototype,
      "catch",
      /** @this {Promise<unknown>} @param {any} onRejected */
      function hostileCatch(onRejected) {
        hostileCatchCalls += 1;
        return Reflect.apply(promiseCatch, this, [onRejected]);
      },
      () => assert.rejects(
        withTestTimeout(
          requestSpec(request, "rid-cancel"),
          "requestSpec waited for stream cancel"
        ),
        /Durable Object fetch body exceeds/
      )
    )
  );
  assert.equal(cancellations, 1);
  assert.equal(hostileCancelCalls, 0);
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
      "x-request-id": "tenant-forged",
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

  let liveResponseJsonCalls = 0;
  const response = await withMockedProperty(Response, "json", () => {
    liveResponseJsonCalls += 1;
    return new Response("forged", { status: 200 });
  }, () => ns.get(ns.idFromName("room-hint-ws-fail")).fetch("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  }));

  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "owner_unavailable");
  assert.equal(routerCalls.length, 1);
  assert.equal(liveResponseJsonCalls, 0);
});

test("DurableObjectNamespace POST fetch does not replay through router after direct owner failure", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  let cancellations = 0;
  const backend = {
    fetch: makeRecordingFetch(routerCalls, {
      response: () => routerCalls.length === 1
        ? doOwnerHintResponse()
        : new Response("router-ok"),
    }),
  };
  const ownerNetwork = {
    async fetch() {
      return cancellableResponse(() => {
        cancellations += 1;
        return Promise.reject(new Error("cancel failed"));
      }, { status: 504 });
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
  assert.equal(cancellations, 1);
});

test("DurableObjectNamespace POST drops stale cached owner hints without replay after endpoint timeout", async () => {
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {any[]} */
  const ownerCalls = [];
  let cancellations = 0;
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
        : cancellableResponse(() => {
            cancellations += 1;
            return new Promise(() => {});
          }, { status: 504 }),
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
  const response = await withTestTimeout(
    ns.get(id).fetch("https://demo.workers.example/two", { method: "POST", body: "second" }),
    "owner dispatch waited for response cancellation"
  );
  const body = await readJsonResponse(response, 503);
  assert.equal(body.error, "owner_unavailable");

  assert.equal(ownerCalls.length, 2);
  assert.equal(routerCalls.length, 1);
  assert.equal(cancellations, 1);
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
        : routerCalls.length === 2
          ? Response.json({ error: "owner_claim_raced", message: "retry" }, {
              status: 503,
              headers: doOwnershipErrorHeaders("owner_claim_raced"),
            })
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
  assert.equal(routerCalls.length, 3);
  assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null);
  assert.equal(headerValue(routerCalls[2].init.headers, "x-wdl-do-accept-owner-hint"), null);
});

test("DurableObjectNamespace drops stale cached owner hints after owner lease budget errors", async () => {
  for (const error of ["owner_lease_expired", "owner_lease_too_short"]) {
    /** @type {any[]} */
    const routerCalls = [];
    /** @type {any[]} */
    const ownerCalls = [];
    let cancellations = 0;
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
          : cancellableResponse(() => {
              cancellations += 1;
            }, {
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
    assert.equal(headerValue(routerCalls[1].init.headers, "x-wdl-do-accept-owner-hint"), null, error);
    assert.equal(cancellations, 1, error);
  }
});

test("DurableObjectNamespace facade rejects foreign ids", async () => {
  const ns = new DurableObjectNamespace({ fetchObject() {} });
  assert.throws(() => ns.idFromName(""), /requires a non-empty string/);
  for (const value of ["\ud800", "\udc00"]) {
    assert.throws(() => ns.idFromName(value), /requires well-formed Unicode/);
    assert.throws(() => ns.idFromString(value), /requires well-formed Unicode/);
  }
  await withMockedProperty(String.prototype, "isWellFormed", () => true, () => {
    assert.throws(() => ns.idFromName("\ud800"), /requires well-formed Unicode/);
  });
  assert.throws(() => ns.get({ name: "room-a" }), /requires an id returned by this namespace/);
});
