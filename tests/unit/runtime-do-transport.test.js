import assert from "node:assert/strict";
import { test } from "node:test";

import {
  MAX_DO_INVOKE_ENVELOPE_BYTES,
  MAX_DO_REQUEST_BODY_BYTES,
  MAX_DO_REQUEST_HEADER_BYTES,
  MAX_DO_REQUEST_HEADER_COUNT,
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
import {
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
import { settlementWithin } from "../helpers/timing.js";

const { normalizeDoInvokeRequest } = await loadDoProtocol();

/**
 * @param {Record<string, string>} props
 * @param {string} objectName
 * @param {string} method
 * @param {unknown[]} args
 */
function referenceRpcInvokeBody(props, objectName, method, args) {
  const metadataBytes = new TextEncoder().encode(JSON.stringify({
    ns: props.ns,
    worker: props.worker,
    version: props.version,
    doStorageId: props.doStorageId,
    className: props.className,
    objectName,
    kind: "rpc",
    rpc: { method, args },
  }));
  const envelope = new Uint8Array(4 + metadataBytes.byteLength);
  new DataView(envelope.buffer).setUint32(0, metadataBytes.byteLength, false);
  envelope.set(metadataBytes, 4);
  return envelope;
}

/**
 * @template T
 * @param {Promise<T>} promise
 * @param {string} message
 */
async function withTestTimeout(promise, message) {
  const outcome = await settlementWithin(promise, 1000);
  if (outcome.status === "fulfilled") return outcome.value;
  if (outcome.status === "rejected") throw outcome.reason;
  throw new Error(message);
}

test("DO requestSpec header budget counts exact UTF-8 bytes", async () => {
  const exactValue = `${"\u00e9".repeat((MAX_DO_REQUEST_HEADER_BYTES - 2) / 2)}a`;
  const exact = new Request("https://demo.workers.example/send", {
    headers: { x: exactValue },
  });
  await assert.doesNotReject(() => requestSpec(exact, null));

  const oversized = new Request("https://demo.workers.example/send", {
    headers: { x: `${exactValue}\u00e9` },
  });
  await assert.rejects(
    () => requestSpec(oversized, null),
    /fetch headers exceed 65536 bytes/
  );
});

test("DO requestSpec rejects too many headers before host dispatch", async () => {
  const headers = new Headers();
  for (let i = 0; i <= MAX_DO_REQUEST_HEADER_COUNT; i += 1) {
    headers.set(`x-test-${i}`, "value");
  }

  await assert.rejects(
    () => requestSpec(new Request("https://demo.workers.example/send", { headers }), null),
    /Durable Object fetch headers exceed 128 entries/
  );
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

test("DO RPC rejects oversized args before host dispatch", () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };

  assert.throws(
    () => rpcInvokeBody(props, "room-a", "save", ["x".repeat(MAX_DO_REQUEST_BODY_BYTES + 1)]),
    /rpc\.args exceeds 1048576 bytes/
  );
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

test("DO RPC envelope preserves canonical metadata bytes", () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };
  for (const { objectName, method, args } of [
    { objectName: "room-a", method: "ping", args: [] },
    {
      objectName: "房间-1",
      method: "save",
      args: [{ text: "line\n雪", nested: [true, null, 1.25] }],
    },
    {
      objectName: "room-large",
      method: "append",
      args: ["abc雪".repeat(4096)],
    },
  ]) {
    assert.deepEqual(
      rpcInvokeBody(props, objectName, method, args),
      referenceRpcInvokeBody(props, objectName, method, args),
    );
  }
});

test("DO requestSpec header budget uses captured UTF-8 intrinsics", async () => {
  const textEncodeInto = TextEncoder.prototype.encodeInto;
  const stringCharCodeAt = String.prototype.charCodeAt;
  let hostileEncodeIntoCalls = 0;
  let hostileCharCodeAtCalls = 0;
  const request = new Request("https://demo.workers.example/send", {
    headers: { "x-oversized": "\u00e9".repeat((MAX_DO_REQUEST_HEADER_BYTES / 2) + 1) },
  });

  await withMockedProperty(
    TextEncoder.prototype,
    "encodeInto",
    /** @this {TextEncoder} */
    function targetedEncodeInto(value = "", destination) {
      if (value.length > MAX_DO_REQUEST_HEADER_BYTES / 2) {
        hostileEncodeIntoCalls += 1;
        return { read: value.length, written: 0 };
      }
      return Reflect.apply(textEncodeInto, this, [value, destination]);
    },
    () => withMockedProperty(
      String.prototype,
      "charCodeAt",
      /** @this {string} */
      function targetedCharCodeAt(index) {
        if (this.length > MAX_DO_REQUEST_HEADER_BYTES / 2) {
          hostileCharCodeAtCalls += 1;
          return 0;
        }
        return Reflect.apply(stringCharCodeAt, this, [index]);
      },
      async () => {
        await assert.rejects(
          () => requestSpec(request, null),
          /fetch headers exceed 65536 bytes/
        );
      }
    )
  );
  assert.equal(hostileEncodeIntoCalls, 0);
  assert.equal(hostileCharCodeAtCalls, 0);
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

test("DO fetch rejects oversized invoke envelopes before host dispatch", async () => {
  const props = {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  };
  const request = new Request(
    `https://demo.workers.example/${"x".repeat(MAX_DO_INVOKE_ENVELOPE_BYTES)}`
  );

  await assert.rejects(
    () => fetchInvokeInit(props, "room-a", request, null),
    /Durable Object invoke envelope exceeds 2097152 bytes/
  );
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

test("DO owner-hint fallback preserves current-request replay safety", async (t) => {
  const cachedHint = ownerHintFromHeaders(doOwnerHintResponse().headers);
  assert.ok(cachedHint);

  for (const { name, method, failure, replay, expectedStatus, expectedRouterCalls } of [
    {
      name: "cached owner failure replays GET",
      method: "GET",
      failure: "cached",
      replay: true,
      expectedStatus: 204,
      expectedRouterCalls: 1,
    },
    {
      name: "cached owner failure replays HEAD",
      method: "HEAD",
      failure: "cached",
      replay: true,
      expectedStatus: 204,
      expectedRouterCalls: 1,
    },
    {
      name: "cached owner failure does not replay POST",
      method: "POST",
      failure: "cached",
      replay: false,
      expectedStatus: 503,
      expectedRouterCalls: 0,
    },
    {
      name: "trusted second owner hint reroutes POST",
      method: "POST",
      failure: "second-hint",
      replay: false,
      expectedStatus: 204,
      expectedRouterCalls: 2,
    },
  ]) {
    await t.test(name, async () => {
      const cache = new Map();
      if (failure === "cached") cache.set("room-a", cachedHint);
      /** @type {RequestInit[]} */
      const routerCalls = [];
      let ownerCalls = 0;
      const response = await dispatchDoInvokeWithHintCache({
        routerFetch: async (_url, requestInit) => {
          assert.ok(requestInit);
          routerCalls.push(requestInit);
          if (failure === "second-hint" && routerCalls.length === 1) {
            return doOwnerHintResponse();
          }
          return new Response(null, { status: 204 });
        },
        routerUrl: "http://do-runtime/internal/do/invoke",
        ownerFetch: async () => {
          ownerCalls += 1;
          return failure === "cached"
            ? new Response("owner timeout", { status: 504 })
            : doOwnerHintResponse({
              taskId: "do-runtime-b",
              endpoint: "do-runtime-b:8788",
              generation: 4,
            });
        },
        ownerPath: "/internal/do/invoke",
        init: {
          method,
          headers: { "x-wdl-do-accept-owner-hint": "1" },
        },
        cache,
        hintKey: "room-a",
        replayOwnerUnavailable: replay,
      });

      assert.equal(response.status, expectedStatus);
      assert.equal(ownerCalls, 1);
      assert.equal(routerCalls.length, expectedRouterCalls);
      if (routerCalls.length > 0 && (failure === "cached" || routerCalls.length > 1)) {
        const lastRouterCall = routerCalls[routerCalls.length - 1];
        assert.ok(lastRouterCall);
        assert.equal(
          new Headers(lastRouterCall.headers).get("x-wdl-do-accept-owner-hint"),
          null
        );
      }
      if (expectedStatus === 503) {
        assert.equal((await response.json()).error, "owner_unavailable");
      }
    });
  }
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

test("DO owner hint endpoints accept deployed private targets and reject unsafe targets", () => {
  for (const endpoint of [
    "do-runtime-0.do-runtime-headless:8788",
    "do-runtime-0.do-runtime-headless.tenant.svc.cluster.local:8788",
    "10.0.42.17:8788",
    "100.64.30.52:8788",
  ]) {
    assert.equal(ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ endpoint })))?.endpoint, endpoint);
  }

  for (const endpoint of [
    "do-runtime-0.do-runtime-headless.evil.com:8788",
    "do-runtime-a:8788/../../runtime/load",
    "8.8.8.8:8788",
    "127.0.0.1:8788",
    "169.254.169.254:8788",
  ]) {
    assert.equal(ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ endpoint }))), null, endpoint);
  }
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

test("DO requestSpec cancels a stalled request body when the caller aborts", async () => {
  let cancellations = 0;
  const controller = new AbortController();
  const reason = new DOMException("caller cancelled", "AbortError");
  const body = new ReadableStream({
    pull() {
      return new Promise(() => {});
    },
    cancel() {
      cancellations += 1;
    },
  });
  const request = new Request("https://demo.workers.example/send", /** @type {RequestInit} */ ({
    method: "POST",
    body,
    duplex: "half",
    signal: controller.signal,
  }));

  await withMockedPropertyDescriptor(
    Request.prototype,
    "signal",
    {
      configurable: true,
      enumerable: true,
      get() {
        return new AbortController().signal;
      },
    },
    () => withMockedProperty(
      EventTarget.prototype,
      "addEventListener",
      function hostileAddEventListener() {},
      () => withMockedProperty(
        AbortSignal.prototype,
        "throwIfAborted",
        function hostileThrowIfAborted() {},
        async () => {
          const pending = requestSpec(request, "rid-abort");
          controller.abort(reason);
          await assert.rejects(
            withTestTimeout(pending, "requestSpec ignored the caller abort"),
            (error) => error === reason
          );
        }
      )
    )
  );

  assert.equal(cancellations, 1);
});

test("DO requestSpec rejects already-aborted requests before body admission", async () => {
  for (const method of ["GET", "POST"]) {
    const reason = new DOMException(`${method} cancelled`, "AbortError");
    const request = new Request("https://demo.workers.example/send", {
      method,
      signal: AbortSignal.abort(reason),
    });
    await assert.rejects(requestSpec(request, null), (error) => error === reason);
  }
});
