import assert from "node:assert/strict";
import { test } from "node:test";

import { doTransportDataUrl, loadDoProtocol } from "../helpers/load-do-protocol.js";
import { decodeDoEnvelope } from "../helpers/do-envelope.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import {
  doOwnerHintHeaders,
  doOwnerHintResponse,
  doOwnerMetadataHeaders,
  doOwnershipErrorHeaders,
} from "../helpers/do-owner-hint.js";
import {
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
import { settlementWithin } from "../helpers/timing.js";

const {
  DO_INVOKE_CONTENT_TYPE,
  MAX_DO_INVOKE_ENVELOPE_BYTES,
  MAX_DO_REQUEST_BODY_BYTES,
  MAX_DO_REQUEST_HEADER_BYTES,
  MAX_DO_REQUEST_HEADER_COUNT,
  dispatchDoConnectWithHintCache,
  dispatchDoInvokeWithHintCache,
  doOwnerHintCacheKey,
  fetchInvokeInit,
  isWebSocketUpgrade,
  ownerHintFromHeaders,
  replayOwnerUnavailableForFetch,
  requestSpec,
  rpcInvokeBody,
} = await import(doTransportDataUrl());
const { normalizeDoInvokeRequest } = await loadDoProtocol();
const OWNER_KEY = "do_0123456789abcdef0123456789abcdef:Room:shard0";
const doOwnerShardFixture = /** @type {any} */ (
  readRepositoryJson("tests/fixtures/do-owner-shards.json")
);

/** @param {string} [code] @param {number} [status] */
function privateOwnershipErrorResponse(code = "stale_owner_generation", status = 503) {
  return Response.json({
    error: code,
    message: `DO scope ${OWNER_KEY} owner generation is stale`,
    details: {
      taskId: "task-private",
      endpoint: "do-runtime-private:8788",
    },
  }, {
    status,
    headers: doOwnershipErrorHeaders(code),
  });
}

test("DO owner hint keys match the canonical owner-shard fixture", () => {
  for (const item of doOwnerShardFixture.cases) {
    assert.equal(
      doOwnerHintCacheKey(item, item.objectName),
      `${item.doStorageId}:${item.className}:shard${item.shard}`
    );
  }
});

test("a stale owner response relearns the final shared shard hint", async () => {
  const firstObject = doOwnerShardFixture.cases.find(
    (/** @type {any} */ item) => item.objectName === "hint-11"
  );
  const secondObject = doOwnerShardFixture.cases.find(
    (/** @type {any} */ item) => item.objectName === "hint-4"
  );
  assert.ok(firstObject);
  assert.ok(secondObject);
  const firstKey = doOwnerHintCacheKey(firstObject, firstObject.objectName);
  const secondKey = doOwnerHintCacheKey(secondObject, secondObject.objectName);
  assert.equal(firstKey, secondKey);
  const hint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ ownerKey: firstKey })));
  assert.ok(hint);
  const cache = new Map([[firstKey, hint]]);
  let ownerCalls = 0;
  let routerCalls = 0;
  const routerFetch = async (/** @type {string} */ _url, /** @type {RequestInit | undefined} */ init) => {
    routerCalls += 1;
    const headers = new Headers(init?.headers);
    assert.equal(headers.get("x-wdl-do-owner-key"), null);
    assert.equal(headers.get("x-wdl-do-owner-task-id"), null);
    assert.equal(headers.get("x-wdl-do-owner-generation"), null);
    return new Response(null, {
      status: 204,
      headers: doOwnerMetadataHeaders({
        ownerKey: firstKey,
        taskId: "do-runtime-b",
        endpoint: "do-runtime-b:8788",
        generation: 4,
      }),
    });
  };
  const ownerFetch = async (/** @type {string} */ _url, /** @type {RequestInit | undefined} */ init) => {
    ownerCalls += 1;
    const headers = new Headers(init?.headers);
    assert.equal(headers.get("x-wdl-do-owner-key"), firstKey);
    assert.equal(headers.get("x-wdl-do-owner-task-id"), ownerCalls === 1 ? "do-runtime-a" : "do-runtime-b");
    assert.equal(headers.get("x-wdl-do-owner-endpoint"), ownerCalls === 1 ? "do-runtime-a:8788" : "do-runtime-b:8788");
    assert.equal(headers.get("x-wdl-do-owner-generation"), ownerCalls === 1 ? "3" : "4");
    return ownerCalls === 1
      ? Response.json({ error: "stale_owner_generation", message: "retry" }, {
          status: 503,
          headers: doOwnershipErrorHeaders("stale_owner_generation"),
        })
      : new Response(null, { status: 204 });
  };
  const options = {
    routerFetch,
    routerUrl: "http://do-runtime/internal/do/invoke",
    ownerFetch,
    ownerPath: "/internal/do/invoke",
    init: { method: "POST" },
    cache,
    hintKey: secondKey,
  };

  const response = await dispatchDoInvokeWithHintCache(options);

  assert.equal(response.status, 204);
  assert.equal(ownerCalls, 1);
  assert.equal(routerCalls, 1);
  assert.equal(cache.get(firstKey)?.taskId, "do-runtime-b");

  const nextResponse = await dispatchDoInvokeWithHintCache(options);
  assert.equal(nextResponse.status, 204);
  assert.equal(ownerCalls, 2);
  assert.equal(routerCalls, 1);
});

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
  const ownerMetadata = new Headers(doOwnerMetadataHeaders());
  const cachedHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders()));
  assert.ok(cachedHint);
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
            throw new Error("owner-race router retry failed");
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
          cache: new Map([[OWNER_KEY, cachedHint]]),
          hintKey: OWNER_KEY,
          replayOwnerUnavailable: false,
        }),
        /owner-race router retry failed/
      );
      assert.equal(ownerCalls, 1);
      assert.equal(routerCalls, 1);
    });
  }

  await t.test("cached endpoint fallback", async () => {
    const hint = ownerHintFromHeaders(doOwnerHintResponse().headers);
    assert.ok(hint);
    const cache = new Map();
    cache.set(OWNER_KEY, hint);
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
        hintKey: OWNER_KEY,
        replayOwnerUnavailable: true,
      }),
      /owner-race router retry failed/
    );
    assert.equal(ownerCalls, 1);
    assert.equal(routerCalls, 2);
  });
});

test("final trusted DO ownership errors are sanitized without private details", async (t) => {
  await t.test("ordinary invoke sanitizes the second ownership response", async () => {
    let routerCalls = 0;
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return privateOwnershipErrorResponse();
      },
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: null,
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(routerCalls, 2);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "stale_owner_generation",
      message: "Durable Object ownership is unavailable",
    });
    assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
  });

  await t.test("WebSocket dispatch sanitizes its first ownership response", async () => {
    let routerCalls = 0;
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return privateOwnershipErrorResponse();
      },
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: null,
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(routerCalls, 1);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "stale_owner_generation",
      message: "Durable Object ownership is unavailable",
    });
    assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
  });

  await t.test("unknown private codes fail closed to owner_unavailable", async () => {
    let routerCalls = 0;
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return privateOwnershipErrorResponse("future_private_code");
      },
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: null,
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(routerCalls, 1);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "owner_unavailable",
      message: "Durable Object ownership is unavailable",
    });
    assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
  });

  for (const { name, status } of [
    { name: "successful status", status: 200 },
    { name: "unrecognized error status", status: 418 },
  ]) {
    await t.test(`${name} fails closed to a 503 owner_unavailable`, async () => {
      const response = await dispatchDoInvokeWithHintCache({
        routerFetch: async () => privateOwnershipErrorResponse("stale_owner_generation", status),
        routerUrl: "http://do-runtime/internal/do/invoke",
        ownerFetch: null,
        ownerPath: "/internal/do/invoke",
        init: { method: "POST" },
        cache: new Map(),
        hintKey: OWNER_KEY,
      });

      assert.equal(response.status, 503);
      assert.deepEqual(await response.json(), {
        error: "owner_unavailable",
        message: "Durable Object ownership is unavailable",
      });
      assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
    });
  }

  await t.test("WebSocket preserves a valid ownership code after router handoff", async () => {
    let routerCalls = 0;
    let ownerCalls = 0;
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return doOwnerHintResponse();
      },
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: async () => {
        ownerCalls += 1;
        return privateOwnershipErrorResponse();
      },
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(routerCalls, 1);
    assert.equal(ownerCalls, 1);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "stale_owner_generation",
      message: "Durable Object ownership is unavailable",
    });
  });
});

test("cached broad DO ownership errors replay only idempotent fetches", async (t) => {
  const cachedHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders()));
  assert.ok(cachedHint);

  for (const {
    name,
    dispatch,
    method,
    routerUrl,
    ownerPath,
    headers,
    replay,
    expectedRouterCalls,
    expectedStatus,
  } of [
    {
      name: "GET replays through the router",
      dispatch: dispatchDoInvokeWithHintCache,
      method: "GET",
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerPath: "/internal/do/invoke",
      replay: true,
      expectedRouterCalls: 1,
      expectedStatus: 204,
    },
    {
      name: "POST does not replay",
      dispatch: dispatchDoInvokeWithHintCache,
      method: "POST",
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerPath: "/internal/do/invoke",
      replay: false,
      expectedRouterCalls: 0,
      expectedStatus: 503,
    },
    {
      name: "RPC does not replay",
      dispatch: dispatchDoInvokeWithHintCache,
      method: "POST",
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerPath: "/internal/do/invoke",
      headers: { "content-type": DO_INVOKE_CONTENT_TYPE },
      replay: false,
      expectedRouterCalls: 0,
      expectedStatus: 503,
    },
    {
      name: "WebSocket does not replay",
      dispatch: dispatchDoConnectWithHintCache,
      method: "GET",
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerPath: "/internal/do/connect",
      replay: false,
      expectedRouterCalls: 0,
      expectedStatus: 503,
    },
  ]) {
    await t.test(name, async () => {
      let routerCalls = 0;
      const response = await dispatch({
        routerFetch: async () => {
          routerCalls += 1;
          return new Response(null, { status: 204 });
        },
        routerUrl,
        ownerFetch: async () => privateOwnershipErrorResponse("owner_unavailable"),
        ownerPath,
        init: { method, headers },
        cache: new Map([[OWNER_KEY, cachedHint]]),
        hintKey: OWNER_KEY,
        replayOwnerUnavailable: replay,
      });

      assert.equal(routerCalls, expectedRouterCalls);
      assert.equal(response.status, expectedStatus);
      if (expectedStatus === 503) {
        assert.deepEqual(await response.json(), {
          error: "owner_unavailable",
          message: "Durable Object ownership is unavailable",
        });
      }
    });
  }
});

test("DO owner hints must match the canonical shard key before use or caching", async (t) => {
  const otherOwnerKey = OWNER_KEY.replace("shard0", "shard1");

  await t.test("routed hint mismatch", async () => {
    const cache = new Map();
    let routerCalls = 0;
    let ownerCalls = 0;
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return doOwnerHintResponse({ ownerKey: otherOwnerKey });
      },
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache,
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: "owner_unavailable",
      message: "DO owner is unavailable; request outcome may be unknown",
    });
    assert.equal(routerCalls, 1);
    assert.equal(ownerCalls, 0);
    assert.equal(cache.size, 0);
  });

  await t.test("WebSocket routed hint mismatch does not expose owner metadata", async () => {
    const cache = new Map();
    let ownerCalls = 0;
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => doOwnerHintResponse({
        ownerKey: otherOwnerKey,
        taskId: "task-private",
        endpoint: "do-runtime-private:8788",
        generation: 9,
      }),
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache,
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    const body = await response.text();
    assert.deepEqual(JSON.parse(body), {
      error: "owner_unavailable",
      message: "DO owner is unavailable; request outcome may be unknown",
    });
    assert.doesNotMatch(body, /task-private|do-runtime-private|shard1/);
    assert.equal(ownerCalls, 0);
    assert.equal(cache.size, 0);
  });

  await t.test("malformed WebSocket routed hint does not expose owner metadata", async () => {
    const headers = new Headers(doOwnerHintHeaders({
      taskId: "task-private",
      endpoint: "do-runtime-private:8788",
    }));
    headers.delete("x-wdl-do-owner-generation");
    let ownerCalls = 0;
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => Response.json({
        owner: { taskId: "task-private", endpoint: "do-runtime-private:8788" },
      }, { status: 409, headers }),
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    const body = await response.text();
    assert.equal(JSON.parse(body).error, "owner_unavailable");
    assert.doesNotMatch(body, /task-private|do-runtime-private/);
    assert.equal(ownerCalls, 0);
  });

  await t.test("wrong-status WebSocket hint marker does not expose owner metadata", async () => {
    let ownerCalls = 0;
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => Response.json({
        owner: { taskId: "task-private", endpoint: "do-runtime-private:8788" },
      }, {
        status: 500,
        headers: doOwnerHintHeaders({
          taskId: "task-private",
          endpoint: "do-runtime-private:8788",
        }),
      }),
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    const body = await response.text();
    assert.equal(JSON.parse(body).error, "owner_unavailable");
    assert.doesNotMatch(body, /task-private|do-runtime-private/);
    assert.equal(ownerCalls, 0);
  });

  await t.test("malformed cached WebSocket owner hint does not expose owner metadata", async () => {
    const cachedHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders()));
    assert.ok(cachedHint);
    const headers = new Headers(doOwnerHintHeaders({
      taskId: "task-private",
      endpoint: "do-runtime-private:8788",
    }));
    headers.delete("x-wdl-do-owner-generation");
    let routerCalls = 0;
    const cache = new Map([[OWNER_KEY, cachedHint]]);
    const response = await dispatchDoConnectWithHintCache({
      routerFetch: async () => {
        routerCalls += 1;
        return new Response("unexpected router call");
      },
      routerUrl: "http://do-runtime/internal/do/connect",
      ownerFetch: async () => Response.json({
        owner: { taskId: "task-private", endpoint: "do-runtime-private:8788" },
      }, { status: 409, headers }),
      ownerPath: "/internal/do/connect",
      init: { method: "GET" },
      cache,
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    const body = await response.text();
    assert.equal(JSON.parse(body).error, "owner_unavailable");
    assert.doesNotMatch(body, /task-private|do-runtime-private/);
    assert.equal(routerCalls, 0);
    assert.equal(cache.size, 0);
  });

  await t.test("ordinary invokes do not follow legacy router hints", async () => {
    let ownerCalls = 0;
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => doOwnerHintResponse(),
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache: new Map(),
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 503);
    assert.equal((await response.json()).error, "owner_unavailable");
    assert.equal(ownerCalls, 0);
  });

  await t.test("safe replay does not expose a legacy router hint body", async () => {
    const cachedHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders()));
    assert.ok(cachedHint);
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => doOwnerHintResponse({
        taskId: "task-private",
        endpoint: "do-runtime-private:8788",
        generation: 9,
      }),
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: async () => doOwnerHintResponse(),
      ownerPath: "/internal/do/invoke",
      init: { method: "GET" },
      cache: new Map([[OWNER_KEY, cachedHint]]),
      hintKey: OWNER_KEY,
      replayOwnerUnavailable: true,
    });

    assert.equal(response.status, 503);
    const body = await response.text();
    assert.equal(JSON.parse(body).error, "owner_unavailable");
    assert.doesNotMatch(body, /task-private|do-runtime-private/);
  });

  await t.test("cached hint mismatch", async () => {
    const cache = new Map([[OWNER_KEY, ownerHintFromHeaders(new Headers(
      doOwnerHintHeaders({ ownerKey: otherOwnerKey })
    ))]]);
    let ownerCalls = 0;
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => new Response(null, { status: 204 }),
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: async () => {
        ownerCalls += 1;
        return new Response("unexpected owner call");
      },
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache,
      hintKey: OWNER_KEY,
    });

    assert.equal(response.status, 204);
    assert.equal(ownerCalls, 0);
    assert.equal(cache.size, 0);
  });

  await t.test("successful response metadata mismatch", async () => {
    const cache = new Map();
    const response = await dispatchDoInvokeWithHintCache({
      routerFetch: async () => new Response("ok", {
        headers: doOwnerMetadataHeaders({ ownerKey: otherOwnerKey }),
      }),
      routerUrl: "http://do-runtime/internal/do/invoke",
      ownerFetch: null,
      ownerPath: "/internal/do/invoke",
      init: { method: "POST" },
      cache,
      hintKey: OWNER_KEY,
    });

    assert.equal(await response.text(), "ok");
    assert.equal(cache.size, 0);
  });
});

test("cached WebSocket owner hints trigger one router rediscovery", async () => {
  const oldHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders({
    endpoint: "do-runtime-a:8788",
    generation: 3,
  })));
  assert.ok(oldHint);
  const cache = new Map([[OWNER_KEY, oldHint]]);
  const finalOwnerHeaders = new Headers(doOwnerMetadataHeaders({
    endpoint: "do-runtime-b:8788",
    generation: 4,
  }));
  /** @type {string[]} */
  const ownerUrls = [];
  let routerCalls = 0;

  const response = await dispatchDoConnectWithHintCache({
    routerFetch: async (/** @type {string} */ _url, /** @type {RequestInit | undefined} */ init) => {
      routerCalls += 1;
      assert.equal(new Headers(init?.headers).get("x-wdl-do-accept-owner-hint"), "1");
      return doOwnerHintResponse({ endpoint: "do-runtime-b:8788", generation: 4 });
    },
    routerUrl: "http://do-runtime/internal/do/connect",
    ownerFetch: async (/** @type {string} */ url) => {
      ownerUrls.push(url);
      return ownerUrls.length === 1
        ? doOwnerHintResponse({ endpoint: "do-runtime-b:8788", generation: 4 })
        : new Response("connected", { headers: finalOwnerHeaders });
    },
    ownerPath: "/internal/do/connect",
    init: {
      method: "GET",
      headers: { "x-wdl-do-accept-owner-hint": "1" },
    },
    cache,
    hintKey: OWNER_KEY,
  });

  assert.equal(await response.text(), "connected");
  assert.equal(routerCalls, 1);
  assert.deepEqual(ownerUrls, [
    "http://do-runtime-a:8788/internal/do/connect",
    "http://do-runtime-b:8788/internal/do/connect",
  ]);
  assert.equal(cache.get(OWNER_KEY)?.endpoint, "do-runtime-b:8788");
});

test("cached WebSocket pre-dispatch owner races trigger one router rediscovery", async () => {
  const oldHint = ownerHintFromHeaders(new Headers(doOwnerHintHeaders({
    endpoint: "do-runtime-a:8788",
    generation: 3,
  })));
  assert.ok(oldHint);
  const cache = new Map([[OWNER_KEY, oldHint]]);
  const finalOwnerHeaders = new Headers(doOwnerMetadataHeaders({
    endpoint: "do-runtime-b:8788",
    generation: 4,
  }));
  /** @type {string[]} */
  const ownerUrls = [];
  let routerCalls = 0;

  const response = await dispatchDoConnectWithHintCache({
    routerFetch: async () => {
      routerCalls += 1;
      return doOwnerHintResponse({ endpoint: "do-runtime-b:8788", generation: 4 });
    },
    routerUrl: "http://do-runtime/internal/do/connect",
    ownerFetch: async (/** @type {string} */ url) => {
      ownerUrls.push(url);
      return ownerUrls.length === 1
        ? privateOwnershipErrorResponse("stale_owner_generation")
        : new Response("connected", { headers: finalOwnerHeaders });
    },
    ownerPath: "/internal/do/connect",
    init: {
      method: "GET",
      headers: { "x-wdl-do-accept-owner-hint": "1" },
    },
    cache,
    hintKey: OWNER_KEY,
  });

  assert.equal(await response.text(), "connected");
  assert.equal(routerCalls, 1);
  assert.deepEqual(ownerUrls, [
    "http://do-runtime-a:8788/internal/do/connect",
    "http://do-runtime-b:8788/internal/do/connect",
  ]);
  assert.equal(cache.get(OWNER_KEY)?.endpoint, "do-runtime-b:8788");
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
  ]) {
    await t.test(name, async () => {
      const cache = new Map();
      if (failure === "cached") cache.set(OWNER_KEY, cachedHint);
      /** @type {RequestInit[]} */
      const routerCalls = [];
      let ownerCalls = 0;
      const response = await dispatchDoInvokeWithHintCache({
        routerFetch: async (/** @type {string} */ _url, /** @type {RequestInit | undefined} */ requestInit) => {
          assert.ok(requestInit);
          routerCalls.push(requestInit);
          return new Response(null, { status: 204 });
        },
        routerUrl: "http://do-runtime/internal/do/invoke",
        ownerFetch: async () => {
          ownerCalls += 1;
          return new Response("owner timeout", { status: 504 });
        },
        ownerPath: "/internal/do/invoke",
        init: { method },
        cache,
        hintKey: OWNER_KEY,
        replayOwnerUnavailable: replay,
      });

      assert.equal(response.status, expectedStatus);
      assert.equal(ownerCalls, 1);
      assert.equal(routerCalls.length, expectedRouterCalls);
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
