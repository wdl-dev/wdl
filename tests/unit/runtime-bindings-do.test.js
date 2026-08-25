import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { decodeDoEnvelopeMetadata as decodeDoEnvelope } from "../helpers/do-envelope.js";
import {
  doOwnerHintHeaders,
  doOwnerHintResponse,
  doOwnerMetadataHeaders,
  doOwnershipErrorHeaders,
  tenantBodyDoOwnerHintResponse,
} from "../helpers/do-owner-hint.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { makeRecordingFetch, withRecordingFetch } from "../helpers/mock-fetch.js";
import {
  withMockedGlobal,
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";
import { doTransportDataUrl } from "../helpers/load-do-protocol.js";

const transportUrl = doTransportDataUrl();
const scopedRequestUrl = repositoryFileUrl("runtime/_wdl-do-scoped-request.js");
const internalAuthUrl = sharedInternalAuthUrl();
const ownerHintCacheUrl = repositoryFileUrl("runtime/_wdl-owner-hint-cache.js");

const { clearDoOwnerHintsForTest, DurableObjectNamespace } = await importRepositoryModule("runtime/bindings/do.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [/from "runtime-do-transport";/, `from ${JSON.stringify(transportUrl)};`],
  [/from "_wdl-do-scoped-request\.js";/, `from ${JSON.stringify(scopedRequestUrl)};`],
  [/from "runtime-owner-hint-cache";/, `from ${JSON.stringify(ownerHintCacheUrl)};`],
  [/from "shared-internal-auth";/, `from ${JSON.stringify(internalAuthUrl)};`],
]);
const {
  MAX_DO_RPC_RESPONSE_BYTES,
  connectHeaders,
  doOwnerHintCacheKey,
  ownerHintFromHeaders,
  requestSpec,
} = await import(transportUrl);
const {
  decodeDoObjectNameHeader,
  readScopedDoRequest,
  scopedDoRequest,
} = await import(scopedRequestUrl);

beforeEach(() => clearDoOwnerHintsForTest());

const BINDING_PROPS = Object.freeze({
  ns: "tenant",
  worker: "chat",
  version: "v1",
  doStorageId: "do_0123456789abcdef0123456789abcdef",
  className: "Room",
});

/** @param {string} objectName */
function ownerKeyFor(objectName) {
  return doOwnerHintCacheKey(BINDING_PROPS, objectName);
}

/** @param {any} backend */
function bindingWithBackend(backend) {
  return new DurableObjectNamespace({
    props: BINDING_PROPS,
  }, {
    DO_BACKEND: backend,
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
  });
}

/** @param {DurableObjectNamespace} binding @param {string} objectName @param {Request} request */
function bindingFetch(binding, objectName, request) {
  return binding.fetch(scopedDoRequest(objectName, request, null));
}

test("binding-scoped DO object names use canonical reversible ASCII headers", () => {
  for (const objectName of [
    "room",
    " room ",
    "雪",
    "雪".repeat(170),
    "line\nbreak",
    "\u0000",
    "%20",
  ]) {
    const request = scopedDoRequest(
      objectName,
      new Request("https://tenant.workers.example/send"),
      null
    );
    const encoded = request.headers.get("x-wdl-do-binding-object-name");
    assert.match(encoded, /^[\x21-\x7e]+$/);
    assert.equal(readScopedDoRequest(request).objectName, objectName);
  }

  for (const encoded of ["%", "%e9%9b%aa", "%41"]) {
    const request = new Request("https://tenant.workers.example/send", {
      headers: { "x-wdl-do-binding-object-name": encoded },
    });
    assert.throws(() => readScopedDoRequest(request), /invalid object name/);
  }
});

test("binding-scoped DO object name encoding uses captured URI intrinsics", () =>
  withMockedProperty(globalThis, "encodeURIComponent", () => "attacker", () =>
    withMockedProperty(globalThis, "decodeURIComponent", () => "attacker", () => {
      const request = scopedDoRequest(
        " 雪 ",
        new Request("https://tenant.workers.example/send"),
        null
      );
      assert.equal(request.headers.get("x-wdl-do-binding-object-name"), "%20%E9%9B%AA%20");
      assert.equal(readScopedDoRequest(request).objectName, " 雪 ");
    })
  ));

test("DO connect headers strip tenant routing headers and preserve the scope request id", () => {
  const request = new Request("https://tenant.workers.example/ws", {
    headers: {
      "x-wdl-do-hop-count": "99",
      "x-wdl-do-owner-key": "tenant",
      "x-wdl-do-owner-generation": "123",
      "x-request-id": "tenant-rid",
    },
  });
  const headers = connectHeaders({
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
  }, "room-a", request, "scope-rid");

  assert.equal(headers.get("x-wdl-do-hop-count"), null);
  assert.equal(headers.get("x-wdl-do-owner-key"), null);
  assert.equal(headers.get("x-wdl-do-owner-generation"), null);
  assert.equal(headers.get("x-request-id"), "scope-rid");
  assert.equal(headers.get("x-wdl-do-ns"), "tenant");
  assert.equal(headers.get("x-wdl-do-object-name"), "room-a");
});

test("DO connect headers reuse the canonical object-name encoding", () => {
  for (const objectName of [" room ", "room", "雪", "\u0000"]) {
    const headers = connectHeaders({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    }, objectName, new Request("https://tenant.workers.example/ws"), null);
    const encoded = headers.get("x-wdl-do-object-name");
    assert.match(encoded, /^[\x21-\x7e]+$/);
    assert.equal(decodeDoObjectNameHeader(encoded), objectName);
  }
});

test("DO fetch requestSpec strips tenant routing headers and preserves the scope request id", async () => {
  const { spec } = await requestSpec(new Request("https://tenant.workers.example/send", {
    method: "POST",
    headers: {
      "x-wdl-do-hop-count": "99",
      "x-wdl-do-accept-owner-hint": "1",
      "x-wdl-do-owner-hint": "1",
      "x-wdl-do-ownership-error": "owner_fence_missing",
      "x-wdl-do-owner-key": "tenant",
      "x-wdl-do-owner-generation": "123",
      "x-request-id": "tenant-rid",
    },
    body: "hello",
  }), "scope-rid");
  const headers = new Headers(spec.headers);
  assert.equal(headers.get("x-wdl-do-hop-count"), null);
  assert.equal(headers.get("x-wdl-do-accept-owner-hint"), null);
  assert.equal(headers.get("x-wdl-do-owner-hint"), null);
  assert.equal(headers.get("x-wdl-do-ownership-error"), null);
  assert.equal(headers.get("x-wdl-do-owner-key"), null);
  assert.equal(headers.get("x-wdl-do-owner-generation"), null);
  assert.equal(headers.get("x-request-id"), "scope-rid");
});

test("DO fetch requestSpec uses captured header mutation intrinsics", async () => {
  const request = new Request("https://tenant.workers.example/send", {
    method: "POST",
    headers: {
      "x-wdl-do-owner-key": "tenant",
      "x-wdl-do-ownership-error": "owner_fence_missing",
    },
    body: "hello",
  });

  const { spec } = await withMockedProperty(
    Headers.prototype,
    "delete",
    function mockedDelete() {},
    () => withMockedProperty(
      Headers.prototype,
      "has",
      function mockedHas() { return true; },
      () => withMockedProperty(
        Headers.prototype,
        "set",
        function mockedSet() {},
        () => requestSpec(request, "scope-rid")
      )
    )
  );

  const headers = new Headers(spec.headers);
  assert.equal(headers.get("x-wdl-do-owner-key"), null);
  assert.equal(headers.get("x-wdl-do-ownership-error"), null);
  assert.equal(headers.get("x-request-id"), "scope-rid");
});

test("DO owner hint parser requires positive safe-integer owner generation", () => {
  assert.deepEqual(ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ generation: 3 }))), {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "do-runtime-a",
    endpoint: "do-runtime-a:8788",
    generation: 3,
  });
  assert.equal(ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ generation: 0 }))), null);
  assert.equal(ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ generation: 1.5 }))), null);
  assert.equal(
    ownerHintFromHeaders(new Headers(doOwnerHintHeaders({ generation: 9007199254740992 }))),
    null
  );
  const missingGeneration = new Headers(doOwnerHintHeaders({}));
  missingGeneration.delete("x-wdl-do-owner-generation");
  assert.equal(ownerHintFromHeaders(missingGeneration), null);
});

test("DO-to-DO fetch does not follow legacy router owner hints", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
        response: () => calls.length === 1
          ? doOwnerHintResponse({ ownerKey: ownerKeyFor("room-a") })
          : new Response("router-ok"),
      }),
    });

    const response = await bindingFetch(binding, "room-a", new Request("https://demo.workers.example/send", {
      method: "POST",
      body: "hello",
    }));

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "owner_unavailable");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 0);
    assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
    assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), null);
  });
});

test("DO-to-DO fetch caches owner hints and skips router on later calls", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
        response: new Response("router-forwarded", {
          headers: doOwnerMetadataHeaders({ ownerKey: ownerKeyFor("cached-room") }),
        }),
      }),
    });

    const first = await bindingFetch(binding, "cached-room", new Request("https://demo.workers.example/send"));
    const second = await bindingFetch(binding, "cached-room", new Request("https://demo.workers.example/send"));

    assert.equal(await first.text(), "router-forwarded");
    assert.equal(await second.text(), "owner-ok");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 1);
    assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/invoke");
    assert.equal(new Headers(ownerCalls[0].init.headers).get("x-wdl-internal-auth"), "test-internal-auth-token");
  }, {
    response: new Response("owner-ok", {
      headers: doOwnerMetadataHeaders({ ownerKey: ownerKeyFor("cached-room") }),
    }),
  });
});

test("DO binding replays broad ownership errors only for GET and HEAD", async (t) => {
  const cases = [
    {
      name: "GET",
      invoke: (/** @type {DurableObjectNamespace} */ binding, /** @type {string} */ objectName) =>
        bindingFetch(binding, objectName, new Request("https://demo.workers.example/read")),
      replay: true,
    },
    {
      name: "HEAD",
      invoke: (/** @type {DurableObjectNamespace} */ binding, /** @type {string} */ objectName) =>
        bindingFetch(binding, objectName, new Request("https://demo.workers.example/read", { method: "HEAD" })),
      replay: true,
    },
    {
      name: "POST",
      invoke: (/** @type {DurableObjectNamespace} */ binding, /** @type {string} */ objectName) =>
        bindingFetch(binding, objectName, new Request("https://demo.workers.example/write", {
          method: "POST",
          body: "write",
        })),
      replay: false,
    },
    {
      name: "RPC",
      invoke: (/** @type {DurableObjectNamespace} */ binding, /** @type {string} */ objectName) =>
        binding.rpcObject(objectName, "addMessage", ["write"]),
      replay: false,
      rpc: true,
    },
    {
      name: "WebSocket",
      invoke: (/** @type {DurableObjectNamespace} */ binding, /** @type {string} */ objectName) =>
        bindingFetch(binding, objectName, new Request("https://demo.workers.example/ws", {
          headers: {
            Connection: "Upgrade",
            Upgrade: "websocket",
            "Sec-WebSocket-Key": "abc",
          },
        })),
      replay: false,
    },
  ];

  for (const { name, invoke, replay, rpc = false } of cases) {
    await t.test(name, async () => {
      clearDoOwnerHintsForTest();
      const objectName = `broad-${name.toLowerCase()}`;
      /** @type {any[]} */
      const routerCalls = [];
      /** @type {Array<{ url: string, init: RequestInit }>} */
      const ownerCalls = [];
      await withRecordingFetch(ownerCalls, async () => {
        const binding = bindingWithBackend({
          fetch: makeRecordingFetch(routerCalls, {
            response: () => routerCalls.length === 1
              ? new Response("primed", {
                  headers: doOwnerMetadataHeaders({ ownerKey: ownerKeyFor(objectName) }),
                })
              : new Response(null, { status: 204 }),
          }),
        });
        const prime = await bindingFetch(
          binding,
          objectName,
          new Request("https://demo.workers.example/prime")
        );
        assert.equal(prime.status, 200);

        if (rpc) {
          await assert.rejects(
            invoke(binding, objectName),
            (err) => Reflect.get(/** @type {object} */ (err), "code") === "owner_unavailable"
          );
        } else {
          const response = /** @type {Response} */ (await invoke(binding, objectName));
          assert.equal(response.status, replay ? 204 : 503);
          if (!replay) {
            assert.equal((await response.json()).error, "owner_unavailable");
          }
        }

        assert.equal(routerCalls.length, replay ? 2 : 1);
        assert.equal(ownerCalls.length, 1);
      }, {
        response: Response.json({
          error: "owner_unavailable",
          message: "private owner state",
        }, {
          status: 503,
          headers: doOwnershipErrorHeaders("owner_unavailable"),
        }),
      });
    });
  }
});

test("DO-to-DO fetch shares owner hints across object names in one shard", async () => {
  const firstObject = "hint-11";
  const secondObject = "hint-4";
  assert.equal(ownerKeyFor(firstObject), ownerKeyFor(secondObject));
  /** @type {any[]} */
  const routerCalls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(routerCalls, {
        response: new Response("router-forwarded", {
          headers: doOwnerMetadataHeaders({ ownerKey: ownerKeyFor(firstObject) }),
        }),
      }),
    });

    const first = await bindingFetch(binding, firstObject, new Request("https://demo.workers.example/send"));
    const second = await bindingFetch(binding, secondObject, new Request("https://demo.workers.example/send"));

    assert.equal(await first.text(), "router-forwarded");
    assert.equal(await second.text(), "owner-ok");
    assert.equal(routerCalls.length, 1);
    assert.equal(ownerCalls.length, 1);
  }, {
    response: new Response("owner-ok", {
      headers: doOwnerMetadataHeaders({ ownerKey: ownerKeyFor(firstObject) }),
    }),
  });
});

test("DO-to-DO fetch does not follow tenant body owner hints", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, { response: tenantBodyDoOwnerHintResponse() }),
    });

    const response = await bindingFetch(binding, "room-ignore-race-hint", new Request("https://demo.workers.example/send"));

    const body = await readJsonResponse(response, 409);
    assert.equal(body.message, "tenant body");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 0);
  }, {
    response: new Response("internal"),
  });
});

test("DO-to-DO fetch does not follow tenant 409 responses with owner metadata", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
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
    });

    const response = await bindingFetch(binding, "room-ignore-race-hint-response", new Request("https://demo.workers.example/send"));

    const body = await readJsonResponse(response, 409);
    assert.equal(body.message, "tenant conflict");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 0);
  }, {
    response: new Response("duplicate"),
  });
});

test("DO-to-DO RPC forwards through do-runtime and decodes structured result", async () => {
  /** @type {any[]} */
  const calls = [];
  const binding = bindingWithBackend({
    fetch: makeRecordingFetch(calls, { response: Response.json({ ok: true, result: { count: 2 } }) }),
  });

  const result = await binding.rpcObject("room-rpc", "addMessage", ["hello"]);

  assert.deepEqual(result, { count: 2 });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls.some((call) => call.url === "http://do-runtime/internal/do/connect"), false);
  const body = decodeDoEnvelope(calls[0].init.body);
  assert.equal(body.kind, "rpc");
  assert.equal(body.objectName, "room-rpc");
  assert.deepEqual(body.rpc, { method: "addMessage", args: ["hello"] });
});

test("DO-to-DO RPC preserves a valid undefined result", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      return Response.json({ ok: true });
    },
  });

  assert.equal(await binding.rpcObject("room-rpc-undefined", "touch", []), undefined);
});

test("DO-to-DO RPC ignores inherited response envelope fields", async () => {
  const objectPrototype = /** @type {Record<string, unknown>} */ (
    /** @type {unknown} */ (Object.prototype)
  );
  await withMockedProperty(objectPrototype, "result", "polluted", async () => {
    const binding = bindingWithBackend({
      async fetch() {
        return Response.json({ ok: true });
      },
    });
    assert.equal(await binding.rpcObject("room-rpc-inherited-result", "touch", []), undefined);
  });

  await withMockedProperty(objectPrototype, "message", "PRIVATE polluted message", () =>
    withMockedProperty(objectPrototype, "error", "polluted_code", async () => {
      const binding = bindingWithBackend({
        async fetch() {
          return Response.json({}, { status: 500 });
        },
      });
      await assert.rejects(
        binding.rpcObject("room-rpc-inherited-error", "mutate", []),
        (err) => err instanceof Error &&
          err.message === "Durable Object RPC failed with status 500" &&
          Reflect.get(err, "code") === undefined
      );
    })
  );
});

test("DO-to-DO RPC uses its captured Error constructor", async () => {
  const NativeError = Error;
  const PoisonedError = /** @type {ErrorConstructor} */ (
    /** @type {unknown} */ (function PoisonedError() {
      throw new TypeError("global Error constructor was used");
    })
  );
  await withMockedGlobal("Error", PoisonedError, async () => {
    const malformedBinding = bindingWithBackend({
      async fetch() {
        return new Response("not-json");
      },
    });
    await assert.rejects(
      malformedBinding.rpcObject("room-rpc-poisoned-error", "mutate", []),
      (err) => err instanceof NativeError &&
        Reflect.get(err, "code") === "do_rpc_result_unknown"
    );

    const structuredBinding = bindingWithBackend({
      async fetch() {
        return Response.json({ error: "do_rpc_error", message: "remote failure" }, {
          status: 500,
        });
      },
    });
    await assert.rejects(
      structuredBinding.rpcObject("room-rpc-poisoned-structured-error", "mutate", []),
      (err) => err instanceof NativeError &&
        Reflect.get(err, "code") === "do_rpc_error" &&
        Reflect.get(err, "message") === "remote failure"
    );
  });
});

test("DO-to-DO RPC defines structured error fields without prototype setters", async () => {
  await withMockedPropertyDescriptor(Error.prototype, "name", {
    get() {
      return "Error";
    },
    set() {
      throw new TypeError("Error.prototype.name setter was used");
    },
  }, async () => {
    const binding = bindingWithBackend({
      async fetch() {
        return Response.json({
          error: "do_rpc_error",
          name: "RemoteRpcError",
          message: "remote failure",
          stack: "RemoteRpcError: remote failure\n    at remote-do",
        }, { status: 500 });
      },
    });
    await assert.rejects(
      binding.rpcObject("room-rpc-error-prototype", "mutate", []),
      (err) => err instanceof Error &&
        err.name === "RemoteRpcError" &&
        err.message === "remote failure" &&
        err.stack === "RemoteRpcError: remote failure\n    at remote-do" &&
        Reflect.get(err, "code") === "do_rpc_error"
    );
  });
});

test("DO-to-DO RPC defines error fields with null-prototype descriptors", async () => {
  const objectPrototype = /** @type {Record<string, unknown>} */ (
    /** @type {unknown} */ (Object.prototype)
  );
  const malformedResponse = new Response("not-json");
  const structuredResponse = Response.json({
    error: "do_rpc_error",
    name: "RemoteRpcError",
    message: "remote failure",
    stack: "RemoteRpcError: remote failure\n    at remote-do",
  }, { status: 500 });
  await withMockedProperty(objectPrototype, "get", () => "polluted getter", async () => {
    const malformedBinding = bindingWithBackend({
      async fetch() {
        return malformedResponse;
      },
    });
    await assert.rejects(
      malformedBinding.rpcObject("room-rpc-descriptor-unknown", "mutate", []),
      (err) => err instanceof Error &&
        Reflect.get(err, "code") === "do_rpc_result_unknown"
    );

    const structuredBinding = bindingWithBackend({
      async fetch() {
        return structuredResponse;
      },
    });
    await assert.rejects(
      structuredBinding.rpcObject("room-rpc-descriptor-structured", "mutate", []),
      (err) => err instanceof Error &&
        err.name === "RemoteRpcError" &&
        err.stack === "RemoteRpcError: remote failure\n    at remote-do" &&
        Reflect.get(err, "code") === "do_rpc_error"
    );
  });
});

test("DO-to-DO RPC fails closed on malformed success responses", async () => {
  // Non-fatal UTF-8 decoding would replace 0xff and accept this as valid JSON.
  const invalidUtf8Envelope = Uint8Array.from([
    ...new TextEncoder().encode('{"ok":true,"result":"'),
    0xff,
    ...new TextEncoder().encode('"}'),
  ]);
  for (const response of [
    new Response("not-json"),
    new Response(invalidUtf8Envelope),
    new Response(new ReadableStream({
      pull() {
        throw new Error("PRIVATE owner response read failure");
      },
    })),
    Response.json({ result: "missing success marker" }),
  ]) {
    const binding = bindingWithBackend({
      async fetch() {
        return response;
      },
    });
    await assert.rejects(
      binding.rpcObject("room-rpc-malformed", "mutate", []),
      (err) => err instanceof Error &&
        Reflect.get(err, "code") === "do_rpc_result_unknown" &&
        err.message === "Durable Object RPC result is unavailable; request outcome may be unknown"
    );
  }
});

test("DO-to-DO RPC rejects declared oversized responses and cancels their body", async () => {
  let canceled = false;
  const body = new ReadableStream({
    pull() {
      return new Promise(() => {});
    },
    cancel() {
      canceled = true;
    },
  });
  const binding = bindingWithBackend({
    async fetch() {
      return new Response(body, {
        headers: { "content-length": String(MAX_DO_RPC_RESPONSE_BYTES + 1) },
      });
    },
  });

  await assert.rejects(
    binding.rpcObject("room-rpc-declared-oversized", "mutate", []),
    (err) => err instanceof Error && Reflect.get(err, "code") === "do_rpc_result_unknown"
  );
  assert.equal(canceled, true);
});

test("DO-to-DO RPC stops streaming responses when they cross the byte limit", async () => {
  let pulls = 0;
  let canceled = false;
  const body = new ReadableStream({
    pull(controller) {
      pulls += 1;
      if (pulls <= 2) controller.enqueue(new Uint8Array(600 * 1024));
      else return new Promise(() => {});
    },
    cancel() {
      canceled = true;
    },
  });
  const binding = bindingWithBackend({
    async fetch() {
      return new Response(body);
    },
  });

  await assert.rejects(
    binding.rpcObject("room-rpc-streaming-oversized", "mutate", []),
    (err) => err instanceof Error && Reflect.get(err, "code") === "do_rpc_result_unknown"
  );
  assert.equal(canceled, true);
});

test("DO-to-DO fetch retries owner generation races without hint opt-in", async () => {
  /** @type {any[]} */
  const calls = [];
  const binding = bindingWithBackend({
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("stale_owner_generation"),
          })
        : new Response("retried"),
    }),
  });

  const response = await bindingFetch(binding, "room-race", new Request("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  }));

  assert.equal(await response.text(), "retried");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[1].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), null);
  assert.equal(new Headers(calls[1].init.headers).get("x-wdl-do-accept-owner-hint"), null);
});

test("DO-to-DO fetch ignores owner hints attached to race responses", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
        response: () => calls.length === 1
          ? Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
              status: 503,
              headers: doOwnershipErrorHeaders(
                "stale_owner_generation",
                doOwnerHintResponse({ ownerKey: ownerKeyFor("room-a") }).headers
              ),
            })
          : new Response("retried"),
      }),
    });

    const response = await bindingFetch(binding, "room-a", new Request("https://demo.workers.example/send"));

    assert.equal(await response.text(), "retried");
    assert.equal(calls.length, 2);
    assert.equal(ownerCalls.length, 0);
    assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), null);
    assert.equal(new Headers(calls[1].init.headers).get("x-wdl-do-accept-owner-hint"), null);
  }, {
    response: new Response("owner-should-not-be-called"),
  });
});

test("DO-to-DO RPC retries owner claim races without hint opt-in", async () => {
  /** @type {any[]} */
  const calls = [];
  const binding = bindingWithBackend({
    fetch: makeRecordingFetch(calls, {
      response: () => calls.length === 1
        ? Response.json({ error: "owner_claim_raced", message: "retry" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("owner_claim_raced"),
          })
        : Response.json({ ok: true, result: "retried-rpc" }),
    }),
  });

  const result = await binding.rpcObject("room-rpc-race", "addMessage", ["hello"]);

  assert.equal(result, "retried-rpc");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[1].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), null);
  assert.equal(new Headers(calls[1].init.headers).get("x-wdl-do-accept-owner-hint"), null);
});

test("DO-to-DO RPC rejects non-JSON data before transport", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      throw new Error("backend should not be called");
    },
  });

  await assert.rejects(binding.rpcObject("room-a", "save", [new Map([["key", "value"]])]), /plain JSON object/);
  await assert.rejects(binding.rpcObject("room-a", "save", [{ value: undefined }]), /rpc\.args\[0\]\.value must be JSON data/);
});

test("DO-to-DO RPC rejects invalid and reserved methods before transport", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      throw new Error("backend should not be called");
    },
  });

  await assert.rejects(binding.rpcObject("room-a", "not-valid-method", []), /rpc\.method is not valid/);
  await assert.rejects(binding.rpcObject("room-a", "fetch", []), /rpc\.method is reserved/);
});

test("DO-to-DO RPC throws structured do-runtime errors", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      return Response.json({
        error: "do_rpc_method_not_found",
        message: "missing",
        stack: "Error: missing\n    at remote-do",
      }, { status: 404 });
    },
  });

  await assert.rejects(
    binding.rpcObject("room-structured-error", "missing", []),
    (err) => err instanceof Error &&
      Reflect.get(err, "code") === "do_rpc_method_not_found" &&
      err.message === "missing" &&
      err.stack === "Error: missing\n    at remote-do"
  );
});

test("DO-to-DO websocket does not fall back to router when direct owner hint retry fails", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
        response: doOwnerHintResponse({ ownerKey: ownerKeyFor("room-a") }),
      }),
    });

    const response = await bindingFetch(binding, "room-a", new Request("https://demo.workers.example/ws", {
      headers: {
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Key": "abc",
      },
    }));

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "owner_unavailable");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 1);
    assert.equal(calls[0].url, "http://do-runtime/internal/do/connect");
  }, {
    response: async () => {
      throw new Error("owner unavailable");
    },
  });
});

test("DO-to-DO websocket fails closed on an invalid successful ownership marker", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      return new Response("upgrade", {
        headers: {
          ...doOwnerMetadataHeaders(),
          "x-wdl-do-ownership-error": "owner_fence_missing",
        },
      });
    },
  });

  const response = await bindingFetch(binding, "room-a", new Request("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  }));

  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), {
    error: "owner_unavailable",
    message: "Durable Object ownership is unavailable",
  });
  assert.equal(response.headers.get("x-wdl-do-owner-key"), null);
  assert.equal(response.headers.get("x-wdl-do-owner-hint"), null);
  assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
});

test("binding-scoped fetch fixes DO identity and strips its private envelope", async () => {
  /** @type {any[]} */
  const calls = [];
  const binding = bindingWithBackend({
    fetch: makeRecordingFetch(calls, { response: new Response("upgrade") }),
  });
  const tenantRequest = new Request("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
      "x-wdl-do-ns": "attacker",
    },
  });
  const response = await binding.fetch(scopedDoRequest(
    " room-scoped ",
    tenantRequest,
    "rid-scoped"
  ));

  assert.equal(await response.text(), "upgrade");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/connect");
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-ns"), "tenant");
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-object-name"), "%20room-scoped%20");
  assert.equal(new Headers(calls[0].init.headers).get("x-request-id"), "rid-scoped");
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-binding-object-name"), null);
});
