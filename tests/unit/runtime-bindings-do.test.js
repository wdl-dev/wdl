import assert from "node:assert/strict";
import { test } from "node:test";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { decodeDoEnvelopeMetadata as decodeDoEnvelope } from "../helpers/do-envelope.js";
import {
  doOwnerHintHeaders,
  doOwnerHintResponse,
  doOwnershipErrorHeaders,
  tenantBodyDoOwnerHintResponse,
} from "../helpers/do-owner-hint.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { makeRecordingFetch, withRecordingFetch } from "../helpers/mock-fetch.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const transportUrl = repositoryFileUrl("runtime/_wdl-do-transport.js");
const internalAuthUrl = sharedInternalAuthUrl();
const ownerHintCacheUrl = repositoryFileUrl("runtime/_wdl-owner-hint-cache.js");

const { DurableObjectNamespace } = await importRepositoryModule("runtime/bindings/do.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [/from "runtime-do-transport";/, `from ${JSON.stringify(transportUrl)};`],
  [/from "runtime-owner-hint-cache";/, `from ${JSON.stringify(ownerHintCacheUrl)};`],
  [/from "shared-internal-auth";/, `from ${JSON.stringify(internalAuthUrl)};`],
]);
const { connectHeaders, ownerHintFromHeaders, requestSpec } = await import(transportUrl);

/** @param {any} backend */
function bindingWithBackend(backend) {
  return new DurableObjectNamespace({
    props: {
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
    },
  }, {
    DO_BACKEND: backend,
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
  });
}

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

test("DO-to-DO fetch does not replay through router when direct owner hint retry fails", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, {
        response: () => calls.length === 1 ? doOwnerHintResponse() : new Response("router-ok"),
      }),
    });

    const response = await binding.fetchObject("room-a", new Request("https://demo.workers.example/send", {
      method: "POST",
      body: "hello",
    }));

    const body = await readJsonResponse(response, 503);
    assert.equal(body.error, "owner_unavailable");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 1);
    assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
    assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), "1");
  }, {
    response: async () => {
      throw new Error("owner unavailable");
    },
  });
});

test("DO-to-DO fetch caches owner hints and skips router on later calls", async () => {
  /** @type {any[]} */
  const calls = [];
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const ownerCalls = [];
  await withRecordingFetch(ownerCalls, async () => {
    const binding = bindingWithBackend({
      fetch: makeRecordingFetch(calls, { response: doOwnerHintResponse() }),
    });

    const first = await binding.fetchObject("cached-room", new Request("https://demo.workers.example/send"));
    const second = await binding.fetchObject("cached-room", new Request("https://demo.workers.example/send"));

    assert.equal(await first.text(), "owner-ok");
    assert.equal(await second.text(), "owner-ok");
    assert.equal(calls.length, 1);
    assert.equal(ownerCalls.length, 2);
    assert.equal(ownerCalls[0].url, "http://do-runtime-a:8788/internal/do/invoke");
    assert.equal(ownerCalls[1].url, "http://do-runtime-a:8788/internal/do/invoke");
    assert.equal(new Headers(ownerCalls[1].init.headers).get("x-wdl-internal-auth"), "test-internal-auth-token");
  }, {
    response: new Response("owner-ok", { headers: doOwnerHintHeaders() }),
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

    const response = await binding.fetchObject("room-ignore-race-hint", new Request("https://demo.workers.example/send"));

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

    const response = await binding.fetchObject("room-ignore-race-hint-response", new Request("https://demo.workers.example/send"));

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

  const response = await binding.fetchObject("room-race", new Request("https://demo.workers.example/send", {
    method: "POST",
    body: "hello",
  }));

  assert.equal(await response.text(), "retried");
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[1].url, "http://do-runtime/internal/do/invoke");
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), "1");
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
                doOwnerHintResponse().headers
              ),
            })
          : new Response("retried"),
      }),
    });

    const response = await binding.fetchObject("room-a", new Request("https://demo.workers.example/send"));

    assert.equal(await response.text(), "retried");
    assert.equal(calls.length, 2);
    assert.equal(ownerCalls.length, 0);
    assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), "1");
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
  assert.equal(new Headers(calls[0].init.headers).get("x-wdl-do-accept-owner-hint"), "1");
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
      fetch: makeRecordingFetch(calls, { response: doOwnerHintResponse() }),
    });

    const response = await binding.fetchObject("room-a", new Request("https://demo.workers.example/ws", {
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

test("DO-to-DO websocket strips owner hint headers from successful upgrades", async () => {
  const binding = bindingWithBackend({
    async fetch() {
      return new Response("upgrade", {
        headers: {
          "x-wdl-do-owner-key": "do_0123456789abcdef0123456789abcdef:Room:shard0",
          "x-wdl-do-owner-task-id": "do-runtime-a",
          "x-wdl-do-owner-endpoint": "do-runtime-a:8788",
          "x-wdl-do-owner-generation": "3",
          "x-wdl-do-owner-hint": "1",
          "x-wdl-do-ownership-error": "owner_fence_missing",
        },
      });
    },
  });

  const response = await binding.fetchObject("room-a", new Request("https://demo.workers.example/ws", {
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
    },
  }));

  assert.equal(await response.text(), "upgrade");
  assert.equal(response.headers.get("x-wdl-do-owner-key"), null);
  assert.equal(response.headers.get("x-wdl-do-owner-hint"), null);
  assert.equal(response.headers.get("x-wdl-do-ownership-error"), null);
});
