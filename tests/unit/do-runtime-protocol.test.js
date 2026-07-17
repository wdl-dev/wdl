import assert from "node:assert/strict";
import { test } from "node:test";

import { decodeDoEnvelope } from "../helpers/do-envelope.js";
import { loadDoProtocol } from "../helpers/load-do-protocol.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";
import {
  dispatchDoInvokeWithHintCache,
  retryableOwnerRaceResponse,
  staleDoOwnerHintResponse,
} from "../../runtime/_wdl-do-transport.js";
import { DO_HOST_SHARD_COUNT, MAX_ID_BYTES } from "../../do-runtime/protocol/wire-grammar.js";
import {
  doOwnerHintHeaders,
  doOwnershipErrorHeaders,
} from "../helpers/do-owner-hint.js";

const {
  DoRuntimeError,
  DO_INVOKE_CONTENT_TYPE,
  DO_OWNERSHIP_ERROR_CONTROL_HEADER,
  DO_OWNERSHIP_CODE,
  buildFacetName,
  buildAlarmRequest,
  buildForwardRequest,
  buildRpcRequest,
  encodeDoInvokeRequest,
  buildLocalActorRequest,
  doErrorResponse,
  doPlatformErrorResponse,
  hostIdForObject,
  hostIdForShard,
  normalizeDoConnectRequest,
  normalizeDoInvokeRequest,
  readDoInvokeRequest,
  readLocalActorInvokeRequest,
  readJsonBody,
} = await loadDoProtocol();
const versionFixture = readRepositoryJson("tests/fixtures/version-tags.json");
const doAlarmIdentityFixture = /** @type {any} */ (
  readRepositoryJson("tests/fixtures/do-alarm-identity.json")
);

test("DO ownership errors stay aligned with runtime hint and retry handling", () => {
  const ownershipCodes = new Set(Object.values(DO_OWNERSHIP_CODE));
  /** @type {Set<string>} */
  const ownerRaceRetryCodes = new Set([
    DO_OWNERSHIP_CODE.STALE_OWNER_GENERATION,
    DO_OWNERSHIP_CODE.OWNER_CLAIM_RACED,
    DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING,
    DO_OWNERSHIP_CODE.OWNER_LEASE_EXPIRED,
    DO_OWNERSHIP_CODE.STALE_OWNER_STORAGE,
    DO_OWNERSHIP_CODE.OWNER_LEASE_TOO_SHORT,
    DO_OWNERSHIP_CODE.OWNER_RENEW_RACED,
    DO_OWNERSHIP_CODE.TASK_DRAINING,
  ]);
  for (const code of ownershipCodes) {
    const untrusted = Response.json({ error: code }, { status: 503 });
    assert.equal(staleDoOwnerHintResponse(untrusted), false, code);
    assert.equal(retryableOwnerRaceResponse(untrusted), false, code);

    const response = Response.json({ error: code }, {
      status: 503,
      headers: doOwnershipErrorHeaders(code),
    });
    assert.equal(staleDoOwnerHintResponse(response), true, code);
    assert.equal(
      retryableOwnerRaceResponse(response),
      ownerRaceRetryCodes.has(code),
      code
    );
  }
});

test("DO invoke dispatch does not cache hints carried by owner-race responses", async () => {
  /** @type {Array<{ url: string, init: RequestInit | undefined }>} */
  const routerCalls = [];
  /** @type {unknown[]} */
  const cachedHints = [];
  const init = {
    method: "POST",
    headers: { "x-wdl-do-accept-owner-hint": "1" },
  };

  const response = await dispatchDoInvokeWithHintCache({
    async routerFetch(url, requestInit) {
      routerCalls.push({ url, init: requestInit });
      return routerCalls.length === 1
        ? Response.json({ error: "stale_owner_generation", message: "owner moved" }, {
            status: 503,
            headers: doOwnershipErrorHeaders("stale_owner_generation", doOwnerHintHeaders()),
          })
        : new Response("retried");
    },
    routerUrl: "http://do-runtime/internal/do/invoke",
    async ownerFetch() {
      assert.fail("owner endpoint must not receive an owner-race response hint");
    },
    ownerPath: "/internal/do/invoke",
    init,
    cache: {
      get() {
        return null;
      },
      set(_key, hint) {
        cachedHints.push(hint);
      },
      delete() {},
    },
    hintKey: "room-a",
  });

  assert.equal(await response.text(), "retried");
  assert.equal(routerCalls.length, 2);
  assert.deepEqual(cachedHints, []);
  assert.equal(new Headers(routerCalls[1].init?.headers).get("x-wdl-do-accept-owner-hint"), null);
});

const DO_STORAGE_ID = "do_0123456789abcdef0123456789abcdef";
const CHAT_ROOM_HOST_ID = `${DO_STORAGE_ID}:ChatRoom:shard12`;

const BASE_BODY = {
  ns: "tenant",
  worker: "chat",
  version: "v1",
  doStorageId: DO_STORAGE_ID,
  className: "ChatRoom",
  objectName: "room-a",
  props: { ns: "tenant" },
  request: {
    method: "POST",
    url: "https://demo.workers.example/messages",
    headers: { "content-type": "text/plain" },
  },
};

/**
 * @param {any} invoke
 * @param {Uint8Array} bodyBytes
 */
function withRequestBody(invoke, bodyBytes) {
  assert.ok("request" in invoke);
  return {
    ...invoke,
    request: {
      ...invoke.request,
      bodyBytes: bodyBytes instanceof Uint8Array ? bodyBytes : Buffer.from(bodyBytes),
    },
  };
}

test("normalizes do invoke request", () => {
  const invoke = normalizeDoInvokeRequest(BASE_BODY);

  assert.equal(invoke.hostId, CHAT_ROOM_HOST_ID);
  assert.equal(invoke.hostId, hostIdForObject(DO_STORAGE_ID, "ChatRoom", "room-a"));
  assert.equal(invoke.workerId, "tenant:chat:v1");
  assert.equal(invoke.className, "ChatRoom");
  assert.deepEqual(invoke.props, {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: DO_STORAGE_ID,
    className: "ChatRoom",
  });
  assert.equal("workerCode" in invoke ? invoke.workerCode : undefined, undefined);
  assert.ok("request" in invoke);
  assert.deepEqual(invoke.request.headers, [["content-type", "text/plain"]]);
  assert.equal(buildFacetName(invoke), "ChatRoom:room-a");
});

test("DO invoke version ingress matches the shared JS/Rust fixture", () => {
  for (const { tag, parsed } of versionFixture.cases) {
    if (parsed == null) {
      assert.throws(
        () => normalizeDoInvokeRequest({ ...BASE_BODY, version: tag }),
        /version/,
        tag
      );
    } else {
      const invoke = normalizeDoInvokeRequest({ ...BASE_BODY, version: tag });
      assert.ok("version" in invoke, tag);
      assert.equal(invoke.version, tag, tag);
    }
  }
});

test("normalizes do invoke requests for runtime-load reserved namespaces", () => {
  for (const ns of ["__system__", "__platform__"]) {
    const invoke = normalizeDoInvokeRequest({
      ...BASE_BODY,
      ns,
    });

    assert.equal(invoke.workerId, `${ns}:chat:v1`);
    assert.deepEqual(invoke.props, {
      ns,
      worker: "chat",
      version: "v1",
      doStorageId: DO_STORAGE_ID,
      className: "ChatRoom",
    });
  }
});

test("DO invoke namespace ingress uses the canonical runtime-load grammar", () => {
  for (const ns of ["-tenant", "tenant-", "a".repeat(64), "admin", "__community__"]) {
    assert.throws(
      () => normalizeDoInvokeRequest({ ...BASE_BODY, ns }),
      /ns is not valid/,
      ns
    );
  }
});

test("normalizes do invoke request for mixed-case worker names", () => {
  const invoke = normalizeDoInvokeRequest({
    ...BASE_BODY,
    worker: "My_Worker-2",
  });

  assert.equal(invoke.workerId, "tenant:My_Worker-2:v1");
  assert.equal(invoke.props.worker, "My_Worker-2");
});

test("normalizes owner fence and ignores caller supplied props", () => {
  const invoke = normalizeDoInvokeRequest({
    ...BASE_BODY,
    props: { ns: "attacker", callerSecrets: { token: "nope" } },
    owner: {
      ownerKey: CHAT_ROOM_HOST_ID,
      taskId: "task-a",
      generation: 3,
    },
  });

  assert.deepEqual(invoke.owner, {
    ownerKey: CHAT_ROOM_HOST_ID,
    taskId: "task-a",
    generation: 3,
  });
  assert.deepEqual(invoke.props, {
    ns: "tenant",
    worker: "chat",
    version: "v1",
    doStorageId: DO_STORAGE_ID,
    className: "ChatRoom",
  });
});

test("normalizes do alarm invoke request", async () => {
  const invoke = normalizeDoInvokeRequest({
    ...BASE_BODY,
    kind: "alarm",
    alarm: { retryCount: 2, isRetry: true, token: "alarm-token" },
  });

  assert.equal(invoke.kind, "alarm");
  assert.equal(invoke.hostId, CHAT_ROOM_HOST_ID);
  assert.ok("alarm" in invoke);
  assert.deepEqual(invoke.alarm, { retryCount: 2, isRetry: true, token: "alarm-token" });
  assert.equal("request" in invoke ? invoke.request : undefined, undefined);

  const request = buildAlarmRequest(invoke.alarm, "rid-alarm");
  assert.equal(request.method, "POST");
  assert.equal(request.headers.get("x-wdl-do-internal-alarm"), "1");
  assert.equal(request.headers.get("x-request-id"), "rid-alarm");
  assert.deepEqual(await request.json(), { retryCount: 2, isRetry: true, token: "alarm-token" });
});

test("normalizes do rpc invoke request", () => {
  const invoke = normalizeDoInvokeRequest({
    ...BASE_BODY,
    kind: "rpc",
    rpc: {
      method: "addMessage",
      args: ["hello", { role: "user" }],
    },
  });

  assert.equal(invoke.kind, "rpc");
  assert.equal(invoke.hostId, CHAT_ROOM_HOST_ID);
  assert.equal("request" in invoke, false);
  assert.deepEqual("rpc" in invoke ? invoke.rpc : null, {
    method: "addMessage",
    args: ["hello", { role: "user" }],
  });
});

test("builds private rpc dispatch requests", async () => {
  const request = buildRpcRequest({
    method: "addMessage",
    args: ["hello", { role: "user" }],
  }, "rid-rpc");

  assert.equal(request.method, "POST");
  assert.equal(request.url, "https://do.internal/__wdl_rpc");
  assert.equal(request.headers.get("x-wdl-do-internal-rpc"), "1");
  assert.equal(request.headers.get("x-request-id"), "rid-rpc");
  assert.deepEqual(await request.json(), {
    method: "addMessage",
    args: ["hello", { role: "user" }],
  });
});

test("rejects invalid do rpc shapes", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "not-valid-method", args: [] },
    }),
    /rpc\.method is not valid/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "fetch", args: [] },
    }),
    /rpc\.method is reserved/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "addMessage", args: "hello" },
    }),
    /rpc\.args must be an array/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "addMessage", args: ["x".repeat(1024 * 1024 + 1)] },
    }),
    /rpc\.args exceeds 1048576 bytes/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "addMessage", args: [new Map([["key", "value"]])] },
    }),
    /rpc\.args\[0\] must be a plain JSON object/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      kind: "rpc",
      rpc: { method: "addMessage", args: [{ fn() {} }] },
    }),
    /rpc\.args\[0\]\.fn must be JSON data/
  );
});

test("rejects inline workerCode", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      workerCode: {
        mainModule: "worker.js",
        modules: { "worker.js": "export class ChatRoom {}" },
      },
    }),
    /workerCode is not accepted/
  );
});

test("builds forwarded Request for user durable object fetch", async () => {
  const invoke = withRequestBody(normalizeDoInvokeRequest({
    ...BASE_BODY,
    request: {
      ...BASE_BODY.request,
      headers: {
        ...BASE_BODY.request.headers,
        "x-wdl-do-internal-alarm": "1",
        "x-wdl-do-internal-rpc": "1",
        "x-wdl-internal-auth": "platform-token",
      },
    },
  }), new TextEncoder().encode("hello"));
  assert.ok("request" in invoke);
  const request = buildForwardRequest(invoke.request);

  assert.equal(request.method, "POST");
  assert.equal(request.url, "https://demo.workers.example/messages");
  assert.equal(request.headers.get("content-type"), "text/plain");
  assert.equal(request.headers.get("x-wdl-do-internal-alarm"), null);
  assert.equal(request.headers.get("x-wdl-do-internal-rpc"), null);
  assert.equal(request.headers.get("x-wdl-internal-auth"), null);
  assert.equal(await request.text(), "hello");
});

test("normalizes DO connect without exposing internal auth to tenant code", () => {
  const invoke = normalizeDoConnectRequest(new Request("https://do-runtime.internal/connect", {
    method: "GET",
    headers: {
      "x-wdl-do-ns": "tenant",
      "x-wdl-do-worker": "chat",
      "x-wdl-do-version": "v1",
      "x-wdl-do-storage-id": DO_STORAGE_ID,
      "x-wdl-do-class-name": "ChatRoom",
      "x-wdl-do-object-name": "room-a",
      "x-wdl-do-request-url": "https://demo.workers.example/ws",
      "x-wdl-internal-auth": "platform-token",
      "x-wdl-do-ownership-error": "stale_owner_generation",
      "x-tenant-visible": "ok",
    },
  }));

  assert.equal(invoke.kind, "fetch");
  assert.ok("request" in invoke);
  assert.deepEqual(invoke.request.headers, [["x-tenant-visible", "ok"]]);
});

test("builds forwarded Request with binary body bytes", async () => {
  const bytes = Buffer.from([0, 0xff, 0x61]);
  const invoke = withRequestBody(normalizeDoInvokeRequest(BASE_BODY), bytes);
  assert.ok("request" in invoke);
  const request = buildForwardRequest(invoke.request);
  assert.deepEqual([...new Uint8Array(await request.arrayBuffer())], [0, 0xff, 0x61]);
});

test("local actor requests preserve body bytes without base64 in metadata", async () => {
  const bytes = Buffer.from([0, 0xff, 0x61]);
  const invoke = withRequestBody(normalizeDoInvokeRequest({
    ...BASE_BODY,
    owner: {
      ownerKey: CHAT_ROOM_HOST_ID,
      taskId: "task-a",
      generation: 7,
    },
  }), bytes);

  const actorRequest = buildLocalActorRequest("https://do-runtime.internal/invoke", invoke, "req-1");
  assert.equal(actorRequest.headers.get("x-request-id"), "req-1");
  assert.equal(actorRequest.headers.get("x-wdl-do-local-envelope"), "binary");
  const envelope = new Uint8Array(await actorRequest.clone().arrayBuffer());
  const { metadata, bodyBytes } = /** @type {{ metadata: { request: { bodyBase64?: unknown } }, bodyBytes: Uint8Array }} */ (
    decodeDoEnvelope(envelope)
  );
  assert.equal(metadata.request.bodyBase64, undefined);
  assert.deepEqual([...bodyBytes], [0, 0xff, 0x61]);

  const localInvoke = await readLocalActorInvokeRequest(actorRequest);
  assert.ok("request" in localInvoke);
  assert.equal(localInvoke.request.bodyBase64, undefined);
  assert.ok("bodyBytes" in localInvoke.request);
  const localRequest = /** @type {{ bodyBytes: Uint8Array }} */ (/** @type {unknown} */ (localInvoke.request));
  assert.deepEqual([...localRequest.bodyBytes], [0, 0xff, 0x61]);
  const request = buildForwardRequest(localInvoke.request);
  assert.deepEqual([...new Uint8Array(await request.arrayBuffer())], [0, 0xff, 0x61]);
});

test("do invoke endpoint reads binary envelopes and rejects JSON", async () => {
  const invoke = withRequestBody(normalizeDoInvokeRequest(BASE_BODY), Buffer.from([0, 0xff, 0x61]));
  const request = new Request("http://do-runtime/internal/do/invoke", {
    method: "POST",
    headers: { "content-type": DO_INVOKE_CONTENT_TYPE },
    body: encodeDoInvokeRequest(invoke),
  });
  const decoded = await readDoInvokeRequest(request);
  assert.ok("request" in decoded);
  assert.equal(decoded.request.bodyBase64, undefined);
  const decodedRequest = /** @type {{ bodyBytes: Uint8Array }} */ (/** @type {unknown} */ (decoded.request));
  assert.deepEqual([...decodedRequest.bodyBytes], [0, 0xff, 0x61]);

  await assert.rejects(
    readDoInvokeRequest(new Request("http://do-runtime/internal/do/invoke", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(BASE_BODY),
    })),
    /DO invoke endpoint requires/
  );
});

test("do invoke endpoint rejects oversized binary envelopes before decoding", async () => {
  await assert.rejects(
    readDoInvokeRequest(new Request("http://do-runtime/internal/do/invoke", {
      method: "POST",
      headers: { "content-type": DO_INVOKE_CONTENT_TYPE },
      body: new Uint8Array((2 * 1024 * 1024) + 1),
    })),
    (err) => err instanceof DoRuntimeError &&
      err.status === 413 &&
      err.code === "request_body_too_large"
  );
});

test("local actor requests reject legacy JSON metadata", async () => {
  await assert.rejects(
    readLocalActorInvokeRequest(new Request("https://do-runtime.internal/invoke", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(BASE_BODY),
    })),
    /DO host actor requests require the local envelope/
  );
});

test("normalizes do websocket connect request from internal headers", () => {
  const request = new Request("http://do-runtime/internal/do/connect", {
    method: "GET",
    headers: {
      Connection: "Upgrade",
      Upgrade: "websocket",
      "Sec-WebSocket-Key": "abc",
      "x-request-id": "rid-1",
      "x-wdl-do-ns": "tenant",
      "x-wdl-do-worker": "chat",
      "x-wdl-do-version": "v1",
      "x-wdl-do-storage-id": DO_STORAGE_ID,
      "x-wdl-do-class-name": "ChatRoom",
      "x-wdl-do-object-name": "room-a",
      "x-wdl-do-request-url": "https://demo.workers.example/room?name=room-a",
      "x-wdl-do-hop-count": "1",
      "x-wdl-do-accept-owner-hint": "1",
      "x-wdl-do-owner-key": CHAT_ROOM_HOST_ID,
      "x-wdl-do-owner-task-id": "task-a",
      "x-wdl-do-owner-endpoint": "do-runtime-a:8788",
      "x-wdl-do-owner-generation": "4",
      "x-wdl-do-owner-hint": "1",
      "x-wdl-do-ownership-error": "stale_owner_generation",
    },
  });
  const invoke = normalizeDoConnectRequest(request);

  assert.equal(invoke.kind, "fetch");
  assert.equal(invoke.hostId, CHAT_ROOM_HOST_ID);
  assert.equal(invoke.objectName, "room-a");
  assert.deepEqual(invoke.owner, {
    ownerKey: CHAT_ROOM_HOST_ID,
    taskId: "task-a",
    generation: 4,
  });
  assert.ok("request" in invoke);
  assert.equal(invoke.request.url, "https://demo.workers.example/room?name=room-a");
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-ns"), false);
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-accept-owner-hint"), false);
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-owner-generation"), false);
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-owner-endpoint"), false);
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-owner-hint"), false);
  assert.equal(invoke.request.headers.some(([name]) => name === "x-wdl-do-ownership-error"), false);
  assert.ok(invoke.request.headers.some(([name, value]) => name === "upgrade" && value === "websocket"));
  assert.ok(invoke.request.headers.some(([name, value]) => name === "x-request-id" && value === "rid-1"));
});

test("rejects invalid do alarm retry counts", () => {
  for (const retryCount of [-1, 1.5, "2", true, false, "", [], [2]]) {
    assert.throws(
      () => normalizeDoInvokeRequest({
        ...BASE_BODY,
        kind: "alarm",
        alarm: { retryCount },
      }),
      /alarm\.retryCount must be a non-negative integer/,
      `retryCount=${JSON.stringify(retryCount)}`
    );
  }
});

test("rejects invalid owner fence generation", () => {
  for (const generation of [-1, 0, 9007199254740992]) {
    assert.throws(
      () => normalizeDoInvokeRequest({
        ...BASE_BODY,
        owner: { ownerKey: CHAT_ROOM_HOST_ID, taskId: "task-a", generation },
      }),
      /owner\.generation must be a positive safe integer/
    );
  }
});

test("rejects invalid class names", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({ ...BASE_BODY, className: "not-valid-name" }),
    (err) => err instanceof DoRuntimeError && err.status === 400 && err.code === "invalid_request"
  );
  assert.throws(
    () => hostIdForShard(DO_STORAGE_ID, "not-valid-name", 0),
    (err) => err instanceof DoRuntimeError && err.status === 400 && err.code === "invalid_request"
  );
});

/** @param {string | { repeat: string, count: number }} value */
function fixtureString(value) {
  return typeof value === "string" ? value : value.repeat.repeat(value.count);
}

test("DO alarm ingress matches the cross-language identity fixture", () => {
  assert.equal(doAlarmIdentityFixture.maxBytes, MAX_ID_BYTES);
  assert.equal(doAlarmIdentityFixture.hostShardCount, DO_HOST_SHARD_COUNT);
  for (const entry of doAlarmIdentityFixture.cases) {
    const input = {
      ...BASE_BODY,
      kind: "alarm",
      doStorageId: fixtureString(entry.doStorageId),
      className: fixtureString(entry.className),
      objectName: fixtureString(entry.objectName),
      alarm: { retryCount: 0, token: fixtureString(entry.token) },
    };
    if (entry.valid) {
      assert.doesNotThrow(() => normalizeDoInvokeRequest(input), entry.name);
    } else {
      assert.throws(
        () => normalizeDoInvokeRequest(input),
        (err) => err instanceof DoRuntimeError &&
          err.status === 400 &&
          err.code === "invalid_request",
        entry.name
      );
    }
  }
});

test("rejects bare numeric versions", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({ ...BASE_BODY, version: "1" }),
    /version is not valid/
  );
});

test("rejects malformed, out-of-range, and mismatched host ids", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({ ...BASE_BODY, hostId: "tenant-worker" }),
    /hostId is not valid/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      hostId: `${DO_STORAGE_ID}:ChatRoom:shard16`,
    }),
    /hostId shard is not valid/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      hostId: `${DO_STORAGE_ID}:ChatRoom:shard012`,
    }),
    /hostId is not valid/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      hostId: `${DO_STORAGE_ID}:ChatRoom:shard0`,
    }),
    /hostId does not match object shard/
  );
});

test("generated host ids honor the aggregate protocol size limit", () => {
  const maxHostId = hostIdForShard("a".repeat(496), "ChatRoom", 0);
  assert.equal(Buffer.byteLength(maxHostId), 512);
  assert.throws(
    () => hostIdForShard("a".repeat(497), "ChatRoom", 0),
    /hostId is too large/
  );
});

test("shards object names using UTF-8 bytes", () => {
  assert.equal(hostIdForObject(DO_STORAGE_ID, "ChatRoom", "中文"), `${DO_STORAGE_ID}:ChatRoom:shard5`);
  assert.equal(hostIdForObject(DO_STORAGE_ID, "ChatRoom", "🚀"), `${DO_STORAGE_ID}:ChatRoom:shard10`);
});

test("rejects request bodies in invoke metadata", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: { method: "POST", url: "https://demo.workers.example/", bodyBase64: "bm9wZQ==" },
    }),
    /request\.bodyBase64 is not supported in invoke metadata/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: { method: "POST", url: "https://demo.workers.example/", bodyText: "nope" },
    }),
    /request\.bodyText is not supported in invoke metadata/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: {
        method: "POST",
        url: "https://demo.workers.example/",
        bodyBytes: new Uint8Array([1]),
      },
    }),
    /request\.bodyBytes is not supported in invoke metadata/
  );
});

test("rejects non-string header values", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: {
        method: "POST",
        url: "https://demo.workers.example/",
        headers: { "x-test": 1 },
      },
    }),
    /request\.headers\.x-test must be a string/
  );
});

test("rejects too many request headers", () => {
  const headers = Object.fromEntries(
    Array.from({ length: 129 }, (_, index) => [`x-test-${index}`, "value"])
  );

  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: {
        method: "POST",
        url: "https://demo.workers.example/",
        headers,
      },
    }),
    /request\.headers must not exceed 128 entries/
  );
});

test("rejects aggregate request header bytes over the protocol budget", () => {
  const headers = Object.fromEntries(
    Array.from({ length: 128 }, (_, index) => [`x-test-${index}`, "x".repeat(600)])
  );

  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: {
        method: "POST",
        url: "https://demo.workers.example/",
        headers,
      },
    }),
    /request\.headers exceeds 65536 bytes/
  );
});

test("allows empty header values", () => {
  const invoke = normalizeDoInvokeRequest({
    ...BASE_BODY,
    request: {
      method: "POST",
      url: "https://demo.workers.example/",
      headers: { "x-empty": "" },
    },
  });
  assert.ok("request" in invoke);
  assert.deepEqual(invoke.request.headers, [["x-empty", ""]]);
});

test("rejects invalid header names and control characters", () => {
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      request: {
        method: "POST",
        url: "https://demo.workers.example/",
        headers: { "bad header": "value" },
      },
    }),
    /request header name "bad header" is not valid/
  );
  assert.throws(
    () => normalizeDoInvokeRequest({
      ...BASE_BODY,
      objectName: "room\u0000a",
    }),
    /objectName must not contain control characters/
  );
});

test("rejects unpaired surrogates in DO object identities", () => {
  for (const objectName of ["\ud800", "\udc00"]) {
    assert.throws(
      () => normalizeDoInvokeRequest({ ...BASE_BODY, objectName }),
      /objectName must contain well-formed Unicode/,
    );
    assert.throws(
      () => hostIdForObject(BASE_BODY.doStorageId, BASE_BODY.className, objectName),
      /objectName must contain well-formed Unicode/,
    );
  }
});

test("maps invalid JSON bodies to protocol errors", async () => {
  await assert.rejects(
    readJsonBody(new Request("https://demo.workers.example/", { method: "POST", body: "{" })),
    (err) => err instanceof DoRuntimeError && err.status === 400 && err.code === "invalid_json"
  );
});

test("rejects oversized JSON bodies before parsing", async () => {
  await assert.rejects(
    readJsonBody(new Request("https://demo.workers.example/", {
      method: "POST",
      body: "x".repeat((2 * 1024 * 1024) + 1),
    })),
    (err) => err instanceof DoRuntimeError &&
      err.status === 413 &&
      err.code === "request_body_too_large"
  );
});

test("do runtime error responses keep internal exception messages out of the wire body", async () => {
  const response = doErrorResponse(new Error("redis password leaked in stack"));
  await assertJsonResponse(response, 500, {
    error: "internal_error",
    message: "Internal error",
  });
});

test("do runtime ownership errors carry a private control marker", async () => {
  const ownershipResponse = doPlatformErrorResponse(new DoRuntimeError(
    503,
    DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING,
    "DO owner fence is missing"
  ));
  assert.equal(
    ownershipResponse.headers.get(DO_OWNERSHIP_ERROR_CONTROL_HEADER),
    DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING
  );

  const tenantResponse = doPlatformErrorResponse(new DoRuntimeError(
    503,
    "tenant_failure",
    "Tenant failure"
  ));
  assert.equal(
    tenantResponse.headers.get(DO_OWNERSHIP_ERROR_CONTROL_HEADER),
    null
  );

  const forgedResponse = doPlatformErrorResponse({
    status: 503,
    code: DO_OWNERSHIP_CODE.OWNER_FENCE_MISSING,
    message: "forged ownership error",
  });
  assert.equal(
    forgedResponse.headers.get(DO_OWNERSHIP_ERROR_CONTROL_HEADER),
    null
  );
});

test("do runtime error responses keep top-level fields stable while preserving nested details", async () => {
  const response = doErrorResponse(new DoRuntimeError(503, "owner_unavailable", "DO owner is unavailable", {
    error: "socket leaked",
    message: "connect ECONNREFUSED 10.0.0.1",
    reason: "legacy",
    upstreamStatus: 503,
    nested: {
      error: "nested",
      message: "nested message",
      value: "kept",
    },
  }));

  await assertJsonResponse(response, 503, {
    error: "owner_unavailable",
    message: "DO owner is unavailable",
    details: {
      upstreamStatus: 503,
      nested: {
        error: "nested",
        message: "nested message",
        value: "kept",
      },
    },
  });
});
