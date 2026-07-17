import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";

import { doProtocolDataUrl } from "../helpers/load-do-protocol.js";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { readJsonResponse } from "../helpers/response-json.js";
import {
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryJson,
  readRepositoryModuleSource,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

/** @type {any} */ (globalThis).__doIndexHostResponse = null;
/** @type {any} */ (globalThis).__doIndexOwner = null;
/** @type {any} */ (globalThis).__doIndexOwnerError = null;
/** @type {any} */ (globalThis).__doIndexProbeOwner = null;
/** @type {any} */ (globalThis).__doIndexRenewResult = null;
/** @type {any} */ (globalThis).__doIndexHostFetches = null;

/** @param {string} src */
function stub(src) {
  return moduleDataUrl(src);
}

const protocolUrl = doProtocolDataUrl();
const sharedRespondUrl = repositoryFileUrl("shared/respond.js");
const sharedEnvUrl = repositoryFileUrl("shared/env.js");
const sharedOptimisticRetryUrl = repositoryFileUrl("shared/optimistic-retry.js");
const sharedOwnerLeaseUrl = repositoryModuleDataUrl("shared/owner-lease.js", [
  [/from "shared-env";/, `from ${JSON.stringify(sharedEnvUrl)};`],
  [/from "shared-optimistic-retry";/, `from ${JSON.stringify(sharedOptimisticRetryUrl)};`],
]);
const httpUrl = stub(readRepositoryModuleSource("do-runtime/http.js", importSpecifierReplacements({
  "shared-respond": sharedRespondUrl,
})));
const requestScopeUrl = stub(`
export function createHttpRequestScope({ request }) {
  return {
    requestId: request.headers.get("x-request-id") || null,
    respond(response) { return response; },
    markError() {},
    complete() {},
  };
}
`);
const workerIdUrl = repositoryFileUrl("shared/worker-id.js");
const workerContractUrl = repositoryFileUrl("shared/worker-contract.js");
const actorUrl = stub(`export class WdlDoHostActor {}`);
const alarmDispatchUrl = stub(readRepositoryModuleSource("do-runtime/alarm-dispatch.js", importSpecifierReplacements({
  "shared-worker-id": workerIdUrl,
  "shared-worker-contract": workerContractUrl,
  "do-runtime-protocol": protocolUrl,
  "do-runtime-http": httpUrl,
})));
const objectRegistryUrl = stub(`
export function parseObjectRegistryMember(member) {
  if (member === "Room:room-a:0") return { className: "Room", objectName: "room-a", shard: 0 };
  return null;
}
`);
const stateUrl = stub(`
export const SERVICE = "do-runtime";
export const ownedScopes = new Map();
export const metrics = { renderPrometheus() { return "# HELP do_runtime_test_metric\\n"; } };
let draining = false;
export function currentInFlightDispatches() {
  return /** @type {any} */ (globalThis).__doIndexCurrentInFlightDispatches || 0;
}
export function isDraining() { return draining; }
export function log() {}
export async function recordDoInvoke(_kind, fn) { return await fn(); }
export async function recordDoWebSocketUpgrade(fn) { return await fn(); }
export function setDraining(value) { draining = value === true; }
export async function waitForInFlightDispatches() {
  return /** @type {any} */ (globalThis).__doIndexWaitForInFlightResult || { drained: true, inFlight: 0, waitedMs: 0 };
}
`);
const taskIdentityUrl = stub(`
export function peekTaskIdentity() {
  return { taskId: "task-a", endpoint: "do-runtime-a:8788" };
}
export async function resolveTaskIdentity() {
  return { taskId: "task-a", endpoint: "do-runtime-a:8788" };
}
`);
const ownerRegistryUrl = stub(`
import { DoRuntimeError } from ${JSON.stringify(protocolUrl)};
export async function drainOwnedScopes() { return { released: 0 }; }
export function ownerTtlSeconds() { return 120; }
export async function readOwner() { return /** @type {any} */ (globalThis).__doIndexProbeOwner; }
export async function renewOwnedScopes() {
  return /** @type {any} */ (globalThis).__doIndexRenewResult || { draining: false, owned: 0, renewed: 0, lost: 0, errors: [] };
}
export async function resolveDoOwner(_env, invoke) {
  if (/** @type {any} */ (globalThis).__doIndexOwnerError) throw /** @type {any} */ (globalThis).__doIndexOwnerError;
  if (/** @type {any} */ (globalThis).__doIndexOwner) return /** @type {any} */ (globalThis).__doIndexOwner;
  return { ownerKey: invoke.hostId, taskId: "task-a", generation: 1, endpoint: "do-runtime-a:8788" };
}
export { DoRuntimeError };
`);
const ownerClientUrl = stub(`
export function parseHopCount() { return 0; }
export async function forwardToOwner() { throw new Error("unexpected forward"); }
export async function forwardConnectToOwner() { throw new Error("unexpected connect forward"); }
`);
const doTransportUrl = repositoryFileUrl("runtime/_wdl-do-transport.js");
const emptyBindingUrl = stub(`
export class KV {}
export class Assets {}
export class ServiceBinding {}
export class QueueProducer {}
export class D1Database {}
export class R2Bucket {}
export class DurableObjectNamespace {}
export class DoAlarmBinding {}
export class InternalAuthBackend {}
`);

// do-runtime/index.js is intentionally tested as the real dispatcher with
// lightweight edge stubs. Keep this map in sync when index.js imports change.
const IMPORT_STUBS = {
  "shared-observability": OBSERVABILITY_NOOP_URL,
  "shared-internal-auth": sharedInternalAuthUrl(),
  "shared-owner-lease": sharedOwnerLeaseUrl,
  "shared-respond": sharedRespondUrl,
  "shared-request-scope": requestScopeUrl,
  "shared-worker-contract": workerContractUrl,
  "shared-worker-id": workerIdUrl,
  "do-runtime-actor": actorUrl,
  "do-runtime-alarm-dispatch": alarmDispatchUrl,
  "do-runtime-protocol": protocolUrl,
  "do-runtime-http": httpUrl,
  "do-runtime-object-registry": objectRegistryUrl,
  "do-runtime-state": stateUrl,
  "do-runtime-task-identity": taskIdentityUrl,
  "do-runtime-owner-registry": ownerRegistryUrl,
  "do-runtime-owner-client": ownerClientUrl,
  "runtime-do-transport": doTransportUrl,
  "runtime-bindings-kv": emptyBindingUrl,
  "runtime-bindings-assets": emptyBindingUrl,
  "runtime-bindings-service": emptyBindingUrl,
  "runtime-bindings-queue": emptyBindingUrl,
  "runtime-bindings-d1": emptyBindingUrl,
  "runtime-bindings-r2": emptyBindingUrl,
  "runtime-bindings-do": emptyBindingUrl,
  "runtime-bindings-internal-auth-backend": emptyBindingUrl,
  "do-runtime-alarm-binding": emptyBindingUrl,
};

const src = readRepositoryModuleSource("do-runtime/index.js", importSpecifierReplacements(IMPORT_STUBS));

const { default: app } = await import(stub(src));
const { DO_INVOKE_CONTENT_TYPE, DoRuntimeError, encodeDoInvokeRequest } = await import(protocolUrl);
const doState = await import(stateUrl);
const versionFixture = readRepositoryJson("tests/fixtures/version-tags.json");

beforeEach(() => {
  /** @type {any} */ (globalThis).__doIndexHostResponse = null;
  /** @type {any} */ (globalThis).__doIndexOwner = null;
  /** @type {any} */ (globalThis).__doIndexOwnerError = null;
  /** @type {any} */ (globalThis).__doIndexProbeOwner = null;
  /** @type {any} */ (globalThis).__doIndexRenewResult = null;
  /** @type {any} */ (globalThis).__doIndexHostFetches = [];
  /** @type {any} */ (globalThis).__doIndexCurrentInFlightDispatches = 0;
  /** @type {any} */ (globalThis).__doIndexWaitForInFlightResult = null;
  doState.ownedScopes.clear();
  doState.setDraining(false);
});

function env() {
  return {
    WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN,
    DO_HOSTS: {
      /** @param {string} name */
      idFromName(name) {
        return name;
      },
      get() {
        return {
          /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
          async fetch(input, init) {
            const hostFetches = /** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches);
            hostFetches.push({ input, init });
            return /** @type {any} */ (globalThis).__doIndexHostResponse || Response.json({ ok: true });
          },
        };
      },
    },
  };
}

/** @param {string} url @param {RequestInit} [init] */
function internalRequest(url, init = {}) {
  const headers = new Headers(init.headers);
  headers.set("x-wdl-internal-auth", TEST_INTERNAL_AUTH_TOKEN);
  return new Request(url, { ...init, headers });
}

/** @param {Response} response */
async function jsonBody(response) {
  return await response.json();
}

test("do-runtime metrics endpoint uses the shared Prometheus response contract", async () => {
  const response = await app.fetch(new Request("https://do-runtime/_metrics"), env());

  assert.equal(response.status, 200);
  assert.equal(
    response.headers.get("content-type"),
    "text/plain; version=0.0.4; charset=utf-8"
  );
  assert.equal(await response.text(), "# HELP do_runtime_test_metric\n");
});

test("do-runtime rejects private routes without valid internal auth token", async () => {
  for (const headers of [new Headers(), new Headers({ "x-wdl-internal-auth": "wrong" })]) {
    const response = await app.fetch(new Request("https://do-runtime/internal/do/renew", {
      method: "POST",
      headers,
    }), env());

    assert.equal(response.status, 401);
    assert.deepEqual(await jsonBody(response), {
      error: "internal_auth_failed",
      message: "Internal authentication failed",
    });
  }
});

test("do-runtime alarm dispatch endpoint invokes the local alarm shim path", async () => {
  /** @type {any} */ (globalThis).__doIndexHostResponse = Response.json({ ok: true, ignored: true });

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      ns: "tenant",
      worker: "alarms",
      version: "v7",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "alice",
      retryCount: 2,
      token: "row-token",
    }),
  }), env());

  assert.equal(response.status, 200);
  assert.deepEqual(await jsonBody(response), { ok: true, ignored: true });
  const hostFetches = /** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches);
  const [fetchCall] = hostFetches;
  assert.equal(fetchCall.input.url, "https://do-runtime.internal/invoke");
});

test("do-runtime alarm dispatch delegates object identity validation", async () => {
  const hostFetches = /** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches);
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      ns: "tenant",
      worker: "alarms",
      version: "v7",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "\ud800",
      retryCount: 0,
      token: "row-token",
    }),
  }), env());

  const body = await readJsonResponse(response, 400, "unpaired surrogate objectName");
  assert.equal(body.error, "invalid_request");
  assert.equal(hostFetches.length, 0);
});

test("do-runtime alarm dispatch requires retryCount and token before dispatch", async () => {
  const hostFetches = /** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches);
  const validBody = {
    ns: "tenant",
    worker: "alarms",
    version: "v7",
    doStorageId: "do_0123456789abcdef0123456789abcdef",
    className: "Room",
    objectName: "alice",
    retryCount: 0,
    token: "row-token",
  };
  for (const { label, field, value } of [
    { label: "missing retryCount", field: "retryCount", value: undefined },
    { label: "null retryCount", field: "retryCount", value: null },
    { label: "missing token", field: "token", value: undefined },
    { label: "null token", field: "token", value: null },
  ]) {
    const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...validBody, [field]: value }),
    }), env());

    const body = await readJsonResponse(response, 400, label);
    assert.equal(body.error, "invalid_request", label);
    assert.equal(hostFetches.length, 0, label);
  }
});

test("do-runtime alarm dispatch delegates retryCount validation before dispatch", async () => {
  const hostFetches = /** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches);
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      ns: "tenant",
      worker: "alarms",
      version: "v7",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "alice",
      retryCount: "2",
      token: "row-token",
    }),
  }), env());

  const body = await readJsonResponse(response, 400, "non-number retryCount");
  assert.equal(body.error, "invalid_request");
  assert.equal(hostFetches.length, 0);
});

test("do-runtime alarm dispatch rejects non-canonical versions from the shared fixture", async () => {
  for (const { tag, parsed } of versionFixture.cases) {
    if (parsed != null) continue;
    const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ns: "tenant",
        worker: "alarms",
        version: tag,
        doStorageId: "do_0123456789abcdef0123456789abcdef",
        className: "Room",
        objectName: "alice",
        retryCount: 0,
        token: "row-token",
      }),
    }), env());
    assert.equal(response.status, 400, tag);
    assert.match((await jsonBody(response)).message, /version/, tag);
  }
  assert.deepEqual(/** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches), []);
});

test("do-runtime alarm dispatch endpoint maps failed local dispatch to retryable 503", async () => {
  /** @type {any} */ (globalThis).__doIndexHostResponse = Response.json({
    error: "alarm_failed",
    message: "alarm body failed",
  }, { status: 500 });

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/alarms/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      ns: "tenant",
      worker: "alarms",
      version: "v7",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "alice",
      retryCount: 2,
      token: "row-token",
    }),
  }), env());

  assert.equal(response.status, 503);
  assert.deepEqual(await jsonBody(response), {
    error: "do_alarm_dispatch_failed",
    message: "DO alarm dispatch failed",
    details: {
      upstream_status: 500,
      upstream_body: "{\"error\":\"alarm_failed\",\"message\":\"alarm body failed\"}",
    },
  });
});

test("do-runtime index maps invalid binary dispatch bodies to stable JSON errors", async () => {
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/invoke", {
    method: "POST",
    headers: { "content-type": DO_INVOKE_CONTENT_TYPE },
    body: new Uint8Array([0, 0, 0, 20, 1, 2, 3]),
  }), env());

  assert.equal(response.status, 400);
  assert.deepEqual(await jsonBody(response), {
    error: "invalid_request",
    message: "DO invoke envelope metadata is invalid",
  });
});

test("do-runtime index maps dispatch owner failures without leaking transport details", async () => {
  /** @type {any} */ (globalThis).__doIndexOwnerError = new DoRuntimeError(
    503,
    "registry_unavailable",
    "DO owner registry is not configured",
    { error: "redis leaked", message: "redis://secret" }
  );

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/invoke", {
    method: "POST",
    headers: { "content-type": DO_INVOKE_CONTENT_TYPE },
    body: encodeDoInvokeRequest({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "room-a",
    }),
  }), env());

  assert.equal(response.status, 503);
  assert.deepEqual(await jsonBody(response), {
    error: "registry_unavailable",
    message: "DO owner registry is not configured",
  });
});

test("do-runtime probe exposes task and owner state", async () => {
  /** @type {any} */ (globalThis).__doIndexProbeOwner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-a",
    endpoint: "do-runtime-a:8788",
    generation: 4,
  };

  const response = await app.fetch(internalRequest(
    "https://do-runtime/internal/do/probe?ownerKey=do_0123456789abcdef0123456789abcdef%3ARoom%3Ashard0&generation=4"
  ), env());

  assert.equal(response.status, 200);
  assert.deepEqual(await jsonBody(response), {
    status: "owner_alive",
    service: "do-runtime",
    draining: false,
    taskId: "task-a",
    endpoint: "do-runtime-a:8788",
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    generation: 4,
    owner: /** @type {any} */ (globalThis).__doIndexProbeOwner,
    ownerScopes: { owned: 0 },
  });
});

test("do-runtime probe treats empty generation query as absent", async () => {
  const response = await app.fetch(internalRequest(
    "https://do-runtime/internal/do/probe?ownerKey=do_0123456789abcdef0123456789abcdef%3ARoom%3Ashard0&generation="
  ), env());

  assert.equal(response.status, 200);
  const body = await jsonBody(response);
  assert.equal(body.generation, null);
});

test("do-runtime renew endpoint returns owner renewal summary", async () => {
  /** @type {any} */ (globalThis).__doIndexRenewResult = {
    draining: false,
    owned: 2,
    renewed: 2,
    lost: 0,
    errors: [],
  };

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/renew", {
    method: "POST",
  }), env());

  assert.equal(response.status, 200);
  assert.deepEqual(await jsonBody(response), /** @type {any} */ (globalThis).__doIndexRenewResult);
});

test("do-runtime drain timeout reports owned scopes separately from in-flight dispatches", async () => {
  doState.ownedScopes.set("do_storage:Room:shard0", { generation: 1 });
  doState.ownedScopes.set("do_storage:Room:shard1", { generation: 1 });
  /** @type {any} */ (globalThis).__doIndexWaitForInFlightResult = {
    drained: false,
    inFlight: 3,
    waitedMs: 25,
  };

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/drain", {
    method: "POST",
  }), env());

  assert.equal(response.status, 503);
  assert.deepEqual(await jsonBody(response), {
    error: "do_drain_in_flight_timeout",
    message: "DO drain timed out waiting for in-flight handlers",
    draining: true,
    inFlight: 3,
    waitedMs: 25,
    owned: 2,
    released: 0,
  });
});

test("do-runtime returns owner hints instead of forwarding when caller accepts them", async () => {
  /** @type {any} */ (globalThis).__doIndexOwner = {
    ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
    taskId: "task-b",
    endpoint: "do-runtime-b:8788",
    generation: 9,
  };

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/invoke", {
    method: "POST",
    headers: {
      "content-type": DO_INVOKE_CONTENT_TYPE,
      "x-wdl-do-accept-owner-hint": "1",
    },
    body: encodeDoInvokeRequest({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      className: "Room",
      objectName: "room-a",
      request: { method: "GET", url: "https://demo.workers.example/" },
    }),
  }), env());

  assert.equal(response.status, 409);
  assert.equal(response.headers.get("x-wdl-do-owner-hint"), "1");
  assert.equal(response.headers.get("x-wdl-do-owner-endpoint"), "do-runtime-b:8788");
  assert.deepEqual(await jsonBody(response), {
    error: "do_owner_hint",
    message: "Durable Object owner is remote; retry the owner endpoint",
    owner: {
      ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
      taskId: "task-b",
      endpoint: "do-runtime-b:8788",
      generation: 9,
    },
  });
});

test("do-runtime connect strips tenant owner hint control markers", async () => {
  /** @type {any} */ (globalThis).__doIndexHostResponse = new Response("maintenance", {
    status: 503,
    headers: {
      "x-wdl-do-owner-hint": "1",
      "x-wdl-do-owner-key": "tenant-forged-owner",
      "x-wdl-do-owner-task-id": "tenant-forged-task",
      "x-wdl-do-owner-endpoint": "do-runtime-forged:8788",
      "x-wdl-do-owner-generation": "99",
    },
  });

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/connect", {
    method: "GET",
    headers: {
      "x-wdl-do-ns": "tenant",
      "x-wdl-do-worker": "chat",
      "x-wdl-do-version": "v1",
      "x-wdl-do-storage-id": "do_0123456789abcdef0123456789abcdef",
      "x-wdl-do-class-name": "Room",
      "x-wdl-do-object-name": "room-a",
      "x-wdl-do-request-url": "https://demo.workers.example/ws",
      Upgrade: "websocket",
    },
  }), env());

  assert.equal(response.status, 503);
  assert.equal(response.headers.get("x-wdl-do-owner-hint"), null);
  assert.equal(response.headers.get("x-wdl-do-owner-key"), "do_0123456789abcdef0123456789abcdef:Room:shard12");
  assert.equal(response.headers.get("x-wdl-do-owner-task-id"), "task-a");
  assert.equal(response.headers.get("x-wdl-do-owner-endpoint"), "do-runtime-a:8788");
  assert.equal(response.headers.get("x-wdl-do-owner-generation"), "1");
  assert.equal(await response.text(), "maintenance");
});

test("do-runtime storage-delete-worker classifies invalid members as 207 partial errors", async () => {
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/storage/delete-worker", {
    method: "POST",
    body: JSON.stringify({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      members: ["bad-member"],
    }),
  }), env());

  assert.equal(response.status, 207);
  assert.deepEqual(await jsonBody(response), {
    ok: false,
    deleted: 0,
    errors: [{ member: "bad-member", error: "invalid_member" }],
  });
});

test("do-runtime storage-delete-worker rejects non-canonical versions before dispatch", async () => {
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/storage/delete-worker", {
    method: "POST",
    body: JSON.stringify({
      ns: "tenant",
      worker: "chat",
      version: "v9007199254740992",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      members: ["Room:room-a:0"],
    }),
  }), env());

  assert.equal(response.status, 400);
  assert.deepEqual(await jsonBody(response), {
    error: "invalid_request",
    message: "version is invalid",
  });
  assert.deepEqual(/** @type {any[]} */ (/** @type {any} */ (globalThis).__doIndexHostFetches), []);
});

test("do-runtime storage-delete-worker preserves stable member error codes from host responses", async () => {
  /** @type {any} */ (globalThis).__doIndexHostResponse = Response.json({
    error: "stale_owner_generation",
    message: "DO scope owner generation is stale",
    details: { message: "raw ignored" },
  }, { status: 503 });

  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/storage/delete-worker", {
    method: "POST",
    body: JSON.stringify({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      members: ["Room:room-a:0"],
    }),
  }), env());

  assert.equal(response.status, 207);
  assert.deepEqual(await jsonBody(response), {
    ok: false,
    deleted: 0,
    errors: [{
      member: "Room:room-a:0",
      error: "stale_owner_generation",
      message: "DO scope owner generation is stale",
      status: 503,
    }],
  });
});

test("do-runtime storage-delete-worker maps thrown member failures to stable generic errors", async () => {
  const response = await app.fetch(internalRequest("https://do-runtime/internal/do/storage/delete-worker", {
    method: "POST",
    body: JSON.stringify({
      ns: "tenant",
      worker: "chat",
      version: "v1",
      doStorageId: "do_0123456789abcdef0123456789abcdef",
      members: ["Room:room-a:0"],
    }),
  }), {
    WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN,
    DO_HOSTS: {
      /** @param {string} name */
      idFromName(name) {
        return name;
      },
      get() {
        return {
          async fetch() {
            throw new Error("redis://secret socket reset");
          },
        };
      },
    },
  });

  assert.equal(response.status, 207);
  assert.deepEqual(await jsonBody(response), {
    ok: false,
    deleted: 0,
    errors: [{
      member: "Room:room-a:0",
      error: "storage_delete_failed",
      message: "DO storage delete failed",
    }],
  });
});
