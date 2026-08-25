import {
  applyModuleReplacements,
  readRepositoryFile,
  moduleDataUrl,
  repositoryFileUrl,
  sharedModuleDataUrl,
} from "./load-shared-module.js";
import { d1ProtocolDataUrl, d1QueryWireDataUrl } from "./load-d1-protocol.js";

const protocolUrl = moduleDataUrl(`
export function classifyD1Error(err) {
  if (err?.code === "limit-exceeded") {
    return { status: err.status, code: err.code, category: "limit", retryable: false, message: err.message };
  }
  return { status: 500, code: "internal", category: "internal", retryable: false, message: err.message };
}
export class D1ProtocolError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = "D1ProtocolError";
    this.status = status;
    this.code = code;
  }
}
export function d1ErrorResponse(err) {
  return Response.json(
    { success: false, error: err.code || err.message, message: err.message },
    { status: err.status || 500 }
  );
}
export async function readD1ActorQueryRequest(request) {
  const gate = globalThis.__d1ActorQueryReadGate;
  if (gate) {
    gate.started?.();
    await gate.promise;
  }
  return await request.json();
}
export async function readD1ActorControlRequest(request) {
  return await request.json();
}
`);
const d1ProtocolUrl = d1ProtocolDataUrl();
const d1QueryWireUrl = d1QueryWireDataUrl();

const ownerRegistryUrl = moduleDataUrl(`
export async function assertCurrentOwnerWithLeaseBudget(_env, owner) {
  const assertion = globalThis.__d1ActorOwnerAssertion || {};
  return {
    owner,
    leaseRemainingMs: assertion.leaseRemainingMs ?? 60_000,
    guardMs: assertion.guardMs ?? 1_000,
  };
}
`);

const httpUrl = moduleDataUrl(`
import {
  D1_QUERY_NATIVE_VALUE_ENCODING,
  d1QuerySuccessHeaders,
} from ${JSON.stringify(d1QueryWireUrl)};
export function json(data, init = {}) {
  return Response.json(data, init);
}
export function d1QuerySuccessResponse(data, init = {}) {
  const { changedDb = false, ...responseInit } = init;
  const headers = new Headers(responseInit.headers);
  for (const [name, value] of Object.entries(
    d1QuerySuccessHeaders(changedDb, D1_QUERY_NATIVE_VALUE_ENCODING)
  )) {
    headers.set(name, value);
  }
  return Response.json(data, { ...responseInit, headers });
}
export function jsonError(status, error, message, extra = {}) {
  return json({ ...extra, error, message }, { status });
}
`);

const readCacheUrl = repositoryFileUrl("d1-runtime/read-cache.js");

const stateUrl = moduleDataUrl(`
export const storageRecords = [];
let pendingQueries = 0;
export function beginPendingQuery() { pendingQueries += 1; }
export function endPendingQuery() { pendingQueries = Math.max(0, pendingQueries - 1); }
export function pendingQueryCount() { return pendingQueries; }
export function isDraining() { return false; }
export function recordPayloadStorageSize(dbKey, payload) {
  storageRecords.push({ dbKey, payload });
}
export function recordStorageSizeForDb(dbKey, size) {
  storageRecords.push({ dbKey, size });
}
`);

const testHooksUrl = moduleDataUrl(`
export function isD1ActorTestHook() { return false; }
export async function runD1ActorTestHook() {
  throw new Error("unexpected test hook");
}
`);

const source = applyModuleReplacements(readRepositoryFile("d1-runtime/actor.js"), [
  [
    /import \{ DurableObject \} from "cloudflare:workers";/,
    "class DurableObject {}"
  ],
  [
    /import \{\n {2}classifyD1Error,\n {2}D1ProtocolError,\n {2}d1ErrorResponse,\n {2}readD1ActorControlRequest,\n {2}readD1ActorQueryRequest,\n {2}sqliteBindParams,\n\} from "d1-runtime-protocol";/,
    `import { classifyD1Error, D1ProtocolError, d1ErrorResponse, readD1ActorControlRequest, readD1ActorQueryRequest } from ${JSON.stringify(protocolUrl)};
     import { sqliteBindParams } from ${JSON.stringify(d1ProtocolUrl)};`
  ],
  [
    /import \{ assertCurrentOwnerWithLeaseBudget \} from "d1-runtime-owner-registry";/,
    `import { assertCurrentOwnerWithLeaseBudget } from ${JSON.stringify(ownerRegistryUrl)};`
  ],
  [
    /import \{ d1QuerySuccessResponse, json, jsonError \} from "d1-runtime-http";/,
    `import { d1QuerySuccessResponse, json, jsonError } from ${JSON.stringify(httpUrl)};`
  ],
  [
    /import \{\n {2}parseIdempotentSchemaDdl,\n {2}payloadChangedDb,\n {2}statementMayChangeDb,\n\} from "d1-runtime-read-cache";/,
    `import { parseIdempotentSchemaDdl, payloadChangedDb, statementMayChangeDb } from ${JSON.stringify(readCacheUrl)};`
  ],
  // Keep these import rewrites exact so new actor dependencies force the test
  // stub to be reviewed instead of being swallowed by a broad lazy match.
  [
    /import \{\n {2}beginPendingQuery,\n {2}endPendingQuery,\n {2}isDraining,\n {2}pendingQueryCount,\n {2}recordPayloadStorageSize,\n {2}recordStorageSizeForDb,\n\} from "d1-runtime-state";/,
    `import { beginPendingQuery, endPendingQuery, isDraining, pendingQueryCount, recordPayloadStorageSize, recordStorageSizeForDb } from ${JSON.stringify(stateUrl)};`
  ],
  [
    /import \{\n {2}isD1ActorTestHook,\n {2}runD1ActorTestHook,\n\} from "d1-runtime-test-hooks";/,
    `import { isD1ActorTestHook, runD1ActorTestHook } from ${JSON.stringify(testHooksUrl)};`
  ],
  [
    /import \{ fnv1a32Utf8 \} from "shared-fnv1a32";/,
    `import { fnv1a32Utf8 } from ${JSON.stringify(sharedModuleDataUrl("shared/fnv1a32.js"))};`
  ],
  [
    /import \{ utf8ByteLength \} from "shared-utf8";/,
    `import { utf8ByteLength } from ${JSON.stringify(sharedModuleDataUrl("shared/utf8.js"))};`
  ],
  [
    /import \{ contentTypeEssence \} from "shared-respond";/,
    `import { contentTypeEssence } from ${JSON.stringify(sharedModuleDataUrl("shared/respond.js"))};`
  ],
]);

/** @returns {Promise<any>} */
export async function loadD1Actor() {
  return await import(moduleDataUrl(source));
}
