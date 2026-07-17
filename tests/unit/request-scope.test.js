import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
} from "../helpers/load-shared-module.js";

const observabilityUrl = moduleDataUrl(`
export const calls = { complete: [] };
export function ensureRequestId(headers) {
  return headers.get("x-request-id") || "generated-rid";
}
export function recordRequestComplete(fields) {
  calls.complete.push(fields);
}
`);

const respondUrl = moduleDataUrl(`
export function echoResponseWithRequestId(response, requestId, filterHeaders) {
  const out = new Response(response.body, response);
  filterHeaders?.(out.headers);
  out.headers.set("x-request-id", requestId);
  return out;
}
`);

const src = applyModuleReplacements(readRepositoryFile("shared/request-scope.js"), [
  [
    /import \{\n {2}ensureRequestId,\n {2}recordRequestComplete,\n\} from "shared-observability";/,
    `import { ensureRequestId, recordRequestComplete } from ${JSON.stringify(observabilityUrl)};`
  ],
  [
    /import \{ echoResponseWithRequestId \} from "shared-respond";/,
    `import { echoResponseWithRequestId } from ${JSON.stringify(respondUrl)};`
  ],
]);

const [{ createHttpRequestScope }, observability] = await Promise.all([
  import(moduleDataUrl(src)),
  import(observabilityUrl),
]);

test("createHttpRequestScope echoes request id and records final request state", async () => {
  observability.calls.complete.length = 0;
  const metrics = { increment() {}, observe() {} };
  const log = () => {};
  const request = new Request("http://runtime.test/path", {
    method: "POST",
    headers: { "x-request-id": "rid-123" },
  });
  const scope = createHttpRequestScope({
    request,
    service: "runtime",
    metrics,
    log,
    route: "initial",
    extras: () => ({ namespace: "tenant-a" }),
    responseHeaderFilter(/** @type {Headers} */ headers) {
      headers.delete("x-private");
    },
  });
  scope.setRoute("worker_fetch");
  const err = false;
  scope.markError(err);
  const response = scope.respond(Response.json({ ok: false }, {
    status: 502,
    headers: { "x-private": "hidden", "x-public": "visible" },
  }));
  scope.complete();

  assert.equal(response.headers.get("x-request-id"), "rid-123");
  assert.equal(response.headers.get("x-private"), null);
  assert.equal(response.headers.get("x-public"), "visible");
  assert.equal(scope.requestId, "rid-123");
  assert.equal(observability.calls.complete.length, 1);
  assert.equal(observability.calls.complete[0].route, "worker_fetch");
  assert.equal(observability.calls.complete[0].status, 502);
  assert.equal(observability.calls.complete[0].error, err);
  assert.equal(observability.calls.complete[0].hasError, true);
  assert.deepEqual(observability.calls.complete[0].extras, { namespace: "tenant-a" });
});

test("createHttpRequestScope records nullish thrown values as errors", () => {
  for (const err of [null, undefined]) {
    observability.calls.complete.length = 0;
    const scope = createHttpRequestScope({
      request: new Request("http://runtime.test/path"),
      service: "runtime",
      log() {},
      route: "worker_fetch",
    });

    scope.markError(err);
    scope.respond(new Response(null, { status: 502 }));
    scope.complete();

    assert.equal(observability.calls.complete[0].hasError, true);
    assert.equal(observability.calls.complete[0].error, err);
  }
});
