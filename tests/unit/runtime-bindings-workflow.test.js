import assert from "node:assert/strict";
import { test } from "node:test";
import {
  importRepositoryModule,
  importSpecifierReplacements,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "../helpers/load-shared-module.js";
import { parseJsonObjectRequestBody } from "../helpers/request-body.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";

const workflowJsonUrl = repositoryModuleDataUrl(
  "runtime/dispatch/workflow-json.js",
  importSpecifierReplacements({
    "shared-utf8": repositoryFileUrl("shared/utf8.js"),
  })
);
const { WorkflowBinding, WORKFLOW_BINDING_REQUEST_BYTES_MAX } = await importRepositoryModule(
  "runtime/bindings/workflow.js",
  [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/from "runtime-dispatch-workflow-json";/, `from ${JSON.stringify(workflowJsonUrl)};`],
    [/from "shared-bounded-body";/, `from ${JSON.stringify(repositoryFileUrl("shared/bounded-body.js"))};`],
    [/from "shared-internal-auth";/, `from ${JSON.stringify(sharedInternalAuthUrl())};`],
    [/from "shared-observability";/, `from ${JSON.stringify(OBSERVABILITY_NOOP_URL)};`],
  ]
);

/** @param {(call: { url: string, init: RequestInit }) => Response} respond */
function bindingWithBackend(respond) {
  return new WorkflowBinding({
    props: {
      ns: "tenant",
      worker: "agent",
      version: "v7",
      name: "orders",
      workflowKey: "wf_0123456789abcdef0123456789abcdef",
      className: "OrderWorkflow",
    },
  }, {
    WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
    WORKFLOWS_BACKEND: {
      async fetch(
        /** @type {RequestInfo | URL | string} */ url,
        /** @type {RequestInit} */ init
      ) {
        return respond({ url: String(url), init });
      },
    },
  });
}

test("WorkflowBinding fixes identity and limits callers to public operations", async () => {
  /** @type {{ url: string, init: RequestInit } | null} */
  let captured = null;
  const binding = bindingWithBackend((call) => {
    captured = call;
    return Response.json({ id: "instance-1" });
  });
  const response = await binding.fetch(new Request("https://binding.invalid/internal/workflows/create", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-request-id": "rid-workflow",
      "x-wdl-internal-auth": "spoofed",
    },
    body: JSON.stringify({
      instanceId: "instance-1",
      params: { ok: true },
      ns: "attacker",
      worker: "attacker",
      workflowKey: "attacker",
    }),
  }));

  assert.equal(response.status, 200);
  assert.ok(captured);
  const call = /** @type {{ url: string, init: RequestInit }} */ (captured);
  assert.equal(call.url, "http://workflows/internal/workflows/create");
  assert.equal(new Headers(call.init.headers).get("x-wdl-internal-auth"), "test-internal-auth-token");
  assert.deepEqual(parseJsonObjectRequestBody(call.init, "workflow binding body"), {
    instanceId: "instance-1",
    params: { ok: true },
    ns: "tenant",
    worker: "agent",
    frozenVersion: "v7",
    workflowName: "orders",
    workflowKey: "wf_0123456789abcdef0123456789abcdef",
    className: "OrderWorkflow",
    requestId: "rid-workflow",
  });

  await assert.rejects(
    binding.fetch(new Request("https://binding.invalid/internal/workflows/do-alarms/set", {
      method: "POST",
      body: "{}",
    })),
    /operation is not allowed/
  );
});

test("WorkflowBinding bounds and validates its request envelope", async () => {
  const binding = bindingWithBackend(() => {
    throw new Error("backend must not be called");
  });
  await assert.rejects(
    binding.fetch(new Request("https://binding.invalid/internal/workflows/create")),
    /requires POST/
  );
  await assert.rejects(
    binding.fetch(new Request("https://binding.invalid/internal/workflows/create", {
      method: "POST",
      body: "[]",
    })),
    /must be an object/
  );
  await assert.rejects(
    binding.fetch(new Request("https://binding.invalid/internal/workflows/create", {
      method: "POST",
      headers: { "content-length": String(WORKFLOW_BINDING_REQUEST_BYTES_MAX + 1) },
      body: "{}",
    })),
    /must be bounded JSON/
  );
});

test("WorkflowBinding bounds the reconstructed backend envelope", async () => {
  let backendCalls = 0;
  const binding = bindingWithBackend(() => {
    backendCalls += 1;
    return Response.json({ id: "unexpected" });
  });
  const prefix = new TextEncoder().encode('{"params":"');
  const suffix = new TextEncoder().encode('"}');
  const invalidUtf8 = new Uint8Array(Math.ceil(WORKFLOW_BINDING_REQUEST_BYTES_MAX / 3));
  invalidUtf8.fill(0x80);
  const input = new Blob([prefix, invalidUtf8, suffix]);
  assert.ok(input.size < WORKFLOW_BINDING_REQUEST_BYTES_MAX);

  await assert.rejects(
    binding.fetch(new Request("https://binding.invalid/internal/workflows/create", {
      method: "POST",
      body: input,
    })),
    /backend request body exceeds the 2097152 byte limit/
  );
  assert.equal(backendCalls, 0);
});
