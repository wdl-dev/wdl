import { test } from "node:test";
import assert from "node:assert/strict";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");

const toBytesStub = `const toBytes = (value) => {
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error("KV put: value must be string | ArrayBuffer | typed array | ReadableStream");
};`;

const buildAssetUrlStub = `const buildAssetUrl = (cdnBase, prefix, path) => {
  if (!cdnBase) throw new Error("ASSETS.url: cdnBase is not configured");
  if (typeof prefix !== "string" || !prefix || !prefix.endsWith("/")) {
    throw new Error("ASSETS.url: prefix must be a non-empty string ending in '/'");
  }
  if (typeof path !== "string") throw new Error("ASSETS.url: path must be a string");
  const base = cdnBase.replace(/\\/+$/, "");
  const stripped = path.replace(/^\\/+/, "");
  if (stripped === "") return base + "/" + prefix;
  const segments = stripped.split("/");
  for (const seg of segments) {
    if (seg === "" || seg === "." || seg === "..") {
      throw new Error("ASSETS.url: invalid path segment");
    }
  }
  return base + "/" + prefix + segments.map((s) => encodeURIComponent(s)).join("/");
}`;

test("KV host RPC surface exposes only public namespace methods", async () => {
  const { KV } = await importRepositoryModule("runtime/bindings/kv.js", [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/import \{ toBytes \} from "runtime-lib";/, toBytesStub],
    [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
    [/from "shared-base64";/, `from ${JSON.stringify(SHARED_BASE64_URL)};`],
    [/from "shared-bounded-body";/, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)};`],
    [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
    [/from "shared-respond";/, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
  ]);

  assert.deepEqual(Object.getOwnPropertyNames(KV.prototype).toSorted(), [
    "constructor",
    "delete",
    "get",
    "getWithMetadata",
    "list",
    "put",
  ]);
});

test("ASSETS host RPC surface exposes only url", async () => {
  const { Assets } = await importRepositoryModule("runtime/bindings/assets.js", [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/import \{ buildAssetUrl \} from "runtime-lib";/, buildAssetUrlStub],
  ]);

  assert.deepEqual(Object.getOwnPropertyNames(Assets.prototype).toSorted(), [
    "constructor",
    "url",
  ]);
});
