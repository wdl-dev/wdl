import { test } from "node:test";
import assert from "node:assert/strict";
import { importRepositoryModule, moduleDataUrl, repositoryFileUrl } from "../helpers/load-shared-module.js";
import { CLOUDFLARE_WORKERS_URL } from "../helpers/mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "../helpers/mocks/runtime-metrics.js";
import { runtimeProxyBindingStubUrl } from "../helpers/runtime-proxy-stub.js";
import { KV_FACADE_RPC_METHOD } from "../../runtime/infrastructure-error.js";

const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_BOUNDED_BODY_URL = repositoryFileUrl("shared/bounded-body.js");
const SHARED_NS_PATTERN_URL = repositoryFileUrl("shared/ns-pattern.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const RUNTIME_INFRASTRUCTURE_ERROR_STUB_URL = moduleDataUrl(`
export const KV_FACADE_RPC_METHOD = ${JSON.stringify(KV_FACADE_RPC_METHOD)};
export function isRuntimeInfrastructureError() { return false; }
export function runtimeInfrastructureError(message) { return new Error(message); }
`);
const KV_CAPACITY_STUB_URL = moduleDataUrl(`
export function acquireKvReadLease() { return { contentLength: null, release() {} }; }
export function kvReadCapacityError() { return new Error("capacity"); }
export function withKvReadDeadline(callback) { return callback(new AbortController()); }
`);

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

test("KV host RPC surface exposes public namespace methods and one internal trampoline", async () => {
  const { KV } = await importRepositoryModule("runtime/bindings/kv.js", [
    [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
    [/import \{ toBytes \} from "runtime-lib";/, toBytesStub],
    [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
    [/from "runtime-infrastructure-error";/, `from ${JSON.stringify(RUNTIME_INFRASTRUCTURE_ERROR_STUB_URL)};`],
    [/from "runtime-bindings-kv-capacity";/, `from ${JSON.stringify(KV_CAPACITY_STUB_URL)};`],
    [/from "shared-base64";/, `from ${JSON.stringify(SHARED_BASE64_URL)};`],
    [/from "shared-bounded-body";/, `from ${JSON.stringify(SHARED_BOUNDED_BODY_URL)};`],
    [/from "shared-ns-pattern";/, `from ${JSON.stringify(SHARED_NS_PATTERN_URL)};`],
    [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
    [/from "shared-respond";/, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
  ]);

  assert.deepEqual(Object.getOwnPropertyNames(KV.prototype).toSorted(), [
    KV_FACADE_RPC_METHOD,
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
