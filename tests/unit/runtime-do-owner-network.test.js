import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
  repositoryFileUrl,
} from "../helpers/load-shared-module.js";
import { withMockedFetch } from "../helpers/mock-fetch.js";
import {
  withMockedProperty,
  withMockedPropertyDescriptor,
} from "../helpers/mock-global.js";
import { readJsonResponse } from "../helpers/response-json.js";
import { sharedInternalAuthUrl } from "../helpers/runtime-proxy-stub.js";
import { validOwnerEndpointForService } from "../../shared/owner-endpoint.js";

const OWNER_ENDPOINT_URL = repositoryFileUrl("shared/owner-endpoint.js");
const TEST_INTERNAL_AUTH_TOKEN = "test-internal-auth-token";

async function loadWorker() {
  const source = applyModuleReplacements(readRepositoryFile("runtime/do-owner-network.js"), [
    [/from "shared-owner-endpoint";/g, `from ${JSON.stringify(OWNER_ENDPOINT_URL)};`],
    [/from "shared-internal-auth";/g, `from ${JSON.stringify(sharedInternalAuthUrl())};`],
  ]);
  return await import(moduleDataUrl(source));
}

test("do owner network forwards only valid DO owner endpoints", async () => {
  const { default: worker } = await loadWorker();
  /** @type {string[]} */
  const urls = [];
  await withMockedFetch(async (request) => {
    urls.push(request instanceof Request ? request.url : String(request));
    assert.equal(
      request instanceof Request ? request.headers.get("x-wdl-internal-auth") : null,
      TEST_INTERNAL_AUTH_TOKEN
    );
    return new Response("owner-ok");
  }, async () => {
    for (const request of [
      new Request("http://do-runtime-a:8788/internal/do/invoke", { method: "POST" }),
      new Request("http://do-runtime-a:8788/internal/do/connect", { method: "GET" }),
    ]) {
      const response = await worker.fetch(request, { WDL_INTERNAL_AUTH_TOKEN: TEST_INTERNAL_AUTH_TOKEN });
      assert.equal(response.status, 200);
      assert.equal(await response.text(), "owner-ok");
    }
    assert.deepEqual(urls, [
      "http://do-runtime-a:8788/internal/do/invoke",
      "http://do-runtime-a:8788/internal/do/connect",
    ]);
  });
});

test("do owner network rejects invalid owner endpoints before forwarding", async () => {
  const { default: worker } = await loadWorker();
  let called = false;
  await withMockedFetch(async () => {
    called = true;
    return new Response("unexpected");
  }, async () => {
    for (const url of [
      "http://d1-runtime-a:8787/internal/do/invoke",
      "http://do-runtime-a:8787/internal/do/invoke",
      "http://169.254.169.254:8788/internal/do/invoke",
      "http://8.8.8.8:8788/internal/do/invoke",
      "http://evil.test:8788/internal/do/invoke",
    ]) {
      const response = await worker.fetch(new Request(url));
      const body = await readJsonResponse(response, 400, url);
      assert.equal(body.error, "invalid_owner_endpoint");
    }
    assert.equal(called, false);
  });
});

test("owner endpoint validation uses captured tenant-realm intrinsics", async () => {
  const invalidEndpoint = "redis-proxy-user:7070";

  let hostileUrlCalls = 0;
  await withMockedProperty(
    globalThis,
    "URL",
    /** @type {typeof URL} */ (/** @type {unknown} */ (class HostileURL {
      constructor() {
        hostileUrlCalls += 1;
        this.host = invalidEndpoint;
        this.hostname = "do-runtime-a";
        this.port = "8788";
        this.username = "";
        this.password = "";
        this.pathname = "/";
        this.search = "";
        this.hash = "";
      }
    })),
    () => {
      assert.equal(validOwnerEndpointForService(invalidEndpoint, 8788, "do-runtime"), false);
    }
  );
  assert.equal(hostileUrlCalls, 0);

  let hostileGetterCalls = 0;
  await withMockedPropertyDescriptor(URL.prototype, "port", {
    configurable: true,
    get() {
      hostileGetterCalls += 1;
      return "8788";
    },
  }, () => withMockedPropertyDescriptor(URL.prototype, "hostname", {
    configurable: true,
    get() {
      hostileGetterCalls += 1;
      return "do-runtime-a";
    },
  }, () => {
    assert.equal(validOwnerEndpointForService(invalidEndpoint, 8788, "do-runtime"), false);
  }));
  assert.equal(hostileGetterCalls, 0);

  const nativeRegExpTest = RegExp.prototype.test;
  let hostileRegExpCalls = 0;
  await withMockedProperty(RegExp.prototype, "test", /** @this {RegExp} */ function hostileTest(value) {
    if (value === "redis-proxy-user") {
      hostileRegExpCalls += 1;
      return true;
    }
    return Reflect.apply(nativeRegExpTest, this, [value]);
  }, () => {
    assert.equal(validOwnerEndpointForService("redis-proxy-user:8788", 8788, "do-runtime"), false);
  });
  assert.equal(hostileRegExpCalls, 0);

  const nativeString = String;
  let hostileStringCalls = 0;
  await withMockedProperty(globalThis, "String", /** @type {StringConstructor} */ (function hostileString(value) {
    if (value === 8788) {
      hostileStringCalls += 1;
      return "7070";
    }
    return nativeString(value);
  }), () => {
    assert.equal(validOwnerEndpointForService("do-runtime-a:7070", 8788, "do-runtime"), false);
  });
  assert.equal(hostileStringCalls, 0);
});

test("do owner network rejects non-owner-dispatch paths before forwarding", async () => {
  const { default: worker } = await loadWorker();
  let called = false;
  await withMockedFetch(async () => {
    called = true;
    return new Response("unexpected");
  }, async () => {
    for (const request of [
      new Request("http://do-runtime-a:8788/internal/do/drain", { method: "POST" }),
      new Request("http://do-runtime-a:8788/internal/do/storage/delete-worker", { method: "POST" }),
      new Request("http://do-runtime-a:8788/internal/do/invoke", { method: "GET" }),
      new Request("http://do-runtime-a:8788/internal/do/connect", { method: "POST" }),
      new Request("http://do-runtime-a:8788/_metrics", { method: "GET" }),
    ]) {
      const response = await worker.fetch(request);
      const body = await readJsonResponse(response, 400, request.url);
      assert.equal(body.error, "invalid_owner_path");
    }
    assert.equal(called, false);
  });
});
