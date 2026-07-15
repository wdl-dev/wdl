import { CLOUDFLARE_WORKERS_URL } from "./mocks/cloudflare-workers.js";
import { RUNTIME_METRICS_NOOP_URL } from "./mocks/runtime-metrics.js";
import {
  importRepositoryModule,
  repositoryFileUrl,
  repositoryModuleDataUrl,
} from "./load-shared-module.js";
import { makeRecordingFetch } from "./mock-fetch.js";
import { installMockProperty } from "./mock-global.js";
import { runtimeProxyBindingStubUrl } from "./runtime-proxy-stub.js";

const R2_UTILS_URL = repositoryFileUrl("runtime/r2-utils.js");
const PROXY_BINDING_URL = runtimeProxyBindingStubUrl();
const SHARED_S3_XML_URL = repositoryFileUrl("shared/s3-xml.js");
const SHARED_RESPOND_URL = repositoryFileUrl("shared/respond.js");
const SHARED_BASE64_URL = repositoryFileUrl("shared/base64.js");
const SHARED_S3_RETRY_URL = repositoryFileUrl("shared/s3-retry.js");
const R2_METADATA_URL = repositoryModuleDataUrl("runtime/bindings/r2/metadata.js", [
  [/from "runtime-r2-utils";/, `from ${JSON.stringify(R2_UTILS_URL)};`],
]);
const R2_XML_URL = repositoryModuleDataUrl("runtime/bindings/r2/xml.js", [
  [/from "runtime-r2-utils";/, `from ${JSON.stringify(R2_UTILS_URL)};`],
  [/from "shared-s3-xml";/, `from ${JSON.stringify(SHARED_S3_XML_URL)};`],
  [/from "runtime-bindings-r2-metadata";/, `from ${JSON.stringify(R2_METADATA_URL)};`],
]);

export const R2_HOST_TEST_STATE = {
  /** @type {any} */
  fetch: null,
  /** @type {any[]} */
  awsClientConfigs: [],
};
/** @type {typeof globalThis & { __r2HostTestState?: typeof R2_HOST_TEST_STATE }} */
const r2HostGlobal = globalThis;
r2HostGlobal.__r2HostTestState = R2_HOST_TEST_STATE;

const mod = await importRepositoryModule("runtime/bindings/r2.js", [
  [/from "cloudflare:workers";/, `from ${JSON.stringify(CLOUDFLARE_WORKERS_URL)};`],
  [
    /import \{ SigV4Client \} from "@wdl-dev\/aws-sigv4";/,
    `class SigV4Client {
       constructor(config) {
         globalThis.__r2HostTestState.awsClientConfigs.push(config);
       }
       fetch(url, init) {
         return globalThis.__r2HostTestState.fetch(url, init);
       }
     }`
  ],
  [/from "runtime-metrics";/, `from ${JSON.stringify(RUNTIME_METRICS_NOOP_URL)};`],
  [/from "runtime-r2-utils";/, `from ${JSON.stringify(R2_UTILS_URL)};`],
  [/from "runtime-bindings-proxy";/, `from ${JSON.stringify(PROXY_BINDING_URL)};`],
  [/from "runtime-bindings-r2-metadata";/, `from ${JSON.stringify(R2_METADATA_URL)};`],
  [/from "runtime-bindings-r2-xml";/, `from ${JSON.stringify(R2_XML_URL)};`],
  [/from "shared-respond";/, `from ${JSON.stringify(SHARED_RESPOND_URL)};`],
  [/from "shared-base64";/, `from ${JSON.stringify(SHARED_BASE64_URL)};`],
  [/from "shared-s3-retry";/, `from ${JSON.stringify(SHARED_S3_RETRY_URL)};`],
]);

export const { R2Bucket } = mod;

export function makeR2Bucket(envOverrides = {}) {
  return new R2Bucket(
    { props: { ns: "demo", bucketName: "uploads" } },
    {
      SERVICE_NAME: "user-runtime",
      R2_S3_ACCESS_KEY_ID: "test",
      R2_S3_SECRET_ACCESS_KEY: "test",
      R2_S3_ENDPOINT: "http://s3mock:9090",
      R2_S3_BUCKET: "wdl-r2",
      R2_S3_REGION: "us-east-1",
      WDL_INTERNAL_AUTH_TOKEN: "test-internal-auth-token",
      ...envOverrides,
    }
  );
}

/** @param {any} handler */
export function installR2FetchMock(handler) {
  return installMockProperty(R2_HOST_TEST_STATE, "fetch", handler);
}

/**
 * @param {Array<{ url: string, init: any }>} calls
 * @param {Parameters<typeof makeRecordingFetch>[1]} [options]
 */
export function installRecordingR2FetchMock(calls, options = {}) {
  return installR2FetchMock(makeRecordingFetch(calls, options));
}
