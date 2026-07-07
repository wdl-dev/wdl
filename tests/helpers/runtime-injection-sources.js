import {
  importSpecifierReplacements,
  moduleDataUrl,
  readRepositoryFile,
  readRepositoryModuleSource,
} from "./load-shared-module.js";

const REAL_RUNTIME_INJECTION_SOURCE_PATHS = Object.freeze({
  d1ClientSource: "runtime/d1-client.js",
  d1DataFieldSource: "shared/d1-data-field.js",
  d1ParamsSource: "shared/d1-params.js",
  sqlSplitterSource: "shared/sql-splitter.js",
  d1TransportSource: "shared/d1-transport.js",
  r2ClientSource: "runtime/r2-client.js",
  r2UtilsSource: "runtime/r2-utils.js",
  doClientSource: "runtime/do-client.js",
  doTransportSource: "runtime/_wdl-do-transport.js",
  ownerEndpointSource: "runtime/_wdl-owner-endpoint.js",
  ownerHintCacheSource: "runtime/_wdl-owner-hint-cache.js",
  requestIdSource: "runtime/_wdl-request-id.js",
  workflowsClientSource: "runtime/workflows-client.js",
});

const RUNTIME_INJECTION_SOURCE_SPECIFIERS = Object.freeze({
  d1ClientSource: "runtime-d1-client-source",
  d1DataFieldSource: "runtime-d1-data-field-source",
  d1ParamsSource: "runtime-d1-params-source",
  sqlSplitterSource: "runtime-sql-splitter-source",
  d1TransportSource: "runtime-d1-transport-source",
  r2ClientSource: "runtime-r2-client-source",
  r2UtilsSource: "runtime-r2-utils-source",
  doClientSource: "runtime-do-client-source",
  doTransportSource: "runtime-do-transport-source",
  ownerEndpointSource: "runtime-owner-endpoint-source",
  ownerHintCacheSource: "runtime-owner-hint-cache-source",
  requestIdSource: "runtime-request-id-source",
  workflowsClientSource: "runtime-workflows-client-source",
});

export const STUB_RUNTIME_INJECTION_SOURCES = Object.freeze({
  d1ClientSource:
    "const state = new WeakMap(); export class D1Database { constructor(stub) { state.set(this, { stub }); } }",
  d1DataFieldSource:
    "export function setDataField(target, key, value) { target[key] = value; }",
  d1ParamsSource:
    "export function normalizeD1Param(value) { return value; }",
  sqlSplitterSource:
    "export function splitSqlStatements(sql) { return [{ sql, params: [] }]; }",
  d1TransportSource:
    'import { setDataField } from "shared-d1-data-field"; export function decodeD1Transport(value) { setDataField({}, "ok", value); return value; }',
  r2ClientSource:
    "const state = new WeakMap(); export class R2Bucket { constructor(stub) { state.set(this, { stub }); } }",
  r2UtilsSource:
    "export const R2_OBJECT_MAX_BUFFER_BYTES = 26214400;",
  doClientSource:
    "export class DurableObjectNamespace { constructor(stub) { this.stub = stub; } }",
  doTransportSource:
    "export function requestSpec() {}",
  ownerEndpointSource:
    "export function validOwnerEndpointForService() { return true; }",
  ownerHintCacheSource:
    "export function createOwnerHintCache() { return {}; }",
  requestIdSource:
    "export function requestIdFromOptions() { return null; }",
  workflowsClientSource:
    "export class Workflow { constructor(metadata) { this.metadata = metadata; } }",
});

/** @param {string} source */
function defaultTextModuleUrl(source) {
  return moduleDataUrl(`export default ${JSON.stringify(source)};`);
}

/** @param {Record<string, string>} sourceByProperty */
function sourceSpecifierEntries(sourceByProperty) {
  return Object.entries(RUNTIME_INJECTION_SOURCE_SPECIFIERS).map(([property, specifier]) => [
    specifier,
    sourceByProperty[property],
  ]);
}

/** @param {Record<string, string>} sourceByProperty */
function runtimeInjectionSourcesUrlFromText(sourceByProperty) {
  return moduleDataUrl(readRepositoryModuleSource(
    "runtime/load/injection-sources.js",
    importSpecifierReplacements(Object.fromEntries(
      sourceSpecifierEntries(sourceByProperty).map(([specifier, source]) => [
        specifier,
        defaultTextModuleUrl(source),
      ])
    ))
  ));
}

/** @param {Record<string, string>} [overrides] */
export function stubRuntimeInjectionSourcesUrl(overrides = {}) {
  return runtimeInjectionSourcesUrlFromText({
    ...STUB_RUNTIME_INJECTION_SOURCES,
    ...overrides,
  });
}

export function realRuntimeInjectionSourcesUrl() {
  return runtimeInjectionSourcesUrlFromText(Object.fromEntries(
    Object.entries(REAL_RUNTIME_INJECTION_SOURCE_PATHS).map(([property, path]) => [
      property,
      readRepositoryFile(path),
    ])
  ));
}
