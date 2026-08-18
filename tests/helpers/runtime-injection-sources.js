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
  utf8Source: "shared/utf8.js",
  sqlSplitterSource: "shared/sql-splitter.js",
  d1TransportSource: "shared/d1-transport.js",
  r2ClientSource: "runtime/r2-client.js",
  r2UtilsSource: "runtime/r2-utils.js",
  doClientSource: "runtime/do-client.js",
  doScopedRequestSource: "runtime/_wdl-do-scoped-request.js",
  requestIdSource: "runtime/_wdl-request-id.js",
  workflowsClientSource: "runtime/workflows-client.js",
  aiClientSource: "runtime/ai-client.js",
});

const RUNTIME_INJECTION_SOURCE_SPECIFIERS = Object.freeze({
  d1ClientSource: "runtime-d1-client-source",
  d1DataFieldSource: "runtime-d1-data-field-source",
  d1ParamsSource: "runtime-d1-params-source",
  utf8Source: "runtime-utf8-source",
  sqlSplitterSource: "runtime-sql-splitter-source",
  d1TransportSource: "runtime-d1-transport-source",
  r2ClientSource: "runtime-r2-client-source",
  r2UtilsSource: "runtime-r2-utils-source",
  doClientSource: "runtime-do-client-source",
  doScopedRequestSource: "runtime-do-scoped-request-source",
  requestIdSource: "runtime-request-id-source",
  workflowsClientSource: "runtime-workflows-client-source",
  aiClientSource: "runtime-ai-client-source",
});

/** @param {string[]} imports @param {string} body */
function stubSource(imports, body) {
  return `${imports.map((specifier) => `import ${JSON.stringify(specifier)};`).join(" ")} ${body}`;
}

export const STUB_RUNTIME_INJECTION_SOURCES = Object.freeze({
  d1ClientSource: stubSource(
    [
      "./_wdl-sql-splitter.js",
      "./_wdl-d1-params.js",
      "./_wdl-d1-transport.js",
      "./_wdl-d1-data-field.js",
      "./_wdl-request-id.js",
    ],
    "const state = new WeakMap(); export class D1Database { constructor(stub) { state.set(this, { stub }); } }",
  ),
  d1DataFieldSource:
    "export function setDataField(target, key, value) { target[key] = value; }",
  d1ParamsSource:
    'import { utf8ByteLength } from "./utf8.js"; export function normalizeD1Param(value) { utf8ByteLength(String(value)); return value; }',
  utf8Source:
    "export function utf8ByteLength(value) { return value.length; }",
  sqlSplitterSource:
    "export function splitSqlStatements(sql) { return [{ sql, params: [] }]; }",
  d1TransportSource:
    'import { setDataField } from "shared-d1-data-field"; export function decodeD1Transport(value) { setDataField({}, "ok", value); return value; }',
  r2ClientSource: stubSource(
    ["./_wdl-r2-utils.js", "./_wdl-request-id.js"],
    "const state = new WeakMap(); export class R2Bucket { constructor(stub) { state.set(this, { stub }); } }",
  ),
  r2UtilsSource:
    "export const R2_OBJECT_MAX_BUFFER_BYTES = 26214400;",
  doClientSource: stubSource(
    ["./_wdl-do-transport.js", "./_wdl-request-id.js"],
    "export class DurableObjectNamespace { constructor(stub) { this.stub = stub; } }",
  ),
  doScopedRequestSource: stubSource(
    ["./_wdl-request-id.js"],
    "export function scopedDoRequest(_objectName, request) { return request; }",
  ),
  requestIdSource:
    "export function requestIdFromOptions() { return null; } export function sanitizeRequestId() { return null; }",
  workflowsClientSource: stubSource(
    ["./_wdl-request-id.js"],
    "export class Workflow { constructor(backend) { this.backend = backend; } create() {} }",
  ),
  aiClientSource: stubSource(
    ["./_wdl-request-id.js"],
    "export class Ai { constructor(fetcher) { this.fetcher = fetcher; } }",
  ),
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

export function realRuntimeInjectionSources() {
  return Object.fromEntries(
    Object.entries(REAL_RUNTIME_INJECTION_SOURCE_PATHS).map(([property, path]) => [
      property,
      readRepositoryFile(path),
    ])
  );
}

export function realRuntimeInjectionSourcesUrl() {
  return runtimeInjectionSourcesUrlFromText(realRuntimeInjectionSources());
}
