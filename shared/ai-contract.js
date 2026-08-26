import {
  isValidAiModelAlias,
  isValidAiProviderName,
  isValidRuntimeLoadNs,
} from "shared-ns-pattern";
import { utf8ByteLength } from "shared-utf8";

export const AI_PROVIDER_MAX_COUNT = 8;
export const AI_MODELS_PER_PROVIDER_MAX = 32;
export const AI_NAMESPACE_MODEL_MAX_COUNT = 128;
export const AI_PROVIDER_NAME_MAX_BYTES = 32;
export const AI_PROVIDER_RECORD_MAX_BYTES = 64 * 1024;
export const AI_UPSTREAM_MODEL_MAX_BYTES = 256;
export const AI_CREDENTIAL_MAX_BYTES = 16 * 1024;
export const AI_CREDENTIAL_ENVELOPE_MAX_BYTES = 64 * 1024;
export const AI_PROVIDER_REVISION_RE = /^[0-9a-f]{32}$/;

export const AI_PROVIDER_KINDS = Object.freeze(["openai", "xai", "deepseek"]);
const PROTOCOL_TRANSPORTS = new Map([
  ["responses", new Set(["http", "sse", "responses_websocket"])],
  ["chat_completions", new Set(["http", "sse"])],
  ["embeddings", new Set(["http"])],
  ["realtime", new Set(["realtime_websocket"])],
]);
export const AI_TRANSPORTS = Object.freeze([
  ...new Set([...PROTOCOL_TRANSPORTS.values()].flatMap((transports) => [...transports])),
]);

const PROVIDER_KINDS = new Set(AI_PROVIDER_KINDS);
const TRANSPORTS = new Set(AI_TRANSPORTS);
const INPUT_MODALITIES = new Set(["text", "image", "audio", "file"]);
const OUTPUT_MODALITIES = new Set(["text", "audio"]);
const CAPABILITY_KEYS = new Set([
  "functionTools",
  "structuredOutput",
  "reasoning",
  "previousResponseId",
  "providerTools",
  "binaryFrames",
]);
const MODEL_DESCRIPTOR_KEYS = new Set([
  "upstreamModel",
  "protocol",
  "transports",
  "inputModalities",
  "outputModalities",
  "capabilities",
]);
const PROVIDER_RECORD_KEYS = new Set(["revision", "kind", "models"]);
const PROVIDER_WRITE_KEYS = new Set(["kind", "models"]);
const RESOLVE_REQUEST_KEYS = new Set(["ns", "model", "protocol", "transport"]);
const MODELS_REQUEST_KEYS = new Set(["ns"]);
const RESOLVE_RESPONSE_KEYS = new Set([
  "provider",
  "alias",
  "kind",
  "upstreamModel",
  "protocol",
  "transport",
  "destination",
  "credential",
  "inputModalities",
  "capabilities",
]);
const MODEL_LIST_ENTRY_KEYS = new Set([
  "id",
  "protocol",
  "transports",
  "inputModalities",
  "outputModalities",
  "capabilities",
]);
const MODELS_RESPONSE_KEYS = new Set(["models"]);

/**
 * @typedef {{
 *   functionTools: boolean,
 *   structuredOutput: boolean,
 *   reasoning: boolean,
 *   previousResponseId: boolean,
 *   providerTools: boolean,
 *   binaryFrames: boolean,
 * }} AiCapabilities
 */

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** @param {Record<string, unknown>} value @param {Set<string>} allowed @param {string} scope */
function rejectUnknownFields(value, allowed, scope) {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`${scope}.${key} is not supported`);
  }
}

/** @param {unknown} value @param {string} scope */
function requireRecord(value, scope) {
  if (!isRecord(value)) throw new Error(`${scope} must be an object`);
  return value;
}

/** @param {string} left @param {string} right */
function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

/** @param {unknown} raw @param {Set<string>} allowed @param {string} scope */
function normalizeStringSet(raw, allowed, scope) {
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error(`${scope} must be a non-empty array`);
  }
  const values = [];
  const seen = new Set();
  for (const value of raw) {
    if (typeof value !== "string" || !allowed.has(value)) {
      throw new Error(`${scope} contains unsupported value ${JSON.stringify(value)}`);
    }
    if (seen.has(value)) throw new Error(`${scope} contains duplicate value ${JSON.stringify(value)}`);
    seen.add(value);
    values.push(value);
  }
  return values.toSorted();
}

/** @param {unknown} raw @param {string} scope @returns {AiCapabilities} */
function normalizeCapabilities(raw, scope) {
  const value = raw === undefined ? {} : requireRecord(raw, scope);
  rejectUnknownFields(value, CAPABILITY_KEYS, scope);
  /** @type {Record<string, boolean>} */
  const out = {};
  for (const key of CAPABILITY_KEYS) {
    const field = value[key];
    if (field !== undefined && typeof field !== "boolean") {
      throw new Error(`${scope}.${key} must be boolean`);
    }
    out[key] = field === true;
  }
  return /** @type {AiCapabilities} */ (out);
}

/** @param {unknown} raw @param {string} scope */
function normalizeUpstreamModel(raw, scope) {
  if (
    typeof raw !== "string" ||
    raw.length === 0 ||
    !raw.isWellFormed() ||
    utf8ByteLength(raw) > AI_UPSTREAM_MODEL_MAX_BYTES
  ) {
    throw new Error(
      `${scope}.upstreamModel must be well-formed, non-empty, and at most ${AI_UPSTREAM_MODEL_MAX_BYTES} UTF-8 bytes`
    );
  }
  return raw;
}

/** @param {unknown} rawProtocol @param {unknown} rawTransports @param {string} scope */
function normalizeProtocolTransports(rawProtocol, rawTransports, scope) {
  if (typeof rawProtocol !== "string") {
    throw new Error(`${scope}.protocol is not supported`);
  }
  const allowedTransports = PROTOCOL_TRANSPORTS.get(rawProtocol);
  if (!allowedTransports) throw new Error(`${scope}.protocol is not supported`);
  const transports = normalizeStringSet(rawTransports, TRANSPORTS, `${scope}.transports`);
  for (const transport of transports) {
    if (!allowedTransports.has(transport)) {
      throw new Error(`${scope}.transports contains ${transport} for protocol ${rawProtocol}`);
    }
  }
  return { protocol: rawProtocol, transports };
}

/** @param {unknown} raw @param {string} scope */
function normalizeModelDescriptor(raw, scope) {
  const value = requireRecord(raw, scope);
  rejectUnknownFields(value, MODEL_DESCRIPTOR_KEYS, scope);
  const { protocol, transports } = normalizeProtocolTransports(
    value.protocol,
    value.transports,
    scope
  );
  return {
    upstreamModel: normalizeUpstreamModel(value.upstreamModel, scope),
    protocol,
    transports,
    inputModalities: normalizeStringSet(
      value.inputModalities ?? ["text"],
      INPUT_MODALITIES,
      `${scope}.inputModalities`
    ),
    outputModalities: normalizeStringSet(
      value.outputModalities ?? ["text"],
      OUTPUT_MODALITIES,
      `${scope}.outputModalities`
    ),
    capabilities: normalizeCapabilities(value.capabilities, `${scope}.capabilities`),
  };
}

/** @param {string} kind @param {{ protocol: string, transports: string[], inputModalities: string[], capabilities: AiCapabilities }} descriptor @param {string} scope */
function assertProviderRequestSupport(kind, descriptor, scope) {
  if (kind !== "deepseek") return;
  if (descriptor.protocol !== "responses" && descriptor.protocol !== "chat_completions") {
    throw new Error(`${scope}.protocol is not available for provider kind deepseek`);
  }
  if (descriptor.transports.some((transport) => transport !== "http" && transport !== "sse")) {
    throw new Error(`${scope}.transports contains a transport unavailable for provider kind deepseek`);
  }
}

/**
 * @param {unknown} raw
 * @param {{ revision?: string, requireRevision?: boolean }} [options]
 */
function normalizeProvider(raw, { revision, requireRevision = true } = {}) {
  const value = requireRecord(raw, "provider");
  rejectUnknownFields(value, requireRevision ? PROVIDER_RECORD_KEYS : PROVIDER_WRITE_KEYS, "provider");
  const normalizedRevision = requireRevision ? value.revision : revision;
  if (typeof normalizedRevision !== "string" || !AI_PROVIDER_REVISION_RE.test(normalizedRevision)) {
    throw new Error("provider.revision must be 32 lowercase hex characters");
  }
  if (typeof value.kind !== "string" || !PROVIDER_KINDS.has(value.kind)) {
    throw new Error("provider.kind is not supported");
  }
  const models = requireRecord(value.models, "provider.models");
  const entries = Object.entries(models);
  if (entries.length === 0 || entries.length > AI_MODELS_PER_PROVIDER_MAX) {
    throw new Error(`provider.models must contain 1-${AI_MODELS_PER_PROVIDER_MAX} entries`);
  }
  /** @type {Record<string, ReturnType<typeof normalizeModelDescriptor>>} */
  const normalizedModels = {};
  for (const [alias, descriptor] of entries.toSorted(([a], [b]) => compareStrings(a, b))) {
    if (!isValidAiModelAlias(alias)) {
      throw new Error(`provider.models alias ${JSON.stringify(alias)} is invalid`);
    }
    const normalizedDescriptor = normalizeModelDescriptor(descriptor, `provider.models.${alias}`);
    assertProviderRequestSupport(value.kind, normalizedDescriptor, `provider.models.${alias}`);
    normalizedModels[alias] = normalizedDescriptor;
  }
  const out = { revision: normalizedRevision, kind: value.kind, models: normalizedModels };
  if (utf8ByteLength(JSON.stringify(out)) > AI_PROVIDER_RECORD_MAX_BYTES) {
    throw new Error(`provider record exceeds ${AI_PROVIDER_RECORD_MAX_BYTES} UTF-8 bytes`);
  }
  return out;
}

/** @param {unknown} raw */
export function normalizeAiProviderRecord(raw) {
  return normalizeProvider(raw);
}

/** @param {unknown} raw @param {string} revision */
export function normalizeAiProviderWrite(raw, revision) {
  return normalizeProvider(raw, { revision, requireRevision: false });
}

/** @param {unknown} value */
export function parseAiModelReference(value) {
  if (typeof value !== "string") throw new Error("model must be <provider>/<alias>");
  const parts = value.split("/");
  if (parts.length !== 2 || !isValidAiProviderName(parts[0]) || !isValidAiModelAlias(parts[1])) {
    throw new Error("model must be <provider>/<alias>");
  }
  return { provider: parts[0], alias: parts[1] };
}

/** @param {unknown} raw */
export function normalizeAiResolveRequest(raw) {
  const value = requireRecord(raw, "resolve request");
  rejectUnknownFields(value, RESOLVE_REQUEST_KEYS, "resolve request");
  if (!isValidRuntimeLoadNs(value.ns)) throw new Error("resolve request.ns is invalid");
  parseAiModelReference(value.model);
  if (typeof value.protocol !== "string" || !PROTOCOL_TRANSPORTS.has(value.protocol)) {
    throw new Error("resolve request.protocol is not supported");
  }
  if (typeof value.transport !== "string" || !TRANSPORTS.has(value.transport)) {
    throw new Error("resolve request.transport is not supported");
  }
  return {
    ns: value.ns,
    model: value.model,
    protocol: value.protocol,
    transport: value.transport,
  };
}

/** @param {unknown} raw */
export function normalizeAiModelsRequest(raw) {
  const value = requireRecord(raw, "models request");
  rejectUnknownFields(value, MODELS_REQUEST_KEYS, "models request");
  if (!isValidRuntimeLoadNs(value.ns)) throw new Error("models request.ns is invalid");
  return { ns: value.ns };
}

/** @param {unknown} raw */
export function normalizeAiResolveResponse(raw) {
  const value = requireRecord(raw, "resolve response");
  rejectUnknownFields(value, RESOLVE_RESPONSE_KEYS, "resolve response");
  if (!isValidAiProviderName(value.provider)) throw new Error("resolve response.provider is invalid");
  if (!isValidAiModelAlias(value.alias)) throw new Error("resolve response.alias is invalid");
  if (typeof value.kind !== "string" || !PROVIDER_KINDS.has(value.kind)) {
    throw new Error("resolve response.kind is not supported");
  }
  const descriptor = {
    upstreamModel: normalizeUpstreamModel(value.upstreamModel, "resolve response"),
    ...normalizeProtocolTransports(value.protocol, [value.transport], "resolve response"),
    inputModalities: normalizeStringSet(
      value.inputModalities ?? ["text"],
      INPUT_MODALITIES,
      "resolve response.inputModalities"
    ),
    capabilities: normalizeCapabilities(value.capabilities, "resolve response.capabilities"),
  };
  assertProviderRequestSupport(value.kind, descriptor, "resolve response");
  if (typeof value.destination !== "string" || value.destination.length === 0) {
    throw new Error("resolve response.destination is invalid");
  }
  const credential = assertAiCredential(value.credential);
  return {
    provider: value.provider,
    alias: value.alias,
    kind: value.kind,
    upstreamModel: descriptor.upstreamModel,
    protocol: descriptor.protocol,
    transport: descriptor.transports[0],
    destination: value.destination,
    credential,
    inputModalities: descriptor.inputModalities,
    capabilities: descriptor.capabilities,
  };
}

/** @param {unknown} raw @param {string} scope */
function normalizeModelListEntry(raw, scope) {
  const value = requireRecord(raw, scope);
  rejectUnknownFields(value, MODEL_LIST_ENTRY_KEYS, scope);
  if (typeof value.id !== "string") throw new Error(`${scope}.id is invalid`);
  parseAiModelReference(value.id);
  const descriptor = normalizeProtocolTransports(value.protocol, value.transports, scope);
  return {
    id: value.id,
    protocol: descriptor.protocol,
    transports: descriptor.transports,
    inputModalities: normalizeStringSet(
      value.inputModalities ?? ["text"],
      INPUT_MODALITIES,
      `${scope}.inputModalities`
    ),
    outputModalities: normalizeStringSet(
      value.outputModalities ?? ["text"],
      OUTPUT_MODALITIES,
      `${scope}.outputModalities`
    ),
    capabilities: normalizeCapabilities(value.capabilities, `${scope}.capabilities`),
  };
}

/** @param {unknown} raw */
export function normalizeAiModelsResponse(raw) {
  const value = requireRecord(raw, "models response");
  rejectUnknownFields(value, MODELS_RESPONSE_KEYS, "models response");
  if (!Array.isArray(value.models) || value.models.length > AI_NAMESPACE_MODEL_MAX_COUNT) {
    throw new Error(`models response.models must contain at most ${AI_NAMESPACE_MODEL_MAX_COUNT} entries`);
  }
  const models = value.models.map((entry, index) => normalizeModelListEntry(
    entry,
    `models response.models[${index}]`
  ));
  for (let index = 1; index < models.length; index += 1) {
    if (models[index - 1].id >= models[index].id) {
      throw new Error("models response.models must be uniquely sorted by id");
    }
  }
  return { models };
}

/** @param {string} ns */
export function aiProvidersKey(ns) {
  return `ai:providers:${ns}`;
}

/** @param {string} ns */
export function aiProviderCredentialsKey(ns) {
  return `ai:provider-credentials:${ns}`;
}

/** @param {unknown} value */
export function assertAiCredential(value) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    utf8ByteLength(value) > AI_CREDENTIAL_MAX_BYTES ||
    !/^[\x21-\x7e]+$/.test(value)
  ) {
    throw new Error(
      `credential must contain only visible ASCII without whitespace and be at most ${AI_CREDENTIAL_MAX_BYTES} bytes`
    );
  }
  return value;
}
