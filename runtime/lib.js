// Pure helpers for the runtime worker.

import { base64ToBytes, bytesToBase64 } from "shared-base64";
import { WORKER_NAME_RE, isValidRouteNs } from "shared-ns-pattern";
import { parseVersion } from "shared-worker-contract";
import {
  DEFAULT_DYNAMIC_WORKER_COMPATIBILITY_DATE,
  ENHANCED_ERROR_SERIALIZATION_DEFAULT_DATE,
  ENHANCED_ERROR_SERIALIZATION_FLAG,
  LEGACY_ERROR_SERIALIZATION_FLAG,
  MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE,
  isWorkerdExperimentalCompatFlag,
} from "shared-workerd-compat-flags";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();


// Coerce a KV.put value into Uint8Array. ReadableStream is handled by the
// caller (await body); here we stick to sync inputs.
/** @param {unknown} value */
export function toBytes(value) {
  if (typeof value === "string") return utf8Encoder.encode(value);
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new Error("KV put: value must be string | ArrayBuffer | typed array | ReadableStream");
}

export const MAX_QUEUE_DELAY_SECONDS = 86_400;
export const QUEUE_CONTENT_TYPES = Object.freeze({
  JSON: "json",
  TEXT: "text",
  BYTES: "bytes",
  V8: "v8",
});
export const QUEUE_ENVELOPE_FIELDS = Object.freeze({
  ID: "id",
  BODY_B64: "body_b64",
  CONTENT_TYPE: "content_type",
  ATTEMPTS: "attempts",
  FIRST_SEEN_MS: "first_seen_ms",
});

/** @param {unknown} value @param {number} [fallback] @param {string} [field] */
export function normalizeQueueDelaySeconds(value, fallback = 0, field = "delaySeconds") {
  const raw = value == null ? fallback : value;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0 || n > MAX_QUEUE_DELAY_SECONDS) {
    throw new Error(`${field} must be an integer in [0, ${MAX_QUEUE_DELAY_SECONDS}]`);
  }
  return n;
}

/** @param {unknown} body @param {string} contentType */
function encodedQueueBody(body, contentType) {
  let bytes;
  switch (contentType) {
    case QUEUE_CONTENT_TYPES.JSON:
      {
        const json = JSON.stringify(body);
        if (json === undefined) {
          throw new TypeError("queue send: json contentType requires JSON-serializable body");
        }
        bytes = utf8Encoder.encode(json);
      }
      break;
    case QUEUE_CONTENT_TYPES.TEXT:
      if (typeof body !== "string") throw new Error("queue send: text contentType requires string body");
      bytes = utf8Encoder.encode(body);
      break;
    case QUEUE_CONTENT_TYPES.BYTES:
      bytes = toBytes(body);
      break;
    case QUEUE_CONTENT_TYPES.V8:
      throw new Error("queue send: v8 contentType not supported - use json, text, or bytes");
    default:
      throw new Error(`queue send: unsupported contentType "${contentType}"`);
  }
  return { bodyB64: bytesToBase64(bytes), byteLength: bytes.length };
}

/** @param {unknown} body @param {string} contentType @param {number} now */
export function buildQueueEnvelope(body, contentType, now) {
  const encoded = encodedQueueBody(body, contentType);
  return {
    entry: {
      [QUEUE_ENVELOPE_FIELDS.ID]: crypto.randomUUID(),
      [QUEUE_ENVELOPE_FIELDS.BODY_B64]: encoded.bodyB64,
      [QUEUE_ENVELOPE_FIELDS.CONTENT_TYPE]: contentType,
      [QUEUE_ENVELOPE_FIELDS.ATTEMPTS]: "0",
      [QUEUE_ENVELOPE_FIELDS.FIRST_SEEN_MS]: String(now),
    },
    byteLength: encoded.byteLength,
  };
}

/** @param {unknown} internalAttempts */
export function workerQueueAttempts(internalAttempts) {
  const n = Number(internalAttempts);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) + 1 : 1;
}

// Mirror of control/bundle.js#deepFreeze — bundle meta is immutable on the
// wire, so the post-parse object should be too.
/** @template T @param {T} obj @returns {T} */
function deepFreeze(obj) {
  if (obj === null || typeof obj !== "object" || Object.isFrozen(obj)) return obj;
  const record = /** @type {Record<string, unknown>} */ (obj);
  for (const key of Object.keys(record)) deepFreeze(record[key]);
  return Object.freeze(obj);
}

// Loaded workers may declare an older compatibilityDate than the platform
// workers. Keep enhanced error serialization as a floor only before the date
// where workerd made it the default; workerd owns later flag compatibility.
/** @param {string} compatibilityDate */
function platformFloorCompatFlags(compatibilityDate) {
  const out = [];
  if (compatibilityDate < ENHANCED_ERROR_SERIALIZATION_DEFAULT_DATE) {
    out.push(ENHANCED_ERROR_SERIALIZATION_FLAG);
  }
  return out;
}

// Throws on non-array / non-string elements rather than silently coercing
// to []. Control validates this shape at deploy ingress (see
// control/bundle.js#validateCompatibilityFlags), so a throw here only fires
// if Redis bytes got corrupted or were written out-of-band — a failure
// loud enough to surface in cold-load logs, not one that silently drops
// the user's flag list and leaves only the platform floor.
/** @param {unknown} userFlags @param {string} compatibilityDate */
function mergeCompatFlags(userFlags, compatibilityDate) {
  if (userFlags !== undefined && !Array.isArray(userFlags)) {
    throw new Error(
      `meta.compatibilityFlags must be an array (control validates this at deploy), got ${typeof userFlags}`
    );
  }
  const out = [];
  const flags = Array.isArray(userFlags) ? userFlags : [];
  for (const f of flags) {
    if (typeof f !== "string" || f === "") {
      throw new Error(
        `meta.compatibilityFlags entries must be non-empty strings, got ${JSON.stringify(f)}`
      );
    }
    if (isWorkerdExperimentalCompatFlag(f)) {
      throw new Error(
        `meta.compatibilityFlags contains experimental workerd flag ${JSON.stringify(f)}, which WDL does not support for tenant workers`
      );
    }
    if (f === LEGACY_ERROR_SERIALIZATION_FLAG) {
      throw new Error(
        `meta.compatibilityFlags contains unsupported flag ${JSON.stringify(f)}; WDL requires enhanced error serialization`
      );
    }
    out.push(f);
  }
  for (const f of platformFloorCompatFlags(compatibilityDate)) {
    if (!out.includes(f)) out.push(f);
  }
  return out;
}

// Convert an HGETALL Redis result into the shape workerLoader expects.
// `hash` maps module path → bytes; __meta__ is the JSON metadata blob.
// `meta` is returned alongside for buildWorkerEnv; the load-callback
// strips it before handing the object to workerLoader.
/**
 * @typedef {{
 *   modules: Record<string, { type?: unknown }>,
 *   mainModule: string,
 *   compatibilityDate?: unknown,
 *   compatibilityFlags?: unknown,
 *   bindings?: Record<string, Record<string, unknown> & { type?: string, className?: unknown }> | null,
 *   workflows?: { binding?: unknown, className?: unknown }[] | null,
 *   exports?: { entrypoint?: unknown }[] | null,
 *   [key: string]: unknown,
 * }} WorkerBundleMeta
 * @typedef {string | { cjs: string } | { text: string } | { json: unknown } | { wasm: Uint8Array } | { data: Uint8Array }} WorkerModuleValue
 */

/** @type {[string, (bytes: Uint8Array) => WorkerModuleValue][]} */
const moduleDecoderEntries = [
  ["module", (bytes) => utf8Decoder.decode(bytes)],
  ["cjs", (bytes) => ({ cjs: utf8Decoder.decode(bytes) })],
  ["text", (bytes) => ({ text: utf8Decoder.decode(bytes) })],
  ["json", (bytes) => ({ json: JSON.parse(utf8Decoder.decode(bytes)) })],
  ["wasm", (bytes) => ({ wasm: bytes })],
  ["data", (bytes) => ({ data: bytes })],
];
/** @type {ReadonlyMap<string, (bytes: Uint8Array) => WorkerModuleValue>} */
const moduleDecoders = new Map(moduleDecoderEntries);

/** @param {Record<string, Uint8Array | undefined>} hash */
export function bundleToWorkerCode(hash) {
  const metaBytes = hash.__meta__;
  if (!metaBytes) throw new Error("Bundle missing __meta__ field");
  const parsedMeta = JSON.parse(utf8Decoder.decode(metaBytes));
  if (
    !parsedMeta ||
    typeof parsedMeta !== "object" ||
    Array.isArray(parsedMeta) ||
    !/** @type {{ modules?: unknown }} */ (parsedMeta).modules ||
    typeof /** @type {{ modules?: unknown, mainModule?: unknown }} */ (parsedMeta).modules !== "object" ||
    Array.isArray(/** @type {{ modules?: unknown }} */ (parsedMeta).modules) ||
    typeof /** @type {{ mainModule?: unknown }} */ (parsedMeta).mainModule !== "string" ||
    /** @type {{ mainModule?: string }} */ (parsedMeta).mainModule === ""
  ) {
    throw new Error("Bundle metadata is invalid");
  }
  const meta = /** @type {WorkerBundleMeta} */ (deepFreeze(parsedMeta));

  /** @type {Record<string, WorkerModuleValue>} */
  const modules = {};
  for (const [path, info] of Object.entries(meta.modules)) {
    const bytes = hash[path];
    if (!bytes) throw new Error(`Bundle missing module "${path}"`);
    if (info.type === "py") {
      throw new Error(`Module "${path}": Python Workers modules are not supported by WDL`);
    }
    const decoder = typeof info.type === "string" ? moduleDecoders.get(info.type) : undefined;
    if (!decoder) throw new Error(`Module "${path}": unknown type "${info.type}"`);
    modules[path] = decoder(bytes);
  }

  // Default past 2024-03-26 so Fetcher's removed get/put/delete shortcut
  // methods cannot shadow RPC method names on bindings.
  const compatibilityDate = typeof meta.compatibilityDate === "string" && meta.compatibilityDate
    ? meta.compatibilityDate
    : DEFAULT_DYNAMIC_WORKER_COMPATIBILITY_DATE;
  if (compatibilityDate < MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE) {
    throw new Error(
      `meta.compatibilityDate ${compatibilityDate} is older than WDL supports (${MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE})`
    );
  }

  return {
    compatibilityDate,
    compatibilityFlags: mergeCompatFlags(meta.compatibilityFlags, compatibilityDate),
    mainModule: meta.mainModule,
    modules,
    meta,
  };
}

// WDL's Redis envelope is internal; handlers see Workerd/Cloudflare's
// native message shape after this decode step.
/** @param {string} bodyB64 @param {string} contentType */
export function decodeQueueBody(bodyB64, contentType) {
  const bytes = base64ToBytes(bodyB64 || "");
  switch (contentType) {
    case QUEUE_CONTENT_TYPES.TEXT:  return utf8Decoder.decode(bytes);
    case QUEUE_CONTENT_TYPES.JSON:  return JSON.parse(utf8Decoder.decode(bytes));
    case QUEUE_CONTENT_TYPES.BYTES: return bytes;
    default: throw new Error(`queue message: unsupported contentType "${contentType}"`);
  }
}

/** @param {unknown} value @returns {Record<string, unknown> | null} */
function objectRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? /** @type {Record<string, unknown>} */ (value)
    : null;
}

/** @param {unknown} body */
export function normalizeScheduledDispatchBody(body) {
  const record = objectRecord(body);
  const scheduledTime = record?.scheduledTime;
  if (typeof scheduledTime !== "number" || !Number.isFinite(scheduledTime)) {
    throw new Error("scheduledTime must be a finite number");
  }
  const cron = requiredString(record, "cron");
  return { scheduledTime, cron };
}

/** @param {unknown} body */
export function normalizeQueuedDispatchBody(body) {
  const record = objectRecord(body);
  const queueName = requiredString(record, "queue");
  const messages = record?.messages;
  if (!Array.isArray(messages)) {
    throw new Error("messages must be an array");
  }
  return { queueName, messages };
}

/** @param {Record<string, unknown> | null} body @param {string} field */
function requiredString(body, field) {
  const value = body?.[field];
  if (typeof value !== "string" || value === "") {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value;
}

/** @param {Record<string, unknown> | null} body @param {string} field @param {RegExp} pattern @param {string} description */
function requiredPatternString(body, field, pattern, description) {
  const value = requiredString(body, field);
  if (!pattern.test(value)) {
    throw new Error(`${field} must be ${description}`);
  }
  return value;
}

/** @param {Record<string, unknown> | null} body */
function requiredRouteNs(body) {
  const ns = requiredString(body, "ns");
  if (!isValidRouteNs(ns)) {
    throw new Error("ns must be a route namespace");
  }
  return ns;
}

/** @param {Record<string, unknown> | null} body */
function requiredWorkerName(body) {
  return requiredPatternString(body, "worker", WORKER_NAME_RE, "a worker name");
}

/** @param {Record<string, unknown> | null} body */
function requiredVersion(body) {
  const version = requiredString(body, "frozenVersion");
  if (parseVersion(version) == null) {
    throw new Error("frozenVersion must be an immutable worker version");
  }
  return version;
}

/** @param {Record<string, unknown> | null} body @param {string} field */
function requiredPositiveInteger(body, field) {
  const value = body?.[field];
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${field} must be a positive integer`);
  }
  return value;
}

/** @param {unknown} body */
export function normalizeWorkflowRunBody(body) {
  const record = objectRecord(body);
  return {
    ns: requiredRouteNs(record),
    worker: requiredWorkerName(record),
    frozenVersion: requiredVersion(record),
    workflowName: requiredString(record, "workflowName"),
    workflowKey: requiredString(record, "workflowKey"),
    className: requiredString(record, "className"),
    instanceId: requiredString(record, "instanceId"),
    generation: requiredPositiveInteger(record, "generation"),
    createdAtMs: requiredPositiveInteger(record, "createdAtMs"),
    runToken: requiredString(record, "runToken"),
    event: objectRecord(record?.event) ?? { payload: record?.params ?? null },
  };
}

/** @param {unknown} body */
export function normalizeWorkflowNotifyBody(body) {
  const record = objectRecord(body);
  return {
    ns: requiredRouteNs(record),
    worker: requiredWorkerName(record),
    frozenVersion: requiredVersion(record),
    workflowName: requiredString(record, "workflowName"),
    workflowKey: requiredString(record, "workflowKey"),
    className: requiredString(record, "className"),
    instanceId: requiredString(record, "instanceId"),
    generation: requiredPositiveInteger(record, "generation"),
    callback: objectRecord(record?.callback),
    progress: objectRecord(record?.progress),
  };
}

/** @param {Array<Record<string, string>>} messages */
export function decodeQueuedDispatchMessages(messages) {
  return messages.map((m) => {
    const firstSeenMs = Number(m.first_seen_ms);
    return {
      id: m.id,
      timestamp: new Date(Number.isFinite(firstSeenMs) ? firstSeenMs : Date.now()),
      attempts: workerQueueAttempts(m.attempts),
      body: decodeQueueBody(m.body_b64, m.content_type),
    };
  });
}

// Path is untrusted worker input: reject empty/./.. segments so
// url("../../otherns/...") can't normalize client-side outside this
// worker's prefix; percent-encode each segment so "?", "#", or spaces
// can't hijack names into query strings or fragments.
/** @param {string | undefined | null} cdnBase @param {string} prefix @param {unknown} path */
export function buildAssetUrl(cdnBase, prefix, path) {
  if (!cdnBase) throw new Error("ASSETS.url: cdnBase is not configured");
  if (typeof prefix !== "string" || !prefix || !prefix.endsWith("/")) {
    throw new Error("ASSETS.url: prefix must be a non-empty string ending in '/'");
  }
  if (typeof path !== "string") throw new Error("ASSETS.url: path must be a string");
  const base = cdnBase.replace(/\/+$/, "");
  const stripped = path.replace(/^\/+/, "");
  if (stripped === "") return `${base}/${prefix}`;
  const segments = stripped.split("/");
  for (const seg of segments) {
    if (seg === "" || seg === "." || seg === "..") {
      throw new Error(`ASSETS.url: invalid path segment ${JSON.stringify(seg)} in ${JSON.stringify(path)}`);
    }
  }
  const encoded = segments.map((s) => encodeURIComponent(s)).join("/");
  return `${base}/${prefix}${encoded}`;
}
