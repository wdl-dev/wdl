const DEFAULT_READ_CACHE_TTL_MS = 10_000;
const DEFAULT_READ_CACHE_MAX_ENTRIES = 128;
const DEFAULT_READ_CACHE_MAX_BYTES = 64 * 1024 * 1024;
const MAX_JSON_KEY_RETAINED_BYTES_PER_CODE_UNIT = 12;
const VOLATILE_READ_SQL_RE = /\b(?:random|randomblob|changes|total_changes|last_insert_rowid|date|time|datetime|julianday|strftime|unixepoch)\s*\(|\bcurrent_(?:timestamp|date|time)\b/i;
const WRITE_SQL_RE = /\b(?:insert|update|delete|replace|create|drop|alter|pragma|vacuum|attach|detach|reindex|begin|commit|rollback|savepoint|release|analyze)\b/i;
const IDEMPOTENT_SCHEMA_DDL_RE = /^\s*create\s+(table|(?:unique\s+)?index)\s+if\s+not\s+exists\s+("(?:""|[^"])+"|`(?:``|[^`])+`|\[(?:\]\]|[^\]])+\]|[A-Za-z_][A-Za-z0-9_$]*)(?=\s|\(|$)/i;

/**
 * @typedef {{ ttlMs: number, maxEntries: number, maxBytes?: number, D1_READ_CACHE_TTL_MS?: unknown, D1_READ_CACHE_MAX_ENTRIES?: unknown, D1_READ_CACHE_MAX_BYTES?: unknown }} ReadCacheConfigInput
 * @typedef {{ ttlMs: number, maxEntries: number, maxBytes: number }} ReadCacheConfig
 * @typedef {{ sql?: unknown, params?: unknown[] }} ReadCacheStatement
 * @typedef {{ dbKey?: unknown, mode?: unknown, statements?: ReadCacheStatement[] }} ReadCacheQuery
 * @typedef {{ dbKey?: unknown, generation?: unknown }} ReadCacheOwner
 * @typedef {{ increment(name: string, labels?: Record<string, unknown>, value?: number): void }} ReadCacheMetrics
 * @typedef {{ epoch: symbol, version: number, expiresAt: number }} ReadCacheToken
 * @typedef {{ expiresAt: number, bytes: Uint8Array<ArrayBuffer>, size: number, valueEncoding: string | null }} ReadCacheEntry
 * @typedef {{ hit: boolean, bytes?: Uint8Array<ArrayBuffer>, valueEncoding?: string | null, token?: ReadCacheToken | null }} ReadCacheBeginResult
 */

/**
 * @param {unknown} value
 * @param {number} fallback
 */
function nonNegativeIntOr(value, fallback) {
  if (value == null || value === "") return fallback;
  const n = Number(value);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}

/**
 * @param {Partial<ReadCacheConfigInput> | null | undefined} env
 * @returns {ReadCacheConfig}
 */
export function readCacheConfig(env) {
  const ttlMs = env?.ttlMs;
  const maxEntries = env?.maxEntries;
  const maxBytes = env?.maxBytes;
  if (Number.isInteger(ttlMs) && Number.isInteger(maxEntries)) {
    return {
      ttlMs: /** @type {number} */ (ttlMs),
      maxEntries: /** @type {number} */ (maxEntries),
      maxBytes: Number.isInteger(maxBytes)
        ? /** @type {number} */ (maxBytes)
        : DEFAULT_READ_CACHE_MAX_BYTES,
    };
  }
  return {
    ttlMs: nonNegativeIntOr(env?.D1_READ_CACHE_TTL_MS, DEFAULT_READ_CACHE_TTL_MS),
    maxEntries: nonNegativeIntOr(env?.D1_READ_CACHE_MAX_ENTRIES, DEFAULT_READ_CACHE_MAX_ENTRIES),
    maxBytes: nonNegativeIntOr(env?.D1_READ_CACHE_MAX_BYTES, DEFAULT_READ_CACHE_MAX_BYTES),
  };
}

/**
 * @param {ReadCacheQuery | null | undefined} query
 * @param {Partial<ReadCacheConfigInput> | null | undefined} env
 */
export function isReadCacheableQuery(query, env = {}) {
  const config = readCacheConfig(env);
  if (config.ttlMs <= 0 || config.maxEntries <= 0 || config.maxBytes <= 0) return false;
  if (!query || (query.mode !== "all" && query.mode !== "raw")) return false;
  if (!Array.isArray(query.statements) || query.statements.length !== 1) return false;
  const statement = query.statements[0];
  const sql = statement?.sql;
  if (typeof sql !== "string") return false;
  if (Array.isArray(statement.params) && statement.params.some((param) => param instanceof Uint8Array)) {
    return false;
  }
  const trimmed = sql.trim();
  if (!/^(?:select|with)\b/i.test(trimmed)) return false;
  if (statementMayChangeDb(trimmed) || VOLATILE_READ_SQL_RE.test(trimmed)) return false;
  return true;
}

/** @param {unknown} sql */
export function statementMayChangeDb(sql) {
  // Heuristic: false positives on write keywords inside SELECT string literals
  // only cause a spurious per-db read-cache invalidation, not stale reads.
  return typeof sql === "string" && WRITE_SQL_RE.test(sql.trim());
}

/** @param {unknown} sql */
export function statementMayBeIdempotentSchemaDdl(sql) {
  return parseIdempotentSchemaDdl(sql) !== null;
}

/** @param {string} sql */
function hasNonTrailingStatementSeparator(sql) {
  const trimmed = sql.trimEnd();
  const body = trimmed.endsWith(";") ? trimmed.slice(0, -1) : trimmed;
  return body.includes(";");
}

/** @param {string} identifier */
function unquoteSqlIdentifier(identifier) {
  if (identifier.startsWith('"') && identifier.endsWith('"')) {
    return identifier.slice(1, -1).replaceAll('""', '"');
  }
  if (identifier.startsWith("`") && identifier.endsWith("`")) {
    return identifier.slice(1, -1).replaceAll("``", "`");
  }
  if (identifier.startsWith("[") && identifier.endsWith("]")) {
    return identifier.slice(1, -1).replaceAll("]]", "]");
  }
  return identifier;
}

/** @param {unknown} sql */
export function parseIdempotentSchemaDdl(sql) {
  if (typeof sql !== "string") return null;
  if (hasNonTrailingStatementSeparator(sql)) return null;
  const match = IDEMPOTENT_SCHEMA_DDL_RE.exec(sql);
  if (!match) return null;
  return {
    type: /\bindex\b/i.test(match[1]) ? "index" : "table",
    name: unquoteSqlIdentifier(match[2]),
  };
}

/** @param {unknown} payload */
export function payloadChangedDb(payload) {
  const items = Array.isArray(payload) ? payload : [payload];
  return items.some((item) => {
    const record = /** @type {Record<string, unknown>} */ (Object(item));
    const meta = /** @type {Record<string, unknown>} */ (Object(record.meta));
    return meta.changed_db === true;
  });
}

/**
 * @param {ReadCacheQuery} query
 * @param {ReadCacheOwner} owner
 */
function readCacheKey(query, owner) {
  return JSON.stringify({
    dbKey: owner.dbKey || query.dbKey || null,
    generation: owner.generation,
    mode: query.mode,
    statements: query.statements,
  });
}

/**
 * Bound JSON escaping before materializing the key. One source UTF-16 code unit
 * can become six ASCII code units (for example, `\u0000`), and retained JS
 * strings may use two bytes per code unit.
 *
 * @param {ReadCacheQuery} query
 * @param {ReadCacheOwner} owner
 * @param {number} maxBytes
 */
export function cacheKeyStringsCouldFit(query, owner, maxBytes) {
  let remaining = Math.floor(maxBytes / MAX_JSON_KEY_RETAINED_BYTES_PER_CODE_UNIT);
  /** @param {unknown} value */
  const consume = (value) => {
    if (typeof value !== "string") return true;
    remaining -= value.length;
    return remaining >= 0;
  };
  if (!consume(owner.dbKey || query.dbKey) || !consume(query.mode)) return false;
  for (const statement of query.statements || []) {
    if (!consume(statement?.sql)) return false;
    for (const param of statement?.params || []) {
      if (!consume(param)) return false;
    }
  }
  return true;
}

export class D1ReadCache {
  /**
   * @param {Partial<ReadCacheConfigInput> | null | undefined} env
   * @param {ReadCacheMetrics | null} [metrics]
   * @param {Record<string, unknown>} [labels]
   */
  constructor(env, metrics = null, labels = {}) {
    /** @type {ReadCacheConfig} */
    this.config = readCacheConfig(env);
    /** @type {ReadCacheMetrics | null} */
    this.metrics = metrics;
    this.labels = labels;
    /** @type {Map<string, ReadCacheEntry>} */
    this.entries = new Map();
    this.retainedBytes = 0;
    this.mutationVersion = 0;
    this.epoch = Symbol("d1-read-cache");
  }

  /** @param {string} name */
  observe(name) {
    this.metrics?.increment("d1_read_cache", { ...this.labels, outcome: name });
  }

  /** @param {string} reason */
  observeInvalidation(reason) {
    this.metrics?.increment("d1_read_cache_invalidations", { ...this.labels, reason });
  }

  /** @param {string} key */
  deleteEntry(key) {
    const entry = this.entries.get(key);
    if (!entry || !this.entries.delete(key)) return false;
    this.retainedBytes -= entry.size;
    return true;
  }

  /** @param {number} [now] */
  purgeExpired(now = Date.now()) {
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt <= now) this.deleteEntry(key);
    }
  }

  /**
   * @param {ReadCacheQuery} query
   * @param {ReadCacheOwner} owner
   * @returns {ReadCacheBeginResult}
   */
  beginRead(query, owner) {
    if (!isReadCacheableQuery(query, this.config)) {
      this.observe("bypass");
      return { hit: false, token: null };
    }
    const now = Date.now();
    this.purgeExpired(now);
    if (!cacheKeyStringsCouldFit(query, owner, this.config.maxBytes)) {
      this.observe("bypass");
      return { hit: false, token: null };
    }
    const key = readCacheKey(query, owner);
    const keyBytes = key.length * 2;
    if (keyBytes > this.config.maxBytes) {
      this.observe("bypass");
      return { hit: false, token: null };
    }
    const entry = this.entries.get(key);
    if (!entry) {
      this.observe("miss");
      return {
        hit: false,
        token: {
          epoch: this.epoch,
          version: this.mutationVersion,
          expiresAt: now + this.config.ttlMs,
        },
      };
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    this.observe("hit");
    return {
      hit: true,
      bytes: entry.bytes,
      valueEncoding: entry.valueEncoding,
    };
  }

  /**
   * @param {ReadCacheToken | null | undefined} token
   * @param {ReadCacheQuery} query
   * @param {ReadCacheOwner} owner
   * @param {Uint8Array<ArrayBuffer>} bytes
   * @param {string | null} [valueEncoding]
   */
  finishRead(token, query, owner, bytes, valueEncoding = null) {
    if (
      !token ||
      token.epoch !== this.epoch ||
      token.version !== this.mutationVersion ||
      !cacheKeyStringsCouldFit(query, owner, this.config.maxBytes)
    ) {
      return false;
    }
    const key = readCacheKey(query, owner);
    const size = (key.length * 2) + bytes.byteLength;
    if (size > this.config.maxBytes) {
      this.observe("bypass");
      return false;
    }
    this.deleteEntry(key);
    this.entries.set(key, {
      expiresAt: token.expiresAt,
      bytes,
      size,
      valueEncoding,
    });
    this.retainedBytes += size;
    while (this.entries.size > this.config.maxEntries || this.retainedBytes > this.config.maxBytes) {
      const oldestKey = this.entries.keys().next().value;
      if (oldestKey === undefined) break;
      this.deleteEntry(oldestKey);
    }
    this.observe("store");
    return true;
  }

  retire() {
    this.epoch = Symbol("d1-read-cache");
    this.entries.clear();
    this.retainedBytes = 0;
  }

  /** @param {string} [reason] */
  invalidate(reason = "write") {
    this.mutationVersion += 1;
    const hadEntries = this.entries.size > 0;
    this.entries.clear();
    this.retainedBytes = 0;
    if (hadEntries) this.observeInvalidation(reason);
  }
}
