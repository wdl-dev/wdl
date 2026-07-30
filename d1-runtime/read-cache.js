const DEFAULT_READ_CACHE_TTL_MS = 10_000;
const DEFAULT_READ_CACHE_MAX_ENTRIES = 128;
const VOLATILE_READ_SQL_RE = /\b(?:random|randomblob|changes|total_changes|last_insert_rowid|date|time|datetime|julianday|strftime|unixepoch)\s*\(|\bcurrent_(?:timestamp|date|time)\b/i;
const WRITE_SQL_RE = /\b(?:insert|update|delete|replace|create|drop|alter|pragma|vacuum|attach|detach|reindex|begin|commit|rollback|savepoint|release|analyze)\b/i;
const IDEMPOTENT_SCHEMA_DDL_RE = /^\s*create\s+(table|(?:unique\s+)?index)\s+if\s+not\s+exists\s+("(?:""|[^"])+"|`(?:``|[^`])+`|\[(?:\]\]|[^\]])+\]|[A-Za-z_][A-Za-z0-9_$]*)(?=\s|\(|$)/i;

/**
 * @typedef {{ ttlMs: number, maxEntries: number, D1_READ_CACHE_TTL_MS?: unknown, D1_READ_CACHE_MAX_ENTRIES?: unknown }} ReadCacheConfigInput
 * @typedef {{ ttlMs: number, maxEntries: number }} ReadCacheConfig
 * @typedef {{ sql?: unknown }} ReadCacheStatement
 * @typedef {{ dbKey?: unknown, mode?: unknown, statements?: ReadCacheStatement[] }} ReadCacheQuery
 * @typedef {{ dbKey?: unknown, generation?: unknown }} ReadCacheOwner
 * @typedef {{ increment(name: string, labels?: Record<string, unknown>, value?: number): void }} ReadCacheMetrics
 * @typedef {{ key: string, version: number, expiresAt: number, maxEntries: number }} ReadCacheToken
 * @typedef {{ expiresAt: number, bytes: Uint8Array<ArrayBuffer> }} ReadCacheEntry
 * @typedef {{ hit: boolean, bytes?: Uint8Array<ArrayBuffer>, token?: ReadCacheToken | null }} ReadCacheBeginResult
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
  if (Number.isInteger(ttlMs) && Number.isInteger(maxEntries)) {
    return { ttlMs: /** @type {number} */ (ttlMs), maxEntries: /** @type {number} */ (maxEntries) };
  }
  return {
    ttlMs: nonNegativeIntOr(env?.D1_READ_CACHE_TTL_MS, DEFAULT_READ_CACHE_TTL_MS),
    maxEntries: nonNegativeIntOr(env?.D1_READ_CACHE_MAX_ENTRIES, DEFAULT_READ_CACHE_MAX_ENTRIES),
  };
}

/**
 * @param {ReadCacheQuery | null | undefined} query
 * @param {Partial<ReadCacheConfigInput> | null | undefined} env
 */
export function isReadCacheableQuery(query, env = {}) {
  const config = readCacheConfig(env);
  if (config.ttlMs <= 0 || config.maxEntries <= 0) return false;
  if (!query || (query.mode !== "all" && query.mode !== "raw")) return false;
  if (!Array.isArray(query.statements) || query.statements.length !== 1) return false;
  const sql = query.statements[0]?.sql;
  if (typeof sql !== "string") return false;
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
    this.mutationVersion = 0;
  }

  /** @param {string} name */
  observe(name) {
    this.metrics?.increment("d1_read_cache", { ...this.labels, outcome: name });
  }

  /** @param {string} reason */
  observeInvalidation(reason) {
    this.metrics?.increment("d1_read_cache_invalidations", { ...this.labels, reason });
  }

  /** @param {number} [now] */
  purgeExpired(now = Date.now()) {
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt <= now) this.entries.delete(key);
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
    const key = readCacheKey(query, owner);
    const entry = this.entries.get(key);
    if (!entry) {
      this.observe("miss");
      return {
        hit: false,
        token: {
          key,
          version: this.mutationVersion,
          expiresAt: now + this.config.ttlMs,
          maxEntries: this.config.maxEntries,
        },
      };
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    this.observe("hit");
    return {
      hit: true,
      bytes: entry.bytes,
    };
  }

  /**
   * @param {ReadCacheToken | null | undefined} token
   * @param {Uint8Array<ArrayBuffer>} bytes
   */
  finishRead(token, bytes) {
    if (!token || token.version !== this.mutationVersion) return false;
    this.entries.set(token.key, {
      expiresAt: token.expiresAt,
      bytes,
    });
    while (this.entries.size > token.maxEntries) {
      const oldestKey = this.entries.keys().next().value;
      if (oldestKey === undefined) break;
      this.entries.delete(oldestKey);
    }
    this.observe("store");
    return true;
  }

  /** @param {string} [reason] */
  invalidate(reason = "write") {
    this.mutationVersion += 1;
    const hadEntries = this.entries.size > 0;
    this.entries.clear();
    if (hadEntries) this.observeInvalidation(reason);
  }
}
