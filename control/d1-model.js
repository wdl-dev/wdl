import { bytesToHex } from "shared-hex";
import { D1_DATABASE_ID_RE } from "shared-ns-pattern";
export { splitSqlStatements } from "shared-sql-splitter";

export { D1_DATABASE_ID_RE };
export const D1_DATABASE_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
export const D1_MIGRATION_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,191}$/;
export const EXECUTE_MODES = new Set(["all", "raw", "run", "exec"]);
export const D1_DATABASE_STATE_PROVISIONAL = "provisional";
export const D1_DATABASE_STATE_READY = "ready";
export const D1_DATABASE_STATE_TOMBSTONED = "tombstoned";
const utf8Encoder = new TextEncoder();
export const MIGRATIONS_TABLE_SQL = `
  create table if not exists _wdl_d1_migrations (
    id text primary key,
    name text not null,
    checksum text not null,
    applied_at text not null
  )
`;

/**
 * @typedef {{
 *   databaseId: string,
 *   databaseName: string | null,
 *   state?: string | null,
 *   provisionalUntil?: string | null,
 *   createdAt: string | undefined,
 *   updatedAt: string | undefined,
 * }} D1DatabaseRecord
 * @typedef {{ id: string, name: string, checksum: string | null }} D1MigrationRef
 * @typedef {D1MigrationRef & { sql: string }} D1MigrationApply
 * @typedef {{ id: string, checksum?: string | null, appliedAt?: string | null }} D1AppliedMigration
 */

/** @param {unknown} databaseId */
export function validateDatabaseId(databaseId) {
  if (typeof databaseId !== "string" || !D1_DATABASE_ID_RE.test(databaseId)) {
    throw new Error(`databaseId must match ${D1_DATABASE_ID_RE}, got ${JSON.stringify(databaseId)}`);
  }
}

/** @param {unknown} databaseRef */
export function validateDatabaseRef(databaseRef) {
  if (typeof databaseRef !== "string" || !D1_DATABASE_ID_RE.test(databaseRef)) {
    throw new Error(`databaseRef must match ${D1_DATABASE_ID_RE}, got ${JSON.stringify(databaseRef)}`);
  }
}

/** @param {unknown} databaseName */
export function validateDatabaseName(databaseName) {
  if (databaseName == null) return;
  if (typeof databaseName !== "string" || !D1_DATABASE_NAME_RE.test(databaseName)) {
    throw new Error(`databaseName must match ${D1_DATABASE_NAME_RE}, got ${JSON.stringify(databaseName)}`);
  }
}

/** @param {unknown} id */
export function validateMigrationId(id) {
  if (typeof id !== "string" || !D1_MIGRATION_ID_RE.test(id)) {
    throw new Error(`migration id must match ${D1_MIGRATION_ID_RE}, got ${JSON.stringify(id)}`);
  }
}

/**
 * @param {Record<string, string | null | undefined> | null | undefined} hash
 * @returns {D1DatabaseRecord | null}
 */
export function decodeDatabaseHash(hash) {
  if (!hash || Object.keys(hash).length === 0) return null;
  const databaseId = typeof hash.databaseId === "string" ? hash.databaseId : "";
  if (!databaseId) return null;
  return {
    databaseId,
    databaseName: typeof hash.databaseName === "string" ? hash.databaseName : null,
    state: typeof hash.state === "string" ? hash.state : null,
    provisionalUntil: typeof hash.provisionalUntil === "string" ? hash.provisionalUntil : null,
    createdAt: typeof hash.createdAt === "string" ? hash.createdAt : undefined,
    updatedAt: typeof hash.updatedAt === "string" ? hash.updatedAt : undefined,
  };
}

/** @param {{ state?: unknown } | null | undefined} database */
export function isReadyDatabase(database) {
  return database?.state === D1_DATABASE_STATE_READY;
}

/**
 * @param {unknown} raw
 * @returns {D1MigrationRef}
 */
export function normalizeMigrationRef(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("migration must be an object");
  }
  // id is the idempotency key; name is display metadata. Simple callers may
  // provide only name, in which case the name is also used as the id.
  const record = /** @type {Record<string, unknown>} */ (raw);
  const id = record.id || record.name;
  validateMigrationId(id);
  const normalizedId = /** @type {string} */ (id);
  const name = typeof record.name === "string" && record.name ? record.name : normalizedId;
  const checksum = typeof record.checksum === "string" ? record.checksum : null;
  return { id: normalizedId, name, checksum };
}

/**
 * @param {unknown} raw
 * @returns {D1MigrationApply}
 */
export function normalizeMigrationApply(raw) {
  const ref = normalizeMigrationRef(raw);
  const record = /** @type {Record<string, unknown>} */ (raw);
  if (typeof record.sql !== "string" || !record.sql.trim()) {
    throw new Error(`migration ${ref.id} requires non-empty sql`);
  }
  return { ...ref, sql: record.sql };
}

/**
 * @param {D1MigrationRef[]} localMigrations
 * @param {D1AppliedMigration[]} appliedMigrations
 */
export function migrationStatus(localMigrations, appliedMigrations) {
  const appliedById = new Map(appliedMigrations.map((migration) => [migration.id, migration]));
  return localMigrations.map((migration) => {
    const applied = appliedById.get(migration.id);
    if (!applied) return { ...migration, state: "pending" };
    if (migration.checksum && applied.checksum !== migration.checksum) {
      return {
        ...migration,
        state: "drifted",
        appliedAt: applied.appliedAt,
        appliedChecksum: applied.checksum,
      };
    }
    return {
      ...migration,
      state: "applied",
      appliedAt: applied.appliedAt,
      appliedChecksum: applied.checksum,
    };
  });
}

/** @param {string} text */
export async function sha256Hex(text) {
  const digest = await crypto.subtle.digest("SHA-256", utf8Encoder.encode(text));
  return bytesToHex(new Uint8Array(digest));
}
