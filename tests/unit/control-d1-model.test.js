import assert from "node:assert/strict";
import { test } from "node:test";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";

const SQL_SPLITTER_URL = repositoryFileUrl("shared/sql-splitter.js");
const SHARED_HEX_URL = repositoryFileUrl("shared/hex.js");
const SHARED_NS_URL = repositoryFileUrl("shared/ns-pattern.js");
const {
  decodeDatabaseHash,
  isReadyDatabase,
  migrationStatus,
  normalizeMigrationApply,
  normalizeMigrationRef,
  sha256Hex,
  splitSqlStatements,
  validateDatabaseId,
  validateDatabaseRef,
} = await importRepositoryModule("control/d1-model.js", [
  [/export \{ splitSqlStatements \} from "shared-sql-splitter";/,
    `export { splitSqlStatements } from ${JSON.stringify(SQL_SPLITTER_URL)};`],
  [/from "shared-hex";/, `from ${JSON.stringify(SHARED_HEX_URL)};`],
  [/from "shared-ns-pattern";/, `from ${JSON.stringify(SHARED_NS_URL)};`],
]);

test("control D1: database metadata state is explicit", () => {
  const ready = decodeDatabaseHash({
    databaseId: "d1_main",
    databaseName: "main",
    state: "ready",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
  });
  const provisional = decodeDatabaseHash({
    databaseId: "d1_tmp",
    databaseName: "tmp",
    state: "provisional",
    provisionalUntil: "2026-01-01T00:10:00.000Z",
  });

  assert.equal(isReadyDatabase(ready), true);
  assert.equal(isReadyDatabase(provisional), false);
  assert.equal(provisional.provisionalUntil, "2026-01-01T00:10:00.000Z");
});

test("control D1: SQL splitter preserves trigger bodies", () => {
  const statements = splitSqlStatements(`
    create table posts (id integer primary key, title text);
    create table audit (message text);
    create trigger posts_ai after insert on posts
    begin
      insert into audit values ('created;inside');
      update posts set title = title || ';done' where id = new.id;
    end;
    insert into posts (title) values ('hello');
  `);

  assert.equal(statements.length, 4);
  assert.match(statements[2].sql, /create trigger posts_ai/i);
  assert.match(statements[2].sql, /created;inside/);
  assert.match(statements[2].sql, /;done/);
});

test("control D1: migration status classifies applied pending and drifted", () => {
  assert.deepEqual(
    migrationStatus(
      [
        { id: "0001_init.sql", name: "0001_init.sql", checksum: "a" },
        { id: "0002_add.sql", name: "0002_add.sql", checksum: "b" },
        { id: "0003_seed.sql", name: "0003_seed.sql", checksum: "c" },
      ],
      [
        { id: "0001_init.sql", checksum: "a", appliedAt: "2026-01-01T00:00:00.000Z" },
        { id: "0002_add.sql", checksum: "old", appliedAt: "2026-01-02T00:00:00.000Z" },
      ]
    ).map((/** @type {any} */ migration) => ({
      id: migration.id,
      state: migration.state,
      appliedChecksum: migration.appliedChecksum,
    })),
    [
      { id: "0001_init.sql", state: "applied", appliedChecksum: "a" },
      { id: "0002_add.sql", state: "drifted", appliedChecksum: "old" },
      { id: "0003_seed.sql", state: "pending", appliedChecksum: undefined },
    ]
  );
});

test("control D1: migration normalization validates shape", () => {
  assert.deepEqual(
    normalizeMigrationRef({ id: "0001_init.sql", checksum: "abc" }),
    { id: "0001_init.sql", name: "0001_init.sql", checksum: "abc" }
  );
  assert.deepEqual(
    normalizeMigrationApply({ name: "0002_add.sql", sql: "select 1;" }),
    { id: "0002_add.sql", name: "0002_add.sql", checksum: null, sql: "select 1;" }
  );
  assert.throws(() => normalizeMigrationApply({ id: "0003_empty.sql", sql: "   " }), /requires non-empty sql/);
  assert.throws(() => validateDatabaseId("bad:id"), /databaseId must match/);
  assert.throws(() => validateDatabaseRef("bad:id"), /databaseRef must match/);
});

test("control D1: checksum is stable sha256 hex", async () => {
  assert.equal(
    await sha256Hex("select 1;\n"),
    "4a45092ccf992ea92250053a80b931b787924ba61648f420555511b84f10ab6c"
  );
});
