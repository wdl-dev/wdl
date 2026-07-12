import { test } from "node:test";
import assert from "node:assert/strict";
import { importRepositoryModule, repositoryFileUrl } from "../helpers/load-shared-module.js";

const requestIdFromOptionsStub = `const requestIdFromOptions = (options) => {
  if (!options || typeof options !== "object") return null;
  if (typeof options.requestIdProvider === "function") {
    const requestId = options.requestIdProvider();
    return typeof requestId === "string" && requestId ? requestId : null;
  }
  return typeof options.requestId === "string" && options.requestId ? options.requestId : null;
};`;

const d1Facade = await importRepositoryModule("runtime/d1-client.js", [
  [/import \{ splitSqlStatements \} from "\.\/_wdl-sql-splitter\.js";/, "const splitSqlStatements = (sql) => [{ sql, params: [] }];"],
  [/import \{ normalizeD1Param \} from "\.\/_wdl-d1-params\.js";/, "const normalizeD1Param = (value) => value;"],
  [/import \{ decodeD1Transport \} from "\.\/_wdl-d1-transport\.js";/, "const decodeD1Transport = (value) => value;"],
  [/import \{ requestIdFromOptions \} from "\.\/_wdl-request-id\.js";/, requestIdFromOptionsStub],
]);

const r2Facade = await importRepositoryModule("runtime/r2-client.js", [
  [
    /from "\.\/_wdl-r2-utils\.js";/,
    `from ${JSON.stringify(repositoryFileUrl("runtime/r2-utils.js"))};`,
  ],
  [/import \{ requestIdFromOptions \} from "\.\/_wdl-request-id\.js";/, requestIdFromOptionsStub],
]);

test("loaded D1 facade does not expose the runtime RPC stub handle", () => {
  const { D1Database, D1PreparedStatement, D1DatabaseSession } = d1Facade;
  const db = new D1Database({ query: async () => ({ results: [] }) });

  assert.equal("_stub" in db, false);
  assert.equal(db._stub, undefined);
  assert.deepEqual(Object.getOwnPropertyNames(D1Database.prototype).toSorted(), [
    "batch",
    "constructor",
    "exec",
    "prepare",
    "withSession",
  ]);
  assert.deepEqual(Object.getOwnPropertyNames(D1PreparedStatement.prototype).toSorted(), [
    "all",
    "bind",
    "constructor",
    "first",
    "raw",
    "run",
  ]);
  assert.deepEqual(Object.getOwnPropertyNames(D1DatabaseSession.prototype).toSorted(), [
    "batch",
    "constructor",
    "getBookmark",
    "prepare",
  ]);
});

test("loaded R2 facade does not expose the runtime RPC stub handle", () => {
  const { R2Bucket } = r2Facade;
  const bucket = new R2Bucket({
    head: async () => null,
    get: async () => null,
    put: async () => null,
    delete: async () => {},
    list: async () => ({ objects: [] }),
  });

  assert.equal("_stub" in bucket, false);
  assert.equal(bucket._stub, undefined);
  assert.deepEqual(Object.getOwnPropertyNames(R2Bucket.prototype).toSorted(), [
    "constructor",
    "createMultipartUpload",
    "delete",
    "get",
    "head",
    "list",
    "put",
    "resumeMultipartUpload",
  ]);
});
