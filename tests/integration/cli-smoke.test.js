// @wdl-cli-integration
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { withTempDir } from "../helpers/temp-dir.js";
import {
  adminGetFresh,
  assertOk,
  gatewayFetch,
  responseJson,
  runWdlCli,
  uniqueNs,
  setupIntegrationSuite,
} from "./helpers/index.js";

setupIntegrationSuite();

test("wdl CLI exercises deploy, workers, secrets, and delete lifecycle", async () => {
  const ns = uniqueNs("wdl-smoke");

  const deploy = runWdlCli(["deploy", "test-workers/kv-demo", "--ns", ns]);
  assertOk(deploy);
  assert.match(deploy.stdout, new RegExp(`${RegExp.escape(ns)}/kv-demo@v1 live`));

  const routed = await gatewayFetch(ns, "/kv-demo/alice");
  assert.equal(routed.status, 200);
  const routedBody = await responseJson(routed);
  assert.equal(routedBody.greeting, "hello from kv-demo");
  assert.equal(routedBody.you, "alice");

  const workersV1 = runWdlCli(["workers", "--ns", ns]);
  assertOk(workersV1);
  assert.match(workersV1.stdout, /kv-demo\tactive=v1\tversions=v1\tsecrets=no/);

  const putSecret = runWdlCli(
    ["secret", "put", "--ns", ns, "--worker", "kv-demo", "CLI_SMOKE_SECRET"],
    { input: "secret-value\n" }
  );
  assertOk(putSecret);
  assert.match(putSecret.stdout, /kv-demo\/CLI_SMOKE_SECRET set .* promoted v1 .* v2/);

  const listSecret = runWdlCli(["secrets", "list", "--ns", ns, "--worker", "kv-demo"]);
  assertOk(listSecret);
  assert.match(listSecret.stdout, /^CLI_SMOKE_SECRET$/m);

  const deleteSecret = runWdlCli([
    "secret",
    "delete",
    "--ns",
    ns,
    "--worker",
    "kv-demo",
    "CLI_SMOKE_SECRET",
    "--yes",
  ]);
  assertOk(deleteSecret);
  assert.match(deleteSecret.stdout, /kv-demo\/CLI_SMOKE_SECRET deleted .* promoted v2 .* v3/);

  const deleteVersion = runWdlCli(["delete", "version", "--ns", ns, "kv-demo", "v1"]);
  assertOk(deleteVersion);
  assert.match(deleteVersion.stdout, new RegExp(`OK ${RegExp.escape(ns)}/kv-demo@v1 deleted`));

  const workersV3 = runWdlCli(["workers", "--ns", ns]);
  assertOk(workersV3);
  assert.match(workersV3.stdout, /kv-demo\tactive=v3\tversions=v2,v3\tsecrets=no/);

  const dryRun = runWdlCli(["delete", "worker", "--ns", ns, "kv-demo", "--dry-run"]);
  assertOk(dryRun);
  assert.match(dryRun.stdout, new RegExp(`DRY RUN ${RegExp.escape(ns)}/kv-demo wouldDelete=yes active=v3 versions=v2,v3`));

  const versions = await adminGetFresh(`/ns/${ns}/worker/kv-demo/versions`);
  assert.equal(versions.status, 200);
  assert.deepEqual(versions.json.versions, [
    { version: "v2", active: false },
    { version: "v3", active: true },
  ]);

  const deleteWorker = runWdlCli(["delete", "worker", "--ns", ns, "kv-demo", "--yes"]);
  assertOk(deleteWorker);
  assert.match(deleteWorker.stdout, new RegExp(`OK ${RegExp.escape(ns)}/kv-demo deleted active=v3 versions=v2,v3`));

  const workersAfterDelete = runWdlCli(["workers", "--ns", ns]);
  assertOk(workersAfterDelete);
  assert.match(workersAfterDelete.stdout, /^\(no workers\)$/m);
});

test("wdl CLI exercises D1 create, migrations, execute, deploy, and delete", async () => {
  const ns = uniqueNs("wdl-d1");

  await withTempDir("wdl-d1-cli-", async (project) => {
    mkdirSync(path.join(project, "src"), { recursive: true });
    mkdirSync(path.join(project, "schema"), { recursive: true });
    writeFileSync(
      path.join(project, "wrangler.toml"),
      [
        'name = "d1-cli-demo"',
        'main = "src/index.js"',
        'compatibility_date = "2026-04-24"',
        "",
        "[[d1_databases]]",
        'binding = "DB"',
        'database_name = "main"',
        'migrations_dir = "schema"',
        "",
      ].join("\n")
    );
    writeFileSync(
      path.join(project, "src", "index.js"),
      [
        "export default {",
        "  async fetch(_request, env) {",
        "    const { results } = await env.DB.prepare(\"select value from cli_smoke order by value\").all();",
        "    return Response.json(results);",
        "  },",
        "};",
        "",
      ].join("\n")
    );
    writeFileSync(
      path.join(project, "schema", "0001_init.sql"),
      [
        "create table cli_smoke (value text not null);",
        "insert into cli_smoke (value) values ('a');",
        "insert into cli_smoke (value) values ('b');",
        "",
      ].join("\n")
    );

    const createDb = runWdlCli(["d1", "create", "--ns", ns, "main"]);
    assertOk(createDb);
    assert.match(createDb.stdout, new RegExp(`OK ${RegExp.escape(ns)}/d1_[a-f0-9]+ created name=main`));

    const migrationStatus = runWdlCli(["d1", "migrations", "status", "--ns", ns, "main"], { cwd: project });
    assertOk(migrationStatus);
    assert.match(migrationStatus.stdout, /0001_init\.sql\s+state=pending/);

    const migrationApply = runWdlCli(["d1", "migrations", "apply", "--ns", ns, "main"], { cwd: project });
    assertOk(migrationApply);
    assert.match(migrationApply.stdout, /Applied 0001_init\.sql\s+statements=3/);

    const execute = runWdlCli([
      "d1", "execute", "--ns", ns, "main", "--sql", "select value from cli_smoke order by value",
    ]);
    assertOk(execute);
    assert.match(execute.stdout, /a/);
    assert.match(execute.stdout, /b/);

    const deploy = runWdlCli(["deploy", project, "--ns", ns]);
    assertOk(deploy);
    assert.match(deploy.stdout, new RegExp(`${RegExp.escape(ns)}/d1-cli-demo@v1 live`));

    const routed = await gatewayFetch(ns, "/d1-cli-demo/");
    assert.equal(routed.status, 200);
    assert.deepEqual(await responseJson(routed), [{ value: "a" }, { value: "b" }]);

    const deleteWorker = runWdlCli(["delete", "worker", "--ns", ns, "d1-cli-demo", "--yes"]);
    assertOk(deleteWorker);
    assert.match(deleteWorker.stdout, new RegExp(`OK ${RegExp.escape(ns)}/d1-cli-demo deleted active=v1 versions=v1`));

    const deleteDb = runWdlCli(["d1", "delete", "--ns", ns, "main", "--yes"]);
    assertOk(deleteDb);
    assert.match(deleteDb.stdout, new RegExp(`OK ${RegExp.escape(ns)}/d1_[a-f0-9]+ deleted`));
  });
});
