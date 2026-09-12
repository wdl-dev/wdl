// @wdl-cli-integration
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { sessionPolicyKey } from "../../shared/worker-contract.js";
import { withTempDir } from "../helpers/temp-dir.js";
import { workflowDefinitionsUrl } from "../helpers/workflow-definitions.js";
import { redisDel, redisGetJson } from "./helpers/redis.js";
import { WORKER_CODE, workerMeta, workflowInstanceStateKey } from "./helpers/workflows-scenarios.js";
import {
  adminGetFresh,
  assertOk,
  deployAndPromote,
  gatewayFetch,
  parseStdoutJson,
  readIntegrationJson,
  responseJson,
  runWdlCli,
  uniqueNs,
  setupIntegrationSuite,
  waitUntil,
} from "./helpers/index.js";

setupIntegrationSuite();

const { WORKFLOW_DEFINITION_PAGE_MAX_WORKERS } = await import(workflowDefinitionsUrl);

test("wdl CLI follows definition cursors through empty and short pages", async () => {
  const ns = uniqueNs("wdl-wf-pages");
  const middleWorker = `page-${String(WORKFLOW_DEFINITION_PAGE_MAX_WORKERS).padStart(2, "0")}`;
  for (let index = 0; index < 2 * WORKFLOW_DEFINITION_PAGE_MAX_WORKERS; index += 1) {
    const name = `page-${String(index).padStart(2, "0")}`;
    await deployAndPromote(ns, name, name === middleWorker ? {
      code: WORKER_CODE,
      workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }],
    } : { code: 'export default { fetch() { return new Response("ok"); } };' });
  }
  await deployAndPromote(ns, "z-flow", { code: WORKER_CODE, workflows: [
    { name: "orders", binding: "ORDERS", className: "OrderWorkflow" },
    { name: "refunds", binding: "REFUNDS", className: "OrderWorkflow" },
  ] });

  const expected = [[], [`${middleWorker}/orders`], ["z-flow/orders", "z-flow/refunds"]];
  /** @type {string | null} */
  let cursor = null;
  let pages = 0;
  do {
    assert.ok(pages < expected.length, "definition pagination must terminate");
    const args = ["workflows", "list", "--ns", ns, "--limit", "2"];
    if (cursor !== null) args.push("--cursor", cursor);
    const json = runWdlCli([...args, "--json"]);
    assertOk(json);
    const page = parseStdoutJson(json.stdout, "Workflow definition page");
    assert.equal(page.namespace, ns);
    assert.deepEqual(page.workflows.map((/** @type {{worker: string, name: string}} */ entry) =>
      `${entry.worker}/${entry.name}`), expected[pages]);
    const human = runWdlCli(args);
    assertOk(human);
    if (pages === 0) assert.match(human.stdout, /^\(no workflows on this page\)$/m);
    for (const name of expected[pages]) assert.ok(human.stdout.includes(`${name}\t`));
    if (pages < expected.length - 1) {
      assert.equal(typeof page.cursor, "string");
      assert.ok(page.cursor.length > 0);
      assert.notEqual(page.cursor, cursor);
      assert.ok(human.stdout.split(/\r?\n/).includes(`Next cursor: ${page.cursor}`));
    } else {
      assert.equal(page.cursor, null);
      assert.doesNotMatch(human.stdout, /^Next cursor:/m);
    }
    cursor = page.cursor;
    pages += 1;
  } while (cursor !== null);
  assert.equal(pages, expected.length);
});

test("wdl CLI follows instance cursors after an empty page without skipping results", async () => {
  const ns = uniqueNs("wdl-wf-instances");
  const version = await deployAndPromote(ns, "shop", {
    code: WORKER_CODE,
    vars: { LABEL: "cli-pagination" },
    workflows: [{ name: "orders", binding: "ORDERS", className: "OrderWorkflow" }],
  });
  const ids = ["item-0", "item-1", "item-2"];
  for (const id of ids) {
    const created = await readIntegrationJson(await gatewayFetch(ns, `/shop/create?id=${id}`), 200);
    assert.equal(created.id, id);
  }
  const command = ["workflows", "instances", "shop", "orders", "--ns", ns];
  await waitUntil("CLI pagination fixtures have durable outputs", async () => {
    const result = runWdlCli([...command, "--json"]);
    assertOk(result);
    const page = parseStdoutJson(result.stdout, "Workflow instances before pagination");
    return page.instances.length === ids.length && page.instances.every(
      (/** @type {{status: string}} */ instance) => instance.status === "completed"
    );
  });
  // Model a state disappearing after its rank-index entry was selected.
  const workflowKey = workerMeta(ns, "shop", version).workflows[0].workflowKey;
  redisDel(workflowInstanceStateKey(ns, workflowKey, ids[0]), { db: 2 });

  /** @type {string[]} */
  const seen = [];
  /** @type {string | null} */
  let cursor = null;
  let pages = 0;
  do {
    assert.ok(pages < ids.length, "instance pagination must terminate");
    const args = [...command, "--limit", "1"];
    if (cursor !== null) args.push("--cursor", cursor);
    const json = runWdlCli([...args, "--json"]);
    assertOk(json);
    const page = parseStdoutJson(json.stdout, "Workflow instance page");
    const human = runWdlCli(args);
    assertOk(human);
    if (pages === 0) {
      assert.deepEqual(page.instances, []);
      assert.match(human.stdout, /^\(no workflow instances on this page\)$/m);
    } else {
      assert.equal(page.instances.length, 1);
      const instance = page.instances[0];
      assert.equal(instance.id, ids[pages]);
      assert.equal(instance.status, "completed");
      assert.equal(instance.output.instanceId, instance.id);
      assert.equal(instance.output.fromEnv, "cli-pagination");
      assert.ok(human.stdout.split(/\r?\n/).includes(`${instance.id}\tstatus=completed`));
      seen.push(instance.id);
    }
    if (pages < ids.length - 1) {
      assert.equal(page.cursor, String(pages + 1));
      assert.ok(human.stdout.split(/\r?\n/).includes(`Next cursor: ${page.cursor}`));
    } else {
      assert.equal(page.cursor, null);
      assert.doesNotMatch(human.stdout, /^Next cursor:/m);
    }
    cursor = page.cursor;
    pages += 1;
  } while (cursor !== null);
  assert.equal(pages, ids.length);
  assert.deepEqual(seen, ids.slice(1));
});

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

  const deleteVersion = runWdlCli([
    "delete",
    "version",
    "--ns",
    ns,
    "kv-demo",
    "v1",
    "--yes",
  ]);
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

test("wdl CLI carries restart session policy through promotion", async () => {
  const ns = uniqueNs("wdl-session-policy");

  await withTempDir("wdl-session-policy-cli-", async (project) => {
    mkdirSync(path.join(project, "src"), { recursive: true });
    writeFileSync(
      path.join(project, "wrangler.toml"),
      [
        'name = "session-policy-cli"',
        'main = "src/index.js"',
        'compatibility_date = "2026-04-24"',
        "",
        "[wdl]",
        'session_policy = "restart"',
        "",
      ].join("\n")
    );
    writeFileSync(
      path.join(project, "src", "index.js"),
      'export default { fetch() { return new Response("ok"); } };\n'
    );

    const deploy = runWdlCli(["deploy", project, "--ns", ns]);
    assertOk(deploy);
    assert.match(
      deploy.stdout,
      new RegExp(`${RegExp.escape(ns)}/session-policy-cli@v1 live`)
    );
    assert.deepEqual(redisGetJson(sessionPolicyKey(ns, "session-policy-cli")), {
      version: "v1",
      mode: "restart",
      restartSequence: 1,
    });

    const deleted = runWdlCli([
      "delete", "worker", "--ns", ns, "session-policy-cli", "--yes",
    ]);
    assertOk(deleted);
  });
});

test("wdl CLI configures, deploys, and invokes an AI binding", async () => {
  const ns = uniqueNs("wdl-ai");

  await withTempDir("wdl-ai-cli-", async (project) => {
    mkdirSync(path.join(project, "src"), { recursive: true });
    const providerFile = path.join(project, "provider.openai.json");
    writeFileSync(
      path.join(project, "wrangler.toml"),
      [
        'name = "ai-cli-demo"',
        'main = "src/index.js"',
        'compatibility_date = "2026-08-11"',
        "",
        "[ai]",
        'binding = "AI"',
        "",
      ].join("\n")
    );
    writeFileSync(
      path.join(project, "src", "index.js"),
      [
        "export default {",
        "  async fetch(_request, env) {",
        '    const result = await env.AI.run("openai/primary", { input: "CLI integration" });',
        "    return Response.json({",
        "      model: result.model,",
        "      status: result.status,",
        "      text: result.output?.[0]?.content?.[0]?.text,",
        "    });",
        "  },",
        "};",
        "",
      ].join("\n")
    );
    writeFileSync(
      providerFile,
      JSON.stringify({
        kind: "openai",
        models: {
          primary: {
            upstreamModel: "gpt-test",
            protocol: "responses",
            transports: ["http"],
            inputModalities: ["text"],
            outputModalities: ["text"],
          },
        },
      }, null, 2)
    );

    const created = runWdlCli(
      ["ai", "providers", "put", "openai", "--file", providerFile, "--ns", ns, "--json"],
      { cwd: project }
    );
    assertOk(created);
    assert.equal(
      parseStdoutJson(created.stdout, "AI provider create").provider.credentialConfigured,
      false
    );

    const models = runWdlCli(["ai", "models", "--ns", ns, "--json"]);
    assertOk(models);
    assert.deepEqual(
      parseStdoutJson(models.stdout, "AI model list").models.map(
        (/** @type {{ id: string }} */ model) => model.id
      ),
      ["openai/primary"]
    );

    const credential = runWdlCli(
      ["ai", "credential", "put", "openai", "--ns", ns, "--json"],
      { input: "fake-openai-key\n" }
    );
    assertOk(credential);

    const provider = runWdlCli([
      "ai", "providers", "get", "openai", "--ns", ns, "--json",
    ]);
    assertOk(provider);
    assert.equal(
      parseStdoutJson(provider.stdout, "AI provider read").provider.credentialConfigured,
      true
    );

    const deploy = runWdlCli(["deploy", project, "--ns", ns]);
    assertOk(deploy);
    assert.match(deploy.stdout, new RegExp(`${RegExp.escape(ns)}/ai-cli-demo@v1 live`));

    const routed = await gatewayFetch(ns, "/ai-cli-demo/");
    assert.equal(routed.status, 200);
    assert.deepEqual(await responseJson(routed), {
      model: "gpt-test",
      status: "completed",
      text: "fake response",
    });

    const deleted = runWdlCli([
      "ai", "providers", "delete", "openai", "--ns", ns, "--yes", "--json",
    ]);
    assertOk(deleted);
    assert.equal(parseStdoutJson(deleted.stdout, "AI provider delete").deleted, true);
  });
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
