// @wdl-cli-integration
// R2 binding end-to-end through the CLI deploy path and a Wrangler project.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { withTempDir } from "../helpers/temp-dir.js";
import {
  ROOT,
  assertOk,
  gatewayFetch,
  responseJson,
  runWdlCli,
  uniqueNs,
  setupIntegrationSuite,
} from "./helpers/index.js";

const R2_DEMO_SRC = readFileSync(
  path.join(ROOT, "test-workers/r2-demo/src/index.js"),
  "utf8"
);

setupIntegrationSuite();

/**
 * @param {string} ns @param {string} worker @param {string} method @param {string} pathSuffix
 * @param {string | Buffer} [body] @param {Record<string, string>} [headers]
 */
async function call(ns, worker, method, pathSuffix, body, headers = {}) {
  return gatewayFetch(ns, `/${worker}${pathSuffix}`, { method, body, headers });
}

/** @param {string} dir @param {string} name */
function writeR2Project(dir, name) {
  mkdirSync(path.join(dir, "src"), { recursive: true });
  writeFileSync(
    path.join(dir, "wrangler.toml"),
    [
      `name = "${name}"`,
      'main = "src/index.js"',
      'compatibility_date = "2026-04-24"',
      "",
      "[[r2_buckets]]",
      'binding = "B"',
      'bucket_name = "uploads"',
      "",
    ].join("\n")
  );
  writeFileSync(path.join(dir, "src/index.js"), R2_DEMO_SRC);
}

test("R2 demo: CLI deploy covers binding operations, stream copy, and buffer integrity", async () => {
  const ns = uniqueNs("r2-cli");
  const deploy = runWdlCli(["deploy", "test-workers/r2-demo", "--ns", ns]);
  assertOk(deploy);
  assert.match(deploy.stdout, new RegExp(`${RegExp.escape(ns)}/r2-demo@v1 live`));

  let res = await call(
    ns,
    "r2-demo",
    "PUT",
    "/object?key=a%26b.txt&source=stream-put",
    "hello-r2",
    { "content-type": "text/plain" }
  );
  assert.equal(res.status, 200);
  const put = await responseJson(res);
  assert.equal(put.key, "a&b.txt");
  assert.equal(put.size, 8);
  assert.deepEqual(put.httpMetadata, {
    contentType: "text/plain",
    cacheControl: "max-age=60",
  });
  assert.deepEqual(put.customMetadata, { source: "stream-put" });

  res = await call(ns, "r2-demo", "GET", "/object?key=a%26b.txt");
  assert.equal(res.status, 200);
  const got = await responseJson(res);
  assert.equal(got.text, "hello-r2");
  assert.equal(got.size, 8);
  assert.equal(got.httpMetadata.contentType, "text/plain");
  assert.equal(got.customMetadata.source, "stream-put");

  res = await call(ns, "r2-demo", "GET", "/head?key=a%26b.txt");
  assert.equal(res.status, 200);
  const head = await responseJson(res);
  assert.equal(head.key, "a&b.txt");
  assert.equal(head.size, 8);
  assert.equal(head.text, null);

  res = await call(ns, "r2-demo", "GET", "/list");
  assert.equal(res.status, 200);
  const listed = await responseJson(res);
  assert.deepEqual(listed.objects.map((/** @type {any} */ o) => o.key), ["a&b.txt"]);
  assert.equal(listed.objects[0].httpMetadata.contentType, "text/plain");
  assert.equal(listed.objects[0].customMetadata.source, "stream-put");

  res = await call(
    ns,
    "r2-demo",
    "PUT",
    "/object?key=range.txt",
    "0123456789",
    { "content-type": "text/plain" }
  );
  assert.equal(res.status, 200);

  res = await call(ns, "r2-demo", "GET", "/object?key=range.txt&range=1&offset=5&length=2");
  assert.equal(res.status, 200);
  const range = await responseJson(res);
  assert.equal(range.text, "56");
  assert.equal(range.size, 10);
  assert.deepEqual(range.range, { offset: 5, length: 2 });

  res = await call(ns, "r2-demo", "POST", "/copy-stream?src=range.txt&dst=copied.txt");
  assert.equal(res.status, 200);
  const copied = await responseJson(res);
  assert.equal(copied.put.key, "copied.txt");
  assert.equal(copied.put.size, 10);
  assert.equal(copied.copied.text, "0123456789");
  assert.deepEqual(copied.copied.customMetadata, { copiedfrom: "range.txt" });

  res = await call(ns, "r2-demo", "POST", "/buffer-integrity?key=buffer.bin");
  assert.equal(res.status, 200);
  assert.deepEqual(await responseJson(res), {
    size: 4,
    text: "help",
    disguisedStreamBytes: [108, 112, 33, 34],
    queuedStreamBytes: [0, 0, 0, 0],
    fixedStreamBytes: [104, 101, 108, 112],
    resizableStreamBytes: [104, 101, 108, 112],
    outOfBoundsRejected: true,
    outOfBoundsAbsent: true,
  });

  const buckets = runWdlCli(["r2", "buckets", "list", "--ns", ns]);
  assertOk(buckets);
  assert.match(buckets.stdout, /R2 buckets/);
  assert.match(buckets.stdout, /uploads/);

  const objects = runWdlCli(["r2", "objects", "list", "uploads", "--ns", ns, "--prefix", "range"]);
  assertOk(objects);
  assert.match(objects.stdout, /range\.txt/);

  const headObject = runWdlCli(["r2", "objects", "head", "uploads", "copied.txt", "--ns", ns]);
  assertOk(headObject);
  assert.match(headObject.stdout, /R2 object .*\/uploads\/copied\.txt:/);
  assert.match(headObject.stdout, /size: 10/);
  assert.match(headObject.stdout, /customMetadata\.copiedfrom: range\.txt/);

  const outFile = path.join(tmpdir(), `${ns}-copied.txt`);
  const getObject = runWdlCli(["r2", "objects", "get", "uploads", "copied.txt", "--ns", ns, "--out", outFile]);
  assertOk(getObject);
  assert.equal(readFileSync(outFile, "utf8"), "0123456789");
  rmSync(outFile, { force: true });

  const deleteObject = runWdlCli(["r2", "objects", "delete", "uploads", "copied.txt", "--ns", ns, "--yes"]);
  assertOk(deleteObject);
  assert.match(deleteObject.stdout, /copied\.txt deleted/);
  res = await call(ns, "r2-demo", "GET", "/object?key=copied.txt");
  assert.equal(res.status, 200);
  assert.equal(await responseJson(res), null);

  res = await call(ns, "r2-demo", "GET", "/conditional?key=range.txt");
  assert.equal(res.status, 200);
  const conditional = await responseJson(res);
  assert.equal(conditional.matched.text, "0123456789");
  assert.equal(conditional.missedHasBody, false);
  assert.equal(conditional.missedSize, 10);
  assert.equal(typeof conditional.missedEtag, "string");
  assert.ok(conditional.missedEtag.length > 0);

  res = await call(ns, "r2-demo", "DELETE", "/object?key=a%26b.txt");
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "deleted");
  res = await call(ns, "r2-demo", "GET", "/object?key=a%26b.txt");
  assert.equal(res.status, 200);
  assert.equal(await responseJson(res), null);
});

test("R2 demo: CLI-deployed workers in the same namespace share the same bucket_name", async () => {
  const ns = uniqueNs("r2-cli-shared");
  await withTempDir("r2-demo-b-", async (otherProject) => {
    writeR2Project(otherProject, "r2-demo-b");
    const deployA = runWdlCli(["deploy", "test-workers/r2-demo", "--ns", ns]);
    assertOk(deployA);
    const deployB = runWdlCli(["deploy", otherProject, "--ns", ns]);
    assertOk(deployB);
    assert.match(deployA.stdout, new RegExp(`${RegExp.escape(ns)}/r2-demo@v1 live`));
    assert.match(deployB.stdout, new RegExp(`${RegExp.escape(ns)}/r2-demo-b@v1 live`));

    let res = await call(
      ns,
      "r2-demo",
      "PUT",
      "/object?key=shared.txt",
      "from-a",
      { "content-type": "text/plain" }
    );
    assert.equal(res.status, 200);

    res = await call(ns, "r2-demo", "GET", "/object?key=shared.txt");
    assert.equal((await responseJson(res)).text, "from-a");
    res = await call(ns, "r2-demo-b", "GET", "/object?key=shared.txt");
    assert.equal((await responseJson(res)).text, "from-a");
  });
});
