import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatWorkerId,
  parseDispatchWorkerId,
  parseRuntimeLoadWorkerId,
  parseWorkerId,
} from "../../shared/worker-id.js";

test("worker id helpers round-trip exact three-part ids", () => {
  const workerId = formatWorkerId({ namespace: "demo", worker: "hello", version: "v1" });
  assert.equal(workerId, "demo:hello:v1");
  assert.deepEqual(parseWorkerId(workerId), ["demo", "hello", "v1"]);
  assert.deepEqual(parseDispatchWorkerId(workerId), {
    namespace: "demo",
    worker: "hello",
    version: "v1",
  });
});

test("dispatch worker id parser validates route namespace, worker, and version grammar", () => {
  assert.deepEqual(parseDispatchWorkerId("demo:Worker_1:v42"), {
    namespace: "demo",
    worker: "Worker_1",
    version: "v42",
  });
  assert.deepEqual(parseDispatchWorkerId("__system__:s3-cleanup:v1"), {
    namespace: "__system__",
    worker: "s3-cleanup",
    version: "v1",
  });
  assert.equal(parseDispatchWorkerId("__platform__:platform-api:v1"), null);

  for (const value of [
    "-demo:worker:v1",
    "demo-:worker:v1",
    `${"a".repeat(64)}:worker:v1`,
    "Demo:worker:v1",
    "__community__:worker:v1",
    "demo:/bad:v1",
    "demo:worker:v0",
    "demo:worker:v01",
  ]) {
    assert.equal(parseDispatchWorkerId(value), null, value);
  }
});

test("runtime-load worker id parser allows platform-tier cold-load targets", () => {
  assert.deepEqual(parseRuntimeLoadWorkerId("__platform__:platform-api:v1"), {
    namespace: "__platform__",
    worker: "platform-api",
    version: "v1",
  });
  assert.deepEqual(parseRuntimeLoadWorkerId("__system__:s3-cleanup:v1"), {
    namespace: "__system__",
    worker: "s3-cleanup",
    version: "v1",
  });

  for (const value of [
    "-demo:worker:v1",
    "demo-:worker:v1",
    `${"a".repeat(64)}:worker:v1`,
    "Demo:worker:v1",
    "__community__:worker:v1",
    "demo:/bad:v1",
    "demo:worker:v0",
    "demo:worker:v01",
  ]) {
    assert.equal(parseRuntimeLoadWorkerId(value), null, value);
  }
});

test("worker id helpers reject null, empty, and malformed ids", () => {
  for (const value of [
    null,
    undefined,
    "",
    "demo:hello",
    "demo::v1",
    ":hello:v1",
    "demo:hello:",
    "a:b:c:d",
  ]) {
    assert.equal(parseWorkerId(value), null);
    assert.equal(parseDispatchWorkerId(value), null);
    assert.equal(parseRuntimeLoadWorkerId(value), null);
  }
});
