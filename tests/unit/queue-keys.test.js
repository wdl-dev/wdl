import assert from "node:assert/strict";
import { test } from "node:test";
import {
  QUEUE_DELAYED_WAKE_STREAM,
  queueConsumerKey,
  queueDelayedKey,
  queueStreamKey,
} from "../../shared/queue-keys.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";

const QUEUE_KEY_PARSE_CASES = readRepositoryJson("tests/fixtures/queue-key-parse.json");

test("queue key builders compose canonical Redis shapes", () => {
  assert.equal(queueStreamKey("demo", "jobs"), "queue:demo:jobs:s");
  assert.equal(queueDelayedKey("demo", "jobs"), "queue-delayed:demo:jobs");
  assert.equal(queueConsumerKey("demo", "jobs"), "queue-consumer:demo:jobs");
  assert.equal(QUEUE_DELAYED_WAKE_STREAM, "queue-delayed-wake");
});

test("queue key builders match the keys accepted by the Rust parser fixture", () => {
  const builders = {
    stream: queueStreamKey,
    delayed: queueDelayedKey,
    consumer: queueConsumerKey,
  };
  for (const [kind, build] of Object.entries(builders)) {
    const validCases = QUEUE_KEY_PARSE_CASES[kind].filter(
      (/** @type {{ parsed: unknown }} */ entry) => entry.parsed !== null
    );
    assert.ok(validCases.length > 0, `${kind} needs accepted key cases`);
    for (const { key, parsed } of validCases) {
      assert.equal(build(parsed.ns, parsed.queue), key, `${kind}:${JSON.stringify(key)}`);
    }
  }
});
