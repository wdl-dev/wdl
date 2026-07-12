import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseConsumerKey,
  parseDelayedKey,
  parseStreamKey,
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
});

test("queue key parsers match the cross-language fixture", () => {
  const parsers = {
    stream: parseStreamKey,
    delayed: parseDelayedKey,
    consumer: parseConsumerKey,
  };
  for (const [kind, parser] of Object.entries(parsers)) {
    for (const { key, parsed } of QUEUE_KEY_PARSE_CASES[kind]) {
      assert.deepEqual(parser(key), parsed, `${kind}:${JSON.stringify(key)}`);
    }
  }
});
