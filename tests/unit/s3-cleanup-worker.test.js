import assert from "node:assert/strict";
import { test } from "node:test";

import {
  deletePrefix,
  nextBackoffMs,
  processTask,
} from "../../system-workers/s3-cleanup/src/index.js";
import {
  S3_CLEANUP_OUTCOME,
  S3_CLEANUP_TASK_FIELDS,
  S3_CLEANUP_TASK_STATUS,
} from "../../shared/s3-cleanup-lifecycle.js";
import { withMockedProperty } from "../helpers/mock-global.js";

const TASK_PREFIX = "assets/demo/worker/019dd83e7d1c345302f9b0f3b4f6/";

/** @param {Response[]} responses */
function s3Mock(responses) {
  /** @type {Array<{ url: string, init?: RequestInit }>} */
  const calls = [];
  return {
    calls,
    s3: /** @type {any} */ ({
      endpoint: "http://s3.test",
      bucket: "wdl",
      aws: {
        /** @param {RequestInfo | URL} url @param {RequestInit} [init] */
        async fetch(url, init) {
          calls.push({ url: String(url), init });
          const response = responses.shift();
          if (!response) throw new Error("unexpected S3 request");
          return response;
        },
      },
    }),
  };
}

/** @param {Partial<Record<string, unknown>>} [overrides] */
function taskRow(overrides = {}) {
  return {
    [S3_CLEANUP_TASK_FIELDS.ID]: "s3cleanup:unit",
    [S3_CLEANUP_TASK_FIELDS.SOURCE_JSON]: JSON.stringify({
      kind: "delete-worker",
      ns: "demo",
      worker: "worker",
    }),
    [S3_CLEANUP_TASK_FIELDS.PREFIXES_JSON]: JSON.stringify([TASK_PREFIX]),
    [S3_CLEANUP_TASK_FIELDS.STATE]: S3_CLEANUP_TASK_STATUS.PENDING,
    [S3_CLEANUP_TASK_FIELDS.ATTEMPTS]: 0,
    [S3_CLEANUP_TASK_FIELDS.CREATED_AT]: 0,
    [S3_CLEANUP_TASK_FIELDS.UPDATED_AT]: 0,
    [S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT]: 0,
    [S3_CLEANUP_TASK_FIELDS.LAST_ERROR]: null,
    [S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON]: null,
    ...overrides,
  };
}

/** @param {Record<string, unknown>} row */
function cleanupDb(row) {
  const state = { row, deleted: false };
  return {
    state,
    prepare(/** @type {string} */ sql) {
      return {
        /** @param {unknown[]} values */
        bind(...values) {
          return {
            async first() {
              if (/SELECT \*/.test(sql)) return state.deleted ? null : state.row;
              throw new Error(`unexpected first SQL: ${sql}`);
            },
            async all() {
              if (/SELECT \*/.test(sql)) return { results: state.deleted ? [] : [state.row] };
              throw new Error(`unexpected all SQL: ${sql}`);
            },
            async run() {
              if (/DELETE FROM/.test(sql)) {
                state.deleted = true;
                return { meta: { changes: 1 } };
              }
              if (/checkpoint_json/.test(sql)) {
                state.row[S3_CLEANUP_TASK_FIELDS.UPDATED_AT] = values[0];
                state.row[S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT] = values[1];
                state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS] = 0;
                state.row[S3_CLEANUP_TASK_FIELDS.LAST_ERROR] = null;
                state.row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON] = values[2];
                return { meta: { changes: 1 } };
              }
              if (/SET\s+updated_at\s*=/.test(sql)) {
                state.row[S3_CLEANUP_TASK_FIELDS.UPDATED_AT] = values[0];
                state.row[S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT] = values[1];
                return { meta: { changes: 1 } };
              }
              throw new Error(`unexpected run SQL: ${sql}`);
            },
          };
        },
      };
    },
  };
}

test("s3-cleanup retry horizon reaches the 30 minute cap before final failure", () => {
  assert.equal(nextBackoffMs(1), 60_000);
  assert.equal(nextBackoffMs(6), 30 * 60_000);
  assert.equal(nextBackoffMs(10), 30 * 60_000);
});

test("deletePrefix follows truncated empty pages before marking cleanup done", async () => {
  const { s3, calls } = s3Mock([
    new Response([
      "<ListBucketResult>",
      "<IsTruncated>true</IsTruncated>",
      "<NextContinuationToken>cursor&amp;1</NextContinuationToken>",
      "</ListBucketResult>",
    ].join("")),
    new Response([
      "<ListBucketResult>",
      "<s3:IsTruncated>false</s3:IsTruncated>",
      "<s3:Contents><s3:Key>assets/demo/a&amp;b.txt</s3:Key></s3:Contents>",
      "</ListBucketResult>",
    ].join("")),
    new Response("<DeleteResult><Deleted><Key>assets/demo/a&amp;b.txt</Key></Deleted></DeleteResult>"),
  ]);

  const result = await deletePrefix(s3, "assets/demo/");

  assert.deepEqual(result, { deletedCount: 1 });
  assert.equal(calls.length, 3);
  assert.equal(new URL(calls[1].url).searchParams.get("continuation-token"), "cursor&1");
  assert.equal(calls[2].init?.method, "POST");
});

test("deletePrefix encodes ListObjectsV2 query spaces as percent bytes", async () => {
  const { s3, calls } = s3Mock([
    new Response("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
  ]);

  const result = await deletePrefix(s3, "assets/demo/has space/");

  assert.deepEqual(result, { deletedCount: 0 });
  assert.match(calls[0].url, /prefix=assets%2Fdemo%2Fhas%20space%2F/);
  assert.doesNotMatch(calls[0].url, /\+/);
});

test("processTask checkpoints one S3 list page and resumes from the continuation token", async () => {
  const db = cleanupDb(taskRow({ [S3_CLEANUP_TASK_FIELDS.ATTEMPTS]: 3 }));
  const { s3, calls } = s3Mock([
    new Response([
      "<ListBucketResult>",
      "<IsTruncated>true</IsTruncated>",
      "<Contents><Key>assets/demo/worker/019dd83e7d1c345302f9b0f3b4f6/a.txt</Key></Contents>",
      "<NextContinuationToken>cursor&amp;1</NextContinuationToken>",
      "</ListBucketResult>",
    ].join("")),
    new Response([
      "<DeleteResult>",
      "<Deleted><Key>assets/demo/worker/019dd83e7d1c345302f9b0f3b4f6/a.txt</Key></Deleted>",
      "</DeleteResult>",
    ].join("")),
    new Response("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
  ]);

  await withMockedProperty(console, "log", () => {}, async () => {
    assert.equal(
      await processTask(/** @type {any} */ (db), s3, "s3cleanup:unit"),
      S3_CLEANUP_OUTCOME.RETRY
    );
    assert.equal(db.state.deleted, false);
    assert.deepEqual(JSON.parse(String(db.state.row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON])), {
      prefixIndex: 0,
      continuationToken: "cursor&1",
      pageCount: 1,
    });
    assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS], 0);
    assert.equal(
      await processTask(/** @type {any} */ (db), s3, "s3cleanup:unit"),
      S3_CLEANUP_OUTCOME.DONE
    );
    assert.equal(db.state.deleted, true);
  });
  assert.equal(new URL(calls[2].url).searchParams.get("continuation-token"), "cursor&1");
});

test("deletePrefix retries transient DeleteObjects responses", async () => {
  const { s3, calls } = s3Mock([
    new Response([
      "<ListBucketResult>",
      "<IsTruncated>false</IsTruncated>",
      "<Contents><Key>assets/demo/retry.txt</Key></Contents>",
      "</ListBucketResult>",
    ].join("")),
    new Response("slow down", { status: 500 }),
    new Response("<DeleteResult><Deleted><Key>assets/demo/retry.txt</Key></Deleted></DeleteResult>"),
  ]);

  const result = await deletePrefix(s3, "assets/demo/");

  assert.deepEqual(result, { deletedCount: 1 });
  assert.equal(calls.length, 3);
  assert.equal(calls[1].init?.method, "POST");
  assert.equal(calls[2].init?.method, "POST");
  assert.equal(calls[1].url, calls[2].url);
});

test("deletePrefix returns the last transient DeleteObjects response after retry exhaustion", async () => {
  const { s3, calls } = s3Mock([
    new Response([
      "<ListBucketResult>",
      "<IsTruncated>false</IsTruncated>",
      "<Contents><Key>assets/demo/retry.txt</Key></Contents>",
      "</ListBucketResult>",
    ].join("")),
    ...Array.from({ length: 11 }, () => new Response("still slow", { status: 429 })),
  ]);

  await withMockedProperty(Math, "random", () => 0, async () => {
    await assert.rejects(
      () => deletePrefix(s3, "assets/demo/"),
      /s3 delete assets\/demo\/ → 429: still slow/
    );
  });

  assert.equal(calls.length, 12);
  assert.ok(calls.slice(1).every((call) => call.init?.method === "POST"));
});

test("deletePrefix retries truncated list responses without continuation tokens", async () => {
  const { s3 } = s3Mock([
    new Response("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"),
  ]);

  await assert.rejects(
    () => deletePrefix(s3, "assets/demo/"),
    /truncated without NextContinuationToken/
  );
});
