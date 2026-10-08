import assert from "node:assert/strict";
import { test } from "node:test";

import {
  deletePrefixPage,
  nextBackoffMs,
  processTask,
} from "../../system-workers/s3-cleanup/src/index.js";
import {
  S3_CLEANUP_OUTCOME,
  S3_CLEANUP_TASK_FIELDS,
  S3_CLEANUP_TASK_STATUS,
} from "../../shared/s3-cleanup-lifecycle.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { S3_ERROR_BODY_MAX_BYTES } from "../../shared/s3-request.js";

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
              if (/SET\s+attempts\s*=/.test(sql)) {
                state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS] = values[0];
                state.row[S3_CLEANUP_TASK_FIELDS.UPDATED_AT] = values[1];
                state.row[S3_CLEANUP_TASK_FIELDS.NEXT_ATTEMPT_AT] = values[2];
                state.row[S3_CLEANUP_TASK_FIELDS.LAST_ERROR] = values[3];
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

test("cleanup keeps a timed-out delete pending without advancing the checkpoint", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 0 });
  const db = cleanupDb(taskRow());
  const started = Promise.withResolvers();
  /** @type {AbortSignal | null | undefined} */
  let signal;
  const s3 = /** @type {any} */ ({
    endpoint: "http://s3.test", bucket: "assets",
    aws: { async fetch(/** @type {string} */ _url, /** @type {RequestInit} */ init) {
      if (init.method === "GET") {
        return new Response(`<ListBucketResult><Contents><Key>${TASK_PREFIX}key</Key></Contents></ListBucketResult>`);
      }
      signal = init.signal;
      started.resolve(undefined);
      return new Response(new ReadableStream({
        pull() { return new Promise(() => {}); },
      }));
    } },
  });
  const pending = processTask(db, s3, "s3cleanup:unit");
  await started.promise;
  t.mock.timers.tick(30_000);
  assert.equal(await pending, S3_CLEANUP_OUTCOME.RETRY);
  assert.ok(signal);
  assert.equal(signal.aborted, true);
  assert.equal(db.state.deleted, false);
  assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON], null);
  assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS], 1);
});

test("cleanup list keeps the backend status when its error body is unreadable", async () => {
  const { s3 } = s3Mock([new Response(new ReadableStream({
    start(controller) { controller.error(new TypeError("body disconnected")); },
  }), { status: 503 })]);
  await assert.rejects(deletePrefixPage(s3, TASK_PREFIX), /s3 list .*503/);
});

test("cleanup omits oversized list/delete errors without advancing its checkpoint", async () => {
  for (const phase of ["list", "delete"]) {
    const db = cleanupDb(taskRow());
    let cancelled = false;
    let reads = 0;
    const errorResponse = new Response(new ReadableStream({
      pull(controller) { reads += 1; controller.close(); },
      cancel() { cancelled = true; },
    }, { highWaterMark: 0 }), {
      status: 400,
      headers: { "content-length": String(S3_ERROR_BODY_MAX_BYTES + 1) },
    });
    const listResponse = new Response(`<ListBucketResult><Contents><Key>${TASK_PREFIX}key</Key></Contents></ListBucketResult>`);
    const { s3 } = s3Mock(phase === "list" ? [errorResponse] : [listResponse, errorResponse]);
    await withMockedProperty(console, "log", () => {}, async () => {
      assert.equal(await processTask(db, s3, "s3cleanup:unit"), S3_CLEANUP_OUTCOME.RETRY);
    });
    assert.equal(cancelled, true);
    assert.equal(reads, 0);
    assert.equal(db.state.deleted, false);
    assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON], null);
    assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS], 1);
    assert.match(String(db.state.row[S3_CLEANUP_TASK_FIELDS.LAST_ERROR]), new RegExp(`^s3 ${phase} .*400: $`));
  }
});

test("cleanup includes only a short detail from an exact-limit list/delete error", async () => {
  for (const phase of ["list", "delete"]) {
    const errorResponse = new Response("x".repeat(S3_ERROR_BODY_MAX_BYTES), { status: 400 });
    const listResponse = new Response(`<ListBucketResult><Contents><Key>${TASK_PREFIX}key</Key></Contents></ListBucketResult>`);
    const { s3 } = s3Mock(phase === "list" ? [errorResponse] : [listResponse, errorResponse]);
    await assert.rejects(deletePrefixPage(s3, TASK_PREFIX), (error) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, new RegExp(`^s3 ${phase} .*400: `));
      assert.equal(error.message.split("400: ")[1], "x".repeat(200));
      return true;
    });
  }
});

test("deletePrefixPage returns continuation tokens without deleting empty pages", async () => {
  const { s3, calls } = s3Mock([
    new Response([
      "<ListBucketResult>",
      "<IsTruncated>true</IsTruncated>",
      "<NextContinuationToken>cursor&amp;1</NextContinuationToken>",
      "</ListBucketResult>",
    ].join("")),
  ]);

  const result = await deletePrefixPage(s3, "assets/demo/");

  assert.deepEqual(result, { deletedCount: 0, nextContinuationToken: "cursor&1" });
  assert.equal(calls.length, 1);
});

test("deletePrefixPage encodes ListObjectsV2 query spaces as percent bytes", async () => {
  const { s3, calls } = s3Mock([
    new Response("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
  ]);

  const result = await deletePrefixPage(s3, "assets/demo/has space/");

  assert.deepEqual(result, { deletedCount: 0, nextContinuationToken: null });
  assert.match(calls[0].url, /prefix=assets%2Fdemo%2Fhas%20space%2F/);
  assert.doesNotMatch(calls[0].url, /\+/);
});

test("processTask checkpoints one S3 list page and resumes from the continuation token", async () => {
  const db = cleanupDb(taskRow({ [S3_CLEANUP_TASK_FIELDS.ATTEMPTS]: 3 }));
  /** @type {Record<string, unknown>[]} */
  const logs = [];
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

  await withMockedProperty(console, "log", (line) => {
    logs.push(JSON.parse(String(line)));
  }, async () => {
    assert.equal(
      await processTask(/** @type {any} */ (db), s3, "s3cleanup:unit"),
      S3_CLEANUP_OUTCOME.RETRY
    );
    assert.equal(db.state.deleted, false);
    assert.deepEqual(JSON.parse(String(db.state.row[S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON])), {
      prefixIndex: 0,
      continuationToken: "cursor&1",
      pageCount: 1,
      deletedCount: 1,
    });
    assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS], 0);
    assert.equal(
      await processTask(/** @type {any} */ (db), s3, "s3cleanup:unit"),
      S3_CLEANUP_OUTCOME.DONE
    );
    assert.equal(db.state.deleted, true);
  });
  assert.equal(new URL(calls[2].url).searchParams.get("continuation-token"), "cursor&1");
  assert.equal(
    logs.find((entry) => entry.event === "s3_cleanup_task_done")?.deleted_count,
    1
  );
});

test("processTask rejects checkpoint prefix indexes past the prefix list", async () => {
  const db = cleanupDb(taskRow({
    [S3_CLEANUP_TASK_FIELDS.CHECKPOINT_JSON]: JSON.stringify({
      prefixIndex: 2,
      continuationToken: null,
      pageCount: 0,
      deletedCount: 0,
    }),
  }));
  const { s3, calls } = s3Mock([]);

  await withMockedProperty(console, "log", () => {}, async () => {
    assert.equal(
      await processTask(/** @type {any} */ (db), s3, "s3cleanup:unit"),
      S3_CLEANUP_OUTCOME.RETRY
    );
  });
  assert.equal(db.state.deleted, false);
  assert.equal(calls.length, 0);
  assert.equal(db.state.row[S3_CLEANUP_TASK_FIELDS.ATTEMPTS], 1);
  assert.match(
    String(db.state.row[S3_CLEANUP_TASK_FIELDS.LAST_ERROR]),
    /checkpoint prefixIndex 2 exceeds prefix count 1/
  );
});

test("deletePrefixPage retries transient DeleteObjects responses", async () => {
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

  const result = await deletePrefixPage(s3, "assets/demo/");

  assert.deepEqual(result, { deletedCount: 1, nextContinuationToken: null });
  assert.equal(calls.length, 3);
  assert.equal(calls[1].init?.method, "POST");
  assert.equal(calls[2].init?.method, "POST");
  assert.equal(calls[1].url, calls[2].url);
});

test("deletePrefixPage returns the last transient DeleteObjects response after retry exhaustion", async () => {
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
      () => deletePrefixPage(s3, "assets/demo/"),
      /s3 delete assets\/demo\/ → 429: still slow/
    );
  });

  assert.equal(calls.length, 12);
  assert.ok(calls.slice(1).every((call) => call.init?.method === "POST"));
});

test("deletePrefixPage rejects truncated list responses without continuation tokens", async () => {
  const { s3 } = s3Mock([
    new Response("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"),
  ]);

  await assert.rejects(
    () => deletePrefixPage(s3, "assets/demo/"),
    /truncated without NextContinuationToken/
  );
});
