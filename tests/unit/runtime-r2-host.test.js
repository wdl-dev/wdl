import { test } from "node:test";
import assert from "node:assert/strict";
import {
  installR2FetchMock,
  installRecordingR2FetchMock,
  makeR2Bucket,
  R2Bucket,
  R2_HOST_TEST_STATE,
} from "../helpers/load-runtime-r2-binding.js";
import { withMockedProperty } from "../helpers/mock-global.js";
import { requestBodyString } from "../helpers/request-body.js";
import { delay } from "../helpers/timing.js";

test("R2 host RPC surface exposes only public bucket methods", () => {
  assert.deepEqual(Object.getOwnPropertyNames(R2Bucket.prototype).toSorted(), [
    "constructor",
    "delete",
    "get",
    "head",
    "list",
    "put",
  ]);
});

test("R2 host get preserves etag arrays as multiple HTTP validators", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: async (_url, init) => {
      if (init.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "content-length": "7",
            etag: '"abc"',
          },
        });
      }
      return new Response(null, { status: 304 });
    },
  });
  try {
    const result = await makeR2Bucket().get(
      "a.txt",
      {
        onlyIf: {
          etagMatches: ["a", "b"],
          etagDoesNotMatch: ["c"],
        },
      },
      { requestId: "rid-1" }
    );

    assert.deepEqual(Object.keys(result), ["meta"]);
    assert.equal(result.meta.size, 7);
    assert.equal(result.meta.etag, "abc");
    assert.equal(calls.length, 2);
    assert.equal(calls[0].init.method, "GET");
    assert.equal(calls[0].init.headers.get("if-match"), '"a", "b"');
    assert.equal(calls[0].init.headers.get("if-none-match"), '"c"');
    assert.equal(calls[0].init.headers.get("x-request-id"), "rid-1");
    assert.equal(calls[1].init.method, "HEAD");
    assert.equal(calls[1].init.headers.get("x-request-id"), "rid-1");
  } finally {
    restore();
  }
});

test("R2 host range get reports object size and range slice separately", async () => {
  const restore = installR2FetchMock(async () => new Response("0123456789", {
    status: 206,
    headers: {
      "content-length": "10",
      "content-range": "bytes 5-14/100",
      etag: '"etag-1"',
    },
  }));
  try {
    const result = await makeR2Bucket().get("a.txt", { range: { offset: 5, length: 10 } });
    assert.equal(result.meta.size, 100);
    assert.deepEqual(result.meta.range, { offset: 5, length: 10 });
  } finally {
    restore();
  }
});

test("R2 host put returns metadata from PUT response without a follow-up HEAD", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response(null, {
      status: 200,
      headers: {
        etag: '"put-etag"',
        "x-amz-version-id": "v1",
      },
    }),
  });
  try {
    const meta = await makeR2Bucket().put(
      "a.txt",
      new TextEncoder().encode("hello"),
      {
        httpMetadata: { contentType: "text/plain" },
        customMetadata: { color: "blue" },
      },
      { requestId: "rid-put" }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0].init.method, "PUT");
    assert.equal(calls[0].init.headers.get("x-request-id"), "rid-put");
    assert.equal(meta.key, "a.txt");
    assert.equal(meta.size, 5);
    assert.equal(meta.etag, "put-etag");
    assert.equal(meta.version, "v1");
    assert.deepEqual(meta.httpMetadata, { contentType: "text/plain" });
    assert.deepEqual(meta.customMetadata, { color: "blue" });
  } finally {
    restore();
  }
});

test("R2 host reads every mapped HTTP metadata response header", async () => {
  const restore = installR2FetchMock(async () => new Response(null, {
    status: 200,
    headers: {
      "content-type": "text/plain",
      "content-language": "en",
      "content-disposition": "inline",
      "content-encoding": "gzip",
      "cache-control": "max-age=60",
      expires: "Thu, 01 Jan 1970 00:00:00 GMT",
    },
  }));
  try {
    const meta = await makeR2Bucket().head("metadata.txt");
    assert.deepEqual(meta.httpMetadata, {
      contentType: "text/plain",
      contentLanguage: "en",
      contentDisposition: "inline",
      contentEncoding: "gzip",
      cacheControl: "max-age=60",
      cacheExpiry: 0,
    });
  } finally {
    restore();
  }
});

test("R2 host metadata preserves magic custom metadata keys as data fields", async () => {
  const restore = installR2FetchMock(async () => new Response(null, {
    status: 200,
    headers: {
      etag: '"put-etag"',
      "x-amz-meta-__proto__": "metadata-value",
    },
  }));
  try {
    const meta = await makeR2Bucket().put("magic.txt", new TextEncoder().encode("hello"));
    assert.equal(Object.hasOwn(meta.customMetadata, "__proto__"), true);
    assert.equal(meta.customMetadata.__proto__, "metadata-value");
  } finally {
    restore();
  }
});

test("R2 host put rejects non byte-like values instead of writing an empty body", async () => {
  const restore = installR2FetchMock(async () => {
    throw new Error("unexpected S3 PUT");
  });
  try {
    await assert.rejects(
      () => makeR2Bucket().put("a.txt", "hello"),
      /R2 put: value must be ArrayBuffer or ArrayBufferView/
    );
  } finally {
    restore();
  }
});

test("R2 host put applies onlyIf headers and returns null on precondition failure", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response(null, { status: 412 }),
  });
  try {
    const meta = await makeR2Bucket().put(
      "a.txt",
      new TextEncoder().encode("hello"),
      {
        onlyIf: {
          etagMatches: ["a", "b"],
          etagDoesNotMatch: ["c"],
        },
      },
      { requestId: "rid-put-if" }
    );

    assert.equal(meta, null);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].init.method, "PUT");
    assert.equal(calls[0].init.headers.get("if-match"), '"a", "b"');
    assert.equal(calls[0].init.headers.get("if-none-match"), '"c"');
    assert.equal(calls[0].init.headers.get("x-request-id"), "rid-put-if");
  } finally {
    restore();
  }
});

test("R2 host delete batches array deletes through S3 DeleteObjects", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<DeleteResult/>", { status: 200 }),
  });
  try {
    await makeR2Bucket().delete(["a&b.txt", "nested/c.txt"], { requestId: "rid-2" });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, "http://s3mock:9090/wdl-r2?delete");
    assert.equal(calls[0].init.method, "POST");
    assert.equal(calls[0].init.headers.get("x-request-id"), "rid-2");
    const deleteBody = requestBodyString(calls[0].init, "R2 delete request body");
    assert.match(deleteBody, /<Key>r2\/demo\/uploads\/a&amp;b\.txt<\/Key>/);
    assert.match(deleteBody, /<Key>r2\/demo\/uploads\/nested\/c\.txt<\/Key>/);
  } finally {
    restore();
  }
});

test("R2 host retries transient S3 DeleteObjects responses", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  let attempt = 0;
  const restore = installR2FetchMock(async (
    /** @type {RequestInfo | URL} */ url,
    /** @type {RequestInit} */ init
  ) => {
    calls.push({ url: String(url), init });
    attempt += 1;
    return attempt === 1
      ? new Response("slow down", { status: 429 })
      : new Response("<DeleteResult/>", { status: 200 });
  });
  try {
    await makeR2Bucket().delete(["retry.txt"]);

    assert.equal(calls.length, 2);
    assert.equal(calls[0].init.method, "POST");
    assert.equal(calls[1].init.method, "POST");
    assert.equal(calls[0].url, calls[1].url);
    assert.equal(
      requestBodyString(calls[0].init, "first R2 delete request body"),
      requestBodyString(calls[1].init, "second R2 delete request body")
    );
  } finally {
    restore();
  }
});

test("R2 host DeleteObjects returns the last transient response after retry exhaustion", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installR2FetchMock(async (
    /** @type {RequestInfo | URL} */ url,
    /** @type {RequestInit} */ init
  ) => {
    calls.push({ url: String(url), init });
    return new Response("still slow", { status: 429 });
  });
  try {
    await withMockedProperty(Math, "random", () => 0, async () => {
      await assert.rejects(
        () => makeR2Bucket().delete(["retry-exhausted.txt"]),
        /R2 DELETE failed with 429/
      );
    });

    assert.equal(calls.length, 11);
    assert.ok(calls.every((call) => call.init.method === "POST"));
  } finally {
    restore();
  }
});

test("R2 host delete([]) is an explicit no-op", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<DeleteResult/>", { status: 200 }),
  });
  try {
    await makeR2Bucket().delete([]);
    assert.equal(calls.length, 0);
  } finally {
    restore();
  }
});

test("R2 host delete batch still validates object keys before writing XML", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<DeleteResult/>", { status: 200 }),
  });
  try {
    await assert.rejects(
      () => makeR2Bucket().delete(["../escape.txt"]),
      /must not contain \. or \.\. path segments/
    );
    assert.equal(calls.length, 0);
  } finally {
    restore();
  }
});

test("R2 host delete partial failure redacts backend physical keys and messages", async () => {
  const restore = installR2FetchMock(async () => new Response(
    [
      "<DeleteResult>",
      "<Error>",
      "<Key>r2/demo/uploads/a&amp;b.txt</Key>",
      "<Code>AccessDenied</Code>",
      "<Message>bucket policy denied for r2/demo/uploads/a&amp;b.txt</Message>",
      "</Error>",
      "</DeleteResult>",
    ].join(""),
    { status: 200 }
  ));
  try {
    await assert.rejects(
      () => makeR2Bucket().delete(["a&b.txt"]),
      /partial failure \(1 errors, showing 1\): a&b\.txt/
    );
    await assert.rejects(
      () => makeR2Bucket().delete(["a&b.txt"]),
      (err) => {
        const message = String(/** @type {Error} */ (err).message);
        assert.equal(message.includes("r2/demo/uploads"), false);
        assert.equal(message.includes("bucket policy denied"), false);
        assert.equal(message.includes("AccessDenied"), false);
        return true;
      }
    );
  } finally {
    restore();
  }
});

test("R2 host single-object errors do not expose backend response bodies", async () => {
  const restore = installR2FetchMock(async () => new Response(
    "backend mentions r2/demo/uploads/secret.txt",
    { status: 500 }
  ));
  try {
    for (const op of [
      () => makeR2Bucket().put("secret.txt", new Uint8Array([1])),
      () => makeR2Bucket().delete("secret.txt"),
      () => makeR2Bucket().list({ prefix: "secret" }),
    ]) {
      await assert.rejects(op, (err) => {
        const message = String(/** @type {Error} */ (err).message);
        assert.match(message, /R2 (PUT|DELETE|LIST) failed with 500/);
        assert.equal(message.includes("backend mentions"), false);
        assert.equal(message.includes("r2/demo/uploads"), false);
        return true;
      });
    }
  } finally {
    restore();
  }
});

test("R2 host list include hydrates metadata with per-object HEAD", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: async (url) => {
      if (String(url).includes("list-type=2")) {
        return new Response(
          [
            "<ListBucketResult>",
            "<IsTruncated>false</IsTruncated>",
            "<Contents>",
            "<Key>r2/demo/uploads/a&amp;b.txt</Key>",
            "<LastModified>2026-04-26T00:00:00.000Z</LastModified>",
            "<ETag>&quot;etag-1&quot;</ETag>",
            "<Size>7</Size>",
            "</Contents>",
            "</ListBucketResult>",
          ].join(""),
          { status: 200 }
        );
      }
      return new Response(null, {
        status: 200,
        headers: {
          "content-length": "7",
          "content-type": "text/plain",
          etag: '"etag-1"',
          "x-amz-meta-color": "blue",
        },
      });
    },
  });
  try {
    const listed = await makeR2Bucket().list({
      include: ["httpMetadata", "customMetadata"],
    });

    assert.equal(calls.length, 2);
    assert.equal(calls[1].init.method, "HEAD");
    assert.equal(calls[1].url, "http://s3mock:9090/wdl-r2/r2/demo/uploads/a%26b.txt");
    assert.equal(listed.objects[0].key, "a&b.txt");
    assert.deepEqual(listed.objects[0].httpMetadata, { contentType: "text/plain" });
    assert.deepEqual(listed.objects[0].customMetadata, { color: "blue" });
  } finally {
    restore();
  }
});

test("R2 host list parses escaped keys, prefixes, truncation cursor, and timestamps", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response(
      [
        "<ListBucketResult>",
        "<IsTruncated>true</IsTruncated>",
        "<NextContinuationToken>cursor&amp;1</NextContinuationToken>",
        "<CommonPrefixes><Prefix>r2/demo/uploads/folder&amp;/</Prefix></CommonPrefixes>",
        "<Contents>",
        "<Key>r2/demo/uploads/a&amp;b&apos;c.txt</Key>",
        "<LastModified>2026-04-26T00:00:00.000Z</LastModified>",
        "<ETag>&quot;etag&amp;1&quot;</ETag>",
        "<Size>7</Size>",
        "</Contents>",
        "</ListBucketResult>",
      ].join(""),
      { status: 200 }
    ),
  });
  try {
    const listed = await makeR2Bucket().list({ delimiter: "/" });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].init.method, "GET");
    assert.equal(listed.truncated, true);
    assert.equal(listed.cursor, "cursor&1");
    assert.deepEqual(listed.delimitedPrefixes, ["folder&/"]);
    assert.equal(listed.objects.length, 1);
    assert.equal(listed.objects[0].key, "a&b'c.txt");
    assert.equal(listed.objects[0].size, 7);
    assert.equal(listed.objects[0].etag, "etag&1");
    assert.equal(listed.objects[0].httpEtag, "\"etag&1\"");
    assert.equal(listed.objects[0].uploaded, Date.parse("2026-04-26T00:00:00.000Z"));
  } finally {
    restore();
  }
});

test("R2 host list rejects CommonPrefixes outside the binding prefix", async () => {
  const restore = installR2FetchMock(async () => new Response(
    [
      "<ListBucketResult>",
      "<CommonPrefixes><Prefix>r2/other/uploads/private/</Prefix></CommonPrefixes>",
      "</ListBucketResult>",
    ].join(""),
    { status: 200 }
  ));
  try {
    await assert.rejects(
      () => makeR2Bucket().list({ delimiter: "/" }),
      /R2 backend returned an object outside the binding prefix/
    );
  } finally {
    restore();
  }
});

test("R2 host list normalizes prefix and startAfter before S3 requests", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
  });
  try {
    await makeR2Bucket().list({ prefix: "folder name", startAfter: "folder name/a.txt" });

    const url = new URL(calls[0].url);
    assert.match(calls[0].url, /prefix=r2%2Fdemo%2Fuploads%2Ffolder%20name(?:&|$)/);
    assert.match(calls[0].url, /start-after=r2%2Fdemo%2Fuploads%2Ffolder%20name%2Fa\.txt(?:&|$)/);
    assert.equal(url.searchParams.get("prefix"), "r2/demo/uploads/folder name");
    assert.equal(url.searchParams.get("start-after"), "r2/demo/uploads/folder name/a.txt");
    await assert.rejects(
      () => makeR2Bucket().list({ prefix: "../secret" }),
      /R2 key must not contain . or .. path segments/
    );
    await assert.rejects(
      () => makeR2Bucket().list({ startAfter: "../secret" }),
      /R2 key must not contain . or .. path segments/
    );
  } finally {
    restore();
  }
});

test("R2 host list accepts namespaced XML tags by local name", async () => {
  const restore = installR2FetchMock(async () => new Response(
    [
      "<aws:ListBucketResult>",
      "<aws:IsTruncated>true</aws:IsTruncated>",
      "<aws:NextContinuationToken>cursor-2</aws:NextContinuationToken>",
      "<aws:CommonPrefixes><aws:Prefix>r2/demo/uploads/ns/</aws:Prefix></aws:CommonPrefixes>",
      "<aws:Contents>",
      "<aws:Key>r2/demo/uploads/ns/a.txt</aws:Key>",
      "<aws:LastModified>2026-04-26T00:00:00.000Z</aws:LastModified>",
      "<aws:ETag>&quot;etag-ns&quot;</aws:ETag>",
      "<aws:Size>11</aws:Size>",
      "</aws:Contents>",
      "</aws:ListBucketResult>",
    ].join(""),
    { status: 200 }
  ));
  try {
    const listed = await makeR2Bucket().list({ delimiter: "/" });

    assert.equal(listed.truncated, true);
    assert.equal(listed.cursor, "cursor-2");
    assert.deepEqual(listed.delimitedPrefixes, ["ns/"]);
    assert.equal(listed.objects.length, 1);
    assert.equal(listed.objects[0].key, "ns/a.txt");
    assert.equal(listed.objects[0].size, 11);
    assert.equal(listed.objects[0].etag, "etag-ns");
  } finally {
    restore();
  }
});

test("R2 host list validates limit before S3 call", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<ListBucketResult/>", { status: 200 }),
  });
  try {
    for (const limit of [0, 1001, 1.5, "abc", true]) {
      await assert.rejects(
        () => makeR2Bucket().list({ limit }),
        /R2 list: limit must be an integer in \[1, 1000\]/,
        `expected limit ${JSON.stringify(limit)} to fail before S3 call`
      );
    }
    assert.equal(calls.length, 0);
  } finally {
    restore();
  }
});

test("R2 host list forwards valid limit as S3 max-keys", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response("<ListBucketResult/>", { status: 200 }),
  });
  try {
    await makeR2Bucket().list({ limit: "1000" });

    assert.equal(calls.length, 1);
    assert.equal(new URL(calls[0].url).searchParams.get("max-keys"), "1000");
  } finally {
    restore();
  }
});

test("R2 host list ignores malformed incomplete object entries", async () => {
  const restore = installR2FetchMock(async () => new Response(
    [
      "<ListBucketResult>",
      "<IsTruncated>false</IsTruncated>",
      "<Contents>",
      "<Key>r2/demo/uploads/incomplete.txt</Key>",
      "<Size>7</Size>",
      "</ListBucketResult>",
    ].join(""),
    { status: 200 }
  ));
  try {
    const listed = await makeR2Bucket().list();

    assert.equal(listed.truncated, false);
    assert.deepEqual(listed.objects, []);
    assert.deepEqual(listed.delimitedPrefixes, []);
  } finally {
    restore();
  }
});

test("R2 host S3 client cache evicts least-recently-used configs", async () => {
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response(null, { status: 404 }),
  });
  try {
    await withMockedProperty(R2_HOST_TEST_STATE, "awsClientConfigs", [], async () => {
      for (let i = 0; i < 129; i++) {
        await makeR2Bucket({ R2_S3_ACCESS_KEY_ID: `key-${i}` }).head("a.txt");
      }
      assert.equal(R2_HOST_TEST_STATE.awsClientConfigs.length, 129);

      await makeR2Bucket({ R2_S3_ACCESS_KEY_ID: "key-0" }).head("a.txt");
      assert.equal(R2_HOST_TEST_STATE.awsClientConfigs.length, 130);
    });
  } finally {
    restore();
  }
});

test("R2 host S3 clients restore transient retry budget", async () => {
  const restore = installR2FetchMock(async () => new Response(null, { status: 404 }));
  try {
    await makeR2Bucket({ R2_S3_ACCESS_KEY_ID: "retry-budget-key" }).head("a.txt");
    assert.equal(R2_HOST_TEST_STATE.awsClientConfigs.at(-1).retries, 10);
  } finally {
    restore();
  }
});

test("R2 host list parses 1000 returned keys without metadata HEAD hydration", async () => {
  const keys = Array.from({ length: 1000 }, (_, i) => `bulk/${String(i).padStart(4, "0")}.txt`);
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: new Response(
      [
        "<ListBucketResult>",
        "<IsTruncated>false</IsTruncated>",
        ...keys.map((key, index) => [
          "<Contents>",
          `<Key>r2/demo/uploads/${key}</Key>`,
          "<LastModified>2026-04-26T00:00:00.000Z</LastModified>",
          `<ETag>&quot;etag-${index}&quot;</ETag>`,
          `<Size>${index}</Size>`,
          "</Contents>",
        ].join("")),
        "</ListBucketResult>",
      ].join(""),
      { status: 200 }
    ),
  });
  try {
    const listed = await makeR2Bucket().list({ limit: 1000 });

    assert.equal(calls.length, 1);
    assert.equal(listed.truncated, false);
    assert.equal(listed.objects.length, 1000);
    assert.equal(listed.objects[0].key, "bulk/0000.txt");
    assert.equal(listed.objects[0].size, 0);
    assert.equal(listed.objects[999].key, "bulk/0999.txt");
    assert.equal(listed.objects[999].etag, "etag-999");
  } finally {
    restore();
  }
});

test("R2 host list include caps concurrent HEAD hydration", async () => {
  const keys = Array.from({ length: 20 }, (_, i) => `k${i}.txt`);
  let active = 0;
  let maxActive = 0;
  /** @type {Array<{ url: string, init: any }>} */
  const calls = [];
  const restore = installRecordingR2FetchMock(calls, {
    response: async (url) => {
      if (String(url).includes("list-type=2")) {
        return new Response(
          [
            "<ListBucketResult>",
            "<IsTruncated>false</IsTruncated>",
            ...keys.map((key) => [
              "<Contents>",
              `<Key>r2/demo/uploads/${key}</Key>`,
              "<LastModified>2026-04-26T00:00:00.000Z</LastModified>",
              "<ETag>&quot;etag-1&quot;</ETag>",
              "<Size>1</Size>",
              "</Contents>",
            ].join("")),
            "</ListBucketResult>",
          ].join(""),
          { status: 200 }
        );
      }
      active++;
      maxActive = Math.max(maxActive, active);
      await delay(5);
      active--;
      return new Response(null, {
        status: 200,
        headers: {
          "content-length": "1",
          "content-type": "text/plain",
          etag: '"etag-1"',
        },
      });
    },
  });
  try {
    const listed = await makeR2Bucket().list({ include: ["httpMetadata"] });
    assert.equal(listed.objects.length, 20);
    assert.equal(calls.length, 21);
    assert.equal(maxActive, 16);
  } finally {
    restore();
  }
});
