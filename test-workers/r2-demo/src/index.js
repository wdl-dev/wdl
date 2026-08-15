// r2-demo — exercises the Cloudflare R2 Worker API surface WDL supports.

function json(value, init) {
  return Response.json(value, init);
}

function textOrNull(obj) {
  return obj ? obj.text() : Promise.resolve(null);
}

function objectShape(obj, text) {
  if (!obj) return null;
  return {
    key: obj.key,
    size: obj.size,
    etag: obj.etag,
    httpEtag: obj.httpEtag,
    uploaded: obj.uploaded.toISOString(),
    range: obj.range,
    httpMetadata: obj.httpMetadata,
    customMetadata: obj.customMetadata,
    text,
  };
}

function streamWithMutationOnSecondPull(chunk, mutate) {
  let firstRead = true;
  return new ReadableStream({
    pull(controller) {
      if (firstRead) {
        firstRead = false;
        controller.enqueue(chunk);
        return;
      }
      mutate();
      controller.close();
    },
  }, { highWaterMark: 0 });
}

function streamWithQueuedMutationOnPull(chunk, mutate) {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(chunk);
    },
    pull(controller) {
      mutate();
      controller.close();
    },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") || "demo.txt";
    const path = url.pathname.replace(/^\/+/, "");

    try {
      if (request.method === "PUT" && path === "object") {
        const meta = await env.B.put(key, request.body || "", {
          httpMetadata: {
            contentType: request.headers.get("content-type") || "application/octet-stream",
            cacheControl: "max-age=60",
          },
          customMetadata: {
            source: url.searchParams.get("source") || "r2-demo",
          },
        });
        return json(objectShape(meta, null));
      }

      if (request.method === "GET" && path === "object") {
        const offset = url.searchParams.get("offset");
        const length = url.searchParams.get("length");
        const range = url.searchParams.get("range") === "1"
          ? { offset: Number(offset ?? 0),
              length: length == null || length === "" ? 1 : Number(length) }
          : undefined;
        const obj = await env.B.get(key, range ? { range } : undefined);
        return json(objectShape(obj, await textOrNull(obj)));
      }

      if (request.method === "GET" && path === "head") {
        return json(objectShape(await env.B.head(key), null));
      }

      if (request.method === "GET" && path === "list") {
        const listed = await env.B.list({
          prefix: url.searchParams.get("prefix") || undefined,
          include: ["httpMetadata", "customMetadata"],
        });
        return json({
          ...listed,
          objects: listed.objects.map((obj) => objectShape(obj, null)),
        });
      }

      if (request.method === "POST" && path === "copy-stream") {
        const src = url.searchParams.get("src") || key;
        const dst = url.searchParams.get("dst") || `${src}.copy`;
        const obj = await env.B.get(src);
        if (!obj) return new Response("missing source", { status: 404 });
        const meta = await env.B.put(dst, obj.body, {
          httpMetadata: obj.httpMetadata,
          customMetadata: { copiedFrom: src },
        });
        const copied = await env.B.get(dst);
        return json({
          put: objectShape(meta, null),
          copied: objectShape(copied, await copied.text()),
        });
      }

      if (request.method === "POST" && path === "buffer-integrity") {
        class MisleadingWords extends Uint16Array {
          get byteLength() { return 0; }
        }
        const source = new ArrayBuffer(8);
        new Uint8Array(source).set([0, 0, 104, 101, 108, 112, 0, 0]);
        const meta = await env.B.put(key, new MisleadingWords(source, 2, 2));

        const queuedStreamKey = `${key}.queued-stream`;
        const queuedStreamChunk = new Uint8Array([104, 101, 108, 112]);
        await env.B.put(queuedStreamKey, streamWithQueuedMutationOnPull(
          queuedStreamChunk,
          () => queuedStreamChunk.fill(0)
        ));

        const fixedStreamKey = `${key}.fixed-stream`;
        const fixedStreamChunk = new Uint8Array([104, 101, 108, 112]);
        await env.B.put(fixedStreamKey, streamWithMutationOnSecondPull(
          fixedStreamChunk,
          () => fixedStreamChunk.fill(0)
        ));

        const resizableStreamKey = `${key}.resizable-stream`;
        const resizableStream = new ArrayBuffer(4, { maxByteLength: 4 });
        new Uint8Array(resizableStream).set([104, 101, 108, 112]);
        await env.B.put(resizableStreamKey, streamWithMutationOnSecondPull(
          new Uint8Array(resizableStream),
          () => {
            resizableStream.resize(2);
            resizableStream.resize(4);
          }
        ));

        const blobKey = `${key}.blob`;
        await env.B.put(blobKey, new Blob(["help"]));

        const resizable = new ArrayBuffer(8, { maxByteLength: 16 });
        const outOfBounds = new Uint8Array(resizable, 2, 4);
        resizable.resize(1);
        let outOfBoundsRejected = false;
        try {
          await env.B.put(`${key}.oob`, outOfBounds);
        } catch (error) {
          if (!(error instanceof TypeError)) throw error;
          outOfBoundsRejected = true;
        }

        const stored = await env.B.get(key);
        const queuedStreamStored = await env.B.get(queuedStreamKey);
        const fixedStreamStored = await env.B.get(fixedStreamKey);
        const resizableStreamStored = await env.B.get(resizableStreamKey);
        const blobStored = await env.B.get(blobKey);
        const absent = await env.B.get(`${key}.oob`);
        return json({
          size: meta.size,
          text: await stored.text(),
          queuedStreamBytes: Array.from(new Uint8Array(
            await queuedStreamStored.arrayBuffer()
          )),
          fixedStreamBytes: Array.from(new Uint8Array(
            await fixedStreamStored.arrayBuffer()
          )),
          resizableStreamBytes: Array.from(new Uint8Array(
            await resizableStreamStored.arrayBuffer()
          )),
          blobText: await blobStored.text(),
          outOfBoundsRejected,
          outOfBoundsAbsent: absent === null,
        });
      }

      if (request.method === "GET" && path === "conditional") {
        const head = await env.B.head(key);
        if (!head) return new Response("missing", { status: 404 });
        const matched = await env.B.get(key, {
          onlyIf: { etagMatches: ["not-the-etag", head.etag] },
        });
        const missed = await env.B.get(key, {
          onlyIf: { etagMatches: ["not-the-etag"] },
        });
        return json({
          matched: objectShape(matched, await matched.text()),
          missedHasBody: "body" in missed,
          missedSize: missed.size,
          missedEtag: missed.etag,
        });
      }

      if (request.method === "DELETE" && path === "object") {
        await env.B.delete([key]);
        return new Response("deleted");
      }

      return new Response("not found", { status: 404 });
    } catch (err) {
      return new Response(`err: ${err.stack || err.message}`, { status: 500 });
    }
  },
};
