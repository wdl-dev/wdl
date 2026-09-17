import * as fs from "node:fs";
import { open } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { promisify } from "node:util";
import { Readable, Writable, PassThrough, compose } from "node:stream";
import { finished, pipeline } from "node:stream/promises";
import { createServer, get } from "node:http";
import { connect } from "node:net";
import { httpServerHandler } from "cloudflare:node";

async function errorCode(callback) {
  try {
    await callback();
    return null;
  } catch (error) {
    return error.code ?? error.name;
  }
}

async function filesystemProbe() {
  const path = `/tmp/wdl-node-io-${crypto.randomUUID()}`;
  const views = [];
  const truncation = [];
  try {
    for (const mode of ["sync", "callback", "promise"]) {
      fs.writeFileSync(path, "0123456789");
      const handle = await open(path, "r+");
      try {
        const read = mode === "sync"
          ? (buffer, options) => fs.readSync(handle.fd, buffer, options)
          : mode === "callback"
            ? (buffer, options) => promisify(fs.read)(handle.fd, buffer, options)
            : (buffer, options) => handle.read(buffer, options);
        const write = mode === "sync"
          ? (buffer, options) => fs.writeSync(handle.fd, buffer, options)
          : mode === "callback"
            ? (buffer, options) => promisify(fs.write)(handle.fd, buffer, options)
            : (buffer, options) => handle.write(buffer, options);
        const input = Buffer.from("!ABC?").subarray(1, 4);
        const output = Buffer.alloc(8, ".");
        await write(input, { offset: 1, length: 2, position: 0 });
        await read(output.subarray(2, 6), { offset: 1, length: 2, position: 0 });
        views.push({
          mode,
          input: input.toString(),
          output: output.toString(),
          stored: fs.readFileSync(path, "utf8"),
          readBounds: await errorCode(() => read(output.subarray(2, 6), { offset: 3, length: 2, position: 0 })),
          writeBounds: await errorCode(() => write(input, { offset: 2, length: 2, position: 0 })),
        });
      } finally {
        await handle.close();
      }
      for (const flags of ["w", "w+", fs.constants.O_WRONLY | fs.constants.O_TRUNC]) {
        fs.writeFileSync(path, "must be truncated");
        if (mode === "promise") {
          const file = await open(path, flags);
          try {
            truncation.push({ mode, flags, size: (await file.stat()).size });
          } finally {
            await file.close();
          }
        } else {
          const fd = mode === "sync" ? fs.openSync(path, flags) : await promisify(fs.open)(path, flags);
          try {
            truncation.push({ mode, flags, size: fs.fstatSync(fd).size });
          } finally {
            fs.closeSync(fd);
          }
        }
      }
    }
    return { views, truncation };
  } finally {
    fs.unlinkSync(path);
  }
}

async function streamsProbe() {
  const piped = [];
  await pipeline(
    new ReadableStream({ start(controller) {
      controller.enqueue(Buffer.from("one"));
      controller.enqueue(Buffer.from("two"));
      controller.close();
    } }),
    new PassThrough({ highWaterMark: 1 }),
    new WritableStream({ write(chunk) { piped.push(Buffer.from(chunk).toString()); } })
  );

  const batched = [];
  const writable = Writable.fromWeb(new WritableStream({
    write(chunk) { batched.push(Buffer.from(chunk).toString()); },
  }));
  const completed = finished(writable);
  writable.cork();
  writable.write(Buffer.from("first"));
  writable.write(Buffer.from("second"));
  writable.end();
  await completed;

  // A Node tail keeps this composition within the default C++ stream surface.
  const composed = compose(
    new PassThrough(),
    new TransformStream({ transform(chunk, controller) {
      controller.enqueue(Buffer.from(Buffer.from(chunk).toString().toUpperCase()));
    } }),
    new PassThrough()
  );
  const collected = (async () => {
    const chunks = [];
    for await (const chunk of composed) chunks.push(chunk);
    return Buffer.concat(chunks).toString();
  })();
  composed.end("compose");
  return { piped: piped.join(""), batched, composed: await collected };
}

async function streamFailuresProbe() {
  let cancelled = false;
  const failure = new Error("probe sink failed");
  const destinationError = await pipeline(
    new ReadableStream({
      start(controller) { controller.enqueue(Buffer.from("one")); },
      cancel() { cancelled = true; },
    }),
    new WritableStream({ write() { throw failure; } })
  ).then(() => null, (error) => error.message);

  const started = Promise.withResolvers();
  const controller = new AbortController();
  let abortedSource = false;
  const sink = new Writable({ write(_chunk, _encoding, callback) { callback(); } });
  const aborted = pipeline(new ReadableStream({
    pull() { started.resolve(); },
    cancel() { abortedSource = true; },
  }), sink, { signal: controller.signal }).then(() => null, (error) => error.name);
  await started.promise;
  controller.abort();
  const abortName = await aborted;

  const locked = new WritableStream();
  const writer = locked.getWriter();
  let lockedDestination;
  try {
    lockedDestination = await errorCode(() => pipeline(Readable.from(["one"]), locked));
  } finally {
    writer.releaseLock();
  }
  const failedSink = new Writable({ write(_chunk, _encoding, callback) { callback(); } });
  const sourceError = await pipeline(new ReadableStream({
    start(source) { source.error(new Error("probe source failed")); },
  }), failedSink).then(() => null, (error) => error.message);
  return { destinationError, cancelled, abortName, abortedSource, sinkDestroyed: sink.destroyed,
    lockedDestination, sourceError, failedSinkDestroyed: failedSink.destroyed };
}

function networkProbe(request) {
  const url = new URL(request.url);
  const target = new URL(url.searchParams.get("target"));
  return new Promise((resolve) => {
    const failed = (error) => resolve({ outcome: "threw", name: error.name, message: error.message });
    const client = url.pathname === "/network/http"
      ? get(target, (response) => {
        response.resume();
        resolve({ outcome: "ok", status: response.statusCode });
      })
      : connect({ host: target.hostname, port: Number(target.port) }, () => {
        client.destroy();
        resolve({ outcome: "ok" });
      });
    client.once("error", failed);
    client.setTimeout(5000, () => client.destroy(new Error("network probe timed out")));
  });
}

const server = createServer(async (request, response) => {
  if (request.url === "/http/reject") throw new Error("expected async Node HTTP listener failure");
  if (request.url === "/http/echo") {
    await pipeline(request, response);
    return;
  }
  if (request.url === "/http/buffer") {
    const chunk = new TextEncoder().encode("before");
    response.flushHeaders();
    response.write(chunk, () => {
      chunk.fill(120);
      response.end("after");
    });
    return;
  }
  response.end("node-http-ok");
});
const nodeHandler = httpServerHandler(server);

export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    if (path.startsWith("/http/")) return await nodeHandler.fetch(request, env, ctx);
    if (path.startsWith("/network/")) return Response.json(await networkProbe(request));
    if (path === "/fs") return Response.json(await filesystemProbe());
    if (path === "/streams") return Response.json(await streamsProbe());
    if (path === "/stream-failures") return Response.json(await streamFailuresProbe());
    return new Response(null, { status: 404 });
  },
};
