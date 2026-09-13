import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { generateKeyPairSync } from "node:crypto";
import { BoundSocket } from "node:net";

const storage = new AsyncLocalStorage();
const globalResource = new AsyncResource("wdl-global-probe");
let retained;

function attempt(callback) {
  try {
    return { returned: true, value: callback() ?? null };
  } catch (error) {
    return { returned: false, isError: error instanceof Error, code: error.code ?? null };
  }
}

function captureResource() {
  return storage.run("request-a", () => {
    const resource = new AsyncResource("wdl-request-probe");
    const bound = resource.bind(() => storage.getStore());
    retained = { resource, bound };
    return {
      direct: resource.runInAsyncScope(() => storage.getStore()),
      bound: bound(),
    };
  });
}

function checkResource() {
  if (!retained) throw new Error("AsyncResource capture must precede the next request");
  return storage.run("request-b", () => {
    let callbacks = 0;
    const callback = () => { callbacks += 1; return storage.getStore(); };
    const direct = attempt(() => retained.resource.runInAsyncScope(callback));
    const bound = attempt(() => retained.bound());
    const rebound = attempt(() => retained.resource.bind(callback)());
    return {
      direct, bound, rebound, callbacks,
      global: globalResource.runInAsyncScope(() => storage.getStore() ?? "root"),
      current: storage.getStore(),
    };
  });
}

function keyExportProbe() {
  const rsa = generateKeyPairSync("rsa", { modulusLength: 1024 }).privateKey;
  const ec = generateKeyPairSync("ec", { namedCurve: "prime256v1" }).privateKey;
  const encrypted = { format: "der", cipher: "aes-256-cbc", passphrase: "fixture-passphrase" };
  return {
    pkcs1: attempt(() => rsa.export({ ...encrypted, type: "pkcs1" }).byteLength),
    sec1: attempt(() => ec.export({ ...encrypted, type: "sec1" }).byteLength),
    pkcs8Encrypted: rsa.export({ ...encrypted, type: "pkcs8" }).byteLength > 0,
    sec1Unencrypted: ec.export({ format: "der", type: "sec1" }).byteLength > 0,
  };
}

async function rejectionReentryProbe() {
  const unhandled = Promise.withResolvers();
  const handled = Promise.withResolvers();
  let target;
  let handledEvents = 0;
  const onUnhandled = (event) => {
    if (event.promise !== target) return;
    event.preventDefault();
    unhandled.resolve();
  };
  const onHandled = (event) => {
    if (event.promise !== target) return;
    handledEvents += 1;
    if (handledEvents === 1) event.promise.catch(() => {});
    handled.resolve();
  };
  globalThis.addEventListener("unhandledrejection", onUnhandled);
  globalThis.addEventListener("rejectionhandled", onHandled);
  try {
    target = Promise.reject(new Error("expected native rejection reentry probe"));
    await unhandled.promise;
    target.catch(() => {});
    await handled.promise;
    await new Promise((resolve) => setTimeout(resolve, 10));
    return { handledEvents };
  } finally {
    globalThis.removeEventListener("unhandledrejection", onUnhandled);
    globalThis.removeEventListener("rejectionhandled", onHandled);
  }
}

function boundSocketProbe() {
  const bound = new BoundSocket({ host: "127.0.0.1", port: 0 });
  let address;
  let fd;
  let conflict;
  try {
    address = bound.address();
    fd = bound.fd();
    conflict = attempt(() => {
      const duplicate = new BoundSocket({ host: address.address, port: address.port });
      duplicate.close();
      return true;
    });
  } finally {
    bound.close();
  }
  const reused = new BoundSocket({ host: address.address, port: address.port });
  try {
    return {
      address: address.address,
      family: address.family,
      ephemeralPort: address.port >= 49152 && address.port <= 65535,
      fd,
      conflict,
      reused: reused.address().port === address.port,
    };
  } finally {
    reused.close();
  }
}

export default {
  async fetch(request) {
    switch (new URL(request.url).pathname) {
      case "/capture": return Response.json(captureResource());
      case "/check": return Response.json(checkResource());
      case "/crypto": return Response.json(keyExportProbe());
      case "/rejection-reentry": return Response.json(await rejectionReentryProbe());
      case "/bound-socket": return Response.json(boundSocketProbe());
      default: return new Response(null, { status: 404 });
    }
  },
};
