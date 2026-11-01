function temporalProbe() {
  const type = typeof Temporal;
  if (type === "undefined") return { type };
  return {
    type,
    nextDate: Temporal.PlainDate.from("2024-02-28").add({ days: 2 }).toString(),
    utc: Temporal.Instant.from("2024-02-29T23:30:00Z")
      .toZonedDateTimeISO("UTC").add({ hours: 1 }).toString(),
  };
}

function lockedBodyError(construct) {
  const body = new ReadableStream();
  const reader = body.getReader();
  try {
    construct(body);
    return null;
  } catch (error) {
    return error.name;
  } finally {
    reader.releaseLock();
  }
}

function responseProbe() {
  return {
    statusText: {
      default: new Response(null).statusText,
      notFound: new Response(null, { status: 404 }).statusText,
      custom: new Response(null, { statusText: "Fixture" }).statusText,
    },
    lockedBody: {
      response: lockedBodyError((body) => new Response(body)),
      request: lockedBodyError((body) => new Request("https://response.test/", {
        method: "POST", body,
      })),
    },
  };
}

async function nodeProbe() {
  // These imports are only resolved by the Node-enabled deployment.
  const { createSecretKey, KeyObject } = await import("node:crypto");
  const { createRequire } = await import("node:module");
  const { default: process } = await import("node:process");
  const rawKey = createSecretKey(Uint8Array.of(1, 2, 3, 4, 5));
  const aesKey = await crypto.subtle.importKey(
    "raw", new Uint8Array(16), "AES-GCM", true, ["encrypt"]
  );
  const require = createRequire("/worker.js");
  const requiredProcess = require("node:process");
  return {
    rawKeyBytes: rawKey.symmetricKeySize,
    aesKeyBytes: KeyObject.from(aesKey).symmetricKeySize,
    processMatchesImport: requiredProcess === process,
    bareProcessMatchesPrefixed: require("process") === requiredProcess,
  };
}

export default {
  async fetch(request) {
    switch (new URL(request.url).pathname) {
      case "/temporal": return Response.json(temporalProbe());
      case "/response": return Response.json(responseProbe());
      case "/node": return Response.json(await nodeProbe());
      default: return new Response(null, { status: 404 });
    }
  },
};
