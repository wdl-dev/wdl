import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import http from "node:http";
import { Socket } from "node:net";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";

import { withMockedProperty } from "../helpers/mock-global.js";
import { settlementWithin } from "../helpers/timing.js";
import { adminFetch, fetchWithToken } from "../integration/helpers/admin-http.js";
import { gatewayFetch, hostFetch, rawHttpGet } from "../integration/helpers/gateway-http.js";
import { responseJson } from "../integration/helpers/http-response.js";

const binaryClients = [
  { name: "adminFetch", run: () => adminFetch("/probe") },
  { name: "rawHttpGet", run: () => rawHttpGet("http://localhost/probe") },
  { name: "gatewayFetch", run: () => gatewayFetch("probe", "/probe") },
];
const clients = [
  ...binaryClients,
  { name: "hostFetch", run: () => hostFetch("probe.test", "/probe") },
  { name: "fetchWithToken", run: () => fetchWithToken("test-token", "/probe") },
];

/**
 * @template T
 * @param {() => Promise<T>} run
 * @param {(response: import("node:http").IncomingMessage) => void} deliver
 * @param {number} [status]
 */
async function withHttpResponse(run, deliver, status = 201) {
  const socket = new Socket();
  const response = new http.IncomingMessage(socket);
  response.statusCode = status;
  response.headers = { "content-type": "application/json", "x-probe": "present" };
  // Keep a broken collector's error observable as a pending Promise instead
  // of letting an unhandled stream error terminate the unit-test process.
  response.on("error", () => {});
  /**
   * @param {import("node:http").RequestOptions} _options
   * @param {(response: import("node:http").IncomingMessage) => void} onResponse
   */
  const request = (_options, onResponse) => Object.assign(new EventEmitter(), {
    end() { onResponse(response); },
  });

  try {
    return await withMockedProperty(
      http,
      "request",
      /** @type {typeof http.request} */ (/** @type {unknown} */ (request)),
      async () => {
        const outcome = settlementWithin(run(), 1000);
        await setImmediate();
        deliver(response);
        return await outcome;
      }
    );
  } finally {
    response.destroy();
    socket.destroy();
  }
}

for (const { name, run } of clients) {
  test(`${name} collects split UTF-8 text without changing its response shape`, async () => {
    const payload = Buffer.from('{"value":"\u96ea"}');
    const outcome = await withHttpResponse(async () => run(), (response) => {
      response.push(payload.subarray(0, payload.length - 3));
      response.push(payload.subarray(payload.length - 3));
      response.complete = true;
      response.push(null);
    });
    assert.equal(outcome.status, "fulfilled");
    if (outcome.status !== "fulfilled") return;
    const value = outcome.value;
    assert.equal(value.status, 201);
    if (!("headers" in value)) {
      assert.equal(value.text, payload.toString("utf8"));
      assert.deepEqual(value.json, { value: "\u96ea" });
    } else {
      assert.equal(await value.text(), payload.toString("utf8"));
      assert.deepEqual(await responseJson(value), { value: "\u96ea" });
      if (typeof value.headers.get === "function") {
        assert.equal(value.headers.get("X-Probe"), "present");
      } else {
        assert.ok("x-probe" in value.headers);
        assert.equal(value.headers["x-probe"], "present");
      }
      if ("ok" in value) assert.equal(value.ok, true);
    }
  });

  test(`${name} preserves empty responses`, async () => {
    const outcome = await withHttpResponse(async () => run(), (response) => {
      response.complete = true;
      response.push(null);
    }, 204);
    assert.equal(outcome.status, "fulfilled");
    if (outcome.status !== "fulfilled") return;
    const value = outcome.value;
    assert.equal(value.status, 204);
    assert.equal(typeof value.text === "string" ? value.text : await value.text(), "");
    if (!("headers" in value)) assert.equal(value.json, null);
    if ("jsonOrNull" in value && typeof value.jsonOrNull === "function") {
      assert.equal(await value.jsonOrNull(), null);
    }
  });

  for (const failure of ["after headers", "after partial body", "premature close"]) {
    test(`${name} rejects ${failure}`, async () => {
      const error = new Error("response body aborted");
      const outcome = await withHttpResponse(async () => run(), (response) => {
        if (failure !== "after headers") response.push(Buffer.from("partial"));
        response.destroy(failure === "premature close" ? undefined : error);
      });
      assert.equal(outcome.status, "rejected", "body failure must settle the request Promise");
      if (outcome.status !== "rejected") return;
      if (failure === "premature close") {
        assert.match(String(outcome.reason), /aborted|premature close/i);
      } else {
        assert.equal(outcome.reason, error);
      }
    });
  }
}

for (const { name, run } of binaryClients) {
  test(`${name} preserves binary bytes and reusable arrayBuffer reads`, async () => {
    const payload = Buffer.from([0, 255, 128, 1, 13, 10]);
    const outcome = await withHttpResponse(async () => run(), (response) => {
      response.push(payload.subarray(0, 2));
      response.push(payload.subarray(2));
      response.complete = true;
      response.push(null);
    });
    assert.equal(outcome.status, "fulfilled");
    if (outcome.status !== "fulfilled") return;
    const value = outcome.value;
    const first = await value.arrayBuffer();
    assert.deepEqual(Buffer.from(first), payload);
    new Uint8Array(first).fill(0);
    assert.deepEqual(Buffer.from(await value.arrayBuffer()), payload);
    assert.equal(await value.text(), payload.toString("utf8"));
  });
}
