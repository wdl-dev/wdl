// Minimal RFC 6455 framing for integration tests. Keeping this local avoids
// adding `ws` just to verify platform upgrade paths.

import crypto from "node:crypto";
import { readFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";

import { ROOT, GATEWAY_HOST, GATEWAY_PORT } from "./env.js";
import { runProbeNode } from "./compose.js";
import { parseJsonText, parseStdoutJson } from "./json-payload.js";

const INTERNAL_AUTH_HEADER = "x-wdl-internal-auth";
const INTERNAL_AUTH_TOKEN = process.env.WDL_INTERNAL_AUTH_TOKEN || "local-internal-auth-token";

/** @param {Record<string, string>} headers */
function internalWebSocketHeaders(headers) {
  /** @type {Record<string, string>} */
  const out = { ...headers };
  const hasInternalAuth = Object.keys(out).some((key) => key.toLowerCase() === INTERNAL_AUTH_HEADER);
  if (!hasInternalAuth) out[INTERNAL_AUTH_HEADER] = INTERNAL_AUTH_TOKEN;
  return out;
}

/** @param {number} opcode @param {Buffer} payload */
function encodeClientFrame(opcode, payload) {
  const len = payload.length;
  if ((opcode & 0x08) !== 0 && len > 125) {
    throw new Error("WebSocket control frame payload exceeds 125 bytes");
  }
  const mask = crypto.randomBytes(4);
  let header;
  if (len < 126) {
    header = Buffer.from([0x80 | opcode, 0x80 | len]);
  } else if (len <= 0xffff) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  const masked = Buffer.alloc(len);
  for (let i = 0; i < len; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}

/** @param {string} text */
export function encodeClientTextFrame(text) {
  return encodeClientFrame(0x1, Buffer.from(text, "utf8"));
}

/** @param {Uint8Array} bytes */
export function encodeClientBinaryFrame(bytes) {
  return encodeClientFrame(0x2, Buffer.from(bytes));
}

export function encodeClientCloseFrame(code = 1000, reason = "") {
  const reasonBytes = Buffer.from(reason, "utf8");
  const payload = Buffer.alloc(2 + reasonBytes.length);
  payload.writeUInt16BE(code, 0);
  reasonBytes.copy(payload, 2);
  return encodeClientFrame(0x8, payload);
}

export function encodeClientCloseFrameWithoutStatus() {
  return encodeClientFrame(0x8, Buffer.alloc(0));
}

// TCP doesn't guarantee one `data` event == one WebSocket frame; accumulate
// chunks until the complete frame is available.
/**
 * @param {import("node:net").Socket} socket
 * @param {number} expectedOpcode
 * @param {string} frameType
 * @param {number} timeoutMs
 * @returns {Promise<Buffer>}
 */
function readOneServerFrame(socket, expectedOpcode, frameType, timeoutMs) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for ${frameType} frame (have ${buf.length} bytes)`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("end", onEnd);
    }
    /** @param {unknown} err */
    function onError(err) { cleanup(); reject(err); }
    function onEnd() { cleanup(); reject(new Error("socket ended before full frame")); }
    /** @param {Buffer} chunk */
    function onData(chunk) {
      buf = Buffer.concat([buf, chunk]);
      if (buf.length < 2) return;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      if (opcode !== expectedOpcode) { cleanup(); reject(new Error(`expected ${frameType} frame, got opcode ${opcode}`)); return; }
      if (masked) { cleanup(); reject(new Error("server frames must be unmasked")); return; }
      if (frameType === "close" && len >= 126) { cleanup(); reject(new Error("close frame payload exceeds 125 bytes")); return; }
      let offset = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        cleanup();
        reject(new Error("test websocket helper does not handle 64-bit payload lengths"));
        return;
      }
      const need = offset + len;
      if (buf.length < need) return;
      const payload = buf.subarray(offset, need);
      cleanup();
      if (buf.length > need) {
        reject(new Error(`unexpected trailing ${buf.length - need} bytes after frame`));
        return;
      }
      resolve(payload);
    }

    socket.on("data", onData);
    socket.on("error", onError);
    socket.on("end", onEnd);
  });
}

/** @param {import("node:net").Socket} socket @param {{ timeoutMs?: number }} [opts] */
export async function readOneServerTextFrame(socket, { timeoutMs = 5000 } = {}) {
  const payload = await readOneServerFrame(socket, 0x1, "text", timeoutMs);
  return payload.toString("utf8");
}

/** @param {import("node:net").Socket} socket @param {{ timeoutMs?: number }} [opts] */
export async function readOneServerBinaryFrame(socket, { timeoutMs = 5000 } = {}) {
  return await readOneServerFrame(socket, 0x2, "binary", timeoutMs);
}

/** @param {string} text @param {string} [label] @returns {any} */
export function frameJson(text, label = "WebSocket text frame") {
  return parseJsonText(text, label);
}

/** @param {import("node:net").Socket} socket @param {{ timeoutMs?: number, label?: string }} [opts] */
export async function readJsonServerFrame(socket, { timeoutMs = 5000, label = "WebSocket text frame" } = {}) {
  return frameJson(await readOneServerTextFrame(socket, { timeoutMs }), label);
}

/** @param {import("node:net").Socket} socket @param {{ timeoutMs?: number }} [opts] */
export async function readOneServerCloseFrame(socket, { timeoutMs = 12_000 } = {}) {
  const payload = await readOneServerFrame(socket, 0x8, "close", timeoutMs);
  const code = payload.length >= 2 ? payload.readUInt16BE(0) : null;
  const reason = payload.length > 2 ? payload.subarray(2).toString("utf8") : "";
  return { code, reason };
}

/** @param {string} ns @param {string} pathWithQuery */
export function wsHandshake(ns, pathWithQuery) {
  return wsHandshakeWithHost(`${ns}.workers.local`, pathWithQuery);
}

/** @param {string} host @param {string} pathWithQuery */
export function hostWsHandshake(host, pathWithQuery) {
  return wsHandshakeWithHost(host, pathWithQuery);
}

/** @param {string} hostHeader @param {string} pathWithQuery */
function wsHandshakeWithHost(hostHeader, pathWithQuery) {
  const key = crypto.randomBytes(16).toString("base64");
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: GATEWAY_HOST,
      port: GATEWAY_PORT,
      method: "GET",
      path: pathWithQuery,
      headers: {
        Host: hostHeader,
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": key,
      },
      agent: false,
    });
    req.on("upgrade", (res, socket, head) => {
      resolve({ status: res.statusCode, headers: res.headers, socket, head });
    });
    req.on("response", (res) => {
      /** @type {Buffer[]} */
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        reject(Object.assign(
          new Error(`expected 101, got ${res.statusCode}`),
          { status: res.statusCode, body: Buffer.concat(chunks).toString("utf8") },
        ));
      });
    });
    req.on("error", reject);
    req.end();
  });
}

/**
 * @param {string} host
 * @param {number} port
 * @param {string} pathWithQuery
 * @param {Record<string, string>} headers
 * @param {string} message
 */
export function serviceWebSocketRoundTrip(host, port, pathWithQuery, headers, message) {
  const rewritten = Buffer.from(JSON.stringify({
    host,
    port,
    path: pathWithQuery,
    headers: internalWebSocketHeaders(headers),
    message,
  })).toString("base64");
  const script = readFileSync(path.join(ROOT, "tests/integration/helpers/ws-roundtrip-runner.cjs"), "utf8");
  const out = runProbeNode(script, { env: { WDL_WS_REQ: rewritten } });
  const parsed = parseStdoutJson(out, "internal websocket round trip stdout");
  if (parsed.error) throw new Error(`internal websocket round trip failed: ${parsed.error}`);
  return parsed;
}
