import assert from "node:assert/strict";

/**
 * @param {unknown} body
 * @param {RequestInit} [init]
 * @returns {RequestInit}
 */
function jsonRequestInit(body, init = {}) {
  return { ...init, body: JSON.stringify(body) };
}

/**
 * @param {string | URL} url
 * @param {unknown} body
 * @param {RequestInit} [init]
 */
export function jsonRequest(url, body, init = {}) {
  return new Request(url, jsonRequestInit(body, init));
}

/**
 * @param {{ body?: unknown } | RequestInit | undefined} init
 * @param {string} [label]
 */
export function requestBodyString(init, label = "request body") {
  const body = init?.body;
  assert.equal(typeof body, "string", `expected ${label} to be a string`);
  return /** @type {string} */ (body);
}

/**
 * @param {{ body?: unknown } | RequestInit | undefined} init
 * @param {string} [label]
 */
function parseJsonRequestBody(init, label = "request body") {
  const body = requestBodyString(init, label);
  return JSON.parse(body);
}

/**
 * @param {{ body?: unknown } | RequestInit | undefined} init
 * @param {string} [label]
 */
export function parseJsonObjectRequestBody(init, label = "request body") {
  const parsed = parseJsonRequestBody(init, label);
  assert.ok(parsed && typeof parsed === "object" && !Array.isArray(parsed), `expected ${label} to be a JSON object`);
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {{ body?: unknown } | RequestInit | undefined} init
 * @param {string} [label]
 */
export function parseJsonArrayRequestBody(init, label = "request body") {
  const parsed = parseJsonRequestBody(init, label);
  assert.ok(Array.isArray(parsed), `expected ${label} to be a JSON array`);
  return /** @type {unknown[]} */ (parsed);
}
