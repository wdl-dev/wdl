// Pairs with load-runtime-dispatch.js — that one builds the module graph,
// this one builds the call-site shapes the SUT consumes.

import assert from "node:assert/strict";
import { installMockFetch, makeRecordingFetch } from "./mock-fetch.js";
import { jsonRequest as makeJsonRequest, parseJsonObjectRequestBody } from "./request-body.js";

export function makeScope() {
  return {
    /** @type {unknown[]} */
    errors: [],
    /** @type {string | undefined} */
    requestId: undefined,
    /** @param {unknown} err */
    markError(err) {
      this.errors.push(err);
    },
    /** @param {Response} response */
    respond(response) {
      return response;
    },
  };
}

/**
 * @param {any} entrypoint
 * @param {{ onGetEntrypoint?: (options: { props?: Record<string, unknown> } | undefined) => void }} [stubOptions]
 */
export function makeStub(entrypoint, stubOptions = {}) {
  return {
    /** @param {string} [name] @param {{ props?: Record<string, unknown> }} [options] */
    getEntrypoint(name, options) {
      stubOptions.onGetEntrypoint?.(options);
      if (name && entrypoint?.entrypoints) {
        return entrypoint.entrypoints[name];
      }
      return entrypoint;
    },
  };
}

/** @param {new (ctx: any, env: any) => unknown} [WorkflowInfrastructureReporter] */
export function makeCtx(WorkflowInfrastructureReporter) {
  /** @type {Promise<unknown>[]} */
  const tasks = [];
  return {
    /** @param {Promise<unknown>} promise */
    waitUntil(promise) {
      tasks.push(promise);
    },
    exports: WorkflowInfrastructureReporter
      ? {
          /** @param {{ props: Record<string, unknown> }} options */
          WorkflowInfrastructureReporter(options) {
            return new WorkflowInfrastructureReporter(
              { props: options.props },
              {}
            );
          },
        }
      : {},
    tasks,
  };
}

/** @param {unknown} body */
export function jsonRequest(body) {
  return makeJsonRequest("http://runtime.test/_dispatch", body, { method: "POST" });
}

/**
 * @typedef {Record<string, unknown> & {
 *   attempt?: unknown,
 *   config?: unknown,
 *   createdAtMs?: unknown,
 *   nameCount?: unknown,
 *   output?: unknown,
 *   stepName?: unknown,
 * }} WorkflowBackendBody
 * @typedef {WorkflowBackendBody & { startOrdinal: number }} WorkflowReplayBody
 * @typedef {{ url: string, body: WorkflowBackendBody, headers: Record<string, string> }} BackendCall
 * @typedef {(url: string, body: WorkflowBackendBody) => Promise<Response>} BackendHandler
 * @typedef {{ replayPage?: (body: WorkflowReplayBody) => Response | Record<string, unknown> }} BackendOptions
 */

/** @param {WorkflowBackendBody} body */
function workflowReplayBody(body) {
  assert.equal(typeof body.startOrdinal, "number", "workflow replay request body must include numeric startOrdinal");
  return /** @type {WorkflowReplayBody} */ (body);
}

/**
 * @param {BackendHandler} handler
 * @param {BackendOptions} [options]
 */
export function makeWorkflowBackend(handler, options = {}) {
  /** @type {BackendCall[]} */
  const calls = [];
  return {
    calls,
    fetch: makeRecordingFetch(calls, {
      capture(_call, url, init) {
        const body = parseJsonObjectRequestBody(init, "workflow backend request body");
        return { url: String(url), body, headers: Object.fromEntries(new Headers(init.headers)) };
      },
      async response(url, _init, call) {
        const backendCall = /** @type {BackendCall} */ (call);
        const body = backendCall.body;
        if (String(url).endsWith("/replay-steps")) {
          const replayBody = workflowReplayBody(body);
          const replayPage = typeof options.replayPage === "function"
            ? options.replayPage(replayBody)
            : { steps: [], nextOrdinal: replayBody.startOrdinal, done: true };
          if (replayPage instanceof Response) return replayPage;
          return Response.json(replayPage);
        }
        return await handler(String(url), body);
      },
    }),
  };
}

/** @typedef {{ url: string, init: any }} TailCall */
/** @typedef {Record<string, unknown> & { path?: string, path_truncated?: boolean }} TailPayload */

/** @param {TailCall[]} calls */
export function tailAppendPayloads(calls) {
  return calls
    .filter((c) => c.url === "http://proxy/logs/tail/append")
    .map((c) => {
      const body = parseJsonObjectRequestBody(c.init, "tail append request body");
      const payload = /** @type {TailPayload} */ (parseJsonObjectRequestBody({ body: body.json }, "tail append payload"));
      return { ns: body.ns, worker: body.worker, payload };
    });
}

/** @param {unknown} activeKeys */
export function installTailFetchSpy(activeKeys) {
  /** @type {Array<{ url: string, init: RequestInit }>} */
  const calls = [];
  const restore = installMockFetch(
    makeRecordingFetch(calls, {
      response: (url) => {
        if (String(url) === "http://proxy/logs/tail/active") {
          return Response.json({ active: activeKeys });
        }
        if (String(url) === "http://proxy/logs/tail/append") {
          return Response.json({ ok: true });
        }
        return Response.json({}, { status: 404 });
      },
    })
  );
  return { calls, restore };
}
