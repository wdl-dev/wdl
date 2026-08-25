import { loadDoProtocol } from "./load-do-protocol.js";

const { DO_OWNERSHIP_ERROR_CONTROL_HEADER } = await loadDoProtocol();

/**
 * @param {{
 *   ownerKey?: string,
 *   taskId?: string,
 *   endpoint?: string,
 *   generation?: number,
 * }} [options]
 */
export function doOwnerHintHeaders({
  ownerKey = "do_0123456789abcdef0123456789abcdef:Room:shard0",
  taskId = "do-runtime-a",
  endpoint = "do-runtime-a:8788",
  generation = 3,
} = {}) {
  return {
    "x-wdl-do-owner-key": ownerKey,
    "x-wdl-do-owner-task-id": taskId,
    "x-wdl-do-owner-endpoint": endpoint,
    "x-wdl-do-owner-generation": String(generation),
    "x-wdl-do-owner-hint": "1",
  };
}

/**
 * Final owner responses carry learnable metadata without the private handoff marker.
 * @param {Parameters<typeof doOwnerHintHeaders>[0]} [options]
 */
export function doOwnerMetadataHeaders(options = {}) {
  const headers = doOwnerHintHeaders(options);
  Reflect.deleteProperty(headers, "x-wdl-do-owner-hint");
  return headers;
}

/**
 * @param {Parameters<typeof doOwnerHintHeaders>[0]} [options]
 */
export function doOwnerHintResponse(options = {}) {
  const headers = doOwnerHintHeaders(options);
  return Response.json({
    error: "do_owner_hint",
    message: "Durable Object owner is remote; retry the owner endpoint",
    owner: {
      ownerKey: headers["x-wdl-do-owner-key"],
      taskId: headers["x-wdl-do-owner-task-id"],
      endpoint: headers["x-wdl-do-owner-endpoint"],
      generation: Number(headers["x-wdl-do-owner-generation"]),
    },
  }, {
    status: 409,
    headers,
  });
}

/** @param {string} code @param {HeadersInit | undefined} [headers] */
export function doOwnershipErrorHeaders(code, headers = undefined) {
  const out = new Headers(headers);
  out.set(DO_OWNERSHIP_ERROR_CONTROL_HEADER, code);
  return out;
}

export function tenantBodyDoOwnerHintResponse() {
  return Response.json({
    error: "do_owner_hint",
    message: "tenant body",
    owner: {
      ownerKey: "do_0123456789abcdef0123456789abcdef:Room:shard0",
      taskId: "redis-proxy-user",
      endpoint: "redis-proxy-user:7070/runtime/load?ignore=",
      generation: 3,
    },
  }, { status: 409 });
}
