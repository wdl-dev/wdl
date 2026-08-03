import { jsonResponse, publishReload } from "control-shared";

/**
 * @typedef {{ ok: boolean, channel: string, duration_ms: number, receivers?: number, error?: string }} PublishResult
 * @typedef {{ ok: boolean, duration_ms: number, declaredHosts?: number, declarationKeysRemoved?: number, error?: string }} RepairResult
 */

/** @param {PublishResult} result */
function publishResultForResponse({ duration_ms: durationMs, ...result }) {
  return { ...result, durationMs: durationMs };
}

/** @param {RepairResult} result */
function repairResultForResponse({ duration_ms: durationMs, ...result }) {
  return { ...result, durationMs: durationMs };
}

/** @param {Awaited<ReturnType<typeof publishReload>>} result */
function reloadResultForResponse(result) {
  const base = {
    ok: result.ok,
    declarations: repairResultForResponse(result.declarations),
  };
  if (result.routes === undefined || result.patterns === undefined) return base;
  return {
    ...base,
    routes: publishResultForResponse(result.routes),
    patterns: publishResultForResponse(result.patterns),
  };
}

/** @param {{ requestId: string }} args */
export async function handle({ requestId }) {
  const publishResult = await publishReload(requestId);
  const status = publishResult.ok ? 200 : 502;
  return jsonResponse(status, { reload: reloadResultForResponse(publishResult) });
}
