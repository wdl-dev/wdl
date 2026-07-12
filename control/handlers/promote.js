import {
  jsonResponse,
  jsonError,
  readJsonBody,
  formatError,
  codedErrorResponse,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { parseVersion } from "shared-version";
import { promoteWithRoutes, RoutingError } from "control-routing";
import { platformDomainFromEnv } from "control-lib";

/**
 * @param {{ request: Request, env: Record<string, unknown>, ns: string, name: string, requestId: string }} args
 */
export async function handle({ request, env, ns, name, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();
  const parsed = await readJsonBody(request, { requireObject: true });
  if (parsed.response) return parsed.response;
  const body = /** @type {Record<string, unknown>} */ (parsed.body);
  if (typeof body.version !== "string" || !body.version) {
    return jsonError(400, "invalid_request", "Missing 'version' in body");
  }
  if (parseVersion(body.version) == null) {
    return jsonError(400, "invalid_request", `Invalid version "${body.version}" (expected "v<int>")`);
  }

  try {
    const platformDomain = platformDomainFromEnv(env);
    const result = await promoteWithRoutes(redis, ns, name, body.version, { log, requestId });
    log("info", "worker_promoted", {
      request_id: requestId,
      namespace: ns,
      worker: name,
      version: body.version,
      affected_hosts: result.affectedHosts.length,
    });
    return jsonResponse(200, {
      namespace: ns,
      name,
      version: body.version,
      active: true,
      affectedHosts: result.affectedHosts,
      platformDomain,
    });
  } catch (err) {
    if (err instanceof RoutingError) {
      log("warn", "worker_promote_rejected", {
        request_id: requestId,
        namespace: ns,
        worker: name,
        version: body.version,
        status: err.status,
        ...formatError(err),
        ...(err.details.attempts ? { attempts: err.details.attempts } : {}),
      });
      return codedErrorResponse(err, "routing_error");
    }
    throw err;
  }
}
